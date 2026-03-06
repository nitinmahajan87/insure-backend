import asyncio
import os
import logging
from datetime import datetime, timezone, date
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Depends, status, UploadFile, File, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, String, cast

from app.core.database import get_db
from app.core.security import get_current_tenant, TenantContext
from app.core.processor import process_insurer_response, FileParseError, MissingColumnsError
from app.models.models import SyncLog, Employee, SyncStatus, PolicyStatus, SyncLogEvent
from app.models.schemas import InsurerResponseReport, RejectedRow
from app.services.file_service import save_upload_file, FileTooLargeError

logger = logging.getLogger(__name__)
router = APIRouter()

CALLBACK_SECRET = os.getenv("INSURER_CALLBACK_SECRET", "change-me-in-prod")

_APPROVED_STATUSES = {"APPROVED", "ACCEPTED", "ISSUED"}


# ---------------------------------------------------------------------------
# Request schema
# ---------------------------------------------------------------------------

class InsurerCallbackPayload(BaseModel):
    """
    Generic inbound callback schema.
    Each insurer adapter maps its own response format to this before posting here,
    or the insurer posts directly if they support our generic format.
    """
    our_transaction_id: str               # Idempotency-Key we sent them
    insurer_reference_id: Optional[str] = None
    status: str                           # "APPROVED" | "REJECTED" | "PENDING"
    policy_number: Optional[str] = None
    policy_effective_date: Optional[str] = None   # ISO-8601 date string
    rejection_reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Auth dependency
# ---------------------------------------------------------------------------

async def verify_callback_secret(
    x_callback_secret: Optional[str] = Header(default=None),
) -> None:
    """
    Validates the shared secret sent by the insurer.
    Missing or wrong secret → 403 (not 422, which Header(...) would raise).
    """
    if not x_callback_secret or x_callback_secret != CALLBACK_SECRET:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or missing callback secret (x-callback-secret).",
        )


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.post("/insurer/callback", dependencies=[Depends(verify_callback_secret)])
async def handle_insurer_callback(
    payload: InsurerCallbackPayload,
    db: AsyncSession = Depends(get_db),
):
    """
    Receives async policy decisions from insurers.

    Flow:
      1. Locate the SyncLog by our original transaction_id.
      2. Idempotency: if we already processed a callback for this log, return 200.
      3. Resolve the outcome (approved / soft-rejected).
      4. Update SyncLog, Employee, and write an audit event atomically.
    """
    # 1. Locate the originating SyncLog
    result = await db.execute(
        select(SyncLog).where(SyncLog.transaction_id == payload.our_transaction_id)
    )
    log = result.scalars().first()

    if not log:
        # Return 200 — we don't want the insurer to keep retrying an unknown ID.
        logger.warning(
            f"Callback for unknown transaction_id '{payload.our_transaction_id}' — ignored."
        )
        return {"status": "acknowledged", "detail": "transaction not found, ignoring"}

    # 2. Idempotency guard — reject duplicate callbacks silently
    if log.callback_received_at is not None:
        logger.info(
            f"Duplicate callback for transaction '{payload.our_transaction_id}' — already processed."
        )
        return {"status": "acknowledged", "detail": "already processed"}

    # 3. Resolve outcome
    is_approved = payload.status.upper() in _APPROVED_STATUSES
    new_sync_status = SyncStatus.ACTIVE if is_approved else SyncStatus.SOFT_REJECTED
    new_policy_status = PolicyStatus.ISSUED if is_approved else PolicyStatus.SOFT_REJECTED

    # Parse policy_effective_date string → Python date (best-effort)
    policy_date: Optional[date] = None
    if payload.policy_effective_date:
        try:
            policy_date = date.fromisoformat(payload.policy_effective_date)
        except ValueError:
            logger.warning(
                f"Could not parse policy_effective_date "
                f"'{payload.policy_effective_date}' for tx '{payload.our_transaction_id}'."
            )

    # 4a. Update SyncLog
    log.sync_status = new_sync_status
    log.insurer_reference_id = payload.insurer_reference_id
    log.callback_received_at = datetime.now(timezone.utc)
    log.rejection_reason = payload.rejection_reason
    # Store the full callback payload for debugging / audit
    log.raw_response = payload.model_dump()

    # 4b. Update Employee
    emp_code = log.payload.get("employee_code")
    if emp_code:
        emp_result = await db.execute(
            select(Employee).where(
                Employee.corporate_id == log.corporate_id,
                Employee.employee_code == emp_code,
            )
        )
        employee = emp_result.scalars().first()

        if employee:
            employee.delivery_status = new_sync_status
            employee.policy_status = new_policy_status
            employee.insurer_reference_id = payload.insurer_reference_id
            employee.rejection_reason = payload.rejection_reason
            if policy_date:
                employee.policy_effective_date = policy_date
            if is_approved and payload.policy_number:
                employee.policy_number = payload.policy_number

    # 4c. Audit event
    db.add(SyncLogEvent(
        sync_log_id=log.id,
        event_status=new_sync_status,
        actor="INSURER_CALLBACK",
        details={
            "insurer_reference_id": payload.insurer_reference_id,
            "policy_number": payload.policy_number,
            "policy_effective_date": payload.policy_effective_date,
            "rejection_reason": payload.rejection_reason,
        },
        policy_status=new_policy_status.value,
    ))

    await db.commit()
    logger.info(
        f"Callback processed for tx '{payload.our_transaction_id}': {new_sync_status}"
    )
    return {"status": "processed", "new_status": new_sync_status}


# ---------------------------------------------------------------------------
# POST /insurer/process-response-file
# Broker uploads the Excel/CSV response file received from the insurer.
# Bulk-updates policy_status (ISSUED / SOFT_REJECTED) for each matched employee.
# ---------------------------------------------------------------------------

# Statuses eligible for receiving a response — records already in terminal/confirmed
# states (ACTIVE already processed) should not be blindly overwritten.
_AWAITING_RESPONSE_STATUSES = {
    SyncStatus.COMPLETED_OFFLINE,       # Legacy
    SyncStatus.PENDING_OFFLINE,
    SyncStatus.PENDING_BOTH,
    SyncStatus.ACTIVE,
    SyncStatus.BROKER_REVIEW_PENDING,   # Report generated, awaiting insurer response
}


@router.post("/insurer/process-response-file", response_model=InsurerResponseReport)
async def process_insurer_response_file(
    file: UploadFile = File(...),
    corporate_id: str = Query(..., description="Corporate this response file belongs to"),
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
):
    """
    Broker uploads the batch response file received from the insurer.

    Flow:
      1. Validate broker scope + corporate ownership (via TenantContext).
      2. Parse the file — flexible column aliases cover all major Indian insurer formats.
      3. For each valid row, find the most recent open SyncLog for that employee.
      4. Apply ISSUED or SOFT_REJECTED to SyncLog, Employee, and SyncLogEvent.
      5. Return a summary report so the broker can see what was applied.
    """
    # 1. Broker-only endpoint
    if not tenant.is_broker_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only broker-admin API keys can upload insurer response files.",
        )

    # TenantContext already validated corporate ownership when it resolved corporate_id.
    corporate = tenant.corporate  # raises HTTP 400 if corporate_id was missing (never here)

    # 2. Save upload to temp file
    try:
        temp_path = await save_upload_file(file)
    except FileTooLargeError as exc:
        raise HTTPException(status_code=413, detail=str(exc))

    # 3. Parse file
    try:
        valid_rows, parse_errors = await asyncio.to_thread(
            process_insurer_response, temp_path
        )
    except MissingColumnsError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except FileParseError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        try:
            os.unlink(temp_path)
        except OSError:
            pass

    if not valid_rows:
        raise HTTPException(
            status_code=422,
            detail={
                "message": "No valid rows found in the response file.",
                "parse_errors": [e.model_dump() for e in parse_errors],
            },
        )

    # 4. Apply each row to the DB
    issued = 0
    soft_rejected = 0
    unmatched = 0
    now = datetime.now(timezone.utc)

    for row in valid_rows:
        is_approved = row.status in _APPROVED_STATUSES

        # Find the most recent open SyncLog for this employee in this corporate
        log_result = await db.execute(
            select(SyncLog)
            .where(
                SyncLog.corporate_id == corporate.id,
                cast(SyncLog.payload["employee_code"], String) == row.employee_code,
                SyncLog.sync_status.in_(list(_AWAITING_RESPONSE_STATUSES)),
                SyncLog.callback_received_at.is_(None),
            )
            .order_by(SyncLog.timestamp.desc())
            .limit(1)
        )
        log = log_result.scalars().first()

        if not log:
            unmatched += 1
            logger.info(
                f"Insurer response: no open SyncLog for emp '{row.employee_code}' "
                f"in corporate '{corporate.id}' — skipping."
            )
            continue

        # Determine new statuses
        new_sync_status   = SyncStatus.ACTIVE        if is_approved else SyncStatus.SOFT_REJECTED
        new_policy_status = PolicyStatus.ISSUED       if is_approved else PolicyStatus.SOFT_REJECTED

        # Update SyncLog
        log.sync_status          = new_sync_status
        log.insurer_reference_id = row.insurer_reference_id
        log.callback_received_at = now
        log.rejection_reason     = row.rejection_reason
        log.policy_status        = new_policy_status.value

        # Update Employee
        emp_result = await db.execute(
            select(Employee).where(
                Employee.corporate_id == corporate.id,
                Employee.employee_code == row.employee_code,
            )
        )
        employee = emp_result.scalars().first()
        if employee:
            employee.delivery_status     = new_sync_status
            employee.policy_status       = new_policy_status
            employee.insurer_reference_id = row.insurer_reference_id
            employee.rejection_reason    = row.rejection_reason
            if row.effective_date:
                employee.policy_effective_date = row.effective_date
            if is_approved and row.policy_number:
                employee.policy_number = row.policy_number

        # Audit trail
        db.add(SyncLogEvent(
            sync_log_id  = log.id,
            event_status = new_sync_status,
            actor        = f"INSURER_RESPONSE_FILE:{tenant.broker.name}",
            details      = {
                "insurer_reference_id": row.insurer_reference_id,
                "policy_number":        row.policy_number,
                "certificate_number":   row.certificate_number,
                "rejection_reason":     row.rejection_reason,
                "uploaded_by_broker":   tenant.broker.name,
            },
            policy_status = new_policy_status.value,
        ))

        if is_approved:
            issued += 1
        else:
            soft_rejected += 1

    await db.commit()

    total = len(valid_rows)
    logger.info(
        f"Insurer response file processed for corporate '{corporate.id}': "
        f"issued={issued}, soft_rejected={soft_rejected}, unmatched={unmatched}, "
        f"parse_errors={len(parse_errors)}"
    )

    return InsurerResponseReport(
        total_rows          = total + len(parse_errors),
        issued_count        = issued,
        soft_rejected_count = soft_rejected,
        unmatched_count     = unmatched,
        parse_error_count   = len(parse_errors),
        message             = (
            f"Processed {total} rows: {issued} issued, {soft_rejected} rejected, "
            f"{unmatched} unmatched"
            + (f", {len(parse_errors)} could not be parsed" if parse_errors else "")
            + "."
        ),
        parse_errors        = parse_errors,
    )
