import os
import logging
from datetime import datetime, timezone, date
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Depends, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.database import get_db
from app.models.models import SyncLog, Employee, SyncStatus, PolicyStatus, SyncLogEvent

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
    ))

    await db.commit()
    logger.info(
        f"Callback processed for tx '{payload.our_transaction_id}': {new_sync_status}"
    )
    return {"status": "processed", "new_status": new_sync_status}
