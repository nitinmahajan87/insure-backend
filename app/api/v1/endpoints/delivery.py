# app/api/v1/endpoints/delivery.py

import asyncio
import os

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import case, desc, func, select, update
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from app.core.database import get_db
from app.core.security import get_current_tenant, TenantContext
from app.core.storage import get_storage
from app.models.models import SyncLog, Employee, SyncStatus, PolicyStatus, SyncLogEvent
from app.services.outbound_service import OutboundTransformer
from fastapi import status as http_status

router = APIRouter()
BASE_PATH = os.getenv("BASE_OUTBOUND_PATH", "/app/outbound_files")


def _require_broker_admin(tenant: TenantContext) -> None:
    """Raises 403 if the caller is not a broker-admin (BROKER-scoped) key."""
    if not tenant.is_broker_admin:
        raise HTTPException(
            status_code=http_status.HTTP_403_FORBIDDEN,
            detail=(
                "Dispatching to the insurer requires a broker-admin API key. "
                "HR users may preview the report but only the broker can dispatch."
            ),
        )


@router.post("/generate-offline-report")
async def generate_offline_report(
        request: Request,
        tenant: TenantContext = Depends(get_current_tenant),
        db: AsyncSession = Depends(get_db)
):
    _require_broker_admin(tenant)
    try:
        # 1. Fetch pending sync logs for this corporate
        stmt = select(SyncLog).where(
            SyncLog.corporate_id == tenant.corporate.id,
            SyncLog.sync_status.in_([
                SyncStatus.PENDING_OFFLINE,
                SyncStatus.PENDING_BOTH,
            ])
        )
        result = await db.execute(stmt)
        pending_logs = result.scalars().all()

        if not pending_logs:
            return {"message": "No pending real-time records to sweep.", "download_url": None}

        # 2. Extract JSON payloads and track employee codes
        data = []
        employee_codes = []

        for log in pending_logs:
            data.append(log.payload)
            emp_code = log.payload.get("employee_code")
            if emp_code:
                employee_codes.append(emp_code)

        # 3. Generate File using the universal Transformer (upload to object storage)
        full_directory = os.path.join(BASE_PATH, tenant.corporate.base_folder)
        format_type = getattr(tenant.corporate, 'insurer_format', 'excel')

        s3_key, filename = await asyncio.to_thread(
            OutboundTransformer.to_file,
            data,
            "offline_sweep",
            full_directory,
            format_type,
        )

        # Pre-signed URL for direct download from storage
        download_url = await asyncio.to_thread(get_storage().presigned_url, s3_key)

        # 4. Update SyncLog statuses
        for log in pending_logs:
            # PENDING_BOTH → COMPLETED_BOTH (webhook already sent, offline now dispatched too)
            # All offline-only records → BROKER_REVIEW_PENDING (report generated, broker to send)
            if log.sync_status == SyncStatus.PENDING_BOTH:
                log.sync_status = SyncStatus.COMPLETED_BOTH
                new_status = SyncStatus.COMPLETED_BOTH
            else:
                log.sync_status = SyncStatus.BROKER_REVIEW_PENDING
                new_status = SyncStatus.BROKER_REVIEW_PENDING

            log.file_path = s3_key

            broker_actor = f"BROKER_{tenant.broker.id}" if tenant.broker else "BROKER_UNKNOWN"
            audit_event = SyncLogEvent(
                sync_log_id=log.id,
                event_status=new_status,
                actor=broker_actor,
                policy_status=log.policy_status,
                details={
                    "file_generated": filename,
                    "dispatched_by": tenant.broker.name if tenant.broker else "unknown",
                },
            )
            db.add(audit_event)

        # 5. Bulk update Employee delivery_status + policy_status (two queries avoids CASE cast issues)
        if employee_codes:
            # PENDING_BOTH employees: webhook already done, offline now dispatched
            await db.execute(
                update(Employee)
                .where(Employee.corporate_id == tenant.corporate.id)
                .where(Employee.employee_code.in_(employee_codes))
                .where(Employee.delivery_status == SyncStatus.PENDING_BOTH)
                .values(delivery_status=SyncStatus.COMPLETED_BOTH)
            )
            # All other offline employees: report generated, awaiting broker to send
            await db.execute(
                update(Employee)
                .where(Employee.corporate_id == tenant.corporate.id)
                .where(Employee.employee_code.in_(employee_codes))
                .where(Employee.delivery_status != SyncStatus.PENDING_BOTH)
                .values(delivery_status=SyncStatus.BROKER_REVIEW_PENDING)
            )

        # Commit everything in one transaction
        await db.commit()

        return {
            "message": f"Successfully swept {len(data)} pending records into a {format_type.upper()} file.",
            "download_url": download_url,
            "record_count": len(data),
        }
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to generate offline report: {str(e)}")


@router.get("/queue-count")
async def get_queue_count(
        tenant: TenantContext = Depends(get_current_tenant),
        db: AsyncSession = Depends(get_db)
):
    """
    Returns counts the PendingQueuePanel needs to drive its UI state:
      - pending_count:        PENDING_OFFLINE + PENDING_BOTH → records ready to dispatch
      - review_pending_count: BROKER_REVIEW_PENDING → report generated, awaiting Confirm Sent
    """
    result = await db.execute(
        select(
            func.sum(
                case((SyncLog.sync_status.in_([SyncStatus.PENDING_OFFLINE, SyncStatus.PENDING_BOTH]), 1), else_=0)
            ).label("pending_count"),
            func.sum(
                case((SyncLog.sync_status == SyncStatus.BROKER_REVIEW_PENDING, 1), else_=0)
            ).label("review_pending_count"),
        ).where(SyncLog.corporate_id == tenant.corporate.id)
    )
    row = result.one()
    return {
        "pending_count": int(row.pending_count or 0),
        "review_pending_count": int(row.review_pending_count or 0),
    }


@router.get("/preview-offline-report")
async def preview_offline_report(
        request: Request,
        tenant: TenantContext = Depends(get_current_tenant),
        db: AsyncSession = Depends(get_db)
):
    """
    Generates an offline report for review WITHOUT graduating the statuses in the DB.
    """
    try:
        # 1. Fetch pending sync logs for this corporate
        stmt = select(SyncLog).where(
            SyncLog.corporate_id == tenant.corporate.id,
            SyncLog.sync_status.in_([SyncStatus.PENDING_OFFLINE, SyncStatus.PENDING_BOTH])
        )
        result = await db.execute(stmt)
        pending_logs = result.scalars().all()

        if not pending_logs:
            return {"message": "No pending real-time records to preview.", "download_url": None}

        data = [log.payload for log in pending_logs]

        # 3. Generate File (preview_ prefix — HR knows this isn't the final dispatch)
        full_directory = os.path.join(BASE_PATH, tenant.corporate.base_folder)
        format_type = getattr(tenant.corporate, 'insurer_format', 'excel')

        s3_key, filename = await asyncio.to_thread(
            OutboundTransformer.to_file,
            data,
            "preview_sweep",
            full_directory,
            format_type,
        )

        download_url = await asyncio.to_thread(get_storage().presigned_url, s3_key)

        # 🚨 Notice: NO Database updates or commits here! The queue remains untouched.

        return {
            "message": f"Preview generated for {len(data)} pending records.",
            "download_url": download_url,
            "record_count": len(data),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate preview report: {str(e)}")


@router.get("/history")
async def get_delivery_history(
        request: Request,
        tenant: TenantContext = Depends(get_current_tenant),
        db: AsyncSession = Depends(get_db)
):
    """
    Returns a list of all previously generated batch files.
    Groups by file_path to avoid duplicate rows for the same file.
    """
    try:
        # Use SQLAlchemy func to aggregate data per file
        stmt = select(
            SyncLog.file_path,
            func.max(SyncLog.timestamp).label('generated_at'),
            func.count(SyncLog.id).label('record_count'),
            func.sum(
                case((SyncLog.sync_status == SyncStatus.BROKER_REVIEW_PENDING, 1), else_=0)
            ).label('pending_count'),
        ).where(
            SyncLog.corporate_id == tenant.corporate.id,
            SyncLog.file_path.isnot(None)
        ).group_by(
            SyncLog.file_path
        ).order_by(
            desc('generated_at')
        )

        result = await db.execute(stmt)
        rows = result.all()

        storage = get_storage()
        history = []
        for row in rows:
            # file_path is the S3 key (e.g. "outbound/wipro/addition_report_...xlsx")
            # Extract the filename as the last segment for display.
            file_name_only = row.file_path.split('/')[-1] if row.file_path else ""
            s3_key = row.file_path  # already the full S3 key

            try:
                download_url = await asyncio.to_thread(storage.presigned_url, s3_key)
            except Exception:
                download_url = None  # file may have been deleted from storage

            has_pending = (row.pending_count or 0) > 0
            history.append({
                "file_name": file_name_only,
                "file_path": row.file_path,
                "generated_at": row.generated_at,
                "record_count": row.record_count,
                "download_url": download_url,
                "dispatch_status": "BROKER_REVIEW_PENDING" if has_pending else "COMPLETED_OFFLINE",
            })

        return {"data": history}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch history: {str(e)}")


class ConfirmDispatchRequest(BaseModel):
    file_path: Optional[str] = None


@router.post("/confirm-dispatch")
async def confirm_dispatch(
        body: ConfirmDispatchRequest,
        tenant: TenantContext = Depends(get_current_tenant),
        db: AsyncSession = Depends(get_db)
):
    """
    Broker confirms that the generated report file has been physically sent to the insurer.
    Transitions records from BROKER_REVIEW_PENDING → COMPLETED_OFFLINE and sets
    policy_status = PENDING_ISSUANCE (the insurer now has the file).

    Pass file_path to confirm only one specific report; omit to confirm all pending records.
    """
    _require_broker_admin(tenant)
    try:
        stmt = select(SyncLog).where(
            SyncLog.corporate_id == tenant.corporate.id,
            SyncLog.sync_status == SyncStatus.BROKER_REVIEW_PENDING,
        )
        if body.file_path:
            stmt = stmt.where(SyncLog.file_path == body.file_path)

        result = await db.execute(stmt)
        logs = result.scalars().all()

        if not logs:
            return {"message": "No records in BROKER_REVIEW_PENDING state.", "confirmed_count": 0}

        addition_codes = []
        deletion_codes = []
        broker_actor = f"BROKER_{tenant.broker.id}" if tenant.broker else "BROKER_UNKNOWN"

        for log in logs:
            is_deletion = "DELETION" in (log.transaction_type or "")
            confirmed_policy = PolicyStatus.LAPSED.value if is_deletion else PolicyStatus.PENDING_ISSUANCE.value

            log.sync_status = SyncStatus.COMPLETED_OFFLINE
            log.policy_status = confirmed_policy

            emp_code = log.payload.get("employee_code")
            if emp_code:
                if is_deletion:
                    deletion_codes.append(emp_code)
                else:
                    addition_codes.append(emp_code)

            db.add(SyncLogEvent(
                sync_log_id=log.id,
                event_status=SyncStatus.COMPLETED_OFFLINE,
                actor="BROKER_CONFIRMED_DISPATCH",
                details={
                    "note": "Broker confirmed file sent to insurer",
                    "file_path": body.file_path or log.file_path,
                    "confirmed_by": tenant.broker.name if tenant.broker else "unknown",
                },
                policy_status=confirmed_policy,
            ))

        # Additions/updates: set PENDING_ISSUANCE
        if addition_codes:
            await db.execute(
                update(Employee)
                .where(Employee.corporate_id == tenant.corporate.id)
                .where(Employee.employee_code.in_(addition_codes))
                .where(Employee.delivery_status == SyncStatus.BROKER_REVIEW_PENDING)
                .values(
                    delivery_status=SyncStatus.COMPLETED_OFFLINE,
                    policy_status=PolicyStatus.PENDING_ISSUANCE,
                )
            )
        # Deletions: keep LAPSED
        if deletion_codes:
            await db.execute(
                update(Employee)
                .where(Employee.corporate_id == tenant.corporate.id)
                .where(Employee.employee_code.in_(deletion_codes))
                .where(Employee.delivery_status == SyncStatus.BROKER_REVIEW_PENDING)
                .values(
                    delivery_status=SyncStatus.COMPLETED_OFFLINE,
                    policy_status=PolicyStatus.LAPSED,
                )
            )

        await db.commit()
        return {
            "message": f"Confirmed {len(logs)} records dispatched to insurer.",
            "confirmed_count": len(logs),
        }
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to confirm dispatch: {str(e)}")