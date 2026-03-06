from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc
from app.core.database import get_db
from app.core.security import get_current_tenant, TenantContext
from app.models.models import SyncLog, SyncStatus, SyncLogEvent
from app.tasks.sync_tasks import process_sync_event
from app.services.employee_service import record_audit_event_async  # Import your new helper

router = APIRouter()


@router.get("/transactions")
async def get_transactions(
        limit: int = Query(50, ge=1, le=100, description="Items per page"),
        offset: int = Query(0, ge=0, description="Pagination offset"),
        tenant: TenantContext = Depends(get_current_tenant),
        db: AsyncSession = Depends(get_db)
):
    """
    The Ledger: Returns all successfully processed real-time and hybrid transactions.
    """
    # 1. Base query for successful syncs
    base_query = select(SyncLog).where(
        SyncLog.corporate_id == tenant.corporate.id,
        SyncLog.sync_status.in_([
            SyncStatus.ACTIVE,
            SyncStatus.COMPLETED_BOTH,
            SyncStatus.COMPLETED_OFFLINE,
            SyncStatus.SOFT_REJECTED,
            SyncStatus.PENDING_BOTH,
            SyncStatus.PENDING_OFFLINE,
            SyncStatus.BROKER_REVIEW_PENDING,
        ])
    )

    # 2. Get total count for UI pagination controls
    count_stmt = select(func.count()).select_from(base_query.subquery())
    total_result = await db.execute(count_stmt)
    total_count = total_result.scalar() or 0

    # 3. Fetch paginated data
    stmt = base_query.order_by(desc(SyncLog.timestamp)).offset(offset).limit(limit)
    result = await db.execute(stmt)
    logs = result.scalars().all()

    return {
        "data": [
            {
                "id": log.id,
                "transaction_type": log.transaction_type,
                "source": log.source,
                "employee_code": log.payload.get("employee_code"),
                "status": log.sync_status,
                "policy_status": log.policy_status,
                "insurer_reference_id": log.insurer_reference_id,
                "rejection_reason": log.rejection_reason,
                "callback_received_at": log.callback_received_at,
                "timestamp": log.timestamp,
                "raw_response": log.raw_response,
                "is_force": log.is_force,
            }
            for log in logs
        ],
        "pagination": {
            "total": total_count,
            "limit": limit,
            "offset": offset
        }
    }


@router.get("/errors")
async def get_errors(
        limit: int = Query(50, ge=1, le=100),
        offset: int = Query(0, ge=0),
        tenant: TenantContext = Depends(get_current_tenant),
        db: AsyncSession = Depends(get_db)
):
    """
    The Audit Trail: Returns all failed transactions with their error messages.
    """
    base_query = select(SyncLog).where(
        SyncLog.corporate_id == tenant.corporate.id,
        SyncLog.sync_status == SyncStatus.FAILED
    )

    count_stmt = select(func.count()).select_from(base_query.subquery())
    total_result = await db.execute(count_stmt)
    total_count = total_result.scalar() or 0

    stmt = base_query.order_by(desc(SyncLog.timestamp)).offset(offset).limit(limit)
    result = await db.execute(stmt)
    logs = result.scalars().all()

    return {
        "data": [
            {
                "id": log.id,
                "transaction_type": log.transaction_type,
                "source": log.source,  # 👈 Added Source (ONLINE/BATCH)
                "employee_code": log.payload.get("employee_code"),
                "error_message": log.error_message,
                "retry_count": log.retry_count,
                "timestamp": log.timestamp,
                "payload": log.payload
            }
            for log in logs
        ],
        "pagination": {
            "total": total_count,
            "limit": limit,
            "offset": offset
        }
    }


@router.post("/{log_id}/retry")
async def retry_failed_log(
        log_id: int,
        tenant: TenantContext = Depends(get_current_tenant),
        db: AsyncSession = Depends(get_db)
):
    """
    The Safety Net: Re-queues a failed transaction directly into Celery.
    Records a 'MANUAL_RETRY' event in the ledger.
    """
    stmt = select(SyncLog).where(
        SyncLog.id == log_id,
        SyncLog.corporate_id == tenant.corporate.id
    )
    result = await db.execute(stmt)
    log = result.scalars().first()

    if not log:
        raise HTTPException(status_code=404, detail="Log not found.")

    # Reset the status so it can be picked up cleanly
    log.sync_status = SyncStatus.PENDING
    log.error_message = None

    # 🚨 NEW: Log the manual intervention in the immutable ledger
    await record_audit_event_async(
        db=db,
        log_id=log.id,
        status=SyncStatus.PENDING,
        actor="HR_USER_MANUAL_RETRY",
        details={"note": "Retry triggered via HR Portal API"}
    )

    await db.commit()

    # Dispatch to Celery worker
    process_sync_event.delay(log.id)

    return {
        "message": f"Log {log_id} has been re-queued for processing.",
        "status": "pending"
    }


# 🚨 NEW ROUTE: The Timeline API
@router.get("/employee/{employee_code}/history")
async def get_employee_history(
        employee_code: str,
        tenant: TenantContext = Depends(get_current_tenant),
        db: AsyncSession = Depends(get_db)
):
    """
    Full 360° history for an employee — all transactions (Add, Update, Remove)
    ordered chronologically with nested events for each transaction.
    """
    # Filter by employee_code in Python — JSON column uses -> (returns quoted value),
    # not ->> (plain text), so DB-level string comparison is unreliable without JSONB.
    stmt = (
        select(SyncLog)
        .where(SyncLog.corporate_id == tenant.corporate.id)
        .order_by(SyncLog.timestamp.asc())
    )
    result = await db.execute(stmt)
    all_logs = result.scalars().all()
    logs = [log for log in all_logs if log.payload and log.payload.get("employee_code") == employee_code]

    if not logs:
        raise HTTPException(status_code=404, detail="No history found for this employee.")

    transactions = []
    for log in logs:
        events_stmt = (
            select(SyncLogEvent)
            .where(SyncLogEvent.sync_log_id == log.id)
            .order_by(SyncLogEvent.timestamp.asc())
        )
        events_result = await db.execute(events_stmt)
        events = events_result.scalars().all()

        transactions.append({
            "log_id": log.id,
            "transaction_id": log.transaction_id,
            "transaction_type": log.transaction_type,
            "source": log.source,
            "status": log.sync_status,
            "policy_status": log.policy_status,
            "is_force": log.is_force,
            "timestamp": log.timestamp,
            "events": [
                {
                    "status": e.event_status,
                    "actor": e.actor,
                    "timestamp": e.timestamp,
                    "details": e.details,
                    "policy_status": e.policy_status,
                }
                for e in events
            ],
        })

    return {
        "employee_code": employee_code,
        "total_transactions": len(transactions),
        "transactions": transactions,
    }


@router.get("/{log_id}/history")
async def get_log_history(
        log_id: int,
        tenant: TenantContext = Depends(get_current_tenant),
        db: AsyncSession = Depends(get_db)
):
    """
    Returns the full chronological history (audit trail) for a specific transaction.
    Used by the UI to draw the 'Status Timeline'.
    """
    # 1. Security Check: Ensure log belongs to this corporate
    stmt = select(SyncLog).where(
        SyncLog.id == log_id,
        SyncLog.corporate_id == tenant.corporate.id
    )
    result = await db.execute(stmt)
    log = result.scalars().first()

    if not log:
        raise HTTPException(status_code=404, detail="Log not found.")

    # 2. Fetch all events for this log, ordered by time
    events_stmt = select(SyncLogEvent).where(
        SyncLogEvent.sync_log_id == log_id
    ).order_by(SyncLogEvent.timestamp.asc())

    events_result = await db.execute(events_stmt)
    events = events_result.scalars().all()

    return {
        "transaction_id": log.transaction_id,
        "employee_code": log.payload.get("employee_code"),
        "current_status": log.sync_status,
        "timeline": [
            {
                "status": event.event_status,
                "actor": event.actor,
                "timestamp": event.timestamp,
                "details": event.details,
                "policy_status": event.policy_status,
            }
            for event in events
        ]
    }