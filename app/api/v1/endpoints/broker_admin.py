from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.core.database import get_db
from app.core.security import get_current_tenant, TenantContext
from app.models.models import Corporate, Employee, SyncLog, SyncStatus

router = APIRouter()


def _require_broker_admin(tenant: TenantContext) -> None:
    """Raises 403 if the caller is not using a broker-admin (BROKER-scoped) key."""
    if not tenant.is_broker_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This endpoint requires a broker-admin API key (scope=BROKER).",
        )


# ---------------------------------------------------------------------------
# GET /broker/me
# Broker profile + aggregate portfolio summary across all corporates.
# ---------------------------------------------------------------------------
@router.get("/me")
async def get_broker_profile(
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
):
    """
    Broker-admin only.
    Returns the broker profile and aggregate stats across its entire portfolio.
    """
    _require_broker_admin(tenant)
    broker = tenant.broker

    total_corporates = (await db.execute(
        select(func.count(Corporate.id)).where(Corporate.broker_id == broker.id)
    )).scalar() or 0

    total_employees = (await db.execute(
        select(func.count(Employee.id))
        .join(Corporate, Employee.corporate_id == Corporate.id)
        .where(Corporate.broker_id == broker.id)
    )).scalar() or 0

    pending_syncs = (await db.execute(
        select(func.count(SyncLog.id))
        .join(Corporate, SyncLog.corporate_id == Corporate.id)
        .where(
            Corporate.broker_id == broker.id,
            SyncLog.sync_status.in_([SyncStatus.PENDING_OFFLINE, SyncStatus.PENDING_BOTH, SyncStatus.BROKER_REVIEW_PENDING]),
        )
    )).scalar() or 0

    return {
        "broker": {
            "id": broker.id,
            "name": broker.name,
            "allowed_formats": broker.allowed_formats,
        },
        "portfolio": {
            "total_corporates": total_corporates,
            "total_employees": total_employees,
            "pending_syncs": pending_syncs,
        },
    }


# ---------------------------------------------------------------------------
# GET /broker/corporates
# Lists every corporate in the broker's portfolio with per-corporate counts.
# ---------------------------------------------------------------------------
@router.get("/corporates")
async def list_broker_corporates(
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
):
    """
    Broker-admin only.
    Returns every corporate under this broker with a brief summary.
    To operate on a specific corporate, pass ?corporate_id=<id> on other endpoints.
    """
    _require_broker_admin(tenant)

    result = await db.execute(
        select(Corporate).where(Corporate.broker_id == tenant.broker.id)
    )
    corporates = result.scalars().all()

    if not corporates:
        return {"corporates": []}

    corporate_ids = [c.id for c in corporates]

    # Employee counts per corporate — single query
    emp_rows = (await db.execute(
        select(Employee.corporate_id, func.count(Employee.id).label("count"))
        .where(Employee.corporate_id.in_(corporate_ids))
        .group_by(Employee.corporate_id)
    )).all()
    emp_counts = {row.corporate_id: row.count for row in emp_rows}

    # Pending-sync counts per corporate — offline queue only (actionable by broker)
    pending_rows = (await db.execute(
        select(SyncLog.corporate_id, func.count(SyncLog.id).label("count"))
        .where(
            SyncLog.corporate_id.in_(corporate_ids),
            SyncLog.sync_status.in_([SyncStatus.PENDING_OFFLINE, SyncStatus.PENDING_BOTH, SyncStatus.BROKER_REVIEW_PENDING]),
        )
        .group_by(SyncLog.corporate_id)
    )).all()
    pending_counts = {row.corporate_id: row.count for row in pending_rows}

    return {
        "corporates": [
            {
                "id": c.id,
                "name": c.name,
                "hrms_provider": c.hrms_provider,
                "insurer_provider": c.insurer_provider,
                "delivery_channel": c.delivery_channel,
                "employee_count": emp_counts.get(c.id, 0),
                "pending_syncs": pending_counts.get(c.id, 0),
            }
            for c in corporates
        ]
    }


# ---------------------------------------------------------------------------
# GET /broker/corporates/{corporate_id}/summary
# Detailed view of one corporate — enforces broker ownership.
# ---------------------------------------------------------------------------
@router.get("/corporates/{corporate_id}/summary")
async def get_corporate_summary(
    corporate_id: str,
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
):
    """
    Broker-admin only.
    Detailed status breakdown for one corporate in the broker's portfolio.
    """
    _require_broker_admin(tenant)

    corp_result = await db.execute(
        select(Corporate).where(
            Corporate.id == corporate_id,
            Corporate.broker_id == tenant.broker.id,
        )
    )
    corporate = corp_result.scalars().first()

    if not corporate:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Corporate '{corporate_id}' not found in your portfolio.",
        )

    # Sync-status breakdown in one query
    breakdown_rows = (await db.execute(
        select(SyncLog.sync_status, func.count(SyncLog.id).label("count"))
        .where(SyncLog.corporate_id == corporate_id)
        .group_by(SyncLog.sync_status)
    )).all()
    status_breakdown = {str(row.sync_status): row.count for row in breakdown_rows}

    employee_count = (await db.execute(
        select(func.count(Employee.id)).where(Employee.corporate_id == corporate_id)
    )).scalar() or 0

    return {
        "corporate": {
            "id": corporate.id,
            "name": corporate.name,
            "hrms_provider": corporate.hrms_provider,
            "insurer_provider": corporate.insurer_provider,
            "insurer_format": corporate.insurer_format,
            "delivery_channel": corporate.delivery_channel,
            "webhook_url": corporate.webhook_url,
        },
        "stats": {
            "employee_count": employee_count,
            "sync_status_breakdown": status_breakdown,
        },
    }
