from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import datetime

from app.core.database import get_db
from app.core.security import get_current_tenant, TenantContext
from app.models.models import SyncSource, Employee
from app.models.stream_schemas import AddEmployeeRequest, RemoveEmployeeRequest
from app.core.parsers.payload_parser import universal_payload_parser
from app.core.adapters.factory import get_hrms_adapter
from app.services.employee_service import record_employee_event
from app.tasks.sync_tasks import process_sync_event

router = APIRouter()


async def _resolve_add_event_type(
    db: AsyncSession, corporate_id: str, payload: AddEmployeeRequest
) -> tuple[str, bool]:
    """
    Determine whether this is a new ADDITION or an UPDATE to an existing employee.

    Returns:
        (event_type, is_duplicate)
        event_type  — "ADDITION", "UPDATE", or "DUPLICATE" (no-op)
        is_duplicate — True means identical re-submission, skip processing
    """
    stmt = select(Employee).where(
        Employee.corporate_id == corporate_id,
        Employee.employee_code == payload.employee_code,
    )
    result = await db.execute(stmt)
    existing = result.scalars().first()

    if not existing or existing.status != "active":
        # New employee or previously removed → fresh enrolment
        return "ADDITION", False

    # Employee is active — check if data actually changed
    data_changed = (
        existing.first_name != payload.first_name
        or existing.last_name != payload.last_name
        or str(existing.date_of_joining or "") != str(payload.date_of_joining)
    )

    if not data_changed:
        return "DUPLICATE", True

    # Active employee with changed data → update
    existing.updated_at = datetime.utcnow()
    return "UPDATE", False


@router.post("/add")
async def stream_addition(
    request: Request,
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db)
):
    try:
        # STEP 1: Format Parsing (JSON/XML -> Dict)
        raw_dict = await universal_payload_parser(request)

        # STEP 2: Schema Normalization (Vendor Dict -> Standard Dict)
        provider = getattr(tenant.corporate, 'hrms_provider', 'standard')
        adapter = get_hrms_adapter(provider)
        normalized_dict = adapter.normalize_addition(raw_dict)

        try:
            # STEP 3: Pydantic Validation
            payload = AddEmployeeRequest(**normalized_dict)
        except ValidationError as e:
            raise HTTPException(status_code=422, detail=e.errors())

        # STEP 4: Resolve whether this is ADD / UPDATE / duplicate no-op
        event_type, is_duplicate = await _resolve_add_event_type(
            db, tenant.corporate.id, payload
        )

        if is_duplicate:
            return {
                "status": "skipped",
                "message": f"{payload.employee_code} is already enrolled with identical data. No action taken.",
                "tracking_id": None,
            }

        # STEP 5: Record intent and trigger Celery
        log_entry = await record_employee_event(
            db=db,
            corporate_id=tenant.corporate.id,
            employee_data=payload.model_dump(),
            event_type=event_type,
            source=SyncSource.ONLINE,
        )
        await db.commit()
        process_sync_event.delay(log_entry.id)

        action = "updated" if event_type == "UPDATE" else "queued for addition"
        return {
            "status": "accepted",
            "message": f"{payload.employee_code} {action} via {provider} adapter.",
            "tracking_id": log_entry.id,
        }
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/remove")
async def stream_removal(
    request: Request,
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db)
):
    try:
        raw_dict = await universal_payload_parser(request)

        provider = getattr(tenant.corporate, 'hrms_provider', 'standard')
        adapter = get_hrms_adapter(provider)
        normalized_dict = adapter.normalize_deletion(raw_dict)

        try:
            payload = RemoveEmployeeRequest(**normalized_dict)
        except ValidationError as e:
            raise HTTPException(status_code=422, detail=e.errors())

        log_entry = await record_employee_event(
            db=db,
            corporate_id=tenant.corporate.id,
            employee_data=payload.model_dump(),
            event_type="DELETION",
            source=SyncSource.ONLINE,
        )
        await db.commit()
        process_sync_event.delay(log_entry.id)

        return {
            "status": "accepted",
            "message": f"Removal for {payload.employee_code} queued via {provider} adapter.",
            "tracking_id": log_entry.id,
        }
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
