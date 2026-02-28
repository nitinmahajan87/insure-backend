from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime

from app.core.database import get_db
from app.core.security import get_current_tenant, TenantContext
from app.models.stream_schemas import AddEmployeeRequest, RemoveEmployeeRequest
from app.core.parsers.payload_parser import universal_payload_parser
from app.core.adapters.factory import get_hrms_adapter
from app.services.employee_service import record_employee_event
from app.tasks.sync_tasks import process_sync_event

router = APIRouter()

@router.post("/add")
async def stream_addition(
    request: Request,
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db)
):
    try:
        # STEP 1: Format Parsing (JSON/XML -> Dict)
        raw_dict = await universal_payload_parser(request)

        # 🚨 ADD THESE TWO LINES 🚨
        print(f"🕵️ RAW DICT OUTPUT: {raw_dict}")
        print(f"🕵️ ROUTING TO ADAPTER: {getattr(tenant.corporate, 'hrms_provider', 'standard')}")

        # STEP 2: Schema Normalization (Vendor Dict -> Standard Dict)
        provider = getattr(tenant.corporate, 'hrms_provider', 'standard')
        adapter = get_hrms_adapter(provider)
        normalized_dict = adapter.normalize_addition(raw_dict)


        try:
            # STEP 3: Pydantic Validation
            payload = AddEmployeeRequest(**normalized_dict)
        except ValidationError as e:
            raise HTTPException(status_code=422, detail=e.errors())

        # 1. Record Intent (Status: PENDING)
        log_entry = await record_employee_event(
            db=db,
            corporate_id=tenant.corporate.id,
            employee_data=payload.model_dump(),
            event_type="ADDITION"
        )
        await db.commit() # Save to DB first

        # 2. Trigger Background Sync
        process_sync_event.delay(log_entry.id)

        return {
            "status": "accepted",
            "message": f"Addition for {payload.employee_code} queued. via {provider} adapter.",
            "tracking_id": log_entry.id
        }
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

        # STEP 2: Schema Normalization (Vendor Dict -> Standard Dict)
        provider = getattr(tenant.corporate, 'hrms_provider', 'standard')
        adapter = get_hrms_adapter(provider)
        normalized_dict = adapter.normalize_deletion(raw_dict)

        try:
            payload = RemoveEmployeeRequest(**normalized_dict)
        except ValidationError as e:
            raise HTTPException(status_code=422, detail=e.errors())

        # 1. Record Intent (Status: PENDING)
        log_entry = await record_employee_event(
            db=db,
            corporate_id=tenant.corporate.id,
            employee_data=payload.model_dump(),
            event_type="DELETION"
        )
        await db.commit()

        # 2. Trigger Background Sync
        process_sync_event.delay(log_entry.id)

        return {
            "status": "accepted",
            "message": f"Removal for {payload.employee_code} queued. via {provider} adapter.",
            "tracking_id": log_entry.id
        }
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))