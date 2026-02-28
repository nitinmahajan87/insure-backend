# app/api/v1/endpoints/delivery.py

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, case
import os

from app.core.database import get_db
from app.core.security import get_current_tenant, TenantContext
from app.models.models import SyncLog, Employee, SyncStatus
from app.services.outbound_service import OutboundTransformer

router = APIRouter()
BASE_PATH = os.getenv("BASE_OUTBOUND_PATH", "/app/outbound_files")


@router.post("/generate-offline-report")
async def generate_offline_report(
        request: Request,
        tenant: TenantContext = Depends(get_current_tenant),
        db: AsyncSession = Depends(get_db)
):
    try:
        # 1. Fetch pending sync logs for this corporate
        stmt = select(SyncLog).where(
            SyncLog.corporate_id == tenant.corporate.id,
            SyncLog.sync_status.in_([SyncStatus.PENDING_OFFLINE, SyncStatus.PENDING_BOTH])
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

        # 3. Generate File using the universal Transformer
        full_directory = os.path.join(BASE_PATH, tenant.corporate.base_folder)
        format_type = getattr(tenant.corporate, 'insurer_format', 'excel')

        file_path, filename = OutboundTransformer.to_file(
            data=data,
            filename_prefix="offline_sweep",
            output_dir=full_directory,
            format_type=format_type
        )

        # Generate the dynamic download link
        download_url = str(request.url_for("download_outbound_file", file_name=filename))

        # 4. Update SyncLog statuses
        for log in pending_logs:
            # Graduate the status based on its origin
            if log.sync_status == SyncStatus.PENDING_BOTH:
                log.sync_status = SyncStatus.COMPLETED_BOTH
            else:
                log.sync_status = SyncStatus.COMPLETED_OFFLINE
            log.file_path = f"{tenant.corporate.base_folder}/{filename}"

        # 5. Bulk update the Employee table sync_status
        if employee_codes:
            # We use a CASE statement to elegantly graduate the statuses in a single query
            status_case = case(
                (Employee.sync_status == SyncStatus.PENDING_BOTH, SyncStatus.COMPLETED_BOTH),
                (Employee.sync_status == SyncStatus.PENDING_OFFLINE, SyncStatus.COMPLETED_OFFLINE),
                else_=Employee.sync_status  # If it's something else, don't break it
            )

            await db.execute(
                update(Employee)
                .where(Employee.corporate_id == tenant.corporate.id)
                .where(Employee.employee_code.in_(employee_codes))
                .values(sync_status=status_case)
            )

        # Commit everything in one transaction
        await db.commit()

        return {
            "message": f"Successfully swept {len(data)} pending records into a {format_type.upper()} file.",
            "download_url": download_url
        }
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to generate offline report: {str(e)}")