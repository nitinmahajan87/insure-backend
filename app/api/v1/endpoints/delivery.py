# app/api/v1/endpoints/delivery.py

import asyncio
import os

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import case, desc, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from app.core.database import get_db
from app.core.security import get_current_tenant, TenantContext
from app.core.storage import get_storage
from app.models.models import SyncLog, Employee, SyncStatus, SyncLogEvent
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
            # Graduate the status based on its origin
            if log.sync_status == SyncStatus.PENDING_BOTH:
                log.sync_status = SyncStatus.COMPLETED_BOTH
                new_status = SyncStatus.COMPLETED_BOTH
            else:
                log.sync_status = SyncStatus.COMPLETED_OFFLINE
                new_status = SyncStatus.COMPLETED_OFFLINE

            log.file_path = s3_key

            # Log the Sweeper action
            audit_event = SyncLogEvent(
                sync_log_id=log.id,
                event_status=new_status,
                actor="HR_SWEEPER",
                details={"file_generated": filename}
            )
            db.add(audit_event)

        # 5. Bulk update the Employee table delivery_status
        if employee_codes:
            status_case = case(
                (Employee.delivery_status == SyncStatus.PENDING_BOTH, SyncStatus.COMPLETED_BOTH),
                (Employee.delivery_status == SyncStatus.PENDING_OFFLINE, SyncStatus.COMPLETED_OFFLINE),
                else_=Employee.delivery_status
            )

            await db.execute(
                update(Employee)
                .where(Employee.corporate_id == tenant.corporate.id)
                .where(Employee.employee_code.in_(employee_codes))
                .values(delivery_status=status_case)
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
            func.count(SyncLog.id).label('record_count')
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

            history.append({
                "file_name": file_name_only,
                "generated_at": row.generated_at,
                "record_count": row.record_count,
                "download_url": download_url,
            })

        return {"data": history}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch history: {str(e)}")