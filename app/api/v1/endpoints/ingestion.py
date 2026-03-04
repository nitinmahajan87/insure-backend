from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
import os
from starlette.responses import FileResponse
from celery import group as celery_group

from app.core.database import get_db
from app.core.security import get_current_tenant, TenantContext
from app.services.file_service import save_upload_file
from app.core.processor import process_additions, process_deletions
from app.services.outbound_service import OutboundTransformer
from app.models.schemas import IngestionResponse
from app.services.employee_service import record_employee_event
from app.tasks.sync_tasks import CHUNK_SIZE, process_batch_chunk
from app.models.models import DeliveryChannel, SyncSource
router = APIRouter()

BASE_PATH = os.getenv("BASE_OUTBOUND_PATH", "/app/outbound_files")

@router.post("/additions", response_model=IngestionResponse)
async def upload_additions(
        request: Request,
        file: UploadFile = File(...),
        tenant: TenantContext = Depends(get_current_tenant),
        db: AsyncSession = Depends(get_db)
):
    file_ext = file.filename.split('.')[-1].lower()
    if file_ext not in tenant.broker.allowed_formats:
        raise HTTPException(status_code=400, detail=f"Format {file_ext} not allowed")

    temp_path = save_upload_file(file)

    try:
        # 1. Parse the file
        report = process_additions(temp_path)

        # 2. Generate the outbound file upfront for OFFLINE / BOTH channels.
        excel_url = ""
        excel_filename = ""
        if tenant.corporate.delivery_channel in [DeliveryChannel.OFFLINE, DeliveryChannel.BOTH]:
            full_directory = os.path.join(BASE_PATH, tenant.corporate.base_folder)
            os.makedirs(full_directory, exist_ok=True)
            dict_data = [r.model_dump() for r in report.additions]
            _, excel_filename = OutboundTransformer.to_file(
                data=dict_data,
                filename_prefix="addition_report",
                output_dir=full_directory,
                format_type=tenant.corporate.insurer_format,
            )
            excel_url = str(request.url_for("download_outbound_file", file_name=excel_filename))

        # 3. Process rows and Queue Tasks for API Sync
        # --- PHASE 1: BULK PERSIST (replaces sequential for loop) ---
        log_ids = []
        for addition in report.additions:
            log_entry = await record_employee_event(
                db=db,
                corporate_id=tenant.corporate.id,
                employee_data=addition.model_dump(),
                event_type="BATCH_ADDITION",
                source=SyncSource.BATCH,
            )
            if excel_url:
                log_entry.file_path = f"{tenant.corporate.base_folder}/{excel_filename}"
            log_ids.append(log_entry.id)

        # Phase 1 commit: all rows persisted before workers start.
        await db.commit()

        # Phase 2: fan-out — one Celery worker per chunk of CHUNK_SIZE rows.
        chunks = [log_ids[i:i + CHUNK_SIZE] for i in range(0, len(log_ids), CHUNK_SIZE)]
        celery_group(process_batch_chunk.s(chunk) for chunk in chunks).apply_async()

        return IngestionResponse(
            filename=file.filename,
            message=f"{len(report.additions)} records persisted. "
                    f"{len(chunks)} parallel workers dispatched.",
            report=report,
            api_payload={},
            excel_download_url=excel_url
        )
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Addition processing failed: {str(e)}")


@router.post("/deletions", response_model=IngestionResponse)
async def upload_deletions(
        request: Request,
        file: UploadFile = File(...),
        tenant: TenantContext = Depends(get_current_tenant),
        db: AsyncSession = Depends(get_db)
):
    temp_path = save_upload_file(file)
    try:
        report = process_deletions(temp_path)

        excel_url = ""
        excel_filename = ""
        if tenant.corporate.delivery_channel in [DeliveryChannel.OFFLINE, DeliveryChannel.BOTH]:
            full_directory = os.path.join(BASE_PATH, tenant.corporate.base_folder)
            os.makedirs(full_directory, exist_ok=True)
            dict_data = [r.model_dump() for r in report.deletions]
            _, excel_filename = OutboundTransformer.to_file(
                data=dict_data,
                filename_prefix="removal_report",
                output_dir=full_directory,
                format_type=tenant.corporate.insurer_format,
            )
            excel_url = str(request.url_for("download_outbound_file", file_name=excel_filename))

        # Phase 1: bulk persist all rows in one transaction.
        log_ids = []
        for deletion in report.deletions:
            log_entry = await record_employee_event(
                db=db,
                corporate_id=tenant.corporate.id,
                employee_data=deletion.model_dump(),
                event_type="BATCH_DELETION",
                source=SyncSource.BATCH,
            )
            if excel_url:
                log_entry.file_path = f"{tenant.corporate.base_folder}/{excel_filename}"
            log_ids.append(log_entry.id)

        await db.commit()

        # Phase 2: fan-out — one Celery worker per chunk of CHUNK_SIZE rows.
        chunks = [log_ids[i:i + CHUNK_SIZE] for i in range(0, len(log_ids), CHUNK_SIZE)]
        celery_group(process_batch_chunk.s(chunk) for chunk in chunks).apply_async()

        return IngestionResponse(
            filename=file.filename,
            message=f"{len(report.deletions)} records persisted. "
                    f"{len(chunks)} parallel workers dispatched.",
            report=report,
            api_payload={},
            excel_download_url=excel_url,
        )
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Deletion processing failed: {str(e)}")


@router.get("/download/{file_name}")
async def download_outbound_file(
        file_name: str,
        tenant: TenantContext = Depends(get_current_tenant)
):
    """
    Serves the generated Excel reports.
    Paths are resolved relative to the Docker volume root.
    """
    # 1. Construct the primary path: /app/outbound_files/wipro/filename.xlsx
    # This uses the cleaned 'base_folder' from your DB (e.g., 'wipro')
    file_path = os.path.join(BASE_PATH, tenant.corporate.base_folder, file_name)

    if os.path.exists(file_path):
        print(f"📥 File {file_name} downloaded by {tenant.corporate.name}")
        return FileResponse(
            path=file_path,
            filename=file_name,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    # 2. Updated Fallback: Check the root of the outbound folder
    # This covers files generated before we implemented sub-folders
    fallback_path = os.path.join(BASE_PATH, file_name)

    if os.path.exists(fallback_path):
        print(f"📥 File {file_name} found in fallback root for {tenant.corporate.name}")
        return FileResponse(
            path=fallback_path,
            filename=file_name,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    # 3. Final Error if neither path exists
    raise HTTPException(
        status_code=404,
        detail=f"File '{file_name}' not found in {tenant.corporate.base_folder} or root."
    )