import asyncio
import os

from celery import group as celery_group
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import RedirectResponse

from app.core.database import get_db
from app.core.security import get_current_tenant, TenantContext
from app.services.file_service import save_upload_file, FileTooLargeError
from app.core.storage import get_storage
from app.core.processor import (
    process_additions, process_deletions,
    FileParseError, WrongFileError, MissingColumnsError,
)
from app.services.outbound_service import OutboundTransformer
from app.core.outbound.factory import get_insurer_adapter
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

    try:
        temp_path = await save_upload_file(file)
    except FileTooLargeError as exc:
        raise HTTPException(status_code=413, detail=str(exc))

    # ── File parsing (typed exceptions → correct HTTP codes) ──────────────────
    try:
        report = await asyncio.to_thread(
            process_additions, temp_path, tenant.corporate.hrms_provider
        )
    except WrongFileError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except MissingColumnsError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except FileParseError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        # Temp upload file is no longer needed after parsing.
        try:
            os.unlink(temp_path)
        except OSError:
            pass

    # ── No valid rows at all → return early with 422 ──────────────────────────
    if not report.additions:
        raise HTTPException(
            status_code=422,
            detail={
                "message": "No valid rows found in file.",
                "rejected_count": report.rejected_count,
                "rejected_rows": [r.model_dump() for r in report.rejected_rows],
            },
        )

    try:
        # 1. Generate outbound file for OFFLINE / BOTH channels.
        excel_url = ""
        s3_key = ""
        excel_filename = ""
        if tenant.corporate.delivery_channel in [DeliveryChannel.OFFLINE, DeliveryChannel.BOTH]:
            full_directory = os.path.join(BASE_PATH, tenant.corporate.base_folder)
            dict_data = [r.model_dump() for r in report.additions]
            s3_key, excel_filename = await asyncio.to_thread(
                OutboundTransformer.to_file,
                dict_data,
                "addition_report",
                full_directory,
                tenant.corporate.insurer_format,
                get_insurer_adapter(tenant.corporate.insurer_provider),
                False,
            )
            # Pre-signed URL — frontend downloads directly from storage (15 min TTL).
            excel_url = await asyncio.to_thread(get_storage().presigned_url, s3_key)

        # 2. Bulk persist all accepted rows in one transaction.
        log_ids = []
        for addition in report.additions:
            log_entry = await record_employee_event(
                db=db,
                corporate_id=tenant.corporate.id,
                employee_data=addition.model_dump(),
                event_type="BATCH_ADDITION",
                source=SyncSource.BATCH,
            )
            log_ids.append(log_entry.id)

        await db.commit()

        # 3. Fan-out — one Celery worker per chunk.
        chunks = [log_ids[i:i + CHUNK_SIZE] for i in range(0, len(log_ids), CHUNK_SIZE)]
        celery_group(process_batch_chunk.s(chunk) for chunk in chunks).apply_async()

        return IngestionResponse(
            filename=file.filename,
            accepted_count=len(report.additions),
            rejected_count=len(report.rejected_rows),
            message=(
                f"{len(report.additions)} records accepted"
                + (f", {len(report.rejected_rows)} rejected" if report.rejected_rows else "")
                + f". {len(chunks)} worker(s) dispatched."
            ),
            report=report,
            file_download_url=excel_url,
        )
    except Exception as exc:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Addition processing failed: {exc}")


@router.post("/deletions", response_model=IngestionResponse)
async def upload_deletions(
        request: Request,
        file: UploadFile = File(...),
        tenant: TenantContext = Depends(get_current_tenant),
        db: AsyncSession = Depends(get_db)
):
    file_ext = file.filename.split('.')[-1].lower()
    if file_ext not in tenant.broker.allowed_formats:
        raise HTTPException(status_code=400, detail=f"Format {file_ext} not allowed")

    try:
        temp_path = await save_upload_file(file)
    except FileTooLargeError as exc:
        raise HTTPException(status_code=413, detail=str(exc))

    # ── File parsing (typed exceptions → correct HTTP codes) ──────────────────
    try:
        report = await asyncio.to_thread(
            process_deletions, temp_path, tenant.corporate.hrms_provider
        )
    except WrongFileError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except MissingColumnsError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except FileParseError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        try:
            os.unlink(temp_path)
        except OSError:
            pass

    # ── No valid rows at all → return early with 422 ──────────────────────────
    if not report.deletions:
        raise HTTPException(
            status_code=422,
            detail={
                "message": "No valid rows found in file.",
                "rejected_count": report.rejected_count,
                "rejected_rows": [r.model_dump() for r in report.rejected_rows],
            },
        )

    try:
        # 1. Generate outbound file for OFFLINE / BOTH channels.
        excel_url = ""
        s3_key = ""
        excel_filename = ""
        if tenant.corporate.delivery_channel in [DeliveryChannel.OFFLINE, DeliveryChannel.BOTH]:
            full_directory = os.path.join(BASE_PATH, tenant.corporate.base_folder)
            dict_data = [r.model_dump() for r in report.deletions]
            s3_key, excel_filename = await asyncio.to_thread(
                OutboundTransformer.to_file,
                dict_data,
                "removal_report",
                full_directory,
                tenant.corporate.insurer_format,
                get_insurer_adapter(tenant.corporate.insurer_provider),
                True,
            )
            excel_url = await asyncio.to_thread(get_storage().presigned_url, s3_key)

        # 2. Bulk persist all accepted rows in one transaction.
        log_ids = []
        for deletion in report.deletions:
            log_entry = await record_employee_event(
                db=db,
                corporate_id=tenant.corporate.id,
                employee_data=deletion.model_dump(),
                event_type="BATCH_DELETION",
                source=SyncSource.BATCH,
            )
            log_ids.append(log_entry.id)

        await db.commit()

        # 3. Fan-out — one Celery worker per chunk.
        chunks = [log_ids[i:i + CHUNK_SIZE] for i in range(0, len(log_ids), CHUNK_SIZE)]
        celery_group(process_batch_chunk.s(chunk) for chunk in chunks).apply_async()

        return IngestionResponse(
            filename=file.filename,
            accepted_count=len(report.deletions),
            rejected_count=len(report.rejected_rows),
            message=(
                f"{len(report.deletions)} records accepted"
                + (f", {len(report.rejected_rows)} rejected" if report.rejected_rows else "")
                + f". {len(chunks)} worker(s) dispatched."
            ),
            report=report,
            file_download_url=excel_url,
        )
    except Exception as exc:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Deletion processing failed: {exc}")


@router.get("/download/{file_name}")
async def download_outbound_file(
        file_name: str,
        tenant: TenantContext = Depends(get_current_tenant)
):
    """
    Generates a pre-signed URL for the requested file and redirects to it.
    The client downloads directly from object storage (MinIO locally, S3 in production).
    Tenant-scoped: only files belonging to this corporate's folder are accessible.
    """
    s3_key = f"outbound/{tenant.corporate.base_folder}/{file_name}"
    storage = get_storage()

    if not await asyncio.to_thread(storage.key_exists, s3_key):
        raise HTTPException(
            status_code=404,
            detail=f"File '{file_name}' not found for {tenant.corporate.name}."
        )

    presigned = await asyncio.to_thread(storage.presigned_url, s3_key)
    # Return JSON so the frontend can call this with auth headers (X-API-KEY)
    # to get a fresh URL at any time — solves pre-signed URL expiry for HR.
    return {"download_url": presigned}