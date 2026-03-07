import asyncio
import os

from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Request

from app.core.security import get_current_tenant, TenantContext
from app.services.file_service import save_upload_file, FileTooLargeError
from app.core.storage import get_storage
from app.core.processor import (
    process_additions, process_deletions,
    FileParseError, WrongFileError, MissingColumnsError,
)
from app.models.schemas import BatchAcceptedResponse
from app.tasks.sync_tasks import process_master_batch

router = APIRouter()


@router.post("/additions", status_code=202, response_model=BatchAcceptedResponse)
async def upload_additions(
        request: Request,
        file: UploadFile = File(...),
        tenant: TenantContext = Depends(get_current_tenant),
):
    """
    202 Accepted — parse the file synchronously, return parse results immediately.
    All DB writes (Employee upserts, SyncLogs, SyncLogEvents), Excel generation,
    and Celery fan-out happen inside process_master_batch in the background.
    """
    file_ext = file.filename.split('.')[-1].lower()
    if file_ext not in tenant.broker.allowed_formats:
        raise HTTPException(status_code=400, detail=f"Format {file_ext} not allowed")

    try:
        temp_path = await save_upload_file(file)
    except FileTooLargeError as exc:
        raise HTTPException(status_code=413, detail=str(exc))

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
        try:
            os.unlink(temp_path)
        except OSError:
            pass

    if not report.additions:
        raise HTTPException(
            status_code=422,
            detail={
                "message": "No valid rows found in file.",
                "rejected_count": report.rejected_count,
                "rejected_rows": [r.model_dump() for r in report.rejected_rows],
            },
        )

    # Dispatch all DB work and fan-out to background — API returns immediately.
    # Dates and Decimals are serialised to strings via model_dump(mode='json')
    # so Celery's JSON serialiser handles them cleanly.
    process_master_batch.delay(
        corporate_id=tenant.corporate.id,
        rows=[r.model_dump(mode='json') for r in report.additions],
        event_type="BATCH_ADDITION",
        delivery_channel=(
            tenant.corporate.delivery_channel.value
            if tenant.corporate.delivery_channel else "webhook"
        ),
        insurer_provider=tenant.corporate.insurer_provider,
        insurer_format=tenant.corporate.insurer_format,
        base_folder=tenant.corporate.base_folder,
        is_deletion=False,
    )

    return BatchAcceptedResponse(
        filename=file.filename,
        accepted_count=len(report.additions),
        rejected_count=len(report.rejected_rows),
        message=(
            f"{len(report.additions)} records accepted and queued for processing"
            + (f", {len(report.rejected_rows)} rejected" if report.rejected_rows else "")
            + "."
        ),
        rejected_rows=report.rejected_rows,
    )


@router.post("/deletions", status_code=202, response_model=BatchAcceptedResponse)
async def upload_deletions(
        request: Request,
        file: UploadFile = File(...),
        tenant: TenantContext = Depends(get_current_tenant),
):
    """
    202 Accepted — same pattern as /additions.
    """
    file_ext = file.filename.split('.')[-1].lower()
    if file_ext not in tenant.broker.allowed_formats:
        raise HTTPException(status_code=400, detail=f"Format {file_ext} not allowed")

    try:
        temp_path = await save_upload_file(file)
    except FileTooLargeError as exc:
        raise HTTPException(status_code=413, detail=str(exc))

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

    if not report.deletions:
        raise HTTPException(
            status_code=422,
            detail={
                "message": "No valid rows found in file.",
                "rejected_count": report.rejected_count,
                "rejected_rows": [r.model_dump() for r in report.rejected_rows],
            },
        )

    process_master_batch.delay(
        corporate_id=tenant.corporate.id,
        rows=[r.model_dump(mode='json') for r in report.deletions],
        event_type="BATCH_DELETION",
        delivery_channel=(
            tenant.corporate.delivery_channel.value
            if tenant.corporate.delivery_channel else "webhook"
        ),
        insurer_provider=tenant.corporate.insurer_provider,
        insurer_format=tenant.corporate.insurer_format,
        base_folder=tenant.corporate.base_folder,
        is_deletion=True,
    )

    return BatchAcceptedResponse(
        filename=file.filename,
        accepted_count=len(report.deletions),
        rejected_count=len(report.rejected_rows),
        message=(
            f"{len(report.deletions)} records accepted and queued for processing"
            + (f", {len(report.rejected_rows)} rejected" if report.rejected_rows else "")
            + "."
        ),
        rejected_rows=report.rejected_rows,
    )


@router.get("/download/{file_name}")
async def download_outbound_file(
        file_name: str,
        tenant: TenantContext = Depends(get_current_tenant)
):
    """
    Generates a pre-signed URL for the requested file and returns it as JSON.
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
    return {"download_url": presigned}
