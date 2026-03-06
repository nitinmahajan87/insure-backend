import os
import uuid

from fastapi import UploadFile

UPLOAD_DIR = "/app/temp_uploads"
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(50 * 1024 * 1024)))  # 50 MB default

os.makedirs(UPLOAD_DIR, exist_ok=True)


class FileTooLargeError(ValueError):
    """Uploaded file exceeds the configured size limit → HTTP 413."""


async def save_upload_file(file: UploadFile) -> str:
    """
    Read the upload content asynchronously (awaits the client network I/O),
    enforce the size limit, sanitize the filename, then write to a temp file.

    Raises FileTooLargeError if the content exceeds MAX_UPLOAD_BYTES.
    Returns the absolute path of the saved temp file.
    """
    content = await file.read()

    if len(content) > MAX_UPLOAD_BYTES:
        limit_mb = MAX_UPLOAD_BYTES // (1024 * 1024)
        received_mb = len(content) / (1024 * 1024)
        raise FileTooLargeError(
            f"File is too large ({received_mb:.1f} MB). "
            f"Maximum allowed size is {limit_mb} MB."
        )

    # Strip path separators from the original filename to prevent path traversal.
    safe_name = os.path.basename(file.filename or "upload")
    file_id = f"{uuid.uuid4().hex}_{safe_name}"
    file_path = os.path.join(UPLOAD_DIR, file_id)

    with open(file_path, "wb") as buffer:
        buffer.write(content)

    return file_path
