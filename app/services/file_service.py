import shutil
import os
import uuid
from fastapi import UploadFile

UPLOAD_DIR = "/app/temp_uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

def save_upload_file(file: UploadFile) -> str:
    """Saves the file to a temp directory and returns the path."""
    file_id = f"{uuid.uuid4()}_{file.filename}"
    file_path = os.path.join(UPLOAD_DIR, file_id)
    with open(file_path, "wb") as buffer:
        buffer.write(file.file.read())
    return file_path