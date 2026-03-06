"""
Object storage backend — S3-compatible.

Works with:
  - MinIO (local docker-compose dev)      STORAGE_ENDPOINT=http://minio:9000
  - Cloudflare R2 (if ever needed)        STORAGE_ENDPOINT=https://<account>.r2.cloudflarestorage.com
  - AWS S3 ap-south-1 (production)        STORAGE_ENDPOINT unset (boto3 uses native endpoint)

Switch environments by changing env vars only — zero code changes.
"""
import logging
import os
from typing import IO

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Backend:
    """
    Single S3-compatible backend for all environments.
    Reads configuration from environment variables at instantiation time.
    """

    def __init__(self) -> None:
        endpoint = os.getenv("STORAGE_ENDPOINT") or None  # None = native AWS endpoint
        self.bucket = os.environ["STORAGE_BUCKET"]
        self._presign_ttl = int(os.getenv("STORAGE_PRESIGN_TTL", "900"))

        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=os.environ["STORAGE_ACCESS_KEY"],
            aws_secret_access_key=os.environ["STORAGE_SECRET_KEY"],
            region_name=os.getenv("STORAGE_REGION", "ap-south-1"),
            # s3v4 signatures required by MinIO, R2, and AWS
            config=Config(signature_version="s3v4"),
        )

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def upload_fileobj(self, fileobj: IO[bytes], key: str, content_type: str = "application/octet-stream") -> str:
        """Upload a file-like object. Returns the S3 key."""
        self._client.upload_fileobj(
            fileobj,
            self.bucket,
            key,
            ExtraArgs={"ContentType": content_type},
        )
        return key

    def presigned_url(self, key: str, expires_in: int | None = None) -> str:
        """Generate a pre-signed GET URL. Default TTL from env (STORAGE_PRESIGN_TTL)."""
        ttl = expires_in if expires_in is not None else self._presign_ttl
        return self._client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=ttl,
        )

    def delete(self, key: str) -> None:
        """Delete an object. Logs a warning on error but does not raise."""
        try:
            self._client.delete_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            logger.warning(f"Storage delete failed for key '{key}': {exc}")

    def key_exists(self, key: str) -> bool:
        """Returns True if the object exists in the bucket."""
        try:
            self._client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False


# ---------------------------------------------------------------------------
# Module-level singleton — instantiated once, reused across requests/tasks
# ---------------------------------------------------------------------------

_storage: S3Backend | None = None


def get_storage() -> S3Backend:
    """Return the shared storage backend instance (lazy init)."""
    global _storage
    if _storage is None:
        _storage = S3Backend()
    return _storage
