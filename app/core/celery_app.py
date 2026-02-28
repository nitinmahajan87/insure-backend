from celery import Celery
import os
from dotenv import load_dotenv

load_dotenv()

# Get Redis URL from environment or default
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

celery_app = Celery(
    "insurtech_tasks",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["app.tasks.sync_tasks"] # We will create this file next
)

# Optional: Celery Configuration
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    # Fix for the Warning: Retrying connections on startup
    broker_connection_retry_on_startup=True,
    # Restart worker if it consumes too much memory (good for MacBook Air)
    worker_max_memory_per_child=200000,
)