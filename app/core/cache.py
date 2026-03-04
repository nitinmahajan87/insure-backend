"""
Thin Redis cache layer.

Uses Redis DB 1 (DB 0 is reserved for Celery broker/backend).
All operations are wrapped in try/except so a Redis hiccup never
breaks the main request/task flow — it simply falls through to the DB.

Clients:
  async_redis  — used by FastAPI (redis.asyncio)
  sync_redis   — used by Celery workers (redis.Redis)
"""
import json
import os
from typing import Optional

import redis
import redis.asyncio as aioredis

CACHE_REDIS_URL = os.getenv("CACHE_REDIS_URL", "redis://redis:6379/1")

# TTLs (seconds)
APIKEY_TTL = 300      # 5 min — API key + tenant context
CORPORATE_TTL = 300   # 5 min — corporate config used by workers

# ---------------------------------------------------------------------------
# Client singletons (created at import time; share a connection pool)
# ---------------------------------------------------------------------------
async_redis: aioredis.Redis = aioredis.Redis.from_url(
    CACHE_REDIS_URL, decode_responses=True
)
sync_redis: redis.Redis = redis.Redis.from_url(
    CACHE_REDIS_URL, decode_responses=True
)


# ---------------------------------------------------------------------------
# Async helpers  (FastAPI)
# ---------------------------------------------------------------------------

async def async_cache_get(key: str) -> Optional[dict]:
    try:
        raw = await async_redis.get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None


async def async_cache_set(key: str, value: dict, ttl: int) -> None:
    try:
        await async_redis.set(key, json.dumps(value), ex=ttl)
    except Exception:
        pass


async def async_cache_delete(key: str) -> None:
    try:
        await async_redis.delete(key)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Sync helpers  (Celery workers)
# ---------------------------------------------------------------------------

def cache_get(key: str) -> Optional[dict]:
    try:
        raw = sync_redis.get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None


def cache_set(key: str, value: dict, ttl: int) -> None:
    try:
        sync_redis.set(key, json.dumps(value), ex=ttl)
    except Exception:
        pass


def cache_delete(key: str) -> None:
    try:
        sync_redis.delete(key)
    except Exception:
        pass
