from types import SimpleNamespace

from fastapi import Security, HTTPException, status, Depends, Query
from fastapi.security.api_key import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload
from typing import Optional

from app.core.database import get_db
from app.core.cache import async_cache_get, async_cache_set, APIKEY_TTL, CORPORATE_TTL
from app.models.models import ApiKey, Corporate, Broker, ApiKeyScope, DeliveryChannel

api_key_header = APIKeyHeader(name="x-api-key", auto_error=False)


class TenantContext:
    """
    Unified caller context. Populated by get_current_tenant.

    For CORPORATE-scoped keys:  corporate is always set.
    For BROKER-scoped keys:     corporate is set only when ?corporate_id=<id>
                                 is present in the request. Accessing .corporate
                                 without it raises HTTP 400 automatically.
    """

    def __init__(self, corporate, broker, scope: ApiKeyScope):
        self._corporate = corporate
        self.broker = broker
        self.scope = scope

    @property
    def corporate(self):
        """
        Raises HTTP 400 if a broker-admin key was used without ?corporate_id=.
        All existing endpoints access tenant.corporate directly, so they are
        guarded for free without any code changes.
        """
        if self._corporate is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    "This endpoint operates on a single corporate. "
                    "Broker-admin keys must include ?corporate_id=<id> in the request."
                ),
            )
        return self._corporate

    @property
    def is_broker_admin(self) -> bool:
        return self.scope == ApiKeyScope.BROKER


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _corporate_to_dict(c: Corporate) -> dict:
    return {
        "id": c.id,
        "name": c.name,
        "broker_id": c.broker_id,
        "webhook_url": c.webhook_url,
        "insurer_format": c.insurer_format,
        "delivery_channel": c.delivery_channel.value if c.delivery_channel else None,
        "base_folder": c.base_folder,
        "insurer_provider": getattr(c, "insurer_provider", "standard"),
        "hrms_provider": getattr(c, "hrms_provider", "standard"),
    }


def _broker_to_dict(b: Broker) -> dict:
    return {
        "id": b.id,
        "name": b.name,
        "allowed_formats": b.allowed_formats,
    }


def _dict_to_corporate(d: dict) -> SimpleNamespace:
    corp = SimpleNamespace(**d)
    if d.get("delivery_channel"):
        corp.delivery_channel = DeliveryChannel(d["delivery_channel"])
    return corp


def _dict_to_broker(d: dict) -> SimpleNamespace:
    return SimpleNamespace(**d)


# ---------------------------------------------------------------------------
# Main dependency
# ---------------------------------------------------------------------------

async def get_current_tenant(
    api_key_token: str = Security(api_key_header),
    # alias keeps the query-string name as ?corporate_id=... but avoids
    # colliding with {corporate_id} path params in routes that use this dep.
    tenant_corporate_id: Optional[str] = Query(
        default=None,
        alias="corporate_id",
        description="Broker-admin only: target corporate within the broker's portfolio.",
    ),
    db: AsyncSession = Depends(get_db),
) -> TenantContext:
    """
    The Bouncer. Resolves an API key to a TenantContext.

    CORPORATE key  →  context scoped to that corporate (existing behaviour).
    BROKER key     →  context scoped to the whole broker; individual endpoints
                       narrow it to a corporate via ?corporate_id=.

    Redis cache layer (DB 1):
      ins:apikey:{token}  — stores scope + broker (+ corporate for CORPORATE keys)
      ins:corp:{id}       — stores corporate config (used by broker-key paths)
    """
    if not api_key_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API Key header (x-api-key).",
        )

    cache_key = f"ins:apikey:{api_key_token}"

    # ------------------------------------------------------------------
    # Cache hit path
    # ------------------------------------------------------------------
    cached = await async_cache_get(cache_key)
    if cached:
        scope = ApiKeyScope(cached["scope"])
        broker = _dict_to_broker(cached["broker"])

        if scope == ApiKeyScope.CORPORATE:
            corporate = _dict_to_corporate(cached["corporate"])
            return TenantContext(corporate=corporate, broker=broker, scope=scope)

        # BROKER scope — resolve corporate separately (may also be cached)
        if scope == ApiKeyScope.BROKER:
            target_corporate = None
            if tenant_corporate_id:
                corp_cache_key = f"ins:corp:{tenant_corporate_id}"
                corp_data = await async_cache_get(corp_cache_key)
                if corp_data:
                    target_corporate = _dict_to_corporate(corp_data)
                    # Validate broker ownership from cached data
                    if target_corporate.broker_id != broker.id:
                        raise HTTPException(
                            status_code=status.HTTP_403_FORBIDDEN,
                            detail=(
                                f"Corporate '{tenant_corporate_id}' does not belong to "
                                f"broker '{broker.name}', or does not exist."
                            ),
                        )
                else:
                    # Corporate not in cache — fall through to DB below
                    target_corporate = await _resolve_broker_corporate(
                        db, tenant_corporate_id, broker
                    )
                    # Warm the corporate cache for future worker lookups
                    await async_cache_set(
                        corp_cache_key, _corporate_to_dict(target_corporate), CORPORATE_TTL
                    )
            return TenantContext(corporate=target_corporate, broker=broker, scope=scope)

    # ------------------------------------------------------------------
    # Cache miss — hit the DB
    # ------------------------------------------------------------------
    query = (
        select(ApiKey)
        .options(
            joinedload(ApiKey.corporate).joinedload(Corporate.broker),
            joinedload(ApiKey.broker),
        )
        .where(ApiKey.key == api_key_token, ApiKey.is_active == True)
    )
    result = await db.execute(query)
    key_record = result.scalars().first()

    if not key_record:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or inactive API key.",
        )

    # ------------------------------------------------------------------
    # CORPORATE-scoped key
    # ------------------------------------------------------------------
    if key_record.scope == ApiKeyScope.CORPORATE:
        corporate = key_record.corporate
        broker = key_record.corporate.broker

        await async_cache_set(cache_key, {
            "scope": ApiKeyScope.CORPORATE.value,
            "corporate": _corporate_to_dict(corporate),
            "broker": _broker_to_dict(broker),
        }, APIKEY_TTL)

        return TenantContext(corporate=corporate, broker=broker, scope=ApiKeyScope.CORPORATE)

    # ------------------------------------------------------------------
    # BROKER-scoped key
    # ------------------------------------------------------------------
    if key_record.scope == ApiKeyScope.BROKER:
        broker = key_record.broker

        await async_cache_set(cache_key, {
            "scope": ApiKeyScope.BROKER.value,
            "broker": _broker_to_dict(broker),
        }, APIKEY_TTL)

        target_corporate = None
        if tenant_corporate_id:
            target_corporate = await _resolve_broker_corporate(
                db, tenant_corporate_id, broker
            )
            await async_cache_set(
                f"ins:corp:{tenant_corporate_id}",
                _corporate_to_dict(target_corporate),
                CORPORATE_TTL,
            )

        return TenantContext(
            corporate=target_corporate,
            broker=broker,
            scope=ApiKeyScope.BROKER,
        )

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Unknown API key scope.",
    )


async def _resolve_broker_corporate(db: AsyncSession, corporate_id: str, broker) -> Corporate:
    """Fetch and validate a corporate belongs to the given broker."""
    corp_result = await db.execute(
        select(Corporate).where(
            Corporate.id == corporate_id,
            Corporate.broker_id == broker.id,
        )
    )
    target_corporate = corp_result.scalars().first()

    if not target_corporate:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Corporate '{corporate_id}' does not belong to broker "
                f"'{broker.name}', or does not exist."
            ),
        )
    return target_corporate
