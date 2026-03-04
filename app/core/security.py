from fastapi import Security, HTTPException, status, Depends, Query
from fastapi.security.api_key import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload
from typing import Optional

from app.core.database import get_db
from app.models.models import ApiKey, Corporate, Broker, ApiKeyScope

api_key_header = APIKeyHeader(name="x-api-key", auto_error=False)


class TenantContext:
    """
    Unified caller context. Populated by get_current_tenant.

    For CORPORATE-scoped keys:  corporate is always set.
    For BROKER-scoped keys:     corporate is set only when ?corporate_id=<id>
                                 is present in the request. Accessing .corporate
                                 without it raises HTTP 400 automatically.
    """

    def __init__(self, corporate: Optional[Corporate], broker: Broker, scope: ApiKeyScope):
        self._corporate = corporate
        self.broker = broker
        self.scope = scope

    @property
    def corporate(self) -> Corporate:
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


async def get_current_tenant(
    api_key_token: str = Security(api_key_header),
    corporate_id: Optional[str] = Query(
        default=None,
        description="Broker-admin only: target corporate within the broker's portfolio.",
    ),
    db: AsyncSession = Depends(get_db),
) -> TenantContext:
    """
    The Bouncer. Resolves an API key to a TenantContext.

    CORPORATE key  →  context scoped to that corporate (existing behaviour).
    BROKER key     →  context scoped to the whole broker; individual endpoints
                       narrow it to a corporate via ?corporate_id=.
    """
    if not api_key_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API Key header (x-api-key).",
        )

    # Single query: key → corporate → broker  AND  key → broker (for broker keys)
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
    # CORPORATE-scoped key — existing behaviour, zero change
    # ------------------------------------------------------------------
    if key_record.scope == ApiKeyScope.CORPORATE:
        return TenantContext(
            corporate=key_record.corporate,
            broker=key_record.corporate.broker,
            scope=ApiKeyScope.CORPORATE,
        )

    # ------------------------------------------------------------------
    # BROKER-scoped key — new behaviour
    # ------------------------------------------------------------------
    if key_record.scope == ApiKeyScope.BROKER:
        broker = key_record.broker
        target_corporate: Optional[Corporate] = None

        if corporate_id:
            # Validate that the requested corporate belongs to this broker.
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

        return TenantContext(
            corporate=target_corporate,  # None if no corporate_id given
            broker=broker,
            scope=ApiKeyScope.BROKER,
        )

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Unknown API key scope.",
    )
