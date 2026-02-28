from fastapi import Security, HTTPException, status, Depends
from fastapi.security.api_key import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload

from app.core.database import get_db
from app.models.models import ApiKey, Corporate, Broker

# This tells FastAPI to look for "x-api-key" in the header
api_key_header = APIKeyHeader(name="x-api-key", auto_error=False)


class TenantContext:
    """
    A unified object that holds everything we need to know about the caller.
    """

    def __init__(self, corporate: Corporate, broker: Broker):
        self.corporate = corporate
        self.broker = broker


async def get_current_tenant(
        api_key_token: str = Security(api_key_header),
        db: AsyncSession = Depends(get_db)  # <--- Injecting the DB session
) -> TenantContext:
    """
    The 'Bouncer'. Now queries PostgreSQL instead of mock_db.
    """
    if not api_key_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API Key Header (x-api-key)"
        )

    # Use joinedload to fetch Key -> Corporate -> Broker in ONE query
    query = (
        select(ApiKey)
        .options(
            joinedload(ApiKey.corporate).joinedload(Corporate.broker)
        )
        .where(ApiKey.key == api_key_token, ApiKey.is_active == True)
    )

    result = await db.execute(query)
    key_record = result.scalars().first()

    if not key_record:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or Inactive API Key"
        )

    # 4. Success! Use the relationship attributes to build context
    return TenantContext(
        corporate=key_record.corporate,
        broker=key_record.corporate.broker
    )