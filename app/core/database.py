import os
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker, DeclarativeBase

# Database URL: driver://user:password@host:port/dbname
# Note: 'insurtech_db' is the service name from docker-compose.
# If running locally without docker-network, use 'localhost:5433'
# 1. URLs
ASYNC_DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://admin:password123@localhost:5433/insurtech_gateway"
)

# Sync URL (Change asyncpg to psycopg2)
SYNC_DATABASE_URL = ASYNC_DATABASE_URL.replace("asyncpg", "psycopg2")

# 2. Engines
async_engine = create_async_engine(ASYNC_DATABASE_URL, echo=True)
sync_engine = create_engine(SYNC_DATABASE_URL, echo=False)

# 3. Session Factories
# For FastAPI (Async)
AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# For Celery Worker (Sync) - This resolves your red line!
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=sync_engine
)

# 3. Dependency to get DB session in FastAPI routes
async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()