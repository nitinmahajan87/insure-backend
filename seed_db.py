"""
Seed script — idempotent (safe to run multiple times).
Uses INSERT ... ON CONFLICT DO NOTHING so re-runs are always a no-op.
"""
import asyncio
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.database import AsyncSessionLocal
from app.models.models import Broker, Corporate, User, ApiKey, DeliveryChannel, ApiKeyScope


async def seed_data():
    async with AsyncSessionLocal() as session:
        async with session.begin():

            # 1. Brokers
            await session.execute(pg_insert(Broker).values([
                dict(id="brk_marsh", name="Marsh Insurance", allowed_formats=["csv", "xlsx"]),
                dict(id="brk_aon",   name="Aon Brokers",     allowed_formats=["csv"]),
            ]).on_conflict_do_nothing(index_elements=["id"]))

            # 2. Corporates
            await session.execute(pg_insert(Corporate).values([
                dict(
                    id="corp_infosys", broker_id="brk_marsh", name="Infosys Ltd",
                    webhook_url="https://webhook.site/your-id-here",
                    insurer_format="json", delivery_channel=DeliveryChannel.WEBHOOK,
                    base_folder="outbound_files/infosys",
                ),
                dict(
                    id="corp_wipro", broker_id="brk_aon", name="Wipro Technologies",
                    webhook_url="NA",
                    insurer_format="xml", delivery_channel=DeliveryChannel.OFFLINE,
                    base_folder="outbound_files/wipro",
                ),
            ]).on_conflict_do_nothing(index_elements=["id"]))

            # 3. Users  (conflict on username unique index)
            await session.execute(pg_insert(User).values([
                dict(
                    id="user_hr_infosys", corporate_id="corp_infosys",
                    username="hr@infosys.com", hashed_password="admin123", role="admin",
                ),
                dict(
                    id="user_hr_wipro", corporate_id="corp_wipro",
                    username="hr@wipro.com", hashed_password="admin123", role="admin",
                ),
            ]).on_conflict_do_nothing(index_elements=["username"]))

            # 4. API Keys (all rows must declare same columns for multi-row INSERT)
            await session.execute(pg_insert(ApiKey).values([
                dict(key="sk_live_infosys_001", corporate_id="corp_infosys", broker_id=None,
                     is_active=True, scope=ApiKeyScope.CORPORATE),
                dict(key="sk_live_wipro_999",   corporate_id="corp_wipro",   broker_id=None,
                     is_active=True, scope=ApiKeyScope.CORPORATE),
                dict(key="sk_broker_marsh_admin", corporate_id=None, broker_id="brk_marsh",
                     is_active=True, scope=ApiKeyScope.BROKER),
            ]).on_conflict_do_nothing(index_elements=["key"]))

    print("✅ Database seeded successfully!")


if __name__ == "__main__":
    asyncio.run(seed_data())
