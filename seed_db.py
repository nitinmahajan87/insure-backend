import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import AsyncSessionLocal, engine
from app.models.models import Broker, Corporate, User, ApiKey, DeliveryChannel
import uuid

async def seed_data():
    async with AsyncSessionLocal() as session:
        async with session.begin():
            # 1. Create Brokers
            marsh = Broker(id="brk_marsh", name="Marsh Insurance", allowed_formats=["csv", "xlsx"])
            aon = Broker(id="brk_aon", name="Aon Brokers", allowed_formats=["csv"])
            session.add_all([marsh, aon])

            # 2. Corporates (Updated with Delivery Channel and Base Folder)
            infosys = Corporate(
                id="corp_infosys",
                broker_id="brk_marsh",
                name="Infosys Ltd",
                webhook_url="https://webhook.site/your-id-here",
                insurer_format="json",
                delivery_channel=DeliveryChannel.WEBHOOK,
                base_folder="outbound_files/infosys"
            )
            wipro = Corporate(
                id="corp_wipro",
                broker_id="brk_aon",
                name="Wipro Technologies",
                webhook_url="NA",  # No webhook for offline
                insurer_format="xml",
                delivery_channel=DeliveryChannel.OFFLINE,
                base_folder="outbound_files/wipro"
            )
            session.add_all([infosys, wipro])

            # 3. Create Users (HR Managers)
            # NOTE: In production, use hashed passwords!
            hr_infosys = User(
                id=str(uuid.uuid4()),
                corporate_id="corp_infosys",
                username="hr@infosys.com",
                hashed_password="admin123", # Plain text for now to match your mock
                role="admin"
            )
            hr_wipro = User(
                id=str(uuid.uuid4()),
                corporate_id="corp_wipro",
                username="hr@wipro.com",
                hashed_password="admin123",
                role="admin"
            )
            session.add_all([hr_infosys, hr_wipro])

            # 4. Create API Keys
            key1 = ApiKey(key="sk_live_infosys_001", corporate_id="corp_infosys", is_active=True)
            key2 = ApiKey(key="sk_live_wipro_999", corporate_id="corp_wipro", is_active=True)
            session.add_all([key1, key2])

        print("✅ Database seeded successfully!")

if __name__ == "__main__":
    asyncio.run(seed_data())