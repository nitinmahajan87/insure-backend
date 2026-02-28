import uuid
import enum
from datetime import datetime
from sqlalchemy import Column, String, Boolean, ForeignKey, DateTime, Date, Float, Enum, JSON, Integer, func
from sqlalchemy.orm import relationship, DeclarativeBase


class Base(DeclarativeBase):
    pass


class AuditMixin:
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    created_by = Column(String, nullable=True)  # Will store the User.id
    updated_by = Column(String, nullable=True)
    is_deleted = Column(Boolean, default=False)


# --- 1. Infrastructure Tables ---

class DeliveryChannel(enum.Enum):
    WEBHOOK = "webhook"
    OFFLINE = "offline"
    BOTH = "both"

class SyncStatus(enum.Enum):
    PENDING = "pending"
    PROVISIONING = "provisioning"
    ACTIVE = "active"
    FAILED = "failed"
    COMPLETED_OFFLINE = "completed_offline"

class Broker(Base, AuditMixin):
    __tablename__ = "brokers"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=False)
    allowed_formats = Column(JSON, default=["csv", "xlsx"])

    corporates = relationship("Corporate", back_populates="broker")


class Corporate(Base, AuditMixin):
    __tablename__ = "corporates"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    broker_id = Column(String, ForeignKey("brokers.id"))
    name = Column(String, nullable=False)
    webhook_url = Column(String)
    insurer_format = Column(String, default="json")
    delivery_channel = Column(Enum(DeliveryChannel), default=DeliveryChannel.WEBHOOK)
    base_folder = Column(String, default="outbound_files/default")
    last_report_path = Column(String, nullable=True)

    broker = relationship("Broker", back_populates="corporates")
    api_keys = relationship("ApiKey", back_populates="corporate")
    employees = relationship("Employee", back_populates="corporate")
    users = relationship("User", back_populates="corporate")  # Link to Users


class User(Base, AuditMixin):
    __tablename__ = "users"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    corporate_id = Column(String, ForeignKey("corporates.id"))
    username = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)  # Store hashes, not plain text!
    is_active = Column(Boolean, default=True)
    role = Column(String, default="admin")  # e.g., admin, viewer

    corporate = relationship("Corporate", back_populates="users")


class ApiKey(Base, AuditMixin):
    __tablename__ = "api_keys"
    key = Column(String, primary_key=True)
    corporate_id = Column(String, ForeignKey("corporates.id"))
    is_active = Column(Boolean, default=True)

    corporate = relationship("Corporate", back_populates="api_keys")


# --- 2. Business Data (Census & Logs) ---

class Employee(Base, AuditMixin):
    __tablename__ = "employees"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    corporate_id = Column(String, ForeignKey("corporates.id"), index=True)
    employee_code = Column(String, nullable=False, index=True)
    first_name = Column(String)
    last_name = Column(String)
    date_of_joining = Column(Date, nullable=True)
    sum_insured = Column(Float)
    policy_number = Column(String, nullable=True)
    status = Column(String, default="active")
    date_of_leaving = Column(Date, nullable=True)
    sync_status = Column(Enum(SyncStatus), default=SyncStatus.PENDING)

    corporate = relationship("Corporate", back_populates="employees")


class SyncLog(Base):
    __tablename__ = "sync_logs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    corporate_id = Column(String, ForeignKey("corporates.id"))
    transaction_id = Column(String, index=True, nullable=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=True)  # Who triggered the sync?
    transaction_type = Column(String)
    payload = Column(JSON)
    status = Column(String)
    sync_status = Column(Enum(SyncStatus), default=SyncStatus.PENDING)
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    raw_response = Column(JSON, nullable=True)  # Insurer's exact response
    error_message = Column(String, nullable=True)
    file_path = Column(String, nullable=True)  # Path to offline CSV/Excel
    timestamp = Column(DateTime, server_default=func.now())