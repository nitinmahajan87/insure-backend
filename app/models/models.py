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


class SyncStatus(str, enum.Enum):
    PENDING = "PENDING"
    PROVISIONING = "PROVISIONING"
    ACTIVE = "ACTIVE"  # Delivered to insurer
    FAILED = "FAILED"
    SOFT_REJECTED = "SOFT_REJECTED"  # NEW: Insurer accepted receipt but rejected data later
    COMPLETED_OFFLINE = "COMPLETED_OFFLINE"
    PENDING_OFFLINE = "PENDING_OFFLINE"
    COMPLETED_BOTH = "COMPLETED_BOTH"
    PENDING_BOTH = "PENDING_BOTH"


class ApiKeyScope(str, enum.Enum):  # NEW ENUM
    CORPORATE = "CORPORATE"  # Current behavior: scoped to one corporate
    BROKER = "BROKER"  # New: broker-admin, manages all its corporates


class PolicyStatus(str, enum.Enum):  # NEW ENUM
    PENDING_ISSUANCE = "PENDING_ISSUANCE"  # Sent, awaiting confirmation
    ISSUED = "ISSUED"  # Insurer confirmed coverage
    SOFT_REJECTED = "SOFT_REJECTED"  # Business rule failure (age limit, etc.)
    LAPSED = "LAPSED"  # Coverage ended
    CANCELLED = "CANCELLED"

#Differentiate API vs File Uploads
class SyncSource(str, enum.Enum):
    ONLINE = "ONLINE"
    BATCH = "BATCH"

class Broker(Base, AuditMixin):
    __tablename__ = "brokers"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=False)
    allowed_formats = Column(JSON, default=["csv", "xlsx"])

    corporates = relationship("Corporate", back_populates="broker")
    api_keys = relationship("ApiKey", back_populates="broker")  # NEW


class Corporate(Base, AuditMixin):
    __tablename__ = "corporates"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    broker_id = Column(String, ForeignKey("brokers.id"))
    name = Column(String, nullable=False)
    webhook_url = Column(String)
    insurer_provider = Column(String, default="standard")  # e.g., 'hdfc_ergo', 'icici'
    insurer_format = Column(String, default="json") # 'json', 'xml', 'csv', 'excel'
    delivery_channel = Column(Enum(DeliveryChannel), default=DeliveryChannel.WEBHOOK)
    base_folder = Column(String, default="outbound_files/default")
    last_report_path = Column(String, nullable=True)

    broker = relationship("Broker", back_populates="corporates")
    api_keys = relationship("ApiKey", back_populates="corporate")
    hrms_provider = Column(String, default="standard", server_default="standard")
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
    corporate_id = Column(String, ForeignKey("corporates.id"), nullable=True)
    broker_id = Column(String, ForeignKey("brokers.id"), nullable=True)
    scope = Column(Enum(ApiKeyScope), default=ApiKeyScope.CORPORATE)  # NEW
    is_active = Column(Boolean, default=True)

    corporate = relationship("Corporate", back_populates="api_keys")
    broker = relationship("Broker", back_populates="api_keys")  # NEW relationship


# --- 2. Business Data (Census & Logs) ---

class Employee(Base, AuditMixin):
    __tablename__ = "employees"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    corporate_id = Column(String, ForeignKey("corporates.id"), index=True)
    employee_code = Column(String, nullable=False, index=True)
    first_name = Column(String)
    last_name = Column(String)
    email = Column(String, nullable=True)
    gender = Column(String, nullable=True)
    date_of_birth = Column(Date, nullable=True)
    date_of_joining = Column(Date, nullable=True)
    sum_insured = Column(Float)
    policy_number = Column(String, nullable=True)
    status = Column(String, default="active")
    date_of_leaving = Column(Date, nullable=True)
    resignation_reason = Column(String, nullable=True)

    delivery_status = Column(Enum(SyncStatus), default=SyncStatus.PENDING)
    policy_status = Column(Enum(PolicyStatus), nullable=True)  # NEW

    policy_effective_date = Column(Date, nullable=True)
    insurer_reference_id = Column(String, nullable=True)  # insurer's internal ID
    rejection_reason = Column(String, nullable=True)  # human-readable rejection

    corporate = relationship("Corporate", back_populates="employees")


class SyncLog(Base):
    __tablename__ = "sync_logs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    corporate_id = Column(String, ForeignKey("corporates.id"))
    transaction_id = Column(String, index=True, nullable=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=True)  # Who triggered the sync?
    transaction_type = Column(String)
    source = Column(Enum(SyncSource), nullable=False, default=SyncSource.ONLINE)
    payload = Column(JSON)
    status = Column(String)
    sync_status = Column(Enum(SyncStatus), default=SyncStatus.PENDING)
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    raw_response = Column(JSON, nullable=True)  # Insurer's exact response
    error_message = Column(String, nullable=True)
    file_path = Column(String, nullable=True)  # Path to offline CSV/Excel
    timestamp = Column(DateTime, server_default=func.now())

    # --- PILLAR 1: Reconciliation fields ---
    insurer_reference_id = Column(String, nullable=True, index=True)
    callback_received_at = Column(DateTime, nullable=True)
    rejection_reason = Column(String, nullable=True)

    # Snapshot of policy_status at the time this log was processed.
    # Prevents bleed: employee.policy_status changes over time but log history must be immutable.
    policy_status = Column(String, nullable=True)

    # True when HR explicitly bypassed the "not enrolled" guard (force-removal escape hatch).
    is_force = Column(Boolean, default=False, nullable=False)

    events = relationship("SyncLogEvent", back_populates="sync_log", cascade="all, delete-orphan")

class SyncLogEvent(Base):
    __tablename__ = "sync_log_events"
    id = Column(Integer, primary_key=True, autoincrement=True)
    sync_log_id = Column(Integer, ForeignKey("sync_logs.id", ondelete="CASCADE"), index=True)
    event_status = Column(Enum(SyncStatus), nullable=False)
    actor = Column(String, nullable=False) # e.g., "HR_USER", "CELERY", "SYSTEM"
    details = Column(JSON, nullable=True) # Context like errors or webhook IDs
    # Set only when this event caused a policy_status transition (null = no change)
    policy_status = Column(String, nullable=True)
    timestamp = Column(DateTime, server_default=func.now())

    sync_log = relationship("SyncLog", back_populates="events")