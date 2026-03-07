import enum
from decimal import Decimal
from typing import Any, Dict, List, Literal, Optional
from datetime import date

from pydantic import BaseModel, field_validator


# ── Shared Enums ──────────────────────────────────────────────────────────────

class GenderEnum(str, enum.Enum):
    MALE    = "Male"
    FEMALE  = "Female"
    OTHER   = "Other"
    UNKNOWN = "Unknown"


class RelationshipEnum(str, enum.Enum):
    SELF    = "Self"
    SPOUSE  = "Spouse"
    CHILD   = "Child"
    PARENT  = "Parent"
    SIBLING = "Sibling"


# ── Row-level error reporting ─────────────────────────────────────────────────

class RejectedRow(BaseModel):
    row_index: int
    raw_data:  Dict[str, Any]
    errors:    List[str]


# ── Inbound Batch Records ─────────────────────────────────────────────────────

class EmployeeBase(BaseModel):
    employee_code: str
    first_name:    str
    last_name:     str
    date_of_birth: Optional[date]       = None
    gender:        Optional[GenderEnum] = None

    @field_validator("gender", mode="before")
    @classmethod
    def normalize_gender(cls, v: Any) -> Optional[str]:
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return None
        _MAP = {
            "m": "Male",   "male": "Male",
            "f": "Female", "female": "Female",
            "o": "Other",  "other": "Other",
            "u": "Unknown", "unknown": "Unknown",
        }
        return _MAP.get(str(v).lower().strip(), "Unknown")


class AdditionRecord(EmployeeBase):
    relationship:   RelationshipEnum = RelationshipEnum.SELF
    sum_insured:    Decimal
    date_of_joining: Optional[date] = None

    @field_validator("relationship", mode="before")
    @classmethod
    def normalize_relationship(cls, v: Any) -> str:
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return "Self"
        _MAP = {
            "self": "Self",     "employee": "Self",
            "spouse": "Spouse", "wife": "Spouse",   "husband": "Spouse",
            "child": "Child",   "son": "Child",     "daughter": "Child", "kid": "Child",
            "parent": "Parent", "father": "Parent", "mother": "Parent",
            "sibling": "Sibling", "brother": "Sibling", "sister": "Sibling",
        }
        return _MAP.get(str(v).lower().strip(), str(v).strip())

    @field_validator("sum_insured", mode="before")
    @classmethod
    def validate_sum_insured(cls, v: Any) -> Decimal:
        if v is None or (isinstance(v, str) and v.strip() in ("", "-", "NA", "null", "0")):
            raise ValueError("sum_insured is required and cannot be empty or zero")
        try:
            val = Decimal(str(v))
        except Exception:
            raise ValueError(f"sum_insured must be a valid number, got: {v!r}")
        if val <= 0:
            raise ValueError(f"sum_insured must be greater than zero, got: {val}")
        return val


class DeletionRecord(BaseModel):
    employee_code:  str
    member_id:      Optional[str] = None
    date_of_leaving: date


# ── Processed Report ──────────────────────────────────────────────────────────

class InsuranceUpdateReport(BaseModel):
    total_rows_in_file: int
    accepted_count:     int
    rejected_count:     int
    additions:          List[AdditionRecord]
    deletions:          List[DeletionRecord]
    rejected_rows:      List[RejectedRow]
    status:             Literal["success", "partial", "failed"]


# ── API Response ──────────────────────────────────────────────────────────────

class IngestionResponse(BaseModel):
    filename:        str
    accepted_count:  int
    rejected_count:  int
    message:         str
    report:          InsuranceUpdateReport
    file_download_url: str


class BatchAcceptedResponse(BaseModel):
    """
    202 Accepted response for batch uploads.
    Returns parse results immediately; DB writes and Celery fan-out happen
    in the background via process_master_batch.
    """
    filename:      str
    accepted_count: int
    rejected_count: int
    message:       str
    rejected_rows: List[RejectedRow]


# ── Insurer Response File ─────────────────────────────────────────────────────
# Schemas for processing the response Excel/CSV sent back by the insurer to the broker.

class InsurerResponseRow(BaseModel):
    """One row parsed from the insurer's response file."""
    employee_code:        str
    status:               str               # normalised to uppercase: ISSUED, REJECTED, PENDING …
    policy_number:        Optional[str] = None
    effective_date:       Optional[date] = None
    certificate_number:   Optional[str] = None
    insurer_reference_id: Optional[str] = None
    rejection_reason:     Optional[str] = None


class InsurerResponseReport(BaseModel):
    """Summary returned to the broker after processing the insurer response file."""
    total_rows:          int
    issued_count:        int
    soft_rejected_count: int
    unmatched_count:     int    # employee_code not found / no open SyncLog
    parse_error_count:   int    # rows we could not parse at all
    message:             str
    parse_errors:        List[RejectedRow]  # row-level parse failures for broker to review
