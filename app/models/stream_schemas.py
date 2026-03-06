from pydantic import BaseModel, Field, field_validator
from datetime import date, datetime
from typing import Optional

from app.models.schemas import GenderEnum, RelationshipEnum


# Base Event (Shared fields — real-time API tracking metadata)
class EmployeeEvent(BaseModel):
    transaction_id: Optional[str]      = None
    timestamp:      Optional[datetime] = None


# 1. Payload for Adding an Employee (Real-Time)
class AddEmployeeRequest(EmployeeEvent):
    employee_code:   str            = Field(..., min_length=1)
    first_name:      str            = Field(..., min_length=1)
    last_name:       str            = Field(..., min_length=1)
    date_of_birth:   Optional[date] = None
    date_of_joining: date
    gender:          Optional[GenderEnum]      = GenderEnum.UNKNOWN
    relationship:    RelationshipEnum          = RelationshipEnum.SELF
    sum_insured:     float                     = 0.0

    @field_validator("gender", mode="before")
    @classmethod
    def normalize_gender(cls, v):
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return GenderEnum.UNKNOWN
        _MAP = {
            "m": "Male",   "male": "Male",
            "f": "Female", "female": "Female",
            "o": "Other",  "other": "Other",
            "u": "Unknown", "unknown": "Unknown",
        }
        return _MAP.get(str(v).lower().strip(), "Unknown")

    @field_validator("relationship", mode="before")
    @classmethod
    def normalize_relationship(cls, v):
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return "Self"
        _MAP = {
            "self": "Self",     "employee": "Self",
            "spouse": "Spouse", "wife": "Spouse",   "husband": "Spouse",
            "child": "Child",   "son": "Child",     "daughter": "Child",
            "parent": "Parent", "father": "Parent", "mother": "Parent",
            "sibling": "Sibling",
        }
        return _MAP.get(str(v).lower().strip(), str(v).strip())

    @field_validator("date_of_birth")
    @classmethod
    def check_dob(cls, v):
        if v and v > date.today():
            raise ValueError("Date of birth cannot be in the future")
        return v

    @field_validator("date_of_joining")
    @classmethod
    def check_doj(cls, v):
        if v > date.today():
            raise ValueError("Date of joining cannot be in the future")
        return v


# 2. Payload for Removing an Employee (Real-Time)
class RemoveEmployeeRequest(EmployeeEvent):
    employee_code:  str            = Field(..., min_length=1)
    member_id:      Optional[str]  = Field(None, description="The insurance policy or member ID")
    date_of_leaving: date