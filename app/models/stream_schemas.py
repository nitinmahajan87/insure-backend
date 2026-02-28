from pydantic import BaseModel, Field, field_validator
from datetime import date, datetime
from typing import Optional

# Base Event (Shared fields)
class EmployeeEvent(BaseModel):
    transaction_id: Optional[str] = None
    timestamp: Optional[    datetime] = None

# 1. Payload for Adding an Employee (Real-Time)
class AddEmployeeRequest(EmployeeEvent):
    employee_code: str = Field(..., min_length=1)
    # Replaced insured_name with explicit name fields
    first_name: str = Field(..., min_length=1)
    last_name: str = Field(..., min_length=1)
    date_of_birth: Optional[date] = None
    date_of_joining: date
    gender: Optional[str] = "Unknown"
    relationship: str = "Self"
    sum_insured: float = 0.0

    @field_validator('date_of_birth')
    def check_age(cls, v):
        if v.year > date.today().year:
            raise ValueError('Date of birth cannot be in the future')
        return v

# 2. Payload for Removing an Employee (Real-Time)
class RemoveEmployeeRequest(EmployeeEvent):
    employee_code: str = Field(..., min_length=1)
    member_id: Optional[str] = Field(None, description="The insurance policy or member ID")
    date_of_leaving: date