from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import date, datetime


class EmployeeBase(BaseModel):
    employee_code: str
    # Removed insured_name, added explicit first/last name
    first_name: str
    last_name: str
    date_of_birth: Optional[date] = None
    gender: Optional[str] = None
    transaction_id: Optional[str] = None
    timestamp: Optional[datetime] = None

class AdditionRecord(EmployeeBase):
    relationship: str = "Self"
    sum_insured: float
    date_of_joining: Optional[date] = None
    age: Optional[int] = None

class DeletionRecord(BaseModel):
    employee_code: str
    member_id: Optional[str] = None
    date_of_leaving: date

class InsuranceUpdateReport(BaseModel):
    total_records: int
    additions: List[AdditionRecord]
    deletions: List[DeletionRecord]
    status: str

# --- API Response ---
class IngestionResponse(BaseModel):
    filename: str
    message: str
    report: InsuranceUpdateReport
    api_payload: Dict[str, Any]
    excel_download_url: str