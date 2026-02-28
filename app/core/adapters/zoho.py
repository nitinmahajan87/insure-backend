from typing import Dict, Any
from app.core.adapters.base import BaseHRMSAdapter


class ZohoAdapter(BaseHRMSAdapter):
    def normalize_addition(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        # 1. Generate the base tracking data (transaction_id, timestamp)
        normalized = self.get_base_metadata()

        # 2. Add the mapped Zoho data (handling different casing variations Zoho uses)
        normalized.update({
            "employee_code": str(raw_payload.get("EmployeeID", raw_payload.get("EmpID", ""))),
            "first_name": raw_payload.get("FirstName", ""),
            "last_name": raw_payload.get("LastName", ""),
            "date_of_birth": self.parse_flexible_date(raw_payload.get("DateofBirth", raw_payload.get("DOB"))),
            "date_of_joining": self.parse_flexible_date(raw_payload.get("DateofJoining", raw_payload.get("DOJ"))),
            "gender": raw_payload.get("Gender", "Unknown"),
            "relationship": "Self",
            "sum_insured": 0.0,
        })

        # 3. Clean up the dictionary by removing strictly None values
        return {k: v for k, v in normalized.items() if v is not None}

    def normalize_deletion(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = self.get_base_metadata()

        normalized.update({
            "employee_code": str(raw_payload.get("EmployeeID", raw_payload.get("EmpID", ""))),
            "date_of_leaving": self.parse_flexible_date(raw_payload.get("ExitDate", raw_payload.get("DateofExit")))
        })

        return {k: v for k, v in normalized.items() if v is not None}