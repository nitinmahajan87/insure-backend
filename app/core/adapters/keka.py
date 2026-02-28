from typing import Dict, Any
from app.core.adapters.base import BaseHRMSAdapter


class KekaAdapter(BaseHRMSAdapter):
    def normalize_addition(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        # 1. Generate the base tracking data
        normalized = self.get_base_metadata()

        # 2. Add the mapped Keka data
        normalized.update({
            "employee_code": str(raw_payload.get("employeeNumber", raw_payload.get("id", ""))),
            "first_name": raw_payload.get("firstName", ""),
            "last_name": raw_payload.get("lastName", ""),
            "date_of_birth": self.parse_flexible_date(raw_payload.get("dateOfBirth")),
            "date_of_joining": self.parse_flexible_date(raw_payload.get("joiningDate")),
            "gender": raw_payload.get("gender", "Unknown"),
            "relationship": "Self",
            "sum_insured": 0.0,
        })

        # 3. Clean up the dictionary by removing any keys that are strictly None
        # This prevents Pydantic from crashing on 'None' dates
        return {k: v for k, v in normalized.items() if v is not None}

    def normalize_deletion(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = self.get_base_metadata()
        normalized.update({
            "employee_code": str(raw_payload.get("employeeNumber", raw_payload.get("id", ""))),
            "date_of_leaving": self.parse_flexible_date(raw_payload.get("exitDate", raw_payload.get("relievingDate")))
        })
        return {k: v for k, v in normalized.items() if v is not None}