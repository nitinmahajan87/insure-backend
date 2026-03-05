"""
Zoho People Adapter
===================
Handles two directions:

1. INBOUND (Webhook):  Zoho pushes employee events to our /api/v1/stream/* endpoints.
   Zoho webhook payload uses PascalCase fields: EmployeeID, FirstName, LastName, etc.

2. OUTBOUND (Polling): We GET employees from Zoho People API when webhook config
   isn't available. Uses OAuth 2.0.
   GET https://people.zoho.com/api/forms/employee/getRecords

Zoho People API docs: https://www.zoho.com/people/api/bulk-records.html
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
import requests

from app.core.adapters.base import BaseHRMSAdapter, NormalizedEmployee

# Zoho People bulk-records API endpoint
_ZOHO_EMPLOYEE_URL = "https://people.zoho.com/api/forms/employee/getRecords"


class ZohoAdapter(BaseHRMSAdapter):

    # ── Inbound: Zoho webhook → canonical dict ────────────────────────────────
    # Zoho webhook payload uses PascalCase + their own field label names.

    def normalize_addition(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = self.get_base_metadata()
        normalized.update({
            # EmployeeID or EmpID — Zoho uses both across webhook versions
            "employee_code": str(raw_payload.get("EmployeeID", raw_payload.get("EmpID", ""))),
            "first_name":    raw_payload.get("FirstName", raw_payload.get("First_Name", "")),
            "last_name":     raw_payload.get("LastName",  raw_payload.get("Last_Name", "")),
            "email":         raw_payload.get("EmailID",   raw_payload.get("Email", None)),
            "date_of_birth": self.parse_flexible_date(
                raw_payload.get("DateofBirth", raw_payload.get("DOB", raw_payload.get("Date_of_Birth")))
            ),
            "date_of_joining": self.parse_flexible_date(
                raw_payload.get("DateofJoining", raw_payload.get("DOJ", raw_payload.get("Date_of_Joining")))
            ),
            "gender":       raw_payload.get("Gender", "Unknown"),
            "relationship": "Self",
            "sum_insured":  0.0,
        })
        return {k: v for k, v in normalized.items() if v is not None}

    def normalize_deletion(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = self.get_base_metadata()
        normalized.update({
            "employee_code":  str(raw_payload.get("EmployeeID", raw_payload.get("EmpID", ""))),
            "date_of_leaving": self.parse_flexible_date(
                raw_payload.get("ExitDate", raw_payload.get("DateofExit", raw_payload.get("Date_of_Exit")))
            ),
        })
        return {k: v for k, v in normalized.items() if v is not None}

    # ── Outbound: Poll Zoho GET API → list of canonical employees ─────────────

    def fetch_employees(
        self,
        credentials: Dict[str, Any],
        since: Optional[datetime] = None,
    ) -> Optional[List[NormalizedEmployee]]:
        """
        Poll Zoho People API for all active employees.

        credentials dict keys:
            oauth_token  — Zoho OAuth 2.0 Bearer token (required)
            search_value — optional filter (e.g. department name)

        Zoho returns up to 200 records per page. We paginate using sIndex.
        """
        oauth_token = credentials.get("oauth_token")
        if not oauth_token:
            return None

        headers = {"Authorization": f"Zoho-oauthtoken {oauth_token}"}
        results: List[NormalizedEmployee] = []
        s_index = 1
        limit = 200

        while True:
            params: Dict[str, Any] = {"sIndex": s_index, "limit": limit, "searchColumn": "Employeestatus",
                                      "searchValue": "Active"}
            # Zoho does not support a native modifiedSince filter on getRecords,
            # but we can filter active employees by status.

            try:
                resp = requests.get(_ZOHO_EMPLOYEE_URL, headers=headers, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception:
                break

            # Zoho wraps records under {"response": {"result": [{...}, ...]}}
            records = (
                data.get("response", {}).get("result", [])
                or data.get("result", [])
                or []
            )
            if not records:
                break

            for record in records:
                # Each record is a dict of {field_label: value}
                normalized = self.normalize_polled_employee(record)
                if normalized.get("employee_code"):
                    results.append(normalized)

            if len(records) < limit:
                break
            s_index += limit

        return results

    def normalize_polled_employee(self, raw_record: Dict[str, Any]) -> NormalizedEmployee:
        """
        Translate a single Zoho GET API employee record into our canonical dict.
        Zoho GET responses use their internal label names (same as webhook but
        wrapped in a dict keyed by form field label).
        """
        return {
            "employee_code": str(raw_record.get("EmployeeID", raw_record.get("Employee_ID", ""))),
            "first_name":    raw_record.get("FirstName",   raw_record.get("First Name", "")),
            "last_name":     raw_record.get("LastName",    raw_record.get("Last Name", "")),
            "email":         raw_record.get("EmailID",     raw_record.get("Email address", None)),
            "date_of_birth": self.parse_flexible_date(
                raw_record.get("DateofBirth", raw_record.get("Date Of Birth"))
            ),
            "date_of_joining": self.parse_flexible_date(
                raw_record.get("DateofJoining", raw_record.get("Date of Joining"))
            ),
            "gender": raw_record.get("Gender", "Unknown"),
        }
