"""
Keka HR Adapter
===============
Handles three directions:

1. INBOUND (Webhook):  Keka pushes employee events to our /api/v1/stream/* endpoints.
   Keka webhook payload uses camelCase fields: employeeNumber, firstName, etc.

2. INBOUND (Batch file): HR admin uploads a Keka CSV/Excel export.
   Column names are normalised by Polars then translated via ADDITION_COLUMN_MAP /
   DELETION_COLUMN_MAP before Pydantic validation.

3. OUTBOUND (Polling): We GET employees from Keka HRIS API when webhook config
   isn't available. Uses OAuth 2.0 Bearer token.
   GET https://{company}.{env}.com/api/v1/hris/employees

Keka developer docs: https://developers.keka.com/reference/get_hris-employees
"""
from typing import ClassVar, Dict, Any, List, Optional
from datetime import datetime
import requests

from app.core.adapters.base import BaseHRMSAdapter, NormalizedEmployee, _SPLIT_NAME_SENTINEL

# Keka gender enum mapping: Keka returns int 0-3
_KEKA_GENDER_MAP = {0: "Unknown", 1: "Male", 2: "Female", 3: "Other"}


class KekaAdapter(BaseHRMSAdapter):

    # ── Batch file column maps ─────────────────────────────────────────────────
    # Keys = column name after Polars normalisation (lowercase + underscore).
    # Keka CSV/Excel exports use a mix of snake_case and compact forms.
    ADDITION_COLUMN_MAP: ClassVar[Dict[str, str]] = {
        # Employee identifier
        "emp_id":           "employee_code",
        "employee_id":      "employee_code",
        "employee_number":  "employee_code",
        "empno":            "employee_code",
        # Name (Keka may export a combined "Employee Name" column)
        "employee_name":    _SPLIT_NAME_SENTINEL,
        "name":             _SPLIT_NAME_SENTINEL,
        "firstname":        "first_name",
        "lastname":         "last_name",
        # Dates — Keka exports use their UI label names
        "dob":              "date_of_birth",
        "birth_date":       "date_of_birth",
        "doj":              "date_of_joining",
        "joining_date":     "date_of_joining",
        "date_joined":      "date_of_joining",
        # pre-Phase-3 normalization forms (Polars lowercases camelCase as one token)
        "dateofbirth":      "date_of_birth",
        "dateofjoining":    "date_of_joining",
        # Sum insured — Keka doesn't natively track SI; may appear in custom fields
        "sum_assured":      "sum_insured",
        "cover_amount":     "sum_insured",
        "insured_amount":   "sum_insured",
        # Email / gender — already canonical in Keka exports; listed for completeness
        "work_email":       "email",
        "official_email":   "email",
    }

    DELETION_COLUMN_MAP: ClassVar[Dict[str, str]] = {
        # Employee identifier
        "emp_id":              "employee_code",
        "employee_id":         "employee_code",
        "employee_number":     "employee_code",
        "empno":               "employee_code",
        # Leaving date — Keka uses several label names across versions
        "exit_date":           "date_of_leaving",
        "last_working_date":   "date_of_leaving",
        "last_working_day":    "date_of_leaving",
        "relieving_date":      "date_of_leaving",
        "separation_date":     "date_of_leaving",
        # pre-Phase-3 compact forms
        "lastworkingdate":     "date_of_leaving",
        "exitdate":            "date_of_leaving",
    }

    # ── Inbound: Keka webhook → canonical dict ────────────────────────────────
    # Keka webhook payload uses camelCase.

    def normalize_addition(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = self.get_base_metadata()
        normalized.update({
            "employee_code":   str(raw_payload.get("employeeNumber", raw_payload.get("id", ""))),
            "first_name":      raw_payload.get("firstName", ""),
            "last_name":       raw_payload.get("lastName", ""),
            "email":           raw_payload.get("email", raw_payload.get("workEmail", None)),
            "date_of_birth":   self.parse_flexible_date(raw_payload.get("dateOfBirth")),
            "date_of_joining": self.parse_flexible_date(
                raw_payload.get("joiningDate", raw_payload.get("dateOfJoining"))
            ),
            "gender":       self._resolve_gender(raw_payload.get("gender")),
            "relationship": "Self",
            "sum_insured":  0.0,
        })
        return {k: v for k, v in normalized.items() if v is not None}

    def normalize_deletion(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = self.get_base_metadata()
        normalized.update({
            "employee_code":  str(raw_payload.get("employeeNumber", raw_payload.get("id", ""))),
            "date_of_leaving": self.parse_flexible_date(
                raw_payload.get("exitDate", raw_payload.get("relievingDate", raw_payload.get("lastWorkingDate")))
            ),
        })
        return {k: v for k, v in normalized.items() if v is not None}

    # ── Outbound: Poll Keka GET API → list of canonical employees ─────────────

    def fetch_employees(
        self,
        credentials: Dict[str, Any],
        since: Optional[datetime] = None,
    ) -> Optional[List[NormalizedEmployee]]:
        """
        Poll Keka HRIS API for employees.

        credentials dict keys:
            oauth_token  — Keka OAuth 2.0 Bearer token (required)
            base_url     — e.g. "https://acmecorp.keka.com" (required)

        Keka supports `lastModified` UTC filter and pagination (default 100, max 200).
        Ref: https://developers.keka.com/reference/get_hris-employees
        """
        oauth_token = credentials.get("oauth_token")
        base_url = credentials.get("base_url", "").rstrip("/")
        if not oauth_token or not base_url:
            return None

        url = f"{base_url}/api/v1/hris/employees"
        headers = {"Authorization": f"Bearer {oauth_token}"}
        results: List[NormalizedEmployee] = []
        page = 1
        page_size = 200

        while True:
            params: Dict[str, Any] = {"pageSize": page_size, "page": page}
            if since:
                # Keka supports ISO 8601 UTC for incremental syncs
                params["lastModified"] = since.strftime("%Y-%m-%dT%H:%M:%SZ")

            try:
                resp = requests.get(url, headers=headers, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception:
                break

            # Keka wraps records under {"data": [...]}
            records = data.get("data", data if isinstance(data, list) else [])
            if not records:
                break

            for record in records:
                normalized = self.normalize_polled_employee(record)
                if normalized.get("employee_code"):
                    results.append(normalized)

            if len(records) < page_size:
                break
            page += 1

        return results

    def normalize_polled_employee(self, raw_record: Dict[str, Any]) -> NormalizedEmployee:
        """
        Translate a single Keka GET API employee record into our canonical dict.
        Keka REST API returns camelCase — same convention as their webhooks.
        """
        return {
            "employee_code":   str(raw_record.get("employeeNumber", raw_record.get("id", ""))),
            "first_name":      raw_record.get("firstName", ""),
            "last_name":       raw_record.get("lastName", ""),
            "email":           raw_record.get("email", raw_record.get("workEmail", None)),
            "date_of_birth":   self.parse_flexible_date(raw_record.get("dateOfBirth")),
            "date_of_joining": self.parse_flexible_date(
                raw_record.get("joiningDate", raw_record.get("joiningDetails", {}).get("joiningDate"))
            ),
            "gender": self._resolve_gender(raw_record.get("gender")),
        }

    def _resolve_gender(self, raw_gender: Any) -> str:
        """Keka returns gender as int (0-3) or string. Normalize both."""
        if isinstance(raw_gender, int):
            return _KEKA_GENDER_MAP.get(raw_gender, "Unknown")
        if isinstance(raw_gender, str) and raw_gender:
            return raw_gender
        return "Unknown"
