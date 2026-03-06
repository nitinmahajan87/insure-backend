from typing import ClassVar, Dict, Any

from app.core.adapters.base import BaseHRMSAdapter, _SPLIT_NAME_SENTINEL
from app.core.adapters.zoho import ZohoAdapter
from app.core.adapters.keka import KekaAdapter


class StandardAdapter(BaseHRMSAdapter):
    """
    Fallback adapter for HR systems not covered by a named provider.

    Webhook direction: raw payload passes through unchanged (assumes the caller
    already speaks our canonical schema — e.g. the HR portal, Postman tests).

    Batch file direction: ADDITION_COLUMN_MAP and DELETION_COLUMN_MAP cover the
    widest possible range of column name aliases seen across generic HR exports,
    so that HR teams can upload files without renaming headers.
    """

    # ── Catch-all addition column aliases ─────────────────────────────────────
    ADDITION_COLUMN_MAP: ClassVar[Dict[str, str]] = {
        # ── Employee identifier ───────────────────────────────────────────────
        "emp_id":            "employee_code",
        "employee_id":       "employee_code",
        "employeeid":        "employee_code",
        "staff_id":          "employee_code",
        "staffid":           "employee_code",
        "empno":             "employee_code",
        "emp_no":            "employee_code",
        "employee_number":   "employee_code",
        "employeenumber":    "employee_code",
        "personnel_no":      "employee_code",
        "personnel_number":  "employee_code",

        # ── Name — combined full-name column triggers split ───────────────────
        "insured_name":      _SPLIT_NAME_SENTINEL,
        "employee_name":     _SPLIT_NAME_SENTINEL,
        "full_name":         _SPLIT_NAME_SENTINEL,
        "name":              _SPLIT_NAME_SENTINEL,
        # Compact forms (Polars lowercases without inserting underscores)
        "firstname":         "first_name",
        "lastname":          "last_name",

        # ── Dates ─────────────────────────────────────────────────────────────
        "dob":               "date_of_birth",
        "birth_date":        "date_of_birth",
        "birthdate":         "date_of_birth",
        "dateofbirth":       "date_of_birth",   # compact pre-Phase-3 form

        "doj":               "date_of_joining",
        "joining_date":      "date_of_joining",
        "date_joined":       "date_of_joining",
        "start_date":        "date_of_joining",
        "dateofjoining":     "date_of_joining",  # compact pre-Phase-3 form

        # ── Sum insured ───────────────────────────────────────────────────────
        "sum_assured":       "sum_insured",
        "cover_amount":      "sum_insured",
        "coverage_amount":   "sum_insured",
        "insured_amount":    "sum_insured",
        "insurance_amount":  "sum_insured",
        "si":                "sum_insured",
        "cover":             "sum_insured",

        # ── Email ─────────────────────────────────────────────────────────────
        "work_email":        "email",
        "official_email":    "email",
        "email_id":          "email",
        "emailid":           "email",

        # ── Relationship ──────────────────────────────────────────────────────
        # GenderEnum validator in the schema handles value normalisation;
        # the column alias just ensures the key is canonical.
        "relation":          "relationship",
        "relation_type":     "relationship",
        "member_type":       "relationship",

        # ── Gender ────────────────────────────────────────────────────────────
        "sex":               "gender",
    }

    # ── Catch-all deletion column aliases ─────────────────────────────────────
    DELETION_COLUMN_MAP: ClassVar[Dict[str, str]] = {
        # Employee identifier
        "emp_id":              "employee_code",
        "employee_id":         "employee_code",
        "employeeid":          "employee_code",
        "staff_id":            "employee_code",
        "empno":               "employee_code",
        "employee_number":     "employee_code",

        # Leaving date
        "exit_date":           "date_of_leaving",
        "exitdate":            "date_of_leaving",
        "last_working_day":    "date_of_leaving",
        "last_working_date":   "date_of_leaving",
        "lastworkingdate":     "date_of_leaving",
        "relieving_date":      "date_of_leaving",
        "separation_date":     "date_of_leaving",
        "termination_date":    "date_of_leaving",
        "dol":                 "date_of_leaving",
        "date_of_exit":        "date_of_leaving",
        "dateofexit":          "date_of_leaving",
        "offboarding_date":    "date_of_leaving",
    }

    def normalize_addition(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        return raw_payload

    def normalize_deletion(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        return raw_payload


def get_hrms_adapter(provider_name: str) -> BaseHRMSAdapter:
    """Returns the correct HRMS adapter based on the Corporate's hrms_provider setting."""
    if not provider_name:
        return StandardAdapter()

    adapters: Dict[str, BaseHRMSAdapter] = {
        "zoho":     ZohoAdapter(),
        "keka":     KekaAdapter(),
        "standard": StandardAdapter(),
    }

    # .get() with fallback prevents crashes when a provider is misspelled in the DB.
    return adapters.get(provider_name.lower().strip(), StandardAdapter())