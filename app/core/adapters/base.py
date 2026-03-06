from abc import ABC, abstractmethod
from typing import ClassVar, Dict, Any, List, Optional
from datetime import datetime, date
import uuid


# ── Canonical normalized employee dict produced by every adapter ──────────────
# This is the shape that both inbound webhook normalization AND outbound polling
# must produce. It maps directly to AddEmployeeRequest / PortalAddRequest.
NormalizedEmployee = Dict[str, Any]
# Required keys: employee_code (str), first_name (str), last_name (str)
# Optional keys: email, date_of_birth, date_of_joining, gender

# Special sentinel value in a COLUMN_MAP: when a source column contains a full
# name (e.g. "insured_name") this value triggers a first/last name split instead
# of a direct rename.
_SPLIT_NAME_SENTINEL = "__split_to_names__"


class BaseHRMSAdapter(ABC):

    # ── Batch file column maps ─────────────────────────────────────────────────
    # Keys   = column name as it appears AFTER Polars header normalisation
    #          (lowercased, spaces/dots → underscores).
    # Values = canonical field name used by our Pydantic schemas, OR the sentinel
    #          _SPLIT_NAME_SENTINEL to split a full-name column into first/last.
    #
    # Subclasses override these with provider-specific column aliases.
    # The StandardAdapter covers generic / unknown HR systems.
    ADDITION_COLUMN_MAP: ClassVar[Dict[str, str]] = {}
    DELETION_COLUMN_MAP: ClassVar[Dict[str, str]] = {}

    # ── Inbound: HRMS webhook → canonical dict ────────────────────────────────

    @abstractmethod
    def normalize_addition(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Translates HRMS-specific webhook JSON into our standard AddEmployeeRequest dict."""
        pass

    @abstractmethod
    def normalize_deletion(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Translates HRMS-specific webhook JSON into our standard RemoveEmployeeRequest dict."""
        pass

    # ── Batch file row normalisation ──────────────────────────────────────────

    def normalize_file_row(self, row: Dict[str, Any], is_deletion: bool = False) -> Dict[str, Any]:
        """
        Translate a single file row dict (from Polars df.to_dicts(), after header
        normalisation) into canonical field names using ADDITION_COLUMN_MAP or
        DELETION_COLUMN_MAP.

        Called by the processor AFTER Polars header normalisation and BEFORE
        Pydantic validation.  Performs three steps:

        1. Apply the column map — rename provider-specific keys to canonical ones.
        2. Handle the _SPLIT_NAME_SENTINEL — split a full-name column into
           first_name / last_name when the HR file has a single name column.
        3. Re-parse any date fields that are still raw strings.  Polars only
           casts columns it recognised by canonical name; aliased columns that
           were renamed in step 1 arrive as strings and need explicit parsing.
        """
        col_map = self.DELETION_COLUMN_MAP if is_deletion else self.ADDITION_COLUMN_MAP

        # Step 1 & 2: apply column map + handle name split sentinel
        translated: Dict[str, Any] = {}
        for k, v in row.items():
            target = col_map.get(k, k)  # unmapped keys pass through as-is

            if target == _SPLIT_NAME_SENTINEL:
                parts = str(v or "").strip().split(" ", 1)
                # Only set first_name / last_name if they are not already present
                # (a more-specific column like "first_name" takes precedence).
                translated.setdefault("first_name", parts[0] if parts else "")
                translated.setdefault("last_name", parts[1] if len(parts) > 1 else "")
            else:
                translated[target] = v

        # Step 3: parse date strings that Polars left as raw strings after renaming
        for date_field in ("date_of_birth", "date_of_joining", "date_of_leaving"):
            val = translated.get(date_field)
            if val is not None and isinstance(val, str):
                translated[date_field] = self.parse_flexible_date(val)
            # date / datetime objects (Polars-parsed canonical columns) pass through

        return translated

    # ── Outbound: Poll HRMS GET API → list of canonical employees ────────────

    def fetch_employees(
        self,
        credentials: Dict[str, Any],
        since: Optional[datetime] = None,
    ) -> Optional[List[NormalizedEmployee]]:
        """
        Poll the HRMS's own GET API and return a list of normalized employee dicts.

        Args:
            credentials: Provider-specific auth config (api_key, base_url, etc.)
                         Stored per-corporate in the DB as hrms_credentials (JSON).
            since:       If provided, fetch only records modified after this timestamp.
                         Enables incremental delta sync instead of full fetch.

        Returns:
            List of NormalizedEmployee dicts, or None if polling is unsupported.
            Each dict must contain at minimum: employee_code, first_name, last_name.

        Override in concrete adapters that support GET-based polling.
        Default returns None — used to signal "polling not supported" to the
        scheduler task, which will then skip this corporate.
        """
        return None

    def normalize_polled_employee(self, raw_record: Dict[str, Any]) -> NormalizedEmployee:
        """
        Translate a single raw record from the HRMS GET API into our canonical dict.
        Called by fetch_employees() for each record in the polling response.
        Override alongside fetch_employees().
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not implement polling.")

    def parse_flexible_date(self, date_str: Any) -> str | None:
        """A utility to safely parse various HRMS date formats into strict YYYY-MM-DD."""
        if not date_str:
            return None

        # Common HRMS date formats
        formats = [
            "%Y-%m-%d",  # 2024-12-31
            "%d-%m-%Y",  # 31-12-2024
            "%d/%m/%Y",  # 31/12/2024
            "%Y-%m-%dT%H:%M:%SZ",  # 2024-12-31T00:00:00Z (ISO)
            "%m-%d-%Y"  # 12-31-2024 (US)
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(str(date_str).strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        # If it completely fails, return None and let Pydantic throw a 422 validation error
        return None

    def get_base_metadata(self) -> dict:
        """Automatically generates tracking metadata for incoming webhooks."""
        return {
            "transaction_id": f"WEBHOOK-{uuid.uuid4().hex[:8].upper()}",
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }