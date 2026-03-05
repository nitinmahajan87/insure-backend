from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime
import uuid


# ── Canonical normalized employee dict produced by every adapter ──────────────
# This is the shape that both inbound webhook normalization AND outbound polling
# must produce. It maps directly to AddEmployeeRequest / PortalAddRequest.
NormalizedEmployee = Dict[str, Any]
# Required keys: employee_code (str), first_name (str), last_name (str)
# Optional keys: email, date_of_birth, date_of_joining, gender


class BaseHRMSAdapter(ABC):

    # ── Inbound: HRMS webhook → canonical dict ────────────────────────────────

    @abstractmethod
    def normalize_addition(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Translates HRMS-specific webhook JSON into our standard AddEmployeeRequest dict."""
        pass

    @abstractmethod
    def normalize_deletion(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Translates HRMS-specific webhook JSON into our standard RemoveEmployeeRequest dict."""
        pass

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