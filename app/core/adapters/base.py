from abc import ABC, abstractmethod
from typing import Dict, Any
from datetime import datetime
import uuid


class BaseHRMSAdapter(ABC):
    @abstractmethod
    def normalize_addition(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Translates HRMS-specific JSON into our standard AddEmployeeRequest dict."""
        pass

    @abstractmethod
    def normalize_deletion(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Translates HRMS-specific JSON into our standard RemoveEmployeeRequest dict."""
        pass

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