from app.core.adapters.base import BaseHRMSAdapter
from app.core.adapters.zoho import ZohoAdapter
from app.core.adapters.keka import KekaAdapter
from typing import Dict, Any


class StandardAdapter(BaseHRMSAdapter):
    """Fallback adapter for systems that already send data matching our Pydantic schema perfectly."""

    def normalize_addition(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        return raw_payload

    def normalize_deletion(self, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
        return raw_payload


def get_hrms_adapter(provider_name: str) -> BaseHRMSAdapter:
    """Returns the correct parsing adapter based on the Corporate's HRMS setting."""
    if not provider_name:
        return StandardAdapter()

    adapters = {
        "zoho": ZohoAdapter(),
        "keka": KekaAdapter(),
        "standard": StandardAdapter()
    }

    # .get() with a fallback prevents the app from crashing if a provider is misspelled in the DB
    return adapters.get(provider_name.lower().strip(), StandardAdapter())