from app.core.outbound.base import BaseInsurerAdapter
from app.core.outbound.icici_lombard import IciciLombardAdapter
from app.core.outbound.hdfc_ergo import HdfcErgoAdapter
from typing import Dict, Any


class StandardJSONAdapter(BaseInsurerAdapter):
    def transform_addition(self, p: Dict[str, Any]) -> Dict[str, Any]: return p

    def transform_deletion(self, p: Dict[str, Any]) -> Dict[str, Any]: return p

    def get_headers(self, api_key: str) -> Dict[str, str]:
        return {"X-API-Key": api_key, "Content-Type": "application/json"}


def get_insurer_adapter(provider_name: str) -> BaseInsurerAdapter:
    if not provider_name:
        return StandardJSONAdapter()

    adapters = {
        "icici_lombard": IciciLombardAdapter(),
        "hdfc_ergo": HdfcErgoAdapter(),
        "standard": StandardJSONAdapter()
    }
    return adapters.get(provider_name.lower().strip(), StandardJSONAdapter())