from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class BaseInsurerAdapter(ABC):
    @abstractmethod
    def transform_addition(self, standard_payload: Dict[str, Any]) -> Any:
        """Transforms our internal DB payload into the Insurer's required structure."""
        pass

    @abstractmethod
    def transform_deletion(self, standard_payload: Dict[str, Any]) -> Any:
        pass

    @abstractmethod
    def get_headers(self, api_key: str) -> Dict[str, str]:
        """Returns the specific headers required by this insurer."""
        pass

    def check_policy_status(
        self, transaction_id: str, api_key: str
    ) -> Optional[Dict[str, Any]]:
        """
        Optional polling hook for the reconciliation sweeper.

        Poll the insurer's status endpoint for a previously submitted transaction.
        Return None (default) if this insurer does not support status polling —
        the sweeper will log the record for manual review instead.

        Implementations that support polling should return a normalised dict:
            {
                "status": "APPROVED" | "REJECTED" | "PENDING",
                "insurer_reference_id": str | None,
                "policy_number": str | None,
                "policy_effective_date": str | None,   # ISO-8601 date string
                "rejection_reason": str | None,
            }
        """
        return None