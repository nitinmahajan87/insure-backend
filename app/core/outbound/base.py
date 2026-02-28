from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple


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