from abc import ABC, abstractmethod
from typing import ClassVar, Dict, Any, Optional


class BaseInsurerAdapter(ABC):

    # ── Outbound batch file column maps ───────────────────────────────────────
    # Keys   = our canonical field name (from AdditionRecord / DeletionRecord)
    # Values = column header the insurer expects in their batch Excel / CSV file
    #
    # When the map is empty (StandardJSONAdapter), build_file_row() strips only
    # internal-tracking fields and passes everything else through unchanged.
    # Concrete adapters override to emit exactly the columns the insurer requires.
    ADDITION_FILE_COLUMNS: ClassVar[Dict[str, str]] = {}
    DELETION_FILE_COLUMNS: ClassVar[Dict[str, str]] = {}

    # Fields that are internal tracking artefacts — never written to outbound files.
    _INTERNAL_FIELDS: ClassVar[frozenset] = frozenset({"transaction_id", "timestamp"})

    def build_file_row(self, canonical: dict, is_deletion: bool = False) -> dict:
        """
        Transform a canonical record dict into the insurer-specific column layout
        for outbound batch files (Excel / CSV).

        Two modes:
        - FILE_COLUMNS defined  → select only the declared fields, rename to
                                  insurer column headers, preserve insertion order.
        - FILE_COLUMNS empty    → passthrough: strip internal-only tracking fields,
                                  keep all business fields.  Used by StandardJSONAdapter
                                  and as a safe fallback.

        None values are preserved so every row has the same columns (Polars needs a
        consistent schema across rows).  The caller / OutboundTransformer may choose
        to coerce them to empty strings if the insurer's file template requires it.
        """
        col_map = self.DELETION_FILE_COLUMNS if is_deletion else self.ADDITION_FILE_COLUMNS

        if col_map:
            return {
                insurer_col: canonical.get(our_col)
                for our_col, insurer_col in col_map.items()
            }

        # Passthrough — strip internal fields only
        return {
            k: v for k, v in canonical.items()
            if k not in self._INTERNAL_FIELDS
        }

    # ── Webhook / API transform methods ──────────────────────────────────────

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