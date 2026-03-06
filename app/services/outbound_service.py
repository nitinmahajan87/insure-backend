import os
import tempfile
import uuid
from datetime import datetime, date
from decimal import Decimal
from typing import List, Optional, Tuple

import polars as pl

from app.core.outbound.base import BaseInsurerAdapter
from app.core.storage import get_storage


def _to_polars_safe(val):
    """
    Convert values that Polars cannot ingest natively into safe Python scalars.
      Decimal  → float   (avoids Polars inference failure on Decimal objects)
      date     → str     ISO YYYY-MM-DD (consistent representation in files)
      datetime → str     ISO
      everything else passes through unchanged (str, int, float, None, bool)
    """
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(val, date):
        return val.strftime("%Y-%m-%d")
    return val


class OutboundTransformer:
    @staticmethod
    def to_file(
        data: List[dict],
        filename_prefix: str,
        output_dir: str,
        format_type: str,
        insurer_adapter: Optional[BaseInsurerAdapter] = None,
        is_deletion: bool = False,
    ) -> Tuple[str, str]:
        """
        Universal file generator for batch ingestion and the delivery sweeper.
        Outputs CSV or Excel based on insurer_format, then uploads to object storage.

        Args:
            data:             List of canonical record dicts (from model_dump()).
            filename_prefix:  e.g. 'addition_report' or 'removal_report'.
            output_dir:       Corporate outbound folder path — used to derive the
                              storage key prefix (basename only; no local disk write).
            format_type:      'csv' → CSV file; anything else → Excel (.xlsx).
            insurer_adapter:  When provided, applies build_file_row() to select and
                              rename columns to the insurer's expected headers.
                              When None, uses base passthrough (strips internals).
            is_deletion:      Passed to build_file_row to select the correct map.

        Returns:
            (s3_key, filename) — s3_key is the object path in the storage bucket.
        """
        if not data:
            raise ValueError("No data provided to generate file.")

        # ── 1. Apply insurer-specific column mapping / selection ──────────────
        adapter = insurer_adapter
        if adapter is None:
            from app.core.outbound.factory import StandardJSONAdapter
            adapter = StandardJSONAdapter()

        mapped: List[dict] = [
            adapter.build_file_row(row, is_deletion=is_deletion) for row in data
        ]

        # ── 2. Coerce complex Python types to Polars-safe scalars ─────────────
        safe: List[dict] = [
            {k: _to_polars_safe(v) for k, v in row.items()}
            for row in mapped
        ]

        # ── 3. Build DataFrame ────────────────────────────────────────────────
        df = pl.from_dicts(safe)

        # ── 4. Determine output format ────────────────────────────────────────
        is_csv = bool(format_type) and format_type.lower() == "csv"
        extension = "csv" if is_csv else "xlsx"
        content_type = (
            "text/csv"
            if is_csv
            else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # ── 5. Collision-safe filename (timestamp + 6-char UUID suffix) ───────
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        uid = uuid.uuid4().hex[:6]
        filename = f"{filename_prefix}_{timestamp}_{uid}.{extension}"

        # ── 6. Write to temp file → upload to object storage → clean up ───────
        # Derive storage key from the corporate folder name (basename of output_dir).
        corp_folder = os.path.basename(output_dir)
        s3_key = f"outbound/{corp_folder}/{filename}"

        tmp_fd, tmp_path = tempfile.mkstemp(suffix=f".{extension}")
        os.close(tmp_fd)
        try:
            if is_csv:
                df.write_csv(tmp_path)
            else:
                df.write_excel(tmp_path)

            with open(tmp_path, "rb") as fobj:
                get_storage().upload_fileobj(fobj, s3_key, content_type=content_type)
        finally:
            os.unlink(tmp_path)

        return s3_key, filename
