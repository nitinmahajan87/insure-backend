import polars as pl
import os
from datetime import datetime
from typing import List, Tuple


class OutboundTransformer:
    @staticmethod
    def to_file(data: List[dict], filename_prefix: str, output_dir: str, format_type: str) -> Tuple[str, str]:
        """
        Universal file generator for batch ingestion and sweepers.
        Outputs CSV or Excel based on insurer_format.
        """
        os.makedirs(output_dir, exist_ok=True)

        # Guard against empty data
        if not data:
            raise ValueError("No data provided to generate file.")

        df = pl.from_dicts(data)

        # Determine format (default to excel if blank or unknown)
        is_csv = format_type and format_type.lower() == "csv"
        extension = "csv" if is_csv else "xlsx"

        filename = f"{filename_prefix}_{datetime.now().strftime('%Y%m%d%H%M%S')}.{extension}"
        file_path = os.path.join(output_dir, filename)

        if is_csv:
            df.write_csv(file_path)
        else:
            df.write_excel(file_path)

        return file_path, filename