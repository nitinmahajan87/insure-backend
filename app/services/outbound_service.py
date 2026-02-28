import polars as pl
import os
from datetime import datetime
from app.models.schemas import InsuranceUpdateReport

class OutboundTransformer:
    @staticmethod
    def to_payload(report: InsuranceUpdateReport):
        """Prepares the clean JSON payload for an API call."""
        # We extract the relevant list based on what was processed
        data = report.additions if report.additions else report.deletions
        return {
            "timestamp": datetime.now().isoformat(),
            "transaction_type": "ADDITION" if report.additions else "DELETION",
            "count": report.total_records,
            "records": [r.model_dump(mode='json') for r in data]
        }

    @staticmethod
    def to_excel(report: InsuranceUpdateReport, filename: str) -> str:
        """Generates an Excel file for legacy systems."""
        output_dir = "outbound_files"
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, filename)

        # Convert our Pydantic models back to a Polars DataFrame
        data = report.additions if report.additions else report.deletions
        df = pl.from_dicts([r.model_dump() for r in data])

        # Write to Excel using the high-performance xlsxwriter engine
        df.write_excel(file_path)
        return file_path