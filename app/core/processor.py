import polars as pl
import os
from app.models.schemas import InsuranceUpdateReport, AdditionRecord, DeletionRecord


def process_additions(file_path: str) -> InsuranceUpdateReport:
    try:
        # Support both CSV and Excel
        if file_path.endswith('.csv'):
            df = pl.read_csv(file_path, ignore_errors=True, null_values=["NA", "null", "-", ""])
        else:
            df = pl.read_excel(file_path)

        # Standardize headers: lowercase, underscores, remove dots
        df.columns = [col.lower().strip().replace(" ", "_").replace(".", "") for col in df.columns]

        # --- SAFETY CHECK ---
        if "date_of_joining" not in df.columns:
            if "date_of_leaving" in df.columns:
                raise ValueError("❌ Wrong File: You uploaded a Deletion file to the Additions endpoint.")
            raise ValueError("Invalid File: Missing 'date_of_joining' column.")

        # 1. Parse Dates safely
        date_cols = ["date_of_joining", "date_of_birth"]
        for col in date_cols:
            if col in df.columns:
                # If already date type (from Excel), this is a no-op; if string, it parses
                df = df.with_columns(pl.col(col).cast(pl.Date, strict=False))

        # 2. Handle Name Mapping (Split insured_name if first_name is missing)
        if "insured_name" in df.columns and "first_name" not in df.columns:
            # Simple split: first word is first_name, rest is last_name
            df = df.with_columns([
                pl.col("insured_name").str.splitn(" ", 2).struct.field("field_0").alias("first_name"),
                pl.col("insured_name").str.splitn(" ", 2).struct.field("field_1").fill_null("").alias("last_name")
            ])

        records = []
        for row in df.to_dicts():
            # Skip rows where critical data is missing
            if not row.get("employee_code") or row.get("date_of_joining") is None:
                continue

            records.append(AdditionRecord(
                employee_code=str(row.get("employee_code", "")),
                first_name=str(row.get("first_name", "")),
                last_name=str(row.get("last_name", "")),
                date_of_birth=row.get("date_of_birth"),
                gender=row.get("gender"),
                relationship=str(row.get("relationship", "Self")),
                sum_insured=float(row.get("sum_insured", 0)),
                date_of_joining=row.get("date_of_joining")
            ))

        return InsuranceUpdateReport(total_records=len(df), additions=records, deletions=[], status="success")

    except Exception as e:
        raise ValueError(f"Processing Error: {str(e)}")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


def process_deletions(file_path: str) -> InsuranceUpdateReport:
    try:
        if file_path.endswith('.csv'):
            df = pl.read_csv(file_path, ignore_errors=True, null_values=["NA", "null", "-", ""])
        else:
            df = pl.read_excel(file_path)

        df.columns = [col.lower().strip().replace(" ", "_").replace(".", "") for col in df.columns]

        if "date_of_leaving" not in df.columns:
            raise ValueError("Invalid File: Missing 'date_of_leaving' column.")

        # Parse leaving date
        df = df.with_columns(pl.col("date_of_leaving").cast(pl.Date, strict=False))

        records = [DeletionRecord(**row) for row in df.to_dicts() if row.get("employee_code")]
        return InsuranceUpdateReport(total_records=len(df), additions=[], deletions=records, status="success")

    except Exception as e:
        raise ValueError(f"Processing Error: {str(e)}")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)