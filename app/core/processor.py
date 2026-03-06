"""
processor.py
============
Parses batch upload files (CSV / Excel) into validated InsuranceUpdateReport objects.

Processing pipeline
-------------------
1. Read file — strict mode; unreadable files surface as FileParseError (→ HTTP 400).
2. Normalise headers — camelCase, hyphen, space, dot → snake_case.
3. Apply HRMS adapter column map at the DataFrame level (rename in bulk).
4. Handle full-name split sentinel columns (insured_name / employee_name → first/last).
5. Safety-check required columns — raises WrongFileError or MissingColumnsError.
6. Cast canonical date columns with strict=False (bad dates → null → per-row error).
7. Iterate rows: validate individually; collect AdditionRecord/DeletionRecord or RejectedRow.
8. Return InsuranceUpdateReport with status = success | partial | failed.
"""

import re
import os
from datetime import datetime
from decimal import Decimal

import polars as pl
from pydantic import ValidationError

from app.core.adapters.base import _SPLIT_NAME_SENTINEL
from app.core.adapters.factory import get_hrms_adapter
from app.models.schemas import (
    AdditionRecord,
    DeletionRecord,
    InsuranceUpdateReport,
    RejectedRow,
)


# ── Typed exception hierarchy ─────────────────────────────────────────────────
# ingestion.py catches each type and maps it to the correct HTTP status code.

class FileParseError(ValueError):
    """File is unreadable or structurally corrupt → HTTP 400."""

class WrongFileError(ValueError):
    """Additions file sent to deletions endpoint (or vice versa) → HTTP 400."""

class MissingColumnsError(ValueError):
    """Required column absent after all alias resolution → HTTP 422."""


# ── Internal helpers ──────────────────────────────────────────────────────────

def _normalize_header(col: str) -> str:
    """
    Convert any column header to consistent snake_case:

      'DateOfJoining'   → 'date_of_joining'   (camelCase)
      'Date-Of-Joining' → 'date_of_joining'   (hyphen-separated)
      'Date Of Joining' → 'date_of_joining'   (space-separated)
      'date.of.joining' → 'date_of_joining'   (dot-separated)
      'DOJ'             → 'doj'               (all-caps acronym; left for adapter map)
    """
    col = col.strip()
    col = re.sub(r'([a-z])([A-Z])', r'\1_\2', col)   # camelCase before lowercasing
    col = col.lower()
    col = re.sub(r'[-\s.]+', '_', col)               # hyphens / spaces / dots → _
    return col.strip('_')


def _apply_column_map(df: pl.DataFrame, col_map: dict) -> pl.DataFrame:
    """
    Rename DataFrame columns using the adapter's column map.
    Sentinel-mapped columns are left untouched (handled by _apply_name_split).
    First-wins rule: if two source columns map to the same target, only the
    first one encountered is renamed; the other keeps its original name.
    If the target already exists in the DataFrame, the rename is also skipped.
    """
    existing = set(df.columns)
    rename: dict[str, str] = {}
    for col in df.columns:
        target = col_map.get(col)
        if target and target != _SPLIT_NAME_SENTINEL:
            if target not in existing and target not in rename.values():
                rename[col] = target
    return df.rename(rename) if rename else df


def _flexible_parse_date(val) -> str | None:
    """
    Try common date formats in priority order, return ISO YYYY-MM-DD string or None.
    Priority: ISO > DD-MM-YYYY > DD/MM/YYYY > ISO-datetime > MM-DD-YYYY > DD.MM.YYYY.
    Applied to String-type date columns so non-ISO values aren't silently nulled
    by Polars' strict ISO-only cast.
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    _FORMATS = [
        "%Y-%m-%d",          # 2024-12-31  (ISO — try first)
        "%d-%m-%Y",          # 31-12-2024
        "%d/%m/%Y",          # 31/12/2024  (most common in India)
        "%Y-%m-%dT%H:%M:%SZ",# 2024-12-31T00:00:00Z
        "%m-%d-%Y",          # 12-31-2024  (US)
        "%d.%m.%Y",          # 31.12.2024
    ]
    for fmt in _FORMATS:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _parse_date_col(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """
    Apply flexible date parsing to a single column, then cast to pl.Date.
    If the column is already a Date/Datetime type, skip the string-parsing step.
    """
    if col not in df.columns:
        return df
    if df[col].dtype in (pl.Date, pl.Datetime):
        return df
    # Column is String / Utf8 — apply multi-format parser then cast
    df = df.with_columns(
        pl.col(col).map_elements(_flexible_parse_date, return_dtype=pl.Utf8)
    )
    return df.with_columns(pl.col(col).cast(pl.Date, strict=False))


def _apply_name_split(df: pl.DataFrame, col_map: dict) -> pl.DataFrame:
    """
    Handle columns whose map value is _SPLIT_NAME_SENTINEL.
    Splits 'John Doe' → first_name='John', last_name='Doe'.
    Only populates first_name/last_name when they are not already present
    (a more-specific column takes precedence).
    Drops the sentinel source column after splitting.
    """
    sentinel_sources = [
        k for k, v in col_map.items()
        if v == _SPLIT_NAME_SENTINEL and k in df.columns
    ]
    for src in sentinel_sources:
        if "first_name" not in df.columns:
            df = df.with_columns([
                pl.col(src).cast(pl.Utf8).str.splitn(" ", 2)
                  .struct.field("field_0").alias("first_name"),
                pl.col(src).cast(pl.Utf8).str.splitn(" ", 2)
                  .struct.field("field_1").fill_null("").alias("last_name"),
            ])
        df = df.drop(src)
    return df


# ── Public API ────────────────────────────────────────────────────────────────

def process_additions(file_path: str, hrms_provider: str = "standard") -> InsuranceUpdateReport:
    try:
        # ── 1. Read file ──────────────────────────────────────────────────────
        try:
            if file_path.endswith(".csv"):
                df = pl.read_csv(
                    file_path,
                    null_values=["NA", "N/A", "null", "NULL", "none", "None", "-", ""],
                )
            else:
                df = pl.read_excel(file_path)
        except Exception as exc:
            raise FileParseError(f"Cannot read file: {exc}") from exc

        # ── 2. Normalise headers ──────────────────────────────────────────────
        df = df.rename({col: _normalize_header(col) for col in df.columns})

        # ── 3. Apply HRMS adapter column map ─────────────────────────────────
        adapter = get_hrms_adapter(hrms_provider)
        df = _apply_column_map(df, adapter.ADDITION_COLUMN_MAP)

        # ── 4. Name-split sentinel columns ───────────────────────────────────
        df = _apply_name_split(df, adapter.ADDITION_COLUMN_MAP)

        # Fallback for insured_name not covered by the current adapter's map
        if "insured_name" in df.columns and "first_name" not in df.columns:
            df = df.with_columns([
                pl.col("insured_name").cast(pl.Utf8).str.splitn(" ", 2)
                  .struct.field("field_0").alias("first_name"),
                pl.col("insured_name").cast(pl.Utf8).str.splitn(" ", 2)
                  .struct.field("field_1").fill_null("").alias("last_name"),
            ])

        # ── 5. Required-column safety check ──────────────────────────────────
        if "employee_code" not in df.columns:
            raise MissingColumnsError(
                "Missing required column 'employee_code'. "
                "Expected one of: employee_code, emp_id, employee_id, staff_id, empno, …"
            )
        if "date_of_joining" not in df.columns:
            if "date_of_leaving" in df.columns:
                raise WrongFileError(
                    "Wrong file: a deletions file was uploaded to the additions endpoint."
                )
            raise MissingColumnsError(
                "Missing required column 'date_of_joining'. "
                "Expected one of: date_of_joining, doj, joining_date, start_date, …"
            )

        # ── 6. Parse canonical date columns ───────────────────────────────────
        # _parse_date_col handles both String (multi-format) and already-Date types.
        for date_col in ("date_of_joining", "date_of_birth"):
            df = _parse_date_col(df, date_col)

        # ── 7. Per-row validation ─────────────────────────────────────────────
        records: list[AdditionRecord] = []
        rejected: list[RejectedRow]  = []

        for idx, row in enumerate(df.to_dicts()):
            row_errors: list[str] = []

            # Explicit pre-checks — cleaner messages than raw Pydantic errors
            if not row.get("employee_code"):
                row_errors.append("employee_code is missing or blank")
            if row.get("date_of_joining") is None:
                row_errors.append("date_of_joining is missing or could not be parsed")
            if not row.get("first_name"):
                row_errors.append("first_name is missing or blank")

            if row_errors:
                rejected.append(RejectedRow(
                    row_index=idx + 1,
                    raw_data={k: str(v) for k, v in row.items()},
                    errors=row_errors,
                ))
                continue

            try:
                records.append(AdditionRecord(
                    employee_code   = str(row["employee_code"]),
                    first_name      = str(row.get("first_name", "")),
                    last_name       = str(row.get("last_name") or ""),
                    date_of_birth   = row.get("date_of_birth"),
                    gender          = row.get("gender"),
                    relationship    = row.get("relationship", "Self"),
                    sum_insured     = row.get("sum_insured"),
                    date_of_joining = row.get("date_of_joining"),
                ))
            except ValidationError as exc:
                # Extract field-level messages from Pydantic v2
                errors = [
                    f"{e['loc'][0]}: {e['msg']}" if e.get("loc") else e["msg"]
                    for e in exc.errors()
                ]
                rejected.append(RejectedRow(
                    row_index=idx + 1,
                    raw_data={k: str(v) for k, v in row.items()},
                    errors=errors,
                ))

        # ── 8. Build and return report ────────────────────────────────────────
        if not records:
            status = "failed"
        elif rejected:
            status = "partial"
        else:
            status = "success"

        return InsuranceUpdateReport(
            total_rows_in_file = len(df),
            accepted_count     = len(records),
            rejected_count     = len(rejected),
            additions          = records,
            deletions          = [],
            rejected_rows      = rejected,
            status             = status,
        )

    except (FileParseError, WrongFileError, MissingColumnsError):
        raise   # let ingestion.py handle with the correct HTTP status code
    except Exception as exc:
        raise FileParseError(f"Unexpected processing error: {exc}") from exc
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


def process_deletions(file_path: str, hrms_provider: str = "standard") -> InsuranceUpdateReport:
    try:
        # ── 1. Read file ──────────────────────────────────────────────────────
        try:
            if file_path.endswith(".csv"):
                df = pl.read_csv(
                    file_path,
                    null_values=["NA", "N/A", "null", "NULL", "none", "None", "-", ""],
                )
            else:
                df = pl.read_excel(file_path)
        except Exception as exc:
            raise FileParseError(f"Cannot read file: {exc}") from exc

        # ── 2. Normalise headers ──────────────────────────────────────────────
        df = df.rename({col: _normalize_header(col) for col in df.columns})

        # ── 3. Apply HRMS adapter deletion column map ─────────────────────────
        adapter = get_hrms_adapter(hrms_provider)
        df = _apply_column_map(df, adapter.DELETION_COLUMN_MAP)

        # ── 4. Required-column safety check ──────────────────────────────────
        if "employee_code" not in df.columns:
            raise MissingColumnsError(
                "Missing required column 'employee_code'. "
                "Expected one of: employee_code, emp_id, employee_id, staff_id, empno, …"
            )
        if "date_of_leaving" not in df.columns:
            if "date_of_joining" in df.columns:
                raise WrongFileError(
                    "Wrong file: an additions file was uploaded to the deletions endpoint."
                )
            raise MissingColumnsError(
                "Missing required column 'date_of_leaving'. "
                "Expected one of: date_of_leaving, exit_date, last_working_day, dol, …"
            )

        # ── 5. Parse date column ──────────────────────────────────────────────
        df = _parse_date_col(df, "date_of_leaving")

        # ── 6. Per-row validation ─────────────────────────────────────────────
        records: list[DeletionRecord] = []
        rejected: list[RejectedRow]  = []

        for idx, row in enumerate(df.to_dicts()):
            row_errors: list[str] = []

            if not row.get("employee_code"):
                row_errors.append("employee_code is missing or blank")
            if row.get("date_of_leaving") is None:
                row_errors.append("date_of_leaving is missing or could not be parsed")

            if row_errors:
                rejected.append(RejectedRow(
                    row_index=idx + 1,
                    raw_data={k: str(v) for k, v in row.items()},
                    errors=row_errors,
                ))
                continue

            try:
                records.append(DeletionRecord(
                    employee_code   = str(row["employee_code"]),
                    member_id       = row.get("member_id"),
                    date_of_leaving = row["date_of_leaving"],
                ))
            except ValidationError as exc:
                errors = [
                    f"{e['loc'][0]}: {e['msg']}" if e.get("loc") else e["msg"]
                    for e in exc.errors()
                ]
                rejected.append(RejectedRow(
                    row_index=idx + 1,
                    raw_data={k: str(v) for k, v in row.items()},
                    errors=errors,
                ))

        # ── 7. Build and return report ────────────────────────────────────────
        if not records:
            status = "failed"
        elif rejected:
            status = "partial"
        else:
            status = "success"

        return InsuranceUpdateReport(
            total_rows_in_file = len(df),
            accepted_count     = len(records),
            rejected_count     = len(rejected),
            additions          = [],
            deletions          = records,
            rejected_rows      = rejected,
            status             = status,
        )

    except (FileParseError, WrongFileError, MissingColumnsError):
        raise
    except Exception as exc:
        raise FileParseError(f"Unexpected processing error: {exc}") from exc
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)
