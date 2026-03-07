import json
import logging
import os
from datetime import date as _date
from types import SimpleNamespace
from typing import List, Optional

from celery import group as celery_group
from celery.exceptions import Retry
from requests.exceptions import RequestException
from sqlalchemy import insert as sa_insert
from sqlalchemy.exc import OperationalError as DBOperationalError

from app.core.celery_app import celery_app
from app.core.database import SessionLocal
from app.core.cache import cache_get, cache_set, CORPORATE_TTL
from app.core.outbound.factory import get_insurer_adapter
from app.models.models import (
    SyncLog, Corporate, Employee,
    SyncStatus, PolicyStatus, DeliveryChannel, SyncLogEvent, SyncSource,
)
from app.services.outbound_service import OutboundTransformer
from app.services.insurer_connector import InsurerConnector, INSURER_API_KEY

logger = logging.getLogger(__name__)

CHUNK_SIZE = 100  # records per Celery worker; tune to worker memory / insurer rate limits
BASE_PATH = os.getenv("BASE_OUTBOUND_PATH", "/app/outbound_files")

# Terminal statuses: logs in these states must not be reprocessed.
_TERMINAL_STATUSES = {
    SyncStatus.ACTIVE,
    SyncStatus.COMPLETED_OFFLINE,
    SyncStatus.COMPLETED_BOTH,
    SyncStatus.PENDING_OFFLINE,
    SyncStatus.PENDING_BOTH,
    SyncStatus.SOFT_REJECTED,
    SyncStatus.BROKER_REVIEW_PENDING,  # Report generated — broker to send; do not reprocess
}


# ---------------------------------------------------------------------------
# Corporate cache helpers  (sync Redis — DB 1)
# ---------------------------------------------------------------------------

def _corporate_to_dict(c: Corporate) -> dict:
    return {
        "id": c.id,
        "name": c.name,
        "broker_id": c.broker_id,
        "webhook_url": c.webhook_url,
        "insurer_format": getattr(c, "insurer_format", "json"),
        "delivery_channel": c.delivery_channel.value if c.delivery_channel else None,
        "base_folder": getattr(c, "base_folder", ""),
        "insurer_provider": getattr(c, "insurer_provider", "standard"),
        "hrms_provider": getattr(c, "hrms_provider", "standard"),
    }


def _dict_to_corporate(d: dict) -> SimpleNamespace:
    corp = SimpleNamespace(**d)
    if d.get("delivery_channel"):
        corp.delivery_channel = DeliveryChannel(d["delivery_channel"])
    return corp


def _get_corporate(db, corporate_id: str) -> Optional[SimpleNamespace]:
    """
    Fetch corporate by id. Checks Redis first; falls back to DB on miss.
    Writes through to cache on DB hit.
    """
    cache_key = f"ins:corp:{corporate_id}"
    cached = cache_get(cache_key)
    if cached:
        return _dict_to_corporate(cached)

    corporate = db.query(Corporate).filter(Corporate.id == corporate_id).first()
    if corporate:
        cache_set(cache_key, _corporate_to_dict(corporate), CORPORATE_TTL)
    return corporate


# ---------------------------------------------------------------------------
# Shared audit helper  (defined first — used by both helpers below)
# ---------------------------------------------------------------------------

def record_audit_event_sync(
    db, log_id: int, status: SyncStatus, actor: str,
    details: dict = None, policy_status: str = None
):
    db.add(SyncLogEvent(
        sync_log_id=log_id,
        event_status=status,
        actor=actor,
        details=details,
        policy_status=policy_status,
    ))


# ---------------------------------------------------------------------------
# _process_single_log
# Pure helper: processes one SyncLog row.  No DB commit — caller commits.
# Designed to run inside process_batch_chunk (fan-out) and is fully isolated:
# exceptions are caught per-record so one bad row never rolls back the chunk.
# ---------------------------------------------------------------------------

def _process_single_log(db, log: SyncLog, employee: Optional[Employee] = None) -> None:
    # -- Idempotency guard --------------------------------------------------
    # A chunk retry must not re-send records that already reached a terminal
    # state in a previous attempt.
    if log.sync_status in _TERMINAL_STATUSES:
        logger.info(f"Log {log.id} already terminal ({log.sync_status}). Skipping.")
        return

    corporate = _get_corporate(db, log.corporate_id)
    if not corporate:
        logger.warning(f"Log {log.id}: corporate {log.corporate_id} not found. Skipping.")
        return

    # employee is pre-fetched by process_batch_chunk (bulk IN query).
    # For the real-time path (process_sync_event), it is fetched below as before.

    pre_ps = None
    if employee and employee.policy_status:
        ps = employee.policy_status
        pre_ps = ps.value if hasattr(ps, "value") else str(ps)

    log.sync_status = SyncStatus.PROVISIONING
    record_audit_event_sync(db, log.id, SyncStatus.PROVISIONING, "CELERY_CHUNK_WORKER",
                            policy_status=pre_ps)

    is_deletion = log.transaction_type in ("DELETION", "BATCH_DELETION")
    is_addition = not is_deletion  # ADDITION, UPDATE, BATCH_ADDITION all use transform_addition

    # -- OFFLINE channel: validated and queued; broker generates dispatch file later ------
    if corporate.delivery_channel == DeliveryChannel.OFFLINE:
        # Both additions and deletions stay PENDING_DISPATCH until broker confirms sent.
        # LAPSED / PENDING_ISSUANCE only set by confirm-dispatch after insurer receives the file.
        new_policy_status = PolicyStatus.PENDING_DISPATCH
        ps_value = new_policy_status.value
        # Reset from PROVISIONING back to PENDING_OFFLINE so the delivery endpoint can pick it up.
        log.sync_status = SyncStatus.PENDING_OFFLINE
        log.policy_status = ps_value
        if employee:
            employee.delivery_status = SyncStatus.PENDING_OFFLINE
            employee.policy_status = new_policy_status
        record_audit_event_sync(
            db, log.id, SyncStatus.PENDING_OFFLINE, "CELERY_CHUNK_WORKER",
            {"note": "Validated and queued for offline batch dispatch by broker"},
            policy_status=ps_value,
        )
        return

    # -- WEBHOOK or BOTH channel: call the insurer --------------------------
    try:
        adapter = get_insurer_adapter(getattr(corporate, "insurer_provider", "standard"))
        final_data = (
            adapter.transform_addition(log.payload)
            if is_addition
            else adapter.transform_deletion(log.payload)
        )
        if isinstance(final_data, dict):
            final_data = json.dumps(final_data)

        headers = adapter.get_headers(api_key=INSURER_API_KEY)
        headers["Idempotency-Key"] = log.transaction_id or str(log.id)

        response_data = InsurerConnector.push_to_insurer_sync(
            data=final_data,
            target_url=corporate.webhook_url,
            headers=headers,
        )
        log.raw_response = response_data

        new_policy_status = PolicyStatus.LAPSED if is_deletion else PolicyStatus.PENDING_ISSUANCE
        ps_value = new_policy_status.value

        if corporate.delivery_channel == DeliveryChannel.BOTH:
            log.sync_status = SyncStatus.PENDING_BOTH
            log.policy_status = ps_value
            if employee:
                employee.delivery_status = SyncStatus.PENDING_BOTH
                employee.policy_status = new_policy_status
            record_audit_event_sync(
                db, log.id, SyncStatus.PENDING_BOTH, "CELERY_CHUNK_WORKER",
                {"insurer_response": response_data}, policy_status=ps_value,
            )
        else:
            log.sync_status = SyncStatus.ACTIVE
            log.policy_status = ps_value
            if employee:
                employee.delivery_status = SyncStatus.ACTIVE
                employee.policy_status = new_policy_status
            record_audit_event_sync(
                db, log.id, SyncStatus.ACTIVE, "CELERY_CHUNK_WORKER",
                {"insurer_response": response_data}, policy_status=ps_value,
            )

    except Exception as exc:
        # Per-record isolation: mark this log as FAILED but let the chunk
        # continue processing the remaining records.
        log.sync_status = SyncStatus.FAILED
        log.error_message = str(exc)
        record_audit_event_sync(
            db, log.id, SyncStatus.FAILED, "CELERY_CHUNK_WORKER",
            {"error": str(exc)},
        )
        logger.error(f"Log {log.id} failed in chunk: {exc}")


# ---------------------------------------------------------------------------
# process_batch_chunk  (Celery task)
# Receives a list of SyncLog IDs, processes each via _process_single_log,
# and commits the whole chunk in one transaction.
# ---------------------------------------------------------------------------

_RETRIABLE = (RequestException, ConnectionError, TimeoutError, DBOperationalError)


@celery_app.task(
    bind=True,
    max_retries=3,
    autoretry_for=_RETRIABLE,
    retry_backoff=True,
    retry_jitter=True,
)
def process_batch_chunk(self, log_ids: List[int]):
    """
    Fan-out worker.  Receives a chunk of log IDs dispatched by a Celery Group
    from the ingestion endpoint.

    Two key hardening changes vs the naive implementation:
    1. Bulk employee pre-fetch — one IN query for the whole chunk instead of
       100 individual SELECTs (eliminates the N+1 query problem).
    2. Per-record commit with per-record isolation — each log is committed
       before the next HTTP call fires, so a DB failure on record N cannot
       cause records 1..N-1 to be reprocessed on retry (phantom HTTP calls).
    """
    db = SessionLocal()
    try:
        logs = db.query(SyncLog).filter(SyncLog.id.in_(log_ids)).all()

        # ── Bulk pre-fetch employees (1 IN query instead of N SELECTs) ────────
        emp_keys = [
            (log.corporate_id, log.payload.get("employee_code"))
            for log in logs
            if log.payload.get("employee_code")
        ]
        employees_map: dict[tuple, Employee] = {}
        if emp_keys:
            corp_ids  = list({k[0] for k in emp_keys})
            emp_codes = list({k[1] for k in emp_keys})
            fetched = db.query(Employee).filter(
                Employee.corporate_id.in_(corp_ids),
                Employee.employee_code.in_(emp_codes),
            ).all()
            employees_map = {(e.corporate_id, e.employee_code): e for e in fetched}

        # ── Per-record commit + isolation ─────────────────────────────────────
        processed = 0
        for log in logs:
            try:
                emp_code = log.payload.get("employee_code")
                employee = employees_map.get((log.corporate_id, emp_code))
                _process_single_log(db, log, employee=employee)
                db.commit()
                processed += 1
            except Exception as exc:
                db.rollback()
                logger.error(f"Log {log.id} failed in chunk, rolled back: {exc}")

        logger.info(f"Chunk complete: {processed}/{len(logs)} logs processed.")
        return f"Chunk processed: {processed}/{len(logs)} records"
    finally:
        db.close()


# ---------------------------------------------------------------------------
# process_sync_event  (Celery task)
# Original single-record task — still used by real-time stream endpoints.
# ---------------------------------------------------------------------------

@celery_app.task(
    bind=True,
    max_retries=5,
    autoretry_for=_RETRIABLE,
    retry_backoff=True,
    retry_jitter=True,
)
def process_sync_event(self, log_id: int):
    """
    Real-time worker.  Processes a single SyncLog, used by stream.py.
    Batch uploads use process_batch_chunk (fan-out) instead.
    """
    db = SessionLocal()
    log = None
    try:
        log = db.query(SyncLog).filter(SyncLog.id == log_id).first()
        if not log:
            return f"Log {log_id} not found"

        # Outbound idempotency: skip if already reached a terminal state.
        if log.sync_status in _TERMINAL_STATUSES:
            logger.info(f"Log {log_id} already processed ({log.sync_status}). Skipping.")
            return f"Already processed: {log.sync_status}"

        corporate = _get_corporate(db, log.corporate_id)

        # Fetch employee BEFORE PROVISIONING to snapshot existing policy_status.
        emp_code = log.payload.get("employee_code")
        employee = db.query(Employee).filter(
            Employee.corporate_id == log.corporate_id,
            Employee.employee_code == emp_code,
        ).first()

        pre_ps = None
        if employee and employee.policy_status:
            ps = employee.policy_status
            pre_ps = ps.value if hasattr(ps, "value") else str(ps)

        log.sync_status = SyncStatus.PROVISIONING
        record_audit_event_sync(db, log.id, SyncStatus.PROVISIONING, "CELERY_WORKER",
                                policy_status=pre_ps)
        db.commit()

        is_deletion = log.transaction_type in ("DELETION", "BATCH_DELETION")
        is_addition = not is_deletion  # ADDITION, UPDATE, BATCH_ADDITION all use transform_addition

        new_policy_status = PolicyStatus.LAPSED if is_deletion else PolicyStatus.PENDING_ISSUANCE
        ps_value = new_policy_status.value

        # -- Route by delivery channel --------------------------------------
        if corporate.delivery_channel in (DeliveryChannel.WEBHOOK, DeliveryChannel.BOTH):
            try:
                adapter = get_insurer_adapter(
                    getattr(corporate, "insurer_provider", "standard")
                )
                final_data = (
                    adapter.transform_addition(log.payload)
                    if is_addition
                    else adapter.transform_deletion(log.payload)
                )
                if isinstance(final_data, dict):
                    final_data = json.dumps(final_data)

                headers = adapter.get_headers(api_key=INSURER_API_KEY)
                headers["Idempotency-Key"] = log.transaction_id or str(log.id)

                response_data = InsurerConnector.push_to_insurer_sync(
                    data=final_data,
                    target_url=corporate.webhook_url,
                    headers=headers,
                )
                log.raw_response = response_data

                if corporate.delivery_channel == DeliveryChannel.BOTH:
                    log.sync_status = SyncStatus.PENDING_BOTH
                    log.policy_status = ps_value
                    if employee:
                        employee.delivery_status = SyncStatus.PENDING_BOTH
                        employee.policy_status = new_policy_status
                    record_audit_event_sync(
                        db, log.id, SyncStatus.PENDING_BOTH, "CELERY_WORKER",
                        {"insurer_response": response_data}, policy_status=ps_value,
                    )
                else:
                    log.sync_status = SyncStatus.ACTIVE
                    log.policy_status = ps_value
                    if employee:
                        employee.delivery_status = SyncStatus.ACTIVE
                        employee.policy_status = new_policy_status
                    record_audit_event_sync(
                        db, log.id, SyncStatus.ACTIVE, "CELERY_WORKER",
                        {"insurer_response": response_data}, policy_status=ps_value,
                    )

            except Exception as exc:
                log.retry_count += 1
                log.error_message = str(exc)
                record_audit_event_sync(
                    db, log.id, SyncStatus.FAILED, "CELERY_WORKER",
                    {"error": str(exc), "attempt": log.retry_count},
                )
                db.commit()
                raise self.retry(exc=exc)

        elif corporate.delivery_channel == DeliveryChannel.OFFLINE:
            # Real-time OFFLINE: park for the delivery sweeper.
            # (Batch OFFLINE records never reach this task — they use process_batch_chunk.)
            # Both additions and deletions stay PENDING_DISPATCH until broker confirms sent.
            new_offline_ps = PolicyStatus.PENDING_DISPATCH
            offline_ps = new_offline_ps.value
            log.sync_status = SyncStatus.PENDING_OFFLINE
            log.policy_status = offline_ps
            if employee:
                employee.delivery_status = SyncStatus.PENDING_OFFLINE
                employee.policy_status = new_offline_ps
            record_audit_event_sync(
                db, log.id, SyncStatus.PENDING_OFFLINE, "CELERY_WORKER",
                {"note": "Parked for sweeper"}, policy_status=offline_ps,
            )

        db.commit()
        return f"Processed log {log_id} successfully"

    except Retry:
        # Celery retry signal — let it propagate cleanly without marking FAILED.
        db.rollback()
        raise
    except Exception as exc:
        db.rollback()
        logger.error(f"Task crashed for log {log_id}: {exc}")
        if log:
            try:
                log.sync_status = SyncStatus.FAILED
                log.error_message = str(exc)
                record_audit_event_sync(
                    db, log.id, SyncStatus.FAILED, "CELERY_WORKER_CRASH", {"error": str(exc)}
                )
                db.commit()
            except Exception:
                db.rollback()
                logger.error(f"Failed to persist FAILED state for log {log_id}")
        raise
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Helpers for process_master_batch
# ---------------------------------------------------------------------------

def _parse_date(v):
    """Convert ISO date string from Celery-serialised task args back to date object."""
    if v is None or v == "":
        return None
    if isinstance(v, _date):
        return v
    try:
        return _date.fromisoformat(str(v))
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# process_master_batch  (Celery task)
# Entry point for 202 Accepted batch uploads.  Called directly by the API
# endpoint immediately after file parsing; the API returns to the client
# while this task handles all DB writes and worker fan-out in the background.
#
# Flow:
#   1. Bulk sync inserts — Employee upsert + SyncLog + SyncLogEvent (4 queries)
#   2. Generate Excel outbound file for OFFLINE / BOTH channels
#   3. Fan-out process_batch_chunk group
# ---------------------------------------------------------------------------

@celery_app.task(
    bind=True,
    max_retries=3,
    autoretry_for=_RETRIABLE,
    retry_backoff=True,
    retry_jitter=True,
)
def process_master_batch(
    self,
    corporate_id: str,
    rows: list,
    event_type: str,
    delivery_channel: str,
    insurer_provider: str,
    insurer_format: str,
    base_folder: str,
    is_deletion: bool = False,
):
    """
    Master batch task dispatched by the 202 ingestion endpoints.
    Rows arrive as JSON-safe dicts (dates as ISO strings, Decimals as strings)
    because Celery serialises task args via JSON.
    """
    from datetime import datetime

    db = SessionLocal()
    try:
        emp_codes = [r["employee_code"] for r in rows]
        now = datetime.utcnow()

        # ── Round-trip 1: Bulk SELECT existing employees ──────────────────────
        existing = db.query(Employee).filter(
            Employee.corporate_id == corporate_id,
            Employee.employee_code.in_(emp_codes),
        ).all()
        emp_map = {e.employee_code: e for e in existing}

        # ── Python-side: update existing / create new — zero extra DB hits ────
        policy_status_map: dict = {}
        for row in rows:
            code = row["employee_code"]
            emp = emp_map.get(code)
            if emp:
                ps = emp.policy_status
                policy_status_map[code] = ps.value if hasattr(ps, "value") else str(ps) if ps else None
                if is_deletion:
                    emp.status = "inactive"
                    emp.date_of_leaving = _parse_date(row.get("date_of_leaving"))
                    emp.resignation_reason = row.get("reason")
                else:
                    emp.status = "active"
                    emp.date_of_leaving = None
                    emp.resignation_reason = None
                    emp.first_name = row.get("first_name", emp.first_name)
                    emp.last_name = row.get("last_name", emp.last_name)
                    emp.email = row.get("email", emp.email)
                    emp.gender = row.get("gender", emp.gender)
                    emp.date_of_birth = _parse_date(row.get("date_of_birth")) or emp.date_of_birth
                    emp.date_of_joining = _parse_date(row.get("date_of_joining")) or emp.date_of_joining
                    emp.sum_insured = row.get("sum_insured", emp.sum_insured)
            else:
                policy_status_map[code] = None
                db.add(Employee(
                    corporate_id=corporate_id,
                    employee_code=code,
                    first_name=row.get("first_name") or "Unknown",
                    last_name=row.get("last_name") or "Unknown",
                    email=row.get("email"),
                    gender=row.get("gender"),
                    date_of_birth=_parse_date(row.get("date_of_birth")),
                    status="inactive" if is_deletion else "active",
                    delivery_status=SyncStatus.PENDING,
                    policy_status=None,
                    date_of_joining=_parse_date(row.get("date_of_joining")) if not is_deletion else None,
                    date_of_leaving=_parse_date(row.get("date_of_leaving")) if is_deletion else None,
                    resignation_reason=row.get("reason") if is_deletion else None,
                    sum_insured=row.get("sum_insured", 0),
                ))

        # ── Round-trip 2: Flush all Employee inserts/updates ──────────────────
        db.flush()

        # ── Round-trip 3: Bulk INSERT SyncLogs, return IDs in one query ───────
        log_rows = [
            {
                "corporate_id": corporate_id,
                "transaction_id": row.get("transaction_id"),
                "transaction_type": event_type,
                "payload": json.loads(json.dumps(row, default=str)),
                "source": SyncSource.BATCH,
                "status": "success",
                "sync_status": SyncStatus.PENDING,
                "timestamp": now,
            }
            for row in rows
        ]
        insert_result = db.execute(
            sa_insert(SyncLog).values(log_rows).returning(SyncLog.id)
        )
        log_ids = [r[0] for r in insert_result]

        # ── Round-trip 4: Bulk INSERT SyncLogEvents ───────────────────────────
        event_rows = [
            {
                "sync_log_id": log_id,
                "event_status": SyncStatus.PENDING,
                "actor": "SYSTEM_INGESTION",
                "details": {"source": SyncSource.BATCH.value},
                "policy_status": policy_status_map.get(row["employee_code"]),
            }
            for log_id, row in zip(log_ids, rows)
        ]
        db.execute(sa_insert(SyncLogEvent).values(event_rows))
        db.commit()

        # ── Generate Excel for OFFLINE / BOTH channels ────────────────────────
        channel = DeliveryChannel(delivery_channel)
        if channel in (DeliveryChannel.OFFLINE, DeliveryChannel.BOTH):
            full_directory = os.path.join(BASE_PATH, base_folder)
            filename_prefix = "removal_report" if is_deletion else "addition_report"
            try:
                OutboundTransformer.to_file(
                    data=rows,
                    filename_prefix=filename_prefix,
                    output_dir=full_directory,
                    format_type=insurer_format,
                    insurer_adapter=get_insurer_adapter(insurer_provider),
                    is_deletion=is_deletion,
                )
                logger.info(f"Excel generated for corporate {corporate_id} ({filename_prefix})")
            except Exception as exc:
                # Non-fatal: records are safely in DB; broker can re-generate via delivery endpoint.
                logger.error(f"Excel generation failed for {corporate_id}: {exc}")

        # ── Fan-out process_batch_chunk group ─────────────────────────────────
        chunks = [log_ids[i:i + CHUNK_SIZE] for i in range(0, len(log_ids), CHUNK_SIZE)]
        celery_group(process_batch_chunk.s(chunk) for chunk in chunks).apply_async()

        logger.info(
            f"Master batch complete: {len(log_ids)} records, {len(chunks)} chunk(s) dispatched "
            f"for corporate {corporate_id}"
        )
        return {"corporate_id": corporate_id, "log_count": len(log_ids), "chunks": len(chunks)}

    except Exception as exc:
        db.rollback()
        logger.error(f"process_master_batch failed for {corporate_id}: {exc}")
        raise
    finally:
        db.close()
