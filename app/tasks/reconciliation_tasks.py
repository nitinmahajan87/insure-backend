"""
Reconciliation Sweeper — Celery Beat periodic task.

Handles the case where an insurer returns HTTP 200 (receipt acknowledged) but
never sends the async callback with the final policy decision.  Two hours after
delivery, this task sweeps every un-acknowledged ACTIVE log and either:

  a) Polls the insurer's status API (if the adapter implements check_policy_status)
     and records the result exactly as the callback endpoint would.

  b) Writes a RECONCILIATION_PENDING audit event so the operations team knows
     which records need manual follow-up.

Schedule: every hour (configured in celery_app.py beat_schedule).
"""

import logging
from datetime import datetime, timezone, timedelta, date
from typing import Optional

from app.core.celery_app import celery_app
from app.core.database import SessionLocal
from app.core.outbound.factory import get_insurer_adapter
from app.models.models import (
    SyncLog, Corporate, Employee,
    SyncStatus, PolicyStatus, SyncLogEvent,
)
from app.services.insurer_connector import INSURER_API_KEY
from app.tasks.sync_tasks import record_audit_event_sync

logger = logging.getLogger(__name__)

# Logs must be at least this old before the sweeper touches them.
# Gives insurers time to send their own callback before we start polling.
RECONCILE_AFTER_HOURS: int = 2

# Max logs processed per sweep run — prevents overwhelming the insurer API.
RECONCILE_BATCH_SIZE: int = 200

_APPROVED_STATUSES = {"APPROVED", "ACCEPTED", "ISSUED"}


# ---------------------------------------------------------------------------
# Internal helper — mirrors the logic in insurer_callbacks.py
# ---------------------------------------------------------------------------

def _apply_poll_result(
    db,
    log: SyncLog,
    poll_result: dict,
) -> None:
    """
    Applies a poll result from check_policy_status() to the SyncLog and Employee.
    Mirrors the callback endpoint's update logic so reconciliation is consistent
    with direct insurer callbacks.
    """
    is_approved = poll_result.get("status", "").upper() in _APPROVED_STATUSES
    new_sync_status = SyncStatus.ACTIVE if is_approved else SyncStatus.SOFT_REJECTED
    new_policy_status = PolicyStatus.ISSUED if is_approved else PolicyStatus.SOFT_REJECTED

    policy_date: Optional[date] = None
    raw_date = poll_result.get("policy_effective_date")
    if raw_date:
        try:
            policy_date = date.fromisoformat(raw_date)
        except ValueError:
            logger.warning(f"Log {log.id}: unparseable policy_effective_date '{raw_date}'")

    log.sync_status = new_sync_status
    log.insurer_reference_id = poll_result.get("insurer_reference_id")
    log.callback_received_at = datetime.now(timezone.utc)
    log.rejection_reason = poll_result.get("rejection_reason")
    log.raw_response = poll_result

    emp_code = log.payload.get("employee_code")
    if emp_code:
        employee = db.query(Employee).filter(
            Employee.corporate_id == log.corporate_id,
            Employee.employee_code == emp_code,
        ).first()

        if employee:
            employee.delivery_status = new_sync_status
            employee.policy_status = new_policy_status
            employee.insurer_reference_id = poll_result.get("insurer_reference_id")
            employee.rejection_reason = poll_result.get("rejection_reason")
            if policy_date:
                employee.policy_effective_date = policy_date
            if is_approved and poll_result.get("policy_number"):
                employee.policy_number = poll_result["policy_number"]

    record_audit_event_sync(
        db, log.id, new_sync_status, "RECONCILIATION_POLLER",
        {"polled_result": poll_result},
    )
    logger.info(f"Log {log.id}: reconciled via polling → {new_sync_status}")


def _reconcile_single_log(db, log: SyncLog) -> None:
    """
    Attempts to resolve one un-acknowledged log.
    Exceptions are caught so one failing log cannot abort the whole sweep.
    """
    try:
        corporate = db.query(Corporate).filter(
            Corporate.id == log.corporate_id
        ).first()

        if not corporate:
            logger.warning(f"Log {log.id}: corporate {log.corporate_id} missing — skipping.")
            return

        adapter = get_insurer_adapter(
            getattr(corporate, "insurer_provider", "standard")
        )
        transaction_id = log.transaction_id or str(log.id)

        poll_result = adapter.check_policy_status(
            transaction_id=transaction_id,
            api_key=INSURER_API_KEY,
        )

        if poll_result is not None:
            _apply_poll_result(db, log, poll_result)
        else:
            # Insurer doesn't support polling — mark for manual review.
            record_audit_event_sync(
                db, log.id, SyncStatus.ACTIVE, "RECONCILIATION_SWEEPER",
                {
                    "note": (
                        "Awaiting insurer callback. "
                        f"Provider '{getattr(corporate, 'insurer_provider', 'standard')}' "
                        "does not support status polling."
                    )
                },
            )

    except Exception as exc:
        logger.error(f"Log {log.id}: reconciliation failed — {exc}")


# ---------------------------------------------------------------------------
# Celery Beat task
# ---------------------------------------------------------------------------

@celery_app.task(name="app.tasks.reconciliation_tasks.reconcile_pending_syncs")
def reconcile_pending_syncs() -> str:
    """
    Hourly sweep.  Finds ACTIVE logs older than RECONCILE_AFTER_HOURS that
    have not yet received an insurer callback and tries to resolve them.
    """
    db = SessionLocal()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=RECONCILE_AFTER_HOURS)

        pending_logs = (
            db.query(SyncLog)
            .filter(
                SyncLog.sync_status == SyncStatus.ACTIVE,
                SyncLog.callback_received_at == None,  # noqa: E711
                SyncLog.timestamp < cutoff,
            )
            .limit(RECONCILE_BATCH_SIZE)
            .all()
        )

        logger.info(f"Reconciliation sweep: {len(pending_logs)} logs to process.")

        for log in pending_logs:
            _reconcile_single_log(db, log)

        db.commit()
        return f"Sweep complete: {len(pending_logs)} logs processed."

    except Exception as exc:
        db.rollback()
        logger.error(f"Reconciliation sweep failed: {exc}")
        raise exc
    finally:
        db.close()
