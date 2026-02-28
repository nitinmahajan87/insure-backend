from app.core.celery_app import celery_app
from app.core.database import SessionLocal  # Standard Sync Session
from app.models.models import SyncLog, Corporate, Employee, SyncStatus, DeliveryChannel
from app.services.insurer_connector import InsurerConnector
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def process_sync_event(self, log_id: int):
    db = SessionLocal()
    log = None
    try:
        log = db.query(SyncLog).filter(SyncLog.id == log_id).first()
        if not log:
            return f"Log {log_id} not found"

        corporate = db.query(Corporate).filter(Corporate.id == log.corporate_id).first()

        # 1. Update states to PROVISIONING
        log.sync_status = SyncStatus.PROVISIONING
        db.commit()

        # 2. Identify the target employee
        emp_code = log.payload.get("employee_code")
        employee = db.query(Employee).filter(
            Employee.corporate_id == log.corporate_id,
            Employee.employee_code == emp_code
        ).first()

        # 3. ROUTE BY DELIVERY CHANNEL
        if corporate.delivery_channel in [DeliveryChannel.WEBHOOK, DeliveryChannel.BOTH]:
            try:
                # Use the new Sync wrapper
                response_data = InsurerConnector.push_to_insurer_sync(
                    payload=log.payload,
                    target_url=corporate.webhook_url,
                    format_type=corporate.insurer_format
                )
                log.raw_response = response_data
                log.sync_status = SyncStatus.ACTIVE

                if employee:
                    # If it was a deletion, we might mark as FAILED or handle differently,
                    # but for now, we follow your logic to mark as ACTIVE (synced)
                    employee.sync_status = SyncStatus.ACTIVE

            except Exception as exc:
                log.retry_count += 1
                log.error_message = str(exc)
                db.commit()
                # Trigger retry if max_retries not reached
                raise self.retry(exc=exc)

        elif corporate.delivery_channel == DeliveryChannel.OFFLINE:
            # Mark as completed (File was generated during upload)
            log.sync_status = SyncStatus.COMPLETED_OFFLINE
            if employee:
                employee.sync_status = SyncStatus.COMPLETED_OFFLINE
        # Final commit for success
        db.commit()
        return f"Processed log {log_id} successfully"

    except Exception as e:
        db.rollback()
        if log:
            log.sync_status = SyncStatus.FAILED
            log.error_message = str(e)
            db.commit()
        logger.error(f"Task failed for log {log_id}: {str(e)}")
        # We don't raise e here as we want the task to finish as "Failed" in Celery
        return f"Failed: {str(e)}"
    finally:
        db.close()