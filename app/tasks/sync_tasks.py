import json

from app.core.celery_app import celery_app
from app.core.database import SessionLocal  # Standard Sync Session
from app.core.outbound.factory import get_insurer_adapter
from app.models.models import SyncLog, Corporate, Employee, SyncStatus, DeliveryChannel
from app.services.insurer_connector import InsurerConnector, INSURER_API_KEY
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

        is_addition = log.transaction_type in ["ADDITION", "BATCH_ADDITION"]
        is_batch = log.transaction_type in ["BATCH_ADDITION", "BATCH_DELETION"]

        # 3. ROUTE BY DELIVERY CHANNEL
        if corporate.delivery_channel in [DeliveryChannel.WEBHOOK, DeliveryChannel.BOTH]:
            try:

                # A. Get the right Outbound Adapter
                insurer_provider = getattr(corporate, 'insurer_provider', 'standard')
                adapter = get_insurer_adapter(insurer_provider)

                # B. Transform the payload
                #is_addition = log.transaction_type == "ADDITION" or log.transaction_type == "BATCH_ADDITION"
                if is_addition:
                    final_data = adapter.transform_addition(log.payload)
                else:
                    final_data = adapter.transform_deletion(log.payload)

                # Ensure JSON is dumped to string if the adapter returned a dict
                if isinstance(final_data, dict):
                    final_data = json.dumps(final_data)

                # C. Get specific headers
                headers = adapter.get_headers(api_key=INSURER_API_KEY)

                # Use the new Sync wrapper
                response_data = InsurerConnector.push_to_insurer_sync(
                    data= final_data,
                    target_url=corporate.webhook_url,
                    headers=headers
                )
                log.raw_response = response_data
                if corporate.delivery_channel == DeliveryChannel.BOTH:
                    log.sync_status = SyncStatus.PENDING_BOTH
                    if employee:
                        employee.sync_status = SyncStatus.PENDING_BOTH
                else:
                    log.sync_status = SyncStatus.ACTIVE  # Standard webhook success
                    if employee:
                        employee.sync_status = SyncStatus.ACTIVE

            except Exception as exc:
                log.retry_count += 1
                log.error_message = str(exc)
                db.commit()
                # Trigger retry if max_retries not reached
                raise self.retry(exc=exc)

        elif corporate.delivery_channel == DeliveryChannel.OFFLINE:
            if is_batch:
                # File was already generated synchronously in ingestion.py
                log.sync_status = SyncStatus.COMPLETED_OFFLINE
                if employee:
                    employee.sync_status = SyncStatus.COMPLETED_OFFLINE
            else:
                # Real-time stream event. Park it for the Sweeper API.
                log.sync_status = SyncStatus.PENDING_OFFLINE
                if employee:
                    employee.sync_status = SyncStatus.PENDING_OFFLINE
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