import json
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models.models import Employee, SyncLog
from datetime import datetime

async def record_employee_event(
        db: AsyncSession,
        corporate_id: str,
        employee_data: dict,
        event_type: str
):
    """
    Handles the DB persistence for both Real-time and Batch events.
    Matches 'employee_code' within a specific 'corporate_id'.
    """

    # 1. Check for existing employee
    stmt = select(Employee).where(
        Employee.corporate_id == corporate_id,
        Employee.employee_code == employee_data["employee_code"]
    )
    result = await db.execute(stmt)
    existing_employee = result.scalars().first()

    is_deletion = "DELETION" in event_type

    if existing_employee:
        if is_deletion:
            existing_employee.status = "inactive"
            existing_employee.date_of_leaving = employee_data.get("date_of_leaving")
        else:
            #Updating
            existing_employee.status = "active"
            existing_employee.date_of_leaving = None
            existing_employee.date_of_joining = employee_data.get("date_of_joining", existing_employee.date_of_joining)
            existing_employee.first_name = employee_data.get("first_name", existing_employee.first_name)
            existing_employee.last_name = employee_data.get("last_name", existing_employee.last_name)
            existing_employee.sum_insured = employee_data.get("sum_insured", existing_employee.sum_insured)
    else:
        # Create record if doesn't exist (even for deletions, for history)
        new_emp = Employee(
            corporate_id=corporate_id,
            employee_code=employee_data["employee_code"],
            first_name=employee_data.get("first_name", "Unknown"),
            last_name=employee_data.get("last_name", "Unknown"),
            status="inactive" if is_deletion else "active",
            date_of_joining=employee_data.get("date_of_joining") if not is_deletion else None,
            date_of_leaving=employee_data.get("date_of_leaving") if is_deletion else None,
            sum_insured=employee_data.get("sum_insured", 0)
        )
        db.add(new_emp)

    # Extract transaction metadata
    txn_id = employee_data.get("transaction_id")
    # 2. JSON Serialization Fix:
    # Convert dict (with potential date objects) to string-only dict for Postgres
    safe_payload = json.loads(json.dumps(employee_data, default=str))

    # 3. Log the transaction for the History Tab
    log = SyncLog(
        corporate_id=corporate_id,
        transaction_id=txn_id,
        transaction_type=event_type,
        payload=safe_payload, # Use the clean, stringified payload
        status="success",
        timestamp=datetime.utcnow()
    )
    db.add(log)

    # In FastAPI/SQLAlchemy, we commit at the end of the request
    await db.flush()
    return log # Return the log so the API can get the ID for process_sync_event.delay(log.id)