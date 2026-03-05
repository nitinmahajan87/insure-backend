"""
Portal Endpoints — HR Portal (First-Party UI)
==============================================
These endpoints are exclusively for the InsureTech HR Portal frontend.
They accept our canonical internal schema DIRECTLY — no HRMS adapter
translation needed. The portal always speaks our own language.

Contrast with /api/v1/stream/* which is for external HRMS webhooks
and routes through the HRMS adapter layer.
"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import date
from typing import Optional
import uuid

from sqlalchemy import select
from app.core.database import get_db
from app.core.security import get_current_tenant, TenantContext
from app.models.models import SyncSource, Employee, SyncLog, SyncStatus
from app.services.employee_service import record_employee_event
from app.tasks.sync_tasks import process_sync_event
from datetime import datetime

router = APIRouter()


# ── Canonical Portal Schemas ──────────────────────────────────────────────────

class PortalAddRequest(BaseModel):
    employee_code: str = Field(..., min_length=1, description="Unique employee ID in your HRMS")
    first_name: str = Field(..., min_length=1)
    last_name: str = Field(..., min_length=1)
    email: Optional[str] = None
    date_of_birth: Optional[date] = None
    date_of_joining: date
    gender: Optional[str] = "Unknown"
    # Defaults — portal always enrolls the primary employee at base sum insured
    relationship: str = "Self"
    sum_insured: float = 0.0

    @field_validator('date_of_birth')
    def check_dob(cls, v):
        if v and v > date.today():
            raise ValueError('Date of birth cannot be in the future')
        return v

    @field_validator('date_of_joining')
    def check_doj(cls, v):
        if v > date.today():
            raise ValueError('Date of joining cannot be in the future')
        return v


class PortalRemoveRequest(BaseModel):
    employee_code: str = Field(..., min_length=1)
    date_of_leaving: date
    reason: Optional[str] = None
    # UI-only name field — used to populate ghost record on forced removal
    name: Optional[str] = None
    # Force-remove an employee not in our DB (escape hatch)
    force: bool = False


class PortalResponse(BaseModel):
    transaction_id: str
    message: str


# ── Employee Lookup (for form auto-fill) ─────────────────────────────────────

@router.get("/employees/{employee_code}")
async def portal_get_employee(
    employee_code: str,
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
):
    """
    Look up an existing active employee by code.
    Used by the HR Portal form to auto-fill fields when updating.
    Returns 404 if not found or inactive.
    """
    stmt = select(Employee).where(
        Employee.corporate_id == tenant.corporate.id,
        Employee.employee_code == employee_code,
        Employee.status == "active",
    )
    result = await db.execute(stmt)
    emp = result.scalars().first()

    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found or inactive.")

    return {
        "employee_code": emp.employee_code,
        "first_name": emp.first_name,
        "last_name": emp.last_name,
        "email": emp.email,
        "gender": emp.gender,
        "date_of_birth": str(emp.date_of_birth) if emp.date_of_birth else None,
        "date_of_joining": str(emp.date_of_joining) if emp.date_of_joining else None,
        "policy_status": emp.policy_status.value if emp.policy_status else None,
        "delivery_status": emp.delivery_status.value if emp.delivery_status else None,
    }


# ── Add Employee ──────────────────────────────────────────────────────────────

@router.post("/employees/add", response_model=PortalResponse)
async def portal_add_employee(
    payload: PortalAddRequest,
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
):
    """
    HR Portal: Enrol a single employee into insurance coverage.

    Duplicate handling:
    - Employee active + no data change  → return early (idempotent, no re-dispatch)
    - Employee active + data changed    → update record + re-sync to insurer
    - Employee inactive (was removed)   → treat as fresh re-enrolment
    - New employee                      → enrol normally
    """
    try:
        # ── Duplicate / re-submission check ──────────────────────────────────
        emp_stmt = select(Employee).where(
            Employee.corporate_id == tenant.corporate.id,
            Employee.employee_code == payload.employee_code,
        )
        result = await db.execute(emp_stmt)
        existing = result.scalars().first()

        if existing and existing.status == "active":
            # Check whether any meaningful field has changed
            data_changed = (
                existing.first_name != payload.first_name
                or existing.last_name != payload.last_name
                or str(existing.date_of_joining or "") != str(payload.date_of_joining)
            )
            if not data_changed:
                # Truly identical re-submission — acknowledge without re-processing
                return PortalResponse(
                    transaction_id="DUPLICATE",
                    message=(
                        f"{payload.first_name} {payload.last_name} ({payload.employee_code}) "
                        f"is already enrolled and up to date. No action taken."
                    ),
                )
            # Data changed — fall through to update + re-sync, but mark updated_at explicitly
            existing.updated_at = datetime.utcnow()

        # ── Normal enrol / update flow ────────────────────────────────────────
        is_update = existing is not None and existing.status == "active"
        event_type = "UPDATE" if is_update else "ADDITION"

        employee_dict = payload.model_dump()
        employee_dict["transaction_id"] = f"PORTAL-{uuid.uuid4().hex[:8].upper()}"

        log_entry = await record_employee_event(
            db=db,
            corporate_id=tenant.corporate.id,
            employee_data=employee_dict,
            event_type=event_type,
            source=SyncSource.ONLINE,
        )
        await db.commit()
        process_sync_event.delay(log_entry.id)

        action = "updated and re-synced" if is_update else "queued for coverage"
        return PortalResponse(
            transaction_id=str(log_entry.id),
            message=f"{payload.first_name} {payload.last_name} ({payload.employee_code}) {action}.",
        )
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# ── Remove Employee ───────────────────────────────────────────────────────────

@router.post("/employees/remove", response_model=PortalResponse)
async def portal_remove_employee(
    payload: PortalRemoveRequest,
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
):
    """
    HR Portal: Terminate an employee's insurance coverage.

    Standard flow  (force=False): rejects if employee not found or already inactive.
    Escape hatch   (force=True):  proceeds even if not in DB — creates a minimal
                                  ghost record using the name provided by HR.
    """
    try:
        # ── Existence check ───────────────────────────────────────────────────
        emp_stmt = select(Employee).where(
            Employee.corporate_id == tenant.corporate.id,
            Employee.employee_code == payload.employee_code,
        )
        result = await db.execute(emp_stmt)
        existing = result.scalars().first()

        if not payload.force:
            if not existing:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"Employee '{payload.employee_code}' is not enrolled in our system. "
                        "Cannot remove an employee who was never added. "
                        "If you believe this is an error, use the force-remove option."
                    ),
                )
            if existing.status != "active":
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"Employee '{payload.employee_code}' is already inactive. "
                        "No action taken."
                    ),
                )

        # ── Build payload — inject first/last name for ghost record if forced ─
        employee_dict = payload.model_dump(exclude={"force", "name"})
        employee_dict["transaction_id"] = f"PORTAL-{uuid.uuid4().hex[:8].upper()}"

        if not existing and payload.name:
            # Split HR-provided full name for the ghost record
            parts = payload.name.strip().split(maxsplit=1)
            employee_dict["first_name"] = parts[0]
            employee_dict["last_name"] = parts[1] if len(parts) > 1 else ""

        log_entry = await record_employee_event(
            db=db,
            corporate_id=tenant.corporate.id,
            employee_data=employee_dict,
            event_type="DELETION",
            source=SyncSource.ONLINE,
        )
        # Stamp force flag so audit history can surface this as a force-removal
        log_entry.is_force = payload.force
        await db.commit()
        process_sync_event.delay(log_entry.id)

        forced_note = " (forced — employee was not previously enrolled)" if not existing else ""
        return PortalResponse(
            transaction_id=str(log_entry.id),
            message=f"Removal for {payload.employee_code} queued.{forced_note}",
        )
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
