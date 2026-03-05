"""add policy_status to sync_logs and sync_log_events; add dob and resignation_reason to employees

Revision ID: a2b4c6d8e0f1
Revises: a9f3c2d1e8b7
Create Date: 2026-03-05

"""
from alembic import op
import sqlalchemy as sa

revision = 'a2b4c6d8e0f1'
down_revision = 'a9f3c2d1e8b7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Employee: missing demographic / offboarding fields
    op.add_column('employees', sa.Column('date_of_birth', sa.Date(), nullable=True))
    op.add_column('employees', sa.Column('resignation_reason', sa.String(), nullable=True))

    # SyncLog: policy_status snapshot — prevents bleed when employee.policy_status changes later
    op.add_column('sync_logs', sa.Column('policy_status', sa.String(), nullable=True))

    # SyncLogEvent: records policy_status transitions in the immutable audit trail
    op.add_column('sync_log_events', sa.Column('policy_status', sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column('sync_log_events', 'policy_status')
    op.drop_column('sync_logs', 'policy_status')
    op.drop_column('employees', 'resignation_reason')
    op.drop_column('employees', 'date_of_birth')
