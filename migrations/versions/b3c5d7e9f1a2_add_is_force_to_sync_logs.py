"""add is_force to sync_logs

Revision ID: b3c5d7e9f1a2
Revises: a2b4c6d8e0f1
Create Date: 2026-03-05

Tracks whether a removal was a forced escape-hatch (employee not in our DB).
"""
from alembic import op
import sqlalchemy as sa

revision = 'b3c5d7e9f1a2'
down_revision = 'a2b4c6d8e0f1'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'sync_logs',
        sa.Column('is_force', sa.Boolean(), nullable=False, server_default='false')
    )


def downgrade():
    op.drop_column('sync_logs', 'is_force')
