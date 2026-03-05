"""add email and gender to employees

Revision ID: a9f3c2d1e8b7
Revises: e3a1b2c4d5f6
Create Date: 2026-03-05

"""
from alembic import op
import sqlalchemy as sa

revision = 'a9f3c2d1e8b7'
down_revision = 'e3a1b2c4d5f6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('employees', sa.Column('email', sa.String(), nullable=True))
    op.add_column('employees', sa.Column('gender', sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column('employees', 'gender')
    op.drop_column('employees', 'email')
