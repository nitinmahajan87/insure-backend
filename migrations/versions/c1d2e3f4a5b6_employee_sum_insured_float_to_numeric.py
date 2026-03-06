"""employee sum_insured float to numeric

Revision ID: c1d2e3f4a5b6
Revises: b3c5d7e9f1a2
Create Date: 2026-03-06

Changes Employee.sum_insured from FLOAT to NUMERIC(15, 2) to prevent
floating-point precision loss on monetary values (e.g. 500000.10 → 500000.1).
Existing FLOAT values are cast losslessly — NUMERIC(15,2) covers up to
9,999,999,999,999.99 which is well beyond any realistic sum insured.
"""
from alembic import op
import sqlalchemy as sa

revision = 'c1d2e3f4a5b6'
down_revision = 'b3c5d7e9f1a2'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        'employees',
        'sum_insured',
        existing_type=sa.Float(),
        type_=sa.Numeric(precision=15, scale=2),
        existing_nullable=True,
        postgresql_using='sum_insured::numeric(15,2)',
    )


def downgrade():
    op.alter_column(
        'employees',
        'sum_insured',
        existing_type=sa.Numeric(precision=15, scale=2),
        type_=sa.Float(),
        existing_nullable=True,
        postgresql_using='sum_insured::float',
    )
