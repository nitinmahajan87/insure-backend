"""add unique constraint on employees(corporate_id, employee_code)

Revision ID: d4e5f6a7b8c9
Revises: 4127433ba992
Create Date: 2026-03-07

Adds a unique constraint on (corporate_id, employee_code) in the employees table.
This enforces at the DB level what the application already assumes — one employee
record per corporate — and enables future use of INSERT ... ON CONFLICT DO UPDATE
for true single-query bulk upserts (currently we SELECT-then-update in Python).

A CONCURRENTLY-built unique index is used so the operation does not lock the table
during the upgrade on a live database.  Downgrade drops the constraint cleanly.
"""
from alembic import op

revision = 'd4e5f6a7b8c9'
down_revision = '4127433ba992'
branch_labels = None
depends_on = None


def upgrade():
    # Build the unique index concurrently so it does not hold a table lock.
    # CREATE UNIQUE INDEX CONCURRENTLY cannot run inside a transaction block,
    # so we disable the implicit transaction Alembic wraps around migrations.
    op.execute("COMMIT")
    op.execute(
        """
        CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS
            uq_employee_corporate_code
        ON employees (corporate_id, employee_code)
        """
    )
    # Attach the index as the backing store for the named constraint so that
    # SQLAlchemy / Alembic can introspect it correctly.
    op.execute(
        """
        ALTER TABLE employees
            ADD CONSTRAINT uq_employee_corporate_code
            UNIQUE USING INDEX uq_employee_corporate_code
        """
    )


def downgrade():
    op.drop_constraint('uq_employee_corporate_code', 'employees', type_='unique')
