"""add_lifecycle_schema_phase0

Adds the schema required for Pillars 1-3 of the scalability hardening:
  - Pillar 1: Reconciliation fields on sync_logs (insurer_reference_id,
              callback_received_at, rejection_reason)
  - Pillar 2: Split Employee status into delivery_status + policy_status
              (renames employees.sync_status → employees.delivery_status,
               adds policy_status / policy_effective_date / insurer_reference_id /
               rejection_reason)
  - Pillar 3: Broker-scoped API keys (adds api_keys.broker_id FK and scope enum;
              makes api_keys.corporate_id nullable)

Enum changes:
  - syncstatus:   adds SOFT_REJECTED value
  - policystatus: new type (PENDING_ISSUANCE, ISSUED, SOFT_REJECTED, LAPSED, CANCELLED)
  - apikeyscope:  new type (CORPORATE, BROKER)

Revision ID: e3a1b2c4d5f6
Revises: 365001e83183
Create Date: 2026-03-04 10:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e3a1b2c4d5f6'
down_revision: Union[str, Sequence[str], None] = '365001e83183'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # 1. Extend the existing syncstatus PostgreSQL enum.
    #
    #    Migration f78ff425daf2 created syncstatus with only 5 values.
    #    Migration 365001e83183 referenced PENDING_OFFLINE, COMPLETED_BOTH,
    #    PENDING_BOTH in sa.Enum() Python-side but never issued ALTER TYPE,
    #    so those values are missing from the live PG enum. Add them all
    #    here along with SOFT_REJECTED (Pillar 1).
    #
    #    ALTER TYPE ... ADD VALUE is transactional in PostgreSQL 12+ but
    #    the new value cannot be referenced (in defaults/constraints)
    #    within the same transaction.
    # ------------------------------------------------------------------
    op.execute(sa.text("ALTER TYPE syncstatus ADD VALUE IF NOT EXISTS 'PENDING_OFFLINE'"))
    op.execute(sa.text("ALTER TYPE syncstatus ADD VALUE IF NOT EXISTS 'COMPLETED_BOTH'"))
    op.execute(sa.text("ALTER TYPE syncstatus ADD VALUE IF NOT EXISTS 'PENDING_BOTH'"))
    op.execute(sa.text("ALTER TYPE syncstatus ADD VALUE IF NOT EXISTS 'SOFT_REJECTED'"))

    # ------------------------------------------------------------------
    # 2. Create new PostgreSQL enum types.
    # ------------------------------------------------------------------
    policystatus = sa.Enum(
        'PENDING_ISSUANCE', 'ISSUED', 'SOFT_REJECTED', 'LAPSED', 'CANCELLED',
        name='policystatus'
    )
    policystatus.create(op.get_bind(), checkfirst=True)

    apikeyscope = sa.Enum('CORPORATE', 'BROKER', name='apikeyscope')
    apikeyscope.create(op.get_bind(), checkfirst=True)

    # ------------------------------------------------------------------
    # 3. employees table
    #    3a. Rename sync_status → delivery_status (column data is preserved)
    #    3b. Add policy lifecycle columns
    # ------------------------------------------------------------------
    op.alter_column('employees', 'sync_status', new_column_name='delivery_status')

    op.add_column('employees', sa.Column(
        'policy_status',
        sa.Enum(
            'PENDING_ISSUANCE', 'ISSUED', 'SOFT_REJECTED', 'LAPSED', 'CANCELLED',
            name='policystatus'
        ),
        nullable=True
    ))
    op.add_column('employees', sa.Column('policy_effective_date', sa.Date(), nullable=True))
    op.add_column('employees', sa.Column('insurer_reference_id', sa.String(), nullable=True))
    op.add_column('employees', sa.Column('rejection_reason', sa.String(), nullable=True))

    # ------------------------------------------------------------------
    # 4. sync_logs table — reconciliation fields
    # ------------------------------------------------------------------
    op.add_column('sync_logs', sa.Column('insurer_reference_id', sa.String(), nullable=True))
    op.create_index(
        'ix_sync_logs_insurer_reference_id',
        'sync_logs',
        ['insurer_reference_id']
    )
    op.add_column('sync_logs', sa.Column('callback_received_at', sa.DateTime(), nullable=True))
    op.add_column('sync_logs', sa.Column('rejection_reason', sa.String(), nullable=True))

    # ------------------------------------------------------------------
    # 5. api_keys table — broker scope support
    #    5a. Make corporate_id nullable (broker-scoped keys have no corporate)
    #    5b. Add broker_id FK
    #    5c. Add scope enum column (server_default keeps existing rows valid)
    # ------------------------------------------------------------------
    op.alter_column(
        'api_keys', 'corporate_id',
        existing_type=sa.String(),
        nullable=True
    )
    op.add_column('api_keys', sa.Column('broker_id', sa.String(), nullable=True))
    op.create_foreign_key(
        'fk_api_keys_broker_id',
        'api_keys', 'brokers',
        ['broker_id'], ['id']
    )
    op.add_column('api_keys', sa.Column(
        'scope',
        sa.Enum('CORPORATE', 'BROKER', name='apikeyscope'),
        nullable=False,
        server_default='CORPORATE'
    ))


def downgrade() -> None:
    # ------------------------------------------------------------------
    # 5. Revert api_keys changes
    # ------------------------------------------------------------------
    op.drop_column('api_keys', 'scope')
    op.drop_constraint('fk_api_keys_broker_id', 'api_keys', type_='foreignkey')
    op.drop_column('api_keys', 'broker_id')
    op.alter_column(
        'api_keys', 'corporate_id',
        existing_type=sa.String(),
        nullable=False
    )

    # ------------------------------------------------------------------
    # 4. Revert sync_logs changes
    # ------------------------------------------------------------------
    op.drop_column('sync_logs', 'rejection_reason')
    op.drop_column('sync_logs', 'callback_received_at')
    op.drop_index('ix_sync_logs_insurer_reference_id', table_name='sync_logs')
    op.drop_column('sync_logs', 'insurer_reference_id')

    # ------------------------------------------------------------------
    # 3. Revert employees changes
    # ------------------------------------------------------------------
    op.drop_column('employees', 'rejection_reason')
    op.drop_column('employees', 'insurer_reference_id')
    op.drop_column('employees', 'policy_effective_date')
    op.drop_column('employees', 'policy_status')
    op.alter_column('employees', 'delivery_status', new_column_name='sync_status')

    # ------------------------------------------------------------------
    # 2. Drop new enum types
    # ------------------------------------------------------------------
    sa.Enum(name='apikeyscope').drop(op.get_bind(), checkfirst=True)
    sa.Enum(name='policystatus').drop(op.get_bind(), checkfirst=True)

    # ------------------------------------------------------------------
    # 1. NOTE: PostgreSQL does not support removing values from an enum
    #    type. SOFT_REJECTED will remain in the syncstatus enum after
    #    downgrade. This is safe — no column uses it as a default.
    # ------------------------------------------------------------------
