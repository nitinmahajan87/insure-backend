"""add_syn_log_events

Revision ID: 365001e83183
Revises: 53849444a78c
Create Date: 2026-03-02 15:56:49.320767

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ENUM as pgEnum


# revision identifiers, used by Alembic.
revision: str = '365001e83183'
down_revision: Union[str, Sequence[str], None] = '53849444a78c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # syncstatus already exists (created by f78ff425daf2).
    # Use pgEnum with create_type=False so SQLAlchemy doesn't try to CREATE
    # the type again — it would fail with DuplicateObject.
    # The new values (PENDING_OFFLINE, COMPLETED_BOTH, PENDING_BOTH) are
    # added via ALTER TYPE in the Phase-0 migration (e3a1b2c4d5f6).
    existing_syncstatus = pgEnum(
        'PENDING', 'PROVISIONING', 'ACTIVE', 'FAILED', 'COMPLETED_OFFLINE',
        'PENDING_OFFLINE', 'COMPLETED_BOTH', 'PENDING_BOTH',
        name='syncstatus',
        create_type=False,
    )

    conn = op.get_bind()
    table_exists = conn.execute(sa.text(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
        "WHERE table_name = 'sync_log_events')"
    )).scalar()

    if not table_exists:
        op.create_table(
            'sync_log_events',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('sync_log_id', sa.Integer(), nullable=True),
            sa.Column('event_status', existing_syncstatus, nullable=False),
            sa.Column('actor', sa.String(), nullable=False),
            sa.Column('details', sa.JSON(), nullable=True),
            sa.Column('timestamp', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
            sa.ForeignKeyConstraint(['sync_log_id'], ['sync_logs.id'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
        )
        op.create_index(
            op.f('ix_sync_log_events_sync_log_id'),
            'sync_log_events', ['sync_log_id'], unique=False,
        )

    # syncsource is a new type. Use raw SQL with IF NOT EXISTS so this
    # migration is idempotent in case the column was created by a previous
    # partial run. The type is auto-created if it doesn't exist.
    op.execute(sa.text(
        "DO $$ BEGIN "
        "  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'syncsource') THEN "
        "    CREATE TYPE syncsource AS ENUM ('ONLINE', 'BATCH'); "
        "  END IF; "
        "END $$;"
    ))
    op.execute(sa.text(
        "ALTER TABLE sync_logs ADD COLUMN IF NOT EXISTS "
        "source syncsource DEFAULT 'ONLINE';"
    ))


def downgrade() -> None:
    op.drop_column('sync_logs', 'source')
    op.drop_index(op.f('ix_sync_log_events_sync_log_id'), table_name='sync_log_events')
    # create_type=False means SQLAlchemy won't try to DROP the shared syncstatus type
    op.drop_table('sync_log_events')
    sa.Enum(name='syncsource').drop(op.get_bind(), checkfirst=True)
