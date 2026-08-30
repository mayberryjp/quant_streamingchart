"""drop replay_events

Revision ID: 0003
Revises: 0002
Create Date: 2026-08-30

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table("replay_events")


def downgrade() -> None:
    op.create_table(
        "replay_events",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column(
            "session_id",
            postgresql.UUID(as_uuid=False),
            sa.ForeignKey("replay_sessions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("bar_time", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("emitted_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("kafka_partition", sa.Integer(), nullable=True),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=True),
        sa.UniqueConstraint("session_id", "sequence", name="uq_replay_events_seq"),
    )
