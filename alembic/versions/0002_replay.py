"""replay_sessions and replay_events

Revision ID: 0002
Revises: 0001
Create Date: 2026-08-23

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "replay_sessions",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column("ticker", sa.Text(), nullable=False),
        sa.Column("interval", sa.Text(), nullable=False),
        sa.Column("replay_interval_seconds", sa.Numeric(6, 3), nullable=False),
        sa.Column("kafka_topic", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("total_slices", sa.Integer(), nullable=False),
        sa.Column("emitted_slices", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_sequence", sa.Integer(), nullable=False, server_default="-1"),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("completed_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_replay_sessions_status_created",
        "replay_sessions",
        ["status", "created_at"],
    )
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


def downgrade() -> None:
    op.drop_table("replay_events")
    op.drop_index("ix_replay_sessions_status_created", table_name="replay_sessions")
    op.drop_table("replay_sessions")
