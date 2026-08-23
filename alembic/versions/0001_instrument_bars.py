"""instrument_bars

Revision ID: 0001
Revises:
Create Date: 2026-08-23

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "instrument_bars",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("ticker", sa.Text(), nullable=False),
        sa.Column("interval", sa.Text(), nullable=False),
        sa.Column("bar_time", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("open", sa.Numeric(18, 6), nullable=False),
        sa.Column("high", sa.Numeric(18, 6), nullable=False),
        sa.Column("low", sa.Numeric(18, 6), nullable=False),
        sa.Column("close", sa.Numeric(18, 6), nullable=False),
        sa.Column("volume", sa.BigInteger(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False, server_default="yahoo"),
        sa.Column("fetched_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.UniqueConstraint("ticker", "interval", "bar_time", name="uq_instrument_bars_tib"),
    )
    op.create_index(
        "ix_instrument_bars_ticker_interval_time",
        "instrument_bars",
        ["ticker", "interval", "bar_time"],
    )


def downgrade() -> None:
    op.drop_index("ix_instrument_bars_ticker_interval_time", table_name="instrument_bars")
    op.drop_table("instrument_bars")
