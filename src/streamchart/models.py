from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Column,
    Integer,
    MetaData,
    Numeric,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID

metadata = MetaData()

instrument_bars = Table(
    "instrument_bars",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("ticker", Text, nullable=False),
    Column("interval", Text, nullable=False),
    Column("bar_time", TIMESTAMP(timezone=True), nullable=False),
    Column("open", Numeric(18, 6), nullable=False),
    Column("high", Numeric(18, 6), nullable=False),
    Column("low", Numeric(18, 6), nullable=False),
    Column("close", Numeric(18, 6), nullable=False),
    Column("volume", BigInteger, nullable=True),
    Column("source", Text, nullable=False, server_default="yahoo"),
    Column("fetched_at", TIMESTAMP(timezone=True), nullable=False),
    UniqueConstraint("ticker", "interval", "bar_time", name="uq_instrument_bars_tib"),
)

replay_sessions = Table(
    "replay_sessions",
    metadata,
    Column("id", UUID(as_uuid=False), primary_key=True),
    Column("ticker", Text, nullable=False),
    Column("interval", Text, nullable=False),
    Column("replay_interval_seconds", Numeric(6, 3), nullable=False),
    Column("kafka_topic", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("total_slices", Integer, nullable=False),
    Column("emitted_slices", Integer, nullable=False, server_default="0"),
    Column("last_sequence", Integer, nullable=False, server_default="-1"),
    Column("error", Text, nullable=True),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
    Column("started_at", TIMESTAMP(timezone=True), nullable=True),
    Column("completed_at", TIMESTAMP(timezone=True), nullable=True),
)
