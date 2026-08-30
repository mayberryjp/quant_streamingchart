from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from streamchart.db import get_engine
from streamchart.domain.bars import Bar
from streamchart.models import instrument_bars
from streamchart.timeutil import utcnow


def _row_to_bar(row: Any) -> Bar:
    return Bar(
        ticker=row["ticker"],
        interval=row["interval"],
        bar_time=row["bar_time"],
        open=float(row["open"]),
        high=float(row["high"]),
        low=float(row["low"]),
        close=float(row["close"]),
        volume=int(row["volume"]) if row["volume"] is not None else 0,
        source=row["source"],
    )


def upsert_bars(bars: list[Bar]) -> int:
    """Insert or update bars keyed on (ticker, interval, bar_time). Idempotent."""
    if not bars:
        return 0
    now = utcnow()
    rows = [
        {
            "ticker": bar.ticker,
            "interval": bar.interval,
            "bar_time": bar.bar_time,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "source": bar.source,
            "fetched_at": now,
        }
        for bar in bars
    ]
    stmt = pg_insert(instrument_bars).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["ticker", "interval", "bar_time"],
        set_={
            "open": stmt.excluded.open,
            "high": stmt.excluded.high,
            "low": stmt.excluded.low,
            "close": stmt.excluded.close,
            "volume": stmt.excluded.volume,
            "fetched_at": stmt.excluded.fetched_at,
        },
    )
    with get_engine().begin() as conn:
        conn.execute(stmt)
    return len(bars)


def get_bars(ticker: str, interval: str, day: date | None = None) -> list[Bar]:
    stmt = (
        select(instrument_bars)
        .where(instrument_bars.c.ticker == ticker.upper())
        .where(instrument_bars.c.interval == interval)
        .order_by(instrument_bars.c.bar_time.asc())
    )
    if day is not None:
        start = datetime(day.year, day.month, day.day, tzinfo=UTC)
        stmt = stmt.where(
            instrument_bars.c.bar_time >= start,
            instrument_bars.c.bar_time < start + timedelta(days=1),
        )
    with get_engine().connect() as conn:
        rows = conn.execute(stmt).mappings().all()
    return [_row_to_bar(row) for row in rows]


def list_fetch_summaries(ticker: str | None = None) -> list[dict[str, Any]]:
    """Aggregate stored bars into one summary row per (ticker, interval)."""
    stmt = select(
        instrument_bars.c.ticker,
        instrument_bars.c.interval,
        func.count().label("bars"),
        func.min(instrument_bars.c.bar_time).label("first_bar"),
        func.max(instrument_bars.c.bar_time).label("last_bar"),
        func.max(instrument_bars.c.fetched_at).label("last_fetched_at"),
    )
    if ticker is not None:
        stmt = stmt.where(instrument_bars.c.ticker == ticker.upper())
    stmt = stmt.group_by(instrument_bars.c.ticker, instrument_bars.c.interval).order_by(
        instrument_bars.c.ticker.asc(), instrument_bars.c.interval.asc()
    )
    with get_engine().connect() as conn:
        rows = conn.execute(stmt).mappings().all()
    return [dict(row) for row in rows]
