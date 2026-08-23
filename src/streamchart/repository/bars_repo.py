from __future__ import annotations

from typing import Any

from sqlalchemy import select
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


def get_bars(ticker: str, interval: str) -> list[Bar]:
    stmt = (
        select(instrument_bars)
        .where(instrument_bars.c.ticker == ticker.upper())
        .where(instrument_bars.c.interval == interval)
        .order_by(instrument_bars.c.bar_time.asc())
    )
    with get_engine().connect() as conn:
        rows = conn.execute(stmt).mappings().all()
    return [_row_to_bar(row) for row in rows]
