from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from streamchart.timeutil import iso_utc

_UNIT_MINUTES = {"m": 1, "h": 60, "d": 1440}


@dataclass
class Bar:
    """A single OHLCV bar (one chart slice)."""

    ticker: str
    interval: str
    bar_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    source: str = "yahoo"


def parse_interval_minutes(interval: str) -> int:
    """Parse an interval string such as '1m', '5m', '1h' into minutes."""
    text = interval.strip().lower()
    if len(text) < 2 or text[-1] not in _UNIT_MINUTES or not text[:-1].isdigit():
        raise ValueError(f"invalid interval: {interval!r}")
    count = int(text[:-1])
    if count <= 0:
        raise ValueError(f"invalid interval: {interval!r}")
    return count * _UNIT_MINUTES[text[-1]]


def _floor_to_interval(dt: datetime, minutes: int) -> datetime:
    epoch_minutes = int(dt.timestamp() // 60)
    floored = epoch_minutes - (epoch_minutes % minutes)
    return datetime.fromtimestamp(floored * 60, tz=UTC)


def resample(bars: list[Bar], target_interval: str) -> list[Bar]:
    """Aggregate bars into a coarser interval.

    open=first, high=max, low=min, close=last, volume=sum within each window.
    Windows are labelled by their start (open) time. Input must be sorted
    ascending by bar_time; a 1m target over 1m input is a pass-through.
    """
    minutes = parse_interval_minutes(target_interval)
    out: list[Bar] = []
    current_key: datetime | None = None
    agg: Bar | None = None
    for bar in bars:
        key = _floor_to_interval(bar.bar_time, minutes)
        if agg is None or key != current_key:
            if agg is not None:
                out.append(agg)
            current_key = key
            agg = Bar(
                ticker=bar.ticker,
                interval=target_interval,
                bar_time=key,
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                source=bar.source,
            )
        else:
            agg.high = max(agg.high, bar.high)
            agg.low = min(agg.low, bar.low)
            agg.close = bar.close
            agg.volume += bar.volume
    if agg is not None:
        out.append(agg)
    return out


def bar_to_dict(bar: Bar) -> dict[str, Any]:
    return {
        "ticker": bar.ticker,
        "interval": bar.interval,
        "bar_time": iso_utc(bar.bar_time),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
    }
