from __future__ import annotations

import pytest
from tests.conftest import make_bar

from streamchart.domain.bars import parse_interval_minutes, resample


def test_parse_interval_minutes() -> None:
    assert parse_interval_minutes("1m") == 1
    assert parse_interval_minutes("5m") == 5
    assert parse_interval_minutes("1h") == 60


@pytest.mark.parametrize("bad", ["", "m", "0m", "-5m", "5x", "abc"])
def test_parse_interval_minutes_invalid(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_interval_minutes(bad)


def test_resample_1m_passthrough() -> None:
    bars = [make_bar(i, 100.0 + i) for i in range(3)]
    out = resample(bars, "1m")
    assert len(out) == 3
    assert [b.open for b in out] == [100.0, 101.0, 102.0]


def test_resample_5m_aggregates_ohlcv() -> None:
    # 10 one-minute bars -> two 5-minute bars.
    bars = [make_bar(i, 100.0 + i, volume=10) for i in range(10)]
    out = resample(bars, "5m")
    assert len(out) == 2

    first = out[0]
    assert first.open == bars[0].open
    assert first.close == bars[4].close
    assert first.high == max(b.high for b in bars[:5])
    assert first.low == min(b.low for b in bars[:5])
    assert first.volume == sum(b.volume for b in bars[:5])
    assert first.interval == "5m"

    second = out[1]
    assert second.open == bars[5].open
    assert second.close == bars[9].close


def test_resample_empty() -> None:
    assert resample([], "5m") == []
