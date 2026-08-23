from __future__ import annotations

from tests.conftest import make_bar

from streamchart.repository.bars_repo import get_bars, upsert_bars


def test_upsert_and_get(db) -> None:
    bars = [make_bar(i, 100.0 + i) for i in range(3)]
    assert upsert_bars(bars) == 3

    got = get_bars("MSFT", "1m")
    assert len(got) == 3
    assert [b.open for b in got] == [100.0, 101.0, 102.0]


def test_upsert_is_idempotent(db) -> None:
    bars = [make_bar(i, 100.0 + i) for i in range(3)]
    upsert_bars(bars)
    upsert_bars(bars)
    assert len(get_bars("MSFT", "1m")) == 3


def test_upsert_updates_existing(db) -> None:
    upsert_bars([make_bar(0, 100.0)])
    upsert_bars([make_bar(0, 999.0)])
    got = get_bars("MSFT", "1m")
    assert len(got) == 1
    assert got[0].open == 999.0


def test_empty_upsert_returns_zero(db) -> None:
    assert upsert_bars([]) == 0
