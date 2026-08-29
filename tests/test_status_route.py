from __future__ import annotations

from datetime import UTC, datetime

from webtest import TestApp

import streamchart.api.routes.status as status_mod
from streamchart.domain.replay import ReplaySession

_NOW = datetime(2026, 8, 23, 10, 0, 0, tzinfo=UTC)


def _session(ticker: str = "MSFT") -> ReplaySession:
    return ReplaySession(
        id="abc-123",
        ticker=ticker,
        interval="1m",
        replay_interval_seconds=1.0,
        kafka_topic="market.replay.bars",
        status="completed",
        total_slices=3,
        emitted_slices=3,
        last_sequence=2,
        created_at=_NOW,
    )


def _summary(ticker: str = "MSFT") -> dict:
    return {
        "ticker": ticker,
        "interval": "1m",
        "bars": 390,
        "first_bar": _NOW,
        "last_bar": _NOW,
        "last_fetched_at": _NOW,
    }


def test_status_combines_fetches_and_replays(app: TestApp, monkeypatch) -> None:
    monkeypatch.setattr(status_mod, "list_fetch_summaries", lambda t=None: [_summary("MSFT")])
    monkeypatch.setattr(status_mod, "list_sessions", lambda: [_session("MSFT")])

    resp = app.get("/api/v1/status")
    assert resp.status_code == 200
    entries = resp.json["status"]
    assert len(entries) == 1
    entry = entries[0]
    assert entry["ticker"] == "MSFT"
    assert entry["fetches"][0]["bars"] == 390
    assert entry["fetches"][0]["last_fetched_at"] == "2026-08-23T10:00:00Z"
    assert entry["replays"][0]["status"] == "completed"


def test_status_filters_by_ticker(app: TestApp, monkeypatch) -> None:
    seen: dict[str, str | None] = {}

    def fake_summaries(ticker: str | None = None) -> list[dict]:
        seen["ticker"] = ticker
        return [_summary("UBER")]

    monkeypatch.setattr(status_mod, "list_fetch_summaries", fake_summaries)
    monkeypatch.setattr(
        status_mod, "list_sessions", lambda: [_session("UBER"), _session("MSFT")]
    )

    resp = app.get("/api/v1/status?ticker=uber")
    assert resp.status_code == 200
    assert seen["ticker"] == "UBER"
    entries = resp.json["status"]
    assert [e["ticker"] for e in entries] == ["UBER"]
    assert len(entries[0]["replays"]) == 1
