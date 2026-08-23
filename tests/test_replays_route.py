from __future__ import annotations

from datetime import UTC, datetime

from tests.conftest import make_bar
from webtest import TestApp

import streamchart.api.routes.replays as replays_mod
from streamchart.domain.replay import ReplaySession
from streamchart.errors import ConflictError

_NOW = datetime(2026, 8, 23, 10, 0, 0, tzinfo=UTC)


def _session(status: str = "pending", total: int = 3) -> ReplaySession:
    return ReplaySession(
        id="abc-123",
        ticker="MSFT",
        interval="1m",
        replay_interval_seconds=1.0,
        kafka_topic="market.replay.bars",
        status=status,
        total_slices=total,
        emitted_slices=0,
        last_sequence=-1,
        created_at=_NOW,
    )


def test_create_replay(app: TestApp, monkeypatch) -> None:
    monkeypatch.setattr(
        replays_mod, "get_bars", lambda t, i: [make_bar(i, 1.0 + i) for i in range(3)]
    )
    monkeypatch.setattr(
        replays_mod, "create_session", lambda **kwargs: _session(total=kwargs["total_slices"])
    )

    resp = app.post_json("/api/v1/replays", {"ticker": "MSFT", "interval": "1m"})
    assert resp.status_code == 200
    assert resp.json["status"] == "pending"
    assert resp.json["total_slices"] == 3


def test_create_replay_no_bars_returns_422(app: TestApp, monkeypatch) -> None:
    monkeypatch.setattr(replays_mod, "get_bars", lambda t, i: [])
    resp = app.post_json("/api/v1/replays", {"ticker": "MSFT"}, expect_errors=True)
    assert resp.status_code == 422
    assert resp.json["code"] == "validation_error"


def test_get_replay_not_found(app: TestApp, monkeypatch) -> None:
    monkeypatch.setattr(replays_mod, "get_session", lambda sid: None)
    resp = app.get("/api/v1/replays/missing", expect_errors=True)
    assert resp.status_code == 404


def test_cancel_conflict(app: TestApp, monkeypatch) -> None:
    def boom(sid: str) -> ReplaySession:
        raise ConflictError("already done")

    monkeypatch.setattr(replays_mod, "request_cancel", boom)
    resp = app.post("/api/v1/replays/abc/cancel", expect_errors=True)
    assert resp.status_code == 409
