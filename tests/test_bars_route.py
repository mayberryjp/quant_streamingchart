from __future__ import annotations

from tests.conftest import make_bar
from webtest import TestApp

import streamchart.api.routes.bars as bars_mod


def test_bars_requires_ticker(app: TestApp) -> None:
    resp = app.get("/api/v1/bars", expect_errors=True)
    assert resp.status_code == 422
    assert resp.json["code"] == "validation_error"


def test_bars_passthrough_1m(app: TestApp, monkeypatch) -> None:
    bars = [make_bar(i, 100.0 + i) for i in range(3)]
    monkeypatch.setattr(bars_mod, "get_bars", lambda t, i: bars)
    resp = app.get("/api/v1/bars?ticker=MSFT&interval=1m")
    assert resp.status_code == 200
    assert resp.json["count"] == 3
    assert resp.json["interval"] == "1m"


def test_bars_resampled_5m(app: TestApp, monkeypatch) -> None:
    bars = [make_bar(i, 100.0 + i) for i in range(10)]
    monkeypatch.setattr(bars_mod, "get_bars", lambda t, i: bars)
    resp = app.get("/api/v1/bars?ticker=MSFT&interval=5m")
    assert resp.status_code == 200
    assert resp.json["interval"] == "5m"
    assert resp.json["count"] == 2
