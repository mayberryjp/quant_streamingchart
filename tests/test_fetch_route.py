from __future__ import annotations

from tests.conftest import make_bar
from webtest import TestApp

import streamchart.api.routes.fetch as fetch_mod
from streamchart.errors import NoDataError


def test_fetch_ok(app: TestApp, monkeypatch) -> None:
    bars = [make_bar(0, 100.0), make_bar(1, 101.0)]
    monkeypatch.setattr(fetch_mod, "fetch_intraday", lambda t, i, r: bars)
    monkeypatch.setattr(fetch_mod, "upsert_bars", lambda b: len(b))

    resp = app.post_json("/api/v1/fetch", {"ticker": "msft"})
    assert resp.status_code == 200
    assert resp.json["ticker"] == "MSFT"
    assert resp.json["count"] == 2
    assert resp.json["first_bar"] == "2026-08-21T14:30:00Z"


def test_fetch_no_data_returns_422(app: TestApp, monkeypatch) -> None:
    def boom(*_args: object) -> list:
        raise NoDataError("nothing here")

    monkeypatch.setattr(fetch_mod, "fetch_intraday", boom)
    resp = app.post_json("/api/v1/fetch", {"ticker": "ZZZZ"}, expect_errors=True)
    assert resp.status_code == 422
    assert resp.json["code"] == "no_data"
