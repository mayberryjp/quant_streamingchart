from __future__ import annotations

from webtest import TestApp

import streamchart.api.routes.health as health_mod


def test_ready_ok(app: TestApp, monkeypatch) -> None:
    monkeypatch.setattr(health_mod, "check_database", lambda: (True, "ok"))
    resp = app.get("/ready")
    assert resp.status_code == 200
    assert resp.json["status"] == "ok"


def test_ready_db_down(app: TestApp, monkeypatch) -> None:
    monkeypatch.setattr(health_mod, "check_database", lambda: (False, "db down"))
    resp = app.get("/ready", expect_errors=True)
    assert resp.status_code == 503
    assert resp.json["code"] == "not_ready"
