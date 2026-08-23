from __future__ import annotations

from webtest import TestApp


def test_health_ok(app: TestApp) -> None:
    resp = app.get("/health")
    assert resp.status_code == 200
    assert resp.json["status"] == "ok"
    assert resp.json["service"] == "streamchart-api"
