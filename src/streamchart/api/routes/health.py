from __future__ import annotations

from typing import Any

from bottle import Bottle, response

from streamchart.config import settings
from streamchart.db import check_database
from streamchart.integrations.kafka_producer import check_broker

SERVICE_NAME = "streamchart-api"


def register_health_routes(app: Bottle) -> None:
    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "service": SERVICE_NAME}

    @app.get("/ready")
    def ready() -> dict[str, Any]:
        ok, detail = check_database()
        if not ok:
            response.status = 503
            return {"status": "error", "code": "not_ready", "error": detail}
        if settings.kafka_bootstrap_servers:
            broker_ok, broker_detail = check_broker()
            if not broker_ok:
                response.status = 503
                return {"status": "error", "code": "not_ready", "error": broker_detail}
        return {"status": "ok"}
