from __future__ import annotations

import json
from typing import Any

from bottle import Bottle, response

from streamchart.api.plugins import AppPlugin
from streamchart.api.routes.bars import register_bars_routes
from streamchart.api.routes.fetch import register_fetch_routes
from streamchart.api.routes.health import register_health_routes
from streamchart.api.routes.replays import register_replay_routes
from streamchart.api.routes.status import register_status_routes
from streamchart.logging import get_logger

SERVICE_NAME = "streamchart-api"


def create_app() -> Bottle:
    app = Bottle()
    app.install(AppPlugin(get_logger("streamchart.api")))

    register_health_routes(app)
    register_fetch_routes(app)
    register_bars_routes(app)
    register_replay_routes(app)
    register_status_routes(app)

    def add_cors_headers() -> None:
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Origin, Content-Type, Accept"

    app.add_hook("after_request", add_cors_headers)

    @app.route("/<:re:.*>", method="OPTIONS")
    def cors_preflight() -> str:
        return ""

    @app.error(404)
    def not_found(_err: Any) -> str:
        add_cors_headers()
        response.content_type = "application/json"
        return json.dumps({"status": "error", "code": "not_found", "error": "not found"})

    return app


app = create_app()
