from __future__ import annotations

import json
from typing import Any

from bottle import Bottle, response

from streamchart.api.plugins import AppPlugin
from streamchart.api.routes.bars import register_bars_routes
from streamchart.api.routes.fetch import register_fetch_routes
from streamchart.api.routes.health import register_health_routes
from streamchart.api.routes.replays import register_replay_routes
from streamchart.logging import get_logger

SERVICE_NAME = "streamchart-api"


def create_app() -> Bottle:
    app = Bottle()
    app.install(AppPlugin(get_logger("streamchart.api")))

    register_health_routes(app)
    register_fetch_routes(app)
    register_bars_routes(app)
    register_replay_routes(app)

    @app.error(404)
    def not_found(_err: Any) -> str:
        response.content_type = "application/json"
        return json.dumps({"status": "error", "code": "not_found", "error": "not found"})

    return app


app = create_app()
