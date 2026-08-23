from __future__ import annotations

from typing import Any

from bottle import Bottle, request

from streamchart.config import settings
from streamchart.domain.bars import resample
from streamchart.domain.replay import session_to_dict
from streamchart.errors import NotFoundError, ValidationError
from streamchart.repository.bars_repo import get_bars
from streamchart.repository.replays_repo import (
    create_session,
    get_session,
    list_sessions,
    request_cancel,
)


def register_replay_routes(app: Bottle) -> None:
    @app.post("/api/v1/replays")
    def create_replay() -> dict[str, Any]:
        body = request.json or {}
        ticker = str(body.get("ticker") or settings.default_ticker).upper()
        interval = str(body.get("interval") or settings.target_interval)
        delay = float(body.get("replay_interval_seconds") or settings.replay_interval_seconds)
        topic = str(body.get("topic") or settings.kafka_topic)

        base = get_bars(ticker, settings.base_interval)
        slices = resample(base, interval)
        if not slices:
            raise ValidationError(f"no bars stored for {ticker} {interval}; fetch first")

        session = create_session(
            ticker=ticker,
            interval=interval,
            replay_interval_seconds=delay,
            kafka_topic=topic,
            total_slices=len(slices),
        )
        return session_to_dict(session)

    @app.get("/api/v1/replays")
    def list_replays() -> dict[str, Any]:
        return {"replays": [session_to_dict(s) for s in list_sessions()]}

    @app.get("/api/v1/replays/<session_id>")
    def get_replay(session_id: str) -> dict[str, Any]:
        session = get_session(session_id)
        if session is None:
            raise NotFoundError(f"replay {session_id} not found")
        return session_to_dict(session)

    @app.post("/api/v1/replays/<session_id>/cancel")
    def cancel_replay(session_id: str) -> dict[str, Any]:
        session = request_cancel(session_id)
        return session_to_dict(session)
