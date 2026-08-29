from __future__ import annotations

from typing import Any

from bottle import Bottle, request

from streamchart.domain.replay import session_to_dict
from streamchart.repository.bars_repo import list_fetch_summaries
from streamchart.repository.replays_repo import list_sessions
from streamchart.timeutil import iso_utc


def register_status_routes(app: Bottle) -> None:
    @app.get("/api/v1/status")
    def status() -> dict[str, Any]:
        raw = request.query.get("ticker")
        ticker = raw.upper() if raw else None

        by_ticker: dict[str, dict[str, Any]] = {}

        def entry(symbol: str) -> dict[str, Any]:
            return by_ticker.setdefault(
                symbol, {"ticker": symbol, "fetches": [], "replays": []}
            )

        for f in list_fetch_summaries(ticker):
            entry(f["ticker"])["fetches"].append(
                {
                    "interval": f["interval"],
                    "bars": int(f["bars"]),
                    "first_bar": iso_utc(f["first_bar"]),
                    "last_bar": iso_utc(f["last_bar"]),
                    "last_fetched_at": iso_utc(f["last_fetched_at"]),
                }
            )

        for session in list_sessions():
            if ticker is not None and session.ticker != ticker:
                continue
            entry(session.ticker)["replays"].append(session_to_dict(session))

        return {"status": sorted(by_ticker.values(), key=lambda e: e["ticker"])}
