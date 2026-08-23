from __future__ import annotations

from typing import Any

from bottle import Bottle, request

from streamchart.config import settings
from streamchart.integrations.yahoo import fetch_intraday
from streamchart.repository.bars_repo import upsert_bars
from streamchart.timeutil import iso_utc


def register_fetch_routes(app: Bottle) -> None:
    @app.post("/api/v1/fetch")
    def fetch() -> dict[str, Any]:
        body = request.json or {}
        ticker = str(body.get("ticker") or settings.default_ticker).upper()
        interval = str(body.get("interval") or settings.base_interval)
        range_ = str(body.get("range") or settings.source_range)

        bars = fetch_intraday(ticker, interval, range_)
        count = upsert_bars(bars)
        return {
            "ticker": ticker,
            "interval": interval,
            "count": count,
            "first_bar": iso_utc(bars[0].bar_time),
            "last_bar": iso_utc(bars[-1].bar_time),
        }
