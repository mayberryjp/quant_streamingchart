from __future__ import annotations

from typing import Any

from bottle import Bottle, request

from streamchart.config import settings
from streamchart.domain.bars import bar_to_dict, resample
from streamchart.errors import ValidationError
from streamchart.repository.bars_repo import get_bars


def register_bars_routes(app: Bottle) -> None:
    @app.get("/api/v1/bars")
    def bars() -> dict[str, Any]:
        ticker = request.query.get("ticker")
        if not ticker:
            raise ValidationError("query param 'ticker' is required")
        interval = request.query.get("interval") or settings.base_interval

        base = get_bars(ticker.upper(), settings.base_interval)
        slices = base if interval == settings.base_interval else resample(base, interval)
        return {
            "ticker": ticker.upper(),
            "interval": interval,
            "count": len(slices),
            "bars": [bar_to_dict(b) for b in slices],
        }
