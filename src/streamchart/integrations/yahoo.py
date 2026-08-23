from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx

from streamchart.config import settings
from streamchart.domain.bars import Bar
from streamchart.errors import FetchError, NoDataError


def _at(seq: list[Any] | None, index: int) -> Any:
    if not seq or index >= len(seq):
        return None
    return seq[index]


def parse_chart(payload: dict[str, Any], *, ticker: str, interval: str) -> list[Bar]:
    """Convert a Yahoo chart API payload into an ordered list of Bars."""
    chart = payload.get("chart") or {}
    error = chart.get("error")
    if error:
        code = error.get("code") if isinstance(error, dict) else error
        raise FetchError(f"yahoo returned an error: {code}")

    results = chart.get("result") or []
    if not results:
        raise NoDataError(f"no chart data for {ticker}")

    result = results[0]
    timestamps = result.get("timestamp") or []
    quote_list = ((result.get("indicators") or {}).get("quote")) or [{}]
    quote = quote_list[0] if quote_list else {}
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []

    bars: list[Bar] = []
    for index, ts in enumerate(timestamps):
        o = _at(opens, index)
        h = _at(highs, index)
        low = _at(lows, index)
        c = _at(closes, index)
        if o is None or h is None or low is None or c is None:
            continue
        volume = _at(volumes, index) or 0
        bars.append(
            Bar(
                ticker=ticker.upper(),
                interval=interval,
                bar_time=datetime.fromtimestamp(int(ts), tz=UTC),
                open=float(o),
                high=float(h),
                low=float(low),
                close=float(c),
                volume=int(volume),
                source="yahoo",
            )
        )

    if not bars:
        raise NoDataError(f"no usable bars for {ticker}")
    return bars


def fetch_intraday(
    ticker: str,
    interval: str,
    range_: str,
    *,
    client: httpx.Client | None = None,
) -> list[Bar]:
    """Fetch a single day of intraday bars for a ticker from Yahoo Finance."""
    url = f"{settings.yf_base_url}/{ticker.upper()}"
    params = {"interval": interval, "range": range_}
    headers = {"User-Agent": settings.yf_user_agent}

    owns_client = client is None
    http = client or httpx.Client(timeout=settings.yf_timeout_seconds)
    try:
        response = http.get(url, params=params, headers=headers)
        response.raise_for_status()
        payload: dict[str, Any] = response.json()
    except httpx.HTTPError as exc:
        raise FetchError(f"yahoo request failed: {type(exc).__name__}") from exc
    finally:
        if owns_client:
            http.close()

    return parse_chart(payload, ticker=ticker, interval=interval)
