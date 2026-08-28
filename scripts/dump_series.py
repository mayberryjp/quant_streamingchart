"""Standalone: fetch a ticker's intraday chart from Yahoo and print time/price.

No venv, no dependencies — uses only the Python standard library.

Usage:
    python scripts/dump_series.py [TICKER] [INTERVAL] [RANGE]
    python scripts/dump_series.py MSFT 1m 1d
"""

from __future__ import annotations

import json
import sys
import urllib.parse
import urllib.request
from datetime import UTC, datetime

BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
USER_AGENT = "quant-streamingchart/0.1 (+contact: /u/homelabids)"


def fetch(ticker: str, interval: str, range_: str) -> dict:
    query = urllib.parse.urlencode({"interval": interval, "range": range_})
    url = f"{BASE_URL}/{ticker.upper()}?{query}"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310
        return json.load(response)


def main() -> None:
    ticker = sys.argv[1] if len(sys.argv) > 1 else "MSFT"
    interval = sys.argv[2] if len(sys.argv) > 2 else "1m"
    range_ = sys.argv[3] if len(sys.argv) > 3 else "1d"

    payload = fetch(ticker, interval, range_)
    result = payload["chart"]["result"][0]
    timestamps = result.get("timestamp") or []
    closes = result["indicators"]["quote"][0].get("close") or []

    print(f"{ticker.upper()} {interval} {range_} — {len(timestamps)} bars")
    for ts, close in zip(timestamps, closes):
        if close is None:
            continue
        when = datetime.fromtimestamp(int(ts), tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"{when}\t{close}")


if __name__ == "__main__":
    main()
