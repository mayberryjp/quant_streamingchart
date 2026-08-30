"""Scheduled watchlist ingest.

When enabled, fires once per day at a configured local time. On each fire it
reads the latest active watchlist from the sticky-note service, then for each
ticker calls this service's fetch API (grab + store minute bars) followed by the
replay API (schedule a replay).
"""

from __future__ import annotations

import time
from datetime import date, datetime
from datetime import time as dtime
from zoneinfo import ZoneInfo

import httpx

from streamchart.config import settings
from streamchart.logging import configure_logging, get_logger

log = get_logger("streamchart.worker.scheduler")

WATCHLIST_PATH = "/sticky-notes/latest"


def parse_hhmm(value: str) -> dtime:
    """Parse a 'HH:MM' string into a time."""
    hour_text, minute_text = value.strip().split(":")
    return dtime(hour=int(hour_text), minute=int(minute_text))


def is_trigger_due(now_local: datetime, trigger: dtime, last_run_date: date | None) -> bool:
    """True when the local clock has reached the trigger and it has not run today.

    Never fires on weekends (Saturday/Sunday) in local time.
    """
    if now_local.weekday() >= 5:
        return False
    if last_run_date == now_local.date():
        return False
    return (now_local.hour, now_local.minute) >= (trigger.hour, trigger.minute)


def fetch_watchlist(client: httpx.Client, stickynote_base_url: str) -> tuple[str | None, list[str]]:
    """Return (signal_date, unique tickers) from the sticky-note latest endpoint."""
    response = client.get(f"{stickynote_base_url.rstrip('/')}{WATCHLIST_PATH}")
    response.raise_for_status()
    data = response.json()
    signal_date = data.get("signal_date")
    tickers: list[str] = []
    seen: set[str] = set()
    for note in data.get("results") or []:
        symbol = str(note.get("symbol") or "").upper()
        if symbol and symbol not in seen:
            seen.add(symbol)
            tickers.append(symbol)
    return signal_date, tickers


def wait_for_api(client: httpx.Client, api_base_url: str, timeout_seconds: float) -> None:
    """Block until the local API answers /health, so we don't race it on startup."""
    base = api_base_url.rstrip("/")
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            client.get(f"{base}/health").raise_for_status()
            return
        except httpx.HTTPError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1.0)


def trigger_ticker(
    client: httpx.Client,
    api_base_url: str,
    ticker: str,
    *,
    interval: str,
    replay_interval_seconds: float,
) -> None:
    """Fetch+store bars for one ticker, then schedule its replay."""
    base = api_base_url.rstrip("/")
    fetch_response = client.post(
        f"{base}/api/v1/fetch",
        json={"ticker": ticker, "interval": interval},
    )
    fetch_response.raise_for_status()
    replay_response = client.post(
        f"{base}/api/v1/replays",
        json={
            "ticker": ticker,
            "interval": interval,
            "replay_interval_seconds": replay_interval_seconds,
        },
    )
    replay_response.raise_for_status()


def run_job(
    client: httpx.Client,
    *,
    stickynote_base_url: str,
    api_base_url: str,
    interval: str,
    replay_interval_seconds: float,
) -> int:
    """Run one scheduled ingest cycle. Returns the number of tickers processed."""
    signal_date, tickers = fetch_watchlist(client, stickynote_base_url)
    log.info("scheduler run signal_date=%s tickers=%d", signal_date, len(tickers))
    processed = 0
    for ticker in tickers:
        try:
            trigger_ticker(
                client,
                api_base_url,
                ticker,
                interval=interval,
                replay_interval_seconds=replay_interval_seconds,
            )
            processed += 1
        except Exception:
            log.exception("scheduler failed to process %s", ticker)
    return processed


def main() -> None:  # pragma: no cover - long-running loop wiring
    configure_logging(settings.log_level)
    if not settings.scheduler_enabled:
        log.info("scheduler disabled (SERVICE_SCHEDULER_ENABLED=false); idling")
        while True:
            time.sleep(3600)

    tz = ZoneInfo(settings.scheduler_timezone)
    trigger = parse_hhmm(settings.scheduler_trigger_time)
    last_run: date | None = None
    log.info(
        "scheduler started trigger=%s tz=%s",
        settings.scheduler_trigger_time,
        settings.scheduler_timezone,
    )
    while True:
        now_local = datetime.now(tz)
        if is_trigger_due(now_local, trigger, last_run):
            try:
                with httpx.Client(timeout=settings.scheduler_http_timeout_seconds) as client:
                    wait_for_api(
                        client,
                        settings.internal_api_base_url,
                        settings.scheduler_http_timeout_seconds,
                    )
                    run_job(
                        client,
                        stickynote_base_url=settings.stickynote_base_url,
                        api_base_url=settings.internal_api_base_url,
                        interval=settings.base_interval,
                        replay_interval_seconds=settings.replay_interval_seconds,
                    )
            except Exception:
                log.exception("scheduler job failed")
            last_run = now_local.date()
        time.sleep(settings.scheduler_check_interval_seconds)


if __name__ == "__main__":  # pragma: no cover
    main()
