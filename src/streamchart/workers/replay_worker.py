from __future__ import annotations

import os
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any, Protocol

from sqlalchemy import inspect

from streamchart.config import settings
from streamchart.db import get_engine
from streamchart.domain.bars import Bar, resample
from streamchart.domain.replay import ReplaySession
from streamchart.integrations.kafka_producer import DeliveryResult, SliceProducer, create_producer
from streamchart.logging import configure_logging, get_logger
from streamchart.timeutil import utcnow

log = get_logger("streamchart.worker.replay")


class SessionsRepo(Protocol):
    def claim_next_runnable(self) -> ReplaySession | None: ...
    def is_cancelled(self, session_id: str) -> bool: ...
    def update_progress(self, session_id: str, *, emitted: int, last_sequence: int) -> None: ...
    def record_event(
        self,
        session_id: str,
        sequence: int,
        bar_time: datetime,
        emitted_at: datetime,
        partition: int,
        offset: int,
    ) -> None: ...
    def mark_completed(self, session_id: str) -> None: ...
    def mark_failed(self, session_id: str, error: str) -> None: ...


class BarsRepo(Protocol):
    def get_bars(self, ticker: str, interval: str) -> list[Bar]: ...


class Producer(Protocol):
    def produce_slice(
        self,
        session: ReplaySession,
        sequence: int,
        bar: Bar,
        *,
        is_first: bool,
        is_last: bool,
        emitted_at: datetime,
    ) -> DeliveryResult: ...


def build_slices(bars_repo: BarsRepo, session: ReplaySession, base_interval: str) -> list[Bar]:
    bars = bars_repo.get_bars(session.ticker, base_interval)
    return resample(bars, session.interval)


def process_next(
    sessions_repo: SessionsRepo,
    bars_repo: BarsRepo,
    producer: Producer,
    *,
    base_interval: str,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], datetime] = utcnow,
) -> bool:
    """Claim one runnable session and stream its remaining slices. Returns True if a
    session was processed, False if none was available."""
    session = sessions_repo.claim_next_runnable()
    if session is None:
        return False
    stream_session(
        sessions_repo, bars_repo, producer, session, base_interval=base_interval, sleep=sleep, now=now
    )
    return True


def stream_session(
    sessions_repo: SessionsRepo,
    bars_repo: BarsRepo,
    producer: Producer,
    session: ReplaySession,
    *,
    base_interval: str,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], datetime] = utcnow,
) -> None:
    """Stream the remaining slices of an already-claimed (running) session."""
    try:
        slices = build_slices(bars_repo, session, base_interval)
        total = len(slices)
        start_sequence = session.last_sequence + 1
        log.info(
            "replay started session=%s ticker=%s interval=%s slices=%s from=%s",
            session.id,
            session.ticker,
            session.interval,
            total,
            start_sequence,
        )
        for sequence in range(start_sequence, total):
            if sessions_repo.is_cancelled(session.id):
                log.info("replay cancelled session=%s at sequence=%s", session.id, sequence)
                return
            bar = slices[sequence]
            emitted_at = now()
            result = producer.produce_slice(
                session,
                sequence,
                bar,
                is_first=sequence == 0,
                is_last=sequence == total - 1,
                emitted_at=emitted_at,
            )
            sessions_repo.record_event(
                session.id,
                sequence,
                bar.bar_time,
                emitted_at,
                result.partition,
                result.offset,
            )
            sessions_repo.update_progress(session.id, emitted=sequence + 1, last_sequence=sequence)
            log.info(
                "replay emit session=%s ticker=%s seq=%s/%s bar_time=%s partition=%s offset=%s",
                session.id,
                session.ticker,
                sequence + 1,
                total,
                bar.bar_time.isoformat(),
                result.partition,
                result.offset,
            )
            if sequence < total - 1:
                sleep(session.replay_interval_seconds)
        if not sessions_repo.is_cancelled(session.id):
            sessions_repo.mark_completed(session.id)
            log.info(
                "replay completed session=%s ticker=%s emitted=%s",
                session.id,
                session.ticker,
                total,
            )
    except Exception as exc:
        log.exception("replay failed for session %s", session.id)
        sessions_repo.mark_failed(session.id, f"{type(exc).__name__}: {exc}")


REQUIRED_TABLE = "replay_sessions"


def wait_for_schema(*, attempts: int = 60, delay: float = 2.0) -> bool:
    """Block until migrations have created the required tables.

    Returns True once the schema is present, or False if it gives up after
    ``attempts`` tries (the caller then relies on per-iteration retries).
    """
    for attempt in range(1, attempts + 1):
        try:
            if inspect(get_engine()).has_table(REQUIRED_TABLE):
                return True
            log.info(
                "waiting for schema: table %r not found (attempt %s/%s)",
                REQUIRED_TABLE,
                attempt,
                attempts,
            )
        except Exception as exc:
            log.info(
                "waiting for database: %s (attempt %s/%s)",
                type(exc).__name__,
                attempt,
                attempts,
            )
        time.sleep(delay)
    return False


def _run_session_child(session_id: str) -> int:  # pragma: no cover - runs in forked child
    """Forked child entrypoint: stream one already-claimed session, then exit."""
    from streamchart.repository import bars_repo, replays_repo

    # Drop connections inherited across fork; open fresh ones in this process.
    get_engine().dispose()
    try:
        producer = SliceProducer(create_producer(), settings.kafka_topic)
        session = replays_repo.get_session(session_id)
        if session is None:
            return 0
        stream_session(
            replays_repo, bars_repo, producer, session, base_interval=settings.base_interval
        )
    except Exception:
        log.exception("replay child crashed session=%s", session_id)
        return 1
    return 0


def _reap_children(active: dict[str, int]) -> None:  # pragma: no cover - process wiring
    """Remove finished children from the active map (non-blocking)."""
    for session_id, pid in list(active.items()):
        try:
            reaped, _status = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            active.pop(session_id, None)
            continue
        if reaped == 0:
            continue
        active.pop(session_id, None)
        log.info("replay child finished session=%s pid=%s", session_id, pid)


def main() -> None:  # pragma: no cover - long-running loop wiring
    from streamchart.repository import replays_repo

    configure_logging(settings.log_level)
    if not wait_for_schema():
        log.warning("schema not confirmed; iterations will retry until migrations apply")
    log.info(
        "replay_worker started topic=%s poll=%ss",
        settings.kafka_topic,
        settings.replay_worker_poll_seconds,
    )

    active: dict[str, int] = {}
    while True:
        _reap_children(active)
        try:
            pending = replays_repo.list_pending()
        except Exception:
            log.exception("failed to list pending replays")
            pending = []
        if pending:
            log.info("found %s pending replay(s); active=%s", len(pending), len(active))
        for session in pending:
            if session.id in active:
                continue
            claimed = replays_repo.claim_session(session.id)
            if claimed is None:
                continue
            pid = os.fork()
            if pid == 0:
                os._exit(_run_session_child(claimed.id))
            active[claimed.id] = pid
            log.info(
                "replay forked session=%s ticker=%s pid=%s",
                claimed.id,
                claimed.ticker,
                pid,
            )
        time.sleep(settings.replay_worker_poll_seconds)


if __name__ == "__main__":  # pragma: no cover
    main()
