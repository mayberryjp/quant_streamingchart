from __future__ import annotations

import os
import time
from collections.abc import Callable
from datetime import datetime
from typing import Protocol

from sqlalchemy import inspect

from streamchart.config import settings
from streamchart.db import get_engine
from streamchart.domain.bars import Bar, resample
from streamchart.domain.replay import PENDING, ReplaySession
from streamchart.integrations.kafka_producer import DeliveryResult, SliceProducer, create_producer
from streamchart.logging import configure_logging, get_logger
from streamchart.timeutil import utcnow

log = get_logger("streamchart.worker.replay")


class SessionsRepo(Protocol):
    def claim_next_runnable(self) -> ReplaySession | None: ...
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
    """Claim one runnable session, load its bars, and stream them. Returns True if a
    session was processed, False if none was available. The claim and the final
    status write touch the DB; streaming itself does not."""
    session = sessions_repo.claim_next_runnable()
    if session is None:
        return False
    try:
        slices = build_slices(bars_repo, session, base_interval)
        stream_session(producer, session, slices, sleep=sleep, now=now)
    except Exception as exc:
        log.exception("replay failed for session %s", session.id)
        sessions_repo.mark_failed(session.id, f"{type(exc).__name__}: {exc}")
        return True
    sessions_repo.mark_completed(session.id)
    log.info(
        "replay completed session=%s ticker=%s emitted=%s",
        session.id,
        session.ticker,
        len(slices),
    )
    return True


def stream_session(
    producer: Producer,
    session: ReplaySession,
    slices: list[Bar],
    *,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], datetime] = utcnow,
) -> None:
    """Produce a session's remaining slices to Kafka. Zero database access: the
    caller preloads the bars and records the final status."""
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


def _run_session_child(
    session: ReplaySession, slices: list[Bar]
) -> int:  # pragma: no cover - runs in forked child
    """Forked child entrypoint: stream preloaded slices to Kafka, then exit.

    The child performs NO database access; the parent claimed the session, loaded
    the bars, and records the final status when it reaps this process.
    """
    # Detach from the pool inherited across fork so exiting never closes the
    # parent's sockets. The child itself opens no database connections.
    get_engine().dispose(close=False)
    try:
        producer = SliceProducer(create_producer(), settings.kafka_topic)
        stream_session(producer, session, slices)
    except Exception:
        log.exception("replay child crashed session=%s", session.id)
        return 1
    return 0


def _reap_children(
    active: dict[str, int], sessions_repo: SessionsRepo
) -> None:  # pragma: no cover - process wiring
    """Reap finished children and record their final status in the DB."""
    for session_id, pid in list(active.items()):
        try:
            reaped, status = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            active.pop(session_id, None)
            continue
        if reaped == 0:
            continue
        active.pop(session_id, None)
        exit_code = os.waitstatus_to_exitcode(status)
        if exit_code == 0:
            sessions_repo.mark_completed(session_id)
            log.info("replay completed session=%s pid=%s", session_id, pid)
        else:
            sessions_repo.mark_failed(session_id, f"replay child exited {exit_code}")
            log.warning("replay failed session=%s pid=%s exit=%s", session_id, pid, exit_code)


def main() -> None:  # pragma: no cover - long-running loop wiring
    from streamchart.repository import bars_repo, replays_repo

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
        _reap_children(active, replays_repo)
        try:
            candidates = replays_repo.list_resumable()
        except Exception:
            log.exception("failed to list resumable replays")
            candidates = []
        if candidates:
            log.info("found %s resumable replay(s); active=%s", len(candidates), len(active))
        for session in candidates:
            if session.id in active:
                continue
            if session.status == PENDING:
                claimed = replays_repo.claim_session(session.id)
                if claimed is None:
                    continue
            else:
                # Orphaned RUNNING session from a previous worker; resume it.
                claimed = session
                log.info(
                    "resuming orphaned replay session=%s ticker=%s last_sequence=%s",
                    session.id,
                    session.ticker,
                    session.last_sequence,
                )
            try:
                slices = build_slices(bars_repo, claimed, settings.base_interval)
            except Exception:
                log.exception("failed to load bars for session %s", claimed.id)
                replays_repo.mark_failed(claimed.id, "failed to load bars")
                continue
            pid = os.fork()
            if pid == 0:
                os._exit(_run_session_child(claimed, slices))
            active[claimed.id] = pid
            log.info(
                "replay forked session=%s ticker=%s pid=%s slices=%s",
                claimed.id,
                claimed.ticker,
                pid,
                len(slices),
            )
        time.sleep(settings.replay_worker_poll_seconds)


if __name__ == "__main__":  # pragma: no cover
    main()
