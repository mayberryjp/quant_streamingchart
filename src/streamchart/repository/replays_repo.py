from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from streamchart.db import get_engine
from streamchart.domain.replay import (
    CANCELLED,
    COMPLETED,
    FAILED,
    PENDING,
    RUNNABLE,
    RUNNING,
    TERMINAL,
    ReplaySession,
)
from streamchart.errors import ConflictError, NotFoundError
from streamchart.models import replay_events, replay_sessions
from streamchart.timeutil import utcnow


def _row_to_session(row: Any) -> ReplaySession:
    return ReplaySession(
        id=str(row["id"]),
        ticker=row["ticker"],
        interval=row["interval"],
        replay_interval_seconds=float(row["replay_interval_seconds"]),
        kafka_topic=row["kafka_topic"],
        status=row["status"],
        total_slices=int(row["total_slices"]),
        emitted_slices=int(row["emitted_slices"]),
        last_sequence=int(row["last_sequence"]),
        created_at=row["created_at"],
        started_at=row["started_at"],
        completed_at=row["completed_at"],
        error=row["error"],
    )


def create_session(
    *,
    ticker: str,
    interval: str,
    replay_interval_seconds: float,
    kafka_topic: str,
    total_slices: int,
) -> ReplaySession:
    session_id = str(uuid4())
    now = utcnow()
    with get_engine().begin() as conn:
        conn.execute(
            replay_sessions.insert().values(
                id=session_id,
                ticker=ticker,
                interval=interval,
                replay_interval_seconds=replay_interval_seconds,
                kafka_topic=kafka_topic,
                status=PENDING,
                total_slices=total_slices,
                emitted_slices=0,
                last_sequence=-1,
                created_at=now,
            )
        )
    return ReplaySession(
        id=session_id,
        ticker=ticker,
        interval=interval,
        replay_interval_seconds=replay_interval_seconds,
        kafka_topic=kafka_topic,
        status=PENDING,
        total_slices=total_slices,
        emitted_slices=0,
        last_sequence=-1,
        created_at=now,
    )


def get_session(session_id: str) -> ReplaySession | None:
    stmt = select(replay_sessions).where(replay_sessions.c.id == session_id)
    with get_engine().connect() as conn:
        row = conn.execute(stmt).mappings().first()
    return _row_to_session(row) if row else None


def list_sessions() -> list[ReplaySession]:
    stmt = select(replay_sessions).order_by(replay_sessions.c.created_at.desc())
    with get_engine().connect() as conn:
        rows = conn.execute(stmt).mappings().all()
    return [_row_to_session(row) for row in rows]


def list_pending() -> list[ReplaySession]:
    stmt = (
        select(replay_sessions)
        .where(replay_sessions.c.status == PENDING)
        .order_by(replay_sessions.c.created_at.asc())
    )
    with get_engine().connect() as conn:
        rows = conn.execute(stmt).mappings().all()
    return [_row_to_session(row) for row in rows]


def list_resumable() -> list[ReplaySession]:
    """Pending sessions plus running sessions left orphaned by a prior worker."""
    stmt = (
        select(replay_sessions)
        .where(replay_sessions.c.status.in_([PENDING, RUNNING]))
        .order_by(replay_sessions.c.created_at.asc())
    )
    with get_engine().connect() as conn:
        rows = conn.execute(stmt).mappings().all()
    return [_row_to_session(row) for row in rows]


def claim_session(session_id: str) -> ReplaySession | None:
    """Atomically transition one pending session to running. Returns it, or None
    if it is not pending / already claimed by another process."""
    now = utcnow()
    with get_engine().begin() as conn:
        row = (
            conn.execute(
                select(replay_sessions)
                .where(replay_sessions.c.id == session_id)
                .where(replay_sessions.c.status == PENDING)
                .with_for_update(skip_locked=True)
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        conn.execute(
            update(replay_sessions)
            .where(replay_sessions.c.id == session_id)
            .values(status=RUNNING, started_at=now)
        )
        data: dict[str, Any] = dict(row)
        data["status"] = RUNNING
        data["started_at"] = now
    return _row_to_session(data)


def claim_next_runnable() -> ReplaySession | None:
    """Return the next pending/running session, transitioning pending -> running."""
    now = utcnow()
    with get_engine().begin() as conn:
        row = (
            conn.execute(
                select(replay_sessions)
                .where(replay_sessions.c.status.in_(list(RUNNABLE)))
                .order_by(replay_sessions.c.created_at.asc())
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        data: dict[str, Any] = dict(row)
        if data["status"] == PENDING:
            conn.execute(
                update(replay_sessions)
                .where(replay_sessions.c.id == data["id"])
                .values(status=RUNNING, started_at=now)
            )
            data["status"] = RUNNING
            data["started_at"] = now
    return _row_to_session(data)


def is_cancelled(session_id: str) -> bool:
    stmt = select(replay_sessions.c.status).where(replay_sessions.c.id == session_id)
    with get_engine().connect() as conn:
        row = conn.execute(stmt).first()
    return bool(row and row[0] == CANCELLED)


def update_progress(session_id: str, *, emitted: int, last_sequence: int) -> None:
    with get_engine().begin() as conn:
        conn.execute(
            update(replay_sessions)
            .where(replay_sessions.c.id == session_id)
            .values(emitted_slices=emitted, last_sequence=last_sequence)
        )


def record_event(
    session_id: str,
    sequence: int,
    bar_time: datetime,
    emitted_at: datetime,
    partition: int,
    offset: int,
) -> None:
    stmt = pg_insert(replay_events).values(
        session_id=session_id,
        sequence=sequence,
        bar_time=bar_time,
        emitted_at=emitted_at,
        kafka_partition=partition,
        kafka_offset=offset,
    )
    stmt = stmt.on_conflict_do_nothing(index_elements=["session_id", "sequence"])
    with get_engine().begin() as conn:
        conn.execute(stmt)


def mark_completed(session_id: str) -> None:
    with get_engine().begin() as conn:
        conn.execute(
            update(replay_sessions)
            .where(replay_sessions.c.id == session_id)
            .where(replay_sessions.c.status == RUNNING)
            .values(
                status=COMPLETED,
                completed_at=utcnow(),
                emitted_slices=replay_sessions.c.total_slices,
                last_sequence=replay_sessions.c.total_slices - 1,
            )
        )


def mark_failed(session_id: str, error: str) -> None:
    with get_engine().begin() as conn:
        conn.execute(
            update(replay_sessions)
            .where(replay_sessions.c.id == session_id)
            .where(replay_sessions.c.status == RUNNING)
            .values(status=FAILED, error=error, completed_at=utcnow())
        )


def request_cancel(session_id: str) -> ReplaySession:
    now = utcnow()
    with get_engine().begin() as conn:
        row = (
            conn.execute(
                select(replay_sessions)
                .where(replay_sessions.c.id == session_id)
                .with_for_update()
            )
            .mappings()
            .first()
        )
        if row is None:
            raise NotFoundError(f"replay {session_id} not found")
        if row["status"] in TERMINAL:
            raise ConflictError(f"cannot cancel session in status {row['status']}")
        conn.execute(
            update(replay_sessions)
            .where(replay_sessions.c.id == session_id)
            .values(status=CANCELLED, completed_at=now)
        )
        data: dict[str, Any] = dict(row)
        data["status"] = CANCELLED
        data["completed_at"] = now
    return _row_to_session(data)
