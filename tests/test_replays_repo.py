from __future__ import annotations

import pytest
from sqlalchemy import func, select

from streamchart.db import get_engine
from streamchart.errors import ConflictError, NotFoundError
from streamchart.models import replay_events
from streamchart.repository.replays_repo import (
    claim_next_runnable,
    create_session,
    get_session,
    is_cancelled,
    mark_completed,
    record_event,
    request_cancel,
    update_progress,
)
from streamchart.timeutil import utcnow


def _create():
    return create_session(
        ticker="MSFT",
        interval="1m",
        replay_interval_seconds=1.0,
        kafka_topic="market.replay.bars",
        total_slices=3,
    )


def test_create_and_get(db) -> None:
    session = _create()
    got = get_session(session.id)
    assert got is not None
    assert got.status == "pending"
    assert got.total_slices == 3
    assert got.last_sequence == -1


def test_claim_progress_and_complete(db) -> None:
    session = _create()
    claimed = claim_next_runnable()
    assert claimed is not None
    assert claimed.status == "running"

    update_progress(session.id, emitted=3, last_sequence=2)
    mark_completed(session.id)

    got = get_session(session.id)
    assert got is not None
    assert got.status == "completed"
    assert got.emitted_slices == 3
    assert claim_next_runnable() is None


def test_cancel_flow(db) -> None:
    session = _create()
    assert is_cancelled(session.id) is False

    cancelled = request_cancel(session.id)
    assert cancelled.status == "cancelled"
    assert is_cancelled(session.id) is True

    with pytest.raises(ConflictError):
        request_cancel(session.id)


def test_record_event_is_idempotent(db) -> None:
    session = _create()
    record_event(session.id, 0, utcnow(), utcnow(), 0, 0)
    record_event(session.id, 0, utcnow(), utcnow(), 0, 5)

    with get_engine().connect() as conn:
        count = conn.execute(
            select(func.count())
            .select_from(replay_events)
            .where(replay_events.c.session_id == session.id)
        ).scalar()
    assert count == 1


def test_request_cancel_not_found(db) -> None:
    with pytest.raises(NotFoundError):
        request_cancel("00000000-0000-0000-0000-000000000000")
