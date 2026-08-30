from __future__ import annotations

import pytest

from streamchart.errors import ConflictError, NotFoundError
from streamchart.repository.replays_repo import (
    claim_next_runnable,
    create_session,
    get_session,
    is_cancelled,
    mark_completed,
    request_cancel,
    update_progress,
)


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


def test_request_cancel_not_found(db) -> None:
    with pytest.raises(NotFoundError):
        request_cancel("00000000-0000-0000-0000-000000000000")
