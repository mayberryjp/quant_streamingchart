from __future__ import annotations

from datetime import UTC, datetime

from tests.conftest import make_bar

from streamchart.domain.replay import ReplaySession
from streamchart.integrations.kafka_producer import DeliveryResult
from streamchart.workers.replay_worker import process_next

_NOW = datetime(2026, 8, 23, 10, 0, 0, tzinfo=UTC)


def _session(status: str = "pending", last_sequence: int = -1, emitted: int = 0) -> ReplaySession:
    return ReplaySession(
        id="s-1",
        ticker="MSFT",
        interval="1m",
        replay_interval_seconds=1.0,
        kafka_topic="market.replay.bars",
        status=status,
        total_slices=3,
        emitted_slices=emitted,
        last_sequence=last_sequence,
        created_at=_NOW,
    )


class FakeSessions:
    def __init__(self, session: ReplaySession) -> None:
        self.session = session
        self.failed: str | None = None
        self._claimed = False

    def claim_next_runnable(self) -> ReplaySession | None:
        if self._claimed:
            return None
        self._claimed = True
        if self.session.status == "pending":
            self.session.status = "running"
        return self.session

    def mark_completed(self, session_id: str) -> None:
        self.session.status = "completed"

    def mark_failed(self, session_id: str, error: str) -> None:
        self.session.status = "failed"
        self.failed = error


class FakeBars:
    def __init__(self, count: int = 3) -> None:
        self._bars = [make_bar(i, 100.0 + i) for i in range(count)]

    def get_bars(self, ticker: str, interval: str, day=None):
        return self._bars

    def get_latest_day(self, ticker: str, interval: str):
        return None


class FakeProducer:
    def __init__(self, fail_on: int | None = None) -> None:
        self.calls: list[int] = []
        self.fail_on = fail_on

    def produce_slice(self, session, sequence, bar, *, is_first, is_last, emitted_at):
        if self.fail_on is not None and sequence == self.fail_on:
            raise RuntimeError("kafka down")
        self.calls.append(sequence)
        return DeliveryResult(partition=0, offset=sequence)


def _run(sessions: FakeSessions, bars: FakeBars, producer: FakeProducer) -> bool:
    return process_next(
        sessions, bars, producer, base_interval="1m", sleep=lambda _s: None, now=lambda: _NOW
    )


def test_full_run_emits_all_and_completes() -> None:
    sessions = FakeSessions(_session())
    producer = FakeProducer()
    assert _run(sessions, FakeBars(), producer) is True
    assert producer.calls == [0, 1, 2]
    assert sessions.session.status == "completed"


def test_no_session_returns_false() -> None:
    sessions = FakeSessions(_session())
    sessions._claimed = True  # nothing to claim
    assert _run(sessions, FakeBars(), FakeProducer()) is False


def test_resume_from_last_sequence() -> None:
    sessions = FakeSessions(_session(status="running", last_sequence=0, emitted=1))
    producer = FakeProducer()
    assert _run(sessions, FakeBars(), producer) is True
    assert producer.calls == [1, 2]
    assert sessions.session.status == "completed"


def test_failure_marks_failed() -> None:
    sessions = FakeSessions(_session())
    producer = FakeProducer(fail_on=1)
    assert _run(sessions, FakeBars(), producer) is True
    assert producer.calls == [0]
    assert sessions.session.status == "failed"
    assert sessions.failed is not None
    assert "RuntimeError" in sessions.failed


class _Inspector:
    def __init__(self, present: bool) -> None:
        self._present = present

    def has_table(self, name: str, schema: str | None = None) -> bool:
        return self._present


def test_wait_for_schema_present(monkeypatch) -> None:
    import streamchart.workers.replay_worker as worker

    monkeypatch.setattr(worker, "get_engine", lambda: object())
    monkeypatch.setattr(worker, "inspect", lambda _engine: _Inspector(True))
    assert worker.wait_for_schema(attempts=1, delay=0.0) is True


def test_wait_for_schema_absent_gives_up(monkeypatch) -> None:
    import streamchart.workers.replay_worker as worker

    monkeypatch.setattr(worker, "get_engine", lambda: object())
    monkeypatch.setattr(worker, "inspect", lambda _engine: _Inspector(False))
    assert worker.wait_for_schema(attempts=2, delay=0.0) is False


def test_wait_for_schema_handles_db_error(monkeypatch) -> None:
    import streamchart.workers.replay_worker as worker

    def _boom(_engine: object) -> object:
        raise RuntimeError("db down")

    monkeypatch.setattr(worker, "get_engine", lambda: object())
    monkeypatch.setattr(worker, "inspect", _boom)
    assert worker.wait_for_schema(attempts=1, delay=0.0) is False
