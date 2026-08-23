from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from tests.conftest import make_bar

from streamchart.domain.replay import ReplaySession
from streamchart.integrations.kafka_producer import (
    DeliveryResult,
    SliceProducer,
    build_payload,
    message_key,
    serialize,
)

_EMITTED = datetime(2026, 8, 23, 10, 0, 42, tzinfo=UTC)


def _session() -> ReplaySession:
    return ReplaySession(
        id="sess-1",
        ticker="MSFT",
        interval="1m",
        replay_interval_seconds=1.0,
        kafka_topic="market.replay.bars",
        status="running",
        total_slices=3,
        emitted_slices=0,
        last_sequence=-1,
        created_at=_EMITTED,
    )


class _FakeMsg:
    def __init__(self, partition: int, offset: int) -> None:
        self._partition = partition
        self._offset = offset

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class _FakeProducer:
    def __init__(self) -> None:
        self.produced: list[tuple[str, bytes, bytes]] = []

    def produce(self, topic: str, *, key: bytes, value: bytes, on_delivery: Any) -> None:
        self.produced.append((topic, key, value))
        on_delivery(None, _FakeMsg(0, len(self.produced) - 1))

    def flush(self, timeout: float = 1.0) -> int:
        return 0


def test_message_key() -> None:
    assert message_key("MSFT") == b"MSFT"


def test_build_payload_contract() -> None:
    bar = make_bar(0, 415.2)
    payload = build_payload(_session(), 0, bar, is_first=True, is_last=False, emitted_at=_EMITTED)
    assert payload["schema_version"] == 1
    assert payload["ticker"] == "MSFT"
    assert payload["sequence"] == 0
    assert payload["is_first"] is True
    assert payload["bar_time"] == "2026-08-21T14:30:00Z"
    assert payload["emitted_at"] == "2026-08-23T10:00:42Z"


def test_serialize_roundtrip() -> None:
    import json

    payload = build_payload(_session(), 1, make_bar(1, 1.0), is_first=False, is_last=True,
                            emitted_at=_EMITTED)
    assert json.loads(serialize(payload))["sequence"] == 1


def test_slice_producer_returns_delivery_result() -> None:
    fake = _FakeProducer()
    producer = SliceProducer(fake, "market.replay.bars")
    result = producer.produce_slice(
        _session(), 0, make_bar(0, 100.0), is_first=True, is_last=False, emitted_at=_EMITTED
    )
    assert isinstance(result, DeliveryResult)
    assert result.partition == 0
    assert result.offset == 0
    assert fake.produced[0][0] == "market.replay.bars"
    assert fake.produced[0][1] == b"MSFT"
