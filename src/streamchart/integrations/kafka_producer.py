from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from streamchart.config import settings
from streamchart.domain.bars import Bar
from streamchart.domain.replay import ReplaySession
from streamchart.errors import KafkaProduceError
from streamchart.timeutil import iso_utc

SCHEMA_VERSION = 1


@dataclass
class DeliveryResult:
    partition: int
    offset: int


class _Producer(Protocol):
    def produce(
        self,
        topic: str,
        *,
        key: bytes,
        value: bytes,
        on_delivery: Any,
    ) -> None: ...

    def flush(self, timeout: float = ...) -> int: ...


def message_key(ticker: str) -> bytes:
    return ticker.encode("utf-8")


def build_payload(
    session: ReplaySession,
    sequence: int,
    bar: Bar,
    *,
    is_first: bool,
    is_last: bool,
    emitted_at: datetime,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "session_id": session.id,
        "ticker": session.ticker,
        "sequence": sequence,
        "interval": session.interval,
        "bar_time": iso_utc(bar.bar_time),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "emitted_at": iso_utc(emitted_at),
        "is_first": is_first,
        "is_last": is_last,
    }


def serialize(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


class SliceProducer:
    """Publishes replay slices to Kafka, one message per slice, in order."""

    def __init__(self, producer: _Producer, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    def produce_slice(
        self,
        session: ReplaySession,
        sequence: int,
        bar: Bar,
        *,
        is_first: bool,
        is_last: bool,
        emitted_at: datetime,
    ) -> DeliveryResult:
        payload = build_payload(
            session,
            sequence,
            bar,
            is_first=is_first,
            is_last=is_last,
            emitted_at=emitted_at,
        )
        holder: dict[str, Any] = {}

        def _on_delivery(err: Any, msg: Any) -> None:
            if err is not None:
                holder["error"] = err
            else:
                holder["partition"] = msg.partition()
                holder["offset"] = msg.offset()

        self._producer.produce(
            self._topic,
            key=message_key(session.ticker),
            value=serialize(payload),
            on_delivery=_on_delivery,
        )
        self._producer.flush()

        if "error" in holder:
            raise KafkaProduceError(str(holder["error"]))
        return DeliveryResult(
            partition=int(holder.get("partition", 0)),
            offset=int(holder.get("offset", -1)),
        )


def create_producer() -> Any:  # pragma: no cover - requires a live broker
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": settings.kafka_client_id,
            "acks": settings.kafka_acks,
            "enable.idempotence": True,
        }
    )


def check_broker() -> tuple[bool, str]:  # pragma: no cover - requires a live broker
    try:
        from confluent_kafka import Producer
    except Exception as exc:
        return False, f"kafka client unavailable: {type(exc).__name__}"
    try:
        producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "client.id": settings.kafka_client_id,
            }
        )
        producer.list_topics(timeout=3.0)
        return True, "ok"
    except Exception as exc:
        return False, f"kafka check failed: {type(exc).__name__}"
