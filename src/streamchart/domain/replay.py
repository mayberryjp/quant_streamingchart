from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from streamchart.timeutil import iso_utc

PENDING = "pending"
RUNNING = "running"
COMPLETED = "completed"
FAILED = "failed"
CANCELLED = "cancelled"

TERMINAL = frozenset({COMPLETED, FAILED, CANCELLED})
RUNNABLE = frozenset({PENDING, RUNNING})


@dataclass
class ReplaySession:
    id: str
    ticker: str
    interval: str
    replay_interval_seconds: float
    kafka_topic: str
    status: str
    total_slices: int
    emitted_slices: int
    last_sequence: int
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None


def session_to_dict(session: ReplaySession) -> dict[str, Any]:
    percent = (
        (session.emitted_slices / session.total_slices * 100.0)
        if session.total_slices
        else 0.0
    )
    return {
        "id": session.id,
        "ticker": session.ticker,
        "interval": session.interval,
        "replay_interval_seconds": session.replay_interval_seconds,
        "kafka_topic": session.kafka_topic,
        "status": session.status,
        "total_slices": session.total_slices,
        "emitted_slices": session.emitted_slices,
        "last_sequence": session.last_sequence,
        "percent": round(percent, 2),
        "created_at": iso_utc(session.created_at),
        "started_at": iso_utc(session.started_at) if session.started_at else None,
        "completed_at": iso_utc(session.completed_at) if session.completed_at else None,
        "error": session.error,
    }
