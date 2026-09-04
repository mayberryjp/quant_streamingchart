from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "postgresql+psycopg://app:app@localhost:5432/app")
os.environ.setdefault("SERVICE_KAFKA_BOOTSTRAP_SERVERS", "")

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, text
from webtest import TestApp

from streamchart.api.app import create_app
from streamchart.config import settings
from streamchart.db import get_engine
from streamchart.domain.bars import Bar
from streamchart.models import SCHEMA_NAME, metadata


@pytest.fixture
def app() -> TestApp:
    return TestApp(create_app())


def _database_available() -> bool:
    """Probe the configured database with a short timeout so tests skip fast."""
    probe = create_engine(settings.database_url, connect_args={"connect_timeout": 2})
    try:
        with probe.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
    finally:
        probe.dispose()


@pytest.fixture
def db():
    if not _database_available():
        pytest.skip("database not available")
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_NAME}"'))
    metadata.drop_all(engine)
    metadata.create_all(engine)
    yield engine
    metadata.drop_all(engine)


def make_bar(minute: int, price: float, volume: int = 100, interval: str = "1m") -> Bar:
    """Build a 1-minute bar at 2026-08-21 14:30:00Z + `minute` minutes."""
    base = datetime(2026, 8, 21, 14, 30, tzinfo=UTC)
    ts = base.timestamp() + minute * 60
    bar_time = datetime.fromtimestamp(ts, tz=UTC)
    return Bar(
        ticker="MSFT",
        interval=interval,
        bar_time=bar_time,
        open=price,
        high=price + 1.0,
        low=price - 1.0,
        close=price + 0.5,
        volume=volume,
        source="yahoo",
    )
