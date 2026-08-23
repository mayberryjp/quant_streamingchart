from __future__ import annotations

import pytest
from tests.conftest import _database_available

from streamchart.db import check_database


def test_check_database_ok() -> None:
    if not _database_available():
        pytest.skip("database not available")
    ok, detail = check_database()
    assert ok is True
    assert detail == "ok"
