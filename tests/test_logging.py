from __future__ import annotations

import logging

from webtest import TestApp

from streamchart.logging import configure_logging, get_logger


def test_request_is_logged(app: TestApp, caplog) -> None:
    with caplog.at_level(logging.INFO, logger="streamchart.api"):
        app.get("/health")
    messages = [record.getMessage() for record in caplog.records]
    assert any("/health" in msg and "status=200" in msg for msg in messages)


def test_configure_logging_and_get_logger() -> None:
    configure_logging("INFO")
    logger = get_logger("streamchart.test")
    assert logger.name == "streamchart.test"
