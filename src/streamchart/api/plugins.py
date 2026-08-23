from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from bottle import request, response

from streamchart.errors import (
    ConflictError,
    FetchError,
    NoDataError,
    NotFoundError,
    ValidationError,
)


def error_envelope(
    status: int,
    code: str,
    message: str,
    detail: str | None = None,
) -> dict[str, Any]:
    response.status = status
    response.content_type = "application/json"
    body: dict[str, Any] = {"status": "error", "code": code, "error": message}
    if detail:
        body["detail"] = detail
    return body


class AppPlugin:
    """Bottle plugin: maps domain errors to JSON envelopes and logs each request."""

    name = "app"
    api = 2

    def __init__(self, logger: logging.Logger) -> None:
        self._log = logger

    def apply(self, callback: Callable[..., Any], context: Any) -> Callable[..., Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.perf_counter()
            try:
                return callback(*args, **kwargs)
            except NoDataError as exc:
                return error_envelope(422, "no_data", str(exc))
            except ValidationError as exc:
                return error_envelope(422, "validation_error", str(exc))
            except NotFoundError as exc:
                return error_envelope(404, "not_found", str(exc))
            except ConflictError as exc:
                return error_envelope(409, "conflict", str(exc))
            except FetchError as exc:
                return error_envelope(502, "fetch_failed", str(exc))
            except Exception:
                self._log.exception("unhandled error")
                return error_envelope(500, "internal_error", "internal error")
            finally:
                duration_ms = (time.perf_counter() - start) * 1000.0
                self._log.info(
                    "%s %s status=%s duration_ms=%.2f request_id=%s",
                    request.method,
                    request.path,
                    response.status_code,
                    duration_ms,
                    request.get_header("X-Request-ID") or "-",
                )

        return wrapper
