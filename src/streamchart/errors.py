"""Domain-level exceptions shared across the service."""


class StreamchartError(Exception):
    """Base class for all domain errors."""


class FetchError(StreamchartError):
    """Upstream (Yahoo) request or transport failure -> maps to HTTP 502."""


class NoDataError(FetchError):
    """No usable bars returned (e.g. non-trading day) -> maps to HTTP 422."""


class ValidationError(StreamchartError):
    """Invalid caller input -> maps to HTTP 422."""


class NotFoundError(StreamchartError):
    """Requested resource does not exist -> maps to HTTP 404."""


class ConflictError(StreamchartError):
    """Illegal state transition -> maps to HTTP 409."""


class KafkaProduceError(StreamchartError):
    """Kafka delivery failed for a produced message."""
