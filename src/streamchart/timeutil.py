from datetime import UTC, datetime


def utcnow() -> datetime:
    return datetime.now(UTC)


def iso_utc(dt: datetime) -> str:
    """Render a datetime as an ISO-8601 UTC string with a trailing Z."""
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
