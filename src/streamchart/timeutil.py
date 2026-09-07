import os
from datetime import datetime
from zoneinfo import ZoneInfo

# The container's local timezone. All timestamps are generated and rendered in
# this zone, and database sessions are pinned to it.
LOCAL_TZ_NAME = os.environ.get("TZ", "America/New_York")
LOCAL_TZ = ZoneInfo(LOCAL_TZ_NAME)


def localnow() -> datetime:
    """Current time in the container's local timezone."""
    return datetime.now(LOCAL_TZ)


def iso_local(dt: datetime) -> str:
    """Render a datetime in the container's local timezone as an ISO-8601 string."""
    return dt.astimezone(LOCAL_TZ).isoformat(timespec="seconds")
