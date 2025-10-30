"""Datetime parsing utilities for database operations."""
from datetime import datetime, timezone

def parse_utc(value) -> datetime | None:
    """Convert a datetime, string, or timestamp to a timezone-aware UTC datetime.

    - If already timezone-aware, normalize to UTC.
    - If naive, assume UTC.
    - If string, try ISO 8601 parsing.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, (int, float)):
        # Treat numeric timestamps as seconds since epoch
        return datetime.fromtimestamp(value, tz=timezone.utc)

    if isinstance(value, str):
        # Handle ISO 8601 style strings with or without timezone
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            # fallback for sloppy formats
            dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    raise TypeError(f"Unsupported datetime value: {value!r}")


def utcnow() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc)
