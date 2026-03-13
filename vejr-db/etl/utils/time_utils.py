from datetime import datetime, timedelta, timezone


def utc_str(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def floor_to_interval(dt: datetime, interval_minutes: int) -> datetime:
    return dt.replace(
        microsecond=0,
        second=0,
        minute=(dt.minute // interval_minutes) * interval_minutes,
    )


def iter_time_chunks(start: datetime, end: datetime, chunk_hours: int = 24):
    chunk_size = timedelta(hours=chunk_hours)
    current = start

    while current < end:
        chunk_end = min(current + chunk_size, end)
        yield current, chunk_end
        current = chunk_end