from __future__ import annotations

"""Utilities for recurring quiet-trading windows (UTC+3) shared by CLI and GUI."""

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Iterator, List, Tuple

UTC = timezone.utc
UTC_PLUS_3 = timezone(timedelta(hours=3))


@dataclass(frozen=True)
class QuietWindow:
    """Definitions for a daily quiet interval in a specific timezone."""

    start: time
    end: time

    def spans_midnight(self) -> bool:
        return self.start > self.end


# Default blackout: 23:30 -> 01:00 (UTC+3) to avoid rollover spread spikes
QUIET_WINDOWS_UTC3: Tuple[QuietWindow, ...] = (
    QuietWindow(start=time(hour=23, minute=45), end=time(hour=0, minute=59)),
)


def _as_utc(dt: datetime) -> datetime:
    """Return ``dt`` as a timezone-aware UTC datetime."""

    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _daily_quiet_intervals(day_local: date) -> List[Tuple[datetime, datetime]]:
    """Compute quiet intervals for a given local day as UTC datetimes."""

    intervals: List[Tuple[datetime, datetime]] = []
    for window in QUIET_WINDOWS_UTC3:
        start_local = datetime.combine(day_local, window.start, tzinfo=UTC_PLUS_3)
        if window.spans_midnight():
            end_local = datetime.combine(day_local + timedelta(days=1), window.end, tzinfo=UTC_PLUS_3)
        else:
            end_local = datetime.combine(day_local, window.end, tzinfo=UTC_PLUS_3)
        intervals.append((start_local.astimezone(UTC), end_local.astimezone(UTC)))
    return intervals


def iter_quiet_utc_ranges(start: datetime, end: datetime) -> Iterator[Tuple[datetime, datetime]]:
    """Yield quiet intervals in UTC overlapping the [start, end) window."""

    start_utc = _as_utc(start)
    end_utc = _as_utc(end)
    if start_utc >= end_utc:
        return

    start_local = start_utc.astimezone(UTC_PLUS_3)
    end_local = end_utc.astimezone(UTC_PLUS_3)
    day = start_local.date() - timedelta(days=1)
    last_day = end_local.date() + timedelta(days=1)

    intervals: List[Tuple[datetime, datetime]] = []
    while day <= last_day:
        for interval in _daily_quiet_intervals(day):
            qs, qe = interval
            if qe <= start_utc or qs >= end_utc:
                continue
            intervals.append((max(qs, start_utc), min(qe, end_utc)))
        day += timedelta(days=1)
    intervals.sort(key=lambda rng: rng[0])

    for qs, qe in intervals:
        if qs < qe:
            yield qs, qe


def iter_active_utc_ranges(start: datetime, end: datetime) -> Iterator[Tuple[datetime, datetime]]:
    """Yield UTC sub-ranges that exclude quiet windows within [start, end)."""

    start_utc = _as_utc(start)
    end_utc = _as_utc(end)
    if start_utc >= end_utc:
        return

    quiet = list(iter_quiet_utc_ranges(start_utc, end_utc))
    cursor = start_utc
    for qs, qe in quiet:
        if cursor < qs:
            yield cursor, qs
        cursor = max(cursor, qe)
    if cursor < end_utc:
        yield cursor, end_utc


def is_quiet_time(dt: datetime) -> bool:
    """Return True when ``dt`` (interpreted as UTC if naive) is inside a quiet window."""

    dt_utc = _as_utc(dt)
    dt_local = dt_utc.astimezone(UTC_PLUS_3)
    local_time = dt_local.timetz().replace(tzinfo=None)
    for window in QUIET_WINDOWS_UTC3:
        if not window.spans_midnight():
            if window.start <= local_time < window.end:
                return True
        else:
            if local_time >= window.start or local_time < window.end:
                return True
    return False


def next_quiet_transition(dt: datetime) -> datetime:
    """Return the next UTC instant where quiet/active state toggles."""

    dt_utc = _as_utc(dt)
    dt_local = dt_utc.astimezone(UTC_PLUS_3)
    day = dt_local.date()
    candidates: List[datetime] = []
    for delta_days in (-1, 0, 1, 2):
        curr_day = day + timedelta(days=delta_days)
        for interval in _daily_quiet_intervals(curr_day):
            start_utc, end_utc = interval
            if start_utc > dt_utc:
                candidates.append(start_utc)
            if end_utc > dt_utc:
                candidates.append(end_utc)
    if not candidates:
        return dt_utc
    return min(candidates)


__all__ = [
    "QuietWindow",
    "QUIET_WINDOWS_UTC3",
    "UTC",
    "UTC_PLUS_3",
    "iter_quiet_utc_ranges",
    "iter_active_utc_ranges",
    "is_quiet_time",
    "next_quiet_transition",
]
