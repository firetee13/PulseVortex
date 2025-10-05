from __future__ import annotations

"""Utilities for recurring quiet-trading windows (UTC+3) shared by CLI and GUI."""

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Iterator, List, Tuple

from monitor.symbols import classify_symbol

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


WEEKEND_START_WEEKDAY = 5  # Saturday in Python's weekday()


def _as_utc(dt: datetime) -> datetime:
    """Return ``dt`` as a timezone-aware UTC datetime."""

    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _daily_quiet_intervals(day_local: date, weekend_quiet: bool) -> List[Tuple[datetime, datetime]]:
    """Compute quiet intervals for a given local day as UTC datetimes."""

    intervals: List[Tuple[datetime, datetime]] = []
    for window in QUIET_WINDOWS_UTC3:
        start_local = datetime.combine(day_local, window.start, tzinfo=UTC_PLUS_3)
        if window.spans_midnight():
            end_local = datetime.combine(day_local + timedelta(days=1), window.end, tzinfo=UTC_PLUS_3)
        else:
            end_local = datetime.combine(day_local, window.end, tzinfo=UTC_PLUS_3)
        intervals.append((start_local.astimezone(UTC), end_local.astimezone(UTC)))

    if weekend_quiet and day_local.weekday() == WEEKEND_START_WEEKDAY:
        weekend_start_local = datetime.combine(day_local, time.min, tzinfo=UTC_PLUS_3)
        weekend_end_local = datetime.combine(
            day_local + timedelta(days=2), time(hour=0, minute=59), tzinfo=UTC_PLUS_3
        )
        intervals.append((weekend_start_local.astimezone(UTC), weekend_end_local.astimezone(UTC)))

    return intervals


def _resolve_asset_kind(asset_kind: str | None, symbol: str | None) -> str | None:
    if asset_kind:
        return asset_kind.lower()
    if symbol:
        return classify_symbol(symbol)
    return None


def iter_quiet_utc_ranges(
    start: datetime,
    end: datetime,
    *,
    asset_kind: str | None = None,
    symbol: str | None = None,
) -> Iterator[Tuple[datetime, datetime]]:
    """Yield quiet intervals in UTC overlapping the [start, end) window."""

    start_utc = _as_utc(start)
    end_utc = _as_utc(end)
    if start_utc >= end_utc:
        return

    start_local = start_utc.astimezone(UTC_PLUS_3)
    end_local = end_utc.astimezone(UTC_PLUS_3)
    day = start_local.date() - timedelta(days=1)
    last_day = end_local.date() + timedelta(days=1)

    resolved_kind = _resolve_asset_kind(asset_kind, symbol)
    weekend_quiet = resolved_kind != "crypto"

    intervals: List[Tuple[datetime, datetime]] = []
    while day <= last_day:
        for interval in _daily_quiet_intervals(day, weekend_quiet):
            qs, qe = interval
            if qe <= start_utc or qs >= end_utc:
                continue
            intervals.append((max(qs, start_utc), min(qe, end_utc)))
        day += timedelta(days=1)
    intervals.sort(key=lambda rng: rng[0])

    merged: List[Tuple[datetime, datetime]] = []
    for qs, qe in intervals:
        if qs >= qe:
            continue
        if not merged:
            merged.append((qs, qe))
            continue
        last_start, last_end = merged[-1]
        if qs <= last_end:
            merged[-1] = (last_start, max(last_end, qe))
        else:
            merged.append((qs, qe))

    for qs, qe in merged:
        yield qs, qe


def iter_active_utc_ranges(
    start: datetime,
    end: datetime,
    *,
    asset_kind: str | None = None,
    symbol: str | None = None,
) -> Iterator[Tuple[datetime, datetime]]:
    """Yield UTC sub-ranges that exclude quiet windows within [start, end)."""

    start_utc = _as_utc(start)
    end_utc = _as_utc(end)
    if start_utc >= end_utc:
        return

    quiet = list(iter_quiet_utc_ranges(start_utc, end_utc, asset_kind=asset_kind, symbol=symbol))
    cursor = start_utc
    for qs, qe in quiet:
        if cursor < qs:
            yield cursor, qs
        cursor = max(cursor, qe)
    if cursor < end_utc:
        yield cursor, end_utc


def is_quiet_time(
    dt: datetime,
    *,
    asset_kind: str | None = None,
    symbol: str | None = None,
) -> bool:
    """Return True when ``dt`` is inside a quiet window for the given asset kind."""

    dt_utc = _as_utc(dt)
    dt_local = dt_utc.astimezone(UTC_PLUS_3)
    day = dt_local.date()
    resolved_kind = _resolve_asset_kind(asset_kind, symbol)
    weekend_quiet = resolved_kind != "crypto"
    for delta_days in (-1, 0, 1):
        curr_day = day + timedelta(days=delta_days)
        for start_utc, end_utc in _daily_quiet_intervals(curr_day, weekend_quiet):
            if start_utc <= dt_utc < end_utc:
                return True
    return False


def next_quiet_transition(
    dt: datetime,
    *,
    asset_kind: str | None = None,
    symbol: str | None = None,
) -> datetime:
    """Return the next UTC instant where quiet/active state toggles."""

    dt_utc = _as_utc(dt)
    inside_quiet = is_quiet_time(dt_utc, asset_kind=asset_kind, symbol=symbol)

    search_start = dt_utc - timedelta(days=2)
    search_end = dt_utc + timedelta(days=7)
    intervals = list(
        iter_quiet_utc_ranges(
            search_start,
            search_end,
            asset_kind=asset_kind,
            symbol=symbol,
        )
    )

    for start_utc, end_utc in intervals:
        if start_utc <= dt_utc < end_utc:
            return end_utc
        if dt_utc < start_utc and not inside_quiet:
            return start_utc

    return dt_utc


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
