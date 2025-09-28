from __future__ import annotations

import unittest
from datetime import datetime, timezone

from monitor.quiet_hours import (
    UTC,
    iter_active_utc_ranges,
    iter_quiet_utc_ranges,
    is_quiet_time,
    next_quiet_transition,
)


class QuietHoursTests(unittest.TestCase):
    def test_is_quiet_time_boundaries(self) -> None:
        inside = datetime(2024, 5, 1, 20, 45, tzinfo=UTC)
        self.assertTrue(is_quiet_time(inside))

        # Naive datetimes are interpreted as UTC
        inside_naive = datetime(2024, 5, 1, 20, 45)
        self.assertTrue(is_quiet_time(inside_naive))

        outside = datetime(2024, 5, 1, 22, 5, tzinfo=UTC)
        self.assertFalse(is_quiet_time(outside))

        boundary_end = datetime(2024, 5, 1, 22, 0, tzinfo=UTC)
        self.assertFalse(is_quiet_time(boundary_end))

    def test_iter_quiet_ranges_multi_day(self) -> None:
        start = datetime(2024, 5, 1, 0, 0, tzinfo=UTC)
        end = datetime(2024, 5, 3, 0, 0, tzinfo=UTC)
        ranges = list(iter_quiet_utc_ranges(start, end))
        expected = [
            (datetime(2024, 5, 1, 20, 45, tzinfo=UTC), datetime(2024, 5, 1, 21, 59, tzinfo=UTC)),
            (datetime(2024, 5, 2, 20, 45, tzinfo=UTC), datetime(2024, 5, 2, 21, 59, tzinfo=UTC)),
        ]
        self.assertEqual(ranges, expected)

    def test_iter_active_ranges_excludes_quiet(self) -> None:
        start = datetime(2024, 5, 1, 18, 0, tzinfo=UTC)
        end = datetime(2024, 5, 1, 23, 0, tzinfo=UTC)
        active = list(iter_active_utc_ranges(start, end))
        expected = [
            (datetime(2024, 5, 1, 18, 0, tzinfo=UTC), datetime(2024, 5, 1, 20, 45, tzinfo=UTC)),
            (datetime(2024, 5, 1, 21, 59, tzinfo=UTC), datetime(2024, 5, 1, 23, 0, tzinfo=UTC)),
        ]
        self.assertEqual(active, expected)

    def test_active_ranges_empty_inside_quiet(self) -> None:
        start = datetime(2024, 5, 1, 20, 45, tzinfo=UTC)
        end = datetime(2024, 5, 1, 21, 15, tzinfo=UTC)
        active = list(iter_active_utc_ranges(start, end))
        self.assertEqual(active, [])

    def test_next_quiet_transition(self) -> None:
        before_quiet = datetime(2024, 5, 1, 19, 0, tzinfo=UTC)
        self.assertEqual(
            next_quiet_transition(before_quiet),
            datetime(2024, 5, 1, 20, 45, tzinfo=UTC),
        )

        during_quiet = datetime(2024, 5, 1, 21, 0, tzinfo=UTC)
        self.assertEqual(
            next_quiet_transition(during_quiet),
            datetime(2024, 5, 1, 21, 59, tzinfo=UTC),
        )


if __name__ == "__main__":
    unittest.main()
