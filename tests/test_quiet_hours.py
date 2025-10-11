from __future__ import annotations

import unittest
from datetime import datetime, time
from unittest.mock import patch

from monitor.core.quiet_hours import (
    UTC,
    QuietWindow,
    is_quiet_time,
    iter_active_utc_ranges,
    iter_quiet_utc_ranges,
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
            (
                datetime(2024, 5, 1, 20, 45, tzinfo=UTC),
                datetime(2024, 5, 1, 21, 59, tzinfo=UTC),
            ),
            (
                datetime(2024, 5, 2, 20, 45, tzinfo=UTC),
                datetime(2024, 5, 2, 21, 59, tzinfo=UTC),
            ),
        ]
        self.assertEqual(ranges, expected)

    def test_iter_active_ranges_excludes_quiet(self) -> None:
        start = datetime(2024, 5, 1, 18, 0, tzinfo=UTC)
        end = datetime(2024, 5, 1, 23, 0, tzinfo=UTC)
        active = list(iter_active_utc_ranges(start, end))
        expected = [
            (
                datetime(2024, 5, 1, 18, 0, tzinfo=UTC),
                datetime(2024, 5, 1, 20, 45, tzinfo=UTC),
            ),
            (
                datetime(2024, 5, 1, 21, 59, tzinfo=UTC),
                datetime(2024, 5, 1, 23, 0, tzinfo=UTC),
            ),
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

    def test_weekend_is_quiet(self) -> None:
        saturday_midday = datetime(2024, 5, 4, 9, 0, tzinfo=UTC)
        sunday_evening = datetime(2024, 5, 5, 20, 0, tzinfo=UTC)
        monday_boundary = datetime(2024, 5, 5, 21, 58, tzinfo=UTC)

        self.assertTrue(is_quiet_time(saturday_midday))
        self.assertTrue(is_quiet_time(sunday_evening))
        self.assertTrue(is_quiet_time(monday_boundary))

        monday_active = datetime(2024, 5, 5, 22, 0, tzinfo=UTC)
        self.assertFalse(is_quiet_time(monday_active))

    def test_quiet_ranges_include_weekend_block(self) -> None:
        start = datetime(2024, 5, 3, 18, 0, tzinfo=UTC)
        end = datetime(2024, 5, 6, 0, 0, tzinfo=UTC)
        ranges = list(iter_quiet_utc_ranges(start, end))

        expected = [
            (
                datetime(2024, 5, 3, 20, 45, tzinfo=UTC),
                datetime(2024, 5, 5, 21, 59, tzinfo=UTC),
            ),
        ]
        self.assertEqual(ranges, expected)

    def test_next_transition_during_weekend(self) -> None:
        sunday_noon = datetime(2024, 5, 5, 12, 0, tzinfo=UTC)
        self.assertEqual(
            next_quiet_transition(sunday_noon),
            datetime(2024, 5, 5, 21, 59, tzinfo=UTC),
        )

    def test_crypto_weekend_active(self) -> None:
        saturday_midday = datetime(2024, 5, 4, 9, 0, tzinfo=UTC)
        sunday_evening = datetime(2024, 5, 5, 20, 0, tzinfo=UTC)

        self.assertFalse(is_quiet_time(saturday_midday, asset_kind="crypto"))
        self.assertFalse(is_quiet_time(sunday_evening, asset_kind="crypto"))

        nightly_quiet = datetime(2024, 5, 5, 20, 50, tzinfo=UTC)
        self.assertTrue(is_quiet_time(nightly_quiet, asset_kind="crypto"))

    def test_crypto_quiet_ranges_exclude_weekend_block(self) -> None:
        start = datetime(2024, 5, 3, 18, 0, tzinfo=UTC)
        end = datetime(2024, 5, 6, 0, 0, tzinfo=UTC)
        ranges = list(iter_quiet_utc_ranges(start, end, asset_kind="crypto"))

        expected = [
            (
                datetime(2024, 5, 3, 20, 45, tzinfo=UTC),
                datetime(2024, 5, 3, 21, 59, tzinfo=UTC),
            ),
            (
                datetime(2024, 5, 4, 20, 45, tzinfo=UTC),
                datetime(2024, 5, 4, 21, 59, tzinfo=UTC),
            ),
            (
                datetime(2024, 5, 5, 20, 45, tzinfo=UTC),
                datetime(2024, 5, 5, 21, 59, tzinfo=UTC),
            ),
        ]
        self.assertEqual(ranges, expected)

    def test_crypto_next_transition_during_weekend(self) -> None:
        sunday_noon = datetime(2024, 5, 5, 12, 0, tzinfo=UTC)
        self.assertEqual(
            next_quiet_transition(sunday_noon, asset_kind="crypto"),
            datetime(2024, 5, 5, 20, 45, tzinfo=UTC),
        )

    def test_iter_quiet_ranges_returns_empty_for_inverted_bounds(self) -> None:
        start = datetime(2024, 5, 1, 12, 0, tzinfo=UTC)
        end = datetime(2024, 5, 1, 11, 0, tzinfo=UTC)
        self.assertEqual(list(iter_quiet_utc_ranges(start, end)), [])

    def test_iter_quiet_ranges_skip_zero_length_windows(self) -> None:
        custom_window = QuietWindow(start=time(hour=12), end=time(hour=12))
        start = datetime(2024, 5, 1, 8, 0, tzinfo=UTC)
        end = datetime(2024, 5, 1, 10, 0, tzinfo=UTC)
        with patch("monitor.core.quiet_hours.QUIET_WINDOWS_UTC3", (custom_window,)):
            ranges = list(iter_quiet_utc_ranges(start, end))
        self.assertEqual(ranges, [])

    def test_iter_active_ranges_returns_empty_for_inverted_bounds(self) -> None:
        start = datetime(2024, 5, 1, 12, 0, tzinfo=UTC)
        end = datetime(2024, 5, 1, 11, 0, tzinfo=UTC)
        self.assertEqual(list(iter_active_utc_ranges(start, end)), [])

    def test_next_transition_without_quiet_windows(self) -> None:
        check_time = datetime(2024, 5, 1, 12, 0, tzinfo=UTC)
        with patch("monitor.core.quiet_hours.QUIET_WINDOWS_UTC3", ()):
            self.assertEqual(
                next_quiet_transition(check_time, asset_kind="crypto"),
                check_time,
            )


if __name__ == "__main__":
    unittest.main()
