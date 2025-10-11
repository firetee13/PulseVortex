"""Edge case tests for hit checker functions to improve coverage."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from monitor.cli.hit_checker import (
    CandidateWindow,
    RateBar,
    _bar_crosses_price,
    _evaluate_setup,
    _merge_windows,
    _rates_to_bars,
    _resolve_timeframe,
    scan_for_hit_with_chunks,
)
from monitor.core.domain import Hit, Setup, TickFetchStats

UTC = timezone.utc


class HitCheckerEdgeCaseTests(unittest.TestCase):
    """Test edge cases and uncovered branches in hit checker."""

    def test_merge_windows_empty_list(self):
        """Test window merging with empty list."""
        merged = _merge_windows([])
        self.assertEqual(merged, [])

    def test_merge_windows_single_window(self):
        """Test window merging with single window."""
        base = datetime(2024, 1, 1, tzinfo=UTC)
        window = CandidateWindow(1, base, base + timedelta(seconds=10), base, base)
        merged = _merge_windows([window])
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0], window)

    def test_merge_windows_non_overlapping(self):
        """Test window merging with non-overlapping windows."""
        base = datetime(2024, 1, 1, tzinfo=UTC)
        win1 = CandidateWindow(1, base, base + timedelta(seconds=5), base, base)
        win2 = CandidateWindow(
            1, base + timedelta(seconds=10), base + timedelta(seconds=15), base, base
        )
        win3 = CandidateWindow(2, base, base + timedelta(seconds=5), base, base)

        merged = _merge_windows([win1, win2, win3])
        self.assertEqual(len(merged), 3)  # No merging should occur

    def test_merge_windows_different_setup_ids(self):
        """Test that windows with different setup IDs don't merge."""
        base = datetime(2024, 1, 1, tzinfo=UTC)
        win1 = CandidateWindow(1, base, base + timedelta(seconds=10), base, base)
        win2 = CandidateWindow(
            2, base + timedelta(seconds=5), base + timedelta(seconds=15), base, base
        )

        merged = _merge_windows([win1, win2])
        self.assertEqual(len(merged), 2)

    def test_bar_crosses_price_edge_cases(self):
        """Test edge cases in _bar_crosses_price function."""
        bar = RateBar(
            start_utc=datetime(2024, 1, 1, tzinfo=UTC),
            end_utc=datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
            low=1.0,
            high=2.0,
        )

        # Test with sell direction and spread guard
        setup_sell = SimpleNamespace(direction="sell", sl=0.9, tp=2.1)
        self.assertTrue(_bar_crosses_price(bar, setup_sell, spread_guard=0.1))

        # Test with buy direction where price exactly equals SL/TP
        setup_buy_exact = SimpleNamespace(direction="buy", sl=1.0, tp=2.0)
        self.assertTrue(_bar_crosses_price(bar, setup_buy_exact, spread_guard=0.0))

        # Test with spread guard - let's test with larger range to ensure crossing
        setup_buy_spread = SimpleNamespace(
            direction="buy", sl=0.9, tp=1.8
        )  # Within range
        self.assertTrue(_bar_crosses_price(bar, setup_buy_spread, spread_guard=0.0))

    def test_rates_to_bars_with_invalid_data(self):
        """Test _rates_to_bars with various invalid data."""
        # Test with completely invalid rate
        invalid_rate = {"invalid": "data"}
        bars = _rates_to_bars([invalid_rate], timeframe_seconds=60, offset_hours=0)
        self.assertEqual(len(bars), 0)

        # Test with mixed valid/invalid rates
        valid_rate = {"time": 1_700_000_000, "low": 1.0, "high": 2.0}
        invalid_time = {"time": None, "low": 1.1, "high": 2.1}
        invalid_low = {"time": 1_700_000_001, "low": None, "high": 2.2}
        bars = _rates_to_bars(
            [valid_rate, invalid_time, invalid_low],
            timeframe_seconds=60,
            offset_hours=0,
        )
        self.assertEqual(len(bars), 1)

    def test_resolve_timeframe_with_invalid_codes(self):
        """Test _resolve_timeframe with various inputs."""
        with patch(
            "monitor.cli.hit_checker.timeframe_from_code", return_value=None
        ), patch("monitor.cli.hit_checker.timeframe_m1", return_value=164):
            # Test with None code
            self.assertEqual(_resolve_timeframe(None), 164)

            # Test with invalid code that returns None
            self.assertEqual(_resolve_timeframe("INVALID"), 164)

    def test_scan_for_hit_with_chunks_edge_cases(self):
        """Test scan_for_hit_with_chunks with edge cases."""
        start_utc = datetime(2024, 1, 1, tzinfo=UTC)
        end_utc = start_utc + timedelta(minutes=10)

        # Test with empty range (start >= end)
        hit, stats, chunks = scan_for_hit_with_chunks(
            symbol="EURUSD",
            direction="buy",
            sl=1.0,
            tp=2.0,
            offset_hours=0,
            start_utc=end_utc,
            end_utc=start_utc,  # Reversed
            chunk_minutes=None,
            trace=False,
        )
        self.assertIsNone(hit)
        self.assertEqual(stats.total_ticks, 0)
        self.assertEqual(chunks, 0)

        # Test with chunk_minutes = 0
        with patch("monitor.cli.hit_checker.ticks_range_all") as mock_ticks, patch(
            "monitor.cli.hit_checker.earliest_hit_from_ticks"
        ) as mock_hit:
            mock_ticks.return_value = (
                [],
                TickFetchStats(
                    pages=1,
                    total_ticks=0,
                    elapsed_s=0.1,
                    fetch_s=0.05,
                    early_stop=False,
                ),
            )
            mock_hit.return_value = None

            hit, stats, chunks = scan_for_hit_with_chunks(
                symbol="EURUSD",
                direction="buy",
                sl=1.0,
                tp=2.0,
                offset_hours=0,
                start_utc=start_utc,
                end_utc=end_utc,
                chunk_minutes=0,  # Should be treated as None
                trace=False,
            )
            self.assertIsNone(hit)

    def test_scan_for_hit_with_chunks_chunk_edge_cases(self):
        """Test edge cases in chunk processing."""
        start_utc = datetime(2024, 1, 1, tzinfo=UTC)
        end_utc = start_utc + timedelta(minutes=25)

        call_count = 0

        def mock_ticks_range_all(symbol, start, end, trace):
            nonlocal call_count
            call_count += 1
            # Return empty ticks for all chunks
            return [], TickFetchStats(
                pages=1, total_ticks=0, elapsed_s=0.1, fetch_s=0.05, early_stop=False
            )

        def mock_earliest_hit(ticks, direction, sl, tp, offset_hours):
            return None

        with patch(
            "monitor.cli.hit_checker.ticks_range_all", side_effect=mock_ticks_range_all
        ), patch(
            "monitor.cli.hit_checker.earliest_hit_from_ticks",
            side_effect=mock_earliest_hit,
        ), patch(
            "monitor.cli.hit_checker.to_server_naive", return_value=start_utc
        ):
            hit, stats, chunks = scan_for_hit_with_chunks(
                symbol="EURUSD",
                direction="buy",
                sl=1.0,
                tp=2.0,
                offset_hours=0,
                start_utc=start_utc,
                end_utc=end_utc,
                chunk_minutes=10,
                trace=False,
            )

            # Should have been called 3 times (10+10+5 minutes)
            self.assertEqual(call_count, 3)
            self.assertIsNone(hit)
            self.assertEqual(chunks, 3)

    def test_evaluate_setup_ignored_hit_quiet_hours(self):
        """Test _evaluate_setup ignores hits during quiet hours."""
        setup = Setup(
            id=1,
            symbol="EURUSD",
            direction="buy",
            sl=1.0,
            tp=2.0,
            entry_price=None,
            as_of_utc=datetime(2024, 1, 1, 22, 30, tzinfo=UTC),  # During quiet hours
        )
        now_utc = datetime(2024, 1, 1, 23, 30, tzinfo=UTC)

        # Mock a hit during quiet hours
        hit_time = datetime(2024, 1, 1, 23, 0, tzinfo=UTC)
        fake_stats = TickFetchStats(
            pages=1, total_ticks=10, elapsed_s=0.1, fetch_s=0.05, early_stop=True
        )

        def fake_scan(**kwargs):
            return Hit(kind="TP", time_utc=hit_time, price=2.0), fake_stats, 1

        with patch(
            "monitor.cli.hit_checker.scan_for_hit_with_chunks", side_effect=fake_scan
        ), patch(
            "monitor.cli.hit_checker.classify_symbol", return_value="forex"
        ), patch(
            "monitor.cli.hit_checker.iter_active_utc_ranges",
            return_value=[(setup.as_of_utc, now_utc)],
        ), patch(
            "monitor.cli.hit_checker.is_quiet_time", return_value=True
        ):
            result = _evaluate_setup(
                setup=setup,
                last_checked_utc=setup.as_of_utc,
                bars=[],
                resolved_symbol="EURUSD",
                offset_hours=0,
                spread_guard=0.0,
                now_utc=now_utc,
                chunk_minutes=None,
                tick_padding_seconds=0.0,
                trace_ticks=False,
            )

            # Hit should be ignored due to quiet hours
            self.assertIsNone(result.hit)
            self.assertTrue(result.ignored_hit)

    def test_evaluate_setup_fallback_no_bars(self):
        """Test _evaluate_setup fallback when no bars available."""
        setup = Setup(
            id=1,
            symbol="EURUSD",
            direction="buy",
            sl=1.0,
            tp=2.0,
            entry_price=None,
            as_of_utc=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
        )
        now_utc = datetime(2024, 1, 1, 12, 30, tzinfo=UTC)

        with patch(
            "monitor.cli.hit_checker.scan_for_hit_with_chunks"
        ) as mock_scan, patch(
            "monitor.cli.hit_checker.classify_symbol", return_value="forex"
        ), patch(
            "monitor.cli.hit_checker.iter_active_utc_ranges",
            return_value=[(setup.as_of_utc, now_utc)],
        ):
            mock_scan.return_value = (
                None,
                TickFetchStats(
                    pages=0, total_ticks=0, elapsed_s=0.0, fetch_s=0.0, early_stop=False
                ),
                0,
            )

            result = _evaluate_setup(
                setup=setup,
                last_checked_utc=setup.as_of_utc,
                bars=[],  # No bars
                resolved_symbol="EURUSD",
                offset_hours=0,
                spread_guard=0.0,
                now_utc=now_utc,
                chunk_minutes=None,
                tick_padding_seconds=0.0,
                trace_ticks=False,
            )

            # Should have created fallback window
            self.assertEqual(result.last_checked_utc, now_utc)
            self.assertIsNone(result.hit)

    def test_evaluate_setup_tick_padding_scenarios(self):
        """Test various tick padding scenarios."""
        setup = Setup(
            id=1,
            symbol="EURUSD",
            direction="buy",
            sl=1.0,
            tp=2.0,
            entry_price=None,
            as_of_utc=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
        )
        now_utc = datetime(2024, 1, 1, 12, 30, tzinfo=UTC)
        bar = RateBar(
            start_utc=setup.as_of_utc,
            end_utc=now_utc,
            low=0.9,
            high=2.1,
        )

        # Test with negative tick padding (should be treated as 0)
        with patch(
            "monitor.cli.hit_checker.scan_for_hit_with_chunks"
        ) as mock_scan, patch(
            "monitor.cli.hit_checker.classify_symbol", return_value="forex"
        ), patch(
            "monitor.cli.hit_checker.iter_active_utc_ranges",
            return_value=[(setup.as_of_utc, now_utc)],
        ):
            mock_scan.return_value = (
                None,
                TickFetchStats(
                    pages=0, total_ticks=0, elapsed_s=0.0, fetch_s=0.0, early_stop=False
                ),
                0,
            )

            result = _evaluate_setup(
                setup=setup,
                last_checked_utc=setup.as_of_utc,
                bars=[bar],
                resolved_symbol="EURUSD",
                offset_hours=0,
                spread_guard=0.0,
                now_utc=now_utc,
                chunk_minutes=None,
                tick_padding_seconds=-1.0,  # Negative padding
                trace_ticks=False,
            )

            # Should still work correctly
            self.assertEqual(result.last_checked_utc, now_utc)

    def test_evaluate_setup_window_adjustment_edge_cases(self):
        """Test edge cases in window start/end adjustments."""
        setup = Setup(
            id=1,
            symbol="EURUSD",
            direction="buy",
            sl=1.0,
            tp=2.0,
            entry_price=None,
            as_of_utc=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
        )
        now_utc = datetime(2024, 1, 1, 12, 5, tzinfo=UTC)
        bar = RateBar(
            start_utc=setup.as_of_utc,
            end_utc=now_utc,
            low=0.9,
            high=2.1,
        )

        with patch(
            "monitor.cli.hit_checker.scan_for_hit_with_chunks"
        ) as mock_scan, patch(
            "monitor.cli.hit_checker.classify_symbol", return_value="forex"
        ), patch(
            "monitor.cli.hit_checker.iter_active_utc_ranges",
            return_value=[(setup.as_of_utc, now_utc)],
        ):

            def mock_scan_with_time_adjustment(**kwargs):
                # Simulate case where window_end <= window_start after padding
                start, end = kwargs["start_utc"], kwargs["end_utc"]
                if end <= start:
                    # Return empty result
                    return (
                        None,
                        TickFetchStats(
                            pages=0,
                            total_ticks=0,
                            elapsed_s=0.0,
                            fetch_s=0.0,
                            early_stop=False,
                        ),
                        0,
                    )
                return (
                    None,
                    TickFetchStats(
                        pages=1,
                        total_ticks=0,
                        elapsed_s=0.1,
                        fetch_s=0.05,
                        early_stop=False,
                    ),
                    1,
                )

            mock_scan.side_effect = mock_scan_with_time_adjustment

            result = _evaluate_setup(
                setup=setup,
                last_checked_utc=setup.as_of_utc,
                bars=[bar],
                resolved_symbol="EURUSD",
                offset_hours=0,
                spread_guard=0.0,
                now_utc=now_utc,
                chunk_minutes=None,
                tick_padding_seconds=10.0,  # Large padding
                trace_ticks=False,
            )

            # Should handle edge case gracefully
            self.assertEqual(result.last_checked_utc, now_utc)


if __name__ == "__main__":
    unittest.main()
