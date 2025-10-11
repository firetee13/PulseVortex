"""Tests for main setup analyzer functions to improve coverage."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from monitor.cli import setup_analyzer as sa

UTC = timezone.utc


class SetupAnalyzerMainFunctionTests(unittest.TestCase):
    """Test main setup analyzer functions."""

    @patch("sys.argv", ["script.py"])
    def test_parse_args_defaults(self):
        args = sa.parse_args()
        self.assertEqual(args.symbols, "")
        self.assertEqual(args.min_rrr, 1.0)
        self.assertIsNone(args.top)
        self.assertFalse(args.brief)
        self.assertFalse(args.watch)
        self.assertEqual(args.interval, 1.0)
        self.assertFalse(args.debug)
        self.assertEqual(args.exclude, "")

    @patch(
        "sys.argv",
        [
            "script.py",
            "--symbols",
            "EURUSD,BTCUSD",
            "--min-rrr",
            "2.0",
            "--top",
            "5",
            "--brief",
            "--watch",
            "--interval",
            "2.0",
            "--debug",
            "--exclude",
            "GLMUSD",
        ],
    )
    def test_parse_args_with_values(self):
        args = sa.parse_args()
        self.assertEqual(args.symbols, "EURUSD,BTCUSD")
        self.assertEqual(args.min_rrr, 2.0)
        self.assertEqual(args.top, 5)
        self.assertTrue(args.brief)
        self.assertTrue(args.watch)
        self.assertEqual(args.interval, 2.0)
        self.assertTrue(args.debug)
        self.assertEqual(args.exclude, "GLMUSD")

    @patch("monitor.cli.setup_analyzer._mt5_ensure_init")
    def test__mt5_ensure_init_success(self, mock_init):
        mock_init.return_value = True
        result = sa._mt5_ensure_init()
        self.assertTrue(result)

    @patch("monitor.cli.setup_analyzer._mt5_ensure_init")
    def test__mt5_ensure_init_failure(self, mock_init):
        mock_init.return_value = False
        result = sa._mt5_ensure_init()
        self.assertFalse(result)

    @patch("monitor.cli.setup_analyzer._mt5_ensure_init")
    def test__mt5_copy_rates_cached(self, mock_init):
        mock_init.return_value = True
        mock_rates = [{"time": 1, "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05}]

        with patch("monitor.cli.setup_analyzer.mt5") as mock_mt5:
            mock_mt5.copy_rates_from_pos.return_value = mock_rates

            result = sa._mt5_copy_rates_cached("EURUSD", 1440, 10)
            self.assertEqual(result, mock_rates)
            mock_mt5.copy_rates_from_pos.assert_called_once()

    @patch("monitor.cli.setup_analyzer._mt5_copy_rates_cached")
    def test__pivots_from_prev_day(self, mock_rates):
        daily_rates = [
            {"high": 1.2, "low": 1.0, "close": 1.1},  # Previous day
            {"high": 1.25, "low": 1.05, "close": 1.15},  # Current day
        ]
        s1, r1 = sa._pivots_from_prev_day(daily_rates)
        pivot = (1.2 + 1.0 + 1.1) / 3.0
        expected_s1 = 2 * pivot - 1.2
        expected_r1 = 2 * pivot - 1.0
        self.assertAlmostEqual(s1, expected_s1)
        self.assertAlmostEqual(r1, expected_r1)

    def test__pivots_from_prev_day_insufficient_data(self):
        daily_rates = [{"high": 1.25, "low": 1.05, "close": 1.15}]
        s1, r1 = sa._pivots_from_prev_day(daily_rates)
        self.assertIsNone(s1)
        self.assertIsNone(r1)

    def test__atr_calculation(self):
        values = [
            (1.1, 1.0, 1.05),  # high, low, close
            (1.15, 1.05, 1.1),
            (1.2, 1.1, 1.15),
            (1.18, 1.08, 1.12),
            (1.22, 1.12, 1.18),
            (1.25, 1.15, 1.22),
            (1.23, 1.13, 1.18),
            (1.28, 1.18, 1.25),
            (1.3, 1.2, 1.28),
            (1.27, 1.17, 1.22),
            (1.32, 1.22, 1.3),
            (1.35, 1.25, 1.33),
            (1.33, 1.23, 1.28),
            (1.38, 1.28, 1.35),
            (1.4, 1.3, 1.37),
            (1.37, 1.27, 1.32),
        ]
        atr = sa._atr(values, period=14)
        self.assertIsNotNone(atr)
        self.assertGreater(atr, 0)

    def test__atr_insufficient_data(self):
        values = [(1.1, 1.0, 1.05)]
        atr = sa._atr(values, period=14)
        self.assertIsNone(atr)

    def test__pct_change_completed(self):
        rates = [
            {"time": 1609459200, "close": 1.0},  # 2021-01-01
            {"time": 1609459260, "close": 1.01},  # 2021-01-01 + 1 min
        ]
        now_utc = datetime(2021, 1, 1, 0, 2, tzinfo=UTC)
        result = sa._pct_change_completed(rates, 60, now_utc, 0)
        self.assertAlmostEqual(result, 1.0)  # 1% change

    def test__pct_change_completed_no_rates(self):
        result = sa._pct_change_completed(None, 60, datetime.now(UTC), 0)
        self.assertIsNone(result)

    @patch("monitor.cli.setup_analyzer.read_series_mt5")
    @patch("monitor.cli.setup_analyzer.analyze")
    @patch("monitor.cli.setup_analyzer.insert_results_to_db")
    def test_process_once_basic(self, mock_insert, mock_analyze, mock_read):
        # Create non-empty series to trigger analysis
        series = {"EURUSD": [sa.Snapshot(datetime.now(UTC), {"symbol": "EURUSD"})]}
        mock_read.return_value = series, None, datetime.now(UTC)
        mock_analyze.return_value = [], {}

        sa.process_once(["EURUSD"], 1.0, None, False, debug=False)

        mock_read.assert_called_once()
        mock_analyze.assert_called_once()
        mock_insert.assert_called_once()

    @patch("monitor.cli.setup_analyzer.read_series_mt5")
    @patch("monitor.cli.setup_analyzer.analyze")
    @patch("monitor.cli.setup_analyzer.insert_results_to_db")
    def test_process_once_with_exclude_set(self, mock_insert, mock_analyze, mock_read):
        series = {"EURUSD": [sa.Snapshot(datetime.now(UTC), {"symbol": "EURUSD"})]}
        mock_read.return_value = series, None, datetime.now(UTC)
        mock_analyze.return_value = [], {}

        exclude_set = {"EURUSD"}
        sa.process_once(
            ["EURUSD"], 1.0, None, False, debug=False, exclude_set=exclude_set
        )

        # Should filter out EURUSD before analysis
        mock_analyze.assert_called_once_with(
            {}, min_rrr=1.0, as_of_ts=unittest.mock.ANY, debug=False
        )

    @patch("monitor.cli.setup_analyzer.process_once")
    @patch("time.sleep")
    def test_watch_loop(self, mock_sleep, mock_process):
        mock_process.side_effect = [None, KeyboardInterrupt()]

        sa.watch_loop(
            ["EURUSD"], 1.0, None, False, False, debug=False, exclude_set=None
        )

        self.assertEqual(mock_process.call_count, 2)

    @patch("monitor.cli.setup_analyzer.process_once")
    @patch("time.sleep")
    def test_watch_loop_with_exclude_set(self, mock_sleep, mock_process):
        mock_process.side_effect = [None, KeyboardInterrupt()]
        exclude_set = {"EURUSD"}

        sa.watch_loop(
            ["EURUSD"], 1.0, None, False, True, debug=True, exclude_set=exclude_set
        )

        # First call should include exclude_set
        args, kwargs = mock_process.call_args_list[0]
        self.assertEqual(kwargs["exclude_set"], exclude_set)

    @patch("monitor.cli.setup_analyzer.watch_loop")
    @patch("monitor.cli.setup_analyzer.parse_args")
    @patch("monitor.cli.setup_analyzer._mt5_ensure_init")
    def test_main_watch_mode(self, mock_mt5_init, mock_parse_args, mock_watch):
        mock_mt5_init.return_value = True
        mock_args = SimpleNamespace(
            symbols="EURUSD,BTCUSD",
            min_rrr=1.0,
            top=None,
            brief=False,
            watch=True,
            interval=1.0,
            debug=False,
            exclude="EURUSD",
        )
        mock_parse_args.return_value = mock_args

        with patch("monitor.cli.setup_analyzer.mt5") as mock_mt5:
            mock_mt5.symbols_get.return_value = []
            sa.main()

            mock_watch.assert_called_once()

    @patch("monitor.cli.setup_analyzer.process_once")
    @patch("monitor.cli.setup_analyzer.parse_args")
    @patch("monitor.cli.setup_analyzer._mt5_ensure_init")
    def test_main_single_run_mode(self, mock_mt5_init, mock_parse_args, mock_process):
        mock_mt5_init.return_value = True
        mock_args = SimpleNamespace(
            symbols="EURUSD",
            min_rrr=1.0,
            top=5,
            brief=True,
            watch=False,
            interval=1.0,
            debug=False,
            exclude="",
        )
        mock_parse_args.return_value = mock_args

        with patch("monitor.cli.setup_analyzer.mt5") as mock_mt5:
            mock_mt5.symbols_get.return_value = []
            sa.main()

            mock_process.assert_called_once()


class SetupAnalyzerUtilityFunctionTests(unittest.TestCase):
    """Test utility functions in setup analyzer."""

    def test__rate_field_dict_access(self):
        rate = {"close": 1.05}
        result = sa._rate_field(rate, "close")
        self.assertEqual(result, 1.05)

    def test__rate_field_attribute_access(self):
        rate = SimpleNamespace(close=1.05)
        result = sa._rate_field(rate, "close")
        self.assertEqual(result, 1.05)

    def test__rate_field_missing(self):
        rate = {"high": 1.1}
        result = sa._rate_field(rate, "close")
        self.assertIsNone(result)

    def test__rate_time_utc(self):
        rate = {"time": 1609459200}  # 2021-01-01 00:00:00 UTC
        result = sa._rate_time_utc(rate, offset_hours=0)
        expected = datetime(2021, 1, 1, 0, 0, tzinfo=UTC)
        self.assertEqual(result, expected)

    def test__rate_time_utc_with_offset(self):
        rate = {"time": 1609459200}  # 2021-01-01 00:00:00 UTC
        result = sa._rate_time_utc(rate, offset_hours=2)
        expected = datetime(2020, 12, 31, 22, 0, tzinfo=UTC)
        self.assertEqual(result, expected)

    def test__rate_time_utc_invalid(self):
        rate = {"time": "invalid"}
        result = sa._rate_time_utc(rate, offset_hours=0)
        self.assertIsNone(result)

    def test_Snapshot_g_method(self):
        row = {"close": 1.05, "high": "1.1"}
        snapshot = sa.Snapshot(datetime.now(UTC), row)

        # Test with float
        self.assertEqual(snapshot.g("close"), 1.05)

        # Test with string that can be parsed
        self.assertEqual(snapshot.g("high"), 1.1)

        # Test missing field
        self.assertIsNone(snapshot.g("low"))

    def test_to_input_tz_with_aware_datetime(self):
        dt = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        result = sa.to_input_tz(dt)
        self.assertEqual(result.tzinfo, sa.INPUT_TZ)

    def test_to_input_tz_with_naive_datetime(self):
        dt = datetime(2024, 1, 1, 12, 0)
        result = sa.to_input_tz(dt)
        self.assertEqual(result.tzinfo, sa.INPUT_TZ)
        self.assertEqual(result.hour, 12)  # Should not change the time

    def test_utc_naive(self):
        dt = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        result = sa.utc_naive(dt)
        self.assertIsNone(result.tzinfo)
        self.assertEqual(result.hour, 12)


if __name__ == "__main__":
    unittest.main()
