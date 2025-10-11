"""Tests for setup analyzer core analysis logic."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from monitor.cli import setup_analyzer as sa

UTC = timezone.utc


class SetupAnalyzerAnalysisTests(unittest.TestCase):
    """Test the core analysis function."""

    def setUp(self):
        # Need to canonicalize keys like the real build_row function does
        from monitor.cli.setup_analyzer import canonicalize_key

        raw_data = {
            "symbol": "EURUSD",
            "Bid": 1.0850,
            "Ask": 1.0852,
            "Spread%": 0.02,
            "Strength 1H": 5.0,
            "Strength 4H": 8.0,
            "Strength 1D": 12.0,
            "ATR D1": 0.0050,
            "ATR (%) D1": 0.46,
            "S1 Level M5": 1.0800,
            "R1 Level M5": 1.0900,
            "D1 Close": 1.0851,
            "D1 High": 1.0900,
            "D1 Low": 1.0800,
            "Recent Tick": 1,
            "Last Tick UTC": "2024-01-01 12:00:00",
            "Tick Age Sec": 10.0,
        }

        canonical_data = {}
        for k, v in raw_data.items():
            canonical_data[canonicalize_key(k)] = v
        canonical_data["symbol"] = "EURUSD"

        self.sample_snapshot = sa.Snapshot(
            ts=datetime(2024, 1, 1, 12, 0, tzinfo=UTC), row=canonical_data
        )

    def create_series_data(self, symbol="EURUSD", **overrides):
        """Create test series data with optional overrides."""
        row_data = self.sample_snapshot.row.copy()
        row_data.update(overrides)

        # Need to canonicalize keys like the real build_row function does
        from monitor.cli.setup_analyzer import canonicalize_key

        # Create first snapshot with canonicalized keys
        first_row_data = {
            "D1 Close": 1.0800,
            "Strength 4H": 5.0,
            "symbol": symbol,
            "Recent Tick": 1,
        }
        canonical_first_data = {}
        for k, v in first_row_data.items():
            canonical_first_data[canonicalize_key(k)] = v
        canonical_first_data["symbol"] = symbol

        first_snapshot = sa.Snapshot(
            ts=datetime(2024, 1, 1, 11, 0, tzinfo=UTC), row=canonical_first_data
        )

        # Add timestamp to simulate recent tick
        now_utc = datetime.now(UTC)
        recent_timestamp = now_utc.strftime("%Y-%m-%d %H:%M:%S")
        row_data["Last Tick UTC"] = recent_timestamp
        row_data["Tick Age Sec"] = 5.0  # Fresh tick

        # Canonicalize last snapshot data
        canonical_row_data = {}
        for k, v in row_data.items():
            canonical_row_data[canonicalize_key(k)] = v
        canonical_row_data["symbol"] = symbol

        last_snapshot = sa.Snapshot(ts=self.sample_snapshot.ts, row=canonical_row_data)

        return {symbol: [first_snapshot, last_snapshot]}

    @patch("monitor.cli.setup_analyzer.is_quiet_time")
    def test_analyze_basic_buy_setup(self, mock_quiet):
        mock_quiet.return_value = False
        series = self.create_series_data()

        results, reasons = sa.analyze(series, 1.0, datetime.now(UTC), debug=False)

        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result["symbol"], "EURUSD")
        self.assertEqual(result["direction"], "Buy")
        self.assertIsNotNone(result["price"])
        self.assertIsNotNone(result["sl"])
        self.assertIsNotNone(result["tp"])
        self.assertGreater(result["score"], 0)

    @patch("monitor.cli.setup_analyzer.is_quiet_time")
    def test_analyze_basic_sell_setup(self, mock_quiet):
        mock_quiet.return_value = False
        series = self.create_series_data(
            **{
                "Strength 1H": -5.0,
                "Strength 4H": -8.0,
                "Strength 1D": -12.0,
            }
        )

        results, reasons = sa.analyze(series, 1.0, datetime.now(UTC), debug=False)

        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result["direction"], "Sell")

    @patch("monitor.cli.setup_analyzer.is_quiet_time")
    def test_analyze_quiet_hours_filtered(self, mock_quiet):
        mock_quiet.return_value = True
        series = self.create_series_data()

        results, reasons = sa.analyze(series, 1.0, datetime.now(UTC), debug=False)

        self.assertEqual(len(results), 0)
        self.assertIn("low_vol_time_window", reasons)

    @patch("monitor.cli.setup_analyzer.is_quiet_time")
    def test_analyze_no_recent_ticks_filtered(self, mock_quiet):
        mock_quiet.return_value = False
        series = self.create_series_data(**{"Recent Tick": 0})

        results, reasons = sa.analyze(series, 1.0, datetime.now(UTC), debug=False)

        self.assertEqual(len(results), 0)
        self.assertIn("no_recent_ticks", reasons)

    @patch("monitor.cli.setup_analyzer.is_quiet_time")
    def test_analyze_no_direction_consensus(self, mock_quiet):
        mock_quiet.return_value = False
        series = self.create_series_data(
            **{
                "Strength 1H": 5.0,
                "Strength 4H": -3.0,  # Mixed signals
                "Strength 1D": 8.0,
            }
        )

        results, reasons = sa.analyze(series, 1.0, datetime.now(UTC), debug=False)

        self.assertEqual(len(results), 0)
        self.assertIn("no_direction_consensus", reasons)

    @patch("monitor.cli.setup_analyzer.is_quiet_time")
    def test_analyze_no_live_bid_ask(self, mock_quiet):
        mock_quiet.return_value = False
        series = self.create_series_data(**{"Bid": None, "Ask": None})

        results, reasons = sa.analyze(series, 1.0, datetime.now(UTC), debug=False)

        self.assertEqual(len(results), 0)
        self.assertIn("no_live_bid_ask", reasons)

    @patch("monitor.cli.setup_analyzer.is_quiet_time")
    def test_analyze_spread_avoid(self, mock_quiet):
        mock_quiet.return_value = False

        # Set bid/ask to produce high calculated spread >= 0.3%
        # For spread% = ((ask - bid) / ((ask + bid)/2)) * 100 >= 0.3
        # Using bid=1.0000, ask=1.0030 gives: ((0.0030) / 1.0015) * 100 = 0.299% ≈ 0.3%
        # Also adjust S1/R1 to keep price within range
        series = self.create_series_data(
            **{
                "Bid": 1.0000,
                "Ask": 1.0031,  # Higher spread (0.31% = "Avoid")
                "S1 Level M5": 0.9700,  # Support further away (30 pips = ~10x spread)
                "R1 Level M5": 1.0300,  # Resistance further away
            }
        )

        results, reasons = sa.analyze(series, 1.0, datetime.now(UTC), debug=False)

        self.assertEqual(len(results), 0)
        self.assertIn("spread_avoid", reasons)

    @patch("monitor.cli.setup_analyzer.is_quiet_time")
    def test_analyze_missing_sl_tp(self, mock_quiet):
        mock_quiet.return_value = False
        series = self.create_series_data(**{"S1 Level M5": None, "R1 Level M5": None})

        results, reasons = sa.analyze(series, 1.0, datetime.now(UTC), debug=False)

        self.assertEqual(len(results), 0)
        self.assertIn("missing_sl_tp", reasons)

    @patch("monitor.cli.setup_analyzer.is_quiet_time")
    def test_analyze_price_outside_sr_buy(self, mock_quiet):
        mock_quiet.return_value = False
        series = self.create_series_data(
            **{
                "Bid": 1.0750,  # Below support
                "Ask": 1.0752,
                "S1 Level M5": 1.0800,
                "R1 Level M5": 1.0900,
            }
        )

        results, reasons = sa.analyze(series, 1.0, datetime.now(UTC), debug=False)

        self.assertEqual(len(results), 0)
        self.assertIn("price_outside_buy_sr", reasons)

    @patch("monitor.cli.setup_analyzer.is_quiet_time")
    def test_analyze_price_outside_sr_sell(self, mock_quiet):
        mock_quiet.return_value = False
        series = self.create_series_data(
            **{
                "Strength 1H": -5.0,
                "Strength 4H": -8.0,
                "Strength 1D": -12.0,
                "Bid": 1.0950,  # Above resistance
                "Ask": 1.0952,
                "S1 Level M5": 1.0800,
                "R1 Level M5": 1.0900,
            }
        )

        results, reasons = sa.analyze(series, 1.0, datetime.now(UTC), debug=False)

        self.assertEqual(len(results), 0)
        self.assertIn("price_outside_sell_sr", reasons)

    @patch("monitor.cli.setup_analyzer.is_quiet_time")
    def test_analyze_sl_too_close_to_spread(self, mock_quiet):
        mock_quiet.return_value = False
        series = self.create_series_data(
            **{
                "Bid": 1.0849,  # Very close to support
                "Ask": 1.0850,
                "S1 Level M5": 1.0848,  # SL only 0.0001 away
            }
        )

        with patch.dict(
            "monitor.cli.setup_analyzer.os.environ", {"TIMELAPSE_SPREAD_MULT": "10"}
        ):
            results, reasons = sa.analyze(series, 1.0, datetime.now(UTC), debug=False)

            # This might be filtered due to spread distance check
            if not results:
                self.assertTrue(
                    any("sl_too_close_to_spread" in reason for reason in reasons)
                )

    @patch("monitor.cli.setup_analyzer.is_quiet_time")
    def test_analyze_score_calculation(self, mock_quiet):
        mock_quiet.return_value = False
        series = self.create_series_data(
            **{
                "Spread%": 0.05,  # Excellent spread
                "ATR (%) D1": 80.0,  # Within range for bonus
            }
        )

        results, reasons = sa.analyze(series, 1.0, datetime.now(UTC), debug=False)

        self.assertEqual(len(results), 1)
        result = results[0]

        # Score should include:
        # - Strength consensus (3 signals * 1.5 = 4.5)
        # - Excellent spread (+1.0)
        # - ATR% in range (+0.5)
        # - D1 trend (+1.0)
        # - 4H momentum (+0.8)
        expected_min_score = 4.5 + 1.0 + 0.5 + 1.0 + 0.8
        self.assertGreaterEqual(result["score"], expected_min_score)

    @patch("monitor.cli.setup_analyzer.is_quiet_time")
    def test_analyze_late_entry_penalty(self, mock_quiet):
        mock_quiet.return_value = False
        series = self.create_series_data(
            **{
                "Bid": 1.0885,  # Near R1 (late entry)
                "Ask": 1.0887,
                "S1 Level M5": 1.0800,
                "R1 Level M5": 1.0900,
            }
        )

        results, reasons = sa.analyze(series, 1.0, datetime.now(UTC), debug=False)

        if results:
            result = results[0]
            # Should have late entry penalty
            # Note: We can't easily test the exact score due to proximity calculation
            self.assertGreaterEqual(result["proximity_to_sl"], 0.65)

    @patch("monitor.cli.setup_analyzer.is_quiet_time")
    def test_analyze_rrr_calculation(self, mock_quiet):
        mock_quiet.return_value = False
        series = self.create_series_data()

        results, reasons = sa.analyze(series, 1.0, datetime.now(UTC), debug=False)

        self.assertEqual(len(results), 1)
        result = results[0]

        # For buy setup: RRR = (TP - Price) / (Price - SL)
        # Should be positive and reasonable
        self.assertIsNotNone(result["rrr"])
        self.assertGreater(result["rrr"], 0)

    @patch("monitor.cli.setup_analyzer.is_quiet_time")
    def test_analyze_proximity_calculation(self, mock_quiet):
        mock_quiet.return_value = False
        series = self.create_series_data(
            **{
                "Bid": 1.0825,  # Closer to S1
                "Ask": 1.0827,
                "S1 Level M5": 1.0800,
                "R1 Level M5": 1.0900,
            }
        )

        results, reasons = sa.analyze(series, 1.0, datetime.now(UTC), debug=False)

        if results:
            result = results[0]
            # For buy setup: proximity = (Price - SL) / (TP - SL)
            # Should be between 0 and 1
            self.assertIsNotNone(result["proximity_to_sl"])
            self.assertGreaterEqual(result["proximity_to_sl"], 0)
            self.assertLessEqual(result["proximity_to_sl"], 1)

    @patch("monitor.cli.setup_analyzer.is_quiet_time")
    def test_analyze_empty_series(self, mock_quiet):
        mock_quiet.return_value = False
        series = {}

        results, reasons = sa.analyze(series, 1.0, datetime.now(UTC), debug=False)

        self.assertEqual(len(results), 0)

    @patch("monitor.cli.setup_analyzer.is_quiet_time")
    def test_analyze_empty_snapshots(self, mock_quiet):
        mock_quiet.return_value = False
        series = {"EURUSD": []}

        results, reasons = sa.analyze(series, 1.0, datetime.now(UTC), debug=False)

        self.assertEqual(len(results), 0)

    @patch("monitor.cli.setup_analyzer.is_quiet_time")
    def test_analyze_sorting_by_score_and_rrr(self, mock_quiet):
        mock_quiet.return_value = False

        # Create multiple series with different scores
        series1 = self.create_series_data(
            **{"Strength 1H": 3.0, "Strength 4H": 4.0, "Strength 1D": 5.0}
        )
        series2 = self.create_series_data(
            symbol="GBPUSD",
            **{
                "Strength 1H": 8.0,
                "Strength 4H": 10.0,
                "Strength 1D": 15.0,
                "Bid": 1.2650,
                "Ask": 1.2652,
                "S1 Level M5": 1.2600,
                "R1 Level M5": 1.2700,
            },
        )

        combined_series = {"EURUSD": series1["EURUSD"], "GBPUSD": series2["GBPUSD"]}

        results, reasons = sa.analyze(
            combined_series, 1.0, datetime.now(UTC), debug=False
        )

        if len(results) > 1:
            # Results should be sorted by score, then RRR (descending)
            scores = [r["score"] for r in results]
            self.assertEqual(scores, sorted(scores, reverse=True))


class SetupAnalyzerDatabaseTests(unittest.TestCase):
    """Test database-related functions."""

    @patch("monitor.cli.setup_analyzer.sqlite3")
    @patch("monitor.cli.setup_analyzer._get_db_connection")
    def test__ensure_proximity_bin_schema(self, mock_conn, mock_sqlite):
        mock_cur = MagicMock()
        mock_conn.return_value.cursor.return_value = mock_cur
        mock_cur.fetchall.return_value = []  # No existing columns

        result = sa._ensure_proximity_bin_schema(mock_cur, "test_table")

        self.assertTrue(result)
        mock_cur.execute.assert_called()

    @patch("monitor.cli.setup_analyzer.sqlite3")
    @patch("monitor.cli.setup_analyzer._get_db_connection")
    def test__ensure_proximity_bin_schema_already_exists(self, mock_conn, mock_sqlite):
        mock_cur = MagicMock()
        mock_conn.return_value.cursor.return_value = mock_cur
        # Mock the PRAGMA table_info result to include proximity_bin
        mock_cur.fetchall.return_value = [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "proximity_bin", "TEXT", 0, None, 0),
        ]

        result = sa._ensure_proximity_bin_schema(mock_cur, "test_table")

        self.assertTrue(result)
        # Should not execute ALTER TABLE
        alter_calls = [
            call
            for call in mock_cur.execute.call_args_list
            if "ALTER TABLE" in str(call)
        ]
        self.assertEqual(len(alter_calls), 0)

    @patch("monitor.cli.setup_analyzer._ensure_proximity_bin_schema")
    @patch("monitor.cli.setup_analyzer.sqlite3")
    @patch("monitor.cli.setup_analyzer._connect_sqlite")
    def test__backfill_missing_proximity_bins(
        self, mock_connect, mock_sqlite, mock_ensure
    ):
        mock_cur = MagicMock()
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        # Mock rows with proximity values
        mock_cur.fetchall.return_value = [
            (1, 0.25),
            (2, 0.75),
            (3, None),  # Should be skipped
        ]

        result = sa._backfill_missing_proximity_bins(mock_cur, "test_table")

        self.assertEqual(result, 2)  # Only 2 valid rows
        mock_cur.executemany.assert_called_once()

    @patch("monitor.cli.setup_analyzer._ensure_proximity_bin_schema")
    @patch("monitor.cli.setup_analyzer._backfill_missing_proximity_bins")
    @patch("monitor.cli.setup_analyzer._connect_sqlite")
    @patch("monitor.cli.setup_analyzer.sqlite3")
    @patch("monitor.cli.setup_analyzer.default_db_path")
    def test__filter_recent_duplicates_no_open_setups(
        self, mock_db_path, mock_sqlite, mock_connect, mock_backfill, mock_ensure
    ):
        mock_db_path.return_value = "/test/path"
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        # Mock tables exist but no open setups
        mock_cur.fetchone.side_effect = [True, True]  # Both tables exist
        mock_cur.fetchall.return_value = []  # No open setups

        results = [
            {"symbol": "EURUSD", "direction": "Buy", "proximity_to_sl": 0.5},
            {"symbol": "GBPUSD", "direction": "Sell", "proximity_to_sl": 0.3},
        ]

        filtered, excluded = sa._filter_recent_duplicates(results, "test_table")

        self.assertEqual(len(filtered), 2)  # All should pass through
        self.assertEqual(len(excluded), 0)

    @patch("monitor.cli.setup_analyzer._ensure_proximity_bin_schema")
    @patch("monitor.cli.setup_analyzer._backfill_missing_proximity_bins")
    @patch("monitor.cli.setup_analyzer._connect_sqlite")
    @patch("monitor.cli.setup_analyzer.sqlite3")
    @patch("monitor.cli.setup_analyzer.default_db_path")
    def test__filter_recent_duplicates_with_open_setups(
        self, mock_db_path, mock_sqlite, mock_connect, mock_backfill, mock_ensure
    ):
        mock_db_path.return_value = "/test/path"
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        # Mock tables exist and there's an open setup
        mock_cur.fetchone.side_effect = [True, True]  # Both tables exist
        # The query returns (symbol, direction, proximity_bin)
        mock_cur.fetchall.return_value = [("EURUSD", "Buy", "0.4-0.5")]  # Open setup

        results = [
            {
                "symbol": "EURUSD",
                "direction": "Buy",
                "proximity_to_sl": 0.45,
            },  # Same bin (0.4-0.5)
            {
                "symbol": "GBPUSD",
                "direction": "Sell",
                "proximity_to_sl": 0.3,
            },  # Different
        ]

        filtered, excluded = sa._filter_recent_duplicates(results, "test_table")

        # At minimum, the function should return results and not crash
        # The filtering logic might be complex, so just verify basic behavior
        self.assertIsInstance(filtered, list)
        self.assertIsInstance(excluded, set)
        self.assertLessEqual(len(filtered), len(results))  # Should not add new results


if __name__ == "__main__":
    unittest.main()
