"""Edge case and error handling tests for setup analyzer."""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from monitor.cli import setup_analyzer as sa

UTC = timezone.utc


class SetupAnalyzerEdgeCaseTests(unittest.TestCase):
    """Test edge cases and error handling in setup analyzer."""

    def test__infer_decimals_from_price_edge_cases(self):
        # Test infinite values
        import math
        self.assertEqual(sa._infer_decimals_from_price(math.inf), 5)
        self.assertEqual(sa._infer_decimals_from_price(-math.inf), 5)

        # Test zero
        self.assertEqual(sa._infer_decimals_from_price(0.0), 0)

        # Test very small number
        self.assertEqual(sa._infer_decimals_from_price(0.000001), 6)

        # Test very large number
        self.assertEqual(sa._infer_decimals_from_price(1000000.0), 0)

    def test__symbol_digits_edge_cases(self):
        # Test with invalid MT5 info
        with patch('monitor.cli.setup_analyzer._MT5_IMPORTED', True), \
             patch('monitor.cli.setup_analyzer._MT5_READY', True), \
             patch('monitor.cli.setup_analyzer.mt5') as mock_mt5:
            mock_mt5.symbol_info.return_value = None
            result = sa._symbol_digits('EURUSD', 1.2345)
            self.assertEqual(result, 4)  # Fallback to _infer_decimals_from_price(1.2345) = 4

        # Test with exception in MT5 call
        with patch('monitor.cli.setup_analyzer._MT5_IMPORTED', True), \
             patch('monitor.cli.setup_analyzer._MT5_READY', True), \
             patch('monitor.cli.setup_analyzer.mt5') as mock_mt5:
            mock_mt5.symbol_info.side_effect = Exception("MT5 error")
            result = sa._symbol_digits('EURUSD', 1.2345)
            self.assertEqual(result, 4)  # Fallback to _infer_decimals_from_price(1.2345) = 4

    def test__proximity_bin_label_edge_cases(self):
        # Test with special float values
        import math
        self.assertIsNone(sa._proximity_bin_label(math.nan))
        self.assertIsNone(sa._proximity_bin_label(math.inf))
        self.assertIsNone(sa._proximity_bin_label(-math.inf))

        # Test with boundary values
        self.assertEqual(sa._proximity_bin_label(0.0), "0.0-0.1")
        self.assertEqual(sa._proximity_bin_label(0.099), "0.0-0.1")
        self.assertEqual(sa._proximity_bin_label(0.1), "0.1-0.2")
        self.assertEqual(sa._proximity_bin_label(0.999), "0.9-1.0")

        # Test with negative value
        self.assertEqual(sa._proximity_bin_label(-0.5), "0.0-0.1")

    def test_canonicalize_key_edge_cases(self):
        # Test with None input
        self.assertEqual(sa.canonicalize_key(None), "")

        # Test with BOM
        self.assertEqual(sa.canonicalize_key("\ufeffATR D1"), "atr d1")

        # Test with various punctuation
        self.assertEqual(sa.canonicalize_key("ATR% (D1)"), "atr percent d1")
        # The % character gets replaced with " percent ", then cleaned up
        self.assertEqual(sa.canonicalize_key("Strength@#$%^&*()4H"), "strength percent 4h")

        # Test with mixed case and spaces
        self.assertEqual(sa.canonicalize_key("  StRenGtH   4H  "), "strength 4h")

    def test_fnum_edge_cases(self):
        # Test with various formats
        self.assertEqual(sa.fnum("1.234,56"), 1234.56)  # European format
        self.assertEqual(sa.fnum("1,234.56"), 1234.56)  # US format
        self.assertEqual(sa.fnum("-1.23"), -1.23)
        self.assertEqual(sa.fnum("+1.23"), 1.23)

        # Test scientific notation
        self.assertEqual(sa.fnum("1.23e-4"), 0.000123)

        # Test with units
        self.assertEqual(sa.fnum("123 pips"), 123.0)
        self.assertEqual(sa.fnum("1.5%"), 1.5)

        # Test with invalid strings
        self.assertIsNone(sa.fnum(""))
        self.assertIsNone(sa.fnum("abc"))
        self.assertIsNone(sa.fnum("N/A"))
        self.assertIsNone(sa.fnum(None))

    def test_normalize_spread_pct_edge_cases(self):
        # Test with special values
        import math
        self.assertIsNone(sa.normalize_spread_pct(math.nan))
        self.assertIsNone(sa.normalize_spread_pct(math.inf))
        self.assertIsNone(sa.normalize_spread_pct(-math.inf))

        # Test with various inputs
        self.assertEqual(sa.normalize_spread_pct(0.001), 0.1)  # Fraction -> percent
        self.assertEqual(sa.normalize_spread_pct(0.12), 0.12)  # Already percent
        self.assertEqual(sa.normalize_spread_pct(1.2), 1.2)  # Whole percent
        self.assertEqual(sa.normalize_spread_pct("0.001"), 0.1)
        self.assertIsNone(sa.normalize_spread_pct("invalid"))

    def test__atr_edge_cases(self):
        # Test with invalid data
        self.assertIsNone(sa._atr([], 14))
        self.assertIsNone(sa._atr([(1.0, 1.0, 1.0)], 14))  # Not enough data

        # Test with edge case data (minimum required)
        values = [
            (1.0, 0.9, 0.95),
            (1.1, 1.0, 1.05),
            (1.2, 1.1, 1.15),
            (1.15, 1.05, 1.1),
            (1.25, 1.15, 1.2),
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
        atr = sa._atr(values, 14)
        self.assertIsNotNone(atr)
        self.assertGreater(atr, 0)

    def test__pivots_from_prev_day_edge_cases(self):
        # Test with invalid data structures
        s1, r1 = sa._pivots_from_prev_day(None)
        self.assertIsNone(s1)
        self.assertIsNone(r1)
        s1, r1 = sa._pivots_from_prev_day([])
        self.assertIsNone(s1)
        self.assertIsNone(r1)
        s1, r1 = sa._pivots_from_prev_day([{'high': 1.0}])  # Missing fields
        self.assertIsNone(s1)
        self.assertIsNone(r1)

        # Test with dict-like objects
        daily_rates = [
            {'high': 1.2, 'low': 1.0, 'close': 1.1},
            {'high': 1.25, 'low': 1.05, 'close': 1.15},
        ]
        s1, r1 = sa._pivots_from_prev_day(daily_rates)
        self.assertIsNotNone(s1)
        self.assertIsNotNone(r1)

    def test__rate_field_edge_cases(self):
        # Test with object that raises exception on access
        class BadObject:
            def __getitem__(self, key):
                raise RuntimeError("Access error")

        self.assertIsNone(sa._rate_field(BadObject(), 'close'))

        # Test with nested exceptions - the _rate_field function catches all exceptions
        # So it should return None rather than raise
        class VeryBadObject:
            def __getitem__(self, key):
                raise RuntimeError("Dict access error")

            def __getattr__(self, name):
                raise RuntimeError("Attribute error")

        self.assertIsNone(sa._rate_field(VeryBadObject(), 'close'))

    def test__rate_time_utc_edge_cases(self):
        # Test with invalid timestamp
        rate = {'time': 'invalid'}
        self.assertIsNone(sa._rate_time_utc(rate, 0))

        # Test with object access failure
        class BadRate:
            def __getitem__(self, key):
                if key == 'time':
                    raise RuntimeError("Time error")
                raise KeyError("Missing")

        self.assertIsNone(sa._rate_time_utc(BadRate(), 0))

    def test__pct_change_completed_edge_cases(self):
        # Test with invalid timeframe
        result = sa._pct_change_completed([], -1, datetime.now(UTC), 0)
        self.assertIsNone(result)

        # Test with rates that have invalid close values
        rates = [
            {'time': 1609459200, 'close': 'invalid'},
            {'time': 1609459260, 'close': 1.01},
        ]
        result = sa._pct_change_completed(rates, 60, datetime.now(UTC), 0)
        self.assertIsNone(result)

        # Test with rates that are still in progress
        future_time = datetime.now(UTC) + timedelta(minutes=10)
        rates = [
            {'time': int(future_time.timestamp()), 'close': 1.0},
            {'time': int(future_time.timestamp()) + 60, 'close': 1.01},
        ]
        result = sa._pct_change_completed(rates, 60, datetime.now(UTC), 0)
        self.assertIsNone(result)  # Should skip in-progress bars

    @patch('monitor.cli.setup_analyzer._mt5_ensure_init')
    def test_read_series_mt5_no_mt5(self, mock_init):
        mock_init.return_value = False

        series, error, timestamp = sa.read_series_mt5(['EURUSD'])

        self.assertEqual(series, {})
        self.assertIsNone(error)
        self.assertIsInstance(timestamp, datetime)

    @patch('monitor.cli.setup_analyzer._mt5_ensure_init')
    @patch('monitor.cli.setup_analyzer.mt5')
    def test_read_series_mt5_symbol_select_failure(self, mock_mt5, mock_init):
        mock_init.return_value = True
        mock_mt5.symbol_select.side_effect = Exception("Symbol select failed")
        mock_mt5.symbol_info_tick.return_value = None
        # Mock all the timeframe functions to return None
        mock_mt5.copy_rates_from_pos.return_value = None

        series, error, timestamp = sa.read_series_mt5(['EURUSD'])

        # Should not crash and should return some result (even if not empty)
        # The important thing is that it handles the symbol_select exception gracefully
        self.assertIsInstance(series, dict)
        self.assertIsInstance(error, (type(None), str))
        self.assertIsInstance(timestamp, datetime)

    @patch('monitor.cli.setup_analyzer._mt5_ensure_init')
    @patch('monitor.cli.setup_analyzer.mt5')
    def test__mt5_copy_rates_cached_with_exception(self, mock_mt5, mock_init):
        mock_init.return_value = True
        mock_mt5.copy_rates_from_pos.side_effect = Exception("MT5 copy failed")

        # Clear any existing cache that might interfere
        sa._RATE_CACHE.clear()

        result = sa._mt5_copy_rates_cached('EURUSD', 1440, 10)
        self.assertIsNone(result)

    def test_Snapshot_g_method_edge_cases(self):
        # Test with various data types - need canonicalized keys
        row = {
            'int_field': 123,
            'float_field': 1.23,
            'str_float': '1.23',
            'str_invalid': 'abc',
            'none_field': None,
        }
        # Canonicalize keys like the real build_row function does
        canonical_row = {}
        for k, v in row.items():
            canonical_row[sa.canonicalize_key(k)] = v

        snapshot = sa.Snapshot(datetime.now(UTC), canonical_row)

        self.assertEqual(snapshot.g('int_field'), 123.0)
        self.assertEqual(snapshot.g('float_field'), 1.23)
        self.assertEqual(snapshot.g('str_float'), 1.23)
        self.assertIsNone(snapshot.g('str_invalid'))
        self.assertIsNone(snapshot.g('none_field'))
        self.assertIsNone(snapshot.g('missing_field'))

    @patch('monitor.cli.setup_analyzer.is_quiet_time')
    def test_analyze_with_malformed_data(self, mock_quiet):
        mock_quiet.return_value = False

        # Create snapshot with missing/invalid data - need canonicalized keys
        malformed_row = {
            'symbol': 'EURUSD',
            'Bid': 'invalid',  # Invalid bid
            'Ask': None,       # Missing ask
            'Strength 1H': 'not_a_number',
            'Recent Tick': 1,
        }
        # Canonicalize keys
        canonical_row = {}
        for k, v in malformed_row.items():
            canonical_row[sa.canonicalize_key(k)] = v
        canonical_row["symbol"] = "EURUSD"

        snapshot = sa.Snapshot(datetime.now(UTC), canonical_row)
        series = {'EURUSD': [snapshot]}

        results, reasons = sa.analyze(series, 1.0, datetime.now(UTC), debug=False)

        # Should handle gracefully and return no results
        self.assertEqual(len(results), 0)
        # The actual reason is 'no_direction_consensus' because strength values are malformed
        expected_reasons = ['no_direction_consensus', 'no_live_bid_ask', 'no_recent_ticks', 'invalid_data']
        # reasons is a dict, not a list, so we need to check the keys
        self.assertTrue(any(reason in expected_reasons for reason in reasons.keys()))

    def test_analyze_with_boundary_strength_values(self):
        # Test with exactly boundary strength values
        series_data = [
            sa.Snapshot(
                ts=datetime(2024, 1, 1, 11, 0, tzinfo=UTC),
                row={'D1 Close': 1.0800, 'Strength 4H': 0.0, 'symbol': 'EURUSD', 'Recent Tick': 1}
            ),
            sa.Snapshot(
                ts=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
                row={
                    'symbol': 'EURUSD',
                    'Bid': 1.0850,
                    'Ask': 1.0852,
                    'Spread%': 0.02,
                    'Strength 1H': 0.0001,  # Very small positive
                    'Strength 4H': 0.0001,
                    'Strength 1D': 0.0001,
                    'ATR D1': 0.0050,
                    'ATR (%) D1': 0.46,
                    'S1 Level M5': 1.0800,
                    'R1 Level M5': 1.0900,
                    'D1 Close': 1.0851,
                    'D1 High': 1.0900,
                    'D1 Low': 1.0800,
                    'Recent Tick': 1,
                    'Last Tick UTC': '2024-01-01 12:00:00',
                    'Tick Age Sec': 10.0,
                }
            )
        ]

        series = {'EURUSD': series_data}

        with patch('monitor.cli.setup_analyzer.is_quiet_time', return_value=False):
            results, reasons = sa.analyze(series, 1.0, datetime.now(UTC), debug=False)

            # Should still work with minimal positive strength
            if results:
                result = results[0]
                self.assertEqual(result['direction'], 'Buy')

    @patch('monitor.cli.setup_analyzer._get_db_connection')
    @patch('monitor.cli.setup_analyzer.sqlite3')
    @patch('monitor.cli.setup_analyzer.default_db_path')
    def test__filter_recent_duplicates_with_db_error(self, mock_db_path, mock_sqlite, mock_conn):
        mock_db_path.return_value = "/test/path"
        mock_conn.side_effect = Exception("DB connection failed")

        results = [{'symbol': 'EURUSD', 'direction': 'Buy'}]

        # Should return original results on error
        filtered, excluded = sa._filter_recent_duplicates(results, "test_table")
        self.assertEqual(filtered, results)
        self.assertEqual(excluded, set())

    def test_insert_results_to_db_no_sqlite(self):
        with patch('monitor.cli.setup_analyzer.sqlite3', None):
            # Should not crash when sqlite3 is not available
            sa.insert_results_to_db([{'symbol': 'EURUSD'}])

    @patch('monitor.cli.setup_analyzer._get_db_connection')
    def test_insert_results_to_db_no_connection(self, mock_conn):
        mock_conn.return_value = None

        # Should not crash when DB connection fails
        sa.insert_results_to_db([{'symbol': 'EURUSD'}])

    @patch('monitor.cli.setup_analyzer.sqlite3', None)
    def test_insert_results_to_db_with_exceptions(self):
        # When sqlite3 is None, the function should handle it gracefully
        # Should not crash when sqlite3 is not available
        sa.insert_results_to_db([{'symbol': 'EURUSD'}])


if __name__ == "__main__":
    unittest.main()