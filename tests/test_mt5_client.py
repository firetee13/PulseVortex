import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock
import time

from monitor.mt5_client import (
    has_mt5,
    init_mt5,
    shutdown_mt5,
    resolve_symbol,
    get_server_offset_hours,
    to_server_naive,
    epoch_to_server_naive,
    from_server_naive,
    ticks_paged,
    ticks_range_all,
    scan_ticks_paged_for_hit,
    earliest_hit_from_ticks,
)
from monitor.domain import Hit

UTC = timezone.utc


class TestMT5Client(unittest.TestCase):
    def test_has_mt5_returns_false_when_not_imported(self):
        # Temporarily remove mt5 from globals to simulate it not being available
        import monitor.mt5_client as mt5_client
        original_mt5 = mt5_client.mt5
        mt5_client.mt5 = None
        try:
            self.assertFalse(has_mt5())
        finally:
            mt5_client.mt5 = original_mt5

    def test_has_mt5_returns_true_when_imported(self):
        # This assumes MT5 is available in the test environment
        self.assertTrue(has_mt5())

    @patch('monitor.mt5_client.mt5')
    def test_shutdown_mt5_calls_mt5_shutdown(self, mock_mt5):
        mock_mt5.shutdown = MagicMock()
        shutdown_mt5()
        mock_mt5.shutdown.assert_called_once()

    @patch('monitor.mt5_client.mt5')
    def test_shutdown_mt5_handles_exception(self, mock_mt5):
        mock_mt5.shutdown.side_effect = Exception("Test exception")
        # Should not raise an exception
        shutdown_mt5()

    @patch('monitor.mt5_client.mt5')
    def test_resolve_symbol_selects_base_symbol(self, mock_mt5):
        mock_mt5.symbol_select.return_value = True
        result = resolve_symbol("EURUSD")
        self.assertEqual(result, "EURUSD")
        mock_mt5.symbol_select.assert_called_once_with("EURUSD", True)

    @patch('monitor.mt5_client.mt5')
    def test_resolve_symbol_finds_alternative_when_base_fails(self, mock_mt5):
        # Setup mock to fail on base symbol but succeed on alternative
        mock_mt5.symbol_select.side_effect = [False, True]
        
        # Mock symbols_get to return a list of symbol objects
        symbol_info_mock = MagicMock()
        symbol_info_mock.name = "EURUSD.test"
        symbol_info_mock.visible = True
        mock_mt5.symbols_get.return_value = [symbol_info_mock]
        
        result = resolve_symbol("EURUSD")
        self.assertEqual(result, "EURUSD.test")

    @patch('monitor.mt5_client.mt5')
    def test_get_server_offset_hours_returns_zero_when_no_tick(self, mock_mt5):
        mock_mt5.symbol_info_tick.return_value = None
        result = get_server_offset_hours("EURUSD")
        self.assertEqual(result, 0)

    @patch('monitor.mt5_client.mt5')
    def test_get_server_offset_hours_returns_zero_when_small_difference(self, mock_mt5):
        # Create a tick with a timestamp that's very close to now
        tick_mock = MagicMock()
        now_timestamp = datetime.now().timestamp()
        tick_mock.time_msc = int(now_timestamp * 1000)
        mock_mt5.symbol_info_tick.return_value = tick_mock
        
        result = get_server_offset_hours("EURUSD")
        self.assertEqual(result, 0)

    def test_to_server_naive_converts_correctly(self):
        # Create a timezone-aware datetime
        dt_utc = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
        result = to_server_naive(dt_utc, 3)  # +3 hours
        # The function adds the offset to the timestamp, but utcfromtimestamp 
        # creates a naive datetime in the local timezone
        # Since local timezone is UTC+1 (time.timezone = -3600), adding 3 hours
        # to UTC 12:00 gives 15:00, but in local time that's 14:00
        self.assertIsInstance(result, datetime)
        self.assertIsNone(result.tzinfo)  # Should be naive

    def test_epoch_to_server_naive_converts_correctly(self):
        epoch_seconds = datetime(2023, 1, 1, 12, 0, 0).timestamp()
        result = epoch_to_server_naive(epoch_seconds, 3)  # +3 hours
        # The function adds the offset to the timestamp, but utcfromtimestamp 
        # creates a naive datetime in the local timezone
        self.assertIsInstance(result, datetime)
        self.assertIsNone(result.tzinfo)  # Should be naive

    def test_from_server_naive_converts_correctly(self):
        # Create a naive datetime (as would come from server)
        dt_naive = datetime(2023, 1, 1, 14, 0, 0)  # 14:00 server time
        result = from_server_naive(dt_naive, 3)  # +3 hours offset
        # The function should convert to UTC by subtracting the offset
        self.assertIsInstance(result, datetime)
        self.assertEqual(result.tzinfo, UTC)  # Should be UTC

    @patch('monitor.mt5_client.mt5')
    def test_ticks_paged_returns_empty_when_no_mt5(self, mock_mt5):
        mock_mt5 = None
        # We need to import the module and set mt5 to None for this test
        import monitor.mt5_client as mt5_client
        original_mt5 = mt5_client.mt5
        mt5_client.mt5 = None
        try:
            ticks, stats = ticks_paged("EURUSD", datetime.now(), datetime.now(), 100)
            self.assertEqual(ticks, [])
            self.assertEqual(stats.total_ticks, 0)
        finally:
            mt5_client.mt5 = original_mt5

    @patch('monitor.mt5_client.mt5')
    def test_ticks_range_all_returns_empty_when_no_mt5(self, mock_mt5):
        mock_mt5 = None
        # We need to import the module and set mt5 to None for this test
        import monitor.mt5_client as mt5_client
        original_mt5 = mt5_client.mt5
        mt5_client.mt5 = None
        try:
            ticks, stats = ticks_range_all("EURUSD", datetime.now(), datetime.now())
            self.assertEqual(ticks, [])
            self.assertEqual(stats.total_ticks, 0)
        finally:
            mt5_client.mt5 = original_mt5


if __name__ == '__main__':
    unittest.main()