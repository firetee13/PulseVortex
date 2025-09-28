
import os
import shutil
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch, MagicMock
import sqlite3
from decimal import Decimal

import timelapse_setups as tls

UTC = timezone.utc


class FakeMT5:
    # Class-level constants
    TIMEFRAME_D1 = 1
    TIMEFRAME_H4 = 2
    TIMEFRAME_W1 = 3
    TIMEFRAME_H1 = 4
    TIMEFRAME_M15 = 5
    TIMEFRAME_M1 = 6
    COPY_TICKS_ALL = 99
    TICK_FLAG_BID = 1
    TICK_FLAG_ASK = 2

    def __init__(self, tick_time: datetime):
        self.tick_time = tick_time
        self.initialized = False
        self.copy_ticks_calls = 0
        self.copy_ticks_history = [
            {'flags': self.TICK_FLAG_BID | self.TICK_FLAG_ASK, 'bid': 1.2340, 'ask': 1.2344}
        ]
        self.rates_calls = []
        self.rates_return = {
            self.TIMEFRAME_D1: self._make_rates(20),
            self.TIMEFRAME_H4: self._make_rates(4),
            self.TIMEFRAME_W1: self._make_rates(4),
            self.TIMEFRAME_H1: self._make_rates(1),
            self.TIMEFRAME_M15: self._make_rates(1),
        }

    def _make_rates(self, count: int):
        base = 1.2300
        out = []
        for idx in range(count):
            out.append(
                {
                    'time': idx,
                    'close': base + 0.001 * idx,
                    'high': base + 0.001 * idx + 0.0005,
                    'low': base + 0.001 * idx - 0.0005,
                }
            )
        return out

    def initialize(self):
        self.initialized = True
        return True

    def symbol_select(self, sym: str, select: bool):
        return True

    def symbol_info_tick(self, sym: str):
        if self.tick_time is None:
            return SimpleNamespace(bid=0.0, ask=0.0, time=None, time_msc=None)
        ts = int(self.tick_time.timestamp())
        return SimpleNamespace(bid=1.2345, ask=1.2347, time=ts, time_msc=ts * 1000)

    def copy_ticks_range(self, sym: str, start: datetime, end: datetime, flags: int):
        self.copy_ticks_calls += 1
        return list(self.copy_ticks_history)

    def copy_rates_from_pos(self, sym: str, timeframe: int, pos: int, count: int):
        self.rates_calls.append((sym, timeframe, count))
        return list(self.rates_return.get(timeframe, []))

    def copy_rates_range(self, sym: str, timeframe: int, start: datetime, end: datetime):
        return list(self.rates_return.get(timeframe, []))

    def symbols_get(self):
        return []



class HelperFunctionsTests(unittest.TestCase):
    def test_infer_decimals_from_price(self):
        # Test with various price formats
        self.assertEqual(tls._infer_decimals_from_price(1.2345), 4)
        self.assertEqual(tls._infer_decimals_from_price(1.23), 2)
        self.assertEqual(tls._infer_decimals_from_price(1.0), 0)
        self.assertEqual(tls._infer_decimals_from_price(1), 0)
        self.assertEqual(tls._infer_decimals_from_price(None), 5)  # Default fallback
        self.assertEqual(tls._infer_decimals_from_price(float("nan")), 5)  # Exception fallback

    def test_symbol_digits(self):
        # Test with MT5 available
        with patch.object(tls, '_MT5_IMPORTED', True), \
             patch.object(tls, '_mt5_ensure_init', return_value=True), \
             patch.object(tls, 'mt5') as mock_mt5:

            # Mock symbol_info with digits
            mock_info = SimpleNamespace()
            mock_info.digits = 3
            mock_mt5.symbol_info.return_value = mock_info

            self.assertEqual(tls._symbol_digits("EURUSD", 1.234), 3)

            # Test with None symbol_info
            mock_mt5.symbol_info.return_value = None
            self.assertEqual(tls._symbol_digits("EURUSD", 1.2345), 4)

        # Test with MT5 not available
        with patch.object(tls, '_MT5_IMPORTED', False):
            self.assertEqual(tls._symbol_digits("EURUSD", 1.2345), 4)

    def test_canonicalize_key(self):
        # Test various key formats
        self.assertEqual(tls.canonicalize_key("ATR (%) D1"), "atr percent d1")
        self.assertEqual(tls.canonicalize_key("Strength 4H"), "strength 4h")
        self.assertEqual(tls.canonicalize_key("D1_Close"), "d1 close")
        self.assertEqual(tls.canonicalize_key(None), "")
        self.assertEqual(tls.canonicalize_key(""), "")

        # Test caching
        self.assertEqual(tls.canonicalize_key("ATR (%) D1"), "atr percent d1")
        self.assertIn("ATR (%) D1", tls.CANONICAL_KEYS)

    def test_fnum(self):
        # Test various number formats
        self.assertEqual(tls.fnum("1.2345"), 1.2345)
        self.assertEqual(tls.fnum("1,2345"), 1.2345)  # Comma as decimal
        self.assertEqual(tls.fnum("1,234.56"), 1234.56)  # Comma as thousands
        self.assertEqual(tls.fnum("1.234,56"), 1234.56)  # European format
        self.assertEqual(tls.fnum("1 234.56"), 1234.56)  # Space as thousands
        self.assertEqual(tls.fnum("(123.45)"), -123.45)  # Negative in parentheses
        self.assertEqual(tls.fnum("123.45%"), 123.45)  # With percent sign
        self.assertEqual(tls.fnum("123.45 pips"), 123.45)  # With units
        self.assertEqual(tls.fnum("N/A"), None)  # Not available
        self.assertEqual(tls.fnum(""), None)  # Empty string
        self.assertEqual(tls.fnum(None), None)  # None
        self.assertEqual(tls.fnum("invalid"), None)  # Invalid format

    def test_normalize_spread_pct(self):
        # Test various spread percentage formats
        self.assertEqual(tls.normalize_spread_pct(0.12), 0.12)  # Already in percent
        self.assertEqual(tls.normalize_spread_pct(1.2), 1.2)  # Already in percent
        self.assertEqual(tls.normalize_spread_pct(0.0012), 0.12)  # Fraction of price
        self.assertEqual(tls.normalize_spread_pct(None), None)  # None
        self.assertEqual(tls.normalize_spread_pct(float("nan")), None)  # Invalid

    def test_spread_class(self):
        # Test spread classification
        self.assertEqual(tls.spread_class(0.05), "Excellent")
        self.assertEqual(tls.spread_class(0.15), "Good")
        self.assertEqual(tls.spread_class(0.25), "Acceptable")
        self.assertEqual(tls.spread_class(0.35), "Avoid")
        self.assertEqual(tls.spread_class(None), "Unknown")


class TimezoneFunctionsTests(unittest.TestCase):
    def test_to_input_tz(self):
        # Test with naive datetime (assumed to be in input timezone)
        naive_dt = datetime(2023, 1, 1, 12, 0)
        result = tls.to_input_tz(naive_dt)
        self.assertEqual(result.tzinfo, tls.INPUT_TZ)
        self.assertEqual(result.hour, 12)

        # Test with aware datetime (converted to input timezone)
        utc_dt = datetime(2023, 1, 1, 10, 0, tzinfo=UTC)
        result = tls.to_input_tz(utc_dt)
        self.assertEqual(result.tzinfo, tls.INPUT_TZ)
        self.assertEqual(result.hour, 12)  # UTC+2

    def test_utc_naive(self):
        # Test conversion to naive UTC datetime
        dt = datetime(2023, 1, 1, 12, 0, tzinfo=tls.INPUT_TZ)
        result = tls.utc_naive(dt)
        self.assertIsNone(result.tzinfo)
        self.assertEqual(result.hour, 10)  # UTC+2 to UTC


class MT5FunctionsTests(unittest.TestCase):
    def setUp(self):
        self.original_mt5 = tls.mt5
        self.original_imported = tls._MT5_IMPORTED
        self.original_ready = tls._MT5_READY

    def tearDown(self):
        tls.mt5 = self.original_mt5
        tls._MT5_IMPORTED = self.original_imported
        tls._MT5_READY = self.original_ready

    def test_mt5_ensure_init_success(self):
        # Test successful MT5 initialization
        with patch.object(tls.mt5_client, 'init_mt5') as mock_init:
            mock_init.return_value = None
            tls._MT5_IMPORTED = True
            tls._MT5_READY = False

            result = tls._mt5_ensure_init()
            self.assertTrue(result)
            self.assertTrue(tls._MT5_READY)
            mock_init.assert_called_once()

    def test_mt5_ensure_init_failure(self):
        # Test MT5 initialization failure
        with patch.object(tls.mt5_client, 'init_mt5') as mock_init:
            mock_init.side_effect = RuntimeError("Failed to initialize")
            tls._MT5_IMPORTED = True
            tls._MT5_READY = False

            result = tls._mt5_ensure_init()
            self.assertFalse(result)
            self.assertFalse(tls._MT5_READY)
            mock_init.assert_called_once()

    def test_mt5_ensure_init_not_imported(self):
        # Test when MT5 is not imported
        tls._MT5_IMPORTED = False
        tls._MT5_READY = False

        result = tls._mt5_ensure_init()
        self.assertFalse(result)
        self.assertFalse(tls._MT5_READY)

    def test_mt5_copy_rates_cached(self):
        # Test cached rate fetching
        with patch.object(tls, 'mt5') as mock_mt5:
            # Set up mock MT5
            mock_rates = [{'time': 1, 'close': 1.1}, {'time': 2, 'close': 1.2}]
            mock_mt5.copy_rates_from_pos.return_value = mock_rates

            # Clear cache
            tls._RATE_CACHE.clear()

            # First call should fetch from MT5
            result = tls._mt5_copy_rates_cached("EURUSD", 1, 2)
            self.assertEqual(result, mock_rates)
            mock_mt5.copy_rates_from_pos.assert_called_once_with("EURUSD", 1, 0, 2)

            # Second call should use cache
            mock_mt5.copy_rates_from_pos.reset_mock()
            result = tls._mt5_copy_rates_cached("EURUSD", 1, 2)
            self.assertEqual(result, mock_rates)
            mock_mt5.copy_rates_from_pos.assert_not_called()

            # Test cache expiration
            with patch('time.time', return_value=time.time() + 100):
                mock_mt5.copy_rates_from_pos.reset_mock()
                result = tls._mt5_copy_rates_cached("EURUSD", 1, 2)
                self.assertEqual(result, mock_rates)
                mock_mt5.copy_rates_from_pos.assert_called_once()

            # Test None result
            mock_mt5.copy_rates_from_pos.return_value = None
            mock_mt5.copy_rates_from_pos.reset_mock()
            result = tls._mt5_copy_rates_cached("EURUSD", 1, 2)
            self.assertIsNone(result)
            self.assertNotIn(("EURUSD", 1, 2), tls._RATE_CACHE)


class CalculationFunctionsTests(unittest.TestCase):
    def test_atr(self):
        # Test ATR calculation
        values = [
            (1.1, 1.0, 1.05),  # high, low, close
            (1.2, 1.05, 1.15),
            (1.3, 1.1, 1.25),
            (1.4, 1.2, 1.35),
            (1.5, 1.3, 1.45),
            (1.6, 1.4, 1.55),
            (1.7, 1.5, 1.65),
            (1.8, 1.6, 1.75),
            (1.9, 1.7, 1.85),
            (2.0, 1.8, 1.95),
            (2.1, 1.9, 2.05),
            (2.2, 2.0, 2.15),
            (2.3, 2.1, 2.25),
            (2.4, 2.2, 2.35),
            (2.5, 2.3, 2.45),
        ]

        result = tls._atr(values, 14)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, float)
        if result is not None:
            self.assertTrue(result > 0.0)

        # Test with insufficient data
        result = tls._atr(values[:5], 14)
        self.assertIsNone(result)

    def test_pivots_from_prev_day(self):
        # Test pivot point calculation
        daily_rates = [
            {'high': 1.2, 'low': 1.0, 'close': 1.1},
            {'high': 1.3, 'low': 1.1, 'close': 1.2},
        ]

        s1, r1 = tls._pivots_from_prev_day(daily_rates)
        self.assertIsNotNone(s1)
        self.assertIsNotNone(r1)
        if s1 is not None and r1 is not None:
            self.assertLess(s1, r1)

        # Test with insufficient data
        s1, r1 = tls._pivots_from_prev_day(daily_rates[:1])
        self.assertIsNone(s1)
        self.assertIsNone(r1)

        # Test with None
        s1, r1 = tls._pivots_from_prev_day(None)
        self.assertIsNone(s1)
        self.assertIsNone(r1)

        # Test with list format instead of dict
        daily_rates_list = [
            [0, 1.2, 1.0, 1.1, 1.1],  # time, open, high, low, close
            [1, 1.3, 1.1, 1.2, 1.2],
        ]
        s1, r1 = tls._pivots_from_prev_day(daily_rates_list)
        self.assertIsNotNone(s1)
        self.assertIsNotNone(r1)


class DatabaseFunctionsTests(unittest.TestCase):
    def setUp(self):
        # Create a temporary database for testing
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test_timelapse.db")
        self.original_default_db_path = tls.default_db_path
        tls.default_db_path = lambda: self.db_path

    def tearDown(self):
        # Clean up
        tls.default_db_path = self.original_default_db_path
        tls._DB_CONN = None
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.rmdir(self.temp_dir)

    def test_get_db_connection(self):
        # Test database connection creation
        tls._DB_CONN = None
        conn = tls._get_db_connection()
        self.assertIsNotNone(conn)
        self.assertIsInstance(conn, sqlite3.Connection)
        self.assertEqual(tls._DB_CONN, conn)

        # Test connection reuse
        conn2 = tls._get_db_connection()
        self.assertEqual(conn, conn2)

    def test_close_db_connection(self):
        # Test database connection closing
        conn = tls._get_db_connection()
        tls._close_db_connection()
        self.assertIsNone(tls._DB_CONN)

        # Test closing already closed connection
        tls._close_db_connection()
        self.assertIsNone(tls._DB_CONN)


class FilterFunctionsTests(unittest.TestCase):
    def setUp(self):
        # Create a temporary database for testing
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test_timelapse.db")
        self.original_default_db_path = tls.default_db_path
        tls.default_db_path = lambda: self.db_path

    def tearDown(self):
        # Clean up
        tls.default_db_path = self.original_default_db_path
        tls._DB_CONN = None
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.rmdir(self.temp_dir)

    def test_filter_recent_duplicates_no_db(self):
        # Test with no database
        with patch.object(tls, 'sqlite3', None):
            results = [{"symbol": "EURUSD"}]
            filtered, excluded = tls._filter_recent_duplicates(results)  # type: ignore
            self.assertEqual(filtered, results)
            self.assertEqual(excluded, set())

    def test_filter_recent_duplicates_no_tables(self):
        # Test with database but no tables
        results = [{"symbol": "EURUSD"}]
        filtered, excluded = tls._filter_recent_duplicates(results)  # type: ignore
        self.assertEqual(filtered, results)
        self.assertEqual(excluded, set())

    def test_filter_recent_duplicates_with_open_setups(self):
        # Test with open setups in database
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        # Create tables
        cur.execute("""
            CREATE TABLE timelapse_setups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                price REAL,
                sl REAL,
                tp REAL,
                rrr REAL,
                score REAL,
                explain TEXT,
                as_of TEXT NOT NULL,
                UNIQUE(symbol, direction, as_of)
            )
        """)

        cur.execute("""
            CREATE TABLE timelapse_hits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                setup_id INTEGER UNIQUE,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                sl REAL,
                tp REAL,
                hit TEXT NOT NULL CHECK (hit IN ('TP','SL')),
                hit_price REAL,
                hit_time TEXT NOT NULL,
                hit_time_utc3 TEXT,
                entry_time_utc3 TEXT,
                entry_price REAL,
                checked_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
            )
        """)

        # Insert an open setup (no hit record)
        cur.execute("""
            INSERT INTO timelapse_setups (symbol, direction, price, sl, tp, rrr, score, explain, as_of)
            VALUES ('EURUSD', 'Buy', 1.1, 1.0, 1.2, 2.0, 1.5, 'test', '2023-01-01 00:00:00')
        """)

        conn.commit()
        conn.close()

        # Test filtering
        results = [{"symbol": "EURUSD"}, {"symbol": "GBPUSD"}]
        filtered, excluded = tls._filter_recent_duplicates(results)  # type: ignore

        # EURUSD should be filtered out
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["symbol"], "GBPUSD")
        self.assertEqual(excluded, {"EURUSD"})

    def test_filter_recent_duplicates_with_closed_setups(self):
        # Test with closed setups in database
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        # Create tables
        cur.execute("""
            CREATE TABLE timelapse_setups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                price REAL,
                sl REAL,
                tp REAL,
                rrr REAL,
                score REAL,
                explain TEXT,
                as_of TEXT NOT NULL,
                UNIQUE(symbol, direction, as_of)
            )
        """)

        cur.execute("""
            CREATE TABLE timelapse_hits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                setup_id INTEGER UNIQUE,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                sl REAL,
                tp REAL,
                hit TEXT NOT NULL CHECK (hit IN ('TP','SL')),
                hit_price REAL,
                hit_time TEXT NOT NULL,
                hit_time_utc3 TEXT,
                entry_time_utc3 TEXT,
                entry_price REAL,
                checked_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
            )
        """)

        # Insert a setup with a hit record (closed)
        cur.execute("""
            INSERT INTO timelapse_setups (symbol, direction, price, sl, tp, rrr, score, explain, as_of)
            VALUES ('EURUSD', 'Buy', 1.1, 1.0, 1.2, 2.0, 1.5, 'test', '2023-01-01 00:00:00')
        """)

        setup_id = cur.lastrowid

        cur.execute("""
            INSERT INTO timelapse_hits (setup_id, symbol, direction, sl, tp, hit, hit_price, hit_time)
            VALUES (?, 'EURUSD', 'Buy', 1.0, 1.2, 'TP', 1.2, '2023-01-01 01:00:00')
        """, (setup_id,))

        conn.commit()
        conn.close()

        # Test filtering
        results = [{"symbol": "EURUSD"}, {"symbol": "GBPUSD"}]
        filtered, excluded = tls._filter_recent_duplicates(results)  # type: ignore

        # Both should be included since EURUSD setup is closed
        self.assertEqual(len(filtered), 2)
        self.assertEqual(excluded, set())


class SlDistanceFilterTests(unittest.TestCase):
    def setUp(self):
        self.original_mt5 = tls.mt5
        self.original_imported = tls._MT5_IMPORTED
        self.original_ready = tls._MT5_READY
        tls.mt5 = None
        tls._MT5_IMPORTED = True
        tls._MT5_READY = True

    def tearDown(self):
        tls.mt5 = self.original_mt5
        tls._MT5_IMPORTED = self.original_imported
        tls._MT5_READY = self.original_ready

    def test_sell_uses_ask_for_sl_distance(self):
        # Test case where SL is too close to spread (should be rejected)
        now = datetime.now(UTC)
        sym = 'TESTIDX'
        bid = 97239.9
        ask = 97271.9  # spread = 32.0
        s1 = 96788.26666666665
        r1 = 97571.56666666665

        # Set up fake MT5
        fake_mt5 = FakeMT5(now)
        tls.mt5 = fake_mt5

        first = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('D1 Close'): 97000.0,
            tls.canonicalize_key('Strength 4H'): -0.1,
        })
        last = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('Bid'): bid,
            tls.canonicalize_key('Ask'): ask,
            tls.canonicalize_key('S1 Level M5'): s1,
            tls.canonicalize_key('R1 Level M5'): r1,
            tls.canonicalize_key('Strength 4H'): -0.6,
            tls.canonicalize_key('Strength 1D'): -0.2,
            tls.canonicalize_key('Strength 1W'): -0.1,  # ensure overall Sell (>=2 negatives)
            tls.canonicalize_key('D1 Close'): 97500.0,
            tls.canonicalize_key('D1 High'): 97600.0,
            tls.canonicalize_key('D1 Low'): 96800.0,
            tls.canonicalize_key('Recent Tick'): 1,
            tls.canonicalize_key('Last Tick UTC'): now.strftime('%Y-%m-%d %H:%M:%S'),
        })
        series = {sym: [first, last]}
        results, reasons = tls.analyze(series, min_rrr=1.0, min_prox_sl=0.0, max_prox_sl=1.0, as_of_ts=now, debug=False)
        # No results should be produced due to SL too close to spread
        self.assertEqual(results, [])
        self.assertIn('sl_too_close_to_spread', reasons)
        self.assertIn(sym, reasons['sl_too_close_to_spread'])

    def test_invalid_bid_ask_rejection(self):
        # Test case where bid/ask are invalid (should be rejected at SL distance check)
        now = datetime.now(UTC)
        sym = 'TESTIDX2'
        bid = 1.1  # Valid bid to pass initial checks
        ask = 1.0  # Invalid ask (ask <= bid)
        # Set S1/R1 levels that would normally be valid for a sell setup
        s1 = 0.9   # Lower support level
        r1 = 1.2   # Higher resistance level

        # Set up fake MT5
        fake_mt5 = FakeMT5(now)
        tls.mt5 = fake_mt5

        first = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('D1 Close'): 1.0,
            tls.canonicalize_key('Strength 4H'): -0.1,
        })
        last = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('Bid'): bid,
            tls.canonicalize_key('Ask'): ask,
            tls.canonicalize_key('S1 Level M5'): s1,
            tls.canonicalize_key('R1 Level M5'): r1,
            tls.canonicalize_key('Strength 4H'): -0.6,
            tls.canonicalize_key('Strength 1D'): -0.2,
            tls.canonicalize_key('Strength 1W'): -0.1,  # ensure overall Sell (>=2 negatives)
            tls.canonicalize_key('D1 Close'): 1.05,
            tls.canonicalize_key('D1 High'): 1.15,
            tls.canonicalize_key('D1 Low'): 0.95,
            tls.canonicalize_key('Recent Tick'): 1,
            tls.canonicalize_key('Last Tick UTC'): now.strftime('%Y-%m-%d %H:%M:%S'),
        })
        series = {sym: [first, last]}
        results, reasons = tls.analyze(series, min_rrr=1.0, min_prox_sl=0.0, max_prox_sl=1.0, as_of_ts=now, debug=False)
        # No results should be produced due to invalid bid/ask (ask <= bid)
        self.assertEqual(results, [])
        self.assertIn('invalid_bid_ask_for_spread_calculation', reasons)
        self.assertIn(sym, reasons['invalid_bid_ask_for_spread_calculation'])


class AnalyzeFunctionTests(unittest.TestCase):
    def setUp(self):
        self.original_mt5 = tls.mt5
        self.original_imported = tls._MT5_IMPORTED
        self.original_ready = tls._MT5_READY
        tls.mt5 = None
        tls._MT5_IMPORTED = True
        tls._MT5_READY = True

    def tearDown(self):
        tls.mt5 = self.original_mt5
        tls._MT5_IMPORTED = self.original_imported
        tls._MT5_READY = self.original_ready

    def test_analyze_time_filter(self):
        # Test time-based filtering (low volume time window)
        now = datetime.now(UTC)
        sym = 'TESTIDX'

        # Create a snapshot at 23:30 UTC+3 (should be filtered out)
        blocked_time = now.replace(hour=21, minute=30)  # 21:30 UTC = 23:30 UTC+3
        blocked_time = blocked_time.replace(tzinfo=UTC)

        first = tls.Snapshot(ts=blocked_time, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('D1 Close'): 1.0,
            tls.canonicalize_key('Strength 4H'): 0.1,
        })
        last = tls.Snapshot(ts=blocked_time, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('Bid'): 1.1,
            tls.canonicalize_key('Ask'): 1.1001,
            tls.canonicalize_key('Strength 4H'): 0.6,
            tls.canonicalize_key('Strength 1D'): 0.2,
            tls.canonicalize_key('Strength 1W'): 0.1,  # ensure overall Buy (>=2 positives)
            tls.canonicalize_key('D1 Close'): 1.05,
            tls.canonicalize_key('D1 High'): 1.15,
            tls.canonicalize_key('D1 Low'): 0.95,
            tls.canonicalize_key('Recent Tick'): 1,
            tls.canonicalize_key('Last Tick UTC'): blocked_time.strftime('%Y-%m-%d %H:%M:%S'),
        })
        series = {sym: [first, last]}
        results, reasons = tls.analyze(series, min_rrr=1.0, min_prox_sl=0.0, max_prox_sl=1.0, as_of_ts=blocked_time, debug=False)
        self.assertEqual(results, [])
        self.assertIn('low_vol_time_window', reasons)
        self.assertIn(sym, reasons['low_vol_time_window'])

    def test_analyze_no_recent_ticks(self):
        # Test filtering when no recent ticks
        now = datetime.now(UTC)
        sym = 'TESTIDX'

        first = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('D1 Close'): 1.0,
            tls.canonicalize_key('Strength 4H'): 0.1,
        })
        last = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('Bid'): 1.1,
            tls.canonicalize_key('Ask'): 1.1001,
            tls.canonicalize_key('Strength 4H'): 0.6,
            tls.canonicalize_key('Strength 1D'): 0.2,
            tls.canonicalize_key('Strength 1W'): 0.1,  # ensure overall Buy (>=2 positives)
            tls.canonicalize_key('D1 Close'): 1.05,
            tls.canonicalize_key('D1 High'): 1.15,
            tls.canonicalize_key('D1 Low'): 0.95,
            tls.canonicalize_key('Recent Tick'): 0,  # No recent tick
            tls.canonicalize_key('Last Tick UTC'): now.strftime('%Y-%m-%d %H:%M:%S'),
        })
        series = {sym: [first, last]}
        results, reasons = tls.analyze(series, min_rrr=1.0, min_prox_sl=0.0, max_prox_sl=1.0, as_of_ts=now, debug=False)
        self.assertEqual(results, [])
        self.assertIn('no_recent_ticks', reasons)
        self.assertIn(sym, reasons['no_recent_ticks'])

    def test_analyze_no_direction_consensus(self):
        # Test filtering when no direction consensus
        now = datetime.now(UTC)
        sym = 'TESTIDX'

        first = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('D1 Close'): 1.0,
            tls.canonicalize_key('Strength 4H'): 0.1,
        })
        last = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('Bid'): 1.1,
            tls.canonicalize_key('Ask'): 1.1001,
            tls.canonicalize_key('Strength 4H'): -0.1,  # Mixed signals
            tls.canonicalize_key('Strength 1D'): 0.1,
            tls.canonicalize_key('Strength 1W'): -0.1,  # No clear consensus
            tls.canonicalize_key('D1 Close'): 1.05,
            tls.canonicalize_key('D1 High'): 1.15,
            tls.canonicalize_key('D1 Low'): 0.95,
            tls.canonicalize_key('Recent Tick'): 1,
            tls.canonicalize_key('Last Tick UTC'): now.strftime('%Y-%m-%d %H:%M:%S'),
        })
        series = {sym: [first, last]}
        results, reasons = tls.analyze(series, min_rrr=1.0, min_prox_sl=0.0, max_prox_sl=1.0, as_of_ts=now, debug=False)
        self.assertEqual(results, [])
        self.assertIn('no_direction_consensus', reasons)
        self.assertIn(sym, reasons['no_direction_consensus'])

    def test_analyze_no_live_bid_ask(self):
        # Test filtering when no live bid/ask
        now = datetime.now(UTC)
        sym = 'TESTIDX'

        first = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('D1 Close'): 1.0,
            tls.canonicalize_key('Strength 4H'): 0.1,
        })
        last = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('Bid'): None,  # No bid
            tls.canonicalize_key('Ask'): None,  # No ask
            tls.canonicalize_key('Strength 4H'): 0.6,
            tls.canonicalize_key('Strength 1D'): 0.2,
            tls.canonicalize_key('Strength 1W'): 0.1,  # ensure overall Buy (>=2 positives)
            tls.canonicalize_key('D1 Close'): 1.05,
            tls.canonicalize_key('D1 High'): 1.15,
            tls.canonicalize_key('D1 Low'): 0.95,
            tls.canonicalize_key('Recent Tick'): 1,
            tls.canonicalize_key('Last Tick UTC'): now.strftime('%Y-%m-%d %H:%M:%S'),
        })
        series = {sym: [first, last]}
        results, reasons = tls.analyze(series, min_rrr=1.0, min_prox_sl=0.0, max_prox_sl=1.0, as_of_ts=now, debug=False)
        self.assertEqual(results, [])
        self.assertIn('no_live_bid_ask', reasons)
        self.assertIn(sym, reasons['no_live_bid_ask'])

    def test_analyze_spread_avoid(self):
        # Test filtering when spread is too high
        now = datetime.now(UTC)
        sym = 'TESTIDX'

        first = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('D1 Close'): 1.0,
            tls.canonicalize_key('Strength 4H'): 0.1,
        })
        last = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('Bid'): 1.1,
            tls.canonicalize_key('Ask'): 1.104,  # 0.4% spread (should be avoided)
            tls.canonicalize_key('Strength 4H'): 0.6,
            tls.canonicalize_key('Strength 1D'): 0.2,
            tls.canonicalize_key('Strength 1W'): 0.1,  # ensure overall Buy (>=2 positives)
            tls.canonicalize_key('D1 Close'): 1.05,
            tls.canonicalize_key('D1 High'): 1.15,
            tls.canonicalize_key('D1 Low'): 0.95,
            tls.canonicalize_key('Recent Tick'): 1,
            tls.canonicalize_key('Last Tick UTC'): now.strftime('%Y-%m-%d %H:%M:%S'),
        })
        series = {sym: [first, last]}
        results, reasons = tls.analyze(series, min_rrr=1.0, min_prox_sl=0.0, max_prox_sl=1.0, as_of_ts=now, debug=False)
        self.assertEqual(results, [])
        self.assertIn('spread_avoid', reasons)
        self.assertIn(sym, reasons['spread_avoid'])

    def test_analyze_missing_sl_tp(self):
        # Test filtering when SL/TP are missing
        now = datetime.now(UTC)
        sym = 'TESTIDX'

        first = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('D1 Close'): 1.0,
            tls.canonicalize_key('Strength 4H'): 0.1,
        })
        last = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('Bid'): 1.1,
            tls.canonicalize_key('Ask'): 1.1001,
            tls.canonicalize_key('Strength 4H'): 0.6,
            tls.canonicalize_key('Strength 1D'): 0.2,
            tls.canonicalize_key('Strength 1W'): 0.1,  # ensure overall Buy (>=2 positives)
            tls.canonicalize_key('D1 Close'): 1.05,
            tls.canonicalize_key('D1 High'): 1.15,
            tls.canonicalize_key('D1 Low'): 0.95,
            tls.canonicalize_key('S1 Level M5'): None,  # Missing S1
            tls.canonicalize_key('R1 Level M5'): None,  # Missing R1
            tls.canonicalize_key('Recent Tick'): 1,
            tls.canonicalize_key('Last Tick UTC'): now.strftime('%Y-%m-%d %H:%M:%S'),
        })
        series = {sym: [first, last]}
        results, reasons = tls.analyze(series, min_rrr=1.0, min_prox_sl=0.0, max_prox_sl=1.0, as_of_ts=now, debug=False)
        self.assertEqual(results, [])
        self.assertIn('missing_sl_tp', reasons)
        self.assertIn(sym, reasons['missing_sl_tp'])

    def test_analyze_price_outside_buy_sr(self):
        # Test filtering when price is outside buy S/R range
        now = datetime.now(UTC)
        sym = 'TESTIDX'

        first = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('D1 Close'): 1.0,
            tls.canonicalize_key('Strength 4H'): 0.1,
        })
        last = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('Bid'): 1.1,
            tls.canonicalize_key('Ask'): 1.1001,
            tls.canonicalize_key('Strength 4H'): 0.6,
            tls.canonicalize_key('Strength 1D'): 0.2,
            tls.canonicalize_key('Strength 1W'): 0.1,  # ensure overall Buy (>=2 positives)
            tls.canonicalize_key('D1 Close'): 1.05,
            tls.canonicalize_key('D1 High'): 1.15,
            tls.canonicalize_key('D1 Low'): 0.95,
            tls.canonicalize_key('S1 Level M5'): 1.0,  # SL
            tls.canonicalize_key('R1 Level M5'): 1.05,  # TP
            tls.canonicalize_key('Recent Tick'): 1,
            tls.canonicalize_key('Last Tick UTC'): now.strftime('%Y-%m-%d %H:%M:%S'),
        })
        series = {sym: [first, last]}
        results, reasons = tls.analyze(series, min_rrr=1.0, min_prox_sl=0.0, max_prox_sl=1.0, as_of_ts=now, debug=False)
        self.assertEqual(results, [])
        self.assertIn('price_outside_buy_sr', reasons)
        self.assertIn(sym, reasons['price_outside_buy_sr'])

    def test_analyze_price_outside_sell_sr(self):
        # Test filtering when price is outside sell S/R range
        now = datetime.now(UTC)
        sym = 'TESTIDX'

        first = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('D1 Close'): 1.0,
            tls.canonicalize_key('Strength 4H'): -0.1,
        })
        last = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('Bid'): 1.1,
            tls.canonicalize_key('Ask'): 1.1001,
            tls.canonicalize_key('Strength 4H'): -0.6,
            tls.canonicalize_key('Strength 1D'): -0.2,
            tls.canonicalize_key('Strength 1W'): -0.1,  # ensure overall Sell (>=2 negatives)
            tls.canonicalize_key('D1 Close'): 1.05,
            tls.canonicalize_key('D1 High'): 1.15,
            tls.canonicalize_key('D1 Low'): 0.95,
            tls.canonicalize_key('S1 Level M5'): 1.0,  # TP
            tls.canonicalize_key('R1 Level M5'): 1.05,  # SL
            tls.canonicalize_key('Recent Tick'): 1,
            tls.canonicalize_key('Last Tick UTC'): now.strftime('%Y-%m-%d %H:%M:%S'),
        })
        series = {sym: [first, last]}
        results, reasons = tls.analyze(series, min_rrr=1.0, min_prox_sl=0.0, max_prox_sl=1.0, as_of_ts=now, debug=False)
        self.assertEqual(results, [])
        self.assertIn('price_outside_sell_sr', reasons)
        self.assertIn(sym, reasons['price_outside_sell_sr'])

    def test_analyze_too_close_to_sl_prox(self):
        # Test filtering when entry is too close to SL
        now = datetime.now(UTC)
        sym = 'TESTIDX'

        first = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('D1 Close'): 1.0,
            tls.canonicalize_key('Strength 4H'): 0.1,
        })
        last = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('Bid'): 1.1,
            tls.canonicalize_key('Ask'): 1.1001,
            tls.canonicalize_key('Strength 4H'): 0.6,
            tls.canonicalize_key('Strength 1D'): 0.2,
            tls.canonicalize_key('Strength 1W'): 0.1,  # ensure overall Buy (>=2 positives)
            tls.canonicalize_key('D1 Close'): 1.05,
            tls.canonicalize_key('D1 High'): 1.15,
            tls.canonicalize_key('D1 Low'): 0.95,
            tls.canonicalize_key('S1 Level M5'): 1.09,  # SL (very close to entry)
            tls.canonicalize_key('R1 Level M5'): 1.15,  # TP
            tls.canonicalize_key('Recent Tick'): 1,
            tls.canonicalize_key('Last Tick UTC'): now.strftime('%Y-%m-%d %H:%M:%S'),
        })
        series = {sym: [first, last]}
        results, reasons = tls.analyze(series, min_rrr=1.0, min_prox_sl=0.1, max_prox_sl=1.0, as_of_ts=now, debug=False)
        self.assertEqual(results, [])
        self.assertIn('too_close_to_sl_prox', reasons)
        self.assertIn(sym, reasons['too_close_to_sl_prox'])

    def test_analyze_too_far_from_sl_prox(self):
        # Test filtering when entry is too far from SL
        now = datetime.now(UTC)
        sym = 'TESTIDX'

        first = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('D1 Close'): 1.0,
            tls.canonicalize_key('Strength 4H'): 0.1,
        })
        last = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('Bid'): 1.1,
            tls.canonicalize_key('Ask'): 1.1001,
            tls.canonicalize_key('Strength 4H'): 0.6,
            tls.canonicalize_key('Strength 1D'): 0.2,
            tls.canonicalize_key('Strength 1W'): 0.1,  # ensure overall Buy (>=2 positives)
            tls.canonicalize_key('D1 Close'): 1.05,
            tls.canonicalize_key('D1 High'): 1.15,
            tls.canonicalize_key('D1 Low'): 0.95,
            tls.canonicalize_key('S1 Level M5'): 0.95,  # SL (far from entry)
            tls.canonicalize_key('R1 Level M5'): 1.15,  # TP
            tls.canonicalize_key('Recent Tick'): 1,
            tls.canonicalize_key('Last Tick UTC'): now.strftime('%Y-%m-%d %H:%M:%S'),
        })
        series = {sym: [first, last]}
        results, reasons = tls.analyze(series, min_rrr=1.0, min_prox_sl=0.0, max_prox_sl=0.1, as_of_ts=now, debug=False)
        self.assertEqual(results, [])
        self.assertIn('too_far_from_sl_prox', reasons)
        self.assertIn(sym, reasons['too_far_from_sl_prox'])

    def test_analyze_valid_buy_setup(self):
        # Test a valid buy setup
        now = datetime.now(UTC)
        sym = 'TESTIDX'

        first = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('D1 Close'): 1.0,
            tls.canonicalize_key('Strength 4H'): 0.1,
        })
        last = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('Bid'): 1.1,
            tls.canonicalize_key('Ask'): 1.1001,
            tls.canonicalize_key('Strength 4H'): 0.6,
            tls.canonicalize_key('Strength 1D'): 0.2,
            tls.canonicalize_key('Strength 1W'): 0.1,  # ensure overall Buy (>=2 positives)
            tls.canonicalize_key('D1 Close'): 1.05,
            tls.canonicalize_key('D1 High'): 1.15,
            tls.canonicalize_key('D1 Low'): 0.95,
            tls.canonicalize_key('S1 Level M5'): 1.0,  # SL
            tls.canonicalize_key('R1 Level M5'): 1.15,  # TP
            tls.canonicalize_key('Recent Tick'): 1,
            tls.canonicalize_key('Last Tick UTC'): now.strftime('%Y-%m-%d %H:%M:%S'),
        })
        series = {sym: [first, last]}
        results, reasons = tls.analyze(series, min_rrr=1.0, min_prox_sl=0.0, max_prox_sl=1.0, as_of_ts=now, debug=False)

        # Should have one result
        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result['symbol'], sym)
        self.assertEqual(result['direction'], 'Buy')
        self.assertEqual(result['price'], 1.1001)  # Ask price
        self.assertEqual(result['sl'], 1.0)
        self.assertEqual(result['tp'], 1.15)
        rrr = float(result['rrr'])  # type: ignore
        score = float(result['score'])  # type: ignore
        self.assertTrue(rrr > 1.0)
        self.assertTrue(score > 0)
        self.assertIn('proximity_to_sl', result)

    def test_analyze_valid_sell_setup(self):
        # Test a valid sell setup
        now = datetime.now(UTC)
        sym = 'TESTIDX'

        first = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('D1 Close'): 1.0,
            tls.canonicalize_key('Strength 4H'): -0.1,
        })
        last = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('Bid'): 1.1,
            tls.canonicalize_key('Ask'): 1.1001,
            tls.canonicalize_key('Strength 4H'): -0.6,
            tls.canonicalize_key('Strength 1D'): -0.2,
            tls.canonicalize_key('Strength 1W'): -0.1,  # ensure overall Sell (>=2 negatives)
            tls.canonicalize_key('D1 Close'): 1.05,
            tls.canonicalize_key('D1 High'): 1.15,
            tls.canonicalize_key('D1 Low'): 0.95,
            tls.canonicalize_key('S1 Level M5'): 0.95,  # TP
            tls.canonicalize_key('R1 Level M5'): 1.15,  # SL
            tls.canonicalize_key('Recent Tick'): 1,
            tls.canonicalize_key('Last Tick UTC'): now.strftime('%Y-%m-%d %H:%M:%S'),
        })
        series = {sym: [first, last]}
        results, reasons = tls.analyze(series, min_rrr=1.0, min_prox_sl=0.0, max_prox_sl=1.0, as_of_ts=now, debug=False)

        # Should have one result
        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result['symbol'], sym)
        self.assertEqual(result['direction'], 'Sell')
        self.assertEqual(result['price'], 1.1)  # Bid price
        self.assertEqual(result['sl'], 1.15)
        self.assertEqual(result['tp'], 0.95)
        rrr = float(result['rrr'])  # type: ignore
        score = float(result['score'])  # type: ignore
        self.assertTrue(rrr > 1.0)
        self.assertTrue(score > 0)
        self.assertIn('proximity_to_sl', result)


class ReadSeriesMT5Tests(unittest.TestCase):
    def setUp(self):
        self.original_mt5 = tls.mt5
        self.original_imported = tls._MT5_IMPORTED
        self.original_ready = tls._MT5_READY
        tls.mt5 = None
        tls._MT5_IMPORTED = True
        tls._MT5_READY = True

    def tearDown(self):
        tls.mt5 = self.original_mt5
        tls._MT5_IMPORTED = self.original_imported
        tls._MT5_READY = self.original_ready

    def test_read_series_mt5_no_mt5(self):
        # Test when MT5 is not imported
        tls._MT5_IMPORTED = False
        symbols = ['EURUSD', 'GBPUSD']
        series, mkttvc, as_of_ts = tls.read_series_mt5(symbols)
        self.assertEqual(series, {})
        self.assertIsNone(mkttvc)
        self.assertIsNotNone(as_of_ts)

    def test_read_series_mt5_init_failure(self):
        # Test when MT5 initialization fails
        with patch.object(tls, '_mt5_ensure_init', return_value=False):
            symbols = ['EURUSD', 'GBPUSD']
            series, mkttvc, as_of_ts = tls.read_series_mt5(symbols)
            self.assertEqual(series, {})
            self.assertIsNone(mkttvc)
            self.assertIsNotNone(as_of_ts)

    def test_read_series_mt5_success(self):
        # Test successful series reading
        symbols = ['EURUSD', 'GBPUSD']
        fake_mt5 = FakeMT5(datetime.now(UTC))
        tls.mt5 = fake_mt5

        with patch.object(tls, '_mt5_ensure_init', return_value=True):
            series, mkttvc, as_of_ts = tls.read_series_mt5(symbols)
            self.assertEqual(len(series), 2)  # EURUSD and GBPUSD
            self.assertIn('EURUSD', series)
            self.assertIn('GBPUSD', series)
            self.assertIsNotNone(as_of_ts)

            # Check that each symbol has at least one snapshot
            for sym, snapshots in series.items():
                self.assertGreater(len(snapshots), 0)
                for snapshot in snapshots:
                    self.assertIsInstance(snapshot, tls.Snapshot)
                    self.assertEqual(snapshot.row[tls.HEADER_SYMBOL], sym)

    def test_read_series_mt5_with_custom_symbols(self):
        # Test with custom symbols list
        symbols = ['EURUSD']
        fake_mt5 = FakeMT5(datetime.now(UTC))
        tls.mt5 = fake_mt5

        with patch.object(tls, '_mt5_ensure_init', return_value=True):
            series, mkttvc, as_of_ts = tls.read_series_mt5(symbols)
            self.assertEqual(len(series), 1)  # Only EURUSD
            self.assertIn('EURUSD', series)
            self.assertNotIn('GBPUSD', series)
            self.assertIsNotNone(as_of_ts)

    def test_read_series_mt5_empty_symbols(self):
        # Test with empty symbols list
        symbols = []
        fake_mt5 = FakeMT5(datetime.now(UTC))
        tls.mt5 = fake_mt5

        with patch.object(tls, '_mt5_ensure_init', return_value=True):
            series, mkttvc, as_of_ts = tls.read_series_mt5(symbols)
            self.assertEqual(len(series), 0)  # No symbols
            self.assertIsNotNone(as_of_ts)


class ProcessOnceTests(unittest.TestCase):
    def setUp(self):
        self.original_mt5 = tls.mt5
        self.original_imported = tls._MT5_IMPORTED
        self.original_ready = tls._MT5_READY
        self.original_sqlite = tls.sqlite3
        tls.mt5 = None
        tls._MT5_IMPORTED = True
        tls._MT5_READY = True
        tls.sqlite3 = None  # Disable DB for most tests

    def tearDown(self):
        tls.mt5 = self.original_mt5
        tls._MT5_IMPORTED = self.original_imported
        tls._MT5_READY = self.original_ready
        tls.sqlite3 = self.original_sqlite

    def test_process_once_no_symbols(self):
        # Test with empty symbols list
        symbols = []
        with patch.object(tls, 'read_series_mt5', return_value=({}, None, datetime.now(UTC))):
            with patch.object(tls, 'analyze', return_value=([], {})):
                tls.process_once(
                    symbols=symbols,
                    min_rrr=1.0,
                    min_prox_sl=0.0,
                    max_prox_sl=1.0,
                    top=None,
                    brief=True,
                    debug=False
                )

    def test_process_once_with_exclude_set(self):
        # Test with exclude set
        symbols = ['EURUSD', 'GBPUSD']
        exclude_set = {'EURUSD'}
        fake_mt5 = FakeMT5(datetime.now(UTC))
        tls.mt5 = fake_mt5

        with patch.object(tls, '_mt5_ensure_init', return_value=True):
            with patch.object(tls, 'analyze', return_value=([], {})):
                tls.process_once(
                    symbols=symbols,
                    min_rrr=1.0,
                    min_prox_sl=0.0,
                    max_prox_sl=1.0,
                    top=None,
                    brief=True,
                    debug=False,
                    exclude_set=exclude_set
                )

    def test_process_once_with_results(self):
        # Test with results
        symbols = ['EURUSD', 'GBPUSD']
        fake_mt5 = FakeMT5(datetime.now(UTC))
        tls.mt5 = fake_mt5
        results = [
            {
                'symbol': 'EURUSD',
                'direction': 'Buy',
                'price': 1.1001,
                'sl': 1.0,
                'tp': 1.15,
                'rrr': 1.5,
                'score': 3.0,
                'explain': 'Test explanation',
                'as_of': datetime.now(UTC),
                'proximity_to_sl': 0.3
            }
        ]

        with patch.object(tls, '_mt5_ensure_init', return_value=True):
            with patch.object(tls, 'analyze', return_value=(results, {})):
                with patch.object(tls, 'insert_results_to_db'):
                    tls.process_once(
                        symbols=symbols,
                        min_rrr=1.0,
                        min_prox_sl=0.0,
                        max_prox_sl=1.0,
                        top=None,
                        brief=True,
                        debug=False
                    )

    def test_process_once_with_debug(self):
        # Test with debug mode
        symbols = ['EURUSD', 'GBPUSD']
        fake_mt5 = FakeMT5(datetime.now(UTC))
        tls.mt5 = fake_mt5
        results = [
            {
                'symbol': 'EURUSD',
                'direction': 'Buy',
                'price': 1.1001,
                'sl': 1.0,
                'tp': 1.15,
                'rrr': 1.5,
                'score': 3.0,
                'explain': 'Test explanation',
                'as_of': datetime.now(UTC),
                'proximity_to_sl': 0.3
            }
        ]

        with patch.object(tls, '_mt5_ensure_init', return_value=True):
            with patch.object(tls, 'analyze', return_value=(results, {})):
                with patch.object(tls, 'insert_results_to_db'):
                    with patch('builtins.print') as mock_print:
                        tls.process_once(
                            symbols=symbols,
                            min_rrr=1.0,
                            min_prox_sl=0.0,
                            max_prox_sl=1.0,
                            top=None,
                            brief=True,
                            debug=True
                        )
                        # Check that debug output was printed
                        self.assertTrue(any('EURUSD | Buy @ 1.1001' in call[0][0] for call in mock_print.call_args_list))

    def test_process_once_with_top_limit(self):
        # Test with top limit
        symbols = ['EURUSD', 'GBPUSD']
        fake_mt5 = FakeMT5(datetime.now(UTC))
        tls.mt5 = fake_mt5
        results = [
            {
                'symbol': 'EURUSD',
                'direction': 'Buy',
                'price': 1.1001,
                'sl': 1.0,
                'tp': 1.15,
                'rrr': 1.5,
                'score': 3.0,
                'explain': 'Test explanation',
                'as_of': datetime.now(UTC),
                'proximity_to_sl': 0.3
            },
            {
                'symbol': 'GBPUSD',
                'direction': 'Sell',
                'price': 1.3,
                'sl': 1.35,
                'tp': 1.25,
                'rrr': 1.2,
                'score': 2.5,
                'explain': 'Test explanation',
                'as_of': datetime.now(UTC),
                'proximity_to_sl': 0.4
            }
        ]

        with patch.object(tls, '_mt5_ensure_init', return_value=True):
            with patch.object(tls, 'analyze', return_value=(results, {})):
                with patch.object(tls, 'insert_results_to_db') as mock_insert:
                    tls.process_once(
                        symbols=symbols,
                        min_rrr=1.0,
                        min_prox_sl=0.0,
                        max_prox_sl=1.0,
                        top=1,  # Only top 1
                        brief=True,
                        debug=False
                    )
                    # Check that only the top result was inserted
                    args, kwargs = mock_insert.call_args
                    inserted_results = args[0]
                    self.assertEqual(len(inserted_results), 1)
                    self.assertEqual(inserted_results[0]['symbol'], 'EURUSD')


class WatchLoopTests(unittest.TestCase):
    def setUp(self):
        self.original_mt5 = tls.mt5
        self.original_imported = tls._MT5_IMPORTED
        self.original_ready = tls._MT5_READY
        self.original_sqlite = tls.sqlite3
        tls.mt5 = None
        tls._MT5_IMPORTED = True
        tls._MT5_READY = True
        tls.sqlite3 = None  # Disable DB for most tests

    def tearDown(self):
        tls.mt5 = self.original_mt5
        tls._MT5_IMPORTED = self.original_imported
        tls._MT5_READY = self.original_ready
        tls.sqlite3 = self.original_sqlite

    def test_watch_loop_keyboard_interrupt(self):
        # Test that KeyboardInterrupt is handled gracefully
        symbols = ['EURUSD', 'GBPUSD']

        with patch('time.sleep', side_effect=KeyboardInterrupt()):
            with patch.object(tls, 'process_once') as mock_process:
                with patch('builtins.print') as mock_print:
                    tls.watch_loop(
                        symbols=symbols,
                        interval=1.0,
                        min_rrr=1.0,
                        min_prox_sl=0.0,
                        max_prox_sl=1.0,
                        top=None,
                        brief=True,
                        debug=True
                    )
                    # Check that process_once was called at least once
                    mock_process.assert_called()
                    # Check that the stop message was printed
                    self.assertTrue(any('Stopped watching' in call[0][0] for call in mock_print.call_args_list))

    def test_watch_loop_with_exclude_set(self):
        # Test watch_loop with exclude set
        symbols = ['EURUSD', 'GBPUSD']
        exclude_set = {'EURUSD'}

        with patch('time.sleep', side_effect=[None, KeyboardInterrupt()]):
            with patch.object(tls, 'process_once') as mock_process:
                with patch('builtins.print') as mock_print:
                    tls.watch_loop(
                        symbols=symbols,
                        interval=1.0,
                        min_rrr=1.0,
                        min_prox_sl=0.0,
                        max_prox_sl=1.0,
                        top=None,
                        brief=True,
                        debug=True,
                        exclude_set=exclude_set
                    )
                    # Check that process_once was called with exclude_set
                    args, kwargs = mock_process.call_args
                    self.assertEqual(kwargs['exclude_set'], exclude_set)
                    # Check that exclude message was printed
                    self.assertTrue(any('Will filter out symbols' in call[0][0] for call in mock_print.call_args_list))

    def test_watch_loop_events(self):
        # Test _watch_loop_events function
        symbols = ['EURUSD', 'GBPUSD']

        with patch.object(tls, 'watch_loop') as mock_watch:
            tls._watch_loop_events(
                symbols=symbols,
                min_rrr=1.0,
                min_prox_sl=0.0,
                max_prox_sl=1.0,
                top=None,
                brief=True,
                debug=True
            )
            # Check that watch_loop was called with interval=1.0
            mock_watch.assert_called_once()
            args, kwargs = mock_watch.call_args
            self.assertEqual(kwargs['interval'], 1.0)


if __name__ == '__main__':
    unittest.main()
