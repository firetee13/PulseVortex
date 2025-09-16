
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import timelapse_setups as tls

UTC = timezone.utc


class FakeMT5:
    TIMEFRAME_D1 = 1
    TIMEFRAME_H4 = 2
    TIMEFRAME_W1 = 3
    TIMEFRAME_H1 = 4
    TIMEFRAME_M15 = 5
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

    def symbols_get(self):
        return []


class ReadSeriesMT5Tests(unittest.TestCase):
    def setUp(self):
        self.original_mt5 = tls.mt5
        self.original_imported = tls._MT5_IMPORTED
        self.original_ready = tls._MT5_READY
        self.original_last_tick = dict(tls._LAST_TICK_CACHE)
        self.original_rate_cache = dict(tls._RATE_CACHE)
        tls._LAST_TICK_CACHE.clear()
        tls._RATE_CACHE.clear()
        tls._MT5_IMPORTED = True
        tls._MT5_READY = True

    def tearDown(self):
        tls.mt5 = self.original_mt5
        tls._MT5_IMPORTED = self.original_imported
        tls._MT5_READY = self.original_ready
        tls._LAST_TICK_CACHE.clear()
        tls._LAST_TICK_CACHE.update(self.original_last_tick)
        tls._RATE_CACHE.clear()
        tls._RATE_CACHE.update(self.original_rate_cache)

    def test_read_series_mt5_skips_history_when_tick_fresh(self):
        fake = FakeMT5(datetime.now(UTC))
        tls.mt5 = fake
        series, _, _ = tls.read_series_mt5(['EURUSD'])
        self.assertIn('EURUSD', series)
        self.assertEqual(fake.copy_ticks_calls, 0)
        self.assertTrue(all(fake_call[1] in {FakeMT5.TIMEFRAME_D1, FakeMT5.TIMEFRAME_H4, FakeMT5.TIMEFRAME_W1, FakeMT5.TIMEFRAME_H1, FakeMT5.TIMEFRAME_M15} for fake_call in fake.rates_calls))

    def test_read_series_mt5_uses_history_when_tick_stale(self):
        stale_tick = datetime.now(UTC) - timedelta(seconds=tls.TICK_FRESHNESS_SEC + 10)
        fake = FakeMT5(stale_tick)
        tls.mt5 = fake
        tls.read_series_mt5(['EURUSD'])
        self.assertGreaterEqual(fake.copy_ticks_calls, 1)

    def test_rate_cache_reuses_copy_rates_calls(self):
        fake = FakeMT5(datetime.now(UTC))
        tls.mt5 = fake
        tls.read_series_mt5(['EURUSD'])
        first_call_count = len(fake.rates_calls)
        fake.tick_time = datetime.now(UTC)
        tls.read_series_mt5(['EURUSD'])
        self.assertEqual(len(fake.rates_calls), first_call_count)


class SnapshotHelpersTests(unittest.TestCase):
    def test_snapshot_g_returns_numeric_without_parsing(self):
        now = datetime.now(UTC)
        row = {tls.canonicalize_key('Bid'): 1.2345, tls.HEADER_SYMBOL: 'EURUSD'}
        snap = tls.Snapshot(ts=now, row=row)
        self.assertEqual(snap.g('Bid'), 1.2345)


class InsertResultsDbTests(unittest.TestCase):
    def setUp(self):
        self.orig_db_conn = tls._DB_CONN
        if tls._DB_CONN is not None:
            try:
                tls._DB_CONN.close()
            except Exception:
                pass
        tls._DB_CONN = None
        self.orig_path = tls.DEFAULT_SQLITE_PATH
        self.temp_dir = tempfile.mkdtemp(prefix='tls_db_test_')
        self.db_path = os.path.join(self.temp_dir, 'test.db')
        tls.DEFAULT_SQLITE_PATH = self.db_path

    def tearDown(self):
        if tls._DB_CONN is not None:
            try:
                tls._DB_CONN.close()
            except Exception:
                pass
        tls._DB_CONN = self.orig_db_conn
        tls.DEFAULT_SQLITE_PATH = self.orig_path
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_reuses_sqlite_connection(self):
        as_of = datetime.now(UTC)
        results = [
            {
                'symbol': 'EURUSD',
                'direction': 'Buy',
                'price': 1.2345,
                'sl': 1.2330,
                'tp': 1.2390,
                'rrr': 2.0,
                'score': 1.0,
                'explain': 'unit test',
                'as_of': as_of,
            }
        ]
        tls.insert_results_to_db(results, detected_at=as_of)
        first_conn = tls._DB_CONN
        self.assertIsNotNone(first_conn)
        tls.insert_results_to_db(results, detected_at=as_of)
        self.assertIs(tls._DB_CONN, first_conn)


if __name__ == '__main__':
    unittest.main()
