
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


class SlDistanceFilterTests(unittest.TestCase):
    def test_sell_uses_ask_for_sl_distance(self):
        # Reproduce SA40-like case where using Bid would pass, but Ask should fail
        now = datetime.now(UTC)
        sym = 'TESTIDX'
        bid = 97239.9
        ask = 97271.9  # spread = 32.0
        s1 = 96788.26666666665
        r1 = 97571.56666666665

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
        results, reasons = tls.analyze(series, min_rrr=1.0, min_prox_sl=0.0, min_sl_pct=0.0, as_of_ts=now, debug=False)
        # No results should be produced due to SL too close to spread
        self.assertEqual(results, [])
        self.assertIn('sl_too_close_to_spread', reasons)
        self.assertIn(sym, reasons['sl_too_close_to_spread'])


class VolumeFilterTests(unittest.TestCase):
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

    def test_get_tick_volume_last_5_bars_high_volume(self):
        now = datetime.now(UTC)
        fake_mt5 = FakeMT5(now)

        # Simulate M1 bars where each has high tick volume (>= 10)
        high_volume_m1_rates = [
            {'time': i, 'close': 1.0, 'high': 1.1, 'low': 0.9, 'tick_volume': 10} for i in range(5)
        ]
        fake_mt5.rates_return[FakeMT5.TIMEFRAME_M1] = high_volume_m1_rates
        tls.mt5 = fake_mt5

        volume_check = tls._get_tick_volume_last_5_bars('EURUSD')
        self.assertTrue(volume_check)

    def test_get_tick_volume_last_5_bars_one_bar_low_volume(self):
        now = datetime.now(UTC)
        fake_mt5 = FakeMT5(now)

        # Simulate M1 bars where one bar has low tick volume (< 10)
        mixed_volume_m1_rates = [
            {'time': i, 'close': 1.0, 'high': 1.1, 'low': 0.9, 'tick_volume': 15} for i in range(4)
        ]
        mixed_volume_m1_rates.append({'time': 4, 'close': 1.0, 'high': 1.1, 'low': 0.9, 'tick_volume': 5}) # Last bar has low volume
        fake_mt5.rates_return[FakeMT5.TIMEFRAME_M1] = mixed_volume_m1_rates
        tls.mt5 = fake_mt5

        volume_check = tls._get_tick_volume_last_5_bars('EURUSD')
        self.assertFalse(volume_check)

    def test_analyze_filters_low_volume_symbol(self):
        now = datetime.now(UTC)
        sym = 'LOWVOL'
        fake_mt5 = FakeMT5(now)

        # Simulate M1 bars where one bar has low tick volume (< 10)
        low_volume_m1_rates = [
            {'time': i, 'close': 1.0, 'high': 1.1, 'low': 0.9, 'tick_volume': 15} for i in range(4)
        ]
        low_volume_m1_rates.append({'time': 4, 'close': 1.0, 'high': 1.1, 'low': 0.9, 'tick_volume': 5}) # Last bar has low volume
        fake_mt5.rates_return[FakeMT5.TIMEFRAME_M1] = low_volume_m1_rates
        tls.mt5 = fake_mt5

        # Create snapshots for a valid Buy setup that should pass all other checks
        first = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('D1 Close'): 1.0000,
            tls.canonicalize_key('Strength 4H'): 0.1,
        })
        last = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('Bid'): 1.1000,
            tls.canonicalize_key('Ask'): 1.1002, # small spread
            tls.canonicalize_key('S1 Level M5'): 1.0900,
            tls.canonicalize_key('R1 Level M5'): 1.1200,
            tls.canonicalize_key('Strength 4H'): 0.6,
            tls.canonicalize_key('Strength 1D'): 0.2,
            tls.canonicalize_key('Strength 1W'): 0.1,  # ensure overall Buy (>=2 positives)
            tls.canonicalize_key('D1 Close'): 1.1050,
            tls.canonicalize_key('D1 High'): 1.1100,
            tls.canonicalize_key('D1 Low'): 1.0900,
            tls.canonicalize_key('Spread%'): 0.018, # Good spread
            tls.canonicalize_key('Recent Tick'): 1,
            tls.canonicalize_key('Last Tick UTC'): now.strftime('%Y-%m-%d %H:%M:%S'),
        })
        series = {sym: [first, last]}
        results, reasons = tls.analyze(series, min_rrr=1.0, min_prox_sl=0.0, min_sl_pct=0.0, as_of_ts=now, debug=True)

        self.assertEqual(results, [])
        self.assertIn('low_tick_volume_last_5_bars', reasons)
        self.assertIn(sym, reasons['low_tick_volume_last_5_bars'])

    def test_analyze_passes_high_volume_symbol(self):
        now = datetime.now(UTC)
        sym = 'HIGHVOL'
        fake_mt5 = FakeMT5(now)

        # Simulate M1 bars where each has high tick volume (>= 10)
        high_volume_m1_rates = [
            {'time': i, 'close': 1.0, 'high': 1.1, 'low': 0.9, 'tick_volume': 10} for i in range(5)
        ]
        fake_mt5.rates_return[FakeMT5.TIMEFRAME_M1] = high_volume_m1_rates
        tls.mt5 = fake_mt5

        # Create snapshots for a valid Buy setup that should pass
        first = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('D1 Close'): 1.0000,
            tls.canonicalize_key('Strength 4H'): 0.1,
        })
        last = tls.Snapshot(ts=now, row={
            tls.HEADER_SYMBOL: sym,
            tls.canonicalize_key('Bid'): 1.1000,
            tls.canonicalize_key('Ask'): 1.1002, # small spread
            tls.canonicalize_key('S1 Level M5'): 1.0900,
            tls.canonicalize_key('R1 Level M5'): 1.1200,
            tls.canonicalize_key('Strength 4H'): 0.6,
            tls.canonicalize_key('Strength 1D'): 0.2,
            tls.canonicalize_key('Strength 1W'): 0.1,  # ensure overall Buy (>=2 positives)
            tls.canonicalize_key('D1 Close'): 1.1050,
            tls.canonicalize_key('D1 High'): 1.1100,
            tls.canonicalize_key('D1 Low'): 1.0900,
            tls.canonicalize_key('Spread%'): 0.018, # Good spread
            tls.canonicalize_key('Recent Tick'): 1,
            tls.canonicalize_key('Last Tick UTC'): now.strftime('%Y-%m-%d %H:%M:%S'),
        })
        series = {sym: [first, last]}
        results, reasons = tls.analyze(series, min_rrr=1.0, min_prox_sl=0.0, min_sl_pct=0.0, as_of_ts=now, debug=True)

        self.assertGreater(len(results), 0)
        self.assertEqual(results[0]['symbol'], sym)
        self.assertNotIn('low_tick_volume_last_5_bars', reasons)


if __name__ == '__main__':
    unittest.main()
