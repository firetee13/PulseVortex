
import os
import shutil
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

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


if __name__ == '__main__':
    unittest.main()
