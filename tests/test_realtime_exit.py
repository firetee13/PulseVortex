import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

try:
    import fakeredis
except ImportError:  # pragma: no cover - optional dependency
    fakeredis = None

from monitor.domain import TickFetchStats
from monitor.realtime_exit import RedisRealtimeExitManager, SetupFilters
from monitor.redis_ticks import RedisTickCache, TICKS_KEY_FMT

UTC = timezone.utc


def _make_temp_db() -> str:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE timelapse_setups (
            id INTEGER PRIMARY KEY,
            symbol TEXT,
            direction TEXT,
            sl REAL,
            tp REAL,
            price REAL,
            as_of TEXT,
            inserted_at TEXT
        )
        """
    )
    conn.commit()
    conn.close()
    return path


def _insert_setup(db_path: str, *, setup_id: int, symbol: str, direction: str, sl: float, tp: float, price: float, as_of: datetime) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        INSERT INTO timelapse_setups (id, symbol, direction, sl, tp, price, as_of, inserted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            setup_id,
            symbol,
            direction,
            sl,
            tp,
            price,
            as_of.strftime("%Y-%m-%d %H:%M:%S"),
            datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )
    conn.commit()
    conn.close()


def _fake_tick(time_msc: int, bid: float, ask: float) -> dict:
    return {"time_msc": time_msc, "bid": bid, "ask": ask}


def _fake_ticks_range_factory(ticks_by_symbol):
    def _fake(symbol, start, end, trace=False):  # pragma: no cover - helper
        ticks = list(ticks_by_symbol.get(symbol, []))
        stats = TickFetchStats(
            pages=1 if ticks else 0,
            total_ticks=len(ticks),
            elapsed_s=0.0,
            fetch_s=0.0,
        )
        return ticks, stats

    return _fake


def _make_manager(db_path: str) -> RedisRealtimeExitManager:
    return RedisRealtimeExitManager(
        redis_url="redis://localhost:6379/0",
        db_path=db_path,
        filters=SetupFilters(since_hours=None, ids=None, symbols=None),
        tick_window_seconds=10,
        poll_interval_ms=200,
        fallback_interval_s=30,
        dry_run=False,
        verbose=False,
        redis_prefix="test",
    )


@unittest.skipUnless(fakeredis is not None, "fakeredis required for Redis realtime tests")
class RedisRealtimeExitTests(unittest.TestCase):
    def _patch_mt5(self, ticks):
        return mock.patch.multiple(
            "monitor.realtime_exit",
            get_server_offset_hours=mock.DEFAULT,
            resolve_symbol=mock.DEFAULT,
            ticks_range_all=mock.DEFAULT,
        )

    def test_bootstrap_writes_trade_and_ticks(self):
        db_path = _make_temp_db()
        self.addCleanup(lambda: os.path.exists(db_path) and os.remove(db_path))
        entry_time = datetime.now(UTC) - timedelta(seconds=5)
        _insert_setup(db_path, setup_id=1, symbol="BTCUSD", direction="buy", sl=90.0, tp=200.0, price=150.0, as_of=entry_time)
        ticks = {
            "BTCUSD": [_fake_tick(int(entry_time.timestamp() * 1000) + 1000, 180.0, 181.0)]
        }

        fake = fakeredis.FakeRedis(decode_responses=True)

        with mock.patch("monitor.realtime_exit.get_server_offset_hours", return_value=0),             mock.patch("monitor.realtime_exit.resolve_symbol", side_effect=lambda s: s),             mock.patch("monitor.realtime_exit.ticks_range_all", side_effect=_fake_ticks_range_factory(ticks)):
            manager = _make_manager(db_path)
            manager._redis = fake
            manager._conn = sqlite3.connect(db_path)
            self.addCleanup(manager._conn.close)

            manager._bootstrap_trades()

            trade_key = "test:trade:1"
            stored = manager._redis.hgetall(trade_key)
            self.assertEqual(stored["symbol"], "BTCUSD")
            self.assertEqual(stored["direction"], "buy")
            self.assertEqual(stored["mt5_symbol"], "BTCUSD")

            tick_entries = manager._redis.zrange("test:ticks:BTCUSD", 0, -1, withscores=True)
            self.assertEqual(len(tick_entries), 1)
            payload, score = tick_entries[0]
            self.assertEqual(int(score), int(entry_time.timestamp() * 1000) + 1000)
            self.assertIn("180", payload)

    def test_evaluate_records_tp_hit(self):
        db_path = _make_temp_db()
        self.addCleanup(lambda: os.path.exists(db_path) and os.remove(db_path))
        entry_time = datetime.now(UTC) - timedelta(seconds=5)
        _insert_setup(db_path, setup_id=1, symbol="BTCUSD", direction="buy", sl=90.0, tp=200.0, price=150.0, as_of=entry_time)
        tick_ts = int((entry_time + timedelta(seconds=3)).timestamp() * 1000)
        ticks = {
            "BTCUSD": [
                _fake_tick(tick_ts, 205.0, 205.5),
            ]
        }

        fake = fakeredis.FakeRedis(decode_responses=True)

        with mock.patch("monitor.realtime_exit.get_server_offset_hours", return_value=0),             mock.patch("monitor.realtime_exit.resolve_symbol", side_effect=lambda s: s),             mock.patch("monitor.realtime_exit.ticks_range_all", side_effect=_fake_ticks_range_factory(ticks)):
            manager = _make_manager(db_path)
            manager._redis = fake
            manager._conn = sqlite3.connect(db_path)
            self.addCleanup(manager._conn.close)

            manager._bootstrap_trades()

            from monitor.db import ensure_hits_table_sqlite

            ensure_hits_table_sqlite(manager._conn)

            entry = next(iter(manager._setups.values()))
            manager._evaluate_setup(entry)

            conn = sqlite3.connect(db_path)
            self.addCleanup(conn.close)
            row = conn.execute("SELECT hit, hit_price FROM timelapse_hits WHERE setup_id = 1").fetchone()

            self.assertIsNotNone(row)
            self.assertEqual(row[0], "TP")
            self.assertAlmostEqual(row[1], 205.0)

            active = manager._redis.smembers("test:trades:active")
            self.assertNotIn("1", active)

    def test_tick_cache_window(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        cache = RedisTickCache(
            redis_url="redis://localhost:6379/0",
            prefix="test",
            client=fake,
            test_connection=False,
        )
        key = TICKS_KEY_FMT.format(prefix="test", symbol="BTCUSD")
        payload1 = json.dumps({"time_msc": 1000, "bid": 1.1, "ask": 1.2}, separators=(",", ":"))
        payload2 = json.dumps({"time_msc": 2000, "bid": 1.3, "ask": 1.4}, separators=(",", ":"))
        fake.zadd(key, {payload1: 1000, payload2: 2000})

        ticks = cache.window("BTCUSD", 500, 2500)
        self.assertEqual(len(ticks), 2)
        self.assertAlmostEqual(ticks[0].bid or 0.0, 1.1)
        self.assertEqual(ticks[1].time_msc, 2000)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
