import sqlite3
from datetime import datetime, timedelta, timezone
import unittest

from monitor.core.db import (
    backfill_hit_columns_sqlite,
    ensure_hits_table_sqlite,
    load_recorded_ids_sqlite,
    load_setups_sqlite,
    record_hit_sqlite,
)
from monitor.core.domain import Hit, Setup


UTC = timezone.utc


class DbHelpersTests(unittest.TestCase):

    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("PRAGMA foreign_keys = ON")
        ensure_hits_table_sqlite(self.conn)
        self._create_setups_table()

    def tearDown(self) -> None:
        self.conn.close()

    def _create_setups_table(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE timelapse_setups (
                id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                sl REAL NOT NULL,
                tp REAL NOT NULL,
                price REAL,
                as_of TEXT NOT NULL,
                inserted_at TEXT NOT NULL
            )
            """
        )

    def _insert_setup(self, **overrides):
        defaults = dict(
            id=1,
            symbol="EURUSD",
            direction="buy",
            sl=1.0500,
            tp=1.0900,
            price=1.0700,
            as_of=datetime(2025, 1, 1, 12, 0, tzinfo=UTC).isoformat(timespec="seconds"),
            inserted_at=datetime(2025, 1, 1, 12, 1, tzinfo=UTC).isoformat(timespec="seconds"),
        )
        defaults.update(overrides)
        columns = ",".join(defaults.keys())
        placeholders = ",".join(["?"] * len(defaults))
        self.conn.execute(
            f"INSERT INTO timelapse_setups ({columns}) VALUES ({placeholders})",
            tuple(defaults.values()),
        )

    def test_backfill_populates_missing_columns(self) -> None:
        as_of = datetime(2025, 1, 2, 8, 45, tzinfo=UTC)
        hit_time = as_of + timedelta(hours=5)
        self._insert_setup(id=7, price=1.2345, as_of=as_of.isoformat(timespec="seconds"))
        self.conn.execute(
            """
            INSERT INTO timelapse_hits (
                setup_id, symbol, direction, sl, tp, hit, hit_price, hit_time,
                hit_time_utc3, entry_time_utc3, entry_price
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
            """,
            (
                7,
                "EURUSD",
                "buy",
                1.2000,
                1.2400,
                "TP",
                1.23999,
                hit_time.strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )

        backfill_hit_columns_sqlite(self.conn, "timelapse_setups", utc3_hours=3)

        cur = self.conn.cursor()
        cur.execute("SELECT entry_time_utc3, hit_time_utc3, entry_price FROM timelapse_hits WHERE setup_id = 7")
        entry_time_utc3, hit_time_utc3, entry_price = cur.fetchone()

        self.assertEqual(entry_time_utc3, (as_of + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S"))
        self.assertEqual(hit_time_utc3, (hit_time + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S"))
        self.assertAlmostEqual(entry_price, 1.2345)

    def test_load_setups_filters_by_ids_and_symbols(self) -> None:
        as_of = datetime(2025, 3, 10, 6, 0, tzinfo=UTC)
        self._insert_setup(id=1, symbol="EURUSD", as_of=as_of.isoformat(timespec="seconds"))
        self._insert_setup(id=2, symbol="BTCUSD", as_of=(as_of + timedelta(minutes=1)).isoformat(timespec="seconds"))
        self._insert_setup(id=3, symbol="US500", as_of=(as_of + timedelta(minutes=2)).isoformat(timespec="seconds"))

        by_id = load_setups_sqlite(self.conn, "timelapse_setups", since_hours=None, ids=[2], symbols=None)
        self.assertEqual([setup.id for setup in by_id], [2])
        self.assertEqual(by_id[0].symbol, "BTCUSD")
        self.assertEqual(by_id[0].as_of_utc.tzinfo, UTC)

        by_symbol = load_setups_sqlite(self.conn, "timelapse_setups", since_hours=None, ids=None, symbols=["EURUSD", "US500"])
        self.assertEqual([setup.id for setup in by_symbol], [1, 3])

    def test_record_hit_sqlite_inserts_and_updates_rows(self) -> None:
        setup = Setup(
            id=11,
            symbol="EURUSD",
            direction="buy",
            sl=1.05001,
            tp=1.09999,
            entry_price=1.0754321,
            as_of_utc=datetime(2025, 4, 1, 9, 30, tzinfo=UTC),
        )
        hit_time = datetime(2025, 4, 1, 10, 15, tzinfo=UTC)
        hit = Hit(kind="TP", time_utc=hit_time, price=1.0998765)

        record_hit_sqlite(self.conn, setup, hit, dry_run=False, verbose=False)

        cur = self.conn.cursor()
        cur.execute(
            "SELECT sl, tp, hit_price, hit_time, entry_price FROM timelapse_hits WHERE setup_id = ?",
            (11,),
        )
        row = cur.fetchone()
        self.assertIsNotNone(row)
        sl, tp, hit_price, hit_time_db, entry_price = row
        self.assertAlmostEqual(sl, 1.05001)
        self.assertAlmostEqual(tp, 1.09999)
        self.assertAlmostEqual(hit_price, 1.09988)
        self.assertEqual(hit_time_db, hit_time.strftime("%Y-%m-%d %H:%M:%S"))
        self.assertAlmostEqual(entry_price, 1.07543)

        updated_setup = Setup(
            id=11,
            symbol="EURUSD",
            direction="buy",
            sl=1.0499,
            tp=1.0995,
            entry_price=1.0767,
            as_of_utc=datetime(2025, 4, 1, 9, 30, tzinfo=UTC),
        )
        updated_hit = Hit(kind="SL", time_utc=hit_time + timedelta(hours=1), price=1.0200001)
        record_hit_sqlite(self.conn, updated_setup, updated_hit, dry_run=False, verbose=False)

        cur.execute(
            "SELECT sl, tp, hit, hit_price, entry_price FROM timelapse_hits WHERE setup_id = ?",
            (11,),
        )
        sl, tp, hit_kind, hit_price, entry_price = cur.fetchone()
        self.assertAlmostEqual(sl, 1.0499)
        self.assertAlmostEqual(tp, 1.0995)
        self.assertEqual(hit_kind, "TP")  # current implementation keeps original hit kind on update
        self.assertAlmostEqual(hit_price, 1.02)
        self.assertAlmostEqual(entry_price, 1.0767)

    def test_record_hit_respects_dry_run(self) -> None:
        setup = Setup(
            id=21,
            symbol="USDJPY",
            direction="sell",
            sl=151.000,
            tp=149.500,
            entry_price=150.250,
            as_of_utc=datetime(2025, 6, 1, 0, 0, tzinfo=UTC),
        )
        hit = Hit(kind="TP", time_utc=datetime(2025, 6, 1, 1, 0, tzinfo=UTC), price=149.600)

        record_hit_sqlite(self.conn, setup, hit, dry_run=True, verbose=False)
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM timelapse_hits WHERE setup_id = ?", (21,))
        count = cur.fetchone()[0]
        self.assertEqual(count, 0)

    def test_load_recorded_ids_sqlite(self) -> None:
        self.conn.execute(
            "INSERT INTO timelapse_hits (setup_id, symbol, direction, sl, tp, hit, hit_price, hit_time) VALUES (?,?,?,?,?,?,?,?)",
            (1, "EURUSD", "buy", 1.0, 2.0, "TP", 1.5, "2025-01-01 00:00:00"),
        )
        self.conn.execute(
            "INSERT INTO timelapse_hits (setup_id, symbol, direction, sl, tp, hit, hit_price, hit_time) VALUES (?,?,?,?,?,?,?,?)",
            (2, "EURUSD", "buy", 1.0, 2.0, "TP", 1.5, "2025-01-01 00:00:00"),
        )

        existing = load_recorded_ids_sqlite(self.conn, [2, 3, 4])
        self.assertEqual(existing, {2})


if __name__ == "__main__":
    unittest.main()
