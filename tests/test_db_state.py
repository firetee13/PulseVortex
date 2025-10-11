from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

import monitor.core.db as db_module
from monitor.core.db import (
    _parse_utc_datetime,
    backfill_hit_columns_sqlite,
    ensure_hits_table_sqlite,
    ensure_tp_sl_setup_state_sqlite,
    load_setups_sqlite,
    load_tp_sl_setup_state_sqlite,
    persist_tp_sl_setup_state_sqlite,
    record_hit_sqlite,
)
from monitor.core.domain import Hit, Setup

UTC = timezone.utc


def make_conn():
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def test_parse_utc_datetime_variants():
    aware = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    naive = datetime(2024, 1, 1, 0, 0)
    assert _parse_utc_datetime(None) is None
    assert _parse_utc_datetime(aware) == aware
    parsed_naive = _parse_utc_datetime(naive)
    assert parsed_naive.tzinfo == UTC
    assert _parse_utc_datetime("2024-01-01T00:00:00").tzinfo == UTC
    assert _parse_utc_datetime("bad-date") is None


def test_tp_sl_state_roundtrip_handles_naive_and_aware():
    conn = make_conn()
    ensure_tp_sl_setup_state_sqlite(conn)

    naive_dt = datetime(2024, 1, 1, 12, 0)
    aware_dt = datetime(2024, 1, 2, 6, 0, tzinfo=timezone.utc)

    persist_tp_sl_setup_state_sqlite(
        conn,
        {
            1: naive_dt,
            2: aware_dt,
            3: None,  # skipped
        },
    )

    loaded = load_tp_sl_setup_state_sqlite(conn, [1, 2, 3])
    assert set(loaded.keys()) == {1, 2}
    assert loaded[1].tzinfo == UTC
    assert loaded[2].tzinfo == UTC
    assert load_tp_sl_setup_state_sqlite(conn, []) == {}
    conn.close()


def test_backfill_handles_missing_setups_table():
    conn = make_conn()
    ensure_hits_table_sqlite(conn)
    hit_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    conn.execute(
        """
        INSERT INTO timelapse_hits (setup_id, symbol, direction, sl, tp, hit, hit_price, hit_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1,
            "EURUSD",
            "buy",
            1.0,
            1.2,
            "TP",
            1.1,
            hit_time.strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )

    backfill_hit_columns_sqlite(conn, "nonexistent_table", utc3_hours=3)

    cur = conn.cursor()
    cur.execute(
        "SELECT hit_time_utc3, entry_time_utc3 FROM timelapse_hits WHERE setup_id = ?",
        (1,),
    )
    hit_time_utc3, entry_time_utc3 = cur.fetchone()
    assert hit_time_utc3 == (hit_time + timedelta(hours=3)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    assert entry_time_utc3 is None
    conn.close()


def test_load_setups_since_hours_filters_rows():
    conn = make_conn()
    ensure_hits_table_sqlite(conn)
    cur = conn.cursor()
    cur.execute(
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
    old_inserted = datetime.now(UTC) - timedelta(hours=10)
    recent_inserted = datetime.now(UTC) - timedelta(hours=1)
    cur.execute(
        """
        INSERT INTO timelapse_setups (id, symbol, direction, sl, tp, price, as_of, inserted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1,
            "EURUSD",
            "buy",
            1.0,
            1.2,
            1.1,
            old_inserted.strftime("%Y-%m-%d %H:%M:%S"),
            old_inserted.strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )
    cur.execute(
        """
        INSERT INTO timelapse_setups (id, symbol, direction, sl, tp, price, as_of, inserted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            2,
            "BTCUSD",
            "sell",
            20000.0,
            19000.0,
            19500.0,
            recent_inserted.strftime("%Y-%m-%d %H:%M:%S"),
            recent_inserted.strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )

    rows = load_setups_sqlite(
        conn, "timelapse_setups", since_hours=2, ids=None, symbols=None
    )
    assert [row.id for row in rows] == [2]
    conn.close()


def test_record_hit_uses_precious_metals_digits(monkeypatch):
    conn = make_conn()
    ensure_hits_table_sqlite(conn)

    original_fullmatch = db_module.re.fullmatch

    def fake_fullmatch(pattern, string):
        if pattern == r"[A-Z]{6}":
            return None
        if pattern == r"XA[UG][A-Z]{3}" and string == "XAUUSD":
            return object()
        return original_fullmatch(pattern, string)

    monkeypatch.setattr(db_module.re, "fullmatch", fake_fullmatch)

    setup = Setup(
        id=9,
        symbol="XAUUSD",
        direction="buy",
        sl=1900.1234,
        tp=1950.9876,
        entry_price=None,
        as_of_utc=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
    )
    hit = Hit(
        kind="TP", time_utc=datetime(2024, 1, 1, 1, 0, tzinfo=UTC), price=1951.2345
    )

    record_hit_sqlite(conn, setup, hit, dry_run=False, verbose=False)

    cur = conn.cursor()
    cur.execute("SELECT sl, tp, hit_price FROM timelapse_hits WHERE setup_id = ?", (9,))
    sl, tp, hit_price = cur.fetchone()
    assert sl == pytest.approx(1900.12)
    assert tp == pytest.approx(1950.99)
    assert hit_price == pytest.approx(1951.23)
    conn.close()
