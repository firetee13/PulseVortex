import sqlite3
from datetime import datetime, timezone

from monitor.db import (
    ensure_tp_sl_setup_state_sqlite,
    load_tp_sl_setup_state_sqlite,
    persist_order_sent_sqlite,
    persist_tp_sl_setup_state_sqlite,
)


def _utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def test_state_table_roundtrip():
    conn = sqlite3.connect(":memory:")
    ensure_tp_sl_setup_state_sqlite(conn)

    cols = {row[1] for row in conn.execute("PRAGMA table_info('tp_sl_setup_state')")}
    assert "order_ticket" in cols
    assert "order_sent_at" in cols

    first = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    second = datetime(2024, 1, 2, 8, 30, tzinfo=timezone.utc)

    persist_tp_sl_setup_state_sqlite(conn, {1: first, 2: second})

    loaded = load_tp_sl_setup_state_sqlite(conn, [1, 2])

    assert 1 in loaded
    assert 2 in loaded
    assert _utc(loaded[1]) == first
    assert _utc(loaded[2]) == second


def test_state_update_overwrites_existing():
    conn = sqlite3.connect(":memory:")
    ensure_tp_sl_setup_state_sqlite(conn)

    initial = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    updated = datetime(2024, 1, 3, 0, 0, tzinfo=timezone.utc)

    persist_tp_sl_setup_state_sqlite(conn, {5: initial})
    persist_tp_sl_setup_state_sqlite(conn, {5: updated})

    loaded = load_tp_sl_setup_state_sqlite(conn, [5])

    assert loaded[5].tzinfo == timezone.utc
    assert _utc(loaded[5]) == updated


def test_persist_order_sent_respects_existing_last_checked():
    conn = sqlite3.connect(":memory:")
    ensure_tp_sl_setup_state_sqlite(conn)

    baseline = datetime(2024, 2, 1, 0, 0, tzinfo=timezone.utc)
    persist_tp_sl_setup_state_sqlite(conn, {10: baseline})

    before_row = conn.execute(
        "SELECT last_checked_utc FROM tp_sl_setup_state WHERE setup_id = 10"
    ).fetchone()
    assert before_row is not None
    assert before_row[0] == baseline.strftime("%Y-%m-%d %H:%M:%S")

    sent_at = datetime(2024, 2, 1, 1, 0, tzinfo=timezone.utc)
    persist_order_sent_sqlite(conn, setup_id=10, ticket="123456", sent_at=sent_at)

    row = conn.execute(
        "SELECT last_checked_utc, order_ticket, order_sent_at FROM tp_sl_setup_state WHERE setup_id = 10"
    ).fetchone()
    assert row is not None
    last_checked_utc, order_ticket, order_sent_at = row
    assert last_checked_utc == baseline.strftime("%Y-%m-%d %H:%M:%S")
    assert order_ticket == "123456"
    assert order_sent_at == sent_at.strftime("%Y-%m-%d %H:%M:%S")
