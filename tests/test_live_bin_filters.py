import sqlite3
from monitor.db import (
    ensure_live_bin_filters_sqlite,
    load_live_bin_filters_sqlite,
    persist_live_bin_filters_sqlite,
)


def test_live_bin_filters_roundtrip():
    conn = sqlite3.connect(":memory:")
    ensure_live_bin_filters_sqlite(conn)

    initial = load_live_bin_filters_sqlite(conn)
    assert initial is not None
    assert initial["min_edge"] is None
    assert initial["min_trades"] is None
    assert initial["allowed_bins"] in (None, {})
    assert initial["live_enabled"] is False

    persist_live_bin_filters_sqlite(
        conn,
        min_edge=0.15,
        min_trades=25,
        allowed_bins={"forex": {"0.2-0.3", "0.3-0.4"}},
        live_trading_enabled=True,
    )
    updated = load_live_bin_filters_sqlite(conn)
    assert updated is not None
    assert updated["min_edge"] == 0.15
    assert updated["min_trades"] == 25
    assert isinstance(updated["allowed_bins"], dict)
    assert "forex" in updated["allowed_bins"]
    assert updated["allowed_bins"]["forex"] == {"0.2-0.3", "0.3-0.4"}
    assert updated["live_enabled"] is True
