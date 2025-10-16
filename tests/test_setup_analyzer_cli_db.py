from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from monitor.cli import setup_analyzer as sa

UTC = timezone.utc


@pytest.fixture(autouse=True)
def reset_db_connection():
    # Ensure each test uses a fresh SQLite connection handle
    sa._close_db_connection()
    yield
    sa._close_db_connection()


def test_insert_results_to_db_inserts_and_logs(monkeypatch, tmp_path, capsys):
    db_path = tmp_path / "timelapse.db"
    monkeypatch.setattr(sa, "default_db_path", lambda: db_path)

    detected_at = datetime(2024, 1, 2, 12, 0, tzinfo=UTC)
    result = {
        "symbol": "EURUSD",
        "direction": "buy",
        "price": 1.23456,
        "sl": 1.22000,
        "tp": 1.25000,
        "rrr": 2.5,
        "score": 1.75,
        "strength_1h": 0.4,
        "strength_4h": 0.6,
        "strength_1d": 0.8,
        "as_of": datetime(2024, 1, 2, 11, 55, tzinfo=UTC),
        "proximity_to_sl": 0.12,
        "bid": 1.23450,
        "ask": 1.23470,
        "tick_utc": "2024-01-02 11:55:00",
        "source": "mt5",
    }

    sa.insert_results_to_db([result], detected_at=detected_at)

    out = capsys.readouterr().out
    assert "[DB] Inserted 1 new setup(s):" in out
    assert "EURUSD" in out

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT symbol, direction, proximity_to_sl, detected_at FROM timelapse_setups"
        ).fetchall()
    finally:
        conn.close()
    assert rows == [
        ("EURUSD", "buy", 0.12, detected_at.strftime("%Y-%m-%d %H:%M:%S.%f"))
    ]


def test_main_invokes_process_once_with_cli_symbols(monkeypatch):
    original_override = sa._MT5_PATH_OVERRIDE
    try:
        namespace = SimpleNamespace(
            symbols="EURUSD,GBPUSD",
            min_rrr=1.5,
            top=3,
            brief=True,
            watch=False,
            interval=2.0,
            debug=False,
            exclude="eurusd",
            mt5_path=' "C:\\Custom\\terminal64.exe" ',
        )
        monkeypatch.setattr(sa, "parse_args", lambda: namespace)
        monkeypatch.setattr(sa, "_mt5_ensure_init", lambda: False)

        captured_calls = []

        def fake_process_once(*args, **kwargs):
            captured_calls.append((args, kwargs))

        monkeypatch.setattr(sa, "process_once", fake_process_once)
        monkeypatch.setattr(
            sa, "watch_loop", lambda *a, **k: pytest.fail("watch_loop should not run")
        )

        sa.main()

        assert sa._MT5_PATH_OVERRIDE == "C:\\Custom\\terminal64.exe"
        assert len(captured_calls) == 1
        args, kwargs = captured_calls[0]
        assert not args  # invoked with keyword arguments
        assert kwargs["symbols"] == ["GBPUSD"]
        assert kwargs["min_rrr"] == 1.5
        assert kwargs["top"] == 3
        assert kwargs["brief"] is True
        assert kwargs["debug"] is False
        assert kwargs["exclude_set"] == {"EURUSD"}
    finally:
        sa._MT5_PATH_OVERRIDE = original_override
