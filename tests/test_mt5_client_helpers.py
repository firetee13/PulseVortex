from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from monitor.core import mt5_client


def test_coerce_price_handles_edge_cases():
    assert mt5_client._coerce_price("1.25") == pytest.approx(1.25)
    assert mt5_client._coerce_price("not-a-number") is None
    assert mt5_client._coerce_price(float("inf")) is None
    assert mt5_client._coerce_price(None) is None


def test_candidate_terminal_paths_deduplicates(monkeypatch, tmp_path):
    fake_paths = [
        str(tmp_path / "MetaTrader 5" / "terminal64.exe"),
        str(tmp_path / "Other" / "terminal64.exe"),
    ]

    monkeypatch.setenv("PROGRAMFILES", str(tmp_path))
    monkeypatch.setenv("PROGRAMFILES(X86)", str(tmp_path))
    monkeypatch.setattr(
        mt5_client.glob, "glob", lambda pattern: [fake_paths[0], fake_paths[0], fake_paths[1]]
    )
    monkeypatch.setattr(
        mt5_client.os.path, "isfile", lambda path: path in {fake_paths[0], fake_paths[1]}
    )

    result = mt5_client._candidate_terminal_paths("C:\\Custom\\terminal64.exe")

    assert result[0] is None  # auto probe first
    assert "C:\\Custom\\terminal64.exe" in result
    # ensure discovered paths are deduplicated but preserved
    assert [fake_paths[0], fake_paths[1]] == [p for p in result if p in fake_paths]


def test_init_mt5_uses_candidates_and_succeeds(monkeypatch):
    calls = []

    class FakeMT5:
        COPY_TICKS_ALL = object()

        def __init__(self):
            self.version_checked = False
            self.shutdown_calls = 0

        def initialize(self, *args, **kwargs):
            calls.append((args, kwargs))
            if args and args[0] == "good-path":
                return True
            return False

        def version(self):
            self.version_checked = True

        def last_error(self):
            return (42, "not ready")

        def shutdown(self):
            self.shutdown_calls += 1

    fake_mt5 = FakeMT5()
    monkeypatch.setattr(mt5_client, "mt5", fake_mt5)
    monkeypatch.setattr(
        mt5_client, "_candidate_terminal_paths", lambda hint: [None, "good-path"]
    )
    monkeypatch.setattr(mt5_client.time, "sleep", lambda _: None)

    mt5_client.init_mt5(portable=True, timeout=5, retries=1, verbose=True)

    assert calls[0][0] == ()
    assert calls[1][0] == ("good-path",)
    assert fake_mt5.version_checked is True


def test_init_mt5_failure_raises_runtimeerror(monkeypatch):
    class FailingMT5:
        def initialize(self, *args, **kwargs):
            return False

        def last_error(self):
            return (-1, "boom")

        def shutdown(self):
            pass

    failing = FailingMT5()
    monkeypatch.setattr(mt5_client, "mt5", failing)
    monkeypatch.setattr(mt5_client.time, "sleep", lambda _: None)
    monkeypatch.setattr(mt5_client, "_candidate_terminal_paths", lambda hint: [None])

    with pytest.raises(RuntimeError) as excinfo:
        mt5_client.init_mt5(timeout=1, retries=1)

    assert "boom" in str(excinfo.value)


def test_resolve_symbol_prefers_visible_variant(monkeypatch):
    class SymbolInfo:
        def __init__(self, name, visible):
            self.name = name
            self.visible = visible

    class FakeMT5:
        def __init__(self):
            self.select_calls = []

        def symbol_select(self, symbol, enable):
            self.select_calls.append(symbol)
            return symbol == "EURUSD.m"

        def symbols_get(self, pattern):
            return [
                SymbolInfo("EURUSD_forex", False),
                SymbolInfo("EURUSD.m", True),
            ]

    fake_mt5 = FakeMT5()
    monkeypatch.setattr(mt5_client, "mt5", fake_mt5)

    result = mt5_client.resolve_symbol("EURUSD")

    assert result == "EURUSD.m"
    assert fake_mt5.select_calls[0] == "EURUSD"
    assert "EURUSD.m" in fake_mt5.select_calls


def test_timeframe_helpers(monkeypatch):
    monkeypatch.setattr(mt5_client, "_TIMEFRAME_SECOND_MAP", {123: 900})
    assert mt5_client.timeframe_seconds(123) == 900
    assert mt5_client.timeframe_seconds("bad") == 60

    stub_mt5 = SimpleNamespace(TIMEFRAME_M5=5)
    monkeypatch.setattr(mt5_client, "mt5", stub_mt5)
    assert mt5_client.timeframe_from_code("M5") == 5
    assert mt5_client.timeframe_from_code("") is None


def test_get_server_offset_hours(monkeypatch):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2024, 1, 1, 12, 0, tzinfo=tz)

    three_hours_ahead = datetime(2024, 1, 1, 15, 0, tzinfo=mt5_client.UTC)

    class Tick:
        def __init__(self, dt):
            self.time_msc = int(dt.timestamp() * 1000)

    fake_mt5 = SimpleNamespace(
        symbol_info_tick=lambda symbol: Tick(three_hours_ahead)
    )

    monkeypatch.setattr(mt5_client, "mt5", fake_mt5)
    monkeypatch.setattr(mt5_client, "datetime", FixedDateTime)

    assert mt5_client.get_server_offset_hours("EURUSD") == 3


def test_ticks_paged_collects_multiple_pages(monkeypatch):
    calls = []

    class FakeMT5:
        COPY_TICKS_ALL = object()

        def copy_ticks_from(self, symbol, cur, page, mode):
            calls.append(cur)
            if len(calls) == 1:
                ts = int((cur.timestamp() + 1) * 1000)
                return [{"time_msc": ts}]
            return []

    fake_mt5 = FakeMT5()
    monkeypatch.setattr(mt5_client, "mt5", fake_mt5)

    start = datetime(2024, 1, 1, 0, 0, 0)
    end = datetime(2024, 1, 1, 0, 5, 0)

    ticks, stats = mt5_client.ticks_paged(
        "EURUSD", start, end, page=10, server_offset_hours=0
    )

    assert len(ticks) == 1
    assert stats.pages == 1
    assert stats.total_ticks == 1
