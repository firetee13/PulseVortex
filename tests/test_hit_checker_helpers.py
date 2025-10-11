from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from monitor.cli import hit_checker as hc
from monitor.core.domain import Hit, TickFetchStats

UTC = timezone.utc


def test_parse_ids_parses_integers():
    assert hc._parse_ids("1, 2,3") == [1, 2, 3]
    assert hc._parse_ids(None) is None


def test_parse_ids_rejects_invalid(capsys, monkeypatch):
    monkeypatch.setattr(hc.sys, "exit", lambda code: (_ for _ in ()).throw(SystemExit(code)))

    with pytest.raises(SystemExit) as excinfo:
        hc._parse_ids("1,abc")

    assert excinfo.value.code == 2
    assert "Invalid --ids value" in capsys.readouterr().out


def test_parse_symbols_handles_none():
    assert hc._parse_symbols("EURUSD, BTCUSD ") == ["EURUSD", "BTCUSD"]
    assert hc._parse_symbols(None) is None


def test_resolve_timeframe_falls_back(monkeypatch):
    monkeypatch.setattr(hc, "timeframe_from_code", lambda code: 42 if code == "M42" else None)
    monkeypatch.setattr(hc, "timeframe_m1", lambda: 1)
    assert hc._resolve_timeframe("M42") == 42
    assert hc._resolve_timeframe(None) == 1


def test_rate_helpers_extract_values():
    rate_obj = SimpleNamespace(low="1.1000", high=1.2000, time=1_700_000_000)
    assert hc._rate_field(rate_obj, "low") == pytest.approx(1.1)
    assert hc._rate_time(rate_obj, offset_hours=2) == datetime.fromtimestamp(
        rate_obj.time, tz=UTC
    ) - timedelta(hours=2)

    rate_dict = {"low": "1.05", "time": 1_700_000_000}
    assert hc._rate_field(rate_dict, "low") == pytest.approx(1.05)
    assert hc._rate_field(rate_dict, "missing") is None


def test_rates_to_bars_filters_bad_records():
    valid_rate = {"time": 1_700_000_000, "low": 1.0, "high": 2.0}
    missing = {"time": None, "low": 1.0, "high": 2.0}
    bars = hc._rates_to_bars([valid_rate, missing], timeframe_seconds=60, offset_hours=0)
    assert len(bars) == 1
    assert isinstance(bars[0], hc.RateBar)
    assert bars[0].end_utc - bars[0].start_utc == timedelta(seconds=60)


def test_compute_spread_guard(monkeypatch):
    fake_info = SimpleNamespace(point=0.01, spread=4)
    monkeypatch.setattr(hc, "get_symbol_info", lambda symbol: fake_info)
    assert hc._compute_spread_guard("EURUSD") == pytest.approx(0.06)

    monkeypatch.setattr(hc, "get_symbol_info", lambda symbol: None)
    assert hc._compute_spread_guard("EURUSD") == 0.0


def test_bar_crosses_price_branches():
    setup_buy = SimpleNamespace(direction="buy", sl=1.05, tp=1.15)
    bar = hc.RateBar(
        start_utc=datetime(2024, 1, 1, tzinfo=UTC),
        end_utc=datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
        low=1.04,
        high=1.16,
    )
    assert hc._bar_crosses_price(bar, setup_buy, spread_guard=0.0)

    setup_sell = SimpleNamespace(direction="sell", sl=1.05, tp=1.15)
    assert hc._bar_crosses_price(bar, setup_sell, spread_guard=0.01)


def test_merge_windows_combines_adjacent():
    base = datetime(2024, 1, 1, tzinfo=UTC)
    win1 = hc.CandidateWindow(1, base, base + timedelta(seconds=5), base, base)
    win2 = hc.CandidateWindow(
        1, base + timedelta(seconds=6), base + timedelta(seconds=10), base, base
    )
    merged = hc._merge_windows([win1, win2])
    assert len(merged) == 1
    assert merged[0].start_utc == base
    assert merged[0].end_utc == base + timedelta(seconds=10)


def test_evaluate_setup_records_hit(monkeypatch):
    now = datetime(2024, 1, 1, 0, 5, tzinfo=UTC)
    as_of = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    setup = SimpleNamespace(id=1, direction="buy", sl=1.0, tp=2.0, as_of_utc=as_of)
    bar = hc.RateBar(
        start_utc=as_of + timedelta(minutes=1),
        end_utc=as_of + timedelta(minutes=2),
        low=0.9,
        high=2.1,
    )

    hit_time = as_of + timedelta(minutes=1, seconds=30)
    fake_stats = TickFetchStats(pages=1, total_ticks=10, elapsed_s=0.1, fetch_s=0.05, early_stop=True)

    def fake_scan(**kwargs):
        return Hit(kind="TP", time_utc=hit_time, price=2.0), fake_stats, 1

    monkeypatch.setattr(hc, "classify_symbol", lambda symbol: "forex")
    monkeypatch.setattr(
        hc,
        "iter_active_utc_ranges",
        lambda start, end, asset_kind, symbol: [(start, end)],
    )
    monkeypatch.setattr(hc, "scan_for_hit_with_chunks", fake_scan)

    result = hc._evaluate_setup(
        setup=setup,
        last_checked_utc=as_of,
        bars=[bar],
        resolved_symbol="EURUSD",
        offset_hours=0,
        spread_guard=0.0,
        now_utc=now,
        chunk_minutes=5,
        tick_padding_seconds=0.0,
        trace_ticks=False,
    )

    assert result.hit is not None
    assert result.hit.time_utc == hit_time
    assert result.ticks == fake_stats.total_ticks
    assert result.last_checked_utc == hit_time
    assert not result.ignored_hit


def test_evaluate_setup_ignored_hit_when_before_as_of(monkeypatch):
    now = datetime(2024, 1, 1, 0, 5, tzinfo=UTC)
    as_of = datetime(2024, 1, 1, 0, 2, tzinfo=UTC)
    setup = SimpleNamespace(id=2, direction="sell", sl=1.1, tp=0.9, as_of_utc=as_of)
    bar = hc.RateBar(
        start_utc=as_of,
        end_utc=as_of + timedelta(minutes=1),
        low=0.85,
        high=1.2,
    )

    fake_stats = TickFetchStats(pages=1, total_ticks=5, elapsed_s=0.05, fetch_s=0.02, early_stop=True)
    hit_time = as_of

    def fake_scan(**kwargs):
        return Hit(kind="SL", time_utc=hit_time, price=1.1), fake_stats, 1

    monkeypatch.setattr(hc, "classify_symbol", lambda symbol: "forex")
    monkeypatch.setattr(
        hc,
        "iter_active_utc_ranges",
        lambda start, end, asset_kind, symbol: [(start, end)],
    )
    monkeypatch.setattr(hc, "scan_for_hit_with_chunks", fake_scan)

    result = hc._evaluate_setup(
        setup=setup,
        last_checked_utc=as_of - timedelta(minutes=1),
        bars=[bar],
        resolved_symbol="EURUSD",
        offset_hours=0,
        spread_guard=0.0,
        now_utc=now,
        chunk_minutes=None,
        tick_padding_seconds=0.0,
        trace_ticks=False,
    )

    assert result.hit is None
    assert result.ignored_hit is True
    assert result.last_checked_utc >= as_of


def test_scan_for_hit_with_chunks_aggregates(monkeypatch):
    start_utc = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    end_utc = start_utc + timedelta(minutes=30)
    stats = TickFetchStats(pages=1, total_ticks=20, elapsed_s=0.05, fetch_s=0.02, early_stop=False)

    call_counter = {"count": 0}

    def fake_ticks_range_all(symbol, start, end, trace):
        call_counter["count"] += 1
        return (["tick"], stats)

    def fake_hit_from_ticks(ticks, direction, sl, tp, offset_hours):
        if call_counter["count"] == 2:
            return Hit(kind="TP", time_utc=start_utc + timedelta(minutes=20), price=tp)
        return None

    monkeypatch.setattr(hc, "ticks_range_all", fake_ticks_range_all)
    monkeypatch.setattr(hc, "earliest_hit_from_ticks", fake_hit_from_ticks)
    monkeypatch.setattr(hc, "to_server_naive", lambda dt, offset: dt)

    hit, stats_out, chunks = hc.scan_for_hit_with_chunks(
        symbol="EURUSD",
        direction="buy",
        sl=1.0,
        tp=2.0,
        offset_hours=0,
        start_utc=start_utc,
        end_utc=end_utc,
        chunk_minutes=10,
        trace=False,
    )

    assert call_counter["count"] == 2
    assert chunks == 2
    assert hit is not None
    assert stats_out.total_ticks == stats.total_ticks * 2
    assert stats_out.pages == stats.pages * 2


def test_scan_for_hit_with_chunks_empty_range():
    hit, stats_out, chunks = hc.scan_for_hit_with_chunks(
        symbol="EURUSD",
        direction="buy",
        sl=1.0,
        tp=2.0,
        offset_hours=0,
        start_utc=datetime(2024, 1, 1, tzinfo=UTC),
        end_utc=datetime(2024, 1, 1, tzinfo=UTC),
        chunk_minutes=None,
        trace=False,
    )

    assert hit is None
    assert stats_out.total_ticks == 0
    assert chunks == 0
