from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from monitor.cli import setup_analyzer as sa


def test_infer_decimals_from_price_handles_various_inputs():
    assert sa._infer_decimals_from_price(1.2345) == 4
    assert sa._infer_decimals_from_price(123.0) == 0
    assert sa._infer_decimals_from_price(float("nan")) == 5
    assert sa._infer_decimals_from_price(None) == 5


def test_symbol_digits_uses_mt5_when_available(monkeypatch):
    info = SimpleNamespace(digits=3)
    fake_mt5 = SimpleNamespace(symbol_info=lambda symbol: info)

    monkeypatch.setattr(sa, "_MT5_IMPORTED", True)
    monkeypatch.setattr(sa, "_MT5_READY", True)
    monkeypatch.setattr(sa, "mt5", fake_mt5)
    monkeypatch.setattr(sa, "_mt5_ensure_init", lambda: True)

    assert sa._symbol_digits("EURUSD", 1.23456) == 3


def test_symbol_digits_falls_back_when_mt5_missing(monkeypatch):
    monkeypatch.setattr(sa, "_MT5_IMPORTED", False)
    assert sa._symbol_digits("EURUSD", 1.23456) == 5


def test_proximity_bin_label_buckets_values():
    assert sa._proximity_bin_label(0.42) == "0.4-0.5"
    assert sa._proximity_bin_label(-0.1) == "0.0-0.1"
    assert sa._proximity_bin_label(None) is None


def test_canonicalize_key_normalizes_and_caches(monkeypatch):
    monkeypatch.setattr(sa, "CANONICAL_KEYS", {})
    first = sa.canonicalize_key(" ATR (%) D1 ")
    second = sa.canonicalize_key(" ATR (%) D1 ")
    assert first == "atr percent d1"
    assert second == first  # cached path
    assert " ATR (%) D1 " in sa.CANONICAL_KEYS


def test_normalize_spread_pct_variants():
    assert sa.normalize_spread_pct(0.0012) == pytest.approx(0.12)
    assert sa.normalize_spread_pct(0.12) == pytest.approx(0.12)
    assert sa.normalize_spread_pct(None) is None
    assert sa.normalize_spread_pct("bad") is None


def test_spread_class_thresholds():
    assert sa.spread_class(0.05) == "Excellent"
    assert sa.spread_class(0.15) == "Good"
    assert sa.spread_class(0.25) == "Acceptable"
    assert sa.spread_class(0.35) == "Avoid"
    assert sa.spread_class(None) == "Unknown"


def test_fnum_parses_localized_numbers():
    assert sa.fnum("1,234.5 pips") == pytest.approx(1234.5)
    assert sa.fnum("1.234,5") == pytest.approx(1234.5)
    assert sa.fnum("(12,5)") == pytest.approx(-12.5)
    assert sa.fnum("N/A") is None


def test_timezone_helpers():
    aware = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    naive = datetime(2024, 1, 1, 12, 0)

    converted = sa.to_input_tz(aware)
    assert converted.tzinfo == sa.INPUT_TZ

    naive_converted = sa.to_input_tz(naive)
    assert naive_converted.tzinfo == sa.INPUT_TZ
    assert naive_converted.hour == naive.hour

    back_to_utc = sa.utc_naive(converted)
    assert back_to_utc.tzinfo is None
    assert back_to_utc.hour == aware.hour
