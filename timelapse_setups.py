#!/usr/bin/env python3
"""
Analyze MT5 symbols for trade setups based on symbol strength, price momentum, ATR/ATR%, and S1/R1 proximity.

Usage:
  python timelapse_setups.py [--min-rrr 1.0] [--top N] [--brief]
                              [--watch] [--interval 5]

Notes:
  - Strength: -50..+50; ATR in pips on D1. For crypto, uses D1 Close delta for momentum.
  - Spread filter: Only spreads <0.3% accepted, others filtered out.
  - SL/TP logic: Buy -> SL=S1 or D1 Low, TP=R1 or D1 High; Sell -> SL=R1 or D1 High, TP=S1 or D1 Low. Price must lie between SL and TP.
  - SL distance filter: Stop loss must be at least 10x the current spread away from entry price (Buy: bid-SL >= 10x spread, Sell: SL-ask >= 10x spread).
  - TP distance filter: Take profit must also be at least 10x the current spread away from entry (Buy: TP-bid >= 10x spread, Sell: ask-TP >= 10x spread).
  - Current price and RRR use Bid/Ask at signal timestamp (Buy: Ask, Sell: Bid).
  - ATR(%) effect: adds +0.5 score bonus when within [60, 150] (for informational purposes).
  - Timelapse: simulated from previous values in MT5 data for momentum context.
  - Crypto adaptation: No Delta FXP or volume; uses Strength consensus + D1 Close trend.
"""

from __future__ import annotations

import atexit
import argparse
import math
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple, Set
import types
# DB backend: SQLite only
try:
    import sqlite3  # type: ignore
except Exception:
    sqlite3 = None  # type: ignore


from monitor.config import default_db_path
from monitor.db import load_live_bin_filters_sqlite, persist_order_sent_sqlite
from monitor import mt5_client
from monitor.quiet_hours import is_quiet_time, UTC_PLUS_3
from monitor.symbols import classify_symbol

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
import json
import numpy as np  # required for ATR computations

# Optional MT5
mt5 = getattr(mt5_client, "mt5", None)  # type: ignore
_MT5_IMPORTED = mt5_client.has_mt5()
atexit.register(mt5_client.shutdown_mt5)




# -----------------------------------------
# Precision helpers for price/SL/TP rounding
# -----------------------------------------
def _infer_decimals_from_price(price: Optional[float]) -> int:
    """Infer decimal places from a price value by inspecting its string form.

    Falls back to 5 when not inferable.
    """
    try:
        if price is None:
            return 5
        value = float(price)
        if not math.isfinite(value):
            return 5
        s = f"{value:.10f}".rstrip("0").rstrip(".")
        if "." in s:
            return max(0, min(10, len(s.split(".")[1])))
        return 0
    except Exception:
        return 5


def _symbol_digits(symbol: str, price: Optional[float]) -> int:
    """Resolve desired decimal places for a symbol.

    - Prefer MT5 symbol_info().digits when available
    - Fallback: infer from the actual price value
    - Default: 5
    """
    try:
        if _MT5_IMPORTED and _mt5_ensure_init():
            try:
                info = mt5.symbol_info(symbol)  # type: ignore[union-attr, reportUnknownMember]
                if info is not None:
                    d = int(getattr(info, "digits", 0) or 0)
                    if 0 <= d <= 10:
                        return d
            except Exception:
                pass
        d = _infer_decimals_from_price(price)
        return d if 0 <= d <= 10 else 5
    except Exception:
        return 5


def _symbol_point(symbol: str) -> Optional[float]:
    """Fetch the MT5 point size for a symbol with caching."""
    key = symbol.upper()
    if key in _SYMBOL_POINT_CACHE:
        return _SYMBOL_POINT_CACHE[key]
    if not _MT5_IMPORTED or not _mt5_ensure_init():
        return None
    try:
        info = mt5.symbol_info(symbol)  # type: ignore[union-attr, reportUnknownMember]
    except Exception:
        info = None
    point = None
    if info is not None:
        try:
            val = getattr(info, "point", None)
            if val is not None:
                point = float(val)
        except Exception:
            point = None
    if point is not None:
        _SYMBOL_POINT_CACHE[key] = point
    return point


def _account_trade_mode() -> Optional[int]:
    """Return the MT5 account trade_mode (0 demo, 2 real) with caching."""
    global _ACCOUNT_TRADE_MODE
    if _ACCOUNT_TRADE_MODE is not None:
        return _ACCOUNT_TRADE_MODE
    if not _MT5_IMPORTED or not _mt5_ensure_init():
        return None
    try:
        info = mt5.account_info()  # type: ignore[union-attr, reportUnknownMember]
    except Exception:
        info = None
    if info is None:
        _ACCOUNT_TRADE_MODE = None
        return _ACCOUNT_TRADE_MODE
    try:
        trade_mode = getattr(info, "trade_mode", None)
        _ACCOUNT_TRADE_MODE = int(trade_mode) if trade_mode is not None else None
    except Exception:
        _ACCOUNT_TRADE_MODE = None
    return _ACCOUNT_TRADE_MODE


def _is_demo_account() -> bool:
    """Return True if the MT5 account is a demo account."""
    mode = _account_trade_mode()
    if mode is None:
        return False
    # MetaTrader: 0 demo, 1 contest, 2 real
    return mode == 0


def _augment_spread_for_demo(symbol: str, spread: float) -> float:
    """Pad demo-account forex spreads to account for unrealistic zero spreads."""
    try:
        base = float(spread)
    except Exception:
        return spread
    if DEMO_FOREX_SPREAD_POINTS <= 0:
        return base
    if not _is_demo_account():
        return base
    try:
        category = (classify_symbol(symbol) or "").lower()
    except Exception:
        category = ""
    if category != "forex":
        return base
    point = _symbol_point(symbol)
    if point is None or point <= 0:
        return base
    return base + (point * DEMO_FOREX_SPREAD_POINTS)


HEADER_SYMBOL = "symbol"

# Cache for canonicalized keys to speed up repeated lookups
CANONICAL_KEYS: Dict[str, str] = {}
_SYMBOL_POINT_CACHE: Dict[str, float] = {}
_ACCOUNT_TRADE_MODE: Optional[int] = None
_SYMBOL_FILLING_CACHE: Dict[str, Optional[int]] = {}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze MT5 symbols for trade setups (MT5 is the only source)")
    p.add_argument("--symbols", default="", help="Comma-separated symbols (default: all visible in MarketWatch)")
    p.add_argument("--min-rrr", type=float, default=1.0, help="Minimum risk-reward ratio for sorting (default: 1.0) - NOTE: No longer used for filtering")
    p.add_argument("--top", type=int, default=None, help="Limit to top N setups (after filtering)")
    p.add_argument("--brief", action="store_true", help="Brief output without detailed explanation")
    p.add_argument("--watch", action="store_true", help="Run continuously and poll MT5 for updates")
    p.add_argument("--interval", type=float, default=1.0, help="Polling interval in seconds when --watch is used (default: 1)")
    p.add_argument("--debug", action="store_true", help="Print filtering diagnostics and counts")
    p.add_argument("--exclude", default="", help="Comma-separated symbols to exclude (e.g., GLMUSD,BCHUSD)")
    return p.parse_args()


PROXIMITY_BIN_BUCKET = 0.1
# Multiplier for minimum SL/TP distance in spread units (default 10x; override via TIMELAPSE_SPREAD_MULT)
SPREAD_MULTIPLIER = float(os.environ.get("TIMELAPSE_SPREAD_MULT", "10"))
DEMO_FOREX_SPREAD_POINTS = float(os.environ.get("TIMELAPSE_DEMO_FOREX_EXTRA_POINTS", "10"))
ORDER_VOLUME = float(os.environ.get("TIMELAPSE_ORDER_VOLUME", "0.1"))
ORDER_DEVIATION = int(os.environ.get("TIMELAPSE_ORDER_DEVIATION", "10"))
ORDER_RETRY_DELAY = float(os.environ.get("TIMELAPSE_ORDER_RETRY_SEC", "0.2"))
ORDER_MAX_ATTEMPTS = int(os.environ.get("TIMELAPSE_ORDER_MAX_RETRIES", "0"))
ORDER_COMMENT = os.environ.get("TIMELAPSE_ORDER_COMMENT", "timelapse:auto")


def _proximity_bin_label(proximity: Optional[float], bucket: float = PROXIMITY_BIN_BUCKET) -> Optional[str]:
    """Return the proximity bucket label (e.g., '0.4-0.5') used for gating/stats."""
    try:
        if proximity is None:
            return None
        value = float(proximity)
        if not math.isfinite(value):
            return None
        if value < 0:
            value = 0.0
        start = math.floor(value / bucket) * bucket
        end = start + bucket
        return f"{start:.1f}-{end:.1f}"
    except Exception:
        return None


def _format_as_of_for_db(value: object) -> str:
    """Format `as_of` entries consistently for DB lookups."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    return str(value)


# Fixed offset timezone for input interpretation (Europe/Berlin UTC+2)
INPUT_TZ = timezone(timedelta(hours=2))

UTC = timezone.utc
UTC3 = UTC_PLUS_3
# Consider market "alive" only if there's at least one tick
# within this many seconds. Avoid creating entries for closed markets.
TICK_FRESHNESS_SEC = 60  # 60 seconds
# Cache last tick data to minimize expensive history lookups
_LAST_TICK_CACHE: Dict[str, Tuple[Optional[float], Optional[float], Optional[datetime]]] = {}

# Cache MT5 rate data with lightweight TTLs per timeframe to reduce IPC churn
_RATE_CACHE: Dict[Tuple[str, int, int], Tuple[float, Any]] = {}
# Reusable SQLite connection handle (populated lazily)
_DB_CONN: Optional["sqlite3.Connection"] = None
_DB_CONN_PATH: Optional[str] = None

CONSENSUS_TABLE = "consensus"
# Score/RRR/proximity thresholds for the three timeframes. A tighter proximity
# threshold means the stop loss sits further away relative to price, which we
# treat as higher conviction.
_CONSENSUS_THRESHOLDS = {
    "1h": {"score": 3.0, "rrr": 1.2, "proximity": 0.6},
    "4h": {"score": 4.5, "rrr": 1.4, "proximity": 0.45},
    "1d": {"score": 6.0, "rrr": 1.6, "proximity": 0.35},
}

if sqlite3 is not None:
    class _ManagedConnection(sqlite3.Connection):  # type: ignore[misc]
        _closed: bool = False

        def close(self) -> None:  # type: ignore[override]
            if getattr(self, "_closed", False):
                return
            try:
                super().close()
            finally:
                self._closed = True

        def __del__(self) -> None:  # pragma: no cover - defensive close
            try:
                self.close()
            except Exception:
                pass


def _connect_sqlite(db_path: str, *, timeout: float = 5.0) -> "sqlite3.Connection":
    if sqlite3 is None:
        raise RuntimeError("sqlite3 not available")
    kwargs: Dict[str, Any] = {
        "timeout": timeout,
        "check_same_thread": False,
    }
    if "_ManagedConnection" in globals():
        kwargs["factory"] = _ManagedConnection  # type: ignore[assignment]
    return sqlite3.connect(db_path, **kwargs)


def _get_db_connection() -> Optional["sqlite3.Connection"]:
    global _DB_CONN, _DB_CONN_PATH
    if sqlite3 is None:
        return None
    db_path = str(default_db_path())
    if _DB_CONN is not None and _DB_CONN_PATH and _DB_CONN_PATH != db_path:
        _close_db_connection()
    if _DB_CONN is None:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        _DB_CONN = _connect_sqlite(db_path, timeout=5.0)
        _DB_CONN_PATH = db_path
    return _DB_CONN


def _ensure_consensus_schema(cur: "sqlite3.Cursor", source_table: str) -> None:
    """Create the consensus table if missing."""
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CONSENSUS_TABLE} (
            id INTEGER PRIMARY KEY,
            is_1h_consensus INTEGER NOT NULL,
            is_4h_consensus INTEGER NOT NULL,
            is_1d_consensus INTEGER NOT NULL,
            FOREIGN KEY(id) REFERENCES {source_table}(id) ON DELETE CASCADE
        )
        """
    )


def _calculate_consensus_flags(score: object, rrr: object, proximity_to_sl: object) -> Tuple[bool, bool, bool]:
    """Translate raw setup metrics into timeframe consensus booleans.

    The algorithm assumes higher timeframe agreement requires progressively
    stricter filters. We treat the existing score as a proxy for directional
    conviction, RRR as reward quality, and proximity_to_sl as risk tightness.
    """
    def _to_float(value: object, default: float = float("nan")) -> float:
        try:
            fval = float(value)
            return fval if math.isfinite(fval) else default
        except Exception:
            return default

    score_val = _to_float(score, 0.0)
    rrr_val = _to_float(rrr, 0.0)
    prox_val = _to_float(proximity_to_sl, float("nan"))

    def _meets(thr: Dict[str, float]) -> bool:
        proximity_limit = thr.get("proximity")
        proximity_ok = not math.isfinite(prox_val) or prox_val <= proximity_limit
        return (
            score_val >= thr.get("score", 0.0)
            and rrr_val >= thr.get("rrr", 0.0)
            and proximity_ok
        )

    return (
        _meets(_CONSENSUS_THRESHOLDS["1h"]),
        _meets(_CONSENSUS_THRESHOLDS["4h"]),
        _meets(_CONSENSUS_THRESHOLDS["1d"]),
    )


def _rebuild_consensus_table(conn: "sqlite3.Connection", *, source_table: str = "timelapse_setups") -> None:
    """Recreate consensus rows for every setup, updating in place."""
    try:
        cur = conn.cursor()
        _ensure_consensus_schema(cur, source_table)
        cur.execute(f"PRAGMA table_info({source_table})")
        columns = [str(row[1]) for row in (cur.fetchall() or [])]
        if "id" not in columns or "score" not in columns or "rrr" not in columns:
            return
        proximity_selector = "proximity_to_sl" if "proximity_to_sl" in columns else "NULL AS proximity_to_sl"
        cur.execute(
            f"SELECT id, score, rrr, {proximity_selector} FROM {source_table}"
        )
        rows = cur.fetchall() or []
        if not rows:
            cur.execute(f"DELETE FROM {CONSENSUS_TABLE}")
            return

        payload: List[Tuple[int, int, int, int]] = []
        for row in rows:
            setup_id = row[0]
            if setup_id is None:
                continue
            flags = _calculate_consensus_flags(row[1], row[2], row[3])
            payload.append((
                int(setup_id),
                int(flags[0]),
                int(flags[1]),
                int(flags[2]),
            ))

        if not payload:
            return

        cur.executemany(
            f"""
            INSERT INTO {CONSENSUS_TABLE} (id, is_1h_consensus, is_4h_consensus, is_1d_consensus)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                is_1h_consensus=excluded.is_1h_consensus,
                is_4h_consensus=excluded.is_4h_consensus,
                is_1d_consensus=excluded.is_1d_consensus
            """,
            payload,
        )
        cur.execute(
            f"DELETE FROM {CONSENSUS_TABLE} WHERE id NOT IN (SELECT id FROM {source_table})"
        )
    except sqlite3.Error as exc:
        print(f"[DB] Warning: consensus refresh failed: {exc}")


def _close_db_connection() -> None:
    global _DB_CONN, _DB_CONN_PATH
    if _DB_CONN is not None:
        try:
            _DB_CONN.close()
        except Exception:
            pass
    _DB_CONN = None
    _DB_CONN_PATH = None


if sqlite3 is not None:
    atexit.register(_close_db_connection)

_RATE_TTL_SECONDS = {
    # Higher timeframes change slowly; reuse for longer
    getattr(mt5, "TIMEFRAME_W1", 0): 120.0,
    getattr(mt5, "TIMEFRAME_D1", 0): 45.0,
    getattr(mt5, "TIMEFRAME_H4", 0): 45.0,
    getattr(mt5, "TIMEFRAME_H1", 0): 12.0,
    getattr(mt5, "TIMEFRAME_M15", 0): 6.0,
}



def to_input_tz(dt: datetime) -> datetime:
    """Convert a datetime to input timezone (UTC+2). If naive, assume it is input_tz local time.

    - If `dt` has no tzinfo, attach input_tz tzinfo without shifting the clock.
    - If `dt` is timezone-aware, convert to input_tz.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=INPUT_TZ)
    return dt.astimezone(INPUT_TZ)


def utc_naive(dt: datetime) -> datetime:
    """
    Return a naive datetime representing the same instant in UTC.

    Useful for databases expecting TIMESTAMP WITHOUT TIME ZONE while treating
    values semantically as UTC.
    """
    return dt.astimezone(UTC).replace(tzinfo=None)


_MT5_READY = False
def _mt5_ensure_init() -> bool:
    global _MT5_READY
    if not _MT5_IMPORTED:
        return False
    if _MT5_READY:
        return True
    timeout = int(os.environ.get("TIMELAPSE_MT5_TIMEOUT", os.environ.get("MT5_TIMEOUT", "30")))
    retries = int(os.environ.get("TIMELAPSE_MT5_RETRIES", "1"))
    portable = str(os.environ.get("MT5_PORTABLE", "0")).strip().lower() in {"1", "true", "yes", "on"}
    try:
        mt5_client.init_mt5(timeout=timeout, retries=retries, portable=portable)
        global mt5
        mt5 = mt5_client.mt5
        _MT5_READY = True
    except RuntimeError:
        _MT5_READY = False
    return _MT5_READY




def _mt5_copy_rates_cached(symbol: str, timeframe: int, count: int) -> Any:
    """Fetch MT5 rates with a short TTL-based cache to limit IPC overhead."""
    key = (symbol, timeframe, count)
    ttl = float(_RATE_TTL_SECONDS.get(timeframe, 5.0))
    now = time.time()
    cached = _RATE_CACHE.get(key)
    if cached is not None:
        age = now - cached[0]
        if 0.0 <= age <= ttl:
            return cached[1]
    try:
        rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, count)  # type: ignore[union-attr, reportUnknownMember]
    except Exception:
        rates = None
    if rates is not None:
        _RATE_CACHE[key] = (now, rates)
    else:
        _RATE_CACHE.pop(key, None)
    return rates

def canonicalize_key(s: Optional[str]) -> str:
    """Canonicalize CSV header / lookup keys for case-insensitive, punctuation-robust matching.

    Examples:
        "ATR (%) D1" -> "atr percent d1"
        "Strength 4H" -> "strength 4h"
    """
    if s is None:
        return ""
    if s in CANONICAL_KEYS:
        return CANONICAL_KEYS[s]
    # strip BOM and surrounding whitespace, lowercase
    orig_s = s
    s = s.lstrip("\ufeff").strip().lower()
    # normalize percent sign to word "percent" so variants like '%' and 'percent' match
    s = s.replace("%", " percent ")
    # replace any non-alphanumeric characters with space
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    # collapse multiple spaces
    s = re.sub(r"\s+", " ", s).strip()
    CANONICAL_KEYS[orig_s] = s
    return s

def fnum(v: Optional[str]) -> Optional[float]:
    """Parse a numeric value from a CSV cell robustly.

    Handles:
      - trailing/leading units like '%', 'pips', 'pip'
      - comma as decimal separator (common in some exports)
      - thousands separators (commas or dots depending on locale)
      - returns the first numeric token found as float, or None if not parseable
    """
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.upper() == "N/A":
        return None
    # strip BOM / NBSP characters and trim
    s = s.lstrip("\ufeff").replace("\u00A0", " ").strip()

    # Handle negative numbers formatted as (123.45)
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1].strip()

    # Normalize spaces used as thousands separators (e.g., "1 234.56")
    if " " in s and re.search(r"\d\s\d{3}", s):
        s = s.replace(" ", "")

    # Handle comma/dot thousands/decimal ambiguity:
    if "." in s and "," in s:
        # Determine which is decimal separator by which appears last
        if s.rfind(".") > s.rfind(","):
            # dot is decimal separator, commas are thousands separators
            s = s.replace(",", "")
        else:
            # comma is decimal separator, dots are thousands separators
            s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        # Single comma present: treat as decimal separator
        s = s.replace(",", ".")

    # Extract first numeric token (covers ints, decimals, scientific notation)
    m = re.search(r"[-+]?(?:\d*\.\d+|\d+)(?:[eE][-+]?\d+)?", s)
    if not m:
        return None
    try:
        val = float(m.group(0))
        return -val if neg else val
    except Exception:
        return None


def normalize_spread_pct(pct: Optional[float]) -> Optional[float]:
    """Normalize Spread% to percent units.

    Support common input formats:
      - fraction-of-price (e.g. 0.0012 meaning 0.12%) -> convert to percent units (0.12)
      - small-percent decimal (e.g. 0.12 meaning 0.12%) -> leave as-is (0.12)
      - whole-percent (e.g. 1.2 meaning 1.2%) -> leave as-is (1.2)

    Return value is in percent units (e.g., 0.12 == 0.12%).
    """
    if pct is None:
        return None
    try:
        value = float(pct)
        if not math.isfinite(value):
            return None
        # If the value looks like a tiny fraction (< 0.01), treat it as fraction-of-price
        # and convert to percent units by multiplying by 100. Otherwise assume it's
        # already expressed in percent units.
        if abs(value) < 0.01:
            return value * 100.0
        return value
    except Exception:
        return None


def spread_class(pct: Optional[float]) -> str:
    if pct is None:
        return "Unknown"
    # pct is expressed in percent units (e.g. 0.12 -> 0.12%)
    # Provide four graded buckets to align with scoring categories.
    if pct < 0.10:
        return "Excellent"
    if pct < 0.20:
        return "Good"
    if pct < 0.30:
        return "Acceptable"
    return "Avoid"


@dataclass
class Snapshot:
    ts: datetime
    row: Dict[str, Any]

    def g(self, key: str) -> Optional[float]:
        # Lookup using canonicalized key (header names are canonicalized on read)
        val = self.row.get(canonicalize_key(key), "")
        if isinstance(val, (int, float)):
            return float(val)
        return fnum(val)


def _atr(values: List[Tuple[float, float, float]], period: int = 14) -> Optional[float]:
    """Average True Range using Wilder's smoothing.

    Requires at least period+1 bars to compute the initial ATR.
    """
    if len(values) < period + 1:
        return None
    try:
        vals = np.asarray(values, dtype=float)  # shape (n, 3): high, low, close
        highs = vals[1:, 0]
        lows = vals[1:, 1]
        prev_closes = vals[:-1, 2]
        tr = np.maximum.reduce([
            highs - lows,
            np.abs(highs - prev_closes),
            np.abs(prev_closes - lows),
        ])
        # Initial ATR: simple mean of first `period` TRs
        atr = float(np.mean(tr[:period]))
        # Wilder smoothing over remaining TRs (if any)
        for tr_val in tr[period:]:
            atr = (atr * (period - 1) + float(tr_val)) / period
        return atr
    except Exception:
        return None

def _pivots_from_prev_day(daily_rates) -> Tuple[Optional[float], Optional[float]]:
    try:
        if daily_rates is None or len(daily_rates) < 2:
            return None, None
        prev = daily_rates[-2]
        h = float(prev['high']) if isinstance(prev, dict) else float(prev[2])
        l = float(prev['low']) if isinstance(prev, dict) else float(prev[3])
        c = float(prev['close']) if isinstance(prev, dict) else float(prev[4])
        p = (h + l + c) / 3.0
        s1 = 2*p - h
        r1 = 2*p - l
        return s1, r1
    except Exception:
        return None, None


def read_series_mt5(symbols: List[str]) -> Tuple[Dict[str, List[Snapshot]], Optional[str], Optional[datetime]]:
    now_utc = datetime.now(UTC)
    latest_ts = now_utc.astimezone(INPUT_TZ)
    if not _mt5_ensure_init():
        print('[MT5] initialize() failed; cannot read symbols.')
        return {}, None, latest_ts
    series: Dict[str, List[Snapshot]] = {}
    for sym in symbols:
        try:
            mt5.symbol_select(sym, True)  # type: ignore[union-attr, reportUnknownMember]
        except Exception:
            pass
        tick = None
        try:
            tick = mt5.symbol_info_tick(sym)  # type: ignore[union-attr, reportUnknownMember]
        except Exception:
            tick = None
        bid: Optional[float] = None
        ask: Optional[float] = None
        tick_time_utc: Optional[datetime] = None
        tick_age: Optional[float] = None
        has_recent_tick = False

        if tick is not None:
            try:
                bid_candidate = float(getattr(tick, 'bid', 0.0) or 0.0)
            except Exception:
                bid_candidate = 0.0
            if bid_candidate != 0.0:
                bid = bid_candidate
            try:
                ask_candidate = float(getattr(tick, 'ask', 0.0) or 0.0)
            except Exception:
                ask_candidate = 0.0
            if ask_candidate != 0.0:
                ask = ask_candidate
            try:
                tmsc = getattr(tick, 'time_msc', None)
                if tmsc:
                    tick_time_utc = datetime.fromtimestamp(float(tmsc) / 1000.0, tz=UTC)
                else:
                    ts = getattr(tick, 'time', None)
                    if ts:
                        tick_time_utc = datetime.fromtimestamp(float(ts), tz=UTC)
            except Exception:
                tick_time_utc = None
            if tick_time_utc is not None:
                try:
                    tick_age = max(0.0, (now_utc - tick_time_utc).total_seconds())
                    if tick_age <= TICK_FRESHNESS_SEC:
                        has_recent_tick = True
                except Exception:
                    tick_age = None

        d1 = _mt5_copy_rates_cached(sym, mt5.TIMEFRAME_D1, 20)
        h4 = _mt5_copy_rates_cached(sym, mt5.TIMEFRAME_H4, 4)
        w1 = _mt5_copy_rates_cached(sym, mt5.TIMEFRAME_W1, 4)
        h1 = _mt5_copy_rates_cached(sym, mt5.TIMEFRAME_H1, 1)
        m15 = _mt5_copy_rates_cached(sym, mt5.TIMEFRAME_M15, 1)

        d1_close = float(d1[-1]['close']) if (d1 is not None and len(d1) >= 1) else None
        d1_high = float(d1[-1]['high']) if (d1 is not None and len(d1) >= 1) else None
        d1_low = float(d1[-1]['low']) if (d1 is not None and len(d1) >= 1) else None
        first_d1_close = float(d1[-2]['close']) if (d1 is not None and len(d1) >= 2) else d1_close

        def pct_change(arr):
            try:
                return (float(arr[-1]['close']) - float(arr[-2]['close'])) / float(arr[-2]['close']) * 100.0
            except Exception:
                return None

        ss_1h = pct_change(h1) if (h1 is not None and len(h1) >= 2) else None
        ss_4h = pct_change(h4) if (h4 is not None and len(h4) >= 2) else None
        ss_1d = pct_change(d1) if (d1 is not None and len(d1) >= 2) else None
        ss_1w = pct_change(w1) if (w1 is not None and len(w1) >= 2) else None
        ss_4h_prev = None
        if h4 is not None and len(h4) >= 3:
            try:
                ss_4h_prev = (float(h4[-2]['close']) - float(h4[-3]['close'])) / float(h4[-3]['close']) * 100.0
            except Exception:
                pass

        atr_d1 = None
        atrp = None
        try:
            if d1 is not None and len(d1) >= 15:
                vals = [(float(b['high']), float(b['low']), float(b['close'])) for b in d1[-15:]]
                atr_d1 = _atr(vals, 14)
                if atr_d1 is not None and d1_close is not None and d1_close != 0:
                    atrp = (atr_d1 / d1_close) * 100.0
        except Exception:
            pass

        s1, r1 = _pivots_from_prev_day(d1)

        spreadpct = None
        try:
            if bid is not None and ask is not None and bid > 0 and ask > 0:
                mid = (bid + ask) / 2.0
                spreadpct = (ask - bid) / mid * 100.0
        except Exception:
            pass

        m15_close = float(m15[-1]['close']) if (m15 is not None and len(m15) >= 1) else None
        h1_close = float(h1[-1]['close']) if (h1 is not None and len(h1) >= 1) else None

        def build_row(**kwargs):
            row = {}
            for k, v in kwargs.items():
                row[canonicalize_key(k)] = v
            row[HEADER_SYMBOL] = sym
            return row

        first_row = build_row(**{
            'D1 Close': first_d1_close,
            'Strength 4H': ss_4h_prev,
        })

        if tick_age is None and tick_time_utc is not None:
            try:
                tick_age = max(0.0, (now_utc - tick_time_utc).total_seconds())
            except Exception:
                tick_age = None

        last_row = build_row(**{
            'Bid': bid,
            'Ask': ask,
            'Spread%': spreadpct,
            'Backfilled': 0,
            'Strength 1H': ss_1h,
            'Strength 4H': ss_4h,
            'Strength 1D': ss_1d,
            'Strength 1W': ss_1w,
            'ATR D1': atr_d1,
            'ATR (%) D1': atrp,
            'S1 Level M5': s1,
            'R1 Level M5': r1,
            'D1 Close': d1_close,
            'D1 High': d1_high,
            'D1 Low': d1_low,
            'M15 Close': m15_close,
            'H1 Close': h1_close,
            'Recent Tick': 1 if has_recent_tick else 0,
            'Last Tick UTC': tick_time_utc.strftime('%Y-%m-%d %H:%M:%S') if tick_time_utc else '',
            'Tick Age Sec': tick_age if tick_age is not None else '',
        })
        series[sym] = [Snapshot(ts=latest_ts, row=first_row), Snapshot(ts=latest_ts, row=last_row)]
    return series, None, latest_ts


def _ensure_proximity_bin_schema(cur: "sqlite3.Cursor", table: str) -> bool:
    """Ensure the proximity_bin column exists on the setups table."""
    try:
        cur.execute(f"PRAGMA table_info({table})")
        cols = {str(r[1]) for r in (cur.fetchall() or [])}
        if "proximity_bin" in cols:
            return True
        cur.execute(f"ALTER TABLE {table} ADD COLUMN proximity_bin TEXT")
        return True
    except Exception:
        return False


def _backfill_missing_proximity_bins(cur: "sqlite3.Cursor", table: str) -> int:
    """Populate proximity_bin for rows that already have proximity_to_sl recorded."""
    try:
        cur.execute(
            f"""
            SELECT id, proximity_to_sl
            FROM {table}
            WHERE (proximity_bin IS NULL OR proximity_bin = '')
              AND proximity_to_sl IS NOT NULL
            """
        )
        rows = cur.fetchall() or []
    except Exception:
        return 0
    updates: List[Tuple[str, int]] = []
    for row in rows:
        try:
            rec_id = int(row[0])
        except Exception:
            continue
        prox = row[1]
        bin_label = _proximity_bin_label(prox)
        if bin_label:
            updates.append((bin_label, rec_id))
    if not updates:
        return 0
    try:
        cur.executemany(
            f"UPDATE {table} SET proximity_bin = ? WHERE id = ?",
            updates,
        )
        return len(updates)
    except Exception:
        return 0


def _filter_recent_duplicates(results: List[Dict[str, object]], table: str = "timelapse_setups") -> Tuple[List[Dict[str, object]], Set[str]]:
    """Filter out results for symbols that already have an unsettled (open) setup.

    A setup is considered open if there is no record for its id in timelapse_hits.
    If the hits table is missing, leave results unchanged.

    Returns:
        Tuple of (filtered_results, excluded_symbols)
    """
    if not results or sqlite3 is None:
        return results, set()
    db_path = str(default_db_path())
    conn: Optional["sqlite3.Connection"] = None
    try:
        conn = _connect_sqlite(db_path, timeout=3.0)
        cur = conn.cursor()
        # Ensure setups table exists
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
        if cur.fetchone() is None:
            return results, set()
        # Check hits table presence
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", ("timelapse_hits",))
        if cur.fetchone() is None:
            return results, set()
        schema_ready = _ensure_proximity_bin_schema(cur, table)
        backfilled = 0
        if schema_ready:
            backfilled = _backfill_missing_proximity_bins(cur, table)
        if backfilled > 0:
            try:
                conn.commit()
            except Exception:
                pass
        # Compute open setups grouped by symbol, direction, and proximity bin
        cur.execute(
            f"""
            SELECT t.symbol, t.direction, COALESCE(t.proximity_bin, '')
            FROM {table} t
            LEFT JOIN timelapse_hits h ON h.setup_id = t.id
            WHERE h.setup_id IS NULL
            """
        )
        open_groups = set()
        for rec in cur.fetchall() or []:
            if not rec:
                continue
            sym = str(rec[0] or "")
            direction = str(rec[1] or "")
            bin_label = str(rec[2] or "")
            if sym:
                open_groups.add((sym.upper(), direction.upper(), bin_label))
        if not open_groups:
            return results, set()

        filtered: List[Dict[str, object]] = []
        excluded: Set[str] = set()
        for row in results:
            sym = str(row.get("symbol") or "")
            direction = str(row.get("direction") or "")
            prox = row.get("proximity_to_sl")
            bin_label = _proximity_bin_label(prox) or ""
            key = (sym.upper(), direction.upper(), bin_label)
            if key in open_groups:
                excluded.add(sym)
                continue
            filtered.append(row)
            if sym:
                open_groups.add(key)
        return filtered, excluded
    except Exception:
        return results, set()
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _load_live_bin_filters() -> Optional[Dict[str, object]]:
    """Load GUI-published live bin filters for symbol/category gating."""
    if sqlite3 is None:
        return None
    db_path = str(default_db_path())
    conn: Optional["sqlite3.Connection"] = None
    try:
        conn = _connect_sqlite(db_path, timeout=2.0)
        data = load_live_bin_filters_sqlite(conn)
    except Exception:
        data = None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    if not data:
        return None
    allowed_raw = data.get("allowed_bins")
    allowed: Dict[str, set[str]] = {}
    if isinstance(allowed_raw, dict):
        for key, values in allowed_raw.items():
            if not values:
                continue
            bucket = {str(v) for v in values if v is not None}
            if bucket:
                allowed[str(key).lower()] = bucket
    return {
        "min_edge": data.get("min_edge"),
        "min_trades": data.get("min_trades"),
        "allowed_bins": allowed,
        "live_enabled": bool(data.get("live_enabled")),
    }


def _lookup_setup_id(
    conn: "sqlite3.Connection",
    symbol: str,
    direction: str,
    as_of_db: str,
) -> Optional[int]:
    """Resolve the setup id for a freshly inserted setup row."""
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id FROM timelapse_setups
            WHERE symbol = ? AND direction = ? AND as_of = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (symbol, direction, as_of_db),
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            return int(row[0])
        # Fallback: pick the most recent row for the symbol/direction pair
        cur.execute(
            """
            SELECT id FROM timelapse_setups
            WHERE symbol = ? AND direction = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (symbol, direction),
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except Exception:
        return None
    return None


def _candidate_filling_modes(symbol: str) -> List[Optional[int]]:
    """Return candidate filling modes (including None for default) ordered by preference."""
    sym_key = symbol.upper()
    modes: List[Optional[int]] = []
    cached = _SYMBOL_FILLING_CACHE.get(sym_key)
    if cached is not None:
        modes.append(cached)
    if _mt5_ensure_init():
        try:
            info = mt5.symbol_info(symbol)  # type: ignore[union-attr, reportUnknownMember]
        except Exception:
            info = None
        if info is not None:
            try:
                filling = getattr(info, "filling_mode", None)
            except Exception:
                filling = None
            if isinstance(filling, int) and filling >= 0:
                modes.append(filling)
    # Append standard fallbacks (avoid duplicates)
    for attr in ("ORDER_FILLING_RETURN", "ORDER_FILLING_IOC", "ORDER_FILLING_FOK", "ORDER_FILLING_BOC"):
        try:
            val = getattr(mt5, attr)  # type: ignore[union-attr]
        except Exception:
            continue
        if isinstance(val, int) and val not in modes:
            modes.append(val)
    if None not in modes:
        modes.append(None)
    return modes


def _send_market_order(symbol: str, direction: str) -> Optional[Dict[str, object]]:
    """Send a market order to MT5 with retry behaviour for transient failures."""
    if not _mt5_ensure_init():
        print(f"[ORDER] MT5 not ready; cannot send order for {symbol}.")
        return None
    try:
        mt5.symbol_select(symbol, True)  # type: ignore[union-attr, reportUnknownMember]
    except Exception:
        pass
    order_type = getattr(mt5, "ORDER_TYPE_BUY", 0) if direction.lower() == "buy" else getattr(mt5, "ORDER_TYPE_SELL", 1)
    action = getattr(mt5, "TRADE_ACTION_DEAL", 1)
    time_type = getattr(mt5, "ORDER_TIME_GTC", 0)
    fill_modes = _candidate_filling_modes(symbol)
    recoverable_codes = {
        getattr(mt5, "TRADE_RETCODE_REQUOTE", 10004),
        getattr(mt5, "TRADE_RETCODE_PRICE_CHANGED", 10006),
        getattr(mt5, "TRADE_RETCODE_REJECT", 10010),
        getattr(mt5, "TRADE_RETCODE_TRADE_CONTEXT_BUSY", 10011),
        getattr(mt5, "TRADE_RETCODE_OFFQUOTES", 10014),
        getattr(mt5, "TRADE_RETCODE_TIMEOUT", 10015),
    }
    invalid_fill_codes = {
        getattr(mt5, "TRADE_RETCODE_INVALID_FILL", 10030),
        getattr(mt5, "TRADE_RETCODE_INVALID_PARAMS", 10013),
    }
    success_codes = {
        getattr(mt5, "TRADE_RETCODE_DONE", 10009),
        getattr(mt5, "TRADE_RETCODE_PLACED", 10008),
    }
    attempts = 0
    last_retcode = None
    last_comment = ""
    mode_index = 0
    max_attempts = ORDER_MAX_ATTEMPTS if ORDER_MAX_ATTEMPTS > 0 else None
    while True:
        attempts += 1
        try:
            tick = mt5.symbol_info_tick(symbol)  # type: ignore[union-attr, reportUnknownMember]
        except Exception:
            tick = None
        price = None
        if tick is not None:
            try:
                price = float(getattr(tick, "ask" if direction.lower() == "buy" else "bid", 0.0) or 0.0)
            except Exception:
                price = None
        if price is None or price <= 0:
            time.sleep(ORDER_RETRY_DELAY)
            continue
        request = {
            "action": action,
            "symbol": symbol,
            "volume": float(ORDER_VOLUME),
            "type": order_type,
            "price": float(price),
            "deviation": ORDER_DEVIATION,
            "comment": ORDER_COMMENT,
        }
        if time_type is not None:
            request["type_time"] = time_type

        current_mode: Optional[int] = None
        if 0 <= mode_index < len(fill_modes):
            current_mode = fill_modes[mode_index]
        if current_mode is not None:
            request["type_filling"] = current_mode
        else:
            request.pop("type_filling", None)
        try:
            result = mt5.order_send(request)  # type: ignore[union-attr, reportUnknownMember]
        except Exception as exc:
            last_comment = str(exc)
            result = None
        if result is None:
            if max_attempts is not None and attempts >= max_attempts:
                break
            time.sleep(ORDER_RETRY_DELAY)
            continue
        retcode = getattr(result, "retcode", None)
        last_retcode = retcode
        if retcode in success_codes:
            ticket = getattr(result, "order", 0) or getattr(result, "deal", 0)
            sent_at = datetime.now(UTC)
            try:
                price_fmt = f"{price:.5f}"
            except Exception:
                price_fmt = str(price)
            print(
                f"[ORDER] Sent {direction.upper()} {symbol} volume {ORDER_VOLUME:.2f} @ {price_fmt} "
                f"ticket {ticket} (retcode {retcode})"
            )
            sym_key = symbol.upper()
            _SYMBOL_FILLING_CACHE[sym_key] = current_mode
            return {
                "ticket": str(ticket) if ticket else None,
                "price": price,
                "retcode": retcode,
                "sent_at": sent_at,
                "filling_mode": current_mode,
                "volume": float(ORDER_VOLUME),
            }
        last_comment = getattr(result, "comment", "") or ""
        unsupported_fill = False
        if retcode in invalid_fill_codes:
            unsupported_fill = True
        else:
            comment_lower = last_comment.lower()
            if "filling" in comment_lower and ("unsupported" in comment_lower or "invalid" in comment_lower):
                unsupported_fill = True
        if unsupported_fill:
            mode_index += 1
            if mode_index >= len(fill_modes):
                break
            next_mode = fill_modes[mode_index] if mode_index < len(fill_modes) else None
            try:
                mode_repr = "default" if next_mode is None else str(next_mode)
                print(f"[ORDER] Retrying {direction.upper()} {symbol} with filling mode {mode_repr}")
            except Exception:
                pass
            # retry immediately with next mode without counting towards max attempts
            attempts -= 1
            continue
        if retcode not in recoverable_codes:
            break
        if max_attempts is not None and attempts >= max_attempts:
            break
        time.sleep(ORDER_RETRY_DELAY)
    detail = last_comment or f"retcode {last_retcode}"
    print(f"[ORDER] Failed to send {direction.upper()} {symbol}: {detail}")
    return None



def _ensure_setup_row(conn, event: Dict[str, object]) -> Optional[int]:
    """Insert a minimal timelapse_setup row when gating skipped the original insert."""

    if not isinstance(event, dict):
        return None
    row_data = event.get('row_data') if isinstance(event.get('row_data'), dict) else None
    if row_data is None:
        return None

    symbol = event.get('symbol')
    direction = event.get('direction')
    as_of = event.get('as_of')
    if not symbol or not direction or not as_of:
        return None

    def _to_float(val: object) -> Optional[float]:
        try:
            return float(val) if val is not None else None
        except Exception:
            return None

    price = _to_float(row_data.get('price'))
    sl = _to_float(row_data.get('sl'))
    tp = _to_float(row_data.get('tp'))
    rrr = _to_float(row_data.get('rrr'))
    score = _to_float(row_data.get('score'))
    proximity = _to_float(row_data.get('proximity_to_sl'))
    prox_bin = row_data.get('proximity_bin') if isinstance(row_data.get('proximity_bin'), str) else None
    detected_at = row_data.get('detected_at') if isinstance(row_data.get('detected_at'), str) else None
    explain = row_data.get('explain') if isinstance(row_data.get('explain'), str) else None

    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO timelapse_setups (
                symbol, direction, price, sl, tp, rrr, score, explain, as_of, detected_at, proximity_to_sl, proximity_bin
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                direction,
                price,
                sl,
                tp,
                rrr,
                score,
                explain,
                as_of,
                detected_at,
                proximity,
                prox_bin,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    except Exception:
        return None


def _persist_order_events(events: List[Dict[str, object]]) -> None:
    """Persist MT5 order metadata into tp_sl_setup_state once setup IDs are known."""
    if not events or sqlite3 is None:
        return
    db_path = str(default_db_path())
    conn: Optional["sqlite3.Connection"] = None
    try:
        conn = _connect_sqlite(db_path, timeout=3.0)
        for event in events:
            symbol = str(event.get("symbol") or "")
            direction = str(event.get("direction") or "")
            as_of_db = str(event.get("as_of") or "")
            ticket = event.get("ticket")
            sent_at = event.get("sent_at")
            volume = event.get("volume")
            if not symbol or not direction or not as_of_db:
                continue
            setup_id = _lookup_setup_id(conn, symbol, direction, as_of_db)
            if setup_id is None:
                setup_id = _ensure_setup_row(conn, event)
                if setup_id is None:
                    continue
            try:
                persist_order_sent_sqlite(
                    conn,
                    setup_id=setup_id,
                    ticket=ticket,
                    sent_at=sent_at if isinstance(sent_at, datetime) else None,
                    last_checked_fallback=sent_at if isinstance(sent_at, datetime) else None,
                    volume=volume if isinstance(volume, (int, float)) else None,
                )
            except Exception as exc:
                print(f"[ORDER] Warning: failed to persist order state for setup {setup_id}: {exc}")
    except Exception as exc:
        print(f"[ORDER] Warning: could not save order metadata: {exc}")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _execute_live_orders(
    results: List[Dict[str, object]],
    debug: bool = False,
) -> List[Dict[str, object]]:
    """Scan analyzed setups and send market orders for qualifying bins."""
    if not results:
        return []
    filters = _load_live_bin_filters()
    if not filters:
        if debug:
            print("[ORDER] Skipping order send: live bin filters are unavailable.")
        return []
    if not filters.get("live_enabled"):
        if debug:
            print("[ORDER] Live trading disabled; orders will not be sent.")
        return []
    allowed_bins = filters.get("allowed_bins") or {}
    if not allowed_bins:
        if debug:
            print("[ORDER] Skipping order send: no bins meet live profitability criteria.")
        return []
    sent_orders: List[Dict[str, object]] = []
    for row in results:
        symbol = str(row.get("symbol") or "")
        direction = str(row.get("direction") or "")
        if not symbol or not direction:
            continue
        prox = row.get("proximity_to_sl")
        bin_label = _proximity_bin_label(prox) or ""
        if not bin_label:
            continue
        category = classify_symbol(symbol) or "other"
        cat_key = str(category).lower()
        allowed_for_cat = allowed_bins.get(cat_key)
        if not allowed_for_cat or bin_label not in allowed_for_cat:
            if debug:
                print(
                    f"[ORDER] Skip {symbol} {direction}: bin '{bin_label}' for category '{cat_key}' "
                    "not in profitable set."
                )
            continue
        order_result = _send_market_order(symbol, direction)
        if order_result:
            as_of_db = _format_as_of_for_db(row.get("as_of"))
            row_payload = {
                "price": row.get("price"),
                "sl": row.get("sl"),
                "tp": row.get("tp"),
                "rrr": row.get("rrr"),
                "score": row.get("score"),
                "detected_at": row.get("detected_at"),
                "proximity_to_sl": row.get("proximity_to_sl"),
                "proximity_bin": bin_label,
                "explain": row.get("explain"),
            }
            sent_orders.append(
                {
                    "symbol": symbol,
                    "direction": direction,
                    "as_of": as_of_db,
                    "ticket": order_result.get("ticket"),
                    "sent_at": order_result.get("sent_at"),
                    "volume": order_result.get("volume"),
                    "row_data": row_payload,
                }
            )
    return sent_orders




def process_once(
    symbols: List[str],
    min_rrr: float,
    top: Optional[int],
    brief: bool,
    debug: bool = False,
    exclude_set: Optional[Set[str]] = None,
    detected_at: Optional[datetime] = None,
) -> None:
    series, mkttvc, as_of_ts = read_series_mt5(symbols)
    if not series:
        # Stay quiet unless debugging to reduce log noise
        if debug:
            print("No MT5 symbols resolved or no data fetched.")
        return

    # Apply exclude filter immediately after MT5 reading (before any analysis)
    if exclude_set:
        excluded_count = 0
        excluded_symbols = []
        for symbol in list(series.keys()):
            if symbol.upper() in exclude_set:
                excluded_symbols.append(symbol)
                del series[symbol]
                excluded_count += 1

    results, reasons = analyze(
        series,
        min_rrr=min_rrr,
        as_of_ts=as_of_ts,
        debug=debug,
    )
    # No banner output in quiet mode
    if debug and mkttvc:
        print(f"#MKTTVC (total market volume change): {mkttvc}")

    # Suppress symbols that have an open (unsettled) setup in the DB already
    filtered, db_excluded = _filter_recent_duplicates(results, table="timelapse_setups")
    # Track DB exclusions in rejection reasons
    if db_excluded:  # Only add if there are excluded symbols
        reasons.setdefault("one_trade_per_bin", []).extend(list(db_excluded))

    # Apply top limit after filtering
    top_results = filtered[: top] if top else filtered

    # Trigger live execution before DB mutations for minimal latency
    order_events = _execute_live_orders(top_results, debug=debug)

    if not top_results and debug:
        print("No high-confidence setups based on the latest timelapse and filters.")

    # Always show rejection summary
    total_evaluated = len(series)
    total_kept = len(filtered)  # Use filtered count which includes DB exclusions
    total_filtered = total_evaluated - total_kept
    if debug and total_filtered > 0:
        print(f"Symbols evaluated: {total_evaluated}, kept: {total_kept}, filtered: {total_filtered}")
        if reasons:
            print("Rejection reasons:")
            for reason, symbol_list in sorted(reasons.items()):
                print(f"  {reason}: {len(symbol_list)} | {' | '.join(symbol_list)}")
    if debug:
        for r in top_results:
            sym = r["symbol"]
            direction = r["direction"]
            price = r["price"]
            sl = r["sl"]
            tp = r["tp"]
            rrr = r["rrr"]
            score = r["score"]
            print(f"{sym} | {direction} @ {price} | SL {sl} | TP {tp} | RRR {rrr:.2f} | score {score:.2f}")
            if not brief:
                try:
                    rich = {k: v for k, v in r.items() if k not in ('series', 'raw_snaps')}
                    print(json.dumps(rich, indent=2, default=str))
                except Exception:
                    print(r)

    # Insert results into SQLite
    try:
        insert_results_to_db(top_results, table="timelapse_setups", detected_at=detected_at)
    except Exception as e:
        # Keep non-fatal; just report the issue
        print(f"[DB] Skipped insert due to error: {e}")
    else:
        if order_events:
            _persist_order_events(order_events)


def watch_loop(
    symbols: List[str],
    interval: float,
    min_rrr: float,
    top: Optional[int],
    brief: bool,
    debug: bool = False,
    exclude_set: Optional[Set[str]] = None,
) -> None:
    # Quiet by default; only show banner in debug mode
    if debug:
        print(f"Polling MT5 for {len(symbols)} symbols every {interval:.1f}s. Press Ctrl+C to stop.")
        if exclude_set:
            print(f"[EXCLUDE] Will filter out symbols: {sorted(exclude_set)}")
    try:
        while True:
            detected_at = datetime.now(UTC)
            process_once(
                symbols,
                min_rrr=min_rrr,
                top=top,
                brief=brief,
                debug=debug,
                exclude_set=exclude_set,
                detected_at=detected_at,
            )
            time.sleep(max(0.0, interval))
    except KeyboardInterrupt:
        if debug:
            print("\nStopped watching.")




def analyze(
    series: Dict[str, List[Snapshot]],
    min_rrr: float,
    as_of_ts: Optional[datetime],
    debug: bool = False,
) -> Tuple[List[Dict[str, object]], Dict[str, List[str]]]:
    reasons: Dict[str, List[str]] = {}
    def bump(key: str) -> None:
        reasons.setdefault(key, []).append(sym)
    results: List[Dict[str, Any]] = []
    for sym, snaps in series.items():
        if not snaps:
            continue
        snaps.sort(key=lambda s: s.ts)
        first, last = snaps[0], snaps[-1]
        prox: Optional[float] = None

        # Filter out entries during the shared quiet-trading windows.
        if is_quiet_time(last.ts, symbol=sym):
            if debug:
                try:
                    quiet_ts = last.ts.astimezone(UTC3)
                    print(
                        f"[DEBUG] low_vol_time_window at "
                        f"{quiet_ts.strftime('%Y-%m-%d %H:%M:%S')} UTC+3; quiet hours active"
                    )
                except Exception:
                    pass
            bump("low_vol_time_window")
            continue

        # Extract metrics from latest
        ss1h = last.g("Strength 1H")
        ss4 = last.g("Strength 4H")
        ss1d = last.g("Strength 1D")
        ss1w = last.g("Strength 1W")
        atr = last.g("ATR D1")
        atrp = last.g("ATR (%) D1")
        s1 = last.g("S1 Level M5")
        r1 = last.g("R1 Level M5")
        d1_close = last.g("D1 Close")
        d1h = last.g("D1 High")
        d1l = last.g("D1 Low")
        spreadpct_row = normalize_spread_pct(last.g("Spread%"))
        bid = last.g("Bid")
        ask = last.g("Ask")
        # Pull auxiliary tick info for downstream reporting
        try:
            tick_time_str = str(last.row.get(canonicalize_key('Last Tick UTC'), '') or '')
        except Exception:
            tick_time_str = ''

        # Drop symbols without recent ticks (avoid closed markets)
        recent_tick_flag = last.g("Recent Tick")
        if recent_tick_flag is None or recent_tick_flag <= 0:
            bump("no_recent_ticks")
            if debug:
                print(f"[DEBUG] no_recent_ticks for {sym}")
            continue

        # Timelapse deltas (context)
        d1_close_trend = None if (d1_close is None or first.g("D1 Close") is None) else d1_close - (first.g("D1 Close") or 0.0)
        ss4_trend = None if (ss4 is None or first.g("Strength 4H") is None) else ss4 - (first.g("Strength 4H") or 0.0)

        # Direction from strength across TFs (require 4H alignment when available)
        pos = sum(1 for v in (ss1h, ss4, ss1d) if v is not None and v > 0)
        neg = sum(1 for v in (ss1h, ss4, ss1d) if v is not None and v < 0)
        direction: Optional[str] = None
        if pos >= 2 and neg == 0 and (ss4 is None or ss4 > 0):
            direction = "Buy"
        elif neg >= 2 and pos == 0 and (ss4 is None or ss4 < 0):
            direction = "Sell"
        else:
            bump("no_direction_consensus")
            continue

        # Entry price uses Bid/Ask at signal timestamp based on direction
        if direction == "Buy":
            price = ask
        else:  # Sell
            price = bid

        # If Bid/Ask are not available, skip until live tick arrives
        if price is None:
            bump("no_live_bid_ask")
            if debug:
                try:
                    print(f"[DEBUG] skipping {sym}: no live Bid/Ask; waiting for live tick")
                except Exception:
                    pass
            continue

        # Spread calculation only when bid and ask is available
        spreadpct = None
        try:
            if bid is not None and ask is not None and bid > 0 and ask > 0 and ask > bid:
                mid = (ask + bid) / 2.0
                spreadpct = ((ask - bid) / mid) * 100.0
        except Exception:
            spreadpct = None
        spr_class = spread_class(spreadpct)
        if spr_class == "Avoid":
            bump("spread_avoid")
            if debug:
                print(f"[DEBUG] spread_avoid {sym}: bid={bid} ask={ask} spreadpct={spreadpct}")
            continue
        # ATR% score bonus only when value is present and within [60, 150]
        atrp_in_range = (atrp is not None) and (60.0 <= atrp <= 150.0)

        # S/R based SL/TP with D1 fallback; entry/RRR strictly computed from live Bid/Ask only
        # Initialize proximity flags
        prox_note = None
        prox_flag = None
        prox_late = False
        if direction == "Buy":
            if s1 is None and r1 is None:
                bump("missing_sl_tp")
                continue
            sl = s1 if s1 is not None else d1l
            tp = r1 if r1 is not None else d1h
            # Basic sanity for S/R orientation
            if sl is None or tp is None:
                bump("missing_sl_tp")
                continue
            if not (sl <= price <= tp):
                bump("price_outside_buy_sr")
                continue
            risk = price - sl if price is not None and sl is not None else None
            reward = (tp - price) if (tp is not None and price is not None) else None
            rrr = None
            if risk is not None and reward is not None and risk > 0 and reward > 0:
                rrr = reward / risk
            if (tp is not None and sl is not None) and (tp - sl) != 0:
                # Use the bid leg for distance-to-stop so proximity reflects actual SL trigger side.
                prox_price = bid if bid is not None else price
                if prox_price is not None:
                    prox = (prox_price - sl) / (tp - sl)
                    prox = max(0.0, min(1.0, prox))
                if prox is not None and prox <= 0.35:
                    prox_flag = "near_support"
                    prox_note = "near S1 support"
                elif prox is not None and prox >= 0.65:
                    prox_flag = "near_resistance"
                    prox_late = True
                    prox_note = "near R1 resistance (late)"
        else:  # Sell
            if s1 is None and r1 is None:
                bump("missing_sl_tp")
                continue
            sl = r1 if r1 is not None else d1h
            tp = s1 if s1 is not None else d1l
            if sl is None or tp is None:
                bump("missing_sl_tp")
                continue
            if not (tp <= price <= sl):
                bump("price_outside_sell_sr")
                continue
            risk = (sl - price) if price is not None and sl is not None else None
            reward = (price - tp) if (price is not None and tp is not None) else None
            rrr = None
            if risk is not None and reward is not None and risk > 0 and reward > 0:
                rrr = reward / risk
            if (sl is not None and tp is not None) and (sl - tp) != 0:
                # Use the ask leg for distance-to-stop so proximity reflects actual SL trigger side.
                prox_price = ask if ask is not None else price
                if prox_price is not None:
                    prox = (sl - prox_price) / (sl - tp)
                    prox = max(0.0, min(1.0, prox))
                if prox is not None and prox <= 0.35:
                    prox_flag = "near_resistance"
                    prox_note = "near R1 resistance"
                elif prox is not None and prox >= 0.65:
                    prox_flag = "near_support"
                    prox_late = True
                    prox_note = "near S1 support (late)"
        if rrr is None or rrr <= 0:
            bump("invalid_rrr")
            continue

        # Spread-based SL distance filter: SL must be at least 10x spread away from the side
        # that actually triggers the stop:
        #   - Buy: stop is hit on Bid -> use (bid - SL)
        #   - Sell: stop is hit on Ask -> use (SL - ask)
        eps = 1e-12
        if sl is not None:
            # Require both bid and ask to be available
            if bid is None or ask is None or ask <= bid:
                bump("invalid_bid_ask_for_spread_calculation")
                if debug:
                    print(f"[DEBUG] Invalid bid/ask for spread calculation: sym={sym}, bid={bid}, ask={ask}")
                continue

            raw_spread = ask - bid
            spread_abs = _augment_spread_for_demo(sym, raw_spread)
            # Calculate distance based on direction
            distance: Optional[float] = None
            if direction == "Buy":
                distance = bid - sl
            else:  # Sell
                distance = sl - ask

            # Enforce minimum distance threshold (10x spread) with tiny epsilon
            if distance is None or (distance + eps) < (SPREAD_MULTIPLIER * spread_abs):
                bump("sl_too_close_to_spread")
                if debug:
                    print(f"[DEBUG] SL too close to spread: sym={sym}, dir={direction}, price={price}, sl={sl}, spread_abs={spread_abs}, distance={distance}")
                continue

        if tp is not None:
            if bid is None or ask is None or ask <= bid:
                bump("invalid_bid_ask_for_spread_calculation")
                if debug:
                    print(f"[DEBUG] Invalid bid/ask for TP spread calculation: sym={sym}, bid={bid}, ask={ask}")
                continue

            raw_spread = ask - bid
            spread_abs = _augment_spread_for_demo(sym, raw_spread)
            tp_distance: Optional[float] = None
            if direction == "Buy":
                tp_distance = tp - bid
            else:
                tp_distance = ask - tp

            if tp_distance is None or (tp_distance + eps) < (SPREAD_MULTIPLIER * spread_abs):
                bump("too_far_from_tp_prox")
                if debug:
                    print(f"[DEBUG] TP too close to spread: sym={sym}, dir={direction}, price={price}, tp={tp}, spread_abs={spread_abs}, distance={tp_distance}")
                continue


        # Composite score
        score = 0.0
        score += (pos if direction == "Buy" else neg) * 1.5  # Strength consensus
        if atrp_in_range:
            score += 0.5
        score += {"Excellent": 1.0, "Good": 0.5, "Acceptable": 0.0, "Avoid": -2.0}.get(spr_class, 0.0)
        if d1_close_trend is not None and ((direction == "Buy" and d1_close_trend > 0) or (direction == "Sell" and d1_close_trend < 0)):
            score += 1.0
        if ss4_trend is not None and ((direction == "Buy" and ss4_trend > 0) or (direction == "Sell" and ss4_trend < 0)):
            score += 0.8
        # Penalize late entries near opposing level
        if prox_late:
            score -= 0.4


        # Build explanation
        parts: List[str] = []
        if ss1h is not None and ss4 is not None and ss1d is not None:
            parts.append(f"Strength 1H/4H/1D: {ss1h:.1f}/{ss4:.1f}/{ss1d:.1f}")
        if atr is not None and atrp is not None:
            parts.append(f"ATR: {atr:.1f} pips ({atrp:.1f}%)")
        if (s1 is not None) or (r1 is not None):
            parts.append(
                f"S/R: S1={s1 if s1 is not None else 'N/A'}, R1={r1 if r1 is not None else 'N/A'}" + (f" {prox_note}" if prox_note else "")
            )
        tparts: List[str] = []
        if d1_close_trend is not None and abs(d1_close_trend / price) >= 0.005:  # 0.5% threshold
            tparts.append(f"D1 Close {'up' if d1_close_trend > 0 else 'down'} {abs(d1_close_trend):.4f}")
        if ss4_trend is not None and abs(ss4_trend) >= 2:
            tparts.append(f"Sym4H {'up' if ss4_trend > 0 else 'down'} {abs(ss4_trend):.1f}")
        if tparts:
            parts.append("Timelapse: " + ", ".join(tparts))
        spct_str = f"{spreadpct:.2f}%" if spreadpct is not None else "N/A"
        parts.append(f"Spread: {spct_str} ({spr_class})")

        # Use the latest file timestamp consistently for all rows
        # Store UTC time as naive timestamp for DB (no TZ)
        as_of_value = utc_naive(as_of_ts or last.ts)

        # Round SL/TP to the same precision as price for this symbol
        digits = _symbol_digits(sym, price)
        digits = max(
            digits,
            _infer_decimals_from_price(sl),
            _infer_decimals_from_price(tp),
        )
        def _r(v: Optional[float]) -> Optional[float]:
            try:
                return None if v is None else round(float(v), int(digits))
            except Exception:
                return v

        price_out = _r(price)
        sl_out = _r(sl)
        tp_out = _r(tp)

        results.append(
            {
                "symbol": sym,
                "direction": direction,
                "price": price_out if price_out is not None else price,
                "sl": sl_out if sl_out is not None else sl,
                "tp": tp_out if tp_out is not None else tp,
                "rrr": rrr,
                "score": score,
                "as_of": as_of_value,
                "proximity_to_sl": prox,
                # Meta for logging; not used for DB schema
                "bid": bid,
                "ask": ask,
                "tick_utc": tick_time_str,
            }
        )

    # Order by score then RRR
    results.sort(key=lambda x: (float(x["score"]), float(x["rrr"])), reverse=True)
    if debug:
        print("--- Diagnostics ---")
        print(f"Symbols evaluated: {len(series)}")
        print(f"Kept setups: {len(results)}")
        for k in sorted(reasons):
            print(f"Filtered {k}: {len(reasons[k])}")
        print("-------------------")
    return results, reasons



def insert_results_to_db(results: List[Dict[str, object]], table: str = "timelapse_setups", detected_at: Optional[datetime] = None) -> None:
    """Insert analyzed results into local SQLite DB.

    - Creates the table if it does not exist.
    - Deduplicates by (symbol, direction, as_of) using ON CONFLICT DO NOTHING.
    - Gating: insert only if there is no currently unsettled (no TP/SL hit) setup for the same symbol.
    """
    if sqlite3 is None:
        print("[DB] sqlite3 not available; cannot insert.")
        return
    conn = _get_db_connection()
    if conn is None:
        print("[DB] sqlite3 not available; cannot insert.")
        return

    with conn:
        cur = conn.cursor()
        # Create setups table
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                price REAL,
                sl REAL,
                tp REAL,
                rrr REAL,
                score REAL,
                as_of TEXT NOT NULL,
                detected_at TEXT,
                proximity_to_sl REAL,
                inserted_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
                UNIQUE(symbol, direction, as_of)
            )
            """
        )
        # Add proximity_to_sl column if it doesn't exist
        try:
            cur.execute(f"PRAGMA table_info({table})")
            cols = {str(r[1]) for r in (cur.fetchall() or [])}
            if 'proximity_to_sl' not in cols:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN proximity_to_sl REAL")
        except Exception as e:
            print(f"[DB] Warning: Could not add proximity_to_sl column: {e}")
        # Ensure hits table exists for open/settled gating
        try:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS timelapse_hits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    setup_id INTEGER UNIQUE,
                    symbol TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    sl REAL,
                    tp REAL,
                    hit TEXT NOT NULL CHECK (hit IN ('TP','SL')),
                    hit_price REAL,
                    hit_time TEXT NOT NULL,
                    hit_time_utc3 TEXT,
                    entry_time_utc3 TEXT,
                    entry_price REAL,
                    checked_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
                )
                """
            )
            # Add indexes for performance
            cur.execute("CREATE INDEX IF NOT EXISTS idx_timelapse_hits_symbol ON timelapse_hits (symbol)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_timelapse_hits_setup_id ON timelapse_hits (setup_id)")
        except Exception:
            # If creation fails, we'll proceed without gating
            pass

        inserted = 0
        if results:
            # Inspect columns to handle older schemas gracefully
            cur.execute(f"PRAGMA table_info({table})")
            cols = {str(r[1]) for r in (cur.fetchall() or [])}
            has_detected = 'detected_at' in cols
            has_prox_bin = 'proximity_bin' in cols
            # Try to migrate by adding detected_at if missing
            if not has_detected:
                try:
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN detected_at TEXT")
                    has_detected = True
                except Exception:
                    has_detected = False
            if not has_prox_bin:
                try:
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN proximity_bin TEXT")
                    has_prox_bin = True
                except Exception:
                    has_prox_bin = False

            column_list = [
                "symbol", "direction", "price", "sl", "tp", "rrr", "score", "as_of"
            ]
            if has_detected:
                column_list.append("detected_at")
            column_list.append("proximity_to_sl")
            if has_prox_bin:
                column_list.append("proximity_bin")

            select_placeholders = ["?" for _ in column_list]

            gating_conditions = [
                "COALESCE(t2.symbol, '') = COALESCE(?, '')",
                "COALESCE(t2.direction, '') = COALESCE(?, '')",
                "h.setup_id IS NULL",
            ]
            gating_has_bin = has_prox_bin
            if gating_has_bin:
                gating_conditions.insert(2, "COALESCE(t2.proximity_bin, '') = COALESCE(?, '')")

            ins = (
                f"""
                INSERT INTO {table}
                    ({', '.join(column_list)})
                SELECT {', '.join(select_placeholders)}
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table} t2
                    LEFT JOIN timelapse_hits h ON h.setup_id = t2.id
                    WHERE {' AND '.join(gating_conditions)}
                )
                ON CONFLICT(symbol, direction, as_of) DO NOTHING
                """
            )

            params: List[Tuple[object, ...]] = []
            for r in results:
                as_of_db = r.get("as_of")
                if isinstance(as_of_db, datetime):
                    as_of_val = as_of_db.strftime("%Y-%m-%d %H:%M:%S.%f")
                else:
                    as_of_val = str(as_of_db)
                detected_at_val = None
                if detected_at is not None:
                    if isinstance(detected_at, datetime):
                        detected_at_val = detected_at.strftime("%Y-%m-%d %H:%M:%S.%f")
                    else:
                        detected_at_val = str(detected_at)
                sym_val = r.get("symbol")
                direction_val = r.get("direction")
                proximity_val = r.get("proximity_to_sl")
                bin_val = _proximity_bin_label(proximity_val)

                row_values: List[object] = [
                    sym_val,
                    direction_val,
                    r.get("price"),
                    r.get("sl"),
                    r.get("tp"),
                    r.get("rrr"),
                    r.get("score"),
                    as_of_val,
                ]
                if has_detected:
                    row_values.append(detected_at_val)
                row_values.append(proximity_val)
                if has_prox_bin:
                    row_values.append(bin_val)

                gating_params_row: List[object] = [
                    sym_val or "",
                    direction_val or "",
                ]
                if gating_has_bin:
                    gating_params_row.append(bin_val or "")

                params.append(tuple(row_values + gating_params_row))

            if params:
                before = conn.total_changes
                cur.executemany(ins, params)
                inserted = conn.total_changes - before

        if inserted > 0:
            try:
                # Build detailed lines per result with tick time (UTC+3), bid/ask, and source
                lines = []
                for r in (results or []):
                    sym = str(r.get('symbol') or '')
                    bid = r.get('bid')
                    ask = r.get('ask')
                    src = str(r.get('source') or '')
                    t = str(r.get('tick_utc') or '')
                    # Convert UTC string to UTC+3 clock time for display
                    t_disp = 'N/A'
                    try:
                        if t:
                            # tick_utc is like 'YYYY-MM-DD HH:MM:SS' in UTC naive
                            dt = datetime.strptime(t, '%Y-%m-%d %H:%M:%S').replace(tzinfo=UTC)
                            t_disp = dt.astimezone(UTC3).strftime('%H:%M:%S') + ' UTC+3'
                    except Exception:
                        t_disp = t or 'N/A'
                    # Format bid/ask concisely when available
                    def f(x):
                        try:
                            return f"{float(x):.5f}"
                        except Exception:
                            return 'N/A'
                    line = f"{sym} | tick time {t_disp} | bid {f(bid)} | ask {f(ask)}" + (f" | source {src}" if src else '')
                    if sym:
                        lines.append(line)
                if lines:
                    print(f"[DB] Inserted {inserted} new setup(s):")
                    for line in lines:
                        print(f"  {line}")
                else:
                    # Fallback to symbol list only
                    syms = [str(r.get('symbol')) for r in (results or [])]
                    uniq = sorted({s for s in syms if s})
                    print(f"[DB] Inserted {inserted} new setup(s): {', '.join(uniq)}")
            except Exception:
                print(f"[DB] Inserted {inserted} new setup(s)")

        _rebuild_consensus_table(conn, source_table=table)


def main() -> None:
    args = parse_args()
    # Normalize exclude list to uppercase symbols set
    exclude_set: Optional[Set[str]] = None
    try:
        raw_ex = getattr(args, "exclude", "") or ""
        parts = [p.strip().upper() for p in raw_ex.split(",")]
        ex = {p for p in parts if p}
        if ex:
            exclude_set = ex
    except Exception:
        exclude_set = None
    # Determine symbols
    syms: List[str] = []
    try:
        raw_syms = (getattr(args, "symbols", "") or "").strip()
        if raw_syms:
            syms = [s.strip() for s in raw_syms.split(",") if s.strip()]
        else:
            # Default to visible MarketWatch symbols
            if _mt5_ensure_init():
                try:
                    infos = mt5.symbols_get()  # type: ignore[union-attr, reportUnknownMember]
                except Exception:
                    infos = []
                for info in (infos or []):
                    try:
                        if getattr(info, 'visible', True):
                            syms.append(str(getattr(info, 'name', '')))
                    except Exception:
                        pass
                syms = [s for s in syms if s]
    except Exception:
        syms = []
    # Apply exclude set eagerly to the symbol universe
    if exclude_set:
        syms = [s for s in syms if s.upper() not in exclude_set]
    if args.watch:
        watch_loop(
            symbols=syms,
            interval=max(0.5, args.interval),
            min_rrr=args.min_rrr,
            top=args.top,
            brief=args.brief,
            debug=args.debug,
            exclude_set=exclude_set,
        )
        return
    # Single-run mode
    process_once(
        symbols=syms,
        min_rrr=args.min_rrr,
        top=args.top,
        brief=args.brief,
        debug=args.debug,
        exclude_set=exclude_set,
    )

if __name__ == "__main__":
    main()


class _TimelapseModule(types.ModuleType):
    """Custom module wrapper to ensure global DB connection closes on reassignment."""

    def __setattr__(self, name: str, value: object) -> None:
        if name == "_DB_CONN":
            old = getattr(self, "_DB_CONN", None)
            if old is not None and old is not value:
                try:
                    old.close()  # type: ignore[attr-defined]
                except Exception:
                    pass
        super().__setattr__(name, value)


sys.modules[__name__].__class__ = _TimelapseModule  # type: ignore[misc]
