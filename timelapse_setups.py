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
  - Current price and RRR use Bid/Ask at signal timestamp (Buy: Ask, Sell: Bid).
  - ATR(%) effect: adds +0.5 score bonus when within [60, 150] (for informational purposes).
  - Timelapse: simulated from previous values in MT5 data for momentum context.
  - Crypto adaptation: No Delta FXP or volume; uses Strength consensus + D1 Close trend.
"""

from __future__ import annotations

import atexit
import argparse
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple, Set
# DB backend: SQLite only
try:
    import sqlite3  # type: ignore
except Exception:
    sqlite3 = None  # type: ignore

# Optional filesystem event support (watchdog)
HAS_WATCHDOG = False
try:
    from watchdog.observers import Observer  # type: ignore
    from watchdog.events import PatternMatchingEventHandler  # type: ignore
    HAS_WATCHDOG = True
except Exception:
    HAS_WATCHDOG = False

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_SQLITE_PATH = os.path.join(SCRIPT_DIR, "timelapse.db")
import json

# Optional MT5 for tick backfill
_MT5_IMPORTED = False
try:
    import MetaTrader5 as mt5  # type: ignore
    _MT5_IMPORTED = True
except Exception:
    mt5 = None  # type: ignore
    _MT5_IMPORTED = False


HEADER_SYMBOL = "symbol"

# Cache for canonicalized keys to speed up repeated lookups
CANONICAL_KEYS: Dict[str, str] = {}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze MT5 symbols for trade setups (MT5 is the only source)")
    p.add_argument("--symbols", default="", help="Comma-separated symbols (default: all visible in MarketWatch)")
    p.add_argument("--min-rrr", type=float, default=1.0, help="Minimum risk-reward ratio (default: 1.0)")
    # Optional guards (disabled by default). Primary fix is bid/ask backfill.
    p.add_argument("--min-prox-sl", type=float, default=0.0, help="Optional: require entry to be at least this fraction away from SL relative to SL..TP range")
    p.add_argument("--min-sl-pct", type=float, default=0.0, help="Optional: require |price-SL| to exceed this percent of price (units in %%)")
    p.add_argument("--top", type=int, default=None, help="Limit to top N setups (after filtering)")
    p.add_argument("--brief", action="store_true", help="Brief output without detailed explanation")
    p.add_argument("--watch", action="store_true", help="Run continuously and poll MT5 for updates")
    p.add_argument("--interval", type=float, default=1.0, help="Polling interval in seconds when --watch is used (default: 1)")
    p.add_argument("--debug", action="store_true", help="Print filtering diagnostics and counts")
    p.add_argument("--exclude", default="", help="Comma-separated symbols to exclude (e.g., GLMUSD,BCHUSD)")
    return p.parse_args()


# Fixed offset timezone for input interpretation (Europe/Berlin UTC+2)
INPUT_TZ = timezone(timedelta(hours=2))

UTC = timezone.utc
UTC3 = timezone(timedelta(hours=3))
# Consider market "alive" only if there's at least one tick
# within this many seconds. Avoid creating entries for closed markets.
TICK_FRESHNESS_SEC = 30  # 30 seconds
# Cache last tick data to minimize expensive history lookups
_LAST_TICK_CACHE: Dict[str, Tuple[Optional[float], Optional[float], Optional[datetime]]] = {}

# Cache MT5 rate data with lightweight TTLs per timeframe to reduce IPC churn
_RATE_CACHE: Dict[Tuple[str, int, int], Tuple[float, Any]] = {}
# Reusable SQLite connection handle (populated lazily)
_DB_CONN: Optional["sqlite3.Connection"] = None

def _get_db_connection() -> Optional["sqlite3.Connection"]:
    global _DB_CONN
    if sqlite3 is None:
        return None
    if _DB_CONN is None:
        db_path = DEFAULT_SQLITE_PATH
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        _DB_CONN = sqlite3.connect(db_path, timeout=5, check_same_thread=False)
    return _DB_CONN

def _close_db_connection() -> None:
    global _DB_CONN
    if _DB_CONN is not None:
        try:
            _DB_CONN.close()
        except Exception:
            pass
        _DB_CONN = None

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
    try:
        ok = mt5.initialize()
        _MT5_READY = bool(ok)
    except Exception:
        _MT5_READY = False
    return _MT5_READY


def _mt5_backfill_bid_ask(symbol: str, as_of: datetime, need_bid: bool, need_ask: bool, max_lookback_sec: int = 180) -> Tuple[Optional[float], Optional[float]]:
    """Fetch nearest prior Bid/Ask from MT5 tick history up to max_lookback_sec.

    Searches backwards from `as_of` (interpreted as UTC) within a sliding window
    for the last tick that updated the needed side(s). Returns (bid, ask) for
    the sides requested; non-requested sides are returned as None.
    """
    if not _mt5_ensure_init():
        return (None, None)
    try:
        # Convert as_of to UTC aware
        if as_of.tzinfo is None:
            end_utc = as_of.replace(tzinfo=UTC)
        else:
            end_utc = as_of.astimezone(UTC)

        window = 7  # seconds per fetch window
        looked = 0
        while looked < max_lookback_sec and ((need_bid and out_bid is None) or (need_ask and out_ask is None)):
            start_utc = end_utc - timedelta(seconds=min(window, max_lookback_sec - looked))
            ticks = mt5.copy_ticks_range(symbol, start_utc, end_utc, mt5.COPY_TICKS_ALL)
            if ticks is not None and len(ticks) > 0:
                # Scan backwards for last updates
                for t in reversed(ticks):
                    # mt5 returns numpy structured array; handle by attribute names if present
                    try:
                        flags = int(t['flags'])
                        bid_v = float(t['bid'])
                        ask_v = float(t['ask'])
                    except Exception:
                        try:
                            # Fallback for tuple-like
                            flags = int(t[6]) if len(t) > 6 else 0
                            bid_v = float(t[1])
                            ask_v = float(t[2])
                        except Exception:
                            continue
                    if need_bid and out_bid is None:
                        has = (flags & getattr(mt5, 'TICK_FLAG_BID', 0)) != 0
                        if has or bid_v != 0.0:
                            out_bid = bid_v
                    if need_ask and out_ask is None:
                        has = (flags & getattr(mt5, 'TICK_FLAG_ASK', 0)) != 0
                        if has or ask_v != 0.0:
                            out_ask = ask_v
                    if ((not need_bid) or out_bid is not None) and ((not need_ask) or out_ask is not None):
                        break
            # expand window backwards
            end_utc = start_utc
            looked += window
        return (out_bid if need_bid else None, out_ask if need_ask else None)
    except Exception:
        return (None, None)



def _mt5_copy_rates_cached(symbol: str, timeframe: int, count: int) -> Any:
    """Fetch MT5 rates with a short TTL-based cache to limit IPC overhead."""
    key = (symbol, timeframe, count)
    ttl = float(_RATE_TTL_SECONDS.get(timeframe, 5.0))
    now = time.time()
    cached = _RATE_CACHE.get(key)
    if cached is not None and (now - cached[0]) <= ttl:
        return cached[1]
    try:
        rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, count)
    except Exception:
        rates = None
    if rates is not None:
        _RATE_CACHE[key] = (now, rates)
    else:
        _RATE_CACHE.pop(key, None)
    return rates

def _get_tick_volume_last_2_bars(symbol: str) -> Optional[bool]:
    """Check tick volume for each of the last 2 completed minutes (M1 bars).

    For each minute in the last 2 minutes (excluding the current open minute),
    ensures the M1 bar exists and has tick_volume >= 10. Missing bars are treated
    as zero volume and fail the check.

    Returns:
        True  -> all 2 completed minutes have tick_volume >= 10
        False -> any minute missing or tick_volume < 10
        None  -> MT5 not available/initialized
    """
    if not _mt5_ensure_init():
        return None
    try:
        now_ts = int(time.time())
        this_minute_start = (now_ts // 60) * 60
        # Minutes to check: t-60, t-120
        target_opens = [this_minute_start - i * 60 for i in range(1, 3)]

        # Fetch bars covering exactly that window
        dt_from = datetime.fromtimestamp(target_opens[-1], tz=UTC)
        dt_to = datetime.fromtimestamp(this_minute_start, tz=UTC)
        try:
            rates = mt5.copy_rates_range(symbol, mt5.TIMEFRAME_M1, dt_from, dt_to)
        except Exception:
            rates = None

        if rates is None or len(rates) == 0:
            return False

        # Map open time -> tick_volume using numpy for speed
        try:
            times = rates['time'].astype(int)
            vols = rates['tick_volume'].astype(int)
            vol_by_time = dict(zip(times, vols))
        except Exception:
            # Fallback to loop
            vol_by_time: Dict[int, int] = {}
            for r in rates:
                try:
                    t_open = int(r['time'])
                except Exception:
                    try:
                        t_open = int(r[0])
                    except Exception:
                        continue
                try:
                    vol = int(r['tick_volume'])
                except Exception:
                    try:
                        vol = int(r[5])
                    except Exception:
                        continue
                vol_by_time[t_open] = vol

        # Verify each minute
        for t_open in target_opens:
            vol = vol_by_time.get(t_open)
            if vol is None or vol < 10:
                return False
        return True
    except Exception:
        return None

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
        # If the value looks like a tiny fraction (< 0.01), treat it as fraction-of-price
        # and convert to percent units by multiplying by 100. Otherwise assume it's
        # already expressed in percent units.
        if abs(pct) < 0.01:
            return pct * 100.0
        return pct
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
    if len(values) < period + 1:
        return None
    vals = np.array(values)  # shape (n, 3): high, low, close
    highs = vals[1:period+1, 0]
    lows = vals[1:period+1, 1]
    closes = vals[1:period+1, 2]
    prev_closes = vals[0:period, 2]
    tr1 = highs - lows
    tr2 = np.abs(highs - prev_closes)
    tr3 = np.abs(prev_closes - lows)
    trs = np.maximum.reduce([tr1, tr2, tr3])
    return np.mean(trs)

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
    if not _mt5_ensure_init():
        print('[MT5] initialize() failed; cannot read symbols.')
        return {}, None, None
    series: Dict[str, List[Snapshot]] = {}
    now_utc = datetime.now(UTC)
    latest_ts = now_utc.astimezone(INPUT_TZ)
    for sym in symbols:
        try:
            mt5.symbol_select(sym, True)
        except Exception:
            pass
        tick = None
        try:
            tick = mt5.symbol_info_tick(sym)
        except Exception:
            tick = None
        bid: Optional[float] = None
        ask: Optional[float] = None
        tick_time_utc: Optional[datetime] = None
        tick_age: Optional[float] = None
        has_recent_tick = False
        used_backfill = False

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

        if not has_recent_tick:
            cached_tick = _LAST_TICK_CACHE.get(sym)
            if cached_tick is not None:
                _, _, cached_ts = cached_tick
                if cached_ts is not None:
                    try:
                        cache_age = abs((now_utc - cached_ts).total_seconds())
                        if cache_age <= TICK_FRESHNESS_SEC:
                            has_recent_tick = True
                    except Exception:
                        pass

        # Ensure Bid/Ask represent the latest available tick values.
        # Previous behavior only backfilled when a side was None, which could
        # leave us with stale prices if symbol_info_tick had both sides but an
        # old or missing timestamp. To honor the "previous second" intent,
        # refresh from tick history when the tick is stale or un-timestamped.
        STALE_TICK_THRESHOLD_SEC = 2.0
        needs_refresh = (
            (bid is None or ask is None)
            or (tick_age is None)  # missing/unknown tick time
            or (tick_age is not None and tick_age > STALE_TICK_THRESHOLD_SEC)
        )
        if needs_refresh:
            used_backfill = True
            mt5_bid, mt5_ask = _mt5_backfill_bid_ask(
                sym,
                now_utc,
                need_bid=True,
                need_ask=True,
            )
            if mt5_bid is not None:
                bid = mt5_bid
            if mt5_ask is not None:
                ask = mt5_ask
            # Try to also refresh tick_time_utc using the last tick in a short window
            # so downstream freshness checks reflect what we used.
            try:
                ticks_recent = mt5.copy_ticks_range(
                    sym,
                    now_utc - timedelta(seconds=5),
                    now_utc,
                    mt5.COPY_TICKS_ALL,
                )
                if ticks_recent is not None and len(ticks_recent) > 0:
                    lt = ticks_recent[-1]
                    try:
                        # numpy structured array style
                        tmsc = None
                        try:
                            tmsc = lt['time_msc']  # type: ignore[index]
                        except Exception:
                            tmsc = getattr(lt, 'time_msc', None)
                        if tmsc:
                            tick_time_utc = datetime.fromtimestamp(float(tmsc) / 1000.0, tz=UTC)
                        else:
                            ts_val = None
                            try:
                                ts_val = lt['time']  # type: ignore[index]
                            except Exception:
                                ts_val = getattr(lt, 'time', None)
                            if ts_val is not None:
                                tick_time_utc = datetime.fromtimestamp(float(ts_val), tz=UTC)
                    except Exception:
                        pass
                    if tick_time_utc is not None:
                        try:
                            tick_age = max(0.0, (now_utc - tick_time_utc).total_seconds())
                        except Exception:
                            pass
            except Exception:
                pass

        if not has_recent_tick:
            try:
                recent = mt5.copy_ticks_range(
                    sym,
                    now_utc - timedelta(seconds=TICK_FRESHNESS_SEC),
                    now_utc,
                    mt5.COPY_TICKS_ALL,
                )
                has_recent_tick = bool(recent is not None and len(recent) > 0)
            except Exception:
                has_recent_tick = False

        if bid is not None or ask is not None:
            cache_ts = tick_time_utc
            if cache_ts is None:
                prev = _LAST_TICK_CACHE.get(sym)
                cache_ts = prev[2] if prev else None
            _LAST_TICK_CACHE[sym] = (bid, ask, cache_ts)

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
            'Backfilled': 1 if used_backfill else 0,
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


def _filter_recent_duplicates(results: List[Dict[str, object]], table: str = "timelapse_setups") -> Tuple[List[Dict[str, object]], Set[str]]:
    """Filter out results for symbols that already have an unsettled (open) setup.

    A setup is considered open if there is no record for its id in timelapse_hits.
    If the hits table is missing, leave results unchanged.

    Returns:
        Tuple of (filtered_results, excluded_symbols)
    """
    if not results or sqlite3 is None:
        return results, set()
    db_path = DEFAULT_SQLITE_PATH
    try:
        conn = sqlite3.connect(db_path, timeout=3)
    except Exception:
        return results, set()
    try:
        cur = conn.cursor()
        # Ensure setups table exists
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
        if cur.fetchone() is None:
            return results, set()
        # Check hits table presence
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", ("timelapse_hits",))
        if cur.fetchone() is None:
            return results, set()
        # Compute symbols that currently have at least one open setup
        cur.execute(
            f"""
            SELECT DISTINCT t.symbol
            FROM {table} t
            LEFT JOIN timelapse_hits h ON h.setup_id = t.id
            WHERE h.setup_id IS NULL
            """
        )
        open_symbols = {str(r[0]) for r in (cur.fetchall() or []) if r and r[0] is not None}
        if not open_symbols:
            return results, set()
        filtered = [r for r in results if str(r.get("symbol")) not in open_symbols]
        excluded = {str(r.get("symbol")) for r in results if str(r.get("symbol")) in open_symbols}
        return filtered, excluded
    except Exception:
        return results, set()
    finally:
        try:
            conn.close()
        except Exception:
            pass




def process_once(
    symbols: List[str],
    min_rrr: float,
    min_prox_sl: float,
    min_sl_pct: float,
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
        min_prox_sl=min_prox_sl,
        min_sl_pct=min_sl_pct,
        as_of_ts=as_of_ts,
        debug=debug,
    )
    # No banner output in quiet mode
    if debug and mkttvc:
        print(f"#MKTTVC (total market volume change): {mkttvc}")

    # Suppress symbols that have an open (unsettled) setup in the DB already
    filtered, db_excluded = _filter_recent_duplicates(results, table="timelapse_setups")
    # Track DB exclusions in rejection reasons
    if db_excluded: # Only add if there are excluded symbols
        reasons.setdefault("already_in_db", []).extend(list(db_excluded)) # Extend with the list of excluded symbols

    # Apply top limit after filtering
    top_results = filtered[: top] if top else filtered

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
                print(f"  -> {r['explain']}")

    # Insert results into SQLite
    try:
        insert_results_to_db(top_results, table="timelapse_setups", detected_at=detected_at)
    except Exception as e:
        # Keep non-fatal; just report the issue
        print(f"[DB] Skipped insert due to error: {e}")


def watch_loop(
    symbols: List[str],
    interval: float,
    min_rrr: float,
    min_prox_sl: float,
    min_sl_pct: float,
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
            time.sleep(interval)
            detected_at = datetime.now(UTC)
            process_once(
                symbols,
                min_rrr=min_rrr,
                min_prox_sl=min_prox_sl,
                min_sl_pct=min_sl_pct,
                top=top,
                brief=brief,
                debug=debug,
                exclude_set=exclude_set,
                detected_at=detected_at,
            )
    except KeyboardInterrupt:
        if debug:
            print("\nStopped watching.")


def _watch_loop_events(
    symbols: List[str],
    min_rrr: float,
    min_prox_sl: float,
    min_sl_pct: float,
    top: Optional[int],
    brief: bool,
    debug: bool = False,
    exclude_set: Optional[Set[str]] = None,
    settle_delay: float = 0.2,
) -> None:
    """Poll MT5 periodically (watchdog not used)."""
    watch_loop(
        symbols=symbols,
        interval=1.0,
        min_rrr=min_rrr,
        min_prox_sl=min_prox_sl,
        min_sl_pct=min_sl_pct,
        top=top,
        brief=brief,
        debug=debug,
        exclude_set=exclude_set,
    )
    return


def analyze(
    series: Dict[str, List[Snapshot]],
    min_rrr: float,
    min_prox_sl: float,
    min_sl_pct: float,
    as_of_ts: Optional[datetime],
    debug: bool = False,
) -> Tuple[List[Dict[str, object]], Dict[str, List[str]]]:
    reasons: Dict[str, List[str]] = {}
    def bump(key: str) -> None:
        reasons.setdefault(key, []).append(sym)
    results: List[Dict[str, object]] = []
    for sym, snaps in series.items():
        if not snaps:
            continue
        snaps.sort(key=lambda s: s.ts)
        first, last = snaps[0], snaps[-1]

        # Filter out entries between 23:00 and 01:00 (UTC+3).
        # Intention: block 23:00–00:59 inclusive, allow from 01:00 onward.
        last_ts_utc3 = last.ts.astimezone(UTC3)
        hour = last_ts_utc3.hour
        minute = last_ts_utc3.minute
        # Block hours 23 and 0; do not block entire hour 1
        if hour == 23 or hour == 0:
            if debug:
                try:
                    print(f"[DEBUG] low_vol_time_window at {last_ts_utc3.strftime('%Y-%m-%d %H:%M:%S')} UTC+3; gating 23:00–00:59")
                except Exception:
                    pass
            bump("low_vol_time_window")
            continue

        # Extract metrics from latest
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
        backfilled_flag = 0
        try:
            val_bf = last.row.get(canonicalize_key('Backfilled'))
            if isinstance(val_bf, str):
                val_bf = val_bf.strip()
                if val_bf.isdigit():
                    backfilled_flag = int(val_bf)
            elif isinstance(val_bf, (int, float)):
                backfilled_flag = int(val_bf)
        except Exception:
            backfilled_flag = 0

        # Try MT5 tick backfill first if Bid/Ask missing at the latest snapshot
        if bid is None or ask is None:
            # Reference timestamp: prefer global latest (as_of_ts) if available
            ref_ts = as_of_ts or last.ts
            # Convert to UTC aware for MT5
            if ref_ts.tzinfo is None:
                ref_utc = ref_ts.replace(tzinfo=INPUT_TZ).astimezone(UTC)
            else:
                ref_utc = ref_ts.astimezone(UTC)
            need_bid = bid is None
            need_ask = ask is None
            mt5_bid, mt5_ask = _mt5_backfill_bid_ask(sym, ref_utc, need_bid=need_bid, need_ask=need_ask)
            if need_bid and mt5_bid is not None:
                bid = mt5_bid
            if need_ask and mt5_ask is not None:
                ask = mt5_ask


        # Drop symbols without recent ticks (avoid closed markets)
        recent_tick_flag = last.g("Recent Tick")
        if recent_tick_flag is None or recent_tick_flag <= 0:
            bump("no_recent_ticks")
            if debug:
                print(f"[DEBUG] no_recent_ticks for {sym}")
            continue

        # Filter out symbols with low tick volume in the last 2 M1 bars
        volume_check_passed = _get_tick_volume_last_2_bars(sym)
        if volume_check_passed is not None and not volume_check_passed:
            bump("low_tick_volume_last_2_bars")
            if debug:
                print(f"[DEBUG] low_tick_volume_last_2_bars for {sym}: insufficient tick volume detected in the last 2 bars")
            continue

        # Timelapse deltas (context)
        d1_close_trend = None if (d1_close is None or first.g("D1 Close") is None) else d1_close - (first.g("D1 Close") or 0.0)
        ss4_trend = None if (ss4 is None or first.g("Strength 4H") is None) else ss4 - (first.g("Strength 4H") or 0.0)

        # Direction from strength across TFs (require 4H alignment when available)
        pos = sum(1 for v in (ss4, ss1d, ss1w) if v is not None and v > 0)
        neg = sum(1 for v in (ss4, ss1d, ss1w) if v is not None and v < 0)
        direction: Optional[str] = None
        if pos >= 2 and (ss4 is None or ss4 > 0):
            direction = "Buy"
        if neg >= 2 and (ss4 is None or ss4 < 0) and direction is None:
            direction = "Sell"
        if direction is None:
            bump("no_direction_consensus")
            continue

        # Entry price uses Bid/Ask at signal timestamp based on direction
        if direction == "Buy":
            price = ask
        else:  # Sell
            price = bid
        # Fallback to Close if still no Bid/Ask available after backfill
        if price is None:
            price = last.g("M15 Close") or last.g("H1 Close") or last.g("D1 Close")

        if price is None:
            bump("no_price")
            continue

        # Spread and ATR(%) handling — prefer computing directly from Bid/Ask
        spreadpct = None
        try:
            if bid is not None and ask is not None and bid > 0 and ask > 0 and ask > bid:
                mid = (ask + bid) / 2.0
                spreadpct = ((ask - bid) / mid) * 100.0
        except Exception:
            spreadpct = None
        if spreadpct is None:
            spreadpct = spreadpct_row
        spr_class = spread_class(spreadpct)
        if spr_class == "Avoid":
            bump("spread_avoid")
            if debug:
                print(f"[DEBUG] spread_avoid {sym}: bid={bid} ask={ask} spreadpct={spreadpct}")
            continue
        # ATR% score bonus only when value is present and within [60, 150]
        atrp_in_range = (atrp is not None) and (60.0 <= atrp <= 150.0)

        # S/R based SL/TP with D1 fallback, using Close as entry for orientation and RRR
        # Initialize proximity flags
        prox_note = None
        prox_flag = None
        prox_late = False
        if direction == "Buy":
            sl = s1 if s1 is not None else d1l
            tp = r1 if r1 is not None else d1h
            # Basic sanity for S/R orientation
            if sl is None or tp is None:
                bump("missing_sl_tp")
                continue
            if not (sl <= price <= tp):
                bump("price_outside_buy_sr")
                continue
            risk = price - sl
            reward = tp - price
            rrr = reward / risk if risk > 0 else None
            if (tp is not None and sl is not None) and (tp - sl) != 0:
                prox = (price - sl) / (tp - sl)
                # Gate: require minimum distance from SL as fraction of range
                try:
                    thr = max(0.0, min(0.49, float(min_prox_sl)))
                except Exception:
                    thr = 0.0
                if prox < thr:
                    bump("too_close_to_sl_prox")
                    continue
                if prox <= 0.35:
                    prox_flag = "near_support"
                    prox_note = "near S1 support"
                elif prox >= 0.65:
                    prox_flag = "near_resistance"
                    prox_late = True
                    prox_note = "near R1 resistance (late)"
        else:  # Sell
            sl = r1 if r1 is not None else d1h
            tp = s1 if s1 is not None else d1l
            if sl is None or tp is None:
                bump("missing_sl_tp")
                continue
            if not (tp <= price <= sl):
                bump("price_outside_sell_sr")
                continue
            risk = sl - price
            reward = price - tp
            rrr = reward / risk if risk > 0 else None
            if (sl is not None and tp is not None) and (sl - tp) != 0:
                prox = (sl - price) / (sl - tp)
                # Gate: require minimum distance from SL as fraction of range
                try:
                    thr = max(0.0, min(0.49, float(min_prox_sl)))
                except Exception:
                    thr = 0.0
                if prox < thr:
                    bump("too_close_to_sl_prox")
                    continue
                if prox <= 0.35:
                    prox_flag = "near_resistance"
                    prox_note = "near R1 resistance"
                elif prox >= 0.65:
                    prox_flag = "near_support"
                    prox_late = True
                    prox_note = "near S1 support (late)"

        # Spread-based SL distance filter: SL must be at least 10x spread away from the side
        # that actually triggers the stop:
        #   - Buy: stop is hit on Bid -> use (bid - SL)
        #   - Sell: stop is hit on Ask -> use (SL - ask)
        # If the preferred side is missing, fall back to entry `price` to avoid false negatives.
        # Also support deriving absolute spread from `spreadpct` when bid/ask are missing.
        eps = 1e-12
        if sl is not None:
            spread_abs: Optional[float] = None
            # Prefer absolute spread from ask-bid when available
            if bid is not None and ask is not None and ask > bid:
                spread_abs = ask - bid
            elif spreadpct is not None and price is not None:
                try:
                    # spreadpct is in percent units (e.g., 0.12 == 0.12%)
                    spread_abs = (spreadpct / 100.0) * abs(price)
                except Exception:
                    spread_abs = None
            # If we have a usable spread, evaluate distance
            if spread_abs is not None and spread_abs > 0:
                distance: Optional[float] = None
                # Prefer distance measured from the correct side that triggers the stop
                if direction == "Buy":
                    if bid is not None:
                        distance = bid - sl
                    elif price is not None:
                        # Fallback to entry price (typically Ask); this is lenient but avoids None
                        distance = price - sl
                else:  # Sell
                    if ask is not None:
                        distance = sl - ask
                    elif price is not None:
                        # Fallback to entry price (typically Bid)
                        distance = sl - price
                # Enforce minimum distance threshold (10x spread) with tiny epsilon
                if distance is None or (distance + eps) < (10 * spread_abs):
                    bump("sl_too_close_to_spread")
                    if debug:
                        print(f"[DEBUG] SL too close to spread: sym={sym}, dir={direction}, price={price}, sl={sl}, spread_abs={spread_abs}, distance={distance}")
                    continue

        # Additional SL distance gate in percent of price (works even without Bid/Ask)
        if min_sl_pct and price is not None and sl is not None:
            try:
                dist = (price - sl) if direction == "Buy" else (sl - price)
                if dist <= 0:
                    bump("sl_distance_nonpositive")
                    continue
                pct = (dist / abs(price)) * 100.0
                if pct < float(min_sl_pct):
                    bump("sl_distance_pct_too_small")
                    if debug:
                        print(f"[DEBUG] SL pct too small: sym={sym}, dir={direction}, price={price}, sl={sl}, pct={pct:.5f} < {min_sl_pct}")
                    continue
            except Exception:
                pass

        if rrr is None or rrr < min_rrr:
            bump("rrr_too_low")
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
        if ss4 is not None and ss1d is not None and ss1w is not None:
            parts.append(f"Strength 4H/1D/1W: {ss4:.1f}/{ss1d:.1f}/{ss1w:.1f}")
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
        results.append(
            {
                "symbol": sym,
                "direction": direction,
                "price": price,
                "sl": sl,
                "tp": tp,
                "rrr": rrr,
                "score": score,
                "explain": "; ".join(parts),
                "as_of": as_of_value,
                # Meta for logging; not used for DB schema
                "bid": bid,
                "ask": ask,
                "tick_utc": tick_time_str,
                "source": ("backfill" if backfilled_flag else "normal"),
            }
        )

    # Order by score then RRR
    results.sort(key=lambda x: (-x["score"], -x["rrr"]))
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
                explain TEXT,
                as_of TEXT NOT NULL,
                detected_at TEXT,
                inserted_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
                UNIQUE(symbol, direction, as_of)
            )
            """
        )
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
            # Try to migrate by adding detected_at if missing
            if not has_detected:
                try:
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN detected_at TEXT")
                    has_detected = True
                except Exception:
                    has_detected = False

            if has_detected:
                ins = (
                    f"""
                    INSERT INTO {table}
                        (symbol, direction, price, sl, tp, rrr, score, explain, as_of, detected_at)
                    SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {table} t2
                        LEFT JOIN timelapse_hits h ON h.setup_id = t2.id
                        WHERE t2.symbol = ?
                          AND h.setup_id IS NULL
                    )
                    ON CONFLICT(symbol, direction, as_of) DO NOTHING
                    """
                )
            else:
                ins = (
                    f"""
                    INSERT INTO {table}
                        (symbol, direction, price, sl, tp, rrr, score, explain, as_of)
                    SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {table} t2
                        LEFT JOIN timelapse_hits h ON h.setup_id = t2.id
                        WHERE t2.symbol = ?
                          AND h.setup_id IS NULL
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
                if has_detected:
                    params.append(
                        (
                            sym_val,
                            r.get("direction"),
                            r.get("price"),
                            r.get("sl"),
                            r.get("tp"),
                            r.get("rrr"),
                            r.get("score"),
                            r.get("explain"),
                            as_of_val,
                            detected_at_val,
                            sym_val,
                        )
                    )
                else:
                    params.append(
                        (
                            sym_val,
                            r.get("direction"),
                            r.get("price"),
                            r.get("sl"),
                            r.get("tp"),
                            r.get("rrr"),
                            r.get("score"),
                            r.get("explain"),
                            as_of_val,
                            sym_val,
                        )
                    )

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
                    print(f"[DB] Inserted {inserted} new setup(s): " + "; ".join(lines))
                else:
                    # Fallback to symbol list only
                    syms = [str(r.get('symbol')) for r in (results or [])]
                    uniq = sorted({s for s in syms if s})
                    print(f"[DB] Inserted {inserted} new setup(s): {', '.join(uniq)}")
            except Exception:
                print(f"[DB] Inserted {inserted} new setup(s)")


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
                    infos = mt5.symbols_get()
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
        if HAS_WATCHDOG:
            _watch_loop_events(
                symbols=syms,
                min_rrr=args.min_rrr,
                min_prox_sl=max(0.0, min(0.49, args.min_prox_sl)),
                min_sl_pct=max(0.0, args.min_sl_pct),
                top=args.top,
                brief=args.brief,
                debug=args.debug,
                exclude_set=exclude_set,
            )
        else:
            print("[watch] watchdog not installed; falling back to polling. Run 'pip install watchdog' for real-time events.")
            watch_loop(
                symbols=syms,
                interval=max(0.5, args.interval),
                min_rrr=args.min_rrr,
                min_prox_sl=max(0.0, min(0.49, args.min_prox_sl)),
                min_sl_pct=max(0.0, args.min_sl_pct),
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
        min_prox_sl=max(0.0, min(0.49, args.min_prox_sl)),
        min_sl_pct=max(0.0, args.min_sl_pct),
        top=args.top,
        brief=args.brief,
        debug=args.debug,
        exclude_set=exclude_set,
    )


if __name__ == "__main__":
    main()
