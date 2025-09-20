#!/usr/bin/env python3
"""
Backtest timelapse strategy on a single symbol from MetaTrader CSV ticks (UTC+3).

Input CSV columns: DATE, TIME, BID, ASK, LAST, VOLUME, FLAGS
Times in CSV are server-local UTC+3 by default; we convert to UTC internally.

The analysis mirrors timelapse_setups.py:
 - Direction via Strength 4H/1D/1W consensus (percent close change of last two completed bars)
 - SL/TP via S1/R1 from previous day's pivots, with D1 High/Low fallback (up to now)
 - Spread class gating and 10x-spread SL distance rule
 - Time filter: exclude 23:00–00:59 UTC+3
 - Volume check: require last 2 completed M1 bars to have tick_volume >= 10
Entry: Buy uses Ask, Sell uses Bid.
Backtest: Walk forward tick-by-tick to earliest TP/SL using the correct side (Bid for Buy, Ask for Sell).
"""
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple, Dict
import os
import math

UTC = timezone.utc

def tz_from_offset(hours: int) -> timezone:
    return timezone(timedelta(hours=hours))

@dataclass
class Tick:
    t_utc: datetime
    t_utc3: datetime
    bid: Optional[float]
    ask: Optional[float]

def parse_float(s: str) -> Optional[float]:
    try:
        s = (s or "").strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None

def parse_csv_ticks(path: str, utc3: timezone) -> List[Tick]:
    ticks: List[Tick] = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        # Fallback to default CSV if no header was detected
        if reader.fieldnames is None or reader.fieldnames == [None]:
            f.seek(0)
            reader = csv.DictReader(f)
        for row in reader:
            date_s = row.get("DATE") or row.get("<DATE>") or row.get("Date") or ""
            time_s = row.get("TIME") or row.get("<TIME>") or row.get("Time") or ""
            bid_s = row.get("BID") or row.get("<BID>") or row.get("Bid") or ""
            ask_s = row.get("ASK") or row.get("<ASK>") or row.get("Ask") or ""
            if not date_s or not time_s:
                continue
            try:
                if "." in time_s:
                    dt3 = datetime.strptime(f"{date_s} {time_s}", "%Y.%m.%d %H:%M:%S.%f").replace(tzinfo=utc3)
                else:
                    dt3 = datetime.strptime(f"{date_s} {time_s}", "%Y.%m.%d %H:%M:%S").replace(tzinfo=utc3)
            except Exception:
                try:
                    if "." in time_s:
                        dt3 = datetime.strptime(f"{date_s} {time_s}", "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=utc3)
                    else:
                        dt3 = datetime.strptime(f"{date_s} {time_s}", "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc3)
                except Exception:
                    continue
            dt_utc = dt3.astimezone(UTC)
            bid = parse_float(bid_s)
            ask = parse_float(ask_s)
            ticks.append(Tick(t_utc=dt_utc, t_utc3=dt3, bid=bid, ask=ask))
    ticks.sort(key=lambda x: x.t_utc)
    return ticks

@dataclass
class M1Bar:
    start_utc: datetime
    start_utc3: datetime
    open: float
    high: float
    low: float
    close: float
    tick_volume: int
    last_tick_index: int

def mid_price(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    if bid is not None:
        return bid
    if ask is not None:
        return ask
    return None

def group_m1(ticks: List[Tick]) -> Tuple[List[M1Bar], Dict[datetime,int]]:
    """Return list of M1 bars and map minute_start_utc -> last_tick_index."""
    bars: List[M1Bar] = []
    last_index_by_minute: Dict[datetime,int] = {}
    if not ticks:
        return bars, last_index_by_minute
    i = 0
    n = len(ticks)
    while i < n:
        tk = ticks[i]
        mstart_utc = tk.t_utc.replace(second=0, microsecond=0)
        mstart_utc3 = tk.t_utc3.replace(second=0, microsecond=0)
        j = i
        opens: Optional[float] = None
        high = -math.inf
        low = math.inf
        close: Optional[float] = None
        vol = 0
        last_idx = i
        while j < n:
            tkj = ticks[j]
            if tkj.t_utc.replace(second=0, microsecond=0) != mstart_utc:
                break
            m = mid_price(tkj.bid, tkj.ask)
            if m is not None:
                if opens is None:
                    opens = m
                high = max(high, m)
                low = min(low, m)
                close = m
            vol += 1
            last_idx = j
            j += 1
        if opens is not None and close is not None and low != math.inf and high != -math.inf:
            bars.append(M1Bar(
                start_utc=mstart_utc,
                start_utc3=mstart_utc3,
                open=opens,
                high=high,
                low=low,
                close=close,
                tick_volume=vol,
                last_tick_index=last_idx
            ))
            last_index_by_minute[mstart_utc] = last_idx
        i = j
    return bars, last_index_by_minute

@dataclass
class TFBar:
    start_utc3: datetime
    open: float
    high: float
    low: float
    close: float

def aggregate_bars_mins(m1: List[M1Bar], minutes: int) -> List[TFBar]:
    if not m1:
        return []
    out: List[TFBar] = []
    cur_key: Optional[datetime] = None
    cur_open = cur_high = cur_low = cur_close = None
    for b in m1:
        # timeframe start in UTC+3 clock
        if minutes >= 60:
            total = b.start_utc3.hour * 60 + b.start_utc3.minute
            kmin = (total // minutes) * minutes
            hour = kmin // 60
            minute = kmin % 60
            key = b.start_utc3.replace(hour=hour, minute=minute, second=0, microsecond=0)
        else:
            kmin = (b.start_utc3.minute // minutes) * minutes
            key = b.start_utc3.replace(minute=kmin, second=0, microsecond=0)
        if (cur_key is None) or (key != cur_key):
            if cur_key is not None:
                out.append(TFBar(start_utc3=cur_key, open=cur_open, high=cur_high, low=cur_low, close=cur_close))  # type: ignore
            cur_key = key
            cur_open = b.open
            cur_high = b.high
            cur_low = b.low
            cur_close = b.close
        else:
            cur_high = max(cur_high, b.high)  # type: ignore
            cur_low = min(cur_low, b.low)  # type: ignore
            cur_close = b.close  # type: ignore
    if cur_key is not None:
        out.append(TFBar(start_utc3=cur_key, open=cur_open, high=cur_high, low=cur_low, close=cur_close))  # type: ignore
    return out

def aggregate_daily(m1: List[M1Bar]) -> List[TFBar]:
    if not m1:
        return []
    out: List[TFBar] = []
    cur_day: Optional[datetime.date] = None
    cur_open = cur_high = cur_low = cur_close = None
    cur_day_start: Optional[datetime] = None
    for b in m1:
        d = b.start_utc3.date()
        if cur_day is None or d != cur_day:
            if cur_day is not None:
                out.append(TFBar(start_utc3=cur_day_start, open=cur_open, high=cur_high, low=cur_low, close=cur_close))  # type: ignore
            cur_day = d
            cur_day_start = datetime(b.start_utc3.year, b.start_utc3.month, b.start_utc3.day, tzinfo=b.start_utc3.tzinfo)
            cur_open = b.open
            cur_high = b.high
            cur_low = b.low
            cur_close = b.close
        else:
            cur_high = max(cur_high, b.high)  # type: ignore
            cur_low = min(cur_low, b.low)  # type: ignore
            cur_close = b.close  # type: ignore
    if cur_day is not None:
        out.append(TFBar(start_utc3=cur_day_start, open=cur_open, high=cur_high, low=cur_low, close=cur_close))  # type: ignore
    return out

def percent_change(last_two: List[TFBar]) -> Optional[float]:
    if len(last_two) < 2:
        return None
    prev = last_two[-2].close
    last = last_two[-1].close
    if prev == 0:
        return None
    try:
        return (last - prev) / prev * 100.0
    except Exception:
        return None

def pivots_from_prev_day(daily: List[TFBar], as_of_local_date: datetime.date) -> Tuple[Optional[float], Optional[float]]:
    prev_idx = None
    for i, dbar in enumerate(daily):
        if dbar.start_utc3.date() == as_of_local_date:
            prev_idx = i - 1
            break
    if prev_idx is None:
        if len(daily) >= 2:
            prev_idx = len(daily) - 2
        else:
            return None, None
    if prev_idx < 0 or prev_idx >= len(daily):
        return None, None
    prev = daily[prev_idx]
    p = (prev.high + prev.low + prev.close) / 3.0
    s1 = 2 * p - prev.high
    r1 = 2 * p - prev.low
    return s1, r1

def spread_class(pct: Optional[float]) -> str:
    if pct is None:
        return "Unknown"
    if pct < 0.10:
        return "Excellent"
    if pct < 0.20:
        return "Good"
    if pct < 0.30:
        return "Acceptable"
    return "Avoid"

def compute_strengths_at(m1: List[M1Bar], i: int, h1: List[TFBar], h4: List[TFBar], d1: List[TFBar], w1: List[TFBar]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Return (ss_4h, ss_1d, ss_1w) at minute index i using last two completed bars before or at that minute."""
    t_local = m1[i].start_utc3
    def completed(bars: List[TFBar]) -> List[TFBar]:
        return [b for b in bars if b.start_utc3 <= t_local]
    ss4 = percent_change(completed(h4))
    ss1d = percent_change(completed(d1))
    ss1w = percent_change(completed(w1))
    return ss4, ss1d, ss1w

def compute_day_high_low_up_to(m1: List[M1Bar], i: int) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Return (d1_close, d1_high, d1_low) for current local day using bars up to index i inclusive."""
    t_local = m1[i].start_utc3
    day = t_local.date()
    highs: List[float] = []
    lows: List[float] = []
    close: Optional[float] = None
    for b in m1:
        if b.start_utc3.date() != day:
            continue
        if b.start_utc3 > t_local:
            break
        highs.append(b.high)
        lows.append(b.low)
        close = b.close
    if close is None or not highs or not lows:
        return None, None, None
    return close, max(highs), min(lows)

def atr_d1_percent(daily: List[TFBar], i_local_time: datetime) -> Tuple[Optional[float], Optional[float]]:
    """Compute ATR(14) (absolute and %) using last 15 completed daily bars ending before i_local_time."""
    bars = [b for b in daily if b.start_utc3 < i_local_time]
    if len(bars) < 15:
        return None, None
    highs = [b.high for b in bars[-15:]]
    lows = [b.low for b in bars[-15:]]
    closes = [b.close for b in bars[-15:]]
    trs: List[float] = []
    for k in range(1, 15):
        h = highs[k]
        l = lows[k]
        pc = closes[k-1]
        tr = max(h-l, abs(h-pc), abs(pc-l))
        trs.append(tr)
    atr = sum(trs) / len(trs)
    last_close = closes[-1]
    if last_close == 0:
        return atr, None
    return atr, (atr/last_close)*100.0

def r_digits_from_price(price: Optional[float]) -> int:
    try:
        if price is None:
            return 5
        s = f"{float(price):.10f}".rstrip("0").rstrip(".")
        if "." in s:
            return max(0, min(10, len(s.split('.')[1])))
        return 0
    except Exception:
        return 5

def round_like_price(v: Optional[float], price: Optional[float]) -> Optional[float]:
    try:
        if v is None:
            return None
        d = r_digits_from_price(price)
        return round(float(v), d)
    except Exception:
        return v

@dataclass
class SetupResult:
    symbol: str
    direction: str
    entry_time_utc: datetime
    entry_time_utc3: datetime
    price: float
    sl: float
    tp: float
    rrr: float
    score: float
    spread_pct: Optional[float]
    spread_class: str
    explain: str
    hit: Optional[str]
    hit_time_utc: Optional[datetime]
    hit_time_utc3: Optional[datetime]
    hit_price: Optional[float]

def earliest_hit_from_ticks(ticks: List[Tick], start_index: int, direction: str, sl: float, tp: float) -> Tuple[Optional[str], Optional[int], Optional[float]]:
    """Scan ticks strictly after start_index for earliest TP/SL. Returns (kind, hit_index, hit_price)."""
    n = len(ticks)
    j = start_index + 1
    while j < n:
        tk = ticks[j]
        bid = tk.bid if tk.bid is not None else None
        ask = tk.ask if tk.ask is not None else None
        if direction.lower() == "buy":
            if bid is not None and bid <= sl:
                return "SL", j, bid
            if bid is not None and bid >= tp:
                return "TP", j, bid
        else:
            if ask is not None and ask >= sl:
                return "SL", j, ask
            if ask is not None and ask <= tp:
                return "TP", j, ask
        j += 1
    return None, None, None

def analyze_and_backtest(csv_path: str, symbol: str, utc3_offset: int, min_rrr: float, min_prox_sl: float, min_sl_pct: float, allow_overlap: bool) -> List[SetupResult]:
    tz3 = tz_from_offset(utc3_offset)
    ticks = parse_csv_ticks(csv_path, tz3)
    if not ticks:
        return []
    m1, last_idx = group_m1(ticks)
    if not m1:
        return []
    h1 = aggregate_bars_mins(m1, 60)
    h4 = aggregate_bars_mins(m1, 240)
    d1 = aggregate_daily(m1)
    w1: List[TFBar] = []
    if d1:
        cur_week = None
        cur_open = cur_high = cur_low = cur_close = None
        cur_key = None
        for b in d1:
            iso = b.start_utc3.isocalendar()
            wk = (iso[0], iso[1])
            if cur_week is None or wk != cur_week:
                if cur_week is not None:
                    w1.append(TFBar(start_utc3=cur_key, open=cur_open, high=cur_high, low=cur_low, close=cur_close))  # type: ignore
                cur_week = wk
                cur_key = b.start_utc3
                cur_open = b.open
                cur_high = b.high
                cur_low = b.low
                cur_close = b.close
            else:
                cur_high = max(cur_high, b.high)  # type: ignore
                cur_low = min(cur_low, b.low)  # type: ignore
                cur_close = b.close  # type: ignore
        if cur_week is not None:
            w1.append(TFBar(start_utc3=cur_key, open=cur_open, high=cur_high, low=cur_low, close=cur_close))  # type: ignore
    results: List[SetupResult] = []
    open_until_index: Optional[int] = None
    for i, bar in enumerate(m1):
        if i < 2:
            continue
        if (not allow_overlap) and open_until_index is not None and last_idx[bar.start_utc] <= open_until_index:
            continue
        h = bar.start_utc3.hour
        if h == 23 or h == 0:
            continue
        if m1[i-1].tick_volume < 10 or m1[i-2].tick_volume < 10:
            continue
        entry_tick_index = last_idx[bar.start_utc]
        entry_tick = ticks[entry_tick_index]
        bid = entry_tick.bid
        ask = entry_tick.ask
        spread_pct: Optional[float] = None
        if bid is not None and ask is not None and bid > 0 and ask > 0 and ask > bid:
            mid = (bid + ask) / 2.0
            spread_pct = (ask - bid) / mid * 100.0
        sclass = spread_class(spread_pct)
        if sclass == "Avoid":
            continue
        ss4, ss1d, ss1w = compute_strengths_at(m1, i, h1, h4, d1, w1)
        pos = sum(1 for v in (ss4, ss1d, ss1w) if v is not None and v > 0)
        neg = sum(1 for v in (ss4, ss1d, ss1w) if v is not None and v < 0)
        direction: Optional[str] = None
        if pos >= 2 and (ss4 is None or ss4 > 0):
            direction = "Buy"
        if direction is None and neg >= 2 and (ss4 is None or ss4 < 0):
            direction = "Sell"
        if direction is None:
            continue
        d1_close, d1_high, d1_low = compute_day_high_low_up_to(m1, i)
        s1, r1 = pivots_from_prev_day(d1, bar.start_utc3.date())
        if direction == "Buy":
            price = ask
            sl = s1 if s1 is not None else d1_low
            tp = r1 if r1 is not None else d1_high
        else:
            price = bid
            sl = r1 if r1 is not None else d1_high
            tp = s1 if s1 is not None else d1_low
        if price is None or sl is None or tp is None:
            continue
        if direction == "Buy":
            if not (sl <= price <= tp):
                continue
            risk = price - sl
            reward = tp - price
        else:
            if not (tp <= price <= sl):
                continue
            risk = sl - price
            reward = price - tp
        if risk <= 0:
            continue
        rrr = reward / risk
        prox_flag = None
        prox_late = False
        if (tp - sl) != 0:
            if direction == "Buy":
                prox = (price - sl) / (tp - sl)
                thr = max(0.0, min(0.49, float(min_prox_sl)))
                if prox < thr:
                    continue
                if prox <= 0.35:
                    prox_flag = "near_support"
                elif prox >= 0.65:
                    prox_flag = "near_resistance"
                    prox_late = True
            else:
                prox = (sl - price) / (sl - tp)
                thr = max(0.0, min(0.49, float(min_prox_sl)))
                if prox < thr:
                    continue
                if prox <= 0.35:
                    prox_flag = "near_resistance"
                elif prox >= 0.65:
                    prox_flag = "near_support"
                    prox_late = True
        spread_abs: Optional[float] = None
        if bid is not None and ask is not None and ask > bid:
            spread_abs = ask - bid
        elif spread_pct is not None and price is not None:
            spread_abs = (spread_pct / 100.0) * abs(price)
        if spread_abs is not None and spread_abs > 0:
            if direction == "Buy":
                distance = (bid if bid is not None else price) - sl
            else:
                distance = sl - (ask if ask is not None else price)
            if distance is None or distance < (10 * spread_abs) - 1e-12:
                continue
        if min_sl_pct and price is not None and sl is not None:
            dist = (price - sl) if direction == "Buy" else (sl - price)
            pct = (dist / abs(price)) * 100.0
            if pct < float(min_sl_pct):
                continue
        if rrr < min_rrr:
            continue
        atr_abs, atrp = atr_d1_percent(d1, bar.start_utc3)
        atrp_in_range = (atrp is not None) and (60.0 <= atrp <= 150.0)
        score = (pos if direction == "Buy" else neg) * 1.5
        if atrp_in_range:
            score += 0.5
        score += {"Excellent": 1.0, "Good": 0.5, "Acceptable": 0.0, "Avoid": -2.0}.get(sclass, 0.0)
        if prox_late:
            score -= 0.4
        parts: List[str] = []
        if ss4 is not None or ss1d is not None or ss1w is not None:
            a = f"{ss4:.1f}" if ss4 is not None else "N/A"
            b = f"{ss1d:.1f}" if ss1d is not None else "N/A"
            c = f"{ss1w:.1f}" if ss1w is not None else "N/A"
            parts.append(f"Strength 4H/1D/1W: {a}/{b}/{c}")
        if atr_abs is not None and atrp is not None:
            parts.append(f"ATR: {atr_abs:.1f} ({atrp:.1f}%)")
        s1s = f"{s1:.6f}" if s1 is not None else "N/A"
        r1s = f"{r1:.6f}" if r1 is not None else "N/A"
        if prox_flag:
            parts.append(f"S/R: S1={s1s}, R1={r1s} {prox_flag}")
        spct_str = f"{spread_pct:.2f}%" if spread_pct is not None else "N/A"
        parts.append(f"Spread: {spct_str} ({sclass})")
        explain = "; ".join(parts)
        kind, hit_idx, hit_price = earliest_hit_from_ticks(ticks, entry_tick_index, direction, sl, tp)
        hit_time_utc = ticks[hit_idx].t_utc if hit_idx is not None else None
        hit_time_utc3 = ticks[hit_idx].t_utc3 if hit_idx is not None else None
        price_out = round_like_price(price, price)
        sl_out = round_like_price(sl, price)
        tp_out = round_like_price(tp, price)
        res = SetupResult(
            symbol=symbol,
            direction=direction,
            entry_time_utc=entry_tick.t_utc,
            entry_time_utc3=entry_tick.t_utc3,
            price=price_out if price_out is not None else price,  # type: ignore
            sl=sl_out if sl_out is not None else sl,  # type: ignore
            tp=tp_out if tp_out is not None else tp,  # type: ignore
            rrr=rrr,
            score=score,
            spread_pct=spread_pct,
            spread_class=sclass,
            explain=explain,
            hit=kind,
            hit_time_utc=hit_time_utc,
            hit_time_utc3=hit_time_utc3,
            hit_price=hit_price,
        )
        results.append(res)
        if (not allow_overlap) and hit_idx is not None:
            open_until_index = hit_idx
    return results

def write_results_csv(path: str, rows: List[SetupResult]) -> None:
    hdr = [
        "symbol","direction","entry_time_utc","entry_time_utc3","entry_price",
        "sl","tp","rrr","score","spread_pct","spread_class","hit","hit_time_utc","hit_time_utc3","hit_price","explain"
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(hdr)
        for r in rows:
            w.writerow([
                r.symbol,
                r.direction,
                r.entry_time_utc.strftime("%Y-%m-%d %H:%M:%S"),
                r.entry_time_utc3.strftime("%Y-%m-%d %H:%M:%S"),
                f"{r.price:.6f}",
                f"{r.sl:.6f}",
                f"{r.tp:.6f}",
                f"{r.rrr:.4f}",
                f"{r.score:.3f}",
                f"{r.spread_pct:.4f}" if r.spread_pct is not None else "",
                r.spread_class,
                r.hit or "",
                r.hit_time_utc.strftime("%Y-%m-%d %H:%M:%S") if r.hit_time_utc else "",
                r.hit_time_utc3.strftime("%Y-%m-%d %H:%M:%S") if r.hit_time_utc3 else "",
                f"{r.hit_price:.6f}" if r.hit_price is not None else "",
                r.explain,
            ])

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backtest timelapse strategy on single-symbol CSV ticks (UTC+3).")
    p.add_argument("--csv", required=True, help="Path to MetaTrader CSV ticks file")
    p.add_argument("--symbol", default=None, help="Symbol name (default: inferred from filename)")
    p.add_argument("--tz", default="UTC+3", help="CSV local timezone, e.g., UTC+3 or +3 (default: UTC+3)")
    p.add_argument("--min-rrr", type=float, default=1.0)
    p.add_argument("--min-prox-sl", type=float, default=0.0)
    p.add_argument("--min-sl-pct", type=float, default=0.0)
    p.add_argument("--allow-overlap", action="store_true", help="Allow multiple open setups without waiting for previous to close")
    p.add_argument("--out", default=None, help="Output CSV path (default: <csv>_backtest.csv)")
    return p.parse_args()

def main() -> None:
    args = parse_args()
    csv_path = args.csv
    if not os.path.isfile(csv_path):
        print(f"CSV not found: {csv_path}")
        return
    sym = args.symbol
    if not sym:
        base = os.path.basename(csv_path)
        sym = base.split("-")[0]
    tz_s = str(getattr(args, "tz", "UTC+3") or "UTC+3").upper().replace("UTC", "")
    try:
        if tz_s.startswith("+") or tz_s.startswith("-"):
            offset = int(tz_s)
        else:
            offset = int(tz_s)
    except Exception:
        offset = 3
    rows = analyze_and_backtest(csv_path, sym, offset, args.min_rrr, args.min_prox_sl, args.min_sl_pct, args.allow_overlap)
    out = args.out or (csv_path + ".backtest.csv")
    write_results_csv(out, rows)
    tps = sum(1 for r in rows if r.hit == "TP")
    sls = sum(1 for r in rows if r.hit == "SL")
    print(f"Backtest complete. Setups: {len(rows)} | TP: {tps} | SL: {sls} | Out: {out}")

if __name__ == "__main__":
    main()