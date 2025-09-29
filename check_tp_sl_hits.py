#!/usr/bin/env python3
"""
Check TP/SL hits for timelapse setups using MetaTrader 5 ticks.

Workflow:
  1) Read setups from local SQLite DB (timelapse.db, table: timelapse_setups)
  2) For each, infer broker server offset (+/- whole hours) from current tick
  3) Fetch ticks from as_of (UTC) to now, adjusted for server offset
  4) Determine earliest hit (TP or SL) based on side-specific trigger price
     - Buy: use Bid; TP if bid >= tp; SL if bid <= sl
     - Sell: use Ask; TP if ask <= tp; SL if ask >= sl
  5) Store hits in timelapse_hits (one row per setup)

Usage examples:
  python check_tp_sl_hits.py --since-hours 24
  python check_tp_sl_hits.py --ids 9,10,11,12
  python check_tp_sl_hits.py --symbols SOLUSD,BTCUSD

Notes:
  - `as_of` in DB is UTC stored as ISO text; we apply UTC+3 for display-only fields.
  - The script uses ticks (preferred) and pages in chunks to cover the whole range.
  - If no ticks arrive (very unlikely for crypto), the script reports "no hit yet".
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from time import perf_counter
import os
import sys
import time
from typing import Dict, Iterable, List, Optional, Tuple

try:
    import sqlite3  # type: ignore
except ImportError:
    sqlite3 = None  # type: ignore

from monitor.domain import Hit, Setup, TickFetchStats
from monitor.config import db_path_str
from monitor.db import (
    backfill_hit_columns_sqlite,
    ensure_hits_table_sqlite,
    ensure_tp_sl_setup_state_sqlite,
    load_recorded_ids_sqlite,
    load_setups_sqlite,
    load_tp_sl_setup_state_sqlite,
    persist_tp_sl_setup_state_sqlite,
    record_hit_sqlite,
)
from monitor.mt5_client import (
    earliest_hit_from_ticks,
    get_server_offset_hours,
    get_symbol_info,
    init_mt5,
    resolve_symbol,
    rates_range_utc,
    shutdown_mt5,
    ticks_range_all,
    timeframe_from_code,
    timeframe_m1,
    timeframe_seconds,
    to_server_naive,
)

UTC = timezone.utc


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check TP/SL hits for timelapse setups via MT5 ticks (SQLite)"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--since-hours",
        type=int,
        default=None,
        help="Optional: only check setups inserted in the last N hours (default: all)",
    )
    group.add_argument("--ids", help="Comma-separated setup IDs to check (overrides --since-hours)")
    parser.add_argument(
        "--symbols",
        help="Optional comma-separated symbols filter (e.g., BTCUSD,SOLUSD)",
    )
    parser.add_argument(
        "--max-mins",
        type=int,
        default=24 * 60,
        help="Maximum minutes per fetch chunk when scanning history (default 1440)",
    )
    parser.add_argument(
        "--page",
        type=int,
        default=200000,
        help="Ignored: kept for backwards compatibility",
    )
    parser.add_argument(
        "--trace-pages",
        action="store_true",
        help="When verbose, print per-page tick fetch timings",
    )
    parser.add_argument(
        "--db",
        dest="db",
        default=db_path_str(),
        help="Path to SQLite DB file (default: timelapse.db next to script)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Do not write hit records to DB")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--mt5-path",
        dest="mt5_path",
        default=os.environ.get("MT5_TERMINAL_PATH"),
        help="Path to terminal64.exe (env: MT5_TERMINAL_PATH)",
    )
    parser.add_argument(
        "--mt5-timeout",
        dest="mt5_timeout",
        type=int,
        default=int(os.environ.get("MT5_TIMEOUT", "90")),
        help="MT5 initialize timeout seconds (default/env: 90)",
    )
    parser.add_argument(
        "--mt5-retries",
        dest="mt5_retries",
        type=int,
        default=int(os.environ.get("MT5_RETRIES", "2")),
        help="Retries for MT5 initialize on transient errors (default/env: 2)",
    )
    parser.add_argument(
        "--mt5-portable",
        dest="mt5_portable",
        action="store_true",
        default=_env_bool("MT5_PORTABLE", False),
        help="Pass portable=True to MT5 (env: MT5_PORTABLE)",
    )
    parser.add_argument("--watch", action="store_true", help="Run continuously, polling every --interval seconds")
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Polling interval in seconds for --watch mode (default 60)",
    )
    parser.add_argument(
        "--bar-timeframe",
        default=os.environ.get("TP_SL_BAR_TIMEFRAME", "M1"),
        help="MT5 timeframe code for bar prefiltering (default/env: M1)",
    )
    parser.add_argument(
        "--bar-backtrack",
        type=int,
        default=int(os.environ.get("TP_SL_BAR_BACKTRACK", "2")),
        help="Minutes to backtrack when loading bars for prefiltering (default/env: 2)",
    )
    parser.add_argument(
        "--tick-padding",
        type=float,
        default=float(os.environ.get("TP_SL_TICK_PADDING", "1.0")),
        help="Extra seconds added around candidate windows when fetching ticks (default/env: 1.0)",
    )
    return parser.parse_args()


def db_path_from_args(args: argparse.Namespace) -> str:
    return db_path_str(getattr(args, "db", None))


def _parse_ids(ids_arg: Optional[str]) -> Optional[List[int]]:
    if not ids_arg:
        return None
    try:
        return [int(x.strip()) for x in ids_arg.split(",") if x.strip()]
    except (ValueError, TypeError):
        print("Invalid --ids value. Use comma-separated integers.")
        sys.exit(2)


def _parse_symbols(symbols_arg: Optional[str]) -> Optional[List[str]]:
    if not symbols_arg:
        return None
    return [s.strip() for s in symbols_arg.split(",") if s.strip()]



@dataclass
class RateBar:
    start_utc: datetime
    end_utc: datetime
    low: float
    high: float


@dataclass
class CandidateWindow:
    setup_id: int
    start_utc: datetime
    end_utc: datetime
    bar_start_utc: datetime
    bar_end_utc: datetime


@dataclass
class SetupResult:
    setup_id: int
    hit: Optional[Hit]
    ticks: int
    pages: int
    fetch_s: float
    elapsed_s: float
    windows: int
    last_checked_utc: datetime
    ignored_hit: bool = False


def _resolve_timeframe(code: Optional[str]) -> int:
    if code:
        timeframe = timeframe_from_code(code)
        if timeframe is not None:
            return timeframe
    return timeframe_m1()


def _rate_field(rate: object, name: str) -> Optional[float]:
    try:
        value = getattr(rate, name)
    except AttributeError:
        try:
            value = rate[name]  # type: ignore[index]
        except Exception:
            if isinstance(rate, dict):
                value = rate.get(name)
            else:
                value = None
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _rate_time(rate: object, offset_hours: int) -> Optional[datetime]:
    time_val = _rate_field(rate, "time")
    if time_val is None:
        return None
    try:
        dt_server = datetime.fromtimestamp(float(time_val), tz=UTC)
    except Exception:
        return None
    return dt_server - timedelta(hours=offset_hours)


def _rates_to_bars(rates: Iterable[object], timeframe_seconds: int, offset_hours: int) -> List[RateBar]:
    bars: List[RateBar] = []
    for rate in rates:
        start = _rate_time(rate, offset_hours)
        if start is None:
            continue
        low = _rate_field(rate, "low")
        high = _rate_field(rate, "high")
        if low is None or high is None:
            continue
        end = start + timedelta(seconds=timeframe_seconds)
        bars.append(RateBar(start_utc=start, end_utc=end, low=float(low), high=float(high)))
    return bars


def _compute_spread_guard(symbol: str) -> float:
    info = get_symbol_info(symbol)
    if info is None:
        return 0.0
    try:
        point = float(getattr(info, "point", 0.0) or 0.0)
        spread = float(getattr(info, "spread", 0.0) or 0.0)
    except Exception:
        return 0.0
    if point <= 0.0:
        return 0.0
    guard_points = max(spread * 1.5, 5.0)
    return point * guard_points


def _bar_crosses_price(bar: RateBar, setup, spread_guard: float) -> bool:
    direction = (setup.direction or "").lower()
    sl = float(setup.sl)
    tp = float(setup.tp)
    if direction == "sell":
        upper = bar.high + spread_guard
        lower = bar.low - spread_guard
    else:
        upper = bar.high
        lower = bar.low
    if lower <= sl <= upper:
        return True
    if lower <= tp <= upper:
        return True
    return False


def _merge_windows(windows: List[CandidateWindow]) -> List[CandidateWindow]:
    if not windows:
        return []
    windows_sorted = sorted(windows, key=lambda w: (w.setup_id, w.start_utc))
    merged: List[CandidateWindow] = []
    for win in windows_sorted:
        if not merged:
            merged.append(win)
            continue
        last = merged[-1]
        if win.setup_id == last.setup_id and win.start_utc <= last.end_utc + timedelta(seconds=1):
            merged_start = min(last.start_utc, win.start_utc)
            merged_end = max(last.end_utc, win.end_utc)
            merged_bar_start = min(last.bar_start_utc, win.bar_start_utc)
            merged_bar_end = max(last.bar_end_utc, win.bar_end_utc)
            merged[-1] = CandidateWindow(
                setup_id=last.setup_id,
                start_utc=merged_start,
                end_utc=merged_end,
                bar_start_utc=merged_bar_start,
                bar_end_utc=merged_bar_end,
            )
            continue
        merged.append(win)
    return merged


def _evaluate_setup(
    setup,
    last_checked_utc: datetime,
    bars: List[RateBar],
    resolved_symbol: str,
    offset_hours: int,
    spread_guard: float,
    now_utc: datetime,
    chunk_minutes: Optional[int],
    tick_padding_seconds: float,
    trace_ticks: bool,
) -> SetupResult:
    cursor = max(last_checked_utc, setup.as_of_utc)
    progress = cursor
    candidate_windows: List[CandidateWindow] = []

    for bar in bars:
        if bar.end_utc <= cursor:
            continue
        if bar.end_utc <= setup.as_of_utc:
            continue
        window_start = max(progress, bar.start_utc, setup.as_of_utc)
        window_end = min(bar.end_utc, now_utc)
        if window_end <= window_start:
            progress = max(progress, window_end)
            continue
        if _bar_crosses_price(bar, setup, spread_guard):
            candidate_windows.append(
                CandidateWindow(
                    setup_id=setup.id,
                    start_utc=window_start,
                    end_utc=window_end,
                    bar_start_utc=bar.start_utc,
                    bar_end_utc=bar.end_utc,
                )
            )
        progress = max(progress, window_end)

    if not bars and now_utc > cursor:
        fallback_end = now_utc
        candidate_windows.append(
            CandidateWindow(
                setup_id=setup.id,
                start_utc=cursor,
                end_utc=fallback_end,
                bar_start_utc=cursor,
                bar_end_utc=fallback_end,
            )
        )
        progress = fallback_end

    merged_windows = _merge_windows(candidate_windows)
    tick_padding = timedelta(seconds=max(0.0, tick_padding_seconds))

    total_ticks = 0
    total_pages = 0
    total_fetch = 0.0
    total_elapsed = 0.0
    hit: Optional[Hit] = None
    ignored_hit = False
    windows_checked = 0
    new_cursor = max(progress, cursor)

    for window in merged_windows:
        if hit is not None:
            break
        windows_checked += 1
        window_start = max(window.start_utc - tick_padding, setup.as_of_utc)
        window_end = min(window.end_utc + tick_padding, now_utc)
        if window_end <= window_start:
            window_start = window.start_utc
            window_end = min(window.end_utc, now_utc)
        if window_end <= window_start:
            continue
        candidate_hit, stats, _ = scan_for_hit_with_chunks(
            symbol=resolved_symbol,
            direction=setup.direction,
            sl=setup.sl,
            tp=setup.tp,
            offset_hours=offset_hours,
            start_utc=window_start,
            end_utc=window_end,
            chunk_minutes=chunk_minutes,
            trace=trace_ticks,
        )
        total_ticks += stats.total_ticks
        total_pages += stats.pages
        total_fetch += stats.fetch_s
        total_elapsed += stats.elapsed_s
        if candidate_hit is not None:
            if candidate_hit.time_utc <= setup.as_of_utc + timedelta(milliseconds=1):
                ignored_hit = True
            else:
                hit = candidate_hit
                new_cursor = min(candidate_hit.time_utc, now_utc)
                break
        new_cursor = max(new_cursor, min(window_end, now_utc))

    new_cursor = min(new_cursor, now_utc)
    return SetupResult(
        setup_id=setup.id,
        hit=hit,
        ticks=total_ticks,
        pages=total_pages,
        fetch_s=total_fetch,
        elapsed_s=total_elapsed,
        windows=windows_checked,
        last_checked_utc=new_cursor,
        ignored_hit=ignored_hit,
    )




def scan_for_hit_with_chunks(
    symbol: str,
    direction: str,
    sl: float,
    tp: float,
    offset_hours: int,
    start_utc: datetime,
    end_utc: datetime,
    chunk_minutes: Optional[int],
    trace: bool = False,
) -> Tuple[Optional[Hit], TickFetchStats, int]:
    """Fetch ticks in bounded chunks until a hit is found or the range is exhausted."""
    if start_utc >= end_utc:
        return None, TickFetchStats(pages=0, total_ticks=0, elapsed_s=0.0, fetch_s=0.0, early_stop=False), 0
    chunk_span = chunk_minutes if chunk_minutes is not None and chunk_minutes > 0 else None
    chunk_count = 0
    total_ticks = 0
    total_pages = 0
    total_fetch_s = 0.0
    total_scan_s = 0.0
    hit: Optional[Hit] = None
    chunk_start = start_utc
    t0 = perf_counter()
    while chunk_start < end_utc and hit is None:
        chunk_count += 1
        if chunk_span is None:
            chunk_end = end_utc
        else:
            chunk_end = min(end_utc, chunk_start + timedelta(minutes=chunk_span))
        if chunk_end <= chunk_start:
            break
        if trace:
            print(
                f"    [chunk] #{chunk_count} UTC {chunk_start.isoformat(timespec='seconds')} -> {chunk_end.isoformat(timespec='seconds')}"
            )
        fetch_start = perf_counter()
        ticks, stats = ticks_range_all(
            symbol,
            to_server_naive(chunk_start, offset_hours),
            to_server_naive(chunk_end, offset_hours),
            trace=trace,
        )
        fetch_elapsed = perf_counter() - fetch_start
        total_fetch_s += fetch_elapsed
        total_ticks += stats.total_ticks
        total_pages += stats.pages
        scan_start = perf_counter()
        candidate = earliest_hit_from_ticks(ticks, direction, sl, tp, offset_hours)
        total_scan_s += perf_counter() - scan_start
        if candidate is not None:
            hit = candidate
            break
        chunk_start = chunk_end
    elapsed = perf_counter() - t0
    if elapsed == 0.0:
        elapsed = total_fetch_s + total_scan_s
    stats_out = TickFetchStats(
        pages=total_pages,
        total_ticks=total_ticks,
        elapsed_s=elapsed,
        fetch_s=total_fetch_s,
        early_stop=hit is not None,
    )
    return hit, stats_out, chunk_count

def run_once(args: argparse.Namespace) -> None:
    ids = _parse_ids(getattr(args, "ids", None))
    symbols = _parse_symbols(getattr(args, "symbols", None))

    if sqlite3 is None:
        print("ERROR: sqlite3 not available.")
        sys.exit(2)

    t0 = perf_counter()
    db_path = db_path_from_args(args)
    conn = sqlite3.connect(db_path, timeout=5)
    db_conn_s = perf_counter() - t0
    try:
        ensure_hits_table_sqlite(conn)
        ensure_tp_sl_setup_state_sqlite(conn)
        backfill_hit_columns_sqlite(conn, "timelapse_setups")

        t1 = perf_counter()
        setups = load_setups_sqlite(
            conn,
            "timelapse_setups",
            None if ids else getattr(args, "since_hours", None),
            ids,
            symbols,
        )
        db_load_s = perf_counter() - t1
        if not setups:
            print("No setups to check.")
            return

        t2 = perf_counter()
        try:
            init_mt5(
                path=getattr(args, "mt5_path", None),
                timeout=int(getattr(args, "mt5_timeout", 90)),
                retries=int(getattr(args, "mt5_retries", 2)),
                portable=bool(getattr(args, "mt5_portable", False)),
                verbose=bool(getattr(args, "verbose", False)),
            )
        except RuntimeError as exc:
            print(f"ERROR: Failed to initialize MetaTrader5 ({exc})")
            print(
                "Hints: set --mt5-path or MT5_TERMINAL_PATH to your terminal64.exe; "
                "increase --mt5-timeout; ensure the terminal isn't updating and that only one Python process is using MT5."
            )
            return
        mt5_init_s = perf_counter() - t2

        try:
            now_utc = datetime.now(UTC)
            id_list = [setup.id for setup in setups]
            t_recload = perf_counter()
            recorded = set(load_recorded_ids_sqlite(conn, id_list))
            t_recload = perf_counter() - t_recload
            if args.verbose:
                print(f"[timing] preload_recorded={t_recload*1000:.1f}ms (records: {len(recorded)})")

            pending_setups = [setup for setup in setups if setup.id not in recorded]
            if not pending_setups:
                print("No setups to check.")
                return

            raw_state = load_tp_sl_setup_state_sqlite(conn, [setup.id for setup in pending_setups])
            last_checked_map: Dict[int, datetime] = {}
            for setup in pending_setups:
                state_dt = raw_state.get(setup.id)
                if state_dt is None or state_dt < setup.as_of_utc:
                    last_checked_map[setup.id] = setup.as_of_utc
                else:
                    last_checked_map[setup.id] = state_dt

            timeframe = _resolve_timeframe(getattr(args, "bar_timeframe", None))
            timeframe_secs = timeframe_seconds(timeframe)
            backtrack_minutes = max(0, int(getattr(args, "bar_backtrack", 2)))
            tick_padding_seconds = max(
                0.0, float(getattr(args, "tick_padding", getattr(args, "tick_slack", 1.0)))
            )
            chunk_minutes = args.max_mins if args.max_mins and args.max_mins > 0 else None
            trace_ticks = bool(args.verbose and args.trace_pages)

            groups: Dict[str, List[Setup]] = defaultdict(list)
            for setup in pending_setups:
                groups[setup.symbol].append(setup)

            resolve_cache: Dict[str, Optional[str]] = {}
            offset_cache: Dict[str, int] = {}
            spread_cache: Dict[str, float] = {}

            checked = 0
            hits = 0
            hit_symbols: List[str] = []

            for base_symbol, grouped_setups in groups.items():
                t_resolve_start = perf_counter()
                if base_symbol not in resolve_cache:
                    resolve_cache[base_symbol] = resolve_symbol(base_symbol)
                sym_name = resolve_cache[base_symbol]
                t_resolve = perf_counter() - t_resolve_start
                if sym_name is None:
                    print(
                        f"Symbol '{base_symbol}' not found; skipping {len(grouped_setups)} setup(s)."
                    )
                    continue
                if args.verbose and sym_name != base_symbol:
                    print(f"[resolve] '{base_symbol}' -> '{sym_name}'")

                offset_h = offset_cache.get(sym_name)
                if offset_h is None:
                    t_off_start = perf_counter()
                    offset_h = get_server_offset_hours(sym_name)
                    offset_cache[sym_name] = offset_h
                    if args.verbose:
                        mt5_offset_s = perf_counter() - t_off_start
                        sign = "+" if offset_h >= 0 else "-"
                        print(
                            f"[offset] {sym_name} server offset {sign}{abs(offset_h)}h ({mt5_offset_s*1000:.1f}ms)"
                        )

                spread_guard = spread_cache.get(sym_name)
                if spread_guard is None:
                    spread_guard = _compute_spread_guard(sym_name)
                    spread_cache[sym_name] = spread_guard

                min_last_checked = min(last_checked_map[setup.id] for setup in grouped_setups)
                earliest_as_of = min(setup.as_of_utc for setup in grouped_setups)
                fetch_start = min(min_last_checked, earliest_as_of) - timedelta(minutes=backtrack_minutes)
                fetch_start = min(fetch_start, now_utc)
                fetch_end = now_utc + timedelta(seconds=timeframe_secs)
                rates = rates_range_utc(
                    sym_name,
                    timeframe,
                    fetch_start,
                    fetch_end,
                    offset_h,
                    trace=trace_ticks,
                )
                bars = _rates_to_bars(rates, timeframe_secs, offset_h)

                for setup in grouped_setups:
                    last_checked = last_checked_map[setup.id]
                    result = _evaluate_setup(
                        setup,
                        last_checked,
                        bars,
                        sym_name,
                        offset_h,
                        spread_guard,
                        now_utc,
                        chunk_minutes,
                        tick_padding_seconds,
                        trace_ticks,
                    )
                    last_checked_map[setup.id] = result.last_checked_utc
                    checked += 1

                    if result.hit is not None:
                        if args.verbose:
                            t_fetch_ms = result.fetch_s * 1000.0
                            t_scan_ms = max(0.0, (result.elapsed_s - result.fetch_s) * 1000.0)
                            print(
                                f"[HIT TIMING] #{setup.id} {setup.symbol} {setup.direction} | "
                                f"windows {result.windows} | ticks {result.ticks} pages {result.pages} | "
                                f"fetch={t_fetch_ms:.1f}ms scan={t_scan_ms:.1f}ms"
                            )
                        record_hit_sqlite(conn, setup, result.hit, args.dry_run, args.verbose)
                        hits += 1
                        hit_symbols.append(setup.symbol)
                        continue

                    if args.verbose:
                        duration = (now_utc - setup.as_of_utc).total_seconds() / 60.0
                        thr = (result.ticks / result.elapsed_s) if result.elapsed_s > 0 else 0.0
                        avg_per_page = (result.ticks / result.pages) if result.pages > 0 else 0.0
                        print(
                            f"[NO HIT] #{setup.id} {setup.symbol} {setup.direction} | window {duration:.1f} mins | "
                            f"ticks {result.ticks} | pages {result.pages} | "
                            f"fetch={result.fetch_s*1000:.1f}ms scan={(result.elapsed_s - result.fetch_s)*1000:.1f}ms thr={thr:,.0f} avg_pg={avg_per_page:,.1f}"
                        )
                    if result.ignored_hit and args.verbose:
                        print(
                            f"[IGNORED HIT] #{setup.id} {setup.symbol} {setup.direction} (<= as_of)"
                        )

            persist_tp_sl_setup_state_sqlite(conn, last_checked_map)

            if hit_symbols:
                symbols_str = " ".join(sorted(set(hit_symbols)))
                print(f"Checked {checked} setup(s); hits recorded: {hits}. {symbols_str}")
        finally:
            shutdown_mt5()
    finally:
        try:
            conn.close()
        except (sqlite3.Error, AttributeError):
            pass

    if getattr(args, "verbose", False):
        print(
            f"[timing] db_conn={db_conn_s*1000:.1f}ms db_load={db_load_s*1000:.1f}ms mt5_init={mt5_init_s*1000:.1f}ms"
        )


def main() -> None:
    args = parse_args()
    if args.watch:
        print(f"Watch mode enabled. Polling every {args.interval} seconds...")
        try:
            while True:
                run_once(args)
                time.sleep(max(1, int(args.interval)))
        except KeyboardInterrupt:
            print("Interrupted. Exiting watch mode.")
    else:
        run_once(args)


if __name__ == "__main__":
    main()
