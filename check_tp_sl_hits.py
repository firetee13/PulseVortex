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
from datetime import datetime, timedelta, timezone
from time import perf_counter
import os
import sys
import time
from typing import List, Optional, Tuple

try:
    import sqlite3  # type: ignore
except Exception:
    sqlite3 = None  # type: ignore

from monitor.domain import Hit, TickFetchStats
from monitor.config import db_path_str
from monitor.db import (
    backfill_hit_columns_sqlite,
    ensure_hits_table_sqlite,
    load_recorded_ids_sqlite,
    load_setups_sqlite,
    record_hit_sqlite,
)
from monitor.mt5_client import (
    earliest_hit_from_ticks,
    get_server_offset_hours,
    init_mt5,
    resolve_symbol,
    shutdown_mt5,
    ticks_range_all,
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
    return parser.parse_args()


def db_path_from_args(args: argparse.Namespace) -> str:
    return db_path_str(getattr(args, "db", None))


def _parse_ids(ids_arg: Optional[str]) -> Optional[List[int]]:
    if not ids_arg:
        return None
    try:
        return [int(x.strip()) for x in ids_arg.split(",") if x.strip()]
    except Exception:
        print("Invalid --ids value. Use comma-separated integers.")
        sys.exit(2)


def _parse_symbols(symbols_arg: Optional[str]) -> Optional[List[str]]:
    if not symbols_arg:
        return None
    return [s.strip() for s in symbols_arg.split(",") if s.strip()]




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
    chunk_span = 0 if chunk_minutes is None else max(0, int(chunk_minutes))
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
        if chunk_span <= 0:
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
            chunk_start,
            chunk_end,
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
            recorded = load_recorded_ids_sqlite(conn, id_list)
            t_recload = perf_counter() - t_recload
            if args.verbose:
                print(f"[timing] preload_recorded={t_recload*1000:.1f}ms (records: {len(recorded)})")

            resolve_cache: dict[str, Optional[str]] = {}
            checked = 0
            hits = 0

            for setup in setups:
                if setup.id in recorded:
                    if args.verbose:
                        print(f"Setup #{setup.id} already recorded; skipping.")
                    continue

                t_resolve_start = perf_counter()
                sym_name = resolve_cache.get(setup.symbol)
                cache_used = True
                if sym_name is None:
                    cache_used = False
                    sym_name = resolve_symbol(setup.symbol)
                    resolve_cache[setup.symbol] = sym_name
                t_resolve = perf_counter() - t_resolve_start
                if sym_name is None:
                    print(f"Symbol '{setup.symbol}' not found; skipping setup #{setup.id}.")
                    continue
                if args.verbose and not cache_used and sym_name != setup.symbol:
                    print(f"[resolve] '{setup.symbol}' -> '{sym_name}'")

                t_off_start = perf_counter()
                offset_h = get_server_offset_hours(sym_name)
                mt5_offset_s = perf_counter() - t_off_start
                if args.verbose:
                    sign = "+" if offset_h >= 0 else "-"
                    print(f"[offset] {sym_name} server offset {sign}{abs(offset_h)}h")

                start_utc = setup.as_of_utc
                end_utc = now_utc
                start_server = to_server_naive(start_utc, offset_h)
                end_server = to_server_naive(end_utc, offset_h)
                if args.verbose:
                    print(
                        f"[window] #{setup.id} {setup.symbol} {setup.direction} | "
                        f"UTC {start_utc.isoformat(timespec='seconds')} -> {end_utc.isoformat(timespec='seconds')} | "
                        f"server-naive {start_server.isoformat(sep=' ', timespec='seconds')} -> {end_server.isoformat(sep=' ', timespec='seconds')}"
                    )

                chunk_minutes = args.max_mins if args.max_mins and args.max_mins > 0 else None
                hit, stats, chunk_count = scan_for_hit_with_chunks(
                    sym_name,
                    setup.direction,
                    setup.sl,
                    setup.tp,
                    offset_h,
                    start_utc,
                    end_utc,
                    chunk_minutes,
                    trace=(args.verbose and args.trace_pages),
                )
                if args.verbose and chunk_count > 1 and chunk_minutes:
                    total_minutes = (end_utc - start_utc).total_seconds() / 60.0
                    print(
                        f"[chunks] #{setup.id} scanned {chunk_count} chunk(s) (max {chunk_minutes} mins each, total {total_minutes:.1f} mins)"
                    )

                thr = (stats.total_ticks / stats.elapsed_s) if stats.elapsed_s > 0 else 0.0
                avg_per_page = (stats.total_ticks / stats.pages) if stats.pages > 0 else 0.0
                t_fetch_ms = stats.fetch_s * 1000.0
                t_scan_ms = max(0.0, (stats.elapsed_s - stats.fetch_s) * 1000.0)
                if args.verbose:
                    print(
                        f"[fetch] #{setup.id} ticks={stats.total_ticks} pages={stats.pages} "
                        f"time={stats.elapsed_s*1000:.1f}ms thr={thr:,.0f} t/s avg_pg={avg_per_page:,.1f}"
                    )

                checked += 1
                if hit is not None:
                    try:
                        if hit.time_utc <= setup.as_of_utc + timedelta(milliseconds=1):
                            if args.verbose:
                                print(
                                    f"[IGNORED HIT] #{setup.id} {setup.symbol} {setup.direction} -> {hit.kind} "
                                    f"at {hit.time_utc.isoformat()} (<= as_of)"
                                )
                            hit = None
                    except Exception:
                        pass

                if hit is None:
                    if args.verbose:
                        duration = (now_utc - setup.as_of_utc).total_seconds()
                        extra = " cache" if cache_used else ""
                        print(
                            f"[NO HIT] #{setup.id} {setup.symbol} {setup.direction} | "
                            f"window {duration/60:.1f} mins | ticks {stats.total_ticks} | pages {stats.pages} | "
                            f"fetch={t_fetch_ms:.1f}ms scan={t_scan_ms:.1f}ms resolve={t_resolve*1000:.1f}ms{extra}"
                        )
                    continue

                t_store_start = perf_counter()
                record_hit_sqlite(conn, setup, hit, args.dry_run, args.verbose)
                t_store = perf_counter() - t_store_start
                if args.verbose:
                    extra = " cache" if cache_used else ""
                    print(
                        f"[HIT TIMING] #{setup.id} fetch={t_fetch_ms:.1f}ms scan={t_scan_ms:.1f}ms "
                        f"store={t_store*1000:.1f}ms resolve={t_resolve*1000:.1f}ms{extra} | ticks {stats.total_ticks} pages {stats.pages}"
                    )
                hits += 1

            print(f"Checked {checked} setup(s); hits recorded: {hits}.")
        finally:
            shutdown_mt5()
    finally:
        try:
            conn.close()
        except Exception:
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
