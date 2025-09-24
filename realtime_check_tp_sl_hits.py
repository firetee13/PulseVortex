#!/usr/bin/env python3
"""
Real-time TP/SL hits checker for timelapse setups using MetaTrader 5 ticks.

This version checks every 200ms for new ticks in the last second on each running symbol
(symbol that did not hit SL or TP in the database). On first run, it performs a full
historical check from entry time to current timestamp.

Workflow:
  1) Read setups from local SQLite DB (timelapse.db, table: timelapse_setups)
  2) Perform initial historical check for all setups (entry time to now)
  3) Maintain active symbols list (symbols with no hits recorded)
  4) Real-time loop: every 200ms, check latest ticks (last 1 second) for active symbols
  5) Determine hits using bid/ask prices based on direction
  6) Store hits in timelapse_hits and remove from active monitoring

Usage:
  python realtime_check_tp_sl_hits.py
  python realtime_check_tp_sl_hits.py --verbose
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from time import perf_counter, sleep
import os
import sys
from typing import Dict, List, Optional, Set, Tuple

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
        description="Real-time TP/SL hits checker for timelapse setups via MT5 ticks"
    )
    parser.add_argument(
        "--since-hours",
        type=int,
        default=None,
        help="Optional: only check setups inserted in the last N hours (default: all)",
    )
    parser.add_argument("--ids", help="Comma-separated setup IDs to check")
    parser.add_argument(
        "--symbols",
        help="Optional comma-separated symbols filter (e.g., BTCUSD,SOLUSD)",
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


def check_realtime_ticks(
    symbol: str,
    direction: str,
    sl: float,
    tp: float,
    offset_hours: int,
    trace: bool = False,
) -> Tuple[Optional[Hit], TickFetchStats]:
    """
    Check for hits in the last 1 second of ticks (real-time monitoring).
    """
    now_utc = datetime.now(UTC)
    start_utc = now_utc - timedelta(seconds=1)

    if start_utc >= now_utc:
        return None, TickFetchStats(pages=0, total_ticks=0, elapsed_s=0.0, fetch_s=0.0, early_stop=False)

    if trace:
        print(f"    [realtime] Checking last 1 second: {start_utc.isoformat()} -> {now_utc.isoformat()}")

    fetch_start = perf_counter()
    ticks, stats = ticks_range_all(
        symbol,
        to_server_naive(start_utc, offset_hours),
        to_server_naive(now_utc, offset_hours),
        trace=trace,
    )
    fetch_elapsed = perf_counter() - fetch_start

    scan_start = perf_counter()
    hit = earliest_hit_from_ticks(ticks, direction, sl, tp, offset_hours)
    scan_elapsed = perf_counter() - scan_start

    total_elapsed = fetch_elapsed + scan_elapsed
    stats_out = TickFetchStats(
        pages=stats.pages,
        total_ticks=stats.total_ticks,
        elapsed_s=total_elapsed,
        fetch_s=fetch_elapsed,
        early_stop=hit is not None,
    )

    return hit, stats_out


def run_initial_check(args: argparse.Namespace, conn) -> Dict[str, List[Setup]]:
    """
    Perform initial historical check and return active symbols with their setups.
    """
    ids = _parse_ids(getattr(args, "ids", None))
    symbols = _parse_symbols(getattr(args, "symbols", None))

    ensure_hits_table_sqlite(conn)
    backfill_hit_columns_sqlite(conn, "timelapse_setups")

    setups = load_setups_sqlite(
        conn,
        "timelapse_setups",
        None if ids else getattr(args, "since_hours", None),
        ids,
        symbols,
    )

    if not setups:
        print("No setups to check.")
        return {}

    id_list = [setup.id for setup in setups]
    recorded = load_recorded_ids_sqlite(conn, id_list)

    # Group setups by symbol
    symbol_setups: Dict[str, List[Setup]] = {}
    for setup in setups:
        if setup.id not in recorded:
            if setup.symbol not in symbol_setups:
                symbol_setups[setup.symbol] = []
            symbol_setups[setup.symbol].append(setup)

    if not symbol_setups:
        print("All setups already have hits recorded.")
        return {}

    print(f"Performing initial historical check for {len(symbol_setups)} symbols...")

    resolve_cache: dict[str, Optional[str]] = {}
    checked = 0
    hits = 0
    active_symbol_setups: Dict[str, List[Setup]] = {}

    now_utc = datetime.now(UTC)

    for symbol, symbol_setups_list in symbol_setups.items():
        t_resolve_start = perf_counter()
        sym_name = resolve_cache.get(symbol)
        cache_used = True
        if sym_name is None:
            cache_used = False
            sym_name = resolve_symbol(symbol)
            resolve_cache[symbol] = sym_name
        t_resolve = perf_counter() - t_resolve_start

        if sym_name is None:
            print(f"Symbol '{symbol}' not found; skipping.")
            continue

        if args.verbose and not cache_used and sym_name != symbol:
            print(f"[resolve] '{symbol}' -> '{sym_name}'")

        t_off_start = perf_counter()
        offset_h = get_server_offset_hours(sym_name)
        mt5_offset_s = perf_counter() - t_off_start

        if args.verbose:
            sign = "+" if offset_h >= 0 else "-"
            print(f"[offset] {sym_name} server offset {sign}{abs(offset_h)}h")

        active_setups_for_symbol = []

        for setup in symbol_setups_list:
            start_utc = setup.as_of_utc
            end_utc = now_utc

            if args.verbose:
                print(
                    f"[historical] #{setup.id} {setup.symbol} {setup.direction} | "
                    f"UTC {start_utc.isoformat(timespec='seconds')} -> {end_utc.isoformat(timespec='seconds')}"
                )

            hit, stats, chunk_count = scan_for_hit_with_chunks(
                sym_name,
                setup.direction,
                setup.sl,
                setup.tp,
                offset_h,
                start_utc,
                end_utc,
                chunk_minutes=60,  # Use 60-minute chunks for historical scan
                trace=args.verbose,
            )

            if args.verbose and chunk_count > 1:
                total_minutes = (end_utc - start_utc).total_seconds() / 60.0
                print(
                    f"[chunks] #{setup.id} scanned {chunk_count} chunk(s) (max 60 mins each, total {total_minutes:.1f} mins)"
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
                active_setups_for_symbol.append(setup)
            else:
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

        if active_setups_for_symbol:
            active_symbol_setups[symbol] = active_setups_for_symbol

    print(f"Initial check complete. Checked {checked} setup(s); hits recorded: {hits}.")
    print(f"Active symbols for real-time monitoring: {len(active_symbol_setups)}")

    return active_symbol_setups


def realtime_monitoring_loop(args: argparse.Namespace, conn, active_symbol_setups: Dict[str, List[Setup]]) -> None:
    """
    Main real-time monitoring loop that runs every 200ms.
    """
    if not active_symbol_setups:
        print("No active symbols to monitor.")
        return

    print(f"Starting real-time monitoring for {len(active_symbol_setups)} symbols...")

    resolve_cache: dict[str, Optional[str]] = {}
    offset_cache: Dict[str, int] = {}

    try:
        while True:
            loop_start = perf_counter()
            now_utc = datetime.now(UTC)

            symbols_to_remove = []
            total_checks = 0
            total_hits = 0

            for symbol, setups in list(active_symbol_setups.items()):
                if not setups:
                    symbols_to_remove.append(symbol)
                    continue

                # Resolve symbol name
                sym_name = resolve_cache.get(symbol)
                if sym_name is None:
                    sym_name = resolve_symbol(symbol)
                    resolve_cache[symbol] = sym_name

                if sym_name is None:
                    print(f"Symbol '{symbol}' not found; removing from monitoring.")
                    symbols_to_remove.append(symbol)
                    continue

                # Get server offset
                offset_h = offset_cache.get(sym_name)
                if offset_h is None:
                    offset_h = get_server_offset_hours(sym_name)
                    offset_cache[sym_name] = offset_h

                setups_to_remove = []

                for setup in setups:
                    total_checks += 1

                    # Check for hits in the last 1 second
                    hit, stats = check_realtime_ticks(
                        sym_name,
                        setup.direction,
                        setup.sl,
                        setup.tp,
                        offset_h,
                        trace=args.verbose,
                    )

                    if hit is not None:
                        # Validate hit timestamp (should be very recent)
                        time_diff = (now_utc - hit.time_utc).total_seconds()
                        if time_diff <= 2.0:  # Hit within last 2 seconds
                            t_store_start = perf_counter()
                            record_hit_sqlite(conn, setup, hit, args.dry_run, args.verbose)
                            t_store = perf_counter() - t_store_start

                            # Always print basic hit notification
                            print(
                                f"[REALTIME HIT] #{setup.id} {setup.symbol} {setup.direction} -> {hit.kind} "
                                f"at {hit.time_utc.isoformat(timespec='seconds')}"
                            )

                            if args.verbose:
                                t_fetch_ms = stats.fetch_s * 1000.0
                                t_scan_ms = max(0.0, (stats.elapsed_s - stats.fetch_s) * 1000.0)
                                print(
                                    f"  [DETAILS] fetch={t_fetch_ms:.1f}ms scan={t_scan_ms:.1f}ms store={t_store*1000:.1f}ms"
                                )

                            total_hits += 1
                            setups_to_remove.append(setup)

                # Remove hit setups
                for setup in setups_to_remove:
                    setups.remove(setup)

                # Remove symbol if no more setups
                if not setups:
                    symbols_to_remove.append(symbol)

            # Clean up removed symbols
            for symbol in symbols_to_remove:
                if symbol in active_symbol_setups:
                    del active_symbol_setups[symbol]

            loop_time = perf_counter() - loop_start
            sleep_time = max(0.0, 0.2 - loop_time)  # Target 200ms interval

            if args.verbose and total_checks > 0:
                print(f"[realtime] Checked {total_checks} setups, found {total_hits} hits, loop time: {loop_time*1000:.1f}ms, sleeping: {sleep_time*1000:.1f}ms")

            if not active_symbol_setups:
                print("All setups have hit TP/SL. Exiting real-time monitoring.")
                break

            sleep(sleep_time)

    except KeyboardInterrupt:
        print("\nReal-time monitoring interrupted by user.")
    except Exception as e:
        print(f"Error in real-time monitoring loop: {e}")
        raise


def main() -> None:
    args = parse_args()

    if sqlite3 is None:
        print("ERROR: sqlite3 not available.")
        sys.exit(2)

    db_path = db_path_from_args(args)
    conn = sqlite3.connect(db_path, timeout=5)

    try:
        # Initialize MT5
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

        try:
            # Perform initial historical check
            active_symbol_setups = run_initial_check(args, conn)

            # Start real-time monitoring if there are active symbols
            if active_symbol_setups:
                realtime_monitoring_loop(args, conn, active_symbol_setups)
            else:
                print("No active symbols found for real-time monitoring.")

        finally:
            shutdown_mt5()

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()