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
from typing import Any, Dict, List, Optional, Sequence, Tuple, Set

try:
    import sqlite3  # type: ignore
except Exception:
    sqlite3 = None  # type: ignore

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore

from monitor.domain import Hit, Setup, TickFetchStats
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
REALTIME_WINDOW_SECONDS = 1.0
REALTIME_BACKTRACK_SECONDS = 0.2
# Allow widening the fetch window when the script falls behind (e.g., MT5 lag
# or the loop sleeping longer than expected). This limits each catch-up pull to
# a bounded slice so we don't request the entire history on every iteration.
REALTIME_MAX_CATCHUP_SECONDS = 60.0
REALTIME_STALE_HIT_WARNING_SECONDS = 5.0
REDIS_DEFAULT_PREFIX = "timelapse:last_tick"
REFRESH_INTERVAL_SECONDS = 60.0


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
    parser.add_argument(
        "--redis-url",
        dest="redis_url",
        default=os.environ.get("TIMELAPSE_REDIS_URL", "redis://localhost:6379"),
        help="Optional Redis URL for caching last-seen ticks (env: TIMELAPSE_REDIS_URL)",
    )
    parser.add_argument(
        "--redis-prefix",
        dest="redis_prefix",
        default=os.environ.get("TIMELAPSE_REDIS_PREFIX", REDIS_DEFAULT_PREFIX),
        help="Redis key prefix for cached ticks (default/env: timelapse:last_tick)",
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






def fetch_recent_ticks(
    symbol: str,
    offset_hours: int,
    window_end_utc: datetime,
    window_start_utc: Optional[datetime] = None,
    window_seconds: float = REALTIME_WINDOW_SECONDS,
    trace: bool = False,
) -> Tuple[List[object], TickFetchStats]:
    """Fetch MT5 ticks for the recent real-time monitoring window."""
    if window_start_utc is None:
        window_start_utc = window_end_utc - timedelta(seconds=window_seconds)
    if window_start_utc >= window_end_utc:
        empty_stats = TickFetchStats(pages=0, total_ticks=0, elapsed_s=0.0, fetch_s=0.0, early_stop=False)
        return [], empty_stats

    if trace:
        window_len = (window_end_utc - window_start_utc).total_seconds()
        print(
            f"    [realtime] Checking window ({window_len:.3f}s): "
            f"{window_start_utc.isoformat()} -> {window_end_utc.isoformat()}"
        )

    ticks, stats = ticks_range_all(
        symbol,
        to_server_naive(window_start_utc, offset_hours),
        to_server_naive(window_end_utc, offset_hours),
        trace=trace,
    )

    fetch_stats = TickFetchStats(
        pages=stats.pages,
        total_ticks=stats.total_ticks,
        elapsed_s=stats.elapsed_s,
        fetch_s=stats.fetch_s,
        early_stop=False,
    )
    return ticks, fetch_stats


def _tick_to_utc_prices(tick: object, server_offset_hours: int) -> Tuple[Optional[datetime], Optional[float], Optional[float]]:
    bid = getattr(tick, 'bid', None)
    if bid is None:
        try:
            bid = tick['bid']  # type: ignore[index]
        except Exception:
            if isinstance(tick, dict):
                bid = tick.get('bid')
    ask = getattr(tick, 'ask', None)
    if ask is None:
        try:
            ask = tick['ask']  # type: ignore[index]
        except Exception:
            if isinstance(tick, dict):
                ask = tick.get('ask')
    if bid is None and ask is None:
        return None, None, None

    tms = getattr(tick, 'time_msc', None)
    if tms is None:
        try:
            tms = tick['time_msc']  # type: ignore[index]
        except Exception:
            if isinstance(tick, dict):
                tms = tick.get('time_msc')
    dt_raw: Optional[datetime] = None
    if tms is not None:
        dt_raw = datetime.fromtimestamp(float(tms) / 1000.0, tz=UTC)
    else:
        tse = getattr(tick, 'time', None)
        if tse is None:
            try:
                tse = tick['time']  # type: ignore[index]
            except Exception:
                if isinstance(tick, dict):
                    tse = tick.get('time')
        if tse is None:
            return None, None, None
        dt_raw = datetime.fromtimestamp(float(tse), tz=UTC)

    dt_utc = dt_raw - timedelta(hours=server_offset_hours)
    return dt_utc, bid, ask


def _prepare_tick_data(ticks: List[object], server_offset_hours: int) -> List[Tuple[datetime, Optional[float], Optional[float]]]:
    data: List[Tuple[datetime, Optional[float], Optional[float]]] = []
    for tick in ticks:
        dt_utc, bid, ask = _tick_to_utc_prices(tick, server_offset_hours)
        if dt_utc is None:
            continue
        data.append((dt_utc, bid, ask))
    return data


def _find_hit_in_tick_data(
    tick_data: List[Tuple[datetime, Optional[float], Optional[float]]],
    direction: str,
    sl: float,
    tp: float,
    start_utc: datetime,
) -> Optional[Hit]:
    direction_lower = direction.lower()
    for dt_utc, bid, ask in tick_data:
        if dt_utc < start_utc:
            continue
        if direction_lower == 'buy':
            if bid is not None and bid <= sl:
                return Hit(kind='SL', time_utc=dt_utc, price=bid)
            if bid is not None and bid >= tp:
                return Hit(kind='TP', time_utc=dt_utc, price=bid)
        else:
            if ask is not None and ask >= sl:
                return Hit(kind='SL', time_utc=dt_utc, price=ask)
            if ask is not None and ask <= tp:
                return Hit(kind='TP', time_utc=dt_utc, price=ask)
    return None


def create_redis_client(args: argparse.Namespace) -> Optional[Any]:
    redis_url = getattr(args, 'redis_url', None)
    if not redis_url:
        return None
    if redis is None:
        print('WARNING: redis package not available; ignoring --redis-url parameter.')
        return None
    try:
        client = redis.Redis.from_url(
            redis_url,
            socket_timeout=2.0,
            socket_connect_timeout=2.0,
            decode_responses=True,
        )
        client.ping()
    except Exception as exc:  # pragma: no cover - depends on redis availability
        print(
            f'WARNING: Failed to connect to Redis at {redis_url} ({exc}). Continuing without Redis caching.'
        )
        return None
    return client


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)




def _format_db_timestamp(dt: datetime) -> str:
    dt_utc = _ensure_utc(dt)
    return dt_utc.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def _load_new_setups_since(
    conn,
    cutoff_dt: datetime,
    symbols: Optional[List[str]],
    active_setup_ids: Set[int],
    verbose: bool,
) -> List[Setup]:
    cutoff_str = _format_db_timestamp(cutoff_dt)
    where_clauses = ["inserted_at > ?"]
    params: List[object] = [cutoff_str]
    if symbols:
        placeholders = ",".join(["?"] * len(symbols))
        where_clauses.append(f"symbol IN ({placeholders})")
        params.extend(symbols)
    sql = "SELECT id FROM timelapse_setups WHERE " + " AND ".join(where_clauses) + " ORDER BY inserted_at ASC, id ASC"
    cur = conn.cursor()
    try:
        rows = cur.execute(sql, params).fetchall()
    except Exception:
        return []
    new_ids = [int(row[0]) for row in rows or [] if row and row[0] is not None]
    new_ids = [sid for sid in new_ids if sid not in active_setup_ids]
    if not new_ids:
        return []
    recorded = load_recorded_ids_sqlite(conn, new_ids)
    pending_ids = [sid for sid in new_ids if sid not in recorded]
    if not pending_ids:
        return []
    setups = load_setups_sqlite(
        conn,
        "timelapse_setups",
        None,
        pending_ids,
        symbols,
    )
    if verbose and setups:
        ids_str = ", ".join(str(setup.id) for setup in setups)
        print(f"[refresh] Loaded {len(setups)} new setup(s): {ids_str}")
    return setups


def _redis_key(prefix: str, symbol: str) -> str:
    sanitized = symbol.replace(' ', '_')
    return f'{prefix}:{sanitized}'


def _read_last_tick(redis_client: Any, prefix: str, symbol: str) -> Optional[datetime]:
    if redis_client is None:
        return None
    try:
        value = redis_client.get(_redis_key(prefix, symbol))
    except Exception:
        return None
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except Exception:
        return None
    return _ensure_utc(dt)


def _write_last_tick(redis_client: Any, prefix: str, symbol: str, dt: datetime) -> None:
    if redis_client is None:
        return
    try:
        redis_client.set(_redis_key(prefix, symbol), _ensure_utc(dt).isoformat())
    except Exception:
        pass


def _latest_tick_time_utc(ticks: Sequence[object], server_offset_hours: int) -> Optional[datetime]:
    try:
        length = len(ticks)
    except Exception:
        length = None
    if length:
        for idx in range(length - 1, -1, -1):
            try:
                tick = ticks[idx]
            except Exception:
                continue
            dt_utc, _, _ = _tick_to_utc_prices(tick, server_offset_hours)
            if dt_utc is not None:
                return dt_utc
        return None
    latest: Optional[datetime] = None
    for tick in ticks:
        dt_utc, _, _ = _tick_to_utc_prices(tick, server_offset_hours)
        if dt_utc is not None:
            latest = dt_utc
    return latest

def run_initial_check(args: argparse.Namespace, conn, redis_client: Optional[Any] = None, redis_prefix: str = REDIS_DEFAULT_PREFIX) -> Dict[str, List[Setup]]:
    """Perform initial historical check and return active symbols with their setups."""
    ids = _parse_ids(getattr(args, 'ids', None))
    symbols = _parse_symbols(getattr(args, 'symbols', None))

    ensure_hits_table_sqlite(conn)
    backfill_hit_columns_sqlite(conn, 'timelapse_setups')

    setups = load_setups_sqlite(
        conn,
        'timelapse_setups',
        None if ids else getattr(args, 'since_hours', None),
        ids,
        symbols,
    )

    if not setups:
        print('No setups to check.')
        return {}

    id_list = [setup.id for setup in setups]
    recorded = load_recorded_ids_sqlite(conn, id_list)

    symbol_setups: Dict[str, List[Setup]] = {}
    for setup in setups:
        if setup.id not in recorded:
            symbol_setups.setdefault(setup.symbol, []).append(setup)

    if not symbol_setups:
        print('All setups already have hits recorded.')
        return {}

    print(f'Performing initial historical check for {len(symbol_setups)} symbols...')

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
            sign = '+' if offset_h >= 0 else '-'
            print(f"[offset] {sym_name} server offset {sign}{abs(offset_h)}h")

        pending_setups = sorted(symbol_setups_list, key=lambda s: s.as_of_utc)
        if not pending_setups:
            continue

        setup_states = {
            setup.id: {
                'ticks': 0,
                'pages': 0,
                'fetch_s': 0.0,
                'scan_s': 0.0,
                'chunks': 0,
            }
            for setup in pending_setups
        }

        chunk_minutes = 60
        chunk_delta = timedelta(minutes=chunk_minutes)
        chunk_start = min(setup.as_of_utc for setup in pending_setups)

        while pending_setups and chunk_start < now_utc:
            chunk_end = min(now_utc, chunk_start + chunk_delta)

            ticks, chunk_stats = ticks_range_all(
                sym_name,
                to_server_naive(chunk_start, offset_h),
                to_server_naive(chunk_end, offset_h),
                trace=args.verbose,
            )

            tick_data = _prepare_tick_data(ticks, offset_h)

            if redis_client is not None:
                latest_tick_dt = _latest_tick_time_utc(ticks, offset_h)
                if latest_tick_dt is None:
                    latest_tick_dt = _ensure_utc(chunk_end)
                _write_last_tick(redis_client, redis_prefix, sym_name, latest_tick_dt)

            for setup in list(pending_setups):
                if setup.as_of_utc > chunk_end:
                    continue

                state = setup_states[setup.id]
                state['chunks'] += 1
                state['ticks'] += chunk_stats.total_ticks
                state['pages'] += chunk_stats.pages
                state['fetch_s'] += chunk_stats.fetch_s

                scan_start_time = perf_counter()
                hit_candidate = _find_hit_in_tick_data(
                    tick_data,
                    setup.direction,
                    setup.sl,
                    setup.tp,
                    setup.as_of_utc,
                )
                state['scan_s'] += perf_counter() - scan_start_time

                if hit_candidate is not None and hit_candidate.time_utc <= setup.as_of_utc + timedelta(milliseconds=1):
                    hit_candidate = None

                if hit_candidate is None:
                    continue

                total_ticks = state['ticks']
                total_pages = state['pages']
                elapsed_s = state['fetch_s'] + state['scan_s']
                thr = (total_ticks / elapsed_s) if elapsed_s > 0 else 0.0
                avg_per_page = (total_ticks / total_pages) if total_pages > 0 else 0.0
                t_fetch_ms = state['fetch_s'] * 1000.0
                t_scan_ms = state['scan_s'] * 1000.0
                total_minutes = (hit_candidate.time_utc - setup.as_of_utc).total_seconds() / 60.0

                if args.verbose and state['chunks'] > 1:
                    print(
                        f"[chunks] #{setup.id} scanned {int(state['chunks'])} chunk(s) "
                        f"(max {chunk_minutes} mins each, total {total_minutes:.1f} mins)"
                    )
                if args.verbose:
                    print(
                        f"[fetch] #{setup.id} ticks={total_ticks} pages={total_pages} "
                        f"time={elapsed_s*1000:.1f}ms thr={thr:,.0f} t/s avg_pg={avg_per_page:,.1f}"
                    )

                t_store_start = perf_counter()
                record_hit_sqlite(conn, setup, hit_candidate, args.dry_run, args.verbose)
                t_store = perf_counter() - t_store_start

                if args.verbose:
                    extra = ' cache' if cache_used else ''
                    print(
                        f"[HIT TIMING] #{setup.id} fetch={t_fetch_ms:.1f}ms scan={t_scan_ms:.1f}ms "
                        f"store={t_store*1000:.1f}ms resolve={t_resolve*1000:.1f}ms{extra} | "
                        f"ticks {total_ticks} pages {total_pages}"
                    )

                hits += 1
                pending_setups.remove(setup)

            if not pending_setups:
                break

            future_starts = [s.as_of_utc for s in pending_setups if s.as_of_utc > chunk_end]
            if future_starts:
                chunk_start = min(future_starts)
            else:
                chunk_start = chunk_end

        remaining_setups: List[Setup] = []
        for setup in pending_setups:
            state = setup_states[setup.id]
            total_ticks = state['ticks']
            total_pages = state['pages']
            elapsed_s = state['fetch_s'] + state['scan_s']
            thr = (total_ticks / elapsed_s) if elapsed_s > 0 else 0.0
            avg_per_page = (total_ticks / total_pages) if total_pages > 0 else 0.0
            t_fetch_ms = state['fetch_s'] * 1000.0
            t_scan_ms = state['scan_s'] * 1000.0
            duration = (now_utc - setup.as_of_utc).total_seconds()
            if args.verbose:
                extra = ' cache' if cache_used else ''
                print(
                    f"[NO HIT] #{setup.id} {setup.symbol} {setup.direction} | "
                    f"window {duration/60:.1f} mins | ticks {total_ticks} | pages {total_pages} | "
                    f"fetch={t_fetch_ms:.1f}ms scan={t_scan_ms:.1f}ms resolve={t_resolve*1000:.1f}ms{extra}"
                )
            remaining_setups.append(setup)

        if remaining_setups:
            active_symbol_setups[symbol] = remaining_setups

        checked += len(symbol_setups_list)

    print(f'Initial check complete. Checked {checked} setup(s); hits recorded: {hits}.')
    print(f'Active symbols for real-time monitoring: {len(active_symbol_setups)}')

    return active_symbol_setups

def realtime_monitoring_loop(args: argparse.Namespace, conn, active_symbol_setups: Dict[str, List[Setup]], redis_client: Optional[Any] = None, redis_prefix: str = REDIS_DEFAULT_PREFIX) -> None:
    """
    Main real-time monitoring loop that runs every 200ms.
    """
    if not active_symbol_setups:
        print("No active symbols to monitor.")
        return

    print(f"Starting real-time monitoring for {len(active_symbol_setups)} symbols...")

    parsed_ids = _parse_ids(getattr(args, "ids", None))
    parsed_symbols = _parse_symbols(getattr(args, "symbols", None))
    refresh_enabled = parsed_ids is None
    refresh_last_ts = datetime.now(UTC)
    active_setup_ids: Set[int] = {setup.id for setups in active_symbol_setups.values() for setup in setups}

    resolve_cache: dict[str, Optional[str]] = {}
    offset_cache: Dict[str, int] = {}
    last_poll_end: Dict[str, datetime] = {}

    try:
        while True:
            loop_start = perf_counter()
            now_utc = datetime.now(UTC)

            symbols_to_remove = []
            total_checks = 0
            total_hits = 0

            if refresh_enabled and (now_utc - refresh_last_ts).total_seconds() >= REFRESH_INTERVAL_SECONDS:
                new_setups = _load_new_setups_since(conn, refresh_last_ts, parsed_symbols, active_setup_ids, args.verbose)
                if new_setups:
                    added_ids = []
                    for new_setup in new_setups:
                        if new_setup.id in active_setup_ids:
                            continue
                        bucket = active_symbol_setups.setdefault(new_setup.symbol, [])
                        bucket.append(new_setup)
                        bucket.sort(key=lambda s: s.as_of_utc)
                        active_setup_ids.add(new_setup.id)
                        added_ids.append(new_setup.id)
                    if args.verbose and added_ids:
                        ids_str = ", ".join(str(sid) for sid in added_ids)
                        print(f"[refresh] Added {len(added_ids)} setup(s) to monitoring: {ids_str}")
                refresh_last_ts = now_utc

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

                if redis_client is not None and sym_name not in last_poll_end:
                    cached_dt = _read_last_tick(redis_client, redis_prefix, sym_name)
                    if cached_dt is not None:
                        last_poll_end[sym_name] = cached_dt

                # Get server offset
                offset_h = offset_cache.get(sym_name)
                if offset_h is None:
                    offset_h = get_server_offset_hours(sym_name)
                    offset_cache[sym_name] = offset_h

                setups_to_remove = []

                prev_poll_end = last_poll_end.get(sym_name)
                if prev_poll_end is None:
                    window_start_utc = now_utc - timedelta(seconds=REALTIME_WINDOW_SECONDS)
                else:
                    window_start_utc = prev_poll_end - timedelta(seconds=REALTIME_BACKTRACK_SECONDS)
                    # If we fell behind, widen the window (up to the configured
                    # cap) so we can replay missed ticks instead of skipping
                    # straight to "now" and losing earlier hits.
                    max_lookback = timedelta(seconds=REALTIME_MAX_CATCHUP_SECONDS)
                    earliest_allowed = now_utc - max_lookback
                    if window_start_utc < earliest_allowed:
                        window_start_utc = earliest_allowed

                ticks, fetch_stats = fetch_recent_ticks(
                    sym_name,
                    offset_h,
                    now_utc,
                    window_start_utc=window_start_utc,
                    window_seconds=max(
                        REALTIME_WINDOW_SECONDS,
                        (now_utc - window_start_utc).total_seconds(),
                    ),
                    trace=args.verbose,
                )

                latest_tick_dt = _latest_tick_time_utc(ticks, offset_h)
                if latest_tick_dt is None:
                    latest_tick_dt = _ensure_utc(now_utc)

                for setup in setups:
                    total_checks += 1

                    scan_start = perf_counter()
                    hit = earliest_hit_from_ticks(
                        ticks,
                        setup.direction,
                        setup.sl,
                        setup.tp,
                        offset_h,
                    )
                    scan_elapsed = perf_counter() - scan_start

                    if hit is not None:
                        time_diff = (now_utc - hit.time_utc).total_seconds()
                        if abs(time_diff) > REALTIME_STALE_HIT_WARNING_SECONDS and args.verbose:
                            sign = 'behind' if time_diff > 0 else 'ahead'
                            print(
                                f"  [WARNING] Detected {setup.symbol} {hit.kind} tick {abs(time_diff):.1f}s {sign} current time; recording anyway"
                            )

                        t_store_start = perf_counter()
                        record_hit_sqlite(conn, setup, hit, args.dry_run, args.verbose)
                        t_store = perf_counter() - t_store_start

                        # Always print basic hit notification
                        print(
                            f"[REALTIME HIT] #{setup.id} {setup.symbol} {setup.direction} -> {hit.kind} "
                            f"at {hit.time_utc.isoformat(timespec='seconds')}"
                        )

                        if args.verbose:
                            stats = TickFetchStats(
                                pages=fetch_stats.pages,
                                total_ticks=fetch_stats.total_ticks,
                                elapsed_s=fetch_stats.elapsed_s + scan_elapsed,
                                fetch_s=fetch_stats.fetch_s,
                                early_stop=True,
                            )
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
                    active_setup_ids.discard(setup.id)

                # Remove symbol if no more setups
                if not setups:
                    symbols_to_remove.append(symbol)

                last_poll_end[sym_name] = latest_tick_dt
                if redis_client is not None:
                    _write_last_tick(redis_client, redis_prefix, sym_name, latest_tick_dt)

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

    redis_client = create_redis_client(args)
    redis_prefix = getattr(args, "redis_prefix", REDIS_DEFAULT_PREFIX)

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
            active_symbol_setups = run_initial_check(args, conn, redis_client, redis_prefix)

            # Start real-time monitoring if there are active symbols
            if active_symbol_setups:
                realtime_monitoring_loop(args, conn, active_symbol_setups, redis_client, redis_prefix)
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
