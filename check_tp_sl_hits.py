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
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from time import perf_counter
import time
from typing import Iterable, List, Optional, Sequence, Tuple
import sys
import os

try:
    import MetaTrader5 as mt5  # type: ignore
except Exception as e:  # pragma: no cover
    print("ERROR: Failed to import MetaTrader5. Install with: pip install MetaTrader5\n"
          f"Details: {e}")
    sys.exit(1)

# SQLite DB
try:
    import sqlite3  # type: ignore
except Exception:
    sqlite3 = None  # type: ignore


UTC = timezone.utc
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_SQLITE_PATH = os.path.join(SCRIPT_DIR, "timelapse.db")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Check TP/SL hits for timelapse setups via MT5 ticks (SQLite)")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--since-hours", type=int, default=24, help="Check setups inserted in the last N hours")
    g.add_argument("--ids", help="Comma-separated setup IDs to check (overrides --since-hours)")
    p.add_argument("--symbols", help="Optional comma-separated symbols filter (e.g., BTCUSD,SOLUSD)")
    p.add_argument("--max-mins", type=int, default=24*60, help="Safety limit: max minutes of history to scan (default 1440)")
    p.add_argument("--page", type=int, default=200000, help="Ignored: range fetch is used (kept for compatibility)")
    p.add_argument("--trace-pages", action="store_true", help="When verbose, print per-page tick fetch timings")
    p.add_argument("--db", dest="db", default=DEFAULT_SQLITE_PATH, help="Path to SQLite DB file (default: timelapse.db next to script)")
    p.add_argument("--dry-run", action="store_true", help="Do not write hit records to DB")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--watch", action="store_true", help="Run continuously, polling every --interval seconds")
    p.add_argument("--interval", type=int, default=60, help="Polling interval in seconds for --watch mode (default 60)")
    return p.parse_args()


def init_mt5(path: Optional[str] = None) -> None:
    ok = mt5.initialize(path) if path else mt5.initialize()
    if not ok:
        raise RuntimeError(f"mt5.initialize failed: {mt5.last_error()}")


def shutdown_mt5() -> None:
    try:
        mt5.shutdown()
    except Exception:
        pass


def resolve_symbol(base: str) -> Optional[str]:
    if mt5.symbol_select(base, True):
        return base
    try:
        cands = mt5.symbols_get(f"{base}*") or []
    except Exception:
        cands = []
    best: Optional[Tuple[int, str]] = None
    for s in cands:
        name = getattr(s, "name", None)
        if not name:
            continue
        score = 0
        if getattr(s, "visible", False):
            score -= 10
        score += len(name)
        if best is None or score < best[0]:
            best = (score, name)
    if best is not None:
        chosen = best[1]
        if mt5.symbol_select(chosen, True):
            return chosen
    return None


@dataclass
class Setup:
    id: int
    symbol: str
    direction: str
    sl: float
    tp: float
    entry_price: Optional[float]
    as_of_utc: datetime  # naive UTC in DB, here as aware UTC


def db_path_from_args(args) -> str:
    # Choose DB path, default to timelapse.db in script directory
    cand = getattr(args, 'db', None)
    if isinstance(cand, str) and cand.strip():
        return cand.strip()
    return DEFAULT_SQLITE_PATH


def ensure_hits_table_sqlite(conn) -> None:
    with conn:
        cur = conn.cursor()
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


def backfill_hit_columns_sqlite(conn, setups_table: str, utc3_hours: int = 3) -> None:
    with conn:
        cur = conn.cursor()
        # Determine if setups table exists
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (setups_table,))
        has_setups = cur.fetchone() is not None
        # entry_time_utc3 from setups.as_of (+3h) if setups table exists
        if has_setups:
            cur.execute(
                f"""
                UPDATE timelapse_hits
                SET entry_time_utc3 = (
                    SELECT strftime('%Y-%m-%d %H:%M:%S', s.as_of, '+{utc3_hours} hours')
                    FROM {setups_table} s WHERE s.id = timelapse_hits.setup_id
                )
                WHERE entry_time_utc3 IS NULL
                """
            )
        # hit_time_utc3 from hit_time (independent of setups)
        cur.execute(
            f"""
            UPDATE timelapse_hits
            SET hit_time_utc3 = strftime('%Y-%m-%d %H:%M:%S', hit_time, '+{utc3_hours} hours')
            WHERE hit_time_utc3 IS NULL AND hit_time IS NOT NULL
            """
        )
        # entry_price from setups.price if setups table exists
        if has_setups:
            cur.execute(
                f"""
                UPDATE timelapse_hits
                SET entry_price = (
                    SELECT s.price FROM {setups_table} s WHERE s.id = timelapse_hits.setup_id
                )
                WHERE entry_price IS NULL
                """
            )


def load_setups_sqlite(conn, table: str, since_hours: Optional[int], ids: Optional[Sequence[int]], symbols: Optional[Sequence[str]]) -> List[Setup]:
    # If the setups table does not exist yet, return no rows gracefully
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    if cur.fetchone() is None:
        return []

    where = []
    params: List[object] = []
    if ids:
        placeholders = ",".join(["?"] * len(ids))
        where.append(f"id IN ({placeholders})")
        params.extend([int(x) for x in ids])
    elif since_hours is not None:
        # compute threshold in Python to avoid string concatenation in SQL
        from datetime import datetime, timedelta, timezone
        thr = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).strftime("%Y-%m-%d %H:%M:%S")
        where.append("inserted_at >= ?")
        params.append(thr)
    if symbols:
        placeholders = ",".join(["?"] * len(symbols))
        where.append(f"symbol IN ({placeholders})")
        params.extend(list(symbols))
    where_clause = (" WHERE " + " AND ".join(where)) if where else ""
    sql = f"SELECT id, symbol, direction, sl, tp, price, as_of FROM {table}{where_clause} ORDER BY id"
    rows: List[Setup] = []
    cur.execute(sql, params)
    for (sid, sym, direction, sl, tp, price, as_of) in cur.fetchall() or []:
        if as_of is None:
            continue
        # as_of stored as ISO text; parse and attach UTC tz
        if isinstance(as_of, str):
            try:
                as_naive = datetime.fromisoformat(as_of)
            except Exception:
                # fallback: try slicing
                as_naive = datetime.strptime(as_of.split('.')[0], "%Y-%m-%d %H:%M:%S")
        else:
            as_naive = as_of
        as_of_utc = as_naive.replace(tzinfo=UTC)
        if sym is None or direction is None or sl is None or tp is None:
            continue
        rows.append(Setup(
            id=int(sid), symbol=str(sym), direction=str(direction), sl=float(sl), tp=float(tp), entry_price=(float(price) if price is not None else None), as_of_utc=as_of_utc
        ))
    return rows


def load_recorded_ids_sqlite(conn, setup_ids: Sequence[int]) -> set[int]:
    if not setup_ids:
        return set()
    cur = conn.cursor()
    placeholders = ",".join(["?"] * len(setup_ids))
    cur.execute(f"SELECT setup_id FROM timelapse_hits WHERE setup_id IN ({placeholders})", tuple(setup_ids))
    return {int(r[0]) for r in (cur.fetchall() or []) if r and r[0] is not None}


def get_server_offset_hours(symbol_for_probe: str) -> int:
    """Infer whole-hour server offset using latest tick time vs now UTC.

    Returns 0 if within +/-10 minutes or on any error.
    Positive means server time ahead of UTC (e.g., +3).
    """
    t = mt5.symbol_info_tick(symbol_for_probe)
    if t is None:
        return 0
    try:
        ts = float(getattr(t, "time_msc", 0) or 0) / 1000.0
        if ts == 0:
            ts = float(getattr(t, "time", 0) or 0)
        dt_raw = datetime.fromtimestamp(ts, tz=UTC)
        now_utc = datetime.now(UTC)
        diff_hours = (dt_raw - now_utc).total_seconds() / 3600.0
        if abs(diff_hours) <= (10.0/60.0):
            return 0
        est = int(round(diff_hours))
        if -12 <= est <= 12:
            return est
    except Exception:
        pass
    return 0


def to_server_naive(dt_utc: datetime, offset_hours: int) -> datetime:
    """Convert UTC datetime to a naive datetime that maps to server-local time.

    The MetaTrader5 API interprets naive datetimes in the host's local zone when
    converting to epoch. We want an epoch that equals (UTC + server_offset).
    So compute the target epoch explicitly and then create a naive local datetime
    from that epoch.
    """
    target_epoch = dt_utc.timestamp() + (offset_hours * 3600.0)
    return datetime.fromtimestamp(target_epoch)


def epoch_to_server_naive(epoch_seconds: float, offset_hours: int) -> datetime:
    """Convert a UNIX epoch seconds to a naive datetime representing server-local time.

    From our observations, MT5 expects naive datetimes in the server's local clock.
    We therefore add the server offset before constructing the naive datetime.
    """
    return datetime.fromtimestamp(epoch_seconds + (offset_hours * 3600.0))


@dataclass
class TickFetchStats:
    pages: int
    total_ticks: int
    elapsed_s: float
    fetch_s: float
    early_stop: bool


def ticks_paged(symbol: str, start_server_naive: datetime, end_server_naive: datetime, page: int, trace: bool = False, server_offset_hours: int = 0) -> Tuple[List[object], TickFetchStats]:
    """Fetch ticks from start..end (server-local naive) using copy_ticks_from (no early exit)."""
    t0 = perf_counter()
    all_ticks: List[object] = []
    cur = start_server_naive
    pages = 0
    fetch_s = 0.0
    while True:
        call_t0 = perf_counter()
        chunk = mt5.copy_ticks_from(symbol, cur, page, mt5.COPY_TICKS_ALL)
        call_dt = perf_counter() - call_t0
        fetch_s += call_dt
        n = 0 if chunk is None else len(chunk)
        if trace:
            cur_str = cur.isoformat(sep=' ', timespec='seconds')
            print(f"    [ticks] page {pages+1} start={cur_str} -> got {n} ticks in {call_dt*1000:.1f} ms")
        if chunk is None or n == 0:
            break
        all_ticks.extend(chunk)
        pages += 1
        # Next start = last tick time + 1 ms
        last = chunk[-1]
        try:
            tms = getattr(last, 'time_msc', None)
            if tms is None:
                # numpy struct access
                tms = int(last['time_msc']) if isinstance(last, dict) else last['time_msc']
            next_ts = (int(tms) + 1) / 1000.0
        except Exception:
            # Fallback to seconds
            try:
                tse = getattr(last, 'time', None)
                if tse is None:
                    tse = int(last['time']) if isinstance(last, dict) else last['time']
                next_ts = int(tse) + 1
            except Exception:
                break
        cur = epoch_to_server_naive(next_ts, server_offset_hours)
        if cur > end_server_naive:
            break
    elapsed = perf_counter() - t0
    return all_ticks, TickFetchStats(pages=pages, total_ticks=len(all_ticks), elapsed_s=elapsed, fetch_s=fetch_s, early_stop=False)


def ticks_range_all(symbol: str, start_server_naive: datetime, end_server_naive: datetime, trace: bool = False) -> Tuple[List[object], TickFetchStats]:
    """Fetch all ticks for [start, end] using copy_ticks_range in a single call."""
    t0 = perf_counter()
    call_t0 = perf_counter()
    ticks = mt5.copy_ticks_range(symbol, start_server_naive, end_server_naive, mt5.COPY_TICKS_ALL)
    call_dt = perf_counter() - call_t0
    n = 0 if ticks is None else len(ticks)
    if trace:
        print(f"    [ticks-range] {n} ticks in {call_dt*1000:.1f} ms")
    elapsed = perf_counter() - t0
    # Avoid numpy truth-value ambiguity; pass through the array when present
    ticks_out = ticks if ticks is not None else []
    return ticks_out, TickFetchStats(pages=1 if n > 0 else 0, total_ticks=n, elapsed_s=elapsed, fetch_s=call_dt, early_stop=False)


def scan_ticks_paged_for_hit(
    symbol: str,
    start_server_naive: datetime,
    end_server_naive: datetime,
    page: int,
    direction: str,
    sl: float,
    tp: float,
    server_offset_hours: int,
    trace: bool = False,
) -> Tuple[Optional[Hit], TickFetchStats]:
    """Fetch ticks page-by-page and stop as soon as a hit is detected.

    Returns (hit|None, stats). stats.elapsed_s includes fetch+scan time; stats.fetch_s is sum of API time.
    """
    pages = 0
    total_ticks = 0
    t0 = perf_counter()
    fetch_s = 0.0
    cur = start_server_naive
    while True:
        call_t0 = perf_counter()
        chunk = mt5.copy_ticks_from(symbol, cur, page, mt5.COPY_TICKS_ALL)
        call_dt = perf_counter() - call_t0
        fetch_s += call_dt
        n = 0 if chunk is None else len(chunk)
        if trace:
            cur_str = cur.isoformat(sep=' ', timespec='seconds')
            print(f"    [ticks] page {pages+1} start={cur_str} -> got {n} ticks in {call_dt*1000:.1f} ms")
        if chunk is None or n == 0:
            break
        pages += 1
        total_ticks += n
        # Scan this chunk for earliest hit
        hit = earliest_hit_from_ticks(chunk, direction, sl, tp, server_offset_hours)
        if hit is not None:
            elapsed = perf_counter() - t0
            return hit, TickFetchStats(pages=pages, total_ticks=total_ticks, elapsed_s=elapsed, fetch_s=fetch_s, early_stop=True)

        # Advance to next start
        last = chunk[-1]
        try:
            tms = getattr(last, 'time_msc', None)
            if tms is None:
                tms = int(last['time_msc']) if isinstance(last, dict) else last['time_msc']
            next_ts = (int(tms) + 1) / 1000.0
        except Exception:
            try:
                tse = getattr(last, 'time', None)
                if tse is None:
                    tse = int(last['time']) if isinstance(last, dict) else last['time']
                next_ts = int(tse) + 1
            except Exception:
                break
        cur = epoch_to_server_naive(next_ts, server_offset_hours)
        if cur > end_server_naive:
            break

    elapsed = perf_counter() - t0
    return None, TickFetchStats(pages=pages, total_ticks=total_ticks, elapsed_s=elapsed, fetch_s=fetch_s, early_stop=False)


@dataclass
class Hit:
    kind: str  # 'TP' or 'SL'
    time_utc: datetime
    price: float


def earliest_hit_from_ticks(ticks: Sequence[object], direction: str, sl: float, tp: float, server_offset_hours: int) -> Optional[Hit]:
    if ticks is None:
        return None
    # Handle numpy arrays and Python sequences safely
    try:
        n = len(ticks)  # works for lists and numpy arrays
    except Exception:
        try:
            n = int(getattr(ticks, 'size', 0))
        except Exception:
            n = 0
    if n == 0:
        return None
    # Iterate in chronological order
    for tk in ticks:
        # numpy structured arrays are indexable by field name
        try:
            bid = float(getattr(tk, 'bid'))
        except Exception:
            bid = float(tk['bid'])
        try:
            ask = float(getattr(tk, 'ask'))
        except Exception:
            ask = float(tk['ask'])
        # Time
        try:
            tms = getattr(tk, 'time_msc')
        except Exception:
            tms = tk['time_msc'] if 'time_msc' in tk.dtype.names else None  # type: ignore
        if tms:
            dt_raw = datetime.fromtimestamp(float(tms)/1000.0, tz=UTC)
        else:
            try:
                tse = getattr(tk, 'time')
            except Exception:
                tse = tk['time']  # type: ignore
            dt_raw = datetime.fromtimestamp(float(tse), tz=UTC)
        dt_utc = dt_raw - timedelta(hours=server_offset_hours)

        if direction.lower() == 'buy':
            # First check SL to be conservative on jumps crossing both
            if bid <= sl:
                return Hit(kind='SL', time_utc=dt_utc, price=bid)
            if bid >= tp:
                return Hit(kind='TP', time_utc=dt_utc, price=bid)
        else:  # sell
            if ask >= sl:
                return Hit(kind='SL', time_utc=dt_utc, price=ask)
            if ask <= tp:
                return Hit(kind='TP', time_utc=dt_utc, price=ask)
    return None


def record_hit_sqlite(conn, setup: Setup, hit: Hit, dry_run: bool, verbose: bool, utc3_hours: int = 3) -> None:
    if verbose:
        print(f"[HIT] #{setup.id} {setup.symbol} {setup.direction} -> {hit.kind} at {hit.price:.6f} on {hit.time_utc.isoformat(timespec='seconds')}")
    if dry_run:
        return
    dt_hit_utc3 = (hit.time_utc + timedelta(hours=utc3_hours)).strftime("%Y-%m-%d %H:%M:%S")
    dt_entry_utc3 = (setup.as_of_utc + timedelta(hours=utc3_hours)).strftime("%Y-%m-%d %H:%M:%S")
    hit_time = hit.time_utc.strftime("%Y-%m-%d %H:%M:%S")
    with conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO timelapse_hits
               (setup_id, symbol, direction, sl, tp, hit, hit_price, hit_time, hit_time_utc3, entry_time_utc3, entry_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(setup_id) DO UPDATE SET
               hit_time_utc3=excluded.hit_time_utc3,
               entry_time_utc3=excluded.entry_time_utc3,
               entry_price=excluded.entry_price
            """,
            (setup.id, setup.symbol, setup.direction, setup.sl, setup.tp,
             hit.kind, hit.price, hit_time, dt_hit_utc3, dt_entry_utc3, setup.entry_price)
        )


def already_recorded(conn, schema: str, setup_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM {schema}.timelapse_hits WHERE setup_id = %s", (setup_id,))
        return cur.fetchone() is not None


def run_once(args) -> None:
    # Parse filters each run (allows static args while looping)
    ids: Optional[List[int]] = None
    if args.ids:
        try:
            ids = [int(x.strip()) for x in args.ids.split(',') if x.strip()]
        except Exception:
            print("Invalid --ids value. Use comma-separated integers.")
            sys.exit(2)
    symbols: Optional[List[str]] = None
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(',') if s.strip()]

    # DB connect and fetch setups (SQLite only)
    t0 = perf_counter()
    if sqlite3 is None:
        print("ERROR: sqlite3 not available.")
        sys.exit(2)
    db_path = db_path_from_args(args)
    conn = sqlite3.connect(db_path, timeout=5)
    db_conn_s = perf_counter() - t0
    try:
        ensure_hits_table_sqlite(conn)
        backfill_hit_columns_sqlite(conn, 'timelapse_setups')
        t1 = perf_counter()
        setups = load_setups_sqlite(conn, 'timelapse_setups', None if ids else args.since_hours, ids, symbols)
        db_load_s = perf_counter() - t1
        if not setups:
            print("No setups to check.")
            return

        # MT5
        t2 = perf_counter()
        init_mt5()
        mt5_init_s = perf_counter() - t2
        try:
            # Resolve symbols and infer server offset once (from first resolved)
            # Different brokers in same terminal should share offset.
            first_sym_name: Optional[str] = None
            t3 = perf_counter()
            for s in setups:
                sym_name = resolve_symbol(s.symbol)
                if sym_name is None:
                    print(f"Symbol '{s.symbol}' not found/selected; skipping setup #{s.id}.")
                    continue
                first_sym_name = sym_name
                break
            mt5_resolve_first_s = perf_counter() - t3
            if first_sym_name is None:
                print("None of the symbols could be resolved in MT5.")
                return
            t4 = perf_counter()
            offset_h = get_server_offset_hours(first_sym_name)
            mt5_offset_s = perf_counter() - t4
            if args.verbose:
                sign = '+' if offset_h >= 0 else '-'
                print(f"Detected server offset: {sign}{abs(offset_h)}h")
                print(f"[timing] db_conn={db_conn_s*1000:.1f}ms db_load={db_load_s*1000:.1f}ms mt5_init={mt5_init_s*1000:.1f}ms resolve_first={mt5_resolve_first_s*1000:.1f}ms detect_offset={mt5_offset_s*1000:.1f}ms")

            now_utc = datetime.now(UTC)

            checked = 0
            hits = 0
            # Preload recorded ids to avoid per-setup DB roundtrip
            id_list = [s.id for s in setups]
            t_recload = perf_counter()
            recorded = load_recorded_ids_sqlite(conn, id_list)
            t_recload = perf_counter() - t_recload
            if args.verbose:
                print(f"[timing] preload_recorded={t_recload*1000:.1f}ms (records: {len(recorded)})")

            # Cache resolved symbol names
            resolve_cache: dict[str, Optional[str]] = {}

            for setup in setups:
                if setup.id in recorded:
                    if args.verbose:
                        print(f"Setup #{setup.id} already recorded; skipping.")
                    continue

                # Resolve actual symbol in MT5
                t_resolve = perf_counter()
                sym_name = resolve_cache.get(setup.symbol)
                cache_used = True
                if sym_name is None:
                    cache_used = False
                    sym_name = resolve_symbol(setup.symbol)
                    resolve_cache[setup.symbol] = sym_name
                t_resolve = perf_counter() - t_resolve
                if sym_name is None:
                    print(f"Symbol '{setup.symbol}' not found; skipping setup #{setup.id}.")
                    continue
                if args.verbose and not cache_used and sym_name != setup.symbol:
                    print(f"[resolve] '{setup.symbol}' -> '{sym_name}'")

                # Limit scan window by --max-mins
                start_utc = setup.as_of_utc
                if (now_utc - start_utc) > timedelta(minutes=args.max_mins):
                    if args.verbose:
                        full_mins = (now_utc - start_utc).total_seconds() / 60.0
                        print(f"[clip] #{setup.id} full window {full_mins:.1f} mins exceeds --max-mins={args.max_mins}; clipping")
                    start_utc = now_utc - timedelta(minutes=args.max_mins)

                start_server = to_server_naive(start_utc, offset_h)
                end_server = to_server_naive(now_utc, offset_h)
                if args.verbose:
                    print(f"[window] #{setup.id} {setup.symbol} {setup.direction} | UTC {start_utc.isoformat(timespec='seconds')} -> {now_utc.isoformat(timespec='seconds')} | server-naive {start_server.isoformat(sep=' ', timespec='seconds')} -> {end_server.isoformat(sep=' ', timespec='seconds')}")

                # Fetch all via range (single call), then scan
                t_fetch = perf_counter()
                ticks, stats = ticks_range_all(sym_name, start_server, end_server, trace=(args.verbose and args.trace_pages))
                t_fetch = perf_counter() - t_fetch
                t_scan = perf_counter()
                hit = earliest_hit_from_ticks(ticks, setup.direction, setup.sl, setup.tp, offset_h)
                t_scan = perf_counter() - t_scan

                # Ignore hits that occur at or before the setup's as_of (entry) timestamp.
                # Some data sources produce ticks stamped at the same instant as the snapshot;
                # recording these as immediate SL/TP hits results in "SL hit at entry time".
                # Apply a small epsilon (1 ms) to tolerate clock granularity.
                if hit is not None:
                    try:
                        if hit.time_utc <= setup.as_of_utc + timedelta(milliseconds=1):
                            if args.verbose:
                                print(f"[IGNORED HIT] #{setup.id} {setup.symbol} {setup.direction} -> {hit.kind} at {hit.time_utc.isoformat()} (<= as_of)")
                            hit = None
                    except Exception:
                        # If comparison fails for any reason, keep the original hit to avoid hiding valid results.
                        pass

                # Derive metrics for logging
                stats.fetch_s = t_fetch
                stats.elapsed_s = t_fetch + t_scan
                thr = (stats.total_ticks / stats.elapsed_s) if stats.elapsed_s > 0 else 0.0
                avg_per_page = (stats.total_ticks / stats.pages) if stats.pages > 0 else 0.0
                t_fetch_ms = stats.fetch_s * 1000.0
                t_scan_ms = max(0.0, (stats.elapsed_s - stats.fetch_s) * 1000.0)
                if args.verbose:
                    print(f"[fetch] #{setup.id} ticks={stats.total_ticks} pages={stats.pages} time={stats.elapsed_s*1000:.1f}ms thr={thr:,.0f} t/s avg_pg={avg_per_page:,.1f}")

                checked += 1
                if hit is None:
                    if args.verbose:
                        dur = (now_utc - setup.as_of_utc).total_seconds()
                        extra = " cache" if cache_used else ""
                        print(f"[NO HIT] #{setup.id} {setup.symbol} {setup.direction} | window {dur/60:.1f} mins | ticks {stats.total_ticks} | pages {stats.pages} | fetch={t_fetch_ms:.1f}ms scan={t_scan_ms:.1f}ms resolve={t_resolve*1000:.1f}ms{extra}")
                    continue

                t_store = perf_counter()
                record_hit_sqlite(conn, setup, hit, args.dry_run, args.verbose)
                t_store = perf_counter() - t_store
                if args.verbose:
                    extra = " cache" if cache_used else ""
                    print(f"[HIT TIMING] #{setup.id} fetch={t_fetch_ms:.1f}ms scan={t_scan_ms:.1f}ms store={t_store*1000:.1f}ms resolve={t_resolve*1000:.1f}ms{extra} | ticks {stats.total_ticks} pages {stats.pages}")
                hits += 1

            print(f"Checked {checked} setup(s); hits recorded: {hits}.")
        finally:
            shutdown_mt5()
    finally:
        try:
            conn.close()
        except Exception:
            pass


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
