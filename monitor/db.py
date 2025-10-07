from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .domain import Hit, Setup

UTC = timezone.utc


def _table_columns(conn, table_name: str) -> Dict[str, Tuple[int, Optional[str]]]:
    """Return table columns keyed by name -> (cid, type)."""
    cur = conn.cursor()
    try:
        cur.execute(f"PRAGMA table_info('{table_name}')")
    except Exception:
        return {}
    meta = cur.fetchall() or []
    columns: Dict[str, Tuple[int, Optional[str]]] = {}
    for row in meta:
        try:
            cid = int(row[0])
            name = str(row[1])
            col_type = row[2] if len(row) > 2 else None
        except Exception:
            continue
        columns[name] = (cid, col_type if isinstance(col_type, str) else None)
    return columns


def ensure_hits_table_sqlite(conn) -> None:
    """Ensure the timelapse_hits table exists in the target SQLite connection."""
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
    """Populate denormalised columns on timelapse_hits based on the setups table."""
    with conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (setups_table,))
        has_setups = cur.fetchone() is not None
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
        cur.execute(
            f"""
            UPDATE timelapse_hits
            SET hit_time_utc3 = strftime('%Y-%m-%d %H:%M:%S', hit_time, '+{utc3_hours} hours')
            WHERE hit_time_utc3 IS NULL AND hit_time IS NOT NULL
            """
        )
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


def ensure_tp_sl_setup_state_sqlite(conn) -> None:
    """Ensure the checkpoint table used by TP/SL checks exists and carries order metadata columns."""

    with conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tp_sl_setup_state (
                setup_id INTEGER PRIMARY KEY,
                last_checked_utc TEXT NOT NULL
            )
            """
        )
        columns = _table_columns(conn, "tp_sl_setup_state")
        if "order_ticket" not in columns:
            cur.execute("ALTER TABLE tp_sl_setup_state ADD COLUMN order_ticket TEXT")
        if "order_sent_at" not in columns:
            cur.execute("ALTER TABLE tp_sl_setup_state ADD COLUMN order_sent_at TEXT")


def ensure_live_bin_filters_sqlite(conn) -> None:
    """Ensure the live_bin_filters table exists for GUI-to-CLI filter coordination."""

    with conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS live_bin_filters (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                min_edge REAL,
                min_trades INTEGER,
                updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
                allowed_bins_json TEXT,
                live_trading_enabled INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        columns = _table_columns(conn, "live_bin_filters")
        if "allowed_bins_json" not in columns:
            cur.execute("ALTER TABLE live_bin_filters ADD COLUMN allowed_bins_json TEXT")
        if "live_trading_enabled" not in columns:
            cur.execute("ALTER TABLE live_bin_filters ADD COLUMN live_trading_enabled INTEGER NOT NULL DEFAULT 0")
        # Guarantee a single row placeholder so SELECTs can rely on it
        cur.execute(
            """
            INSERT INTO live_bin_filters (id, min_edge, min_trades, updated_at, allowed_bins_json, live_trading_enabled)
            VALUES (1, NULL, NULL, CURRENT_TIMESTAMP, NULL, 0)
            ON CONFLICT(id) DO NOTHING
            """
        )


def load_live_bin_filters_sqlite(conn) -> Optional[Dict[str, Optional[object]]]:
    """Return the currently active live bin filters (min_edge, min_trades, updated_at)."""

    ensure_live_bin_filters_sqlite(conn)
    cur = conn.cursor()
    cur.execute(
        "SELECT min_edge, min_trades, updated_at, allowed_bins_json, live_trading_enabled FROM live_bin_filters WHERE id = 1"
    )
    row = cur.fetchone()
    if not row:
        return None
    min_edge, min_trades, updated_at, allowed_bins_json, live_enabled = row
    updated_dt = _parse_utc_datetime(updated_at) if updated_at else None
    allowed_bins: Dict[str, set[str]] | None = None
    if allowed_bins_json:
        try:
            data = json.loads(allowed_bins_json)
            if isinstance(data, dict):
                allowed_bins = {}
                for key, value in data.items():
                    if isinstance(value, list):
                        allowed_bins[key] = {str(v) for v in value}
        except Exception:
            allowed_bins = None
    return {
        "min_edge": float(min_edge) if min_edge is not None else None,
        "min_trades": int(min_trades) if min_trades is not None else None,
        "updated_at": updated_dt,
        "live_enabled": bool(live_enabled) if live_enabled is not None else False,
        "allowed_bins": allowed_bins,
    }


def persist_live_bin_filters_sqlite(
    conn,
    min_edge: Optional[float],
    min_trades: Optional[int],
    allowed_bins: Optional[Dict[str, Iterable[str]]] = None,
    live_trading_enabled: Optional[bool] = None,
) -> None:
    """Upsert the live bin filter thresholds used by GUI/CLI coordination."""

    ensure_live_bin_filters_sqlite(conn)
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    bins_json: Optional[str]
    if allowed_bins is not None:
        try:
            bins_json = json.dumps(
                {str(k): sorted({str(v) for v in vals}) for k, vals in allowed_bins.items()},
                sort_keys=True,
            )
        except Exception:
            bins_json = None
    else:
        bins_json = None
    enabled_val: Optional[int]
    if live_trading_enabled is None:
        enabled_val = None
    else:
        enabled_val = 1 if live_trading_enabled else 0
    with conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE live_bin_filters
               SET min_edge = ?,
                   min_trades = ?,
                   updated_at = ?,
                   allowed_bins_json = COALESCE(?, allowed_bins_json),
                   live_trading_enabled = COALESCE(?, live_trading_enabled)
             WHERE id = 1
            """,
            (
                float(min_edge) if min_edge is not None else None,
                int(min_trades) if min_trades is not None else None,
                now,
                bins_json,
                enabled_val,
            ),
        )


def load_tp_sl_setup_state_sqlite(conn, setup_ids: Iterable[int]) -> Dict[int, datetime]:
    """Return the last processed UTC timestamp for each setup id."""

    ids = list(dict.fromkeys(int(sid) for sid in setup_ids))
    if not ids:
        return {}
    placeholder = ",".join(["?"] * len(ids))
    cur = conn.cursor()
    cur.execute(
        f"SELECT setup_id, last_checked_utc FROM tp_sl_setup_state WHERE setup_id IN ({placeholder})",
        ids,
    )
    rows = cur.fetchall() or []
    out: Dict[int, datetime] = {}
    for sid, when in rows:
        if when is None:
            continue
        dt = _parse_utc_datetime(when)
        if dt is None:
            continue
        out[int(sid)] = dt
    return out


def persist_tp_sl_setup_state_sqlite(conn, entries: Dict[int, datetime]) -> None:
    """Insert or update checkpoints for the supplied setup ids."""

    if not entries:
        return
    records = []
    for sid, dt in entries.items():
        if dt is None:
            continue
        if dt.tzinfo is None:
            dt_utc = dt.replace(tzinfo=UTC)
        else:
            dt_utc = dt.astimezone(UTC)
        dt_str = dt_utc.strftime("%Y-%m-%d %H:%M:%S")
        records.append((int(sid), dt_str))
    if not records:
        return
    with conn:
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT INTO tp_sl_setup_state (setup_id, last_checked_utc)
            VALUES (?, ?)
            ON CONFLICT(setup_id) DO UPDATE SET last_checked_utc = excluded.last_checked_utc
            """,
            records,
        )


def persist_order_sent_sqlite(
    conn,
    setup_id: int,
    ticket: Optional[str],
    sent_at: Optional[datetime] = None,
    last_checked_fallback: Optional[datetime] = None,
) -> None:
    """Persist MT5 order metadata for a setup while keeping checkpoint semantics intact.

    The table requires last_checked_utc to be non-null, so we fall back to the
    supplied last_checked value (or the order send time when absent) when creating rows.
    """

    ensure_tp_sl_setup_state_sqlite(conn)
    if not isinstance(setup_id, int):
        try:
            setup_id = int(setup_id)
        except Exception:
            raise ValueError("setup_id must be convertible to int")
    sent_dt = sent_at or datetime.now(UTC)
    if sent_dt.tzinfo is None:
        sent_dt = sent_dt.replace(tzinfo=UTC)
    else:
        sent_dt = sent_dt.astimezone(UTC)
    last_checked = last_checked_fallback or sent_dt
    if last_checked.tzinfo is None:
        last_checked = last_checked.replace(tzinfo=UTC)
    else:
        last_checked = last_checked.astimezone(UTC)
    sent_str = sent_dt.strftime("%Y-%m-%d %H:%M:%S")
    last_checked_str = last_checked.strftime("%Y-%m-%d %H:%M:%S")
    with conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO tp_sl_setup_state (setup_id, last_checked_utc, order_ticket, order_sent_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(setup_id) DO UPDATE SET
                order_ticket = excluded.order_ticket,
                order_sent_at = excluded.order_sent_at,
                last_checked_utc = COALESCE(tp_sl_setup_state.last_checked_utc, excluded.last_checked_utc)
            """,
            (setup_id, last_checked_str, ticket, sent_str),
        )


def _parse_utc_datetime(value: object) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    try:
        text = str(value)
        if not text:
            return None
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def load_setups_sqlite(
    conn,
    table: str,
    since_hours: Optional[int],
    ids: Optional[Sequence[int]],
    symbols: Optional[Sequence[str]],
) -> List[Setup]:
    """Load setups from SQLite applying optional filters."""
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
        threshold = (datetime.now(UTC) - timedelta(hours=since_hours)).strftime("%Y-%m-%d %H:%M:%S")
        where.append("inserted_at >= ?")
        params.append(threshold)
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
        if isinstance(as_of, str):
            try:
                as_naive = datetime.fromisoformat(as_of)
            except Exception:
                as_naive = datetime.strptime(as_of.split('.')[0], "%Y-%m-%d %H:%M:%S")
        else:
            as_naive = as_of
        as_of_utc = as_naive.replace(tzinfo=UTC)
        if sym is None or direction is None or sl is None or tp is None:
            continue
        rows.append(
            Setup(
                id=int(sid),
                symbol=str(sym),
                direction=str(direction),
                sl=float(sl),
                tp=float(tp),
                entry_price=float(price) if price is not None else None,
                as_of_utc=as_of_utc,
            )
        )
    return rows


def record_hit_sqlite(conn, setup: Setup, hit: Hit, dry_run: bool, verbose: bool, utc3_hours: int = 3) -> None:
    """Insert or update a hit row for the supplied setup."""

    def infer_decimals_from_price(price: Optional[float]) -> int:
        try:
            if price is None:
                return 5
            s = str(float(price))
            if 'e' in s or 'E' in s:
                s = f"{float(price):.10f}"
            return len(s.split('.')[1]) if '.' in s else 0
        except Exception:
            return 5

    def instrument_digits(symbol: str, ref_price: Optional[float]) -> int:
        try:
            sym = (symbol or '').upper()
            if re.fullmatch(r"[A-Z]{6}", sym):
                quote = sym[3:]
                return 3 if quote == 'JPY' else 5
            if re.fullmatch(r"XA[UG][A-Z]{3}", sym):
                return 2
        except Exception:
            pass
        digits = infer_decimals_from_price(ref_price)
        return max(0, min(10, digits)) or 5

    digits = instrument_digits(setup.symbol, setup.entry_price if setup.entry_price is not None else hit.price)

    def r(value: Optional[float]) -> Optional[float]:
        try:
            return None if value is None else round(float(value), digits)
        except Exception:
            return value

    rounded_sl = r(setup.sl)
    rounded_tp = r(setup.tp)
    rounded_hit_price = r(hit.price)
    rounded_entry_price = r(setup.entry_price)

    if verbose:
        print(
            "[HIT] #{} {} {} -> {} at {:.6f} on {}".format(
                setup.id,
                setup.symbol,
                setup.direction,
                hit.kind,
                (rounded_hit_price if rounded_hit_price is not None else hit.price),
                hit.time_utc.isoformat(timespec='seconds'),
            )
        )
    if dry_run:
        return

    hit_time_str = hit.time_utc.strftime("%Y-%m-%d %H:%M:%S")
    hit_time_utc3 = (hit.time_utc + timedelta(hours=utc3_hours)).strftime("%Y-%m-%d %H:%M:%S")
    entry_time_utc3 = (setup.as_of_utc + timedelta(hours=utc3_hours)).strftime("%Y-%m-%d %H:%M:%S")

    with conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO timelapse_hits (
                setup_id, symbol, direction, sl, tp, hit, hit_price, hit_time, hit_time_utc3,
                entry_time_utc3, entry_price
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(setup_id) DO UPDATE SET
                sl=excluded.sl,
                tp=excluded.tp,
                hit_price=excluded.hit_price,
                hit_time_utc3=excluded.hit_time_utc3,
                entry_time_utc3=excluded.entry_time_utc3,
                entry_price=excluded.entry_price,
                checked_at=CURRENT_TIMESTAMP
            """,
            (
                setup.id,
                setup.symbol,
                setup.direction,
                rounded_sl,
                rounded_tp,
                hit.kind,
                rounded_hit_price,
                hit_time_str,
                hit_time_utc3,
                entry_time_utc3,
                rounded_entry_price,
            ),
        )


def load_recorded_ids_sqlite(conn, setup_ids: Sequence[int]) -> set[int]:
    """Return setup ids that already have entries in timelapse_hits."""
    if not setup_ids:
        return set()
    cur = conn.cursor()
    placeholders = ",".join(["?"] * len(setup_ids))
    cur.execute(
        f"SELECT setup_id FROM timelapse_hits WHERE setup_id IN ({placeholders})",
        tuple(setup_ids),
    )
    return {int(row[0]) for row in (cur.fetchall() or []) if row and row[0] is not None}
