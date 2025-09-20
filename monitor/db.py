from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Sequence

from .domain import Hit, Setup

UTC = timezone.utc


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
