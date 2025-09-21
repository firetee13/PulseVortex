#!/usr/bin/env python3
"""Web-friendly database helpers for the Dash UI.

Provides:
- get_db_rows(since_hours) -> list[dict]  # rows suitable for dash DataTable (includes internal _meta)
- get_setup_meta(setup_id) -> dict | None  # detailed meta for a single setup (used to render charts)
- compute_pnl_series(hours) -> dict       # simplified PnL series (notional + normalized when possible)

This module re-uses the project's DB path resolution in monitor.config.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from monitor.config import db_path_str

UTC = timezone.utc


def _connect(dbname: Optional[str] = None):
    path = db_path_str(dbname)
    conn = sqlite3.connect(path, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def _fmt_price(v: Optional[float]) -> str:
    try:
        if v is None:
            return ""
        return f"{float(v):g}"
    except Exception:
        return ""


def get_db_rows(since_hours: Optional[int] = 168, dbname: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return rows for the DB Results table.

    Each returned dict contains display keys used by the DataTable and an internal '_meta'
    dict with fields useful for rendering charts:
        - setup_id, symbol, direction, entry_utc_str, entry_price, tp, sl, hit_kind, hit_time_utc_str, hit_price
    """
    conn = _connect(dbname)
    try:
        cur = conn.cursor()
        # Ensure setups table exists
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='timelapse_setups'")
        if cur.fetchone() is None:
            return []

        params: List[Any] = []
        where_clause = ""
        if since_hours is not None:
            thr = (datetime.now(UTC) - timedelta(hours=since_hours)).strftime("%Y-%m-%d %H:%M:%S")
            where_clause = "WHERE s.inserted_at >= ?"
            params.append(thr)

        sql = f"""
SELECT s.id, s.symbol, s.direction, s.inserted_at,
       h.hit_time_utc3, h.hit_time, h.hit, h.hit_price,
       s.tp, s.sl, COALESCE(h.entry_price, s.price) AS entry_price
FROM timelapse_setups s
LEFT JOIN timelapse_hits h ON h.setup_id = s.id
{where_clause}
ORDER BY s.inserted_at DESC, s.symbol
"""
        cur.execute(sql, params)
        out: List[Dict[str, Any]] = []
        for row in cur.fetchall() or []:
            sid = row["id"]
            sym = str(row["symbol"]) if row["symbol"] is not None else ""
            direction = str(row["direction"]) if row["direction"] is not None else ""
            inserted_at = row["inserted_at"]
            # Format inserted (entry) time as UTC+3 string for display
            ent_s = ""
            entry_utc_iso = ""
            try:
                if isinstance(inserted_at, str):
                    as_naive = datetime.fromisoformat(inserted_at)
                else:
                    as_naive = inserted_at
                ent_s = (as_naive + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
                entry_utc_iso = as_naive.strftime("%Y-%m-%d %H:%M:%S.%f")
            except Exception:
                ent_s = str(inserted_at or "")
                entry_utc_iso = str(inserted_at or "")

            hit_time_utc3 = row["hit_time_utc3"] or ""
            hit_time = row["hit_time"] or None
            hit = row["hit"] or ""
            tp = row["tp"]
            sl = row["sl"]
            ep = row["entry_price"]
            # Format hit_price for display
            hit_price_val = row["hit_price"]
            hit_price_display = _fmt_price(hit_price_val)
            
            display = {
                "id": sid,
                "symbol": sym,
                "direction": direction,
                "entry_utc3": ent_s,
                "hit_time_utc3": hit_time_utc3 or (str(hit_time) if hit_time else ""),
                "hit": hit,  # Add the hit column to the main display data
                "hit_price": hit_price_display,
                "tp": _fmt_price(tp),
                "sl": _fmt_price(sl),
                "entry_price": _fmt_price(ep),
                # internal meta for charts and callbacks
                "_meta": {
                    "setup_id": sid,
                    "symbol": sym,
                    "direction": direction,
                    "entry_utc_str": entry_utc_iso,
                    "entry_price": float(ep) if ep is not None else None,
                    "tp": float(tp) if tp is not None else None,
                    "sl": float(sl) if sl is not None else None,
                    "hit_kind": (str(hit) if hit else None),
                    "hit_time_utc_str": (str(hit_time) if hit_time else None),
                    "hit_price": float(hit_price_val) if hit_price_val is not None else None,
                },
            }
            out.append(display)
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_setup_meta(setup_id: int, dbname: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Return the metadata for a single setup id (None if not found)."""
    conn = _connect(dbname)
    try:
        cur = conn.cursor()
        cur.execute(
            """
SELECT s.id, s.symbol, s.direction, s.as_of, s.price, s.sl, s.tp,
       h.hit, h.hit_time, h.hit_price
FROM timelapse_setups s
LEFT JOIN timelapse_hits h ON h.setup_id = s.id
WHERE s.id = ?
""",
            (int(setup_id),),
        )
        row = cur.fetchone()
        if row is None:
            return None
        as_of = row["as_of"]
        try:
            if isinstance(as_of, str):
                as_naive = datetime.fromisoformat(as_of)
            else:
                as_naive = as_of
            entry_utc_str = as_naive.strftime("%Y-%m-%d %H:%M:%S.%f")
        except Exception:
            entry_utc_str = str(as_of or "")
        return {
            "setup_id": int(row["id"]),
            "symbol": str(row["symbol"]) if row["symbol"] is not None else "",
            "direction": str(row["direction"]) if row["direction"] is not None else "",
            "entry_utc_str": entry_utc_str,
            "entry_price": float(row["price"]) if row["price"] is not None else None,
            "tp": float(row["tp"]) if row["tp"] is not None else None,
            "sl": float(row["sl"]) if row["sl"] is not None else None,
            "hit_kind": (str(row["hit"]) if row["hit"] else None),
            "hit_time_utc_str": (str(row["hit_time"]) if row["hit_time"] else None),
            "hit_price": float(row["hit_price"]) if row["hit_price"] is not None else None,
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass


def compute_pnl_series(hours: int = 168, dbname: Optional[str] = None) -> Dict[str, Any]:
    """Compute simplified PnL series from recorded hits.

    Returns a dict with:
      - times: list[datetime]
      - norm_returns: list[float]  # placeholder; computed as profit (no ATR normalization unless available)
      - cum: cumulative list
      - avg: average per trade list
      - symbols: list[str]
      - notional_returns: list[float]
      - not_cum: cumulative notional
      - not_avg: average notional
      - error: optional str
    """
    conn = _connect(dbname)
    try:
        cur = conn.cursor()
        thr = (datetime.now(UTC) - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        sql = (
            """
            SELECT COALESCE(h.hit_time, s.inserted_at) as event_time,
                   h.hit, s.symbol, COALESCE(h.entry_price, s.price) AS entry_price,
                   h.hit_price, s.sl, s.direction
            FROM timelapse_setups s
            JOIN timelapse_hits h ON h.setup_id = s.id
            WHERE COALESCE(h.hit_time, s.inserted_at) >= ?
            ORDER BY COALESCE(h.hit_time, s.inserted_at) ASC
            """
        )
        cur.execute(sql, (thr,))
        rows = cur.fetchall() or []
        times: List[datetime] = []
        norm_returns: List[float] = []
        symbols: List[str] = []
        notional_returns: List[float] = []
        for (event_time, hit, symbol, entry_price, hit_price, sl, direction) in rows:
            # parse event_time
            dt = None
            if isinstance(event_time, str):
                try:
                    dt = datetime.fromisoformat(event_time)
                except Exception:
                    try:
                        dt = datetime.strptime(event_time.split(".")[0], "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        dt = None
            elif isinstance(event_time, datetime):
                dt = event_time
            if dt is None:
                continue
            try:
                dt = dt.replace(tzinfo=UTC)
            except Exception:
                pass
            try:
                ep = float(entry_price) if entry_price is not None else None
                hp = float(hit_price) if hit_price is not None else None
            except Exception:
                ep = None
                hp = None
            if ep is None or hp is None:
                continue
            dir_s = (str(direction) or "").lower()
            profit = (hp - ep) if dir_s == "buy" else (ep - hp)
            # Notional (10k)
            try:
                units = 10000.0 / ep if ep not in (None, 0.0) else 0.0
            except Exception:
                units = 0.0
            notional_profit = units * profit
            times.append(dt)
            norm_returns.append(profit)  # placeholder (profit); original used ATR normalization
            symbols.append(str(symbol))
            notional_returns.append(notional_profit)

        # cumulative and averages
        cum: List[float] = []
        ssum = 0.0
        for v in norm_returns:
            ssum += v
            cum.append(ssum)
        avg = [c / (i + 1) for i, c in enumerate(cum)] if cum else []

        not_cum: List[float] = []
        nsum = 0.0
        for v in notional_returns:
            nsum += v
            not_cum.append(nsum)
        not_avg = [c / (i + 1) for i, c in enumerate(not_cum)] if not_cum else []

        return {
            "times": times,
            "norm_returns": norm_returns,
            "cum": cum,
            "avg": avg,
            "symbols": symbols,
            "notional_returns": notional_returns,
            "not_cum": not_cum,
            "not_avg": not_avg,
            "error": None,
        }
    except Exception as e:
        return {"times": [], "norm_returns": [], "cum": [], "avg": [], "symbols": [], "notional_returns": [], "not_cum": [], "not_avg": [], "error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass