#!/usr/bin/env python3
"""
Script to round decimals in the restore table to match the precision used in timelapse_setups table.

This script will:
1. Read all records from the restore table
2. Apply the same rounding logic as timelapse_setups.py
3. Update the restore table with rounded values

Usage:
    python round_restore_table.py [--dry-run]
"""

import argparse
import os
import sqlite3
import sys
from typing import Optional, Tuple

# Add monitor directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "monitor"))

from monitor.core.config import default_db_path


def _infer_decimals_from_price(price: Optional[float]) -> int:
    """Infer decimal places from a price value by inspecting its string form.

    Falls back to 5 when not inferable.
    """
    try:
        if price is None:
            return 5
        import math

        value = float(price)
        if not math.isfinite(value):
            return 5
        s = f"{value:.10f}".rstrip("0").rstrip(".")
        if "." in s:
            return max(0, min(10, len(s.split(".")[1])))
        return 0
    except Exception:
        return 5


def _symbol_digits(symbol: str, price: Optional[float]) -> int:
    """Resolve desired decimal places for a symbol.

    - Prefer MT5 symbol_info().digits when available
    - Fallback: infer from the actual price value
    - Default: 5
    """
    try:
        # Try to infer from symbol pattern first (without MT5 dependency)
        import re

        sym = (symbol or "").upper()
        if re.fullmatch(r"[A-Z]{6}", sym):
            quote = sym[3:]
            return 3 if quote == "JPY" else 5
        if re.fullmatch(r"XA[UG][A-Z]{3}", sym):
            return 2
    except Exception:
        pass

    # Fallback to price-based inference
    d = _infer_decimals_from_price(price)
    return d if 0 <= d <= 10 else 5


def _round_to(v: Optional[float], ndigits: int) -> Optional[float]:
    """Round value to specified decimal places."""
    try:
        return None if v is None else round(float(v), int(max(0, min(10, ndigits))))
    except Exception:
        return v


def get_precision_digits(symbol: str, price: Optional[float]) -> int:
    """Get the precision digits for SL/TP rounding based on symbol and price."""
    symbol_digits = _symbol_digits(symbol, price)

    # Convert price to rounded value first, then infer from it
    price_out = _round_to(price, symbol_digits)

    precision_digits = symbol_digits
    if price_out is not None:
        inferred = _infer_decimals_from_price(price_out)
        if 0 <= inferred <= 10:
            precision_digits = inferred

    return precision_digits


def round_restore_values(
    conn: sqlite3.Connection, dry_run: bool = False
) -> Tuple[int, int]:
    """
    Round values in the restore table to match timelapse_setups precision.

    Returns:
        Tuple of (total_records, updated_records)
    """
    cur = conn.cursor()

    # Check if restore table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='restore'")
    if cur.fetchone() is None:
        print("Error: restore table not found in database")
        return 0, 0

    # Get all records from restore table
    cur.execute(
        "SELECT id, symbol, price, sl, tp, rrr, score, proximity_to_sl FROM restore"
    )
    records = cur.fetchall()

    if not records:
        print("No records found in restore table")
        return 0, 0

    total_records = len(records)
    updated_records = 0

    print(f"Processing {total_records} records from restore table...")

    for record in records:
        try:
            record_id = record[0]
            symbol = record[1]
            price = None if record[2] is None else float(record[2])
            sl = None if record[3] is None else float(record[3])
            tp = None if record[4] is None else float(record[4])
            rrr = None if record[5] is None else float(record[5])
            score = None if record[6] is None else float(record[6])
            proximity_to_sl = None if record[7] is None else float(record[7])

            # Get precision digits for SL/TP (same logic as timelapse_setups.py)
            precision_digits = get_precision_digits(symbol, price)

            # Apply rounding (same logic as timelapse_setups.py lines 1246-1250)
            sl_out = _round_to(sl, precision_digits)
            tp_out = _round_to(tp, precision_digits)
            score_out = _round_to(score, 1)
            prox_out = _round_to(proximity_to_sl, 5)
            rrr_out = _round_to(rrr, 5) if rrr is not None else None

            # Check if any values need updating
            needs_update = (
                sl_out != sl
                or tp_out != tp
                or rrr_out != rrr
                or score_out != score
                or prox_out != proximity_to_sl
            )

            if needs_update:
                if dry_run:
                    print(f"[DRY RUN] Would update record {record_id} ({symbol}):")
                    print(f"  sl: {sl} -> {sl_out}")
                    print(f"  tp: {tp} -> {tp_out}")
                    print(f"  rrr: {rrr} -> {rrr_out}")
                    print(f"  score: {score} -> {score_out}")
                    print(f"  proximity_to_sl: {proximity_to_sl} -> {prox_out}")
                else:
                    cur.execute(
                        """
                        UPDATE restore
                        SET sl = ?, tp = ?, rrr = ?, score = ?, proximity_to_sl = ?
                        WHERE id = ?
                        """,
                        (sl_out, tp_out, rrr_out, score_out, prox_out, record_id),
                    )
                    print(f"Updated record {record_id} ({symbol})")

                updated_records += 1

        except Exception as e:
            print(f"Error processing record {record[0]}: {e}")
            continue

    if not dry_run and updated_records > 0:
        conn.commit()
        print(f"\nSuccessfully updated {updated_records} records")
    elif dry_run:
        print(f"\n[DRY RUN] Would update {updated_records} records")

    return total_records, updated_records


def main():
    parser = argparse.ArgumentParser(
        description="Round decimals in restore table to match timelapse_setups precision"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be updated without making changes",
    )
    parser.add_argument(
        "--db", default=str(default_db_path()), help="Path to SQLite database file"
    )

    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"Error: Database file not found: {args.db}")
        sys.exit(1)

    conn = None
    try:
        conn = sqlite3.connect(args.db)
        total, updated = round_restore_values(conn, args.dry_run)
        print(f"\nSummary: {updated}/{total} records would be updated")

        if not args.dry_run and updated > 0:
            print("Rounding completed successfully!")
        elif args.dry_run:
            print("Dry run completed. Use without --dry-run to apply changes.")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
