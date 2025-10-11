#!/usr/bin/env python3
"""
Fix incorrect RRR values in timelapse_setups table by recalculating them
"""

import sqlite3
from decimal import Decimal, getcontext

# Set precision for decimal calculations
getcontext().prec = 10


def calculate_rrr(price, sl, tp, direction):
    """
    Calculate RRR (Risk Reward Ratio) based on price, SL, TP and direction

    RRR = |TP - entry| / |SL - entry|
    """
    try:
        if direction.lower() == "buy":
            risk = price - sl
            reward = tp - price
        else:  # sell
            risk = sl - price
            reward = price - tp

        if risk == 0:
            return None

        rrr = reward / risk
        return rrr
    except (TypeError, ZeroDivisionError):
        return None


def get_decimal_places(symbol):
    """Get appropriate decimal places for a symbol"""
    # Common forex pairs typically use 4 or 5 decimal places
    # JPY pairs use 2 or 3 decimal places
    # Indices and crypto may have different conventions

    if "JPY" in symbol:
        return 3  # JPY pairs typically use 3 decimal places
    elif any(x in symbol for x in ["US500", "US30", "GER40", "UK100", "NAS100"]):
        return 1  # Indices often use 1 decimal place
    else:
        return 5  # Most other forex pairs use 5 decimal places


def fix_rrr_values():
    """Fix incorrect RRR values in the database"""

    # Connect to database
    conn = sqlite3.connect("timelapse.db")
    cursor = conn.cursor()

    # Get all records from timelapse_setups
    cursor.execute(
        """
        SELECT rowid, symbol, direction, price, sl, tp, rrr
        FROM timelapse_setups
        WHERE price IS NOT NULL AND sl IS NOT NULL AND tp IS NOT NULL
    """
    )

    records = cursor.fetchall()
    print(f"Checking {len(records)} records for RRR calculation accuracy...\n")

    records_to_fix = []

    for record in records:
        rowid, symbol, direction, price, sl, tp, stored_rrr = record

        # Convert to Decimal for precise calculations
        price = Decimal(str(price))
        sl = Decimal(str(sl))
        tp = Decimal(str(tp))

        # Calculate expected RRR
        calculated_rrr = calculate_rrr(price, sl, tp, direction)

        if calculated_rrr is not None:
            calculated_rrr = float(calculated_rrr)

            # Check if calculated matches stored (with small tolerance for floating point)
            if stored_rrr is not None:
                stored_rrr = float(stored_rrr)
                tolerance = 0.001  # Small tolerance for floating point differences

                if abs(calculated_rrr - stored_rrr) > tolerance:
                    records_to_fix.append(
                        {
                            "rowid": rowid,
                            "symbol": symbol,
                            "direction": direction,
                            "price": float(price),
                            "sl": float(sl),
                            "tp": float(tp),
                            "stored_rrr": stored_rrr,
                            "calculated_rrr": calculated_rrr,
                            "difference": abs(calculated_rrr - stored_rrr),
                        }
                    )

    print(f"Found {len(records_to_fix)} records with incorrect RRR values")

    if records_to_fix:
        # Ask for confirmation
        response = input(f"\nDo you want to fix {len(records_to_fix)} records? (y/n): ")

        if response.lower() == "y":
            # Fix the records
            fixed_count = 0

            for record in records_to_fix:
                rowid = record["rowid"]
                calculated_rrr = record["calculated_rrr"]

                # Round to appropriate decimal places
                decimal_places = get_decimal_places(record["symbol"])
                rounded_rrr = round(
                    calculated_rrr, decimal_places + 2
                )  # Extra precision for RRR

                try:
                    cursor.execute(
                        """
                        UPDATE timelapse_setups
                        SET rrr = ?
                        WHERE rowid = ?
                    """,
                        (rounded_rrr, rowid),
                    )

                    fixed_count += 1

                    if fixed_count <= 5:  # Show first 5 fixes
                        print(
                            f"Fixed RowID {rowid}: {record['symbol']} - RRR {record['stored_rrr']:.6f} → {rounded_rrr:.6f}"
                        )

                except sqlite3.Error as e:
                    print(f"Error fixing RowID {rowid}: {e}")

            conn.commit()
            print(f"\nSuccessfully fixed {fixed_count} records")
        else:
            print("No changes made")

    conn.close()


if __name__ == "__main__":
    fix_rrr_values()
