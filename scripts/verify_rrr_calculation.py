#!/usr/bin/env python3
"""
Verify if RRR values in timelapse_setups are correctly calculated
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
        if direction.lower() == 'buy':
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

def verify_rrr_values():
    """Verify RRR calculations in the database"""

    # Connect to database
    conn = sqlite3.connect('timelapse.db')
    cursor = conn.cursor()

    # Get all records from timelapse_setups
    cursor.execute("""
        SELECT rowid, symbol, direction, price, sl, tp, rrr
        FROM timelapse_setups
        WHERE price IS NOT NULL AND sl IS NOT NULL AND tp IS NOT NULL
    """)

    records = cursor.fetchall()
    print(f"Checking {len(records)} records for RRR calculation accuracy...\n")

    mismatched_count = 0
    mismatched_records = []

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
                    mismatched_count += 1
                    mismatched_records.append({
                        'rowid': rowid,
                        'symbol': symbol,
                        'direction': direction,
                        'price': float(price),
                        'sl': float(sl),
                        'tp': float(tp),
                        'stored_rrr': stored_rrr,
                        'calculated_rrr': calculated_rrr,
                        'difference': abs(calculated_rrr - stored_rrr)
                    })

    # Print results
    if mismatched_count == 0:
        print("✅ All RRR values are correctly calculated!")
    else:
        print(f"❌ Found {mismatched_count} records with incorrect RRR values:\n")

        # Show first 10 mismatched records
        for i, record in enumerate(mismatched_records[:10]):
            print(f"Record {i+1}:")
            print(f"  RowID: {record['rowid']}")
            print(f"  Symbol: {record['symbol']} ({record['direction']})")
            print(f"  Price: {record['price']}, SL: {record['sl']}, TP: {record['tp']}")
            print(f"  Stored RRR: {record['stored_rrr']:.6f}")
            print(f"  Calculated RRR: {record['calculated_rrr']:.6f}")
            print(f"  Difference: {record['difference']:.6f}\n")

        if mismatched_count > 10:
            print(f"... and {mismatched_count - 10} more records with incorrect RRR values")

    conn.close()
    return mismatched_count == 0

if __name__ == "__main__":
    is_correct = verify_rrr_values()
    exit(0 if is_correct else 1)