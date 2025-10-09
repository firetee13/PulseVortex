#!/usr/bin/env python3
"""
Find rows in timelapse_setups with the same symbol and proximity_bin
that are not present in timelapse_hits table.
"""

import sqlite3
from monitor.core.config import default_db_path

def find_missing_hits():
    """Find setups that don't have corresponding hits."""
    db_path = str(default_db_path())

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # First check if proximity_bin column exists
        cursor.execute("PRAGMA table_info(timelapse_setups)")
        columns = [row[1] for row in cursor.fetchall()]
        has_proximity_bin = 'proximity_bin' in columns

        if not has_proximity_bin:
            print("proximity_bin column does not exist in timelapse_setups table")
            return

        # Query to find setups with same symbol and proximity_bin that don't have hits
        query = """
        SELECT
            t1.id,
            t1.symbol,
            t1.direction,
            t1.proximity_bin,
            COUNT(*) as count
        FROM timelapse_setups t1
        LEFT JOIN timelapse_hits h ON t1.id = h.setup_id
        WHERE h.setup_id IS NULL
        GROUP BY t1.symbol, t1.proximity_bin
        HAVING COUNT(*) > 1
        ORDER BY t1.symbol, t1.proximity_bin
        """

        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            print("No groups of setups with same symbol and proximity_bin found without hits.")
            return

        print(f"Found {len(results)} groups of setups with same symbol and proximity_bin without hits:\n")

        for row in results:
            setup_id, symbol, direction, proximity_bin, count = row
            print(f"Symbol: {symbol}, Direction: {direction}, Proximity Bin: {proximity_bin or 'NULL'}, Count: {count}")

            # Get the actual setup IDs for this group
            detail_query = """
            SELECT id, as_of, price, sl, tp, rrr
            FROM timelapse_setups
            WHERE symbol = ? AND proximity_bin = ?
            AND id NOT IN (SELECT setup_id FROM timelapse_hits)
            ORDER BY as_of DESC
            """

            cursor.execute(detail_query, (symbol, proximity_bin))
            details = cursor.fetchall()

            for detail in details:
                detail_id, as_of, price, sl, tp, rrr = detail
                print(f"  ID: {detail_id}, Date: {as_of}, Price: {price}, SL: {sl}, TP: {tp}, RRR: {rrr}")
            print()

        # Also show a summary by symbol
        print("\nSummary by symbol:")
        summary_query = """
        SELECT
            t.symbol,
            COUNT(*) as total_setups_without_hits,
            COUNT(DISTINCT t.proximity_bin) as unique_bins
        FROM timelapse_setups t
        LEFT JOIN timelapse_hits h ON t.id = h.setup_id
        WHERE h.setup_id IS NULL
        GROUP BY t.symbol
        ORDER BY total_setups_without_hits DESC
        """

        cursor.execute(summary_query)
        summary = cursor.fetchall()

        for symbol, total, bins in summary:
            print(f"  {symbol}: {total} setups without hits across {bins} proximity bins")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    find_missing_hits()