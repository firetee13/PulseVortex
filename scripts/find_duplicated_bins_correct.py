#!/usr/bin/env python3
"""
Find symbols with duplicated proximity bins (ignoring direction).
This shows which symbols have multiple setups in the same proximity bin,
regardless of whether they are Buy or Sell setups.
"""

import sqlite3
from monitor.core.config import default_db_path

def find_duplicated_bins_correct():
    """Find symbols with duplicated proximity bins without hits (ignoring direction)."""
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

        # Query to find symbols with duplicated proximity bins (count > 1)
        # Grouping only by symbol and proximity_bin, not direction
        query = """
        SELECT
            t.symbol,
            t.proximity_bin,
            COUNT(*) as count,
            GROUP_CONCAT(t.id) as setup_ids,
            GROUP_CONCAT(t.direction) as directions,
            MIN(t.as_of) as earliest_date,
            MAX(t.as_of) as latest_date
        FROM timelapse_setups t
        LEFT JOIN timelapse_hits h ON t.id = h.setup_id
        WHERE h.setup_id IS NULL
        GROUP BY t.symbol, t.proximity_bin
        HAVING COUNT(*) > 1
        ORDER BY t.symbol, t.proximity_bin
        """

        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            print("No symbols with duplicated proximity bins found without hits.")
            return

        print(f"Found {len(results)} duplicated proximity bins:\n")

        # Display results
        for row in results:
            symbol, proximity_bin, count, setup_ids, directions, earliest, latest = row
            print(f"Symbol: {symbol}")
            print(f"  Proximity Bin: {proximity_bin or 'NULL'}")
            print(f"  Total Count: {count} setups")
            print(f"  Directions: {directions}")
            print(f"  Setup IDs: {setup_ids}")
            print(f"  Date range: {earliest} to {latest}")
            print()

        # Summary by symbol
        print("\nSummary by symbol (total duplicated setups):")
        summary_query = """
        SELECT
            symbol,
            SUM(duplicate_count) as total_duplicated_setups,
            COUNT(*) as duplicated_bins
        FROM (
            SELECT
                t.symbol,
                t.proximity_bin,
                COUNT(*) as duplicate_count
            FROM timelapse_setups t
            LEFT JOIN timelapse_hits h ON t.id = h.setup_id
            WHERE h.setup_id IS NULL
            GROUP BY t.symbol, t.proximity_bin
            HAVING COUNT(*) > 1
        ) subquery
        GROUP BY symbol
        ORDER BY total_duplicated_setups DESC
        """

        cursor.execute(summary_query)
        summary = cursor.fetchall()

        for symbol, total, bins in summary:
            print(f"  {symbol}: {total} duplicated setups across {bins} proximity bins")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    find_duplicated_bins_correct()