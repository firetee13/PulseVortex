#!/usr/bin/env python3
"""
Find symbols with duplicated proximity bins that don't have hits.
This shows which symbols have multiple setups in the same proximity bin.
"""

import sqlite3
from monitor.core.config import default_db_path

def find_duplicated_bins():
    """Find symbols with duplicated proximity bins without hits."""
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
        query = """
        SELECT
            t.symbol,
            t.direction,
            t.proximity_bin,
            COUNT(*) as count,
            GROUP_CONCAT(t.id) as setup_ids,
            MIN(t.as_of) as earliest_date,
            MAX(t.as_of) as latest_date
        FROM timelapse_setups t
        LEFT JOIN timelapse_hits h ON t.id = h.setup_id
        WHERE h.setup_id IS NULL
        GROUP BY t.symbol, t.direction, t.proximity_bin
        HAVING COUNT(*) > 1
        ORDER BY t.symbol, t.direction, t.proximity_bin
        """

        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            print("No symbols with duplicated proximity bins found without hits.")
            return

        print(f"Found {len(results)} duplicated proximity bins:\n")

        # Group by symbol for better readability
        symbols = {}
        for row in results:
            symbol, direction, proximity_bin, count, setup_ids, earliest, latest = row
            if symbol not in symbols:
                symbols[symbol] = []
            symbols[symbol].append({
                'direction': direction,
                'proximity_bin': proximity_bin,
                'count': count,
                'setup_ids': setup_ids,
                'earliest': earliest,
                'latest': latest
            })

        # Display results grouped by symbol
        for symbol, bins in sorted(symbols.items()):
            print(f"Symbol: {symbol}")
            for bin_info in bins:
                print(f"  Direction: {bin_info['direction']}, Proximity Bin: {bin_info['proximity_bin'] or 'NULL'}")
                print(f"    Count: {bin_info['count']} setups")
                print(f"    Setup IDs: {bin_info['setup_ids']}")
                print(f"    Date range: {bin_info['earliest']} to {bin_info['latest']}")
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
            GROUP BY t.symbol, t.direction, t.proximity_bin
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
    find_duplicated_bins()