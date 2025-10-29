import sqlite3


def delete_duplicate_setups():
    """Delete duplicate setups, keeping only the earliest one for each group"""

    # Connect to the database
    conn = sqlite3.connect("timelapse.db")
    cursor = conn.cursor()

    print("Finding and deleting duplicate setups...")

    # First, let's see what we're going to delete (for verification)
    cursor.execute(
        """
        SELECT
            t1.id,
            t1.symbol,
            t1.direction,
            t1.proximity_bin,
            t1.as_of,
            t1.price,
            t1.sl,
            t1.tp,
            t1.rrr,
            t1.proximity_to_sl
        FROM timelapse_setups t1
        INNER JOIN (
            SELECT
                t.symbol,
                t.direction,
                t.proximity_bin,
                MIN(t.as_of) as earliest_as_of
            FROM timelapse_setups t
            LEFT JOIN timelapse_hits h ON t.id = h.setup_id
            WHERE h.setup_id IS NULL
            GROUP BY t.symbol, t.direction, t.proximity_bin
            HAVING COUNT(*) > 1
        ) t2 ON t1.symbol = t2.symbol
              AND t1.direction = t2.direction
              AND t1.proximity_bin = t2.proximity_bin
        LEFT JOIN timelapse_hits h ON t1.id = h.setup_id
        WHERE h.setup_id IS NULL
        AND t1.as_of != t2.earliest_as_of
        ORDER BY t1.symbol, t1.direction, t1.proximity_bin, t1.as_of
    """
    )

    duplicates_to_delete = cursor.fetchall()

    if not duplicates_to_delete:
        print("No duplicates found to delete.")
        conn.close()
        return

    print(f"Found {len(duplicates_to_delete)} duplicate records to delete:")
    print("\nRecords to be deleted:")
    print("ID\tSymbol\tDirection\tBin\tAs_of\t\t\tPrice\tSL\tTP\tRRR\tProx_SL")
    print("-" * 100)

    for record in duplicates_to_delete:
        print(
            f"{record[0]}\t{record[1]}\t{record[2]}\t{record[3]}\t{record[4]}\t{record[5]}\t{record[6]}\t{record[7]}\t{record[8]:.4f}\t{record[9]:.4f}"
        )

    # Confirm before deletion
    print(f"\nWARNING: About to delete {len(duplicates_to_delete)} records.")
    response = input("Do you want to proceed? (y/n): ")

    if response.lower() != "y":
        print("Deletion cancelled.")
        conn.close()
        return

    # Delete the duplicates (keep only the earliest one)
    cursor.execute(
        """
        DELETE FROM timelapse_setups
        WHERE id IN (
            SELECT t1.id
            FROM timelapse_setups t1
            INNER JOIN (
                SELECT
                    t.symbol,
                    t.direction,
                    t.proximity_bin,
                    MIN(t.as_of) as earliest_as_of
                FROM timelapse_setups t
                LEFT JOIN timelapse_hits h ON t.id = h.setup_id
                WHERE h.setup_id IS NULL
                GROUP BY t.symbol, t.direction, t.proximity_bin
                HAVING COUNT(*) > 1
            ) t2 ON t1.symbol = t2.symbol
                  AND t1.direction = t2.direction
                  AND t1.proximity_bin = t2.proximity_bin
            LEFT JOIN timelapse_hits h ON t1.id = h.setup_id
            WHERE h.setup_id IS NULL
            AND t1.as_of != t2.earliest_as_of
        )
    """
    )

    deleted_count = cursor.rowcount
    conn.commit()

    print(f"\nSuccessfully deleted {deleted_count} duplicate records.")

    # Verify the remaining duplicates
    cursor.execute(
        """
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
    )

    remaining_duplicates = cursor.fetchall()

    if remaining_duplicates:
        print(f"\nWarning: {len(remaining_duplicates)} duplicate groups still remain:")
        for record in remaining_duplicates:
            print(
                f"  {record[0]} {record[1]} Bin {record[2]}: {record[3]} records (IDs: {record[4]})"
            )
    else:
        print("\nSuccess! No duplicate setups remain.")

    conn.close()


if __name__ == "__main__":
    delete_duplicate_setups()
