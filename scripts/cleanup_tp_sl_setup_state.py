import sqlite3

def cleanup_tp_sl_setup_state():
    """Delete orphaned records from tp_sl_setup_state that reference deleted setup IDs"""

    # Connect to the database
    conn = sqlite3.connect('timelapse.db')
    cursor = conn.cursor()

    print("Checking for orphaned records in tp_sl_setup_state...")

    # First, let's check if there are any records in tp_sl_setup_state
    cursor.execute("SELECT COUNT(*) FROM tp_sl_setup_state")
    total_records = cursor.fetchone()[0]
    print(f"Total records in tp_sl_setup_state: {total_records}")

    if total_records == 0:
        print("No records found in tp_sl_setup_state table.")
        conn.close()
        return

    # Find records in tp_sl_setup_state that reference non-existent setup IDs
    cursor.execute("""
        SELECT tps.setup_id, tps.last_checked_utc
        FROM tp_sl_setup_state tps
        LEFT JOIN timelapse_setups ts ON tps.setup_id = ts.id
        WHERE ts.id IS NULL
    """)

    orphaned_records = cursor.fetchall()

    if not orphaned_records:
        print("No orphaned records found in tp_sl_setup_state.")
        conn.close()
        return

    print(f"\nFound {len(orphaned_records)} orphaned records in tp_sl_setup_state:")
    print("Setup_ID\tLast_Checked_UTC")
    print("-" * 50)

    for record in orphaned_records:
        print(f"{record[0]}\t{record[1]}")

    # Confirm before deletion
    print(f"\nWARNING: About to delete {len(orphaned_records)} orphaned records from tp_sl_setup_state.")
    response = input("Do you want to proceed? (y/n): ")

    if response.lower() != 'y':
        print("Deletion cancelled.")
        conn.close()
        return

    # Delete the orphaned records
    cursor.execute("""
        DELETE FROM tp_sl_setup_state
        WHERE setup_id NOT IN (
            SELECT id FROM timelapse_setups
        )
    """)

    deleted_count = cursor.rowcount
    conn.commit()

    print(f"\nSuccessfully deleted {deleted_count} orphaned records from tp_sl_setup_state.")

    # Verify the cleanup
    cursor.execute("""
        SELECT COUNT(*) FROM tp_sl_setup_state
    """)
    remaining_records = cursor.fetchone()[0]

    print(f"Remaining records in tp_sl_setup_state: {remaining_records}")

    # Double-check for any remaining orphaned records
    cursor.execute("""
        SELECT COUNT(*)
        FROM tp_sl_setup_state tps
        LEFT JOIN timelapse_setups ts ON tps.setup_id = ts.id
        WHERE ts.id IS NULL
    """)

    remaining_orphans = cursor.fetchone()[0]

    if remaining_orphans == 0:
        print("✓ All orphaned records have been successfully cleaned up.")
    else:
        print(f"⚠ Warning: {remaining_orphans} orphaned records still remain.")

    conn.close()

if __name__ == "__main__":
    cleanup_tp_sl_setup_state()