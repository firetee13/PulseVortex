#!/usr/bin/env python3
"""
Script to insert data from restore table into timelapse_setups table
while handling unique constraints and type conversions.
"""

import sqlite3
import sys
from pathlib import Path

def get_db_path():
    """Get the path to the timelapse.db file"""
    db_path = Path("timelapse.db")
    if not db_path.exists():
        print(f"Error: Database file not found at {db_path}")
        sys.exit(1)
    return str(db_path)

def insert_restore_to_setups(dry_run=False, replace_existing=False):
    """
    Insert data from restore table into timelapse_setups table

    Args:
        dry_run: If True, only show what would be inserted without actually doing it
        replace_existing: If True, replace existing records with same symbol/direction/as_of
    """
    db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Get all records from restore table
        cursor.execute("SELECT * FROM restore")
        restore_records = cursor.fetchall()

        # Get column names from restore table
        cursor.execute("PRAGMA table_info(restore)")
        restore_columns = [row[1] for row in cursor.fetchall()]

        print(f"Found {len(restore_records)} records in restore table")

        inserted_count = 0
        skipped_count = 0
        replaced_count = 0

        for record in restore_records:
            # Convert record to dictionary for easier handling
            record_dict = dict(zip(restore_columns, record))

            # Convert text values to appropriate types
            try:
                symbol = record_dict['symbol']
                direction = record_dict['direction']
                price = float(record_dict['price']) if record_dict['price'] else None
                sl = float(record_dict['sl']) if record_dict['sl'] else None
                tp = float(record_dict['tp']) if record_dict['tp'] else None
                rrr = float(record_dict['rrr']) if record_dict['rrr'] else None
                score = float(record_dict['score']) if record_dict['score'] else None
                as_of = record_dict['as_of']
                detected_at = record_dict['detected_at']
                proximity_to_sl = float(record_dict['proximity_to_sl']) if record_dict['proximity_to_sl'] else None
                proximity_bin = record_dict['proximity_bin']
                inserted_at = record_dict['inserted_at']

                # Check if record already exists
                cursor.execute("""
                    SELECT id FROM timelapse_setups
                    WHERE symbol = ? AND direction = ? AND as_of = ?
                """, (symbol, direction, as_of))

                existing_record = cursor.fetchone()

                if existing_record:
                    if replace_existing:
                        # Replace existing record
                        cursor.execute("""
                            DELETE FROM timelapse_setups
                            WHERE symbol = ? AND direction = ? AND as_of = ?
                        """, (symbol, direction, as_of))

                        cursor.execute("""
                            INSERT INTO timelapse_setups
                            (symbol, direction, price, sl, tp, rrr, score, as_of,
                             detected_at, proximity_to_sl, proximity_bin, inserted_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (symbol, direction, price, sl, tp, rrr, score, as_of,
                              detected_at, proximity_to_sl, proximity_bin, inserted_at))

                        replaced_count += 1
                        if dry_run:
                            print(f"Would replace: {symbol} {direction} {as_of}")
                        else:
                            print(f"Replaced: {symbol} {direction} {as_of}")
                    else:
                        skipped_count += 1
                        if dry_run:
                            print(f"Would skip (exists): {symbol} {direction} {as_of}")
                else:
                    # Insert new record
                    cursor.execute("""
                        INSERT INTO timelapse_setups
                        (symbol, direction, price, sl, tp, rrr, score, as_of,
                         detected_at, proximity_to_sl, proximity_bin, inserted_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (symbol, direction, price, sl, tp, rrr, score, as_of,
                          detected_at, proximity_to_sl, proximity_bin, inserted_at))

                    inserted_count += 1
                    if dry_run:
                        print(f"Would insert: {symbol} {direction} {as_of}")
                    else:
                        print(f"Inserted: {symbol} {direction} {as_of}")

            except (ValueError, TypeError) as e:
                print(f"Error converting record {record_dict.get('id', 'unknown')}: {e}")
                continue

        if not dry_run:
            conn.commit()

        print(f"\nSummary:")
        print(f"Total records processed: {len(restore_records)}")
        print(f"Records inserted: {inserted_count}")
        print(f"Records skipped (already exists): {skipped_count}")
        print(f"Records replaced: {replaced_count}")

        if dry_run:
            print("(DRY RUN - No changes were made)")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        if not dry_run:
            conn.rollback()
        return False
    finally:
        conn.close()

    return True

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Insert data from restore table into timelapse_setups table')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be inserted without actually doing it')
    parser.add_argument('--replace', action='store_true',
                       help='Replace existing records with same symbol/direction/as_of')

    args = parser.parse_args()

    print("Inserting restore table data into timelapse_setups...")
    if args.dry_run:
        print("DRY RUN MODE - No changes will be made")
    if args.replace:
        print("REPLACE MODE - Existing records will be replaced")

    success = insert_restore_to_setups(dry_run=args.dry_run, replace_existing=args.replace)

    if success:
        print("Operation completed successfully")
    else:
        print("Operation failed")
        sys.exit(1)

if __name__ == "__main__":
    main()