import unittest
import tempfile
import os
from datetime import datetime, timezone
import sqlite3

from monitor.db import (
    ensure_hits_table_sqlite,
    backfill_hit_columns_sqlite,
    load_setups_sqlite,
    record_hit_sqlite,
    load_recorded_ids_sqlite,
)
from monitor.domain import Setup, Hit

UTC = timezone.utc


class TestDB(unittest.TestCase):
    def setUp(self):
        # Create a temporary database for testing
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, 'test.db')
        self.conn = sqlite3.connect(self.db_path)

    def tearDown(self):
        self.conn.close()
        # Clean up temporary files
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.rmdir(self.temp_dir)

    def test_ensure_hits_table_sqlite_creates_table(self):
        # Ensure the table is created
        ensure_hits_table_sqlite(self.conn)
        
        # Check that the table exists
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='timelapse_hits'")
        result = cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'timelapse_hits')

    def test_load_recorded_ids_sqlite_returns_empty_set_when_no_ids(self):
        ensure_hits_table_sqlite(self.conn)
        result = load_recorded_ids_sqlite(self.conn, [])
        self.assertEqual(result, set())

    def test_load_recorded_ids_sqlite_returns_correct_ids(self):
        ensure_hits_table_sqlite(self.conn)
        
        # Insert some test data
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO timelapse_hits (setup_id, symbol, direction, sl, tp, hit, hit_price, hit_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (1, 'EURUSD', 'Buy', 1.1, 1.2, 'TP', 1.2, '2023-01-01 12:00:00')
        )
        cursor.execute(
            "INSERT INTO timelapse_hits (setup_id, symbol, direction, sl, tp, hit, hit_price, hit_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (2, 'GBPUSD', 'Sell', 1.3, 1.1, 'SL', 1.3, '2023-01-01 12:00:00')
        )
        self.conn.commit()
        
        result = load_recorded_ids_sqlite(self.conn, [1, 2, 3])
        self.assertEqual(result, {1, 2})

    def test_load_setups_sqlite_returns_empty_list_when_no_table(self):
        result = load_setups_sqlite(self.conn, 'nonexistent_table', None, None, None)
        self.assertEqual(result, [])

    def test_record_hit_sqlite_inserts_record(self):
        ensure_hits_table_sqlite(self.conn)
        
        setup = Setup(
            id=1,
            symbol='EURUSD',
            direction='Buy',
            sl=1.1,
            tp=1.2,
            entry_price=1.15,
            as_of_utc=datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
        )
        
        hit = Hit(
            kind='TP',
            time_utc=datetime(2023, 1, 1, 13, 0, 0, tzinfo=UTC),
            price=1.2
        )
        
        # Record the hit
        record_hit_sqlite(self.conn, setup, hit, dry_run=False, verbose=False)
        
        # Check that the record was inserted
        cursor = self.conn.cursor()
        cursor.execute("SELECT setup_id, symbol, direction, hit FROM timelapse_hits")
        result = cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 1)
        self.assertEqual(result[1], 'EURUSD')
        self.assertEqual(result[2], 'Buy')
        self.assertEqual(result[3], 'TP')


if __name__ == '__main__':
    unittest.main()