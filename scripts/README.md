# Database Maintenance Scripts

This folder contains utility scripts for maintaining and cleaning up the timelapse.db database. These are one-time scripts used for specific maintenance tasks.

## Scripts Overview

### 1. round_restore_table.py
**Purpose**: Rounds decimal values in the restore table to match the precision used in timelapse_setups table.
- Rounds sl, tp, rrr, and proximity_to_sl columns
- Uses the same decimal logic as the setup analyzer (monitor-setup)
- Processed 693 records

### 2. insert_restore_to_setups.py
**Purpose**: Inserts data from the restore table into the timelapse_setups table.
- Handles unique constraint checking for (symbol, direction, as_of)
- Converts text values from restore table to proper numeric types
- Successfully inserted all 693 records

### 3. verify_rrr_calculation.py
**Purpose**: Verifies if RRR (Risk Reward Ratio) values in timelapse_setups are correctly calculated.
- Identifies records with incorrect RRR calculations
- Formula: RRR = reward/risk
  - Buy trades: risk = price - sl, reward = tp - price
  - Sell trades: risk = sl - price, reward = price - tp
- Found 214 records with incorrect RRR values

### 4. fix_rrr_values.py
**Purpose**: Fixes incorrect RRR values identified by verify_rrr_calculation.py.
- Recalculates RRR using the correct formula
- Updates 214 records with correct values
- All 929 records in timelapse_setups now have correctly calculated RRR values

### 5. find_missing_hits.py
**Purpose**: Finds setups in timelapse_setups table that don't have corresponding entries in timelapse_hits table.
- Shows which setups haven't been resolved yet
- Useful for tracking outstanding trades

### 6. find_duplicated_bins.py
**Purpose**: Finds symbols with duplicated proximity bins (same symbol, direction, and proximity_bin).
- Groups by symbol, direction, and proximity_bin
- Shows setups that might be duplicate trading signals
- Excludes setups that already have hits

### 7. find_duplicated_bins_correct.py
**Purpose**: Alternative version that only groups by symbol and proximity_bin (ignoring direction).
- Shows all setups in the same proximity bin regardless of direction
- Less useful for identifying actual duplicate trading signals

### 8. delete_duplicate_setups.py
**Purpose**: Deletes duplicate setup records, keeping only the earliest one for each group.
- Removes later duplicate entries (by as_of datetime)
- Deleted 12 duplicate records from timelapse_setups
- Preserves the original trading signals while eliminating duplicates

### 9. cleanup_tp_sl_setup_state.py
**Purpose**: Cleans up orphaned records in tp_sl_setup_state table.
- Deletes records that reference setup IDs no longer exist in timelapse_setups
- Deleted 36 orphaned records
- Ensures referential integrity between tables

## Usage

Run any script with:
```bash
python scripts/script_name.py
```

**Note**: These are one-time maintenance scripts. Always back up your database before running any modification scripts.