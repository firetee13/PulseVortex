# ClickHouse Ticks EA

This MetaTrader 5 Expert Advisor collects tick data from all symbols in the Market Watch and sends it to a ClickHouse database.

## Features

- Collects tick data from ALL symbols in Market Watch (not just the attached symbol)
- Automatically subscribes to all available symbols
- Sends data in batches to optimize performance
- Configurable connection parameters for ClickHouse
- Error handling and retry mechanism
- Debug mode for troubleshooting

## Requirements

- MetaTrader 5 Terminal
- ClickHouse server running
- Internet connection for HTTP requests
- WebRequest permission enabled in MT5

## Installation

1. Copy `ClickHouseTicksEA.mq5` to your MetaTrader 5 `Experts` folder
2. Restart MetaTrader 5 or refresh the Expert Advisors list
3. Enable WebRequest for the ClickHouse server URL:
   - Go to Tools -> Options -> Expert Advisors
   - Check "Allow WebRequest for listed URL"
   - Add `http://127.0.0.1:80` (or your ClickHouse server address)
4. Compile the EA in MetaEditor

## Configuration

The EA has the following input parameters:

| Parameter | Description | Default Value |
|-----------|-------------|---------------|
| InpClickHouseHost | ClickHouse server host | 127.0.0.1 |
| InpClickHousePort | ClickHouse server port | 80 |
| InpClickHouseUser | ClickHouse username | default |
| InpClickHousePassword | ClickHouse password | changeme1 |
| InpClickHouseDatabase | ClickHouse database name | default |
| InpSendIntervalSeconds | Interval for sending data (seconds) | 10 |
| InpMaxTicksPerBatch | Maximum number of ticks per batch | 100 |
| InpDebugMode | Enable debug logging | false |

## Usage

1. Make sure your ClickHouse server is running and accessible
2. Ensure the `ticks` table exists in your ClickHouse database (see table schema below)
3. Drag the EA to any chart in MetaTrader 5
4. Configure the input parameters as needed
5. Click "OK" to start the EA

Note: The EA will automatically subscribe to and collect tick data from ALL symbols in your Market Watch, not just the symbol of the chart it's attached to.

## Table Schema

The EA expects a table named `ticks` with the following schema:

```sql
CREATE TABLE IF NOT EXISTS ticks (
    symbol LowCardinality(String) CODEC(ZSTD),
    time   DateTime64(9)          CODEC(DoubleDelta, ZSTD),
    bid    Float64                CODEC(Gorilla, ZSTD),
    ask    Float64                CODEC(Gorilla, ZSTD)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(time)
ORDER BY (symbol, time);
```

The table uses:
- LowCardinality for the symbol column to optimize storage for repeated string values
- Partitioning by date for efficient data management and query performance
- ZSTD compression for all columns to reduce storage requirements

## Data Format

The EA sends tick data in JSON format, with each tick as a separate JSON object:

```json
{
  "symbol": "EURUSD",
  "time": "2023-09-27 12:34:56.123456000",
  "bid": 1.05632,
  "ask": 1.05635
}
```

Note: The time field is now in human-readable UTC format using DateTime64(9) with nanosecond precision.

## Troubleshooting

1. **Connection Issues**:
   - Verify ClickHouse server is running
   - Check host, port, username, and password
   - Ensure WebRequest is allowed for the ClickHouse URL

2. **No Data Being Sent**:
   - Check the Experts tab in MT5 for error messages
   - Enable Debug Mode to see detailed logs
   - Verify symbols are appearing in Market Watch

3. **Permission Errors**:
   - Ensure the ClickHouse user has INSERT permissions on the ticks table
   - Check if the database name is correct

## Notes

- The EA collects ticks from all symbols in your Market Watch
- Data is sent in batches to optimize performance
- The EA will retry sending data if there's a connection error
- All operations are logged to the Experts tab in MetaTrader 5