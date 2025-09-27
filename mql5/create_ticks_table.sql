-- Create the ticks table for MT5 tick data
-- This table will store tick data from MetaTrader 5

CREATE TABLE IF NOT EXISTS ticks (
    symbol LowCardinality(String) CODEC(ZSTD),
    time   DateTime64(9)          CODEC(DoubleDelta, ZSTD),
    bid    Float64                CODEC(Gorilla, ZSTD),
    ask    Float64                CODEC(Gorilla, ZSTD)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(time)
ORDER BY (symbol, time);

-- Optional: Create a materialized view for real-time analysis
CREATE MATERIALIZED VIEW IF NOT EXISTS ticks_mv
ENGINE = SummingMergeTree()
ORDER BY (symbol, toStartOfMinute(time))
AS SELECT
    symbol,
    toStartOfMinute(time) as minute,
    count() as tick_count,
    avg(bid) as avg_bid,
    avg(ask) as avg_ask
FROM ticks
GROUP BY symbol, minute;

-- Optional: Create a distributed table if you have a ClickHouse cluster
-- CREATE TABLE IF NOT EXISTS ticks_all AS ticks ENGINE = Distributed('cluster_name', 'default', 'ticks', rand());

-- Optional: Create a TTL policy to automatically delete old data (e.g., keep data for 30 days)
-- ALTER TABLE ticks MODIFY TTL time + toIntervalDay(30);