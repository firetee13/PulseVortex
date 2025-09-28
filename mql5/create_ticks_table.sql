-- Create the ticks table for MT5 tick data
-- This table will store tick data from MetaTrader 5

CREATE TABLE ticks (
  symbol LowCardinality(String) CODEC(ZSTD),
  time   DateTime64(3)          CODEC(DoubleDelta, ZSTD),
  bid    Float32                CODEC(Gorilla, ZSTD),
  ask    Float32                CODEC(Gorilla, ZSTD)
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(time)
ORDER BY (symbol, time);