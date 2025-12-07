

-- SILVER TABLE FOR STAGING TRADE DATA
CREATE TABLE IF NOT EXISTS silver.fact_trades_stg (
    trade_id            STRING NOT NULL,
    symbol              STRING,
    price               DECIMAL(18, 4),
    quantity            BIGINT,
    trade_time_utc      TIMESTAMP_NTZ NOT NULL,
    trade_time_epoch_s  BIGINT,
    trade_time_et       TIMESTAMP,
    trade_time_ist      TIMESTAMP,
    trade_date          DATE          
)
USING iceberg
PARTITIONED BY (DAY(trade_time_utc), symbol)
OPTIONS (
    'write.format.default' = 'parquet',          
    'commit.retention-duration-ms' = '604800000', 
    'history.expire.min-snapshots-to-keep' = '10', 
    'format-version' = '2' 
)

-- GOLD TABLE FOR INTRADAY P&L REPORTING
CREATE TABLE IF NOT EXISTS gold.fact_trades_intraday (
    trade_id            STRING NOT NULL,
    client_id           STRING,
    symbol              STRING,
    price               DECIMAL(18, 4),
    quantity            BIGINT,
    trade_time_utc      TIMESTAMP_NTZ NOT NULL,
    trade_type          STRING,
    day_pnl             DECIMAL(18, 2)
)
USING iceberg
PARTITIONED BY (DAY(trade_time_utc), symbol)
OPTIONS (
    'write.format.default' = 'parquet',
    'commit.retention-duration-ms' = '2592000000', 
    'history.expire.min-snapshots-to-keep' = '30', 
    'format-version' = '2',                       
    'read.split.target-size' = '134217728' 
)