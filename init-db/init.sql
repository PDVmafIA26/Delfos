CREATE TABLE IF NOT EXISTS anomalies (
    market_id TEXT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    avg_volume DOUBLE PRECISION,
    max_volume DOUBLE PRECISION,
    is_anomaly BOOLEAN
);

CREATE TABLE IF NOT EXISTS market_stats (
    market_id TEXT PRIMARY KEY,
    mean_volume DOUBLE PRECISION,
    std_volume DOUBLE PRECISION,
    num_records BIGINT
);