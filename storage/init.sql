-- Initialize PostgreSQL schema for IoT traffic analytics
CREATE TABLE IF NOT EXISTS traffic_metrics (
    id SERIAL PRIMARY KEY,
    sensor_id TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    hour TIMESTAMPTZ NOT NULL,
    avg_count NUMERIC(12,3) NOT NULL,
    samples INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_traffic_metrics_sensor_hour
ON traffic_metrics (sensor_id, hour);

-- Daily peak traffic volume across all sensors
CREATE TABLE IF NOT EXISTS daily_peak_volume (
    day DATE PRIMARY KEY,
    peak_hour TIMESTAMPTZ NOT NULL,
    peak_volume BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Daily sensor availability based on hourly data presence
CREATE TABLE IF NOT EXISTS sensor_daily_availability (
    sensor_id TEXT NOT NULL,
    day DATE NOT NULL,
    hours_seen INTEGER NOT NULL,
    hours_expected INTEGER NOT NULL,
    availability NUMERIC(5,2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (sensor_id, day)
);
