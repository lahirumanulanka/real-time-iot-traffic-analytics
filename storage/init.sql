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
