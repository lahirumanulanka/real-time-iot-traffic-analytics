# Real-time IoT Traffic Analytics

Stream, process, store, and visualize live traffic sensor data using Apache Kafka, PostgreSQL, and Grafana.

## Stack

- Kafka + Zookeeper (Bitnami images)
- Kafka UI (topic/browser)
- PostgreSQL (time-series table for aggregates)
- Grafana (dashboard)
- Python producer (CSV → Kafka)
- Python consumer (Kafka → aggregation → Kafka + optional DB)

## Quick start

Prereqs: Docker Desktop (Windows), Python 3.10+.

```powershell
# 1) Start infra (Kafka, ZK, Postgres, Grafana, Kafka UI)
docker compose up -d zookeeper kafka postgres grafana kafka-ui

# 2) Create topics (run inside the Kafka container)
docker cp .\kafka\topics\create_topics.sh kafka:/app/kafka/topics/create_topics.sh
docker exec -it kafka bash -lc "chmod +x /app/kafka/topics/create_topics.sh && /app/kafka/topics/create_topics.sh"

# 3) (Optional) verify topics
docker exec -it kafka bash -lc "/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list"

# 4) Configure Grafana
# Visit http://localhost:3000 (admin/admin), add a PostgreSQL datasource:
#   Host: postgres:5432, DB: traffic, User: analytics, Pass: analytics
#   Then import visualization/grafana-dashboard.json

# 5) Install Python deps (producer)
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r .\producer\requirements.txt

# 6) Install Python deps (consumer)
pip install -r .\consumer\requirements.txt

# 7) Run consumer (aggregation → Kafka + local JSON)
# Leave this running in a terminal
python .\consumer\consumer.py

# 8) Run producer (streams sample CSV to Kafka)
python .\producer\producer.py
```

Kafka UI is available at http://localhost:8080.

## Configuration

Edit `utils/config.env` to adjust:

- `KAFKA_BOOTSTRAP_SERVERS` (host apps use `localhost:9094`)
- `RAW_TOPIC` (`traffic.raw`)
- `PROCESSED_TOPIC` (`traffic.processed`)
- `POSTGRES_*` (for Python apps)
- `STREAM_DELAY_MS` (producer pacing)

> Note: Inside Docker, services reach Kafka via `kafka:9092`; from the host (Python apps), use `localhost:9094`.

## Data flow

1. Producer reads CSV rows and publishes JSON to `traffic.raw`.
2. Consumer reads `traffic.raw`, computes running hourly averages per sensor, writes results to `traffic.processed` and `consumer/output/processed_data.json` for debug.
3. Storage app (`storage/db_connector.py`) can persist processed records into Postgres `traffic_metrics`.
4. Grafana visualizes hourly averages from Postgres.

## Folder structure

```
docker-compose.yml
kafka/
producer/
consumer/
storage/
visualization/
utils/
```

## Next steps

- Replace the sample `producer/dataset/traffic_counts.csv` with the real City of Austin dataset.
- Extend `consumer/analytics.py` for more metrics (daily peaks, rolling windows, anomalies).
- Write a small service to persist `traffic.processed` to Postgres continuously.