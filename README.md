# Real-Time IoT Traffic Analytics (Student Guide)

This project demonstrates a simple real-time data pipeline using Kafka and Python to ingest, process, and view traffic detector data.

- Producer reads real JSON data every few seconds and publishes to a raw Kafka topic.
- Processor consumes raw events, derives useful flags, and republishes to a processed topic.
- Consumer tails the processed topic to verify the end-to-end flow.


## Quick Start (Windows PowerShell)

1) Create and activate a virtual environment, then install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

2) Start Docker services (Kafka, etc.) in the background:

```powershell
# Use the variant that matches your Docker version
docker compose up -d
# or
docker-compose up -d
```

3) (Optional) Create topics inside the Kafka container:

```powershell
python .\kafka-scripts\create-topics.py --bootstrap localhost:9092
```

4) Run all three processes together with the orchestrator:

```powershell
python .\kafka-scripts\run_all.py
```

Press Ctrl+C to stop. The orchestrator will start the processor first, then the producer, then the consumer, and prefix each subprocess line with [PROCESSOR]/[PRODUCER]/[CONSUMER].


## Useful Variants (run_all.py)

```powershell
# Start from latest offsets (no replay), default 5s interval
python .\kafka-scripts\run_all.py --bootstrap localhost:29092

# Replay previous data from the beginning
python .\kafka-scripts\run_all.py --from-beginning

# Quick check: limit consumer output to 10 messages
python .\kafka-scripts\run_all.py --consume-max 10

# Change the send interval (seconds)
python .\kafka-scripts\run_all.py --interval 5

# Use a custom dataset path
python .\kafka-scripts\run_all.py --json-path .\data\dataset\traffic_counts_kafka.json

# Hide keys in consumer output
python .\kafka-scripts\run_all.py --no-show-keys
```

Default topics and dataset:
- Raw topic: `iot.traffic.raw`
- Processed topic: `iot.traffic.processed`
- Dataset: `data/dataset/traffic_counts_kafka.json`


## What Each Part Does

- Producer (`kafka-scripts/producer/traffic_raw_producer.py`):
	- Loads records from `data/dataset/traffic_counts_kafka.json`.
	- Publishes one record every N seconds (default 5) to `iot.traffic.raw`.
	- Uses `detector_id` as the Kafka message key.

- Processor (`kafka-scripts/processor/traffic_processor.py`):
	- Consumes from `iot.traffic.raw`.
	- Derives `is_operational` from `detector_status` (messages with status starting with `OK` are considered operational).
	- Adds `processed_at` timestamp and publishes the enriched record to `iot.traffic.processed`.

- Consumer (`kafka-scripts/consumer/processed_consumer.py`):
	- Tails a topic (usually `iot.traffic.processed`) and prints a concise line for each message.
	- Supports `--show-keys` and `--max` to limit output.

- Orchestrator (`kafka-scripts/run_all.py`):
	- Starts the processor → producer → consumer in that order.
	- Clean shutdown on Ctrl+C.
	- Pass-through options for bootstrap, topics, dataset path, replay behavior, interval, and consumer output limits.


## Real-Time Metrics (Streaming)

You can compute live metrics from the processed topic using a lightweight Python consumer:

```powershell
.\.venv\Scripts\activate
python .\kafka-scripts\metrics\metrics_aggregator.py --bootstrap localhost:29092 --topic iot.traffic.processed --from-beginning --print-every 10 --export-json .\data\metrics\snapshot.json
```

Metrics definitions (based on message timestamps):
- Hourly average vehicle count per sensor: count of events per sensor per hour.
- Daily peak traffic volume (all sensors): within each day, the hour with the highest total events across all sensors (reports hour index and count).
- Daily sensor availability (%): percentage of hours in a day where a sensor has at least one event (hours_with_data/24 × 100).

Notes:
- The aggregator prints a rolling summary and optionally writes a full JSON snapshot to `data/metrics/snapshot.json`.
- It uses the message `timestamp` field (in UTC). If missing, it falls back to the Kafka record timestamp.
- Adjust `--from-beginning` to replay historical data, or omit it to process live-only.


## Grafana + PostgreSQL Sink

This repo provisions Grafana and PostgreSQL via `docker-compose.yml`. We write processed events into PostgreSQL, then visualize with Grafana.

1) Start the stack:

```powershell
docker compose up -d
```

2) Run the PostgreSQL sink (creates table on first run):

```powershell
.\.venv\Scripts\activate
python .\kafka-scripts\sink\postgres_sink.py --bootstrap localhost:29092 --topic iot.traffic.processed --from-beginning
```

3) Open Grafana at http://localhost:3000 (admin/admin). A Postgres datasource is auto-provisioned (name: Postgres). Create a dashboard and run queries like:

```sql
-- Hourly events per sensor (last 24h)
SELECT detector_id,
			 date_trunc('hour', ts) AS hour,
			 COUNT(*) AS event_count
FROM traffic_events
WHERE ts > now() - interval '24 hours'
GROUP BY detector_id, date_trunc('hour', ts)
ORDER BY hour;

-- Daily peak hour across all sensors (last 7 days)
WITH hourly AS (
	SELECT date_trunc('day', ts) AS day,
				 extract(hour from ts) AS hour_idx,
				 COUNT(*) AS cnt
	FROM traffic_events
	WHERE ts > now() - interval '7 days'
	GROUP BY 1,2
)
SELECT day, hour_idx, cnt
FROM (
	SELECT day, hour_idx, cnt,
				 ROW_NUMBER() OVER (PARTITION BY day ORDER BY cnt DESC) AS rn
	FROM hourly
)
WHERE rn = 1
ORDER BY day;

-- Daily sensor availability (%) over last 7 days
WITH hours_present AS (
	SELECT detector_id,
				 date_trunc('day', ts) AS day,
				 COUNT(DISTINCT date_trunc('hour', ts)) AS hours_with_data
	FROM traffic_events
	WHERE ts > now() - interval '7 days'
	GROUP BY 1,2
)
SELECT detector_id,
			 day,
			 ROUND(100.0 * hours_with_data / 24.0, 2) AS availability_pct
FROM hours_present
ORDER BY day, detector_id;
```

Note: If Grafana was already running, restart it to pick up the auto-provisioned Postgres datasource:

```powershell
docker compose restart grafana
```


## Optional: Forward to a Visualization Topic

If you prefer a separate Kafka topic for visualization pipelines, forward processed events to `iot.traffic.viz`:

```powershell
.\.venv\Scripts\activate
python .\kafka-scripts\forwarder\processed_to_viz.py --bootstrap localhost:29092 --from-topic iot.traffic.processed --to-topic iot.traffic.viz --from-beginning --max 1000
```

The broker in this repo is configured with auto-create topics enabled, so the viz topic will be created on first write.

## Visualization Topic (Forwarder)

Forward processed events to a dedicated topic that dashboards or lightweight services can consume:

```powershell
.\.venv\Scripts\activate
python .\kafka-scripts\processor\visualization_forwarder.py --bootstrap localhost:29092 --from-topic iot.traffic.processed --to-topic iot.traffic.visualization --from-beginning
```

By default Kafka auto-creates topics (enabled in `docker-compose.yml`). If disabled, create the topic via `kafka-scripts/create-topics.py` or Kafka CLI.


## PostgreSQL Sink (Historical Storage)

Spin up Postgres (already included in `docker-compose.yml`) and run the sink to persist processed events for historical analysis:

```powershell
docker compose up -d postgres
.\.venv\Scripts\activate
pip install -r requirements.txt
python .\kafka-scripts\sink\postgres_sink.py --bootstrap localhost:29092 --topic iot.traffic.processed --from-beginning --batch 200 --pg-host localhost --pg-port 5432 --pg-db traffic --pg-user traffic --pg-password traffic
```

Details:
- The sink will create a table `traffic_events` if it doesn't exist and add helpful indexes.
- Connection options accept env vars: `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`.
- Columns include `detector_id`, `detector_type`, `detector_status`, `signal_id`, `ts` (timestamp), `is_operational`, `processed_at`, and a `raw` JSONB payload for flexibility.


## Folder Structure (Key Paths)

```
docker-compose.yml
README.md
requirements.txt
data/
	dataset/
		traffic_counts_kafka.json
		traffic_counts.csv
	sample/
		traffic_sample.csv
		traffic_sample.json
grafana/
	dashboards/
		traffic-overview.json
	provisioning/
		dashboards/
			dashboards.yml
		datasources/
			datasource.yml
kafka-scripts/
	create-topics.py
	run_all.py                    <-- Orchestrator (start all 3)
	metrics/
		metrics_aggregator.py       <-- Computes live metrics
	sink/
		postgres_sink.py            <-- Persists processed events to PostgreSQL
	consumer/
		sample_consume_kafkapy.py
		traffic_counts_consumer.py
		processed_consumer.py       <-- Generic consumer used by orchestration
	producer/
		sample_produce_from_json_kafkapy.py
		traffic_counts_producer.py
		traffic_raw_producer.py     <-- Producer reading dataset to raw topic
	processor/
		traffic_processor.py        <-- Processes raw → processed
notebooks/
	preprocess_traffic_counts.ipynb
```


## Notes for Students

- Start services first (`docker compose up -d`), then run the orchestrator.
- If topics don’t exist, use `create-topics.py` once (or rely on your Kafka auto-create setting, if enabled).
- Make sure you are inside the virtual environment when running scripts (`.venv`).
- If you see missing package errors, run `pip install -r requirements.txt` again. If `six` is missing, install it with `pip install six`.


## Next Steps

- Point Grafana at your Kafka-backed store or downstream DB and build on the dashboard in `grafana/dashboards/traffic-overview.json`.
- Extend the processor to compute aggregates (per detector type/status) or enrich with external metadata.
- Containerize the Python apps if you want them orchestrated alongside Kafka in Docker.

