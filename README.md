# Real-Time IoT Traffic Analytics

I built this to learn Kafka + Python with a tiny but complete pipeline. It reads real traffic detector data, processes it, stores it in Postgres, and shows live charts in Grafana.

- Producer publishes records to a raw Kafka topic.
- Processor turns raw → processed and adds `is_operational`.
- Sink writes processed events into Postgres so Grafana can query.
- Dashboard shows a live time-series, daily peak/average, and a small health table.


## Quick Start (Windows PowerShell)

1) Create and activate a virtual environment, then install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

2) One-command real-time run (this starts Docker, producer, processor, and sink):

```powershell
python .\kafka-scripts\run_all.py --start-docker --interval 2 --producer-loop --use-now-ts --no-consumer
```

Now open Grafana and watch the charts update every few seconds:

http://localhost:3000  (user/pass: admin / admin)

Dashboard: Traffic Overview

Notes:
- Time range: keep “Last 1 hour” for the top time‑series (it’s live).
- The two daily panels show the last 14 days automatically (they ignore the 1h window).

3) (Optional) Create topics inside the Kafka container:

```powershell
python .\kafka-scripts\create-topics.py --bootstrap localhost:9092
```

4) Or, run a short smoke test instead of continuous streaming:

```powershell
python .\kafka-scripts\run_all.py --start-docker --interval 2 --producer-max 20 --use-now-ts --no-consumer
```

Press Ctrl+C to stop. The orchestrator starts the sink → processor → producer (and consumer if enabled). Lines are prefixed like [SINK]/[PROCESSOR]/[PRODUCER].


## Useful Variants (run_all.py)

```powershell
# Start everything for real-time (quiet logs, no console consumer)
python .\kafka-scripts\run_all.py --start-docker --interval 2 --producer-loop --use-now-ts --no-consumer

# Small batch/smoke test (sends 20 records then exits)
python .\kafka-scripts\run_all.py --start-docker --interval 2 --producer-max 20 --use-now-ts --no-consumer

# Start services only (custom list)
python .\kafka-scripts\run_all.py --start-docker --compose-services "kafka postgres grafana" --no-sink --no-consumer --producer-max 0

# Replay previous raw data from earliest
python .\kafka-scripts\run_all.py --from-beginning

# Change send interval (seconds)
python .\kafka-scripts\run_all.py --interval 5

# Use a custom dataset path
python .\kafka-scripts\run_all.py --json-path .\data\dataset\traffic_counts_kafka.json

# Show/hide consumer output and keys (if you enable the consumer)
python .\kafka-scripts\run_all.py --consume-max 10 --no-show-keys
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
	- Optional: starts Docker (`kafka`, `postgres`, `grafana`) and waits for them.
	- Starts the sink → processor → producer (and consumer if you want).
	- Clean shutdown on Ctrl+C.
	- Useful flags: `--start-docker`, `--producer-loop`, `--use-now-ts`, `--no-consumer`, `--sink-batch`.


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

3) Open Grafana at http://localhost:3000 (admin/admin). A Postgres datasource is auto-provisioned (name: Postgres). A ready-made dashboard is already in the repo at `grafana/dashboards/traffic-overview.json` (auto-provisioned). It has:
	- Time‑series of events (live, last 1h, refresh 5s)
	- Daily peak traffic (across sensors)
	- Daily average traffic
	- Sensor health table

If you want to write your own panels, here are some example queries:

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

Tip: if a panel says “Data outside time range”, keep the dashboard on “Last 1 hour”. The daily panels already pull 14 days on their own.


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

- Easiest path: just run the orchestrator with `--start-docker` and it will start services for you.
- If topics don’t exist, use `create-topics.py` once (or rely on your Kafka auto-create setting, if enabled).
- Make sure you are inside the virtual environment when running scripts (`.venv`).
- If you see missing package errors, run `pip install -r requirements.txt` again. If `six` is missing, install it with `pip install six`.


## Useful Checks and Stop/Cleanup

Quick Postgres checks (make sure new rows are arriving):

```powershell
docker compose exec postgres psql -U traffic -d traffic -c "SELECT COUNT(*) AS recent FROM traffic_events WHERE ts >= now() - interval '5 minutes';"
docker compose exec postgres psql -U traffic -d traffic -c "SELECT date_trunc('minute', ts) AS t, COUNT(*) FROM traffic_events WHERE ts >= now() - interval '1 hour' GROUP BY 1 ORDER BY 1 DESC LIMIT 10;"
```

Stop everything cleanly:

```powershell
# If run_all.py is in the foreground, press Ctrl+C
docker compose stop
```

If Grafana looks empty:
- Time range “Last 1 hour”.
- Datasource “Postgres” exists (restart Grafana if needed): `docker compose restart grafana`.
- Keep the producer running (use `--producer-loop --use-now-ts`).



## Next Steps

- Point Grafana at your Kafka-backed store or downstream DB and build on the dashboard in `grafana/dashboards/traffic-overview.json`.
- Extend the processor to compute aggregates (per detector type/status) or enrich with external metadata.
- Containerize the Python apps if you want them orchestrated alongside Kafka in Docker.

