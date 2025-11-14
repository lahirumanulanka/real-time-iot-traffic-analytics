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

