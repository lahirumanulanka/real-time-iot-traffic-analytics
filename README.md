## Kafka setup on Docker Desktop (Windows)

This project includes a ready-to-run Apache Kafka setup with Zookeeper and a helper to create the required topics for raw and processed traffic data.

Assumptions:
- Docker Desktop is installed and running.
- We use Confluent Kafka with Zookeeper (already defined in `docker-compose.yml`).
- Topics created: `traffic.raw` (raw sensor data) and `traffic.processed` (aggregated outputs).

### 1) Start Kafka dependencies only

To avoid building optional services, start just Zookeeper, Kafka and the UI:

```powershell
docker compose up -d zookeeper kafka kafka-ui
```

Wait until the Kafka container is healthy (about ~10–20s).

### 2) Create required topics

Use the provided one-off init service to create topics idempotently:

```powershell
docker compose run --rm kafka-init
```

You should see logs indicating `traffic.raw` and `traffic.processed` exist/are created.

Verify:

```powershell
docker exec -it kafka bash -lc "kafka-topics --bootstrap-server kafka:9092 --list"
```

Optional: if you also want a metrics topic (used by the consumer), create it:

```powershell
docker exec -it kafka bash -lc "kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic traffic.metrics --partitions 3 --replication-factor 1"
```

### 3) Run producer/consumer (optional)

Once topics are ready, you can start the producer and consumer services:

```powershell
docker compose up -d traffic-producer traffic-consumer
```

Check consumer logs:

```powershell
docker logs -f traffic-consumer
```

## Kafka Connect: simple file ingestion

Start Kafka Connect:

```powershell
docker compose up -d kafka-connect
```

Register a sample FileStream source connector (reads `connectors/input/traffic_sample.txt` and writes to `traffic.raw`):

```powershell
docker exec -it kafka-connect bash -lc "/data/register.sh"
```

Check connector status:

```powershell
curl http://localhost:8083/connectors/filestream-source/status
```

Verify records in topic:

```powershell
docker exec -it kafka bash -lc "kafka-console-consumer --bootstrap-server kafka:9092 --topic traffic.raw --from-beginning --timeout-ms 5000 | head -n 20"
```

## Grafana setup (Postgres datasource + starter dashboard)

Bring up Postgres and Grafana:

```powershell
docker compose up -d postgres grafana
```

Open Grafana at http://localhost:3000 (user: admin, pass: admin). A datasource named "Traffic Postgres" and a dashboard "Traffic Overview" will be provisioned automatically. The dashboard contains a simple sanity check query; add panels once processed data is in Postgres.

## Data cleaning (Python, no extra deps)

Clean the raw dataset and produce cleaned CSV and JSONL files under `data/cleaned`:

```powershell
.\.venv\Scripts\python.exe utils/clean_data.py --input data/dataset/traffic_counts.csv --outdir data/cleaned
```

This will:
- Parse `created_date`/`modified_date` to ISO-8601 UTC
- Extract `location_latitude`/`location_longitude` from the `LOCATION` WKT
- Normalize IDs and text, drop incomplete rows, and deduplicate
- Write `traffic_counts_clean.csv` and `traffic_counts_clean.jsonl`

### Notes

- The topic creation script is `kafka-scripts/create-topics.sh` and is mounted into the Kafka containers.
- The script waits for Kafka to be reachable and is safe to run multiple times.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE` is disabled in this setup; topics must be created explicitly.
- Access Kafka UI at http://localhost:8080 to browse topics and messages.

