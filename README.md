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

### Notes

- The topic creation script is `kafka-scripts/create-topics.sh` and is mounted into the Kafka containers.
- The script waits for Kafka to be reachable and is safe to run multiple times.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE` is disabled in this setup; topics must be created explicitly.
- Access Kafka UI at http://localhost:8080 to browse topics and messages.

