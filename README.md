## Kafka on Docker (Windows + Docker Desktop)

This project includes a ready-to-run Apache Kafka stack using Docker Compose. It runs Kafka in KRaft mode (no Zookeeper required) and an optional web UI for quick inspection.

### What gets started
- `kafka` (single broker, KRaft mode)
- `kafka-ui` at http://localhost:8080

### Prerequisites
- Docker Desktop (Windows) installed and running
- PowerShell (commands below are copy-paste ready)

### Start
```powershell
docker compose -f "${PWD}\docker-compose.yml" up -d
```

Wait ~10–20 seconds. Verify containers are healthy:
```powershell
docker ps
```

Open the UI at:
- http://localhost:8080

### How to connect
- From apps running on your host machine: `localhost:29092`
- From services inside the Docker network: `kafka:9092`

### Quick test (optional)
Create a topic, produce, and consume test messages:
```powershell
# Create a topic
docker exec kafka bash -lc "kafka-topics --bootstrap-server localhost:9092 --create --topic test-traffic --partitions 1 --replication-factor 1 || true"

# Produce one message
docker exec kafka bash -lc "printf 'hello-from-readme\n' | kafka-console-producer --bootstrap-server localhost:9092 --topic test-traffic 1>$null"

# Consume from the beginning
docker exec kafka bash -lc "kafka-console-consumer --bootstrap-server localhost:9092 --topic test-traffic --from-beginning --timeout-ms 3000"
```

You should see `hello-from-readme` in the output.

## Project topics
Two topics are used by this project:
- `iot.traffic.raw` — raw sensor data ingestion
- `iot.traffic.processed` — processed/aggregated outputs

Create them with the provided script:
```powershell
./kafka-scripts/create-topics.ps1
```

Options (all optional):
```powershell
./kafka-scripts/create-topics.ps1 -Bootstrap "localhost:9092" -RawTopic "iot.traffic.raw" -ProcessedTopic "iot.traffic.processed" -Partitions 3 -ReplicationFactor 1
```

Linux/macOS users can run the bash version:
```bash
bash ./kafka-scripts/create-topics.sh
```

## Grafana dashboard environment
- Service: `grafana` at http://localhost:3000
- Default credentials: admin / admin (change after first login)
- Pre-provisioned:
	- Datasource: TestData (built-in) as default
	- Dashboard: "Traffic Overview (TestData)"

Start only Grafana:
```powershell
docker compose -f "${PWD}\docker-compose.yml" up -d grafana
```

Provisioning locations:
- Datasources: `grafana/provisioning/datasources/`
- Dashboards provisioning: `grafana/provisioning/dashboards/`
- Dashboard JSONs: `grafana/dashboards/`

To add a real data source (e.g., Prometheus, InfluxDB, PostgreSQL), create a datasource file in `grafana/provisioning/datasources` and add dashboards under `grafana/dashboards`, then restart Grafana.

### Stop and clean up
```powershell
# Stop containers
docker compose -f "${PWD}\docker-compose.yml" down

# Remove data volume (optional: wipes topics/messages)
docker volume rm real-time-iot-traffic-analytics_kafka_data
```

### Notes
- This setup uses KRaft (Kafka without Zookeeper), which is the current recommended architecture for Kafka 3.x.
- If you specifically need Zookeeper, we can add a ZK-based variant on request, but it's not required for this stack.

