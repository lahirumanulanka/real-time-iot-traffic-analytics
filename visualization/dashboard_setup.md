# Grafana Dashboard Setup

This project starts Grafana in Docker and Postgres with the `traffic` database. Follow these steps to visualize metrics.

## 1) Start the stack

Use Docker Desktop on Windows, then from the project folder:

```powershell
# start services
docker compose up -d zookeeper kafka postgres grafana kafka-ui

# optional: watch logs
# docker compose logs -f kafka
```

## 2) Create Kafka topics

Run inside the Kafka container:

```powershell
# shell into the container
docker exec -it kafka bash

# inside the container, run the script
bash /app/kafka/topics/create_topics.sh
```

> If `/app/kafka/topics/create_topics.sh` is not found, you can copy it into the container:
>
> ```powershell
> docker cp .\kafka\topics\create_topics.sh kafka:/app/kafka/topics/create_topics.sh
> docker exec -it kafka bash -lc "chmod +x /app/kafka/topics/create_topics.sh && /app/kafka/topics/create_topics.sh"
> ```

## 3) Configure Grafana datasource

1. Open Grafana at http://localhost:3000 (user: `admin`, pass: `admin`).
2. Add a PostgreSQL datasource:
   - Host: `postgres:5432` (Grafana can reach the `postgres` service by name)
   - Database: `traffic`
   - User: `analytics`
   - Password: `analytics`
   - TLS/SSL: disabled (local dev)
3. Save & test.

## 4) Import the dashboard

1. In Grafana, click Dashboards → New → Import.
2. Upload `visualization/grafana-dashboard.json`.
3. Select your Postgres datasource when prompted.

You should now see a time series panel querying the `traffic_metrics` table.
