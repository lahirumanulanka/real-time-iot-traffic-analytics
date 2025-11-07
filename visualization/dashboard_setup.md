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

## 4) Create dashboards (SQL panels)

Below are starter queries for the required visuals. Create a new dashboard and add the panels.

### A) Time-series: hourly traffic flow per sensor

Panel type: Time series

Query (PostgreSQL):

```sql
SELECT
   hour AS "time",
   sensor_id,
   avg_count
FROM traffic_metrics
WHERE $__timeFilter(hour)
   AND sensor_id IN ($sensor)
ORDER BY 1 ASC;
```

Recommended:
- Add a dashboard variable `sensor` (type: Query) with query: `SELECT DISTINCT sensor_id FROM traffic_metrics ORDER BY sensor_id`
- Multi-value enabled.

### B) Bar/Line: daily peak volume across all sensors

Panel type: Bar chart or Time series (bars)

Query:

```sql
SELECT
   day::timestamp AS "time",
   peak_volume
FROM daily_peak_volume
WHERE $__timeFilter(day::timestamp)
ORDER BY 1 ASC;
```

### C) Bar/Line: daily total/average volume (derived from hourly)

Panel type: Time series

Query (daily total volume):

```sql
SELECT
   date(hour) AS day,
   SUM(avg_count * samples) AS daily_total_volume
FROM traffic_metrics
WHERE $__timeFilter(hour)
GROUP BY date(hour)
ORDER BY day ASC;
```

Optionally plot average per sensor:

```sql
SELECT
   date(hour) AS day,
   AVG(avg_count) AS daily_avg_per_row
FROM traffic_metrics
WHERE $__timeFilter(hour)
GROUP BY date(hour)
ORDER BY day ASC;
```

### D) Gauge/Table: sensor availability (%)

Panel type: Table (and/or Gauge)

To show latest day for each sensor:

```sql
WITH latest_day AS (
   SELECT MAX(day) AS d FROM sensor_daily_availability
)
SELECT
   s.sensor_id,
   s.day,
   s.hours_seen,
   s.hours_expected,
   s.availability
FROM sensor_daily_availability s, latest_day ld
WHERE s.day = ld.d
ORDER BY s.sensor_id ASC;
```

For a single-sensor gauge, add a variable `sensor_g` and filter with `WHERE s.sensor_id = '$sensor_g'`.

## 5) Import the example dashboard (optional)

## 4) Import the dashboard

1. In Grafana, click Dashboards → New → Import.
2. Upload `visualization/grafana-dashboard.json`.
3. Select your Postgres datasource when prompted.

You should now see the prebuilt panels.

## Notes
- Publishing paths:
   - Raw events → `traffic.raw`
   - Hourly aggregates → `traffic.processed` (also persisted in `traffic_metrics`)
   - Daily metrics (peak + availability) → `traffic.metrics` (also persisted)
- Direct Kafka in Grafana: not officially supported; use Postgres as the sink (recommended). There are community Kafka datasource plugins, but they’re less maintained and not as reliable as SQL.
