import os
from pathlib import Path
from typing import Dict, Any

import psycopg2
from dotenv import load_dotenv

# Load env
ENV_PATH = Path(__file__).resolve().parents[1] / "utils" / "config.env"
load_dotenv(dotenv_path=ENV_PATH)


def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "traffic"),
        user=os.getenv("POSTGRES_USER", "analytics"),
        password=os.getenv("POSTGRES_PASSWORD", "analytics"),
    )


def insert_processed_metric(record: Dict[str, Any]):
    """
    record example:
    {
      "sensor_id": "A-001",
      "hour": "2025-01-01T08:00:00+0000",
      "avg_count": 18.5,
      "samples": 3,
      "timestamp": "2025-01-01T08:10:00Z"
    }
    """
    hour_iso = record.get("hour")
    sensor_id = record.get("sensor_id")
    avg_count = float(record.get("avg_count", 0))
    samples = int(record.get("samples", 0))

    sql = (
        "INSERT INTO traffic_metrics(sensor_id, hour, avg_count, samples) "
        "VALUES (%s, %s, %s, %s)"
    )

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (sensor_id, hour_iso, avg_count, samples))
        conn.commit()


if __name__ == "__main__":
    # Quick smoke: insert a dummy row
    insert_processed_metric(
        {
            "sensor_id": "DEMO",
            "hour": "2025-01-01T00:00:00+0000",
            "avg_count": 1.23,
            "samples": 5,
        }
    )
    print("Inserted demo row into traffic_metrics.")
