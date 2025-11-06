import json
import os
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
import sys
try:
    import six  # type: ignore
    # Shim for kafka-python expecting vendor.six on newer Python
    sys.modules.setdefault('kafka.vendor.six', six)
    sys.modules.setdefault('kafka.vendor.six.moves', six.moves)
except Exception:
    pass
from kafka import KafkaProducer

# Load env
ENV_PATH = Path(__file__).resolve().parents[1] / "utils" / "config.env"
load_dotenv(dotenv_path=ENV_PATH)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.getenv("RAW_TOPIC", "traffic.raw")
# Default to 5 seconds to simulate near-real-time cadence
STREAM_DELAY_MS = int(os.getenv("STREAM_DELAY_MS", "5000"))
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "0"))  # 0 = no limit


def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        api_version_auto_timeout_ms=15000,
    )


def preprocess_csv(csv_path: str):
    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV not found: {csv_file}")

    # Read CSV with dtype=str to preserve strings like "2,060" and avoid unwanted numeric coercion
    df = pd.read_csv(csv_file, dtype=str, keep_default_na=False).replace({"": None})

    # This dataset appears to be detector metadata. Map to our streaming schema.
    # sensor_id: detector_id (strip commas/spaces)
    # timestamp: prefer modified_date, fallback to created_date
    # count: synthetic 1 (event count) for flow verification
    def clean_id(x):
        if pd.isna(x):
            return None
        return str(x).replace(",", "").strip()

    # Build timestamp column from modified/created
    ts = None
    if "modified_date" in df.columns:
        ts = pd.to_datetime(df["modified_date"], errors="coerce", utc=True)
    if (ts is None or (hasattr(ts, 'isna') and ts.isna().all())) and "created_date" in df.columns:
        ts = pd.to_datetime(df["created_date"], errors="coerce", utc=True)
    if ts is None:
        raise ValueError("CSV missing both 'modified_date' and 'created_date' for timestamp mapping")

    # Compose working frame
    det_series = df["detector_id"] if "detector_id" in df.columns else pd.Series([None] * len(df))
    df2 = pd.DataFrame({
        "sensor_id": det_series.map(clean_id),
        "timestamp": ts,
        # synthetic count to drive pipeline; analytics may compute hourly event rates
        "count": 1,
        # pass-through attributes for downstream enrichment/filters
        "detector_type": df.get("detector_type"),
        "detector_status": df.get("detector_status"),
        "detector_direction": df.get("detector_direction"),
        "detector_movement": df.get("detector_movement"),
        "location_name": df.get("location_name"),
        "atd_location_id": df.get("atd_location_id"),
        "signal_id": df.get("signal_id"),
        "location_latitude": df.get("location_latitude"),
        "location_longitude": df.get("location_longitude"),
    })

    # Ensure required columns
    missing = {c for c in ["sensor_id", "timestamp", "count"] if c not in df2.columns}
    if missing:
        raise ValueError(f"Failed to derive required columns: {missing}")

    # Clean up: drop rows missing requireds, coerce count to int, and sort by timestamp if available
    df2 = df2.dropna(subset=["sensor_id", "timestamp"]).reset_index(drop=True)
    df2["count"] = pd.to_numeric(df2["count"], errors="coerce").fillna(0).astype(int)
    try:
        df2 = df2.sort_values("timestamp").reset_index(drop=True)
    except Exception:
        pass

    # Optional limit for quick tests
    if MAX_RECORDS and len(df2) > MAX_RECORDS:
        df2 = df2.head(MAX_RECORDS).reset_index(drop=True)

    # Write JSONL for Kafka-friendly format
    out_jsonl = Path(__file__).parent / "dataset" / "preprocessed.jsonl"
    with out_jsonl.open("w", encoding="utf-8") as f:
        for _, r in df2.iterrows():
            rec = {
                "sensor_id": str(r["sensor_id"]),
                "timestamp": pd.to_datetime(r["timestamp"]).isoformat(),
                "count": int(r["count"]),
                "detector_type": r.get("detector_type"),
                "detector_status": r.get("detector_status"),
                "detector_direction": r.get("detector_direction"),
                "detector_movement": r.get("detector_movement"),
                "location_name": r.get("location_name"),
                "atd_location_id": r.get("atd_location_id"),
                "signal_id": r.get("signal_id"),
                "location_latitude": r.get("location_latitude"),
                "location_longitude": r.get("location_longitude"),
            }
            f.write(json.dumps(rec) + "\n")

    return df2


def stream_df(df2: pd.DataFrame):

    producer = get_producer()
    print(f"Producing to {TOPIC} on {BOOTSTRAP} ... rows={len(df2)}; delay={STREAM_DELAY_MS}ms")

    sent = 0
    for i, row in df2.iterrows():
        record = {
            "sensor_id": str(row["sensor_id"]),
            "timestamp": pd.to_datetime(row["timestamp"]).isoformat(),
            "count": int(row["count"]),
            # optional attributes for downstream consumers
            "detector_type": row.get("detector_type"),
            "detector_status": row.get("detector_status"),
            "detector_direction": row.get("detector_direction"),
            "detector_movement": row.get("detector_movement"),
            "location_name": row.get("location_name"),
            "atd_location_id": row.get("atd_location_id"),
            "signal_id": row.get("signal_id"),
            "location_latitude": row.get("location_latitude"),
            "location_longitude": row.get("location_longitude"),
        }
        producer.send(topic=TOPIC, value=record, key=record["sensor_id"])
        sent += 1
        if STREAM_DELAY_MS > 0:
            time.sleep(STREAM_DELAY_MS / 1000.0)
        if sent % 100 == 0:
            print(f"Sent {sent} messages...")

    producer.flush()
    print(f"Done. Sent {sent} messages.")


if __name__ == "__main__":
    # Prefer cleaned JSONL if present; otherwise preprocess CSV
    data_dir = Path(__file__).parent / "dataset"
    default_csv = data_dir / "traffic_counts.csv"
    default_jsonl = data_dir / "processed" / "cleaned_detectors.jsonl"

    jsonl_env = os.getenv("JSONL_PATH")
    csv_env = os.getenv("CSV_PATH")

    df2: pd.DataFrame
    if jsonl_env:
        jsonl_path = Path(jsonl_env)
        if not jsonl_path.exists():
            raise FileNotFoundError(f"JSONL not found: {jsonl_path}")
        df2 = pd.read_json(jsonl_path, lines=True, dtype=False)
    elif default_jsonl.exists():
        df2 = pd.read_json(default_jsonl, lines=True, dtype=False)
    else:
        path = Path(csv_env) if csv_env else default_csv
        df2 = preprocess_csv(str(path))

    # Optional cap for quick tests
    if MAX_RECORDS and len(df2) > MAX_RECORDS:
        df2 = df2.head(MAX_RECORDS).reset_index(drop=True)

    stream_df(df2)
