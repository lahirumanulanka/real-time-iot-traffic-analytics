import os
import json
import time
import random
from pathlib import Path
from typing import Dict, Iterable, List

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
STREAM_DELAY_MS = int(os.getenv("STREAM_DELAY_MS", "1000"))  # default 1s for continuous
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "0"))  # 0 = unlimited
TIMESTAMP_MODE = os.getenv("TIMESTAMP_MODE", "original")  # "original" or "now"
SHUFFLE = os.getenv("SHUFFLE", "0") in {"1", "true", "TRUE", "yes", "YES"}
LOG_EVERY = int(os.getenv("LOG_EVERY", "500"))


def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        api_version_auto_timeout_ms=15000,
    )


def load_dataframe() -> pd.DataFrame:
    data_dir = Path(__file__).parent / "dataset"
    default_csv = data_dir / "traffic_counts.csv"
    default_jsonl = data_dir / "processed" / "cleaned_detectors.jsonl"

    jsonl_env = os.getenv("JSONL_PATH")
    csv_env = os.getenv("CSV_PATH")

    if jsonl_env:
        p = Path(jsonl_env)
        if not p.exists():
            raise FileNotFoundError(f"JSONL not found: {p}")
        return pd.read_json(p, lines=True, dtype=False)
    if default_jsonl.exists():
        return pd.read_json(default_jsonl, lines=True, dtype=False)

    # Fallback: preprocess CSV quickly (minimal fields)
    path = Path(csv_env) if csv_env else default_csv
    if not path.exists():
        raise FileNotFoundError(f"No source data found: {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False).replace({"": None})

    # map minimal schema
    def clean_id(x):
        if pd.isna(x):
            return None
        return str(x).replace(",", "").strip()

    ts = None
    if "modified_date" in df.columns:
        ts = pd.to_datetime(df["modified_date"], errors="coerce", utc=True)
    if (ts is None or (hasattr(ts, 'isna') and ts.isna().all())) and "created_date" in df.columns:
        ts = pd.to_datetime(df["created_date"], errors="coerce", utc=True)

    det_series = df["detector_id"] if "detector_id" in df.columns else pd.Series([None] * len(df))
    out = pd.DataFrame({
        "sensor_id": det_series.map(clean_id),
        "timestamp": ts,
        "count": 1,
    })
    out = out.dropna(subset=["sensor_id", "timestamp"]).reset_index(drop=True)
    out = out.sort_values("timestamp").reset_index(drop=True)
    return out


def df_to_records(df: pd.DataFrame) -> List[Dict]:
    recs: List[Dict] = []
    for _, r in df.iterrows():
        ts = pd.to_datetime(str(r.get("timestamp") or ""), errors="coerce", utc=True)
        iso = ts.isoformat() if pd.notna(ts) else pd.Timestamp.utcnow().isoformat()
        recs.append({
            "sensor_id": str(r.get("sensor_id")),
            "timestamp": iso,
            "count": int(r.get("count", 1)),
            # pass through optional fields if present
            "detector_type": r.get("detector_type"),
            "detector_status": r.get("detector_status"),
            "detector_direction": r.get("detector_direction"),
            "detector_movement": r.get("detector_movement"),
            "location_name": r.get("location_name"),
            "atd_location_id": r.get("atd_location_id"),
            "signal_id": r.get("signal_id"),
            "location_latitude": r.get("location_latitude"),
            "location_longitude": r.get("location_longitude"),
        })
    return recs


def continuous_iter(records: List[Dict]) -> Iterable[Dict]:
    idx = list(range(len(records)))
    i = 0
    while True:
        if SHUFFLE and i % len(idx) == 0:
            random.shuffle(idx)
        rec = records[idx[i % len(idx)]]
        i += 1
        yield rec


def maybe_now_timestamp(rec: Dict) -> Dict:
    if TIMESTAMP_MODE.lower() == "now":
        rec = dict(rec)
        rec["timestamp"] = pd.Timestamp.utcnow().isoformat()
    return rec


def main():
    df = load_dataframe()
    records = df_to_records(df)
    if not records:
        print("No records to stream.")
        return

    producer = get_producer()
    print(f"Continuous producing to {TOPIC} on {BOOTSTRAP} ... base_records={len(records)}; delay={STREAM_DELAY_MS}ms; shuffle={SHUFFLE}; ts_mode={TIMESTAMP_MODE}")

    sent = 0
    try:
        for base in continuous_iter(records):
            rec = maybe_now_timestamp(base)
            producer.send(topic=TOPIC, value=rec, key=rec["sensor_id"]) 
            sent += 1
            if STREAM_DELAY_MS > 0:
                time.sleep(STREAM_DELAY_MS / 1000.0)
            if sent % LOG_EVERY == 0:
                print(f"Sent {sent} messages...")
            if MAX_RECORDS and sent >= MAX_RECORDS:
                print(f"Reached MAX_RECORDS={MAX_RECORDS}; exiting.")
                break
    finally:
        producer.flush()
        print(f"Done. Sent {sent} messages.")


if __name__ == "__main__":
    main()
