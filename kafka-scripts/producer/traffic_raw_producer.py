#!/usr/bin/env python3
"""Publish cleaned traffic readings to Kafka topic `iot.traffic.raw`.

Reads records from JSON array or NDJSON at `data/dataset/traffic_counts_kafka.json`.
Normalizes types and fills missing fields, then streams at a fixed interval.
"""
import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from kafka import KafkaProducer


def load_json_records(path: Path, max_records: int = 0) -> List[Dict[str, Any]]:
    if not path.exists():
        print(f"Source JSON not found: {path}", file=sys.stderr)
        return []
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    records: List[Dict[str, Any]] = []
    if text.startswith("["):
        try:
            data = json.loads(text)
            if isinstance(data, list):
                records = [r for r in data if isinstance(r, dict)]
        except json.JSONDecodeError:
            records = []
    else:
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    records.append(obj)
            except json.JSONDecodeError:
                continue
    if max_records > 0:
        records = records[:max_records]
    return records


def to_int(val: Any, default: Optional[int] = None) -> Optional[int]:
    if val is None:
        return default
    try:
        if isinstance(val, str):
            return int(val.replace(",", "").strip())
        return int(val)
    except Exception:
        return default


def to_float(val: Any, default: Optional[float] = None) -> Optional[float]:
    if val is None:
        return default
    try:
        if isinstance(val, str):
            return float(val.replace(",", "").strip())
        return float(val)
    except Exception:
        return default


def clean_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    detector_id = rec.get("detector_id") or rec.get("id") or rec.get("sensor_id")
    if detector_id is None:
        detector_id = "unknown"
    detector_id = str(detector_id).replace(",", "").strip()

    # Common fields in dataset; fall back gracefully
    vehicle_count = to_int(rec.get("vehicle_count") or rec.get("count") or rec.get("volume"), 0)
    avg_speed_kph = to_float(rec.get("avg_speed_kph") or rec.get("speed_kph") or rec.get("avg_speed"), None)
    detector_status = (rec.get("detector_status") or rec.get("status") or "OK").strip()

    # Timestamp normalize to ISO8601 UTC
    ts = rec.get("timestamp") or rec.get("ts")
    if isinstance(ts, str) and ts:
        timestamp = ts
    else:
        timestamp = datetime.now(timezone.utc).isoformat()

    out = dict(rec)  # retain original fields
    out.update({
        "detector_id": detector_id,
        "vehicle_count": vehicle_count,
        "avg_speed_kph": avg_speed_kph,
        "detector_status": detector_status,
        "timestamp": timestamp,
    })
    return out


def build_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        key_serializer=lambda k: (str(k).encode("utf-8") if k is not None else None),
        linger_ms=5,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish cleaned traffic readings to Kafka.")
    parser.add_argument("--bootstrap", default="localhost:29092", help="Kafka bootstrap servers (host:port)")
    parser.add_argument("--topic", default="iot.traffic.raw", help="Target topic")
    parser.add_argument("--json-path", default=str(Path("data")/"dataset"/"traffic_counts_kafka.json"), help="Source JSON path")
    parser.add_argument("--interval", type=float, default=5.0, help="Seconds between messages (default 5)")
    parser.add_argument("--max", type=int, default=0, help="Max messages to send (0=all)")
    args = parser.parse_args()

    src = Path(args.json_path)
    raw = load_json_records(src, args.max if args.max else 0)
    if not raw:
        print("No source records found to send.")
        return 2

    cleaned = [clean_record(r) for r in raw]
    if args.max > 0:
        cleaned = cleaned[:args.max]

    producer = build_producer(args.bootstrap)
    sent = 0
    try:
        for rec in cleaned:
            key = rec.get("detector_id")
            producer.send(args.topic, key=key, value=rec)
            sent += 1
            print(f"Raw#{sent} key={key} vehicle_count={rec.get('vehicle_count')} speed={rec.get('avg_speed_kph')} status={rec.get('detector_status')}")
            producer.flush(5)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        producer.flush(10)
        producer.close()
    print(f"Done. Sent {sent} message(s) to '{args.topic}'.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
