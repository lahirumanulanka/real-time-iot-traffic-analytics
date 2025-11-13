#!/usr/bin/env python3
"""Generate and stream synthetic sensor readings to Kafka at fixed intervals.

Schema aligned to traffic_counts_kafka.json:
    detector_id: str
    detector_type: str (e.g., LOOP, VIDEO, RADAR - Wavetronix)
    detector_status: str (e.g., OK, OK - MINOR ISSUE, BROKEN, REMOVED)
    signal_id: float
    timestamp: ISO8601 UTC string

Usage:
  python sensor_stream_producer.py --bootstrap localhost:29092 \
      --topic iot.traffic.raw --interval 5 --sensor-ids 101 102 103

Ctrl+C to stop.
"""
import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone
from typing import List

from kafka import KafkaProducer


STATUSES = ["OK", "OK - MINOR ISSUE", "BROKEN", "REMOVED"]
TYPES = ["LOOP", "VIDEO", "RADAR - Wavetronix"]


def build_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        key_serializer=lambda k: (str(k).encode("utf-8") if k is not None else None),
        linger_ms=5,
    )


def next_reading(detector_id: str) -> dict:
    return {
        "detector_id": detector_id,
        "detector_type": random.choice(TYPES),
        "detector_status": random.choices(STATUSES, weights=[0.8, 0.1, 0.05, 0.05])[0],
        "signal_id": round(random.uniform(20.0, 1100.0), 1),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Stream synthetic sensor readings to Kafka (dataset schema).")
    parser.add_argument("--bootstrap", default="localhost:29092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="iot.traffic.raw", help="Kafka topic name")
    parser.add_argument("--interval", type=float, default=5.0, help="Seconds between messages (default 5.0)")
    parser.add_argument("--sensor-ids", nargs="*", default=["1001", "1002", "1003"], help="List of detector IDs to cycle through")
    parser.add_argument("--max", type=int, default=0, help="Max messages to send (0=unbounded)")
    args = parser.parse_args()

    sensor_ids: List[str] = [str(s) for s in args.sensor_ids] or ["1001"]
    producer = build_producer(args.bootstrap)
    sent = 0
    try:
        while True:
            for sid in sensor_ids:
                reading = next_reading(sid)
                producer.send(args.topic, key=reading["detector_id"], value=reading)
                sent += 1
                print(
                    f"Sent#{sent} det={reading['detector_id']} type={reading['detector_type']} "
                    f"status={reading['detector_status']} signal_id={reading['signal_id']}"
                )
                producer.flush(5)
                if args.max > 0 and sent >= args.max:
                    break
                time.sleep(args.interval)
            if args.max > 0 and sent >= args.max:
                break
    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        producer.flush(10)
        producer.close()
    print(f"Done. Sent {sent} message(s) to topic '{args.topic}'.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
