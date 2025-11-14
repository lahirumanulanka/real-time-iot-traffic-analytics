#!/usr/bin/env python3
import argparse
import json
import sys
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer


def build_consumer(bootstrap: str, topic: str, from_beginning: bool, group_id: str | None) -> KafkaConsumer:
    auto_offset = "earliest" if from_beginning else "latest"
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset=auto_offset,
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
    )


def build_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if v is not None else None,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Forward processed events to a visualization topic.")
    ap.add_argument("--bootstrap", default="localhost:29092", help="Kafka bootstrap servers")
    ap.add_argument("--from-topic", default="iot.traffic.processed", help="Source processed topic")
    ap.add_argument("--to-topic", default="iot.traffic.visualization", help="Destination visualization topic")
    ap.add_argument("--group-id", default="viz-forwarder", help="Consumer group id")
    ap.add_argument("--from-beginning", action="store_true", help="Consume from earliest")
    ap.add_argument("--max", type=int, default=0, help="Stop after N messages (0 = unlimited)")

    args = ap.parse_args()

    consumer = build_consumer(args.bootstrap, args.from_topic, args.from_beginning, args.group_id)
    producer = build_producer(args.bootstrap)

    total = 0
    print(f"Forwarding from '{args.from_topic}' to '{args.to_topic}' ...")
    try:
        for msg in consumer:
            key = msg.key if isinstance(msg.key, str) else (msg.key.decode("utf-8") if msg.key else None)
            payload = msg.value if isinstance(msg.value, dict) else {}

            # Minimal pass-through for visualization, ensure expected fields exist
            out = {
                "detector_id": payload.get("detector_id"),
                "detector_type": payload.get("detector_type"),
                "detector_status": payload.get("detector_status"),
                "signal_id": payload.get("signal_id"),
                "timestamp": payload.get("timestamp"),
                "is_operational": payload.get("is_operational"),
                "processed_at": payload.get("processed_at"),
            }

            producer.send(args.to_topic, value=out, key=key)
            total += 1
            if args.max and total >= args.max:
                break

        producer.flush()
    finally:
        try:
            producer.close()
        except Exception:
            pass
    print(f"Forwarded {total} message(s) to '{args.to_topic}'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
