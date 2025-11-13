#!/usr/bin/env python3
"""Process raw traffic readings from `iot.traffic.raw` and publish to `iot.traffic.processed`.

Schema updated for traffic_counts_kafka.json:
- Derives `is_operational` from detector_status (True if startswith 'OK').
- Preserves original fields and adds `processed_at`.
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Any, Dict

from kafka import KafkaConsumer, KafkaProducer


def try_json(value: bytes) -> Any:
    text = value.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def derive(rec: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(rec)
    status = (rec.get("detector_status") or "").strip()
    is_operational = status.startswith("OK")
    out.update({
        "is_operational": bool(is_operational),
        "processed_at": datetime.now(timezone.utc).isoformat(),
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
    parser = argparse.ArgumentParser(description="Process raw readings and publish processed outputs.")
    parser.add_argument("--bootstrap", default="localhost:29092", help="Kafka bootstrap servers")
    parser.add_argument("--raw-topic", default="iot.traffic.raw", help="Source raw topic")
    parser.add_argument("--processed-topic", default="iot.traffic.processed", help="Destination processed topic")
    parser.add_argument("--group-id", default="traffic-processor", help="Consumer group id")
    parser.add_argument("--from-beginning", action="store_true", help="Start from earliest offsets")
    parser.add_argument("--max", type=int, default=0, help="Max messages to process (0=unbounded)")
    args = parser.parse_args()

    auto_offset_reset = "earliest" if args.from_beginning else "latest"
    consumer = KafkaConsumer(
        args.raw_topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group_id,
        enable_auto_commit=False,
        auto_offset_reset=auto_offset_reset,
    )
    producer = build_producer(args.bootstrap)

    count = 0
    try:
        for msg in consumer:
            parsed = try_json(msg.value)
            if not isinstance(parsed, dict):
                continue
            key = parsed.get("detector_id")
            processed = derive(parsed)
            producer.send(args.processed_topic, key=key, value=processed)
            print(
                f"Processed#{count+1} key={key} type={processed.get('detector_type')} "
                f"status={processed.get('detector_status')} is_operational={processed.get('is_operational')}"
            )
            count += 1
            if args.max > 0 and count >= args.max:
                break
    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        producer.flush(10)
        producer.close()
        consumer.close()
    print(f"Processed and published {count} message(s) to '{args.processed_topic}'.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
