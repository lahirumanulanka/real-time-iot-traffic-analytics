#!/usr/bin/env python3
import argparse
import json
import sys
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
        key_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Forward processed events to a viz topic.")
    ap.add_argument("--bootstrap", default="localhost:29092", help="Kafka bootstrap servers")
    ap.add_argument("--from-topic", default="iot.traffic.processed", help="Source topic (processed)")
    ap.add_argument("--to-topic", default="iot.traffic.viz", help="Destination topic for visualization")
    ap.add_argument("--group-id", default="viz-forwarder", help="Consumer group id")
    ap.add_argument("--from-beginning", action="store_true", help="Read from earliest")
    ap.add_argument("--max", type=int, default=0, help="Stop after N messages (0=run forever)")

    args = ap.parse_args()
    consumer = build_consumer(args.bootstrap, args.from_topic, args.from_beginning, args.group_id)
    producer = build_producer(args.bootstrap)

    print(f"Forwarding {args.from_topic} -> {args.to_topic} ...")
    total = 0
    try:
        for msg in consumer:
            key = msg.key if isinstance(msg.key, str) else (msg.key.decode("utf-8") if msg.key else None)
            value = msg.value

            # keep same payload; optionally you could slim it down here
            producer.send(args.to_topic, key=key, value=value)
            total += 1
            if total % 100 == 0:
                print(f"Forwarded {total} messages")
            if args.max and total >= args.max:
                break
        producer.flush()
    except KeyboardInterrupt:
        print("Stopping forwarder...")
    print(f"Done. Forwarded {total} message(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
