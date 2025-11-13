#!/usr/bin/env python3
import argparse
import sys
from kafka import KafkaConsumer


def main() -> int:
    parser = argparse.ArgumentParser(description="Consume messages using kafka-python.")
    parser.add_argument("--bootstrap", default="localhost:29092", help="Bootstrap servers (default: localhost:29092)")
    parser.add_argument("--topic", default="sample.traffic.raw", help="Topic to consume")
    parser.add_argument("--max-messages", type=int, default=20, help="Maximum messages to read")
    parser.add_argument("--from-beginning", action="store_true", help="Start from earliest offsets")
    args = parser.parse_args()

    auto_offset_reset = "earliest" if args.from_beginning else "latest"
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id="traffic-consumer-kafkapy",
        enable_auto_commit=False,
        auto_offset_reset=auto_offset_reset,
        value_deserializer=lambda v: v.decode("utf-8", errors="replace"),
    )

    count = 0
    try:
        for msg in consumer:
            print(msg.value)
            count += 1
            if count >= args.max_messages:
                break
    finally:
        consumer.close()
    print(f"Consumed {count} message(s) from '{args.topic}'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
