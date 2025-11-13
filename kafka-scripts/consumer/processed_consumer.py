#!/usr/bin/env python3
import argparse
import json
import sys
from kafka import KafkaConsumer


def try_json(value: bytes):
    text = value.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def main() -> int:
    parser = argparse.ArgumentParser(description="Consume processed traffic messages and print summaries.")
    parser.add_argument("--bootstrap", default="localhost:29092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="iot.traffic.processed", help="Processed topic name")
    parser.add_argument("--group-id", default="processed-consumer", help="Consumer group id")
    parser.add_argument("--from-beginning", action="store_true", help="Start from earliest offsets")
    parser.add_argument("--max", type=int, default=0, help="Max messages to read (0=unbounded)")
    parser.add_argument("--show-keys", action="store_true", help="Print message keys")
    args = parser.parse_args()

    auto_offset_reset = "earliest" if args.from_beginning else "latest"
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group_id,
        enable_auto_commit=False,
        auto_offset_reset=auto_offset_reset,
    )

    count = 0
    try:
        for msg in consumer:
            parsed = try_json(msg.value)
            key_display = f" key={msg.key.decode('utf-8', errors='replace')}" if (args.show_keys and msg.key) else ""
            if isinstance(parsed, dict):
                print(
                    f"Processed#{count+1}{key_display} det={parsed.get('detector_id')} cnt={parsed.get('vehicle_count')} "
                    f"speed_kph={parsed.get('avg_speed_kph')} congested={parsed.get('is_congested')}"
                )
            else:
                print(f"Processed#{count+1}{key_display} {parsed}")
            count += 1
            if args.max > 0 and count >= args.max:
                break
    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        consumer.close()
    print(f"Consumed {count} processed message(s) from '{args.topic}'.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
