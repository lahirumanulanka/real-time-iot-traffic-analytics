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
    parser = argparse.ArgumentParser(description="Consume traffic messages (raw or processed) and print summaries.")
    parser.add_argument("--bootstrap", default="localhost:29092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="iot.traffic.raw", help="Topic name (raw or processed)")
    parser.add_argument("--group-id", default="traffic-consumer", help="Consumer group id")
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
                if 'is_operational' in parsed:
                    # Processed dataset format
                    print(
                        f"Msg#{count+1}{key_display} det={parsed.get('detector_id')} type={parsed.get('detector_type')} "
                        f"status={parsed.get('detector_status')} is_operational={parsed.get('is_operational')}"
                    )
                else:
                    # Raw dataset format
                    print(
                        f"Msg#{count+1}{key_display} det={parsed.get('detector_id')} type={parsed.get('detector_type')} "
                        f"status={parsed.get('detector_status')} ts={parsed.get('timestamp')}"
                    )
            else:
                print(f"Msg#{count+1}{key_display} {parsed}")
            count += 1
            if args.max > 0 and count >= args.max:
                break
    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        consumer.close()
    print(f"Consumed {count} message(s) from '{args.topic}'.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
