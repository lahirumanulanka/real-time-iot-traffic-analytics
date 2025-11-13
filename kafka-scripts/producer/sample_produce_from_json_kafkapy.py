#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path
from kafka import KafkaProducer


def load_json_records(path: Path, max_records: int):
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if text[0] == "[":
        data = json.loads(text)
        if max_records:
            return data[:max_records]
        return data
    # NDJSON fallback
    records = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        records.append(json.loads(line))
        if max_records and len(records) >= max_records:
            break
    return records


def main() -> int:
    parser = argparse.ArgumentParser(description="Produce JSON array or NDJSON file records using kafka-python.")
    parser.add_argument("--bootstrap", default="localhost:29092", help="Bootstrap servers (default: localhost:29092)")
    parser.add_argument("--topic", default="sample.traffic.raw", help="Target topic name")
    parser.add_argument("--json-path", default=str(Path.cwd() / "data" / "sample" / "traffic_sample.json"), help="Path to JSON file")
    parser.add_argument("--max-records", type=int, default=0, help="Maximum records to send (0=all)")
    args = parser.parse_args()

    json_path = Path(args.json_path)
    try:
        records = load_json_records(json_path, args.max_records)
    except FileNotFoundError as e:
        print(str(e), file=sys.stderr)
        return 2

    if not records:
        print("No records found to send.")
        return 0

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        linger_ms=5,
    )

    for rec in records:
        producer.send(args.topic, rec)
    producer.flush(10)
    print(f"Done. Sent {len(records)} record(s) to topic '{args.topic}'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
