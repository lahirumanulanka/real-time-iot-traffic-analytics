import json
import os
from pathlib import Path

from dotenv import load_dotenv
import sys
try:
    import six  # type: ignore
    sys.modules.setdefault('kafka.vendor.six', six)
    sys.modules.setdefault('kafka.vendor.six.moves', six.moves)
except Exception:
    pass
from kafka import KafkaConsumer, KafkaProducer

from analytics import HourlyAggregator
try:
    from storage import db_connector
except Exception:
    db_connector = None  # optional

# Load env
ENV_PATH = Path(__file__).resolve().parents[1] / "utils" / "config.env"
load_dotenv(dotenv_path=ENV_PATH)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
RAW_TOPIC = os.getenv("RAW_TOPIC", "traffic.raw")
PROCESSED_TOPIC = os.getenv("PROCESSED_TOPIC", "traffic.processed")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "traffic-consumer")
OUTPUT_JSON = Path(__file__).parent / "output" / "processed_data.json"
MAX_MESSAGES = int(os.getenv("CONSUMER_MAX_MESSAGES", "0"))  # 0 = run forever
PERSIST_TO_DB = os.getenv("PERSIST_TO_DB", "0") in {"1", "true", "TRUE", "yes", "YES"}


def get_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=BOOTSTRAP.split(","),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        group_id=CONSUMER_GROUP,
        api_version_auto_timeout_ms=15000,
    )


def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        api_version_auto_timeout_ms=15000,
    )


def main():
    consumer = get_consumer()
    producer = get_producer()
    aggregator = HourlyAggregator()

    print(f"Consuming from {RAW_TOPIC} on {BOOTSTRAP} -> producing to {PROCESSED_TOPIC}")

    processed = []
    seen = 0
    try:
        for msg in consumer:
            record = msg.value
            sensor_id = str(record.get("sensor_id"))
            timestamp = str(record.get("timestamp"))
            count = int(record.get("count", 0))

            aggregator.add(sensor_id, timestamp, count)

            # Optionally, emit incremental aggregates (per message) as latest snapshot of that hour
            hour_key = aggregator.hour_key(timestamp)
            ra = next(
                (r for r in aggregator.snapshot() if r["sensor_id"] == sensor_id and r["hour"] == hour_key),
                None,
            )
            if ra:
                out = {
                    **ra,
                    "timestamp": timestamp,  # last seen timestamp
                }
                producer.send(PROCESSED_TOPIC, value=out, key=sensor_id)
                processed.append(out)

                if PERSIST_TO_DB and db_connector is not None:
                    try:
                        db_connector.insert_processed_metric(out)
                    except Exception as e:
                        print(f"DB insert failed: {e}")

            seen += 1
            if len(processed) % 100 == 0 and len(processed) > 0:
                print(f"Processed {len(processed)} records...")

            # Persist for local debugging
            if len(processed) and len(processed) % 20 == 0:
                try:
                    OUTPUT_JSON.write_text(json.dumps(processed[-200:], indent=2))
                except Exception as e:
                    print(f"Failed to write debug JSON: {e}")

            if MAX_MESSAGES and seen >= MAX_MESSAGES:
                print(f"Reached MAX_MESSAGES={MAX_MESSAGES}; exiting.")
                break
    finally:
        producer.flush()
        # Always try to persist the latest records on exit
        try:
            OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
            OUTPUT_JSON.write_text(json.dumps(processed[-200:], indent=2))
        except Exception:
            pass


if __name__ == "__main__":
    main()
