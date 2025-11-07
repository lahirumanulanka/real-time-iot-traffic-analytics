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

from analytics import HourlyAggregator, compute_daily_peak, compute_daily_availability
import csv
from datetime import datetime
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
METRICS_TOPIC = os.getenv("METRICS_TOPIC", "traffic.metrics")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "traffic-consumer")
SIMPLE_CONSUMER = os.getenv("SIMPLE_CONSUMER", "0") in {"1", "true", "TRUE", "yes", "YES"}
OUTPUT_JSON = Path(__file__).parent / "output" / "processed_data.json"
MAX_MESSAGES = int(os.getenv("CONSUMER_MAX_MESSAGES", "0"))  # 0 = run forever
PERSIST_TO_DB = os.getenv("PERSIST_TO_DB", "0") in {"1", "true", "TRUE", "yes", "YES"}
ARCHIVE_TO_S3 = os.getenv("ARCHIVE_TO_S3", "0") in {"1", "true", "TRUE", "yes", "YES"}
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_PREFIX = os.getenv("S3_PREFIX", "traffic/metrics")


def get_consumer() -> KafkaConsumer:
    common = dict(
        bootstrap_servers=BOOTSTRAP.split(","),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="earliest",
        api_version_auto_timeout_ms=15000,
    )
    if SIMPLE_CONSUMER:
        # No group coordinator required; read from earliest without commits
        return KafkaConsumer(RAW_TOPIC, enable_auto_commit=False, group_id=None, **common)
    else:
        return KafkaConsumer(RAW_TOPIC, enable_auto_commit=True, group_id=CONSUMER_GROUP, **common)


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
                # Hourly aggregates go to processed topic
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

            # Periodically compute daily metrics (after each 50 processed messages)
            if len(processed) and len(processed) % 50 == 0:
                snapshot = list(aggregator.snapshot())
                peak = compute_daily_peak(snapshot)
                availability = list(compute_daily_availability(snapshot))
                daily_metrics = {"daily_peak": peak, "daily_availability": availability}
                # Append daily metrics marker record for downstream (could go to separate topic)
                # Daily metrics go to metrics topic (for visualization or downstream)
                producer.send(METRICS_TOPIC, value={"_daily_metrics": daily_metrics}, key="daily")
                processed.append({"_daily_metrics": daily_metrics})
                if PERSIST_TO_DB and db_connector is not None and peak:
                    try:
                        # Insert peak volume row
                        with db_connector.get_conn() as conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    "INSERT INTO daily_peak_volume(day, peak_hour, peak_volume) VALUES (%s, %s, %s) ON CONFLICT (day) DO UPDATE SET peak_hour=EXCLUDED.peak_hour, peak_volume=EXCLUDED.peak_volume, updated_at=NOW()",
                                    (peak["day"], peak["peak_hour"], peak["peak_volume"]),
                                )
                                # Availability rows
                                for av in availability:
                                    cur.execute(
                                        "INSERT INTO sensor_daily_availability(sensor_id, day, hours_seen, hours_expected, availability) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (sensor_id, day) DO UPDATE SET hours_seen=EXCLUDED.hours_seen, hours_expected=EXCLUDED.hours_expected, availability=EXCLUDED.availability, updated_at=NOW()",
                                        (av["sensor_id"], av["day"], av["hours_seen"], av["hours_expected"], av["availability"]),
                                    )
                            conn.commit()
                    except Exception as e:
                        print(f"DB daily metric insert failed: {e}")

                # Optional S3 archival (CSV) of latest snapshots
                if ARCHIVE_TO_S3 and S3_BUCKET:
                    try:
                        import boto3  # lazy import
                        s3 = boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT"))
                        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                        # Hourly snapshot CSV
                        hourly_key = f"{S3_PREFIX}/hourly_snapshot_{ts}.csv"
                        availability_key = f"{S3_PREFIX}/availability_{ts}.csv"
                        from io import StringIO
                        buf1 = StringIO()
                        w1 = csv.writer(buf1)
                        w1.writerow(["sensor_id", "hour", "avg_count", "samples"])
                        for h in snapshot:
                            w1.writerow([h["sensor_id"], h["hour"], h["avg_count"], h["samples"]])
                        s3.put_object(Bucket=S3_BUCKET, Key=hourly_key, Body=buf1.getvalue().encode("utf-8"))
                        buf2 = StringIO()
                        w2 = csv.writer(buf2)
                        w2.writerow(["sensor_id", "day", "hours_seen", "hours_expected", "availability"])
                        for av in availability:
                            w2.writerow([av["sensor_id"], av["day"], av["hours_seen"], av["hours_expected"], av["availability"]])
                        s3.put_object(Bucket=S3_BUCKET, Key=availability_key, Body=buf2.getvalue().encode("utf-8"))
                        print(f"Archived metrics to s3://{S3_BUCKET}/{hourly_key} and {availability_key}")
                    except Exception as e:
                        print(f"S3 archival failed: {e}")

            if MAX_MESSAGES and seen >= MAX_MESSAGES:
                print(f"Reached MAX_MESSAGES={MAX_MESSAGES}; exiting.")
                break
    finally:
        producer.flush()
        # Always try to persist the latest records on exit
        try:
            OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
            OUTPUT_JSON.write_text(json.dumps(processed[-400:], indent=2))
        except Exception:
            pass


if __name__ == "__main__":
    main()
