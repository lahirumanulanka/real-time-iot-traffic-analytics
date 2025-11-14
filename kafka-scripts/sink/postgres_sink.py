#!/usr/bin/env python3
import argparse
import json
import os
import sys
from datetime import datetime, timezone

import psycopg
from kafka import KafkaConsumer


DDL = """
CREATE TABLE IF NOT EXISTS traffic_events (
    id BIGSERIAL PRIMARY KEY,
    detector_id TEXT,
    detector_type TEXT,
    detector_status TEXT,
    signal_id DOUBLE PRECISION,
    ts TIMESTAMPTZ,
    is_operational BOOLEAN,
    processed_at TIMESTAMPTZ,
    raw JSONB
);
CREATE INDEX IF NOT EXISTS idx_traffic_events_ts ON traffic_events (ts);
CREATE INDEX IF NOT EXISTS idx_traffic_events_detector_ts ON traffic_events (detector_id, ts);
"""


def parse_iso8601(ts: str | None) -> datetime | None:
    if not ts:
        return None
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None


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


def get_conn(args):
    return psycopg.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_db,
        user=args.pg_user,
        password=args.pg_password,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Persist processed Kafka events into PostgreSQL.")
    ap.add_argument("--bootstrap", default="localhost:29092", help="Kafka bootstrap servers")
    ap.add_argument("--topic", default="iot.traffic.processed", help="Topic to consume (processed)")
    ap.add_argument("--group-id", default="pg-sink", help="Consumer group id")
    ap.add_argument("--from-beginning", action="store_true", help="Consume from earliest")
    ap.add_argument("--batch", type=int, default=100, help="Insert commit batch size")

    # Postgres connection options (env var defaults)
    ap.add_argument("--pg-host", default=os.getenv("PGHOST", "localhost"))
    ap.add_argument("--pg-port", type=int, default=int(os.getenv("PGPORT", "5432")))
    ap.add_argument("--pg-db", default=os.getenv("PGDATABASE", "traffic"))
    ap.add_argument("--pg-user", default=os.getenv("PGUSER", "traffic"))
    ap.add_argument("--pg-password", default=os.getenv("PGPASSWORD", "traffic"))

    args = ap.parse_args()

    consumer = build_consumer(args.bootstrap, args.topic, args.from_beginning, args.group_id)

    conn = get_conn(args)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute(DDL)
    conn.commit()

    sql = (
        "INSERT INTO traffic_events (detector_id, detector_type, detector_status, signal_id, ts, is_operational, processed_at, raw) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    )

    count = 0
    total = 0
    print("Starting PostgreSQL sink ...")
    try:
        for msg in consumer:
            payload = msg.value if isinstance(msg.value, dict) else {}
            detector_id = payload.get("detector_id")
            detector_type = payload.get("detector_type")
            detector_status = payload.get("detector_status")
            signal_id = payload.get("signal_id")
            ts = parse_iso8601(payload.get("timestamp"))
            processed_at = parse_iso8601(payload.get("processed_at"))
            is_operational = payload.get("is_operational")

            cur.execute(
                sql,
                (
                    detector_id,
                    detector_type,
                    detector_status,
                    signal_id,
                    ts,
                    bool(is_operational) if is_operational is not None else None,
                    processed_at,
                    json.dumps(payload),
                ),
            )
            count += 1
            total += 1
            if count >= args.batch:
                conn.commit()
                print(f"Committed batch of {count}. Total={total}")
                count = 0
    except KeyboardInterrupt:
        print("Stopping sink...")
    finally:
        if count:
            conn.commit()
            print(f"Committed final batch of {count}. Total={total}")
        cur.close()
        conn.close()
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
