#!/usr/bin/env python3
import argparse
import json
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer


def parse_iso8601(ts: str) -> datetime:
    # Handle Z suffix
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts).astimezone(timezone.utc)


def hour_bucket(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def day_bucket(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def clean_sensor_id(raw: str | None) -> str | None:
    if raw is None:
        return None
    return raw.replace(",", "").strip()


class MetricsState:
    def __init__(self):
        # (sensor, hour)->count
        self.hourly_counts_per_sensor: dict[tuple[str, datetime], int] = defaultdict(int)
        # (day)->[24] hour totals across sensors
        self.day_hour_totals: dict[datetime, list[int]] = defaultdict(lambda: [0] * 24)
        # (sensor, day)->set(hours_present)
        self.sensor_day_hours_present: dict[tuple[str, datetime], set[int]] = defaultdict(set)

    def update(self, sensor: str, event_ts: datetime):
        hb = hour_bucket(event_ts)
        db = day_bucket(event_ts)
        hour_idx = hb.hour

        # Hourly per sensor count
        self.hourly_counts_per_sensor[(sensor, hb)] += 1

        # Daily peak calc uses per-hour totals across sensors
        self.day_hour_totals[db][hour_idx] += 1

        # Availability: mark hour as present for sensor in day
        self.sensor_day_hours_present[(sensor, db)].add(hour_idx)

    def current_daily_peak(self, db: datetime) -> tuple[int, int]:
        # returns (hour_index, count)
        if db not in self.day_hour_totals:
            return (0, 0)
        arr = self.day_hour_totals[db]
        mx = max(arr)
        idx = arr.index(mx)
        return (idx, mx)

    def availability_pct(self, sensor: str, db: datetime) -> float:
        hours = self.sensor_day_hours_present.get((sensor, db), set())
        return round(100.0 * len(hours) / 24.0, 2)

    def to_snapshot(self) -> dict:
        # Provide a compact snapshot that can be exported as JSON
        hr_counts = {
            f"{s}|{hb.isoformat()}": c for (s, hb), c in self.hourly_counts_per_sensor.items()
        }
        day_totals = {db.isoformat(): arr for db, arr in self.day_hour_totals.items()}
        availability = {
            f"{s}|{db.isoformat()}": round(100.0 * len(hours) / 24.0, 2)
            for (s, db), hours in self.sensor_day_hours_present.items()
        }
        peaks = {db.isoformat(): self.current_daily_peak(db) for db in self.day_hour_totals}
        return {
            "hourly_counts_per_sensor": hr_counts,
            "day_hour_totals": day_totals,
            "daily_peak_hour": peaks,
            "sensor_daily_availability_pct": availability,
        }


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


def main():
    ap = argparse.ArgumentParser(description="Compute streaming traffic metrics from Kafka events.")
    ap.add_argument("--bootstrap", default="localhost:29092", help="Kafka bootstrap servers")
    ap.add_argument("--topic", default="iot.traffic.processed", help="Topic to consume")
    ap.add_argument("--group-id", default="metrics-agg", help="Consumer group id")
    ap.add_argument("--from-beginning", action="store_true", help="Read from earliest")
    ap.add_argument("--print-every", type=float, default=10.0, help="Seconds between summary prints")
    ap.add_argument("--export-json", default="", help="Path to write metrics snapshot JSON periodically")
    ap.add_argument("--max", type=int, default=0, help="Stop after N messages (0 = run forever)")

    args = ap.parse_args()

    export_path = Path(args.export_json) if args.export_json else None
    state = MetricsState()
    consumer = build_consumer(args.bootstrap, args.topic, args.from_beginning, args.group_id)

    print("Starting metrics aggregation...")
    last_print = time.time()
    total = 0

    stop = False

    def handle_sigint(signum, frame):
        nonlocal stop
        stop = True
        print("\nStopping metrics aggregator...")

    signal.signal(signal.SIGINT, handle_sigint)

    for msg in consumer:
        if stop:
            break
        total += 1
        key = clean_sensor_id(msg.key) if isinstance(msg.key, str) else clean_sensor_id(str(msg.key) if msg.key is not None else None)
        payload = msg.value
        try:
            ts = parse_iso8601(payload.get("timestamp"))
        except Exception:
            # Fallback to Kafka timestamp if message lacks proper field
            ts = datetime.fromtimestamp(msg.timestamp / 1000.0, tz=timezone.utc)

        if key is None:
            key = clean_sensor_id(payload.get("detector_id")) or "unknown"

        state.update(key, ts)

        now = time.time()
        if now - last_print >= args.print_every:
            # Print a small rotating summary
            db = day_bucket(ts)
            peak_hour, peak_count = state.current_daily_peak(db)
            avail = state.availability_pct(key, db)
            print(
                f"Msgs={total} | Day={db.date()} | PeakHour={peak_hour}:00 cnt={peak_count} | Sensor={key} availability={avail}%"
            )
            if export_path:
                try:
                    export_path.parent.mkdir(parents=True, exist_ok=True)
                    with export_path.open("w", encoding="utf-8") as f:
                        json.dump(state.to_snapshot(), f, indent=2)
                except Exception as e:
                    print("Export failed:", e)
            last_print = now

        if args.max and total >= args.max:
            break

    print(f"Processed {total} message(s). Bye.")


if __name__ == "__main__":
    sys.exit(main())
