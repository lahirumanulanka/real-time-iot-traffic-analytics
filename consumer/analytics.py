from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Tuple, Iterable


@dataclass
class RunningAverage:
    total: int = 0
    count: int = 0

    def add(self, value: int):
        self.total += value
        self.count += 1

    @property
    def avg(self) -> float:
        return (self.total / self.count) if self.count else 0.0


@dataclass
class HourlyAggregator:
    # key: (sensor_id, hour_key)
    buckets: Dict[Tuple[str, str], RunningAverage] = field(default_factory=dict)

    @staticmethod
    def hour_key(ts_iso: str) -> str:
        # normalize to hour precision in UTC
        dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00")).astimezone()
        return dt.strftime("%Y-%m-%dT%H:00:00%z")

    def add(self, sensor_id: str, ts_iso: str, count: int):
        key = (sensor_id, self.hour_key(ts_iso))
        self.buckets.setdefault(key, RunningAverage()).add(count)

    def snapshot(self):
        for (sensor_id, hour), ra in self.buckets.items():
            yield {
                "sensor_id": sensor_id,
                "hour": hour,
                "avg_count": round(ra.avg, 3),
                "samples": ra.count,
            }


def day_key(ts_iso: str) -> str:
    dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d")


def compute_daily_peak(hourly_snapshot: Iterable[Dict]) -> Dict:
    """Compute daily peak traffic volume (sum of counts per hour across all sensors)."""
    daily_hours: Dict[Tuple[str, str], float] = {}
    for rec in hourly_snapshot:
        day = rec["hour"][0:10]  # YYYY-MM-DD from hour key
        hour_key = rec["hour"]
        daily_hours.setdefault((day, hour_key), 0.0)
        daily_hours[(day, hour_key)] += rec["avg_count"] * rec["samples"]  # reconstruct total
    # Find peak per day
    peaks: Dict[str, Dict] = {}
    for (day, hour_key), total in daily_hours.items():
        current = peaks.get(day)
        if current is None or total > current["peak_volume"]:
            peaks[day] = {"day": day, "peak_hour": hour_key, "peak_volume": int(total)}
    # Return latest day peak (could expand to list)
    if not peaks:
        return {}
    latest_day = sorted(peaks.keys())[-1]
    return peaks[latest_day]


def compute_daily_availability(hourly_snapshot: Iterable[Dict]) -> Iterable[Dict]:
    """Yield availability per sensor for each day based on number of distinct hours seen."""
    sensor_day_hours: Dict[Tuple[str, str], set] = {}
    for rec in hourly_snapshot:
        sensor = rec["sensor_id"]
        day = rec["hour"][0:10]
        sensor_day_hours.setdefault((sensor, day), set()).add(rec["hour"])
    for (sensor, day), hours in sensor_day_hours.items():
        hours_seen = len(hours)
        # Assume expected 24 hours per day (could adjust for partial days)
        hours_expected = 24
        availability = round((hours_seen / hours_expected) * 100, 2)
        yield {
            "sensor_id": sensor,
            "day": day,
            "hours_seen": hours_seen,
            "hours_expected": hours_expected,
            "availability": availability,
        }
