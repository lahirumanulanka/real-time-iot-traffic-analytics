from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Tuple


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
