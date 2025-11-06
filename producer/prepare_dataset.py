import json
from pathlib import Path
import pandas as pd
from datetime import datetime

RAW_PATH = Path(__file__).parent / "dataset" / "traffic_counts.csv"
OUT_DIR = Path(__file__).parent / "dataset" / "processed"
OUT_JSONL = OUT_DIR / "cleaned_detectors.jsonl"
OUT_PARQUET = OUT_DIR / "cleaned_detectors.parquet"


# (unused helper placeholder removed)


def load_raw() -> pd.DataFrame:
    if not RAW_PATH.exists():
        raise FileNotFoundError(f"Missing dataset: {RAW_PATH}")
    # Read as strings to preserve formatting (e.g., '2,060')
    df = pd.read_csv(RAW_PATH, dtype=str)
    return df


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    # Helper to clean detector_id -> sensor_id
    def clean_id(x):
        if pd.isna(x):
            return None
        return str(x).replace(",", "").strip()

    # Build timestamp from modified_date then created_date
    ts = None
    if "modified_date" in df.columns:
        ts = pd.to_datetime(df["modified_date"], errors="coerce", utc=True)
    if (ts is None or ts.isna().all()) and "created_date" in df.columns:
        ts = pd.to_datetime(df["created_date"], errors="coerce", utc=True)

    # Compose normalized frame
    det_series = df["detector_id"] if "detector_id" in df.columns else pd.Series([None] * len(df))
    out = pd.DataFrame(
        {
            "sensor_id": det_series.map(clean_id),
            "timestamp": ts,
            "count": 1,
            # keep useful attributes
            "detector_type": df.get("detector_type"),
            "detector_status": df.get("detector_status"),
            "detector_direction": df.get("detector_direction"),
            "detector_movement": df.get("detector_movement"),
            "location_name": df.get("location_name"),
            "atd_location_id": df.get("atd_location_id"),
            "signal_id": df.get("signal_id"),
            "location_latitude": df.get("location_latitude"),
            "location_longitude": df.get("location_longitude"),
        }
    )

    # Basic cleaning: drop missing, drop NaT timestamps
    out = out.dropna(subset=["sensor_id", "timestamp"]).reset_index(drop=True)

    # Deduplicate on sensor_id+timestamp
    out = out.drop_duplicates(subset=["sensor_id", "timestamp"])  # keep first

    # Normalize timestamp to ISO8601
    ts_series = pd.to_datetime(out["timestamp"], utc=True, errors="coerce").dt.tz_convert("UTC")
    out["timestamp"] = ts_series.apply(lambda t: t.isoformat())

    # Sort by time
    out = out.sort_values(by=["timestamp", "sensor_id"]).reset_index(drop=True)

    return out


def write_outputs(df: pd.DataFrame):
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # JSONL
    with OUT_JSONL.open("w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            rec = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
            f.write(json.dumps(rec) + "\n")

    # Parquet (requires pyarrow)
    try:
        df.to_parquet(OUT_PARQUET, index=False)
    except Exception as e:
        print(f"Parquet write failed (will skip): {e}")


if __name__ == "__main__":
    raw = load_raw()
    cleaned = clean_df(raw)
    write_outputs(cleaned)
    print(f"Wrote {len(cleaned)} records to:\n - {OUT_JSONL}\n - {OUT_PARQUET if OUT_PARQUET.exists() else '(parquet skipped)'}")
