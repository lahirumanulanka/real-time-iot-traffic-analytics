#!/usr/bin/env python3
"""
Clean and preprocess traffic dataset WITHOUT external dependencies (pure Python).

Steps:
  - Read the quoted CSV (handles commas in values)
  - Parse created_date and modified_date into ISO-8601 UTC strings
  - Normalize numeric fields (detector_id, signal_id)
  - Extract latitude/longitude from WKT POINT in LOCATION
  - Drop rows missing critical identifiers
  - Remove duplicates on (detector_id, location_name, detector_direction)
  - Write cleaned CSV and JSONL for ingestion
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Clean and preprocess traffic dataset (no external deps)")
    p.add_argument("--input", default="data/dataset/traffic_counts.csv", help="Path to input CSV")
    p.add_argument("--outdir", default="data/cleaned", help="Output directory for cleaned files")
    return p.parse_args()


DATE_FORMATS = [
    "%Y %b %d %I:%M:%S %p",
    "%Y-%m-%d %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
]


def parse_date(val: Optional[str]) -> Optional[str]:
    if not val or not str(val).strip():
        return None
    s = str(val).strip()
    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            dt = dt.replace(tzinfo=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            continue
    return None


POINT_RE = re.compile(r"POINT\s*\(\s*(-?\d+\.?\d*)\s+(-?\d+\.?\d*)\s*\)")


def parse_point(wkt: str) -> Tuple[Optional[float], Optional[float]]:
    if not isinstance(wkt, str):
        return None, None
    m = POINT_RE.search(wkt)
    if not m:
        return None, None
    lon = float(m.group(1))
    lat = float(m.group(2))
    return lat, lon


def normalize_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return str(s).strip()


def normalize_title(s: Optional[str]) -> Optional[str]:
    s = normalize_text(s)
    return s.title() if s else s


def normalize_int(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    s = str(s).replace(",", "").strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def clean_rows(rows: Iterable[Dict[str, str]]) -> List[Dict[str, object]]:
    cleaned: List[Dict[str, object]] = []
    seen_keys = set()
    for r in rows:
        # Standardize keys
        r = {k.strip().strip('"'): v for k, v in r.items()}

        detector_id = normalize_int(r.get("detector_id"))
        location_name = normalize_text(r.get("location_name"))
        direction = normalize_title(r.get("detector_direction"))

        created = parse_date(normalize_text(r.get("created_date")))
        modified = parse_date(normalize_text(r.get("modified_date")))

        lat, lon = parse_point(normalize_text(r.get("LOCATION")) or "")

        # Drop rows missing critical identifiers
        if detector_id is None or not location_name:
            continue

        key = (detector_id, location_name, direction)
        if key in seen_keys:
            continue
        seen_keys.add(key)

        out = {
            "detector_id": detector_id,
            "location_name": location_name,
            "detector_direction": direction,
            "created_date": created,
            "modified_date": modified,
            "location_latitude": lat,
            "location_longitude": lon,
            "detector_type": normalize_title(r.get("detector_type")),
            "detector_status": normalize_title(r.get("detector_status")),
            "detector_movement": normalize_text(r.get("detector_movement")),
            "signal_id": normalize_int(r.get("signal_id")),
            "atd_location_id": normalize_text(r.get("atd_location_id")),
        }

        cleaned.append(out)
    return cleaned


def write_outputs(records: List[Dict[str, object]], outdir: Path) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    csv_path = outdir / "traffic_counts_clean.csv"
    jsonl_path = outdir / "traffic_counts_clean.jsonl"

    # CSV
    if records:
        fieldnames = list(records[0].keys())
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(records)

    # JSONL
    with jsonl_path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"Wrote: {csv_path} ({len(records)} rows)")
    print(f"Wrote: {jsonl_path} ({len(records)} rows)")


def main() -> None:
    args = parse_args()
    in_path = Path(args.input)
    outdir = Path(args.outdir)
    if not in_path.exists():
        raise SystemExit(f"Input not found: {in_path}")

    with in_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    before = len(rows)
    records = clean_rows(rows)
    after = len(records)
    print(f"Rows: {before} -> {after} after cleaning")

    write_outputs(records, outdir)


if __name__ == "__main__":
    main()
