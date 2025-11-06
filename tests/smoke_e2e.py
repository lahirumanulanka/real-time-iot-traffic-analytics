import os
import time
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VENV_PY = ROOT / ".venv" / "Scripts" / "python.exe"
CONSUMER = ROOT / "consumer" / "consumer.py"
PRODUCER = ROOT / "producer" / "producer.py"
OUTPUT_JSON = ROOT / "consumer" / "output" / "processed_data.json"
JSONL = ROOT / "producer" / "dataset" / "processed" / "cleaned_detectors.jsonl"


def run_smoke():
    # Ensure paths
    assert JSONL.exists(), f"Missing cleaned JSONL: {JSONL}"

    # Prepare envs
    base_env = os.environ.copy()

    # Start consumer (finite)
    consumer_env = base_env.copy()
    consumer_env["CONSUMER_MAX_MESSAGES"] = "20"
    consumer_env["CONSUMER_GROUP"] = "traffic-consumer-smoke"
    consumer_env.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    consumer_env.setdefault("RAW_TOPIC", "traffic.raw")
    consumer_env.setdefault("PROCESSED_TOPIC", "traffic.processed")

    consumer = subprocess.Popen(
        [str(VENV_PY), str(CONSUMER)],
        cwd=str(ROOT),
        env=consumer_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    # Give the consumer a moment to join group
    time.sleep(2.0)

    # Run producer (short burst)
    producer_env = base_env.copy()
    producer_env["JSONL_PATH"] = str(JSONL)
    producer_env["MAX_RECORDS"] = "20"
    producer_env["STREAM_DELAY_MS"] = "10"
    producer_env.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    code = subprocess.call([str(VENV_PY), str(PRODUCER)], cwd=str(ROOT), env=producer_env)
    assert code == 0, f"Producer exited with code {code}"

    # Wait for consumer to finish or timeout
    try:
        out, _ = consumer.communicate(timeout=60)
        print(out)
    except subprocess.TimeoutExpired:
        consumer.kill()
        raise AssertionError("Consumer did not finish in time")

    # Verify output file
    assert OUTPUT_JSON.exists(), "processed_data.json not found"
    data = json.loads(OUTPUT_JSON.read_text(encoding="utf-8") or "[]")
    assert isinstance(data, list) and len(data) > 0, "No aggregates written to processed_data.json"
    print(f"Smoke test passed with {len(data)} aggregate records.")


if __name__ == "__main__":
    run_smoke()
