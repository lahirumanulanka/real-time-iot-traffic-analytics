import argparse
import os
import signal
import sys
import time
from pathlib import Path
from subprocess import Popen, PIPE, STDOUT
from threading import Thread


def stream_output(proc: Popen, name: str):
    try:
        assert proc.stdout is not None
        for line in iter(proc.stdout.readline, b""):
            if not line:
                break
            try:
                text = line.decode(errors="replace").rstrip()
            except Exception:
                text = str(line).rstrip()
            print(f"[{name}] {text}")
    except Exception as e:
        print(f"[{name}] Output stream error: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Run producer, processor, and consumer together. Press Ctrl+C to stop."
    )
    parser.add_argument("--bootstrap", default="localhost:29092", help="Kafka bootstrap servers")
    parser.add_argument("--raw-topic", default="iot.traffic.raw", help="Raw input topic")
    parser.add_argument(
        "--processed-topic", default="iot.traffic.processed", help="Processed output topic"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Producer send interval in seconds (default 5)",
    )
    parser.add_argument(
        "--json-path",
        default=None,
        help="Path to traffic_counts_kafka.json (defaults to repo data/dataset)",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Start processor from earliest offsets (replays existing messages)",
    )
    parser.add_argument(
        "--consume-max",
        type=int,
        default=0,
        help="Max messages for consumer (0 = unlimited)",
    )
    parser.add_argument(
        "--no-show-keys",
        action="store_true",
        help="Do not show keys in consumer output",
    )

    args = parser.parse_args()

    # Resolve repository root and script paths robustly
    here = Path(__file__).resolve()
    kafka_scripts_dir = here.parent
    repo_root = kafka_scripts_dir.parent

    producer_script = kafka_scripts_dir / "producer" / "traffic_raw_producer.py"
    processor_script = kafka_scripts_dir / "processor" / "traffic_processor.py"
    consumer_script = kafka_scripts_dir / "consumer" / "processed_consumer.py"

    if not producer_script.exists():
        print(f"Producer script not found: {producer_script}")
        sys.exit(1)
    if not processor_script.exists():
        print(f"Processor script not found: {processor_script}")
        sys.exit(1)
    if not consumer_script.exists():
        print(f"Consumer script not found: {consumer_script}")
        sys.exit(1)

    # Default JSON dataset path
    json_path = (
        Path(args.json_path).resolve()
        if args.json_path
        else (repo_root / "data" / "dataset" / "traffic_counts_kafka.json")
    )
    if not json_path.exists():
        print(f"Dataset file not found: {json_path}")
        sys.exit(1)

    py = sys.executable  # use current interpreter (e.g., venv python)

    producer_cmd = [
        py,
        str(producer_script),
        "--bootstrap",
        args.bootstrap,
        "--topic",
        args.raw_topic,
        "--json-path",
        str(json_path),
        "--interval",
        str(args.interval),
    ]

    processor_cmd = [
        py,
        str(processor_script),
        "--bootstrap",
        args.bootstrap,
        "--raw-topic",
        args.raw_topic,
        "--processed-topic",
        args.processed_topic,
    ]
    if args.from_beginning:
        processor_cmd.append("--from-beginning")

    consumer_cmd = [
        py,
        str(consumer_script),
        "--bootstrap",
        args.bootstrap,
        "--topic",
        args.processed_topic,
    ]
    if not args.no_show_keys:
        consumer_cmd.append("--show-keys")
    if args.consume_max and args.consume_max > 0:
        consumer_cmd += ["--max", str(args.consume_max)]

    print("Starting processes:")
    print(" - PRODUCER:", " ".join(producer_cmd))
    print(" - PROCESSOR:", " ".join(processor_cmd))
    print(" - CONSUMER:", " ".join(consumer_cmd))

    procs = {}
    threads = []
    try:
        # Start processor first so it is ready to consume
        processor = Popen(processor_cmd, stdout=PIPE, stderr=STDOUT, cwd=str(repo_root))
        procs["PROCESSOR"] = processor
        t_proc = Thread(target=stream_output, args=(processor, "PROCESSOR"), daemon=True)
        t_proc.start()
        threads.append(t_proc)

        time.sleep(0.8)

        # Start producer
        producer = Popen(producer_cmd, stdout=PIPE, stderr=STDOUT, cwd=str(repo_root))
        procs["PRODUCER"] = producer
        t_prod = Thread(target=stream_output, args=(producer, "PRODUCER"), daemon=True)
        t_prod.start()
        threads.append(t_prod)

        time.sleep(0.8)

        # Start consumer
        consumer = Popen(consumer_cmd, stdout=PIPE, stderr=STDOUT, cwd=str(repo_root))
        procs["CONSUMER"] = consumer
        t_cons = Thread(target=stream_output, args=(consumer, "CONSUMER"), daemon=True)
        t_cons.start()
        threads.append(t_cons)

        print("All processes started. Press Ctrl+C to stop.")

        # Wait until any process exits unexpectedly; keep parent alive otherwise
        while True:
            exited = [name for name, p in procs.items() if p.poll() is not None]
            if exited:
                for name in exited:
                    code = procs[name].returncode
                    print(f"[{name}] exited with code {code}")
                break
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nStopping all processes...")
    finally:
        # Terminate children gracefully
        for name, p in procs.items():
            if p.poll() is None:
                try:
                    p.terminate()
                except Exception:
                    pass
        # Give them a moment to exit
        time.sleep(1.0)
        # Force kill if needed
        for name, p in procs.items():
            if p.poll() is None:
                try:
                    p.kill()
                except Exception:
                    pass
        print("All processes stopped.")


if __name__ == "__main__":
    main()
