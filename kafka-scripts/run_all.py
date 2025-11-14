import argparse
import os
import signal
import socket
import sys
import time
from pathlib import Path
from subprocess import Popen, PIPE, STDOUT, CalledProcessError, run
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
        description=(
            "Run end-to-end pipeline: optional Docker (Kafka/Postgres/Grafana), "
            "Postgres sink, processor, and producer. Press Ctrl+C to stop."
        )
    )
    parser.add_argument("--bootstrap", default="localhost:29092", help="Kafka bootstrap servers")
    parser.add_argument("--raw-topic", default="iot.traffic.raw", help="Raw input topic")
    parser.add_argument(
        "--processed-topic", default="iot.traffic.processed", help="Processed output topic"
    )
    parser.add_argument(
        "--start-docker",
        action="store_true",
        help="Run 'docker compose up -d kafka postgres grafana' before starting scripts",
    )
    parser.add_argument(
        "--compose-services",
        default="kafka postgres grafana",
        help="Space-separated docker compose services to start when --start-docker is set",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Producer send interval in seconds (default 5)",
    )
    parser.add_argument(
        "--producer-max",
        type=int,
        default=0,
        help="Max source records for producer to load (0=all)",
    )
    parser.add_argument(
        "--producer-loop",
        action="store_true",
        help="Continuously loop producer records",
    )
    parser.add_argument(
        "--use-now-ts",
        action="store_true",
        help="Override record timestamps with current UTC time during send",
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
        "--no-consumer",
        action="store_true",
        help="Do not launch the console consumer (reduces noise)",
    )
    parser.add_argument(
        "--no-show-keys",
        action="store_true",
        help="Do not show keys in consumer output",
    )
    parser.add_argument(
        "--no-sink",
        action="store_true",
        help="Do not launch the Postgres sink",
    )
    parser.add_argument(
        "--sink-batch",
        type=int,
        default=1,
        help="Postgres sink batch size for commits (default 1 for realtime)",
    )
    parser.add_argument("--pg-host", default=os.getenv("PGHOST", "localhost"))
    parser.add_argument("--pg-port", type=int, default=int(os.getenv("PGPORT", "5432")))
    parser.add_argument("--pg-db", default=os.getenv("PGDATABASE", "traffic"))
    parser.add_argument("--pg-user", default=os.getenv("PGUSER", "traffic"))
    parser.add_argument("--pg-password", default=os.getenv("PGPASSWORD", "traffic"))

    args = parser.parse_args()

    # Resolve repository root and script paths robustly
    here = Path(__file__).resolve()
    kafka_scripts_dir = here.parent
    repo_root = kafka_scripts_dir.parent

    producer_script = kafka_scripts_dir / "producer" / "traffic_raw_producer.py"
    processor_script = kafka_scripts_dir / "processor" / "traffic_processor.py"
    consumer_script = kafka_scripts_dir / "consumer" / "processed_consumer.py"
    sink_script = kafka_scripts_dir / "sink" / "postgres_sink.py"

    if not producer_script.exists():
        print(f"Producer script not found: {producer_script}")
        sys.exit(1)
    if not processor_script.exists():
        print(f"Processor script not found: {processor_script}")
        sys.exit(1)
    if not consumer_script.exists():
        print(f"Consumer script not found: {consumer_script}")
        sys.exit(1)
    if not sink_script.exists():
        print(f"Sink script not found: {sink_script}")
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

    def wait_for_port(host: str, port: int, timeout: float = 60.0, step: float = 1.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with socket.create_connection((host, port), timeout=step):
                    return True
            except OSError:
                time.sleep(step)
        return False

    if args.start_docker:
        services = args.compose_services.split()
        try:
            print("Starting Docker services:", ", ".join(services))
            run(["docker", "compose", "up", "-d", *services], cwd=str(repo_root), check=True)
        except FileNotFoundError:
            print("docker not found on PATH. Please install Docker Desktop or run without --start-docker.")
            sys.exit(1)
        except CalledProcessError as e:
            print("Failed to start Docker services:", e)
            sys.exit(1)

        # Basic readiness checks
        print("Waiting for Kafka at", args.bootstrap)
        host, port_str = args.bootstrap.split(":", 1)
        if not wait_for_port(host, int(port_str), timeout=90):
            print("Kafka did not become ready in time.")
            sys.exit(1)

        print("Waiting for Postgres at", f"{args.pg_host}:{args.pg_port}")
        if not wait_for_port(args.pg_host, int(args.pg_port), timeout=90):
            print("Postgres did not become ready in time.")
            sys.exit(1)

        print("Waiting for Grafana at", "localhost:3000")
        wait_for_port("localhost", 3000, timeout=60)

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
    if args.producer_max and args.producer_max > 0:
        producer_cmd += ["--max", str(args.producer_max)]
    if args.producer_loop:
        producer_cmd.append("--loop")
    if args.use_now_ts:
        producer_cmd.append("--use-now-timestamps")

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

    consumer_cmd = None
    if not args.no_consumer:
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

    sink_cmd = None
    if not args.no_sink:
        sink_cmd = [
            py,
            str(sink_script),
            "--bootstrap",
            args.bootstrap,
            "--topic",
            args.processed_topic,
            "--batch",
            str(args.sink_batch),
            "--pg-host",
            str(args.pg_host),
            "--pg-port",
            str(args.pg_port),
            "--pg-db",
            str(args.pg_db),
            "--pg-user",
            str(args.pg_user),
            "--pg-password",
            str(args.pg_password),
        ]

    print("Starting processes:")
    if sink_cmd:
        print(" - SINK:", " ".join(sink_cmd))
    print(" - PROCESSOR:", " ".join(processor_cmd))
    print(" - PRODUCER:", " ".join(producer_cmd))
    if consumer_cmd:
        print(" - CONSUMER:", " ".join(consumer_cmd))

    procs = {}
    threads = []
    try:
        # Start sink first (persists processed events)
        if sink_cmd:
            sink = Popen(sink_cmd, stdout=PIPE, stderr=STDOUT, cwd=str(repo_root))
            procs["SINK"] = sink
            t_sink = Thread(target=stream_output, args=(sink, "SINK"), daemon=True)
            t_sink.start()
            threads.append(t_sink)

        # Start processor so it is ready to consume from raw and publish processed
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

        # Start consumer (optional)
        if consumer_cmd:
            consumer = Popen(consumer_cmd, stdout=PIPE, stderr=STDOUT, cwd=str(repo_root))
            procs["CONSUMER"] = consumer
            t_cons = Thread(target=stream_output, args=(consumer, "CONSUMER"), daemon=True)
            t_cons.start()
            threads.append(t_cons)

        print("All processes started. Press Ctrl+C to stop.")
        print("Open Grafana at http://localhost:3000 (admin/admin) and view 'Traffic Overview'.")

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
