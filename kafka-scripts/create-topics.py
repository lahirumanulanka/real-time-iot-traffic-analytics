#!/usr/bin/env python3
import argparse
import subprocess
import sys


def run(cmd: list[str]) -> int:
    try:
        proc = subprocess.run(cmd, check=False)
        return proc.returncode
    except FileNotFoundError:
        print("Error: command not found:", cmd[0], file=sys.stderr)
        return 127


def docker_exec_kafka(args: list[str]) -> int:
    base = ["docker", "exec", "kafka", "bash", "-lc"]
    return run(base + [" ".join(args)])


def create_topic(bootstrap: str, topic: str, partitions: int, repl: int) -> int:
    print(f"- Creating topic: {topic}")
    cmd = [
        f"kafka-topics --bootstrap-server {bootstrap} --create --if-not-exists ",
        f"--topic {topic} ",
        f"--partitions {partitions} ",
        f"--replication-factor {repl}"
    ]
    return docker_exec_kafka(cmd)


def list_topics(bootstrap: str) -> int:
    print("Listing topics:")
    return docker_exec_kafka([f"kafka-topics --bootstrap-server {bootstrap} --list"])


def main() -> int:
    parser = argparse.ArgumentParser(description="Create Kafka topics via docker exec inside the 'kafka' container.")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Bootstrap server address reachable from inside container (default: localhost:9092)")
    parser.add_argument("--raw-topic", default="iot.traffic.raw", help="Raw topic name")
    parser.add_argument("--processed-topic", default="iot.traffic.processed", help="Processed topic name")
    parser.add_argument("--partitions", type=int, default=3, help="Number of partitions")
    parser.add_argument("--replication-factor", type=int, default=1, help="Replication factor")
    args = parser.parse_args()

    print(f"Creating Kafka topics on {args.bootstrap} ...")
    rc1 = create_topic(args.bootstrap, args.raw_topic, args.partitions, args.replication_factor)
    rc2 = create_topic(args.bootstrap, args.processed_topic, args.partitions, args.replication_factor)
    rc3 = list_topics(args.bootstrap)

    if any(r != 0 for r in (rc1, rc2, rc3)):
        return 1
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
