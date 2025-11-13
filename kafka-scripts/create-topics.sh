#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${1:-localhost:9092}"
RAW_TOPIC="${2:-iot.traffic.raw}"
PROCESSED_TOPIC="${3:-iot.traffic.processed}"
PARTITIONS="${4:-3}"
REPL_FACTOR="${5:-1}"

echo "Creating Kafka topics on ${BOOTSTRAP}..."
docker exec kafka bash -lc "kafka-topics --bootstrap-server ${BOOTSTRAP} --create --if-not-exists --topic ${RAW_TOPIC} --partitions ${PARTITIONS} --replication-factor ${REPL_FACTOR}" || true
docker exec kafka bash -lc "kafka-topics --bootstrap-server ${BOOTSTRAP} --create --if-not-exists --topic ${PROCESSED_TOPIC} --partitions ${PARTITIONS} --replication-factor ${REPL_FACTOR}" || true

echo "Listing topics:"
docker exec kafka bash -lc "kafka-topics --bootstrap-server ${BOOTSTRAP} --list"
echo "Done."
