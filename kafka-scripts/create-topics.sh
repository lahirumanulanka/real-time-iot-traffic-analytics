#!/usr/bin/env bash

set -euo pipefail

# Usage: ./create-topics.sh [bootstrap]
# Default bootstrap server is kafka:9092 (inside Docker network)
BOOTSTRAP="${1:-kafka:9092}"

echo "[kafka-init] Using bootstrap server: ${BOOTSTRAP}"

echo "[kafka-init] Waiting for Kafka to be reachable..."
until kafka-topics --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do
	sleep 2
done
echo "[kafka-init] Kafka is reachable. Proceeding to create topics."

# Define topics as name:partitions:replicationFactor
TOPICS=(
	"traffic.raw:3:1"        # Raw sensor data ingestion
	"traffic.processed:3:1"  # Processed/aggregated data outputs
)

for spec in "${TOPICS[@]}"; do
	IFS=":" read -r NAME PARTITIONS RF <<<"$spec"
	if kafka-topics --bootstrap-server "$BOOTSTRAP" --list | grep -Fx "$NAME" >/dev/null 2>&1; then
		echo "[kafka-init] Topic '$NAME' already exists. Skipping."
	else
		echo "[kafka-init] Creating topic '$NAME' (partitions=$PARTITIONS, rf=$RF)"
		kafka-topics \
			--bootstrap-server "$BOOTSTRAP" \
			--create \
			--if-not-exists \
			--topic "$NAME" \
			--partitions "$PARTITIONS" \
			--replication-factor "$RF"
	fi
done

echo "[kafka-init] Topic creation complete. Current topics:"
kafka-topics --bootstrap-server "$BOOTSTRAP" --list | sort

