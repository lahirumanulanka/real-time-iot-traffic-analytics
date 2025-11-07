#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   docker compose up -d kafka
#   docker exec -it kafka bash -lc "/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list" # sanity
#   docker exec -it kafka bash /app/kafka/topics/create_topics.sh

BOOTSTRAP_SERVER=${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}

TOPICS=(
  "traffic.raw:3:1"
  "traffic.processed:3:1"
  "traffic.metrics:3:1"
)

for t in "${TOPICS[@]}"; do
  NAME="${t%%:*}"
  PARTS_REPL="${t#*:}"
  PARTS="${PARTS_REPL%%:*}"
  REPL="${PARTS_REPL##*:}"
  echo "Creating topic $NAME (partitions=$PARTS, replication-factor=$REPL)"
  kafka-topics \
    --create \
    --if-not-exists \
    --topic "$NAME" \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --partitions "$PARTS" \
    --replication-factor "$REPL"

done

echo "Topics created."
