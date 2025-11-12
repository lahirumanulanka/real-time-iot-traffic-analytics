#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL=${CONNECT_URL:-http://localhost:8083}
CONFIG_PATH=${1:-/data/filestream-source.json}
# Fallback connector name if not provided via env
CONNECT_NAME=${CONNECT_NAME:-filestream-source}

if ! curl -fsS ${CONNECT_URL}/connectors >/dev/null; then
  echo "Connect at ${CONNECT_URL} not reachable yet. Retry after the container is healthy." >&2
  exit 1
fi

echo "Registering/Updating connector '${CONNECT_NAME}' from ${CONFIG_PATH}"

# Use PUT to create or update idempotently. The JSON must contain name+config.
curl -fsS -X PUT \
  -H "Content-Type: application/json" \
  --data @${CONFIG_PATH} \
  ${CONNECT_URL}/connectors/${CONNECT_NAME}/config || {
    echo "Failed to register connector." >&2
    exit 2
  }

echo "Done. Inspect status: ${CONNECT_URL}/connectors/${CONNECT_NAME}/status"
