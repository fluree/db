#!/usr/bin/env bash
# Wait for the Fluree server health endpoint to respond.
# Usage: wait-for-server.sh [BASE_URL] [TIMEOUT_SECONDS]

set -euo pipefail

BASE_URL="${1:-http://localhost:8090}"
TIMEOUT="${2:-30}"

echo "Waiting for server at ${BASE_URL}/health (timeout: ${TIMEOUT}s)..."

elapsed=0
while [ "$elapsed" -lt "$TIMEOUT" ]; do
    if curl -sf "${BASE_URL}/health" > /dev/null 2>&1; then
        echo "Server is ready (${elapsed}s)"
        exit 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
done

echo "ERROR: Server did not become ready within ${TIMEOUT}s"
exit 1
