#!/usr/bin/env bash
# Combined scenario: transact + query + index in one run.
# Assumes seed-ledger.sh has already created the ledger and base data.
# Usage: full-cycle.sh [BASE_URL] [LEDGER]

set -euo pipefail

BASE_URL="${1:-http://localhost:8090}"
LEDGER="${2:-otel-test:main}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Full Cycle ==="
echo "Server: ${BASE_URL}"
echo "Ledger: ${LEDGER}"
echo ""

echo "--- Phase 1: Transactions ---"
"${SCRIPT_DIR}/transact-smoke.sh" "${BASE_URL}" "${LEDGER}"
echo ""

echo "--- Phase 2: Queries ---"
"${SCRIPT_DIR}/query-smoke.sh" "${BASE_URL}" "${LEDGER}"
echo ""

echo "--- Phase 3: Index pressure ---"
"${SCRIPT_DIR}/index-smoke.sh" "${BASE_URL}" "${LEDGER}"
echo ""

echo "=== Full cycle complete ==="
echo "Open Jaeger at http://localhost:16686"
echo "Search for service: fluree-server"
