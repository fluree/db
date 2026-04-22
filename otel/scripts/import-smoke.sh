#!/usr/bin/env bash
# Bulk import via fluree CLI with OTEL export.
# Usage: import-smoke.sh CLI_BIN TTL_FILE WORK_DIR [LEDGER]
#
# WORK_DIR should contain a .fluree/ project directory (created by 'make init').
# If .fluree/ doesn't exist, the script will run 'fluree init' automatically.

set -euo pipefail

CLI_BIN="${1:?Usage: import-smoke.sh CLI_BIN TTL_FILE WORK_DIR [LEDGER]}"
TTL_FILE="${2:?Usage: import-smoke.sh CLI_BIN TTL_FILE WORK_DIR [LEDGER]}"
WORK_DIR="${3:?Usage: import-smoke.sh CLI_BIN TTL_FILE WORK_DIR [LEDGER]}"
LEDGER="${4:-import-test:main}"

echo "=== Import OTEL Smoke Test ==="
echo "CLI binary:  ${CLI_BIN}"
echo "TTL file:    ${TTL_FILE}"
echo "Work dir:    ${WORK_DIR}"
echo "Ledger:      ${LEDGER}"
echo ""

# Resolve TTL_FILE to absolute path before cd
TTL_ABS="$(cd "$(dirname "${TTL_FILE}")" && pwd)/$(basename "${TTL_FILE}")"

if [ ! -f "${TTL_ABS}" ]; then
    echo "TTL file not found: ${TTL_ABS}"
    echo "Run 'make generate' first."
    exit 1
fi

FILE_SIZE=$(wc -c < "${TTL_ABS}" | tr -d ' ')
FILE_MB=$(python3 -c "print(f'{${FILE_SIZE}/1048576:.1f}')")
echo "Input file: ${FILE_MB} MB"
echo ""

cd "${WORK_DIR}"

# Ensure .fluree/ exists (idempotent — Makefile 'init' target should have
# already created it, but guard against standalone script invocation)
if [ ! -d ".fluree" ]; then
    echo "Initializing Fluree project..."
    OTEL_SERVICE_NAME=fluree-cli \
    OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
    RUST_LOG="${RUST_LOG:-info,fluree_db_api=debug,fluree_db_transact=debug,fluree_db_indexer=debug}" \
    "${CLI_BIN}" init
fi

echo "Starting bulk import..."
OTEL_SERVICE_NAME=fluree-cli \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
RUST_LOG="${RUST_LOG:-info,fluree_db_api=debug,fluree_db_transact=debug,fluree_db_indexer=debug}" \
"${CLI_BIN}" --verbose create "${LEDGER}" --from "${TTL_ABS}"

echo ""
echo "Import smoke test complete."
echo "Check Jaeger for service: fluree-cli"
echo "  http://localhost:16686/search?service=fluree-cli"
