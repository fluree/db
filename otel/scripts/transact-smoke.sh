#!/usr/bin/env bash
# Exercise transaction API paths with minimal data.
# Usage: transact-smoke.sh [BASE_URL] [LEDGER]

set -euo pipefail

BASE_URL="${1:-http://localhost:8090}"
LEDGER="${2:-otel-test:main}"

echo "=== Transaction Smoke Test ==="
echo "Server: ${BASE_URL}"
echo "Ledger: ${LEDGER}"
echo ""

# Check that the ledger exists before running tests
EXISTS_RESP=$(curl -s "${BASE_URL}/v1/fluree/exists/${LEDGER}" 2>/dev/null || true)
if echo "${EXISTS_RESP}" | grep -q '"exists":true'; then
    :
else
    echo "Ledger '${LEDGER}' does not exist."
    echo "Run 'make seed' first to create and populate the test ledger."
    exit 1
fi

# Helper: run a curl and report status
run_tx() {
    local label="$1"; shift
    local resp
    resp=$(curl -s -w "\n%{http_code}" "$@")
    local code
    code=$(echo "$resp" | tail -n1)
    local body
    body=$(echo "$resp" | sed '$d')
    if [ "$code" = "200" ] || [ "$code" = "201" ]; then
        echo "  [PASS] ${label} (HTTP ${code})"
    else
        echo "  [FAIL] ${label} (HTTP ${code})"
        echo "         ${body}" | head -3
    fi
    sleep 0.5
}

# 1. JSON-LD insert
echo "1. JSON-LD insert..."
run_tx "JSON-LD insert" \
    -X POST "${BASE_URL}/v1/fluree/insert/${LEDGER}" \
    -H "Content-Type: application/json" \
    --data-raw '{
  "@context": {"ex": "http://example.org/ns/"},
  "@id": "ex:smoke-item-1",
  "@type": "ex:SmokeTest",
  "ex:label": "Insert test",
  "ex:value": 42
}'

# 2. JSON-LD upsert
echo "2. JSON-LD upsert..."
run_tx "JSON-LD upsert" \
    -X POST "${BASE_URL}/v1/fluree/upsert/${LEDGER}" \
    -H "Content-Type: application/json" \
    --data-raw '{
  "@context": {"ex": "http://example.org/ns/"},
  "@id": "ex:smoke-item-1",
  "@type": "ex:SmokeTest",
  "ex:label": "Upserted label",
  "ex:value": 99
}'

# 3. JSON-LD update (WHERE / DELETE / INSERT)
echo "3. JSON-LD update (WHERE/DELETE/INSERT)..."
run_tx "JSON-LD update" \
    -X POST "${BASE_URL}/v1/fluree/update/${LEDGER}" \
    -H "Content-Type: application/json" \
    --data-raw '{
  "@context": {"ex": "http://example.org/ns/"},
  "where": [{"@id": "ex:smoke-item-1", "ex:value": "?oldVal"}],
  "delete": [{"@id": "ex:smoke-item-1", "ex:value": "?oldVal"}],
  "insert": [{"@id": "ex:smoke-item-1", "ex:value": 100}]
}'

# 4. Turtle insert
echo "4. Turtle insert..."
run_tx "Turtle insert" \
    -X POST "${BASE_URL}/v1/fluree/insert/${LEDGER}" \
    -H "Content-Type: text/turtle" \
    --data-raw '@prefix ex: <http://example.org/ns/> .
ex:smoke-item-2 a ex:SmokeTest ;
    ex:label "Turtle insert test" ;
    ex:value 77 .
'

# 5. SPARQL UPDATE
echo "5. SPARQL UPDATE..."
run_tx "SPARQL UPDATE" \
    -X POST "${BASE_URL}/v1/fluree/update/${LEDGER}" \
    -H "Content-Type: application/sparql-update" \
    --data-raw 'PREFIX ex: <http://example.org/ns/>
INSERT DATA {
    ex:smoke-item-3 a ex:SmokeTest ;
        ex:label "SPARQL insert test" ;
        ex:value 55 .
}'

echo ""
echo "Transaction smoke test complete."
echo "Check Jaeger for transact_execute > txn_stage > txn_commit spans."

