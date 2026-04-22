#!/usr/bin/env bash
# Insert enough data to push novelty past reindex threshold, then query.
# Usage: index-smoke.sh [BASE_URL] [LEDGER]

set -euo pipefail

BASE_URL="${1:-http://localhost:8090}"
LEDGER="${2:-otel-test:main}"

echo "=== Index Smoke Test ==="
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

# Generate and insert ~500 entities in batches to push novelty up.
# Each batch is a JSON-LD insert with 50 entities.

for batch in $(seq 1 10); do
    echo "Inserting batch ${batch}/10 (50 entities)..."

    # Build a JSON array of 50 products
    ITEMS=""
    for i in $(seq 1 50); do
        n=$(( (batch - 1) * 50 + i ))
        id=$(printf '%05d' "$n")
        price=$(( (n * 13 + 7) % 990 + 10 ))
        price_str="${price}.$(printf '%02d' $((n % 100)))"
        rating=$(( n % 5 + 1 ))
        cat_num=$(( n % 10 + 1 ))
        in_stock="true"
        if [ $(( n % 4 )) -eq 0 ]; then in_stock="false"; fi

        if [ -n "$ITEMS" ]; then ITEMS="${ITEMS},"; fi
        ITEMS="${ITEMS}
      {\"@id\": \"ex:idx-prod-${id}\", \"@type\": \"ex:Product\", \"ex:name\": \"IndexProduct-${id}\", \"ex:price\": {\"@value\": \"${price_str}\", \"@type\": \"xsd:decimal\"}, \"ex:sku\": \"IDX-${id}\", \"ex:category\": {\"@id\": \"ex:cat-${cat_num}\"}, \"ex:inStock\": ${in_stock}, \"ex:rating\": ${rating}}"
    done

    BODY="{
  \"@context\": {\"ex\": \"http://example.org/ns/\", \"xsd\": \"http://www.w3.org/2001/XMLSchema#\"},
  \"@graph\": [${ITEMS}
  ]
}"

    RESP=$(curl -s -w "\n%{http_code}" \
        -X POST "${BASE_URL}/v1/fluree/insert/${LEDGER}" \
        -H "Content-Type: application/json" \
        -d "$BODY")

    CODE=$(echo "$RESP" | tail -n1)
    if [ "$CODE" = "200" ] || [ "$CODE" = "201" ]; then
        echo "  Batch ${batch} committed (HTTP ${CODE})"
    else
        echo "  Batch ${batch} FAILED (HTTP ${CODE})"
        echo "$RESP" | sed '$d' | head -3
    fi
    sleep 0.3
done

# Wait for background indexing to process
echo ""
echo "Waiting for indexing to settle (10s)..."
sleep 10

# Now query to exercise the indexed data path
echo "Querying indexed data..."
RESP=$(curl -s -w "\n%{http_code}" \
    -X POST "${BASE_URL}/v1/fluree/query/${LEDGER}" \
    -H "Content-Type: application/sparql-query" \
    -d 'PREFIX ex: <http://example.org/ns/>
SELECT (COUNT(?product) AS ?total)
WHERE {
    ?product a ex:Product .
}')

CODE=$(echo "$RESP" | tail -n1)
BODY=$(echo "$RESP" | sed '$d')
echo "  Product count query (HTTP ${CODE}): ${BODY}"

# Verify that index files were actually written
echo ""
STORAGE_DIR="${3:-.fluree/storage}"
INDEX_DIRS=$(find "$STORAGE_DIR" -type d -name "index" 2>/dev/null)
if [ -n "$INDEX_DIRS" ]; then
    INDEX_FILES=$(find $INDEX_DIRS -type f 2>/dev/null | wc -l | tr -d ' ')
    echo "Index verification: PASS (${INDEX_FILES} index files found)"
else
    echo "Index verification: WARN (no index directories found — indexing may not have triggered)"
    echo "  Check that --indexing-enabled is set and enough data was inserted."
fi

echo ""
echo "Index smoke test complete."
echo "Check Jaeger for index_build > build_all_indexes > build_index spans."
