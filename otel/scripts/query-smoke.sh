#!/usr/bin/env bash
# Exercise all query API paths. Assumes seed-ledger.sh has run.
# Usage: query-smoke.sh [BASE_URL] [LEDGER]

set -euo pipefail

BASE_URL="${1:-http://localhost:8090}"
LEDGER="${2:-otel-test:main}"

echo "=== Query Smoke Test ==="
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

# Helper: run a curl query and report status + row count
run_query() {
    local label="$1"; shift
    local resp
    resp=$(curl -s -w "\n%{http_code}" "$@")
    local code
    code=$(echo "$resp" | tail -n1)
    local body
    body=$(echo "$resp" | sed '$d')
    if [ "$code" = "200" ]; then
        # Estimate row count — handles both object arrays [{"@id":...}] and tuple arrays [[...]]
        local rows
        # Heuristic row count for various response formats:
        #   Object array: [{"@id":...}]  — count "@id"
        #   Tuple array:  [[...],[...]]   — count ],
        #   SPARQL JSON:  {"results":{"bindings":[{"var":{"value":...}}]}} — count "value"
        rows=$({ echo "$body" | grep -o '"@id"\|"value"' || true; } | wc -l | tr -d ' ')
        if [ "$rows" = "0" ]; then
            rows=$({ echo "$body" | grep -o '\],' || true; } | wc -l | tr -d ' ')
            [ "$rows" -gt 0 ] && rows=$((rows + 1))
        fi
        echo "  [PASS] ${label} (HTTP ${code}, ~${rows} results)"
    else
        echo "  [FAIL] ${label} (HTTP ${code})"
        echo "         ${body}" | head -3
    fi
    sleep 0.5
}

# 1. FQL select — simple pattern
echo "1. FQL select (simple)..."
run_query "FQL simple select" \
    -X POST "${BASE_URL}/v1/fluree/query/${LEDGER}" \
    -H "Content-Type: application/json" \
    -d '{
  "@context": {"ex": "http://example.org/ns/"},
  "select": {"?product": ["*"]},
  "where": [{"@id": "?product", "@type": "ex:Product", "ex:name": "?name"}],
  "limit": 10
}'

# 2. FQL with filter — price range
echo "2. FQL with filter (price range)..."
run_query "FQL filter" \
    -X POST "${BASE_URL}/v1/fluree/query/${LEDGER}" \
    -H "Content-Type: application/json" \
    -d '{
  "@context": {"ex": "http://example.org/ns/"},
  "select": ["?name", "?price"],
  "where": [
    {"@id": "?product", "@type": "ex:Product", "ex:name": "?name", "ex:price": "?price"},
    ["filter", "(> ?price 50)"]
  ],
  "limit": 20
}'

# 3. FQL with sort — ORDER BY price DESC
echo "3. FQL with sort (ORDER BY price DESC)..."
run_query "FQL sort" \
    -X POST "${BASE_URL}/v1/fluree/query/${LEDGER}" \
    -H "Content-Type: application/json" \
    -d '{
  "@context": {"ex": "http://example.org/ns/"},
  "select": ["?name", "?price"],
  "where": [{"@id": "?product", "@type": "ex:Product", "ex:name": "?name", "ex:price": "?price"}],
  "orderBy": [{"var": "?price", "order": "desc"}],
  "limit": 10
}'

# 4. SPARQL select — basic triple pattern
echo "4. SPARQL select (basic)..."
run_query "SPARQL basic" \
    -X POST "${BASE_URL}/v1/fluree/query/${LEDGER}" \
    -H "Content-Type: application/sparql-query" \
    -d 'PREFIX ex: <http://example.org/ns/>
SELECT ?name ?sku
WHERE {
    ?product a ex:Product ;
             ex:name ?name ;
             ex:sku ?sku .
}
LIMIT 10'

# 5. SPARQL with OPTIONAL + FILTER
echo "5. SPARQL OPTIONAL + FILTER..."
run_query "SPARQL OPTIONAL+FILTER" \
    -X POST "${BASE_URL}/v1/fluree/query/${LEDGER}" \
    -H "Content-Type: application/sparql-query" \
    -d 'PREFIX ex: <http://example.org/ns/>
SELECT ?name ?price ?relatedName
WHERE {
    ?product a ex:Product ;
             ex:name ?name ;
             ex:price ?price ;
             ex:inStock true .
    OPTIONAL {
        ?product ex:relatedTo ?related .
        ?related ex:name ?relatedName .
    }
}
LIMIT 20'

# 6. SPARQL with GROUP BY
echo "6. SPARQL GROUP BY..."
run_query "SPARQL GROUP BY" \
    -X POST "${BASE_URL}/v1/fluree/query/${LEDGER}" \
    -H "Content-Type: application/sparql-query" \
    -d 'PREFIX ex: <http://example.org/ns/>
SELECT ?catLabel (COUNT(?product) AS ?productCount)
WHERE {
    ?product a ex:Product ;
             ex:category ?cat .
    ?cat ex:label ?catLabel .
}
GROUP BY ?catLabel
ORDER BY DESC(?productCount)'

# 7. Global query (connection-scoped, uses "from")
echo "7. Global FQL query (connection-scoped)..."
run_query "Global FQL query" \
    -X POST "${BASE_URL}/v1/fluree/query" \
    -H "Content-Type: application/json" \
    -d "{
  \"@context\": {\"ex\": \"http://example.org/ns/\"},
  \"from\": \"${LEDGER}\",
  \"select\": [\"?name\"],
  \"where\": [{\"@id\": \"?s\", \"@type\": \"ex:Category\", \"ex:label\": \"?name\"}]
}"

echo ""
echo "Query smoke test complete."
echo "Check Jaeger for query_execute > query_prepare > query_run > operator spans."
