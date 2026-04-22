#!/usr/bin/env bash
# Re-run only the expensive query battery from the stress test.
# Assumes the server is already running and stress data has been loaded.
#
# Usage: stress-query.sh [BASE_URL] [LEDGER]

set -euo pipefail

# Portable millisecond timestamp (macOS date doesn't support %N)
epoch_ms() { python3 -c "import time; print(int(time.time()*1000))"; }

BASE_URL="${1:-http://localhost:8090}"
LEDGER="${2:-otel-test:main}"

# ── Helper: run a SPARQL query (3 iterations with flush pauses) ───────────────

run_query() {
    local label="$1"
    local sparql="$2"
    local iterations="${3:-3}"

    echo "  Query: ${label}"
    for ((iter = 1; iter <= iterations; iter++)); do
        local start_ms=$(epoch_ms)

        RESP=$(curl -s -w "\n%{http_code}" \
            -X POST "${BASE_URL}/v1/fluree/query/${LEDGER}" \
            -H "Content-Type: application/sparql-query" \
            -d "$sparql")

        local end_ms=$(epoch_ms)
        local duration=$((end_ms - start_ms))
        HTTP_CODE=$(echo "$RESP" | tail -n1)

        echo "    iter ${iter}/${iterations}: HTTP ${HTTP_CODE}, ${duration}ms"
        sleep 0.5  # OTEL flush pause
    done
}

echo "============================================="
echo "  STRESS QUERY BATTERY"
echo "============================================="
echo "Server: ${BASE_URL}"
echo "Ledger: ${LEDGER}"
echo ""

# Q1: Full-table sort
run_query "Full-table sort (ORDER BY price DESC LIMIT 1000)" \
    "PREFIX ex: <http://example.org/ns/>
SELECT ?name ?price WHERE {
  ?s a ex:Product ;
     ex:name ?name ;
     ex:price ?price .
}
ORDER BY DESC(?price)
LIMIT 1000"

echo ""

# Q2: Multi-join + filter
run_query "Multi-join + filter (category=electronics, price>50)" \
    "PREFIX ex: <http://example.org/ns/>
SELECT ?name ?price ?catLabel WHERE {
  ?s a ex:Product ;
     ex:name ?name ;
     ex:price ?price ;
     ex:category ?cat .
  ?cat ex:label ?catLabel .
  FILTER(?cat = ex:cat-electronics && ?price > 50)
}"

echo ""

# Q3: GROUP BY + COUNT + AVG
run_query "GROUP BY + COUNT + AVG per category" \
    "PREFIX ex: <http://example.org/ns/>
SELECT ?catLabel (COUNT(?s) AS ?count) (AVG(?price) AS ?avgPrice) WHERE {
  ?s a ex:Product ;
     ex:price ?price ;
     ex:category ?cat .
  ?cat ex:label ?catLabel .
}
GROUP BY ?catLabel
ORDER BY DESC(?count)"

echo ""

# Q4: OPTIONAL + FILTER
run_query "OPTIONAL relatedTo + FILTER rating>=4" \
    "PREFIX ex: <http://example.org/ns/>
SELECT ?name ?rating ?relatedName WHERE {
  ?s a ex:Product ;
     ex:name ?name ;
     ex:rating ?rating .
  FILTER(?rating >= 4)
  OPTIONAL {
    ?s ex:relatedTo ?rel .
    ?rel ex:name ?relatedName .
  }
}
LIMIT 500"

echo ""

# Q5: Subquery — top 5 categories by count, then products in those
# run_query "Subquery: top 5 categories then their products" \
#     "PREFIX ex: <http://example.org/ns/>
# SELECT ?catLabel ?name ?price WHERE {
#   {
#     SELECT ?cat ?catLabel WHERE {
#       ?p a ex:Product ;
#          ex:category ?cat .
#       ?cat ex:label ?catLabel .
#     }
#     GROUP BY ?cat ?catLabel
#     ORDER BY DESC(COUNT(?p))
#     LIMIT 5
#   }
#   ?s a ex:Product ;
#      ex:name ?name ;
#      ex:price ?price ;
#      ex:category ?cat .
# }
# LIMIT 200"

# echo ""

# Q6: Range scan (broad, no tight filter)
run_query "Range scan: all products with rating >= 4" \
    "PREFIX ex: <http://example.org/ns/>
SELECT ?name ?price ?rating WHERE {
  ?s a ex:Product ;
     ex:name ?name ;
     ex:price ?price ;
     ex:rating ?rating .
  FILTER(?rating >= 4)
}
LIMIT 5000"

echo ""
echo "============================================="
echo "  QUERY BATTERY COMPLETE"
echo "============================================="
echo "Open Jaeger at http://localhost:16686"
