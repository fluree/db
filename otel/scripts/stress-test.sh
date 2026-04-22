#!/usr/bin/env bash
# High-volume stress test: sustained inserts with backpressure handling + expensive queries.
# Generates deterministic product data inline, inserts in batches via HTTP API,
# bounces the server on novelty backpressure (workaround: index writes to disk but
# the in-memory ledger handle doesn't pick up the new index until restart),
# then runs a battery of expensive SPARQL queries.
#
# Usage: stress-test.sh [BASE_URL] [LEDGER] [TOTAL_PRODUCTS] [BATCH_SIZE]
#
# Server lifecycle env vars (set by Makefile):
#   SERVER_BIN, FLUREE_DIR, PID_FILE, LOG_FILE, PORT, RUST_LOG, INDEXING

set -euo pipefail

# Portable millisecond timestamp (macOS date doesn't support %N)
epoch_ms() { python3 -c "import time; print(int(time.time()*1000))"; }

BASE_URL="${1:-http://localhost:8090}"
LEDGER="${2:-otel-test:main}"
TOTAL_PRODUCTS="${3:-50000}"
BATCH_SIZE="${4:-500}"

# ── Server lifecycle config (from Makefile env vars) ─────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OTEL_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

SERVER_BIN="${SERVER_BIN:-${OTEL_DIR}/../target/release/fluree-server}"
FLUREE_DIR="${FLUREE_DIR:-${OTEL_DIR}/.fluree}"
PID_FILE="${PID_FILE:-${FLUREE_DIR}/server.pid}"
LOG_FILE="${LOG_FILE:-${FLUREE_DIR}/server.log}"
PORT="${PORT:-8090}"
RUST_LOG="${RUST_LOG:-info,fluree_db_query=debug,fluree_db_transact=debug,fluree_db_indexer=debug}"
INDEXING="${INDEXING:-true}"

# ── Categories ────────────────────────────────────────────────────────────────

CATEGORIES=(
    electronics clothing home garden sports
    books toys automotive health beauty
    food office pets music furniture
    jewelry tools outdoors baby grocery
)
NUM_CATEGORIES=${#CATEGORIES[@]}

# ── Derived constants ─────────────────────────────────────────────────────────

TOTAL_BATCHES=$(( (TOTAL_PRODUCTS + BATCH_SIZE - 1) / BATCH_SIZE ))
PRODUCT_NAMES=("Widget" "Gadget" "Gizmo" "Thingamajig" "Doohickey"
               "Contraption" "Device" "Tool" "Implement" "Apparatus")
NUM_NAMES=${#PRODUCT_NAMES[@]}

echo "============================================="
echo "  STRESS TEST"
echo "============================================="
echo "Server:         ${BASE_URL}"
echo "Ledger:         ${LEDGER}"
echo "Total products: ${TOTAL_PRODUCTS}"
echo "Batch size:     ${BATCH_SIZE}"
echo "Total batches:  ${TOTAL_BATCHES}"
echo ""

# ── Helper: stop the server ──────────────────────────────────────────────────

stop_server() {
    if [ -f "$PID_FILE" ]; then
        local pid
        pid=$(cat "$PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid"
            # Wait for process to exit (up to 10s)
            for ((w = 0; w < 20; w++)); do
                if ! kill -0 "$pid" 2>/dev/null; then break; fi
                sleep 0.5
            done
        fi
        rm -f "$PID_FILE"
    fi
}

# ── Helper: start the server ─────────────────────────────────────────────────

start_server() {
    mkdir -p "$(dirname "$PID_FILE")"

    # Server reads .fluree/config.toml automatically (cwd walk-up discovery).
    # Config is pre-applied by 'make init' / 'make config' (1GB reindex_max_bytes, etc.)
    OTEL_SERVICE_NAME=fluree-server \
    OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
    RUST_LOG="$RUST_LOG" \
    "$SERVER_BIN" \
        > "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"

    # Wait for health
    "${SCRIPT_DIR}/wait-for-server.sh" "$BASE_URL" 30
}

# ── Helper: bounce the server (stop + start) ─────────────────────────────────

bounce_server() {
    local reason="${1:-backpressure}"
    echo "  >> Bouncing server (${reason}): stopping..."
    stop_server
    echo "  >> Starting server..."
    start_server
}

# ── Helper: insert with backpressure-triggered server bounce ──────────────────

TOTAL_BOUNCES=0

backpressure_insert() {
    local payload="$1"
    local batch_num="$2"
    local max_bounces=10
    local bounces=0

    while true; do
        RESP=$(curl -s -w "\n%{http_code}" \
            -X POST "${BASE_URL}/v1/fluree/insert/${LEDGER}" \
            -H "Content-Type: application/json" \
            -d "$payload")

        HTTP_CODE=$(echo "$RESP" | tail -n1)
        BODY=$(echo "$RESP" | sed '$d')

        if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ]; then
            return 0
        fi

        # Check for novelty backpressure
        if [ "$HTTP_CODE" = "400" ] && echo "$BODY" | grep -qi "novelty"; then
            bounces=$((bounces + 1))
            TOTAL_BOUNCES=$((TOTAL_BOUNCES + 1))
            if [ "$bounces" -gt "$max_bounces" ]; then
                echo "  FATAL: Batch ${batch_num} failed after ${max_bounces} server bounces"
                return 1
            fi
            echo "  Backpressure on batch ${batch_num} (bounce ${bounces}/${max_bounces})..."
            bounce_server "novelty limit on batch ${batch_num}"
            continue
        fi

        # Other error — report and fail
        echo "  ERROR: Batch ${batch_num} failed with HTTP ${HTTP_CODE}"
        echo "  Body: ${BODY:0:200}"
        return 1
    done
}

# ── Helper: generate a batch of products as JSON-LD ───────────────────────────

generate_batch() {
    local start_id="$1"
    local count="$2"
    local end_id=$((start_id + count - 1))

    # Build the @graph array
    local items=""
    for ((i = start_id; i <= end_id; i++)); do
        local name_idx=$((i % NUM_NAMES))
        local cat_idx=$((i % NUM_CATEGORIES))
        local cat="${CATEGORIES[$cat_idx]}"
        local name="${PRODUCT_NAMES[$name_idx]}"
        local price_int=$(( (i * 7 + 13) % 990 + 10 ))
        local price_dec=$(( (i * 3) % 100 ))
        local rating=$(( (i % 5) + 1 ))
        local in_stock="true"
        if (( i % 7 == 0 )); then in_stock="false"; fi
        local related=""
        if (( i % 10 == 0 && i > 1 )); then
            related=", \"ex:relatedTo\": {\"@id\": \"ex:stress-prod-$((i - 1))\"}"
        fi

        if [ -n "$items" ]; then items="${items},"; fi
        items="${items}
    {\"@id\": \"ex:stress-prod-${i}\", \"@type\": \"ex:Product\", \"ex:name\": \"${name} ${i}\", \"ex:price\": {\"@value\": \"${price_int}.${price_dec}\", \"@type\": \"xsd:decimal\"}, \"ex:sku\": \"STRESS-${i}\", \"ex:category\": {\"@id\": \"ex:cat-${cat}\"}, \"ex:inStock\": ${in_stock}, \"ex:rating\": ${rating}${related}}"
    done

    echo "{
  \"@context\": {\"ex\": \"http://example.org/ns/\", \"xsd\": \"http://www.w3.org/2001/XMLSchema#\"},
  \"@graph\": [${items}
  ]
}"
}

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

# ── Phase 0: Create ledger ────────────────────────────────────────────────────

echo "--- Phase 0: Create ledger ---"
echo "  Creating ledger '${LEDGER}'..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "${BASE_URL}/v1/fluree/create" \
    -H "Content-Type: application/json" \
    -d "{\"ledger\": \"${LEDGER}\"}")

if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ]; then
    echo "  Ledger created (HTTP ${HTTP_CODE})"
elif [ "$HTTP_CODE" = "409" ] || [ "$HTTP_CODE" = "400" ]; then
    echo "  Ledger may already exist (HTTP ${HTTP_CODE}), continuing..."
else
    echo "  WARNING: Unexpected response (HTTP ${HTTP_CODE})"
fi
sleep 0.5
echo ""

# ── Phase 1: Seed categories ─────────────────────────────────────────────────

echo "--- Phase 1: Seed categories (${NUM_CATEGORIES}) ---"

CAT_ITEMS=""
for ((i = 0; i < NUM_CATEGORIES; i++)); do
    cat="${CATEGORIES[$i]}"
    if [ -n "$CAT_ITEMS" ]; then CAT_ITEMS="${CAT_ITEMS},"; fi
    CAT_ITEMS="${CAT_ITEMS}
    {\"@id\": \"ex:cat-${cat}\", \"@type\": \"ex:Category\", \"ex:label\": \"${cat}\", \"ex:categoryId\": $((i + 1))}"
done

CAT_PAYLOAD="{
  \"@context\": {\"ex\": \"http://example.org/ns/\"},
  \"@graph\": [${CAT_ITEMS}
  ]
}"

backpressure_insert "$CAT_PAYLOAD" "categories"
echo "  ${NUM_CATEGORIES} categories inserted"
echo ""

# ── Phase 2: Sustained insert load ───────────────────────────────────────────

echo "--- Phase 2: Insert ${TOTAL_PRODUCTS} products (${TOTAL_BATCHES} batches of ${BATCH_SIZE}) ---"

TOTAL_RETRIES=0
TOTAL_INSERTED=0
PHASE2_START=$(epoch_ms)

for ((batch = 0; batch < TOTAL_BATCHES; batch++)); do
    start_id=$((batch * BATCH_SIZE + 1))
    remaining=$((TOTAL_PRODUCTS - batch * BATCH_SIZE))
    count=$BATCH_SIZE
    if [ "$remaining" -lt "$count" ]; then count=$remaining; fi

    payload=$(generate_batch "$start_id" "$count")

    if backpressure_insert "$payload" "$((batch + 1))"; then
        TOTAL_INSERTED=$((TOTAL_INSERTED + count))
    else
        echo "  Batch $((batch + 1)) failed, stopping inserts"
        break
    fi

    # Progress every 10 batches
    if (( (batch + 1) % 10 == 0 )) || (( batch + 1 == TOTAL_BATCHES )); then
        local_now=$(epoch_ms)
        elapsed=$(( (local_now - PHASE2_START) / 1000 ))
        if [ "$elapsed" -gt 0 ]; then
            rate=$((TOTAL_INSERTED / elapsed))
        else
            rate="$TOTAL_INSERTED"
        fi
        echo "  Progress: batch $((batch + 1))/${TOTAL_BATCHES}, ${TOTAL_INSERTED} products, ~${rate} products/sec"
    fi
done

PHASE2_END=$(epoch_ms)
PHASE2_SECS=$(( (PHASE2_END - PHASE2_START) / 1000 ))
echo ""
echo "  Phase 2 complete: ${TOTAL_INSERTED} products in ${PHASE2_SECS}s (${TOTAL_BOUNCES} server bounces)"
echo ""

# ── Phase 3: Index settle + second wave ──────────────────────────────────────

echo "--- Phase 3: Wait for indexing to settle ---"

echo "  Waiting for server to catch up (test query)..."
for ((attempt = 1; attempt <= 30; attempt++)); do
    RESP=$(curl -s -w "\n%{http_code}" \
        -X POST "${BASE_URL}/v1/fluree/query/${LEDGER}" \
        -H "Content-Type: application/sparql-query" \
        -d "SELECT (COUNT(?s) AS ?cnt) WHERE { ?s a <http://example.org/ns/Product> }")

    HTTP_CODE=$(echo "$RESP" | tail -n1)
    if [ "$HTTP_CODE" = "200" ]; then
        echo "  Server responsive (attempt ${attempt})"
        break
    fi
    echo "  Server busy (HTTP ${HTTP_CODE}), waiting 3s... (attempt ${attempt}/30)"
    sleep 3
done
echo ""

# Second wave: 2 bursts of additional products
echo "  Second wave: 2 bursts of additional products..."
WAVE2_BASE=$((TOTAL_PRODUCTS + 1))

for ((wave = 1; wave <= 2; wave++)); do
    BURST_SIZE=5000
    BURST_BATCHES=$(( (BURST_SIZE + BATCH_SIZE - 1) / BATCH_SIZE ))
    echo "  Burst ${wave}/2: ${BURST_SIZE} products..."

    for ((batch = 0; batch < BURST_BATCHES; batch++)); do
        start_id=$((WAVE2_BASE + (wave - 1) * BURST_SIZE + batch * BATCH_SIZE))
        remaining=$((BURST_SIZE - batch * BATCH_SIZE))
        count=$BATCH_SIZE
        if [ "$remaining" -lt "$count" ]; then count=$remaining; fi

        payload=$(generate_batch "$start_id" "$count")
        backpressure_insert "$payload" "wave${wave}-batch$((batch + 1))" || true
    done
    echo "  Burst ${wave} done"
    sleep 2
done
echo ""
sleep 5

# ── Phase 4: Query battery ──────────────────────────────────────────────────

echo "--- Phase 4: Expensive query battery ---"
echo ""
bounce_server "before query battery"



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

# ── Summary ──────────────────────────────────────────────────────────────────

echo "============================================="
echo "  STRESS TEST COMPLETE"
echo "============================================="
echo "Products inserted:  ${TOTAL_INSERTED} (phase 2) + ~10000 (phase 3 bursts)"
echo "Phase 2 duration:   ${PHASE2_SECS}s"
echo "Server bounces:     ${TOTAL_BOUNCES}"
echo "Query battery:      5 queries x 3 iterations"
echo ""
echo "Open Jaeger at http://localhost:16686"
echo "Look for:"
echo "  - index_build traces with gc_walk_chain + gc_delete_entries child spans"
echo "  - query:sparql traces with scan, join, filter, project, sort operators"
echo "  - Multiple service instances from server bounces"
