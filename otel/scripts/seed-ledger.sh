#!/usr/bin/env bash
# Create a ledger and insert a small seed dataset via the HTTP API.
# Usage: seed-ledger.sh [BASE_URL] [LEDGER]

set -euo pipefail

BASE_URL="${1:-http://localhost:8090}"
LEDGER="${2:-otel-test:main}"

echo "=== Seed Ledger ==="
echo "Server: ${BASE_URL}"
echo "Ledger: ${LEDGER}"
echo ""

# ── Create ledger ────────────────────────────────────────────────────────────

echo "Creating ledger '${LEDGER}'..."
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

# ── Insert categories ────────────────────────────────────────────────────────

echo "Inserting categories..."
RESP=$(curl -s -w "\n%{http_code}" \
    -X POST "${BASE_URL}/v1/fluree/insert/${LEDGER}" \
    -H "Content-Type: application/json" \
    -d '{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "@graph": [
    {"@id": "ex:cat-electronics", "@type": "ex:Category", "ex:label": "Electronics", "ex:categoryId": 1},
    {"@id": "ex:cat-clothing",    "@type": "ex:Category", "ex:label": "Clothing",    "ex:categoryId": 2},
    {"@id": "ex:cat-home",        "@type": "ex:Category", "ex:label": "Home",        "ex:categoryId": 3},
    {"@id": "ex:cat-garden",      "@type": "ex:Category", "ex:label": "Garden",      "ex:categoryId": 4},
    {"@id": "ex:cat-sports",      "@type": "ex:Category", "ex:label": "Sports",      "ex:categoryId": 5},
    {"@id": "ex:cat-books",       "@type": "ex:Category", "ex:label": "Books",       "ex:categoryId": 6},
    {"@id": "ex:cat-toys",        "@type": "ex:Category", "ex:label": "Toys",        "ex:categoryId": 7},
    {"@id": "ex:cat-automotive",  "@type": "ex:Category", "ex:label": "Automotive",  "ex:categoryId": 8},
    {"@id": "ex:cat-health",      "@type": "ex:Category", "ex:label": "Health",      "ex:categoryId": 9},
    {"@id": "ex:cat-beauty",      "@type": "ex:Category", "ex:label": "Beauty",      "ex:categoryId": 10}
  ]
}')

HTTP_CODE=$(echo "$RESP" | tail -n1)
echo "  Categories inserted (HTTP ${HTTP_CODE})"
sleep 0.5

# ── Insert products (batch 1: 25 products) ───────────────────────────────────

echo "Inserting products (batch 1/2)..."
RESP=$(curl -s -w "\n%{http_code}" \
    -X POST "${BASE_URL}/v1/fluree/insert/${LEDGER}" \
    -H "Content-Type: application/json" \
    -d '{
  "@context": {
    "ex": "http://example.org/ns/",
    "xsd": "http://www.w3.org/2001/XMLSchema#"
  },
  "@graph": [
    {"@id": "ex:prod-001", "@type": "ex:Product", "ex:name": "Laptop Pro",       "ex:price": {"@value": "999.99", "@type": "xsd:decimal"}, "ex:sku": "SKU-001", "ex:category": {"@id": "ex:cat-electronics"}, "ex:inStock": true,  "ex:rating": 5},
    {"@id": "ex:prod-002", "@type": "ex:Product", "ex:name": "Wireless Mouse",   "ex:price": {"@value": "29.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-002", "ex:category": {"@id": "ex:cat-electronics"}, "ex:inStock": true,  "ex:rating": 4},
    {"@id": "ex:prod-003", "@type": "ex:Product", "ex:name": "USB-C Cable",      "ex:price": {"@value": "12.50",  "@type": "xsd:decimal"}, "ex:sku": "SKU-003", "ex:category": {"@id": "ex:cat-electronics"}, "ex:inStock": true,  "ex:rating": 3},
    {"@id": "ex:prod-004", "@type": "ex:Product", "ex:name": "Monitor 27in",     "ex:price": {"@value": "349.00", "@type": "xsd:decimal"}, "ex:sku": "SKU-004", "ex:category": {"@id": "ex:cat-electronics"}, "ex:inStock": false, "ex:rating": 4},
    {"@id": "ex:prod-005", "@type": "ex:Product", "ex:name": "Keyboard",         "ex:price": {"@value": "79.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-005", "ex:category": {"@id": "ex:cat-electronics"}, "ex:inStock": true,  "ex:rating": 5, "ex:relatedTo": {"@id": "ex:prod-002"}},
    {"@id": "ex:prod-006", "@type": "ex:Product", "ex:name": "Cotton T-Shirt",   "ex:price": {"@value": "19.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-006", "ex:category": {"@id": "ex:cat-clothing"},    "ex:inStock": true,  "ex:rating": 3},
    {"@id": "ex:prod-007", "@type": "ex:Product", "ex:name": "Denim Jeans",      "ex:price": {"@value": "49.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-007", "ex:category": {"@id": "ex:cat-clothing"},    "ex:inStock": true,  "ex:rating": 4},
    {"@id": "ex:prod-008", "@type": "ex:Product", "ex:name": "Running Shoes",    "ex:price": {"@value": "89.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-008", "ex:category": {"@id": "ex:cat-clothing"},    "ex:inStock": false, "ex:rating": 5},
    {"@id": "ex:prod-009", "@type": "ex:Product", "ex:name": "Winter Jacket",    "ex:price": {"@value": "149.99", "@type": "xsd:decimal"}, "ex:sku": "SKU-009", "ex:category": {"@id": "ex:cat-clothing"},    "ex:inStock": true,  "ex:rating": 4},
    {"@id": "ex:prod-010", "@type": "ex:Product", "ex:name": "Silk Scarf",       "ex:price": {"@value": "34.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-010", "ex:category": {"@id": "ex:cat-clothing"},    "ex:inStock": true,  "ex:rating": 2},
    {"@id": "ex:prod-011", "@type": "ex:Product", "ex:name": "Table Lamp",       "ex:price": {"@value": "45.00",  "@type": "xsd:decimal"}, "ex:sku": "SKU-011", "ex:category": {"@id": "ex:cat-home"},        "ex:inStock": true,  "ex:rating": 3},
    {"@id": "ex:prod-012", "@type": "ex:Product", "ex:name": "Throw Pillow",     "ex:price": {"@value": "22.50",  "@type": "xsd:decimal"}, "ex:sku": "SKU-012", "ex:category": {"@id": "ex:cat-home"},        "ex:inStock": true,  "ex:rating": 4},
    {"@id": "ex:prod-013", "@type": "ex:Product", "ex:name": "Wall Clock",       "ex:price": {"@value": "35.00",  "@type": "xsd:decimal"}, "ex:sku": "SKU-013", "ex:category": {"@id": "ex:cat-home"},        "ex:inStock": true,  "ex:rating": 5},
    {"@id": "ex:prod-014", "@type": "ex:Product", "ex:name": "Curtains",         "ex:price": {"@value": "65.00",  "@type": "xsd:decimal"}, "ex:sku": "SKU-014", "ex:category": {"@id": "ex:cat-home"},        "ex:inStock": false, "ex:rating": 3},
    {"@id": "ex:prod-015", "@type": "ex:Product", "ex:name": "Rug 5x7",          "ex:price": {"@value": "129.00", "@type": "xsd:decimal"}, "ex:sku": "SKU-015", "ex:category": {"@id": "ex:cat-home"},        "ex:inStock": true,  "ex:rating": 4},
    {"@id": "ex:prod-016", "@type": "ex:Product", "ex:name": "Garden Hose",      "ex:price": {"@value": "28.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-016", "ex:category": {"@id": "ex:cat-garden"},      "ex:inStock": true,  "ex:rating": 3},
    {"@id": "ex:prod-017", "@type": "ex:Product", "ex:name": "Pruning Shears",   "ex:price": {"@value": "18.50",  "@type": "xsd:decimal"}, "ex:sku": "SKU-017", "ex:category": {"@id": "ex:cat-garden"},      "ex:inStock": true,  "ex:rating": 4},
    {"@id": "ex:prod-018", "@type": "ex:Product", "ex:name": "Plant Pot Large",  "ex:price": {"@value": "15.00",  "@type": "xsd:decimal"}, "ex:sku": "SKU-018", "ex:category": {"@id": "ex:cat-garden"},      "ex:inStock": true,  "ex:rating": 2},
    {"@id": "ex:prod-019", "@type": "ex:Product", "ex:name": "Lawn Mower",       "ex:price": {"@value": "299.00", "@type": "xsd:decimal"}, "ex:sku": "SKU-019", "ex:category": {"@id": "ex:cat-garden"},      "ex:inStock": false, "ex:rating": 5},
    {"@id": "ex:prod-020", "@type": "ex:Product", "ex:name": "Fertilizer 10kg",  "ex:price": {"@value": "24.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-020", "ex:category": {"@id": "ex:cat-garden"},      "ex:inStock": true,  "ex:rating": 3},
    {"@id": "ex:prod-021", "@type": "ex:Product", "ex:name": "Basketball",       "ex:price": {"@value": "25.00",  "@type": "xsd:decimal"}, "ex:sku": "SKU-021", "ex:category": {"@id": "ex:cat-sports"},      "ex:inStock": true,  "ex:rating": 4},
    {"@id": "ex:prod-022", "@type": "ex:Product", "ex:name": "Yoga Mat",         "ex:price": {"@value": "35.00",  "@type": "xsd:decimal"}, "ex:sku": "SKU-022", "ex:category": {"@id": "ex:cat-sports"},      "ex:inStock": true,  "ex:rating": 5},
    {"@id": "ex:prod-023", "@type": "ex:Product", "ex:name": "Tennis Racket",    "ex:price": {"@value": "89.00",  "@type": "xsd:decimal"}, "ex:sku": "SKU-023", "ex:category": {"@id": "ex:cat-sports"},      "ex:inStock": true,  "ex:rating": 4},
    {"@id": "ex:prod-024", "@type": "ex:Product", "ex:name": "Dumbbells 20lb",   "ex:price": {"@value": "42.00",  "@type": "xsd:decimal"}, "ex:sku": "SKU-024", "ex:category": {"@id": "ex:cat-sports"},      "ex:inStock": false, "ex:rating": 3},
    {"@id": "ex:prod-025", "@type": "ex:Product", "ex:name": "Jump Rope",        "ex:price": {"@value": "12.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-025", "ex:category": {"@id": "ex:cat-sports"},      "ex:inStock": true,  "ex:rating": 4, "ex:relatedTo": {"@id": "ex:prod-022"}}
  ]
}')

HTTP_CODE=$(echo "$RESP" | tail -n1)
echo "  Products batch 1 inserted (HTTP ${HTTP_CODE})"
sleep 0.5

# ── Insert products (batch 2: 25 more products) ──────────────────────────────

echo "Inserting products (batch 2/2)..."
RESP=$(curl -s -w "\n%{http_code}" \
    -X POST "${BASE_URL}/v1/fluree/insert/${LEDGER}" \
    -H "Content-Type: application/json" \
    -d '{
  "@context": {
    "ex": "http://example.org/ns/",
    "xsd": "http://www.w3.org/2001/XMLSchema#"
  },
  "@graph": [
    {"@id": "ex:prod-026", "@type": "ex:Product", "ex:name": "Mystery Novel",    "ex:price": {"@value": "14.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-026", "ex:category": {"@id": "ex:cat-books"},       "ex:inStock": true,  "ex:rating": 5},
    {"@id": "ex:prod-027", "@type": "ex:Product", "ex:name": "Cookbook",          "ex:price": {"@value": "29.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-027", "ex:category": {"@id": "ex:cat-books"},       "ex:inStock": true,  "ex:rating": 4},
    {"@id": "ex:prod-028", "@type": "ex:Product", "ex:name": "Sci-Fi Anthology", "ex:price": {"@value": "18.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-028", "ex:category": {"@id": "ex:cat-books"},       "ex:inStock": true,  "ex:rating": 3},
    {"@id": "ex:prod-029", "@type": "ex:Product", "ex:name": "History Atlas",    "ex:price": {"@value": "45.00",  "@type": "xsd:decimal"}, "ex:sku": "SKU-029", "ex:category": {"@id": "ex:cat-books"},       "ex:inStock": false, "ex:rating": 4},
    {"@id": "ex:prod-030", "@type": "ex:Product", "ex:name": "Poetry Collection","ex:price": {"@value": "12.00",  "@type": "xsd:decimal"}, "ex:sku": "SKU-030", "ex:category": {"@id": "ex:cat-books"},       "ex:inStock": true,  "ex:rating": 2},
    {"@id": "ex:prod-031", "@type": "ex:Product", "ex:name": "Building Blocks",  "ex:price": {"@value": "34.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-031", "ex:category": {"@id": "ex:cat-toys"},        "ex:inStock": true,  "ex:rating": 5},
    {"@id": "ex:prod-032", "@type": "ex:Product", "ex:name": "Board Game",       "ex:price": {"@value": "24.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-032", "ex:category": {"@id": "ex:cat-toys"},        "ex:inStock": true,  "ex:rating": 4},
    {"@id": "ex:prod-033", "@type": "ex:Product", "ex:name": "Stuffed Bear",     "ex:price": {"@value": "19.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-033", "ex:category": {"@id": "ex:cat-toys"},        "ex:inStock": true,  "ex:rating": 3},
    {"@id": "ex:prod-034", "@type": "ex:Product", "ex:name": "Puzzle 1000pc",    "ex:price": {"@value": "16.00",  "@type": "xsd:decimal"}, "ex:sku": "SKU-034", "ex:category": {"@id": "ex:cat-toys"},        "ex:inStock": false, "ex:rating": 4},
    {"@id": "ex:prod-035", "@type": "ex:Product", "ex:name": "RC Car",           "ex:price": {"@value": "59.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-035", "ex:category": {"@id": "ex:cat-toys"},        "ex:inStock": true,  "ex:rating": 5, "ex:relatedTo": {"@id": "ex:prod-031"}},
    {"@id": "ex:prod-036", "@type": "ex:Product", "ex:name": "Car Wax",          "ex:price": {"@value": "11.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-036", "ex:category": {"@id": "ex:cat-automotive"},   "ex:inStock": true,  "ex:rating": 3},
    {"@id": "ex:prod-037", "@type": "ex:Product", "ex:name": "Floor Mats",       "ex:price": {"@value": "39.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-037", "ex:category": {"@id": "ex:cat-automotive"},   "ex:inStock": true,  "ex:rating": 4},
    {"@id": "ex:prod-038", "@type": "ex:Product", "ex:name": "Phone Mount",      "ex:price": {"@value": "15.00",  "@type": "xsd:decimal"}, "ex:sku": "SKU-038", "ex:category": {"@id": "ex:cat-automotive"},   "ex:inStock": true,  "ex:rating": 4},
    {"@id": "ex:prod-039", "@type": "ex:Product", "ex:name": "Tire Inflator",    "ex:price": {"@value": "49.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-039", "ex:category": {"@id": "ex:cat-automotive"},   "ex:inStock": false, "ex:rating": 5},
    {"@id": "ex:prod-040", "@type": "ex:Product", "ex:name": "Dash Cam",         "ex:price": {"@value": "79.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-040", "ex:category": {"@id": "ex:cat-automotive"},   "ex:inStock": true,  "ex:rating": 3},
    {"@id": "ex:prod-041", "@type": "ex:Product", "ex:name": "Vitamins Multi",   "ex:price": {"@value": "22.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-041", "ex:category": {"@id": "ex:cat-health"},      "ex:inStock": true,  "ex:rating": 4},
    {"@id": "ex:prod-042", "@type": "ex:Product", "ex:name": "First Aid Kit",    "ex:price": {"@value": "18.00",  "@type": "xsd:decimal"}, "ex:sku": "SKU-042", "ex:category": {"@id": "ex:cat-health"},      "ex:inStock": true,  "ex:rating": 5},
    {"@id": "ex:prod-043", "@type": "ex:Product", "ex:name": "Protein Powder",   "ex:price": {"@value": "34.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-043", "ex:category": {"@id": "ex:cat-health"},      "ex:inStock": true,  "ex:rating": 3},
    {"@id": "ex:prod-044", "@type": "ex:Product", "ex:name": "Blood Pressure Mon","ex:price": {"@value": "45.00", "@type": "xsd:decimal"}, "ex:sku": "SKU-044", "ex:category": {"@id": "ex:cat-health"},      "ex:inStock": false, "ex:rating": 4},
    {"@id": "ex:prod-045", "@type": "ex:Product", "ex:name": "Resistance Bands", "ex:price": {"@value": "15.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-045", "ex:category": {"@id": "ex:cat-health"},      "ex:inStock": true,  "ex:rating": 4, "ex:relatedTo": {"@id": "ex:prod-022"}},
    {"@id": "ex:prod-046", "@type": "ex:Product", "ex:name": "Face Cream",       "ex:price": {"@value": "28.00",  "@type": "xsd:decimal"}, "ex:sku": "SKU-046", "ex:category": {"@id": "ex:cat-beauty"},      "ex:inStock": true,  "ex:rating": 3},
    {"@id": "ex:prod-047", "@type": "ex:Product", "ex:name": "Shampoo Organic",  "ex:price": {"@value": "16.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-047", "ex:category": {"@id": "ex:cat-beauty"},      "ex:inStock": true,  "ex:rating": 5},
    {"@id": "ex:prod-048", "@type": "ex:Product", "ex:name": "Nail Polish Set",  "ex:price": {"@value": "12.50",  "@type": "xsd:decimal"}, "ex:sku": "SKU-048", "ex:category": {"@id": "ex:cat-beauty"},      "ex:inStock": true,  "ex:rating": 4},
    {"@id": "ex:prod-049", "@type": "ex:Product", "ex:name": "Perfume Classic",  "ex:price": {"@value": "65.00",  "@type": "xsd:decimal"}, "ex:sku": "SKU-049", "ex:category": {"@id": "ex:cat-beauty"},      "ex:inStock": false, "ex:rating": 5},
    {"@id": "ex:prod-050", "@type": "ex:Product", "ex:name": "Sunscreen SPF50",  "ex:price": {"@value": "14.99",  "@type": "xsd:decimal"}, "ex:sku": "SKU-050", "ex:category": {"@id": "ex:cat-beauty"},      "ex:inStock": true,  "ex:rating": 3}
  ]
}')

HTTP_CODE=$(echo "$RESP" | tail -n1)
echo "  Products batch 2 inserted (HTTP ${HTTP_CODE})"

echo ""
echo "Seed complete: 10 categories + 50 products in '${LEDGER}'"
