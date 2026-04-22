#!/usr/bin/env bash
# Generate Turtle data files for OTEL testing.
#
# Domain: Products + Categories — exercises scans, joins, filters, sorts,
# aggregates, group-by, and optional patterns.
#
# Usage: generate-data.sh [ENTITIES] [PROPS] [OUTPUT_DIR]
#   ENTITIES   — Number of product entities (default: 100000)
#   PROPS      — Properties per entity (default: 6, informational only)
#   OUTPUT_DIR — Output directory (default: _data/generated)
#
# Output:
#   data.ttl            — Single file for server insert (via HTTP API) or CLI import
#   chunks/chunk_NNNN.ttl — Chunked files for large dataset testing

set -euo pipefail

ENTITIES="${1:-100000}"
PROPS="${2:-6}"
OUTPUT_DIR="${3:-_data/generated}"
CHUNK_SIZE=5000  # entities per chunk

mkdir -p "${OUTPUT_DIR}/chunks"

CATEGORIES=(
    "Electronics" "Clothing" "Home" "Garden" "Sports"
    "Books" "Toys" "Automotive" "Health" "Beauty"
    "Grocery" "Pet" "Office" "Tools" "Music"
    "Movies" "Software" "Jewelry" "Baby" "Outdoors"
)
NUM_CATEGORIES=${#CATEGORIES[@]}

PREAMBLE='@prefix ex: <http://example.org/ns/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
'

echo "Generating ${ENTITIES} products + ${NUM_CATEGORIES} categories..."
echo "Output: ${OUTPUT_DIR}/"

# ── Write categories ─────────────────────────────────────────────────────────

CATEGORY_TTL=""

for i in "${!CATEGORIES[@]}"; do
    cat_name="${CATEGORIES[$i]}"
    cat_id=$((i + 1))
    parent_ref=""
    # Create hierarchy: first 10 are top-level, rest reference a top-level parent
    if [ "$cat_id" -gt 10 ]; then
        parent_idx=$(( (cat_id - 1) % 10 ))
        parent_ref="    ex:parentCategory ex:category-$(printf '%03d' $((parent_idx + 1))) ;"
    fi

    CATEGORY_TTL+="
ex:category-$(printf '%03d' ${cat_id}) a ex:Category ;
    ex:label \"${cat_name}\" ;
${parent_ref:+${parent_ref}
}    ex:categoryId ${cat_id} .
"
done

# ── Write products ───────────────────────────────────────────────────────────

echo "Writing products..."

# Track chunk boundaries
chunk_num=0
chunk_count=0
current_chunk="${CATEGORY_TTL}"

# Full data file
echo -n "${PREAMBLE}${CATEGORY_TTL}" > "${OUTPUT_DIR}/data.ttl"

# Write first chunk with categories
write_chunk() {
    local fname
    fname=$(printf "chunk_%04d.ttl" "$chunk_num")
    echo -n "${PREAMBLE}${current_chunk}" > "${OUTPUT_DIR}/chunks/${fname}"
    chunk_num=$((chunk_num + 1))
    current_chunk=""
    chunk_count=0
}

for i in $(seq 1 "$ENTITIES"); do
    prod_id=$(printf '%06d' "$i")
    name="Product-${prod_id}"
    # Deterministic but varied values
    price_int=$(( (i * 7 + 3) % 99900 + 100 ))  # 1.00 — 999.99
    price_dec=$(( price_int / 100 )).$(printf '%02d' $((price_int % 100)))
    sku="SKU-${prod_id}"
    cat_idx=$(( (i - 1) % NUM_CATEGORIES + 1 ))
    cat_ref="ex:category-$(printf '%03d' ${cat_idx})"
    in_stock="true"
    if [ $(( i % 5 )) -eq 0 ]; then
        in_stock="false"
    fi
    rating=$(( (i % 5) + 1 ))

    # Sparse relatedTo (every 7th product references the previous one)
    related_line=""
    if [ $(( i % 7 )) -eq 0 ] && [ "$i" -gt 1 ]; then
        related_id=$(printf '%06d' $((i - 1)))
        related_line="    ex:relatedTo ex:product-${related_id} ;"
    fi

    triple="
ex:product-${prod_id} a ex:Product ;
    ex:name \"${name}\" ;
    ex:price \"${price_dec}\"^^xsd:decimal ;
    ex:sku \"${sku}\" ;
    ex:category ${cat_ref} ;
    ex:inStock ${in_stock} ;
${related_line:+${related_line}
}    ex:rating ${rating} .
"

    echo -n "$triple" >> "${OUTPUT_DIR}/data.ttl"
    current_chunk+="$triple"
    chunk_count=$((chunk_count + 1))

    if [ "$chunk_count" -ge "$CHUNK_SIZE" ]; then
        write_chunk
    fi

    # Progress
    if [ $(( i % 10000 )) -eq 0 ]; then
        echo "  ${i}/${ENTITIES} products..."
    fi
done

# Write final partial chunk
if [ "$chunk_count" -gt 0 ]; then
    write_chunk
fi

# Stats
data_size=$(wc -c < "${OUTPUT_DIR}/data.ttl" | tr -d ' ')
data_mb=$(echo "scale=1; ${data_size} / 1048576" | bc)
echo ""
echo "Generated:"
echo "  ${OUTPUT_DIR}/data.ttl — ${data_mb} MB (${ENTITIES} products + ${NUM_CATEGORIES} categories)"
echo "  ${OUTPUT_DIR}/chunks/ — ${chunk_num} chunk files (${CHUNK_SIZE} entities each)"
