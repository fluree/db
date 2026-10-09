#!/usr/bin/env bash
# Check matrix: can a fluree binary read Iceberg tables catalogued in AWS Glue?
#
#   FLUREE_BIN=/path/to/fluree ./check.sh baseline   # today's main: proves the gap reproduces
#   FLUREE_BIN=/path/to/fluree ./check.sh target     # what a Glue catalog mode must deliver
#
# Requires ./up.sh first (moto S3 + Glue mock, seeded tables). Exit code = number of failures.
#
# Expectation tokens: a number = exact count; "ERR~text" = an error containing text (case-insensitive,
# `|` separates alternatives); "INFO" = record only; "NOT0" = anything except a count of 0
# (a silently wrong answer is the one outcome that must never pass).
set -uo pipefail
source "$(dirname "$0")/env.sh"
PROFILE="${1:-target}"
case "$PROFILE" in baseline|target) ;; *) echo "usage: $0 baseline|target" >&2; exit 2 ;; esac
FLUREE="${FLUREE_BIN:-fluree}"
MAPS="$HARNESS_DIR/mappings"
[ -f "$RUN_DIR/manifest.json" ] || { echo "run ./up.sh first" >&2; exit 2; }
curl -sf "$MOTO_ENDPOINT/moto-api/" >/dev/null || { echo "moto is not running; run ./up.sh" >&2; exit 2; }

PROJ="$(mktemp -d "$RUN_DIR/proj-$PROFILE.XXXX")"
# Fluree's Iceberg disk caches live under $TMPDIR and outlive the process; a fresh one per run
# keeps a run against a reseeded moto from answering out of an earlier run's cache.
export TMPDIR="$PROJ/tmp"; mkdir -p "$TMPDIR"
cd "$PROJ" && "$FLUREE" init -q >/dev/null 2>&1
S3=(--s3-endpoint "$MOTO_ENDPOINT" --s3-path-style --s3-region "$AWS_REGION")
P='PREFIX ex: <http://example.org/>'
Q_CUSTOMERS="$P SELECT (COUNT(?c) AS ?n) WHERE { ?c a ex:Customer }"
Q_ORDERS="$P SELECT (COUNT(?o) AS ?n) WHERE { ?o a ex:Order }"
Q_JOIN="$P SELECT (COUNT(?o) AS ?n) WHERE { ?o a ex:Order ; ex:customer ?c . ?c a ex:Customer }"

# Mapping for a table that isn't in the seed manifest (negative checks).
mapping_for() {  # $1 = table name; writes a minimal one-table mapping and prints its path
  local f="$PROJ/$1.ttl"
  printf '@base <http://example.org/mapping/> .\n@prefix rr: <http://www.w3.org/ns/r2rml#> .\n@prefix ex: <http://example.org/> .\n<#T> a rr:TriplesMap ;\n  rr:logicalTable [ rr:tableName "demo.%s" ] ;\n  rr:subjectMap [ rr:template "http://example.org/row/{id}" ; rr:class ex:Row ] .\n' "$1" > "$f"
  echo "$f"
}

# observe NAME QUERY MAP_ARGS... -> prints a count, or "ERR: <first error line>"
observe() {
  local name="$1" query="$2"; shift 2
  local out
  out="$("$FLUREE" iceberg map "$name" "$@" 2>&1)"
  if grep -qi '^error' <<<"$out"; then echo "ERR: $(grep -im1 '^error' <<<"$out" | cut -c1-160)"; return; fi
  out="$("$FLUREE" query "$name" "$query" 2>&1)"   # table format: graph-source targets refuse csv
  if grep -qi '^error' <<<"$out"; then echo "ERR: $(grep -im1 '^error' <<<"$out" | cut -c1-160)"; return; fi
  local n; n="$(grep -oE '^\| *[0-9]+ *\|' <<<"$out" | head -1 | tr -dc '0-9')"
  [ -n "$n" ] && echo "$n" || echo "UNPARSED: $(tr '\n' ' ' <<<"$out" | cut -c1-120)"
}

FAILS=0; ROWS=()
grade() {  # ID DESC EXPECT OBSERVED
  local id="$1" desc="$2" exp="$3" obs="$4" verdict
  case "$exp" in
    INFO) verdict=info ;;
    NOT0) [ "$obs" = "0" ] && verdict=FAIL || verdict=pass ;;
    ERR~*) if [[ "$obs" == ERR:* ]] && grep -qiE "${exp#ERR~}" <<<"$obs"; then verdict=pass; else verdict=FAIL; fi ;;
    *) [ "$obs" = "$exp" ] && verdict=pass || verdict=FAIL ;;
  esac
  [ "$verdict" = FAIL ] && FAILS=$((FAILS + 1))
  ROWS+=("$(printf '%-3s | %-6s | %-58s | expect %-28s | got %s' "$id" "$verdict" "$desc" "$exp" "$obs")")
}

# exp PROFILE_BASELINE PROFILE_TARGET -> picks the active one
exp() { [ "$PROFILE" = baseline ] && echo "$1" || echo "$2"; }
GLUE=(--mode glue --region "$AWS_REGION")
UNKNOWN_MODE='ERR~unknown catalog mode|unexpected argument'   # no glue mode at all (4.2.x rejects --region first)

grade C1 "direct: Glue-written table + version-hint (control)" "$(exp 50 50)" \
  "$(observe c1 "$Q_CUSTOMERS" --mode direct --table-location "$WAREHOUSE/demo.db/customers_hinted" --r2rml "$MAPS/customers_hinted.ttl" "${S3[@]}")"
grade C2 "direct: Glue-written table, no version-hint" "$(exp 'ERR~version-hint' INFO)" \
  "$(observe c2 "$Q_CUSTOMERS" --mode direct --table-location "$WAREHOUSE/demo.db/customers" --r2rml "$MAPS/customers.ttl" "${S3[@]}")"
grade C3 "glue: one table resolved via Glue GetTable" "$(exp "$UNKNOWN_MODE" 50)" \
  "$(observe c3 "$Q_CUSTOMERS" "${GLUE[@]}" --r2rml "$MAPS/customers.ttl" "${S3[@]}")"
grade C4 "glue: 3 metadata files; Glue's current one must win" "$(exp "$UNKNOWN_MODE" 200)" \
  "$(observe c4 "$Q_ORDERS" "${GLUE[@]}" --r2rml "$MAPS/orders.ttl" "${S3[@]}")"
grade C5 "glue: two-table R2RML join (orders->customers)" "$(exp "$UNKNOWN_MODE" 200)" \
  "$(observe c5 "$Q_JOIN" "${GLUE[@]}" --r2rml "$MAPS/customers_orders.ttl" "${S3[@]}")"
grade C6 "glue: catalog pointer beats an orphan 00099 file" "$(exp "$UNKNOWN_MODE" 100)" \
  "$(observe c6 "$Q_ORDERS" "${GLUE[@]}" --r2rml "$MAPS/orders_orphan.ttl" "${S3[@]}")"
grade C7 "glue: table missing from Glue -> clear error" "$(exp "$UNKNOWN_MODE" 'ERR~not found|EntityNotFound|does not exist')" \
  "$(observe c7 "$P SELECT (COUNT(?r) AS ?n) WHERE { ?r a ex:Row }" "${GLUE[@]}" --r2rml "$(mapping_for nope)" "${S3[@]}")"
grade C8 "glue: non-Iceberg Glue table -> clear error" "$(exp "$UNKNOWN_MODE" 'ERR~metadata_location|not an iceberg|table_type')" \
  "$(observe c8 "$P SELECT (COUNT(?r) AS ?n) WHERE { ?r a ex:Row }" "${GLUE[@]}" --r2rml "$(mapping_for hive_table)" "${S3[@]}")"
grade C9 "direct, no hint, orphan file: never a silent wrong 0" "$(exp 'ERR~version-hint' NOT0)" \
  "$(observe c9 "$Q_ORDERS" --mode direct --table-location "$WAREHOUSE/demo.db/orders_orphan" --r2rml "$MAPS/orders_orphan.ttl" "${S3[@]}")"

echo "fluree: $("$FLUREE" --version 2>&1 | head -1)   profile: $PROFILE   project: $PROJ"
printf '%s\n' "${ROWS[@]}"
echo "failures: $FAILS"
exit "$FAILS"
