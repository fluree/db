#!/usr/bin/env bash
# Self-test: prove the harness's mappings and expected counts with TODAY's binary, before any
# Glue catalog mode exists. It writes version-hint.text for every table, each pointing at the
# metadata file Glue marks as current. Direct mode then reads exactly what a correct Glue mode
# would, so the target counts (50 / 200 / 200 / 100) are checked independently of the code under
# test. Reseeds afterwards, so the hints don't leak into check.sh runs.
#
#   FLUREE_BIN=/path/to/fluree ./selftest.sh
set -uo pipefail
source "$(dirname "$0")/env.sh"
FLUREE="${FLUREE_BIN:-fluree}"
MAPS="$HARNESS_DIR/mappings"
"$HARNESS_DIR/up.sh" >/dev/null

"$PY" -I - "$RUN_DIR/manifest.json" <<'PYEOF'
import json, os, sys, boto3
m = json.load(open(sys.argv[1]))
s3 = boto3.client("s3", endpoint_url=os.environ["MOTO_ENDPOINT"], aws_access_key_id="test",
                  aws_secret_access_key="test", region_name=os.environ["AWS_REGION"])
for name, t in m["tables"].items():
    key = t["location"].split(f"s3://{m['bucket']}/", 1)[1] + "/metadata/version-hint.text"
    s3.put_object(Bucket=m["bucket"], Key=key, Body=t["metadata_location"].rsplit("/", 1)[1].encode())
print("hints written for", ", ".join(m["tables"]))
PYEOF

PROJ="$(mktemp -d "$RUN_DIR/proj-selftest.XXXX")"
export TMPDIR="$PROJ/tmp"; mkdir -p "$TMPDIR"   # fresh Iceberg disk caches (see check.sh)
cd "$PROJ" && "$FLUREE" init -q >/dev/null 2>&1
S3=(--s3-endpoint "$MOTO_ENDPOINT" --s3-path-style --s3-region "$AWS_REGION")
P='PREFIX ex: <http://example.org/>'
FAILS=0
run() {  # NAME EXPECT QUERY MAP_ARGS...
  local name="$1" exp="$2" q="$3"; shift 3
  local out n
  out="$("$FLUREE" iceberg map "$name" "$@" 2>&1 && "$FLUREE" query "$name" "$q" 2>&1)"
  n="$(grep -oE '^\| *[0-9]+ *\|' <<<"$out" | head -1 | tr -dc '0-9')"
  if [ "$n" = "$exp" ]; then echo "pass  $name = $n"; else echo "FAIL  $name expected $exp got ${n:-$(grep -im1 error <<<"$out")}"; FAILS=$((FAILS+1)); fi
}
run st-customers 50  "$P SELECT (COUNT(?c) AS ?n) WHERE { ?c a ex:Customer }" --mode direct --table-location "$WAREHOUSE/demo.db/customers" --r2rml "$MAPS/customers.ttl" "${S3[@]}"
run st-orders    200 "$P SELECT (COUNT(?o) AS ?n) WHERE { ?o a ex:Order }"    --mode direct --table-location "$WAREHOUSE/demo.db/orders" --r2rml "$MAPS/orders.ttl" "${S3[@]}"
run st-orphan    100 "$P SELECT (COUNT(?o) AS ?n) WHERE { ?o a ex:Order }"    --mode direct --table-location "$WAREHOUSE/demo.db/orders_orphan" --r2rml "$MAPS/orders_orphan.ttl" "${S3[@]}"
# Join: warehouse-root direct mode resolves each rr:tableName to its own directory under demo.db.
run st-join      200 "$P SELECT (COUNT(?o) AS ?n) WHERE { ?o a ex:Order ; ex:customer ?c . ?c a ex:Customer }" --mode direct --table-location "$WAREHOUSE/demo.db" --r2rml "$MAPS/customers_orders.ttl" "${S3[@]}"

"$HARNESS_DIR/up.sh" >/dev/null && echo "reseeded (hints removed)"
echo "selftest failures: $FAILS"; exit "$FAILS"
