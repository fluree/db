#!/usr/bin/env bash
#
# Bulk-import dictionary-upload memory repro under a hard cgroup cap.
#
# Proves PR#2 (streaming `upload_dicts_from_disk`): the same upload, run on the
# same fixture under the same container `--memory` cap, OOM-kills on the
# materialized path (term-sized anonymous Vecs — the pre-fix behavior, still
# reachable via trust=0) but survives on the streaming path (trust=1).
#
# A Docker `--memory` cap cleanly OOM-kills on unreclaimable anonymous heap; it
# does NOT cleanly kill on file-backed mmap pages (those are reclaimable). The
# upload's materialization is anonymous Vecs, so this is a clean A/B — and it
# needs only HEAD (trust=0 IS the old memory behavior).
#
# Build runs UNCAPPED (rustc needs more than the cap); only the test process is
# capped. Build artifacts + the crate registry live in named volumes so re-runs
# are fast.
#
# Usage:  scripts/import-memory-repro.sh
# Tunables (env):
#   CAP=512m                 container memory cap (swap disabled)
#   FLUREE_UPLOAD_CHUNKS=200 FLUREE_UPLOAD_LOCAL=50000   -> N = chunks*local
#   IMAGE=rust:1-bookworm
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="${IMAGE:-rust:1-bookworm}"
CAP="${CAP:-512m}"
CHUNKS="${FLUREE_UPLOAD_CHUNKS:-200}"
LOCAL="${FLUREE_UPLOAD_LOCAL:-50000}"
TEST="build::upload_dicts::tests::stress_upload_memory"
VOL_TARGET="fluree-repro-target"
VOL_CARGO="fluree-repro-cargo"

echo "repo=$REPO image=$IMAGE cap=$CAP N=$((CHUNKS*LOCAL)) (chunks=$CHUNKS local=$LOCAL)"

docker volume create "$VOL_TARGET" >/dev/null
docker volume create "$VOL_CARGO" >/dev/null

base=(--rm
  -v "$REPO":/repo:ro
  -v "$VOL_TARGET":/target
  -v "$VOL_CARGO":/usr/local/cargo/registry
  -e CARGO_TARGET_DIR=/target
  -w /repo
  "$IMAGE")

echo "==> [build] compiling test binary (uncapped) ..."
docker run "${base[@]}" \
  cargo test -p fluree-db-indexer --lib --locked --no-run

# Resolve the freshly-built lib test binary inside the shared volume.
find_bin='find /target/debug/deps -maxdepth 1 -type f -name "fluree_db_indexer-*" ! -name "*.d" -perm -u+x -printf "%T@ %p\n" | sort -rn | head -1 | cut -d" " -f2-'

run_one() {
  local trust="$1" label="$2"
  echo
  echo "==> [run] trust=$trust ($label) under --memory=$CAP ..."
  set +e
  docker run --memory="$CAP" --memory-swap="$CAP" \
    -e FLUREE_UPLOAD_CHUNKS="$CHUNKS" -e FLUREE_UPLOAD_LOCAL="$LOCAL" \
    -e FLUREE_UPLOAD_TRUST="$trust" \
    "${base[@]}" \
    bash -lc "bin=\$($find_bin); echo \"binary: \$bin\"; \"\$bin\" --ignored --exact --nocapture $TEST"
  local code=$?
  set -e
  echo "exit code: $code"
  return $code
}

mat_code=0; run_one 0 "materialized / pre-fix" || mat_code=$?
str_code=0; run_one 1 "streaming / fixed"      || str_code=$?

echo
echo "================ VERDICT ================"
echo "materialized (trust=0) exit=$mat_code  (expect non-zero: OOM-killed=137)"
echo "streaming    (trust=1) exit=$str_code  (expect 0: survived)"
if [ "$mat_code" -ne 0 ] && [ "$str_code" -eq 0 ]; then
  echo "PASS: streaming survived the cap that OOM-killed the materialized path."
  exit 0
else
  echo "INCONCLUSIVE: expected materialized to OOM and streaming to pass."
  echo "  If both passed, the cap ($CAP) is too high for N=$((CHUNKS*LOCAL)) — lower CAP or raise N."
  echo "  If both failed, the cap is too low even for streaming — raise CAP."
  exit 1
fi
