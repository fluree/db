#!/usr/bin/env bash
# Start moto (S3 + Glue mock) on loopback, then seed the Iceberg tables. Idempotent: restarts
# moto from empty each time, so every run starts from the same state. Extra arguments go to
# seed.py (write_fixture.sh passes --export-fixture).
set -euo pipefail
source "$(dirname "$0")/env.sh"

if [ -f "$RUN_DIR/moto.pid" ] && kill -0 "$(cat "$RUN_DIR/moto.pid")" 2>/dev/null; then
  kill "$(cat "$RUN_DIR/moto.pid")"; sleep 1
fi
"$RUN_DIR/.venv/bin/moto_server" -H 127.0.0.1 -p "$MOTO_PORT" > "$RUN_DIR/moto.log" 2>&1 &
echo $! > "$RUN_DIR/moto.pid"
for _ in $(seq 1 50); do
  curl -sf "$MOTO_ENDPOINT/moto-api/" >/dev/null 2>&1 && break
  sleep 0.2
done
curl -sf "$MOTO_ENDPOINT/moto-api/" >/dev/null || { echo "moto did not start; see $RUN_DIR/moto.log" >&2; exit 1; }
echo "moto up on $MOTO_ENDPOINT (pid $(cat "$RUN_DIR/moto.pid"))"

"$PY" -I "$HARNESS_DIR/seed.py" "$RUN_DIR/manifest.json" "$@"   # e.g. --export-fixture DIR
