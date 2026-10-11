#!/usr/bin/env bash
# Stop moto. All harness state lives in moto's memory, so stopping it wipes the tables.
set -euo pipefail
source "$(dirname "$0")/env.sh"
if [ -f "$RUN_DIR/moto.pid" ] && kill -0 "$(cat "$RUN_DIR/moto.pid")" 2>/dev/null; then
  kill "$(cat "$RUN_DIR/moto.pid")" && echo "moto stopped"
fi
rm -f "$RUN_DIR/moto.pid"
