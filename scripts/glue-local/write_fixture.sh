#!/usr/bin/env bash
# Regenerate the fixture fluree-db-api's `it_iceberg_glue_moto` test replays into moto:
# Iceberg tables written by pyiceberg's GlueCatalog, exported as their S3 objects plus their
# Glue registrations. Uses its own bucket and Glue database so it never collides with a
# harness run; leaves moto stopped (run ./up.sh again to get the harness tables back).
set -euo pipefail
export BUCKET=fluree-glue-it GLUE_DB=glue_it
source "$(dirname "$0")/env.sh"
DEST="$(cd "$HARNESS_DIR/../.." && pwd)/fluree-db-api/tests/fixtures/iceberg/glue"
rm -rf "$DEST" && mkdir -p "$DEST"
"$HARNESS_DIR/up.sh" --export-fixture "$DEST"
"$HARNESS_DIR/down.sh"
echo "fixture written to $DEST ($(find "$DEST" -type f | wc -l | tr -d ' ') files, $(du -sh "$DEST" | cut -f1))"
