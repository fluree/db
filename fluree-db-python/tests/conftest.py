"""Process-wide setup that must happen before the engine first runs."""

import os
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
ICEBERG_FIXTURES = REPO / "fluree-db-api" / "tests" / "fixtures" / "iceberg"
DELTA_FIXTURES = REPO / "fluree-db-delta" / "tests" / "fixtures"

# The engine reads its local-table allowlist once, when it first opens a local
# Iceberg or Delta table, so it is set before any test runs.
os.environ["FLUREE_ICEBERG_LOCAL_ROOTS"] = ":".join(str(p.resolve()) for p in (ICEBERG_FIXTURES, DELTA_FIXTURES))
