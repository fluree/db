"""Write small local Iceberg tables for fluree/db local-fs testing.

- `silver.people`: two snapshots (3 rows, then +2).
- `silver.people_backlog`: five 600-row appends whose first three snapshots
  have been EXPIRED, so only the two newest remain. This is the shape a
  materialize source is in once its watermark has fallen out of the source
  table's snapshot retention: a full read is forced, and no retained
  snapshot names an early enough checkpoint.
- `silver.people_window`: the same five 600-row appends with every snapshot
  RETAINED. This is the shape of a healthy source that got ahead of its
  materialize job: the watermark still resolves, so the window is read
  incrementally, and it is too large to commit in one pass.
- `silver.amounts`: one snapshot of decimal columns for FILTER pushdown —
  `ratio` is `decimal(38,9)` (BigQuery's `NUMERIC`), with values on either
  side of 0.05 and 1.05 one grid step apart; `price` is `decimal(5,2)`, the
  narrow column a literal like 1000 does not fit.
- `silver.literals`: one snapshot of text that a mapping reads as numbers or
  booleans (`ntext`: "999" and "1000", whose text order is not their numeric
  order; `btext`: "1" and "true"), and a double column holding both zeros.
"""
import shutil, sys
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

WAREHOUSE = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/fluree-local-iceberg")
if WAREHOUSE.exists():
    shutil.rmtree(WAREHOUSE)
WAREHOUSE.mkdir(parents=True)

catalog = SqlCatalog(
    "local",
    uri=f"sqlite:///{WAREHOUSE}/catalog.db",
    warehouse=f"file://{WAREHOUSE}",
)
catalog.create_namespace("silver")

schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("name", pa.string()),
    pa.field("score", pa.float64()),
    pa.field("active", pa.bool_()),
])

table = catalog.create_table("silver.people", schema=schema)

# Snapshot 1: three rows.
table.append(pa.table({
    "id": pa.array([1, 2, 3], pa.int64()),
    "name": pa.array(["alice", "bob", "carol"]),
    "score": pa.array([91.5, 82.0, 77.25], pa.float64()),
    "active": pa.array([True, True, False]),
}))

# Snapshot 2: two more rows (append-only window for incremental scans).
table.append(pa.table({
    "id": pa.array([4, 5], pa.int64()),
    "name": pa.array(["dave", "erin"]),
    "score": pa.array([64.0, 99.9], pa.float64()),
    "active": pa.array([True, False]),
}))

table = catalog.load_table("silver.people")
snaps = list(table.snapshots())
print("table_location:", table.location())
print("metadata_location:", table.metadata_location)
print("snapshots:", [(s.snapshot_id, s.summary.operation.value) for s in snaps])

# ---------------------------------------------------------------------------
# silver.people_backlog: a multi-commit table with EXPIRED early history.
# ---------------------------------------------------------------------------
COMMITS, ROWS_PER_COMMIT, EXPIRE_FIRST = 5, 600, 3

backlog = catalog.create_table("silver.people_backlog", schema=schema)
for c in range(COMMITS):
    lo = c * ROWS_PER_COMMIT + 1
    ids = list(range(lo, lo + ROWS_PER_COMMIT))
    backlog.append(pa.table({
        "id": pa.array(ids, pa.int64()),
        "name": pa.array([f"person-{i:04d}" for i in ids]),
        "score": pa.array([float(i % 100) for i in ids], pa.float64()),
        "active": pa.array([i % 2 == 0 for i in ids]),
    }))

backlog = catalog.load_table("silver.people_backlog")
history = sorted(backlog.snapshots(), key=lambda s: s.sequence_number)
expired = [s.snapshot_id for s in history[:EXPIRE_FIRST]]
backlog.maintenance.expire_snapshots().by_ids(expired).commit()

backlog = catalog.load_table("silver.people_backlog")
retained = sorted(backlog.snapshots(), key=lambda s: s.sequence_number)
print("backlog table_location:", backlog.location())
print("backlog metadata_location:", backlog.metadata_location)
print("backlog expired:", expired)
print("backlog retained:", [(s.snapshot_id, s.sequence_number, s.parent_snapshot_id) for s in retained])

# Expiry drops the snapshots from the metadata; also drop their manifest
# lists from disk, as a real cleanup would, so nothing can resolve them.
meta_dir = Path(backlog.location().removeprefix("file://")) / "metadata"
for sid in expired:
    for f in meta_dir.glob(f"snap-{sid}-*.avro"):
        f.unlink()
        print("removed", f.name)

# ---------------------------------------------------------------------------
# silver.people_window: the same commits, nothing expired.
# ---------------------------------------------------------------------------
window = catalog.create_table("silver.people_window", schema=schema)
for c in range(COMMITS):
    lo = c * ROWS_PER_COMMIT + 1
    ids = list(range(lo, lo + ROWS_PER_COMMIT))
    window.append(pa.table({
        "id": pa.array(ids, pa.int64()),
        "name": pa.array([f"person-{i:04d}" for i in ids]),
        "score": pa.array([float(i % 100) for i in ids], pa.float64()),
        "active": pa.array([i % 2 == 0 for i in ids]),
    }))

window = catalog.load_table("silver.people_window")
print("window table_location:", window.location())
print("window snapshots:", [
    (s.snapshot_id, s.sequence_number, s.parent_snapshot_id, s.summary.get("added-records"))
    for s in sorted(window.snapshots(), key=lambda s: s.sequence_number)
])

# ---------------------------------------------------------------------------
# silver.amounts: decimal columns for FILTER pushdown.
# ---------------------------------------------------------------------------
amounts_schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("ratio", pa.decimal128(38, 9)),
    pa.field("price", pa.decimal128(5, 2)),
])
amounts = catalog.create_table("silver.amounts", schema=amounts_schema)
amounts.append(pa.table({
    "id": pa.array([1, 2, 3, 4, 5], pa.int64()),
    "ratio": pa.array([Decimal(v) for v in [
        "0.049999999", "0.050000000", "0.050000001", "1.050000000", "-0.050000000",
    ]], pa.decimal128(38, 9)),
    "price": pa.array([Decimal(v) for v in [
        "999.99", "1.00", "12.34", "0.05", "-999.99",
    ]], pa.decimal128(5, 2)),
}, schema=amounts_schema))
amounts = catalog.load_table("silver.amounts")
print("amounts table_location:", amounts.location())

# ---------------------------------------------------------------------------
# silver.literals: text read as numbers or booleans, and signed zeros.
# ---------------------------------------------------------------------------
literals_schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("ntext", pa.string()),
    pa.field("btext", pa.string()),
    pa.field("f64", pa.float64()),
])
literals = catalog.create_table("silver.literals", schema=literals_schema)
literals.append(pa.table({
    "id": pa.array([1, 2, 3, 4, 5], pa.int64()),
    "ntext": pa.array(["999", "1000", "50", "-5", "7"]),
    "btext": pa.array(["1", "true", "0", "false", None]),
    "f64": pa.array([-0.0, 0.0, 1.5, -1.5, None], pa.float64()),
}, schema=literals_schema))
literals = catalog.load_table("silver.literals")
print("literals table_location:", literals.location())
