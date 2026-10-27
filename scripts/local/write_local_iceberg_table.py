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
- `silver.cells`: one column of every physical type the R2RML materializer
  renders (int, long, float, double, decimal, boolean, date, timestamp,
  timestamptz, binary, string), in two files, with values whose lexical order
  differs from their natural order, duplicates to group on, and a row that is
  null in every typed column.
"""
import shutil, sys
from datetime import date, datetime, timezone
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
# silver.cells: every physical column type, two snapshots (one file each).
# ---------------------------------------------------------------------------
# A column the mapping leaves un-annotated reads as an xsd:string of its
# rendered value, so grouping and ordering go by that string: "10" sorts
# before "9", -0.0 and 0.0 are different strings. The second file holds only
# integers of two or more digits, so a file-level prune by the column's
# numeric bounds (a scan-side top-k, a pushed comparison) reaches a different
# answer from the string comparison.
cells_schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("grp", pa.string()),
    pa.field("i32", pa.int32()),
    pa.field("i64", pa.int64()),
    pa.field("f32", pa.float32()),
    pa.field("f64", pa.float64()),
    pa.field("dec", pa.decimal128(38, 9)),
    pa.field("flag", pa.bool_()),
    pa.field("day", pa.date32()),
    pa.field("ts", pa.timestamp("us")),
    pa.field("tstz", pa.timestamp("us", tz="UTC")),
    pa.field("raw", pa.binary()),
    pa.field("label", pa.string()),
])
cells = catalog.create_table("silver.cells", schema=cells_schema)
utc = timezone.utc
cells_rows = [
    # id, grp, i32, i64, f32, f64, dec, flag, day, ts (UTC wall clock), raw, label
    (1, "x", 2, 2, 1.5, 1.5, "0.05", True, date(2024, 1, 9),
     datetime(2024, 1, 1), b"\x00\x01", "b"),
    (2, "y", 9, 9, 0.1, 0.1, "1.5", False, date(2024, 1, 10),
     datetime(2024, 1, 1, 0, 0, 0, 1), b"\xff", "a"),
    (3, "x", -3, -3, -0.0, -0.0, "-0.5", True, date(1969, 12, 31),
     datetime(1969, 12, 31, 23, 59, 59, 999999), b"", "c"),
    (4, "y", 9, 9, 0.0, 0.0, "10", True, date(2024, 1, 9),
     datetime(2024, 1, 1), b"\x00\x01", "b"),
    (5, "x", 10, 10, 10.0, 10.0, "0.05", False, date(2000, 2, 29),
     datetime(2000, 2, 29, 12, 30), b"\x0a", "B"),
    (6, "y", 100, 9007199254740993, 2.5, float("nan"), "2", True, date(1970, 1, 1),
     datetime(1970, 1, 1), b"\xff", "\u00e9"),
    (7, "x", 10, 10, 1.5, 1e21, "-0.5", False, date(2024, 1, 10),
     datetime(2024, 1, 1, 0, 0, 0, 500000), b"\x00\x01", "a"),
    (8, "y", None, None, None, None, None, None, None, None, None, "z"),
]

def cells_batch(rows):
    col = lambda i: [r[i] for r in rows]
    return pa.table({
        "id": pa.array(col(0), pa.int64()),
        "grp": pa.array(col(1)),
        "i32": pa.array(col(2), pa.int32()),
        "i64": pa.array(col(3), pa.int64()),
        "f32": pa.array(col(4), pa.float32()),
        "f64": pa.array(col(5), pa.float64()),
        "dec": pa.array([None if v is None else Decimal(v) for v in col(6)],
                        pa.decimal128(38, 9)),
        "flag": pa.array(col(7)),
        "day": pa.array(col(8), pa.date32()),
        "ts": pa.array(col(9), pa.timestamp("us")),
        "tstz": pa.array([None if v is None else v.replace(tzinfo=utc) for v in col(9)],
                         pa.timestamp("us", tz="UTC")),
        "raw": pa.array(col(10), pa.binary()),
        "label": pa.array(col(11)),
    }, schema=cells_schema)

cells.append(cells_batch(cells_rows[:4]))
cells.append(cells_batch(cells_rows[4:]))
cells = catalog.load_table("silver.cells")
print("cells table_location:", cells.location())
