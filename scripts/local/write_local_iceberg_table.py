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
- `silver.cells`: one column of every physical type the R2RML materializer
  renders (int, long, float, double, decimal, boolean, date, timestamp,
  timestamptz, binary, string, and numeric text), in two files, with values whose lexical order
  differs from their natural order, duplicates to group on, and a row that is
  null in every typed column.
- `silver.renamed`: two snapshots across a column rename: three rows with
  `id`, `name`; then `name` renamed to `full_name`; then two more rows. A
  read pinned to the first snapshot must find its rows under the current
  name.
- `silver.customers_history`: a history table for a filtered logical table
  (`rr:sqlQuery ... WHERE is_current = 1 AND is_deleted = FALSE`), in two
  files: customer ids repeat across versions, one current row is soft-deleted,
  one row's `is_current` is null, and a non-current row holds the largest
  balance, so a reader that ignored the filter would answer differently.
- `silver.orders`: orders referencing `customers_history.customer_id`,
  including a current customer, a deleted one, one with no current row, and a
  dangling id.
- `sets.customers_history`, `sets.customers_current`, `sets.orders`: tables
  whose rows mint duplicate triples, for R2RML set semantics (their own
  namespace; see the block's comment).
- `sets.parents_dup`: a table whose `code` and `tier` columns repeat across
  rows that mint different subjects (`id` is unique), in two files, with a
  null in each. A mapping joining it to itself on `code` gives a
  referencing object map whose parent key matches several parent subjects
  (R2RML fan-out); a parent map minting one subject per `code` gives the
  history shape (a repeated key, one subject).
- `sets.order_lines`: order lines in two files, the first holding order 42's
  amount twice (one triple, two rows) above every amount in the second, so a
  scan-side top-k that counted rows rather than triples would stop before
  the second file.
- `sets.customers_us`, `sets.customers_eu`: two tables, each keyed by `id`,
  minting customers with one template; customer 7 has a row in each, so a
  same-subject star over the two pairs each name with each city.
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

# ---------------------------------------------------------------------------
# silver.cells: every physical column type, two snapshots (one file each).
# ---------------------------------------------------------------------------
# A column the mapping leaves un-annotated reads as an xsd:string of its
# rendered value, so grouping and ordering go by that string: "10" sorts
# before "9", -0.0 and 0.0 are different strings. The second file holds only
# integers of two or more digits, so a file-level prune by the column's
# numeric bounds (a scan-side top-k, a pushed comparison) reaches a different
# answer from the string comparison. `ntext` holds `i32`'s values as text, for
# a mapping that declares a numeric datatype over a string column: its terms
# order by number, its file bounds by string ("9" above "100").
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
    pa.field("ntext", pa.string()),
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
        "ntext": pa.array([None if v is None else str(v) for v in col(2)]),
    }, schema=cells_schema)

cells.append(cells_batch(cells_rows[:4]))
cells.append(cells_batch(cells_rows[4:]))
cells = catalog.load_table("silver.cells")
print("cells table_location:", cells.location())

# ---------------------------------------------------------------------------
# silver.renamed: a column renamed between two snapshots.
# ---------------------------------------------------------------------------
renamed = catalog.create_table("silver.renamed", schema=pa.schema([
    pa.field("id", pa.int64()),
    pa.field("name", pa.string()),
]))
renamed.append(pa.table({
    "id": pa.array([1, 2, 3], pa.int64()),
    "name": pa.array(["alice", "bob", "carol"]),
}))
with renamed.update_schema() as update:
    update.rename_column("name", "full_name")
renamed = catalog.load_table("silver.renamed")
renamed.append(pa.table({
    "id": pa.array([4, 5], pa.int64()),
    "full_name": pa.array(["dave", "erin"]),
}))
renamed = catalog.load_table("silver.renamed")
print("renamed table_location:", renamed.location())
print("renamed snapshots:", [
    (s.snapshot_id, s.sequence_number, s.schema_id)
    for s in sorted(renamed.snapshots(), key=lambda s: s.sequence_number)
])

# ---------------------------------------------------------------------------
# sets.*: rows that mint the same triple more than once.
#
# An R2RML graph source answers over a SET of triples, and these tables are
# bags whose rows collide, in two files each so a duplicate can straddle them.
# Their own namespace keeps them apart from the silver.* tables other suites
# own.
#
# - customers_history(customer_id, name, city, valid_from, is_current): no key
#   covers customer_id alone. Customer 1 has three versions, two of them with
#   the same name and city; customer 2 bob, then robert twice with the same
#   values; customer 5 two versions differing in BOTH name and city, so a
#   per-subject cross product (erin x Tampa) differs from the row-correlated
#   pairs; customer 4 a NULL city; one row a NULL customer_id (no subject).
#   Exactly one row per customer has is_current = 1.
# - customers_current(customer_id, name): one row per customer, customer_id an
#   Iceberg identifier field; minted with the same template and class as the
#   history table, plus customer 6 the history lacks.
# - orders(order_id, customer_id, amount): references the history table on
#   customer_id (a parent key holding several rows that mint ONE subject);
#   order 101 is an exact duplicate row, one copy in each file; order 104's
#   customer dangles; order 105's is NULL.
# ---------------------------------------------------------------------------
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

catalog.create_namespace("sets")

sets_history_schema = pa.schema([
    pa.field("customer_id", pa.int64()),
    pa.field("name", pa.string()),
    pa.field("city", pa.string()),
    pa.field("valid_from", pa.date32()),
    pa.field("is_current", pa.int32()),
])
sets_history = catalog.create_table("sets.customers_history", schema=sets_history_schema)
sets_history_rows = [
    # customer_id, name, city, valid_from, is_current
    (1, "alice", "Boston", date(2024, 1, 1), 0),
    (2, "bob", "Austin", date(2024, 1, 1), 0),
    (3, "carol", "Chicago", date(2024, 2, 1), 1),
    (1, "alice", "Denver", date(2024, 6, 1), 0),
    (5, "erin", "Miami", date(2024, 1, 1), 0),
    # second file
    (2, "robert", "Austin", date(2024, 5, 1), 0),
    (2, "robert", "Austin", date(2024, 9, 1), 1),
    (4, "dave", None, date(2024, 3, 1), 1),
    (1, "alice", "Denver", date(2024, 11, 1), 1),
    (5, "erin-m", "Tampa", date(2024, 7, 1), 1),
    (None, "ghost", "Nowhere", date(2024, 1, 1), 0),
]

def sets_history_batch(rows):
    col = lambda i: [r[i] for r in rows]
    return pa.table({
        "customer_id": pa.array(col(0), pa.int64()),
        "name": pa.array(col(1)),
        "city": pa.array(col(2)),
        "valid_from": pa.array(col(3), pa.date32()),
        "is_current": pa.array(col(4), pa.int32()),
    }, schema=sets_history_schema)

sets_history.append(sets_history_batch(sets_history_rows[:5]))
sets_history.append(sets_history_batch(sets_history_rows[5:]))
sets_history = catalog.load_table("sets.customers_history")
print("sets.customers_history table_location:", sets_history.location())

sets_current = catalog.create_table("sets.customers_current", schema=Schema(
    NestedField(1, "customer_id", LongType(), required=True),
    NestedField(2, "name", StringType(), required=False),
    identifier_field_ids=[1],
))
sets_current_arrow = pa.schema([
    pa.field("customer_id", pa.int64(), nullable=False),
    pa.field("name", pa.string()),
])
sets_current.append(pa.table({
    "customer_id": pa.array([1, 2, 3], pa.int64()),
    "name": pa.array(["alice", "robert", "carol"]),
}, schema=sets_current_arrow))
sets_current.append(pa.table({
    "customer_id": pa.array([4, 5, 6], pa.int64()),
    "name": pa.array(["dave", "erin-m", "frank"]),
}, schema=sets_current_arrow))
sets_current = catalog.load_table("sets.customers_current")
print("sets.customers_current table_location:", sets_current.location())
print("sets.customers_current identifier fields:", sets_current.schema().identifier_field_names())

sets_orders_schema = pa.schema([
    pa.field("order_id", pa.int64()),
    pa.field("customer_id", pa.int64()),
    pa.field("amount", pa.decimal128(10, 2)),
])
sets_orders = catalog.create_table("sets.orders", schema=sets_orders_schema)

def sets_orders_batch(rows):
    return pa.table({
        "order_id": pa.array([r[0] for r in rows], pa.int64()),
        "customer_id": pa.array([r[1] for r in rows], pa.int64()),
        "amount": pa.array([Decimal(r[2]) for r in rows], pa.decimal128(10, 2)),
    }, schema=sets_orders_schema)

sets_orders.append(sets_orders_batch([
    (100, 1, "10.00"), (101, 2, "20.00"), (102, 3, "5.50"), (103, 5, "7.25"),
]))
sets_orders.append(sets_orders_batch([
    (101, 2, "20.00"), (104, 99, "1.00"), (105, None, "3.00"), (106, 1, "2.00"),
]))
sets_orders = catalog.load_table("sets.orders")
print("sets.orders table_location:", sets_orders.location())

# ---------------------------------------------------------------------------
# sets.parents_dup: repeated join keys minting different subjects (fan-out).
# ---------------------------------------------------------------------------
# `id` is unique; `code` repeats (A twice, C three times, across both files),
# as does `tier`; row 7 has no `code`, row 8 no `tier`. Self-contained: it
# creates the `sets` namespace if no other block has.
from pyiceberg.exceptions import NamespaceAlreadyExistsError

try:
    catalog.create_namespace("sets")
except NamespaceAlreadyExistsError:
    pass
parents_dup_schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("code", pa.string()),
    pa.field("tier", pa.string()),
    pa.field("name", pa.string()),
])
parents_dup = catalog.create_table("sets.parents_dup", schema=parents_dup_schema)
parents_dup_rows = [
    (1, "A", "gold", "alpha"),
    (2, "A", "silver", "bravo"),
    (3, "B", "gold", "charlie"),
    (4, "C", "silver", "delta"),
    (5, "C", "gold", "echo"),
    (6, "C", "silver", "foxtrot"),
    (7, None, "gold", "golf"),
    (8, "D", None, "hotel"),
]

def parents_dup_batch(rows):
    col = lambda i: [r[i] for r in rows]
    return pa.table({
        "id": pa.array(col(0), pa.int64()),
        "code": pa.array(col(1), pa.string()),
        "tier": pa.array(col(2), pa.string()),
        "name": pa.array(col(3), pa.string()),
    }, schema=parents_dup_schema)

parents_dup.append(parents_dup_batch(parents_dup_rows[:4]))
parents_dup.append(parents_dup_batch(parents_dup_rows[4:]))
parents_dup = catalog.load_table("sets.parents_dup")
print("parents_dup table_location:", parents_dup.location())

# ---------------------------------------------------------------------------
# sets.order_lines: a repeated triple above the rest of the top k.
# ---------------------------------------------------------------------------
# `line_id` is unique; `order_id` is not. The first file holds two lines of
# order 42 with the same amount: mapped per order they mint ONE triple, so
# the top two amounts per order are 100 (order 42) and 50 (order 43), the
# second in the other file, whose upper bound (50) sits below the first
# file's two rows. Mapped per line, every row is its own triple. The
# `sets` namespace exists by now (created above).
order_lines_schema = pa.schema([
    pa.field("line_id", pa.int64()),
    pa.field("order_id", pa.int64()),
    pa.field("amount", pa.int64()),
])
order_lines = catalog.create_table("sets.order_lines", schema=order_lines_schema)

def order_lines_batch(rows):
    col = lambda i: [r[i] for r in rows]
    return pa.table({
        "line_id": pa.array(col(0), pa.int64()),
        "order_id": pa.array(col(1), pa.int64()),
        "amount": pa.array(col(2), pa.int64()),
    }, schema=order_lines_schema)

order_lines.append(order_lines_batch([(1, 42, 100), (2, 42, 100)]))
order_lines.append(order_lines_batch([(3, 43, 50), (4, 44, 40)]))
order_lines = catalog.load_table("sets.order_lines")
print("sets.order_lines table_location:", order_lines.location())

# ---------------------------------------------------------------------------
# sets.customers_us + sets.customers_eu: one subject, a row in each table.
# ---------------------------------------------------------------------------
# Each table holds one row per `id` (a key of each), and both mint
# customer/{id}: customer 7 is in both, with a different name and city in
# each, so its star pairs every name with every city across the two rows.
regional_schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("name", pa.string()),
    pa.field("city", pa.string()),
])
for table_name, rows in [
    ("sets.customers_us", [(7, "Ada", "Miami"), (8, "Bo", "Austin")]),
    ("sets.customers_eu", [(7, "Ada L.", "Lyon"), (9, "Cy", "Paris")]),
]:
    regional = catalog.create_table(table_name, schema=regional_schema)
    regional.append(pa.table({
        "id": pa.array([r[0] for r in rows], pa.int64()),
        "name": pa.array([r[1] for r in rows]),
        "city": pa.array([r[2] for r in rows]),
    }, schema=regional_schema))
    regional = catalog.load_table(table_name)
    print(table_name, "table_location:", regional.location())

# ---------------------------------------------------------------------------
# silver.customers_history + silver.orders: a filtered logical table and a
# reference into it.
# ---------------------------------------------------------------------------
history_schema = pa.schema([
    pa.field("customer_id", pa.int64()),
    pa.field("name", pa.string()),
    pa.field("valid_from", pa.date32()),
    pa.field("is_current", pa.int32()),
    pa.field("is_deleted", pa.bool_()),
    pa.field("tier", pa.string()),
    pa.field("balance", pa.decimal128(10, 2)),
    pa.field("score", pa.float64()),
])
history = catalog.create_table("silver.customers_history", schema=history_schema)
history_rows = [
    # customer_id, name, valid_from, is_current, is_deleted, tier, balance, score
    (1, "alice-v1", date(2024, 1, 1), 0, False, "gold", "10.00", 1.5),
    (1, "alice", date(2024, 6, 1), 1, False, "gold", "25.50", 2.5),
    (2, "bob-v1", date(2024, 1, 1), 0, False, "silver", "5.00", float("nan")),
    (3, "carol", date(2024, 2, 1), 1, True, "bronze", "0.00", -0.0),
    (2, "bob", date(2024, 7, 1), 1, False, "silver", "7.25", 9.0),
    (4, "dave", date(2024, 3, 1), 1, False, None, "100.00", None),
    (5, "erin-v1", date(2024, 1, 1), 0, False, "gold", "500.00", 3.0),
    (5, "erin", date(2024, 8, 1), None, False, "gold", "2.00", 4.0),
]

def history_batch(rows):
    col = lambda i: [r[i] for r in rows]
    return pa.table({
        "customer_id": pa.array(col(0), pa.int64()),
        "name": pa.array(col(1)),
        "valid_from": pa.array(col(2), pa.date32()),
        "is_current": pa.array(col(3), pa.int32()),
        "is_deleted": pa.array(col(4)),
        "tier": pa.array(col(5)),
        "balance": pa.array([Decimal(v) for v in col(6)], pa.decimal128(10, 2)),
        "score": pa.array(col(7), pa.float64()),
    }, schema=history_schema)

history.append(history_batch(history_rows[:4]))
history.append(history_batch(history_rows[4:]))
history = catalog.load_table("silver.customers_history")
print("customers_history table_location:", history.location())

orders_schema = pa.schema([
    pa.field("order_id", pa.int64()),
    pa.field("customer_id", pa.int64()),
    pa.field("total", pa.decimal128(10, 2)),
])
orders = catalog.create_table("silver.orders", schema=orders_schema)
orders.append(pa.table({
    "order_id": pa.array([100, 101, 102, 103, 104, 105], pa.int64()),
    # 1, 2: current customers; 3: soft-deleted; 5: no current row; 99: dangling.
    "customer_id": pa.array([1, 2, 3, 5, 1, 99], pa.int64()),
    "total": pa.array([Decimal(v) for v in [
        "9.99", "20.00", "5.00", "1.00", "3.50", "2.00",
    ]], pa.decimal128(10, 2)),
}, schema=orders_schema))
orders = catalog.load_table("silver.orders")
print("orders table_location:", orders.location())
