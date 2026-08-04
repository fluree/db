#!/usr/bin/env python3
"""Wave-B probe loader: write ONE fact table PARTITIONED BY <date> onto the substrate-B
MinIO/REST fixture. Two partition specs, selected by PART_SPEC, matching the two tables
RESULTS.md's Wave B reports:

  derived    (default) -> namespace DW_SF01_PART   : IDENTITY partition on a derived integer
             date bucket (yyyy / yyyymm / yyyymmdd) computed here. Needs NO pyiceberg_core.
  transform            -> namespace DW_SF01_PART_T : a GENUINE iceberg {year,month,day}
             transform partition spec on the raw date column. This is Wave B's month-transform
             table (DW_SF01_PART_T, spec month(<col>)). Its write path needs pyiceberg's
             transform-partitioned write support (pyiceberg >= 0.7; the pyiceberg_core extra
             accelerates it) -- guarded below with an actionable message if unavailable.

Both land a real multi-file partitioned layout (one data file per partition value), so Wave B
compares an identity-bucket layout AND a genuine month-transform layout against DuckDB.

Why the probe: substrate A (Snowflake-managed S3) stores the FACT tables PARTITION BY(date) as
many small per-partition files, and DuckDB's iceberg reader could not scan them (issue #1568,
Finding 1) while the bloom-filter JOIN gap (#1568, Finding 2) also showed up there. A LOCALLY
partitioned table disambiguates: if partitioned-LOCAL scans fine, Finding 1 is
Snowflake-managed-S3-specific; whether the bloom error appears on partitioned-local (identity
AND genuine transform) tells us if Finding 2's trigger is partitioning vs remoteness.

Config via env:
  SF_PARQUET_SRC  directory holding <table>/data_0.parquet  (REQUIRED)
  PART_TABLE      source table (default fact_web_event)
  PART_COL        DATE column to partition on (default EVENT_DATE)
  PART_GRAIN      year | month | day  (default month)
  PART_SPEC       derived | transform  (default derived)
  REST_URI/S3_ENDPOINT/S3_KEY/S3_SECRET  as in load_tables.py

Run with a python that has pyiceberg[s3fs] + pyarrow (transform mode also wants pyiceberg_core):
  SF_PARQUET_SRC=/path/to/output-sf01 python load_tables_partitioned.py
  SF_PARQUET_SRC=/path/to/output-sf01 PART_SPEC=transform python load_tables_partitioned.py
"""
import os
import sys
import pyarrow.parquet as pq
import pyarrow.compute as pc
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.transforms import (IdentityTransform, YearTransform, MonthTransform,
                                  DayTransform)

SRC = os.environ.get("SF_PARQUET_SRC")
if not SRC:
    sys.exit("set SF_PARQUET_SRC to the directory holding <table>/data_0.parquet")
TABLE = os.environ.get("PART_TABLE", "fact_web_event")
PART_COL = os.environ.get("PART_COL", "EVENT_DATE").upper()
GRAIN = os.environ.get("PART_GRAIN", "month")
SPEC = os.environ.get("PART_SPEC", "derived")
if GRAIN not in ("year", "month", "day"):
    sys.exit("PART_GRAIN must be year|month|day (got %r)" % GRAIN)
if SPEC not in ("derived", "transform"):
    sys.exit("PART_SPEC must be derived|transform (got %r)" % SPEC)
NS = "DW_SF01_PART" if SPEC == "derived" else "DW_SF01_PART_T"

catalog = RestCatalog("sbp", **{
    "uri": os.environ.get("REST_URI", "http://localhost:8181"),
    "warehouse": "s3://warehouse/",
    "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://localhost:9000"),
    "s3.access-key-id": os.environ.get("S3_KEY", "minioadmin"),
    "s3.secret-access-key": os.environ.get("S3_SECRET", "minioadmin"),
    "s3.path-style-access": "true",
    "s3.region": "us-east-1",
})

try:
    catalog.create_namespace_if_not_exists(NS)
except Exception as e:
    print("namespace:", e)

ident = f"{NS}.{TABLE.upper()}"
at = pq.read_table(f"{SRC}/{TABLE}/data_0.parquet")
at = at.rename_columns([c.upper() for c in at.column_names])

try:
    catalog.drop_table(ident)
except Exception:
    pass

if SPEC == "derived":
    # Derived integer bucket from the date column -> IDENTITY partition (no pyiceberg_core).
    d = at.column(PART_COL)
    y = pc.cast(pc.year(d), "int32")
    if GRAIN == "year":
        bucket = y
    elif GRAIN == "day":
        bucket = pc.add(pc.add(pc.multiply(y, 10000),
                               pc.multiply(pc.cast(pc.month(d), "int32"), 100)),
                        pc.cast(pc.day(d), "int32"))
    else:  # month
        bucket = pc.add(pc.multiply(y, 100), pc.cast(pc.month(d), "int32"))
    bucket_col = f"{PART_COL}_BUCKET"
    at = at.append_column(bucket_col, pc.cast(bucket, "int32"))
    tbl = catalog.create_table(ident, schema=at.schema)
    # Create empty, add the identity partition field (allowed on an empty table), reload, then
    # append so the data lands partitioned into one file per bucket (mirroring the A layout).
    with tbl.update_spec() as u:
        u.add_field(bucket_col, IdentityTransform(), f"{bucket_col.lower()}_ident")
else:
    # GENUINE iceberg transform partition on the RAW date column (no derived column).
    transform = {"year": YearTransform(), "month": MonthTransform(),
                 "day": DayTransform()}[GRAIN]
    tbl = catalog.create_table(ident, schema=at.schema)
    with tbl.update_spec() as u:
        u.add_field(PART_COL, transform, f"{PART_COL.lower()}_{GRAIN}")

tbl = catalog.load_table(ident)
try:
    tbl.append(at)
except Exception as e:
    if SPEC == "transform":
        sys.exit("transform-partitioned append failed (%s).\nThe genuine {year,month,day} "
                 "transform write path needs pyiceberg >= 0.7 with the pyiceberg_core extra: "
                 "pip install 'pyiceberg[s3fs]' pyiceberg-core" % e)
    raise

n = tbl.scan().to_arrow().num_rows
nfiles = sum(1 for _ in tbl.scan().plan_files())
print(f"  {ident:<34} rows={n}  data_files={nfiles}  spec={SPEC}  grain={GRAIN}")
print(f"  partition spec: {tbl.spec()}")
print("DONE: partitioned load complete")
