#!/usr/bin/env python3
"""Wave-B probe loader: write ONE fact table PARTITIONED BY <date bucket> onto the same
substrate-B MinIO/REST fixture, into namespace DW_SF01_PART.

Why: substrate A (Snowflake-managed S3) stores the FACT tables PARTITION BY(date) as many
small per-partition parquet files, and DuckDB's iceberg reader could not scan them (issue
#1568, Finding 1) while the bloom-filter JOIN gap (#1568, Finding 2) also showed up there.
A LOCALLY partitioned table disambiguates: if partitioned-LOCAL scans fine, Finding 1 is
Snowflake-managed-S3-specific; whether the bloom error appears/absent on partitioned-local
tells us if Finding 2's trigger is partitioning vs remoteness.

Partitioning is IDENTITY on a derived integer bucket (yyyy / yyyymm / yyyymmdd) computed
here, so it needs NO pyiceberg_core Rust extra (the built-in Month/Day/Year transforms do).
The result is a genuine multi-file partitioned layout (one data file per bucket).

Config via env:
  SF_PARQUET_SRC  directory holding <table>/data_0.parquet  (REQUIRED)
  PART_TABLE      source table (default fact_web_event)
  PART_COL        DATE column to bucket on (default EVENT_DATE)
  PART_GRAIN      month | day | year  (default month)
  REST_URI/S3_ENDPOINT/S3_KEY/S3_SECRET  as in load_tables.py

Run with a python that has pyiceberg[s3fs] + pyarrow:
  SF_PARQUET_SRC=/path/to/output-sf01 python load_tables_partitioned.py
"""
import os
import sys
import pyarrow.parquet as pq
import pyarrow.compute as pc
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.transforms import IdentityTransform

SRC = os.environ.get("SF_PARQUET_SRC")
if not SRC:
    sys.exit("set SF_PARQUET_SRC to the directory holding <table>/data_0.parquet")
NS = "DW_SF01_PART"
TABLE = os.environ.get("PART_TABLE", "fact_web_event")
PART_COL = os.environ.get("PART_COL", "EVENT_DATE").upper()
GRAIN = os.environ.get("PART_GRAIN", "month")
BUCKET_COL = f"{PART_COL}_BUCKET"

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
at = at.append_column(BUCKET_COL, pc.cast(bucket, "int32"))

try:
    catalog.drop_table(ident)
except Exception:
    pass

# Create empty, add the identity partition field (allowed on an empty table), reload, then
# append so the data lands partitioned into one file per bucket (mirroring the A layout).
tbl = catalog.create_table(ident, schema=at.schema)
with tbl.update_spec() as u:
    u.add_field(BUCKET_COL, IdentityTransform(), f"{BUCKET_COL.lower()}_ident")
tbl = catalog.load_table(ident)
tbl.append(at)

n = tbl.scan().to_arrow().num_rows
nfiles = sum(1 for _ in tbl.scan().plan_files())
print(f"  {ident:<32} rows={n}  data_files={nfiles}  grain={GRAIN}  spec={tbl.spec()}")
print("DONE: partitioned load complete")
