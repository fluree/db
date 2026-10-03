#!/usr/bin/env python3
"""Load the 16 SF0.1 tables into the substrate-B REST catalog (MinIO-backed), UNPARTITIONED.

Reads the source parquet (one data_0.parquet per table) and writes each as an Iceberg
table under namespace DW_SF01 (UPPERCASE, to match the R2RML mapping's rr:tableName and
DuckDB's case-insensitive refs). Unpartitioned = one data file per table (the wave-1
layout; deliberately avoids the many-small-files fan-out that broke substrate A).

Config via env (no hard-coded local paths):
  SF_PARQUET_SRC  directory holding <table>/data_0.parquet  (REQUIRED)
  REST_URI        Iceberg REST catalog URI   (default http://localhost:8181)
  S3_ENDPOINT     MinIO S3 endpoint          (default http://localhost:9000)
  S3_KEY/S3_SECRET  MinIO creds              (default minioadmin/minioadmin)

Run with a python that has pyiceberg[s3fs] + pyarrow (e.g. the data-gen venv):
  SF_PARQUET_SRC=/path/to/output-sf01 python load_tables.py
"""
import os
import sys
import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog

SRC = os.environ.get("SF_PARQUET_SRC")
if not SRC:
    sys.exit("set SF_PARQUET_SRC to the directory holding <table>/data_0.parquet")
NS = "DW_SF01"
TABLES = ["dim_account", "dim_customer", "dim_date", "dim_employee", "dim_geography",
          "dim_product", "dim_store", "dim_supplier", "fact_gl_journal",
          "fact_inventory_snapshot", "fact_order", "fact_order_line", "fact_payment",
          "fact_shipment", "fact_support_ticket", "fact_web_event"]

catalog = RestCatalog("sb", **{
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

for t in TABLES:
    ident = f"{NS}.{t.upper()}"
    at = pq.read_table(f"{SRC}/{t}/data_0.parquet")
    # Snowflake folds identifiers UPPERCASE; the shared R2RML mapping references UPPERCASE
    # columns (ORDER_KEY). Uppercase columns here so the SAME mapping works unchanged on
    # substrate B (DuckDB is case-insensitive, so its SQL is fine either way).
    at = at.rename_columns([c.upper() for c in at.column_names])
    try:
        catalog.drop_table(ident)
    except Exception:
        pass
    tbl = catalog.create_table(ident, schema=at.schema)
    tbl.append(at)
    n = tbl.scan().to_arrow().num_rows
    print(f"  {ident:<32} rows={n}")

print("DONE: loaded", len(TABLES), "tables into", NS)
