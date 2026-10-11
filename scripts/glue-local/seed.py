"""Seed Iceberg tables through a (mocked) AWS Glue Data Catalog.

pyiceberg's GlueCatalog writes tables the way Glue-integrated writers do:
  - metadata JSON + manifests + Parquet in S3,
  - the current metadata file recorded ONLY in the Glue table's `metadata_location` parameter,
  - no `metadata/version-hint.text` (that is a Hadoop-catalog convention).

Tables (namespace `demo`):
  customers         50 rows, 2 commits, no version hint          -> the case real Glue produces
  orders           200 rows, 3 commits (2 appends), no hint      -> join partner; newest metadata must win
  customers_hinted  50 rows, + a version-hint.text (control)     -> isolates "missing hint" as the variable
  orders_orphan    100 rows, + an uncommitted higher-numbered metadata file planted in metadata/
                   -> the catalog pointer (Glue) is authoritative; "highest numbered file" is a heuristic

Writes a manifest (expected counts + locations) to argv[1] for check.sh.

With `--export-fixture DIR`, also writes the fixture fluree-db-api's `it_iceberg_glue_moto` test
replays into a fresh moto: every S3 object of the Glue-mode tables under DIR/objects/<key>, and
their Glue registrations in DIR/glue-tables.json. Regenerate it with
`scripts/glue-local/write_fixture.sh` (see README.md).
"""
import argparse
import datetime as dt
import json
import os
import pathlib
import uuid

import boto3
import pyarrow as pa
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import DateType, DoubleType, LongType, NestedField, StringType, TimestampType

args = argparse.ArgumentParser(description=__doc__.splitlines()[0])
args.add_argument("manifest", help="where to write the manifest check.sh reads")
args.add_argument("--export-fixture", metavar="DIR", help="also export the Glue-mode tables here")
args = args.parse_args()

EP = os.environ["MOTO_ENDPOINT"]
BUCKET = os.environ["BUCKET"]
DB = os.environ["GLUE_DB"]
WAREHOUSE = os.environ["WAREHOUSE"]
REGION = os.environ["AWS_REGION"]
CREDS = dict(aws_access_key_id="test", aws_secret_access_key="test", region_name=REGION)

s3 = boto3.client("s3", endpoint_url=EP, **CREDS)
glue = boto3.client("glue", endpoint_url=EP, **CREDS)
s3.create_bucket(Bucket=BUCKET)

catalog = GlueCatalog(
    "glue",
    **{
        "glue.endpoint": EP,
        "glue.region": REGION,
        "glue.access-key-id": "test",
        "glue.secret-access-key": "test",
        "s3.endpoint": EP,
        "s3.region": REGION,
        "s3.access-key-id": "test",
        "s3.secret-access-key": "test",
        "warehouse": WAREHOUSE,
    },
)
catalog.create_namespace(DB, {"location": f"{WAREHOUSE}/{DB}.db"})

CUSTOMERS = Schema(
    NestedField(1, "customer_id", LongType(), required=True),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "country", StringType(), required=False),
    NestedField(4, "birth_date", DateType(), required=False),
    identifier_field_ids=[1],
)
ORDERS = Schema(
    NestedField(1, "order_id", LongType(), required=True),
    NestedField(2, "customer_id", LongType(), required=False),
    NestedField(3, "amount", DoubleType(), required=False),
    NestedField(4, "ordered_at", TimestampType(), required=False),
    identifier_field_ids=[1],
)
COUNTRIES = ["US", "CA", "PT", "GB", "DE"]


def customers_batch(n=50):
    return pa.Table.from_pylist(
        [
            {
                "customer_id": i,
                "name": f"Customer {i:03d}",
                "country": COUNTRIES[i % len(COUNTRIES)],
                "birth_date": dt.date(1950 + i % 50, 1 + i % 12, 1 + i % 28),
            }
            for i in range(1, n + 1)
        ],
        schema=CUSTOMERS.as_arrow(),
    )


def orders_batch(start, n):
    return pa.Table.from_pylist(
        [
            {
                "order_id": i,
                "customer_id": 1 + (i % 50),
                "amount": round(10 + (i * 7.3) % 990, 2),
                "ordered_at": dt.datetime(2026, 1, 1) + dt.timedelta(hours=i),
            }
            for i in range(start, start + n)
        ],
        schema=ORDERS.as_arrow(),
    )


def make(name, schema, batches):
    t = catalog.create_table(f"{DB}.{name}", schema=schema)
    for b in batches:
        t.append(b)
    return catalog.load_table(f"{DB}.{name}")


def metadata_files(location):
    prefix = location.split(f"s3://{BUCKET}/", 1)[1].rstrip("/") + "/metadata/"
    out = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    return sorted(o["Key"].rsplit("/", 1)[1] for o in out.get("Contents", []) if o["Key"].endswith(".metadata.json"))


tables = {}
for name, schema, batches, rows in [
    ("customers", CUSTOMERS, [customers_batch()], 50),
    ("orders", ORDERS, [orders_batch(1, 100), orders_batch(101, 100)], 200),
    ("customers_hinted", CUSTOMERS, [customers_batch()], 50),
    ("orders_orphan", ORDERS, [orders_batch(1, 100)], 100),
]:
    t = make(name, schema, batches)
    tables[name] = {"rows": rows, "location": t.location(), "metadata_location": t.metadata_location}

# Control: write the Hadoop-convention hint so direct mode can resolve this table.
hinted = tables["customers_hinted"]
hint_key = hinted["location"].split(f"s3://{BUCKET}/", 1)[1] + "/metadata/version-hint.text"
s3.put_object(Bucket=BUCKET, Key=hint_key, Body=hinted["metadata_location"].rsplit("/", 1)[1].encode())
hinted["version_hint"] = f"s3://{BUCKET}/{hint_key}"

# Orphan: plant an uncommitted, higher-numbered metadata file (a copy of the table's first, empty
# metadata). Glue still points at the committed file; a "pick the highest number" listing would not.
orph = tables["orders_orphan"]
files = metadata_files(orph["location"])
first_key = orph["location"].split(f"s3://{BUCKET}/", 1)[1] + "/metadata/" + files[0]
orphan_name = f"00099-{uuid.uuid4()}.metadata.json"
s3.copy_object(Bucket=BUCKET, Key=first_key.rsplit("/", 1)[0] + "/" + orphan_name,
               CopySource={"Bucket": BUCKET, "Key": first_key})
orph["orphan_metadata_file"] = orphan_name
orph["rows_if_orphan_is_read"] = 0

# Negative case: a Glue table that is NOT Iceberg (a plain Hive/Parquet registration with no
# `table_type=ICEBERG` and no `metadata_location`). A Glue-mode reader must fail clearly on it.
glue.create_table(
    DatabaseName=DB,
    TableInput={
        "Name": "hive_table",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet"},
        "StorageDescriptor": {
            "Columns": [{"Name": "id", "Type": "bigint"}],
            "Location": f"{WAREHOUSE}/{DB}.db/hive_table",
        },
    },
)

for name, t in tables.items():
    t["metadata_files"] = metadata_files(t["location"])
    g = glue.get_table(DatabaseName=DB, Name=name)["Table"]
    t["glue_parameters"] = {k: v for k, v in g.get("Parameters", {}).items() if k in ("table_type", "metadata_location")}
    t["has_version_hint"] = "version_hint" in t

manifest = {"endpoint": EP, "bucket": BUCKET, "glue_database": DB, "warehouse": WAREHOUSE, "tables": tables}
with open(args.manifest, "w") as f:
    json.dump(manifest, f, indent=2)

for name, t in tables.items():
    print(f"{DB}.{name:17s} rows={t['rows']:4d} hint={'yes' if t['has_version_hint'] else 'no ':3s} "
          f"metadata files={len(t['metadata_files'])} current={t['metadata_location'].rsplit('/', 1)[1][:12]}…")
print(f"manifest -> {args.manifest}")

if args.export_fixture:
    # The tables a Glue-mode reader is tested against (the hinted control is direct-mode only).
    out = pathlib.Path(args.export_fixture)
    exported = []
    for name in ("customers", "orders", "orders_orphan"):
        t = tables[name]
        prefix = t["location"].split(f"s3://{BUCKET}/", 1)[1].rstrip("/") + "/"
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                dest = out / "objects" / obj["Key"]
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read())
        g = glue.get_table(DatabaseName=DB, Name=name)["Table"]
        exported.append({"name": name, "rows": t["rows"], "location": t["location"],
                         "table_type": g.get("TableType", "EXTERNAL_TABLE"),
                         "parameters": g.get("Parameters", {})})
    h = glue.get_table(DatabaseName=DB, Name="hive_table")["Table"]
    fixture = {
        "bucket": BUCKET, "database": DB, "warehouse": WAREHOUSE, "tables": exported,
        "non_iceberg_tables": [{"name": "hive_table", "location": h["StorageDescriptor"]["Location"],
                                "table_type": h["TableType"], "parameters": h.get("Parameters", {})}],
        "orphan_metadata_file": tables["orders_orphan"]["orphan_metadata_file"],
    }
    (out / "glue-tables.json").write_text(json.dumps(fixture, indent=2, sort_keys=True) + "\n")
    print(f"fixture -> {out}")
