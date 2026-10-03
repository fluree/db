#!/usr/bin/env python3
"""Emit CREATE ICEBERG TABLE DDL inferred from the generated Parquet schema.

Reading types straight from the Parquet output means the DDL can never drift from
the data. Writes per-table SQL to <out>/ddl/<table>.sql plus a combined
<out>/ddl/create_all.sql.

Standalone:
    python ddl.py --out ./output --external-volume iceberg_ext_vol
Imported by generate.py after a run.
"""
import argparse
import os
import re
import sys

# DuckDB type -> Snowflake type
_SIMPLE = {
    "BIGINT": "NUMBER(38,0)", "HUGEINT": "NUMBER(38,0)", "INTEGER": "NUMBER(38,0)",
    "SMALLINT": "NUMBER(38,0)", "TINYINT": "NUMBER(38,0)", "UBIGINT": "NUMBER(38,0)",
    "UINTEGER": "NUMBER(38,0)",
    "VARCHAR": "STRING", "TEXT": "STRING",
    "DATE": "DATE", "BOOLEAN": "BOOLEAN",
    "DOUBLE": "DOUBLE", "FLOAT": "FLOAT", "REAL": "FLOAT",
    "TIMESTAMP": "TIMESTAMP_NTZ", "TIMESTAMP_NS": "TIMESTAMP_NTZ",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMP_TZ", "TIMESTAMPTZ": "TIMESTAMP_TZ",
}


def duck_to_snow(t):
    t = t.upper().strip()
    if t in _SIMPLE:
        return _SIMPLE[t]
    m = re.match(r"DECIMAL\((\d+),\s*(\d+)\)", t)
    if m:
        return f"NUMBER({m.group(1)},{m.group(2)})"
    # safe fallback
    return "STRING"


def table_ddl(con, out, name, ext_vol, meta):
    cols = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{os.path.join(out, name)}/*.parquet') LIMIT 0"
    ).fetchall()
    lines = [f"  {c[0]:<22} {duck_to_snow(c[1])}" for c in cols]
    body = ",\n".join(lines)
    part = ""
    if meta.get(name, {}).get("part"):
        part = f"  PARTITION BY ({meta[name]['part']})\n"
    # SNOWFLAKE_MANAGED is a reserved value (internal storage): emit it unquoted
    # and omit BASE_LOCATION (Snowflake manages the path). A user-created
    # external volume is a quoted identifier with an explicit BASE_LOCATION.
    if ext_vol.upper() == "SNOWFLAKE_MANAGED":
        vol_clause = "  EXTERNAL_VOLUME = SNOWFLAKE_MANAGED\n"
    else:
        vol_clause = f"  EXTERNAL_VOLUME = '{ext_vol}'\n  BASE_LOCATION = '{name}'\n"
    return (
        f"CREATE OR REPLACE ICEBERG TABLE {name} (\n{body}\n)\n"
        f"  CATALOG = 'SNOWFLAKE'\n"
        f"{vol_clause}"
        f"{part}"
        f"  STORAGE_SERIALIZATION_POLICY = OPTIMIZED\n"
        f"  TARGET_FILE_SIZE = '128MB';\n"
    )


def emit(con, out, names, ext_vol, meta):
    ddl_dir = os.path.join(out, "ddl")
    os.makedirs(ddl_dir, exist_ok=True)
    combined = [f"-- Generated CREATE ICEBERG TABLE DDL (external volume: {ext_vol})",
                "-- USE SCHEMA <DB>.<SCHEMA>; before running.\n"]
    for name in names:
        ddl = table_ddl(con, out, name, ext_vol, meta)
        with open(os.path.join(ddl_dir, f"{name}.sql"), "w") as f:
            f.write(ddl)
        combined.append(ddl)
    with open(os.path.join(ddl_dir, "create_all.sql"), "w") as f:
        f.write("\n".join(combined))
    print(f"  DDL -> {ddl_dir}/  ({len(names)} tables + create_all.sql)")


def main():
    try:
        import duckdb
    except ImportError:
        sys.exit("duckdb not installed. Run:  pip install -r requirements.txt")
    from generate import TABLES, BY_NAME
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default="./output")
    ap.add_argument("--external-volume", default="iceberg_ext_vol")
    ap.add_argument("--only", default="")
    args = ap.parse_args()
    names = [n.strip() for n in args.only.split(",") if n.strip()] or [t["name"] for t in TABLES]
    present = [n for n in names if os.path.isdir(os.path.join(args.out, n))]
    if not present:
        sys.exit(f"No generated tables found under {args.out}. Run generate.py first.")
    con = duckdb.connect()
    emit(con, args.out, present, args.external_volume, BY_NAME)
    con.close()


if __name__ == "__main__":
    main()
