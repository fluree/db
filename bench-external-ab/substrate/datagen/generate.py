#!/usr/bin/env python3
"""Synthetic enterprise-DW generator for the Snowflake -> Iceberg -> Fluree demo.

DuckDB-driven. One `--scale-factor` knob scales the fact tables linearly and the
dimensions sub-linearly (sqrt), mirroring how real warehouses grow. Output is
~128 MB ZSTD Parquet parts per table under <out>/<table>/, ready for Snowflake
`COPY INTO` (see ../load). Also emits CREATE ICEBERG TABLE DDL inferred from the
actual Parquet schema, so DDL can never drift from the data.

Usage:
    python generate.py --estimate-only --scale-factor 20
    python generate.py --scale-factor 20 --out ./output --threads 8
    python generate.py --scale-factor 0.01 --only dim_date,fact_order,fact_order_line
    python ddl.py --out ./output --external-volume iceberg_ext_vol   # DDL only

Scale anchor: SF=1 ~= 1 GB; SF=20 ~= 20 GB (the recommended starting point).
"""
import argparse
import math
import os
import re
import sys
import time

try:
    import duckdb
except ImportError:
    sys.exit("duckdb not installed. Run:  pip install -r requirements.txt")

from queries import QUERIES, SETUP_SQL

DATE_START = "2006-01-01"
DATE_DAYS = 7670  # ~21 years -> dim_date row count; SPAN = DATE_DAYS - 1

# name, base_rows (SF=1), scale kind, partition_by (None=unpartitioned),
# approx compressed bytes/row (for the pre-run estimate), scd_frac (SCD-2 dims).
TABLES = [
    # dimensions ---------------------------------------------------------------
    {"name": "dim_date",         "base": DATE_DAYS, "scale": "fixed", "part": None,            "bpr": 40},
    {"name": "dim_geography",    "base": 25_000,    "scale": "dim",   "part": None,            "bpr": 60},
    {"name": "dim_supplier",     "base": 2_000,     "scale": "dim",   "part": None,            "bpr": 55},
    {"name": "dim_account",      "base": 15_000,    "scale": "dim",   "part": None,            "bpr": 70},
    {"name": "dim_store",        "base": 500,       "scale": "dim",   "part": None,            "bpr": 55},
    {"name": "dim_employee",     "base": 5_000,     "scale": "dim",   "part": None,            "bpr": 60},
    {"name": "dim_customer",     "base": 300_000,   "scale": "dim",   "part": None,            "bpr": 80, "scd_frac": 0.30},
    {"name": "dim_product",      "base": 30_000,    "scale": "dim",   "part": None,            "bpr": 75, "scd_frac": 0.25},
    # facts --------------------------------------------------------------------
    {"name": "fact_order",              "base": 1_800_000,  "scale": "fact", "part": "order_date",     "bpr": 55},
    {"name": "fact_order_line",         "base": 6_000_000,  "scale": "fact", "part": "order_date",     "bpr": 35},
    {"name": "fact_inventory_snapshot", "base": 3_000_000,  "scale": "fact", "part": "snapshot_date",  "bpr": 30},
    {"name": "fact_shipment",           "base": 1_800_000,  "scale": "fact", "part": "ship_date",      "bpr": 60},
    {"name": "fact_payment",            "base": 2_000_000,  "scale": "fact", "part": "payment_date",   "bpr": 40},
    {"name": "fact_gl_journal",         "base": 2_500_000,  "scale": "fact", "part": "posting_date",   "bpr": 50},
    {"name": "fact_web_event",          "base": 10_000_000, "scale": "fact", "part": "event_date",     "bpr": 75},
    {"name": "fact_support_ticket",     "base": 400_000,    "scale": "fact", "part": "open_date",      "bpr": 65},
]
BY_NAME = {t["name"]: t for t in TABLES}


def base_count(t, sf):
    """Current/base row count (excludes SCD-2 historical rows)."""
    if t["scale"] == "fixed":
        return t["base"]
    if t["scale"] == "dim":
        return max(t["base"], round(t["base"] * math.sqrt(sf)))
    return round(t["base"] * sf)  # fact


def gen_count(t, sf):
    """Total rows actually generated (base + SCD-2 history)."""
    n = base_count(t, sf)
    if "scd_frac" in t:
        n += round(n * t["scd_frac"])
    return n


def human(n):
    for unit in ("", "K", "M", "B"):
        if abs(n) < 1000:
            return f"{n:.0f}{unit}" if unit else f"{n:.0f}"
        n /= 1000
    return f"{n:.1f}T"


def human_bytes(b):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(b) < 1024:
            return f"{b:.1f}{unit}"
        b /= 1024
    return f"{b:.1f}PB"


def context(sf):
    """Placeholder values shared by the SQL templates."""
    return {
        "date_start": DATE_START,
        "span_days": gen_count(BY_NAME["dim_date"], sf) - 1,
        "n_geography":    base_count(BY_NAME["dim_geography"], sf),
        "n_supplier":     base_count(BY_NAME["dim_supplier"], sf),
        "n_account":      base_count(BY_NAME["dim_account"], sf),
        "n_store":        base_count(BY_NAME["dim_store"], sf),
        "n_employee":     base_count(BY_NAME["dim_employee"], sf),
        "n_customer_cur": base_count(BY_NAME["dim_customer"], sf),
        "n_product_cur":  base_count(BY_NAME["dim_product"], sf),
        "n_order":        base_count(BY_NAME["fact_order"], sf),
    }


def print_plan(sf, names):
    print(f"\nPlan @ scale-factor {sf}  (anchor: SF=1 ~= 1 GB)\n")
    print(f"  {'table':<26}{'rows':>10}{'est size':>12}")
    print("  " + "-" * 48)
    total_rows = total_bytes = 0
    for name in names:
        t = BY_NAME[name]
        rows = gen_count(t, sf)
        est = rows * t["bpr"]
        total_rows += rows
        total_bytes += est
        print(f"  {name:<26}{human(rows):>10}{human_bytes(est):>12}")
    print("  " + "-" * 48)
    print(f"  {'TOTAL':<26}{human(total_rows):>10}{human_bytes(total_bytes):>12}\n")


def build_sql(name, sf):
    t = BY_NAME[name]
    ctx = dict(context(sf))
    ctx["n_rows"] = gen_count(t, sf)
    if "scd_frac" in t:
        ctx["n_current"] = base_count(t, sf)
    if name == "fact_order_line":
        ctx["n_order"] = base_count(BY_NAME["fact_order"], sf)
    return QUERIES[name].substitute(ctx)


def generate(con, name, sf, outdir, file_size):
    t = BY_NAME[name]
    sql = build_sql(name, sf)
    dest = os.path.join(outdir, name)
    t0 = time.time()
    con.execute(
        f"COPY ({sql}) TO '{dest}' "
        f"(FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES '{file_size}', "
        f"OVERWRITE_OR_IGNORE true)"
    )
    dt = time.time() - t0
    nbytes = sum(
        os.path.getsize(os.path.join(dest, f))
        for f in os.listdir(dest) if f.endswith(".parquet")
    )
    nfiles = len([f for f in os.listdir(dest) if f.endswith(".parquet")])
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{dest}/*.parquet')").fetchone()[0]
    print(f"  ✓ {name:<26}{human(rows):>10}{human_bytes(nbytes):>12}  "
          f"{nfiles:>4} files  {dt:6.1f}s")
    return {"name": name, "rows": rows, "bytes": nbytes, "files": nfiles}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scale-factor", type=float, default=20.0,
                    help="Fact rows x SF; dims x sqrt(SF). SF=1 ~= 1 GB. Default 20.")
    ap.add_argument("--out", default="./output", help="Output directory.")
    ap.add_argument("--only", default="",
                    help="Comma-separated subset of tables (default: all).")
    ap.add_argument("--file-size", default="128MB",
                    help="Target Parquet part size (DuckDB FILE_SIZE_BYTES). Default 128MB.")
    ap.add_argument("--threads", type=int, default=0, help="DuckDB threads (0=auto).")
    ap.add_argument("--memory-limit", default="", help="e.g. 8GB (DuckDB spills beyond this).")
    ap.add_argument("--seed", type=float, default=0.42,
                    help="Deterministic seed in [-1,1] (DuckDB setseed). Default 0.42.")
    ap.add_argument("--estimate-only", action="store_true",
                    help="Print the row/size plan and exit.")
    ap.add_argument("--no-ddl", action="store_true",
                    help="Skip emitting CREATE ICEBERG TABLE DDL after generation.")
    ap.add_argument("--external-volume", default="iceberg_ext_vol",
                    help="External volume name baked into emitted DDL.")
    args = ap.parse_args()

    names = [n.strip() for n in args.only.split(",") if n.strip()] or [t["name"] for t in TABLES]
    unknown = [n for n in names if n not in BY_NAME]
    if unknown:
        sys.exit(f"Unknown table(s): {unknown}\nKnown: {list(BY_NAME)}")
    # keep canonical (dims-before-facts) order
    names = [t["name"] for t in TABLES if t["name"] in names]

    print_plan(args.scale_factor, names)
    if args.estimate_only:
        return

    os.makedirs(args.out, exist_ok=True)
    con = duckdb.connect()
    if args.threads:
        con.execute(f"SET threads TO {args.threads}")
    if args.memory_limit:
        con.execute(f"SET memory_limit = '{args.memory_limit}'")
    con.execute("SET preserve_insertion_order = false")  # lower memory on big COPYs
    con.execute(SETUP_SQL)
    con.execute(f"SELECT setseed({args.seed})")
    try:
        con.execute("PRAGMA enable_progress_bar")
    except duckdb.Error:
        pass

    print("Generating...\n")
    print(f"  {'table':<26}{'rows':>10}{'size':>12}")
    print("  " + "-" * 60)
    t0 = time.time()
    results = [generate(con, name, args.scale_factor, args.out, args.file_size) for name in names]
    total_bytes = sum(r["bytes"] for r in results)
    total_rows = sum(r["rows"] for r in results)
    print("  " + "-" * 60)
    print(f"  {'TOTAL':<26}{human(total_rows):>10}{human_bytes(total_bytes):>12}  "
          f"{int(time.time()-t0)}s\n")

    if not args.no_ddl:
        import ddl
        ddl.emit(con, args.out, names, args.external_volume, BY_NAME)
    con.close()


if __name__ == "__main__":
    main()
