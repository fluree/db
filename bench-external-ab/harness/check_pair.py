#!/usr/bin/env python3
"""Correctness cross-check: run a pair on the SQL engine and Fluree and compare results under
the manifest's per-pair equivalence rule BEFORE any timing counts (PROTOCOL §6). A pair times
only after it MATCHES here. Engine-specifics come from the adapters in engines/.

Equivalence handling (PROTOCOL §7):
  - exact gate            : int/string AND numeric cells compared for LITERAL equality (a
                            COUNT off-by-one must fail, even a ≤1e-6-relative one at scale)
  - tolerance gate        : numeric cells / double SUM compared with relative tolerance
                            (float reassociation); non-numeric cells still literal
  - templated-IRI subject : reduced to its trailing integer key (so a Fluree IRI
                            http://data.fluree.dev/edw/store/3 compares to SQL store_key 3)
  - rows_only gate        : compare ROW COUNT only (unordered LIMIT)
  - columns aligned by NAME (SQL aliases == SPARQL vars), order-independent.
Gate per pair comes from pairs/manifest.json ("gate": exact|tolerance|rows_only).

  python check_pair.py --pairs cq038_count_current_customers,cq014_channel_mix \\
    --duckdb-target duckdb-iceberg-minio-sf01 --fluree-target fluree-minio-sf01-main
"""
import argparse
import json
import os
import re
import subprocess

from engines import duckdb as duckdb_engine
from engines import fluree as fluree_engine

HERE = os.path.dirname(os.path.abspath(__file__))
PAIRS = os.path.join(HERE, "pairs")
IRI_KEY = re.compile(r"https?://data\.fluree\.dev/edw/[^/]+/(-?\d+)$")
REL_TOL = 1e-6


def load(p):
    with open(p) as f:
        return json.load(f)


def norm_cell(v):
    """Normalize a raw string cell to a comparable python value."""
    if v is None or v == "":
        return None
    m = IRI_KEY.match(v)
    if m:
        return ("k", int(m.group(1)))
    try:
        return ("n", float(v))
    except ValueError:
        return ("s", v)


def run_sql(pair, tgt):
    """Returns (col_names, rows-as-dicts) from the SQL engine. Preamble output -> /dev/null so
    only the query header line + data land on stdout (header -> column names)."""
    with open(os.path.join(PAIRS, pair + ".sql")) as f:
        sql = duckdb_engine.bind_tables(f.read(), tgt)
    script = (".mode csv\n.headers on\n.output /dev/null\n" + duckdb_engine.preamble(tgt)
              + ".output /dev/stdout\n" + sql.strip().rstrip(";") + ";\n")
    cmd = tgt.get("arch_prefix", []) + [tgt["bin"]]
    out = subprocess.run(cmd, input=script, capture_output=True, text=True, timeout=600).stdout
    lines = [l for l in out.splitlines() if l.strip() != ""]
    if not lines:
        return [], []
    names = lines[0].split(",")
    rows = [dict(zip(names, (norm_cell(c) for c in l.split(",")))) for l in lines[1:]]
    return names, rows


def run_sparql(pair, tgt):
    """Returns (var_names, rows-as-dicts) from Fluree."""
    with open(os.path.join(PAIRS, pair + ".rq")) as f:
        rq = f.read()
    doc = fluree_engine.query_json(tgt, rq)
    vars = doc["head"]["vars"]
    rows = [{v: (norm_cell(b[v]["value"]) if v in b else None) for v in vars}
            for b in doc["results"]["bindings"]]
    return vars, rows


def cmp_cell(a, b, numeric_exact):
    """numeric_exact=True (the `exact` gate) requires literal equality even for numeric
    cells; False (the `tolerance` gate) allows REL_TOL on numeric cells for float
    reassociation. Non-numeric cells are always compared literally."""
    if a is None or b is None:
        return a == b
    if a[0] == "n" and b[0] == "n":
        if a[1] == b[1]:
            return True
        if numeric_exact:
            return False
        denom = max(abs(a[1]), abs(b[1]), 1e-12)
        return abs(a[1] - b[1]) / denom <= REL_TOL
    return a == b


def _key(row, cols):
    return tuple(repr(row.get(c)) for c in cols)


def cmp_rows(dres, fres, gate):
    dnames, dk = dres
    fnames, fl = fres
    if gate == "rows_only":
        return len(dk) == len(fl), "rows d=%d f=%d" % (len(dk), len(fl))
    if set(dnames) != set(fnames):
        return False, "COLUMN NAMES differ: sql=%s fluree=%s" % (sorted(dnames), sorted(fnames))
    if len(dk) != len(fl):
        return False, "ROW COUNT d=%d f=%d" % (len(dk), len(fl))
    cols = sorted(set(dnames))  # align by NAME, order-independent
    numeric_exact = gate != "tolerance"  # exact (and any non-tolerance) gate => literal numeric
    ds = sorted(dk, key=lambda r: _key(r, cols))
    fs = sorted(fl, key=lambda r: _key(r, cols))
    for i, (a, b) in enumerate(zip(ds, fs)):
        if not all(cmp_cell(a.get(c), b.get(c), numeric_exact) for c in cols):
            return False, "row %d mismatch: %r vs %r" % (i, {c: a.get(c) for c in cols}, {c: b.get(c) for c in cols})
    return True, "%d rows match" % len(dk)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pairs", required=True)
    ap.add_argument("--targets-file", default=os.path.join(HERE, "targets.json"))
    ap.add_argument("--duckdb-target", default="duckdb-iceberg-minio-sf01")
    ap.add_argument("--fluree-target", default="fluree-minio-sf01-main")
    ap.add_argument("--include-pending", action="store_true",
                    help="also check pairs marked pending_engine (fuse only on a named unmerged "
                         "branch); off by default so the shipped corpus never checks a DNF pair")
    args = ap.parse_args()
    targets = load(args.targets_file)
    man = load(os.path.join(PAIRS, "manifest.json"))["pairs"]
    dt, ft = targets[args.duckdb_target], targets[args.fluree_target]
    allok = True
    for pair in [p.strip() for p in args.pairs.split(",") if p.strip()]:
        pending = man.get(pair, {}).get("pending_engine")
        if pending and not args.include_pending:
            print("%-34s SKIPPED (pending_engine=%s; pass --include-pending)" % (pair, pending))
            continue
        gate = man.get(pair, {}).get("gate", "exact")
        try:
            ok, msg = cmp_rows(run_sql(pair, dt), run_sparql(pair, ft), gate)
        except Exception as e:
            ok, msg = False, "ERROR %s" % e
        allok = allok and ok
        print("%-34s gate=%-9s %s  %s" % (pair, gate, "PASS" if ok else "FAIL", msg))
    raise SystemExit(0 if allok else 1)


if __name__ == "__main__":
    main()
