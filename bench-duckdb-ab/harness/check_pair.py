#!/usr/bin/env python3
"""Correctness cross-check: run a pair on DuckDB (SQL) and Fluree (SPARQL) and compare
results under the manifest's per-pair equivalence rule BEFORE any timing counts (PROTOCOL
§6). A pair times only after it MATCHES here.

Equivalence handling (PROTOCOL §7):
  - int/string cells      : exact multiset compare
  - double cells / SUM    : numeric compare with relative tolerance (float reassociation)
  - templated-IRI subject : reduced to its trailing integer key (so a Fluree IRI
                            http://data.fluree.dev/edw/store/3 compares to SQL store_key 3)
  - rows_only gate        : compare ROW COUNT only (unordered LIMIT)
Gate per pair comes from pairs/manifest.json ("gate": exact|tolerance|rows_only) and
"sum_is_double". Usage mirrors run_pair.py targets.

  python check_pair.py --pairs cq038_count_current_customers,cq014_channel_mix \\
    --duckdb-target duckdb-iceberg-minio-sf01 --fluree-target fluree-minio-sf01-main
"""
import argparse
import json
import os
import re
import subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
PAIRS = os.path.join(HERE, "pairs")
IRI_KEY = re.compile(r"https?://data\.fluree\.dev/edw/[^/]+/(-?\d+)$")
REL_TOL = 1e-6


def load(p):
    with open(p) as f:
        return json.load(f)


def bind_tables(sql, tgt):
    mode = tgt["read_mode"]
    def repl(m):
        t = m.group(1).strip().lower()
        if mode == "read_parquet":
            return "read_parquet('%s/%s/data_0.parquet')" % (tgt["data_dir"], t)
        return "%s.%s.%s" % (tgt["catalog_alias"], tgt["schema"], t.upper())
    return re.sub(r"\{\{([^}]+)\}\}", repl, sql)


def duckdb_preamble(tgt):
    if tgt["read_mode"] != "iceberg_rest":
        return ""
    s = tgt["s3_secret"]
    return ("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;\n"
            "CREATE OR REPLACE SECRET s3sec (TYPE S3, KEY_ID '%s', SECRET '%s', ENDPOINT '%s', "
            "URL_STYLE '%s', USE_SSL %s, REGION '%s');\n"
            "ATTACH '%s' AS %s (TYPE ICEBERG, ENDPOINT '%s', AUTHORIZATION_TYPE %s, "
            "ACCESS_DELEGATION_MODE %s);\n"
            % (s["key_id"], s.get("secret_literal", ""), s["endpoint"], s.get("url_style", "path"),
               str(s.get("use_ssl", False)).lower(), s.get("region", "us-east-1"),
               tgt["warehouse"], tgt["catalog_alias"], tgt["endpoint"],
               tgt.get("rest_auth", "none"), tgt.get("access_delegation", "none")))


def norm_cell(v):
    """Normalize a raw string cell to a comparable python value."""
    if v is None or v == "":
        return None
    m = IRI_KEY.match(v)
    if m:
        return ("k", int(m.group(1)))
    try:
        f = float(v)
        return ("n", f)
    except ValueError:
        return ("s", v)


def run_duckdb(pair, tgt):
    """Returns (col_names, rows-as-dicts). Preamble output is sent to /dev/null so only the
    query's header line + data land on stdout (header -> column names)."""
    with open(os.path.join(PAIRS, pair + ".sql")) as f:
        sql = bind_tables(f.read(), tgt)
    script = (".mode csv\n.headers on\n.output /dev/null\n" + duckdb_preamble(tgt)
              + ".output /dev/stdout\n" + sql.strip().rstrip(";") + ";\n")
    cmd = tgt.get("arch_prefix", []) + [tgt["bin"]]
    out = subprocess.run(cmd, input=script, capture_output=True, text=True, timeout=600).stdout
    lines = [l for l in out.splitlines() if l.strip() != ""]
    if not lines:
        return [], []
    names = lines[0].split(",")
    rows = [dict(zip(names, (norm_cell(c) for c in l.split(",")))) for l in lines[1:]]
    return names, rows


def run_fluree(pair, tgt):
    """Returns (var_names, rows-as-dicts)."""
    with open(os.path.join(PAIRS, pair + ".rq")) as f:
        rq = f.read()
    q = re.sub(r"(?is)\bWHERE\s*\{", "FROM <%s> WHERE {" % tgt["source"], rq, count=1)
    env = os.environ.copy()
    env.update(tgt.get("env", {}))
    cmd = [tgt["fluree_bin"], "query", "--config", tgt["config"], "--connection",
           "--sparql", "--format", "json", "-e", q]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=600, env=env).stdout
    doc = json.loads(out[out.index("{"):out.rindex("}") + 1])
    vars = doc["head"]["vars"]
    rows = [{v: (norm_cell(b[v]["value"]) if v in b else None) for v in vars}
            for b in doc["results"]["bindings"]]
    return vars, rows


def cmp_cell(a, b):
    if a is None or b is None:
        return a == b
    if a[0] == "n" and b[0] == "n":
        if a[1] == b[1]:
            return True
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
        return False, "COLUMN NAMES differ: duckdb=%s fluree=%s" % (sorted(dnames), sorted(fnames))
    if len(dk) != len(fl):
        return False, "ROW COUNT d=%d f=%d" % (len(dk), len(fl))
    cols = sorted(set(dnames))  # align by NAME, order-independent
    ds = sorted(dk, key=lambda r: _key(r, cols))
    fs = sorted(fl, key=lambda r: _key(r, cols))
    for i, (a, b) in enumerate(zip(ds, fs)):
        if not all(cmp_cell(a.get(c), b.get(c)) for c in cols):
            return False, "row %d mismatch: %r vs %r" % (i, {c: a.get(c) for c in cols}, {c: b.get(c) for c in cols})
    return True, "%d rows match" % len(dk)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pairs", required=True)
    ap.add_argument("--targets-file", default=os.path.join(HERE, "targets.json"))
    ap.add_argument("--duckdb-target", default="duckdb-iceberg-minio-sf01")
    ap.add_argument("--fluree-target", default="fluree-minio-sf01-main")
    args = ap.parse_args()
    targets = load(args.targets_file)
    man = load(os.path.join(PAIRS, "manifest.json"))["pairs"]
    dt, ft = targets[args.duckdb_target], targets[args.fluree_target]
    allok = True
    for pair in [p.strip() for p in args.pairs.split(",") if p.strip()]:
        gate = man.get(pair, {}).get("gate", "exact")
        try:
            dk = run_duckdb(pair, dt)
            fl = run_fluree(pair, ft)
            ok, msg = cmp_rows(dk, fl, gate)
        except Exception as e:
            ok, msg = False, "ERROR %s" % e
        allok = allok and ok
        print("%-34s gate=%-9s %s  %s" % (pair, gate, "PASS" if ok else "FAIL", msg))
    raise SystemExit(0 if allok else 1)


if __name__ == "__main__":
    main()
