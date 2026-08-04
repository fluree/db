#!/usr/bin/env python3
"""Engine-agnostic paired A/B runner (SF0.1 ENTERPRISE_DEMO star schema).

Runs a pair (pairs/<id>.sql for the SQL engine, pairs/<id>.rq for Fluree) against targets
from targets.json and appends one normalized JSON row per run to --out:

    {pair_id, engine, target, substrate, engine_version, mode, run_idx, wall_ms, rows, ok,
     peak_rss_bytes, ts, harness_version, timing_boundary, extra:{...}}

The core is engine-agnostic; each engine is an external adapter in engines/ (fluree.py, and
duckdb.py as the first external yardstick engine). See the NOTICE in README.md / PROTOCOL.md
on the nominative, external-CLI-only use of any third-party engine.

BINDING PROTOCOL COMPLIANCE (see PROTOCOL.md):
  * FRESH PROCESS PER TIMED REP, both engines, is the PRIMARY mode ("cold": memo/buffer-pool
    cold, OS page cache warm). Reused-handle timings are a non-headline secondary only.
  * PEAK RSS recorded every run via `/usr/bin/time -l` (macOS "maximum resident set size",
    bytes). Memory fairness = disclosed footprints + neither-starved, NOT equal caps.
  * TIMED REGION ENDS AT FULL RESULT CONSUMPTION: the SQL side serializes every result row
    inside the timed statement (.mode csv + .output); the Fluree side times through complete
    SPARQL-results-JSON serialization. Recorded as timing_boundary="query_exec_serialize".
    For an iceberg-REST target the catalog ATTACH cost is captured as extra.setup_ms.

SCOPE: append-only SF01. MoR / equality-delete tables are OUT of the first A/B.

SECRETS: an OAuth-catalog target reads its secret from env at runtime; no token/secret is
ever written to a file, arg vector, or this script.
"""
import argparse
import json
import os
import tempfile
import time

from engines import duckdb as duckdb_engine
from engines import fluree as fluree_engine

HARNESS_VERSION = "0.3"
TIMING_BOUNDARY = "query_exec_serialize"

HERE = os.path.dirname(os.path.abspath(__file__))
PAIRS_DIR = os.path.join(HERE, "pairs")
_EV_CACHE = {}


def load_targets(path):
    with open(path) as f:
        return json.load(f)


def read_pair(pair_id, ext):
    with open(os.path.join(PAIRS_DIR, pair_id + ext)) as f:
        return f.read()


def engine_version(tgt_id, tgt):
    if tgt_id in _EV_CACHE:
        return _EV_CACHE[tgt_id]
    if tgt.get("engine") == "duckdb":
        ev = duckdb_engine.version(tgt)
    else:
        ev = fluree_engine.version(tgt)
    _EV_CACHE[tgt_id] = ev
    return ev


def emit(out_fh, pair_id, engine, target, substrate, mode, run_idx, wall_ms, rows, ok, rss,
         extra, engine_version="unknown"):
    row = {"pair_id": pair_id, "engine": engine, "target": target, "substrate": substrate,
           "engine_version": engine_version, "mode": mode, "run_idx": run_idx,
           "wall_ms": wall_ms, "rows": rows, "ok": ok, "peak_rss_bytes": rss,
           "ts": time.strftime("%Y-%m-%dT%H:%M:%S"), "harness_version": HARNESS_VERSION,
           "timing_boundary": TIMING_BOUNDARY}
    if extra:
        row["extra"] = {k: v for k, v in extra.items() if v is not None}
    out_fh.write(json.dumps(row) + "\n")
    out_fh.flush()
    rss_mb = ("%.0fMB" % (rss / 1e6)) if rss else "?"
    print("  %-30s %-6s %-24s #%d  wall=%s ms  rows=%s  rss=%s  ok=%s"
          % (pair_id, engine, substrate, run_idx, wall_ms, rows, rss_mb, ok))


def run_duckdb(tgt, tgt_id, pair_id, modes, runs, timeout_s, out_fh):
    bound = duckdb_engine.bind_tables(read_pair(pair_id, ".sql"), tgt)
    is_ice = tgt["read_mode"] == "iceberg_rest"
    substrate = tgt.get("substrate", tgt["read_mode"])
    ev = engine_version(tgt_id, tgt)
    for mode in modes:
        if mode != "cold":
            print("  [duckdb] mode %r (reused-handle secondary) not implemented in v%s; "
                  "primary is fresh-process 'cold'. Skipping." % (mode, HARNESS_VERSION))
            continue
        for i in range(1, runs + 1):
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tf:
                sink = tf.name
            try:
                res = duckdb_engine.exec_query(tgt, bound, is_ice, sink, timeout_s)
            finally:
                try:
                    os.unlink(sink)
                except OSError:
                    pass
            extra = {"proc_wall_ms": round(res["proc_ms"], 1)}
            if res.get("setup_ms") is not None:
                extra["setup_ms"] = round(res["setup_ms"], 1)
            if res.get("dnf"):
                extra["dnf"] = True
            if res.get("err"):
                extra["err"] = res["err"]
            emit(out_fh, pair_id, "duckdb", tgt_id, substrate, mode, i,
                 round(res["wall_ms"], 3) if res["wall_ms"] is not None else None,
                 res["rows"], res["ok"], res["rss"], extra, ev)


def run_fluree_cli(tgt, tgt_id, pair_id, modes, runs, timeout_s, out_fh):
    substrate = tgt.get("substrate", "fluree_minio_rest")
    rq = read_pair(pair_id, ".rq")
    ev = engine_version(tgt_id, tgt)
    for mode in modes:
        for i in range(1, runs + 1):
            res = fluree_engine.run_cli(tgt, rq, mode, timeout_s)
            if res is None:
                print("  [fluree_cli] pair %s: could not inject FROM (no WHERE{); skipping" % pair_id)
                return
            extra = {"proc_wall_ms": round(res["proc_ms"], 1)}
            if res.get("dnf"):
                extra["dnf"] = True
            if res.get("err"):
                extra["err"] = res["err"]
            emit(out_fh, pair_id, "fluree", tgt_id, substrate, mode, i,
                 res["wall_ms"], res["rows"], res["ok"], res["rss"], extra, ev)


def run_fluree_live(tgt, tgt_id, pair_id, modes, runs, timeout_s, out_fh):
    corpus = tgt.get("pair_to_corpus", {}).get(pair_id)
    substrate = tgt.get("substrate", "fluree_virtual_rest")
    if not corpus:
        print("  [fluree] pair %s has no corpus id (pair_to_corpus); skipping." % pair_id)
        return
    ev = engine_version(tgt_id, tgt)
    for mode in modes:
        for i in range(1, runs + 1):
            res = fluree_engine.run_live(tgt, corpus, mode, timeout_s)
            extra = {"proc_wall_ms": round(res["proc_ms"], 1), "corpus_id": corpus}
            if res.get("dnf"):
                extra["dnf"] = True
            if res.get("err"):
                extra["err"] = res["err"]
            emit(out_fh, pair_id, "fluree", tgt_id, substrate, mode, i,
                 res["wall_ms"], res["rows"], res["ok"], res["rss"], extra, ev)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pairs", required=True, help="comma-separated pair ids")
    ap.add_argument("--engines", default="duckdb", help="duckdb,fluree")
    ap.add_argument("--targets-file", default=os.path.join(HERE, "targets.json"))
    ap.add_argument("--duckdb-target", default="duckdb-iceberg-minio-sf01")
    ap.add_argument("--fluree-target", default="fluree-minio-sf01-main")
    ap.add_argument("--modes", default="cold", help="cold (primary, fresh process/rep), warm")
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--timeout-s", type=int, default=180,
                    help="per-run hard wall cap; on overrun the process group is killed and "
                         "the run recorded DNF")
    ap.add_argument("--out", default=os.path.join(HERE, "out", "run.jsonl"))
    ap.add_argument("--run-fluree", action="store_true",
                    help="execute the live-catalog Fluree leg (needs a built vbench)")
    ap.add_argument("--include-pending", action="store_true",
                    help="also run pairs marked pending_engine in manifest.json (they DNF on "
                         "shipped binaries and only fuse on a named unmerged branch); off by "
                         "default so the shipped corpus never runs a guaranteed-DNF pair")
    args = ap.parse_args()

    targets = load_targets(args.targets_file)
    manifest = load_targets(os.path.join(PAIRS_DIR, "manifest.json")).get("pairs", {})
    pairs = [p.strip() for p in args.pairs.split(",") if p.strip()]
    engines = [e.strip() for e in args.engines.split(",") if e.strip()]
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    with open(args.out, "a") as out_fh:
        for pair_id in pairs:
            pending = manifest.get(pair_id, {}).get("pending_engine")
            if pending and not args.include_pending:
                print("== pair %s SKIPPED (pending_engine=%s; pass --include-pending) =="
                      % (pair_id, pending))
                continue
            print("== pair %s (harness v%s, boundary=%s) =="
                  % (pair_id, HARNESS_VERSION, TIMING_BOUNDARY))
            if "duckdb" in engines:
                run_duckdb(targets[args.duckdb_target], args.duckdb_target,
                           pair_id, modes, args.runs, args.timeout_s, out_fh)
            if "fluree" in engines:
                ftgt = targets[args.fluree_target]
                if ftgt.get("engine") == "fluree_cli":
                    run_fluree_cli(ftgt, args.fluree_target, pair_id, modes,
                                   args.runs, args.timeout_s, out_fh)
                elif not args.run_fluree:
                    print("  [fluree] skipped (pass --run-fluree; live catalog).")
                else:
                    run_fluree_live(ftgt, args.fluree_target, pair_id, modes,
                                    args.runs, args.timeout_s, out_fh)
    print("\nwrote -> %s" % args.out)


if __name__ == "__main__":
    main()
