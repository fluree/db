#!/usr/bin/env python3
"""Paired DuckDB-vs-Fluree A/B runner (SF0.1 ENTERPRISE_DEMO star schema).

Runs a pair (pairs/<id>.sql for DuckDB, pairs/<id>.rq for Fluree) against a target
from targets.json and appends one normalized JSON row per run to --out:

    {pair_id, engine, target, mode, run_idx, wall_ms, rows, ok, peak_rss_bytes,
     ts, harness_version, timing_boundary, extra:{...}}

BINDING PROTOCOL COMPLIANCE (from R1 PROTOCOL-AMENDMENTS / red-team GO-WITH-FIXES):
  * FRESH PROCESS PER TIMED REP, both engines, is the PRIMARY mode ("cold" here:
    memoization/buffer-pool cold, OS page cache warm). Reused-handle timings
    ("warm") are a non-headline secondary only.
  * PEAK RSS recorded for every run via `/usr/bin/time -l` (macOS: "maximum
    resident set size" in BYTES) -> peak_rss_bytes. Memory fairness = disclosed
    footprints + neither-starved, NOT equal caps.
  * TIMED REGION ENDS AT FULL RESULT CONSUMPTION: the DuckDB side runs `.mode csv`
    + `.output <sink>` so every result row is produced AND serialized inside the
    timed statement (no display truncation, no lazy streaming). The Fluree side
    (vbench) times through complete SPARQL-results-JSON serialization. Recorded as
    timing_boundary="query_exec_serialize" (wall_ms). For the iceberg-REST target
    the catalog ATTACH+OAuth cost is captured SEPARATELY as extra.setup_ms so the
    cold-boundary (attach in vs out of the timed region) can be decided downstream
    without a re-run; extra.proc_wall_ms is the whole-process wall for reference.

SCOPE: append-only SF01 only. MoR / equality-delete tables are OUT of the first
A/B (DuckDB 1.5.2 crash; fluree-side handling in flux).

SECRETS: the iceberg-REST target reads its OAuth secret from $SF_SECRET at runtime
and interpolates it into an in-memory SQL script piped via stdin. No token/secret
is ever written to a file, arg vector, or this script. Export it in-terminal only.

COMPARISON NOTE (downstream, not enforced here): SUM(double) result columns are
compared with NUMERIC TOLERANCE, not exact hash (float assoc. reordering across
engines). p2 SUM is integer; a revenue pair would be double.
"""
import argparse
import json
import os
import re
import signal
import subprocess
import tempfile
import time

HARNESS_VERSION = "0.3"
TIMING_BOUNDARY = "query_exec_serialize"  # wall_ms = engine produces + serializes all rows

HERE = os.path.dirname(os.path.abspath(__file__))
PAIRS_DIR = os.path.join(HERE, "pairs")
TIMER_RE = re.compile(r"real\s+([0-9.]+)")            # duckdb .timer line: "Run Time (s): real X ..."
RSS_RE = re.compile(r"(\d+)\s+maximum resident set size")  # /usr/bin/time -l (bytes on macOS)


def load_targets(path):
    with open(path) as f:
        return json.load(f)


def bind_tables(sql, tgt):
    """Replace {{table}} tokens with the target's physical reference."""
    mode = tgt["read_mode"]

    def repl(m):
        t = m.group(1).strip().lower()
        if mode == "read_parquet":
            return "read_parquet('%s/%s/data_0.parquet')" % (tgt["data_dir"], t)
        if mode == "iceberg_rest":
            return "%s.%s.%s" % (tgt["catalog_alias"], tgt["schema"], t.upper())
        raise ValueError("unknown read_mode %r" % mode)

    return re.sub(r"\{\{([^}]+)\}\}", repl, sql)


def duckdb_preamble(tgt):
    """SQL run before the timed query. For iceberg-REST: install+attach the catalog.
    Two auth shapes: (1) Snowflake OAuth (iceberg SECRET + vended creds), or (2) a
    BYO-creds S3 target (MinIO/substrate-B): our own S3 secret + AUTHORIZATION_TYPE/
    ACCESS_DELEGATION_MODE none, which bypasses the vended-cred path entirely.
    Emits its own .timer lines (captured as setup) since .timer is on from the top."""
    if tgt["read_mode"] != "iceberg_rest":
        return ""
    lines = ["INSTALL iceberg;", "LOAD iceberg;"]
    attach_opts = ["TYPE ICEBERG", "ENDPOINT '%s'" % tgt["endpoint"]]
    if "s3_secret" in tgt:  # BYO-creds path (MinIO / substrate B)
        s = tgt["s3_secret"]
        secret_val = s.get("secret_literal") or os.environ.get(s.get("secret_env", ""), "")
        lines.append("INSTALL httpfs;")
        lines.append("LOAD httpfs;")
        lines.append("CREATE OR REPLACE SECRET s3sec (TYPE S3, KEY_ID '%s', SECRET '%s', "
                     "ENDPOINT '%s', URL_STYLE '%s', USE_SSL %s, REGION '%s');"
                     % (s["key_id"], secret_val, s["endpoint"], s.get("url_style", "path"),
                        str(s.get("use_ssl", False)).lower(), s.get("region", "us-east-1")))
        attach_opts.append("AUTHORIZATION_TYPE %s" % tgt.get("rest_auth", "none"))
        attach_opts.append("ACCESS_DELEGATION_MODE %s" % tgt.get("access_delegation", "none"))
    else:  # Snowflake OAuth path (substrate A)
        secret = os.environ.get(tgt["secret_env"])
        if not secret:
            raise RuntimeError("env $%s is unset; export the OAuth secret in-terminal "
                               "before running the iceberg target (see PREP.md)"
                               % tgt["secret_env"])
        lines.append("CREATE OR REPLACE SECRET pol (TYPE ICEBERG, CLIENT_ID '%s', "
                     "CLIENT_SECRET '%s', OAUTH2_SCOPE '%s', OAUTH2_SERVER_URI '%s');"
                     % (tgt.get("client_id", ""), secret, tgt["oauth2_scope"],
                        tgt["oauth2_server_uri"]))
        attach_opts.append("SECRET pol")
    lines.append("ATTACH '%s' AS %s ( %s );"
                 % (tgt["warehouse"], tgt["catalog_alias"], ", ".join(attach_opts)))
    lines += [p.rstrip(";") + ";" for p in tgt.get("pragmas", [])]
    return "\n".join(lines) + "\n"


def read_pair_sql(pair_id):
    with open(os.path.join(PAIRS_DIR, pair_id + ".sql")) as f:
        return f.read()


def read_pair_rq(pair_id):
    with open(os.path.join(PAIRS_DIR, pair_id + ".rq")) as f:
        return f.read()


def _parse_reals(text):
    return [float(x) for x in TIMER_RE.findall(text)]


def _parse_rss(text):
    m = RSS_RE.search(text)
    return int(m.group(1)) if m else None


_EV_CACHE = {}


def engine_version(tgt_id, tgt):
    """Binary provenance string for the row, computed once per target and cached.
    duckdb: CLI version; fluree: CLI version + the binary's worktree git commit
    (so a fusion-lacking build can never be silently mistaken for a fixed one)."""
    if tgt_id in _EV_CACHE:
        return _EV_CACHE[tgt_id]
    ev = "unknown"
    try:
        if tgt.get("engine") == "fluree_cli":
            binp = tgt["fluree_bin"]
            ver = subprocess.run([binp, "--version"], capture_output=True, text=True).stdout.strip()
            wt = binp.split("/target/")[0]
            commit = subprocess.run(["git", "-C", wt, "rev-parse", "--short", "HEAD"],
                                    capture_output=True, text=True).stdout.strip()
            ev = "%s @%s" % (ver or "fluree", commit or "?")
        elif tgt.get("engine") == "duckdb":
            v = subprocess.run(tgt.get("arch_prefix", []) + [tgt["bin"], "--version"],
                               capture_output=True, text=True).stdout.strip()
            ev = v or "duckdb"
    except Exception:
        pass
    _EV_CACHE[tgt_id] = ev
    return ev


def _clear_fluree_cache():
    """Clear the machine-global fluree artifact cache (the vbench --cold semantic) so a
    fresh fluree process pays the full cold catalog/metadata cost. Only removes the
    named fluree_binary_cache dirs (never storage/ or ledger data)."""
    import shutil
    tmp = os.environ.get("TMPDIR", "/tmp").rstrip("/")
    for name in ("fluree_binary_cache", "fluree_binary_cache-catalog", "fluree-cache"):
        p = os.path.join(tmp, name)
        if os.path.isdir(p) and p.endswith(("cache", "cache-catalog")):
            shutil.rmtree(p, ignore_errors=True)


def _run_capped(cmd, input_text, cwd, timeout_s, env=None):
    """Run cmd in its own process group with a hard wall cap. On timeout, SIGKILL the
    whole group (so a timed-out remote iceberg scan can't keep draining S3 in the
    background) and report timed_out=True. Returns (stdout, stderr, rc, timed_out, proc_ms)."""
    t0 = time.perf_counter()
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, text=True, cwd=cwd,
                         start_new_session=True, env=env)
    timed_out = False
    try:
        out, err = p.communicate(input=input_text, timeout=timeout_s)
    except subprocess.TimeoutExpired:
        timed_out = True
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        try:
            out, err = p.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            out, err = "", ""
    proc_ms = (time.perf_counter() - t0) * 1000.0
    return out or "", err or "", p.returncode, timed_out, proc_ms


def duckdb_exec(tgt, bound_sql, is_iceberg, sink_path, timeout_s):
    """One fresh duckdb process: preamble (setup) + one timed query, output serialized
    to sink_path (csv, full consumption). Hard-capped at timeout_s -> DNF on overrun.
    Returns dict with wall/setup/proc/rss/rows/ok/dnf."""
    # .timer on from the top so preamble (attach/oauth) timers are captured as setup;
    # preamble output goes to /dev/null so ONLY the query result lands in the sink
    # (correct row count). Switch the sink to the real file just before the query.
    script = ".timer on\n.mode csv\n.headers on\n.output /dev/null\n"
    script += duckdb_preamble(tgt)
    script += ".output %s\n" % sink_path
    script += bound_sql.strip().rstrip(";") + ";\n"
    cmd = ["/usr/bin/time", "-l"] + tgt.get("arch_prefix", []) + [tgt["bin"]]
    out, err_txt, rc, timed_out, proc_ms = _run_capped(cmd, script, None, timeout_s)
    if timed_out:
        return {"wall_ms": None, "setup_ms": None, "proc_ms": proc_ms, "rss": None,
                "rows": None, "ok": False, "dnf": True,
                "err": "DNF: exceeded %ds hard cap (remote over-scan; killed process group)" % timeout_s}
    combined = out + "\n" + err_txt
    reals = _parse_reals(combined)
    rss = _parse_rss(combined)
    # timer lines, in order: [install, load, create_secret, attach, (pragmas), query]
    # for parquet there is no preamble -> reals == [query]
    wall_ms = reals[-1] * 1000.0 if reals else None
    setup_ms = (sum(reals[:-1]) * 1000.0) if (is_iceberg and len(reals) > 1) else None
    rows = None
    try:
        with open(sink_path) as f:
            n = sum(1 for _ in f)
        rows = max(0, n - 1)  # minus the csv header
    except OSError:
        pass
    ok = rc == 0 and wall_ms is not None and rows is not None
    err = None if ok else _duckdb_error(out + "\n" + err_txt)
    return {"wall_ms": wall_ms, "setup_ms": setup_ms, "proc_ms": proc_ms,
            "rss": rss, "rows": rows, "ok": ok, "dnf": False, "err": err}


def _duckdb_error(text):
    """Pull the real DuckDB error line out of combined stdout+stderr, skipping the
    /usr/bin/time -l rusage block (which is always the trailing stderr content)."""
    errs = [ln.strip() for ln in text.splitlines() if "Error" in ln and ln.strip()]
    if errs:
        return errs[-1]
    return "nonzero exit (no DuckDB Error line captured)"


def run_duckdb(tgt, tgt_id, pair_id, modes, runs, timeout_s, out_fh):
    bound = bind_tables(read_pair_sql(pair_id), tgt)
    is_ice = tgt["read_mode"] == "iceberg_rest"
    substrate = tgt.get("substrate", tgt["read_mode"])
    ev = engine_version(tgt_id, tgt)
    for mode in modes:
        if mode != "cold":
            print("  [duckdb] mode %r (reused-handle secondary) not implemented in "
                  "v%s; primary is fresh-process 'cold'. Skipping." % (mode, HARNESS_VERSION))
            continue
        for i in range(1, runs + 1):
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tf:
                sink = tf.name
            try:
                res = duckdb_exec(tgt, bound, is_ice, sink, timeout_s)
            finally:
                try:
                    os.unlink(sink)
                except OSError:
                    pass
            extra = {"proc_wall_ms": round(res["proc_ms"], 1)}
            if res["setup_ms"] is not None:
                extra["setup_ms"] = round(res["setup_ms"], 1)
            if res.get("dnf"):
                extra["dnf"] = True
            if res["err"]:
                extra["err"] = res["err"]
            emit(out_fh, pair_id, "duckdb", tgt_id, substrate, mode, i,
                 round(res["wall_ms"], 3) if res["wall_ms"] is not None else None,
                 res["rows"], res["ok"], res["rss"], extra, ev)


def run_fluree(tgt, tgt_id, pair_id, modes, runs, timeout_s, out_fh):
    corpus = tgt.get("pair_to_corpus", {}).get(pair_id)
    substrate = tgt.get("substrate", "fluree_virtual_rest")
    if not corpus:
        print("  [fluree] pair %s has no corpus id (targets.json pair_to_corpus); add "
              "the .rq to the vbench corpus first (PREP.md). Skipping." % pair_id)
        return
    for mode in modes:
        cold_flag = ["--cold"] if mode == "cold" else []
        for i in range(1, runs + 1):
            cmd = ["/usr/bin/time", "-l", tgt["vbench_bin"], "exec-one",
                   "--query", corpus, "--target", tgt["target"]] + cold_flag
            out, err_txt, rc, timed_out, proc_ms = _run_capped(
                cmd, None, tgt.get("vbench_cwd"), timeout_s)
            rss = _parse_rss(err_txt)
            wall, rows, ok = None, None, False
            for line in out.splitlines():
                s = line.strip()
                if s.startswith("{"):
                    try:
                        rec = json.loads(s)
                        wall = rec.get("wall_ms")
                        rows = rec.get("rows")
                        ok = rec.get("status") == "ok"
                    except json.JSONDecodeError:
                        pass
            extra = {"proc_wall_ms": round(proc_ms, 1), "corpus_id": corpus}
            if timed_out:
                extra["dnf"] = True
                extra["err"] = "DNF: exceeded %ds hard cap" % timeout_s
            elif not ok:
                extra["stderr_tail"] = err_txt.strip()[-200:]
            emit(out_fh, pair_id, "fluree", tgt_id, substrate, mode, i, wall, rows, ok, rss, extra,
                 engine_version(tgt_id, tgt))


def run_fluree_cli(tgt, tgt_id, pair_id, modes, runs, timeout_s, out_fh):
    """Fluree via the prebuilt `fluree query` CLI against a registered R2RML graph source
    (substrate B / MinIO). Fresh process per rep (v0.3 primary). Reuses the pair's .rq,
    injecting a FROM <source> clause for the connection-scoped path; times through full
    JSON serialization (--format json) and reads the CLI's '(N rows, X.Xms)' tally."""
    substrate = tgt.get("substrate", "fluree_minio_rest")
    rq = read_pair_rq(pair_id)
    q = re.sub(r"(?is)\bWHERE\s*\{", "FROM <%s> WHERE {" % tgt["source"], rq, count=1)
    if "FROM <%s>" % tgt["source"] not in q:
        print("  [fluree_cli] pair %s: could not inject FROM (no WHERE{); skipping" % pair_id)
        return
    env = os.environ.copy()
    env.update(tgt.get("env", {}))
    for mode in modes:
        for i in range(1, runs + 1):
            # cold (primary) = true-cold: clear the machine-global fluree artifact cache
            # per rep so each rep pays the full cold catalog/metadata fetch (symmetric
            # with DuckDB re-attaching every fresh process). warm (secondary) leaves it.
            if mode == "cold":
                _clear_fluree_cache()
            cmd = ["/usr/bin/time", "-l", tgt["fluree_bin"], "query", "--config",
                   tgt["config"], "--connection", "--sparql", "--format", "json",
                   "--track-time", "-e", q]
            out, err_txt, rc, timed_out, proc_ms = _run_capped(cmd, None, None, timeout_s, env)
            rss = _parse_rss(err_txt)
            wall, rows = None, None
            # CLI prints "(N rows, X.Xms)" under 1s, "(N rows, X.XXs)" at/over 1s; N carries
            # a thousands-separator comma at >=1000 rows (e.g. "(1,100 rows, 194.1ms)").
            m = re.search(r"\(([\d,]+)\s+rows,\s+([0-9.]+)(ms|s)\)", out + err_txt)
            if m:
                rows = int(m.group(1).replace(",", ""))
                wall = float(m.group(2)) * (1000.0 if m.group(3) == "s" else 1.0)
            ok = rc == 0 and wall is not None and not timed_out
            extra = {"proc_wall_ms": round(proc_ms, 1)}
            if timed_out:
                extra["dnf"] = True
                extra["err"] = "DNF: exceeded %ds hard cap" % timeout_s
            elif not ok:
                tail = [l for l in (out + err_txt).splitlines() if "error" in l.lower()]
                extra["err"] = (tail[-1] if tail else "no timing tally")[:200]
            emit(out_fh, pair_id, "fluree", tgt_id, substrate, mode, i, wall, rows, ok, rss, extra,
                 engine_version(tgt_id, tgt))


def emit(out_fh, pair_id, engine, target, substrate, mode, run_idx, wall_ms, rows, ok, rss,
         extra, engine_version="unknown"):
    row = {"pair_id": pair_id, "engine": engine, "target": target, "substrate": substrate,
           "engine_version": engine_version,
           "mode": mode, "run_idx": run_idx, "wall_ms": wall_ms, "rows": rows, "ok": ok,
           "peak_rss_bytes": rss, "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
           "harness_version": HARNESS_VERSION, "timing_boundary": TIMING_BOUNDARY}
    if extra:
        row["extra"] = {k: v for k, v in extra.items() if v is not None}
    out_fh.write(json.dumps(row) + "\n")
    out_fh.flush()
    rss_mb = ("%.0fMB" % (rss / 1e6)) if rss else "?"
    print("  %-26s %-6s %-24s #%d  wall=%s ms  rows=%s  rss=%s  ok=%s"
          % (pair_id, engine, substrate, run_idx, wall_ms, rows, rss_mb, ok))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pairs", required=True, help="comma-separated pair ids")
    ap.add_argument("--engines", default="duckdb", help="duckdb,fluree")
    ap.add_argument("--targets-file", default=os.path.join(HERE, "targets.json"))
    ap.add_argument("--duckdb-target", default="duckdb-parquet-sf01",
                    help="duckdb-parquet-sf01 (floor) | duckdb-iceberg-sf01 (substrate A)")
    ap.add_argument("--fluree-target", default="fluree-virtual-sf01")
    ap.add_argument("--modes", default="cold", help="cold (primary, fresh process/rep)")
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--timeout-s", type=int, default=180,
                    help="per-run hard wall cap; on overrun the process group is killed "
                         "and the run recorded as DNF (default 180 = the join-attempt cap)")
    ap.add_argument("--out", default=os.path.join(HERE, "out", "run.jsonl"))
    ap.add_argument("--run-fluree", action="store_true",
                    help="execute the Fluree side (live Snowflake; needs VBENCH_PAT + built vbench)")
    args = ap.parse_args()

    targets = load_targets(args.targets_file)
    pairs = [p.strip() for p in args.pairs.split(",") if p.strip()]
    engines = [e.strip() for e in args.engines.split(",") if e.strip()]
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    with open(args.out, "a") as out_fh:
        for pair_id in pairs:
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
                    print("  [fluree] skipped (pass --run-fluree; live Snowflake). Runbook in PREP.md.")
                else:
                    run_fluree(ftgt, args.fluree_target,
                               pair_id, modes, args.runs, args.timeout_s, out_fh)
    print("\nwrote -> %s" % args.out)


if __name__ == "__main__":
    main()
