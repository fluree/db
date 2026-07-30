"""Engine adapter: Fluree (the engine under characterization).

Two paths: `run_cli` drives a prebuilt `fluree query` CLI against a registered R2RML graph
source (substrate B / MinIO), fresh process per rep; `run_live` drives the vbench binary
against a live catalog. `query_json` is the correctness helper (full JSON result). Binary
paths + config live in targets.json.
"""
import json
import os
import re
import subprocess

from . import common

# CLI prints "(N rows, X.Xms)" under 1s, "(N rows, X.XXs)" at/over 1s; N carries a
# thousands-separator comma at >=1000 rows (e.g. "(1,100 rows, 194.1ms)").
_TALLY = re.compile(r"\(([\d,]+)\s+rows,\s+([0-9.]+)(ms|s)\)")


def clear_cache():
    """Clear the machine-global fluree artifact cache (the vbench --cold semantic) so a fresh
    fluree process pays the full cold catalog/metadata cost. Only removes the named cache
    dirs (never storage/ or ledger data)."""
    import shutil
    tmp = os.environ.get("TMPDIR", "/tmp").rstrip("/")
    for name in ("fluree_binary_cache", "fluree_binary_cache-catalog", "fluree-cache"):
        p = os.path.join(tmp, name)
        if os.path.isdir(p) and p.endswith(("cache", "cache-catalog")):
            shutil.rmtree(p, ignore_errors=True)


def version(tgt):
    """Provenance string: CLI version + the binary's worktree git commit, so a fusion-lacking
    build can never be silently mistaken for a fixed one."""
    try:
        if tgt.get("engine") == "fluree_cli":
            binp = tgt["fluree_bin"]
        else:
            binp = tgt.get("vbench_bin", "")
        ver = subprocess.run([binp, "--version"], capture_output=True, text=True).stdout.strip()
        wt = binp.split("/target/")[0]
        commit = subprocess.run(["git", "-C", wt, "rev-parse", "--short", "HEAD"],
                                capture_output=True, text=True).stdout.strip()
        return "%s @%s" % (ver or "fluree", commit or "?")
    except Exception:
        return "unknown"


def _inject_from(rq, source):
    return re.sub(r"(?is)\bWHERE\s*\{", "FROM <%s> WHERE {" % source, rq, count=1)


def run_cli(tgt, rq_text, mode, timeout_s):
    """One fresh `fluree query` process (v0.3 primary = fresh process per rep). cold (primary)
    clears the machine-global artifact cache per rep so each rep pays the full cold
    catalog/metadata fetch (symmetric with a fresh external process re-attaching); warm leaves
    it. Times through full JSON serialization, reads the CLI's '(N rows, X.Xms)' tally."""
    q = _inject_from(rq_text, tgt["source"])
    if "FROM <%s>" % tgt["source"] not in q:
        return None  # could not inject FROM (no WHERE{)
    env = os.environ.copy()
    env.update(tgt.get("env", {}))
    if mode == "cold":
        clear_cache()
    cmd = ["/usr/bin/time", "-l", tgt["fluree_bin"], "query", "--config", tgt["config"],
           "--connection", "--sparql", "--format", "json", "--track-time", "-e", q]
    out, err_txt, rc, timed_out, proc_ms = common.run_capped(cmd, None, None, timeout_s, env)
    rss = common.parse_rss(err_txt)
    wall, rows = None, None
    m = _TALLY.search(out + err_txt)
    if m:
        rows = int(m.group(1).replace(",", ""))
        wall = float(m.group(2)) * (1000.0 if m.group(3) == "s" else 1.0)
    ok = rc == 0 and wall is not None and not timed_out
    err = None
    if timed_out:
        err = "DNF: exceeded %ds hard cap" % timeout_s
    elif not ok:
        tail = [l for l in (out + err_txt).splitlines() if "error" in l.lower()]
        err = (tail[-1] if tail else "no timing tally")[:200]
    return {"wall_ms": wall, "rows": rows, "ok": ok, "rss": rss, "dnf": timed_out,
            "err": err, "proc_ms": proc_ms}


def run_live(tgt, corpus_id, mode, timeout_s):
    """One fresh vbench exec-one process against a live catalog target."""
    cold_flag = ["--cold"] if mode == "cold" else []
    cmd = ["/usr/bin/time", "-l", tgt["vbench_bin"], "exec-one",
           "--query", corpus_id, "--target", tgt["target"]] + cold_flag
    out, err_txt, rc, timed_out, proc_ms = common.run_capped(
        cmd, None, tgt.get("vbench_cwd"), timeout_s)
    rss = common.parse_rss(err_txt)
    wall, rows, ok = None, None, False
    for line in out.splitlines():
        s = line.strip()
        if s.startswith("{"):
            try:
                rec = json.loads(s)
                wall, rows, ok = rec.get("wall_ms"), rec.get("rows"), rec.get("status") == "ok"
            except json.JSONDecodeError:
                pass
    err = None
    if timed_out:
        err = "DNF: exceeded %ds hard cap" % timeout_s
    elif not ok:
        err = err_txt.strip()[-200:]
    return {"wall_ms": wall, "rows": rows, "ok": ok and not timed_out, "rss": rss,
            "dnf": timed_out, "err": err, "proc_ms": proc_ms}


def query_json(tgt, rq_text, timeout_s=600):
    """Correctness helper: run a SPARQL query (FROM-injected) and return the parsed JSON
    results document (head.vars + results.bindings)."""
    q = _inject_from(rq_text, tgt["source"])
    env = os.environ.copy()
    env.update(tgt.get("env", {}))
    cmd = [tgt["fluree_bin"], "query", "--config", tgt["config"], "--connection",
           "--sparql", "--format", "json", "-e", q]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s, env=env).stdout
    return json.loads(out[out.index("{"):out.rindex("}") + 1])
