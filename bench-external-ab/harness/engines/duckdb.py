"""External-engine adapter: DuckDB.

This adapter invokes an INDEPENDENTLY INSTALLED DuckDB CLI binary as an external process,
solely for comparative performance measurement. No DuckDB source code, binaries, extensions,
or platform components are included in, linked into, distributed with, or incorporated into
this repository or any Fluree product. DuckDB® is a trademark of its owner; references
here are nominative. The binary path lives in targets.json and is a documented user install
step (see ../../README.md / ../../substrate/README.md) — it is never auto-fetched by this
code, never committed. This adapter only emits the engine's DOCUMENTED SQL/dot-command and
config surface; it does not parse, vendor, or adapt any DuckDB source.
"""
import os
import subprocess

from . import common


def bind_tables(sql, tgt):
    """Replace {{table}} tokens with the target's physical reference."""
    import re
    mode = tgt["read_mode"]

    def repl(m):
        t = m.group(1).strip().lower()
        if mode == "read_parquet":
            return "read_parquet('%s/%s/data_0.parquet')" % (tgt["data_dir"], t)
        if mode == "iceberg_rest":
            return "%s.%s.%s" % (tgt["catalog_alias"], tgt["schema"], t.upper())
        raise ValueError("unknown read_mode %r" % mode)

    return re.sub(r"\{\{([^}]+)\}\}", repl, sql)


def preamble(tgt):
    """SQL run before the timed query. For iceberg-REST: install + attach the catalog. Two
    auth shapes: (1) OAuth (iceberg SECRET + vended creds), or (2) a BYO-creds S3 target
    (MinIO/substrate-B): our own S3 secret + AUTHORIZATION_TYPE/ACCESS_DELEGATION_MODE none.
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
    else:  # OAuth path (substrate A)
        secret = os.environ.get(tgt["secret_env"])
        if not secret:
            raise RuntimeError("env $%s is unset; export the OAuth secret in-terminal before "
                               "running the iceberg target (see PROTOCOL.md)" % tgt["secret_env"])
        lines.append("CREATE OR REPLACE SECRET pol (TYPE ICEBERG, CLIENT_ID '%s', "
                     "CLIENT_SECRET '%s', OAUTH2_SCOPE '%s', OAUTH2_SERVER_URI '%s');"
                     % (tgt.get("client_id", ""), secret, tgt["oauth2_scope"],
                        tgt["oauth2_server_uri"]))
        attach_opts.append("SECRET pol")
    lines.append("ATTACH '%s' AS %s ( %s );"
                 % (tgt["warehouse"], tgt["catalog_alias"], ", ".join(attach_opts)))
    lines += [p.rstrip(";") + ";" for p in tgt.get("pragmas", [])]
    return "\n".join(lines) + "\n"


def _error(text):
    """Pull the real error line out of combined stdout+stderr, skipping the /usr/bin/time -l
    rusage block (always the trailing stderr content)."""
    errs = [ln.strip() for ln in text.splitlines() if "Error" in ln and ln.strip()]
    return errs[-1] if errs else "nonzero exit (no engine Error line captured)"


def version(tgt):
    try:
        v = subprocess.run(tgt.get("arch_prefix", []) + [tgt["bin"], "--version"],
                           capture_output=True, text=True).stdout.strip()
        return v or "duckdb"
    except Exception:
        return "unknown"


def exec_query(tgt, bound_sql, is_iceberg, sink_path, timeout_s):
    """One fresh CLI process: preamble (setup) + one timed query, output serialized to
    sink_path (csv, full consumption). Hard-capped -> DNF on overrun. The preamble output
    goes to /dev/null so ONLY the query result lands in the sink (correct row count)."""
    script = ".timer on\n.mode csv\n.headers on\n.output /dev/null\n"
    script += preamble(tgt)
    script += ".output %s\n" % sink_path
    script += bound_sql.strip().rstrip(";") + ";\n"
    cmd = common.time_argv() + tgt.get("arch_prefix", []) + [tgt["bin"]]
    out, err_txt, rc, timed_out, proc_ms = common.run_capped(cmd, script, None, timeout_s)
    if timed_out:
        return {"wall_ms": None, "setup_ms": None, "proc_ms": proc_ms, "rss": None,
                "rows": None, "ok": False, "dnf": True,
                "err": "DNF: exceeded %ds hard cap (remote over-scan; killed process group)" % timeout_s}
    combined = out + "\n" + err_txt
    reals = common.parse_reals(combined)
    rss = common.parse_rss(combined)
    # timer lines in order: [install, load, create_secret, attach, (pragmas), query];
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
    err = None if ok else _error(out + "\n" + err_txt)
    return {"wall_ms": wall_ms, "setup_ms": setup_ms, "proc_ms": proc_ms,
            "rss": rss, "rows": rows, "ok": ok, "dnf": False, "err": err}
