#!/usr/bin/env python3
"""Four-way A/B summary: duckdb / fluree-main / fluree-#1528 / fluree-builder-branch,
both cache modes, with binary provenance. Reads all substrate-B jsonl and prints a
per-pair table. wall = median of ok reps (rep1 shown separately for cold-catalog).
Usage: python summarize_ab.py <jsonl...>"""
import json, sys, statistics as st

rows = []
for f in sys.argv[1:]:
    try:
        rows += [json.loads(l) for l in open(f)]
    except FileNotFoundError:
        pass

pairs = ["p1_count_fact", "p2_category_rollup", "p3_open_tickets_by_segment"]
# group key: (engine label, substrate/version)
def label(r):
    if r["engine"] == "duckdb":
        return "duckdb (%s)" % r.get("substrate", "")
    return "fluree %s" % r.get("engine_version", r.get("target", ""))

for mode in ("cold", "warm"):
    print("\n===== mode=%s =====" % mode)
    print("%-40s %-26s %6s %10s %10s %9s %6s" %
          ("engine/version", "pair", "rows", "wall_med", "rep1", "rss_MB", "ok"))
    seen = sorted({label(r) for r in rows if r["mode"] == mode})
    for lab in seen:
        for p in pairs:
            rs = [r for r in rows if r["mode"] == mode and label(r) == lab
                  and r["pair_id"] == p and r["ok"]]
            if not rs:
                bad = [r for r in rows if r["mode"] == mode and label(r) == lab and r["pair_id"] == p]
                if bad:
                    print("%-40s %-26s %6s %10s %10s %9s %6s" % (lab[:40], p, "-", "DNF/err", "-", "-", "F"))
                continue
            walls = [r["wall_ms"] for r in rs]
            rss = st.median([r["peak_rss_bytes"] for r in rs]) / 1e6
            wmed = st.median(walls[1:] if len(walls) > 1 else walls)
            setup = [r.get("extra", {}).get("setup_ms") for r in rs if r.get("extra", {}).get("setup_ms")]
            smed = (" +%.0f" % st.median(setup)) if setup else ""
            print("%-40s %-26s %6d %9.0f%s %10.0f %9.0f %6s" %
                  (lab[:40], p, rs[0]["rows"], wmed, smed, walls[0], rss, "T"))
