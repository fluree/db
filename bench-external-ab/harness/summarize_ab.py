#!/usr/bin/env python3
"""Per-pair A/B summary across any committed wave JSONL. Groups rows by (engine label,
substrate/version) and cache mode and prints wall = median of steady-state reps 2..N, with
rep1 (the cold-catalog first touch) shown separately, plus peak RSS. Pairs are DISCOVERED
from the input (first-seen order), so it summarizes any wave — Wave-1 p1-p3, Wave-C/D cq*/
crt_* — not a hardcoded set. Provenance (`engine_version`, incl. the binary's git commit)
distinguishes shipped-main from the #1528-fusion build.

Usage: python summarize_ab.py <jsonl...>
  e.g. python summarize_ab.py ../results/waveC_ec2.jsonl
       python summarize_ab.py ../results/waveA.jsonl ../results/substrate_b_cold.jsonl"""
import json
import sys
import statistics as st

rows = []
for f in sys.argv[1:]:
    try:
        rows += [json.loads(l) for l in open(f)]
    except FileNotFoundError:
        pass

# Discover the pair set from the data, preserving first-seen order.
pairs = list(dict.fromkeys(r["pair_id"] for r in rows))


def label(r):
    if r["engine"] == "duckdb":
        return "duckdb (%s)" % r.get("substrate", "")
    return "fluree %s" % r.get("engine_version", r.get("target", ""))


def median_rss_mb(rs):
    vals = [r["peak_rss_bytes"] for r in rs if r.get("peak_rss_bytes")]
    return (st.median(vals) / 1e6) if vals else 0.0


for mode in ("cold", "warm"):
    present = [r for r in rows if r["mode"] == mode]
    if not present:
        continue
    print("\n===== mode=%s =====" % mode)
    print("%-40s %-32s %8s %10s %10s %9s %4s" %
          ("engine/version", "pair", "rows", "wall_med", "rep1", "rss_MB", "ok"))
    for lab in sorted({label(r) for r in present}):
        for p in pairs:
            rs = [r for r in present if label(r) == lab and r["pair_id"] == p and r["ok"]]
            if not rs:
                bad = [r for r in present if label(r) == lab and r["pair_id"] == p]
                if bad:
                    print("%-40s %-32s %8s %10s %10s %9s %4s" %
                          (lab[:40], p, "-", "DNF/err", "-", "-", "F"))
                continue
            walls = [r["wall_ms"] for r in rs]
            wmed = st.median(walls[1:] if len(walls) > 1 else walls)
            setup = [r.get("extra", {}).get("setup_ms") for r in rs
                     if r.get("extra", {}).get("setup_ms")]
            smed = (" +%.0f" % st.median(setup)) if setup else ""
            print("%-40s %-32s %8d %9.0f%s %10.0f %9.0f %4s" %
                  (lab[:40], p, rs[0]["rows"], wmed, smed, walls[0], median_rss_mb(rs), "T"))
