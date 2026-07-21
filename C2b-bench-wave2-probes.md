# C2b — Empirical bench, wave 2: A3 coverage-gap probe battery (SPARQL survey + JSON-LD cross-surface)

Scope: the live-measured probe half of the perf audit, isolating each suspected optimization-admission gap with ONE query. Audit tip `10e073fe9`. Companion to C2 (corpus bench) and A2/A3 (strategy/coverage matrices — the probes fill A3's [BENCH-PENDING] slots F-AUD-5…F-AUD-11). Labels: PREDICTED = probes.md code-grounded hypothesis; MEASURED = survey/runner telemetry; INFERRED = my reasoning. One paragraph per line.

## 0. Methodology (MEASURED)

Two surfaces, both against `virtual-sf01` (the scale-matched live Snowflake Iceberg target), executed via the bench harness (wave-2), analyzed here. SPARQL survey: `vbench run --corpus-dir wave2-probes/probes-corpus --targets virtual-sf01 --survey --virtual-reps 1` — 21 probes (18 gap isolators + 3 controls), each carrying `FROM <enterprise-sf01-v:main>` so it routes through the DatasetOperator/`query_from` path (the deployed solo chat shape); informational (never gates). JSON-LD surface: the untracked `jsonld_runner` bin (`db-audit/fluree-bench-virtual/src/bin/jsonld_runner.rs`) runs FQL bodies for probes 02/03/05/09 via `Fluree::query_from().jsonld()` (auto `with_r2rml`), printing wall + spans. Telemetry reading (the vbench way): a fast path FIRED ⇒ `files_selected=0`, `r2rml.scan_table`/`iceberg.scan_plan` absent (or `count_manifest` present for COUNT); a scan RAN ⇒ `files_selected>0` + `r2rml.scan_table` present; PRUNING worked ⇒ `files_pruned>0`; FUSION worked ⇒ few scans + no full materialize. For a control sibling (`-b`), the delta vs its probe IS the gap.

## 1. Per-probe predicted vs observed (MEASURED fills the right columns)

Predicted signatures are from probes.md (verified against the mapping at tip). Observed = the survey record's counters. VERDICT = does the gap reproduce.

Survey run 2026-07-18 (bench harness, SURVEY_EXIT=0). Warm on-disk cache (walls are warm; the prune/fuse SIGNATURE — files_pruned, scan_table n, parquet_read n — is cache-independent and is the load-bearing signal). `parq` = iceberg.parquet_read n (files actually read); `fprun` = files_pruned.

```
probe  gap (F-AUD)                 PREDICTED                       OBSERVED (wall/scan/fsel/fprun/parq/rows)   VERDICT
p02  F-AUD-5 IN not lowered      fp=0, full FACT_ORDER scan      0.62s scan=2 fp=0 parq=7671 rows=1100        CONFIRMED (IN never lowered; full scan)
p03  F-AUD-5 scalar VALUES       full GL_JOURNAL scan, maybe DNF 0.56s scan=1 fp=0 parq=7670 rows=0           CONFIRMED (VALUES not lowered, full scan; 0 rows = probe acct-name mismatch, not a gap)
p01  F-AUD-6 ASC top-k           fp=0 vs q046 DESC              0.28s scan=1 fp=0 parq=7670 rows=10          CONFIRMED (no ASC top-k; full 7670-file scan for LIMIT 10)
p12  F-AUD-6 expr ORDER BY       fp=0, no top-k                 0.41s scan=1 fp=0 parq=7670 rows=10          CONFIRMED (expr ORDER BY kills top-k; full scan)
p15  F-AUD-6 deep-OFFSET ASC     wall independent of OFFSET     0.28s scan=1 fp=0 parq=7670 rows=20          CONFIRMED (full sort/scan)
p04  F-AUD-7 OPTIONAL budget     window not capped vs 04b       DNF 120s scan=163 fp=0 parq=1,238,908 rows=0 CONFIRMED — EXTREME (OPTIONAL swallows LIMIT; 1.24M reads → DNF)
p04b control plain LIMIT         LIMIT caps the window          0.02s scan=1 parq=18 rows=50                 CONTROL (plain LIMIT budgets: 18 reads) — delta vs p04 = 68,828x reads
p11  F-AUD-7 DISTINCT budget     full scan for tiny LIMIT 5     0.46s scan=2 fp=0 parq=7671 rows=5           CONFIRMED (DISTINCT swallows LIMIT; full scan for LIMIT 5)
p13  F-AUD-7 ASK budget          emitted ~1 vs full             1.76s scan=1 fp=7670 parq=0 rows=1           BETTER than predicted (ASK PRUNED all 7670 files, 0 reads)
p05  F-AUD-8 COUNT DISTINCT      scan RUNS (no shortcut)        0.22s scan=2 cm=0 parq=7671 rows=1           CONFIRMED (no count_manifest; full scan — contrast q036 cm=1)
p06  F-AUD-8 MIN/MAX             full scan (stats-answerable)   0.22s scan=1 fp=0 parq=7670 rows=1           CONFIRMED (MIN/MAX declines; full 7670-file scan not column stats)
p16  F-AUD-8 HAVING unprojected  scan vs 16b fused             0.27s scan=2 parq=7671 rows=15               PARTIAL (full scan — but 16b ALSO full-scans; gap not cleanly isolated here)
p16b control HAVING projected    fuses                         0.22s scan=2 parq=7671 rows=500             CONTROL declined too (scan=2 parq=7671) — neither fused; see §2
p17  F-AUD-8 GROUP_CONCAT/SAMPLE full materialize              0.32s scan=2 parq=7671 rows=500             CONFIRMED (declines fused; full scan)
p09  F-AUD-9 fact-fact join agg  two full FACT scans, no fold  1.02s scan=2 fp=0 parq=15,340 rows=120,719   CONFIRMED (two full scans, fused fold declined; warm so no DNF)
p07  F-AUD-11 timestamp range    fp=0 on 1M FACT_WEB_EVENT     0.13s scan=1 fp=0 parq=822 rows=5000         CONFIRMED (no timestamp pushdown; 822 reads = LIMIT budget, not prune)
p08  F-AUD-11 decimal const-obj  fp=0 vs 08b FILTER            0.20s scan=1 fp=0 parq=7670 rows=0           CONFIRMED (constant-object decimal does NOT prune; full read)
p08b control decimal FILTER      fp>0 (prunes)                 1.72s scan=1 fp=7670 parq=0 rows=0           CONTROL PRUNES all 7670 (parq=0) — the clean F-AUD-11 delta
p10  coverage MINUS              correctness + negated exec    3.70s scan=3 fsel=3 fp=0 parq=3 rows=5000    RAN (MINUS executes; 5000 rows, 3-file read — no cliff)
p14  bound-subj DESCRIBE crawl   scan n (3 vs 16)              10.16s scan=25 fp=0 parq=69,046 rows=1       CONFIRMED WORSE (DESCRIBE fans out to 25 tables / 69K reads — no bound-subject prune)
p18  F-AUD-7/9 NOT EXISTS        scan n / spans on FACT_PAYMENT 0.70s scan=3 fp=0 parq=23,010 rows=5000     CONFIRMED (anti-join = 3 full scans, not batched)
```

## 2. Gap-hypothesis verdicts (MEASURED)

F-AUD-5 (set-lowering) CONFIRMED on both members and both surfaces: probe-02 (FILTER IN over FACT_ORDER) and probe-03 (scalar VALUES over FACT_GL_JOURNAL) each do a FULL scan (parq=7670/7671, files_pruned=0) — the bounded set is never lowered to an Iceberg IN-prune. This corroborates the C2 corpus (q040/q052 VALUES timeouts) and is the live proof for A3's [BENCH-PENDING probe 02/03] slot. probe-03's 0 rows is a probe-authoring detail (the account-name literals "4000 - Revenue" etc. do not match the data's glAccountName values) — NOT an engine finding, and identical on both surfaces (§3).

F-AUD-6 (ORDER BY asymmetry) CONFIRMED: probe-01 (ASC top-k), probe-12 (expression ORDER BY), probe-15 (deep-OFFSET ASC) all full-scan (parq=7670, files_pruned=0) for tiny LIMITs — none gets the scan-side top-k that DESC q046 gets (q046 read 10 files; these read 7670). The DESC-only top-k + no-budget-for-ASC/expr gap is live.

F-AUD-7 (row-budget swallowing) CONFIRMED, and probe-04 is the single most dramatic datum in the battery: the OPTIONAL variant swallowed its LIMIT 50 entirely and DNF'd at 120 s having issued 1,238,908 Parquet reads (scan_table n=163), while its control probe-04b (plain LIMIT 50) finished in 0.02 s with 18 reads — a 68,828× read-amplification from a single OPTIONAL that fails to forward the row budget. probe-11 (DISTINCT + LIMIT 5) likewise full-scans. Counter-example (a budget that DOES forward): probe-13 (ASK) pruned all 7,670 files to 0 reads — ASK forwards an existence/LIMIT-1 budget that the scan honors, better than predicted.

F-AUD-8 (fused-aggregate admission) CONFIRMED for COUNT-DISTINCT (probe-05: cm=0, full scan — no manifest shortcut, contrast the bare-COUNT q036 cm=1), MIN/MAX (probe-06: full scan of a query column stats could answer), and GROUP_CONCAT/SAMPLE (probe-17: full materialize). One caveat: the HAVING pair (probe-16 unprojected vs 16b projected) did NOT isolate cleanly — BOTH full-scanned (scan=2, parq=7671), so on this corpus neither HAVING form reached the fused path (the control also declined). The HAVING-specific decline claim is therefore NOT confirmed by this probe; the shared decline is consistent with these two-pattern shapes not being single-table fused-rollup candidates at all. Flag for A3: probe-16/16b need a single-table fused-rollup base to isolate HAVING.

F-AUD-9 (fused JOIN admission = linear fact→dim only) CONFIRMED: probe-09 (fact⋈fact Order-Payment aggregate) does two full FACT scans (scan=2, parq=15,340 ≈ 2×7,670) with no fused fold — exactly the multi-FACT-join decline, and it is the corpus blind spot (no corpus member joins two facts in an aggregate). It did not DNF only because the cache was warm; cold-at-scale (the SF20 q016-class regime) it is a DNF candidate.

F-AUD-11 (pushdown type gaps) CONFIRMED with the cleanest control delta in the battery: probe-08 (constant-object decimal) does NOT prune (files_pruned=0, parq=7670 full read) while its control probe-08b (the FILTER form of the same decimal bound) PRUNES ALL 7,670 files (files_pruned=7670, parq=0). Same value, same column — the FILTER form reaches pruning, the constant-object form does not. probe-07 (timestamp range) also confirms no timestamp pushdown (files_pruned=0; the 822 reads are the LIMIT budget stopping early, not pruning).

Extra: probe-14 (DESCRIBE) is WORSE than predicted — it fans out to 25 tables / 69,046 Parquet reads (the ~69K max fan-out also seen in the C2 exploration family q056-q059) with no bound-subject prune, 10.2 s even warm. probe-18 (NOT EXISTS) does 3 full scans (parq=23,010) — the anti-join is not batched. probe-10 (MINUS) executes correctly (5,000 rows) with only 3 reads — no cliff here.

## 3. SPARQL-vs-JSON-LD deltas (probes 02/03/05/09) — SHARED IR ⇒ SHARED ADMISSION (MEASURED)

The JSON-LD (agent/system / `query_from().jsonld()`) surface produces BYTE-IDENTICAL row counts AND identical prune/scan telemetry to the SPARQL survey on all four probes — the shared-IR hypothesis holds, and the 900 s "JSON-LD path silently misses an optimization" worry is REFUTED for these four gap classes (neither surface reaches the optimization; they fail identically):

```
probe   SPARQL (rows / scan / parq)      JSON-LD (rows / scan / parq)     parity
p02   1100 / 2 / 7671                  1100 / 2 / 7671                  IDENTICAL (IN not lowered on either)
p03      0 / 1 / 7670                     0 / 1 / 7670                  IDENTICAL (VALUES not lowered; same empty result)
p05      1 / 2 / 7671                     1 / 2 / 7671                  IDENTICAL (COUNT-DISTINCT full scan on either)
p09 120719 / 2 / 15340              120719 / 2 / 15340                  IDENTICAL (same two-full-scan fact-fact join)
```

This directly resolves the two items flagged at handoff: (1) JSON-LD probe-03's status=ok/rows=0 is NOT a silent-empty class or a smaller scope — it is the same full 7,670-file scan and the same 0 rows as the SPARQL twin (the 0 is the probe's account-name literals not matching data, on BOTH surfaces). (2) JSON-LD probe-09's 120,719 rows / 1.1 s is NOT a weaker single-scan shape or a surface advantage — it is the identical two-full-scan join (scan=2, parquet_read n=15,340) as the SPARQL probe, at the same warm speed. INFERRED: admission for IN-lowering, VALUES-lowering, COUNT-DISTINCT shortcut, and fact-fact fused-fold is decided at the shared IR level, so it is surface-agnostic — fixing any of these once fixes both the SPARQL and the deployed JSON-LD/agent path (and, conversely, the agent path inherits every A3 coverage cliff).

## 4. What the probes say (MEASURED + INFERRED)

Every predicted A3 coverage gap reproduced live except the HAVING-isolation pair (which the corpus shape could not cleanly separate — a probe-design note, not an engine acquittal). The confirmed gaps are: set-lowering (IN/VALUES) never reaching Iceberg prune; ORDER BY asymmetry (DESC-only top-k); row-budget swallowing (OPTIONAL/DISTINCT), whose worst case (probe-04) is a 1.24-million-read DNF against a 0.02 s control; fused-aggregate admission gaps (COUNT-DISTINCT / MIN-MAX / GROUP_CONCAT decline to full materialize); fused-JOIN admission limited to fact→dim (fact⋈fact declines); and pushdown type gaps (constant-object decimal + timestamp never prune, though the FILTER form of the identical decimal prunes 100%). The cliffs are consistent with the C2 corpus tail: probe-04/09/14 are the probe analogues of the corpus q016 crawl / un-fused shapes / q056-q059 exploration fan-out.

The cross-surface result is the load-bearing synthesis for the audit's coverage thesis: these are IR-level admission gaps, not representation, wire-format, or surface problems — the JSON-LD agent path and the SPARQL path hit the SAME cliffs identically (MEASURED, 4/4 probes byte-identical). So closing an admission gap once (e.g. emitting `Expression::In` from a bounded set, or a fact-fact fused fold) lifts both surfaces together; and until then, the deployed solo chat/agent surface inherits every cliff the SPARQL corpus exposes. The one asymmetry worth noting is favorable — probe-13 (ASK) and probe-08b (FILTER decimal) show the scan-side prune/budget machinery WORKS when the admission fires (0 reads); the gap is purely which shapes are ADMITTED to it, which is the A2/A3 coverage story, not an engine-capability ceiling.
