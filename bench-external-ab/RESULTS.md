# RESULTS — external-engine A/B (Fluree and DuckDB, the first external engine)

All rows follow `PROTOCOL.md`. Every number below is a median (or DNF verdict) over committed raw per-run rows in `results/` — see `results/README.md` for the file-per-section map, and `summarize_ab.py` to re-derive any cell. Substrate B (local MinIO + Iceberg REST fixture, unpartitioned SF0.1) is the engine-pure headline substrate; harness v0.3 (fresh-process/rep, peak RSS, full-result-drain); N=5, median of steady-state reps 2..N with rep1 (cold-catalog first touch) noted separately. DuckDB's comparable cold number is `wall + setup` (query + ATTACH/OAuth). Engines: DuckDB v1.5.5 (`arch -arm64`); Fluree shipped-main and Fluree-with-#1528 (filter-over-join fusion), git-commit-stamped per row. Formatting: one paragraph per line.

## Pair legend

- `p1_count_fact` — bare `COUNT(*)` on FACT_ORDER (metadata-vs-metadata: DuckDB iceberg manifest stats vs Fluree count-manifest shortcut). Corpus q036.
- `p2_category_rollup` — FACT_ORDER_LINE ⋈ DIM_PRODUCT, units by category, `is_current` filter, integer SUM. Shape ~ corpus q012.
- `p3_open_tickets_by_segment` — FACT_SUPPORT_TICKET ⋈ DIM_CUSTOMER, open (`status != 'Closed'`) tickets by segment. FILTER-over-join shape; the discriminating pair for #1528 fusion.

---

## Wave A — clean p1/p2 confirmation (substrate B, SF0.1)

STATUS: DONE (2026-07-30, load1 7.7-9.0 across the three legs, stamped). p1/p2 re-run clean on substrate B — DuckDB + both Fluree binaries, both cache modes, N=5. These replace the load-contaminated baseline rows (kept below for the record). p3 is noise-immune (its fused-vs-declined delta is ~27-76x) and already stands from 2026-07-29. Raw: `results/waveA.jsonl` (clean p1/p2); the 2026-07-29 baseline four-way (incl. the standing p3) is `results/{substrate_b_cold,substrate_b_warm,main_ab,1528_ab}.jsonl`.

### Wave A clean rows (2026-07-30) — median of steady-state reps 2..5 / peak RSS

TRUE-COLD (primary; Fluree cache cleared per rep; DuckDB fresh process). DuckDB shown as `wall+setup` (query + ATTACH). p95 over all 5 reps in parens:

| pair | DuckDB (wall+setup) | fluree-main-shipped | fluree-#1528-fused |
|---|---|---|---|
| p1_count_fact | 220 ms / 48 MB (p95 170+) | 229 ms / 34 MB (p95 322) | 240 ms / 34 MB (p95 343) |
| p2_category_rollup | 294 ms / 63 MB (p95 220+) | 559 ms / 106 MB (p95 571) | 516 ms / 103 MB (p95 564) |

WARM (secondary; Fluree disk cache persists; DuckDB has no cross-process cache so warm = cold):

| pair | DuckDB (=cold) | fluree-main-shipped | fluree-#1528-fused |
|---|---|---|---|
| p1_count_fact | ~220 ms / 48 MB | 2.2 ms / 22 MB | 2.1 ms / 22 MB |
| p2_category_rollup | ~294 ms / 63 MB | 140 ms / 73 MB | 124 ms / 72 MB |

Correctness (both engines, all reps): p1 COUNT=180000 (1 row); p2 top category Beauty=346602 over 10 categories. main and #1528 are equal within noise on p1/p2 (the #1528 fix is targeted to filter-over-join; p1/p2 carry no such filter — no regression; the clean pass tightens the prior contaminated p2 #1528 = 628 ms to 516 ms).

CLEAN MEASUREMENTS: p1 (bare COUNT, metadata-vs-metadata) — near-tie cold (DuckDB 220 ms vs Fluree ~230 ms; DuckDB faster by ~1.04x), Fluree faster WARM (2 ms vs ~220 ms via the count-manifest shortcut #1478; DuckDB re-attaches every fresh process). p2 (join rollup, integer SUM) — DuckDB faster COLD by ~1.8x (294 ms vs ~516-559 ms), Fluree faster WARM by ~2x (124-140 ms vs ~294 ms). Fluree carries ~1.5-2x the RSS on p2 (106 vs 63 MB cold). The comparison is cache-protocol-dependent for p1/p2 (per PROTOCOL.md §2, TRUE-COLD is primary).

The prior baseline p1/p2 rows (load-contaminated, loadavg ~19) are retained below for provenance.

### Baseline four-way (2026-07-29, prior run — p1/p2 rows are LOAD-CONTAMINATED, being replaced)

TRUE-COLD (primary; Fluree cache cleared per rep; DuckDB fresh process). median wall / peak RSS:

| pair | DuckDB (wall+setup) | fluree-main-shipped | fluree-#1528-fused |
|---|---|---|---|
| p1_count_fact | 279 ms / 48 MB | 214 ms / 34 MB | 211 ms / 34 MB |
| p2_category_rollup† | 402 ms / 62 MB | 512 ms / 109 MB | 628 ms / 106 MB |
| p3_open_tickets_by_segment | 352 ms / 64 MB | **12600 ms / 785 MB** | **473 ms / 175 MB** |

WARM (secondary; Fluree disk cache persists; DuckDB has no cross-process cache so warm = cold):

| pair | DuckDB (=cold) | fluree-main-shipped | fluree-#1528-fused |
|---|---|---|---|
| p1_count_fact | ~200 ms / 48 MB | 2 ms / 22 MB | 2 ms / 22 MB |
| p2_category_rollup† | ~260 ms / 62 MB | 144 ms / 75 MB | 172 ms / 75 MB |
| p3_open_tickets_by_segment | ~220 ms / 64 MB | **10585 ms / 576 MB** | **139 ms / 147 MB** |

† p1/p2 captured under heavy sibling load (loadavg ~19) — LOAD-CONTAMINATED; Wave A replaces them. All rows correctness-matched (p1 COUNT=180000; p2 top Beauty=346602 over 10 categories; p3 Consumer/Enterprise/SMB = 10788/2639/2683).

KEY MEASUREMENT (protocol-invariant): #1528 filter-over-join fusion reduces p3 from shipped-main's 12.6 s cold / 10.6 s warm (~600-785 MB) to 473 ms cold / 139 ms warm (147-175 MB) — ~27x cold / ~76x warm faster, ~5x leaner. Against DuckDB (352 ms cold / ~220 ms warm), fused Fluree p3 is a near-tie cold and faster warm; on shipped-main (without the fusion) Fluree is ~43x slower than DuckDB on p3 cold — a named gap that the #1528 engine work closes. main and #1528 are identical on p1/p2 (the fix is targeted; no regression).

---

## Wave B — partitioned-copy probe (substrate B)

STATUS: DONE (2026-07-30). FACT_WEB_EVENT (1,000,000 rows) written to local MinIO PARTITIONED two ways — an identity int month-bucket (namespace DW_SF01_PART) and a genuine iceberg **month-transform** spec `month(6)` (DW_SF01_PART_T) — each landing 252 per-partition data files (a real many-small-files layout). Probed from DuckDB v1.5.5. `#1568` comment posted with these verdicts.

### Probe results (DuckDB, local MinIO iceberg, `arch -arm64`)

| probe | layout | threads | outcome | wall |
|---|---|---|---|---|
| grouped-agg scan (COUNT by event_type) | identity 252-file | default | OK (6 groups, sum 1,000,000) | 0.158 s |
| grouped-agg scan | identity 252-file | 1 | OK | 0.771 s |
| grouped-agg scan | month-transform 252-file | default | OK | 0.125 s |
| fact⋈dim join, `join_filter_pushdown` ON | identity 252-file | default | OK (10 categories), NO bloom error | 0.095 s |
| fact⋈dim join, pushdown ON | month-transform 252-file | default | OK, NO bloom error | 0.074 s |
| fact⋈dim join, pushdown FORCE-ON (`disabled_optimizers=''`) | month-transform 252-file | default | OK, NO bloom error | — |

### #1568 verdicts

FINDING 1 (multi-file fact-scan fails/stalls) → **Snowflake-managed-S3-specific.** The exact grouped-aggregate scan shape that failed with "Could not connect to server" (default threads) and did not complete at threads=1 within ~110 s on substrate A runs fine and sub-second on a 252-file partitioned LOCAL table at BOTH default threads (0.16 s) and threads=1 (0.77 s). Many-small-files partitioning is NOT the cause; the trigger is the parallel-S3-GET connection behavior against Snowflake-managed storage. This narrows the upstream report exactly as #1568 anticipated.

FINDING 2 (bloom-filter JOIN gap) → **NOT tied to the partitioned multi-file layout; also Snowflake-managed-substrate-specific.** This CORRECTS the prior hypothesis (PREP §10 / #1568 body: "tied to the partitioned multi-file scan's filter-index mapping"). The `Can't convert TableFilterType (BLOOM_FILTER) from global to local indexes` error does NOT reproduce on ANY local partitioned layout tested — neither the identity 252-file nor the genuine iceberg month-transform 252-file table — with `join_filter_pushdown` ON (default) or force-on. Combined with the earlier unpartitioned-local result (also no error), the bloom gap is specific to the Snowflake-managed iceberg path (remote/vended-cred metadata or Snowflake's exact partition/metadata encoding), not partitioning per se. Both substrate-A failures therefore point the same way: DuckDB reads partitioned LOCAL iceberg (incl. a date-transform spec) fine; the wall is specifically Snowflake-managed storage.

---

## Wave C — pair-set widening (the core)

STATUS: DONE (2026-07-30). Correctness gate PASSES all 10 pairs; then the full timed set on an UNCONTESTED cloud host (see host identity). Cold is the primary mode (fresh process/rep); warm is the reused-disk-cache secondary. N=5 (median of steady-state reps 2..5). DuckDB shown as `wall+setup`. Both Fluree binaries git-commit-stamped. Raw: `results/waveC_ec2.jsonl` (250 rows = 50 DuckDB-cold + 100 fluree-main + 100 fluree-#1528; DuckDB has no separate cross-process warm mode, so 250 is the full count).

### Host identity (Wave C primary)

Uncontested AWS `c7g.4xlarge` (16 vCPU AWS Graviton3, aarch64, 32 GB), Amazon Linux 2023.12 (kernel 6.1.176 aarch64). DuckDB v1.5.5 (Variegata) `d8cdaa33fd`, official linux-arm64 binary, sha256 `9882c99a9804407d…` — NATIVE arm64 (no macOS/Rosetta caveat; the `arch -arm64` prefix is empty here). Fluree: shipped-main `5598ffd6a` (4.1.4) and #1528-content `c81862d2a` (4.1.3), both release builds from source on the host. Substrate B: local MinIO + `apache/iceberg-rest-fixture`, 16 tables unpartitioned, SF0.1.

### Full table (median ms cold / warm; RSS MB). DuckDB = wall+setup.

| pair (shape) | DuckDB w+s / RSS | fluree-main cold / warm / RSS | fluree-#1528 cold / warm | shipped gap vs DuckDB (cold) | #1528 gap |
|---|---|---|---|---|---|
| cq038 filtered COUNT | 50 / 57 | **74155** / 68635 / 282 | **99** / 72 | 1483x | 2.0x |
| cq014 fused COUNT+SUM | 56 / 64 | 80 / 57 / 56 | 81 / 58 | 1.4x | 1.4x |
| cq027 fused 1M grouped COUNT | 77 / 62 | 468 / 425 / 183 | 468 / 427 | 6.1x | 6.1x |
| cq008 fused fact→dim→dim rollup | 128 / 95 | 628 / 572 / 147 | 614 / 567 | 4.9x | 4.8x |
| cq040 VALUES/IN | 60 / 65 | 193 / 162 / 161 | 197 / 151 | 3.2x | 3.3x |
| cq016 OPTIONAL left-join | 83 / 84 | **30475** / 31520 / 657 | **1020** / 1020 | 367x | 12.3x |
| cq046 DESC top-k (tiebreak) | 66 / 66 | 412 / 386 / 253 | 403 / 376 | 6.2x | 6.1x |
| crt_join_reorder 4-table join | 144 / 104 | **132090** / 129525 / 821 | **129395** / 122625 | 917x | 899x |
| crt_minmax MIN/MAX | 59 / 62 | 254 / 236 / 199 | 156 / 131 | 4.3x | 2.6x |
| crt_highcard high-card GROUP BY | 238 / 156 | 3355 / 3330 / **1206** | 3395 / 3325 | 14.1x | 14.3x |

All cells correctness-matched (Wave C gate). On this uncontested host DuckDB leads every pair cold; cq014 is a near-tie (warm 58 vs 56 ms). The compute-bound gaps (cq038, cq016, crt_join_reorder) show no cold→warm speedup, confirming they are engine-compute limits, not I/O.

### Predictions vs outcomes (against R1 §3.7 pre-registered map)

HELD: q038 filtered COUNT — DuckDB-win predicted, shipped 1483x (held; but see the #1528 finding). q040 VALUES/IN — DuckDB-win predicted, 3.2x (held). q016 OPTIONAL — DuckDB-win predicted, shipped 367x (held). Multi-FACT aggregate join (crt_join_reorder) — DuckDB-win predicted for the uncovered class, 917x (held, the largest gap). q014 fused single-table — Fluree-competitive predicted, 1.4x near-tie (held).

WRONG / OPTIMISTIC: q027 (fused 1M grouped) and q008 (fused rollup) were predicted Fluree-competitive/winning; OUTCOME DuckDB leads 6.1x / 4.9x — fused aggregation streams past the per-row ceiling but the vectorized engine still leads at these sizes. q046 DESC top-k predicted Fluree-competitive (99.9% file prune); OUTCOME DuckDB 6.2x ahead (fluree top-k ~400 ms vs 66 ms). MIN/MAX was predicted to decline to full-materialize (a large gap); OUTCOME only 4.3x (fluree ~254 ms, NOT a full-scan decline) — the prediction of a huge MIN/MAX gap was wrong.

NEW (not in R1's map): the #1528 fusion is BROADER than the filter-over-join p3 case it was scoped as. It also collapses cq038 (single-table doubly-constrained COUNT: 74155→99 ms, ~750x) and cq016 (OPTIONAL: 30475→1020 ms, ~30x). It does NOT touch crt_join_reorder (the multi-FACT join). main==#1528 on all fused/relational pairs (no regression).

### NAMED GAPS (DuckDB leads shipped-fluree beyond noise: shape / ratio / mechanism)

1. crt_join_reorder — 4-table fact⋈fact⋈dim⋈dim + integer SUM, two selective dim constraints. **917x shipped / 899x #1528** (~130 s vs 144 ms), RSS 821 MB vs 104 MB. Mechanism: the multi-FACT aggregate join is uncovered — Fluree materializes the full join into per-row RDF bindings before aggregating; not fused, and #1528's filter-over-join fusion does not apply. The single largest, still-open gap (A3 gap #6). Cache-invariant (compute-bound).
2. cq038 — single-table filtered COUNT (`Customer WHERE isCurrent`). **1483x shipped → 2.0x with #1528** (74 s → 99 ms). Mechanism: shipped materializes ~390k rows to count matches; the #1528 family-C fusion streams the count. Fix pending merge.
3. cq016 — OPTIONAL (LEFT JOIN) fact→fact, unordered LIMIT. **367x shipped → 12.3x with #1528** (30 s → 1.0 s). Mechanism: OPTIONAL swallows the LIMIT budget → full fact+fact scan on shipped; #1528 largely closes it.
4. crt_highcard — high-cardinality GROUP BY (259k groups over 1M rows). **14x both binaries; RSS 1206 MB vs 156 MB (8x)**. Mechanism: per-row RDF-binding materialization of the 1M-row scan + 259k-row grouped output at the ~56k rows/s ceiling; memory-heavy. Unaffected by #1528. (The memory_limit-pinned spill sub-variant was not separately run; the 8x RSS disparity at normal limits already shows the memory-fairness story.)
5. cq027 / cq046 / cq008 / cq040 — fused/pruned relational shapes, **3.2–6.2x cold**. Mechanism: even fused, DuckDB's vectorized scan-agg / top-k / IN engine leads at these sizes; Fluree is competitive (low-hundreds of ms) but trails.

Where Fluree LEADS: none cold on this uncontested host — DuckDB leads or ties every pair (cq014 near-tie). Note the host dependence: on the LOCAL contended host, Fluree WARM beat DuckDB on the bare-COUNT / metadata pairs (count-manifest shortcut #1478, and DuckDB re-attaches per fresh process); on this fast uncontested instance DuckDB's ATTACH is cheap enough that it leads cold. Both are honest, disclosed regimes.

### Secondary: local-host (contended) legs — SUPERSEDED, not mixed

An earlier partial Wave C ran on the local macOS host under heavy sibling load (load1 30–146), which inflated absolute ms; those rows are NOT merged into the table above. Directionally they agreed with the uncontested EC2 numbers: cq038 ~54 s shipped / crt_join_reorder ~90 s shipped, #1528 fixing cq038 (~0.45 s) and cq016, not fixing crt_join_reorder. The EC2 uncontested run is authoritative; the contended local rows are retained only as `results/waveC_{main,1528}.jsonl` provenance.

---

## Wave D — scale-up (SF=1, ~27.5M fact rows)

STATUS: DONE (2026-07-30). SF=1 generated on the same uncontested `c7g.4xlarge` (host identity as Wave C; DuckDB v1.5.5 `d8cdaa33fd`, fluree `5598ffd6a` + `c81862d2a`), loaded into MinIO namespace DW_SF1, unpartitioned. Fact magnitudes: FACT_WEB_EVENT 10,000,000; FACT_ORDER_LINE 6,000,000; FACT_INVENTORY 3,000,000; FACT_GL 2,500,000; FACT_PAYMENT 2,000,000; FACT_ORDER 1,800,000; FACT_SHIPMENT 1,800,000; FACT_SUPPORT_TICKET 400,000 (~27.5M fact rows total; 10× SF0.1). Raw: `results/waveD_ec2.jsonl`.

### Row accounting (153 rows vs Wave C's 250 — by design, to bound instance-hours)

At 10× data the compute-bound pairs run minutes each, so the SF=1 timed set was scoped: DuckDB all-10 cold N=5 (50); a FAST group of 7 scan/agg pairs on fluree-main cold+warm N=5 (70) + fluree-#1528 cold N=3 parity (21); a SLOW group of 3 (cq038, cq016, crt_join_reorder) on both binaries cold N=2, DNF cap 200 s (12) — the DNF-vs-fixed verdict needs no N=5. Not run at SF=1 (deliberate): fluree-#1528 warm, and warm for the slow group. Those cells read "n/a" below, distinct from "DNF" (ran, exceeded the 200 s cap).

### SF=1 table (median ms cold / warm; RSS MB). DuckDB = wall+setup.

| pair | DuckDB w+s / RSS | fluree-main cold / warm / RSS | fluree-#1528 cold | gap main/DuckDB | SF0.1→SF1 ratio delta |
|---|---|---|---|---|---|
| cq038 filtered COUNT | 57 / 58 | **71750** / n/a / 366 | **100** | 1259x (main) / 1.75x (#1528) | 1483x→1259x shipped; fix HOLDS |
| cq014 fused COUNT+SUM | 84 / 97 | 534 / 497 / 224 | 543 | 6.4x | 1.4x→6.4x (WIDENED) |
| cq027 fused 1M→10M grouped | 87 / 86 | 2695 / 2660 / 1729 | 2555 | 31x | 6.1x→31x (WIDENED) |
| cq008 fused rollup | 231 / 149 | 1570 / 1520 / 261 | 1615 | 6.8x | 4.9x→6.8x |
| cq040 VALUES/IN | 94 / 111 | 1480 / 1520 / 496 | 1670 | 15.7x | 3.2x→15.7x (WIDENED) |
| cq016 OPTIONAL | 140 / 227 | **DNF (>200s)** / n/a | **22340** | main DNF / #1528 160x | 367x→DNF shipped; #1528 completes |
| cq046 DESC top-k | 113 / 107 | 3820 / 3770 / 1515 | 4075 | 34x | 6.2x→34x (WIDENED) |
| crt_join_reorder 4-table join | 321 / 239 | **DNF (>200s)** | **DNF (>200s)** | DNF both | 917x→DNF WALL |
| crt_minmax MIN/MAX | 96 / 94 | 2325 / 2345 / 1107 | 1375 | 24x (main) / 14x (#1528) | 4.3x→24x (WIDENED) |
| crt_highcard high-card GROUP BY | 304 / 512 | 15820 / 15700 / **4438** | 15570 | 52x; RSS 8.7x | 14x→52x; RSS 8x→8.7x |

DuckDB scaled sub-linearly (all pairs still 57–321 ms cold-comparable at 10× data; RSS ≤ 512 MB). Fluree scaled ~linearly with rows (per-row RDF materialization), so absolute walls grew ~10× and RSS grew steeply (crt_highcard 4.4 GB). "n/a" = a mode intentionally not run at SF=1; "DNF" = ran, exceeded the 200 s cap.

### SCALE-DELTA reading (which Wave C verdicts changed at 27M rows — the payoff)

1. The fused/scan-heavy family's 3–6x SF0.1 tail WIDENED to ~6–52x at SF1. Every scan-bound pair moved the same direction (cq014 1.4→6.4x, cq027 6.1→31x, cq040 3.2→15.7x, cq046 6.2→34x, crt_minmax 4.3→24x, crt_highcard 14→52x). Mechanism: DuckDB's vectorized engine scales sub-linearly while Fluree pays per-row RDF-binding materialization at a ~fixed rows/s ceiling, so the ratio grows ~5–6x per 10x of data. This is the headline scale finding: Fluree's relative disadvantage on relational scan/agg shapes GROWS with scale, it does not hold constant.
2. The multi-FACT aggregate join (crt_join_reorder) went from 917x-but-completes (130 s) at SF0.1 to a HARD WALL at SF1 — DNF (>200 s) on BOTH binaries — while DuckDB stayed at 321 ms. This is the sharpest delta: the uncovered join class stops completing at 27M rows.
3. #1528's fusion becomes MORE valuable at scale, not less. cq038 stays ~100 ms with #1528 while shipped-main is ~72 s (a ~720x separation preserved); cq016 shipped-main DNFs at SF1 while #1528 completes in ~22 s. At scale the fusion is the difference between "runs" and "does not finish" for the filtered-COUNT and OPTIONAL shapes.
4. RSS shapes did NOT hold — they widened. Fluree's peak RSS scaled with row volume (crt_highcard 1.2 GB→4.4 GB, cq027 0.18→1.7 GB, cq046 0.25→1.5 GB) while DuckDB stayed ≤ 512 MB. The memory-fairness gap is a scale phenomenon.
5. New DNFs introduced by scale: crt_join_reorder (both binaries) and cq016 (shipped-main). These are exactly the pairs that were "large ratio but completed" at SF0.1; at SF1 they cross into non-completion under a 200 s cap.

Net: scale does not merely amplify the SF0.1 ratios uniformly — it changes KIND for two pairs (large-ratio → DNF) and confirms that Fluree's virtual-path gaps on scan/agg/join shapes are scale-sensitive, while #1528's fusion and Fluree's fused single-table aggregates remain the bright spots (cq038 fixed; fused pairs still low-single-digit-seconds at 10M–27M rows).
