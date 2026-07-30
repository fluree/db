# RESULTS — DuckDB-vs-Fluree A/B

All rows follow `PROTOCOL.md`. Substrate B (local MinIO + Iceberg REST fixture, unpartitioned SF0.1) is the engine-pure headline substrate; harness v0.3 (fresh-process/rep, peak RSS, full-result-drain); N=5, median of steady-state reps 2..N with rep1 (cold-catalog first touch) noted separately. DuckDB's comparable cold number is `wall + setup` (query + ATTACH/OAuth). Engines: DuckDB v1.5.5 (`arch -arm64`); Fluree shipped-main and Fluree-with-#1528 (filter-over-join fusion), git-commit-stamped per row. Formatting: one paragraph per line.

## Pair legend

- `p1_count_fact` — bare `COUNT(*)` on FACT_ORDER (metadata-vs-metadata: DuckDB iceberg manifest stats vs Fluree count-manifest shortcut). Corpus q036.
- `p2_category_rollup` — FACT_ORDER_LINE ⋈ DIM_PRODUCT, units by category, `is_current` filter, integer SUM. Shape ~ corpus q012.
- `p3_open_tickets_by_segment` — FACT_SUPPORT_TICKET ⋈ DIM_CUSTOMER, open (`status != 'Closed'`) tickets by segment. FILTER-over-join shape; the discriminating pair for #1528 fusion.

---

## Wave A — clean p1/p2 confirmation (substrate B, SF0.1)

STATUS: DONE (2026-07-30, load1 7.7-9.0 across the three legs, stamped). p1/p2 re-run clean on substrate B — DuckDB + both Fluree binaries, both cache modes, N=5. These replace the load-contaminated baseline rows (kept below for the record). p3 is noise-immune (its fused-vs-declined delta is ~27-76x) and already stands from 2026-07-29.

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

CLEAN VERDICTS: p1 (bare COUNT, metadata-vs-metadata) — near-tie cold (DuckDB 220 ms vs Fluree ~230 ms; DuckDB marginally ahead by ~1.04x), Fluree wins WARM decisively (2 ms vs ~220 ms via the count-manifest shortcut #1478; DuckDB re-attaches every fresh process). p2 (join rollup, integer SUM) — DuckDB wins COLD ~1.8x (294 ms vs ~516-559 ms), Fluree wins WARM ~2x (124-140 ms vs ~294 ms). Fluree carries ~1.5-2x the RSS on p2 (106 vs 63 MB cold). Verdict is cache-protocol-dependent for p1/p2 (per PROTOCOL.md §2, TRUE-COLD is primary).

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

HEADLINE (protocol-invariant, stands): #1528 filter-over-join fusion collapses p3 from shipped-main's 12.6 s cold / 10.6 s warm (~600-785 MB) to 473 ms cold / 139 ms warm (147-175 MB) — ~27x cold / ~76x warm, ~5x leaner. Against DuckDB (352 ms cold / ~220 ms warm), fused Fluree p3 is a near-tie cold and BEATS DuckDB warm. The earlier "43x DuckDB win on p3" was purely the shipped binary lacking filter-over-join fusion. main and #1528 are identical on p1/p2 (the fix is targeted; no regression).

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

## Wave C — pair-set widening (the core, substrate B SF0.1)

STATUS: pending. The full pre-registered pair set (q038/q014/q027/q008/q036/q040/q016/q046 analogs + RT additions: 3+-table join-reorder, MIN/MAX, spill-candidate high-card GROUP BY, high-card GROUP BY at normal limits). Correctness cross-checked first; then substrate B, both fluree binaries + DuckDB, both cache modes, N=5 median+p95.

_full table + PREDICTIONS-vs-OUTCOMES + NAMED-GAPS to be filled_

---

## Wave D — scale-up

STATUS: pending (heavy, last). A larger locally-generated SF (fact tables >= 10M rows total) loaded into MinIO; correctness-checked; the full pair set re-run at scale. The payoff is where verdicts CHANGE vs SF0.1.

_scale table + verdict deltas to be filled_
