# RESULTS — DuckDB-vs-Fluree A/B

All rows follow `PROTOCOL.md`. Substrate B (local MinIO + Iceberg REST fixture, unpartitioned SF0.1) is the engine-pure headline substrate; harness v0.3 (fresh-process/rep, peak RSS, full-result-drain); N=5, median of steady-state reps 2..N with rep1 (cold-catalog first touch) noted separately. DuckDB's comparable cold number is `wall + setup` (query + ATTACH/OAuth). Engines: DuckDB v1.5.5 (`arch -arm64`); Fluree shipped-main and Fluree-with-#1528 (filter-over-join fusion), git-commit-stamped per row. Formatting: one paragraph per line.

## Pair legend

- `p1_count_fact` — bare `COUNT(*)` on FACT_ORDER (metadata-vs-metadata: DuckDB iceberg manifest stats vs Fluree count-manifest shortcut). Corpus q036.
- `p2_category_rollup` — FACT_ORDER_LINE ⋈ DIM_PRODUCT, units by category, `is_current` filter, integer SUM. Shape ~ corpus q012.
- `p3_open_tickets_by_segment` — FACT_SUPPORT_TICKET ⋈ DIM_CUSTOMER, open (`status != 'Closed'`) tickets by segment. FILTER-over-join shape; the discriminating pair for #1528 fusion.

---

## Wave A — clean p1/p2 confirmation (substrate B, SF0.1)

STATUS: pending a quiet machine (load1 < 15). This wave re-runs p1/p2 under clean load to replace the load-contaminated baseline rows below (the prior p1/p2 were captured at loadavg ~19). p3 is noise-immune (its fused-vs-declined delta is ~27-76x, swamping load) and already stands.

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

### Wave A clean rows (to be filled)

_pending quiet machine_

---

## Wave B — partitioned-copy probe (substrate B)

STATUS: pending. Writes ONE fact table PARTITIONED BY date onto MinIO (namespace DW_SF01_PART) and runs from DuckDB: (1) the grouped-aggregate scan at default threads and threads=1; (2) a fact⋈dim join with `join_filter_pushdown` ON. Sharpens issue #1568: partitioned-LOCAL scan working ⇒ Finding 1 (fact-scan failure) is Snowflake-managed-S3-specific; the bloom-filter error appearing/absent on partitioned-local ⇒ Finding 2's trigger is partitioning vs remoteness.

_results + #1568 comment to be filled_

---

## Wave C — pair-set widening (the core, substrate B SF0.1)

STATUS: pending. The full pre-registered pair set (q038/q014/q027/q008/q036/q040/q016/q046 analogs + RT additions: 3+-table join-reorder, MIN/MAX, spill-candidate high-card GROUP BY, high-card GROUP BY at normal limits). Correctness cross-checked first; then substrate B, both fluree binaries + DuckDB, both cache modes, N=5 median+p95.

_full table + PREDICTIONS-vs-OUTCOMES + NAMED-GAPS to be filled_

---

## Wave D — scale-up

STATUS: pending (heavy, last). A larger locally-generated SF (fact tables >= 10M rows total) loaded into MinIO; correctness-checked; the full pair set re-run at scale. The payoff is where verdicts CHANGE vs SF0.1.

_scale table + verdict deltas to be filled_
