# Virtual-Dataset (Iceberg / R2RML) Performance — Hypothesis Map (H1–H8)

**Date:** 2026-07-10
**Branch:** `bench/virtual-dataset-corpus` (worktree `db-vbench`), at `a5528e880`
**Companion:** `01-pathway-inventory.md` — every mechanism below cites an inventory strategy (§N) and its `[verified file:line]` anchors. Read that first; its §0.1 records the adversarial pass that stress-tested H1, H2, and H5 (H1 and H5 survived unchanged; H2's *scan-cost* claim survived but its ORDER-BY *memory* claim was corrected to a bounded top-k heap — reflected below).

This map states, for each hypothesis: the **mechanism** (which strategy's presence or absence creates the cost), **why it should dominate**, the **benchmark evidence** that would confirm or refute it (with the specific span/counter and the A/B lever), and the **query shapes** that exercise it. The shapes are catalogued in §H0 and referenced by tag (e.g. `Q-BI-ORDER`). Hypotheses are ranked by expected share of wall-time on the BI corpus; H1 and H2 are the two we expect to dominate.

The instrumentation assumed below is the hot-path span set added in `988f44ab3` (`parquet_read`, `scan_table`, `scan_plan`, materialize/window spans). Where a needed counter does not yet exist, it is called out as a **[bench-gap]** — a prerequisite for the WP3/WP6 harness, not evidence we already have.

---

## H0. Query-shape catalogue

The corpus (WP5) will tag every query with the shapes it exercises. The load-bearing shapes:

| Tag | Shape | Primary strategy touched |
|---|---|---|
| `Q-DIM-TYPED` | `GRAPH g { ?s a :Dim ; :col ?v }` — single dimension, typed, no modifier | §2 class fusion, §6 pushdown |
| `Q-DIM-DETAIL` | `GRAPH g { <iri> ?p ?o }` — bound-subject inspector | §7 prefix-prune, §6 template-key |
| `Q-FACT-SCAN` | `GRAPH g { ?s a :Fact ; :measure ?m }` — fact table, no modifier | §6 decode wall |
| `Q-BI-ORDER` | `… ORDER BY ?m LIMIT k` over a fact/dim scan | §5/§12 budget absorb |
| `Q-BI-DISTINCT` | `SELECT DISTINCT … LIMIT k` | §5/§12 budget absorb |
| `Q-BI-GROUP` | `… GROUP BY ?d (agg) LIMIT k` (single table) | §11 fused agg, §5 budget |
| `Q-JOIN-DIM` | `Fact ⋈ Dim` via RefObjectMap, `LIMIT k` | §8 nested-loop, §5 budget |
| `Q-JOIN-GROUP` | `Fact ⋈ Dim GROUP BY dim-attr (agg)` — rollup | §11 fused-agg **decline**, §8 |
| `Q-COUNT-STAR` | `SELECT (COUNT(*) AS ?c) GRAPH g { ?s a :Fact }` | §11 GAP |
| `Q-MONEY-FILTER` | `… FILTER(?amount > 1000)` on a DECIMAL column | §6 decimal blindness |
| `Q-VALUES` | `VALUES ?s { … } GRAPH g { ?s :col ?v }` vs the bound-IRI equivalent | §13 non-lowered |
| `Q-PULL` | Fully-bound-subject star (record pull) | §1 star, §7 prefix-prune |

Scale axis for every shape: **SF01** (dev) vs **SF20** (stress; ~7,670 files/fact-table). Condition axis: **cold** (fresh process, empty disk cache), **hot-process** (moka warm), **warm-disk** (parquet on local disk).

---

## H1 — Fact-scan decode wall

**Mechanism.** §6: the Arrow reader **cannot** apply a row-level filter — `RowSelection.skip_records` panics on Snowflake `DELTA_BINARY_PACKED` columns (parquet-rs 54), so `with_row_filter` is deliberately unused `[arrow_reader.rs:135-143]`. The adversarial pass confirmed `with_row_filter`/`RowSelection` have **zero call sites** in the crate (comments only), so this is unconditional — there is no flagged fast path. Pruning is row-group-granular only `[send_parquet.rs:64-88]`; every **surviving** row group is fully decoded, then masked `[arrow_reader.rs:186-191]`. At SF20 a fact table is ~7,670 files; a selective predicate that survives at row-group granularity still forces a full decode of each surviving group.

**Why it should dominate.** Fact scans are the largest data movement on the path, and decode (not I/O, not materialize) is where the CPU goes when a filter can't shrink the row set pre-decode. Per-file fixed overhead (footer parse, column-chunk setup) compounds ~7,670× at SF20. This is the single most physical cost on the pathway and the hardest to design around.

**Confirm / refute.**
- **Confirm:** `parquet_read` span aggregate bytes-decoded ≫ bytes implied by rows returned; decode-time share of `scan_table` wall > ~60% on `Q-FACT-SCAN`; near-linear scaling of wall with **surviving file count** SF01→SF20 (~20×). **[bench-gap:** need a `rows_decoded` vs `rows_emitted` counter on the reader span.**]**
- **Refute:** wall dominated instead by materialize/emit spans (→ H-materialize, a different fix) or by catalog/`loadTable` (→ H7); or `files_pruned` already near-total so decode is small.

**Shapes.** `Q-FACT-SCAN`, `Q-MONEY-FILTER` (decode wall + H4), any fact-table `Q-BI-*` after the budget is absorbed (H2 feeds H1).

---

## H2 — Budget-swallowing modifiers (the dominant BI shape)

**Mechanism.** §5 + §12: `set_row_budget` is ABSORB-by-default `[operator.rs:98]`, forwarded only through `Limit`/`Offset`/`Project`/`Graph`. `SortOperator`, `DistinctOperator`, `GroupAggregateOperator`, `UnionOperator` have **zero** overrides (grep-confirmed), and `join.rs` absorbs `[join.rs:1114-1127]`. So `ORDER BY` / `DISTINCT` / `GROUP BY` / `UNION` between a `LIMIT` and an R2RML scan turns `LIMIT k` into a **full scan** — the budget never reaches `operator.rs:1957-1985`.

*Adversarial refinement (does not change the ranking):* `ORDER BY … LIMIT` is compiled to a **streaming top-k** heap, not a full sort buffer (`operator_tree.rs:3308-3345`, `sort.rs:544`), so its *memory* is bounded — but top-k must see every input row and forwards no budget, so the **scan is still full**. The native order/limit fast paths are `fast_path_store`-gated (native-index only) and never fire on Iceberg. The four modifiers are **not equally fixable** (inventory §12): `ORDER BY`/`GROUP BY` are genuinely scan-bound (must see all rows), while `DISTINCT`/`UNION` *could* early-terminate but don't — a distinction the roadmap should exploit.

**Why it should dominate.** This is the **most common BI shape**: dashboards issue `ORDER BY measure DESC LIMIT 10`, `SELECT DISTINCT dim LIMIT 50`, `GROUP BY dim … LIMIT`. Each pattern-matches exactly onto a budget-absorbing operator sitting above a fact scan. The user asked for 10 rows; the engine scanned 550M. The 165.7 s `Store ⋈ Geography LIMIT 10` baseline is the signature.

**Confirm / refute.**
- **Confirm:** the same query **with vs without** the modifier (e.g. `… LIMIT 10` vs `ORDER BY ?m … LIMIT 10`) shows the scan `scan_table` row/file counts jump from ~budget-bounded to full-table; the modifier variant's `parquet_read` file count ≈ the un-limited scan's. Kill-switch control: `FLUREE_R2RML_LIMIT_PUSHDOWN=off` on the **pure** `Q-DIM-TYPED … LIMIT` (row-preserving chain) should reproduce the *same* full scan the modifier variant shows — proving the modifier, not the LIMIT plumbing, is the sink.
- **Refute:** the modifier variant scans no more than the LIMIT variant (would mean a budget path exists we missed); or the scan is already cheap and the modifier's own materialize/sort dominates.

**Shapes.** `Q-BI-ORDER`, `Q-BI-DISTINCT`, `Q-BI-GROUP` (single-table), and `Q-JOIN-DIM` with a modifier (H2 compounds H3). Control: `Q-DIM-TYPED … LIMIT` (pure chain — should early-terminate; the A/B baseline).

---

## H3 — Correlated-join rebuild (child-always-build-side)

**Mechanism.** §8: the operator is a correlated nested-loop `[operator.rs:8-31]`. The **child** is always the build side (`full_index` HashMap); Iceberg is always the streamed probe `[operator.rs:166-179, 514-564]`. Parent lookups are rebuilt **per child batch** (local map in `build_progress` `[operator.rs:570-938]`) and the parent scan bypasses the scan cache `[operator.rs:889-897]`. There is **no** `record_count`-driven build-side selection (grep-confirmed). When a **fact** table is the parent/inner of a join, "parents assumed small" `[operator.rs:887]` is violated.

**Why it should dominate (when it does).** For `Fact ⋈ Dim` the inner scan cache `[operator.rs:713-734]` amortizes the dimension; but for shapes where a fact table lands on the build/parent side, the operator rebuilds or re-streams a large relation per child batch — an O(child_batches × fact) blowup. The 36-scan `Store ⋈ Geography` baseline suggests scan multiplication is real.

**Confirm / refute.**
- **Confirm:** `scan_table` span **count** scales with child cardinality (not constant) on `Q-JOIN-DIM`; disabling the cache (`FLUREE_R2RML_SCAN_CACHE=off`) leaves join wall ~unchanged when the inner is fact-sized (cache only helps ≤ one window) but sharply worse when the inner is a dimension (isolates which side pays). Wall grows with **batch count**, not just row count.
- **Refute:** `scan_table` count is constant (1 per table) across child cardinalities → the scan cache already covers it and H3 is not active for the tested shapes.

**Shapes.** `Q-JOIN-DIM`, `Q-JOIN-GROUP`, and any multi-hop crawl. Contrast `Q-PULL` (single subject, no fan-out).

---

## H4 — Decimal / double pruning blindness

**Mechanism.** §6: money/measure columns are typically `DECIMAL`. A decimal predicate never becomes a scan filter (`const_object` → operator-only `[rewrite.rs:501]`) **and** `prunable_stats` returns `None` for a decimal column `[pruning.rs:279-281]`; doubles have no `stat_bounds` arm `[pruning.rs:319]`. So a selective `FILTER(?amount > 1000)` prunes **zero** files and the full fact scan runs, with the filter applied only post-decode.

**Why it should matter.** BI filters are overwhelmingly on money/quantity (DECIMAL) — exactly the type the pruner is blind to. A user who writes a highly selective revenue filter still pays a full-table decode (this also feeds H1).

**Confirm / refute.**
- **Confirm:** on `Q-MONEY-FILTER`, `scan_plan` span reports `files_pruned = 0` despite a predicate that excludes most of the value range; the **same** query with the filter re-expressed on an **integer** column (or an int-typed surrogate) shows `files_pruned > 0` and a wall drop. **[bench-gap:** `files_pruned` / `row_groups_pruned` counter on `scan_plan`.**]**
- **Refute:** `files_pruned > 0` on the decimal predicate (would contradict `pruning.rs:279-281` — investigate a hidden cast path).

**Shapes.** `Q-MONEY-FILTER` (primary), any fact `Q-BI-*` with a value FILTER. Contrast: the identical predicate on a date/int column (should prune — §6).

---

## H5 — COUNT(\*) has no manifest shortcut

**Mechanism.** §11 GAP: the fused aggregate `scan_table`s and folds row-by-row `[fused_aggregate.rs:910, 927-999]` even for `COUNT(*)`, although the authoritative row count is free in Iceberg manifest metadata — `aggregate_column_stats` already sums `df.record_count` `[stats.rs:120-133]`. The native PSOT-leaflet count path is native-index-only `[fast_count.rs:1-7]`.

**Why it should matter.** `COUNT(*)` / `COUNT` per group is a table-stakes dashboard tile and a common "does this dataset have data" probe. It should be ~instant (metadata read) and is instead a full parquet scan+fold — a pure, avoidable cliff.

**Confirm / refute.**
- **Confirm:** `Q-COUNT-STAR` wall ∝ full-scan wall (≈ `Q-FACT-SCAN` minus materialize), and `parquet_read` decodes the whole table; SF01→SF20 scales ~20×. A manifest-only count would be O(files) metadata, sub-second.
- **Refute:** `Q-COUNT-STAR` is already sub-second at SF20 (would mean a shortcut exists we missed).

**Shapes.** `Q-COUNT-STAR`, `Q-BI-GROUP` with `COUNT` (the fold runs but the count could be manifest-derived per file-group).

---

## H6 — Aggregate + join falls out of the fused path

**Mechanism.** §11: `detect_fused_r2rml_aggregate` requires a **single** scan and a `GRAPH { triples [+ one FILTER] }` body — any join / OPTIONAL / UNION / subquery in the GRAPH declines `[fused_aggregate.rs:327-333]`. So `Fact ⋈ Dim GROUP BY dim-attr (agg)` — the canonical rollup — misses the fold and materializes every fact row into bindings above the generic `GroupAggregateOperator` (§12), which also absorbs any LIMIT (H2).

**Why it should matter.** Rollups ("revenue by region", "orders by category") are the heart of BI, and they are precisely the joined-aggregate shape the fused path refuses. The cliff between a single-table `GROUP BY` (fused) and a joined `GROUP BY` (full materialize) should be stark.

**Confirm / refute.**
- **Confirm:** the fused-agg span is **present** on `Q-BI-GROUP` (single table) and **absent** on `Q-JOIN-GROUP`; a wall cliff between the two at equal group cardinality; `FLUREE_FUSED_R2RML_AGG=0` on `Q-BI-GROUP` makes it match `Q-JOIN-GROUP`'s per-row-materialize profile (isolates the fold's contribution). **[bench-gap:** a "fused path taken" boolean in the span/EXPLAIN — the inventory notes EXPLAIN shows the *planned* not *executed* path.**]**
- **Refute:** `Q-JOIN-GROUP` is no slower per output row than `Q-BI-GROUP` (would mean materialize is cheap and the fold's win is marginal).

**Shapes.** `Q-JOIN-GROUP` (primary), vs `Q-BI-GROUP` (fused control).

---

## H7 — Cold / warm structure (OAuth + loadTable + footer/disk tiers)

**Mechanism.** §4 + §10: cold latency is OAuth (~1–3 s) + `loadTable` + footer/disk-cache misses. Three tiers absorb repeats: per-query pin `[catalog_session.rs:88-131]`, process-wide `rest_load_tables` TTL 60 s `[cache.rs:42-46]`, `rest_clients` TTL 900 s `[cache.rs:62-66]`; plus the whole-file disk cache `[disk_cache.rs:216-243]`. The client key is a config-**text** fingerprint `[r2rml.rs:69-71]` (the rotation hazard, §4).

**Why it should matter for measurement discipline.** Cold vs hot can differ by seconds of fixed cost that have nothing to do with query complexity. If baselines aren't stratified by condition, H1–H6 measurements are polluted by catalog/OAuth noise — this hypothesis exists mostly to **quantify and control** that noise so the others are clean.

**Confirm / refute.**
- **Confirm:** cold / hot-process / warm-disk ratios per query class; cold overhead ≈ constant per query regardless of scan size (a fixed additive term), shrinking to ~0 hot-process. `loadTable` span count = 1 per (table, TTL-window) hot, N cold.
- **Refute:** cold overhead scales with data size (would mean it's not catalog fixed-cost but I/O — folds into H1).

**Shapes.** Every shape, run under all three conditions. This is the **cold-protocol** column of WP6, not a single query.

---

## H8 — Non-lowered forms evaluate generically over full scans

**Mechanism.** §13: `VALUES`, subqueries, and property paths are **not** converted to R2RML `[rewrite.rs:162-179]` — they evaluate generically above the scan. A `VALUES ?s { <a> <b> }` constraint therefore does **not** become the bound-subject prefix-prune (§7) that the equivalent bound-IRI pattern gets; the scan runs full and the constraint is applied afterward.

**Why it should matter.** Tools and generated SPARQL frequently express subject/value constraints as `VALUES` or subqueries. A user who "restricts to 3 stores" via `VALUES` unknowingly forfeits the prefix-prune and scans every table.

**Confirm / refute.**
- **Confirm:** `Q-VALUES` (VALUES-constrained subject) scans the full table (`scan_table` file count = full) while the semantically-equivalent bound-IRI form (`Q-DIM-DETAIL`) prunes to one table (§7). Same result set, order-of-magnitude scan difference.
- **Refute:** the VALUES form prunes equivalently (would mean a lowering path exists we missed).

**Shapes.** `Q-VALUES` vs its bound-IRI twin `Q-DIM-DETAIL`.

---

## H-summary — ranking, levers, and the A/B matrix

| H | Mechanism (inventory §) | Expected share | Primary A/B lever | Key evidence span/counter | Shapes |
|---|---|---|---|---|---|
| **H1** | §6 decode wall (no row filter) | **high** | `FLUREE_ICEBERG_PREDICATE_PUSHDOWN` (bounds pruning contribution) | `parquet_read` bytes/rows decoded vs emitted **[gap]** | `Q-FACT-SCAN` |
| **H2** | §5/§12 budget absorb | **high** | `FLUREE_R2RML_LIMIT_PUSHDOWN` (on pure chain) | `scan_table` file/row count ± modifier | `Q-BI-ORDER/DISTINCT/GROUP` |
| **H3** | §8 correlated rebuild | med–high | `FLUREE_R2RML_SCAN_CACHE` | `scan_table` **count** vs child cardinality | `Q-JOIN-DIM/GROUP` |
| **H4** | §6 decimal/double blind | med | re-type predicate int vs decimal | `scan_plan` `files_pruned` **[gap]** | `Q-MONEY-FILTER` |
| **H5** | §11 no manifest COUNT | med | (no switch — code change) | wall ∝ full scan; SF scaling | `Q-COUNT-STAR` |
| **H6** | §11 fused-agg join decline | med | `FLUREE_FUSED_R2RML_AGG` (exact `0`/`false`) | fused-agg span present/absent **[gap: executed-path flag]** | `Q-JOIN-GROUP` vs `Q-BI-GROUP` |
| **H7** | §4/§10 cold tiers | fixed cost | `FLUREE_ICEBERG_LOADTABLE_CACHE`, disk budget `=0` | cold/hot/warm ratio; `loadTable` count | all (condition axis) |
| **H8** | §13 non-lowered forms | low–med | rewrite VALUES↔bound-IRI | `scan_table` file count (VALUES vs IRI) | `Q-VALUES` vs `Q-DIM-DETAIL` |

**Bench-gaps to close first (WP3/WP6 prerequisites):** a `rows_decoded` vs `rows_emitted` counter on the reader span (H1); `files_pruned` / `row_groups_pruned` on `scan_plan` (H2, H4); an **executed-path** flag (fused-agg taken? budget reached scan?) distinct from the planned EXPLAIN (H2, H6). Without these, several hypotheses can only be inferred from wall-clock scaling, not attributed directly.

**Attribution discipline.** Each A/B toggles exactly one lever against a fixed query+scale+condition tuple; the pure-row-preserving `Q-DIM-TYPED … LIMIT` is the control that isolates H2's *plumbing* from H2's *modifier sink*. H7's condition stratification is a precondition for trusting H1–H6 numbers, so the cold protocol is captured before the per-hypothesis deep-dives (WP6 before WP7).
