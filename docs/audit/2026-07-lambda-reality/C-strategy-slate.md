# Track C — Strategy slate (DRAFT for adversarial review)

**Status:** draft by audit-doc → lambda-audit adversarial pass (deployed grounding) → team-lead
second pass → survivors to AJ. **No remedy is committed here** — each lever is a candidate with its
predicted effect, cost, and, critically, **what it does NOT fix**. Grounded in `A-engine-reality.md`
(§1–4 + Appendix A.1–A.7) and `lambda-reality-B-deployed-forensics.md` (deployed forensics, measured
throughput, and the 39 production query shapes).

## The epistemic story: three successive reframes (why the adversarial pass matters)

This phase changed its mind about the dominant cost **three times**, each time forced by evidence.
The slate is honest about that arc because it is the reason to distrust any single number here:

1. **Per-file request floor** (`F/C×L`). The epic optimized `DW_SF01`: 7,670 tiny files, 39 rows/file
   — the wall was the count of S3 round-trips. Levers: compaction, concurrency, file pruning.
2. **Cross-region latency.** Track B found the deployed data is cross-region (us-east-1 Lambda →
   us-east-2 parquet + Snowflake Polaris) — a ~1.2 s/table metadata RTT multiplier on every cold read.
3. **Row materialization (the current, measured truth).** A COMPLETED deployed scan
   (`DIM_CUSTOMER`, 1.74 M rows, 1 file, 31 s, `C=1`) gives **≈56,000 rows/s per core** of R2RML
   materialization, and `files_pruned=0` on every table ⇒ **every row is materialized**. FACT tables
   (36–200 M rows) → 107–590 s at `C=6`. This single number explains BOTH the nine 55 s cancels AND
   the 900 s runaway.

**The unifying term:** once the catalog constant (~1.2 s/table) is paid, every shape converges on

```
  wall ≈ rows_materialized / (56k × cores_used)
```

so the slate is organized around **rows materialized** (Tier 1: touch fewer; Tier 2: materialize
less/faster per touched row; Tier 3: the constants + safety around it).

## The headline the slate must state plainly

**The deployed timeout is shape (b), which the epic never optimized.** The three shapes:
- **(a) tiny-file fact** (`DW_SF01`, the epic's target) — request-floor bound.
- **(b) single-large-file dim/fact** (`DW_SVL`, the DEPLOYED dataset) — single-thread materialization
  (`C = min(6, 1) = 1`), the shape the wave never tuned. **This is what times out in production.**
- **(c) catalog-bound many-table first-touch** (`get_data_model`) — the un-deadlined runaway.

The five families the levers are scored against:

| Family | What it is | Observed | Dominant cost |
|---|---|---|---|
| **F-AGG** (~25/39 shapes) | BI rollups: COUNT/SUM/AVG + GROUP BY, **no row filter** | the bulk of chat | full-table materialization to RDF terms |
| **F-LIMIT** (~11/39 shapes) | `SELECT … LIMIT 20`, several no ORDER BY | full scans, some cancelled | **budget not forwarded** (bug, A.7) |
| **F-RUNAWAY** | `get_data_model` / info-stats | 900 s, pins 2 containers | un-deadlined native full-stats scan |
| **F-SINGLE** | a single-large-file table | 31 s at `C=1` | single-threaded materialization |
| **F-CATALOG** | many-table first-touch | ~1.2 s/table cold | cross-region catalog RTT × N |

**The 6-core ceiling that governs the ranking:** materialization is CPU-bound at ~56k rows/s/core on
~6 vCPUs ⇒ hard ceiling ~336k rows/s. A FACT already spans ≥6 files so `C=6` **already saturates all
cores** — therefore **raising `FLUREE_ICEBERG_SCAN_CONCURRENCY` does almost nothing** for the deployed
workload. The only ways past the ceiling: **materialize fewer rows** (Tier 1), **materialize
less/faster per row or use idle cores** (Tier 2), or **more cores** (Lambda is already maxed).

---

## TIER 1 — Touch fewer rows (attacks the dominant term)

### T1.1 — Aggregate pushdown (the MAJORITY lever, ~25/39 shapes). *The "new strategy" tier.*
- **Mechanism:** the majority of production queries are `COUNT/SUM/AVG … GROUP BY` with **no row
  filter** — `files_pruned=0` is CORRECT there (nothing to prune). Instead of materializing every
  row into RDF terms and aggregating on top, compute the rollup at/below the scan: (i) `COUNT` from
  Iceberg manifest `record_count` sums — **already cheap** where sound (`table_row_count` /
  `sound_manifest_row_count`, `r2rml.rs:843,2212`; the F22 lineage); (ii) `SUM/AVG/MIN/MAX` per
  GROUP-BY key from **Parquet column chunks / pushed aggregation** — real column-level compute, not
  free. Prior art: PR-6 fused-aggregate + F22.
- **Family:** F-AGG (the bulk of chat).
- **Predicted effect:** COUNT-class → **sub-second** (manifest, no materialization). SUM/AVG per
  GROUP-BY key → bounded by column-chunk decode of the aggregated columns only (not all columns ×
  all rows to terms) — a large fraction of the 336k-rows/s wall removed, but **not** to sub-second for
  high-cardinality GROUP BY.
- **Cost/risk/owner:** **engine PR — large** (the COUNT slice is moderate/done-cheap; the SUM/AVG
  per-key column compute is the real work — size it honestly). Risk: soundness (delete manifests,
  null handling, GROUP-BY key cardinality), and how far the fused path extends before falling back.
- **Depends on:** nothing hard; extends existing PR-6/F22 machinery.
- **Does NOT fix:** queries that need row values (projections, joins on values — F-LIMIT); F-RUNAWAY
  is a sibling (use T1.2).

### T1.2 — Manifest-backed virtual info-stats routing (kills the runaway, no deadline needed).
- **Mechanism:** route a graph-source-federated dataset's `/info` stats to the **existing**
  metadata-only path (`build_graph_source_info` → snapshot-summary row counts, mapping-derived
  classes/properties, NDV `null`) instead of native `assemble_full_stats`, which materializes the
  federated tables. The metadata path exists and is zero-data-read (`ledger_info.rs:1524-1526`); the
  gap is routing — `LedgerInfoBuilder::execute` reaches it only on the `is_not_found()` fallback
  (`:2172-2188`), so a virtual dataset that is also a committed ledger takes the scanning native path
  (`:487,:527`). See A.6.
- **Family:** F-RUNAWAY (kills it outright).
- **Predicted effect:** 900 s → **sub-second**; removes the container-pin capacity risk.
- **Cost/risk/owner:** **engine PR — low/moderate** (path exists; routing + skip-scan). Risk: output
  parity (consumers already tolerate `null` NDV/flakes — `Option` fields "null when unknown, virtual
  no-scan").
- **Depends on:** nothing.
- **Does NOT fix:** real chat scans (F-AGG/F-LIMIT).

### T1.3 — Family-D fix: forward the LIMIT row-budget through `DatasetOperator` (a confirmed bug).
- **Mechanism:** `DatasetOperator` (`dataset_operator.rs:339`) inherits the no-op
  `set_row_budget`/`set_topk`, so a `LIMIT 20` on the dataset path never reaches the R2RML scan and
  it materializes the full 512 K-row window (A.7, confirmed at the operator level). Mirror
  `GraphOperator`'s forwarding (`graph.rs:639/647`) onto `DatasetOperator` — thread the budget/topk
  to each member's inner operator.
- **Family:** F-LIMIT (~11 shapes).
- **Predicted effect:** a `LIMIT 20` becomes a ~20-row scan — **full scan → sub-second** for the
  no-filter LIMIT shapes. Among the highest unlock-per-cost on the board.
- **Cost/risk/owner:** **engine PR — low** (the pattern is built three times; ~a `set_row_budget`/
  `set_topk` impl on one operator + a forwarding test). Risk: correctness of per-member budgeting
  (a shared budget across members vs per-member) — mirror the GraphOperator semantics.
- **Depends on:** confirming the deployed query uses the dataset path (solo lane; strong indirect
  evidence — the full-scan-on-`LIMIT` symptom).
- **Does NOT fix:** F-AGG (no LIMIT to push), aggregate/GROUP-BY shapes, F-RUNAWAY.

### T1.4 — Predicate / row-filter pushdown: investigate `files_pruned=0` first.
- **Mechanism:** before building new pushdown, **determine why `files_pruned=0` in production** — the
  ~25/39 F-AGG shapes are genuinely un-prunable (whole-table rollups, our prunes CORRECTLY decline),
  so this is likely **not** a "fix a bad decline" but a "the queries have no selective predicate."
  Where filters DO exist, extend PR-7/PR-5 pushdown reach; but the evidence says the majority lever
  is T1.1 (aggregate pushdown), not row-filter pushdown.
- **Family:** F-AGG/F-SINGLE **only where a selective predicate exists** (evidence says: rarely).
- **Predicted effect:** proportional to selectivity; **~0 for whole-table rollups** (the majority).
- **Cost/risk/owner:** **engine PR — high** (soundness). Owner: engine. **Investigate before
  scoping** — the answer decides whether this is a real lever or subsumed by T1.1.
- **Does NOT fix:** unfiltered aggregates (→ T1.1); F-RUNAWAY, F-CATALOG.

---

## TIER 2 — Materialize less / faster per touched row

### T2.1 — Projection narrowing (only demanded columns → fewer term materializations).
- **Mechanism:** materialize RDF terms only for the columns the query actually projects/filters, not
  the full star. The two-scans finding (a subject scan + a `[PRODUCT_KEY]`-only scan, A.5/iii) shows
  the machinery reads narrow projections; ensure the R2RML row path doesn't build terms for
  unreferenced POMs.
- **Family:** F-AGG (partial), F-SINGLE, F-LIMIT.
- **Predicted effect:** proportional to (demanded columns / total POMs) — a star with 20 predicates
  where the query wants 3 could cut materialization ~5–6×. Bounded by how much is already projected.
- **Cost/risk/owner:** **engine PR — moderate**. Risk: correctness of partial-star materialization.
- **Does NOT fix:** a query that projects the whole star; the per-row cost floor (→ T2.2).

### T2.2 — R2RML row-path throughput (the ~56k rows/s profile question).
- **Mechanism:** 56k rows/s/core for a 4-column projection is **slow** — where does it go? Term
  construction, allocation per binding, IRI encoding, POM iteration? A profile-shaped investigation;
  the fix is whatever the profile shows (batch term construction, reuse buffers, avoid per-row
  allocs). Late/lazy materialization (defer term build until a row survives filters/limit) is a
  candidate.
- **Family:** ALL materializing families (the shared per-row constant).
- **Predicted effect:** unknown until profiled — a 2× on the row path is a 2× on the dominant term
  across every F-AGG/F-SINGLE/F-LIMIT query. Potentially the highest-leverage Tier 2 item, but
  **unsized** without a profile.
- **Cost/risk/owner:** **engine — investigate then PR** (profile first). Risk: none to investigate.
- **Does NOT fix:** the row COUNT (→ Tier 1); it makes each row cheaper, not fewer.

### T2.3 — Intra-file (row-group) parallelism (breaks the `C=1` pin on single-file tables).
- **Mechanism:** `decode_large_file` runs the whole file on ONE `spawn_blocking` thread
  (`send_parquet.rs:691`); row groups are independent and enumerated (`:68`). Decode them across
  cores so a single-file table uses all 6 vCPUs instead of 1.
- **Family:** F-SINGLE (and few-file tables where files < cores).
- **Predicted effect:** ~×min(cores, row_groups) — `DIM_CUSTOMER` 31 s → ~5–6 s. **Zero** for a table
  already spread over ≥6 files (already at `C=6`).
- **Cost/risk/owner:** **engine PR — moderate**. Plan-level, survives cold. Risk: memory (N row
  groups resident), emitted-batch ordering.
- **Does NOT fix:** the aggregate 336k-rows/s ceiling (it redistributes to idle cores; a ≥6-file
  table gets nothing); F-RUNAWAY, F-CATALOG.

---

## TIER 3 — Constants + hygiene (safety, cold floor, packing)

### T3.1 — Deadline everywhere + engine cancellation-that-bites. *Unconditional — burn protection.*
- **Mechanism:** (solo) attach `x-query-timeout-secs`/`opts.timeout` to **all** engine invokes incl.
  `execute_query("info", …)` (currently neither — the runaway). (engine) poll `check_cancelled()`
  **mid-sweep** in the scan/decode loop — today it's checked only between operator pulls
  (`operator.rs:909/1090/1332/2377`), never inside the parquet fan-out or `decode_large_file`, and
  detached `tokio::spawn` reads outlive the query (§3d/3e).
- **Family:** F-RUNAWAY (bounds it even without T1.2), F-FACT (frees the container promptly).
- **Predicted effect:** no speedup — bounds the burn and frees containers. Near-mandatory safety.
- **Cost/risk/owner:** **solo PR — trivial** (header) **+ engine PR — moderate** (mid-loop cancel).
- **Does NOT fix:** speed — the query still fails, just bounded and clean.

### T3.2 — Same-region placement (Lambda ↔ parquet ↔ catalog).
- **Mechanism:** co-locate the query Lambda with the BYO parquet (us-east-1 vs us-east-2) + catalog,
  or replicate parquet into the Lambda region.
- **Family:** F-CATALOG (removes ~1.2 s/table RTT), cold-read fetch trim.
- **Predicted effect:** removes a measured ~1.2 s/table cold floor + cross-region fetch latency.
  **Secondary** — does not touch materialization, so a FACT query stays >55 s.
- **Cost/risk/owner:** **infra/CFN + data placement — moderate**; **may not be solo's to move** (BYO
  customer bucket).
- **Does NOT fix:** materialization (the dominant term).

### T3.3 — Catalog persistence across cold containers (shared/EFS/pre-warm).
- **Mechanism:** move the per-container `/tmp` catalog cache (cold on all 16 containers) to a shared
  store, or pre-warm it.
- **Family:** F-CATALOG.
- **Predicted effect:** removes the cold catalog chain (~1.2 s/table × N) on cold containers.
  Secondary vs materialization.
- **Cost/risk/owner:** **architecture — large** (EFS to Lambda, or shared cache). Risk: EFS latency,
  staleness (the 300 s TTL / F21 question).
- **Does NOT fix:** materialization; the parquet data cache stays cold unless also shared.

### T3.4 — `FLUREE_ICEBERG_SCAN_CONCURRENCY` in CFN. *Near-moot — include with the caveat.*
- **Mechanism:** set the env so `C` isn't clamped to `min(available_parallelism, files)`.
- **Family:** F-MANY (fetch-wait-bound, not deployed), marginally.
- **Predicted effect:** **~0 for the deployed workload** — DW_SVL FACTs already have 64–129 files so
  `C = min(6, files) = 6 =` all vCPUs; you cannot run 8 CPU-bound decodes on 6 cores. Real benefit
  only with more vCPUs (Lambda is maxed) or a fetch-wait-bound workload.
- **Cost/risk/owner:** **CFN env — trivial**. Risk: memory (more in-flight).
- **Does NOT fix:** the 6-core CPU ceiling; F-SINGLE (1 file → still 1).

### T3.5 — Compaction (RE-SCOPED: parallelism packing only).
- **Mechanism:** compact source Iceberg tables to `N × 128 MB` where `N ≥ vCPUs` — enough files to
  fill all cores, not so few a table drops to `C=1`.
- **Family:** F-MANY (per-file floor), F-SINGLE (splits a 1-file table so `C>1` without T2.3).
- **Predicted effect:** **reduces neither rows nor bytes materialized** — it only improves
  parallelism packing + per-file overhead. So it does NOT touch the dominant term; a 200 M-row FACT
  is still 200 M rows.
- **Cost/risk/owner:** **Snowflake-side / customer maintenance — external owner**. Risk: write-side
  cost + freshness; **compacting to too FEW files re-creates the `C=1` pathology** (the N≥cores
  target is the guardrail).
- **Depends on:** customer control of the BYO source layout.
- **Does NOT fix:** the materialization term; F-RUNAWAY; F-CATALOG.

---

## NON-ENGINE TIER — Product / prompt design (possibly best unlock-per-cost)

### P.1 — MCP surfaces manifest stats so the LLM needn't issue exploratory rollups.
- **Mechanism:** the agent issued **~39 whole-table rollups in ONE turn**. If the MCP tool surface
  hands the LLM the dataset's row counts / column stats / schema up front (from Iceberg manifests —
  the same zero-read metadata as T1.2/T1.1-COUNT), the model doesn't need to fire exploratory
  `COUNT/GROUP BY` scans to understand the data.
- **Family:** F-AGG (removes the *demand*, not just the cost), F-RUNAWAY (get_data_model is this).
- **Predicted effect:** could remove a large fraction of the 39 rollups outright — the wall you never
  pay. Plausibly the **best unlock-per-cost** of the whole slate.
- **Cost/risk/owner:** **solo/product — low/moderate** (tool-surface + prompt design). Risk: the
  model still issues ad-hoc rollups for genuine questions.
- **Does NOT fix:** a genuine user question that needs a real rollup (→ T1.1); F-LIMIT, F-SINGLE.

---

## Layer ∅ — The honest do-nothing per item

Per family: **F-RUNAWAY** do-nothing = keep pinning two containers 15 min (unacceptable — T1.2/T3.1
near-mandatory). **F-AGG** do-nothing = chat 504s on rollups over facts (current state; correctness
OK, UX bad). **F-LIMIT** do-nothing = a `LIMIT 20` scans the whole table (a *bug* left in place —
low-cost to fix, so do-nothing is hard to justify). **F-CATALOG** do-nothing = a multi-second cold
floor (tolerable). Do-nothing is the yardstick each lever's `(unlock × breadth)/cost` is measured on.

---

## Ranking — `(predicted unlock × breadth) / cost`

| Rank | Lever | Tier | Family | Unlock | Breadth | Cost | Owner |
|---|---|---|---|---|---|---|---|
| **1** | **T1.3** DatasetOperator budget/topk forwarding | 1 | F-LIMIT | full-scan → sub-s | ~11 shapes | **low** | engine (known pattern) |
| **2** | **T1.2** manifest-backed info-stats routing | 1 | F-RUNAWAY | 900 s → sub-s | narrow, top severity | **low-mod** | engine (path exists) |
| **3** | **P.1** MCP surfaces manifest stats | product | F-AGG, F-RUNAWAY | removes the *demand* | broad | **low-mod** | solo/product |
| **4** | **T1.1** aggregate pushdown | 1 | F-AGG | COUNT→sub-s; SUM/AVG big cut | **majority (~25/39)** | **high** | engine (new strategy) |
| **5** | **T3.1** deadline + cancel-that-bites | 3 | F-RUNAWAY, F-FACT | bounds burn | broad (safety) | **low**+mod | solo+engine |
| **6** | **T2.3** intra-file row-group parallelism | 2 | F-SINGLE | ~×6 single-file | moderate | **mod** | engine |
| **7** | **T2.1** projection narrowing | 2 | F-AGG/SINGLE/LIMIT | ~cols-ratio | broad | **mod** | engine |
| **8** | **T2.2** R2RML row-path throughput | 2 | all materializing | unknown (profile) | broad | **mod** (profile first) | engine |
| **9** | **T3.2** same-region placement | 3 | F-CATALOG | ~1.2 s/table | moderate | **mod** | infra (BYO?) |
| **10** | **T1.4** predicate/row pushdown | 1 | F-AGG (if filtered) | selectivity | narrow (evidence: rare) | **high** | engine (investigate first) |
| **11** | **T3.3/3.5** catalog persistence / compaction | 3 | F-CATALOG / F-MANY | cold floor / packing | narrow | **high/mod** | arch / Snowflake |
| **12** | **T3.4** `SCAN_CONCURRENCY` env | 3 | F-MANY | ~0 deployed | narrow | **trivial** | CFN |

**Reading the ranking:** the two cheapest, highest-certainty wins are **T1.3 (a confirmed
forwarding-bug fix, ~11 shapes)** and **T1.2 (route the runaway to the existing metadata path)** —
both engine, both low cost, both attack the two clearest failure modes. **P.1 (product)** may beat
everything on unlock-per-cost by removing the *demand* for rollups. **T1.1 (aggregate pushdown)** is
the majority lever and the "entirely new strategy" tier — the biggest ceiling, the biggest effort.
The intuitive infra levers (concurrency env, compaction, shared cache) are **Tier 3 constants** — they
trim the ~1.2 s/table floor or pack files, but none touch the dominant rows-materialized term.

---

## What I most want the adversarial pass to attack

1. **Which path does the deployed query use** — `DatasetOperator` (T1.3 applies, ~11 shapes rescued)
   or the single-view `GraphOperator` (already forwards, Family D smaller)? An `EXPLAIN` of a
   `LIMIT 20` on `full-enterprise-byo-1:main` settles it. This directly sizes rank #1.
2. **The 56k rows/s ceiling is one measured point** (`DIM_CUSTOMER`, 4 columns). Is it truly
   CPU-bound, or partly fetch that concurrency (T3.4) would overlap? A second measured multi-file-FACT
   rows/s confirms or breaks the whole tiering.
3. **T1.1 scope:** how far does the fused/pushed aggregate extend before falling back? COUNT is cheap
   (manifest); SUM/AVG per high-cardinality GROUP-BY key is real column compute — is the majority of
   the ~25 shapes COUNT (cheap) or SUM/AVG (expensive)? This sizes rank #4.
4. **T1.2 output-parity:** does any `/data-model` / `/info` consumer depend on exact `flakes`/NDV that
   the metadata path returns `null`?
5. **P.1 realism:** will surfacing stats actually stop the LLM issuing rollups, or does the agent loop
   issue them regardless? (A prompt/tooling question lambda-audit + AJ are better placed to judge.)
6. **T2.2 is unsized** — worth a profile before it's ranked; a 2× on the row path is a 2× on
   everything, but it might be irreducible.
