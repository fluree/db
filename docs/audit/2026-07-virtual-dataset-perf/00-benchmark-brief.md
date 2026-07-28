# Virtual Datasets (Iceberg/R2RML): Benchmark Findings Brief

**2026-07-11 · Andrew Johnson (w/ Claude) · branch `bench/virtual-dataset-corpus` (off #1450)**

## What we built and measured

To ground the next wave of virtual-dataset performance work, we built exact-parity sibling datasets from one deterministic generator: the same Parquet feeds both a **native** Fluree ledger (35.2M triples, indexed, local) and a **virtual** dataset (R2RML mapping over Snowflake-managed Iceberg via the Horizon REST catalog, schema `DW_SF01`). Because both sides derive from identical data, every SPARQL query must return identical results — so the benchmark measures correctness and performance at once. A third target, the original `DW` schema at 550M rows (~7,670 Parquet files per fact table), serves as a scale-stress survey.

The corpus is 54 SPARQL queries derived from 26 realistic BI questions (revenue rollups, SLA chains, segmentation, inventory), tagged by SPARQL feature and by which suspected pathology each exercises, with deliberate A/B pairs that isolate mechanisms (e.g. the same query ± ORDER BY; decimal vs date vs int filters on the same table). A new in-repo harness (`fluree-bench-virtual`, binary `vbench`) runs the corpus with per-query pathway counters (table scans, REST calls, files pruned — captured from trace spans), canonical result hashing against oracles blessed from the native side, cold/hot cache protocols, and budget-gated regression comparison. It is durable infrastructure: every future perf PR can re-run it, and it gates the native path too, so virtual-path wins can't silently regress native performance.

## Headline results (identical data, identical queries)

| Outcome | Count | What it means |
|---|---|---|
| Hash-exact parity | 17 of 24 completing | The R2RML translation pipeline is byte-for-byte correct where it completes |
| Policy-exempt | 2 | Nondeterministic-selection queries, gated on row counts by design |
| **Row-divergent** | **5 → 3 bug classes** | Real correctness bugs, all root-caused (below) |
| **Did not finish (≥120–180s)** | **30 of 54** | Native answers these in 0–1.6 s |

Performance falls into four bands, organized by mechanism:

| Band | Ratio vs native | Examples | Mechanism |
|---|---|---|---|
| Strategy fires | **1–2×** | Date-partition pruned scan 1×; fused COUNT/SUM/AVG rollups 2×; LIMIT-pushdown 2× | Existing optimizations working as designed |
| Filtered fact scans | 10–13× | Orders-in-quarter 11×; GL posting-window 10× | Scan works; per-query REST overhead dominates |
| Floor-dominated | 150–3,400× | 1–16 ms dim lookups → 2–5 s | Fixed ~2–3 s REST/catalog floor per fresh process (~17–19 s fully cold) |
| **The wall** | **DNF ≥120–180 s** | Every fact⋈dim rollup, every ORDER BY/DISTINCT/GROUP BY/UNION + LIMIT, negation, VALUES joins, **plain COUNT(*) on any fact table** | Structural pathologies, below |

Three data points anchor the diagnosis. First, `COUNT(*)` on a fact table never finishes while the *same count with a filter* completes in 54.6 s — the engine scans all Parquet for a number the Iceberg manifest already holds. Second, the ± ORDER BY pair: plain `LIMIT 10` over orders takes 266 ms warm-process (the hot-baseline median; ~2.5 s in a fresh process where the REST/catalog floor dominates — see 05 deep-dive 1), adding `ORDER BY` makes it DNF — sort/distinct/group-by/union operators swallow the row budget, forcing full scans under the most common BI shape. Third, the decimal/date/int triple on one table: date filter 3 s (prunes 98.8% of files), decimal filter DNF (decimals never push down — and they're the money columns), int filter DNF (pushes down but file stats don't align with the value distribution, so nothing prunes).

**Scale barely matters; structure does.** At 200× the fact rows (SF20 survey), completing queries barely degrade — the fused rollup runs 1.6 s, a date-pruned scan returns 427K rows in 4.2 s — while exactly the same four query shapes DNF. The wall is architectural, not data-volume.

**Correctness (3 bug classes, all root-caused, all deterministic):**
1. **Transitive property paths (`+`/`*`) silently return empty** — no scan is even issued; no error raised. (Sequence paths like `a/b` work.)
2. **Subqueries silently return empty** — nested SELECT scopes are never translated to R2RML and quietly evaluate against an empty index. The engine's "error loudly on untranslatable patterns" guard covers top-level triples only.
3. **Bound-subject wildcard queries omit `rdf:type`** — `<iri> ?p ?o` returns the mapped columns but not the entity's class (also why the UI subject inspector loses `@type` on virtual datasets).

Silent-wrong-answers outrank slow answers: these three go to the top of the roadmap.

## Where things stand (the hypothesis, post-evidence)

The virtual-dataset engine is **correct where it engages and already fast where its optimizations fire** — the 1–2× band proves tolerable ratios are reachable. The gap is not I/O throughput and not Snowflake: it is (a) a fixed per-query REST floor that dwarfs small queries, (b) missing plumbing that lets LIMIT budgets and filter pushdown reach the scan under the dominant BI shapes, (c) a correlated join that redoes parent work per 1,000-row batch (one dims-only join issued **377 table scans** before timing out; native: 95 ms), and (d) type- and alignment-blind file pruning. Each has a known fix surface.

## The work, in bands

- **Band 0 — correctness (surgical, high certainty):** fix the three bug classes; extend the loud-error guard to sub-scopes so nothing silently returns empty again.
- **Band 1 — surgical perf wins:** answer `COUNT(*)` from Iceberg manifest `record_count` (DNF → milliseconds); forward row budgets through ORDER BY (top-k)/DISTINCT/UNION; stop loading 6 tables for 1-class queries (over-scan found in cold traces).
- **Band 2 — structural:** memoize parent-lookup tables across join batches and pick build side by size; widen predicate pushdown to decimal/double and add a row-level filter path (the current post-decode filter decodes everything that survives file pruning).
- **Band 3 — the floor:** persistent catalog/credential state across queries (server-mode reuse, warm pools) so small queries stop paying seconds of REST setup.

Bands 0–1 are independently shippable, low-risk, and each converts a class of DNFs or wrong answers directly. Every roadmap item ships with the benchmark as its gate: corpus ratio deltas, parity hashes, and native-path budgets must all hold.

## Status

Audit docs, corpus, harness, baselines, and findings register are committed on `bench/virtual-dataset-corpus`. Next: ~6 targeted span-waterfall deep-dives inside the DNF shapes, then the ranked PR roadmap with per-PR impact estimates and regression gates.
