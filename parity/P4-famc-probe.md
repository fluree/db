# P4 — Family-C DNF Probe: FILTER-over-join fused-aggregate decline

Probe of the two deployed production queries (dataset `enterprise-byo-test-10`) that DNF (>180s) on the virtual dataset. Confirms the decline diagnosis by code trace AND measures the same shapes native-vs-virtual on our bench (SF0.1), then projects to deployed scale. All code receipts are at commit `d21001854` (perf/browse-parity tip = the candidate solo pin). Worktree: `<scratch>/wt-probe` (detached). Probe corpus + run.jsonl: `<scratch>/probe-corpus/`, `<scratch>/run-{native,virtual,counts,dims}.jsonl`.

Labels: MEASURED = ran it; READ = read from source at d21001854; INFERRED = derived/projected.

## The two shapes (adapted to our bench star)

Bench vocab is `edw: <http://ns.fluree.dev/edw#>` over the SF0.1 enterprise star (source `enterprise-sf01-v`); the deployed vocab is `ex:`/`https://demo.fluree.com/enterprise#` but the fact⋈dim shapes map 1:1 (READ: `fluree-bench-virtual/targets/enterprise-sf01-mapping.ttl`).

Q1 / p1 — open tickets by segment: `FactSupportTicket` (edw:status plain string, edw:customer FK→DimCustomer) ⋈ `DimCustomer` (edw:segment), GROUP BY ?segment, COUNT, with a FACT-SIDE INEQUALITY FILTER `?status != "Closed"`.

Q2 / p2 — below-reorder by category: `FactInventorySnapshot` (edw:onHandQty, edw:reorderPoint both xsd:integer, edw:product FK→DimProduct) ⋈ `DimProduct` (edw:category), GROUP BY ?category, COUNT + two AVG folds, with a VAR-TO-VAR FILTER `?onHand < ?reorder`.

Contrasts run in the same session: p3 = the SAME SupportTicket⋈Customer join grouped by segment but with a FOLDABLE EQUALITY flag (`edw:isCurrent true`, a star_constraint) instead of a SPARQL FILTER; s022 = the requested fused single-table sentinel (q022, current customers by segment).

## 1. TRACE — the exact decline branch (READ, d21001854)

Both shapes are ADMITTED at the query-analysis gate `detect_fused_r2rml_aggregate` (`fluree-db-query/src/r2rml/fused_aggregate.rs:374`): they have GROUP BY + aggregates (line 388), a FILTER is allowed alongside a non-empty GROUP BY (the cost guard at line 452 fires ONLY when `group_by.is_empty()` — that is the q038 ungrouped-COUNT class, NOT these), COUNT/AVG are foldable (line 466, AVG is InputSemantics::List), and the projection == group_by + aggregates with ORDER BY over output vars (lines 487-494). So `detect` returns `Some(plan)` and the FILTER is carried in `plan.filter` (extracted at line 422).

The fused operator is then BUILT with a generic `fallback` operator tree (`execute/operator_tree.rs:2474-2488`): `FusedR2rmlAggregateOperator::new(plan, fallback)`, where `fallback` is the ordinary GRAPH pipeline with ORDER/LIMIT stripped.

At `open()`, resolution routes by rewritten shape (`fused_aggregate.rs:1665`). Both queries rewrite to a fact→dim FK chain = multiple `R2rml` leaf patterns, so `combine_constrained_class_scan` returns `None` (it declines on any var-object / RefObjectMap member — line 2215) and the join arm is taken (line 1673→1684), delegating to `resolve_join_at_open`.

DECLINE BRANCH (both queries): `resolve_join_at_open`, `fused_aggregate.rs:2263-2265`:
```
// A FILTER over the join is out of scope ...
if self.filter.is_some() { return Ok(None); }
```
This is `resolve_join_at_open`'s FIRST statement — a blanket decline of ANY filter on the join path. `Ok(None)` makes the fused operator stream its `fallback` (comment at operator_tree.rs:2476), i.e. the full GRAPH-scoped R2RML scan → materialize RDF bindings → generic join(fact→dim) → filter → GROUP BY.

CONFIRMED vs the audit diagnosis. The "no-FILTER-over-join gate" is exactly line 2263. Two refinements: (a) the single-table path (`resolve_at_open:1848-1868`) DOES build a `FilterPlan` and would handle both filter types — the decline is specific to the JOIN path, which has no filter support at all; (b) the inequality/var-to-var sub-points are the reason the two escape hatches don't apply: an inequality/var-to-var can never be rerouted as a `star_constraint` (READ: `resolve_star_constraint_checks:2106` matches `(pred, constant)` EQUALITY only) nor pruned to files (no constant bound / no numeric stats on STATUS), so a blanket-declined FILTER is the only outcome. F1's `combine_constrained_class_scan` does NOT interact (REFUTED-as-concern → confirmed harmless): it requires a var-object-free subject-star and returns None on the RefObjectMap FK member both queries carry.

## 2. MEASURE — native-sf01 (local) vs virtual-sf01 (live Snowflake), 1 rep, warm

MEASURED walls (warm, hot cache), rows, cross-engine result-hash, and `r2rml.scan_table` span count:

| q | shape | native ms | virtual ms | v/n | rows | hash match | scan_table_n (virt) | verdict |
|---|-------|-----------|------------|-----|------|-----------|---------------------|---------|
| p1 | Q1: tickets⋈customer, inequality FILTER | 15 | 10280 | 685× | 3 | yes (69006c4ffd39) | 3 | DECLINED→materialize |
| p2 | Q2: inv⋈product, var-to-var FILTER, COUNT+2×AVG | 833 | 5007 | 6× | 10 | yes (d7aba410bb32) | 3 | DECLINED→materialize |
| p3 | CONTRAST: tickets⋈customer, isCurrent flag (no FILTER) | 691 | 241 | 0.35× | 3 | yes (801f18538bb5) | 2 | FUSED (join fold) |
| s022 | SENTINEL: current customers by segment (single-table) | 339 | 61 | 0.18× | 3 | yes (520932a58713) | 1 | FUSED (single-table) |

MEASURED: results are byte-identical cross-engine — full result hashes match on all four (the virtual materialize path is CORRECT, just slow). p1's three grouped counts (Consumer 10788, Enterprise 2639, SMB 2683) and p2's ten AVG rows match native exactly.

MEASURED: `scan_table_n` is the clean fire/decline signal here — 1 = fused single-table (s022), 2 = fused join (p3: one fact fold + one dim FK→GKey map scan), 3 = declined join (p1/p2: generic multi-scan materialize+join+filter). `files_selected`/`files_pruned`/`estimated_row_count` were 0 across the board (warm cache elides the `iceberg.scan_plan` span), so scan_table_n + wall are the discriminators in a warm run.

DECISIVE ISOLATION (p1 vs p3, MEASURED): identical SupportTicket⋈Customer join, GROUP BY segment, COUNT. Swap the SPARQL `FILTER(?status != "Closed")` for a foldable equality flag `isCurrent true` and virtual drops 10280ms → 241ms — a 42× swing from the FILTER alone. The JOIN fuses fine (p3, s022); the FILTER is the sole cause of the decline. This is the empirical twin of the code trace.

## 3. Scale anchors (MEASURED native COUNTs, SF0.1)

SupportTicket = 40,000; InventorySnapshot = 300,000; DimCustomer = 390,000 (SCD history); DimProduct = 37,500. below-reorder = 6,279 and non-Closed tickets = 16,110 (both equal the sums of the p1/p2 grouped results → parity re-confirmed).

INFERRED — why virtual p1 (40K facts, 10.3s) is SLOWER than p2 (300K facts, 5.0s): the declined path materializes the join DIMENSION as RDF too, and DimCustomer (390K rows, ~13 predicates each) dwarfs DimProduct (37.5K). Rows materialized: p1 = 40K + 390K = 430K in 10.3s (~42K rows/s); p2 = 300K + 37.5K = 337.5K in 5.0s (~67K rows/s). Same order — the declined cost is materialize-bound on (fact_rows + dim_rows). The fused path (p3) processes the same 430K in 241ms (~1.78M rows/s) because it hash-folds minimal columns and never builds RDF.

## 4. EXTRAPOLATE — deployed byo-test-10 (INFERRED)

Scale anchor: InventorySnapshot 8M (forensics) / 300K (SF0.1) = 26.7×. Applied uniformly: SupportTicket ≈ 1.07M, DimCustomer ≈ 10.4M, DimProduct ≈ 1.0M.

Virtual (declined/materialize), rows-materialized-linear from the SF0.1 walls:
- p1/Q1: 1.07M facts + 10.4M customer dim = 11.5M rows → 10.3s × (11.5M/430K) = 275s WARM-linear → DNF reproduced (well over 180s; cold + cross-region push it higher).
- p2/Q2: 8M facts + 1.0M product dim = 9M rows → 5.0s × (9M/337.5K) = 133s WARM-linear; borderline UNDER 180s warm, but production is COLD (loadTable GET, manifest fetch, cold parquet decode, cross-region). Applying the audit's cold/cross-region multiplier (≥1.4×) → ≥186s → DNF reproduced. Q2 crosses the ceiling specifically because it is cold, not warm.

Both DNFs are reproduced: Q1 unconditionally (275s warm), Q2 once the cold multiplier is applied to the 133s warm-linear base.

Native-twin projection (native GROUP BY scans ~linear in leaflets touched, from the MEASURED SF0.1 native walls × 26.7):
- Q1 twin: 15ms × 26.7 ≈ 0.4s (indexed, cache-friendly; the FILTER + segment join are cheap on materialized leaflets).
- Q2 twin: 833ms × 26.7 ≈ 22s (genuinely 8M snapshot leaflets + var-to-var filter + product join + 2 AVG; completes, does not DNF).
Native twin completes both (0.4s / 22s) where virtual DNFs (275s / ≥186s) — a 12×–700× gap that is the materialize penalty plus Iceberg/network cold cost.

## 5. Family-C widening scope (INFERRED, one paragraph each)

Q1 (inequality FILTER over join): lift the blanket `if self.filter.is_some() { return Ok(None); }` at `fused_aggregate.rs:2263` and PORT the single-table `FilterPlan` construction (`resolve_at_open:1848-1868`) into `resolve_join_at_open`. For each var the filter references, resolve its ObjectMap against whichever chain pattern OWNS it (here ?status → the fact SupportTicket TM, scalar STATUS column), project that column into the fact scan, and apply the prepared expression per row INSIDE the fact fold — dropping non-matching rows before they increment the group COUNT. Q1's filter is fact-side only, so it needs just fact-scan column projection + a per-row `!=` eval; the `!=` can't fold as a star_constraint (equality-only) and can't prune files (low-cardinality string, no numeric bound), so a row-applied residual is correct and is exactly what the ported FilterPlan does. The single-table path already proves this machinery is byte-parity with the generic filter.

Q2 (var-to-var FILTER over join): the SAME guard-lift + FilterPlan port, but the filter is var-to-var (?onHand < ?reorder), both fact-side scalar columns — the port projects BOTH into the fact scan and materializes both per row for the comparison (the single-table FilterPlan already supports multi-var filters via eval_vars/eval_objmaps). A var-to-var comparison is inherently stats-unprunable (neither side constant → no file min/max pruning, no star_constraint) so it MUST be a row-applied residual, which the fold applies before counting. Q2 also carries two AVG accumulators plus COUNT; `resolve_agg_folds` already folds multiple numeric AVGs from the fact scan, so no new aggregate machinery is needed — the only new work is the filter port. Both widenings are the same one change: give the join path the filter support the single-table path already has, resolving filter vars against the fact/dim pattern that owns them.
