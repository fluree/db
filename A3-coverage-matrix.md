# A3 — Coverage Matrix & Negative Space: R2RML/Iceberg Virtual-Dataset Optimization

Audit tip `10e073fe9` (PR #1514), read-only at `<audit-checkout>`. Scope: where optimization coverage is SPARSE or MISSING across query shapes, execution folds, surfaces, and Iceberg features — plus the probe battery in `probes/` that empirically exposes each. Every row/paragraph is one line for clean paste. Labels: READ(file:line) = verified in code; CORPUS(qNNN) = corpus member; INFERRED = deduced from verified facts; UNKNOWN-PROBE = not settled by reading, needs a run.

## Headline

Shape×mechanism cells examined: ~40. UNCOVERED-CLIFFS (fall to full-FACT materialize or worse): 18. Loud-refuse (correct but errors on virtual): 2 (transitive path, subquery). CORRECTNESS gap: 1 (merge-on-read delete files never applied). UNKNOWN-PROBEs: 6 (ASK budget, DESCRIBE fan-out, MINUS exec, NOT-EXISTS batching, JSON-LD admission parity, schema-evolution rename). Probe battery: 18 probes + 3 positive controls, all runnable against enterprise-sf01 except the delete-file correctness item (needs a MoR fixture). All optimization switches default-ON (READ mod.rs:40-48), so this maps LIVE behavior, not a flag-off floor.

## The 8 most consequential gaps (ranked)

1. CORRECTNESS — merge-on-read POSITION/EQUALITY delete files are never applied. `parse_manifest_list` drops delete manifests, `planner.rs:224` skips `is_deletes()`, and no reader subtracts deleted rows — the scan sees only data files. READ(manifest/manifest_list.rs:21-28 "Phase 2 will SKIP these", scan/planner.rs:224-225, stats.rs:311-350). Any virtual target over a Snowflake table that has undergone a row-level DELETE/UPDATE returns STALE rows with no error. The COUNT(*) manifest shortcut is delete-guarded (READ provider.rs:232-241) but that guard is nearly moot: the fallback scan it defers to ALSO over-counts (it counts data-file rows including deleted ones), so both paths are wrong when deletes exist — the guard buys latency, not correctness. INFERRED severity: conditional on source-table MoR; the append-only bench cannot exhibit it (probes.md flags the fixture needed).

2. VALUES / FILTER IN over a graph-source var is never lowered to a scan filter → full FACT scan + O(N·K) uncancellable correlated join. No set-valued scan filter exists (ScanCmpOp has only Eq/NotEq/Lt/LtEq/Gt/GtEq, READ provider.rs:27-34); VALUES is rewrite pass-through (READ rewrite.rs:228-242); `build_scan_filters` reads only FILTER/object_constant/star_constraints (READ operator.rs:633-718). The file-pruning BACKEND exists (Expression::In in READ pruning.rs:124-139) but nothing emits it. CORPUS(q040/q052 both timeout 180s); the 50M-row round3b #9 stall is the same class (`lambda-w4-3-values-diagnosis.md`). ValuesOperator's nested loop has ZERO check_cancelled, so the fallback is uninterruptible. Probes 02/03.

3. ORDER BY direction/expression asymmetry: only single-column DESC of a bare scan column gets scan-side top-k; ASC and expression sorts get NEITHER top-k NOR budget → full scan + full sort. The wiring sets top-k ONLY on Descending: `if primary.direction == SortDirection::Descending { operator.set_topk(...) }` READ(execute/operator_tree.rs:3393), and `can_topk = limit.is_some() && !distinct` READ(:3377) so DISTINCT also kills it. ScanTopK is DESCENDING-only (READ provider.rs:106-122); resolve_topk_directive declines on any residual filter and needs exactly one POM (READ operator.rs:596-631); SortOperator has NO set_row_budget impl (READ: absent from sort.rs) so it swallows the LIMIT regardless of direction. CORPUS(q046 DESC prunes) vs probe-01 (ASC), probe-12 (expr), probe-15 (deep OFFSET ASC).

4. LIMIT budget-forwarding cliffs — the budget forwards through only offset/join/limit/graph/dataset/project/union/bind (F17 added UNION+BIND). OPTIONAL, MINUS, SORT(non-DESC), DISTINCT, HAVING/GROUP, unabsorbed FILTER, subquery, VALUES all use the no-op default and SWALLOW the budget. READ(set_row_budget impls: operator.rs:101 default-noop, offset.rs:145, join.rs:1132, limit.rs:85, graph.rs:357/536/639, dataset_operator.rs:222/371, project.rs:58, union.rs:231, bind.rs:153; ABSENT in distinct/sort/filter/having/aggregate/optional/minus/values/subquery.rs). A `LIMIT k` above any swallowing operator leaves the driving FACT scan unbounded. Probes 04/04b, 11, 15.

5. Fused-aggregate function coverage is COUNT/COUNT(col)/SUM(List)/AVG(List) ONLY — MIN, MAX, GROUP_CONCAT, SAMPLE, and every DISTINCT aggregate decline to the generic pipeline → full FACT materialize + group. READ(fused_aggregate.rs:385-393 foldable set, :1785 resolve_agg_folds → None otherwise; CountDistinct is the unmatched variant :384 comment). MIN/MAX are a double miss — answerable directly from the same Iceberg column min/max stats the pruner already reads (READ pruning.rs:317-374), yet no stats shortcut analogous to PR-1 exists. Probes 05, 06, 17.

6. Multi-FACT aggregate join is uncovered. The fused JOIN fold (E2) is a fact→DIM tree; order_chain declines on branch/merge/cycle/disconnected (READ fused_aggregate.rs:1809-1864), and route_group_key_sources declines on ≥2 sources / interior-dim keys (READ :918-933). A FACT→FACT aggregate (e.g. SUM of payments per order) is not that shape and has ZERO corpus members (verified: only q015/q016/q017/q068 relate two facts, all single-FK OPTIONAL/anti-join/crawl, none an aggregate) → generic join materializes both facts. Probe 09.

7. Type/shape pushdown seams: (a) xsd:dateTime has no ScanValue variant and no stat_bounds arm (READ provider.rs:44-70, pruning.rs:317-374) — the real column FACT_WEB_EVENT.EVENT_TS (mapping:235) prunes NOTHING while xsd:date prunes fine; (b) constant-object equality with a decimal/double/IRI object is operator-enforced only (READ provider.rs:74-91, operator.rs:669-677) while the FILTER form of the same numeric compare DOES push (ScanValue::Decimal, numeric-stats default-on) — a pure surface asymmetry. Probes 07, 08/08b.

8. Partition-transform pruning is inert — `can_contain_partition` ignores its partition-spec and summary args (all underscore-bound) and returns `_ => true` for every non-boolean-structural expr, and the planner never calls it (READ pruning.rs:32-64, planner.rs:238 uses only can_contain_file). All real pruning is file-level column min/max; identity/bucket/truncate/day/month partition layouts give ZERO extra pruning. INFERRED cliff on any partitioned source whose partition column lacks per-file value stats. (Not separately probed — the bench layout is unpartitioned; flagged for a partitioned fixture.)

## 1. Query-shape coverage matrix

Columns: FUSED-AGG (single/join fold), TOPK (scan-side), BUDGET (LIMIT forwarding), PRUNE (file stats pushdown), LOWERED (rewrite → R2RML scan vs pass-through/error). Cell = does the mechanism apply on this shape, and if not, what runs instead.

| Shape | FUSED-AGG | TOPK | BUDGET | PRUNE | LOWERED / fallback |
|---|---|---|---|---|---|
| BGP star (dims) | n/a | n/a | yes | eq/range yes | yes — CORPUS(q001-q004) READ(rewrite.rs:142-171) |
| BGP star (FACT) + LIMIT | n/a | n/a | yes (LIMIT_PUSHDOWN) | yes | yes — READ(operator.rs:219-225) CORPUS(q045) |
| fact→dim FK chain | n/a | n/a | partial | dim-side yes | yes; parent-scan (trust_fk_refs OFF) CORPUS(q008/q010) |
| path-join fact→dim SUM | join-fold yes | n/a | n/a | n/a | yes — CORPUS(q062) READ(fused_aggregate.rs:1809) |
| multi-FACT aggregate join | NO decline | n/a | n/a | per-scan | UNCOVERED-CLIFF — INFERRED, no corpus member; probe-09 |
| OPTIONAL single | n/a | n/a | NO swallowed | inner yes | LOWERED but BUDGET-CLIFF READ(optional.rs no set_row_budget) CORPUS(q016/q050) probe-04 |
| OPTIONAL nested/multi | n/a | n/a | NO | inner yes | LOWERED; correlated-batch PR-4b/4c/4d; UNKNOWN depth scaling |
| UNION | n/a | n/a | YES (F17) | inner yes | LOWERED READ(union.rs:231) CORPUS(q029/q042) |
| MINUS | n/a | n/a | NO swallowed | inner yes | LOWERED but generic anti-join; ZERO corpus members — probe-10 |
| FILTER NOT EXISTS | n/a | n/a | NO | inner yes | LOWERED; anti-join NOT known-batched — tail CORPUS(q017/q053) probe-18 |
| FILTER EXISTS (positive) | n/a | n/a | NO | inner yes | LOWERED but UNKNOWN-PROBE (no corpus member) |
| VALUES (ref subject) | declines | n/a | NO | NO not lowered | UNCOVERED-CLIFF CORPUS(q040) READ(rewrite.rs:231) |
| VALUES (scalar attr) | declines | n/a | NO | NO not lowered | UNCOVERED-CLIFF READ(fused_aggregate.rs:353-358) probe-03 |
| FILTER IN (set) | n/a | n/a | n/a | NO no ScanCmpOp::In | UNCOVERED-CLIFF READ(provider.rs:27-34) probe-02 |
| BIND (pre-agg) | yes (agg_binds) | n/a | YES (F17) | n/a | yes READ(fused_aggregate.rs:363-369, bind.rs:153) |
| subquery | declines | n/a | NO | n/a | LOUD-ERROR on virtual READ(rewrite.rs:215-218) CORPUS(q051) |
| property path (transitive +/*) | n/a | n/a | n/a | n/a | LOUD-ERROR READ(rewrite.rs:207-210) CORPUS(q034) |
| property path (sequence a/b) | n/a | n/a | n/a | dim yes | LOWERED (decomposed upstream to triples) CORPUS(q035) slow |
| GROUP BY single/multi key | fold yes | n/a | n/a | n/a | yes CORPUS(q008/q014/q060) |
| GROUP BY mixed fact+dim keys | fold yes (W4-2) | n/a | n/a | n/a | yes CORPUS(q066/q067) READ(fused_aggregate.rs:918) |
| HAVING (agg in SELECT) | fold yes (PR-6) | n/a | n/a | n/a | yes READ(fused_aggregate.rs:405-412) CORPUS(q009/q025) |
| HAVING (agg NOT in SELECT) | NO decline | n/a | n/a | n/a | UNCOVERED-CLIFF READ(fused_aggregate.rs:405-412) probe-16 |
| COUNT(*) | manifest shortcut | n/a | n/a | n/a | yes READ(fused_aggregate.rs:1122-1146) CORPUS(q036) |
| COUNT(?col) | column fold | n/a | n/a | n/a | yes READ(fused_aggregate.rs:1733-1737) |
| COUNT DISTINCT | NO decline | n/a | n/a | n/a | UNCOVERED-CLIFF READ(fused_aggregate.rs:384-393) probe-05 |
| SUM / AVG (List) | fold yes | n/a | n/a | n/a | yes CORPUS(q010/q062) |
| SUM / AVG (DISTINCT/Set) | NO decline | n/a | n/a | n/a | UNCOVERED-CLIFF READ(fused_aggregate.rs:387-389 InputSemantics::List) |
| MIN / MAX | NO decline | n/a | n/a | stats exist | UNCOVERED-CLIFF + missed stats shortcut READ(:1785) probe-06 |
| GROUP_CONCAT / SAMPLE | NO decline | n/a | n/a | n/a | UNCOVERED-CLIFF + memory risk READ(:1785) probe-17 |
| ORDER BY DESC(col) LIMIT | wrap-sort | YES | via topk | reads fewer files | yes READ(provider.rs:106) CORPUS(q046) |
| ORDER BY ASC(col) LIMIT | wrap-sort | NO DESC-only | NO | NO | UNCOVERED-CLIFF READ(operator_tree.rs:3393) probe-01 |
| ORDER BY expr LIMIT | declines | NO | NO | NO | UNCOVERED-CLIFF READ(fused_aggregate.rs:316) probe-12 |
| OFFSET deep pagination | n/a | NO (if ASC) | k+offset | NO | UNCOVERED-CLIFF READ(offset.rs:145) probe-15 |
| DISTINCT / REDUCED + LIMIT | n/a | n/a | NO swallowed | n/a | UNCOVERED-CLIFF READ(distinct.rs no set_row_budget) probe-11 |
| ASK | n/a | n/a | UNKNOWN | inner yes | UNKNOWN-PROBE (no corpus member) probe-13 |
| CONSTRUCT | via WHERE | n/a | LIMIT on WHERE | WHERE yes | LOWERED (WHERE rewritten normally) CORPUS(q048/q049) |
| DESCRIBE | n/a | n/a | n/a | bound-subj prune? | UNKNOWN-PROBE (no corpus member) probe-14 |
| SERVICE (virtual+native) | n/a | n/a | n/a | n/a | UNKNOWN-PROBE — recurse arm READ(rewrite.rs:180) may mis-scope |

## 2. Surface parity

Both JSON-LD and SPARQL parse to the SAME IR then share maybe_wrap_for_graph_source + build_executable, so the core rewrite/fusion admission is surface-agnostic in principle. READ(view/query.rs:100-117, 28-42). INFERRED: JSON-LD reaches the same rewrite. BUT three seams. (a) The fused-aggregate detector is SHAPE-FRAGILE: it requires the top-level patterns to be exactly one `Graph{Iri}` block (Triple + ≤1 FILTER inner) optionally followed by BIND, and declines on any extra top-level pattern or non-Triple/Filter inner. READ(fused_aggregate.rs:341-369). A JSON-LD query that lowers to a different top-level structure (a second GRAPH, a top-level VALUES/FILTER, a multi-select) silently misses fusion — consistent with the reported 900s JSON-LD-multi-query runaway. UNKNOWN-PROBE: run probes 02/03/05/09 as JSON-LD and confirm files_selected matches the SPARQL run. (b) The GRAPH `query()` path and the `FROM`/dataset path are DIFFERENT operators: dataset_operator forwards budget/topk to its member (READ dataset_operator.rs:222/371), but the C5/C1 dataset-path admission is separately gated — the deployed chat uses the FROM path, which the pre-q060 corpus never exercised (that is why q060-q068 exist). (c) F9 CURIE alignment was sparql_json-only (#1499) — an OUTPUT-format surface gate: a JSON-LD-format result may not get compact-id alignment. INFERRED formatting-parity gap, not an admission gap.

## 3. Iceberg feature coverage

DELETE FILES — position + equality deletes are NEVER applied; only detected to gate the COUNT(*) shortcut. READ(manifest_list.rs:21-28, planner.rs:224, stats.rs:311-350). CORRECTNESS-CLIFF conditional on source MoR (gap #1). SCHEMA EVOLUTION — projection is by field_id (READ planner.rs:279-287, id-stable across renames), but the R2RML mapping refers to columns by NAME; a renamed/dropped source column breaks the name→column resolution (availability/correctness, not perf). UNKNOWN-PROBE (needs a schema-evolved fixture). PARTITION SPECS/TRANSFORMS — pruning is file-level column min/max only; can_contain_partition is inert (gap #8) READ(pruning.rs:32-64). SNAPSHOT/AS-OF — as_of_t threads through scan_table/table_row_count/compiled_mapping (READ provider.rs:207-241) but "no meaningful dataset t" in multi-ledger mode (doc :157) — time-travel on the FROM path is INFERRED-thin, UNKNOWN-PROBE. NULL SEMANTICS — COUNT(*) shortcut requires every non_null_col provably zero-null else falls back (READ provider.rs:225-241, sound); subject-template keys with NULL components were the pr-1494 subject-key NULL-drop area (memory) — INFERRED handled but verify. STATS-PRUNING TYPE COVERAGE — bool/int32/int64/date/float32/double/FLBA-decimal/bytearray-string all have stat_bounds arms (READ pruning.rs:317-374); TIMESTAMP/dateTime does NOT (gap #7); NaN bounds are neutralized (F15 history, READ provider.rs:52-54). WIDE TABLES / 0-row / empty-snapshot — no evidence of a wide-projection or empty-snapshot special case; UNKNOWN-PROBE (bench tables are moderate width, non-empty).

## 4. Execution-path folds — decline branches classified

detect_fused_r2rml_aggregate declines: order_binds present → generic (READ :316, ACCEPTABLE for DESC-col via wrap-sort, CLIFF for ASC/expr); GROUP BY without aggregates → generic DISTINCT-style (READ :330, acceptable); post-aggregate BIND → generic (READ :337, acceptable-rare); first pattern not Graph{Iri} → generic (READ :344-349, the surface-fragility seam §2); inner non-Triple/non-single-Filter (VALUES/UNION/OPTIONAL/nested) → generic (READ :353-358, CLIFF for VALUES); FILTER without GROUP BY → generic (READ :375-377, ACCEPTABLE — file-prune + vectorized filter is faster); non-List Sum/Avg or non-Count/Sum/Avg func → generic (READ :385-393, CLIFF §5); projection ≠ outs or ORDER BY var ∉ outs → generic (READ :405-412, CLIFF for HAVING-unprojected). order_chain declines: branch/merge/cycle/disconnected → generic join (READ :1825-1864, CLIFF multi-FACT §6). route_group_key_sources declines: ≥2 sources / interior-dim key → generic (READ :918-933, ACCEPTABLE narrow). resolve_topk_directive declines: residual filter present, or sort var maps to ≠1 POM, or non-DESC → full scan (READ operator.rs:596-631, CLIFF ASC/filtered-topk). build_scan_filters skips: predicate maps to ≠1 scalar POM (READ :652-661, soundness, ACCEPTABLE), IRI/decimal/double object constant (READ :669-691, CLIFF §7). rewrite: PropertyPath/ShortestPath/Subquery → LOUD-ERROR (READ :207-218, correct-refuse not cliff); Filter/Bind/Values/Unwind/Service-inner preserved un-lowered (READ :228-242, CLIFF for Values). set_row_budget default no-op on distinct/sort/filter/having/aggregate/optional/minus/values/subquery → budget dies at that boundary (READ, CLIFF §4). The R2RML operator itself records but does not forward budget to its child (READ operator.rs:2549-2550, by design — the child is the scan stream).

## 5. Probe battery

18 probes + 3 controls in `probes/` (see `probes/probes.md` for priority order and telemetry-reading). Priority head: probe-02 (FILTER IN), probe-03 (scalar VALUES), probe-01 (ASC top-k), probe-04/04b (OPTIONAL budget), probe-09 (multi-FACT join), probe-07 (dateTime prune), probe-05 (COUNT DISTINCT), probe-06 (MIN/MAX), probe-08/08b (constant-object decimal). The delete-file correctness gap (#1) is not runnable against the append-only bench — probes.md specifies the MoR fixture required.
