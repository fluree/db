# Row-returning multi-fact virtual joins (the late-materialization corridor)

Status: DESIGN ONLY — sized, not implemented. Recommended as its own unit.

This is the sized design for the S3 item of the multi-fact fused-join generality work
(db #1589). #1589 shipped S1 (K≥2 semi-join branches) and S2 (≥2 ref-IRI group keys) in
the fused *aggregate* operator; S3 — a row-returning (non-aggregate) multi-fact join
over the virtual/R2RML path — is a corridor-architecture item, not an admission-widening
of that operator, so it is captured here rather than built into it. It aligns with the
gap-4 SYNTHESIS ordering (N4 / §4): the accumulators/aggregate corridor before the
row/join corridor.

## Verdict: a new operator, not a widening of the fused aggregate

`fluree-db-query/src/r2rml/fused_aggregate.rs` is structurally a fold-to-accumulator
operator: it never emits rows, it folds typed `ColumnBatch` cells into `Acc`s keyed by
group. A row-returning join (`SELECT ?ol ?cat WHERE { … } LIMIT n`, no aggregate) has no
fold — it must emit surviving rows. Bolting row emission onto this operator would mean
minting per-row `Binding`s, which re-enters the ~56k rows/s per-row-materialization
ceiling that is the exact DNF mechanism the gap-4 SYNTHESIS calls out (N4: "a join that
emits per-row Bindings would re-enter the 56k ceiling on its output"). So the naive
version does not even solve the problem it targets.

## Where it runs today

A row-returning multi-fact R2RML join executes on the generic pipeline: one correlated
`R2rmlScanOperator` (`r2rml/operator.rs`) per BGP pattern — each scans its TriplesMap's
table, resolves RefObjectMap hops via parent-lookup maps, and materializes RDF terms
into `Binding`s — joined by the engine's standard join operators, all under a
`GraphOperator`. Every intermediate row is a fully-materialized `Binding` tuple; a
selective multi-filter join still materializes the whole driving cross-product before the
filters prune it. That is the linear-per-row cost the A/B shows widening with scale.

## The real win: the columnar late-materialization corridor

Reuse the columnar machinery #1589 already hardened, in a NEW row-emitting operator:

1. **Prune first, columnar.** For each pure-filter (semi-join) branch, build the
   keep-min-then-filter membership set with the existing `build_semi_join_membership`
   (unchanged — it already returns a `SemiJoinSet` keyed by the driving-fact FK). For
   projected dim attributes/IRIs, build `GroupKeyResolver`-style FK→value maps (the S2
   machinery). Push constant-object filters down as `ScanFilter`s.
2. **Scan the driving fact columnar**, dropping rows that miss any membership set — the
   same `row_passes_semi_joins` AND-probe, on columnar FK cells, before any term is
   minted.
3. **Late-materialize only survivors.** Mint subject/object terms (and resolve projected
   FK→IRI/attr columns) for the rows that pass — not the cross-product. This is the "late
   materialization" the ADVOCATE case names; it is the fold's structural opposite (emit
   instead of accumulate) but shares every pruning primitive.
4. **Honor LIMIT.** Forward the top-of-tree row budget into the driving scan (the
   existing `ScanTopK` / F17 budget-forwarding hooks in `r2rml/operator.rs`), so a
   `LIMIT n` stops after n survivors instead of materializing the whole join.

## Reuse vs new

Reuse: `build_semi_join_membership` (verbatim), the S2 FK→IRI resolvers,
`ScanTopK`/`ScanFilter`/budget forwarding, the decline discipline, the switch/oracle
harness. New: a row-emitting columnar operator (fold's opposite), late-materialization of
surviving rows, and detection/planning to route an eligible row-returning R2RML join to
it under a dedicated switch.

## Sizing and risk

Comparable to a fused-family PR in code volume (a new operator module + detection +
differentials), but with one genuine hard edge that S1/S2 do not have: **bounding without
a LIMIT.** With a `LIMIT`, the surviving set is bounded and the operator is safe. Without
one, a low-selectivity join can produce an unbounded survivor stream; emission is
streaming (unlike the fold's resident group table), so memory is bounded per batch, but a
sound *spilling* variant for order-sensitive (`ORDER BY` + `LIMIT` over a large survivor
set) cases is exactly the deferred decision point in SYNTHESIS §4 (the grace-hash-join /
external-sort capability). Recommend: build the streaming + LIMIT-bounded corridor first
as its own unit; take the spill/engine-choice decision at §4's pre-agreed point with the
kill criteria armed.

## Recommendation

Schedule S3 as its own unit (aligned with SYNTHESIS N4 / §4 ordering —
accumulators/aggregate corridor before the row/join corridor). S1 + S2 alone meet #1589's
bar (the two most common enterprise join-aggregate shapes one step beyond P3 now fuse
cold). This document is the sized design; the implementation is explicitly out of #1589's
scope.
