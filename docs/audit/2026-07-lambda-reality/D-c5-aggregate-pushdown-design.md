# C5 / T1.1 — Aggregate/GROUP-BY pushdown: design-gate note (for adversarial review)

**Status:** design gate for the lambda-usability epic's "new capability" tier. Draft by
audit-doc → adversarial review (lambda-audit, in lieu of AJ) → implement slice 1. This note
reframes the slice before any code, because the code investigation overturned the slate's premise.

## The reframe: the machinery already exists — C5 is (probably) a dataset-path admission gap, not "build new"

The slate scoped C5 as building aggregate pushdown from scratch. The code says otherwise.
`fluree-db-query/src/r2rml/fused_aggregate.rs` (PR-6 / F22) **already** computes:
- single-`TriplesMap` grouped `COUNT`/`SUM`/`AVG` with GROUP BY keys, folding columnar with no
  per-row RDF materialization (module doc lines 13-14; the `'scan` fold loop at `:1049`);
- the **fact⋈dim** fused aggregate — group by a dim attribute reached via an FK join, with the
  terminal/interior dim scans at `:1774`/`:1825` and the join sub-switch `FLUREE_FUSED_R2RML_AGG_JOIN`
  (`:284`) — i.e. exactly the family-C shape (the P2 "aggregate-by-FK-key + post-join" design is
  substantially present).

So the deployed family-A/B/C rollups **should** fuse — yet lambda-audit measured them fully
materialized (`files_pruned=0`, every row → RDF). Something makes them **decline** the fused path.
This is the same pattern as C1 and C3: the capability is there; the gap is that the **dataset path**
(the C1 root — every chat SPARQL is a `FROM <ledger>` dataset query through `DatasetOperator`) isn't
admitted.

## The load-bearing hypothesis (to verify, not assume)

`detect_fused_r2rml_aggregate` (`fused_aggregate.rs:303`) requires the query's **first pattern to be
`Pattern::Graph { name: GraphName::Iri(iri), .. }`** (`:338-345`); anything else returns `None`
(decline → generic materialize). The virtual-dataset wrapper `maybe_wrap_for_graph_source`
(`view/query.rs:28-42`) supplies that wrapper **only when `db.graph_source_id.is_some()`** and no
`Pattern::Graph` already exists. It is called on the dataset path too (`view/dataset_query.rs:127`
etc.), and `detect_fused_r2rml_aggregate` runs on the shared operator-tree build
(`execute/operator_tree.rs:2474`), so the dataset path **does reach** the detection.

**So the question is exactly one thing:** for a deployed `FROM <full-enterprise-byo-1:main>` rollup,
is `primary.graph_source_id` set (→ `Pattern::Graph` wrapper → detection can fire) or **not** (→ no
wrapper → `detect` declines at `:338` → materialize)? A dataset composed of graph-source members may
leave the primary `graph_source_id` unset (the sources are per-member in `DatasetOperator`), which
would make the whole family decline — the aggregate-side analogue of the C1 `DatasetOperator` gap.

## Verification (name it before designing the fix) — one EXPLAIN

`EXPLAIN` a deployed **family-B** rollup (`SELECT ?segment ?gender (COUNT(?c)) WHERE { ?c a
ex:Customer ; ex:segment ?s ; ex:gender ?g } GROUP BY ?segment ?gender`) on
`full-enterprise-byo-1:main`, and read the plan:
1. Does it show a `FusedR2rmlAggregate` operator (already fusing — then C5 is smaller than thought,
   only family-C/SUM-AVG remains), or a materializing scan + generic `GroupAggregate` (declining)?
2. If declining, WHERE: the `Pattern::Graph`/`graph_source_id` wrapper missing at `detect` (`:338`),
   or a deeper `resolve_at_open` decline (the group-key column or the dim join failing to resolve to
   R2RML columns)? A trace of `detect_fused_r2rml_aggregate` returning `None` vs `resolve_at_open`
   returning `Ok(None)` distinguishes these.

lambda-audit ran exactly this class of `EXPLAIN` to confirm C1's `DatasetOperator` path; the same
tool answers this. **The slice-1 design branches on the answer** — do not implement before it.

## Slice 1 design (grouped-COUNT-only, family B, ~5 shapes) — conditional on the verification

**Case A — declines at the `Pattern::Graph` wrapper (`detect`, most likely):** admit the dataset-path
shape into `detect_fused_r2rml_aggregate`. The fold itself is unchanged and already parity-proven for
the GRAPH-Iri path; we only widen the *admission* so a `FROM <graph-source>` rollup (whose graph
source resolves to R2RML at open, same as today) is recognized. This is the exact aggregate analogue
of C1 (the dataset wrapper lacked the forwarding; here the detection lacks the dataset shape).
- **Soundness:** the fused fold must equal the materialized answer — already true per shape; widening
  admission changes no arithmetic. The `resolve_at_open` R2RML resolution still runs and still
  declines if the source isn't a single resolvable scan. Keep every existing decline (DISTINCT,
  HAVING-lift, non-foldable aggregate, FILTER-without-GROUP-BY).
- **Scope discipline:** slice 1 = `COUNT`-only (no value-column decode) grouped by the scanned
  table's own columns. `SUM`/`AVG` and the fact⋈dim join stay slice 2.
- **Switch:** reuse `FLUREE_FUSED_R2RML_AGG` (no new switch).
- **Tests:** a dataset-path grouped-`COUNT` query fuses (asserts the `FusedR2rmlAggregate` plan / no
  materialization) + parity with the materialized answer + the GRAPH-Iri path unchanged; corpus
  parity at the PR head. Add a DW_SF01 family-B corpus member so it stays gated.

**Case B — declines at `resolve_at_open` (the group-key/dim column doesn't resolve):** the fix is in
the resolution, not the detection — scope it once the trace shows the failing resolve.

## Slice 2 (family C, ~20 shapes) — grouped SUM/AVG over the fact⋈dim join
Extends the SAME dataset-path admission to the join path (`fused_r2rml_agg_join_enabled`), plus the
`SUM`/`AVG` column fold (already a `Fold::Numeric` variant) and the FK-key columnar aggregate +
post-join (the terminal/interior dim scans already exist). Prototype `FACT_ORDER GROUP BY
orderChannel`, gated against the materialized answer. Its own design gate after slice 1 lands.

## What I most want the adversarial pass to attack
1. **The whole reframe rides on the EXPLAIN.** If the deployed rollup already fuses, C5's premise
   (families A/B/C are un-fused) is wrong and the lever is elsewhere — attack that first.
2. **Is `primary.graph_source_id` set on the `FROM <graph-source>` dataset path?** If it IS set (wrapper
   present), the decline is deeper (resolve_at_open) and Case A is the wrong fix.
3. **Parity for the deployed shapes** — nulls in the group key, the boolean-flag family-B variant
   (`ex:isCurrent true` as a triple, not a FILTER — does it even reach the fused path?), and any
   multi-column GROUP BY. The fold's correctness for these exact shapes is the ship-blocker.
4. **Does widening admission risk fusing a shape that should materialize** (a dataset with mixed
   native + graph-source members)? The admission must stay conservative (single resolvable R2RML scan).
