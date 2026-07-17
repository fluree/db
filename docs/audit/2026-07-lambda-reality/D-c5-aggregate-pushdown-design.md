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

## Full-depth design (team-lead's (a)–(g))

### (a) Current decline receipts — where a grouped rollup declines TODAY (file:line)
The fused path is gated at three layers; a deployed rollup dies at the FIRST one it fails.
1. **`detect_fused_r2rml_aggregate`** (`fused_aggregate.rs:303`, runs at `operator_tree.rs:2474`):
   - `:311` expression-ORDER-BY → decline; `:325` GROUP-BY-without-aggregates → decline;
   - **`:338-345` the first pattern MUST be `Pattern::Graph { GraphName::Iri }`** ← the load-bearing
     suspect for the dataset path (see the hypothesis above);
   - `:352` any inner pattern that isn't a Triple or a single Filter → decline; `:370` a FILTER with
     no GROUP BY → decline; `:377-389` a non-`List` (DISTINCT) or non-COUNT/SUM/AVG aggregate →
     decline; `:400-407` projection/ORDER-BY not exactly {group keys ∪ agg outputs} → decline.
2. **`resolve_at_open`** (`:1223`, at operator open): `:1257` `rewrite_patterns_for_r2rml` leaves any
   triple unconverted (graph not recognized as R2RML) → decline; `:1264-1283` the rewrite yields
   neither a single `Pattern::R2rml` (single-table) nor an all-`R2rml` chain under the join
   sub-switch → decline.
3. **`resolve_join_at_open`** (`:1586`, the fact→dim case): `:1594` a FILTER → decline; `:1600`
   `order_chain` isn't a linear `fact→dim…` chain → decline; `:1633` a composite (multi-column) FK →
   decline; `:1658` a GROUP-BY var that isn't a scalar column on the terminal dim → decline; `:1683`
   no GROUP BY (implicit aggregate over a join) → decline; `:1711` a fact object var that isn't a
   scalar column → decline.

**So the fold is complete; slice 1 = find and lift the FIRST decline the deployed grouped-COUNT
shapes hit.** The EXPLAIN + trace above says which — `detect:338` (dataset-path wrapper, most likely)
or `resolve_at_open:1257` (rewrite can't resolve the dataset-path graph as R2RML).

### (b) Slice-1 mechanism — it EXISTS; from the IR
- **Single-dim COUNT (family B)**: `detect` → single `Pattern::R2rml` at `resolve_at_open:1264` → the
  `'scan` fold (`:1049`) counts per GROUP-BY column value over the one scan, no value decode.
- **Fact→dim COUNT (family C, COUNT subset)** — the team-lead's (b): `detect` (multi-Triple inner) →
  `resolve_at_open:1269-1280` (join sub-switch) → `resolve_join_at_open`:
  - the GROUP-BY key lives as a **scalar attribute column on the TERMINAL dim** (`:1654-1680`,
    `scalar_column_for_var(terminal)`); the join key is the **fact's FK column** (`:1698`
    `fact_fk_cols = hops[0].0`);
  - each small dim is scanned ONCE from terminal back to fact, building a **`FK-key → GKey` map**
    (`:1754-1804`, terminal-dim scan `:1772`, interior-dim scan `:1822`);
  - the **fact scan folds per group** by probing that map with each row's FK value (`next_batch`
    `'scan` at `:1049`) — **counting per FK key with no per-row RDF materialization**, then the
    small grouped result already carries the dim attr via the pre-joined map. This is exactly
    "aggregate-by-FK-key-column at the scan + join the small grouped result to the dim."
  From the IR the fuse recognizes it as: `Pattern::Graph{Iri}` whose inner is ≥2 `Triple`s forming a
  `fact —RefObjectMap→ dim` chain (via `order_chain`), a GROUP BY on the terminal dim's attr, and a
  COUNT aggregate over the fact.

### (c) Soundness invariants (already enforced by the existing fold — slice 1 must not weaken them)
- **NULL FK** → the fact row drops: the FK child columns are in `validity_cols` (`:1698-1704`), so a
  null FK yields no ref triple and the row is excluded — matching the inner-join/materialize
  semantics (a null-FK row contributes to no dim group).
- **Duplicate dim join keys** → the #1490 guard: the terminal/interior dim maps are keyed by the
  parent join cols, and a CONFLICTING duplicate (same key → different GKey) DECLINES the whole fused
  plan (`insert_dim_gkeys`, called at `:1800`/the interior scan) — the equal-dup case is kept. Slice
  1 inherits this untouched.
- **FILTER present** → decline (`:1594`) — slice 1 is unfiltered-only (the deployed grouped rollups
  carry no row FILTER; `files_pruned=0` is because there's nothing to filter).
- **DISTINCT COUNT** → decline (`detect:377-389`, only `List`/multiset folds).
- **Multi-key GROUP BY** → **already SUPPORTED** (`group_by`/`group_cols` are `Vec`, `:1655-1680`) —
  a correction to the (v1 single-key) note: the existing path folds multi-column GROUP BY, so slice 1
  need not decline it (the deployed family-A/B/C shapes GROUP BY 2 columns, e.g. `?category
  ?department` — declining multi-key would miss the majority). Keep it supported; the parity test
  covers the 2-key case.

### (d) The decline set as hermetics (mirror the existing `fused_aggregate.rs` test roster)
A dataset-path grouped-COUNT query FUSES (asserts a `FusedR2rmlAggregate` plan, not materialize) +
parity vs the materialized answer; and each decline holds on the dataset path: FILTER-present,
DISTINCT-COUNT, composite FK, non-scalar group key, implicit-aggregate-over-join, and the
conflicting-dup-dim-key decline. The GRAPH-Iri path stays byte-identical (the admission only widens
what reaches the existing fold).

### (e) Predicted effect on the grouped-COUNT shapes
Family B (~5, single-dim COUNT GROUP BY) and any COUNT-only fact→dim (family-C subset): from full
materialization (~31 s DIM / 107–590 s FACT at 56k rows/s/core) to a columnar count over the
group-key/FK columns only — seconds for the DIMs, and for a FACT COUNT the fact scan reads only the
FK column + folds (no value columns, no RDF terms), so it's bounded by the fact's file/decode floor,
not the 336k-rows/s materialization. SUM/AVG shapes (the ~20 with a value fold) are slice 2.

### (f) Switch + kill story
Reuse the existing `FLUREE_FUSED_R2RML_AGG` (master) + `FLUREE_FUSED_R2RML_AGG_JOIN` (join sub-switch)
— no new switch. Off restores full materialization (the current deployed behavior), byte-identical.
The admission fix is inside the same gated path, so both switches kill it.

### (g) Corpus members to add (corpus-gated forever)
On DW_SF01: a **family-B** shape (`(COUNT(?x)) GROUP BY <dim col>`) and a **family-C** shape
(`(COUNT(?f)) GROUP BY <dim attr via FK>`), both exercised on the **dataset path** (a `FROM
<graph-source>` query, not `fluree.graph()`) so they gate C1's dataset-path fix AND C5's admission
together — closing the gap the team-lead flagged (the existing corpus is GRAPH-wrapped only). Bless
their oracles from the materialized answer; hash_gate=full (COUNT is deterministic).

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
