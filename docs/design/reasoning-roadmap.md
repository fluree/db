# Reasoning Roadmap

Status: **proposed** (June 2026). Synthesized from code analysis of the current
reasoner, the incremental-materialization literature, and research into how
ontologists and knowledge-graph practitioners actually work. Two axes: (A)
reasoning performance — static materialization first, then incremental
maintenance on a changing ledger; (B) UX/DX for knowledge-graph practitioners.

## 1. Current state

- **OWL2QL** is handled at query time (rewriting); **OWL2RL** by materialization
  against a snapshot. `reason_owl2rl` (`fluree-db-reasoner/src/lib.rs`) runs a
  semi-naive fixpoint and caches the result in a 16-entry LRU keyed by
  `(ledger_id, db_epoch, to_t, overlay_epoch, ontology_epoch, modes, config_hash)`
  (`cache.rs`). **Any commit changes the key → full re-materialization.** There
  is no retraction handling; deletes just invalidate the cache.
- Derived facts live only in a virtual `DerivedFactsOverlay` (default-graph
  only), rebuilt per state change — never committed, never indexed.
- Fixpoint hotspots: seeding does full PSOT scans per rule-relevant predicate
  plus the whole `rdf:type` extent and merges seeded base facts into the
  derived set; identity rules (prp-fp/ifp/key, max-cardinality) rescan
  delta∪derived per iteration; single-threaded; heavy `Sid`/`Flake` cloning;
  `max_memory_bytes` unenforced; the user-datalog lane is uncached and re-runs
  on every query prepare.
- Latent bugs: the cache key hardcodes `ReasoningModes::default()`;
  `enabled_rules` feeds the cache hash but does not filter rules.
- LUBM passes (derived-overlay join bugs fixed in PR #1294). Ledger-wide
  `f:reasoningDefaults` is applied via `merge_reasoning`
  (`fluree-db-api/src/config_resolver.rs`) with override control. Still
  missing: SPARQL per-query reasoning — lowering hardcodes
  `ReasoningConfig::default()` (`fluree-db-sparql/src/lower/mod.rs`).
- Raw material for incrementality exists: commits carry exact (asserts,
  retracts) deltas as op-flagged flakes; the indexer has a proven async sidecar
  pattern (stats/spatial/fulltext); novelty `fact_state` computes net
  add/remove semantics.

## 2. Design principles (from the literature and established practice)

1. **Hybrid, stratified by cost class.** Keep cheap, hierarchy-shaped
   inference (QL/RDFS) at query time via rewriting; materialize the RL
   closure, whose recursive rules (transitive closure, property chains,
   sameAs) are too expensive to expand per query. The open problem is then
   *maintenance* of the materialization, not the strategy choice.
2. **Insert-only updates need no special machinery** — semi-naive evaluation
   over the delta is optimal in every maintenance-algorithm family, and
   inserts dominate Fluree's workload. Only retracts need a real algorithm.
3. **Special-case `owl:sameAs`.** Equality through generic rules produces
   quadratic closure; mature systems use canonical-representative rewriting
   (union-find) with dedicated retraction handling.
4. **Firewall ontology changes from data changes.** Ontology edits are rare
   and may pay a bigger, coarser recompute; data commits must stay cheap.
5. **Static-performance playbook**: compile the active ontology into
   ontology-specific ground rules (replacing generic rule-family dispatch —
   several-fold wins are reported in the literature), index rules by the facts
   that can fire them (`rulesFor`), and parallelize the fixpoint
   fact-at-a-time; near-linear multicore scaling is achievable.
6. **Make staleness first-class.** A materialized-inference store should
   expose a queryable "reasoned through" watermark rather than pretending the
   closure is always current.

Key papers: DRed (Gupta/Mumick/Subrahmanian, SIGMOD 1993); the
Backward/Forward algorithm (Motik/Nenov/Piro/Horrocks, AAAI 2015);
"Maintenance of Datalog Materialisations Revisited" (AIJ 2019 — including the
empirical comparison of DRed/counting/FBF); incremental `owl:sameAs`
maintenance (IJCAI 2015); stratum-scoped maintenance under rule-set changes
(ZodiacEdge, 2023).

## 3. Axis A — performance

### A1. Static materialization

- **A1.1 — fixpoint engine hygiene (weeks).** Stop merging seeded base facts
  into the derived overlay; incremental grouping maps for identity rules; cache
  the overlay post-build (stop re-sorting four index copies per query);
  path-compress the union-find; intern `Sid`s to dense u64 ids per run.
  Publish a LUBM timing baseline — none exists; A1.2/A1.3 must be sized
  against it.
- **A1.2 — rule compilation + rule indexing (1–2 months, keystone).** Compile
  the active ontology into ontology-specific ground rules once per
  `ontology_epoch`, replacing fixed rule-family dispatch, with a
  `rulesFor(predicate/class)` index. A1.3, A2, and B1 all consume this
  artifact. Do not start A2 before this.
- **A1.3 — parallel fixpoint (1–2 months).** Fact-at-a-time parallel
  semi-naive over compiled rules. Input is an immutable snapshot: keep
  sorted-index seeding, hash-indexed working sets.
- **A1.4 — import-time reasoning = post-import pass, not streaming.**
  Streaming inference inside the import sink is rejected (chunk-local IDs, no
  global view, RL closure needs the complete ontology). Instead run an
  automatic post-import materialization pass against the freshly built index,
  persisting via the A2 path (incremental maintenance with E− = ∅), so a
  ledger comes up "reasoned" instead of paying closure on first query. Rule
  compilation can pipeline with data indexing when a schema source is
  identifiable up front (`f:schemaSource`, else TBox-predicate detection).

### A2. Incremental maintenance (no full re-reasoning required)

**Algorithm: FBF (Backward/Forward).** Insert-only commits (the dominant case)
run plain semi-naive over the commit delta. Commits containing retracts run
FBF: mark retract-affected derived facts doubtful, backward-chain against the
old materialization + explicit facts (provided natively by snapshot-at-t−1)
for surviving proofs. FBF needs **zero persistent auxiliary state**, keeping
the materialization a pure function of (rules, data-at-t) — rebuildable at any
t, consistent with immutability and time travel. The AIJ 2019 empirical study
shows the backward/forward family dominating on small updates, which is
exactly the per-commit case.

Rejected alternatives: *counting* (per-fact counters maintained on every
commit, complex under recursion, poor fit when retracts are rare); *full
justification storage* (derivation sets are often orders of magnitude larger
than derived facts — AIJ 2019); *differential dataflow* (RAM-resident
arrangements with multi-x memory blowup, wrong operational shape for
CAS-backed time-indexed indexes; revisit only if novelty grows native
count-merge); *pure DRed* (FBF strictly generalizes it and wins on small
updates).

**Storage: a "reasoned index head" riding the branching plumbing — not branch
commits, not inferred-marked entries in the main indexes.**

Derived facts live in **derived-only index artifacts** under a branch-like
namespace with their own index root, pinned to a main
`(commit_t, ontology_epoch, rule_config_hash)`. Reused branching machinery:

- `NsRecord` shape (own `index_head_id`/`index_t`, `source_branch` pointer,
  `create/drop/reset` lifecycle) — `reasonedThroughT` *is* this record's
  `index_t`;
- `BranchedContentStore` read-through — the reasoned namespace holds only
  derived artifacts and reads through to the base namespace (no base-index
  copy; N rule configs = N small derived indexes);
- alias routing (`name:reasoned`) plus per-query `"reasoning": "owl2rl"`, both
  resolving to the same artifact;
- per-head background worker and the `graph-sources/{name}/{branch}/` sidecar
  layout (fulltext/vector precedent) — reasoning composes per branch of the
  base ledger.

Query-time composition is a three-way merge (base index + reasoned index +
novelty), same shape as the existing overlay merge. Non-reasoning queries
never open the reasoned root — zero cost, **no inferred-marker bit needed**
(separation by artifact, not by bit).

A literal reasoning *branch* ("merge main + reason") was evaluated and
rejected on three code-verified grounds: (1) rebase semantics replay the
branch's own commits — wrong for derived facts under retracts (FBF must
recompute, not replay) — and every rebase physically copies main's index
artifacts into the branch namespace; (2) branch novelty is reconstructed from
commits, so persistence would force derived-fact commits, and replay/reindex
would then resurrect stale inferences (breaking "reindex re-derives"); (3)
branch t diverges from main t at the first derived commit, killing
time-travel-with-reasoning. (Note: merge/rebase/merge-preview *are* on main —
`fluree-db-api/src/{merge,rebase,merge_preview}.rs`; the reasoned-head
NsRecord design should be coordinated with the custom-merging work on
`feature/merge-preview3`.)

**Consistency model.** The reasoning sidecar lags commits; the reasoned head's
pinned t makes staleness first-class: surfaced in query-response metadata and
ledger status, with a per-query option forcing the existing overlay path for
read-your-writes freshness. Any t lacking persisted derivations falls back to
today's overlay computation — no t regresses. Full reindex drops reasoned
heads and re-derives; the commit log remains the sole source of truth.

**Ontology vs data changes.** Detect TBox-touching commits by predicate match
on the delta (`subClassOf`, `domain`/`range`, `intersectionOf`,
`someValuesFrom`, `hasKey`, …). Data commits run FBF; ontology commits trigger
stratum-scoped recompute (dependency-hypergraph approach per ZodiacEdge) with
full re-reason as fallback. Rule addition = semi-naive with the new rules
only; rule removal = treat that rule's consequences as retracts → FBF.

**sameAs retraction.** Never through generic rules. v1: recompute the affected
equivalence class's connected component. v2: the IJCAI 2015 incremental
algorithm (equality deletion is non-monotonic — it can *restore*
previously-rewritten facts).

**Lineage/provenance placement** (for explanations):

- **Not in `FlakeMeta`** — meta participates in `Flake` Eq/Hash, all four
  index comparators, novelty `FactKey`, and v3 sort keys; lineage there makes
  an asserted+derived (or twice-derived) fact two distinct index rows with
  broken retraction matching. Variable-length premise refs also cannot fit the
  fixed 32-byte `RunRecordV2`.
- **Not as stored RDF-star/edge annotations** — under the
  `feature/edge-annotations` storage model a 2-premise derivation costs 12
  flakes × 4 orders ≈ 7–13× index amplification vs the bare fact, and the
  first annotation flips a sticky ledger-wide `has_annotations` bit (every
  base-edge retract pays a cascade lookup thereafter). OWL2RL closures rival
  base data in size; prohibitive.
- **Yes: provenance sidecar stream** beside the reasoned index leaves
  (`history_sidecar.rs` precedent): keyed by v3 fact identity → `(rule_id,
  premise keys)`, ~85–90 B per derived fact, fetched only on explanation
  queries, supports multi-derivation.
- **Exposed as *virtual* RDF-star when RDF 1.2 lands**: main already
  BIND-projects `f:t`/`f:op` virtually (`fluree-db-sparql/src/lower/rdf_star.rs`),
  and the edge-annotations branch's `expand_edge_annotation_patterns` is a
  pre-planner rewrite — `<< ?s ?p ?o >> f:derivedBy ?rule` expands into
  sidecar lookups. Standard queryability, zero physical amplification.

## 4. Axis B — UX/DX for knowledge-graph practitioners

Research headline: **no real "connect Protégé to a triplestore" integration
exists anywhere** (Protégé's SPARQL tab is local-only; third-party connector
plugins are folklore). Ontologists live in OWL *files* and ROBOT/ODK CI
pipelines (merge → reason → validate-profile → diff → release). Supporting
Protégé users means meeting the file-and-pipeline workflow.

- **B0 — table stakes (with A1.1).** SPARQL per-query reasoning (protocol
  param and/or `PRAGMA`); make `enabled_rules` actually filter; fix the
  hardcoded cache-key modes.
- **B1 — explanations.** Proof trees for any inferred fact — consistently the
  most-valued reasoning feature among practitioners. Built on the A2
  provenance sidecar; reconstruct trees on demand from per-fact `(rule,
  premises)` tags rather than storing whole proofs. Design the sidecar format
  during A2.
- **B2 — Protégé-realistic workflow.** OWL file round-trip (clean
  Turtle/RDF-XML export of a schema graph; fix the Turtle
  `owl:intersectionOf` collection-flattening bug); **`versionIRI ↔ ledger t`
  mapping** with structural axiom diff between any two t's (robot-diff
  semantics over native history — Fluree's differentiator: ontology
  versioning for free); ROBOT-style CLI verbs (`reason --assert-inferred`,
  `validate-profile`, consistency check) for ODK pipelines; store-side
  `owl:imports` resolution (extends existing `f:schemaSource` machinery).
- **B3 — inspection & toggles.** Asserted-vs-inferred scoping falls out of the
  reasoned-head artifact separation (query base-only vs composed) — no
  pseudo-graph filtering needed; inferred-set diff between t1 and t2; named
  reasoning profiles per query (schemaSource bundles + config hash = one
  reasoned head each).
- **B4 — SHACL/OWL division.** SHACL = closed-world validation, OWL =
  open-world inference; ship "inference feeds validation" (SHACL evaluated
  over the materialized closure), not semantic unification.

## 5. Sequencing

1. **A1.1 + B0** now; publish the LUBM baseline.
2. **A1.2 rule compilation** — keystone; A1.3, A2, B1 consume it.
3. **A2 reasoned head + FBF**, designing the provenance sidecar (B1) at the
   same time; A1.4's post-import pass is A2 with E− = ∅.
4. **B2 is independent, can start anytime** — highest differentiation per
   engineering dollar (ontology versioning on an immutable ledger).

Open items: ~~root-cause the historical `fluree index` blank-node
regression~~ resolved June 11, 2026 — does not reproduce on
`refactor/resoning-2` (LUBM-1 import → reindex → identical
reasoning-complete suite; the FINDINGS.md Chair→0 entry predated the
`canonical_split`/#1294-era fixes); guarded by
`fluree-db-api/tests/it_reasoning_reindex.rs`. Remaining: coordinate the
reasoned-head record with `feature/merge-preview3`'s custom-merging work.
