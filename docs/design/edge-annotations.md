# Edge annotations — storage internals

User-facing surface and contract live in [Edge annotations (concept doc)](../concepts/edge-annotations.md). This page is for contributors: the on-disk representation, the indexes that back annotation lookup, and the state machine the indexer maintains.

## Two layers, one source of truth

Annotations live in two coordinated places:

1. **Durable `f:reifies*` flakes** — the source of truth. Each annotation lowers to a small fixed bundle of system-controlled flakes about the annotation subject. These flakes ride the same pipeline as every other flake: commits, replay, history, policy, snapshot visibility.
2. **Annotation arena** — a derived secondary index that maps `EdgeKey ↔ annotation subject`. Rebuilt from the durable flakes at index time. Lookups merge the indexed arena with novelty under one visibility pass.

This layering is why a ledger with no annotations creates zero annotation artifacts: the arena is `Option<AnnotationIndexRoot>` on `IndexRoot`, and it's `None` when no annotations have ever been observed.

## `f:reifies*` durable encoding

Seven Fluree-namespaced predicates encode an attachment. The bundle is atomic: every reifies-bundle assert and retract is a complete set of flakes, never split.

```text
_:ann   f:reifiesGraph     <named-graph-iri>      # optional, omitted for the default graph
_:ann   f:reifiesSubject   <s>                    # required, IRI ref
_:ann   f:reifiesPredicate <p>                    # required, IRI ref
_:ann   f:reifiesObject    <o>                    # required, any FlakeValue
_:ann   f:reifiesDatatype  <dt>                   # optional (JSON-LD lowering omits; arena recovers from f:reifiesObject's flake-level dt)
_:ann   f:reifiesLang      "fr"                   # optional, only for langString objects
_:ann   f:reifiesListIndex 3                      # v1: always omitted, deferred
```

Rules:

- **Bundle flakes share a graph.** Every `f:reifies*` flake in one bundle has the same flake-level `g`. The decoder rejects mixed-graph bundles outright (`MixedFlakeGraphs`).
- **`f:reifiesGraph` value must match the bundle's flake-level `g`.** Named-graph edges carry `f:reifiesGraph`; default-graph edges omit it. Disagreement is `GraphMismatch`.
- **Reserved predicates are firewalled at application write surfaces.** `is_reserved_reifies_predicate(sid)` rejects user-authored `f:reifies*` mention in JSON-LD insert/update/upsert/where+delete+insert and SPARQL UPDATE (all clauses). Bulk import is an administrative bootstrap path and may ingest already-lowered `f:reifies*` bundles; import records `has_annotations=true`, and the indexer builds the arena from those durable facts.
- **Replay validation skips malformed bundles.** A partial bundle (missing required slot), a duplicate slot, or a graph-mismatch is skipped at warmup / arena-build with a `tracing::warn!` and counted in `AttachmentNovelty::observed_malformed_bundle_count()`. The annotation's *non*-`f:reifies` metadata facts remain visible as ordinary RDF; only the attachment binding is lost.
- **Cross-commit multi-target is detected at arena build.** A single annotation SID can reify two different edges across *separate commits* (each bundle is individually well-formed, so the per-bundle decode above can't catch it — the bundles are grouped by `(graph, ann_sid, t, op)`). The transaction path rejects this at stage time (see [Stage-time invariants](#stage-time-invariants)), so it only arises from malformed bulk-import data. `build_arenas_from_flakes` / `build_arenas_from_event_pairs` count annotations whose *net live* state resolves to more than one edge and surface the count on `ArenaBuildOutput.multi_target_annotations` with a `tracing::warn!`. The arena is event-sourced, so the affected reverse lookup returns multiple live edges for those annotations; the count is a data-quality signal, not a rejection (import does not validate input).

## EdgeKey

The arena keys on `EdgeKey`, which captures the edge identity from a flake minus the `t/op/m`-bookkeeping that's tracked separately on attachment rows.

```rust
pub struct EdgeKey {
    pub g:      Option<Sid>,   // None = default graph
    pub s:      Sid,
    pub p:      Sid,
    pub o:      FlakeValue,    // refs, literals, langStrings — full canonical value
    pub dt:     Sid,
    pub lang:   Option<String>,
    pub list_i: Option<i32>,   // v1: always None, reserved for list-occurrence annotations
}
```

`EdgeKey::from_reifies_facts(&[Flake]) -> Result<Self, EdgeKeyDecodeError>` decodes a bundle and enforces the rules listed above. `EdgeKey::to_reifies_facts(ann, t, op)` emits the full bundle including `f:reifiesDatatype`; `EdgeKey::to_reifies_facts_jsonld_compatible` omits `f:reifiesDatatype` so the inverse retract bundle matches the assertion shape produced by the JSON-LD lowering (asserting one shape and retracting the other would leave a phantom retract).

## Sidecar arena layout

The arena lives in `fluree-db-binary-index/src/annotation_arena/` and mirrors the dictionary CAS trees:

- **Forward arena** — `EdgeKey -> annotation subjects`. Branches range-route on `EdgeKey`; leaves store sorted `(EdgeKey, ann_sid, t, op)` rows. Used by inline annotation queries and base-edge retract cascade.
- **Reverse arena** — `annotation subject -> EdgeKey`. Branches range-route on `ann_sid`; leaves store sorted `(ann_sid, EdgeKey, t, op)` rows. Used by `@reifies` / `rdf:reifies` annotation-rooted queries and by-id retract.

Both arenas are content-addressed, immutable, range-routable, lazily loaded. Magic numbers `EAFB1`/`EAFL1` and `EARB1`/`EARL1` mark forward/reverse branch/leaf blobs.

Per-edge multiplicity is preserved: the forward arena is a multimap on `EdgeKey`. Two `@annotation` blocks against the same base edge with anonymous subjects produce two distinct rows; two with the same explicit `@id` produce one (idempotent).

## `AnnotationStats` and planner integration

Each arena seal computes per-slot stats consumed by the planner's cardinality estimator:

```text
forward_rows, reverse_rows                  # total event rows across history
distinct_edges, distinct_annotations         # live counts
live_attachment_pairs                        # number of currently-asserted (edge, ann) pairs
distinct_reified_{subjects,predicates,objects}
reifies_graph_rows, distinct_reified_graphs, distinct_graph_anns
reifies_lang_rows, distinct_reified_langs, distinct_lang_anns
```

`stats_view::merge_annotation_stats` overlays these onto the regular `IndexStats.properties` HLL for the seven `f:reifies*` predicates so the planner gets sharp selectivity for `?ann f:reifies* <const>`-shape probes. `f:reifiesDatatype` is intentionally not synthesized from the arena — the arena reconstructs `dt` from the `f:reifiesObject` row's flake-level dt and can't tell whether the on-wire bundle emitted a separate `f:reifiesDatatype` flake. The regular HLL is the source of truth for that slot.

Every field is `#[serde(default)]` so older arena roots that predate any given field deserialize cleanly. A zero NDV means "no information"; the planner falls back to `IndexStats.properties`.

## IndexRoot signals and the sticky bit

`IndexRoot` carries three coordinated signals around annotations:

| Signal | Meaning |
|---|---|
| `annotation_index: Option<AnnotationIndexRoot>` | When `Some`, forward + reverse arena CIDs and stats are loaded lazily. |
| `has_annotations: bool` | Sticky flag: true once any `f:reifies*` SID has appeared in the predicate dictionary. Drives the cascade fast-path's zero-cost gate. |
| `had_annotation_arena: bool` | **Sticky bit, never cleared.** Lives in the FIR6 extended-flags byte (low byte of the historically-zero `pad(2)` header field). |

The truth table for the indexed-arena guarantee:

| `has_annotations` | `annotation_index` | Meaning |
|---|---|---|
| `false` | `None` | Hard guarantee: zero attachments. Cascade and reads short-circuit. |
| `true` | `Some(_)` | Builder ran. Forward/reverse arenas are authoritative for `t ≤ max_t`; novelty supplies the tail. |
| `true` | `None` | Pre-builder or defensive-drop transitional state. Snapshot may carry `f:reifies*` flakes but no arena yet — readers fall back to scan, cascade still runs. |
| `false` | `Some(_)` | **Invariant violation.** The encoder coerces `has_annotations=true` whenever an arena is present, so this state never reaches the wire. The `encode()` `debug_assert!` catches in-memory regressions in dev/CI. |

### Sticky-bit state machine

`had_annotation_arena`'s load-bearing role is **"base-index bootstrap is not allowed"**. The provider's one-time PSOT scan of `f:reifies*` flakes (`ApiAttachmentEventsProvider::scan_base_index_for_attachment_events`) is correct only when the base index is the complete history. Any indexer pass that's already touched the annotation history owns it from then on; the provider must not later reconstruct a live-only `Authoritative` arena from such a root.

The bit is set in three places, all flipping it to `true` and never clearing:

- `IncrementalRootBuilder::build()` — coerces on any incremental root with `has_annotations=true`, regardless of whether this pass sealed an arena (covers no-provider passes that left `annotation_index=None`).
- `encode_and_write_root_v6` — coerces in the full-rebuild root-assembly path, same condition.
- Decoder coercion — `IndexRoot::decode` and `LedgerSnapshot::from_root_bytes` both coerce the bit from `annotation_index.is_some()` when the wire byte is zero, covering legacy pre-extended-flags roots.

Only fresh bulk-import roots leave it `false`. Bulk import constructs `IndexRoot` directly in `fluree-db-api/src/import.rs`, bypassing both root-assembly paths. That makes `has_annotations=true, annotation_index=None, had_annotation_arena=false` the unique bootstrap-eligible state.

The bootstrap itself runs from `Fluree::reindex` even when the client has no `LedgerManager` (the CLI builds its client `without_ledger_caching()`, so `fluree create --from` and `fluree reindex` have no `AttachmentEventsProvider` to ask). When no provider resolves coverage, `reindex` derives it from the `LedgerState` it already loads (`attachment_events_from_state`): the novelty's attachment events under the same `snapshot.t == 0 → Authoritative, else Augment` rule the provider uses, and the base-index scan when the overlay holds no events and the snapshot is bootstrap-eligible. Before this the indexer received `None`, sealed nothing, and `fluree create` still printed "Annotation arena sealed"; `create` now reports what the reloaded snapshot actually carries.

### Provider 4-gate eligibility

`ApiAttachmentEventsProvider::attachment_events` boots a one-time base-index scan only when **all four** hold:

1. `try_running_attachment_events` returns an empty event set (the running overlay carries no `f:reifies*` events for this ledger).
2. `snapshot.has_annotations` is true.
3. `snapshot.annotation_index.is_none()` (no arena currently sealed).
4. `!snapshot.had_annotation_arena` (the indexer has never touched this ledger's annotation history).

When eligible, the provider walks the base index via per-predicate PSOT scans (PSOT-direction sidesteps a SPOT quirk where constant blank-node subjects don't return rows reliably across all backends), groups results by annotation SID, decodes each bundle into an `EdgeKey`, and returns `AttachmentEventCoverage::Authoritative`. The indexer then seals an authoritative arena from the events. Any subsequent indexer pass coerces `had_annotation_arena = true`, so the same ledger is no longer bootstrap-eligible — defensive drops carry the bit forward and stay in the M2a scan-fallback state until the next provider-backed reindex re-seals.

PSOT scan errors are logged with `tracing::warn!` and surface as `Option::None` (no coverage); the indexer then skips arena seal this pass instead of silently treating the failure as "no annotations."

## Reads merge arena + novelty under one visibility pass

`AnnotationArenaReader` exposes two merged lookups:

```rust
fn current_annotations_merged(edge: &EdgeKey, novelty_events: &[(Sid, i64, bool)], as_of_t: i64) -> Vec<Sid>
fn current_targets_merged(ann: &Sid, novelty_events: &[(EdgeKey, i64, bool)], as_of_t: i64) -> Vec<EdgeKey>
```

Callers (the hydration injector, the cascade pass, the planner-flattened `f:reifies*` triples) feed the arena's `collect_*_events` output into a single `(t, op)` visibility pass that resolves to currently-live state. Arena and novelty events can interleave arbitrarily — the merge applies one latest-wins pass over the union, so an arena `op = true` followed by a novelty `op = false` (or vice versa) resolves correctly without the caller doing any pre-merging.

The scan fallback (used when no arena is sealed) runs `db.range(POST, f:reifiesSubject, edge.s)` to find candidate annotation SIDs, then walks each candidate's bundle via `db.range(SPOT, s=ann_sid)` for structural decode. Bundle decode at the SPOT step bypasses view policy: the `f:reifies*` flakes are system-controlled discriminators, not user data, and policy-filtering them at decode would let an incidental policy that hides FLUREE_DB-namespace predicates collapse the bundle and drop the annotation entirely. The annotation **body** continues through the policy-filtered `format_subject` path, so user-data visibility is unchanged.

## Stage-time invariants

- **Cascade dedup is graph-aware.** Cascade retracts are deduped against existing retracts in the staged flake set via an explicit key tuple `(Option<Sid>, Sid, Sid, FlakeValue, Sid, Option<FlakeMeta>)` — `Flake`'s `Eq`/`Hash` ignore `g`, so a plain `HashSet<Flake>` would collapse two retracts targeting the same `(s, p, o, dt, m)` in different named graphs.
- **Single-target invariant (Fluree v1 storage contract).** RDF 1.2 allows one reifier to be related to several propositions; this implementation deliberately stores one live edge attachment per annotation SID so retract cascade, by-id cleanup, and the forward/reverse arena stay unambiguous. At stage time, for every annotation SID this transaction asserts a `f:reifies*` flake for, the *net* asserted bundle — current snapshot/novelty state, minus this txn's retracts, plus its asserts, deduped as an RDF set keyed on `(g, s, p, o, dt)` — is decoded via `EdgeKey::from_reifies_facts`; a malformed or multi-target result (e.g. a duplicate subject/predicate/object slot) errors with `InvariantViolation`. Validating the *net* bundle rather than counting this txn's `f:reifiesSubject` flakes catches two cases a count misses: re-pointing an `@id` already attached to a different edge in a *prior* transaction (no retract in this txn), and same-subject/different-slot multiplicity within one txn (the subject slot dedupes while predicate/object diverge). The legitimate re-point pattern — retract the old attachment + assert the new — passes because the net bundle resolves to one edge; an idempotent re-assert passes because the set-keyed net bundle is unchanged.
- **Empty `@annotation: {}` is RDF-mode no-op.** No annotation subject is minted; no attachment row is written. In LPG mode (`opts.lpgEdgeLifecycle: true`), an empty block mints a fresh property-less annotation subject so the relationship retains identity.
- **Assertion lowering scopes synthesized siblings per clause.** The standard `@annotation` lowering emits the `f:reifies*` bundle as sibling top-level nodes. For wrapper documents (`{where, insert, delete, …}`) each clause's siblings are attached to *that clause's own value*, not the top-level wrapper. Flushing them onto the wrapper would rewrap `{where, insert}` into `{@graph: […]}`, after which `parse_update` finds no clauses and silently no-ops the whole transaction — the failure mode the per-clause scoping prevents. The blank-node counter stays shared across clauses so anonymous-annotation ids don't collide.

## Cascade and lifecycle modes

The cascade gate is mode-independent: when both `snapshot.has_annotations` and `novelty.attachments.has_annotations()` are false, base-edge retracts skip the cascade lookup entirely (zero-cost gate, non-annotation ledgers pay nothing).

When annotations are possible:

- **Plain edge retract.** Look up annotations attached to the retracted edge via merged arena + novelty. Retract the complete `f:reifies*` bundle for each. Retract anonymous annotation body metadata always (RDF default). Retract explicit-IRI body metadata only when `opts.lpgEdgeLifecycle: true`.
- **By-id retract.** A delete-clause pre-pass (`lower_delete_annotation_blocks`) rewrites `@annotation: { @id ex:foo }` into explicit `f:reifies*` retract templates before the standard lowering. Threads named-graph context (`f:reifiesGraph` + node-level `@graph` selector) so retracts cancel the correct asserted flake identity.
- **By-selector retract.** A WHERE-bound retract: the pre-pass mints a fresh internal variable, synthesizes a `f:reifies*` WHERE pattern that constrains the variable to live annotations matching the selector body, and emits a by-variable delete template. The mint counter seeds past any user-visible `?_fluree_del_ann_N` occurrence to avoid collision.

## GC reachability

Annotation forward and reverse branch CIDs are returned by `IndexRoot::all_cas_ids`, so they participate in the same root-chain reachability model as fact indexes and dictionary trees. When a new index root supersedes an old one:

- New annotation CIDs are reachable from the new root.
- Old annotation CIDs become garbage only when no retained root references them.
- Leaf-level CIDs behind a branch are walked during the GC-diff pass; `drop.rs` and `gc/collector.rs` both call into the expanded-CAS expansion helpers so a strict GC pass never deletes a still-reachable leaf.

## Query IR expansion

`Pattern::EdgeAnnotation` and `Pattern::AnnotationTarget` are not executed as dedicated operators. `expand_edge_annotation_patterns` (`fluree-db-query/src/execute/where_plan.rs`) flattens them into a base-edge `Pattern::Triple` plus three required `f:reifies*` triples (`f:reifiesSubject` / `f:reifiesPredicate` / `f:reifiesObject`) plus body patterns before the join planner runs. The standard scan/join/dedup machinery handles the rest, and the planner's `reorder_patterns` picks the cheaper direction (edge-first vs annotation-first) based on the merged `AnnotationStats` selectivity.

The expansion does **not** emit an `f:reifiesGraph` constraint triple. Graph correlation is structural instead: each chain is scoped to one source at a time (the `Pattern::DefaultGraphSource` wrapper below, or an enclosing `Pattern::Graph`), so every `f:reifies*` lookup resolves against the same flake-level `g` per iteration — matching how `EdgeKey` derives graph identity. **Consequence:** the strict bundle-wellformedness rules under [`f:reifies*` durable encoding](#freifies-durable-encoding) above (required-slot, duplicate-slot, and `f:reifiesGraph`-vs-`g` `GraphMismatch` rejection) are guaranteed only on the **decode paths** — warmup / arena-build / hydration / cascade. Query-side annotation matching keys off the subject/predicate/object slots within the scoped graph; a malformed bundle introduced by bulk import (e.g. a named-graph bundle missing `f:reifiesGraph`) that decode would skip can still satisfy an `@annotation` / `rdf:reifies` query. Well-formed Fluree writes never produce such bundles — the JSON-LD writer emits `f:reifiesGraph` for named-graph edges and the SPARQL UPDATE surface is default-graph only — so this boundary matters only for hand-authored or externally-imported `f:reifies*` data.

Multi-source default-graph datasets (`from: [g1, g2]`) wrap each expanded chain in `Pattern::DefaultGraphSource` so the base-edge match correlates per source — otherwise a base-edge from `g1` would cross-join with annotations from `g2`. `collect_var_stats` walks into `DefaultGraphSource` so bridge variables (e.g. `?ann` shared between the wrapper's inner chain and a sibling external triple) are correctly counted as join vars.

### Read lanes for the expanded chain

`DefaultGraphSourceOperator`'s single-graph delegate (`build_single_graph_delegate`, `fluree-db-query/src/default_graph_source.rs`) recognizes the canonical `[base edge + 3 f:reifies*]` shape (`recognize_annotation_edge`) and picks one of four physical lanes:

1. **Forward-arena probe** (`AnnotationEdgeProbeOperator`) — when a sealed annotation arena exists, the overlay is empty, the query is current-state, and policy is root. One sorted merge-scan of the forward arena per query. Handles untyped (`-[p]->`) relationships (a late-materialized `Binding::EncodedPid` predicate decodes through the store's `p_sid_table`) and literal reified objects (the probe keys them by value + effective datatype + language tag, exactly as `EdgeKey::from_flake` stored them).
2. **Hash sidecar probe** (`HashAnnotationEdgeProbeOperator`, `fluree-db-query/src/annotation_edge_probe.rs`) — no arena needed. Drains the three `f:reifies*` predicates ONCE through ordinary planned scans (overlay-merged, policy-filtered) into one map keyed by the full reified edge `s → p → o → reifiers` (`AnnotationSidecarMaps`), then answers every base-edge row by `(s, p, o)` lookup. The map must be keyed by the whole edge: keyed by subject alone, every lookup walked all reifiers sharing that subject, a sum of squared reified out-degrees on hub-heavy data (StarBench BKR carries one subject with 3,345 `TREATS` edges). Object keys are representation-normalized `GroupKeyOwned`s, so ref and literal reified objects both match. Admitted at plan time when the driving estimate is ≥ 256 rows or unknown and the child does not already bind the reifier, then **re-decided at runtime**. With a bound reifier the chain is one point probe per row, while the lane walks every swept edge per row once the edge subject is unbound: a constant annotation value driving `<< ?s :p ?o >> :q "v"` over 20k edges took 55 s on the lane and 0.33 s on the chain (debug build, novelty only). At runtime: the lane buffers its whole driving stream before it can sweep anyway, so fewer than 256 buffered rows, or fewer than 256 distinct bound driving subjects, run the generic chain over the buffered rows (`probe_generic`, seeded through `BatchReplayOperator`) instead of draining the sidecar and sweeping the base predicate — StarBench P1 swept the whole graph for 9 s to enrich 3 rows. The value-only OPTIONAL lane (`AnnotationValueOptionalBuilder`, `fluree-db-query/src/optional.rs`) shares the same map-building machinery.
3. **Annotation-first enumeration** (`AnnotationEnumerateOperator`) — chosen when the whole arena is the cheapest entry point (a wildcard `<< ?s ?p ?o >>`, or a predicate wider than the arena), both endpoints are variables the child leaves unbound, and the child binds none of the chain's variables. Streams the **reverse** arena leaf by leaf (`AnnotationArenaReader::reverse_leaf_entries` + `live_pairs_in_reverse_leaf`, uncached so the walk retains nothing) and emits `(s, p, o, ann)` per live pair, filtered by the predicate when it is constant. Reifier order is dictionary order, and the lane encodes each ref through the subject dictionary (`find_subject_id_by_parts`, then dictionary novelty) so it emits `EncodedSid` / `EncodedPid` like scan output: the materialized form cost 40% of P2's time in `Arc<str>` clone/drop churn downstream, and random-order encodes of 21M blank-node reifiers would thrash the dictionary. Edge subjects and objects recur across the walk far more often than they are distinct, so the lane memoizes their ids per operator (P4 resolves 33.6M positions over far fewer nodes); reifiers are unique per edge and take the direct lookup. No base-edge scan and no per-reifier probe: on full-scale StarBench P2 the generic chain re-probed the base edge 33.6M times (757 s, single-threaded, no IO). No base-edge visibility check is needed under this lane's gates (root policy, current state, empty overlay) because every write surface asserts the base triple it annotates, `@reifies`-rooted inserts are rejected, and a base-edge retract cascades to its attachments — a live arena row implies a live edge.
4. **Generic join chain** — everything else. `reorder_patterns` orders the chain; the broad-sidecar demotion (`is_broad_annotation_sidecar`) applies only while a connected non-chain alternative can seed **and the sidecar's reifier subject is still unbound** (`reifier_subject_bound`). Once `?ann` is bound the sidecar is a one-row subject probe; demoting it handed the seed to a fan-out step — the base edge with only `(s, p)` bound, or a body triple such as `?ann :derives_from ?x` at ~20 rows per reifier — so every later per-row probe ran once per fanned row (StarBench P2 / P11). An untyped relationship's chain (where `f:reifiesPredicate` joins on the base predicate VAR) still drives from a bound-object `f:reifiesSubject`/`f:reifiesObject` probe (per-row fan ≈ node degree) and never from `f:reifiesPredicate` (fan ≈ sidecar / #relationship-types).

#### Choosing between the lanes

`choose_chain_lane` (`default_graph_source.rs`) decides by what the child binds, then by cost in rows walked:

- **Reifier bound by the child** → the chain: one point probe per driving row, which no sweep beats (P22 / C7 / C10 regressed 200× when the arena probe was taken there).
- **Base-edge subject bound** (a constant, or a variable the child binds) → the arena probe (below the 20M-row buffer ceiling). The probe keys into one forward leaf per driving row, while the chain's `f:reifiesSubject <s>` lookup walks every reifier of that subject — a hub tail the mean-based estimates cannot see. On the full StarBench ledger the fully-bound P23 went from 46 ms to 192 s on the chain and the child-driven P19 ran out of memory.
- **Subject unbound** → the cheapest of three costs. The arena probe pays the base-edge rows plus `LEAF_ROW_EQUIV` (≈400 rows) per leaf it touches; forward leaves are 4,096-row CBOR blobs ordered by `(s, p, o)`, so edges with an unbound subject land in about one leaf each, and a predicate-wide probe decodes the whole arena whatever the predicate's size (P11 / S5 / C8 / C10 all cost the same 13–15 s, 82% of it in leaf decode). The enumeration costs the arena's live pairs times ~12: each pair is a CBOR row decode plus two dictionary re-encodes of the endpoints. The chain costs the reifier candidates its cheapest `f:reifies*` lookup yields — `f:reifiesSubject`, `f:reifiesPredicate` or `f:reifiesObject` with the child's bindings — times ~16 for the generic chain (one scattered point probe per sequential row, the crossover measured on the slice) or ~2 when the chain is elided (below). `f:reifiesPredicate` counts here even though its uniform per-predicate estimate is too coarse to size the wrapper's output by: a predicate-only chain really does drive from that POST range.

#### Eliding the redundant chain checks

`elide_redundant_chain` rewrites the generic chain before it is planned. The write invariants — every reifier carries exactly one `f:reifiesSubject` / `f:reifiesPredicate` / `f:reifiesObject`, written into the edge's own graph, with the base triple asserted (`@reifies` without it is rejected; retracting the base cascades) — mean the base edge never removes a row once a reifies lookup has bound the reifier, and a lookup whose position is a variable nothing reads (neither a later sibling, nor the post-WHERE pipeline, nor the wrapper's own body) is a cardinality-one no-op. So the base edge is dropped, each unread variable position drops its lookup, constant positions keep theirs, and at least one lookup always remains (a reifier bound by the body still has to be a reifier: P3 has three `:derives_from` rows on plain subjects that P2 must not count). A read *variable predicate* blocks the whole rewrite — the base scan binds it as a predicate id, the reifies lookup as a plain ref, and those are not interchangeable downstream. The rewrite is gated on root policy, a single ledger and a non-history query; a past-`t` snapshot qualifies, because a reifier's bundle and its base edge are written and cascaded in the same commit.

Two things count as a read that are easy to miss. A variable in two positions, or naming the reifier, keeps its lookups: once the base edge is gone they are the only thing equating those positions (`<< ?s :p ?s >>` must not match `:a :p :b`). And under `SELECT *` there is no projection-pushdown set, so the post-WHERE read set is every variable `collect_var_stats` finds in the un-expanded WHERE clause. That walk has to look inside `EdgeAnnotation` / `AnnotationTarget`, or the quoted triple's own variables are dropped from every row (W3C `sparql12/eval-triple-terms` basic-4, basic-5, pattern-7, pattern-8).

Measured on the full StarBench ledger the predicate-only chain went from one SPOT point scan per reifier to one POST range plus the body's batched probe: P11 4.9 s → 0.42 s, C11 8.4 s → 0.61 s, S5 6.9 s → 1.8 s, and P2 on the elided chain (a single `f:reifiesSubject` range over all 21.4M reifiers) 10.6 s against 65 s on the enumeration — which is why the elided chain is costed at ~2 rows per reifier and the enumeration at ~12 per pair.

`FLUREE_ANNOTATION_LANE=arena|enumerate|chain` pins the lane for A/B timing; the runtime gates still apply.

#### Placing the wrapper among its siblings

`estimate_pattern` costs `Pattern::DefaultGraphSource` with `estimate_annotation_chain_cardinality`, not the generic branch model. The branch model multiplies the chain's triples in standalone-selectivity order with no regard for connectivity, so `<< ?s :P ?o >>` came out as `f:reifiesPredicate × base edge` (3,846 × 102,555 ≈ 4e8 on StarBench P11) and the wrapper sorted *behind* its own 6.5M-row `:derives_from` body triple, which then drove the chain once per row — the 60-second timeouts on every `<< ?s :P ?o >> :q ?x` shape. The chain binds one reifier per matching base edge, so its cardinality is the cheaper of its two entry points (the base edge, or the most selective `f:reifies*` lookup) times the body's expansion with the edge and reifier bound. The `/explain` output still shows the un-expanded `annotation-target` estimate in its logical plan; the executor plans the expanded wrapper.

`f:reifiesSubject` / `f:reifiesPredicate` carry the `@id` datatype constraint only when their object is a constant. On a variable object it is a no-op filter (those slots are refs by construction) and it disqualified the batched subject-join lane (`is_batched_eligible` requires no dtc), turning the per-reifier probes into scattered scan opens. On a constant ref object the batched existence lane admits it (`is_batched_subject_exists_eligible`): the probe compares a ref constant against ref values only, so the constraint is already enforced, and without the admission every reifier candidate of a predicate-only or object-bound quoted triple opened its own point scan (~150 µs each).

A rel var whose properties are read anywhere (`r.prop`, `properties(r)`, …) lowers to this REQUIRED chain — unreified edges do not match, per Cypher's relationship semantics. Value-only rel vars keep the plain base triple + OPTIONAL probe so unreified edges match with a synthesized relationship value.

#### Buffering and the sweep ceiling

**Both probe lanes are pipeline breakers.** `next_batch` runs the probe to completion before emitting the first row, so a query with a small `LIMIT` over a large driving side pays the full probe rather than stopping early. This is inherited behavior — the arena lane has always buffered — but the hash lane adds a sweep of the *base* edges on top of it, so the over-read is larger there. `PlanningContext` carries no limit, so a LIMIT-aware gate would need real plumbing; until then, treat the buffering as the lane's cost model rather than an oversight.

`base_sweep_bounded` caps that sweep at `BASE_SWEEP_MAX_ROWS = 20_000_000` rows, measured as the predicate's flake count for a typed relationship and `stats.total_property_flakes()` (the whole default graph) for an untyped one. The number is a **backstop against pathological graphs, not a tuned optimum**: the lane was validated at ~190k reified edges over 72k nodes, so 20M is roughly 100× the measured scale, chosen as the point where one sequential sweep (tens of millions of rows through a planned scan) stops being obviously cheaper than the per-row probes it replaces. Nothing between the validated scale and the ceiling has been measured; a future tuner wanting a defensible value should benchmark the sweep against the nested-loop alternative at 1M/5M/20M and set the crossover from data.

One sharp edge inside the sweep: `keep_all` disables the driving-subject filter entirely if **any single** driving row leaves the subject unbound, flipping the sweep from "filtered to the driving subjects" to "every base edge in the graph." That is correct — an unbound subject can match any edge — but it is a cliff, not a gradient, and it is not visible from the gate names.

## See also

- [Edge annotations (concept doc)](../concepts/edge-annotations.md) — the user-facing surface.
- [Index format](index-format.md) — fact indexes, dictionary trees, FIR6 root layout.
- Rustdoc on `fluree_db_core::edge::EdgeKey`, `fluree_db_core::annotation_index::AnnotationIndexRoot`, `fluree_db_novelty::attachments::AttachmentNovelty`, and `fluree_db_binary_index::annotation_arena::AnnotationArenaReader` for type-level contracts.
