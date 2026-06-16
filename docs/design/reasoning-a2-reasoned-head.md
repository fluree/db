# A2 Design: Reasoned Index Head + FBF Incremental Maintenance

Status: **draft** (June 2026). Companion to
[`reasoning-roadmap.md`](reasoning-roadmap.md) (the plan) and
`REASONING_STATUS.md` (where we are). This document specifies roadmap phase
**A2** — persisting OWL2-RL derived facts as index artifacts maintained
incrementally per commit — and the **B1 provenance sidecar** format, which
must be designed with the storage so derivations carry `(rule, premises)`
tags from day one. Every structural claim below is grounded in the current
code; file references are to `refactor/resoning-2`.

## 1. Problem

Derived facts today live only in a transient `DerivedFactsOverlay`
(`fluree-db-reasoner/src/overlay.rs`) — four sorted in-memory `Vec<Flake>`
copies, rebuilt by full re-materialization whenever the cache key
`(ledger_id, db_epoch, to_t, overlay_epoch, ontology_epoch, config_hash)`
changes, i.e. **on every commit**. Measured consequences at LUBM-25
(1.58M derived facts):

- **Per-query overlay tax.** Overlay flakes must be translated to V3 ids and
  merged in every binary cursor walk. Caches (per-execution +
  4-entry global LRU at `fluree-db-query/src/binary_scan.rs:2292`) mitigate
  but don't eliminate it. Relatedly, the batched-probe join paths and the
  cyclic-BGP fast path bail on *any* overlay (novelty included) — which is
  why q02 times out (>600 s) and q09 takes 69 s. That bail is a general
  engine gap, not a reasoning-specific one, and is addressed by the **P0
  precursor workstream** (§7.2, §10) rather than by persistence itself.
- **Resident memory.** ~2.8 GB RSS per materialization; the 16-entry LRU can
  hold several. LUBM-100 projects to ~8–10 GB per entry.
- **Recompute churn.** Every commit and every server restart re-derives the
  full closure (10.6 s at LUBM-25 after A1.2; minutes at LUBM-100).
- **No explanations.** Nothing records *why* a fact was derived (B1).

A2 addresses these problems unevenly, and the distinction is load-bearing:
it removes full re-materialization churn, lowers resident-memory pressure,
creates a place for provenance, and can eliminate the overlay→V3
translation floor. It does **not** make the merge disappear, and it does
**not** by itself fix the q02/q09-class fast-path bails. Those bails are a
general novelty-awareness gap in the query engine and must be solved by P0;
if P0 makes today's in-memory reasoning overlay fast enough, A2's remaining
performance case is mostly operational scale rather than raw hot-query
speed. The A2 mechanism is persisted, derived-only index artifacts with
their own head record, maintained per commit by a background worker running
**FBF** (Backward/Forward) incremental maintenance over the A1.2 compiled
rule set.

## 2. Goals and non-goals

### 2.0 The three delivery modes — one engine, composable layers

All reasoning delivery reduces to the same engine (compiled rules +
semi-naive/FBF) parameterized by *which rule profile* runs and *who waits*
for it. The modes are **not alternatives — they compose**, and a single
ledger plausibly runs all three at once with different profiles:

| | Mode 1 — query-time (today) | Mode 2 — background (this doc) | Mode 3 — transaction-time (future feature) |
|---|---|---|---|
| Rule profile | anything, ad-hoc | heavy profiles (full `owl2rl`, …) | small **transactional entailment profile** (hierarchy/domain-range typing, light datalog x-forms — bounded-cost shapes only) |
| Who waits | the reader (materialization on cache miss) | nobody — staleness disclosed; readers may opt to wait (`strict`) | the writer (small, budgeted, predictable) |
| Consistency | always exact at requested `t` | eventual (`exact`/`strict`/`asOf` per query) | transactionally consistent |
| Why you'd use it | what-ifs, history, ad-hoc rules | analytical/steady-state reasoning at scale | entailments that **gate writes** (commit-time SHACL over inferred state, B4) and must be immediately visible |

**Layering rule: each layer treats the layers below as ground truth.**
Mode 3's derived facts land in the *index* at the commit's `t` (never the
commit log — "commit log is sole source of truth; reindex re-derives"
holds, with reindex re-running the transactional profile), and their
lifecycle is owned by the transaction engine: premises retract ⇒ its
conclusions retract, transactionally. Mode 2's worker therefore treats
txn-derived facts as explicit — it reasons over (asserted ∪ txn-derived)
and never FBF-maintains Mode 3's outputs; when the head's profile ⊇ the
transactional profile, re-derivation is an idempotent no-op. Mode 1 is
the ad-hoc lens over whatever the lower layers produced, and the
universal fallback (historical `t`, no head, lag).

Scoping Mode 3 to a restricted profile is what makes it viable at all:
the per-transaction budget question changes from "cap arbitrary OWL
reasoning" (hopeless in a write path) to "admit only rule shapes with
predictable cost." Recursive/expansive constructs (transitive closure,
`sameAs`, restriction classes) are excluded by construction and belong to
Mode 2. TBox changes never recompute in the write path (degrade to
async). A side payoff: txn-time entailments are stamped at their commit
`t`, so for the transactional profile, time-travel-with-reasoning is
exact at every `t` automatically.

Mode 3 is out of scope for v1; it is Mode 2's maintenance step invoked
synchronously with a restricted profile, so nothing in this document
forecloses it.

**Goals**

1. A queryable, persisted derived-facts index per `(ledger:branch,
   rule-config)` pair, pinned to a base commit `t` — the **reasoned head**.
2. Insert-only commits (the dominant case) maintain it with semi-naive
   evaluation over the commit delta; retract-bearing commits run FBF. No
   full re-materialization for data commits.
3. Zero persistent auxiliary state beyond the derived index + provenance
   sidecar: the head remains a pure function of (rules, data-at-t),
   rebuildable at any time. Reindex re-derives; the commit log stays the
   sole source of truth.
4. Staleness is first-class: a `reasonedThroughT` watermark, surfaced in the
   query response `reasoning` block (shipped on `refactor/resoning-2`) and
   ledger status, with a per-query escape hatch to the existing overlay path
   for read-your-writes freshness.
5. Provenance recorded per derived fact (`rule_id`, premise keys) in a
   sidecar, enabling B1 proof-tree reconstruction and accelerating FBF's
   overdeletion step.
6. Non-reasoning queries never touch any of it — zero cost by artifact
   separation, no inferred-marker bit in the main indexes.

**Performance claim boundary.** A2 must not be sold as "persistence solves
reasoning query performance." The hot-query hypothesis is narrower: a head
can avoid (a) per-query materialization and (b) Flake→V3 translation, then
pay the same kind of sorted side-channel merge that novelty already pays.
The q02/q09 timeout shape is not evidence for persistence; it is evidence
that the optimized probe paths need to become novelty-aware. P0 should be
benchmarked first against today's in-memory overlay and against a
novelty-heavy non-reasoning ledger. If that solves the slow queries, A2 is
still valuable for restart behavior, memory footprint, incremental
maintenance, provenance, and LUBM-100-scale operability — but not as the
primary fix for those queries.

**Non-goals (this phase)**

- Stratum-scoped recompute for ontology changes (ZodiacEdge-style) — TBox
  commits trigger full re-derivation in v1.
- The IJCAI-2015 incremental `owl:sameAs` deletion algorithm — v1 recomputes
  the affected equivalence component (or falls back to full re-derive).
- Parallel fixpoint (A1.3) — orthogonal; FBF consumes the same compiled
  dispatch and benefits automatically when A1.3 lands.
- Datalog (user-rule) persistence — the uncached datalog lane is unchanged;
  only `owl2rl` materializations get heads in v1.
- **Mode 3 (transaction-time entailment) — see §2.0.** In v1, transactions
  are never gated on reasoning of any kind. Mode 3 arrives later as a
  separate, composable per-ledger feature: the same engine invoked
  synchronously with a *restricted, bounded-cost* transactional profile
  (its scoping is what makes a write-path budget tractable). Heavy
  profiles stay in Mode 2 regardless; consistency-needing readers have
  `freshness: strict` meanwhile.

## 3. Architecture overview

```
                       ┌─────────────────────────────┐
 commits ───────────▶  │  ledger  mydb:main          │
                       │  NsRecord: commit_t, index_t│
                       └──────────────┬──────────────┘
                                      │ LedgerCommitPublished events
                                      ▼
                       ┌─────────────────────────────┐
                       │  reasoning worker (new)     │
                       │  per-head state machine     │
                       │  insert-only: semi-naive E+ │
                       │  retracts:    FBF           │
                       │  TBox commit: re-derive     │
                       └──────────────┬──────────────┘
                                      │ publishes
                                      ▼
   ReasonedHeadRecord  ──────────  derived-only index artifacts
   (GraphSourceRecord-shaped)      reasoning/{name}/{branch}/...
   reasoned_t, base_root,          FDR1 manifest + leaflets (V3 rows,
   ontology_epoch, config_hash,    base-id-aligned) + FPS1 provenance
   capped, index_id                sidecar
                                      ▲
                                      │ three-way compose at query time
   query («reasoning: owl2rl») ───────┘
   base index rows ⋈ derived rows ⋈ novelty ops
```

A **literal reasoning branch was already rejected** in the roadmap
(rebase-replay is wrong for derived facts; branch novelty is
commit-reconstructed; branch `t` diverges). Research for this document adds
a fourth, decisive reason: **a derived-only artifact cannot be a FIR6 index
root at all.** `LedgerSnapshot::from_root_bytes`
(`fluree-db-ledger/src/lib.rs:178`) treats the root as the complete ground
truth — full namespace registrations, graph IRIs, and dictionary watermarks
are required to decode and route post-index commits. A root containing only
derived facts is not a loadable snapshot. This forks the storage design
(§5.0): either the head is a **new derived-only artifact kind** loaded
only by the reasoning-aware query path (Strategy A), or it is a
**complete merged FIR6 index family** — a fully loadable snapshot that
sidesteps this problem entirely (Strategy B, recommended). Goal 6
(non-reasoning queries untouched) holds under both: separation is by
artifact/record either way, never by an inferred-marker bit in the base
index.

## 4. The reasoned head record

The roadmap sketched an `NsRecord`-shaped record. The closer existing fit is
**`GraphSourceRecord`** (`fluree-db-nameservice/src/lib.rs`) — the record
the BM25/vector sidecars already use — because a reasoned head is exactly
what that record models: a derived artifact with declared dependencies on a
source ledger, its own index CID + watermark, and a retraction lifecycle:

```rust
GraphSourceRecord {
    graph_source_id: "mydb~reasoning-default:main", // head id (see naming below)
    name:            "mydb~reasoning-default",
    branch:          "main",                        // tracks the base branch
    source_type:     GraphSourceType::Reasoning,    // NEW variant
    config:          { ... JSON, see below ... },
    dependencies:    ["mydb:main"],
    index_id:        Some(<FDR1 manifest CID>),
    index_t:         <reasoned_t>,                  // == reasonedThroughT
    retracted:       false,
}
```

`config` (JSON, hashed into the head identity):

```json
{
  "modes": ["owl2rl"],
  "enabledRules": [],
  "budget": {"maxFacts": 20000000, "maxSeconds": 300},
  "schemaSource": "<resolved schema-source descriptor or null>",
  "profile": "default"
}
```

Head identity = `(base ledger:branch, rule_config_hash)`. N rule configs =
N heads = N small derived indexes (read-through sharing means no base
copies). `rule_config_hash` reuses `ReasoningOptions::config_hash()`
(`fluree-db-reasoner/src/lib.rs`) extended with the schema-source
descriptor. The default `owl2rl` config gets the well-known profile name
`default`; named profiles (B3) map onto additional heads.

Additional head state (in `config` or alongside `index_t` — decide at
implementation time whether `GraphSourceRecord` grows fields or the FDR1
manifest carries them; manifest preferred to keep the record schema stable):

| Field | Meaning |
|---|---|
| `reasoned_t` | base commit `t` the closure is exact for (`= index_t`) |
| `base_root_id` | CID of the base index root the derived rows are id-aligned to |
| `ontology_epoch` | monotone counter bumped on every TBox-touching commit the worker observed |
| `capped` / `capped_reason` | the head's closure hit its budget — propagated into every query's `reasoning` block |
| `derived_count`, `iterations`, `last_maintenance_ms` | diagnostics |

**Lifecycle.** Heads are created on demand: the first `reasoning: owl2rl`
query against a ledger (or an explicit admin/config opt-in,
`f:reasoningDefaults` gaining `f:materialize true`) registers the record;
the worker bootstraps it with a full materialization (the A1.4 post-import
pass is exactly this bootstrap with `E− = ∅`). Drop = `retracted: true` +
artifact GC, mirroring graph-source drop. Branch drop cascades to its heads.

**What heads never do:** participate in merge/rebase (`fluree-db-api/src/
{merge,rebase}.rs` replay *commits* and update `commit_head_id` — a head has
neither), accept transactions, or serve as a base for branching. On a merge
into the tracked branch, the head's `reasoned_t` simply falls behind the new
`commit_t` and the worker catches up through the merged commits like any
others (the merge produces ordinary replayed commits with op-flagged
flakes). A reset/rollback below `reasoned_t` invalidates the head →
re-derive (v1; FBF-rewind is possible later since the closure is a pure
function of state).

## 5. Storage

### 5.0 Two strategies — merged full index (recommended) vs derived-only

Once P0 owns probe performance, the head's honest job description is a
**persistent materialization cache**: survive restarts, maintain
incrementally, carry provenance. That framing (June 11 discussion) exposes
a choice the first draft under-weighed:

**Strategy B — merged full index (recommended, pending the M0
measurement).** The reasoned head is a *complete* FIR6 index family
(base ∪ derived) per rule config, bootstrapped the way `create_branch`
bootstraps a branch at HEAD — copy the base index *references*, fold the
derived facts in as one incremental update — and maintained by the
existing incremental indexer, whose input novelty is simply
base novelty ∪ FBF derived deltas (the existing `Novelty` shape).

- *Query side*: a reasoning query becomes exactly a normal query —
  reasoned snapshot + novelty, two lanes, the standard engine path.
  **Zero cursor changes, no third lane, no id-alignment machinery, no
  `eager_materialization` gate** (no Sid-space overlay exists), and the
  "derived-only roots are not loadable snapshots" problem (§3) vanishes:
  the merged root is a complete snapshot by construction. Steady-state
  reasoning queries don't even depend on P0 (P0 still serves
  novelty-heavy ledgers and the fallback lane).
- *Storage*: the earlier "duplicates the base per config" objection is
  largely defeated by **content addressing** — leaves untouched by
  derived facts keep their CIDs and are stored once, shared with the
  base index. Real amplification = leaves derived facts intrude on
  (heavy in `rdf:type` POST/PSOT extents, light elsewhere) plus branch
  structure. Measurable, not assumed.
- *Maintenance*: ≈ a second background index family per config (~2×
  background indexing per cycle), not per-commit write amplification.
- *Unchanged from this doc*: the head record (§4), the worker and FBF
  (§6 — the derived deltas just land in the reasoned index's novelty
  instead of a separate artifact), freshness/resolution (§7.1),
  provenance sidecar (§5.3 — keyed by fact identity, position-
  independent), head-invisibility, reindex/drop lifecycle (§8). B3
  asserted-vs-inferred = base index vs reasoned index; "inferred-only"
  = FPS1 membership.

**Strategy A — derived-only artifacts + third cursor lane** (the rest of
this §5 and §7.2): smaller artifacts and no second index family, at the
price of new formats (FDR1), base-id alignment, the three-pointer merge,
and derived-lane probe work — substantial new query-engine surface whose
performance then needs to be proven against the same merge costs novelty
pays.

**Two-tier shape (June 11 discussion): the delta log is the head's
novelty; the full index is its compaction target.** The persistence
options — an LSM-like derived-delta log vs. a full index — are not
competitors but the two tiers of the architecture base reads already use:

- *Hot tier — derived delta log.* Each worker run appends one sorted
  segment (the FBF output: derived asserts + retract tombstones, stamped
  with its t-range). Maintenance writes are delta-sized — no index
  rewrite per run — reusing the novelty LSM machinery (flat write slope,
  tiered compaction bounding segment count). Cleanest form: the head owns
  **its own `Novelty` instance**, fed by base commits ∪ derived deltas,
  so a head query is *structurally identical* to a base query (snapshot +
  novelty, two lanes, same engine, same P0 treatment, same compaction
  knobs).
- *Cold tier — fold.* On the same threshold logic that folds base novelty
  into the base index, accumulated segments fold into the reasoned index
  family and the log resets — bounding query-side merge cost exactly the
  way base reads bound it.
- *Bonuses:* segments carry closure transitions with t-ranges, so
  head-window time travel (§7.1's v2 option) becomes "merge segments up
  to t". One real difference from base novelty: derived deltas are NOT in
  the commit log, so the head's log is persisted as append-only blobs
  (zero restart cost; preferred) or re-derived over the gap on restart
  (bounded by fold cadence; fallback).

Strategy A vs. B is therefore a **cold-tier-only** decision.

**Decision gate (M0):** bootstrap a Strategy-B reasoned index from the
LUBM-25 base refs and measure (a) the leaf-sharing ratio (shared vs
diverged CIDs across all four orders), (b) the incremental fold cost per
index cycle, (c) reasoning-query latency vs the base index (expect
parity). Adopt B unless storage divergence or fold cost is pathological;
A remains the fallback and its sections are retained below for that case.

### 5.1 Layout and manifest (Strategy A)

Artifacts live under the graph-sources storage segment, reasoning-scoped:

```
reasoning/{name}/{branch}/{config-hash}/
  manifest.fdr1          # derived-index manifest (new kind, magic "FDR1")
  leaves/{cid}.fdl1      # derived leaflets per sort order (columnar V3 rows)
  prov/{cid}.fps1        # provenance sidecar segments (B1)
```

The **FDR1 manifest** is deliberately *not* a FIR6 root. It contains:

- `reasoned_t`, `base_root_id`, `ontology_epoch`, `rule_config_hash`,
  `capped`/`capped_reason`, counts;
- per-sort-order (SPOT, PSOT, POST, OPST) branch tables → derived leaflet
  CIDs with min/max keys — the same columnar leaf encoding the binary index
  already uses (`RowColumnSlice` layout), reusing the existing leaf
  writer/reader;
- **dict-delta tables** (§5.2);
- per-leaflet provenance segment refs (offset/len), mirroring how
  `LeafEntry.sidecar_cid` attaches the FHS1 history sidecar today
  (`fluree-db-binary-index/src/format/history_sidecar.rs`).

Default graph only, matching `DerivedFactsOverlay` semantics (derived facts
are `g_id = 0`; `fluree-db-reasoner/src/overlay.rs:171`).

### 5.2 Id alignment — Strategy A's load-bearing decision

The translation-side query win depends on derived rows being encoded in the
**base store's V3 id space** (`s_id: u64`, `p_id: u32`, `o_type: u16`,
`o_key: u64`, `o_i: u32` — `FactKeyV3`,
`fluree-db-binary-index/src/read/types.rs:19`). Then the cursor can merge
them like already-translated overlay ops, with **zero per-query
translation** — deleting the overlay→V3 translation tax that A1.1 could only
memoize. This is not the same as eliminating query-time reconciliation:
derived rows are still a side channel, so fast paths must explicitly merge
them just as P0 makes them merge novelty.

Alignment facts established from the dictionary code
(`binary_index_store.rs:2251-2393`):

- **Subjects/strings** are append-only per namespace with watermarks
  persisted in the root — ids are stable across index builds. Derived facts
  reference only terms that already exist in the base data (OWL2-RL derives
  over existing constants; it mints no IRIs), so subject/object ids are
  always resolvable against the base dicts + dict-novelty at `reasoned_t`.
- **Predicates** are the wrinkle: `p_id` is an index into the root's
  `predicate_sids` table, and a derived fact's predicate may never occur in
  a *predicate position* in base data (e.g. a chain-axiom's derived property
  or `owl:sameAs` itself). The FDR1 manifest therefore carries an
  **appended-predicate table**: `p_id`s above the base root's watermark,
  mapping to SIDs — the same mechanism `DictNovelty` uses for unindexed
  commits. Same approach for the rare appended datatype/lang ids.
- The manifest records `base_root_id`. When the base indexer publishes a new
  root, ids remain valid (append-only growth); the worker refreshes
  `base_root_id` opportunistically at its next maintenance step. If a future
  base format change ever renumbers ids, alignment breaks detectably
  (root id mismatch) and the head re-derives — correctness never depends on
  silent compatibility.

### 5.3 Provenance sidecar (B1) — FPS1 (both strategies)

Designed now, written from the first persisted head. Per derived fact, one
or more derivation records, segmented per derived leaflet (FHS1 precedent —
builder accumulates per-leaflet segments; refs stored in the manifest):

```
FPS1 segment:
  entry_count: u32
  entries: [ProvEntry; entry_count]    # sorted by derived FactKeyV3

ProvEntry (variable length):
  fact:        FactKeyV3       (26 B)  # the derived fact
  rule_id:     u16                     # compiled-rule identity (see below)
  n_premises:  u8                      # 0..=4 in OWL2-RL rule bodies
  premises:    [FactKeyV3; n_premises] # 26 B each
  flags:       u8                      # bit0: also-base-asserted, bit1: capped-context
```

Two-premise derivation ≈ 81 B — in the roadmap's 85–90 B envelope, and
*only* fetched for explanation queries or FBF retract handling; the hot
query path never reads it.

`rule_id` is a stable enumeration of the **compiled** rule bindings:
`(RuleKind, axiom discriminator)` from A1.2's `CompiledRules`
(`fluree-db-reasoner/src/compile.rs`) — e.g. *cax-sco via
ex:Professor ⊑ ex:Faculty*. The compile step assigns dense u16 ids and the
manifest stores the id→(rule name, axiom SIDs) table, so proof trees render
without recompilation and survive ontology evolution (ids are per-head,
re-assigned on TBox recompile, never reused within a head generation).

Multi-derivation: multiple `ProvEntry`s per fact are allowed and expected
(a fact often has several proofs). The maintenance algorithm (§6) keeps *at
least one* valid entry per live derived fact; it does not guarantee
exhaustive proof storage (that's justification storage, rejected in the
roadmap for its blow-up). Exposure as virtual RDF-star
(`<< ?s ?p ?o >> f:derivedBy ?rule`) follows the `rdf_star.rs`
BIND-projection precedent and is B1 scope, not A2.

## 6. Maintenance: the reasoning worker

### 6.1 Worker shape

A new `reasoning_worker` in `fluree-db-api` (sibling of `bm25_worker.rs` /
`vector_worker.rs`), with the indexer orchestrator's hardening
(`fluree-db-indexer/src/orchestrator.rs`): subscribes to
`LedgerCommitPublished` nameservice events, maintains a reverse map
ledger → heads, debounces (~100 ms default), coalesces triggers per head
(maintain to current `commit_t`, resolve all waiters with `min_t ≤
reasoned_t`), panic-safe dispatch with exponential-backoff retry, and
clears retry deadlines on idle/cancel (the orchestrator's
retry-deadline invariant). One in-flight maintenance per head.

The worker holds per-head in-memory state between commits — this is what
makes incrementality cheap:

- the loaded derived store handle (mmap'd leaflets);
- the **`CompiledRules` artifact** and extracted `OntologyRL` /
  `RestrictionIndex`. This resolves A1.2's deferred per-epoch caching
  soundly: the worker inspects *every* commit delta, so it recompiles
  exactly when a TBox-touching commit arrives — no reliance on
  `schema_epoch` covering all OWL predicates;
- the `FrozenSameAs` union-find and the identity-rule grouping state
  (`IdentityRuleState`), both reconstructible from the derived store on
  restart (sameAs facts are part of the closure).

### 6.2 Commit classification

For each commit in `(reasoned_t, commit_t]`, taken from the commit's
op-flagged flake delta (the same `CollectedCommitData` shape merge/rebase
consume):

1. **TBox-touching?** Predicate-set match against the OWL/RDFS vocabulary
   (`subClassOf`, `subPropertyOf`, `domain`, `range`, `equivalentClass`,
   `inverseOf`, `propertyChainAxiom`, restriction predicates,
   `intersectionOf`/`unionOf`/`oneOf`, `hasKey`, the property-type classes
   in `rdf:type` objects, …) — the closed list lives next to the extractor
   so it can't drift from what `OntologyRL::from_db_with_overlay` reads.
   → bump `ontology_epoch`, **full re-derive** (v1; stratum-scoped later).
   Rule *addition* via config change → semi-naive with only the new rules;
   rule *removal* → treat that rule's consequences as retracts → FBF.
2. **Retract-free data commit** (`E− = ∅`, the dominant case) → §6.3.
3. **Retract-bearing data commit** → §6.4. If any retract touches
   `owl:sameAs` or a member of a non-singleton equivalence class → §6.5.

Multiple pending commits are folded into one net `(E+, E−)` via the
`fact_state` latest-op semantics before maintenance runs.

### 6.3 Insert-only fast path (semi-naive over E+)

Exactly the A1.2 fixpoint with a different seed: `delta := E+` (plus their
canonicalization through the live union-find) instead of the full base scan;
`derived` is backed by the persisted store (point lookups `contains`,
`get_by_ps/po` served by the derived leaflets + base index — the same
delta ⋈ (derived ∪ base) joins the rules already perform, reading the
persisted artifacts instead of RAM vectors). Each new fact appends a
provenance entry. Fixpoint output is sorted, merged into the four derived
orders (leaflet rewrite is localized — same incremental-leaf machinery the
main indexer uses), sidecar segments appended, manifest + record published
with the new `reasoned_t`.

Semi-naive over the delta is optimal here in every maintenance-algorithm
family — no special machinery, per the roadmap's design principle 2.

### 6.4 Retract path: FBF (Backward/Forward)

Per Motik et al. (AAAI 2015 / AIJ 2019), chosen because it needs **zero
persistent auxiliary state** — the provenance sidecar is an accelerator, not
a correctness dependency:

1. **Overdelete (forward).** Seed `D := E−`. Repeatedly apply the compiled
   rules *forward* over `D ⋈ (old materialization ∪ base-at-t−1)` to find
   derived facts whose recorded derivations could have consumed a deleted
   fact; the FPS1 reverse direction (premise → derived) makes this a sidecar
   range scan instead of rule re-application where entries exist. Everything
   reached goes into the **doubtful set**.
2. **Back-chain (backward).** For each doubtful fact, search for a surviving
   proof: backward-chain through the compiled rules against
   (base-at-t ∪ (materialization \ doubtful)), with memoized proved/disproved
   sets per run. Fluree's snapshot-at-any-t makes "explicit facts at t"
   a native lookup — no logging needed, the property that made FBF the
   roadmap's pick. Facts with a surviving proof are kept (and get a fresh
   provenance entry for the found proof); the rest are deleted.
3. **Re-derive (forward).** Semi-naive from the kept frontier ∪ E+ to
   restore anything the deletions transitively unsupported, as §6.3.

The AIJ 2019 empirical result — backward/forward dominates on small updates
— is exactly the per-commit case.

### 6.5 `owl:sameAs` retraction (v1)

Never through generic rules (quadratic closure). When a retract removes a
sameAs edge or a fact about a merged individual: recompute the affected
**equivalence component** — dissolve the component in the union-find,
re-run identity rules (prp-fp/ifp/key, cls-maxc/maxqc) and
recanonicalization restricted to facts touching the component's members,
then FBF the resulting derived-fact deltas. Equality deletion is
non-monotonic (it can *restore* previously-rewritten facts), which the
component recompute handles by construction. If the component exceeds a
size threshold, fall back to full re-derive — correct and bounded. The
IJCAI-2015 incremental algorithm is the v2 upgrade.

### 6.6 Budgets and capping

Maintenance runs under the same `ReasoningBudget` resolution shipped on
`refactor/resoning-2` (ledger config → env → default; per-query budgets
don't apply to the worker). A capped maintenance run marks the **head**
capped; every query served from it carries `capped: true` in its
`reasoning` response block until an uncapped run succeeds. A capped head is
still monotonically *under* the true closure, so serving it is no worse
than today's capped overlay — but it is now visible.

### 6.7 Overload: when write volume outpaces reasoning

Eventual consistency trades a transaction guarantee for a lag risk:
sustained transaction volume × inference cost can exceed the worker's
throughput, and **which side is the long pole varies per deployment**
(data shape, rule complexity, hardware) — it cannot be designed away, only
bounded, observed, and adapted to:

1. **Writes are never throttled by reasoning** (non-goal above). Under
   either storage strategy the worker is off the transaction path;
   Strategy B's second index family is resource contention, not pipeline
   coupling, and its cadence may lag the base indexer's arbitrarily.
2. **Coalescing is the built-in backpressure absorber.** Each worker run
   folds *all* pending commits into one net `(E+, E−)` — cost scales with
   the net delta and its inference consequences, not with commit count.
   Bursts of overlapping or small commits absorb nearly for free; lag
   grows without bound only when net inference work itself outpaces the
   worker.
3. **The worker is adaptive, not loyal to FBF.** Per run, if the
   accumulated delta (or the FBF doubt set) exceeds a threshold fraction
   of the closure, it switches to a full re-derive at latest — often
   cheaper than incremental catch-up over a huge gap, and bounding
   worst-case catch-up at one full materialization (the cost A1.3
   attacks). This is also the hedge for "you don't know which is the long
   pole": the worker measures and chooses per run rather than the design
   guessing.
4. **Degradation floor: never worse than pre-A2.** As lag grows, `exact`
   reads pay larger top-ups until resolution falls back to the overlay
   path — i.e. today's behavior, caches included. `asOf` reads stay cheap
   and merely age, staleness disclosed. `strict` reads wait (bounded by
   the query timeout, then fall back). No degradation mode touches write
   latency.
5. **Lag is observable and alarmable.** `commit_t − reasoned_t` (and its
   wall-clock age) in worker metrics, ledger status, and every reasoned
   response (`reasonedThroughT`); a configurable lag threshold raises an
   operator alarm, and worker cadence/priority are config knobs.

## 7. Query-time composition

### 7.1 Resolution

**Invariant: a head never changes query results, only how fast they
arrive.** Reasoning is a per-query lens — `reasoning: owl2rl` at `to_t`
means *the closure over data-as-of-`to_t`*, whether or not a head exists,
was bootstrapped later, or lags. If head existence affected answers, the
same query would return different results depending on when an operator
created the head, breaking the pure-function-of-(rules, data-at-t)
property. The fallback path guarantees the invariant; heads only
accelerate.

(`f:reasoningDefaults`-driven reasoning composes with this naturally and
gives "before reasoning was enabled" semantics for free: the config graph
is versioned ledger state, so a time-travel view at `t` resolves the
config *as of `t`* — a default that didn't exist yet simply doesn't apply.
Explicit per-query reasoning at historical `t` still gets the full
closure-at-`t` via fallback.)

`reasoning: owl2rl` (query, view, or ledger default — unchanged precedence)
resolves in order, governed by a per-query
`"reasoning": {"freshness": "exact" | "strict" | "asOf"}` option
(default `exact`):

1. **Head exact** (`to_t == reasoned_t`, common steady-state) → attach the
   derived store; no overlay materialization at all.
2. **Head behind** (`reasoned_t < to_t ≤ commit_t`), by freshness policy:
   - `exact` (default) → attach the derived store **plus a top-up
     overlay**: semi-naive over the gap commits' `E+` — small, in-memory,
     cached by `(head generation, gap epoch)`. Correct latest answers.
     Gap retracts force fallback (3) in v1 rather than in-memory FBF.
   - `strict` → block on the worker's waiter mechanism until
     `reasoned_t ≥ to_t`, then serve as (1). For pipelines that must not
     pay query-side reasoning at all.
   - `asOf` → lower the **whole query's** effective `to_t` to
     `reasoned_t` — base lanes included, not just the derived lane (base
     at `commit_t` joined with closure at `reasoned_t` would be exactly
     the mixed-state inconsistency the time-travel rule forbids) — and
     serve from the head with **no top-up**. The cheapest read,
     semantically an exact answer for a slightly older `t`. The effective
     t is disclosed (below); this is the eventually-consistent read mode
     and is only acceptable *because* it is disclosed.
3. **No head / time-travel below head coverage / gap too complex** → today's
   overlay materialization path, unchanged — including ad-hoc historical
   reasoning (`to_t` + modes + inline `ontology`/`rules`/budget), which
   already works today and stays the universal correctness backstop.
   **No `t` ever regresses.**

Time-travel rule (v1): a closure is only valid *at its pinned t* — for
`to_t < reasoned_t` the head may contain facts derived from data the query
must not see, so heads are never filtered downward by `to_t`; path (3)
serves historical reasoning. (Per-fact `t` filtering is insufficient
because a derivation's premises may individually predate `to_t` while the
closure also lost retracted facts — correctness requires the
pure-function-of-state property, not row filtering.)

*Future option — a replayable head (v2):* FBF maintenance computes, per
commit, exactly which derived facts gained or lost support. Persisting
those transitions as op-flagged derived rows (assert/retract with their
transition `t`s, FHS1-style) would make the head itself time-travelable
across its maintained window `[bootstrap_t, reasoned_t]` using the same
replay machinery base leaflets use — turning path (3)'s historical
fallback into a head read for any maintained `t`. Deliberately not v1
scope; noted because it falls out of the maintenance algorithm nearly for
free and constrains nothing if skipped.

The `reasoning` response block gains `reasonedThroughT`, `effectiveT`
(differs from the requested `to_t` only under `asOf`), and
`source: "head" | "head+gap" | "overlay"`, riding the `ReasoningTally`
plumbing already in place; ledger status reports per-head watermarks.

**API surface.** The existing `"reasoning"` string/array shorthand stays
valid (defaults to `exact`). An **object form** carries freshness and gives
the accumulating reasoning options a single home (the `reasoningBudget`
sibling key stays accepted):

```json
{
  "from": "mydb:main",
  "select": ["?x"],
  "where": {"@id": "?x", "@type": "ex:Chair"},
  "reasoning": {
    "modes": ["owl2rl"],
    "freshness": "asOf",
    "budget": {"maxFacts": 20000000}
  }
}
```

SPARQL adds one pragma alongside the existing ones:

```sparql
# PRAGMA reasoning: owl2rl
# PRAGMA reasoning-freshness: strict
SELECT ?x WHERE { ?x a ex:Chair }
```

Response (tracked body and `x-fdb-reasoning` header), e.g. `asOf` served
from a head lagging at `t=41` while `commit_t=43`:

```json
"reasoning": {
  "source": "head",
  "reasonedThroughT": 41,
  "effectiveT": 41,
  "capped": false,
  "derived_facts": 1584268
}
```

When no head exists, every mode degrades to the overlay path rather than
erroring — `exact`/`strict` are satisfied by it (the closure *is* computed
at `to_t`, just slower) and `asOf` has no older pinned t to offer, so
`effectiveT = to_t`. Freshness never changes whether the answer is
correct, only which `t` it is exact for and what the query pays; `source`
always discloses what happened. Ledger-config may set a default freshness
in `f:reasoningDefaults` (`f:reasoningFreshness`), subject to the group's
override control like every other reasoning setting.

### 7.2 Cursor composition (Strategy A only)

Under Strategy B (§5.0) none of this subsection exists: reasoning queries
run the standard snapshot+novelty path against the merged index. What
follows is Strategy A's query plan, retained for the fallback case.

**What persisting derived-only does and doesn't eliminate.** The "overlay
tax" of §1 is three distinct costs; only the first two are A2's to fix:

1. *Translation* (Flake → V3 ids, dictionary lookups per flake, today
   memoized in the 4-entry LRU): **eliminated** — derived rows are persisted
   already V3-encoded in the base id space (§5.2). Nothing to cache.
2. *Merge*: **remains, and must be measured rather than assumed away** —
   the target is the same windowed, pre-skippable per-leaflet merge that
   novelty already pays on any ledger with unindexed commits: O(rows
   overlapping the scanned range), not O(closure) per query. We add a third
   pointer to an existing two-pointer walk. If the derived side channel is
   large and overlaps the hot ranges, persisted-vs-in-memory may behave
   similarly for hot-cache query latency; the benefit is avoiding
   materialization/translation and keeping the side channel mmap-windowed
   instead of fully resident.
3. *Fast-path bails* (batched-probe joins, cyclic-BGP — the actual q02/q09
   killer): **not fixed by persistence — fixed by the P0 precursor
   workstream (§10).** The bail gate is `ctx.overlay_free_single_graph()`
   (`fluree-db-query/src/context.rs:761`): the probe paths
   (`join.rs:1150`, `property_join.rs:316/793`, `optional.rs:429/782`)
   read base leaflets directly and bail on *any* overlay — **including
   plain novelty on a non-reasoning ledger**. The fix is to make the
   probes merge a sorted side-channel per probed key (the translated
   overlay ops are already in identical V3 sort order and windowable —
   binary-searchable exactly like leaflet s_ids), following the
   "strategy (b): merge overlay" pattern the count fast paths already
   adopted. That work is representation-agnostic: done against today's
   overlay it fixes q02/q09 *before* A2 ships, fixes novelty-heavy
   non-reasoning workloads, and hands the A2 derived lane the same
   reconciliation logic for free. What persistence contributes to the
   probe paths is only scale hygiene: mmap'd leaflet windows instead of a
   multi-GB resident ops vector at LUBM-100 sizes.

Persisting a *merged* base+derived index was considered and rejected: the
RL closure rivals base size, so N rule configs would each duplicate the
full base; every base commit would force merged-index maintenance even
when it triggers zero inferences (defeating per-commit incrementality);
and a merged index still composes with novelty at query time, so a merge
lane exists regardless.

Research confirmed the two-pointer cursor merge
(`merge_overlay_into_batch`, `binary_cursor.rs:539`) cannot absorb a second
*index* in the same walk, and wrapping the derived store as an
`OverlayProvider` would resurrect the Flake→V3 translation tax. The design
is therefore a **derived-rows lane** in the existing merge:

- `GraphDb` gains `reasoned_store: Option<Arc<DerivedIndexStore>>`
  (alongside `binary_store`), populated by the view layer only when
  reasoning resolves to a head.
- `BinaryCursor` gains a second pre-sorted source: derived leaflet windows,
  sliced per leaf exactly like overlay windows (`slice_overlay_for_leaf`
  precedent). The merge becomes three-pointer — base rows ⋈ derived rows ⋈
  novelty ops — over identical V3 sort keys (id alignment, §5.2). Derived
  rows are asserts by construction; net add/remove against novelty follows
  the existing overlay-op lifecycle rules.
- `has_ov`-style pre-skips extend to the derived lane, so leaves with no
  derived rows in range pay one comparison. Non-reasoning queries have
  `reasoned_store = None` and an untouched two-pointer walk.

With P0 in place, the probe paths already know how to reconcile a sorted
side-channel; the derived lane plugs into the same logic (derived leaflet
windows instead of ops windows). A reasoning query in steady state then
runs with no overlay materialization at all and fully live fast paths. The
expected delta versus today's cached in-memory overlay is specifically
"no materialize, no translate, less resident memory"; it is not expected to
turn a side-channel merge into base-index-only access.

## 8. Reindex, GC, and recovery

- **Full reindex / rebuild**: reasoned heads are dropped and re-derived
  against the new base root (new ids ⇒ alignment reset). Prerequisite from
  the roadmap stands: root-cause the historical `fluree index` blank-node
  regression (`benchmark-db/.../FINDINGS.md`, Chair→0) before heads ship
  over reindex, since re-derivation correctness depends on reindexed TBox
  blank nodes.
- **GC**: superseded FDR1 manifests/leaflets/sidecars are CAS-unreferenced
  once the record points at the new manifest; reuse the indexer's
  post-publish GC hook (bounded concurrency).
- **Crash recovery**: publication is atomic at the record update; a crash
  mid-maintenance leaves the old head valid. The worker restarts from the
  record, reloads union-find + identity state from the derived store, and
  re-runs the pending window.

## 9. Coordination and risks

| Item | Plan |
|---|---|
| `feature/merge-preview3` custom merging | Heads don't participate in merge; but the *record* shape and the "head lags after merge" UX should be reviewed with that work so branch tooling lists reasoned heads sensibly. Confirm `GraphSourceType::Reasoning` doesn't collide with its record changes. |
| Blank-node reindex regression | **Resolved as prerequisite** (June 11): does not reproduce on `refactor/resoning-2` — LUBM-1 import → `fluree reindex` → full suite returns identical reasoning-complete results (the FINDINGS.md Chair→0 entry predated the `canonical_split` and #1294-era fixes). Now pinned by `fluree-db-api/tests/it_reasoning_reindex.rs` (minimal blank-node Chair shape, reason → reindex → reason equality, commit f7e5f1216). Residual idea if it ever resurfaces: a dangling-anonymous-`ClassRef` counter in `RestrictionIndex` extraction would turn any blank-node structure corruption into a loud diagnostic instead of silent 0-row answers. |
| Dict id stability across base roots | Manifest pins `base_root_id`; mismatch ⇒ re-derive. Add an invariant test that subject/predicate watermarks only grow across publishes. |
| Identity-rule state size | `IdentityRuleState` grouping maps scale with fp/ifp extents; worker memory must be bounded — spill or rebuild-on-demand if a head's maps exceed a threshold (they are reconstructible). |
| Worker lag under write bursts | Coalescing bounds work to one run per burst; freshness option (§7.1) gives strict readers an out. Lag is visible (`reasonedThroughT`). |

## 10. Milestones

- **P0 — overlay-aware batched probes (precursor, independent of A2).**
  Convert the batched-probe joins (`join.rs` `scan_matches` lane,
  `property_join.rs`, `optional.rs`) and the cyclic-BGP fast path from the
  `overlay_free_single_graph()` bail to "strategy (b)": merge the windowed,
  V3-sorted overlay ops per probed key, including retract reconciliation
  against matched base rows — the pattern the count fast paths already
  use. The mechanism is representation-agnostic (anything implementing
  `OverlayProvider` flows through the same translate-and-merge), which is
  what hands the A2 derived lane its reconciliation for free.
  **Reasoning queries hit a second, independent bail** that novelty-only
  workloads don't: `!ctx.eager_materialization` (`join.rs:1158`), set by
  `execute_prepared` whenever a derived overlay exists because derived
  facts are Sid-space while the batched lanes emit `EncodedSid` (the two
  never compare equal). Fixing q02/q09 against today's in-memory reasoning
  overlay therefore also requires revisiting that eager gate (e.g. batched
  lanes emitting decoded Sids under eager mode, or encoding probe keys on
  entry) — in scope for P0, not deferrable to A2.
  *Acceptance:* q02/q09 at LUBM-25 collapse against **today's**
  reasoning overlay (this requires both gates addressed); batched probes
  stay live on a novelty-heavy non-reasoning ledger (new benchmark to pin
  this — the win applies to every ledger with unindexed commits, not just
  reasoning). This work de-risks the M1 cursor lane: same reconciliation
  case analysis. After P0, rerun the A2 go/no-go performance argument: if
  the current in-memory overlay is already fast enough for the target
  queries, A2's justification should be stated as operational
  persistence/RSS/provenance/incremental maintenance, not as the primary
  q02/q09 performance fix.
- **M0 — spike (1–2 wk).** Decide the §5.0 storage strategy: bootstrap a
  Strategy-B merged reasoned index from the LUBM-25 base refs (branch-style
  ref copy + incremental fold of an existing full materialization) and
  measure leaf-sharing ratio, fold cost, and query parity. Either way:
  FPS1 format stub + `GraphSourceType::Reasoning` record. Only if B is
  rejected: FDR1 stubs + id-alignment validation (Strategy A).
- **M1 — persisted head, full-materialization maintenance (2–4 wk).**
  Worker bootstraps and republishes the head by full re-derivation per
  commit (correct, not yet incremental); query-side three-pointer cursor
  lane + resolution order + `reasonedThroughT` surfacing. *Acceptance:*
  LUBM-25 q01–q14 correct from the head with zero per-query
  materialization and zero translation; restart serves reasoning queries
  without re-deriving; steady-state per-query reasoning overhead is measured
  against three controls: today's cached in-memory overlay with P0, a
  novelty-heavy non-reasoning ledger with equivalent side-channel volume,
  and a base-only indexed run. Success is no hidden full-closure scan and no
  translation floor; any remaining latency should be attributable to the
  same range-overlap merge cost novelty pays.
- **M2 — insert-only FBF (2–3 wk).** §6.3 semi-naive over `E+`; provenance
  entries written. *Acceptance:* steady-state commit maintenance cost is
  O(|E+| consequences), validated on a LUBM-25 ledger with incremental
  inserts; A1.4 post-import pass enabled (bootstrap = E−-empty maintenance).
- **M3 — retracts + sameAs v1 (3–5 wk).** §6.4 FBF + §6.5 component
  recompute; TBox-commit detection + full re-derive; reindex drop/rebuild.
  *Acceptance:* randomized assert/retract soak vs. full re-materialization
  oracle (property test), including sameAs dissolution cases.
- **M4 — B1 surface (with M2/M3 sidecar in place).** `f:derivedBy` /
  proof-tree endpoint reconstructing from FPS1; virtual RDF-star projection.
- Throughout: the LUBM runner gains a head-mode column; baselines recorded
  in `REASONING_STATUS.md`.

## 11. Open questions

1. **Record vs. manifest split** for head metadata (§4) — keep
   `GraphSourceRecord` untouched and put everything in FDR1, or add typed
   fields? Leaning manifest-only until a second consumer needs the fields.
2. **Derived-store leaflet sizing** — derived extents are skewed (huge
   `rdf:type` POST runs); reuse base leaf-split heuristics or tune?
3. **Top-up overlay caching** (§7.1 path 2) — per-head generation cache vs.
   reusing the global reasoning LRU with a head-aware key.
4. **Multiple heads per ledger** — worker scheduling fairness when N
   profiles exist; likely fine with per-head coalescing, verify under load.
5. **`owl:sameAs` canonical-representative choice** must stay stable across
   incremental runs to avoid churning derived rows (today: sorted-first
   member; persist the choice in the manifest?).
6. **What remains after P0?** If novelty-aware probes collapse q02/q09
   against today's overlay, should A2 still be prioritized immediately, or
   should it be framed as an operational milestone for cold restart, RSS,
   incremental commit maintenance, and provenance? The answer should come
   from the P0 benchmark controls above, not from assuming persistence is
   faster than hot in-memory side-channel data.
