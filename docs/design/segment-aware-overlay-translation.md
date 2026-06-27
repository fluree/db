# Segment-aware overlay translation + LIMIT row-budget — implementation plan

**Status:** proposal v2 (review-incorporated) · **Branch:**
`perf/overlay-segmentation-limit-pushdown` (off `perf/novelty-write-cost-lsm`) ·
**Owner:** perf work

Read-side complement to `segmented-novelty.md` (which made the novelty *write* path
O(batch) via immutable `Arc<Segment>` runs). This makes *query-time overlay
translation* segment-aware so a write burst stops re-paying O(total-overlay) per cold
query, and adds a LIMIT row-budget so eager join lanes stop over-producing under small
`LIMIT`s.

Two independent workstreams (different crates), done in parallel:
- **Tier-2** — segment-aware overlay translation. Tasks #1–#5.
- **#4** — LIMIT row-budget. Tasks #6–#7. Independent of novelty; **land first**.

### v2 — review responses (what changed from v1)

| # | Finding | Resolution (below) |
|---|---|---|
| 1 | `(seg_id, order)` is not a sufficient cache key | §2.2(a)/§2.3.1 — **process-global** seg_id + a coarse **reindex-scoped** translation binding; epoch deliberately excluded for cross-commit reuse |
| 2 | reasoning overlays won't get the win unless they forward segments | §2.5 — **Phase 1 scoped to raw `Novelty`**; `ReasoningOverlay`/`DerivedFactsOverlay` segment-awareness is an explicit **Phase 2** (it is the *larger* reasoning win, not an afterthought) |
| 3 | ephemeral predicate ids unsafe across independently-cached merged runs | §2.3.2 — a segment that allocates an **ad-hoc** ephemeral id is **uncacheable** (assembly falls back); only base-dict / `runtime_small_dicts` (shared, deterministic) ids are cacheable |
| 4 | trait shape vs claimed range/window behavior | §2.2(b)/(d) — explicit: **cache whole-segment translations; window + zone-map after merge** (no pre-translation range pruning in the trait) |
| 5 | `@vector` global fallback erases the benefit | §2.2(c) — cache a per-segment **`{ops, untranslated}`** bundle (mirrors the whole-graph product); only that segment's untranslated flakes stream, no global fallback |
| 6 | memory bounds must be designed, not deferred | §2.6 — byte-bounded LRU up front; the whole-graph global translation cache is **bypassed** (not co-resident) when the segment path is active |

---

## 1. What the base branch already gives us

`segmented-novelty.md` is **done** (write side). As-built (`fluree-db-novelty/src/lib.rs`):

- `Novelty { graphs: Vec<Option<Vec<Arc<Segment>>>>, size, flake_count, t, epoch, … }`.
- `struct Segment { flakes, spot/psot/post/opst: Vec<u32>, min_t, max_t, size }` —
  immutable, `Arc`-shared, built once. A commit appends **one segment per touched
  graph** (`min_t == max_t`); compaction yields range segments (`min_t < max_t`).
- Reads k-way-merge per-segment runs with a zone-map (`Segment::may_overlap`) prune and
  a K=1 fast path (`graph_merge`, `read_flakes`, `for_each_overlay_flake` `lib.rs:1335`).
- `epoch` bumps on **every commit and every compaction**.
- The branch explicitly lists **"changing the overlay translation layer" as a non-goal**,
  so `fluree-db-query`'s translation path is untouched — our seam, no conflict.

**Gap left open:** segments have **no stable identity** (`FlakeId.seg` is the array
index, reshuffled by compaction, `lib.rs:84,112`). Task #1 adds one — see §2.2(a).

---

## 2. Tier-2 — segment-aware overlay translation

### 2.1 Objective

`BinaryScanOperator::open()` (`binary_scan.rs:2004-2127`) translates the **whole graph's**
overlay (`translate_overlay_flakes_with_untranslated`, `binary_scan.rs:2357`) into V3
`OverlayOp`s before the first row, memoized per-execution + a cross-query LRU(4). Epoch
bumps every commit, so the **cold query after each write re-translates the entire
overlay** — O(overlay × dict-lookups). During a write burst over a large overlay this is
the flat floor. Goal: translate only **newly-appended segments**, reuse cached
translations of older immutable segments, k-way-merge the per-segment op runs at scan.

### 2.2 Design

**(a) Stable segment id — Task #1.** Add `seg_id: u64` to `Segment`, assigned from a
**process-global `static AtomicU64`** in `Segment::build` (one increment per segment
build — off the per-flake path, negligible). Every distinct segment ever built in the
process gets a unique id; `Arc`-clones share it; reloads / cloned-or-diverged novelties /
derived overlays that build *different* segments get *different* ids. This is
collision-free **by construction** — unlike a per-`Novelty` counter, which collides
across ledgers/reloads/derived/diverged values (Finding 1). *Care:* `tier_compact_graph`
holds a `&mut self.graphs[g]` borrow across its merge loop (`lib.rs:715-764`); the
process-global static needs no `&mut self`, so it sidesteps that borrow entirely. No
observable behavior change; equivalence harness stays byte-identical.

**(b) Trait seam — Task #2.** Extend `OverlayProvider` (`fluree-db-core/src/overlay.rs`)
with **whole-segment** iteration (no range/window args — windowing happens after merge,
§2.2(d)):

```rust
fn for_each_overlay_segment(
    &self, g_id: GraphId, index: IndexType,
    f: &mut dyn FnMut(/* seg_id */ u64, /* min_t */ i64, /* max_t */ i64,
                      /* flakes */ &mut dyn FnMut(&Flake)),
) { /* default: one synthetic segment over for_each_overlay_flake */ }
```

`Novelty` implements it over its `Vec<Arc<Segment>>` in order. Non-`Novelty` overlays get
the default (one synthetic whole-overlay segment) — correct but unsegmented (see §2.5).

**(c) Per-segment translation cache — Task #3.** Cache key `(binding, seg_id, order)`
(§2.3.1) → a **`{ops: Arc<[OverlayOp]>, untranslated: Arc<[Flake]>}`** bundle, where
`ops` are translated + sorted but **NOT lifecycle-resolved, NOT to_t-filtered**, and
`untranslated` are the segment's `@vector`/unsupported flakes (mirrors the whole-graph
`TranslatedOverlayOps`). Reuses `translate_one_flake_v3_pub`. On a burst only new (small)
segments miss; old segments are `Arc`-clone hits. A segment that would need an **ad-hoc
ephemeral predicate id is not cached** (§2.3.2).

**(d) Scan-time assembly — Task #4.** Replace the whole-graph translate in `open()` with:
gather the graph's current segments via the trait; per segment apply the **t zone-map**
(skip if `min_t > to_t`; no per-flake filter if `max_t <= to_t`; per-flake only on a
straddling compacted segment) and the cursor **key window** (`overlay_window_for_range`
on the cached integer ops — cheap); **k-way merge** the per-segment op runs in `order`;
**then** `resolve_overlay_ops` over the merged stream. Feed `set_overlay_ops_window`.
Stream each contributing segment's `untranslated` flakes after the cursor (per-segment,
so no global fallback). If any contributing segment is uncacheable (ad-hoc ephemeral),
fall back to the whole-graph path for the whole assembly.

### 2.3 Correctness invariants (non-negotiable)

**2.3.1 Cache identity (Finding 1).** `seg_id` is process-global (content+lineage
identity). The cache key also carries a **translation binding** = a coarse fingerprint of
the base store + base-dict identity that changes **only on index swap / reindex**, NOT on
commit (mirrors `OverlayOpsBinding`/`GlobalTranslationKey` in spirit but **excludes the
overlay epoch and `to_t`** — those are applied post-merge, and including them would kill
the cross-commit reuse that is the whole point). This relies on `dict_novelty` being
**append-only** (ids never reassigned), so an old segment's translation is stable as
later commits append; **verify this assumption — if a path can reassign ids, that path
must bump the binding or decline caching.**

**2.3.2 Ephemeral predicate ids (Finding 3).** `translate_one_flake_v3_pub`
(`binary_scan.rs:2436-2448`) resolves a predicate id via `store.sid_to_p_id` → else
`runtime_small_dicts.predicate_id` → else an **ad-hoc local counter**
(`next_ephemeral_p_id`). The first two are stable/shared and safe to merge across
independently-cached segment runs; the ad-hoc id is translation-local and would make two
segments' ops incomparable. Rule: translate a segment with a **stable-ids-only** flag; if
any predicate hits the ad-hoc path, that segment is **uncacheable** → assembly falls back
to whole-graph. `runtime_small_dicts` is populated per commit with the commit's
predicates, so ad-hoc allocation is the rare exception, not the common case.

**2.3.3 Resolve-after-`to_t` (the time-travel landmine).** `resolve_overlay_ops`
collapses each fact to its globally-latest op; a query whose `to_t` is below a retraction
would otherwise filter the retract away and **lose the earlier assert** for a
novelty-only fact (the cursor applies retracts regardless of `to_t` and only re-emits
asserts). Lifecycle resolution is cross-segment and runs on the merged, t-filtered
stream — never cached.

### 2.4 Why segments are the right cache unit

Each segment is immutable + `Arc`-shared + carries `min_t/max_t`, mapping 1:1 to a cache
entry and a `to_t` zone-map; the read path already k-way-merges per-segment runs, so op
runs slot into the identical shape; compaction (new seg_ids) is natural invalidation.
*Caveat:* after compaction merges K segments into one (new big seg_id), the next query
translates that whole segment once (a one-time cost at compaction), then caches it.

### 2.5 Scope: raw Novelty now, reasoning overlays as Phase 2 (Finding 2)

`ReasoningOverlay` (`reasoning.rs:14`) merges base novelty + derived flakes and delegates
`as_any()` to the base; `DerivedFactsOverlay` (`fluree-db-reasoner/src/overlay.rs:25`) is
the materialized derived set. With only the default trait impl, a large derived overlay
is **one synthetic segment** and gets retranslated whenever the base epoch changes — so
the headline reasoning/burst win is **not** delivered by Phase 1.

- **Phase 1 (this plan):** segment-aware translation for raw `Novelty` — the
  write-burst-of-real-commits story.
- **Phase 2 (explicit follow-up, Task TBD):** segment-aware `DerivedFactsOverlay` (and
  `ReasoningOverlay` forwarding). The derived set is the **larger** reasoning cost, so
  this is where the reasoning burst win actually lands. Likely keyed off the existing
  `global_reasoning_cache` generation rather than novelty seg_ids.

### 2.6 Memory bounds (Finding 6)

The per-segment cache is a **byte-bounded LRU** (configurable cap), sized at design time —
not an open question. When the segment path is active for an overlay, the whole-graph
`global_translation_cache` and per-execution `translated_overlay_cache` are **bypassed**
for it (not co-resident) so we never hold both products. Compaction orphans old
seg entries; the LRU evicts them by byte pressure (a compacted segment's new entry may be
large — the byte cap, not an entry count, governs).

---

## 3. #4 — LIMIT row-budget (independent of novelty)

### 3.1 Objective

LIMIT is pure backpressure today (`limit.rs`). The `NestedLoopJoinOperator` batched
accumulator (`join.rs:1357`, flush at `BATCHED_JOIN_SIZE = 100_000`,
`fluree-db-core/src/range.rs:37`) builds up to 100k rows even under `LIMIT 5` — the code
already flags this ("draining an entire left batch up front … makes top-level LIMITs much
[worse]", `join.rs:426`). Add an optional row budget so it stops early.

### 3.2 Design

**Plumbing — Task #6.** New `Operator::set_row_budget(&mut self, budget: usize)` —
**default = ABSORB** (no-op; an operator forwards downward only by explicit override).
Safe-by-default. Forwarders that push down: **`{Project, Offset (budget += offset),
Limit (budget = limit)}`**. **NOT** `Bind` (drops rows via clobber/inline-filter), nor
`Filter`/`Sort`/`Distinct`/`GroupAggregate`/hash-build — these absorb. Details from
review:

- **Set the budget before the child's `open()`** (overproduction happens during/after
  `open()` and the first `next_batch()`), not lazily on first pull.
- **Saturating math** for `offset + limit`.
- **Advisory, not semantic.** An operator may stop early only when it can still satisfy
  its parent's requested rows; completeness consumers must never receive a budget. The
  query result is identical with or without the hint.
- **EXPLAIN / debug counters:** `budget_received`, `first_flush_cap`, `rows_avoided`, so
  the optimization is observable and testable.
- Hot-loop purity: consult the budget only at batch/leaflet boundaries, never inside
  fused per-row/per-group merge loops.

**NestedLoop adaptive flush — Task #7.** Under a budget, lower the batched lane's first
flush threshold and grow it geometrically; **first-flush-only** cap (capping every flush
regresses selective small-LIMIT setup). Correctness-neutral: a fully-drained hinted join
yields the identical multiset **and** order (the 100k mid-stream flush already makes
multi-flush a tested invariant). Also bound the `binary_scan` `range_iter` untranslated
drain.

---

## 4. Phased plan (→ tasks)

| Phase | Task | Gate |
|---|---|---|
| limit budget plumbing | #6 | transparency-boundary tests (filter/sort/distinct/bind must NOT propagate) |
| NestedLoop adaptive flush | #7 | join multiset+order unchanged; no-LIMIT regression bench; `rows_avoided` counter |
| seg id foundation | #1 | equivalence harness green; `cargo test -p fluree-db-novelty` |
| trait seam | #2 | workspace compiles; non-Novelty overlays use default |
| per-segment cache | #3 | unit tests: cache hit/miss, {ops,untranslated}, ad-hoc-ephemeral decline |
| wire scan + validate | #4, #5 | differential vs monolithic translate; flat burst-cost bench |
| (follow-up) reasoning | Phase 2 | segment-aware DerivedFactsOverlay |

**Land #4 (Tasks #6–#7) first** — independent, low-risk, immediate value, shared budget
substrate — while this Tier-2 blueprint gets review. Per review: tighten cache identity
(§2.3.1), reasoning forwarding (§2.5), and ephemeral ids (§2.3.2) **before** touching
`fluree-db-novelty`.

## 5. Risks / testing

- **Differential harness is the real safety net** (Task #5), not the `Ok(None)` fallback
  (which only catches explicit bail conditions, not a merge/window logic bug). Build it
  early. Cases: asserts/retracts/reassert/same `(s,p,o,dt)` diff `m`/list `o_i`/
  multi-graph/`to_t` below a retraction/post-compaction/novelty-only predicate.
- Verify the **append-only `dict_novelty`** assumption behind §2.3.1; if any path
  reassigns ids, it must bump the binding.
- K-way merge cost is linear in segment count; the base branch's tiered compaction bounds
  K, so the op-merge inherits the bound.
- #4: budget transparency-boundary tests + no-LIMIT regression bench; SPARQL
  (`it_query_sparql.rs`) + JSON-LD (`it_query.rs`) parity.
