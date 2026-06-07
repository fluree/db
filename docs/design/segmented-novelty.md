# Segmented Novelty (LSM) — Design Proposal

**Status:** proposal (for review) · **Branch:** `perf/novelty-write-cost` · **Owner:** perf work

## 1. Problem

Per-commit write cost is **O(accumulated novelty), not O(transaction size)**. With
auto-indexing disabled so novelty grows monotonically, a constant ~56-flake
transaction costs 1.4 ms at an empty ledger and **393 ms at ~1.1 M novelty flakes**
(275×) on an `m7i.xlarge`, perfectly linear (R² 0.998, ~zero fixed cost). Over a
write window between reindexes this is O(N²) cumulative.

Two per-commit costs scale with total novelty (measured, see
`benchmarks/transact-growth/`):

1. **`merge_batch_into_index` — ~57% of CPU** (flamegraph, 400 k samples). Every
   commit re-merges the batch into all four full per-graph index vectors
   (`spot/psot/post/opst`), rebuilding O(N)-sized `Vec<FlakeId>`s. Leaf hot spots:
   the four comparators + the merge inner loop.
2. **Whole-novelty deep clone** for snapshot isolation. `Novelty` derives `Clone`
   (deep-copies the flake arena + all four index vectors), and the commit path
   `Arc::make_mut`s it. Already partly addressed for the **owned-transact** path
   (move-out + `make_mut` in place → **2.4× slope drop, 340→142 µs/1k flakes**),
   but the **cached/server** path is structurally stuck cloning: every query
   `Arc::clone`s novelty (`ledger_view.rs:107`) and the commit stages out-of-lock
   against a snapshot clone (`tx_builder.rs:1025`), so at commit `novelty`
   refcount ≥ 2 and `make_mut` must clone to preserve readers.

**Both costs have the same root and the same cure.** Tail-append was measured dead
(0 % append-eligibility on all four orders — Sid suffix ordering is not numerically
monotonic for sequential inserts). The general fix is to stop storing each order as
one giant sorted vector.

## 2. Goals / non-goals

**Goals**
- Commit cost **O(batch log batch)**, independent of accumulated novelty.
- `Novelty::clone` cost **O(#segments)** (pointer copies), not O(N) — kills the
  cached-path clone for free (immutable segments are never mutated under readers).
- Byte-identical read results and dedup/set-semantics vs today (verified by an
  equivalence harness, §7).
- Keep the external novelty read surface stable (callers get `&Flake`/`Flake`).

**Non-goals (this phase)**
- Changing the on-disk index/commit format (novelty is in-memory only).
- Changing query operators or the overlay translation layer.
- Tiered/size-leveled compaction tuning beyond a first simple policy.

## 3. Current design (recap, with refs)

`fluree-db-novelty/src/lib.rs`:
- `FlakeStore` — shared arena `Vec<Flake>` + `Vec<usize>` sizes; `FlakeId = u32`.
- `Novelty { store: FlakeStore, graphs: Vec<Option<GraphIndexVectors>>, size, t, epoch }`.
- `GraphIndexVectors { spot, psot, post, opst: Vec<FlakeId> }` — one sorted vector
  per order over the shared arena.
- `apply_commit` (made atomic earlier in this branch — graph routing is resolved
  up front before any mutation, plus `Novelty::can_apply`): resolve graph ids → dedup via
  `fact_currently_asserted_in_graph` (binary-search SPOT for the `(s,p,o,dt)` run,
  walk newest-first for exact `(s,p,o,dt,m)`) → push to arena →
  `merge_batch_into_index` per (graph, order) rebuilding the full vector.
- `slice_for_range(g_id, index, first, rhs, leftmost) -> &[FlakeId]` — binary-search
  the single sorted vector for `[first, rhs)`; the **only** range-read primitive.
- `iter_index`, `get_flake`, `len`, `clear_up_to`, `bulk_apply_commits`.
- `FlakeId`, `FlakeStore`, and `GraphIndexVectors` are **novelty-internal**;
  external consumers (overlay/`BinaryRangeProvider`) receive `&Flake`/`Flake`.

## 4. Proposed design

### 4.1 Segment

An **immutable, `Arc`-shared** unit. A fresh commit produces one segment **per
touched graph** (`min_t == max_t`); compaction produces segments spanning a range
of `t` (`min_t < max_t`):

```text
struct Segment {
    min_t: i64,                  // == max_t for a single-commit segment
    max_t: i64,
    flakes: Vec<Flake>,          // surviving flakes, stored once
    spot: Vec<u32>,              // local indices into `flakes`, sorted by SPOT
    psot: Vec<u32>,
    post: Vec<u32>,
    opst: Vec<u32>,
    size: usize,
}
```

Indices are **local** to the segment, so the global shared arena and the global
`FlakeId` disappear (both were sources of the O(N) clone). A `Segment` is built
once, then never mutated → safe to share across snapshots via `Arc`.

`min_t`/`max_t` (not a single `t`) are required so `clear_up_to` can reason about
a compacted segment that spans many transaction times (§4.6).

### 4.2 Novelty

```text
struct Novelty {
    graphs: Vec<Option<Vec<Arc<Segment>>>>,  // per g_id: segments in commit order
    size: usize,
    t: i64,
    epoch: u64,
}
```

- **Commit**: sort the (deduped) batch by each order, build one `Segment` **per
  touched graph** (matching the existing per-graph routing in `apply_commit`), push
  `Arc<Segment>` onto each graph's list. Cost O(batch log batch). No existing data
  touched. (Segments are never mixed-graph — `graphs` is already per-`g_id`, and a
  mixed segment would complicate every range read.)
- **Clone**: clones each `Vec<Arc<Segment>>` (pointer copies). O(#segments).
- **`Arc::make_mut`** on commit: clones only the small `Vec<Arc<Segment>>` pointer
  list when shared; segment payloads are never copied. → cached-path clone solved.

### 4.3 Read path (range merge)

`slice_for_range` returns a contiguous `&[FlakeId]` today — impossible across
segments. Replace with a **k-way merge** over per-segment sorted runs:

```text
fn range_iter(g_id, index, first, rhs, leftmost) -> impl Iterator<Item = &Flake>
```

For each segment, binary-search its order vector for `[first, rhs)` (same
`partition_point` logic as today), then merge the K resulting sorted runs by the
order comparator. Cost O(result · log K). Internal callers (`collect_range` at
`lib.rs:806`, `iter_index`) re-expressed over the merge; the **external** surface
stays `&Flake`/`Flake`/iterator so the overlay is unaffected.

**Borrow-surface change (the main read risk).** Today `slice_for_range` returns a
**borrowed, zero-copy** `&[FlakeId]` into the single sorted vector. Across segments
there is no single contiguous slice to borrow, so the primitive must return either
an owned `Vec` or a lazy merge **iterator** — a real API change for any caller that
relied on the borrow (see §6). Plan: provide a merge **iterator** as the primitive
(`Item = &Flake`, borrowing each segment's `Arc` payload — no flake copy), and a
thin `collect`-to-`Vec` adapter for call sites that want owned results. This keeps
per-flake zero-copy while only the *index ordering* is merged.

**Decision (reviewed): Hybrid — iterator primitive + packed-`FlakeId` shim.**
- The **source-of-truth primitive** is a merge iterator `range_iter(...) -> impl
  Iterator<Item = &Flake>` (and `iter_flakes(index) -> impl Iterator<Item = &Flake>`
  for full scans), borrowing each segment's `Arc` payload (no flake copy).
- `FlakeId` becomes a **newtype over `u64`** (not a raw alias) packing
  `(g_id, seg_idx, local)`, so packed decoding stays centralized and future
  segment-id/epoch changes are possible. It is **read-scoped only**: a transient
  handle valid within one read, never stored durably or held across an `&mut
  Novelty` op (compaction reshuffles `seg_idx`). Documented as such at the type.
- `slice_for_range` stays as an **owned `Vec<FlakeId>` shim** backed by the merge
  iterator, preserving the harness + most callers while the core changes.
- Migrate `runtime_stats` and any full-scan callers to `iter_flakes(index)` early
  (they want `&Flake`, not ids), shrinking the packed-id surface over time.

### 4.4 Dedup / RDF set semantics (the subtle part)

Today dedup is exact: an incoming assertion is skipped iff the fact
`(s,p,o,dt,m)` is **currently asserted** (latest op by `t` for that identity is an
assert). Two options to preserve it across segments:

- **Option B (recommended): query-across-segments.** For each incoming assertion,
  scan segments **newest→oldest**; in each, binary-search SPOT for the `(s,p,o,dt)`
  run and look for exact `(s,p,o,dt,m)`. The first segment containing that identity
  decides (newer wins, since one segment = one `t`): if its op is assert → skip; if
  retract → keep. No found → keep. This reuses the immutable segments readers
  already share, needs **no extra mutable map**, and so adds **nothing** to clone
  cost. Cost: O(#segments · log seg) per assertion, **bounded by compaction**.
- **Option A (fallback): persistent fact-state map.** A per-graph
  `im::HashMap<(s,p,o,dt,m), asserted>` (structural sharing → O(1) clone, O(log N)
  update). O(1) dedup but adds the `im` dependency and a second structure to keep
  in lockstep with the segments.

**Decision (reviewed): start with B behind a seam that lets A drop in later.**
Implement B now (no dep, no clone cost, reuses shared segments), but route all
"latest op for identity" logic through a small `DedupIndex`/`FactState` abstraction
so Option A (the persistent map) can replace the implementation without touching
`apply_commit`/compaction. If dedup shows up as the hot spot once the merge cost is
gone, switch the seam to A. Intra-commit dedup (within one batch) is handled while
building the segment, exactly as today.

**Key condition for B's safety + cost:** B is only correct-and-cheap if compaction
(§4.5) keeps the segment count bounded **and** preserves latest-retraction
tombstones — otherwise newest→oldest scans get unbounded and a dropped tombstone
would let a deleted indexed fact reappear.

### 4.5 Compaction

Unbounded segment growth makes reads and Option-B dedup degrade. Trigger
compaction when a graph's segment count exceeds `K` (start `K = 16`,
configurable): merge the segments into one new immutable `Segment`, keeping **the
latest op per `(s,p,o,dt,m)` identity — including retractions** — and dropping only
*older* ops for the same identity. The resulting segment carries
`min_t = min(inputs)`, `max_t = max(inputs)`.

> **Critical (review):** compaction must NOT drop a fact whose latest op is a
> retract. A latest retraction is a **tombstone** that suppresses a fact still
> present in the *persisted index*; dropping it would make the deleted fact
> reappear in reads until the next reindex. Tombstones are removed only by
> `clear_up_to(index_t)` once the index has absorbed them (§4.6) — never by
> compaction.

This is the only place the O(N) merge survives, now **amortized O(N/K)** per commit
and movable off the commit path (background compaction) later. First cut:
synchronous threshold compaction under the write lock; tiered/background is a
follow-up. Compaction produces a new `Arc<Segment>`; readers holding the old list
are unaffected (COW).

> **Critical (review): compaction is a separate, stats-aware step — keep it OFF in
> the append-only phase.** `runtime_stats::assemble_fast_stats_inner` consumes
> `novelty.iter_index(POST)` as a **raw +1/−1 delta log** against the indexed
> stats. Dropping older ops during compaction breaks that: e.g. a novelty-local
> `assert` then `retract` of a fact NOT in the persisted index is query-invisible
> either way, but compacting to just the `retract` makes stats apply a bogus `−1`.
> Before enabling compaction we must make stats **effective-state / base-aware**
> (or have compaction preserve enough delta information), with regressions for
> both (a) novelty-local assert+retract and (b) indexed-fact + novelty-retract.
> Append-only segmentation (this phase) keeps every op, so stats are unaffected.

### 4.6 Interactions

- **`clear_up_to(cutoff_t)`** (post-index novelty trim — this is what removes
  tombstones once the index has absorbed them): drop a segment outright only when
  `max_t <= cutoff`. A compacted segment that straddles the cutoff
  (`min_t <= cutoff < max_t`) must be **rebuilt/filtered** to retain only flakes
  with `t > cutoff` (re-sorting the four order vectors). Single-commit segments
  (`min_t == max_t`) always drop or keep wholesale. Replaces the current
  `retain_alive` arena scan.
- **`bulk_apply_commits`** (first-load/catch-up): build one segment per commit (or
  build then compact once). Keep the existing dedup contract.
- **`size`/backpressure**: maintained as sum of segment sizes; unchanged externally.
- **`epoch`**: still bumped once per commit for cache invalidation.

## 5. Why this fixes the cached-path clone

The cached path can't reach refcount 1 (queries `Arc::clone` novelty; staging is
out-of-lock). With segments, that no longer matters: a commit appends an
`Arc<Segment>`; `make_mut` on the segment **list** copies only pointers, and the
segments readers hold are immutable and untouched. So snapshot isolation is
preserved with an O(#segments) cost instead of an O(N) deep clone — no rework of
staging ownership/retries/rollback required (the reason the standalone cached-path
fix was abandoned).

## 6. Blast radius

- Rewrites internals of `fluree-db-novelty/src/lib.rs` (`FlakeStore`,
  `GraphIndexVectors`, `apply_commit`, `merge_batch_into_index`, `slice_for_range`,
  `iter_index`, `clear_up_to`, `bulk_apply_commits`, `fact_currently_asserted_in_graph`).
- **Read surface is NOT fully contained** (review): `slice_for_range` returns a
  borrowed `&[FlakeId]` and `FlakeId`/`iter_index`/`get_flake` are used outside the
  crate — notably by `StagedLedger` and runtime-stats assembly. The migration must
  either update those call sites or give them the new merge-iterator/`&Flake`
  adapter. Audit before coding: `grep -rn "slice_for_range\|iter_index\|get_flake\|FlakeId" --include='*.rs'`
  outside `fluree-db-novelty/`.
- Query/overlay operators that consume *materialized* `&Flake`/`Flake` stay
  unchanged once the adapter is in place; only callers relying on the zero-copy
  `&[FlakeId]` borrow need touching.
- **Stats coupling:** `runtime_stats::assemble_fast_stats_inner` reads
  `iter_index(POST)` as a raw delta log — unaffected by append-only segmentation,
  but a hard constraint on compaction (§4.5).

## 7. Equivalence harness (build first, per review)

A differential/golden harness that pins the **observable contract** of the current
`Novelty` and re-runs it against the segmented impl:
- Random commit sequences covering: duplicate asserts, retract, reassert, same
  `(s,p,o,dt)` with different `m`, list-index metadata `m.i`, named graphs (multiple
  g_ids), and comparator ties.
- After **every** commit, compare for all four orders + a sweep of range reads:
  full materialized contents must be **identical** (not just set-equal — order too).
- Run old-vs-new in one test; also keep a property-style fuzz with a fixed seed.
- Wire a slope check via the existing `it_transact_growth_slope` gate
  (`GROWTH_MAX_SLOPE_US_PER_1K`).

## 8. Risks / open questions

- **Dedup cost (Option B)** grows with #segments → depends on compaction `K`;
  measure before committing to A vs B.
- **Read fan-in** (k-way merge) adds per-read overhead; bounded by `K`. Confirm no
  regression on novelty-heavy read benches (`it_select_star_novelty*`).
- **Comparator-tie ordering** must match today exactly — the equivalence harness is
  the guard.
- **Compaction under the write lock** adds occasional latency spikes; acceptable
  first cut, background later.
- `im` dependency only if Option A is needed.

## 9. Phased plan

1. ✅ Equivalence harness green against current `Novelty` (records the contract:
   sortedness, multiset across all four orders, range==filtered-scan for all
   orders + edge cases, golden digests).
2. **Append-only segmentation (this phase, compaction OFF):** introduce `Segment`
   + segmented `Novelty` behind the harness; `apply_commit` append-only; dedup via
   Option B behind a `DedupIndex` seam; reads via range-merge with a
   borrow-preserving iterator + `Vec` adapter; update external read-surface call
   sites (`StagedLedger`, runtime stats). Harness digest must match exactly
   (append-only preserves every flake). Re-measure slope on the box.
3. **Compaction (separate, stats-aware):** first split the harness into a *raw*
   contract (current) and an *effective/stats* contract; make
   `assemble_fast_stats` effective-state-aware (or compaction base-aware) with
   regressions for novelty-local assert+retract and indexed-fact+novelty-retract;
   only then enable threshold-`K` compaction (preserving tombstones, §4.5).
4. `clear_up_to` / `bulk_apply_commits` over segments.
5. Re-measure on `m7i.xlarge`: growth slope should collapse toward flat; confirm
   cached-path clone gone (flamegraph) and read benches unregressed.
6. (Follow-up) background/tiered compaction; lazy merge iterator if needed.
