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

### 4.5 Compaction — structural compact-all (implemented)

Unbounded segment growth makes reads degrade **linearly in segment count** (one
binary-search probe per segment per range read; measured ~1 µs/segment, e.g. a
4-flake point read costs ~1 ms at 1,000 segments, ~47 ms at 40,000). The first
compaction strategy is **structural compact-all, triggered by segment count,
preserving every flake**:

- For each graph whose segment count exceeds a threshold
  (`DEFAULT_COMPACTION_THRESHOLD = 128`), collect ALL flakes from its segments,
  build ONE new immutable `Segment` with the same flakes (original `t/op/m/g`),
  rebuild the four local order vectors, and swap `Vec<Arc<Segment>>` to a single
  `Arc<Segment>` (`min_t/max_t` span the inputs).
- **Preserve everything**: no dedup, no dropping older asserts, no collapsing
  assert/retract pairs — only the *representation* changes.
- `fact_state`, `size`, `t`, and the live flake multiset are unchanged; `epoch`
  bumps (layout-scoped `FlakeId`s and epoch-keyed caches must refresh).
- API: `compact_over(threshold)`, `compact_all()`; policy queries
  `segment_count` / `max_segment_count` / `needs_compaction(threshold)`. It is a
  maintenance primitive — **not auto-wired into the commit path.**

**Why preserve everything (supersedes the earlier "keep latest op per identity"
sketch):** keeping the whole assert/retract log is safe for immutability and time
travel, AND it sidesteps the stats problem entirely — because nothing is dropped,
`runtime_stats::assemble_fast_stats`'s POST `+1/−1` delta-log and the tombstone
semantics are unchanged. **No stats-aware / effective-state work is required for
log-preserving compaction.** (A future *collapsing* compaction that dropped older
ops — including a latest-retract tombstone that suppresses a still-persisted fact —
WOULD reopen the stats-aware requirement and the tombstone hazard; we are
explicitly not doing that. Tombstones are removed only by `clear_up_to(index_t)`
once the index has absorbed them, §4.6 — never by compaction.)

**Cost (measured, m7i.xlarge):** compact-all is one full re-sort — O(total
novelty), ~1.1–1.4 µs/flake: **525 ms @ 480k, 1.25 s @ 1M, 2.8 s @ 2M flakes.**
Too expensive for the synchronous write path. Policy:

| Context | Compaction policy |
|---|---|
| Cold load / query Lambda load | `bulk_apply_commits` already yields **K=1 per graph** (no replay into thousands of segments). A warm query Lambda that received incremental novelty may `compact_all` before a query — amortized over the request, competing against bad fan-out. |
| Long-lived servers / peers | Background maintenance when `needs_compaction`; **never block insert-only commits**. |
| Transactor Lambda | **Do NOT compact on threshold crossing** — a 0.5–3 s tail erases the write-path win. Insert-only: skip. Read-heavy txn: compact only if estimated read fan-out cost > compaction cost (a few point/narrow reads don't justify it; many broad overlapping reads might). |

**Amortization caveat:** compact-all re-sorts the entire growing novelty, so
triggering it every K commits is O(N/K) amortized per commit — a constant-factor
(1/threshold) cut of the old O(N), **bounded by the reindex window** (novelty
flushes at reindex). That growing-cliff shape is why the read path now uses
**tiered compaction** (§4.5.1); compact-all remains as an explicit maintenance /
cold-consolidation primitive.

#### 4.5.1 Tiered (size-leveled) compaction — implemented, read-path default

Same log-preserving (stats-safe) rule, but incremental. Each segment has a
**size class** derived from its flake count: `size_class(count, T) =
floor(log_T(count))` (no per-segment level stored — derived, so builds/merges
don't thread it). The invariant: merging `T` segments of class `K` yields one of
class `K+1`. `Novelty::tier_compact(T)` cascade-merges the lowest class holding
`>= T` segments into one larger segment, repeating upward, preserving every
flake. This bounds read fan-out to `~T · log_T(N)` segments while each merge
touches only one class's flakes — no full-novelty rewrite. `needs_tier_compaction(T)`
is the cheap policy check; `DEFAULT_TIER_WIDTH = 16`.

The read-side trigger (`LedgerHandle::snapshot` → `compact_if_needed`) runs
`tier_compact(tier_width)`; the per-handle knob is `tier_width`
(`set_tier_width`, `0`/`1` disables). Policy is unchanged (read/maintenance path
only; insert-only commits never trigger it).

**Measured (m7i.xlarge, release, 400 commits × 50 subjects, `tier_width=16`):**
max segment count **K=31** (vs compact-all letting K reach 128), **26 merges,
mean 3.0 ms/merge**, one level-transition cascade at 30.85 ms. So tiered keeps K
bounded *continuously* (better reads throughout) and amortizes compaction into
many small merges, instead of compact-all's K=128 + spikes growing 13 → 29 →
49 ms every ~128 commits. Inherent size-tiered caveat: a level-`L` cascade merges
`T` class-`L` segments (`≈ T · level_size` flakes), so the *largest* single merge
grows per level — but exponentially rarer (O(log N) amortized) and bounded by the
reindex window. A future leveled variant with partial merges could cap the worst
single stall further if needed.

### 4.6 Interactions

- **`clear_up_to(cutoff_t)`** (post-index novelty trim — this is what removes
  tombstones once the index has absorbed them): drop a segment outright only when
  `max_t <= cutoff`. A compacted segment that straddles the cutoff
  (`min_t <= cutoff < max_t`) must be **rebuilt/filtered** to retain only flakes
  with `t > cutoff` (re-sorting the four order vectors). Single-commit segments
  (`min_t == max_t`) always drop or keep wholesale. Replaces the current
  `retain_alive` arena scan.
- **`bulk_apply_commits`** (first-load/catch-up): folds all commits + any existing
  segments into **one consolidated segment per graph (K=1)** in a single dedup
  pass, so cold/query-Lambda load starts already-compacted. Keeps the dedup
  contract.
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

- ✅ **Dedup cost:** chose **Option A** (`NoveltyFactState`, `imbl::OrdMap`) —
  O(log N) per flake, O(1) clone; independent of #segments. (`imbl` pulled in.)
- ✅ **Read fan-in** (k-way merge) is **linear in segment count** (~1 µs/segment,
  measured) — mitigated by zone-map prune + the K=1 fast path; the asymptote is
  removed only by keeping `K` small via compaction.
- **Comparator-tie ordering** must match today exactly — the equivalence harness is
  the guard (golden digests unchanged through every step).
- ✅ **Compaction cost** measured at ~1.1–1.4 µs/flake (one full re-sort) → too
  expensive synchronous on the write path; maintenance/background/cold-load only
  (§4.5). Tiered compaction is the cliff-free follow-up.

## 9. Phased plan

1. ✅ Equivalence harness green against current `Novelty` (records the contract:
   sortedness, multiset across all four orders, range==filtered-scan for all
   orders + edge cases, golden digests).
2. ✅ **Append-only segmentation:** `Segment` + segmented `Novelty`; `apply_commit`
   append-only; dedup via **Option A** (`NoveltyFactState`, an `imbl::OrdMap`
   current-state map — O(log N), O(1) clone), NOT Option B; reads via the k-way
   merge iterator + `Vec` shim; external call sites migrated (`StagedLedger` got
   its own `FlakeId=u32`; runtime stats / dict rebuilds on `iter_flakes`). Harness
   digest unchanged. Slope collapsed 152 → 0.012 µs/1k on the box.
3. ✅ **Read fan-out mitigations:** zone-map prune (skip non-overlapping segments,
   18–35× on disjoint point reads) + K=1 fast path (raw slice loop, no heap/no id
   packing — point/narrow match the old single-vector design).
4. ✅ `clear_up_to` / `bulk_apply_commits` over segments (`bulk` → K=1 per graph).
5. ✅ Re-measure on `m7i.xlarge`: write slope flat; read fan-out characterized
   (linear in segment count); compaction cost measured (~1.1–1.4 µs/flake).
6. ✅ **Compaction — structural compact-all (log-preserving):** segment-count
   triggered, preserves every flake, so **no stats-aware work needed** (the
   earlier "stats-aware/effective-state" prerequisite applied only to a
   *collapsing* compaction, which we are not doing). Maintenance primitive, not on
   the write path (§4.5 policy table).
7. ✅ **Read-side compaction trigger:** `LedgerHandle::snapshot` consolidates
   when needed before serving (read/maintenance path only; insert-only commits
   never trigger). Started as compact-all; now tiered.
8. ✅ **Tiered/size-leveled** structural compaction (§4.5.1) — incremental
   similarly-sized-run merges → ~`T·log_T(N)` segments, bounded per-merge work,
   no growing cliff; same log-preserving (stats-safe) rule.
9. (Follow-up) **config plumbing** — `compaction_enabled` / `tier_width` /
   `max_segments_before_read_compact` through the builder + server config.
   Optional later: leveled (partial-merge) variant to cap the worst single
   cascade; lazy merge iterator if K>1 reads need it.
