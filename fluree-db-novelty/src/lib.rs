//! Novelty overlay for Fluree DB
//!
//! This crate provides in-memory storage for uncommitted transactions (novelty)
//! that overlays the persisted index. It uses sorted vectors per index for
//! cache locality and efficient merge operations.
//!
//! # Design
//!
//! - **Append-only segments**: Each graph holds a `Vec<Arc<Segment>>`; every
//!   commit that touches a graph builds one new immutable [`Segment`] (its own
//!   flakes + four locally-sorted index orders) and appends it. No merge into a
//!   growing vector, so per-commit write cost is `O(batch log batch)` rather than
//!   `O(total novelty)`.
//! - **k-way merge reads**: Range/scan reads merge the per-graph segments on
//!   demand in index-comparator order ([`GraphMergeIter`]).
//! - **Cheap clone**: Segments are `Arc`-wrapped, so cloning a `Novelty`
//!   (snapshot isolation under concurrent readers / `Arc::make_mut` on the
//!   commit path) copies only pointers — never the flakes.
//! - **Batch commit**: Epoch bumps once per commit, not per flake.
//! - **Set-semantics dedup**: `O(log novelty)` per flake via [`fact_state`]'s
//!   persistent current-state map (which itself clones in `O(1)`) — not the old
//!   `O(total novelty)` re-merge.
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_novelty::Novelty;
//!
//! let mut novelty = Novelty::new(0);
//! novelty.apply_commit(flakes, 1, &reverse_graph)?;
//!
//! // Get slice for a specific graph's leaf range
//! let slice = novelty.slice_for_range(g_id, IndexType::Spot, Some(&first), Some(&rhs), false);
//! ```

mod commit;
mod commit_flakes;
pub mod delta;
mod error;
mod fact_state;
mod links;
mod runtime_stats;
mod stats;

pub use commit::{
    collect_dag_cids, collect_first_parent_cids, collect_first_parent_cids_with_split_mode,
    find_common_ancestor, load_commit_by_id, load_commit_envelope_by_id,
    trace_commit_envelopes_by_id, trace_commits_by_id, trace_first_parent_commits_by_id, Commit,
    CommitEnvelope, CommonAncestor, TxnMetaEntry, TxnMetaValue, TxnSignature, MAX_TXN_META_BYTES,
    MAX_TXN_META_ENTRIES,
};
pub use commit_flakes::{
    drop_forged_commit_flakes, generate_commit_flakes, is_forged_commit_flake, iso_to_epoch_ms_opt,
    stamp_commit_flakes_dropping_forgeries, stamp_graph_on_commit_flakes,
    warn_if_forged_commit_flakes_dropped,
};
pub use delta::{compute_delta_keys_and_changes, delta_keys_of, FactKey, NetChangeAccumulator};
pub use error::{NoveltyError, Result};
pub use fluree_db_core::commit::codec::envelope::{MAX_GRAPH_DELTA_ENTRIES, MAX_GRAPH_IRI_LENGTH};
pub use fluree_db_core::commit::codec::format::{CommitSignature, ALGO_ED25519};
pub use fluree_db_core::commit::codec::verify_commit_blob;
pub use fluree_db_credential::SigningKey;
pub use links::LinkBase;
pub use runtime_stats::{
    assemble_fast_stats, assemble_fast_stats_with, assemble_full_stats, assemble_full_stats_with,
    assemble_planner_stats, merge_is_identity, resolve_runtime_predicate_id, stats_merge_site,
    NoveltyDeltaResolver, NoveltyMerge, StatsAssemblyError, StatsLookup, STATS_MERGE_TARGET,
};
pub use stats::current_stats;

use fact_state::NoveltyFactState;
use fluree_db_core::{Flake, GraphId, IndexType, Sid, CONFIG_GRAPH_ID};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

/// Read-scoped handle to a flake inside the novelty overlay.
///
/// Packs `(graph slot, segment_index, local_index)` into a `u64`. A `FlakeId` is only
/// valid within the read scope that produced it ([`Novelty::slice_for_range`] /
/// [`Novelty::iter_index`]) and only against the same [`Novelty`] — segment
/// indices shift when `apply_commit` / `bulk_apply_commits` / `clear_up_to`
/// mutate novelty. Do not store it across mutations, serialize it, or do
/// arithmetic on it; round it straight back to [`Novelty::get_flake`].
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct FlakeId(u64);

impl FlakeId {
    const LOCAL_BITS: u32 = 24;
    const SEG_BITS: u32 = 20;
    // The remaining 20 high bits hold the graph's novelty slot (`GraphSlots`).
    const LOCAL_MASK: u64 = (1 << Self::LOCAL_BITS) - 1;
    const SEG_MASK: u64 = (1 << Self::SEG_BITS) - 1;
    const SLOT_LIMIT: u64 = 1 << (64 - Self::SEG_BITS - Self::LOCAL_BITS);

    #[inline]
    fn pack(slot: u32, seg: usize, local: u32) -> Self {
        debug_assert!(
            u64::from(slot) < Self::SLOT_LIMIT,
            "novelty graph slot overflow"
        );
        debug_assert!(
            seg as u64 <= Self::SEG_MASK,
            "novelty segment index overflow"
        );
        debug_assert!(
            u64::from(local) <= Self::LOCAL_MASK,
            "novelty local index overflow"
        );
        FlakeId(
            (u64::from(slot) << (Self::SEG_BITS + Self::LOCAL_BITS))
                | ((seg as u64) << Self::LOCAL_BITS)
                | u64::from(local),
        )
    }

    #[inline]
    fn graph(self) -> usize {
        (self.0 >> (Self::SEG_BITS + Self::LOCAL_BITS)) as usize
    }

    #[inline]
    fn seg(self) -> usize {
        ((self.0 >> Self::LOCAL_BITS) & Self::SEG_MASK) as usize
    }

    #[inline]
    fn local(self) -> usize {
        (self.0 & Self::LOCAL_MASK) as usize
    }
}

impl std::fmt::Debug for FlakeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FlakeId(g{} s{} l{})",
            self.graph(),
            self.seg(),
            self.local()
        )
    }
}

/// Maximum flakes a single segment can address (local-index width).
pub const MAX_SEGMENT_FLAKES: usize = FlakeId::LOCAL_MASK as usize + 1;

/// Maximum segments a single graph can hold before a reindex is required.
pub const MAX_SEGMENTS: usize = FlakeId::SEG_MASK as usize + 1;

/// Maximum distinct graphs novelty can hold before a reindex is required.
pub const MAX_GRAPH_SLOTS: usize = FlakeId::SLOT_LIMIT as usize;

/// Per-graph segment lists, by a dense slot per graph. Graph ids are 32-bit
/// and sparse, so a vector indexed by id would grow with the largest id (and
/// every copy-on-write clone with it); the slot is what a [`FlakeId`] packs.
#[derive(Clone, Default)]
struct GraphSlots {
    slot_of: HashMap<GraphId, u32>,
    slots: Vec<(GraphId, Option<Vec<Arc<Segment>>>)>,
}

impl GraphSlots {
    #[inline]
    fn slot(&self, g_id: GraphId) -> Option<u32> {
        self.slot_of.get(&g_id).copied()
    }

    #[inline]
    fn get(&self, g_id: GraphId) -> Option<&Option<Vec<Arc<Segment>>>> {
        self.slot(g_id).map(|s| &self.slots[s as usize].1)
    }

    fn get_mut(&mut self, g_id: GraphId) -> Option<&mut Option<Vec<Arc<Segment>>>> {
        let s = self.slot(g_id)?;
        Some(&mut self.slots[s as usize].1)
    }

    /// The graph's entry, giving it a slot if it has none.
    fn entry(&mut self, g_id: GraphId) -> &mut Option<Vec<Arc<Segment>>> {
        let next = self.slots.len() as u32;
        let s = *self.slot_of.entry(g_id).or_insert(next);
        if s == next {
            self.slots.push((g_id, None));
        }
        &mut self.slots[s as usize].1
    }

    #[inline]
    fn by_slot(&self, slot: usize) -> &Option<Vec<Arc<Segment>>> {
        &self.slots[slot].1
    }

    fn by_slot_mut(&mut self, slot: usize) -> Option<&mut Option<Vec<Arc<Segment>>>> {
        self.slots.get_mut(slot).map(|(_, segs)| segs)
    }

    fn len(&self) -> usize {
        self.slots.len()
    }

    fn lists(&self) -> impl Iterator<Item = &Vec<Arc<Segment>>> {
        self.slots.iter().filter_map(|(_, segs)| segs.as_ref())
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = &mut Option<Vec<Arc<Segment>>>> {
        self.slots.iter_mut().map(|(_, segs)| segs)
    }

    /// Graphs holding at least one non-empty segment, ascending.
    fn present(&self) -> Vec<GraphId> {
        let mut graphs: Vec<GraphId> = self
            .slots
            .iter()
            .filter(|(_, segs)| segs.as_ref().is_some_and(|s| !s.is_empty()))
            .map(|(g, _)| *g)
            .collect();
        graphs.sort_unstable();
        graphs
    }

    /// Drop the slots of graphs that hold nothing. Reassigns slots, so it runs
    /// only where `FlakeId`s are already invalidated (a segment rebuild).
    fn compact(&mut self) {
        self.slots
            .retain(|(_, segs)| segs.as_ref().is_some_and(|s| !s.is_empty()));
        self.slot_of = self
            .slots
            .iter()
            .enumerate()
            .map(|(i, (g, _))| (*g, i as u32))
            .collect();
    }
}

/// Default tier width for [`Novelty::tier_compact`]: merge a size class once it
/// holds this many segments. Bounds read fan-out to ~`tier_width · log` of the
/// novelty size with only bounded per-merge work (no full-novelty rewrite).
pub const DEFAULT_TIER_WIDTH: usize = 16;

/// Process-global monotonic source of [`Segment`] ids.
///
/// Every distinct segment built in this process gets a unique id, so a
/// per-segment overlay-translation cache keyed on it never collides across
/// ledgers, reloads, cloned/diverged `Novelty` values, or derived overlays.
/// `Arc`-clones share the id; compaction builds new segments with new ids
/// (orphaning stale cache entries). Internal identity only — not persisted and
/// not part of the observable novelty contract.
static NEXT_SEG_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// An immutable, append-only batch of novelty flakes for one graph.
///
/// Owns its flakes and four locally-sorted index orders (vectors of local
/// indices into `flakes`). Segments are `Arc`-wrapped inside [`Novelty`], so
/// cloning a `Novelty` copies only pointers — never the flakes.
struct Segment {
    /// Process-global unique id; stable cache identity (see [`NEXT_SEG_ID`]).
    seg_id: u64,
    flakes: Vec<Flake>,
    spot: Vec<u32>,
    psot: Vec<u32>,
    post: Vec<u32>,
    opst: Vec<u32>,
    min_t: i64,
    max_t: i64,
    size: usize,
}

impl Segment {
    /// Build a segment from a batch of flakes, sorting the four index orders.
    ///
    /// `parallel` uses the rayon pool for each sort — worth it for the large
    /// single-segment builds in [`Novelty::bulk_apply_commits`], not for the
    /// small per-commit batches where the pool hand-off would dominate.
    fn build(flakes: Vec<Flake>, parallel: bool) -> Self {
        let n = flakes.len();
        let order_by = |index: IndexType| -> Vec<u32> {
            let mut v: Vec<u32> = (0..n as u32).collect();
            if parallel {
                use rayon::prelude::*;
                v.par_sort_unstable_by(|&a, &b| {
                    index.compare(&flakes[a as usize], &flakes[b as usize])
                });
            } else {
                v.sort_unstable_by(|&a, &b| {
                    index.compare(&flakes[a as usize], &flakes[b as usize])
                });
            }
            v
        };

        let spot = order_by(IndexType::Spot);
        let psot = order_by(IndexType::Psot);
        let post = order_by(IndexType::Post);
        let opst = order_by(IndexType::Opst);

        let mut min_t = i64::MAX;
        let mut max_t = i64::MIN;
        let mut size = 0usize;
        for f in &flakes {
            min_t = min_t.min(f.t);
            max_t = max_t.max(f.t);
            size += f.size_bytes();
        }

        Segment {
            seg_id: NEXT_SEG_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            flakes,
            spot,
            psot,
            post,
            opst,
            min_t,
            max_t,
            size,
        }
    }

    /// The flakes after `cutoff_t`, as a segment. Each of the four orders is
    /// filtered rather than re-sorted — dropping flakes leaves the survivors
    /// in order — so trimming a large segment at an index publish costs a
    /// pass over it, not four sorts. `None` when nothing survives.
    fn after(&self, cutoff_t: i64) -> Option<Segment> {
        let mut remap: Vec<u32> = vec![u32::MAX; self.flakes.len()];
        let mut flakes = Vec::new();
        let mut min_t = i64::MAX;
        let mut max_t = i64::MIN;
        let mut size = 0usize;
        for (i, f) in self.flakes.iter().enumerate() {
            if f.t > cutoff_t {
                remap[i] = flakes.len() as u32;
                min_t = min_t.min(f.t);
                max_t = max_t.max(f.t);
                size += f.size_bytes();
                flakes.push(f.clone());
            }
        }
        if flakes.is_empty() {
            return None;
        }
        let filter = |order: &[u32]| -> Vec<u32> {
            order
                .iter()
                .filter_map(|&i| {
                    let mapped = remap[i as usize];
                    (mapped != u32::MAX).then_some(mapped)
                })
                .collect()
        };
        Some(Segment {
            seg_id: NEXT_SEG_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            spot: filter(&self.spot),
            psot: filter(&self.psot),
            post: filter(&self.post),
            opst: filter(&self.opst),
            flakes,
            min_t,
            max_t,
            size,
        })
    }

    #[inline]
    fn order(&self, index: IndexType) -> &[u32] {
        match index {
            IndexType::Spot => &self.spot,
            IndexType::Psot => &self.psot,
            IndexType::Post => &self.post,
            IndexType::Opst => &self.opst,
        }
    }

    /// The sub-slice of this segment's `index` order covering `(first, rhs]`
    /// (left-exclusive unless `leftmost`, right-inclusive) — binary search, as
    /// the pre-segmentation per-graph index vectors did.
    fn range(
        &self,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
    ) -> &[u32] {
        let ids = self.order(index);
        if ids.is_empty() {
            return &[];
        }
        let start = if leftmost {
            0
        } else if let Some(f) = first {
            ids.partition_point(|&id| {
                index.compare(&self.flakes[id as usize], f) != Ordering::Greater
            })
        } else {
            0
        };
        let end = if let Some(r) = rhs {
            ids.partition_point(|&id| {
                index.compare(&self.flakes[id as usize], r) != Ordering::Greater
            })
        } else {
            ids.len()
        };
        if start >= end {
            return &[];
        }
        &ids[start..end]
    }

    /// Zone-map prune: can this segment's `index` order possibly contribute to
    /// the range `(first, rhs]` (left-exclusive unless `leftmost`)? Compares only
    /// the segment's min/max key for that order (`ids[0]` / `ids[last]`), so it's
    /// 0–2 comparisons — far cheaper than the two `partition_point` searches in
    /// [`Self::range`]. Lets the k-way merge skip non-overlapping segments
    /// entirely, which collapses point/narrow fan-out when segments are mostly
    /// disjoint by key (does nothing for full scans or heavily overlapping
    /// ranges — there is no segment to skip).
    fn may_overlap(
        &self,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
    ) -> bool {
        let ids = self.order(index);
        let (Some(&lo), Some(&hi)) = (ids.first(), ids.last()) else {
            return false;
        };
        let seg_min = &self.flakes[lo as usize];
        let seg_max = &self.flakes[hi as usize];
        // Entirely above the inclusive upper bound?
        if let Some(r) = rhs {
            if index.compare(seg_min, r) == Ordering::Greater {
                return false;
            }
        }
        // Entirely at/below the exclusive lower bound?
        if !leftmost {
            if let Some(f) = first {
                if index.compare(seg_max, f) != Ordering::Greater {
                    return false;
                }
            }
        }
        true
    }
}

/// One segment's cursor within a [`GraphMergeIter`]: a sub-range of that
/// segment's index order plus the position reached so far.
struct MergeStream<'a> {
    seg_idx: usize,
    order: &'a [u32],
    pos: usize,
    flakes: &'a [Flake],
}

/// Heap entry for the k-way merge: the current front flake of one stream.
struct MergeHead<'a> {
    flake: &'a Flake,
    stream: usize,
    seg_idx: usize,
    index: IndexType,
}

impl Ord for MergeHead<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; reverse so the comparator-smallest flake
        // (older segment on ties) becomes the max and pops first. Tie order
        // among comparator-equal flakes is not observable in results (identical
        // flakes are indistinguishable), but is kept deterministic.
        self.index
            .compare(self.flake, other.flake)
            .then_with(|| self.seg_idx.cmp(&other.seg_idx))
            .reverse()
    }
}
impl PartialOrd for MergeHead<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for MergeHead<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for MergeHead<'_> {}

/// Lazy comparator-ordered k-way merge across one graph's segments for a given
/// index order and range. Yields `(FlakeId, &Flake)` in fully `index`-sorted
/// order — the single read primitive every novelty read builds on.
struct GraphMergeIter<'a> {
    /// The graph's novelty slot, which the ids it yields pack.
    slot: u32,
    index: IndexType,
    streams: Vec<MergeStream<'a>>,
    heap: BinaryHeap<MergeHead<'a>>,
}

impl<'a> Iterator for GraphMergeIter<'a> {
    type Item = (FlakeId, &'a Flake);

    fn next(&mut self) -> Option<Self::Item> {
        let head = self.heap.pop()?;
        let s = head.stream;
        let (seg_idx, local, advance) = {
            let stream = &mut self.streams[s];
            let local = stream.order[stream.pos];
            stream.pos += 1;
            let advance = if stream.pos < stream.order.len() {
                Some((stream.flakes, stream.order[stream.pos]))
            } else {
                None
            };
            (stream.seg_idx, local, advance)
        };
        let id = FlakeId::pack(self.slot, seg_idx, local);
        if let Some((flakes, next_local)) = advance {
            self.heap.push(MergeHead {
                flake: &flakes[next_local as usize],
                stream: s,
                seg_idx,
                index: self.index,
            });
        }
        Some((id, head.flake))
    }
}

/// A novelty range read yielding `&Flake` with NO `FlakeId` packing.
///
/// `Single` is the K=1 fast path: one segment, so iterate its range slice
/// directly — no heap, no id math, matching the pre-segmentation single-vector
/// read speed. `Merge` is the K>1 k-way merge. Backs [`Novelty::iter_flakes`] and
/// `for_each_overlay_flake` (the hot read paths).
enum FlakeRead<'a> {
    Done,
    Single {
        order: std::slice::Iter<'a, u32>,
        flakes: &'a [Flake],
    },
    Merge(GraphMergeIter<'a>),
}

impl<'a> Iterator for FlakeRead<'a> {
    type Item = &'a Flake;

    fn next(&mut self) -> Option<&'a Flake> {
        match self {
            FlakeRead::Done => None,
            // Slice iterator (pointer-bump, no bounds check) keeps the K=1 full
            // scan close to the old single-vector speed.
            FlakeRead::Single { order, flakes } => {
                order.next().map(|&local| &flakes[local as usize])
            }
            FlakeRead::Merge(it) => it.next().map(|(_, f)| f),
        }
    }
}

/// A novelty range read yielding packed [`FlakeId`]s, for the compat id callers
/// (`slice_for_range`, `iter_index`). `Single` is the K=1 fast path (no heap);
/// it still packs ids because the caller asked for ids. `Merge` is the K>1
/// k-way merge.
enum IdRead<'a> {
    Done,
    Single {
        slot: u32,
        seg_idx: usize,
        order: std::slice::Iter<'a, u32>,
    },
    Merge(GraphMergeIter<'a>),
}

impl Iterator for IdRead<'_> {
    type Item = FlakeId;

    fn next(&mut self) -> Option<FlakeId> {
        match self {
            IdRead::Done => None,
            IdRead::Single {
                slot,
                seg_idx,
                order,
            } => order
                .next()
                .map(|&local| FlakeId::pack(*slot, *seg_idx, local)),
            IdRead::Merge(it) => it.next().map(|(id, _)| id),
        }
    }
}

/// Novelty overlay - in-memory storage for uncommitted transactions
///
/// Append-only, segmented: each graph holds a `Vec` of immutable `Arc<Segment>`,
/// one appended per commit that touches the graph. `apply_commit` never merges
/// into a growing vector — it builds one new segment and pushes it, so per-commit
/// write cost is `O(batch log batch)` instead of `O(total novelty)`. Reads k-way
/// merge the segments on demand (see [`GraphMergeIter`]).
///
/// Graphs are held by dense slot ([`GraphSlots`]).
#[derive(Clone, Default)]
pub struct Novelty {
    /// Per-graph append-only segment lists.
    graphs: GraphSlots,

    /// Total live size in bytes (for backpressure)
    pub size: usize,

    /// Total live flake count (keeps `len`/`is_empty` O(1)).
    flake_count: usize,

    /// Latest transaction time in novelty
    pub t: i64,

    /// Epoch for cache invalidation - bumped once per commit
    pub epoch: u64,
    /// Globally-unique content-version stamp (see
    /// [`OverlayProvider::content_version`]): refreshed from a process-wide
    /// counter on every content mutation, so no two novelty states with
    /// different flake content ever share a version — across instances,
    /// clones, and ledgers. Clones share the version until one mutates,
    /// which is exactly right: identical content, identical key. `0` means
    /// "empty since construction" (all empty overlays are equivalent).
    content_version: u64,
    /// Epoch for the RDFS schema-hierarchy cache — bumped only when a commit
    /// asserts or retracts `rdfs:subClassOf` / `rdfs:subPropertyOf`, so the
    /// shared hierarchy cache stays current without any work on the (vastly
    /// more common) commits that don't touch the schema.
    pub schema_epoch: u64,
    /// Epoch for the compiled-SHACL cache — bumped only when a commit
    /// asserts or retracts SHACL vocabulary (any `sh:*` predicate, or an
    /// `rdf:type` edge to a SHACL / class type). Lets transaction
    /// enforcement reuse the previously compiled shapes when nothing
    /// shape-affecting changed.
    pub shacl_epoch: u64,
    /// Sticky: some flake in this overlay carried an RDF-list position
    /// (`m.i`). Lets filtered-DELETE staging skip list-meta hydration when
    /// both the index root and novelty report no list rows. Never cleared —
    /// a trimmed overlay keeps the bit; the indexed root takes over.
    pub has_list_meta: bool,
    /// The predicates of the flakes behind [`Self::has_list_meta`]. Sticky
    /// the same way, and shared so a clone stays cheap.
    pub list_predicates: std::sync::Arc<std::collections::HashSet<Sid>>,
    /// Sticky: some assertion in this overlay holds a term whose persisted
    /// index key keeps less than the term
    /// ([`FlakeValue::index_key_loses_detail`](fluree_db_core::FlakeValue::index_key_loses_detail)).
    /// A query reads such a term through its key as another term, so a DELETE
    /// that matched it through a WHERE row cannot retract the row as decoded:
    /// staging reads the stored fact instead. Never cleared — a trimmed
    /// overlay keeps the bit; an index holds no such term.
    pub has_lossy_index_terms: bool,

    /// Highest `commit_t` at which the ledger config graph (`CONFIG_GRAPH_ID`)
    /// received a write. Monotonic within a novelty window; resets to 0 when
    /// novelty is rebuilt (e.g. reindex) — a downward reset only forces a
    /// re-resolve, never a stale read. Used as the fail-safe invalidation key
    /// for the resolved-config cache on `LedgerHandle`: config content at head
    /// is fully determined by this marker, so a matching marker guarantees the
    /// cached config is current. Stays 0 for ledgers that never write config.
    pub config_write_t: i64,

    /// Current-state fact index for RDF set-semantics dedup (latest op per
    /// identity, per graph, within this novelty window). Persistent map, so it
    /// clones in O(1). The dedup oracle behind the seam; see [`fact_state`].
    fact_state: NoveltyFactState,

    /// The index's attachments, which reification links are derived against
    /// (see [`links`]); `None` until the ledger installs it.
    link_base: Option<LinkBase>,
    /// Every commit at or before this `t` has its links in novelty.
    links_through: i64,
    /// An annotation flake has been applied; sticky.
    saw_annotations: bool,
}

#[inline]
fn flake_has_list_meta(flake: &Flake) -> bool {
    flake.m.as_ref().is_some_and(|m| m.i.is_some())
}

/// An assertion of a term its index key keeps less of (see
/// [`Novelty::has_lossy_index_terms`]).
#[inline]
fn flake_has_lossy_index_term(flake: &Flake) -> bool {
    flake.op && flake.o.index_key_loses_detail(&flake.dt)
}

/// What the flakes [`Novelty::apply_commit`] keeps change beyond their
/// segments.
#[derive(Default)]
struct KeptEffects {
    /// Asserting OR retracting a hierarchy edge changes the RDFS schema —
    /// invalidate the shared hierarchy cache.
    schema_touched: bool,
    /// Any SHACL-vocabulary flake invalidates the compiled-shapes cache.
    shacl_touched: bool,
}

impl KeptEffects {
    #[inline]
    fn note(&mut self, flake: &Flake) {
        self.schema_touched |= fluree_db_core::namespaces::is_rdfs_hierarchy_predicate(&flake.p);
        self.shacl_touched |= fluree_db_core::namespaces::is_shacl_affecting_flake(flake);
    }
}

impl Novelty {
    /// Record what a kept `flake` changes on novelty itself: whether it is
    /// an annotation, its list position, and whether its index term loses
    /// detail.
    fn note_kept(&mut self, flake: &Flake) {
        self.saw_annotations |= fluree_db_core::is_annotation_predicate(&flake.p);
        self.note_list_meta(flake);
        self.has_lossy_index_terms |= flake_has_lossy_index_term(flake);
    }

    /// Record `flake`'s list position in [`Self::has_list_meta`] and
    /// [`Self::list_predicates`].
    fn note_list_meta(&mut self, flake: &Flake) {
        if flake_has_list_meta(flake) {
            self.has_list_meta = true;
            if !self.list_predicates.contains(&flake.p) {
                std::sync::Arc::make_mut(&mut self.list_predicates).insert(flake.p.clone());
            }
        }
    }

    /// Create a new empty novelty overlay
    pub fn new(t: i64) -> Self {
        Self {
            graphs: GraphSlots::default(),
            size: 0,
            flake_count: 0,
            t,
            epoch: 0,
            content_version: 0,
            schema_epoch: 0,
            shacl_epoch: 0,
            has_list_meta: false,
            list_predicates: Default::default(),
            has_lossy_index_terms: false,
            config_write_t: 0,
            fact_state: NoveltyFactState::new(),
            link_base: None,
            links_through: t,
            saw_annotations: false,
        }
    }

    /// True once novelty has applied an annotation: an `rdf:reifies` link or
    /// a legacy `f:reifies*` bundle flake.
    pub fn has_annotations(&self) -> bool {
        self.saw_annotations
    }

    /// Resolve a flake's graph ID from its `Flake.g` field.
    ///
    /// - `None` → default graph (g_id = 0)
    /// - `Some(sid)` → looked up in `reverse_graph`; returns error if unknown
    fn resolve_flake_g_id(flake: &Flake, reverse_graph: &HashMap<Sid, GraphId>) -> Result<GraphId> {
        match &flake.g {
            None => Ok(0),
            Some(g_sid) => reverse_graph.get(g_sid).copied().ok_or_else(|| {
                NoveltyError::InvalidGraph(format!("flake references unknown graph Sid: {g_sid}"))
            }),
        }
    }

    /// Stamp a fresh globally-unique content version. Pair with every
    /// `epoch += 1`: `epoch` drives lineage-local cache invalidation, while
    /// the content version keys cross-instance caches (see
    /// [`OverlayProvider::content_version`]).
    fn refresh_content_version(&mut self) {
        // Drawn from the process-wide allocator shared by every overlay type
        // (see `next_overlay_content_version`); `0` = empty since construction.
        self.content_version = fluree_db_core::overlay::next_overlay_content_version();
    }

    /// Append a freshly built segment to a graph, growing the graphs vec.
    fn push_segment(&mut self, g_id: GraphId, seg: Segment) {
        self.size += seg.size;
        self.flake_count += seg.flakes.len();
        self.graphs
            .entry(g_id)
            .get_or_insert_with(Vec::new)
            .push(Arc::new(seg));
    }

    /// Ensure appending one more segment to `g_id` won't overflow the packed
    /// [`FlakeId`]'s segment field. Append-only growth (no compaction) makes this
    /// a real ceiling — a reindex resets novelty well before it. The local-index
    /// field needs no such runtime guard: per-segment flake counts are already
    /// bounded by the `MAX_SEGMENT_FLAKES` check in `apply_commit` and by the
    /// chunking in `set_graph_segments`.
    fn check_segment_capacity(&self, g_id: GraphId) -> Result<()> {
        if self.graphs.slot(g_id).is_none() && self.graphs.len() >= MAX_GRAPH_SLOTS {
            return Err(NoveltyError::overflow(
                "novelty graph count exceeds capacity, trigger reindex",
            ));
        }
        let existing = self
            .graphs
            .get(g_id)
            .and_then(Option::as_ref)
            .map_or(0, Vec::len);
        if existing + 1 > MAX_SEGMENTS {
            return Err(NoveltyError::overflow(
                "novelty segment count exceeds capacity, trigger reindex",
            ));
        }
        Ok(())
    }

    /// Replace a graph's segment list with `segs` (chunked so no segment exceeds
    /// the local-index width). Used by the whole-graph-rebuilding paths.
    fn set_graph_segments(&mut self, g_id: GraphId, mut flakes: Vec<Flake>, parallel: bool) {
        if flakes.is_empty() {
            if let Some(segs) = self.graphs.get_mut(g_id) {
                *segs = None;
            }
            return;
        }
        let mut segs: Vec<Arc<Segment>> = Vec::new();
        while flakes.len() > MAX_SEGMENT_FLAKES {
            let tail = flakes.split_off(MAX_SEGMENT_FLAKES);
            segs.push(Arc::new(Segment::build(flakes, parallel)));
            flakes = tail;
        }
        segs.push(Arc::new(Segment::build(flakes, parallel)));
        *self.graphs.entry(g_id) = Some(segs);
    }

    /// Recompute `size` / `flake_count` from the current segments. Used by the
    /// whole-graph-replacing paths (`bulk_apply_commits`, `clear_up_to`).
    fn recompute_totals(&mut self) {
        let mut size = 0usize;
        let mut count = 0usize;
        for segs in self.graphs.lists() {
            for seg in segs {
                size += seg.size;
                count += seg.flakes.len();
            }
        }
        self.size = size;
        self.flake_count = count;
    }

    /// Segments currently held by graph `g_id` (0 if the graph is absent/empty).
    pub fn segment_count(&self, g_id: GraphId) -> usize {
        self.graphs
            .get(g_id)
            .and_then(Option::as_ref)
            .map_or(0, Vec::len)
    }

    /// Largest per-graph segment count across all graphs (the read-fan-out driver).
    pub fn max_segment_count(&self) -> usize {
        self.graphs.lists().map(Vec::len).max().unwrap_or(0)
    }

    /// Whether any graph holds at least `tier_width` segments in one size class —
    /// i.e. [`Self::tier_compact`] would do work. Cheap read-path policy hook.
    pub fn needs_tier_compaction(&self, tier_width: usize) -> bool {
        if tier_width < 2 {
            return false;
        }
        self.graphs.lists().any(|segs| {
            segs.len() >= tier_width && {
                let mut counts: HashMap<u32, usize> = HashMap::new();
                for seg in segs {
                    *counts
                        .entry(size_class(seg.flakes.len(), tier_width))
                        .or_default() += 1;
                }
                counts.values().any(|&c| c >= tier_width)
            }
        })
    }

    /// Tiered structural compaction (size-leveled): one **bounded** cascade pass —
    /// merge `tier_width` segments of the lowest full size class into one (class+1)
    /// and continue strictly upward, each class processed at most once per call.
    /// **Preserves every flake** (no dedup, no tombstone collapse — stats-safe and
    /// observationally transparent). Per-call work is
    /// bounded to one cascade chain (`<= classes` merges of `tier_width`
    /// segments), so even the first read after a long insert-only burst can't
    /// stall on a huge backlog. In steady state (compaction keeps pace with
    /// commits) this converges immediately; a large pre-existing backlog drains
    /// across subsequent calls. Bounds read fan-out to ~`tier_width ·
    /// log_tier_width(N)`. `epoch` bumps if anything merged. Returns the merge
    /// count.
    pub fn tier_compact(&mut self, tier_width: usize) -> usize {
        if tier_width < 2 {
            return 0;
        }
        let mut merges = 0;
        for slot in 0..self.graphs.len() {
            merges += self.tier_compact_graph(slot, tier_width);
        }
        if merges > 0 {
            self.recompute_totals();
            self.epoch += 1;
            self.refresh_content_version();
        }
        merges
    }

    /// One bounded cascade pass for a graph. Starting at the lowest full size
    /// class, merge **exactly `tier_width`** of its segments into one (yielding
    /// class+1), then move strictly upward — each class is processed at most once
    /// per call. This bounds per-merge AND per-call work to one cascade chain
    /// (`<= number_of_classes` merges of `tier_width` segments), so even the first
    /// read after a long insert-only burst can't stall on a huge class-0 backlog;
    /// any remaining backlog drains on subsequent reads.
    fn tier_compact_graph(&mut self, slot: usize, tier_width: usize) -> usize {
        let mut merges = 0;
        let mut min_class = 0u32;
        while let Some(Some(segs)) = self.graphs.by_slot_mut(slot) {
            if segs.len() < tier_width {
                break;
            }
            // Lowest still-unprocessed class that holds a full group.
            let mut counts: HashMap<u32, usize> = HashMap::new();
            for seg in segs.iter() {
                let class = size_class(seg.flakes.len(), tier_width);
                if class >= min_class {
                    *counts.entry(class).or_default() += 1;
                }
            }
            let Some(target_class) = counts
                .iter()
                .filter(|&(_, &c)| c >= tier_width)
                .map(|(&class, _)| class)
                .min()
            else {
                break;
            };

            // Merge exactly `tier_width` segments of the target class (the first
            // ones in vec order); keep the rest (including any surplus of this
            // class, which drains on a later call). `tier_width` class-K segments
            // sum to class K+1 exactly.
            let taken = std::mem::take(segs);
            let mut keep: Vec<Arc<Segment>> = Vec::new();
            let mut flakes: Vec<Flake> = Vec::new();
            let mut merged = 0usize;
            for seg in taken {
                if merged < tier_width && size_class(seg.flakes.len(), tier_width) == target_class {
                    match Arc::try_unwrap(seg) {
                        Ok(s) => flakes.extend(s.flakes),
                        Err(shared) => flakes.extend(shared.flakes.iter().cloned()),
                    }
                    merged += 1;
                } else {
                    keep.push(seg);
                }
            }
            // Chunk the merged flakes at MAX_SEGMENT_FLAKES (matching
            // set_graph_segments) so a tier merge whose sum
            // exceeds the local-index width can't silently truncate FlakeIds in
            // release. (A single class-(K+1) result above the cap becomes a few
            // segments instead; the cascade handles them on later calls.)
            while flakes.len() > MAX_SEGMENT_FLAKES {
                let tail = flakes.split_off(MAX_SEGMENT_FLAKES);
                keep.push(Arc::new(Segment::build(flakes, true)));
                flakes = tail;
            }
            keep.push(Arc::new(Segment::build(flakes, true)));
            *segs = keep;
            merges += 1;
            // Move up so the lowest class is touched at most once per call.
            min_class = target_class + 1;
        }
        merges
    }

    /// Validate that a batch can be applied WITHOUT mutating any state.
    ///
    /// Checks the two conditions that make [`Self::apply_commit`] fallible —
    /// per-segment capacity and graph routability. Callers that mutate a
    /// shared/live Novelty in place (e.g. via `Arc::make_mut`) call this first to
    /// guarantee an all-or-nothing apply: if it returns `Ok`, the subsequent
    /// `apply_commit` cannot fail partway and leave the ledger inconsistent.
    pub fn can_apply(&self, flakes: &[Flake], reverse_graph: &HashMap<Sid, GraphId>) -> Result<()> {
        if flakes.len() > MAX_SEGMENT_FLAKES {
            return Err(NoveltyError::overflow(
                "commit batch exceeds max segment flakes, trigger reindex",
            ));
        }
        // Each touched graph gains at most one segment; guard the packed FlakeId's
        // segment field. Routing is also the only other fallible step.
        let mut checked: HashSet<GraphId> = HashSet::new();
        for flake in flakes {
            let g_id = Self::resolve_flake_g_id(flake, reverse_graph)?;
            if checked.insert(g_id) {
                self.check_segment_capacity(g_id)?;
            }
        }
        Ok(())
    }

    /// Apply a batch of flakes from a commit, routing each flake to its graph.
    ///
    /// Append-only: builds one new immutable [`Segment`] per touched graph and
    /// appends it — no merge into existing storage. Epoch bumps ONCE per call.
    /// Unknown graph Sids cause an error — no silent fallback to the default
    /// graph.
    ///
    /// Atomic: graph routing and link derivation (the fallible steps) run
    /// before any mutation, so an error leaves novelty untouched.
    ///
    /// Returns the reification links derived for the commit (see [`links`]),
    /// which the caller's dictionary novelty must register.
    pub fn apply_commit(
        &mut self,
        flakes: Vec<Flake>,
        commit_t: i64,
        reverse_graph: &HashMap<Sid, GraphId>,
    ) -> Result<Vec<Flake>> {
        if flakes.is_empty() {
            return Ok(Vec::new());
        }

        let span = tracing::debug_span!(
            "novelty_apply_commit",
            commit_t = commit_t,
            flake_count = flakes.len(),
        );
        let _guard = span.enter();

        // A single commit batch becomes at most one segment per graph, so it must
        // fit a segment's local-index width (astronomically larger than any real
        // commit — reindex long before this). Checked before any mutation.
        if flakes.len() > MAX_SEGMENT_FLAKES {
            return Err(NoveltyError::overflow(
                "commit batch exceeds max segment flakes, trigger reindex",
            ));
        }

        // Resolve every flake's graph id FIRST. Graph routing is the only fallible
        // step, so resolving it before any mutation makes apply_commit atomic: a
        // routing error leaves novelty completely untouched. This matters because
        // callers mutate a possibly cache-shared Novelty in place (Arc::make_mut),
        // where a partial mutation would poison live state under the write lock.
        let mut routed: Vec<(Flake, GraphId)> = Vec::with_capacity(flakes.len());
        for flake in flakes {
            let g_id = Self::resolve_flake_g_id(&flake, reverse_graph)?;
            routed.push((flake, g_id));
        }

        // Each touched graph gains at most one segment; guard the packed FlakeId's
        // segment field before any mutation so a capacity error stays atomic.
        let mut checked: HashSet<GraphId> = HashSet::new();
        for (_, g_id) in &routed {
            if checked.insert(*g_id) {
                self.check_segment_capacity(*g_id)?;
            }
        }

        // Links read the index, so they are derived before any mutation too.
        let links_current = self.current_link_base().is_some();
        let mut derived: Vec<Flake> = Vec::new();
        if let Some(base) = self.current_link_base() {
            let touched = links::touched_reifiers(routed.iter().map(|(f, g)| (*g, f)));
            if !touched.is_empty() {
                let links = self.derive_links(base, &touched, self.t)?;
                derived = links.iter().map(|(_, f)| f.clone()).collect();
                routed.extend(links.into_iter().map(|(g_id, f)| (f, g_id)));
            }
        }

        // From here on every step is infallible.
        self.t = self.t.max(commit_t);
        if links_current {
            self.links_through = self.t;
        }
        self.epoch += 1; // Bump epoch once per commit
        self.refresh_content_version();

        // Advance the config-graph write marker so the resolved-config cache
        // (see `LedgerHandle`) invalidates iff this commit touched config.
        if checked.contains(&CONFIG_GRAPH_ID) {
            self.config_write_t = self.config_write_t.max(commit_t);
        }

        // RDF set semantics: skip assertion flakes whose fact (s, p, o, dt, m) is
        // already **currently asserted** in this graph's novelty window. This
        // prevents duplicate facts from accumulating when the same triple is
        // asserted across commits. Retractions are always accepted — they cancel
        // existing assertions. `fact_state` reflects PRIOR-commit state here (it
        // is updated only after this loop).
        //
        // An assertion this commit also retracts is kept: the pair holds the
        // assertion (`fluree_db_core::same_t`), and dropping it would leave the
        // retraction alone. That is what `bulk_apply_commits` keeps too. Such
        // assertions are held until the commit's retractions are known, so a
        // commit with none pays nothing for it.
        let mut per_graph: HashMap<GraphId, Vec<Flake>> = HashMap::new();
        let mut deduped = 0u64;
        let mut restated: Vec<(Flake, GraphId)> = Vec::new();
        let mut effects = KeptEffects::default();
        for (flake, g_id) in routed {
            if flake.op && self.fact_state.is_asserted(g_id, &flake) {
                restated.push((flake, g_id));
                continue;
            }
            effects.note(&flake);
            self.note_kept(&flake);
            per_graph.entry(g_id).or_default().push(flake);
        }
        if !restated.is_empty() {
            let retracted_here: HashSet<(GraphId, &Flake)> = per_graph
                .iter()
                .flat_map(|(&g_id, batch)| batch.iter().filter(|f| !f.op).map(move |f| (g_id, f)))
                .collect();
            let kept: Vec<bool> = restated
                .iter()
                .map(|(flake, g_id)| retracted_here.contains(&(*g_id, flake)))
                .collect();
            for ((flake, g_id), kept) in restated.into_iter().zip(kept) {
                if kept {
                    effects.note(&flake);
                    self.note_kept(&flake);
                    per_graph.entry(g_id).or_default().push(flake);
                } else {
                    deduped += 1;
                }
            }
        }
        let KeptEffects {
            schema_touched,
            shacl_touched,
        } = effects;
        if schema_touched {
            self.schema_epoch += 1;
        }
        if shacl_touched {
            self.shacl_epoch += 1;
        }

        // Record every kept flake (assert + retract) into the current-state
        // index. `record` resolves lifecycle order-independently (highest-t
        // wins, same-t assert wins), so batch order doesn't matter. After the
        // keep loop, so within-batch dedup decisions saw only prior state.
        for (&g_id, batch) in &per_graph {
            for flake in batch {
                self.fact_state.record(g_id, flake);
            }
        }

        if deduped > 0 {
            tracing::debug!(
                deduped,
                "skipped duplicate assertion flakes (set semantics)"
            );
        }

        // Build + append one immutable segment per touched graph. No merge into
        // existing storage — this is the per-commit O(novelty) → O(batch)
        // collapse. Small per-commit batches sort sequentially (rayon hand-off
        // would dominate).
        for (g_id, batch) in per_graph {
            if batch.is_empty() {
                continue;
            }
            let seg = Segment::build(batch, false);
            self.push_segment(g_id, seg);
        }

        Ok(derived)
    }

    /// Bulk-apply many commits' flakes in a single pass (first-load / catch-up).
    ///
    /// Designed for paths like `LedgerState::load_novelty` walking a long commit
    /// chain. Rather than one segment per commit, it:
    /// 1. Routes every flake into a per-graph bucket in one ingest pass.
    /// 2. Folds in any pre-existing segments for each touched graph, sorts the
    ///    combined set once (parallel) by identity-then-t (`s, p, o, dt, m, t,
    ///    op`), and walks each `(s, p, o, dt, m)` group to apply set semantics
    ///    (drop an assertion whose prior kept flake for the same identity was
    ///    also an assertion; retractions always kept).
    /// 3. Emits ONE consolidated [`Segment`] per graph (good read locality for
    ///    freshly loaded novelty), replacing that graph's segment list.
    ///
    /// Total cost is `O(N log N)` over the merged set. `epoch` bumps once per
    /// call; `t` advances to `max(self.t, max_commit_t)`. The post-condition
    /// matches a sequential per-commit `apply_commit` chain.
    pub fn bulk_apply_commits<I>(
        &mut self,
        commit_batches: I,
        reverse_graph: &HashMap<Sid, GraphId>,
    ) -> Result<()>
    where
        I: IntoIterator<Item = (Vec<Flake>, i64)>,
    {
        use rayon::prelude::*;

        let span = tracing::debug_span!(
            "novelty_bulk_apply_commits",
            rayon_threads = rayon::current_num_threads()
        );
        let _guard = span.enter();

        let started = fluree_db_core::clock::Instant::now();

        // ---- Phase 1: partition incoming flakes by graph ----
        let mut per_graph: HashMap<GraphId, Vec<Flake>> = HashMap::new();
        let mut max_t = self.t;
        let mut commit_count: u64 = 0;
        let mut total_flakes: usize = 0;

        for (flakes, commit_t) in commit_batches {
            commit_count += 1;
            max_t = max_t.max(commit_t);
            total_flakes += flakes.len();
            for flake in flakes {
                let g_id = Self::resolve_flake_g_id(&flake, reverse_graph)?;
                if fluree_db_core::namespaces::is_rdfs_hierarchy_predicate(&flake.p) {
                    self.schema_epoch += 1;
                }
                if fluree_db_core::namespaces::is_shacl_affecting_flake(&flake) {
                    self.shacl_epoch += 1;
                }
                self.note_list_meta(&flake);
                self.has_lossy_index_terms |= flake_has_lossy_index_term(&flake);
                per_graph.entry(g_id).or_default().push(flake);
            }
        }

        let links_current = self.current_link_base().is_some();
        if let Some(base) = self.current_link_base() {
            let touched = links::touched_reifiers(
                per_graph
                    .iter()
                    .flat_map(|(g_id, flakes)| flakes.iter().map(move |f| (*g_id, f))),
            );
            if !touched.is_empty() {
                let links = self.derive_links(base, &touched, self.t)?;
                for (g_id, flake) in links {
                    per_graph.entry(g_id).or_default().push(flake);
                }
            }
        }
        if links_current {
            self.links_through = max_t;
        }

        if per_graph.is_empty() {
            self.t = max_t;
            self.epoch += 1;
            self.refresh_content_version();
            return Ok(());
        }

        // ---- Phase 2: per-graph fold-in existing + dedup + single segment ----
        let mut total_dedup: u64 = 0;

        for (g_id, mut new_flakes) in per_graph {
            // Fold in any existing segments for this graph so the dedup pass sees
            // the full universe, then replace them with one consolidated segment.
            let mut combined: Vec<Flake> = Vec::new();
            if let Some(slot) = self.graphs.get_mut(g_id) {
                if let Some(segs) = slot.take() {
                    for seg in segs {
                        match Arc::try_unwrap(seg) {
                            Ok(s) => combined.extend(s.flakes),
                            Err(shared) => combined.extend(shared.flakes.iter().cloned()),
                        }
                    }
                }
            }
            combined.append(&mut new_flakes);

            // Sort by (s, p, o, dt, m, t, op) so each identity forms a contiguous
            // t-ascending run.
            combined.par_sort_unstable_by(|a, b| {
                a.s.cmp(&b.s)
                    .then_with(|| a.p.cmp(&b.p))
                    .then_with(|| cmp_object(a, b))
                    .then_with(|| cmp_meta(a, b))
                    .then_with(|| a.t.cmp(&b.t))
                    .then_with(|| a.op.cmp(&b.op))
            });

            // Linear set-semantics dedup walk over the identity-sorted, owned
            // flakes. Mirrors the per-commit fact_state skip rule exactly:
            // retractions always kept; an assertion is dropped iff the prior kept
            // flake for the same identity was also an assertion.
            let mut kept: Vec<Flake> = Vec::with_capacity(combined.len());
            let mut iter = combined.into_iter();
            let mut current = iter.next();
            while let Some(head) = current {
                // First flake of an identity group is always kept.
                let mut asserted = head.op;
                kept.push(head);
                loop {
                    match iter.next() {
                        Some(f) => {
                            // `kept.last()` is this group's most recent kept flake
                            // (identities are contiguous after the sort).
                            if same_identity(kept.last().expect("just pushed"), &f) {
                                if !f.op {
                                    kept.push(f);
                                    asserted = false;
                                } else if !asserted {
                                    kept.push(f);
                                    asserted = true;
                                } else {
                                    total_dedup += 1;
                                }
                            } else {
                                current = Some(f);
                                break;
                            }
                        }
                        None => {
                            current = None;
                            break;
                        }
                    }
                }
            }

            // Maintain the current-state index so later apply_commit calls dedup
            // against bulk-loaded facts. `kept` is in (s,p,o,dt,m,t,op) order, so
            // the last record per identity is its highest-t (latest) op.
            for flake in &kept {
                self.saw_annotations |= fluree_db_core::is_annotation_predicate(&flake.p);
                self.fact_state.record(g_id, flake);
            }

            // Replace the graph with one consolidated segment (chunked only if it
            // somehow exceeds the local-index width).
            self.set_graph_segments(g_id, kept, true);

            // Keep the config-graph write marker current on the bulk path too
            // (mirrors `apply_commit`), so the resolved-config cache invalidates
            // after a bulk import that writes config.
            if g_id == CONFIG_GRAPH_ID {
                self.config_write_t = self.config_write_t.max(max_t);
            }
        }

        self.t = max_t;
        self.epoch += 1;
        self.refresh_content_version();
        self.recompute_totals();

        tracing::debug!(
            commits = commit_count,
            total_flakes,
            deduped = total_dedup,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "novelty bulk apply complete"
        );

        Ok(())
    }

    /// Clear flakes with t <= cutoff_t (after an index merge publishes them).
    ///
    /// Segments entirely at/below the cutoff are dropped wholesale; segments
    /// entirely above are kept untouched; a straddling segment is rebuilt from
    /// its surviving flakes.
    ///
    /// Note: In the standard Fluree indexing flow, Novelty is replaced entirely
    /// after each index rebuild rather than mutated in-place, so this is rarely
    /// the hot path.
    pub fn clear_up_to(&mut self, cutoff_t: i64) {
        // Commits at or before the cutoff have their links in the index now.
        self.trim_link_base(cutoff_t);
        self.links_through = self.links_through.max(cutoff_t.min(self.t));
        if self.flake_count == 0 {
            return;
        }

        for slot in self.graphs.iter_mut() {
            let Some(segs) = slot.as_mut() else { continue };
            let mut kept: Vec<Arc<Segment>> = Vec::with_capacity(segs.len());
            for seg in segs.drain(..) {
                if seg.max_t <= cutoff_t {
                    continue; // entirely stale
                }
                if seg.min_t > cutoff_t {
                    kept.push(seg); // entirely fresh
                    continue;
                }
                // Straddling: keep the survivors, in the orders they already have.
                if let Some(survivors) = seg.after(cutoff_t) {
                    kept.push(Arc::new(survivors));
                }
            }
            if kept.is_empty() {
                *slot = None;
            } else {
                *slot = Some(kept);
            }
        }

        self.recompute_totals();

        // Rebuild the current-state index from survivors. Per graph, record in
        // SPOT order (t-ascending within an identity) so the latest op wins.
        // slice_for_range returns an owned Vec, so the &self borrow ends before
        // the &mut fact_state writes.
        self.graphs.compact();
        let mut fs = NoveltyFactState::new();
        for g in self.graphs.present() {
            let ids = self.slice_for_range(g, IndexType::Spot, None, None, true);
            for id in ids {
                fs.record(g, self.get_flake(id));
            }
        }
        self.fact_state = fs;

        self.epoch += 1;
        self.refresh_content_version();
    }

    /// Comparator-ordered k-way merge over one graph's segments for `index` and
    /// the given range. The single primitive all reads build on.
    fn graph_merge(
        &self,
        g_id: GraphId,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
    ) -> GraphMergeIter<'_> {
        // Upper-bounded by the graph's segment count (some may zone-map prune),
        // so size both up front to avoid reallocation churn on the K>1 read path.
        let cap = self
            .graphs
            .get(g_id)
            .and_then(Option::as_ref)
            .map_or(0, Vec::len);
        let mut streams: Vec<MergeStream<'_>> = Vec::with_capacity(cap);
        let mut heap: BinaryHeap<MergeHead<'_>> = BinaryHeap::with_capacity(cap);
        if let Some(Some(segs)) = self.graphs.get(g_id) {
            for (seg_idx, seg) in segs.iter().enumerate() {
                // Zone-map prune: skip segments whose key range can't intersect
                // (first, rhs] without paying for the binary search.
                if !seg.may_overlap(index, first, rhs, leftmost) {
                    continue;
                }
                let order = seg.range(index, first, rhs, leftmost);
                if order.is_empty() {
                    continue;
                }
                let stream = streams.len();
                heap.push(MergeHead {
                    flake: &seg.flakes[order[0] as usize],
                    stream,
                    seg_idx,
                    index,
                });
                streams.push(MergeStream {
                    seg_idx,
                    order,
                    pos: 0,
                    flakes: &seg.flakes,
                });
            }
        }
        GraphMergeIter {
            slot: self.graphs.slot(g_id).unwrap_or(0),
            index,
            streams,
            heap,
        }
    }

    /// Graph ids (ascending) that currently hold at least one non-empty segment.
    fn present_graphs(&self) -> Vec<GraphId> {
        self.graphs.present()
    }

    /// Range read over one graph yielding `&Flake`. K=1 takes the no-heap,
    /// no-id-packing fast path (direct segment iteration ≈ old single-vector
    /// speed); K>1 uses the k-way merge.
    fn read_flakes(
        &self,
        g_id: GraphId,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
    ) -> FlakeRead<'_> {
        match self.graphs.get(g_id).and_then(Option::as_ref) {
            Some(segs) if segs.len() == 1 => {
                let seg = &segs[0];
                if seg.may_overlap(index, first, rhs, leftmost) {
                    FlakeRead::Single {
                        order: seg.range(index, first, rhs, leftmost).iter(),
                        flakes: &seg.flakes,
                    }
                } else {
                    FlakeRead::Done
                }
            }
            Some(segs) if !segs.is_empty() => {
                FlakeRead::Merge(self.graph_merge(g_id, index, first, rhs, leftmost))
            }
            _ => FlakeRead::Done,
        }
    }

    /// Range read over one graph yielding packed [`FlakeId`]s (compat id path).
    /// K=1 takes the no-heap fast path; K>1 uses the k-way merge.
    fn read_ids(
        &self,
        g_id: GraphId,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
    ) -> IdRead<'_> {
        match self.graphs.get(g_id).and_then(Option::as_ref) {
            Some(segs) if segs.len() == 1 => {
                let seg = &segs[0];
                if seg.may_overlap(index, first, rhs, leftmost) {
                    IdRead::Single {
                        slot: self
                            .graphs
                            .slot(g_id)
                            .expect("a graph with segments has a slot"),
                        seg_idx: 0,
                        order: seg.range(index, first, rhs, leftmost).iter(),
                    }
                } else {
                    IdRead::Done
                }
            }
            Some(segs) if !segs.is_empty() => {
                IdRead::Merge(self.graph_merge(g_id, index, first, rhs, leftmost))
            }
            _ => IdRead::Done,
        }
    }

    /// Get the ordered flake IDs for a graph's leaf range. Returns an owned `Vec`
    /// — there is no single backing slice once novelty is segmented; callers only
    /// `.iter()` it. Prefer [`Self::iter_flakes`] / `for_each_overlay_flake` for
    /// new code so no ids are materialized.
    ///
    /// **The returned [`FlakeId`]s are read-scoped**: they encode `(graph, seg,
    /// local)` against the *current* segment layout. Any mutation that rebuilds
    /// segments — [`Self::clear_up_to`], [`Self::tier_compact`],
    /// [`Self::bulk_apply_commits`] — invalidates them, after which
    /// [`Self::get_flake`] may panic or resolve a different flake. Resolve every
    /// id (via `get_flake`) before the next mutation.
    ///
    /// Semantics:
    /// - If leftmost=false: left boundary is EXCLUSIVE (> first)
    /// - If leftmost=true: no left boundary
    /// - rhs is INCLUSIVE when present
    ///
    /// Crate-internal: the owned ids it returns are read-scoped (see
    /// [`Self::get_flake`]). External readers use [`Self::range_flakes`].
    #[must_use]
    pub(crate) fn slice_for_range(
        &self,
        g_id: GraphId,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
    ) -> Vec<FlakeId> {
        self.read_ids(g_id, index, first, rhs, leftmost).collect()
    }

    /// Range read over one graph yielding `&Flake` directly — the borrow-friendly
    /// counterpart to [`Self::slice_for_range`]. Prefer this when the caller only
    /// iterates flakes: it materializes no [`FlakeId`] `Vec` and skips the
    /// per-id [`Self::get_flake`] indirection. K=1 takes the no-heap fast path;
    /// K>1 uses the k-way merge. Same comparator order and boundary semantics as
    /// [`Self::slice_for_range`].
    pub fn range_flakes(
        &self,
        g_id: GraphId,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
    ) -> impl Iterator<Item = &Flake> + '_ {
        self.read_flakes(g_id, index, first, rhs, leftmost)
    }

    /// Resolve a [`FlakeId`] produced by this novelty back to its flake.
    ///
    /// Crate-internal on purpose: a `FlakeId` is read-scoped (it encodes the
    /// current segment layout), so resolving one after a segment-rebuilding
    /// mutation would panic or return the wrong flake. Keeping the resolver
    /// `pub(crate)` makes that hazard unreachable from other crates — external
    /// readers use the borrow-safe [`Self::range_flakes`] / [`Self::iter_flakes`]
    /// / `for_each_overlay_flake` which yield `&Flake` directly.
    pub(crate) fn get_flake(&self, id: FlakeId) -> &Flake {
        let segs = self
            .graphs
            .by_slot(id.graph())
            .as_ref()
            .expect("FlakeId references a live graph");
        &segs[id.seg()].flakes[id.local()]
    }

    /// Get the number of live flakes in novelty.
    pub fn len(&self) -> usize {
        self.flake_count
    }

    /// Check if novelty holds no flakes.
    pub fn is_empty(&self) -> bool {
        self.flake_count == 0
    }

    /// Iterate all flake IDs for `index` across ALL graphs, comparator-ordered
    /// per graph. IDs are read-scoped — they encode the current segment layout,
    /// so any segment-rebuilding mutation ([`Self::clear_up_to`],
    /// [`Self::tier_compact`], [`Self::bulk_apply_commits`]) invalidates them;
    /// resolve via [`Self::get_flake`] before the next mutation. Prefer
    /// [`Self::iter_flakes`] for new code (no id packing).
    pub fn iter_index(&self, index: IndexType) -> impl Iterator<Item = FlakeId> + '_ {
        self.present_graphs()
            .into_iter()
            .flat_map(move |g| self.read_ids(g, index, None, None, true))
    }

    /// Iterate all flakes for `index` across ALL graphs, comparator-ordered per
    /// graph. The hot full-scan path (stats, dict rebuilds): K=1 graphs iterate
    /// their segment directly with no heap and no id packing.
    pub fn iter_flakes(&self, index: IndexType) -> impl Iterator<Item = &Flake> + '_ {
        self.present_graphs()
            .into_iter()
            .flat_map(move |g| self.read_flakes(g, index, None, None, true))
    }
}

impl std::fmt::Debug for Novelty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let segments: usize = self.graphs.lists().map(Vec::len).sum();
        f.debug_struct("Novelty")
            .field("flake_count", &self.flake_count)
            .field("graphs", &self.graphs.lists().count())
            .field("segments", &segments)
            .field("size", &self.size)
            .field("t", &self.t)
            .field("epoch", &self.epoch)
            .finish()
    }
}

// === OverlayProvider implementation ===

use fluree_db_core::OverlayProvider;

impl OverlayProvider for Novelty {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn list_predicates(&self) -> Option<std::sync::Arc<std::collections::HashSet<Sid>>> {
        Some(std::sync::Arc::clone(&self.list_predicates))
    }

    fn epoch(&self) -> u64 {
        self.epoch
    }

    fn is_effectively_empty(&self) -> bool {
        // Segments only ever hold live flakes (`clear_up_to` and tiered
        // compaction rebuild from survivors), so "no live bytes" (`size == 0`)
        // and "no flakes" coincide — just defer to `is_empty()`.
        self.is_empty()
    }

    fn content_version(&self) -> Option<u64> {
        Some(self.content_version)
    }

    fn for_each_overlay_flake(
        &self,
        g_id: GraphId,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
        to_t: i64,
        callback: &mut dyn FnMut(&Flake),
    ) {
        // K=1 (the common steady state) takes a raw slice loop — no iterator/
        // enum dispatch per flake — recovering the old single-vector read speed
        // on this query hot path. K>1 k-way merges. Applies the to_t filter.
        match self.graphs.get(g_id).and_then(Option::as_ref) {
            Some(segs) if segs.len() == 1 => {
                let seg = &segs[0];
                if seg.may_overlap(index, first, rhs, leftmost) {
                    let flakes = &seg.flakes;
                    for &local in seg.range(index, first, rhs, leftmost) {
                        let flake = &flakes[local as usize];
                        if flake.t <= to_t {
                            callback(flake);
                        }
                    }
                }
            }
            Some(segs) if !segs.is_empty() => {
                for (_, flake) in self.graph_merge(g_id, index, first, rhs, leftmost) {
                    if flake.t <= to_t {
                        callback(flake);
                    }
                }
            }
            _ => {}
        }
    }

    fn overlay_flake_count(&self, g_id: GraphId) -> Option<usize> {
        Some(
            self.graphs
                .get(g_id)
                .and_then(Option::as_ref)
                .map_or(0, |segs| segs.iter().map(|s| s.flakes.len()).sum()),
        )
    }

    fn overlay_graph_ids(&self) -> Option<Vec<GraphId>> {
        Some(
            self.graphs
                .slots
                .iter()
                .filter(|(_, segs)| segs.as_ref().is_some_and(|segs| !segs.is_empty()))
                .map(|&(g_id, _)| g_id)
                .collect(),
        )
    }

    fn overlay_segments(&self, g_id: GraphId) -> Vec<fluree_db_core::OverlaySegmentMeta> {
        match self.graphs.get(g_id).and_then(Option::as_ref) {
            Some(segs) => segs
                .iter()
                .map(|s| fluree_db_core::OverlaySegmentMeta {
                    seg_id: s.seg_id,
                    min_t: s.min_t,
                    max_t: s.max_t,
                })
                .collect(),
            None => Vec::new(),
        }
    }

    fn for_each_overlay_segment_flake(
        &self,
        g_id: GraphId,
        seg_id: u64,
        seg_idx: usize,
        index: IndexType,
        callback: &mut dyn FnMut(&Flake),
    ) {
        // Whole segment in `index` order, NO `to_t` filter: the query layer
        // caches the segment's full translation and applies `to_t` + the cursor
        // key window after the k-way merge.
        //
        // `seg_idx` is the position from `overlay_segments` (same immutable
        // snapshot the caller enumerated), so index directly — O(1) instead of a
        // linear `seg_id` scan per segment (avoids O(K²) over a K-segment graph).
        if let Some(Some(segs)) = self.graphs.get(g_id) {
            if let Some(seg) = segs.get(seg_idx) {
                debug_assert_eq!(
                    seg.seg_id, seg_id,
                    "seg_idx/seg_id misaligned — caller mixed overlay snapshots"
                );
                let flakes = &seg.flakes;
                for &local in seg.order(index) {
                    callback(&flakes[local as usize]);
                }
            }
        }
    }
}

// =============================================================================
// Identity helpers for the bulk-apply dedup walk
// =============================================================================

/// Compare two flakes by their object value (datatype-aware).
///
/// Mirrors the hidden `cmp_object` in `fluree_db_core::comparator` so the
/// bulk-apply identity sort can group flakes by `(s, p, o, dt, m)` without
/// reaching into core's private comparators.
fn cmp_object(f1: &Flake, f2: &Flake) -> Ordering {
    f1.o.cmp(&f2.o).then_with(|| f1.dt.cmp(&f2.dt))
}

/// Compare two flakes by their metadata (None < Some, then m1 < m2).
///
/// Mirrors `fluree_db_core::comparator::cmp_meta` for the same reason as
/// [`cmp_object`].
fn cmp_meta(f1: &Flake, f2: &Flake) -> Ordering {
    match (&f1.m, &f2.m) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(m1), Some(m2)) => m1.cmp(m2),
    }
}

/// True iff `a` and `b` have identical `(s, p, o, dt, m)` — the fact
/// identity used by [`Novelty::apply_commit`]'s `fact_currently_asserted_in_graph`
/// dedup rule and the [`Novelty::bulk_apply_commits`] dedup walk.
fn same_identity(a: &Flake, b: &Flake) -> bool {
    a.s == b.s
        && a.p == b.p
        && cmp_object(a, b) == Ordering::Equal
        && cmp_meta(a, b) == Ordering::Equal
}

/// Size-tier class of a segment with `count` flakes: `floor(log_tier_width(count))`.
/// Segments of comparable size share a class, and merging `tier_width` segments of
/// class `K` yields one segment of class `K+1` — the invariant
/// [`Novelty::tier_compact`] relies on. Derived from flake count, so no per-segment
/// level needs to be stored or threaded through builds/merges.
fn size_class(count: usize, tier_width: usize) -> u32 {
    if count <= 1 || tier_width < 2 {
        return 0;
    }
    let mut class = 0u32;
    let mut bound = tier_width;
    while count >= bound {
        class += 1;
        match bound.checked_mul(tier_width) {
            Some(b) => bound = b,
            None => break,
        }
    }
    class
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeMeta, FlakeValue, Sid};

    /// Empty reverse_graph — all flakes go to default graph (g_id=0)
    fn no_graphs() -> HashMap<Sid, GraphId> {
        HashMap::new()
    }

    fn make_flake(s: u16, p: u16, o: i64, t: i64, op: bool) -> Flake {
        Flake::new(
            Sid::new(s, format!("s{s}")),
            Sid::new(p, format!("p{p}")),
            FlakeValue::Long(o),
            Sid::new(2, "long"),
            t,
            op,
            None,
        )
    }

    fn make_flake_with_meta(
        s: u16,
        p: u16,
        o: i64,
        t: i64,
        op: bool,
        m: Option<FlakeMeta>,
    ) -> Flake {
        Flake::new(
            Sid::new(s, format!("s{s}")),
            Sid::new(p, format!("p{p}")),
            FlakeValue::Long(o),
            Sid::new(2, "long"),
            t,
            op,
            m,
        )
    }

    fn make_ref_flake(s: u16, p: u16, o_sid: u16, t: i64) -> Flake {
        Flake::new(
            Sid::new(s, format!("s{s}")),
            Sid::new(p, format!("p{p}")),
            FlakeValue::Ref(Sid::new(o_sid, format!("s{o_sid}"))),
            Sid::new(1, "id"), // $id datatype marks it as a ref
            t,
            true,
            None,
        )
    }

    /// Make a flake assigned to a named graph via its `g` field
    fn make_graph_flake(s: u16, p: u16, o: i64, t: i64, g_sid: Sid) -> Flake {
        let mut f = Flake::new(
            Sid::new(s, format!("s{s}")),
            Sid::new(p, format!("p{p}")),
            FlakeValue::Long(o),
            Sid::new(2, "long"),
            t,
            true,
            None,
        );
        f.g = Some(g_sid);
        f
    }

    #[test]
    fn test_novelty_new() {
        let novelty = Novelty::new(5);
        assert_eq!(novelty.t, 5);
        assert_eq!(novelty.epoch, 0);
        assert_eq!(novelty.size, 0);
        assert!(novelty.is_empty());
    }

    /// Novelty records an assertion of a term its index key keeps less of (a
    /// big integer written with an XSD subtype), and only an assertion: a
    /// retraction, or a term the key holds whole (a double, `±INF` and NaN
    /// included), leaves the flag clear. Both apply paths set it.
    #[test]
    fn a_lossy_term_assertion_sets_the_flag() {
        let big = |dt: &str, t: i64, op: bool| {
            Flake::new(
                Sid::new(100, "s"),
                Sid::new(100, "p"),
                FlakeValue::BigInt(Box::new("123456789012345678901234567890".parse().unwrap())),
                Sid::new(fluree_vocab::namespaces::XSD, dt),
                t,
                op,
                None,
            )
        };
        let exact = "integer";
        let lossy = "nonNegativeInteger";
        let double = |d: f64| {
            Flake::new(
                Sid::new(100, "s"),
                Sid::new(100, "d"),
                FlakeValue::Double(d),
                Sid::new(fluree_vocab::namespaces::XSD, "double"),
                1,
                true,
                None,
            )
        };

        let mut novelty = Novelty::new(0);
        novelty
            .apply_commit(
                vec![
                    big(exact, 1, true),
                    big(lossy, 1, false),
                    double(f64::INFINITY),
                    double(f64::NEG_INFINITY),
                    double(f64::NAN),
                ],
                1,
                &no_graphs(),
            )
            .unwrap();
        assert!(!novelty.has_lossy_index_terms);
        novelty
            .apply_commit(vec![big(lossy, 2, true)], 2, &no_graphs())
            .unwrap();
        assert!(novelty.has_lossy_index_terms);

        let mut bulk = Novelty::new(0);
        bulk.bulk_apply_commits(vec![(vec![big(lossy, 1, true)], 1)], &no_graphs())
            .unwrap();
        assert!(bulk.has_lossy_index_terms);
    }

    /// apply_commit must be atomic: a graph-routing error part-way through a
    /// batch must leave novelty completely untouched (t / epoch / size / arena),
    /// so callers that mutate a cache-shared Novelty in place via Arc::make_mut
    /// can't poison live state. Regression guard for the clone-elimination work.
    #[test]
    fn apply_commit_atomic_on_routing_error() {
        let mut novelty = Novelty::new(0);
        novelty
            .apply_commit(vec![make_flake(1, 1, 1, 1, true)], 1, &no_graphs())
            .expect("first commit applies");
        let (t, epoch, size, len) = (novelty.t, novelty.epoch, novelty.size, novelty.len());

        // Batch with a good flake followed by one referencing an unknown named
        // graph: the old in-place code would have bumped t/epoch and pushed the
        // good flake before erroring on the second.
        let good = make_flake(2, 2, 2, 2, true);
        let bad = make_graph_flake(3, 3, 3, 2, Sid::new(9, "g-unknown"));
        let err = novelty.apply_commit(vec![good, bad], 2, &no_graphs());

        assert!(err.is_err(), "unknown graph Sid must error");
        assert_eq!(novelty.t, t, "t unchanged after failed apply");
        assert_eq!(novelty.epoch, epoch, "epoch unchanged after failed apply");
        assert_eq!(novelty.size, size, "size unchanged after failed apply");
        assert_eq!(novelty.len(), len, "no flakes added after failed apply");

        // can_apply reports the same routing failure without mutating.
        assert!(novelty
            .can_apply(
                &[make_graph_flake(4, 4, 4, 3, Sid::new(9, "g-unknown"))],
                &no_graphs()
            )
            .is_err());
        assert_eq!(novelty.len(), len, "can_apply does not mutate");
    }

    #[test]
    fn test_apply_commit_single() {
        let mut novelty = Novelty::new(0);

        let flakes = vec![
            make_flake(1, 1, 100, 1, true),
            make_flake(2, 1, 200, 1, true),
        ];

        novelty.apply_commit(flakes, 1, &no_graphs()).unwrap();

        assert_eq!(novelty.len(), 2);
        assert_eq!(novelty.t, 1);
        assert_eq!(novelty.epoch, 1); // Epoch bumped once
        assert!(novelty.size > 0);
    }

    #[test]
    fn content_version_is_globally_unique_across_divergent_clones() {
        let mut a = Novelty::new(0);
        assert_eq!(
            OverlayProvider::content_version(&a),
            Some(0),
            "empty-since-construction novelty reports version 0"
        );

        let rg = no_graphs();
        a.apply_commit(vec![make_flake(1, 1, 100, 1, true)], 1, &rg)
            .unwrap();
        let v_a1 = OverlayProvider::content_version(&a).unwrap();
        assert_ne!(v_a1, 0, "mutation stamps a fresh version");

        // A clone shares the version while content is identical.
        let mut b = a.clone();
        assert_eq!(OverlayProvider::content_version(&b), Some(v_a1));

        // Divergent mutations from the same base must never share a version
        // (per-instance `epoch` DOES collide here: both go 1 → 2).
        a.apply_commit(vec![make_flake(2, 1, 200, 2, true)], 2, &rg)
            .unwrap();
        b.apply_commit(vec![make_flake(3, 1, 300, 2, true)], 2, &rg)
            .unwrap();
        assert_eq!(a.epoch, b.epoch, "epochs collide across divergent clones");
        let v_a2 = OverlayProvider::content_version(&a).unwrap();
        let v_b2 = OverlayProvider::content_version(&b).unwrap();
        assert_ne!(v_a2, v_b2, "content versions must not collide");
        assert_ne!(v_a2, v_a1);
        assert_ne!(v_b2, v_a1);

        // clear_up_to changes content, so it must also refresh the version.
        let before_clear = OverlayProvider::content_version(&a).unwrap();
        a.clear_up_to(1);
        assert_ne!(OverlayProvider::content_version(&a).unwrap(), before_clear);
    }

    #[test]
    fn test_apply_commit_multiple() {
        let mut novelty = Novelty::new(0);
        let rg = no_graphs();

        // First commit
        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 1, true)], 1, &rg)
            .unwrap();
        assert_eq!(novelty.epoch, 1);

        // Second commit
        novelty
            .apply_commit(vec![make_flake(2, 1, 200, 2, true)], 2, &rg)
            .unwrap();
        assert_eq!(novelty.epoch, 2); // Epoch bumped once per commit

        assert_eq!(novelty.len(), 2);
        assert_eq!(novelty.t, 2);
    }

    #[test]
    fn test_apply_commit_skips_duplicate_assertions_across_commits() {
        let mut novelty = Novelty::new(0);
        let rg = no_graphs();

        // Assert once
        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 1, true)], 1, &rg)
            .unwrap();
        assert_eq!(novelty.len(), 1);

        // Re-assert same fact (different t) -> should be skipped
        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 2, true)], 2, &rg)
            .unwrap();
        assert_eq!(novelty.len(), 1);
    }

    #[test]
    fn test_apply_commit_allows_reassert_after_retract() {
        let mut novelty = Novelty::new(0);
        let rg = no_graphs();

        // Assert -> retract -> re-assert
        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 1, true)], 1, &rg)
            .unwrap();
        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 2, false)], 2, &rg)
            .unwrap();
        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 3, true)], 3, &rg)
            .unwrap();

        // Retractions are always stored; the final assertion must not be deduped away.
        assert_eq!(novelty.len(), 3);
    }

    #[test]
    fn test_apply_commit_does_not_dedup_distinct_metadata() {
        let mut novelty = Novelty::new(0);
        let rg = no_graphs();

        // Same (s,p,o,dt) but different list index metadata -> distinct facts.
        novelty
            .apply_commit(
                vec![make_flake_with_meta(
                    1,
                    1,
                    100,
                    1,
                    true,
                    Some(FlakeMeta::with_index(1)),
                )],
                1,
                &rg,
            )
            .unwrap();
        novelty
            .apply_commit(
                vec![make_flake_with_meta(
                    1,
                    1,
                    100,
                    2,
                    true,
                    Some(FlakeMeta::with_index(2)),
                )],
                2,
                &rg,
            )
            .unwrap();

        assert_eq!(novelty.len(), 2);

        // Re-assert the second meta variant -> should be deduped
        novelty
            .apply_commit(
                vec![make_flake_with_meta(
                    1,
                    1,
                    100,
                    3,
                    true,
                    Some(FlakeMeta::with_index(2)),
                )],
                3,
                &rg,
            )
            .unwrap();

        assert_eq!(novelty.len(), 2);
    }

    #[test]
    fn test_apply_commit_empty() {
        let mut novelty = Novelty::new(0);
        novelty.apply_commit(vec![], 1, &no_graphs()).unwrap();

        // Empty commit should not bump epoch
        assert_eq!(novelty.epoch, 0);
    }

    #[test]
    fn test_spot_ordering() {
        let mut novelty = Novelty::new(0);

        // Add flakes with different subjects
        let flakes = vec![
            make_flake(3, 1, 100, 1, true),
            make_flake(1, 1, 100, 1, true),
            make_flake(2, 1, 100, 1, true),
        ];

        novelty.apply_commit(flakes, 1, &no_graphs()).unwrap();

        // SPOT should order by subject
        let spot_ids: Vec<FlakeId> = novelty.iter_index(IndexType::Spot).collect();
        assert_eq!(spot_ids.len(), 3);

        let s1 = novelty.get_flake(spot_ids[0]).s.namespace_code;
        let s2 = novelty.get_flake(spot_ids[1]).s.namespace_code;
        let s3 = novelty.get_flake(spot_ids[2]).s.namespace_code;

        assert!(s1 <= s2 && s2 <= s3);
    }

    #[test]
    fn test_psot_ordering() {
        let mut novelty = Novelty::new(0);

        // Add flakes with different predicates
        let flakes = vec![
            make_flake(1, 3, 100, 1, true),
            make_flake(1, 1, 100, 1, true),
            make_flake(1, 2, 100, 1, true),
        ];

        novelty.apply_commit(flakes, 1, &no_graphs()).unwrap();

        // PSOT should order by predicate first
        let psot_ids: Vec<FlakeId> = novelty.iter_index(IndexType::Psot).collect();
        assert_eq!(psot_ids.len(), 3);

        let p1 = novelty.get_flake(psot_ids[0]).p.namespace_code;
        let p2 = novelty.get_flake(psot_ids[1]).p.namespace_code;
        let p3 = novelty.get_flake(psot_ids[2]).p.namespace_code;

        assert!(p1 <= p2 && p2 <= p3);
    }

    #[test]
    fn test_opst_all_object_types() {
        let mut novelty = Novelty::new(0);

        // Add mixed flakes - refs and non-refs
        let flakes = vec![
            make_flake(1, 1, 100, 1, true), // not a ref (Long)
            make_ref_flake(2, 1, 10, 1),    // ref
            make_flake(3, 1, 200, 1, true), // not a ref (Long)
            make_ref_flake(4, 1, 5, 1),     // ref
        ];

        novelty.apply_commit(flakes, 1, &no_graphs()).unwrap();

        // OPST should contain ALL flakes, not just refs
        let opst_ids: Vec<FlakeId> = novelty.iter_index(IndexType::Opst).collect();
        assert_eq!(opst_ids.len(), 4);
    }

    #[test]
    fn test_slice_for_range_basic() {
        let mut novelty = Novelty::new(0);

        let flakes = vec![
            make_flake(1, 1, 100, 1, true),
            make_flake(2, 1, 100, 1, true),
            make_flake(3, 1, 100, 1, true),
            make_flake(4, 1, 100, 1, true),
            make_flake(5, 1, 100, 1, true),
        ];

        novelty.apply_commit(flakes, 1, &no_graphs()).unwrap();

        // Full range (leftmost, no rhs) — default graph
        let slice = novelty.slice_for_range(0, IndexType::Spot, None, None, true);
        assert_eq!(slice.len(), 5);

        // From subject 2 (exclusive) to end
        let first = make_flake(2, 1, 100, 1, true);
        let slice = novelty.slice_for_range(0, IndexType::Spot, Some(&first), None, false);
        // Should get subjects 3, 4, 5 (> 2)
        assert_eq!(slice.len(), 3);

        // Absent graph returns empty slice
        let slice = novelty.slice_for_range(99, IndexType::Spot, None, None, true);
        assert!(slice.is_empty());
    }

    #[test]
    fn test_slice_for_range_with_rhs() {
        let mut novelty = Novelty::new(0);

        let flakes = vec![
            make_flake(1, 1, 100, 1, true),
            make_flake(2, 1, 100, 1, true),
            make_flake(3, 1, 100, 1, true),
            make_flake(4, 1, 100, 1, true),
            make_flake(5, 1, 100, 1, true),
        ];

        novelty.apply_commit(flakes, 1, &no_graphs()).unwrap();

        // From leftmost to subject 3 (inclusive) — default graph
        let rhs = make_flake(3, 1, 100, 1, true);
        let slice = novelty.slice_for_range(0, IndexType::Spot, None, Some(&rhs), true);
        // Should get subjects 1, 2, 3 (<= 3)
        assert_eq!(slice.len(), 3);
    }

    /// [`Segment::range`]'s exclusive lower bound is the third site #1711
    /// reached, and the only one no other test in this crate covers: the
    /// `partition_point` skips every flake the ordering calls `<= first`, so a
    /// sibling the comparator wrongly tied with `first` was skipped along with
    /// it. Here `first` is the `@en` flake at list position 0 and `@fr` at the
    /// same position is its sibling — pre-fix the seek returns `[]`.
    ///
    /// The two in-repo callers that pass `leftmost: false` today both build a
    /// synthetic bound with `m: None` and `t: i64::MIN`
    /// (`fluree-db-query`'s `predicate_walk_bounds` / `overlay_walk_bounds`),
    /// and `t` is compared before the metadata tiebreak — so this hazard is
    /// **latent** through today's callers rather than live. It is pinned
    /// anyway because [`Novelty::range_flakes`] is `pub` and takes an
    /// arbitrary `first`: the seek has to be correct on its own terms, not on
    /// its callers'.
    ///
    /// Deliberately one commit, so `fact_state`'s dedup (updated only *after*
    /// the accept loop) never sees either flake. That keeps this assertion
    /// independent of the `FactKey` / `same_identity` route the other #1711
    /// regression tests exercise — it fails for a different reason than they do.
    #[test]
    fn exclusive_seek_past_a_tagged_bound_keeps_its_sibling_tag() {
        let tagged = |lang: &str| {
            make_flake_with_meta(
                1,
                1,
                100,
                1,
                true,
                Some(FlakeMeta {
                    lang: Some(lang.to_string()),
                    i: Some(0),
                }),
            )
        };
        let en = tagged("en");
        let fr = tagged("fr");

        let mut novelty = Novelty::new(0);
        novelty
            .apply_commit(vec![en.clone(), fr.clone()], 1, &no_graphs())
            .unwrap();
        assert_eq!(
            novelty.len(),
            2,
            "both language tags must land in the segment for the seek to be the thing under test"
        );

        let ids = novelty.slice_for_range(0, IndexType::Spot, Some(&en), None, false);
        let langs: Vec<Option<String>> = ids
            .iter()
            .map(|&id| {
                novelty
                    .get_flake(id)
                    .m
                    .as_ref()
                    .and_then(|m| m.lang.clone())
            })
            .collect();
        assert_eq!(
            langs,
            vec![Some("fr".to_string())],
            "seeking strictly past `@en` at list position 0 must still yield its `@fr` sibling"
        );
    }

    #[test]
    fn test_clear_up_to() {
        let mut novelty = Novelty::new(0);
        let rg = no_graphs();

        // Add flakes at different times
        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 1, true)], 1, &rg)
            .unwrap();
        novelty
            .apply_commit(vec![make_flake(2, 1, 100, 2, true)], 2, &rg)
            .unwrap();
        novelty
            .apply_commit(vec![make_flake(3, 1, 100, 3, true)], 3, &rg)
            .unwrap();

        let initial_size = novelty.size;
        let initial_epoch = novelty.epoch;

        // Clear up to t=1 (should remove flake at t=1)
        novelty.clear_up_to(1);

        // Should have 2 flakes in spot index (t=2 and t=3)
        let remaining: Vec<FlakeId> = novelty.iter_index(IndexType::Spot).collect();
        assert_eq!(remaining.len(), 2);

        // Size should be reduced
        assert!(novelty.size < initial_size);

        // Epoch should be bumped
        assert_eq!(novelty.epoch, initial_epoch + 1);
    }

    #[test]
    fn test_is_effectively_empty_after_full_clear() {
        let mut novelty = Novelty::new(0);
        let rg = no_graphs();

        assert!(novelty.is_effectively_empty());

        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 1, true)], 1, &rg)
            .unwrap();
        assert!(!novelty.is_effectively_empty());

        // Drain everything, as an index swap does (apply_index/apply_loaded_db)
        novelty.clear_up_to(1);

        // Segmented novelty drops whole segments on clear (no dead-flake arena),
        // so a full drain genuinely empties it: `is_empty()` and
        // `is_effectively_empty()` agree. The point being pinned is that the
        // drained state reads as effectively-empty *despite the bumped epoch* —
        // that is the gate the post-indexing fast path relies on.
        assert!(novelty.is_empty());
        assert!(novelty.epoch > 0);
        assert!(novelty.is_effectively_empty());
    }

    #[test]
    fn test_merge_preserves_order() {
        let mut novelty = Novelty::new(0);
        let rg = no_graphs();

        // First batch
        novelty
            .apply_commit(
                vec![
                    make_flake(1, 1, 100, 1, true),
                    make_flake(3, 1, 100, 1, true),
                    make_flake(5, 1, 100, 1, true),
                ],
                1,
                &rg,
            )
            .unwrap();

        // Second batch - interleaved subjects
        novelty
            .apply_commit(
                vec![
                    make_flake(2, 1, 100, 2, true),
                    make_flake(4, 1, 100, 2, true),
                ],
                2,
                &rg,
            )
            .unwrap();

        // Check SPOT ordering
        let spot_ids: Vec<FlakeId> = novelty.iter_index(IndexType::Spot).collect();
        assert_eq!(spot_ids.len(), 5);

        // Verify sorted order
        for i in 0..spot_ids.len() - 1 {
            let cmp = IndexType::Spot.compare(
                novelty.get_flake(spot_ids[i]),
                novelty.get_flake(spot_ids[i + 1]),
            );
            assert_ne!(
                cmp,
                Ordering::Greater,
                "SPOT index not sorted at position {i}"
            );
        }
    }

    /// Graph ids past 16 bits live in novelty by slot: reads, packed ids and
    /// a clear that drops a graph's slot all keep each graph's flakes apart.
    #[test]
    fn graph_ids_past_16_bits_read_back_by_slot() {
        let mut novelty = Novelty::new(0);
        let (wide, wider) = (Sid::new(100, "wide"), Sid::new(100, "wider"));
        let mut rg = HashMap::new();
        rg.insert(wide.clone(), 70_000u32);
        rg.insert(wider.clone(), 3_000_000u32);
        novelty
            .apply_commit(
                vec![
                    make_graph_flake(1, 1, 10, 1, wide.clone()),
                    make_graph_flake(2, 1, 20, 1, wider.clone()),
                    make_flake(3, 1, 30, 1, true),
                ],
                1,
                &rg,
            )
            .unwrap();
        novelty
            .apply_commit(vec![make_graph_flake(4, 1, 40, 2, wider.clone())], 2, &rg)
            .unwrap();

        let objects = |n: &Novelty, g: GraphId| -> Vec<i64> {
            n.slice_for_range(g, IndexType::Spot, None, None, true)
                .into_iter()
                .map(|id| match n.get_flake(id).o {
                    FlakeValue::Long(v) => v,
                    ref other => panic!("{other:?}"),
                })
                .collect()
        };
        assert_eq!(objects(&novelty, 70_000), vec![10]);
        assert_eq!(objects(&novelty, 3_000_000), vec![20, 40]);
        assert_eq!(objects(&novelty, 0), vec![30]);
        assert_eq!(novelty.present_graphs(), vec![0, 70_000, 3_000_000]);

        // Graph 70,000 empties at t=1; its slot goes, the others shift.
        novelty.clear_up_to(1);
        assert_eq!(novelty.present_graphs(), vec![3_000_000]);
        assert_eq!(objects(&novelty, 3_000_000), vec![40]);
        assert!(objects(&novelty, 70_000).is_empty());
    }

    #[test]
    fn overlay_graph_ids_lists_the_graphs_holding_flakes() {
        let mut novelty = Novelty::new(0);
        assert_eq!(novelty.overlay_graph_ids(), Some(vec![]));
        let (g2, g5) = (Sid::new(100, "graph2"), Sid::new(100, "graph5"));
        let rg: HashMap<Sid, GraphId> = [(g2.clone(), 2u32), (g5.clone(), 5u32)].into();
        novelty
            .apply_commit(
                vec![
                    make_graph_flake(10, 1, 300, 1, g5),
                    make_graph_flake(11, 1, 400, 1, g2),
                ],
                1,
                &rg,
            )
            .unwrap();
        let mut graphs = novelty.overlay_graph_ids().unwrap();
        graphs.sort_unstable();
        assert_eq!(graphs, vec![2, 5]);
        novelty.clear_up_to(1);
        assert_eq!(novelty.overlay_graph_ids(), Some(vec![]));
    }

    #[test]
    fn test_per_graph_isolation() {
        let mut novelty = Novelty::new(0);

        // Set up: graph 2 mapped to Sid("g", "graph2")
        let g2_sid = Sid::new(100, "graph2");
        let mut rg = HashMap::new();
        rg.insert(g2_sid.clone(), 2u32);

        // Default graph flakes (flake.g = None)
        let default_flakes = vec![
            make_flake(1, 1, 100, 1, true),
            make_flake(2, 1, 200, 1, true),
        ];

        // Named graph flakes (flake.g = Some(g2_sid))
        let named_flakes = vec![
            make_graph_flake(10, 1, 300, 1, g2_sid.clone()),
            make_graph_flake(11, 1, 400, 1, g2_sid.clone()),
            make_graph_flake(12, 1, 500, 1, g2_sid.clone()),
        ];

        let mut all = default_flakes;
        all.extend(named_flakes);
        novelty.apply_commit(all, 1, &rg).unwrap();

        // Default graph (g_id=0) should have 2 flakes
        let g0_slice = novelty.slice_for_range(0, IndexType::Spot, None, None, true);
        assert_eq!(g0_slice.len(), 2);

        // Named graph (g_id=2) should have 3 flakes
        let g2_slice = novelty.slice_for_range(2, IndexType::Spot, None, None, true);
        assert_eq!(g2_slice.len(), 3);

        // Non-existent graph returns empty
        let g99_slice = novelty.slice_for_range(99, IndexType::Spot, None, None, true);
        assert!(g99_slice.is_empty());

        // iter_index returns ALL flakes across graphs
        let all_spot: Vec<FlakeId> = novelty.iter_index(IndexType::Spot).collect();
        assert_eq!(all_spot.len(), 5);
    }

    #[test]
    fn test_unknown_graph_sid_errors() {
        let mut novelty = Novelty::new(0);
        let rg = no_graphs(); // No named graphs registered

        // Flake with a graph Sid that isn't in reverse_graph
        let unknown_g = Sid::new(200, "unknown");
        let flakes = vec![make_graph_flake(1, 1, 100, 1, unknown_g)];

        let result = novelty.apply_commit(flakes, 1, &rg);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("unknown graph Sid"), "got: {err_msg}");
    }

    /// Drift guard: the file-local `cmp_object` / `cmp_meta` /
    /// `same_identity` helpers exist because `fluree_db_core::comparator`'s
    /// equivalents are private. They MUST stay byte-for-byte consistent
    /// with the `(s, p, o, dt, t, op, m)` ordering encoded in
    /// `IndexType::Spot.compare`, since [`Novelty::bulk_apply_commits`]'s
    /// set-semantics dedup depends on `same_identity` matching the
    /// identity used by `fact_currently_asserted_in_graph` in
    /// [`Novelty::apply_commit`].
    ///
    /// If either side ever drifts, this test fires loudly. Symptoms of a
    /// silent drift would be silently dropped or duplicated assertions
    /// during first-load catch-up — extremely hard to track down at
    /// runtime — so a deterministic compile-and-run guard is worth it.
    #[test]
    fn local_identity_helpers_match_core_spot_comparator_semantics() {
        use fluree_db_core::IndexType;

        // Two flakes that share `(s, p, o, dt, m)` but differ in `(t, op)`
        // — `same_identity` must say `true`, and SPOT comparator must
        // disagree only on the t/op tail.
        let id_a = make_flake(101, 200, 42, 1, true);
        let id_b = make_flake(101, 200, 42, 5, false);
        assert!(
            same_identity(&id_a, &id_b),
            "same (s, p, o, dt, m) must be one identity"
        );
        let cmp = IndexType::Spot.compare(&id_a, &id_b);
        assert_eq!(
            cmp,
            Ordering::Less,
            "within an identity group, SPOT must order by ascending t"
        );

        // Differing on each prefix component must break identity.
        let other_s = make_flake(102, 200, 42, 1, true);
        let other_p = make_flake(101, 201, 42, 1, true);
        let other_o = make_flake(101, 200, 99, 1, true);
        for (label, b) in [
            ("subject", other_s),
            ("predicate", other_p),
            ("object", other_o),
        ] {
            assert!(
                !same_identity(&id_a, &b),
                "identity must NOT collapse across differing {label}"
            );
            assert_ne!(
                IndexType::Spot.compare(&id_a, &b),
                Ordering::Equal,
                "SPOT comparator must disagree when {label} differs"
            );
        }

        // `cmp_meta` ordering: None < Some, and Some<m1> < Some<m2>
        // when m1 < m2. Construct two flakes with explicit metadata
        // and verify both `cmp_meta` and the SPOT tail behavior.
        let m_lo = FlakeMeta::with_lang("aa");
        let m_hi = FlakeMeta::with_lang("zz");
        let f_none = make_flake_with_meta(101, 200, 42, 1, true, None);
        let f_lo = make_flake_with_meta(101, 200, 42, 1, true, Some(m_lo.clone()));
        let f_hi = make_flake_with_meta(101, 200, 42, 1, true, Some(m_hi.clone()));
        assert_eq!(cmp_meta(&f_none, &f_lo), Ordering::Less);
        assert_eq!(cmp_meta(&f_lo, &f_hi), Ordering::Less);
        assert_eq!(cmp_meta(&f_lo, &f_lo), Ordering::Equal);
        // `same_identity` must split on metadata: same (s,p,o,dt) but
        // distinct m means distinct identity, just like `apply_commit`'s
        // `fact_currently_asserted_in_graph` walks (s,p,o,dt) and then
        // matches `existing.m == flake.m`.
        assert!(
            !same_identity(&f_none, &f_lo),
            "identity must split on metadata"
        );
        assert!(
            !same_identity(&f_lo, &f_hi),
            "identity must split on differing metadata values"
        );

        // The three metas above all have `i: None`, so they only exercise the
        // branch where the ordering consults `lang` unconditionally. Two tags
        // at the SAME list position are the case that used to collapse:
        // `FlakeMeta::cmp` compared `lang` only when neither side carried an
        // index, so `{en, 0}` and `{fr, 0}` were `Equal` without being equal,
        // and `same_identity` — which tests `cmp_meta(..) == Equal` — folded
        // two distinct facts into one (#1711). Read the assertions above as
        // covering this and you would be wrong; this is the case that pins it.
        let at = |lang: &str, i: i32| FlakeMeta {
            lang: Some(lang.to_string()),
            i: Some(i),
        };
        let f_en0 = make_flake_with_meta(101, 200, 42, 1, true, Some(at("en", 0)));
        let f_fr0 = make_flake_with_meta(101, 200, 42, 1, true, Some(at("fr", 0)));
        let f_en1 = make_flake_with_meta(101, 200, 42, 1, true, Some(at("en", 1)));
        assert_eq!(
            cmp_meta(&f_en0, &f_fr0),
            Ordering::Less,
            "one list position under two tags must order, not tie"
        );
        assert!(
            !same_identity(&f_en0, &f_fr0),
            "two language tags at the same list position are two facts"
        );
        assert_ne!(
            IndexType::Spot.compare(&f_en0, &f_fr0),
            Ordering::Equal,
            "SPOT comparator must disagree when only the language tag differs"
        );
        // The index still dominates the tag: position orders first.
        assert_eq!(
            cmp_meta(&f_fr0, &f_en1),
            Ordering::Less,
            "list index remains the primary discriminator"
        );

        // `cmp_object` mixes value and datatype: equal value + differing
        // datatype must order. Use distinct datatype Sids on otherwise
        // identical flakes.
        let dt_long = Sid::new(2, "long");
        let dt_int = Sid::new(2, "integer");
        let with_long = Flake::new(
            Sid::new(101, "s101"),
            Sid::new(200, "p200"),
            FlakeValue::Long(42),
            dt_long,
            1,
            true,
            None,
        );
        let with_int = Flake::new(
            Sid::new(101, "s101"),
            Sid::new(200, "p200"),
            FlakeValue::Long(42),
            dt_int,
            1,
            true,
            None,
        );
        assert_ne!(
            cmp_object(&with_long, &with_int),
            Ordering::Equal,
            "cmp_object must distinguish equal values across differing datatypes"
        );
        assert!(
            !same_identity(&with_long, &with_int),
            "datatype is part of identity — must split"
        );
    }

    // ===== Compaction (structural, tiered) =====

    /// Tiered compaction must not disturb the dedup oracle: re-asserting an
    /// already-current fact afterwards is still skipped.
    #[test]
    fn compaction_preserves_dedup_fact_state() {
        let mut n = Novelty::new(0);
        let rg = no_graphs();
        for i in 0..10u16 {
            n.apply_commit(
                vec![make_flake(i, 1, i as i64, (i + 1) as i64, true)],
                (i + 1) as i64,
                &rg,
            )
            .unwrap();
        }
        let before = n.len();
        let segs_before = n.max_segment_count();
        assert!(segs_before > 1);

        while n.tier_compact(2) > 0 {}
        assert!(
            n.max_segment_count() < segs_before,
            "tiered compaction must consolidate segments"
        );
        assert_eq!(n.len(), before, "no flakes lost in compaction");

        n.apply_commit(vec![make_flake(3, 1, 3, 100, true)], 100, &rg)
            .unwrap();
        assert_eq!(
            n.len(),
            before,
            "re-assert after compaction must still dedup"
        );
    }

    // ===== Config-graph write marker (resolved-config cache invalidation) =====

    /// `config_write_t` must advance to `commit_t` exactly when a commit touches
    /// `CONFIG_GRAPH_ID`, stay put on data-only commits, and never regress. This
    /// is the fail-safe invalidation key for the resolved-config cache: a
    /// matching marker proves config is unchanged, and a data-only write can
    /// never spuriously invalidate (churn) nor a config write fail to (stale).
    #[test]
    fn config_write_marker_tracks_only_config_graph() {
        let cfg_g = Sid::new(9, "config-graph");
        let mut rg = HashMap::new();
        rg.insert(cfg_g.clone(), CONFIG_GRAPH_ID);

        let mut n = Novelty::new(0);
        assert_eq!(n.config_write_t, 0, "marker starts at 0");

        // Data-only commit (default graph, g = None) must not move the marker.
        n.apply_commit(vec![make_flake(1, 1, 1, 5, true)], 5, &rg)
            .unwrap();
        assert_eq!(n.config_write_t, 0, "default-graph write leaves marker");

        // A commit touching the config graph advances the marker to commit_t.
        n.apply_commit(vec![make_graph_flake(2, 1, 1, 7, cfg_g.clone())], 7, &rg)
            .unwrap();
        assert_eq!(
            n.config_write_t, 7,
            "config write advances marker to commit_t"
        );

        // A later data-only commit must neither bump nor regress the marker.
        n.apply_commit(vec![make_flake(3, 1, 1, 9, true)], 9, &rg)
            .unwrap();
        assert_eq!(n.config_write_t, 7, "later data-only write leaves marker");
    }

    /// The bulk (cold-load) path maintains the same marker as `apply_commit`.
    #[test]
    fn config_write_marker_tracks_config_graph_on_bulk() {
        let cfg_g = Sid::new(9, "config-graph");
        let mut rg = HashMap::new();
        rg.insert(cfg_g.clone(), CONFIG_GRAPH_ID);

        let mut n = Novelty::new(0);
        n.bulk_apply_commits(vec![(vec![make_flake(1, 1, 1, 3, true)], 3)], &rg)
            .unwrap();
        assert_eq!(n.config_write_t, 0, "bulk data-only load leaves marker");

        n.bulk_apply_commits(
            vec![(vec![make_graph_flake(2, 1, 1, 8, cfg_g.clone())], 8)],
            &rg,
        )
        .unwrap();
        assert_eq!(n.config_write_t, 8, "bulk config load advances marker");
    }

    /// A same-`t` assert+retract of one identity holds the assertion in
    /// `fact_state` (`fluree_db_core::same_t`), as overlay lifecycle resolution
    /// and every index build do. `bulk_apply_commits` (cold load) replays raw
    /// persisted flakes, and commits written before the staging fix hold such
    /// pairs, so the state must not depend on staging having cancelled them: a
    /// later re-assert is a no-op, and a later retract removes the fact.
    #[test]
    fn same_t_assert_retract_holds_the_assertion() {
        let rg = no_graphs();
        let mut n = Novelty::new(0);

        // One commit at t=1 carrying both ops for the same identity (s=7,p=1,o=1).
        let commit = vec![
            make_flake(7, 1, 1, 1, true),  // assert
            make_flake(7, 1, 1, 1, false), // retract, same identity, same t
        ];
        n.bulk_apply_commits(vec![(commit, 1)], &rg).unwrap();
        let before = n.len();
        assert!(n.fact_state.is_asserted(0, &make_flake(7, 1, 1, 1, true)));

        // The pair left the fact asserted, so a re-assert is a no-op.
        n.apply_commit(vec![make_flake(7, 1, 1, 2, true)], 2, &rg)
            .unwrap();
        assert_eq!(n.len(), before, "re-assert of an asserted fact is deduped");

        // A retract removes it, so the re-assert after that is kept.
        n.apply_commit(vec![make_flake(7, 1, 1, 3, false)], 3, &rg)
            .unwrap();
        n.apply_commit(vec![make_flake(7, 1, 1, 4, true)], 4, &rg)
            .unwrap();
        assert_eq!(n.len(), before + 2);
    }

    /// `apply_commit` keeps an assertion its own commit also retracts, even
    /// when novelty already holds the fact asserted: the pair holds the
    /// assertion, and `bulk_apply_commits` keeps both ops as well. Dropping
    /// it (as the plain re-assert rule would) leaves the retraction alone.
    #[test]
    fn apply_commit_keeps_an_assertion_its_commit_retracts() {
        let rg = no_graphs();
        let first = || vec![make_flake(7, 1, 1, 1, true)];
        let pair = || vec![make_flake(7, 1, 1, 2, false), make_flake(7, 1, 1, 2, true)];

        let mut per_commit = Novelty::new(0);
        per_commit.apply_commit(first(), 1, &rg).unwrap();
        per_commit.apply_commit(pair(), 2, &rg).unwrap();

        let mut bulk = Novelty::new(0);
        bulk.bulk_apply_commits(vec![(first(), 1), (pair(), 2)], &rg)
            .unwrap();

        assert_eq!(per_commit.len(), 3, "assert@1, retract@2, assert@2");
        assert_eq!(bulk.len(), per_commit.len());
        let probe = make_flake(7, 1, 1, 9, true);
        assert!(per_commit.fact_state.is_asserted(0, &probe));
        assert!(bulk.fact_state.is_asserted(0, &probe));

        // A plain re-assert with no retraction in its commit is still deduped.
        per_commit
            .apply_commit(vec![make_flake(7, 1, 1, 3, true)], 3, &rg)
            .unwrap();
        assert_eq!(per_commit.len(), 3);
    }

    /// Cold/bulk load must produce a K=1-per-graph (already-compacted) shape, so
    /// a cold query Lambda never replays a long chain into thousands of segments.
    #[test]
    fn bulk_apply_yields_single_segment_per_graph() {
        let mut n = Novelty::new(0);
        let rg = no_graphs();
        let commits: Vec<(Vec<Flake>, i64)> = (0..20u16)
            .map(|i| {
                (
                    vec![make_flake(i, 1, i as i64, (i + 1) as i64, true)],
                    (i + 1) as i64,
                )
            })
            .collect();
        n.bulk_apply_commits(commits, &rg).unwrap();
        assert_eq!(
            n.segment_count(0),
            1,
            "bulk/cold load must consolidate to K=1 per graph"
        );
    }

    /// `size_class` invariant: merging `tier_width` segments of class K yields one
    /// of class K+1 (count goes c → tier_width·c → class +1). This is what makes
    /// tiered compaction converge level by level.
    #[test]
    fn size_class_increments_on_tier_merge() {
        for &t in &[2usize, 8, 16] {
            for &c in &[1usize, 3, t, t * t] {
                assert_eq!(
                    size_class(c * t, t),
                    size_class(c, t) + 1,
                    "merging {t} class-{} segments (count {c}) must yield class {}",
                    size_class(c, t),
                    size_class(c, t) + 1
                );
            }
        }
    }

    /// Tiered compaction keeps segment count ~logarithmic in commit count (not
    /// linear), preserving every flake.
    #[test]
    fn tier_compact_bounds_segment_count() {
        let mut n = Novelty::new(0);
        let rg = no_graphs();
        let t_width = 4;
        let commits: u16 = 64;
        for i in 0..commits {
            n.apply_commit(
                vec![make_flake(i, 1, i64::from(i), i64::from(i) + 1, true)],
                i64::from(i) + 1,
                &rg,
            )
            .unwrap();
            n.tier_compact(t_width);
        }
        assert_eq!(
            n.len(),
            commits as usize,
            "tiering must preserve every flake"
        );
        let k = n.max_segment_count();
        assert!(
            k <= t_width * 4,
            "tiered K must stay ~logarithmic; got {k} for {commits} commits"
        );
        assert!(
            k < commits as usize,
            "tiering must reduce segment count well below commit count"
        );
    }

    /// A single `tier_compact` call does BOUNDED work even on a large backlog (the
    /// write-burst-then-first-read case): it merges one cascade chain, not the
    /// whole backlog, so the first read can't stall. Repeated calls converge, and
    /// no flake is lost.
    #[test]
    fn tier_compact_bounds_per_call_work_on_backlog() {
        let mut n = Novelty::new(0);
        let rg = no_graphs();
        let t = 4;
        // Insert-only burst: accumulate a backlog with NO compaction.
        for i in 0..100u16 {
            n.apply_commit(
                vec![make_flake(i, 1, i64::from(i), i64::from(i) + 1, true)],
                i64::from(i) + 1,
                &rg,
            )
            .unwrap();
        }
        assert_eq!(n.max_segment_count(), 100, "no compaction during the burst");

        // First read's compaction is bounded — one cascade chain, not 100/t merges.
        let first = n.tier_compact(t);
        assert!(
            (1..=8).contains(&first),
            "first call must be bounded; got {first}"
        );
        assert!(
            n.max_segment_count() > t,
            "one bounded call must not fully drain a 100-segment backlog"
        );

        // Repeated calls converge to a bounded K, losing nothing.
        let mut guard = 0;
        while n.tier_compact(t) > 0 {
            guard += 1;
            assert!(guard < 1000, "tier_compact must converge");
        }
        assert!(
            n.max_segment_count() <= t * 4,
            "converges to ~logarithmic K; got {}",
            n.max_segment_count()
        );
        assert_eq!(n.len(), 100, "tiering must not lose flakes");
    }

    /// Tiered compaction is observationally transparent: identical reads across
    /// all orders, graphs, and time-travel bounds before vs after.
    #[test]
    fn tier_compaction_is_observationally_transparent() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        const ORDERS: [IndexType; 4] = [
            IndexType::Spot,
            IndexType::Psot,
            IndexType::Post,
            IndexType::Opst,
        ];
        let rg = eq_reverse_graph();
        let mut n = Novelty::new(0);
        let mut rng = 0xABCD_1234u64;
        for c in 0..80 {
            let t = c as i64 + 1;
            let batch_n = 1 + (sm64(&mut rng) % 5) as usize;
            let batch: Vec<Flake> = (0..batch_n).map(|_| eq_make(&mut rng, t)).collect();
            n.apply_commit(batch, t, &rg).expect("apply_commit");
        }
        let digest = |n: &Novelty| -> u64 {
            let mut h = DefaultHasher::new();
            for g in 0u32..=2 {
                for idx in ORDERS {
                    let ids = n.slice_for_range(g, idx, None, None, true);
                    ids.len().hash(&mut h);
                    for id in &ids {
                        format!("{:?}", n.get_flake(*id)).hash(&mut h);
                    }
                    for to_t in [1i64, 20, 50, i64::MAX] {
                        let mut seen = Vec::new();
                        n.for_each_overlay_flake(g, idx, None, None, true, to_t, &mut |f| {
                            seen.push(format!("{f:?}"));
                        });
                        seen.len().hash(&mut h);
                        for s in seen {
                            s.hash(&mut h);
                        }
                    }
                }
            }
            h.finish()
        };

        let before = digest(&n);
        let (size0, count0) = (n.size, n.len());
        let merges = n.tier_compact(4);
        assert!(merges > 0, "fixture should trigger at least one tier merge");
        assert_eq!(
            digest(&n),
            before,
            "tier_compact changed an observable read"
        );
        assert_eq!(n.size, size0, "size preserved");
        assert_eq!(n.len(), count0, "flake count preserved");
    }

    // ===== Equivalence / contract harness for the segmented-novelty rewrite =====
    // Applies random commit sequences (asserts/retracts/reasserts, same (s,p,o,dt)
    // with different list-index meta, multiple named graphs, comparator ties) and,
    // after every commit, checks impl-independent invariants (range reads ==
    // filtered full scan; each order sorted; all four orders hold the same
    // multiset) plus a golden digest of the full snapshot. The golden digests pin
    // the exact observable contract so the segmented rewrite must reproduce it.
    // Uses only the stable public surface (apply_commit / slice_for_range /
    // get_flake), so it survives the internal rewrite unchanged.

    fn sm64(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn eq_reverse_graph() -> HashMap<Sid, GraphId> {
        let mut m = HashMap::new();
        m.insert(Sid::new(8, "g1"), 1u32);
        m.insert(Sid::new(8, "g2"), 2u32);
        m
    }

    // Small value pools force collisions, reasserts, and comparator ties.
    fn eq_make(rng: &mut u64, t: i64) -> Flake {
        let s = sm64(rng) % 4;
        let p = sm64(rng) % 3;
        let o = (sm64(rng) % 4) as i64;
        let op = !sm64(rng).is_multiple_of(3); // ~2/3 assert, 1/3 retract
        let gsel = sm64(rng) % 3; // 0 default, 1 g1, 2 g2
        let m = if sm64(rng).is_multiple_of(2) {
            Some(FlakeMeta {
                lang: None,
                i: Some((sm64(rng) % 3) as i32),
            })
        } else {
            None
        };
        let mut f = Flake::new(
            Sid::new(1, format!("s{s}")),
            Sid::new(1, format!("p{p}")),
            FlakeValue::Long(o),
            Sid::new(2, "long"),
            t,
            op,
            m,
        );
        match gsel {
            1 => f.g = Some(Sid::new(8, "g1")),
            2 => f.g = Some(Sid::new(8, "g2")),
            _ => {}
        }
        f
    }

    fn eq_full(n: &Novelty, g: GraphId, idx: IndexType) -> Vec<FlakeId> {
        n.slice_for_range(g, idx, None, None, true).to_vec()
    }

    fn eq_run(seed: u64) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let rg = eq_reverse_graph();
        let mut rng = seed;
        let mut n = Novelty::new(0);
        let mut digest = DefaultHasher::new();
        const ORDERS: [IndexType; 4] = [
            IndexType::Spot,
            IndexType::Psot,
            IndexType::Post,
            IndexType::Opst,
        ];

        for c in 0..40 {
            let t = c as i64 + 1;
            let batch_n = 1 + (sm64(&mut rng) % 6) as usize;
            let batch: Vec<Flake> = (0..batch_n).map(|_| eq_make(&mut rng, t)).collect();
            // Only routable graphs are used, so apply_commit cannot error here.
            n.apply_commit(batch, t, &rg).expect("apply_commit");

            for g in 0u32..=2 {
                // Each order is sorted by its comparator.
                for idx in ORDERS {
                    let ids = eq_full(&n, g, idx);
                    for w in ids.windows(2) {
                        assert!(
                            idx.compare(n.get_flake(w[0]), n.get_flake(w[1])) != Ordering::Greater,
                            "order {idx:?} g{g} not sorted (seed {seed})"
                        );
                    }
                }
                // All four orders hold the same multiset of flakes.
                let canon = |idx: IndexType| -> Vec<String> {
                    let mut v: Vec<String> = eq_full(&n, g, idx)
                        .iter()
                        .map(|&id| format!("{:?}", n.get_flake(id)))
                        .collect();
                    v.sort();
                    v
                };
                let base = canon(IndexType::Spot);
                for idx in [IndexType::Psot, IndexType::Post, IndexType::Opst] {
                    assert_eq!(
                        base,
                        canon(idx),
                        "multiset spot vs {idx:?} g{g} (seed {seed})"
                    );
                }
                // Range reads == filtered full scan, across ALL four orders, with
                // bounded / first-only / open / empty cases. Exercises every
                // comparator path the future k-way merge must implement.
                for idx in ORDERS {
                    let full = eq_full(&n, g, idx);
                    // open-ended (leftmost, no rhs) == the full ordered scan
                    assert_eq!(
                        n.slice_for_range(g, idx, None, None, true).to_vec(),
                        full,
                        "open range != full {idx:?} g{g} (seed {seed})"
                    );
                    if full.len() < 2 {
                        continue;
                    }
                    let a = sm64(&mut rng) as usize % full.len();
                    let b = sm64(&mut rng) as usize % full.len();
                    let (lo, hi) = (a.min(b), a.max(b));
                    let first = n.get_flake(full[lo]).clone();
                    let rhs = n.get_flake(full[hi]).clone();
                    let scan = |pred: &dyn Fn(&Flake) -> bool| -> Vec<FlakeId> {
                        full.iter()
                            .copied()
                            .filter(|&id| pred(n.get_flake(id)))
                            .collect()
                    };
                    // bounded (first, rhs]
                    assert_eq!(
                        n.slice_for_range(g, idx, Some(&first), Some(&rhs), false)
                            .to_vec(),
                        scan(&|f| {
                            idx.compare(f, &first) == Ordering::Greater
                                && idx.compare(f, &rhs) != Ordering::Greater
                        }),
                        "bounded range {idx:?} g{g} (seed {seed})"
                    );
                    // first-only (first, end]
                    assert_eq!(
                        n.slice_for_range(g, idx, Some(&first), None, false)
                            .to_vec(),
                        scan(&|f| idx.compare(f, &first) == Ordering::Greater),
                        "first-only range {idx:?} g{g} (seed {seed})"
                    );
                    // empty/degenerate: first = max, rhs = min
                    let maxf = n.get_flake(*full.last().unwrap()).clone();
                    let minf = n.get_flake(full[0]).clone();
                    assert_eq!(
                        n.slice_for_range(g, idx, Some(&maxf), Some(&minf), false)
                            .to_vec(),
                        scan(&|f| {
                            idx.compare(f, &maxf) == Ordering::Greater
                                && idx.compare(f, &minf) != Ordering::Greater
                        }),
                        "empty range {idx:?} g{g} (seed {seed})"
                    );
                }

                // Fold the full ordered snapshot into the contract digest.
                for idx in ORDERS {
                    let ids = eq_full(&n, g, idx);
                    ids.len().hash(&mut digest);
                    for id in ids {
                        format!("{:?}", n.get_flake(id)).hash(&mut digest);
                    }
                }
            }
        }
        digest.finish()
    }

    #[test]
    fn novelty_equivalence_contract() {
        const SEEDS: [u64; 6] = [1, 2, 3, 42, 1337, 0xDEAD_BEEF];
        // Golden digests pin the observable contract; the segmented rewrite must
        // reproduce these exactly. Regenerate intentionally only when novelty
        // semantics change on purpose.
        const EXPECTED: &[u64] = &[
            17_085_636_203_747_601_083,
            17_735_258_564_421_583_015,
            10_042_115_320_558_787_806,
            10_849_888_332_386_873_009,
            17_714_828_874_643_605_845,
            5_823_289_256_863_810_933,
        ];
        let mut got = Vec::new();
        for &s in &SEEDS {
            let d1 = eq_run(s);
            let d2 = eq_run(s);
            assert_eq!(d1, d2, "novelty non-deterministic for seed {s}");
            got.push(d1);
        }
        eprintln!("NOVELTY_EQUIVALENCE_DIGESTS={got:?}");
        if !EXPECTED.is_empty() {
            assert_eq!(got.as_slice(), EXPECTED, "novelty contract digest changed");
        }
    }
}
