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
mod runtime_stats;
mod stats;

pub use commit::{
    collect_dag_cids, collect_dag_cids_with_split_mode, find_common_ancestor, load_commit_by_id,
    load_commit_envelope_by_id, trace_commit_envelopes_by_id, trace_commits_by_id, Commit,
    CommitEnvelope, CommonAncestor, TxnMetaEntry, TxnMetaValue, TxnSignature, MAX_TXN_META_BYTES,
    MAX_TXN_META_ENTRIES,
};
pub use commit_flakes::{generate_commit_flakes, stamp_graph_on_commit_flakes};
pub use delta::compute_delta_keys;
pub use error::{NoveltyError, Result};
pub use fluree_db_core::commit::codec::envelope::{MAX_GRAPH_DELTA_ENTRIES, MAX_GRAPH_IRI_LENGTH};
pub use fluree_db_core::commit::codec::format::{CommitSignature, ALGO_ED25519};
pub use fluree_db_core::commit::codec::verify_commit_blob;
pub use fluree_db_credential::SigningKey;
pub use runtime_stats::{
    assemble_fast_stats, assemble_full_stats, resolve_runtime_predicate_id, StatsAssemblyError,
    StatsLookup,
};
pub use stats::current_stats;

use fact_state::NoveltyFactState;
use fluree_db_core::{Flake, GraphId, IndexType, Sid};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

/// Read-scoped handle to a flake inside the novelty overlay.
///
/// Packs `(g_id, segment_index, local_index)` into a `u64`. A `FlakeId` is only
/// valid within the read scope that produced it ([`Novelty::slice_for_range`] /
/// [`Novelty::iter_index`]) and only against the same [`Novelty`] — segment
/// indices shift when `apply_commit` / `bulk_apply_commits` / `clear_up_to`
/// mutate novelty. Do not store it across mutations, serialize it, or do
/// arithmetic on it; round it straight back to [`Novelty::get_flake`].
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct FlakeId(u64);

impl FlakeId {
    const LOCAL_BITS: u32 = 24;
    const SEG_BITS: u32 = 24;
    // The remaining 16 high bits hold the g_id (GraphId = u16).
    const LOCAL_MASK: u64 = (1 << Self::LOCAL_BITS) - 1;
    const SEG_MASK: u64 = (1 << Self::SEG_BITS) - 1;

    #[inline]
    fn pack(g_id: GraphId, seg: usize, local: u32) -> Self {
        debug_assert!(seg as u64 <= Self::SEG_MASK, "novelty segment index overflow");
        debug_assert!(
            u64::from(local) <= Self::LOCAL_MASK,
            "novelty local index overflow"
        );
        FlakeId(
            ((g_id as u64) << (Self::SEG_BITS + Self::LOCAL_BITS))
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

/// An immutable, append-only batch of novelty flakes for one graph.
///
/// Owns its flakes and four locally-sorted index orders (vectors of local
/// indices into `flakes`). Segments are `Arc`-wrapped inside [`Novelty`], so
/// cloning a `Novelty` copies only pointers — never the flakes.
struct Segment {
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
    g_id: GraphId,
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
        let id = FlakeId::pack(self.g_id, seg_idx, local);
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

/// Novelty overlay - in-memory storage for uncommitted transactions
///
/// Append-only, segmented: each graph holds a `Vec` of immutable `Arc<Segment>`,
/// one appended per commit that touches the graph. `apply_commit` never merges
/// into a growing vector — it builds one new segment and pushes it, so per-commit
/// write cost is `O(batch log batch)` instead of `O(total novelty)`. Reads k-way
/// merge the segments on demand (see [`GraphMergeIter`]).
///
/// GraphIds are dense small integers, so graphs are a `Vec<Option<...>>` indexed
/// by `g_id as usize` instead of a HashMap.
#[derive(Clone, Default)]
pub struct Novelty {
    /// Per-graph append-only segment lists, indexed by g_id.
    graphs: Vec<Option<Vec<Arc<Segment>>>>,

    /// Total live size in bytes (for backpressure)
    pub size: usize,

    /// Total live flake count (keeps `len`/`is_empty` O(1)).
    flake_count: usize,

    /// Latest transaction time in novelty
    pub t: i64,

    /// Epoch for cache invalidation - bumped once per commit
    pub epoch: u64,

    /// Current-state fact index for RDF set-semantics dedup (latest op per
    /// identity, per graph, within this novelty window). Persistent map, so it
    /// clones in O(1). The dedup oracle behind the seam; see [`fact_state`].
    fact_state: NoveltyFactState,
}

impl Novelty {
    /// Create a new empty novelty overlay
    pub fn new(t: i64) -> Self {
        Self {
            graphs: Vec::new(),
            size: 0,
            flake_count: 0,
            t,
            epoch: 0,
            fact_state: NoveltyFactState::new(),
        }
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

    /// Append a freshly built segment to a graph, growing the graphs vec.
    fn push_segment(&mut self, g_id: GraphId, seg: Segment) {
        let idx = g_id as usize;
        if idx >= self.graphs.len() {
            self.graphs.resize_with(idx + 1, || None);
        }
        self.size += seg.size;
        self.flake_count += seg.flakes.len();
        self.graphs[idx]
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
        let existing = self
            .graphs
            .get(g_id as usize)
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
        let idx = g_id as usize;
        if idx >= self.graphs.len() {
            self.graphs.resize_with(idx + 1, || None);
        }
        if flakes.is_empty() {
            self.graphs[idx] = None;
            return;
        }
        let mut segs: Vec<Arc<Segment>> = Vec::new();
        while flakes.len() > MAX_SEGMENT_FLAKES {
            let tail = flakes.split_off(MAX_SEGMENT_FLAKES);
            segs.push(Arc::new(Segment::build(flakes, parallel)));
            flakes = tail;
        }
        segs.push(Arc::new(Segment::build(flakes, parallel)));
        self.graphs[idx] = Some(segs);
    }

    /// Recompute `size` / `flake_count` from the current segments. Used by the
    /// whole-graph-replacing paths (`bulk_apply_commits`, `clear_up_to`).
    fn recompute_totals(&mut self) {
        let mut size = 0usize;
        let mut count = 0usize;
        for segs in self.graphs.iter().flatten() {
            for seg in segs {
                size += seg.size;
                count += seg.flakes.len();
            }
        }
        self.size = size;
        self.flake_count = count;
    }

    /// Validate that a batch can be applied WITHOUT mutating any state.
    ///
    /// Checks the two conditions that make [`Self::apply_commit`] fallible —
    /// per-segment capacity and graph routability. Callers that mutate a
    /// shared/live Novelty in place (e.g. via `Arc::make_mut`) call this first to
    /// guarantee an all-or-nothing apply: if it returns `Ok`, the subsequent
    /// `apply_commit` cannot fail partway and leave the ledger inconsistent.
    pub fn can_apply(
        &self,
        flakes: &[Flake],
        reverse_graph: &HashMap<Sid, GraphId>,
    ) -> Result<()> {
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
    /// Atomic: graph routing (the only fallible step) is resolved before any
    /// mutation, so an error leaves novelty untouched.
    pub fn apply_commit(
        &mut self,
        flakes: Vec<Flake>,
        commit_t: i64,
        reverse_graph: &HashMap<Sid, GraphId>,
    ) -> Result<()> {
        if flakes.is_empty() {
            return Ok(());
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

        // From here on every step is infallible.
        self.t = self.t.max(commit_t);
        self.epoch += 1; // Bump epoch once per commit

        // RDF set semantics: skip assertion flakes whose fact (s, p, o, dt, m) is
        // already **currently asserted** in this graph's novelty window. This
        // prevents duplicate facts from accumulating when the same triple is
        // asserted across commits. Retractions are always accepted — they cancel
        // existing assertions. `fact_state` reflects PRIOR-commit state here (it
        // is updated only after this loop).
        let mut per_graph: HashMap<GraphId, Vec<Flake>> = HashMap::new();
        let mut deduped = 0u64;
        for (flake, g_id) in routed {
            if flake.op && self.fact_state.is_asserted(g_id, &flake) {
                deduped += 1;
                continue;
            }
            per_graph.entry(g_id).or_default().push(flake);
        }

        // Record every kept flake (assert + retract) into the current-state
        // index, per graph in batch order so the latest op per identity wins.
        // After the keep loop, so within-batch decisions saw only prior state.
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

        Ok(())
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

        let started = std::time::Instant::now();

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
                per_graph.entry(g_id).or_default().push(flake);
            }
        }

        if per_graph.is_empty() {
            self.t = max_t;
            self.epoch += 1;
            return Ok(());
        }

        // ---- Phase 2: per-graph fold-in existing + dedup + single segment ----
        let mut total_dedup: u64 = 0;

        for (g_id, mut new_flakes) in per_graph {
            // Fold in any existing segments for this graph so the dedup pass sees
            // the full universe, then replace them with one consolidated segment.
            let mut combined: Vec<Flake> = Vec::new();
            if let Some(slot) = self.graphs.get_mut(g_id as usize) {
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
                self.fact_state.record(g_id, flake);
            }

            // Replace the graph with one consolidated segment (chunked only if it
            // somehow exceeds the local-index width).
            self.set_graph_segments(g_id, kept, true);
        }

        self.t = max_t;
        self.epoch += 1;
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
        if self.flake_count == 0 {
            return;
        }

        for slot in &mut self.graphs {
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
                // Straddling: rebuild from survivors.
                let survivors: Vec<Flake> =
                    seg.flakes.iter().filter(|f| f.t > cutoff_t).cloned().collect();
                if !survivors.is_empty() {
                    kept.push(Arc::new(Segment::build(survivors, false)));
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
        let mut fs = NoveltyFactState::new();
        for g in 0..self.graphs.len() {
            if self.graphs[g].as_ref().is_some_and(|s| !s.is_empty()) {
                let ids = self.slice_for_range(g as GraphId, IndexType::Spot, None, None, true);
                for id in ids {
                    fs.record(g as GraphId, self.get_flake(id));
                }
            }
        }
        self.fact_state = fs;

        self.epoch += 1;
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
        let mut streams: Vec<MergeStream<'_>> = Vec::new();
        let mut heap: BinaryHeap<MergeHead<'_>> = BinaryHeap::new();
        if let Some(Some(segs)) = self.graphs.get(g_id as usize) {
            for (seg_idx, seg) in segs.iter().enumerate() {
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
            g_id,
            index,
            streams,
            heap,
        }
    }

    /// Merge across ALL graphs (g_id-ascending), each graph internally
    /// comparator-ordered. Backs the full-scan stats/dict consumers.
    fn iter_all(&self, index: IndexType) -> impl Iterator<Item = (FlakeId, &Flake)> + '_ {
        let present: Vec<GraphId> = (0..self.graphs.len())
            .filter(|&g| self.graphs[g].as_ref().is_some_and(|s| !s.is_empty()))
            .map(|g| g as GraphId)
            .collect();
        present
            .into_iter()
            .flat_map(move |g| self.graph_merge(g, index, None, None, true))
    }

    /// Get the ordered flake IDs for a graph's leaf range, k-way merged across
    /// segments. Returns an owned `Vec` — there is no single backing slice once
    /// novelty is segmented; callers only `.iter()` it.
    ///
    /// Semantics:
    /// - If leftmost=false: left boundary is EXCLUSIVE (> first)
    /// - If leftmost=true: no left boundary
    /// - rhs is INCLUSIVE when present
    pub fn slice_for_range(
        &self,
        g_id: GraphId,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
    ) -> Vec<FlakeId> {
        self.graph_merge(g_id, index, first, rhs, leftmost)
            .map(|(id, _)| id)
            .collect()
    }

    /// Resolve a [`FlakeId`] produced by this novelty back to its flake.
    pub fn get_flake(&self, id: FlakeId) -> &Flake {
        let segs = self.graphs[id.graph()]
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
    /// per graph. IDs are read-scoped — resolve via [`Self::get_flake`] before
    /// the next mutation. Prefer [`Self::iter_flakes`] for new code.
    pub fn iter_index(&self, index: IndexType) -> impl Iterator<Item = FlakeId> + '_ {
        self.iter_all(index).map(|(id, _)| id)
    }

    /// Iterate all flakes for `index` across ALL graphs, comparator-ordered per
    /// graph. Used by stats collection which needs the full picture regardless of
    /// graph.
    pub fn iter_flakes(&self, index: IndexType) -> impl Iterator<Item = &Flake> + '_ {
        self.iter_all(index).map(|(_, f)| f)
    }
}

impl std::fmt::Debug for Novelty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let segments: usize = self.graphs.iter().flatten().map(Vec::len).sum();
        f.debug_struct("Novelty")
            .field("flake_count", &self.flake_count)
            .field(
                "graphs",
                &self.graphs.iter().filter(|s| s.is_some()).count(),
            )
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

    fn epoch(&self) -> u64 {
        self.epoch
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
        // Zero-copy: drive the k-way merge directly, applying the to_t filter.
        for (_, flake) in self.graph_merge(g_id, index, first, rhs, leftmost) {
            if flake.t <= to_t {
                callback(flake);
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

    #[test]
    fn test_per_graph_isolation() {
        let mut novelty = Novelty::new(0);

        // Set up: graph 2 mapped to Sid("g", "graph2")
        let g2_sid = Sid::new(100, "graph2");
        let mut rg = HashMap::new();
        rg.insert(g2_sid.clone(), 2u16);

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
        m.insert(Sid::new(8, "g1"), 1u16);
        m.insert(Sid::new(8, "g2"), 2u16);
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

            for g in 0u16..=2 {
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
                    assert_eq!(base, canon(idx), "multiset spot vs {idx:?} g{g} (seed {seed})");
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
                        full.iter().copied().filter(|&id| pred(n.get_flake(id))).collect()
                    };
                    // bounded (first, rhs]
                    assert_eq!(
                        n.slice_for_range(g, idx, Some(&first), Some(&rhs), false).to_vec(),
                        scan(&|f| {
                            idx.compare(f, &first) == Ordering::Greater
                                && idx.compare(f, &rhs) != Ordering::Greater
                        }),
                        "bounded range {idx:?} g{g} (seed {seed})"
                    );
                    // first-only (first, end]
                    assert_eq!(
                        n.slice_for_range(g, idx, Some(&first), None, false).to_vec(),
                        scan(&|f| idx.compare(f, &first) == Ordering::Greater),
                        "first-only range {idx:?} g{g} (seed {seed})"
                    );
                    // empty/degenerate: first = max, rhs = min
                    let maxf = n.get_flake(*full.last().unwrap()).clone();
                    let minf = n.get_flake(full[0]).clone();
                    assert_eq!(
                        n.slice_for_range(g, idx, Some(&maxf), Some(&minf), false).to_vec(),
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
