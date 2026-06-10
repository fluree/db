//! Novelty overlay for Fluree DB
//!
//! This crate provides in-memory storage for uncommitted transactions (novelty)
//! that overlays the persisted index. It uses sorted vectors per index for
//! cache locality and efficient merge operations.
//!
//! # Design
//!
//! - **Arena storage**: Flakes stored once in a central arena, referenced by FlakeId
//! - **Per-index sorted vectors**: Each index (SPOT, PSOT, POST, OPST) maintains
//!   a sorted vector of FlakeIds ordered by that index's comparator
//! - **Batch commit**: Epoch bumps once per commit, not per flake
//! - **LSM-style merge**: Sort batch by index comparator, then linear merge with existing
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
use rayon::Scope;
use std::cmp::Ordering;
use std::collections::HashMap;

/// Index into FlakeStore - u32 limits to ~4B flakes
pub type FlakeId = u32;

/// Maximum FlakeId before overflow
pub const MAX_FLAKE_ID: u32 = u32::MAX - 1;

/// Arena-style storage for flakes
///
/// Flakes are stored once and referenced by FlakeId across all 4 indexes.
#[derive(Default, Clone)]
pub struct FlakeStore {
    /// The actual flakes
    flakes: Vec<Flake>,
    /// Per-flake size in bytes (for accurate size tracking)
    sizes: Vec<usize>,
}

impl FlakeStore {
    /// Create a new empty flake store
    pub fn new() -> Self {
        Self::default()
    }

    /// Get a flake by ID
    pub fn get(&self, id: FlakeId) -> &Flake {
        &self.flakes[id as usize]
    }

    /// Get the number of flakes stored
    pub fn len(&self) -> usize {
        self.flakes.len()
    }

    /// Check if the store is empty
    pub fn is_empty(&self) -> bool {
        self.flakes.is_empty()
    }

    /// Push a flake with a precomputed size (avoids double size_bytes)
    fn push_with_size(&mut self, flake: Flake, size: usize) -> FlakeId {
        let id = self.flakes.len() as FlakeId;
        self.sizes.push(size);
        self.flakes.push(flake);
        id
    }

    /// Test helper: push a flake (computes size).
    #[cfg(test)]
    fn push(&mut self, flake: Flake) -> FlakeId {
        let size = flake.size_bytes();
        self.push_with_size(flake, size)
    }

    /// Get the size of a flake by ID
    fn size(&self, id: FlakeId) -> usize {
        self.sizes[id as usize]
    }
}

/// Per-graph sorted index vectors.
///
/// Each graph gets its own set of 4 sorted FlakeId vectors (SPOT, PSOT, POST, OPST).
/// FlakeIds reference the shared `FlakeStore` arena.
#[derive(Clone, Default)]
struct GraphIndexVectors {
    spot: Vec<FlakeId>,
    psot: Vec<FlakeId>,
    post: Vec<FlakeId>,
    opst: Vec<FlakeId>,
}

impl GraphIndexVectors {
    fn get_index(&self, index: IndexType) -> &[FlakeId] {
        match index {
            IndexType::Spot => &self.spot,
            IndexType::Psot => &self.psot,
            IndexType::Post => &self.post,
            IndexType::Opst => &self.opst,
        }
    }

    /// Get slice of flake IDs for a leaf's range (binary search).
    fn slice_for_range(
        &self,
        store: &FlakeStore,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
    ) -> &[FlakeId] {
        let ids = self.get_index(index);

        if ids.is_empty() {
            return &[];
        }

        let start = if leftmost {
            0
        } else if let Some(f) = first {
            ids.partition_point(|&id| index.compare(store.get(id), f) != Ordering::Greater)
        } else {
            0
        };

        let end = if let Some(r) = rhs {
            ids.partition_point(|&id| index.compare(store.get(id), r) != Ordering::Greater)
        } else {
            ids.len()
        };

        if start >= end {
            return &[];
        }

        &ids[start..end]
    }

    /// Returns true if all index vectors are empty.
    fn is_empty(&self) -> bool {
        self.spot.is_empty() && self.psot.is_empty() && self.post.is_empty() && self.opst.is_empty()
    }

    /// Retain only alive flake IDs across all index vectors.
    fn retain_alive(&mut self, alive: &[bool]) {
        self.spot.retain(|&id| alive[id as usize]);
        self.psot.retain(|&id| alive[id as usize]);
        self.post.retain(|&id| alive[id as usize]);
        self.opst.retain(|&id| alive[id as usize]);
    }
}

/// Novelty overlay - in-memory storage for uncommitted transactions
///
/// Stores flakes in a shared arena with per-graph, per-index sorted vectors
/// for efficient range queries and merge operations.
///
/// GraphIds are dense small integers, so we use `Vec<Option<GraphIndexVectors>>`
/// indexed by `g_id as usize` instead of a HashMap.
#[derive(Clone, Default)]
pub struct Novelty {
    /// Canonical flake storage (arena), shared across all graphs
    store: FlakeStore,

    /// Per-graph sorted index vectors, indexed by g_id
    graphs: Vec<Option<GraphIndexVectors>>,

    /// Total size in bytes (for backpressure)
    pub size: usize,

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
            store: FlakeStore::new(),
            graphs: Vec::new(),
            size: 0,
            t,
            epoch: 0,
            fact_state: NoveltyFactState::new(),
        }
    }

    /// Ensure the graphs vec has a slot for `g_id`, growing if needed.
    fn ensure_graph(&mut self, g_id: GraphId) -> &mut GraphIndexVectors {
        let idx = g_id as usize;
        if idx >= self.graphs.len() {
            self.graphs.resize_with(idx + 1, || None);
        }
        self.graphs[idx].get_or_insert_with(GraphIndexVectors::default)
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

    /// Validate that a batch can be applied WITHOUT mutating any state.
    ///
    /// Checks the two conditions that make [`apply_commit`] fallible — FlakeId
    /// capacity and graph routability. Callers that mutate a shared/live Novelty
    /// in place (e.g. via `Arc::make_mut`) call this first to guarantee an
    /// all-or-nothing apply: if it returns `Ok`, the subsequent `apply_commit`
    /// cannot fail partway and leave the ledger inconsistent.
    pub fn can_apply(&self, flakes: &[Flake], reverse_graph: &HashMap<Sid, GraphId>) -> Result<()> {
        if self.store.len() + flakes.len() > MAX_FLAKE_ID as usize {
            return Err(NoveltyError::overflow(
                "FlakeId overflow: too many flakes in novelty, trigger reindex",
            ));
        }
        for flake in flakes {
            Self::resolve_flake_g_id(flake, reverse_graph)?;
        }
        Ok(())
    }

    /// Apply a batch of flakes from a commit, routing each flake to its graph.
    ///
    /// Epoch bumps ONCE per call, not per flake.
    /// Each flake is routed to its graph via `reverse_graph`. Unknown graph Sids
    /// cause an error — no silent fallback to the default graph.
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
            rayon_threads = rayon::current_num_threads()
        );
        let _guard = span.enter();

        // Check FlakeId overflow (before any mutation)
        let new_count = self.store.len() + flakes.len();
        if new_count > MAX_FLAKE_ID as usize {
            return Err(NoveltyError::overflow(
                "FlakeId overflow: too many flakes in novelty, trigger reindex",
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

        // From here on every step is infallible.
        self.t = self.t.max(commit_t);
        self.epoch += 1; // Bump epoch once per commit

        // Store flakes in arena and group by graph.
        //
        // RDF set semantics: skip assertion flakes whose fact (s, p, o, dt, m)
        // is already **currently asserted** in novelty. This prevents duplicate
        // facts from accumulating when the same triple is asserted in multiple
        // commits (e.g., via repeated `insert` calls). Retractions are always
        // accepted — they're needed to cancel existing assertions.
        //
        // This mirrors the dedup logic in the indexer's merge pipeline
        // (KWayMerge::next_deduped, novelty_merge::merge_novelty) which
        // deduplicates at index-build time.
        let mut per_graph: HashMap<GraphId, Vec<FlakeId>> = HashMap::new();
        let mut deduped = 0u64;

        for (flake, g_id) in routed {
            // Set semantics: skip assertions already current in this graph's
            // novelty window. `fact_state` reflects PRIOR-commit state here (it
            // is updated only after this loop), matching the previous SPOT-vector
            // dedup which likewise updated after the batch.
            if flake.op && self.fact_state.is_asserted(g_id, &flake) {
                deduped += 1;
                continue;
            }

            let size = flake.size_bytes();
            self.size += size;
            let flake_id = self.store.push_with_size(flake, size);
            per_graph.entry(g_id).or_default().push(flake_id);
        }

        // Record every kept flake (assert + retract) into the current-state
        // index, per graph in batch order so the latest op per identity wins.
        // After the keep loop, so within-batch decisions saw only prior state.
        for (&g_id, ids) in &per_graph {
            for &id in ids {
                self.fact_state.record(g_id, self.store.get(id));
            }
        }

        if deduped > 0 {
            tracing::debug!(
                deduped,
                "skipped duplicate assertion flakes (set semantics)"
            );
        }

        // Ensure all graph slots exist
        for &g_id in per_graph.keys() {
            self.ensure_graph(g_id);
        }

        // Merge each graph's batch into its 4 index vectors
        let store = &self.store;
        let parent = tracing::Span::current();

        for (g_id, batch_ids) in &per_graph {
            let graph_vecs = self.graphs[*g_id as usize]
                .as_mut()
                .expect("graph slot ensured above");
            let (spot, psot, post, opst) = (
                &mut graph_vecs.spot,
                &mut graph_vecs.psot,
                &mut graph_vecs.post,
                &mut graph_vecs.opst,
            );

            rayon::scope(|scope: &Scope<'_>| {
                let parent_spot = parent.clone();
                scope.spawn(move |_| {
                    let _p = parent_spot.enter();
                    merge_batch_into_index(store, spot, batch_ids, IndexType::Spot);
                });
                let parent_psot = parent.clone();
                scope.spawn(move |_| {
                    let _p = parent_psot.enter();
                    merge_batch_into_index(store, psot, batch_ids, IndexType::Psot);
                });
                let parent_post = parent.clone();
                scope.spawn(move |_| {
                    let _p = parent_post.enter();
                    merge_batch_into_index(store, post, batch_ids, IndexType::Post);
                });
                let parent_opst = parent.clone();
                scope.spawn(move |_| {
                    let _p = parent_opst.enter();
                    merge_batch_into_index(store, opst, batch_ids, IndexType::Opst);
                });
            });
        }

        Ok(())
    }

    /// Bulk-apply many commits' flakes in a single pass.
    ///
    /// Designed for first-load / catch-up paths (e.g. `LedgerState::load_novelty`
    /// walking a long commit chain) where calling [`apply_commit`] per commit
    /// degrades to O(N²) cumulative cost: each call's
    /// `merge_batch_into_index` is O(target.len() + batch.len()), so over
    /// `M` commits with average batch `B` it accrues `O(M·N̄)` work, where
    /// `N̄` is the running novelty size.
    ///
    /// This method instead:
    /// 1. Routes every flake into a per-graph bucket in one ingest pass.
    /// 2. Sorts each graph's flakes once (parallel) by an identity-then-t
    ///    key (`s, p, o, dt, m, t, op`).
    /// 3. Walks each `(s, p, o, dt, m)` group linearly to apply set
    ///    semantics — assertion is dropped iff the prior kept flake for the
    ///    same identity was also an assertion (mirroring
    ///    [`apply_commit`]'s `fact_currently_asserted_in_graph` skip rule);
    ///    retractions are always kept.
    /// 4. Re-sorts the deduped set into the 4 index orders (SPOT, PSOT,
    ///    POST, OPST) once each.
    ///
    /// Total cost is `O(N log N)` over the merged set instead of `O(N²)` —
    /// for a 787-commit / ~7M-flake chain this drops the catch-up from
    /// minutes to seconds on Lambda single-CPU.
    ///
    /// Existing graph contents (if any) are preserved by merging their
    /// alive `FlakeId`s into the dedup pass alongside the incoming batches,
    /// so the post-condition matches what a sequential per-commit
    /// `apply_commit` chain would have produced — minus retraction-noise
    /// duplicates that the per-commit path never emits anyway.
    ///
    /// `epoch` bumps once per call, regardless of how many commits were
    /// merged. `t` advances to `max(self.t, max_commit_t)`.
    ///
    /// # Memory contract — differs from [`apply_commit`]
    ///
    /// Unlike [`apply_commit`] (which checks `fact_currently_asserted_in_graph`
    /// **before** pushing into the arena, so deduped duplicates never enter
    /// the [`FlakeStore`]), this method pushes every incoming flake into the
    /// arena in Phase 1 and only drops `FlakeId`s during the post-sort
    /// dedup walk. The underlying `Flake` records and their per-flake
    /// sizes remain in [`FlakeStore::flakes`] / `FlakeStore::sizes` for
    /// the lifetime of the [`Novelty`], and `self.size` (the
    /// backpressure-relevant total) accounts for them.
    ///
    /// For the design call site (one fresh-load chain walk feeding an
    /// otherwise-empty arena, after which the [`Novelty`] is consumed and
    /// dropped), this bloat is bounded and operationally negligible — the
    /// dedup count is logged at the end of every call so the cost stays
    /// observable. **Do not wire this into hot-path mutation code without
    /// either redesigning the dedup to gate `push_with_size` or adding a
    /// post-walk arena rebuild.**
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

        // ---- Phase 1: ingest into arena, partition by graph ----
        let mut per_graph: HashMap<GraphId, Vec<FlakeId>> = HashMap::new();
        let mut max_t = self.t;
        let mut commit_count: u64 = 0;
        let mut total_flakes: usize = 0;

        for (flakes, commit_t) in commit_batches {
            if flakes.is_empty() {
                commit_count += 1;
                max_t = max_t.max(commit_t);
                continue;
            }
            let new_count = self.store.len() + flakes.len();
            if new_count > MAX_FLAKE_ID as usize {
                return Err(NoveltyError::overflow(
                    "FlakeId overflow during bulk apply: too many flakes in novelty, trigger reindex",
                ));
            }
            commit_count += 1;
            total_flakes += flakes.len();
            max_t = max_t.max(commit_t);

            for flake in flakes {
                let g_id = Self::resolve_flake_g_id(&flake, reverse_graph)?;
                let size = flake.size_bytes();
                self.size += size;
                let id = self.store.push_with_size(flake, size);
                per_graph.entry(g_id).or_default().push(id);
            }
        }

        if per_graph.is_empty() {
            self.t = max_t;
            self.epoch += 1;
            return Ok(());
        }

        // Ensure graph slots so we can take existing index vectors.
        for &g_id in per_graph.keys() {
            self.ensure_graph(g_id);
        }

        // ---- Phase 2: per-graph dedup + 4-index sort ----
        let store = &self.store;
        let mut total_dedup: u64 = 0;

        for (g_id, mut new_ids) in per_graph {
            // Pull in the graph's existing alive FlakeIds (any prior
            // novelty content) so the dedup pass sees the full universe.
            // SPOT/PSOT/POST/OPST have identical alive sets (apply_commit
            // pushes the same batch_ids to all four), so taking SPOT is
            // the canonical choice.
            let graph_vecs = self.graphs[g_id as usize]
                .as_mut()
                .expect("ensure_graph above");
            let existing_spot = std::mem::take(&mut graph_vecs.spot);
            // Other indexes get rebuilt below; clear them so we don't
            // double-count if the dedup walk drops some.
            graph_vecs.psot.clear();
            graph_vecs.post.clear();
            graph_vecs.opst.clear();

            let mut combined = existing_spot;
            combined.append(&mut new_ids);

            // Sort by (s, p, o, dt, m, t, op) so each (s, p, o, dt, m)
            // identity forms a contiguous t-ascending run.
            combined.par_sort_unstable_by(|&a, &b| {
                let fa = store.get(a);
                let fb = store.get(b);
                fa.s.cmp(&fb.s)
                    .then_with(|| fa.p.cmp(&fb.p))
                    .then_with(|| cmp_object(fa, fb))
                    .then_with(|| cmp_meta(fa, fb))
                    .then_with(|| fa.t.cmp(&fb.t))
                    .then_with(|| fa.op.cmp(&fb.op))
            });

            // Linear set-semantics dedup: for each (s, p, o, dt, m)
            // identity group, walk in ascending t and drop any assertion
            // whose prior kept flake for the same identity was also an
            // assertion. Retractions are always kept.
            let mut kept: Vec<FlakeId> = Vec::with_capacity(combined.len());
            let mut group_start = 0usize;
            while group_start < combined.len() {
                let head = store.get(combined[group_start]);
                let mut group_end = group_start + 1;
                while group_end < combined.len() {
                    let f = store.get(combined[group_end]);
                    if !same_identity(head, f) {
                        break;
                    }
                    group_end += 1;
                }
                let mut currently_asserted = false;
                for &id in &combined[group_start..group_end] {
                    let f = store.get(id);
                    if !f.op {
                        kept.push(id);
                        currently_asserted = false;
                    } else if !currently_asserted {
                        kept.push(id);
                        currently_asserted = true;
                    } else {
                        total_dedup += 1;
                    }
                }
                group_start = group_end;
            }

            // Maintain the current-state index so later apply_commit calls dedup
            // against bulk-loaded facts. `kept` is in (s,p,o,dt,m,t,op) order, so
            // the last record per identity is its highest-t (latest) op.
            for &id in &kept {
                self.fact_state.record(g_id, store.get(id));
            }

            // Build the 4 sorted index vectors from the deduped set. Each
            // sort is independently O(N log N); kept.clone() copies only
            // the small `FlakeId` (u32) array, not the underlying flakes.
            let mut spot = kept.clone();
            spot.par_sort_unstable_by(|&a, &b| IndexType::Spot.compare(store.get(a), store.get(b)));
            let mut psot = kept.clone();
            psot.par_sort_unstable_by(|&a, &b| IndexType::Psot.compare(store.get(a), store.get(b)));
            let mut post = kept.clone();
            post.par_sort_unstable_by(|&a, &b| IndexType::Post.compare(store.get(a), store.get(b)));
            let mut opst = kept;
            opst.par_sort_unstable_by(|&a, &b| IndexType::Opst.compare(store.get(a), store.get(b)));

            let graph_vecs = self.graphs[g_id as usize]
                .as_mut()
                .expect("ensure_graph above");
            graph_vecs.spot = spot;
            graph_vecs.psot = psot;
            graph_vecs.post = post;
            graph_vecs.opst = opst;
        }

        self.t = max_t;
        self.epoch += 1;

        tracing::debug!(
            commits = commit_count,
            total_flakes,
            deduped = total_dedup,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "novelty bulk apply complete"
        );

        Ok(())
    }

    /// Clear flakes with t <= cutoff_t (after index merge)
    ///
    /// Uses bitmap instead of HashSet for cache-friendly O(n) clear.
    ///
    /// Note: In the standard Fluree indexing flow, Novelty is replaced entirely
    /// after each index rebuild rather than mutated in-place. This method exists
    /// for completeness but is rarely needed.
    pub fn clear_up_to(&mut self, cutoff_t: i64) {
        let n = self.store.len();
        if n == 0 {
            return;
        }

        // Build alive bitmap and compute new size
        let mut alive = vec![false; n];
        let mut new_size = 0usize;

        for (i, is_alive) in alive.iter_mut().enumerate() {
            let flake = self.store.get(i as FlakeId);
            if flake.t > cutoff_t {
                *is_alive = true;
                new_size += self.store.size(i as FlakeId);
            }
        }

        // Retain only alive flakes in each graph's index vectors
        for slot in &mut self.graphs {
            if let Some(graph_vecs) = slot {
                graph_vecs.retain_alive(&alive);
                if graph_vecs.is_empty() {
                    *slot = None;
                }
            }
        }

        // Update size
        self.size = new_size;

        // Rebuild the current-state index from surviving flakes. SPOT order is
        // ascending-t within an identity, so the last record per identity wins.
        self.fact_state = NoveltyFactState::new();
        for (g, slot) in self.graphs.iter().enumerate() {
            if let Some(gv) = slot {
                for &id in &gv.spot {
                    self.fact_state.record(g as GraphId, self.store.get(id));
                }
            }
        }

        self.epoch += 1;
    }

    /// Get slice of flake IDs for a specific graph's leaf range.
    ///
    /// Returns `&[]` if the graph has no novelty.
    ///
    /// Uses binary search for O(log n + k) slicing.
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
    ) -> &[FlakeId] {
        match self.graphs.get(g_id as usize).and_then(Option::as_ref) {
            Some(graph_vecs) => {
                graph_vecs.slice_for_range(&self.store, index, first, rhs, leftmost)
            }
            None => &[],
        }
    }

    /// Get flake reference by ID
    pub fn get_flake(&self, id: FlakeId) -> &Flake {
        self.store.get(id)
    }

    /// Get the number of flakes in novelty
    pub fn len(&self) -> usize {
        self.store.len()
    }

    /// Check if novelty is empty
    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    /// Iterate over all flake IDs for a given index across ALL graphs.
    ///
    /// Used by stats collection which needs the full picture regardless of graph.
    pub fn iter_index(&self, index: IndexType) -> impl Iterator<Item = FlakeId> + '_ {
        self.graphs
            .iter()
            .filter_map(Option::as_ref)
            .flat_map(move |graph_vecs| graph_vecs.get_index(index).iter().copied())
    }
}

impl std::fmt::Debug for Novelty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Novelty")
            .field("flake_count", &self.store.len())
            .field(
                "graphs",
                &self.graphs.iter().filter(|s| s.is_some()).count(),
            )
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
        let slice = self.slice_for_range(g_id, index, first, rhs, leftmost);

        for &id in slice {
            let flake = self.get_flake(id);
            if flake.t <= to_t {
                callback(flake);
            }
        }
    }
}

// =============================================================================
// Parallel merge helpers (read-only store + disjoint mutable index vectors)
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

/// LSM-style merge: sort batch by index comparator, then merge with existing target.
fn merge_batch_into_index(
    store: &FlakeStore,
    target: &mut Vec<FlakeId>,
    batch_ids: &[FlakeId],
    index: IndexType,
) {
    use rayon::prelude::*;

    // Sort batch by this index's comparator
    let mut sorted_batch = batch_ids.to_vec();
    sorted_batch.par_sort_unstable_by(|&a, &b| index.compare(store.get(a), store.get(b)));

    // Two-way merge existing + batch
    let mut merged = Vec::with_capacity(target.len() + sorted_batch.len());
    let mut i = 0;
    let mut j = 0;

    while i < target.len() && j < sorted_batch.len() {
        let cmp = index.compare(store.get(target[i]), store.get(sorted_batch[j]));
        if cmp != Ordering::Greater {
            merged.push(target[i]);
            i += 1;
        } else {
            merged.push(sorted_batch[j]);
            j += 1;
        }
    }
    merged.extend_from_slice(&target[i..]);
    merged.extend_from_slice(&sorted_batch[j..]);

    *target = merged;
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
        let (t, epoch, size, len) = (novelty.t, novelty.epoch, novelty.size, novelty.store.len());

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
        assert_eq!(
            novelty.store.len(),
            len,
            "no flakes added after failed apply"
        );

        // can_apply reports the same routing failure without mutating.
        assert!(novelty
            .can_apply(
                &[make_graph_flake(4, 4, 4, 3, Sid::new(9, "g-unknown"))],
                &no_graphs()
            )
            .is_err());
        assert_eq!(novelty.store.len(), len, "can_apply does not mutate");
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

    #[test]
    fn test_flake_store() {
        let mut store = FlakeStore::new();
        assert!(store.is_empty());

        let f1 = make_flake(1, 1, 100, 1, true);
        let id1 = store.push(f1);
        assert_eq!(id1, 0);
        assert_eq!(store.len(), 1);

        let f2 = make_flake(2, 1, 200, 1, true);
        let id2 = store.push(f2);
        assert_eq!(id2, 1);
        assert_eq!(store.len(), 2);

        assert_eq!(store.get(0).s.namespace_code, 1);
        assert_eq!(store.get(1).s.namespace_code, 2);
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
