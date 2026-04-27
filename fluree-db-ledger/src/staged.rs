//! Staged transaction support
//!
//! This module provides `StagedLedger` for staging transactions before commit.
//! A StagedLedger combines:
//! - Base LedgerState (indexed LedgerSnapshot + committed novelty)
//! - Staged flakes (not yet committed)
//!
//! This enables query against staged changes without committing them.

use crate::error::LedgerError;
use crate::LedgerState;
use fluree_db_core::{Flake, GraphDbRef, GraphId, IndexType, OverlayProvider, Sid};
use fluree_db_novelty::FlakeId;
use std::cmp::Ordering;
use std::collections::HashMap;

/// Arena-style storage for staged flakes
struct StagedStore {
    flakes: Vec<Flake>,
}

impl StagedStore {
    fn new(flakes: Vec<Flake>) -> Self {
        Self { flakes }
    }

    fn get(&self, id: FlakeId) -> &Flake {
        &self.flakes[id as usize]
    }

    fn len(&self) -> usize {
        self.flakes.len()
    }

    fn is_empty(&self) -> bool {
        self.flakes.is_empty()
    }
}

/// Staged overlay - maintains sorted vectors like Novelty, with per-flake graph IDs
/// for efficient graph filtering.
struct StagedOverlay {
    store: StagedStore,
    /// Pre-computed GraphId per flake (parallel to store.flakes, same indices)
    flake_graph_ids: Vec<GraphId>,
    spot: Vec<FlakeId>,
    psot: Vec<FlakeId>,
    post: Vec<FlakeId>,
    opst: Vec<FlakeId>,
}

impl StagedOverlay {
    fn from_flakes(
        flakes: Vec<Flake>,
        reverse_graph: &HashMap<Sid, GraphId>,
    ) -> Result<Self, LedgerError> {
        if flakes.is_empty() {
            return Ok(Self {
                store: StagedStore::new(vec![]),
                flake_graph_ids: vec![],
                spot: vec![],
                psot: vec![],
                post: vec![],
                opst: vec![],
            });
        }

        // Pre-compute graph IDs for all flakes — strict, no silent fallback.
        // Unknown graph Sids are a programming error (reverse_graph is built from
        // build_reverse_graph() which is total).
        let mut flake_graph_ids: Vec<GraphId> = Vec::with_capacity(flakes.len());
        for f in &flakes {
            let g_id = match &f.g {
                None => 0,
                Some(g_sid) => *reverse_graph.get(g_sid).ok_or_else(|| {
                    LedgerError::Core(fluree_db_core::Error::invalid_index(format!(
                        "staged flake has unknown graph Sid '{g_sid}' not in reverse_graph"
                    )))
                })?,
            };
            flake_graph_ids.push(g_id);
        }

        let store = StagedStore::new(flakes);
        let ids: Vec<FlakeId> = (0..store.len() as FlakeId).collect();

        // Build sorted indexes
        let mut spot = ids.clone();
        spot.sort_by(|&a, &b| IndexType::Spot.compare(store.get(a), store.get(b)));

        let mut psot = ids.clone();
        psot.sort_by(|&a, &b| IndexType::Psot.compare(store.get(a), store.get(b)));

        let mut post = ids.clone();
        post.sort_by(|&a, &b| IndexType::Post.compare(store.get(a), store.get(b)));

        // OPST includes all flakes (matching Novelty and DerivedFactsOverlay behavior)
        let mut opst = ids;
        opst.sort_by(|&a, &b| IndexType::Opst.compare(store.get(a), store.get(b)));

        Ok(Self {
            store,
            flake_graph_ids,
            spot,
            psot,
            post,
            opst,
        })
    }

    fn get_index(&self, index: IndexType) -> &[FlakeId] {
        match index {
            IndexType::Spot => &self.spot,
            IndexType::Psot => &self.psot,
            IndexType::Post => &self.post,
            IndexType::Opst => &self.opst,
        }
    }

    fn slice_for_range(
        &self,
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
            ids.partition_point(|&id| index.compare(self.store.get(id), f) != Ordering::Greater)
        } else {
            0
        };

        let end = if let Some(r) = rhs {
            ids.partition_point(|&id| index.compare(self.store.get(id), r) != Ordering::Greater)
        } else {
            ids.len()
        };

        if start >= end {
            return &[];
        }

        &ids[start..end]
    }

    /// Get the pre-computed graph ID for a flake
    fn graph_id(&self, id: FlakeId) -> GraphId {
        self.flake_graph_ids[id as usize]
    }
}

/// A view of a ledger with staged (uncommitted) changes
///
/// This combines:
/// - Base LedgerState (indexed LedgerSnapshot + committed novelty)
/// - Staged flakes (not yet committed)
///
/// Queries against a StagedLedger will see the staged changes.
pub struct StagedLedger {
    /// Base ledger state
    base: LedgerState,
    /// Staged changes
    staged: StagedOverlay,
    /// Unique epoch for cache keys (different from base novelty)
    staged_epoch: u64,
    /// Logical `t` represented by this staged view.
    ///
    /// For ordinary single-parent transactions this is `base.t() + 1`. For
    /// multi-parent merge commits the merge engine supplies an explicit
    /// `merge_t = max(source_t, target_t) + 1`, which can exceed
    /// `base.t() + 1`. Threading the actual `t` through avoids
    /// mid-flight inconsistency between staged-view queries and the eventual
    /// commit header — both see the same time.
    staged_t: i64,
}

impl StagedLedger {
    /// Build a staged ledger by layering `flakes` onto `base`.
    ///
    /// `reverse_graph` maps graph Sids to GraphIds for per-graph filtering.
    /// Pass an empty map when all flakes are default-graph only.
    ///
    /// Equivalent to [`Self::new_with_t`] with `staged_t = base.t() + 1`.
    /// Use [`Self::new_with_t`] when staging a multi-parent merge so the
    /// view and the eventual commit agree on `t`.
    ///
    /// Returns `Err` if any staged flake has a graph Sid not present in
    /// `reverse_graph` (programming error — the map must be complete).
    pub fn new(
        base: LedgerState,
        flakes: Vec<Flake>,
        reverse_graph: &HashMap<Sid, GraphId>,
    ) -> Result<Self, LedgerError> {
        let staged_t = base.t() + 1;
        Self::new_with_t(base, flakes, reverse_graph, staged_t)
    }

    /// Build a staged ledger with an explicit `staged_t`.
    ///
    /// `staged_t` must be strictly greater than `base.t()`. Used by the
    /// merge engine to stage a multi-parent commit at
    /// `max(source_t, target_t) + 1`. For ordinary transactions, prefer
    /// [`Self::new`] (which derives `staged_t` from `base`).
    pub fn new_with_t(
        base: LedgerState,
        flakes: Vec<Flake>,
        reverse_graph: &HashMap<Sid, GraphId>,
        staged_t: i64,
    ) -> Result<Self, LedgerError> {
        if staged_t <= base.t() {
            return Err(LedgerError::Core(fluree_db_core::Error::invalid_index(
                format!(
                    "staged_t ({staged_t}) must be strictly greater than base.t ({})",
                    base.t()
                ),
            )));
        }
        let staged_epoch = base.novelty.epoch + 1;
        Ok(Self {
            staged: StagedOverlay::from_flakes(flakes, reverse_graph)?,
            staged_epoch,
            base,
            staged_t,
        })
    }

    /// Get the base ledger state
    pub fn base(&self) -> &LedgerState {
        &self.base
    }

    /// Consume the view and return the owned base ledger state
    ///
    /// Use this when the staged changes should be discarded (e.g., no-op updates).
    pub fn into_base(self) -> LedgerState {
        self.base
    }

    /// Get the staged epoch
    pub fn epoch(&self) -> u64 {
        self.staged_epoch
    }

    /// Get the number of staged flakes
    pub fn staged_len(&self) -> usize {
        self.staged.store.len()
    }

    /// Check if there are staged flakes
    pub fn has_staged(&self) -> bool {
        !self.staged.store.is_empty()
    }

    /// Get a reference to the staged flakes
    pub fn staged_flakes(&self) -> &[Flake] {
        &self.staged.store.flakes
    }

    /// Get a reference to the underlying database
    pub fn db(&self) -> &fluree_db_core::LedgerSnapshot {
        &self.base.snapshot
    }

    /// Consume the view and return the base state and staged flakes
    pub fn into_parts(self) -> (LedgerState, Vec<Flake>) {
        (self.base, self.staged.store.flakes)
    }

    /// The effective as-of time for this staged view.
    ///
    /// Returns the `staged_t` supplied at construction (default
    /// `base.t() + 1`, or an explicit value via [`Self::new_with_t`] for
    /// multi-parent merges) **regardless of whether staged flakes exist**.
    ///
    /// Why unconditional: a `take-target` no-op merge stages zero flakes but
    /// its commit will still be at `merge_t`. Returning `base.t()` for the
    /// empty case would let query/validate observe a different `t` than the
    /// eventual commit — exactly the inconsistency the explicit-`t`
    /// constructor was added to prevent.
    ///
    /// For non-merge transactions the behavior is unchanged in practice:
    /// callers that build empty staged views via [`Self::new`] discard them
    /// without observing `staged_t()` (no-op transactions don't reach
    /// commit), and callers that do observe it see `base.t() + 1`, which is
    /// also harmless because the overlay contributes nothing.
    pub fn staged_t(&self) -> i64 {
        self.staged_t
    }

    /// Create a `GraphDbRef` bundling snapshot, graph id, overlay, and time.
    ///
    /// Uses `self` as the overlay (merges base novelty + staged flakes)
    /// and `staged_t()` as the time bound — ensuring staged flakes are
    /// visible through the overlay's `to_t` filtering.
    pub fn as_graph_db_ref(&self, g_id: GraphId) -> GraphDbRef<'_> {
        GraphDbRef::new(self.db(), g_id, self, self.staged_t())
    }
}

impl OverlayProvider for StagedLedger {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn epoch(&self) -> u64 {
        self.staged_epoch
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
        // Two-way merge of base novelty slice (already per-graph) + staged slice
        // (filtered by g_id using pre-computed graph IDs)

        let base_slice = self
            .base
            .novelty
            .slice_for_range(g_id, index, first, rhs, leftmost);
        let staged_slice = self.staged.slice_for_range(index, first, rhs, leftmost);

        let mut base_iter = base_slice.iter().map(|&id| self.base.novelty.get_flake(id));
        // Filter staged flakes to only those matching the requested graph
        let mut staged_iter = staged_slice
            .iter()
            .filter(|&&id| self.staged.graph_id(id) == g_id)
            .map(|&id| self.staged.store.get(id));

        let mut base_next = base_iter.next();
        let mut staged_next = staged_iter.next();

        loop {
            match (base_next, staged_next) {
                (Some(base_flake), Some(staged_flake)) => {
                    let cmp = index.compare(base_flake, staged_flake);
                    if cmp != Ordering::Greater {
                        if base_flake.t <= to_t {
                            callback(base_flake);
                        }
                        base_next = base_iter.next();
                    } else {
                        if staged_flake.t <= to_t {
                            callback(staged_flake);
                        }
                        staged_next = staged_iter.next();
                    }
                }
                (Some(base_flake), None) => {
                    if base_flake.t <= to_t {
                        callback(base_flake);
                    }
                    base_next = base_iter.next();
                }
                (None, Some(staged_flake)) => {
                    if staged_flake.t <= to_t {
                        callback(staged_flake);
                    }
                    staged_next = staged_iter.next();
                }
                (None, None) => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeValue, Sid};
    use fluree_db_novelty::Novelty;

    fn make_flake(s: u16, p: u16, o: i64, t: i64) -> Flake {
        Flake::new(
            Sid::new(s, format!("s{s}")),
            Sid::new(p, format!("p{p}")),
            FlakeValue::Long(o),
            Sid::new(2, "long"),
            t,
            true,
            None,
        )
    }

    #[test]
    fn test_staged_overlay_empty() {
        let staged = StagedOverlay::from_flakes(vec![], &HashMap::new()).unwrap();
        assert!(staged.store.is_empty());
    }

    #[test]
    fn test_staged_overlay_sorting() {
        let flakes = vec![
            make_flake(3, 1, 100, 1),
            make_flake(1, 1, 100, 1),
            make_flake(2, 1, 100, 1),
        ];

        let staged = StagedOverlay::from_flakes(flakes, &HashMap::new()).unwrap();

        // SPOT should be sorted by subject
        let spot_subjects: Vec<u16> = staged
            .spot
            .iter()
            .map(|&id| staged.store.get(id).s.namespace_code)
            .collect();
        assert_eq!(spot_subjects, vec![1, 2, 3]);
    }

    #[test]
    fn test_ledger_view_overlay_provider() {
        use fluree_db_core::LedgerSnapshot;

        let snapshot = LedgerSnapshot::genesis("test:main");

        // Create base novelty with some flakes (default graph, no reverse_graph needed)
        let mut novelty = Novelty::new(0);
        novelty
            .apply_commit(
                vec![make_flake(1, 1, 100, 1), make_flake(3, 1, 300, 1)],
                1,
                &HashMap::new(),
            )
            .unwrap();

        let state = LedgerState::new(snapshot, novelty);

        // Create view with interleaved staged flakes
        let staged_flakes = vec![make_flake(2, 1, 200, 2), make_flake(4, 1, 400, 2)];
        let view = StagedLedger::new(state, staged_flakes, &HashMap::new()).unwrap();

        // Collect all flakes via overlay provider (g_id=0 for default graph)
        let mut collected = Vec::new();
        view.for_each_overlay_flake(0, IndexType::Spot, None, None, true, 100, &mut |f| {
            collected.push(f.s.namespace_code);
        });

        // Should be merged in sorted order
        assert_eq!(collected, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_ledger_view_epoch() {
        use fluree_db_core::LedgerSnapshot;

        let snapshot = LedgerSnapshot::genesis("test:main");

        let mut novelty = Novelty::new(0);
        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 1)], 1, &HashMap::new())
            .unwrap();

        let base_epoch = novelty.epoch;
        let state = LedgerState::new(snapshot, novelty);

        let view =
            StagedLedger::new(state, vec![make_flake(2, 1, 200, 2)], &HashMap::new()).unwrap();

        // Staged epoch should be different from base epoch
        assert_eq!(view.epoch(), base_epoch + 1);
    }

    #[test]
    fn test_ledger_view_into_parts() {
        use fluree_db_core::LedgerSnapshot;

        let snapshot = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let state = LedgerState::new(snapshot, novelty);

        let staged_flakes = vec![make_flake(1, 1, 100, 1)];
        let view = StagedLedger::new(state, staged_flakes, &HashMap::new()).unwrap();

        let (base, flakes) = view.into_parts();
        assert_eq!(base.ledger_id(), "test:main");
        assert_eq!(flakes.len(), 1);
    }

    #[test]
    fn test_empty_view_returns_override_staged_t() {
        // Take-target no-op merges produce an empty staged view but the
        // commit will still write at `merge_t`. `staged_t()` must reflect
        // that, otherwise query/validate during the merge flow observes a
        // different `t` than the eventual commit.
        use fluree_db_core::LedgerSnapshot;

        let snapshot = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let state = LedgerState::new(snapshot, novelty);

        let view = StagedLedger::new_with_t(state, vec![], &HashMap::new(), 7).unwrap();
        assert!(!view.has_staged());
        assert_eq!(view.staged_t(), 7, "empty view must report override t");
    }

    #[test]
    fn test_new_with_t_overrides_staged_t() {
        use fluree_db_core::LedgerSnapshot;

        // base.t() == 0 (genesis); merge_t = 7 simulates a merge where the
        // source branch advanced 7 transactions ahead of the merge base.
        let snapshot = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let state = LedgerState::new(snapshot, novelty);
        assert_eq!(state.t(), 0);

        let staged_flakes = vec![make_flake(1, 1, 100, 7)];
        let view = StagedLedger::new_with_t(state, staged_flakes, &HashMap::new(), 7).unwrap();

        assert_eq!(view.staged_t(), 7);
    }

    #[test]
    fn test_new_with_t_rejects_non_monotonic() {
        use fluree_db_core::LedgerSnapshot;

        let snapshot = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let state = LedgerState::new(snapshot, novelty);

        // staged_t == base.t() is rejected (must be strictly greater).
        let result =
            StagedLedger::new_with_t(state, vec![make_flake(1, 1, 1, 0)], &HashMap::new(), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_new_defaults_to_base_t_plus_one() {
        use fluree_db_core::LedgerSnapshot;

        let snapshot = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let state = LedgerState::new(snapshot, novelty);

        // Default constructor stamps staged_t = base.t() + 1 = 1.
        let view = StagedLedger::new(state, vec![make_flake(1, 1, 1, 1)], &HashMap::new()).unwrap();
        assert_eq!(view.staged_t(), 1);
    }

    #[test]
    fn test_staged_overlay_unknown_graph_sid_errors() {
        use fluree_db_core::Flake;

        let graph_sid = Sid::new(99, "unknown:graph");
        let flakes = vec![Flake::new_in_graph(
            graph_sid,
            Sid::new(1, "s1"),
            Sid::new(2, "p1"),
            FlakeValue::Long(100),
            Sid::new(3, "long"),
            1,
            true,
            None,
        )];

        // Empty reverse_graph means the graph Sid is unknown — should error
        let result = StagedOverlay::from_flakes(flakes, &HashMap::new());
        assert!(result.is_err());
    }
}
