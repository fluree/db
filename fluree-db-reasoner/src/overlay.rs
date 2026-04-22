//! Derived facts overlay for OWL2-RL reasoning results
//!
//! This module provides the `DerivedFactsOverlay` type that stores materialized
//! facts from OWL2-RL reasoning and implements `OverlayProvider` for query-time use.
//!
//! # Design
//!
//! - Flakes are stored sorted by each index type for efficient range queries
//! - The overlay includes `FrozenSameAs` for owl:sameAs equivalence handling
//! - Derived facts are canonicalized (use canonical representatives for S/O positions)
//! - Query-time lookups canonicalize the query key first, only expand when necessary

use fluree_db_core::comparator::IndexType;
use fluree_db_core::flake::Flake;
use fluree_db_core::overlay::OverlayProvider;
use fluree_db_core::{GraphId, Sid};
use std::sync::Arc;

use crate::same_as::FrozenSameAs;

/// Derived facts overlay from OWL2-RL reasoning
///
/// Implements `OverlayProvider` to be composable with base overlays (e.g., novelty).
#[derive(Debug, Clone)]
pub struct DerivedFactsOverlay {
    /// Flakes sorted by SPOT index order
    spot: Arc<[Flake]>,
    /// Flakes sorted by PSOT index order
    psot: Arc<[Flake]>,
    /// Flakes sorted by POST index order
    post: Arc<[Flake]>,
    /// Flakes sorted by OPST index order
    opst: Arc<[Flake]>,
    /// owl:sameAs equivalence classes
    same_as: FrozenSameAs,
    /// Epoch for cache key differentiation
    epoch: u64,
}

impl DerivedFactsOverlay {
    /// Create an empty overlay (no derived facts) with default epoch and empty sameAs
    ///
    /// Use `empty_with_metadata()` when you need to preserve specific epoch/sameAs values.
    pub fn empty() -> Self {
        Self::empty_with_metadata(FrozenSameAs::empty(), 0)
    }

    /// Create an empty overlay preserving sameAs state and epoch
    ///
    /// Use this when reasoning produces no derived facts but you still need to
    /// preserve the epoch and sameAs state for cache correctness.
    pub fn empty_with_metadata(same_as: FrozenSameAs, epoch: u64) -> Self {
        Self {
            spot: Arc::from([]),
            psot: Arc::from([]),
            post: Arc::from([]),
            opst: Arc::from([]),
            same_as,
            epoch,
        }
    }

    /// Create an overlay from pre-sorted flakes
    ///
    /// # Arguments
    ///
    /// * `spot` - Flakes sorted by SPOT order
    /// * `psot` - Flakes sorted by PSOT order
    /// * `post` - Flakes sorted by POST order
    /// * `opst` - Flakes sorted by OPST order
    /// * `same_as` - Frozen sameAs equivalence classes
    /// * `epoch` - Epoch for cache differentiation
    pub fn new(
        spot: Vec<Flake>,
        psot: Vec<Flake>,
        post: Vec<Flake>,
        opst: Vec<Flake>,
        same_as: FrozenSameAs,
        epoch: u64,
    ) -> Self {
        Self {
            spot: spot.into(),
            psot: psot.into(),
            post: post.into(),
            opst: opst.into(),
            same_as,
            epoch,
        }
    }

    /// Get the sameAs equivalence structure
    pub fn same_as(&self) -> &FrozenSameAs {
        &self.same_as
    }

    /// Get the canonical representative for a Sid
    pub fn canonical(&self, sid: Sid) -> Sid {
        self.same_as.canonical(sid)
    }

    /// Expand a Sid to all equivalents
    pub fn expand_equivalents(&self, sid: Sid) -> &[Sid] {
        self.same_as.expand(sid)
    }

    /// Get number of derived facts
    pub fn len(&self) -> usize {
        self.spot.len()
    }

    /// Check if empty (no derived facts)
    pub fn is_empty(&self) -> bool {
        self.spot.is_empty()
    }

    /// Get flakes by index type (for iteration/debugging)
    pub fn flakes(&self, index: IndexType) -> &[Flake] {
        match index {
            IndexType::Spot => &self.spot,
            IndexType::Psot => &self.psot,
            IndexType::Post => &self.post,
            IndexType::Opst => &self.opst,
        }
    }

    /// Binary search for the first flake > target in the given index
    fn upper_bound(&self, index: IndexType, target: &Flake) -> usize {
        let flakes = self.flakes(index);
        let cmp = index.comparator();

        flakes.partition_point(|f| cmp(f, target).is_le())
    }
}

impl OverlayProvider for DerivedFactsOverlay {
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
        // Derived facts from reasoning are default-graph only
        if g_id != 0 {
            return;
        }

        let flakes = self.flakes(index);
        if flakes.is_empty() {
            return;
        }

        // Determine start position
        let start = if leftmost {
            0
        } else if let Some(first) = first {
            // Exclusive left boundary: start after first
            self.upper_bound(index, first)
        } else {
            0
        };

        // Determine end position
        let end = if let Some(rhs) = rhs {
            // Inclusive right boundary: include rhs
            self.upper_bound(index, rhs)
        } else {
            flakes.len()
        };

        // Emit flakes in range, filtered by to_t
        for flake in &flakes[start..end] {
            if flake.t <= to_t {
                callback(flake);
            }
        }
    }
}

/// Builder for constructing DerivedFactsOverlay
///
/// Accumulates flakes during reasoning, then sorts and builds the final overlay.
#[derive(Debug, Default)]
pub struct DerivedFactsBuilder {
    /// Unsorted flakes (will be sorted when building)
    flakes: Vec<Flake>,
}

impl DerivedFactsBuilder {
    /// Create a new empty builder
    pub fn new() -> Self {
        Self { flakes: Vec::new() }
    }

    /// Create a builder with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            flakes: Vec::with_capacity(capacity),
        }
    }

    /// Add a flake to the builder
    pub fn push(&mut self, flake: Flake) {
        self.flakes.push(flake);
    }

    /// Extend with an iterator of flakes
    pub fn extend(&mut self, flakes: impl IntoIterator<Item = Flake>) {
        self.flakes.extend(flakes);
    }

    /// Get number of accumulated flakes
    pub fn len(&self) -> usize {
        self.flakes.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.flakes.is_empty()
    }

    /// Build the final overlay
    ///
    /// Sorts flakes by each index and constructs the immutable overlay.
    /// Preserves `same_as` and `epoch` even when no flakes were derived.
    pub fn build(self, same_as: FrozenSameAs, epoch: u64) -> DerivedFactsOverlay {
        if self.flakes.is_empty() {
            // Preserve same_as and epoch even with zero derived flakes
            return DerivedFactsOverlay::new(
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                same_as,
                epoch,
            );
        }

        // Sort by each index
        let mut spot = self.flakes.clone();
        let mut psot = self.flakes.clone();
        let mut post = self.flakes.clone();
        let mut opst = self.flakes;

        spot.sort_by(|a, b| IndexType::Spot.comparator()(a, b));
        psot.sort_by(|a, b| IndexType::Psot.comparator()(a, b));
        post.sort_by(|a, b| IndexType::Post.comparator()(a, b));
        opst.sort_by(|a, b| IndexType::Opst.comparator()(a, b));

        DerivedFactsOverlay::new(spot, psot, post, opst, same_as, epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::value::FlakeValue;

    fn sid(n: u16) -> Sid {
        Sid::new(n, format!("test:{n}"))
    }

    fn make_flake(s: u16, p: u16, o: i64, t: i64) -> Flake {
        // Flake::new(s, p, o, dt, t, op, m)
        Flake::new(sid(s), sid(p), FlakeValue::Long(o), sid(0), t, true, None)
    }

    #[test]
    fn test_empty_overlay() {
        let overlay = DerivedFactsOverlay::empty();
        assert!(overlay.is_empty());
        assert_eq!(overlay.len(), 0);
        assert_eq!(overlay.epoch(), 0);
    }

    #[test]
    fn test_builder_basic() {
        let mut builder = DerivedFactsBuilder::new();
        builder.push(make_flake(1, 1, 100, 1));
        builder.push(make_flake(2, 1, 200, 1));

        let overlay = builder.build(FrozenSameAs::empty(), 42);
        assert_eq!(overlay.len(), 2);
        assert_eq!(overlay.epoch(), 42);
    }

    #[test]
    fn test_overlay_provider_basic() {
        let mut builder = DerivedFactsBuilder::new();
        builder.push(make_flake(1, 1, 100, 1));
        builder.push(make_flake(2, 1, 200, 2));
        builder.push(make_flake(3, 1, 300, 3));

        let overlay = builder.build(FrozenSameAs::empty(), 1);

        // Collect all flakes with to_t = 3 (g_id=0 for default graph)
        let mut collected = Vec::new();
        overlay.for_each_overlay_flake(0, IndexType::Spot, None, None, true, 3, &mut |f| {
            collected.push(f.clone());
        });
        assert_eq!(collected.len(), 3);

        // Collect with to_t = 2 (should exclude t=3 flake)
        collected.clear();
        overlay.for_each_overlay_flake(0, IndexType::Spot, None, None, true, 2, &mut |f| {
            collected.push(f.clone());
        });
        assert_eq!(collected.len(), 2);
    }
}
