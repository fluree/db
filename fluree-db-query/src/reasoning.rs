//! OWL2-RL reasoning integration for query execution
//!
//! This module provides helpers for integrating OWL2-RL materialization
//! with query execution, including overlay composition for derived facts.

use fluree_db_core::{Flake, GraphId, IndexType, OverlayProvider};
use fluree_db_reasoner::{DerivedFactsOverlay, ReasoningCache};
use std::sync::Arc;

/// Overlay that combines a base overlay with derived facts from OWL2-RL reasoning.
///
/// This implements `OverlayProvider` by merging flakes from both sources,
/// allowing queries to see both base data and materialized entailments.
pub struct ReasoningOverlay<'a> {
    /// Base overlay (e.g., novelty)
    base: &'a dyn OverlayProvider,
    /// Derived facts from OWL2-RL reasoning
    derived: Arc<DerivedFactsOverlay>,
    /// Combined epoch for cache invalidation
    epoch: u64,
}

impl<'a> ReasoningOverlay<'a> {
    /// Create a new reasoning overlay combining base and derived facts.
    pub fn new(base: &'a dyn OverlayProvider, derived: Arc<DerivedFactsOverlay>) -> Self {
        // Combine epochs deterministically
        let epoch = base
            .epoch()
            .wrapping_mul(1_000_003)
            .wrapping_add(derived.epoch());
        Self {
            base,
            derived,
            epoch,
        }
    }

    /// Get the derived facts overlay (for diagnostics or further processing).
    pub fn derived(&self) -> &DerivedFactsOverlay {
        &self.derived
    }
}

impl OverlayProvider for ReasoningOverlay<'_> {
    fn as_any(&self) -> &dyn std::any::Any {
        self.base.as_any()
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
        // Collect flakes from both overlays (already sorted by index order)
        let mut base_flakes: Vec<Flake> = Vec::new();
        let mut derived_flakes: Vec<Flake> = Vec::new();

        self.base
            .for_each_overlay_flake(g_id, index, first, rhs, leftmost, to_t, &mut |f| {
                base_flakes.push(f.clone());
            });

        self.derived
            .for_each_overlay_flake(g_id, index, first, rhs, leftmost, to_t, &mut |f| {
                derived_flakes.push(f.clone());
            });

        // Linear merge of two sorted sequences - O(n) instead of O(n log n)
        //
        // Note on duplicates: Derived facts use reasoning_t (to_t + 1) while base
        // facts have their original t values. Since index comparators include t,
        // true duplicates (Equal comparison) are unlikely. If they do occur (same
        // SPO with identical t), we emit base first then derived - this is correct
        // for overlay semantics where multiple versions may exist.
        let mut base_iter = base_flakes.iter().peekable();
        let mut derived_iter = derived_flakes.iter().peekable();

        loop {
            match (base_iter.peek(), derived_iter.peek()) {
                (Some(base), Some(derived)) => match index.compare(base, derived) {
                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal => {
                        callback(base_iter.next().unwrap());
                    }
                    std::cmp::Ordering::Greater => {
                        callback(derived_iter.next().unwrap());
                    }
                },
                (Some(_), None) => {
                    callback(base_iter.next().unwrap());
                }
                (None, Some(_)) => {
                    callback(derived_iter.next().unwrap());
                }
                (None, None) => break,
            }
        }
    }
}

/// Global reasoning cache for cross-query result reuse.
///
/// This uses a lazy_static pattern to maintain a shared cache across
/// queries. The cache is keyed by (ledger, epoch, time, config) so
/// results are reused when the database state hasn't changed.
pub fn global_reasoning_cache() -> &'static ReasoningCache {
    use once_cell::sync::Lazy;
    static CACHE: Lazy<ReasoningCache> = Lazy::new(ReasoningCache::with_default_capacity);
    &CACHE
}

/// Re-export reasoning types for convenience
pub use fluree_db_reasoner::{
    reason_owl2rl, DerivedFactsBuilder, FrozenSameAs, ReasoningBudget, ReasoningDiagnostics,
};

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::NoOverlay;
    use fluree_db_reasoner::FrozenSameAs;

    #[test]
    fn test_reasoning_overlay_epoch() {
        let base = NoOverlay;
        let derived = Arc::new(DerivedFactsOverlay::empty_with_metadata(
            FrozenSameAs::empty(),
            42,
        ));

        let combined = ReasoningOverlay::new(&base, derived);

        // Epoch should be deterministic and non-zero
        assert_ne!(combined.epoch(), 0);
    }

    #[test]
    fn test_global_cache_exists() {
        let cache = global_reasoning_cache();
        // Just verify the cache is accessible and has the expected capacity
        assert_eq!(cache.capacity(), 16);
    }
}
