//! Derived facts overlay for OWL2-RL reasoning results
//!
//! This module provides the `DerivedFactsOverlay` type that stores materialized
//! facts from OWL2-RL reasoning and implements `OverlayProvider` for query-time use.
//!
//! # Design
//!
//! - Flakes are stored once, sorted in SPOT order; the other three index
//!   orders are permutations of that array (`u32` positions), so a derived
//!   fact costs one `Flake` plus twelve bytes rather than four `Flake`s.
//!   Range lookups binary-search the permutation with the index comparator.
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
    /// Flakes sorted by SPOT index order — the only materialized copy.
    spot: Arc<[Flake]>,
    /// Positions into `spot`, ordered by the PSOT comparator.
    psot: Arc<[u32]>,
    /// Positions into `spot`, ordered by the POST comparator.
    post: Arc<[u32]>,
    /// Positions into `spot`, ordered by the OPST comparator.
    opst: Arc<[u32]>,
    /// owl:sameAs equivalence classes
    same_as: FrozenSameAs,
    /// Epoch for cache key differentiation
    epoch: u64,
    /// Process-unique id assigned at construction (shared by clones), drawn
    /// from the overlay-wide content-version allocator so it doubles as this
    /// immutable overlay's [`OverlayProvider::content_version`].
    ///
    /// The `epoch` alone cannot distinguish two materializations built at the
    /// same base-overlay epoch under different rule configs / ontologies —
    /// caches keyed on overlay identity must incorporate this id.
    instance_id: u64,
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
            instance_id: fluree_db_core::overlay::next_overlay_content_version(),
        }
    }

    /// Create an overlay from flakes in any order.
    ///
    /// The flakes are sorted into SPOT order once; the PSOT / POST / OPST
    /// orders are derived as position permutations over that array.
    ///
    /// # Arguments
    ///
    /// * `flakes` - Derived flakes (deduplicated by the caller)
    /// * `same_as` - Frozen sameAs equivalence classes
    /// * `epoch` - Epoch for cache differentiation
    pub fn new(mut flakes: Vec<Flake>, same_as: FrozenSameAs, epoch: u64) -> Self {
        assert!(
            flakes.len() <= u32::MAX as usize,
            "derived-facts overlay exceeds u32 positions"
        );
        flakes.sort_by(|a, b| IndexType::Spot.comparator()(a, b));
        let spot: Arc<[Flake]> = flakes.into();
        let permutation = |index: IndexType| -> Arc<[u32]> {
            let cmp = index.comparator();
            let mut order: Vec<u32> = (0..spot.len() as u32).collect();
            order.sort_by(|a, b| cmp(&spot[*a as usize], &spot[*b as usize]));
            order.into()
        };
        let psot = permutation(IndexType::Psot);
        let post = permutation(IndexType::Post);
        let opst = permutation(IndexType::Opst);
        Self {
            spot,
            psot,
            post,
            opst,
            same_as,
            epoch,
            instance_id: fluree_db_core::overlay::next_overlay_content_version(),
        }
    }

    /// Process-unique identity of this materialization (stable across clones).
    pub fn instance_id(&self) -> u64 {
        self.instance_id
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

    /// The derived flakes in SPOT order.
    pub fn flakes_spot(&self) -> &[Flake] {
        &self.spot
    }

    /// Iterate the derived flakes in the given index order.
    pub fn iter(&self, index: IndexType) -> impl Iterator<Item = &Flake> + '_ {
        let view = self.view(index);
        (0..view.len()).map(move |i| view.get(i))
    }

    /// Positional view of the flakes in one index order.
    fn view(&self, index: IndexType) -> OrderView<'_> {
        let order = match index {
            IndexType::Spot => None,
            IndexType::Psot => Some(&*self.psot),
            IndexType::Post => Some(&*self.post),
            IndexType::Opst => Some(&*self.opst),
        };
        OrderView {
            spot: &self.spot,
            order,
        }
    }

    /// Binary search for the first flake > target in the given index
    fn upper_bound(&self, index: IndexType, target: &Flake) -> usize {
        let view = self.view(index);
        let cmp = index.comparator();
        // partition_point over positions [0, len): the ordered predicate
        // "flake at position <= target" is monotone in every index order.
        let (mut lo, mut hi) = (0usize, view.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if cmp(view.get(mid), target).is_le() {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
}

/// Read-only view of the SPOT array through one index order's permutation
/// (`None` = SPOT itself, the identity order).
#[derive(Clone, Copy)]
struct OrderView<'a> {
    spot: &'a [Flake],
    order: Option<&'a [u32]>,
}

impl<'a> OrderView<'a> {
    fn len(&self) -> usize {
        self.spot.len()
    }

    fn get(&self, i: usize) -> &'a Flake {
        match self.order {
            None => &self.spot[i],
            Some(order) => &self.spot[order[i] as usize],
        }
    }
}

impl OverlayProvider for DerivedFactsOverlay {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn epoch(&self) -> u64 {
        self.epoch
    }

    fn content_version(&self) -> Option<u64> {
        Some(self.instance_id)
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

        let view = self.view(index);
        if view.len() == 0 {
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
            view.len()
        };

        // Emit flakes in range, filtered by to_t
        for i in start..end {
            let flake = view.get(i);
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
        // Preserves same_as and epoch even with zero derived flakes.
        DerivedFactsOverlay::new(self.flakes, same_as, epoch)
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

    /// Every index order must enumerate the same flakes as a fully sorted
    /// copy would, and range bounds must resolve identically through the
    /// permutation — the property the four-copies layout used to give for
    /// free.
    #[test]
    fn permuted_orders_match_fully_sorted_copies() {
        let mut builder = DerivedFactsBuilder::new();
        // Deliberately unsorted, with repeated predicates and objects so the
        // non-SPOT orders differ from SPOT.
        for (s, p, o) in [
            (3, 2, 5),
            (1, 2, 9),
            (2, 1, 5),
            (1, 1, 7),
            (3, 1, 9),
            (2, 2, 7),
        ] {
            builder.push(make_flake(s, p, o, 1));
        }
        let overlay = builder.build(FrozenSameAs::empty(), 1);

        for index in [
            IndexType::Spot,
            IndexType::Psot,
            IndexType::Post,
            IndexType::Opst,
        ] {
            let mut expected: Vec<Flake> = overlay.flakes_spot().to_vec();
            expected.sort_by(|a, b| index.comparator()(a, b));

            let via_iter: Vec<Flake> = overlay.iter(index).cloned().collect();
            assert_eq!(via_iter, expected, "iter order for {index:?}");

            let mut via_scan = Vec::new();
            overlay.for_each_overlay_flake(0, index, None, None, true, i64::MAX, &mut |f| {
                via_scan.push(f.clone());
            });
            assert_eq!(via_scan, expected, "full scan order for {index:?}");

            // A bounded scan (exclusive left = second flake, inclusive right =
            // fourth flake) must yield exactly the sorted slice (2..=3].
            let mut bounded = Vec::new();
            overlay.for_each_overlay_flake(
                0,
                index,
                Some(&expected[1]),
                Some(&expected[3]),
                false,
                i64::MAX,
                &mut |f| bounded.push(f.clone()),
            );
            assert_eq!(
                bounded,
                expected[2..=3].to_vec(),
                "bounded scan for {index:?}"
            );
        }
    }
}
