//! Overlay provider trait for external flake sources
//!
//! This module defines the `OverlayProvider` trait that allows external crates
//! (like `fluree-db-novelty`) to inject additional flakes at leaf resolution time
//! without `fluree-db-core` depending on novelty types.
//!
//! # Design
//!
//! The trait uses a push-based API (`for_each_overlay_flake` with a callback)
//! to avoid `Box<dyn Iterator>` allocations in the hot path.
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_core::{OverlayProvider, IndexType, GraphId, Flake};
//!
//! struct MyOverlay { /* ... */ }
//!
//! impl OverlayProvider for MyOverlay {
//!     fn epoch(&self) -> u64 { 42 }
//!
//!     fn for_each_overlay_flake(
//!         &self,
//!         g_id: GraphId,
//!         index: IndexType,
//!         first: Option<&Flake>,
//!         rhs: Option<&Flake>,
//!         leftmost: bool,
//!         to_t: i64,
//!         callback: &mut dyn FnMut(&Flake),
//!     ) {
//!         // Push flakes for the requested graph in sorted order
//!     }
//! }
//! ```

use crate::comparator::IndexType;
use crate::flake::Flake;
use crate::ids::GraphId;
use std::any::Any;
use std::collections::HashMap;

/// Identity + transaction-time span of one overlay segment.
///
/// Lets the query layer cache translated overlay ops per **immutable** segment
/// so a write burst re-translates only newly-appended segments. `seg_id` is a
/// stable, process-unique cache key (see `fluree-db-novelty`'s segment id).
/// A non-segmented overlay reports a single synthetic segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OverlaySegmentMeta {
    /// Stable, process-unique segment id. `u64::MAX` for a non-segmented
    /// overlay's single synthetic segment.
    pub seg_id: u64,
    /// Lowest transaction time in the segment (`== max_t` for a single commit).
    pub min_t: i64,
    /// Highest transaction time in the segment.
    pub max_t: i64,
}

/// Process-wide counter behind [`next_overlay_content_version`]. Starts at 1
/// so `0` can mean "empty since construction" for overlays that want it.
static NEXT_CONTENT_VERSION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Allocate a process-unique [`OverlayProvider::content_version`] stamp.
///
/// Every overlay type that reports a content version draws from this one
/// counter, which is what makes the version unique across overlay *types* —
/// a staged view and the committed novelty it will become must never share
/// one, or a cache keyed on it would serve the staged product for the
/// committed state.
pub fn next_overlay_content_version() -> u64 {
    NEXT_CONTENT_VERSION.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Remembered compositions, at most this many. Past the cap the table is
/// dropped wholesale: a composition seen again afterwards draws a fresh stamp,
/// which can only miss a cache, never alias one.
const COMPOSED_VERSIONS_CAP: usize = 4096;

static COMPOSED_VERSIONS: once_cell::sync::Lazy<parking_lot::Mutex<HashMap<Box<[u64]>, u64>>> =
    once_cell::sync::Lazy::new(|| parking_lot::Mutex::new(HashMap::new()));

/// The [`OverlayProvider::content_version`] of an overlay whose output is a
/// function of its parts' versions — a reasoning overlay over a novelty, a
/// schema bundle over a novelty, a dataset composite over several.
///
/// Two u64 stamps cannot be packed into one injectively, so compositions are
/// interned: the same ordered `parts` always map to the same stamp, and every
/// stamp comes from [`next_overlay_content_version`], so a composition can
/// never collide with a leaf overlay's version or with a different
/// composition. Ordered, because the same parts in another order describe a
/// different overlay type's output.
pub fn compose_content_version(parts: &[u64]) -> u64 {
    let mut table = COMPOSED_VERSIONS.lock();
    if let Some(&version) = table.get(parts) {
        return version;
    }
    if table.len() >= COMPOSED_VERSIONS_CAP {
        table.clear();
    }
    let version = next_overlay_content_version();
    table.insert(parts.into(), version);
    version
}

/// Overlay provider trait for external flake sources
///
/// Allows external crates to inject extra flakes at leaf resolution time
/// without core depending on novelty types.
///
/// Uses a push-based API to avoid `Box<dyn Iterator>` allocations in hot path.
pub trait OverlayProvider: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    /// Current epoch for cache key differentiation
    ///
    /// MUST be incorporated into leaf materialization cache keys.
    /// When epoch changes, cached leaf materializations are invalidated.
    fn epoch(&self) -> u64;

    /// True when this overlay is guaranteed to contribute no flakes.
    ///
    /// Unlike `epoch() == 0` (never had novelty since load), this also
    /// covers overlays drained after an index swap. Implementations must
    /// only return `true` when emptiness is certain; the conservative
    /// default keeps unknown overlays on merge-correct paths.
    fn is_effectively_empty(&self) -> bool {
        false
    }

    /// Globally-unique version stamp of this overlay's current content, for
    /// keying caches of data derived from a full overlay walk (e.g. V3
    /// overlay-op translations shared across `range_with_overlay` calls).
    ///
    /// Unlike [`Self::epoch`] — which is only unique within one overlay
    /// instance's lineage — implementations must guarantee that **no two
    /// overlays whose `for_each_overlay_flake` output differs ever report
    /// the same version**, across instances, clones, and overlay types.
    /// Return `None` (the default) when no such guarantee exists; callers
    /// must then skip caching and derive from a fresh walk.
    fn content_version(&self) -> Option<u64> {
        None
    }

    /// Push overlay flakes for a leaf's range to the callback
    ///
    /// # Arguments
    ///
    /// * `g_id` - Graph to return flakes for (per-graph partitioning)
    /// * `index` - Which index ordering to use
    /// * `first` - Left boundary of the range (or None for start)
    /// * `rhs` - Right boundary of the range (or None for end)
    /// * `leftmost` - If true, include flakes from the start; if false, exclude `first`
    /// * `to_t` - Maximum transaction time to include
    /// * `callback` - Function called for each flake in the range
    ///
    /// # Ordering Requirements
    ///
    /// Flakes MUST be yielded in order matching the index's comparator.
    ///
    /// # Time Filtering
    ///
    /// Overlay applies `to_t` filter (avoids emitting irrelevant flakes).
    /// Core applies `from_t` filter + stale-removal.
    ///
    /// # Boundary Semantics (compatibility)
    ///
    /// * If `leftmost=false`: left boundary is EXCLUSIVE (`> first`)
    /// * If `leftmost=true`: no left boundary (start from beginning)
    /// * `rhs` is INCLUSIVE when present (`<= rhs`)
    #[allow(clippy::too_many_arguments)]
    fn for_each_overlay_flake(
        &self,
        g_id: GraphId,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
        to_t: i64,
        callback: &mut dyn FnMut(&Flake),
    );

    /// Number of overlay flakes for `g_id`, unfiltered by `to_t`, when the
    /// implementation can answer in O(segments) or better.
    ///
    /// Used as the denominator of selectivity heuristics (e.g. the bounded
    /// overlay walk's fallback guard in `fluree-db-query`), so `None` (the
    /// default) means "unknown — skip the heuristic": callers must never walk
    /// the overlay to count. Because the count is `to_t`-unfiltered while a
    /// bounded walk's matches are filtered, a time-travel query under-states
    /// its match share — which only ever biases the guard toward staying
    /// bounded, never toward a spurious fallback.
    fn overlay_flake_count(&self, _g_id: GraphId) -> Option<usize> {
        None
    }

    /// Segment metadata for `g_id`, in segment (commit) order.
    ///
    /// Enables a per-segment translation cache (only newly-appended segments
    /// re-translate on a write burst; older immutable segments are cache hits).
    /// **Default:** one synthetic segment spanning the whole overlay — correct
    /// but with no per-segment reuse, for non-segmented overlays (reasoner,
    /// schema bundle, …).
    fn overlay_segments(&self, _g_id: GraphId) -> Vec<OverlaySegmentMeta> {
        vec![OverlaySegmentMeta {
            seg_id: u64::MAX,
            min_t: i64::MIN,
            max_t: i64::MAX,
        }]
    }

    /// Push the flakes of segment `seg_id` for `(g_id, index)` in comparator
    /// order, **without** a `to_t` filter (the query layer caches the whole
    /// segment's translation and applies `to_t` + the cursor key window after
    /// the k-way merge). **Default:** the whole overlay (the single synthetic
    /// segment), so non-segmented overlays stay correct.
    ///
    /// `seg_idx` is the segment's position in [`Self::overlay_segments`] order;
    /// segmented overlays use it for an O(1) lookup (avoiding a linear scan by
    /// `seg_id` per segment). `seg_id` remains the stable identity the caller
    /// keys its translation cache on. Callers must pass them from the same
    /// `overlay_segments` enumeration so they stay aligned.
    fn for_each_overlay_segment_flake(
        &self,
        g_id: GraphId,
        _seg_id: u64,
        _seg_idx: usize,
        index: IndexType,
        callback: &mut dyn FnMut(&Flake),
    ) {
        self.for_each_overlay_flake(g_id, index, None, None, true, i64::MAX, callback);
    }
}

/// Null overlay - no extra flakes
///
/// Use this when no novelty overlay is needed (e.g., for pure index queries).
#[derive(Debug, Clone, Copy, Default)]
pub struct NoOverlay;

impl OverlayProvider for NoOverlay {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn epoch(&self) -> u64 {
        0
    }

    fn is_effectively_empty(&self) -> bool {
        true
    }

    /// `0` is the stamp every never-mutated novelty reports; sharing it is
    /// allowed because both outputs are empty.
    fn content_version(&self) -> Option<u64> {
        Some(0)
    }

    fn for_each_overlay_flake(
        &self,
        _g_id: GraphId,
        _index: IndexType,
        _first: Option<&Flake>,
        _rhs: Option<&Flake>,
        _leftmost: bool,
        _to_t: i64,
        _callback: &mut dyn FnMut(&Flake),
    ) {
        // No-op: no overlay flakes
    }

    fn overlay_segments(&self, _g_id: GraphId) -> Vec<OverlaySegmentMeta> {
        // Empty overlay → no segments (not the default synthetic one).
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sid::Sid;
    use crate::value::FlakeValue;

    /// Test overlay that stores a fixed set of flakes
    struct TestOverlay {
        flakes: Vec<Flake>,
        epoch: u64,
    }

    impl OverlayProvider for TestOverlay {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn epoch(&self) -> u64 {
            self.epoch
        }

        fn for_each_overlay_flake(
            &self,
            _g_id: GraphId,
            _index: IndexType,
            _first: Option<&Flake>,
            _rhs: Option<&Flake>,
            _leftmost: bool,
            to_t: i64,
            callback: &mut dyn FnMut(&Flake),
        ) {
            for flake in &self.flakes {
                if flake.t <= to_t {
                    callback(flake);
                }
            }
        }
    }

    fn make_flake(s: u16, t: i64) -> Flake {
        Flake::new(
            Sid::new(s, format!("s{s}")),
            Sid::new(1, "p"),
            FlakeValue::Long(100),
            Sid::new(2, "long"),
            t,
            true,
            None,
        )
    }

    #[test]
    fn composed_content_version_is_stable_ordered_and_never_a_leaf_stamp() {
        let a = next_overlay_content_version();
        let b = next_overlay_content_version();

        let ab = compose_content_version(&[a, b]);
        assert_eq!(
            ab,
            compose_content_version(&[a, b]),
            "same parts, same stamp"
        );
        assert_ne!(ab, compose_content_version(&[b, a]), "order is identity");
        assert_ne!(ab, compose_content_version(&[a, b, b]), "arity is identity");
        assert!(ab != a && ab != b, "a composition is not one of its parts");

        // Leaf stamps drawn before and after can never equal the composition.
        let c = next_overlay_content_version();
        assert!(ab != c && ab < c);
    }

    #[test]
    fn test_no_overlay() {
        let overlay = NoOverlay;
        assert_eq!(overlay.epoch(), 0);
        assert_eq!(overlay.content_version(), Some(0));

        let mut count = 0;
        overlay.for_each_overlay_flake(0, IndexType::Spot, None, None, true, 100, &mut |_| {
            count += 1;
        });
        assert_eq!(count, 0);
    }

    #[test]
    fn test_overlay_callback() {
        let overlay = TestOverlay {
            flakes: vec![make_flake(1, 1), make_flake(2, 2), make_flake(3, 3)],
            epoch: 42,
        };

        assert_eq!(overlay.epoch(), 42);

        let mut collected = Vec::new();
        overlay.for_each_overlay_flake(0, IndexType::Spot, None, None, true, 100, &mut |f| {
            collected.push(f.s.namespace_code);
        });
        assert_eq!(collected, vec![1, 2, 3]);
    }

    #[test]
    fn test_overlay_time_filtering() {
        let overlay = TestOverlay {
            flakes: vec![make_flake(1, 1), make_flake(2, 2), make_flake(3, 3)],
            epoch: 1,
        };

        let mut collected = Vec::new();
        overlay.for_each_overlay_flake(
            0,
            IndexType::Spot,
            None,
            None,
            true,
            2, // Only include t <= 2
            &mut |f| collected.push(f.s.namespace_code),
        );
        assert_eq!(collected, vec![1, 2]);
    }
}
