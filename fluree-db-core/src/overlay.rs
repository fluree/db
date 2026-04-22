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
    fn test_no_overlay() {
        let overlay = NoOverlay;
        assert_eq!(overlay.epoch(), 0);

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
