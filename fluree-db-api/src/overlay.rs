//! Overlay utilities for formatting/query results.
//!
//! Currently used to support dataset (multi-ledger) hydration formatting by
//! composing multiple novelty overlays into a single `OverlayProvider`.

use fluree_db_core::{Flake, GraphId, IndexType, OverlayProvider};

/// Composite overlay that merges multiple overlay providers.
///
/// Used when hydration formatting needs to "see" unindexed flakes from multiple ledgers
/// (dataset mode). This is a correctness-first implementation intended for tests and
/// low-volume usage: it collects flakes from each overlay, sorts them, then streams them
/// to the callback.
pub struct CompositeOverlay {
    epoch: u64,
    overlays: Vec<std::sync::Arc<dyn OverlayProvider>>,
}

impl CompositeOverlay {
    pub fn new(overlays: Vec<std::sync::Arc<dyn OverlayProvider>>) -> Self {
        // Deterministic combined epoch (cheap and stable).
        let mut epoch = 0u64;
        for o in &overlays {
            epoch = epoch.wrapping_mul(1_000_003).wrapping_add(o.epoch());
        }
        Self { epoch, overlays }
    }
}

impl OverlayProvider for CompositeOverlay {
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
        if self.overlays.is_empty() {
            return;
        }

        let mut flakes: Vec<Flake> = Vec::new();
        for overlay in &self.overlays {
            overlay.for_each_overlay_flake(g_id, index, first, rhs, leftmost, to_t, &mut |f| {
                flakes.push(f.clone());
            });
        }

        flakes.sort_by(|a, b| index.compare(a, b));
        for flake in &flakes {
            callback(flake);
        }
    }
}
