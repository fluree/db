//! Lets the background indexer warm the query server's read cache on write.
//!
//! `BackgroundIndexerWorker` is constructed before `LedgerManager` in
//! [`FlureeBuilder::finalize_with_backend`](crate::FlureeBuilder), so the
//! worker can't capture the manager directly. The builder shares a
//! `OnceLock` cell with it instead and fills the cell once the manager is
//! built.

use std::sync::{Arc, OnceLock};

use crate::ledger_manager::LedgerManager;

/// Shared late-binding cell for the api's running `LedgerManager`.
pub(crate) type LedgerManagerCell = Arc<OnceLock<Arc<LedgerManager>>>;

/// Resolves the process-shared read cache from the api's `LedgerManager` once
/// it's constructed — the manager owns the `LeafletCache` the query server
/// reads from, so the background indexer can warm-on-write into that exact
/// cache. Yields `None` until the manager cell is filled (and always for a
/// separate-machine indexer, which has no local manager).
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct LedgerManagerWarmCache {
    pub(crate) manager: LedgerManagerCell,
}

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Debug for LedgerManagerWarmCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LedgerManagerWarmCache")
            .field("bound", &self.manager.get().is_some())
            .finish()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl fluree_db_indexer::WarmCacheSource for LedgerManagerWarmCache {
    fn warm_cache(&self) -> Option<Arc<fluree_db_binary_index::LeafletCache>> {
        self.manager.get().and_then(|m| m.leaflet_cache().cloned())
    }
}
