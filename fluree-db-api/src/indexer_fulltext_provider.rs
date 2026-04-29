//! API-side `FulltextConfigProvider` implementation.
//!
//! Wires the indexer's per-build config refresh hook to the ledger's live
//! `f:fullTextDefaults`. Used by background / CLI incremental indexing so
//! those paths keep collecting configured plain-string values after the
//! first reindex that picked up the config.
//!
//! Config changes still require a manual reindex to take full effect on
//! existing data (see `docs/indexing-and-search/fulltext.md` §"Reindexing
//! after a config change"). What this provider guarantees is that **once**
//! a property is configured and an index exists, new commits adding values
//! on that property continue to flow into the arena during incremental
//! updates. Without this, only the one-shot reindex would pick up config
//! and subsequent incremental runs would silently stop collecting
//! configured values.

use std::sync::Arc;

use async_trait::async_trait;
use fluree_db_indexer::{ConfiguredFulltextProperty, FulltextConfigProvider};
use fluree_db_ledger::LedgerState;
use fluree_db_nameservice::NameService;

use crate::StorageBackend;

/// Provider backed by this connection's storage + nameservice. Loads the
/// target ledger on each call and resolves `f:fullTextDefaults` against
/// the current state. Failures log + return empty — a bad config read
/// should never block indexing.
pub(crate) struct ApiFulltextConfigProvider {
    pub(crate) backend: StorageBackend,
    pub(crate) nameservice: Arc<dyn NameService>,
    pub(crate) leaflet_cache: Arc<fluree_db_binary_index::LeafletCache>,
    pub(crate) cache_dir: std::path::PathBuf,
}

impl std::fmt::Debug for ApiFulltextConfigProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiFulltextConfigProvider").finish()
    }
}

impl ApiFulltextConfigProvider {
    async fn resolve(&self, ledger_id: &str) -> Result<Vec<ConfiguredFulltextProperty>, String> {
        // 1. Load ledger state (snapshot + novelty).
        let mut state = LedgerState::load(self.nameservice.as_ref(), ledger_id, &self.backend)
            .await
            .map_err(|e| format!("LedgerState::load: {e}"))?;

        // 2. If an index exists, load the binary store so the config graph
        //    can be read via the indexed side too. Without this, only
        //    overlay/novelty entries would be visible — fine for configs
        //    committed since the last index, wrong for configs that have
        //    already been indexed.
        crate::ledger_manager::load_and_attach_binary_store(
            &self.backend,
            self.nameservice.as_ref(),
            &mut state,
            &self.cache_dir,
            Some(Arc::clone(&self.leaflet_cache)),
        )
        .await
        .map_err(|e| format!("load binary index store: {e}"))?;

        // 3. Resolve `f:fullTextDefaults` against the loaded state.
        //
        // Use `state.t()` (= max(novelty.t, snapshot.t)) as the upper bound so
        // config flakes committed since the last index are visible. On a
        // first-ever build there is no index, so `snapshot.t == 0` and all
        // config lives in novelty — querying at `snapshot.t` would filter it
        // out (`Novelty::for_each_overlay_flake` keeps only `flake.t <= to_t`).
        let overlay: &dyn fluree_db_core::OverlayProvider = &*state.novelty;
        let to_t = state.t();
        let ledger_config =
            crate::config_resolver::resolve_ledger_config(&state.snapshot, overlay, to_t)
                .await
                .map_err(|e| format!("resolve_ledger_config: {e}"))?;

        Ok(ledger_config
            .map(|cfg| crate::config_resolver::configured_fulltext_properties_for_indexer(&cfg))
            .unwrap_or_default())
    }
}

#[async_trait]
impl FulltextConfigProvider for ApiFulltextConfigProvider {
    async fn fulltext_configured_properties(
        &self,
        ledger_id: &str,
    ) -> Vec<ConfiguredFulltextProperty> {
        match self.resolve(ledger_id).await {
            Ok(list) => list,
            Err(e) => {
                tracing::warn!(
                    ledger_id,
                    error = %e,
                    "fulltext config provider failed; continuing without configured properties \
                     (@fulltext datatype path still works)"
                );
                Vec::new()
            }
        }
    }
}
