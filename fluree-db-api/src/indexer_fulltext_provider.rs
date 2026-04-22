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
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::ContentStore;
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
        if let Some(index_cid) = state
            .ns_record
            .as_ref()
            .and_then(|r| r.index_head_id.as_ref())
            .cloned()
        {
            let ns_ledger_id = state
                .ns_record
                .as_ref()
                .map(|r| r.ledger_id.as_str())
                .unwrap_or(state.snapshot.ledger_id.as_str());
            let cs = self.backend.content_store(ns_ledger_id);
            let bytes = cs
                .get(&index_cid)
                .await
                .map_err(|e| format!("read binary index root {index_cid}: {e}"))?;
            let cache_dir = std::env::temp_dir().join("fluree-cache");
            let mut binary_index_store = BinaryIndexStore::load_from_root_bytes(
                Arc::clone(&cs),
                &bytes,
                &cache_dir,
                Some(Arc::clone(&self.leaflet_cache)),
            )
            .await
            .map_err(|e| format!("load binary index store: {e}"))?;
            crate::ns_helpers::sync_store_and_snapshot_ns(
                &mut binary_index_store,
                &mut state.snapshot,
            )
            .map_err(|e| format!("sync ns: {e}"))?;
            let arc_store = Arc::new(binary_index_store);
            state.binary_store = Some(crate::TypeErasedStore(arc_store.clone()));
            let ns_fallback = Some(Arc::new(state.snapshot.namespaces().clone()));
            let provider = fluree_db_query::BinaryRangeProvider::new(
                Arc::clone(&arc_store),
                state.dict_novelty.clone(),
                state.runtime_small_dicts.clone(),
                ns_fallback,
            );
            state.snapshot.range_provider = Some(Arc::new(provider));
        }

        // 3. Resolve `f:fullTextDefaults` against the loaded state.
        let overlay: &dyn fluree_db_core::OverlayProvider = &*state.novelty;
        let ledger_config = crate::config_resolver::resolve_ledger_config(
            &state.snapshot,
            overlay,
            state.snapshot.t,
        )
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
