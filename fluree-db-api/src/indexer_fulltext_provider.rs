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

        // [DIAG] Full ledger-state shape right after load, before we touch
        // the binary index. Tells us whether the ns lookup saw the current
        // head and whether the snapshot is at `t=0` (genesis) vs. some
        // prior index_t. Remove once the Solo c3000-04 "provider returned
        // empty" bug is diagnosed.
        let ns_index_head_present = state
            .ns_record
            .as_ref()
            .and_then(|r| r.index_head_id.as_ref())
            .is_some();
        let ns_commit_t = state.ns_record.as_ref().map(|r| r.commit_t);
        let ns_index_t = state.ns_record.as_ref().map(|r| r.index_t);
        tracing::info!(
            ledger_id = ledger_id,
            snapshot_t = state.snapshot.t,
            state_t = state.t(),
            novelty_t = state.novelty.t,
            ns_commit_t = ?ns_commit_t,
            ns_index_t = ?ns_index_t,
            ns_index_head_present,
            "[DIAG] provider.resolve: loaded ledger state"
        );

        // 2. If an index exists, load the binary store so the config graph
        //    can be read via the indexed side too. Without this, only
        //    overlay/novelty entries would be visible — fine for configs
        //    committed since the last index, wrong for configs that have
        //    already been indexed.
        let mut binary_index_loaded = false;
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
            binary_index_loaded = true;
        }

        // 3. Resolve `f:fullTextDefaults` against the loaded state.
        //
        // Use `state.t()` (= max(novelty.t, snapshot.t)) as the upper bound so
        // config flakes committed since the last index are visible. On a
        // first-ever build there is no index, so `snapshot.t == 0` and all
        // config lives in novelty — querying at `snapshot.t` would filter it
        // out (`Novelty::for_each_overlay_flake` keeps only `flake.t <= to_t`).
        let overlay: &dyn fluree_db_core::OverlayProvider = &*state.novelty;
        let to_t = state.t();
        // [DIAG] Probe the overlay itself for config-graph activity so we
        // can tell "the novelty has no config flakes" from "the novelty
        // has them but resolve_ledger_config can't find them." Counts
        // flakes at (g_id=CONFIG_GRAPH_ID=2, index=Post, t <= to_t). Expect
        // non-zero when config was committed into novelty (no prior index
        // case) and zero once it's been indexed (post-first-reindex case).
        {
            use fluree_db_core::{IndexType, CONFIG_GRAPH_ID};
            let mut overlay_count: usize = 0;
            overlay.for_each_overlay_flake(
                CONFIG_GRAPH_ID,
                IndexType::Post,
                None,
                None,
                true,
                to_t,
                &mut |_flake| {
                    overlay_count += 1;
                },
            );
            tracing::info!(
                ledger_id = ledger_id,
                to_t,
                overlay_flakes_in_config_graph = overlay_count,
                binary_index_loaded,
                "[DIAG] provider.resolve: overlay/novelty config-graph probe"
            );

            // [DIAG] Dump the graph registry + a flake-count probe for
            // EVERY registered graph. If Solo wrote config to a graph IRI
            // that doesn't match the system-expected
            // `urn:fluree:{ledger_id}#config`, apply_delta assigned it a
            // fresh user-space g_id (>= 3) and the CONFIG_GRAPH_ID=2
            // probe above will be 0 while some OTHER g_id has all the
            // config flakes. Remove after the Solo c3000-04 "provider
            // returned empty" bug is diagnosed.
            let registry_entries: Vec<(fluree_db_core::GraphId, String)> = state
                .snapshot
                .graph_registry
                .iter_entries()
                .map(|(g_id, iri)| (g_id, iri.to_string()))
                .collect();
            tracing::info!(
                ledger_id = ledger_id,
                expected_config_iri = %fluree_db_core::graph_registry::config_graph_iri(ledger_id),
                registry_entry_count = registry_entries.len(),
                registry = ?registry_entries,
                "[DIAG] provider.resolve: graph_registry contents"
            );
            for (g_id, iri) in &registry_entries {
                let mut per_graph_count: usize = 0;
                overlay.for_each_overlay_flake(
                    *g_id,
                    IndexType::Post,
                    None,
                    None,
                    true,
                    to_t,
                    &mut |_flake| {
                        per_graph_count += 1;
                    },
                );
                if per_graph_count > 0 {
                    tracing::info!(
                        g_id = *g_id,
                        iri = %iri,
                        flake_count = per_graph_count,
                        "[DIAG] provider.resolve: per-graph overlay probe (non-empty)"
                    );
                }
            }
            // Also probe the default graph (g_id=0), which is NOT in
            // graph_registry's iter_entries (registry only covers named
            // graphs). If config flakes landed there, the writer sent
            // them with `flake.g = None`.
            let mut default_count: usize = 0;
            overlay.for_each_overlay_flake(
                fluree_db_core::DEFAULT_GRAPH_ID,
                IndexType::Post,
                None,
                None,
                true,
                to_t,
                &mut |_flake| {
                    default_count += 1;
                },
            );
            tracing::info!(
                g_id = fluree_db_core::DEFAULT_GRAPH_ID,
                flake_count = default_count,
                "[DIAG] provider.resolve: default graph overlay probe"
            );
        }
        let ledger_config =
            crate::config_resolver::resolve_ledger_config(&state.snapshot, overlay, to_t)
                .await
                .map_err(|e| format!("resolve_ledger_config: {e}"))?;

        // [DIAG] Shape of what resolve_ledger_config returned, before the
        // per-indexer shape transform. Tells us whether we got a
        // `LedgerConfig` at all, whether `full_text` was set, and how many
        // properties the config carried. Remove after the Solo c3000-04
        // "provider returned empty" bug is diagnosed.
        let config_present = ledger_config.is_some();
        let full_text_present = ledger_config
            .as_ref()
            .and_then(|cfg| cfg.full_text.as_ref())
            .is_some();
        let ledger_wide_property_count = ledger_config
            .as_ref()
            .and_then(|cfg| cfg.full_text.as_ref())
            .map(|ft| ft.properties.len())
            .unwrap_or(0);
        let graph_override_count = ledger_config
            .as_ref()
            .map(|cfg| cfg.graph_overrides.len())
            .unwrap_or(0);
        let result: Vec<ConfiguredFulltextProperty> = ledger_config
            .map(|cfg| crate::config_resolver::configured_fulltext_properties_for_indexer(&cfg))
            .unwrap_or_default();
        tracing::info!(
            ledger_id = ledger_id,
            to_t,
            binary_index_loaded,
            config_present,
            full_text_present,
            ledger_wide_property_count,
            graph_override_count,
            emitted_property_count = result.len(),
            "[DIAG] provider.resolve: resolve_ledger_config result"
        );

        Ok(result)
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
