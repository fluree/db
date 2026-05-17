//! Fluree extension methods for view construction
//!
//! Provides convenience methods on `Fluree` for loading and wrapping views.

use std::sync::Arc;

use chrono::DateTime;

use crate::view::{GraphDb, ReasoningModePrecedence};
use crate::{
    config_resolver, time_resolve, ApiError, Fluree, QueryConnectionOptions, Result, TimeSpec,
};
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::ids::GraphId;
use fluree_db_core::{ContentStore, DictNovelty, IndexType, DEFAULT_GRAPH_ID, TXN_META_GRAPH_ID};
use fluree_db_query::ir::ReasoningModes;
use fluree_db_query::BinaryRangeProvider;

// ============================================================================
// View Loading
// ============================================================================

/// Reference to a named graph, parsed from a fragment but not yet resolved to g_id.
#[derive(Debug)]
enum GraphRef {
    /// Default graph (g_id = 0)
    Default,
    /// Transaction metadata graph (g_id = 1)
    TxnMeta,
    /// User-defined named graph by exact IRI
    Named(String),
}

impl Fluree {
    /// Split a graph reference like `ledger:main#txn-meta` into (ledger_id, graph_ref).
    ///
    /// Accepts both alias form (`mydb:main#txn-meta`) and full IRI form
    /// (`urn:fluree:mydb:main#txn-meta`). The `urn:fluree:` prefix is stripped
    /// so the ledger_id always matches the nameservice alias.
    ///
    /// Supported fragments:
    /// - *(none)* → default graph (g_id = 0)
    /// - `#txn-meta` → txn metadata graph (g_id = 1)
    /// - `#<iri>` → user-defined named graph by exact IRI
    fn parse_graph_ref(ledger_id: &str) -> Result<(&str, GraphRef)> {
        // Strip urn:fluree: prefix so full IRIs resolve to the same ledger alias.
        let ledger_id = ledger_id.strip_prefix("urn:fluree:").unwrap_or(ledger_id);

        match ledger_id.split_once('#') {
            None => Ok((ledger_id, GraphRef::Default)),
            Some((ledger_id, frag)) => {
                if ledger_id.is_empty() {
                    return Err(ApiError::query("Missing ledger before '#'"));
                }
                if frag.is_empty() {
                    return Err(ApiError::query("Missing named graph after '#'"));
                }
                match frag {
                    "txn-meta" => Ok((ledger_id, GraphRef::TxnMeta)),
                    // Any other fragment is treated as a graph IRI (exact match).
                    other => Ok((ledger_id, GraphRef::Named(other.to_string()))),
                }
            }
        }
    }

    /// Apply a graph selection to a loaded view.
    ///
    /// Resolves the `GraphRef` to a concrete g_id, then re-scopes the view's
    /// `Db.range_provider` and sets `view.graph_id` so both range queries
    /// and binary scans use the same graph.
    fn select_graph(mut view: GraphDb, graph_ref: GraphRef) -> Result<GraphDb> {
        let g_id: GraphId = match graph_ref {
            GraphRef::Default => DEFAULT_GRAPH_ID,
            GraphRef::TxnMeta => TXN_META_GRAPH_ID,
            GraphRef::Named(iri) => view
                .snapshot
                .graph_registry
                .graph_id_for_iri(&iri)
                // Fallback for safety: if registry is missing an entry but a binary store
                // has it (should not happen in a consistent ledger), use the store.
                .or_else(|| {
                    view.binary_store
                        .as_ref()
                        .and_then(|s| s.graph_id_for_iri(&iri))
                })
                .ok_or_else(|| ApiError::query(format!("Unknown named graph '#{iri}'")))?,
        };

        if g_id != DEFAULT_GRAPH_ID && view.binary_store.is_some() && view.dict_novelty.is_some() {
            let store = view.binary_store.clone().unwrap();
            let dict_novelty = view.dict_novelty.clone().unwrap();
            let runtime_small_dicts = view.runtime_small_dicts.clone().unwrap_or_else(|| {
                crate::runtime_dicts::build_runtime_small_dicts(&store, view.novelty.as_ref())
            });
            let ns_fallback = Some(Arc::new(view.snapshot.namespaces().clone()));
            let provider =
                BinaryRangeProvider::new(store, dict_novelty, runtime_small_dicts, ns_fallback);
            let mut db = (*view.snapshot).clone();
            db.range_provider = Some(Arc::new(provider));
            view.snapshot = Arc::new(db);
        }

        Ok(view.with_graph_id(g_id))
    }

    /// Read the config graph (g_id=2) and attach effective config to the view.
    ///
    /// This is called after graph selection so the resolved config reflects
    /// the correct per-graph overrides. Returns the view unchanged if the
    /// config graph is empty.
    ///
    /// Note: Reasoning defaults are NOT applied here — they are applied at
    /// the request boundary via `config_resolver::merge_reasoning()` which
    /// respects override control and server-verified identity.
    pub(crate) async fn resolve_and_attach_config(&self, view: GraphDb) -> Result<GraphDb> {
        // Config reads are best-effort. If the config graph is unqueryable
        // (e.g., historical snapshot without a range_provider for g_id=2),
        // treat it as "no config" and apply system defaults.
        let config =
            match config_resolver::resolve_ledger_config(&view.snapshot, &*view.overlay, view.t)
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::debug!(error = %e, "Config graph read failed — using system defaults");
                    return Ok(view);
                }
            };

        let config = match config {
            Some(c) => Arc::new(c),
            None => return Ok(view),
        };

        // Resolve effective config for this view's graph
        let graph_iri = if view.graph_id == DEFAULT_GRAPH_ID {
            None
        } else {
            view.snapshot.graph_registry.iri_for_graph_id(view.graph_id)
        };
        let resolved = config_resolver::resolve_effective_config(&config, graph_iri);

        Ok(view
            .with_ledger_config(config)
            .with_resolved_config(resolved))
    }

    /// Load the current view (immutable snapshot) from a ledger.
    ///
    /// Uses the connection-level ledger cache when available (check cache first,
    /// load + cache if not present). Falls back to a fresh load when caching
    /// is disabled.
    ///
    /// This is the internal loading method. For the public API, use
    /// [`graph()`](Self::graph) which returns a lazy [`Graph`](crate::Graph) handle.
    pub(crate) async fn load_graph_db_impl(
        &self,
        ledger_id: &str,
        include_default_context: bool,
    ) -> Result<GraphDb> {
        let handle = self.ledger_cached(ledger_id).await?;
        let mut snapshot = handle.snapshot().await;

        // If no binary store attached but nameservice has an index address,
        // load the BinaryIndexStore and attach BinaryRangeProvider.
        // This handles the non-cached path (FlureeBuilder::file() without ledger_manager).
        if snapshot.binary_store.is_none() {
            if let Some(index_cid) = snapshot
                .ns_record
                .as_ref()
                .and_then(|r| r.index_head_id.as_ref())
                .cloned()
            {
                // Branch-aware store so leaf/branch/history blobs inherited
                // from the source branch namespace resolve on a fresh branch.
                let cs = self
                    .content_store_for_record_or_id(
                        snapshot.ns_record.as_ref(),
                        &snapshot.snapshot.ledger_id,
                    )
                    .await?;
                let bytes = cs
                    .get(&index_cid)
                    .await
                    .map_err(|e| ApiError::internal(format!("read index root: {e}")))?;
                let cache_dir = std::env::temp_dir().join("fluree-cache");
                let mut store = BinaryIndexStore::load_from_root_bytes(
                    cs,
                    &bytes,
                    &cache_dir,
                    Some(Arc::clone(&self.leaflet_cache)),
                )
                .await
                .map_err(|e| ApiError::internal(format!("load binary index: {e}")))?;

                // Sync namespace codes between store and snapshot (bimap validation).
                crate::ns_helpers::sync_store_and_snapshot_ns(&mut store, &mut snapshot.snapshot)?;

                let arc_store = Arc::new(store);
                let dn = snapshot.dict_novelty.clone();
                let runtime_small_dicts = crate::runtime_dicts::build_runtime_small_dicts(
                    &arc_store,
                    Some(&snapshot.novelty),
                );
                let ns_fallback = Some(Arc::new(snapshot.snapshot.namespaces().clone()));
                let provider = BinaryRangeProvider::new(
                    Arc::clone(&arc_store),
                    dn,
                    Arc::clone(&runtime_small_dicts),
                    ns_fallback,
                );
                snapshot.snapshot.range_provider = Some(Arc::new(provider));
                snapshot.binary_store = Some(arc_store);
                snapshot.runtime_small_dicts = runtime_small_dicts;
            }
        }

        // Default context is loaded lazily on the opt-in path only. The
        // plain `db()` route returns a view with `default_context = None`
        // so query parsing sees no auto-injection unless the caller went
        // through `db_with_default_context`.
        let default_context = if include_default_context {
            match snapshot.ns_record.as_ref() {
                Some(record) => self.load_default_context_blob(record).await.ok().flatten(),
                None => None,
            }
        } else {
            None
        };

        let binary_store = snapshot.binary_store.clone();
        let ledger = snapshot.to_ledger_state();
        let view = GraphDb::from_ledger_state(&ledger).with_default_context(default_context);
        Ok(match binary_store {
            Some(store) => view.with_binary_store(store),
            None => view,
        })
    }

    /// Load a view at current head (no default context injection).
    pub(crate) async fn load_graph_db(&self, ledger_id: &str) -> Result<GraphDb> {
        self.load_graph_db_impl(ledger_id, false).await
    }

    /// Load a view at current head with the ledger's default context attached.
    pub(crate) async fn load_graph_db_with_default_context(
        &self,
        ledger_id: &str,
    ) -> Result<GraphDb> {
        self.load_graph_db_impl(ledger_id, true).await
    }

    /// Load a historical view at a specific transaction time.
    ///
    /// For named graph queries (e.g., `#txn-meta`), this also loads the binary
    /// index store if available, enabling graph-scoped queries.
    pub(crate) async fn load_graph_db_at_t(
        &self,
        ledger_id: &str,
        target_t: i64,
    ) -> Result<GraphDb> {
        // Fast path: time travel to the current head is equivalent to no time travel.
        //
        // Avoid the historical loader (which may fetch index roots / binary stores)
        // when the requested t is already the latest ledger version.
        let handle = self.ledger_cached(ledger_id).await?;
        let snap = handle.snapshot().await;
        if target_t == snap.t {
            let binary_store = snap.binary_store.clone();
            let ledger = snap.to_ledger_state();
            let view = GraphDb::from_ledger_state(&ledger);
            return Ok(match binary_store {
                Some(store) => view.with_binary_store(store),
                None => view,
            });
        }

        let historical = self.ledger_view_at(ledger_id, target_t).await?;
        let mut view = GraphDb::from_historical(&historical);

        // Attach dict_novelty derived from the historical Db's watermarks.
        //
        // Historical views can have an overlay (novelty) even when the binary index
        // is behind the view's `t`. We must populate DictNovelty from the overlay
        // flakes so binary overlay translation can assign subject/string IDs for
        // novelty-only entities (e.g., newly inserted subjects after the last index).
        let mut dict_novelty = DictNovelty::with_watermarks(
            view.snapshot.subject_watermarks.clone(),
            view.snapshot.string_watermark,
        );
        // NOTE: If we successfully attach a BinaryIndexStore below, we will populate
        // dict_novelty using persisted-first routing to avoid allocating IDs for
        // already-indexed entries. For overlay-only historical views (no binary store),
        // we populate without a store (everything is treated as novel).

        // Load the binary index store (for index-backed historical queries only).
        //
        // When the historical view is overlay-only (genesis Db + commit replay),
        // we intentionally skip attaching a binary store so the query engine
        // takes the overlay/range path instead of the binary scan path.
        if view.snapshot.t > 0 {
            // Use nameservice record (not cached handle) to avoid stale index.
            if let Some(record) = self.nameservice().lookup(ledger_id).await? {
                if let Some(index_cid) = record.index_head_id.as_ref() {
                    // Branch-aware store so historical queries on a branch
                    // can resolve inherited leaf/branch/history blobs that
                    // live under the source branch's namespace.
                    let cs = fluree_db_nameservice::branched_content_store_for_record(
                        self.backend(),
                        self.nameservice(),
                        &record,
                    )
                    .await
                    .map_err(ApiError::from)?;
                    let bytes = cs.get(index_cid).await.map_err(|e| {
                        ApiError::internal(format!("failed to read index root {index_cid}: {e}"))
                    })?;
                    let cache_dir = std::env::temp_dir().join("fluree-cache");
                    let mut store = BinaryIndexStore::load_from_root_bytes(
                        cs,
                        &bytes,
                        &cache_dir,
                        Some(Arc::clone(&self.leaflet_cache)),
                    )
                    .await
                    .map_err(|e| {
                        ApiError::internal(format!("load binary index store from {index_cid}: {e}"))
                    })?;

                    // Augment store with snapshot namespace codes and sync split mode.
                    // Unlike the primary load path (sync_store_and_snapshot_ns), we can't
                    // reconcile store codes back into the snapshot because view.snapshot
                    // is Arc<LedgerSnapshot> (shared/read-only). This is safe because the
                    // index root's namespace table is a subset of the commit-derived table
                    // (the root is a materialized cache at index_t ≤ view.to_t).
                    store
                        .augment_namespace_codes(view.snapshot.namespaces())
                        .map_err(|e| ApiError::internal(format!("augment namespace codes: {e}")))?;
                    store.set_ns_split_mode(view.snapshot.ns_split_mode());

                    // Populate dict novelty safely (persisted dict wins).
                    populate_dict_novelty_from_view(
                        &mut dict_novelty,
                        Some(&store),
                        view.novelty.as_ref(),
                    )?;
                    view.dict_novelty = Some(Arc::new(dict_novelty));
                    view.binary_store = Some(Arc::new(store));

                    // Historical views loaded from an index root are metadata-only by default
                    // (`LedgerSnapshot::from_root_bytes` sets `range_provider = None`).
                    // If we loaded a BinaryIndexStore, attach a BinaryRangeProvider so
                    // range-based operators (joins, index lookups) work correctly.
                    if view.snapshot.range_provider.is_none() {
                        let (Some(store), Some(dict_novelty)) =
                            (view.binary_store.as_ref(), view.dict_novelty.as_ref())
                        else {
                            return Ok(view);
                        };
                        let store = Arc::clone(store);
                        let dict_novelty = Arc::clone(dict_novelty);
                        let runtime_small_dicts =
                            view.runtime_small_dicts.clone().unwrap_or_else(|| {
                                crate::runtime_dicts::build_runtime_small_dicts(
                                    &store,
                                    view.novelty.as_ref(),
                                )
                            });
                        let ns_fallback = Some(Arc::new(view.snapshot.namespaces().clone()));
                        let provider = BinaryRangeProvider::new(
                            Arc::clone(&store),
                            dict_novelty,
                            Arc::clone(&runtime_small_dicts),
                            ns_fallback,
                        );
                        let mut db = (*view.snapshot).clone();
                        db.range_provider = Some(Arc::new(provider));
                        view.snapshot = Arc::new(db);
                        view.runtime_small_dicts = Some(runtime_small_dicts);
                    }
                } else {
                    // Commits exist but no index is available — populate without a store.
                    populate_dict_novelty_from_view(
                        &mut dict_novelty,
                        None,
                        view.novelty.as_ref(),
                    )?;
                    view.dict_novelty = Some(Arc::new(dict_novelty));
                }
            } else {
                // Snapshot has commits but nameservice record is missing.
                populate_dict_novelty_from_view(&mut dict_novelty, None, view.novelty.as_ref())?;
                view.dict_novelty = Some(Arc::new(dict_novelty));
            }
        } else {
            // Overlay-only historical view: no persisted dictionaries available.
            populate_dict_novelty_from_view(&mut dict_novelty, None, view.novelty.as_ref())?;
            view.dict_novelty = Some(Arc::new(dict_novelty));
        }

        Ok(view)
    }

    /// Load a view at a flexible time specification.
    ///
    /// Resolves `@t:`, `@iso:`, `@commit:`, or `latest` time specifications.
    pub(crate) async fn load_graph_db_at(
        &self,
        ledger_id: &str,
        spec: TimeSpec,
    ) -> Result<GraphDb> {
        match spec {
            TimeSpec::Latest => self.load_graph_db(ledger_id).await,
            TimeSpec::AtT(t) => self.load_graph_db_at_t(ledger_id, t).await,
            TimeSpec::AtTime(iso) => {
                let handle = self.ledger_cached(ledger_id).await?;
                let snapshot = handle.snapshot().await;
                let ledger = snapshot.to_ledger_state();
                let current_t = ledger.t();
                let dt = DateTime::parse_from_rfc3339(&iso).map_err(|e| {
                    ApiError::internal(format!(
                        "Invalid ISO-8601 timestamp for time travel: {iso} ({e})"
                    ))
                })?;
                // `ledger#time` flakes store epoch milliseconds. If the ISO timestamp includes
                // sub-millisecond precision, `timestamp_millis()` truncates, which can push the
                // target *slightly before* the intended instant. To avoid off-by-one-ms
                // resolution (especially around the first commit after genesis), we ceiling
                // to the next millisecond when sub-ms precision is present.
                let mut target_epoch_ms = dt.timestamp_millis();
                if dt.timestamp_subsec_nanos() % 1_000_000 != 0 {
                    target_epoch_ms += 1;
                }
                let resolved_t = time_resolve::datetime_to_t(
                    &ledger.snapshot,
                    Some(ledger.novelty.as_ref()),
                    target_epoch_ms,
                    current_t,
                )
                .await?;
                self.load_graph_db_at_t(ledger_id, resolved_t).await
            }
            TimeSpec::AtCommit(commit_prefix) => {
                let handle = self.ledger_cached(ledger_id).await?;
                let snapshot = handle.snapshot().await;
                let ledger = snapshot.to_ledger_state();
                let current_t = ledger.t();
                let resolved_t = time_resolve::commit_to_t(
                    &ledger.snapshot,
                    Some(ledger.novelty.as_ref()),
                    &commit_prefix,
                    current_t,
                )
                .await?;
                self.load_graph_db_at_t(ledger_id, resolved_t).await
            }
        }
    }

    /// Load the current snapshot from a ledger.
    ///
    /// Returns a [`GraphDb`] — an immutable, point-in-time snapshot.
    /// For the lazy API, use [`graph()`](Self::graph) instead.
    pub async fn db(&self, ledger_id: &str) -> Result<GraphDb> {
        let (ledger_id, graph_ref) = Self::parse_graph_ref(ledger_id)?;
        let view = self.load_graph_db(ledger_id).await?;
        let view = Self::select_graph(view, graph_ref)?;
        self.resolve_and_attach_config(view).await
    }

    /// Load a historical snapshot at a specific transaction time.
    pub async fn db_at_t(&self, ledger_id: &str, target_t: i64) -> Result<GraphDb> {
        let (ledger_id, graph_ref) = Self::parse_graph_ref(ledger_id)?;
        let view = self.load_graph_db_at_t(ledger_id, target_t).await?;
        let view = Self::select_graph(view, graph_ref)?;
        self.resolve_and_attach_config(view).await
    }

    /// Load a snapshot at a flexible time specification.
    pub async fn db_at(&self, ledger_id: &str, spec: TimeSpec) -> Result<GraphDb> {
        let (ledger_id, graph_ref) = Self::parse_graph_ref(ledger_id)?;
        let view = self.load_graph_db_at(ledger_id, spec).await?;
        let view = Self::select_graph(view, graph_ref)?;
        self.resolve_and_attach_config(view).await
    }

    /// Like [`db()`](Self::db) but includes the ledger's default context.
    ///
    /// Use this at compatibility entrypoints (server HTTP, CLI) where queries
    /// that omit `@context` / `PREFIX` should still resolve prefixes from the
    /// ledger's stored default context.
    pub async fn db_with_default_context(&self, ledger_id: &str) -> Result<GraphDb> {
        let (ledger_id, graph_ref) = Self::parse_graph_ref(ledger_id)?;
        let view = self.load_graph_db_with_default_context(ledger_id).await?;
        let view = Self::select_graph(view, graph_ref)?;
        self.resolve_and_attach_config(view).await
    }

    /// Like [`db_at()`](Self::db_at) but includes the ledger's default context.
    pub async fn db_at_with_default_context(
        &self,
        ledger_id: &str,
        spec: TimeSpec,
    ) -> Result<GraphDb> {
        let (parsed_id, _) = Self::parse_graph_ref(ledger_id)?;
        let mut view = self.db_at(ledger_id, spec).await?;
        // Historical views don't load default_context through their own
        // load path. Fetch it explicitly via the branch-aware helper using
        // the cached current-head record.
        if view.default_context.is_none() {
            view = view.with_default_context(self.get_default_context(parsed_id).await?);
        }
        Ok(view)
    }

    /// Apply a graph selector from a dataset GraphSource to a view.
    ///
    /// Converts the dataset-layer `GraphSelector` to the internal `GraphRef`
    /// and applies graph selection to the view.
    ///
    /// This is called by `load_view_from_source` when a `GraphSource` has
    /// an explicit `graph_selector` set.
    pub(crate) fn apply_graph_selector(
        view: GraphDb,
        selector: &crate::dataset::GraphSelector,
    ) -> Result<GraphDb> {
        let graph_ref = match selector {
            crate::dataset::GraphSelector::Default => GraphRef::Default,
            crate::dataset::GraphSelector::TxnMeta => GraphRef::TxnMeta,
            crate::dataset::GraphSelector::Iri(iri) => GraphRef::Named(iri.clone()),
        };
        Self::select_graph(view, graph_ref)
    }
}

// ============================================================================
// Graph Source Resolution (requires GraphSourcePublisher)
// ============================================================================

impl Fluree {
    /// Load a graph view, falling back to graph source resolution.
    ///
    /// Tries to load a ledger first. If not found, checks if the alias
    /// matches a graph source (Iceberg/R2RML) and creates a minimal genesis
    /// snapshot tagged with the graph source ID. The tag causes query
    /// execution to auto-wrap patterns in `GRAPH <gs_id> { ... }`.
    pub async fn load_graph_db_or_graph_source(&self, ledger_id: &str) -> Result<GraphDb> {
        match self.load_graph_db(ledger_id).await {
            Ok(db) => Ok(db),
            Err(ref e) if e.is_not_found() => {
                let gs_id = fluree_db_core::normalize_ledger_id(ledger_id)
                    .unwrap_or_else(|_| ledger_id.to_string());

                let _record = self
                    .nameservice()
                    .lookup_graph_source(&gs_id)
                    .await
                    .map_err(|e| ApiError::internal(e.to_string()))?
                    .ok_or_else(|| ApiError::NotFound(ledger_id.to_string()))?;

                let snapshot = fluree_db_core::LedgerSnapshot::genesis(&gs_id);
                let state = fluree_db_ledger::LedgerState::new(
                    snapshot,
                    fluree_db_novelty::Novelty::new(0),
                );
                let mut db = GraphDb::from_ledger_state(&state);
                db.graph_source_id = Some(gs_id.into());
                Ok(db)
            }
            Err(e) => Err(e),
        }
    }
}

// ============================================================================
// Policy Wrapping
// ============================================================================

impl Fluree {
    /// Build policy from options and wrap a view.
    ///
    /// If the view has a `ResolvedConfig`, config defaults are merged with query
    /// opts and override control is checked against `server_identity`.
    ///
    /// `server_identity` is the auth-layer-verified identity — NOT `opts.identity`
    /// which is the user-settable policy evaluation context.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let view = fluree.db("mydb:main").await?;
    /// let opts = QueryConnectionOptions {
    ///     identity: Some("did:example:user".into()),
    ///     ..Default::default()
    /// };
    /// let view = fluree.wrap_policy(view, &opts, None).await?;
    /// ```
    pub async fn wrap_policy(
        &self,
        view: GraphDb,
        opts: &QueryConnectionOptions,
        server_identity: Option<&str>,
    ) -> Result<GraphDb> {
        let effective_opts = if let Some(ref resolved) = view.resolved_config {
            config_resolver::merge_policy_opts(resolved, opts, server_identity)
        } else {
            opts.clone()
        };

        // Cross-ledger detection: if the resolved config's
        // f:policySource carries an f:ledger reference, route through
        // the cross-ledger resolver. Same-ledger configs continue
        // through the unchanged local path.
        let source = view
            .resolved_config
            .as_ref()
            .and_then(|c| c.policy.as_ref())
            .and_then(|p| p.policy_source.as_ref());

        let is_cross_ledger = source.is_some_and(|s| s.ledger.is_some());

        if is_cross_ledger {
            // Phase 1a: cross-ledger + identity-mode is not supported.
            // The model ledger contributes policy rules; the data
            // ledger contributes identity binding. Mixing them
            // ambiguously is a fail-closed config error.
            if effective_opts.identity.is_some() {
                return Err(crate::error::ApiError::config(
                    "cross-ledger f:policySource cannot be combined with opts.identity \
                     in Phase 1a; use opts.policy_class with the cross-ledger config",
                ));
            }

            let source = source.expect("checked above");
            let mut ctx =
                crate::cross_ledger::ResolveCtx::new(view.snapshot.ledger_id.as_str(), self);
            let resolved = crate::cross_ledger::resolve_graph_ref(
                source,
                crate::cross_ledger::ArtifactKind::PolicyRules,
                &mut ctx,
            )
            .await?;
            let crate::cross_ledger::GovernanceArtifact::PolicyRules(wire) = &resolved.artifact
            else {
                // resolve_graph_ref dispatches on ArtifactKind, so
                // requesting PolicyRules must yield PolicyRules.
                // Surfacing this as TranslationFailed rather than
                // panicking keeps the failure path uniform for
                // operators reading the response body.
                return Err(crate::error::ApiError::CrossLedger(
                    crate::cross_ledger::CrossLedgerError::TranslationFailed {
                        ledger_id: resolved.model_ledger_id.clone(),
                        graph_iri: resolved.graph_iri.clone(),
                        detail: "resolver returned a non-PolicyRules artifact for an \
                                ArtifactKind::PolicyRules request; this is a bug in \
                                the resolver dispatch"
                            .into(),
                    },
                ));
            };

            // Apply the data ledger's configured policy_class set as
            // an exact-IRI intersection filter on the wire's
            // restrictions. The contract is:
            //
            //   filter = effective_opts.policy_class, OR
            //            {f:AccessPolicy} when no policy_class is set.
            //
            // f:AccessPolicy is the canonical / baseline policy class
            // — declaring `f:policySource` cross-ledger pulls those
            // rules in automatically. Custom-typed rules require
            // an explicit `f:policyClass` in D's config to be
            // enforced. This is the safer default than "load every
            // structurally-policy-looking subject from M," which
            // would silently include rules the operator never opted
            // into.
            const DEFAULT_POLICY_CLASS_IRI: &str = "https://ns.flur.ee/db#AccessPolicy";
            let filter: std::collections::HashSet<String> = effective_opts
                .policy_class
                .as_ref()
                .filter(|v| !v.is_empty())
                .map(|v| v.iter().cloned().collect())
                .unwrap_or_else(|| [DEFAULT_POLICY_CLASS_IRI.to_string()].into_iter().collect());
            let snapshot_ref = &view.snapshot;
            let restrictions = fluree_db_policy::wire_to_restrictions(
                wire,
                |iri| snapshot_ref.encode_iri(iri),
                Some(&filter),
            )
            .map_err(crate::error::ApiError::from)?;

            let policy_ctx =
                crate::policy_builder::build_policy_context_from_opts_with_cross_ledger(
                    &view.snapshot,
                    view.overlay.as_ref(),
                    view.novelty_for_stats(),
                    view.t,
                    &effective_opts,
                    &[0], // identity-mode uses [0]; unused under cross-ledger
                    restrictions,
                )
                .await?;
            return Ok(view.with_policy(Arc::new(policy_ctx)));
        }

        let policy_graphs = if let Some(ref resolved) = view.resolved_config {
            let source = resolved
                .policy
                .as_ref()
                .and_then(|p| p.policy_source.as_ref());
            crate::policy_builder::resolve_policy_source_g_ids(source, &view.snapshot)?
        } else {
            vec![0]
        };

        let policy_ctx = crate::policy_builder::build_policy_context_from_opts(
            &view.snapshot,
            view.overlay.as_ref(),
            view.novelty_for_stats(),
            view.t,
            &effective_opts,
            &policy_graphs,
        )
        .await?;
        Ok(view.with_policy(Arc::new(policy_ctx)))
    }

    /// Load a view at head with policy applied.
    ///
    /// Convenience method that combines `db()` + `wrap_policy()`.
    /// Passes `None` for server identity (no auth layer plumbing yet).
    pub async fn db_with_policy(
        &self,
        ledger_id: &str,
        opts: &QueryConnectionOptions,
    ) -> Result<GraphDb> {
        let view = self.db(ledger_id).await?;
        self.wrap_policy(view, opts, None).await
    }

    /// Load a db at a specific time with policy applied.
    ///
    /// Passes `None` for server identity (no auth layer plumbing yet).
    pub async fn db_at_t_with_policy(
        &self,
        ledger_id: &str,
        target_t: i64,
        opts: &QueryConnectionOptions,
    ) -> Result<GraphDb> {
        let view = self.db_at_t(ledger_id, target_t).await?;
        self.wrap_policy(view, opts, None).await
    }
}

// ============================================================================
// Reasoning Wrapping
// ============================================================================

impl Fluree {
    /// Wrap a view with default reasoning modes.
    ///
    /// This is a pure function (no async) since it just attaches metadata.
    /// Uses `DefaultUnlessQueryOverrides` precedence.
    pub fn wrap_reasoning(&self, view: GraphDb, modes: ReasoningModes) -> GraphDb {
        view.with_reasoning(modes)
    }

    /// Wrap a view with reasoning modes and explicit precedence.
    pub fn wrap_reasoning_with_precedence(
        &self,
        view: GraphDb,
        modes: ReasoningModes,
        precedence: ReasoningModePrecedence,
    ) -> GraphDb {
        view.with_reasoning_precedence(modes, precedence)
    }

    /// Apply config-graph reasoning defaults to a view.
    ///
    /// Reads `ResolvedConfig.reasoning` and converts to reasoning wrapper
    /// with the appropriate precedence based on override control.
    ///
    /// `server_identity` is the auth-layer-verified identity (NOT opts.identity).
    /// Pass `None` when no auth layer is present (Phase 1).
    pub fn apply_config_reasoning(&self, view: GraphDb, server_identity: Option<&str>) -> GraphDb {
        let resolved = match &view.resolved_config {
            Some(r) => r,
            None => return view,
        };

        match config_resolver::merge_reasoning(resolved, server_identity) {
            Some((mode_strings, precedence)) => {
                let modes = ReasoningModes::from_mode_strings(&mode_strings);
                // Always wrap if modes has enabled flags or explicit_none=true
                // (config can force-disable reasoning via "none").
                // Only skip if from_mode_strings produced a truly empty default.
                if modes.has_any_enabled() || modes.is_disabled() {
                    view.with_reasoning_precedence(modes, precedence)
                } else {
                    view
                }
            }
            None => view,
        }
    }

    /// Apply all config-graph defaults (reasoning + datalog) to a view.
    ///
    /// Convenience wrapper that calls both `apply_config_reasoning` and
    /// `apply_config_datalog` in sequence.
    pub fn apply_config_defaults(&self, view: GraphDb, server_identity: Option<&str>) -> GraphDb {
        let view = self.apply_config_reasoning(view, server_identity);
        self.apply_config_datalog(view, server_identity)
    }

    /// Apply config-graph datalog defaults to a view.
    ///
    /// Stores resolved datalog config on the view. Enforcement happens
    /// at query execution time, not here.
    pub fn apply_config_datalog(&self, view: GraphDb, server_identity: Option<&str>) -> GraphDb {
        let resolved = match &view.resolved_config {
            Some(r) => r,
            None => return view,
        };

        match config_resolver::merge_datalog_opts(resolved, server_identity) {
            Some(config) => view
                .with_datalog_enabled(config.enabled)
                .with_query_time_rules_allowed(config.allow_query_time_rules)
                .with_datalog_override_allowed(config.override_allowed),
            None => view,
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Populate `DictNovelty` from a view's novelty overlay, routing through the
/// persisted dictionaries when a `BinaryIndexStore` is available.
///
/// This is the common pattern used across all historical view load paths.
fn populate_dict_novelty_from_view(
    dict_novelty: &mut DictNovelty,
    store: Option<&BinaryIndexStore>,
    novelty: Option<&Arc<fluree_db_novelty::Novelty>>,
) -> crate::Result<()> {
    if let Some(novelty) = novelty {
        fluree_db_binary_index::dict_novelty_safe::populate_dict_novelty_safe(
            dict_novelty,
            store,
            novelty
                .iter_index(IndexType::Spot)
                .map(|id| novelty.get_flake(id)),
        )
        .map_err(|e| ApiError::internal(format!("populate_dict_novelty_safe: {e}")))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FlureeBuilder;

    #[tokio::test]
    async fn test_view_not_found() {
        let fluree = FlureeBuilder::memory().build_memory();

        let result = fluree.db("nonexistent:main").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_view_after_create() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Create a ledger
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        // Load as view
        let view = fluree.db("testdb:main").await.unwrap();

        assert_eq!(&*view.ledger_id, "testdb:main");
        assert_eq!(view.t, 0); // Genesis
        assert!(view.novelty().is_some());
    }

    #[tokio::test]
    async fn test_view_at_t() {
        use serde_json::json;

        let fluree = FlureeBuilder::memory().build_memory();

        // Create and transact
        let ledger = fluree.create_ledger("testdb").await.unwrap();
        let txn = json!({ "@context": {"ex": "http://example.org/"}, "insert": [{"@id": "ex:a", "ex:name": "Alice"}] });
        let _ledger = fluree.update(ledger, &txn).await.unwrap().ledger;

        // Load at t=0 (before transaction)
        let view = fluree.db_at_t("testdb:main", 0).await.unwrap();
        assert_eq!(view.t, 0);

        // Load at t=1 (after transaction)
        let view = fluree.db_at_t("testdb:main", 1).await.unwrap();
        assert_eq!(view.t, 1);
    }

    #[tokio::test]
    async fn test_view_at_future_time_error() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Create a ledger at t=0
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        // Try to load at t=100 (future)
        let result = fluree.db_at_t("testdb:main", 100).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_wrap_reasoning() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        let view = fluree.db("testdb:main").await.unwrap();
        assert!(view.reasoning().is_none());

        let view = fluree.wrap_reasoning(view, ReasoningModes::owl2ql());
        assert!(view.reasoning().is_some());
        assert!(view.reasoning().unwrap().owl2ql);
    }
}
