//! BM25 full-text search index operations.
//!
//! This module provides APIs for creating, loading, syncing, and dropping
//! BM25 full-text search indexes.

use crate::graph_source::config::Bm25CreateConfig;
use crate::graph_source::helpers::{expand_ids_in_results, extract_prefix_map};
use crate::graph_source::result::{
    Bm25CreateResult, Bm25DropResult, Bm25StalenessCheck, Bm25SyncResult, SnapshotSelection,
};
use crate::Result;
use fluree_db_core::{
    ledger_id::split_ledger_id, ContentId, ContentStore, OverlayProvider, Storage,
};
use fluree_db_ledger::LedgerState;
use fluree_db_nameservice::{GraphSourceRecord, GraphSourceType};
use fluree_db_query::bm25::{Bm25IndexBuilder, Bm25Manifest, Bm25SnapshotEntry, PropertyDeps};
use fluree_db_query::parse::parse_query;
use fluree_db_query::{execute, ContextConfig, ExecutableQuery, QueryOutput, VarRegistry};
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Outcome of narrowing an indexing query: the narrowed query, or the reason it
/// could not be narrowed.
///
/// Spelled with `std::result::Result` because `Result` in this module is the
/// crate alias over `ApiError`, and a decline is not an API error — it is the
/// ordinary, correct fallback to the unscoped query.
type ScopeOutcome = std::result::Result<JsonValue, &'static str>;

/// Clauses that shape the result set rather than the scan, paired with why each
/// one refuses narrowing, in **both** spellings the query parser accepts
/// (`parse/options.rs` reads `groupBy` or `group-by`; `parse/mod.rs` reads
/// `selectOne` or `select-one`). Any of them present means the query cannot be
/// narrowed safely — see [`scope_indexing_query_to_subjects`].
///
/// Reason and clause live together so adding a spelling cannot silently inherit
/// the wrong explanation.
const RESULT_SHAPING_CLAUSES: &[(&str, &str)] = &[
    (
        "limit",
        "the query carries `limit`, which truncates the result set",
    ),
    (
        "offset",
        "the query carries `offset`, which truncates the result set",
    ),
    (
        "groupBy",
        "the query carries `groupBy`, which reshapes the result set",
    ),
    (
        "group-by",
        "the query carries `groupBy`, which reshapes the result set",
    ),
    (
        "having",
        "the query carries `having`, which filters the result set",
    ),
    (
        "selectOne",
        "the query uses `selectOne`, which returns a single row",
    ),
    (
        "select-one",
        "the query uses `selectOne`, which returns a single row",
    ),
];

/// Narrow an indexing query to a known set of subject IRIs by binding its
/// subject variable with a `values` clause.
///
/// An incremental sync already knows, from the commit log, exactly which
/// subjects changed — but the indexing query it then runs is the *whole* query
/// over the *whole* ledger, and the affected set is only applied afterwards as
/// a filter. That makes every incremental sync O(corpus) rather than O(delta),
/// which is the dominant cost once an index is maintained continuously.
///
/// Returns `None` whenever the query cannot be narrowed **safely**, in which
/// case the caller runs the original query and filters as before. Falling back
/// is always correct — just slower — so every uncertain case returns `None`
/// rather than guessing:
///
/// - the subject variable cannot be identified (see below);
/// - the query already carries a top-level `values` clause, which this would
///   have to merge with rather than replace;
/// - the query shapes its own result set with `limit`, `offset`, `groupBy` or
///   `having`, or asks for a single row with `selectOne` (see below);
/// - the affected set is empty (the caller treats that as a full resync).
///
/// The subject variable is the single key of an object-form `select` —
/// `{"select": {"?x": ["@id", "ex:title"]}}` — which is the shape
/// `PropertyDeps::from_indexing_query` already assumes and the shape the BM25
/// documentation specifies. A list-form select (`["?x", "?title"]`) does not
/// identify which variable is the document, so it is left alone.
///
/// **Why result-shaping clauses must decline rather than narrow.** Scoping is
/// only sound while it changes how rows are *found* and not which rows come
/// back. Anything that truncates the result set breaks that, because scoping
/// changes what the truncation is applied to. `apply_update` splits the
/// affected set two ways: a subject the query returned is upserted, and a
/// subject in `affected` but not in `seen` is passed to `remove_document`. So
/// for a query carrying `limit`, the two paths take *opposite* actions on the
/// same input. With `"limit": 2` over a four-document corpus, the initial build
/// indexes `doc1, doc2`; a commit then touches `doc4`. Unscoped, the sync runs
/// the full query, gets `doc1, doc2`, finds `doc4` unseen and removes it — a
/// no-op here. Scoped, `doc4` is the only row the query can return, so it is
/// upserted, leaving a three-document index where `resync_bm25_index` would
/// produce two. `offset`, `groupBy` and `having` truncate or reshape for the
/// same reason. `selectOne` is the same class and is stricter, not looser: it
/// returns one row, so scoping changes *which* row rather than merely how it
/// was found. Declining costs nothing but the optimisation on shapes we cannot
/// reason about, which is the trade this function already commits to above.
///
/// The kebab-case spellings matter: `parse/options.rs` reads `groupBy` **or**
/// `group-by`, and `parse/mod.rs` reads `selectOne` **or** `select-one`, so a
/// guard naming only the camelCase form would let the other spelling through
/// into exactly the divergence above. `limit`, `offset` and `having` have no
/// alias. `orderBy` is deliberately absent: ordering alone does not change
/// which rows come back, and every clause that turns an order into a
/// truncation is already declined here.
///
/// The saving comes from the *indexed* portion of the ledger, where a bound
/// subject seeks into the leaflets instead of scanning them: fuel is flat in
/// corpus size when scoped against linear when not, ~270x at 400 documents
/// (`scoped_indexing_query_narrows_the_indexed_scan`). Novelty is a linear
/// structure with no subject index, so rows committed since the last index
/// build are still walked and the win over that portion is only ~1.5x. That
/// makes this complementary to the reindex thresholds rather than a
/// substitute — `reindex_min_bytes` is what bounds how much novelty a sync
/// has to walk.
fn scope_indexing_query_to_subjects(
    query: &JsonValue,
    affected_iris: &HashSet<Arc<str>>,
) -> ScopeOutcome {
    // The error carries WHY, so the caller can say which condition declined
    // instead of leaving an operator to guess why a sync is still slow.
    if affected_iris.is_empty() {
        return Err("the affected set is empty (the caller treats this as a full resync)");
    }
    if query.get("values").is_some() {
        return Err("the query already carries a top-level `values` clause");
    }
    if let Some(&(_, reason)) = RESULT_SHAPING_CLAUSES
        .iter()
        .find(|(clause, _)| query.get(clause).is_some())
    {
        return Err(reason);
    }

    // Object-form select only, with exactly one key: that key is the document
    // variable. `selectOne` is declined above, not accepted here.
    let Some(select) = query.get("select") else {
        return Err("the query has no `select` clause");
    };
    let Some(obj) = select.as_object() else {
        return Err("the `select` is not object-form, so it names no document variable");
    };
    let mut keys = obj.keys();
    let Some(subject_var) = keys.next() else {
        return Err("the `select` object is empty");
    };
    if keys.next().is_some() {
        return Err("the `select` binds more than one variable, so the document is ambiguous");
    }
    if !subject_var.starts_with('?') {
        return Err("the `select` key is not a variable");
    }

    // Bind the document variable to the affected IRIs. Full IRIs, not the
    // prefixed forms: `values` cells resolve against stored IRIs, whereas the
    // prefix expansion the caller builds exists to match formatted JSON-LD
    // output. Sorted so the generated query is deterministic — it shows up in
    // logs and in the differential test.
    let mut iris: Vec<&str> = affected_iris
        .iter()
        .map(std::convert::AsRef::as_ref)
        .collect();
    iris.sort_unstable();
    let rows: Vec<JsonValue> = iris
        .into_iter()
        .map(|iri| serde_json::json!({ "@id": iri }))
        .collect();

    let mut scoped = query.clone();
    let Some(scoped_obj) = scoped.as_object_mut() else {
        return Err("the query is not a JSON object");
    };
    scoped_obj.insert("values".to_string(), serde_json::json!([subject_var, rows]));
    Ok(scoped)
}

/// Maximum concurrent CAS operations for BM25 leaflet reads/writes.
/// Caps socket pressure and S3 throttling for large indexes with many leaflets.
const BM25_IO_CONCURRENCY: usize = 32;

/// Best-effort deletion of old snapshot blobs from storage.
/// Derives storage addresses from CIDs using the graph source namespace.
/// Logs warnings on failure but does not propagate errors.
async fn delete_old_snapshots(storage: &dyn Storage, graph_source_id: &str, cids: &[ContentId]) {
    use fluree_db_core::ContentKind;
    let method = storage.storage_method();
    for cid in cids {
        let addr = fluree_db_core::content_address(
            method,
            ContentKind::GraphSourceSnapshot,
            graph_source_id,
            &cid.digest_hex(),
        );
        if let Err(e) = storage.delete(&addr).await {
            warn!(address = %addr, error = %e, "failed to delete old BM25 snapshot");
        }
    }
}

/// Default snapshot retention for BM25 manifests.
/// Uses the same default as index GC (`gc_max_old_indexes` + 1 for current).
fn snapshot_retention() -> usize {
    (fluree_db_indexer::DEFAULT_MAX_OLD_INDEXES as usize) + 1
}

// =============================================================================
// BM25 Index Creation
// =============================================================================

impl crate::Fluree {
    /// Create a BM25 full-text search index.
    ///
    /// Validates the configuration, then:
    /// 1. Loads the source ledger
    /// 2. Executes the indexing query to get documents
    /// 3. Builds the BM25 index
    /// 4. Persists the index snapshot to storage
    /// 5. Publishes the graph source record to the nameservice
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration specifying the index name, source ledger, and query
    ///
    /// # Returns
    ///
    /// Result containing the created index metadata
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = Bm25CreateConfig::new("search", "docs:main", json!({
    ///     "where": [{"@id": "?x", "@type": "Article"}],
    ///     "select": {"?x": ["@id", "title", "content"]}
    /// }));
    ///
    /// let result = fluree.create_full_text_index(config).await?;
    /// ```
    pub async fn create_full_text_index(
        &self,
        config: Bm25CreateConfig,
    ) -> Result<Bm25CreateResult> {
        config.validate()?;

        let graph_source_id = config.graph_source_id();
        info!(
            graph_source_id = %graph_source_id,
            ledger = %config.ledger,
            "Creating BM25 full-text index"
        );

        // Check if graph source already exists (prevent duplicates)
        if let Some(existing) = self
            .nameservice()
            .lookup_graph_source(&graph_source_id)
            .await?
        {
            if !existing.retracted {
                return Err(crate::ApiError::Config(format!(
                    "Graph source '{graph_source_id}' already exists"
                )));
            }
        }

        // 1. Load source ledger
        let ledger = self.ledger(&config.ledger).await?;
        let source_t = ledger.t();

        info!(
            ledger = %config.ledger,
            t = source_t,
            "Loaded source ledger"
        );

        // 2. Execute indexing query
        let results = self
            .execute_bm25_indexing_query(&ledger, &config.query)
            .await?;

        info!(result_count = results.len(), "Executed indexing query");

        // 2b. Expand prefixed IRIs in @id fields to full IRIs
        let context = config
            .query
            .get("@context")
            .cloned()
            .unwrap_or(serde_json::json!({}));
        let prefix_map = extract_prefix_map(&context);
        let results = expand_ids_in_results(results, &prefix_map);

        // 3. Build BM25 index
        let property_deps = PropertyDeps::from_indexing_query(&config.query);
        let mut builder = Bm25IndexBuilder::new(config.ledger.as_str(), config.bm25_config())
            .with_property_deps(property_deps)
            .with_watermark(source_t);

        builder.add_results(&results)?;

        let doc_count = builder.indexed_count();
        let skipped = builder.skipped_count();
        let index = builder.build();
        let term_count = index.num_terms();

        info!(
            doc_count = doc_count,
            skipped = skipped,
            term_count = term_count,
            "Built BM25 index"
        );

        // 4. Persist index snapshot blob to CAS
        let snapshot_id = self.write_bm25_snapshot(&graph_source_id, &index).await?;

        info!(
            snapshot_id = %snapshot_id,
            index_t = source_t,
            "Persisted versioned index snapshot"
        );

        // 5. Build manifest with initial snapshot entry
        let mut manifest = Bm25Manifest::new(&graph_source_id);
        manifest.append(Bm25SnapshotEntry::new(source_t, snapshot_id));

        // 6. Publish graph source config record to nameservice
        let config_json = serde_json::to_string(&serde_json::json!({
            "k1": config.k1.unwrap_or(1.2),
            "b": config.b.unwrap_or(0.75),
            "query": config.query,
        }))?;

        self.publisher()?
            .publish_graph_source(
                &config.name,
                config.effective_branch(),
                GraphSourceType::Bm25,
                &config_json,
                std::slice::from_ref(&config.ledger),
            )
            .await?;

        // 7. Publish manifest to CAS and head pointer to nameservice
        let index_id = self
            .publish_bm25_manifest(&graph_source_id, &manifest, source_t)
            .await?;

        info!(
            graph_source_id = %graph_source_id,
            doc_count = doc_count,
            index_t = source_t,
            "Created BM25 full-text index"
        );

        Ok(Bm25CreateResult {
            graph_source_id,
            doc_count,
            term_count,
            index_t: source_t,
            index_id: Some(index_id),
        })
    }

    /// Execute the indexing query and return JSON-LD results.
    ///
    /// Executes the query and formats results as JSON-LD objects suitable for indexing.
    /// Each result object will have an `@id` field identifying the document.
    pub(crate) async fn execute_bm25_indexing_query(
        &self,
        ledger: &LedgerState,
        query_json: &JsonValue,
    ) -> Result<Vec<JsonValue>> {
        // Parse the query
        let mut vars = VarRegistry::new();
        let parsed = parse_query(query_json, ledger.snapshot.as_ref(), &mut vars, None)?;

        // Execute with a wildcard select so the operator pipeline does not project away
        // bindings we need for indexing
        let mut parsed_for_exec = parsed.clone();
        parsed_for_exec.output = QueryOutput::wildcard();

        let executable = ExecutableQuery::simple(parsed_for_exec);

        let db = ledger.as_graph_db_ref(0);
        let batches = execute(db, &vars, &executable, ContextConfig::default()).await?;

        // Format using the standard JSON-LD formatter
        let result = crate::query::helpers::build_query_result(
            vars,
            parsed,
            batches,
            Some(ledger.t()),
            Some(ledger.novelty.clone()),
            None,
        );

        let json = result.to_jsonld_async(ledger.as_graph_db_ref(0)).await?;
        match json {
            JsonValue::Array(arr) => Ok(arr),
            JsonValue::Object(_) => Ok(vec![json]),
            _ => Ok(Vec::new()),
        }
    }

    /// Execute an indexing query against a historical `GraphDb`.
    ///
    /// This is used for building BM25 indexes at historical points in time.
    /// Callers must pass a `GraphDb` loaded via [`Fluree::load_graph_db_at_t`]
    /// so the binary index store and range provider are attached — a raw
    /// `HistoricalLedgerView` wrapped via `GraphDb::from_historical` is not
    /// sufficient because it has no `range_provider` when `snapshot.t > 0`.
    pub(crate) async fn execute_bm25_indexing_query_historical(
        &self,
        view: &crate::view::GraphDb,
        query_json: &JsonValue,
    ) -> Result<Vec<JsonValue>> {
        let mut vars = VarRegistry::new();
        let parsed = parse_query(query_json, view.snapshot.as_ref(), &mut vars, None)?;

        let mut parsed_for_exec = parsed.clone();
        parsed_for_exec.output = QueryOutput::wildcard();

        let executable = ExecutableQuery::simple(parsed_for_exec);

        let db = view.as_graph_db_ref();
        let batches = execute(db, &vars, &executable, ContextConfig::default()).await?;

        let novelty = view
            .novelty()
            .cloned()
            .map(|n| n as Arc<dyn OverlayProvider>);
        let result = crate::query::helpers::build_query_result(
            vars,
            parsed,
            batches,
            Some(view.t),
            novelty,
            None,
        );

        let json = result.to_jsonld_async(view.as_graph_db_ref()).await?;
        match json {
            JsonValue::Array(arr) => Ok(arr),
            JsonValue::Object(_) => Ok(vec![json]),
            _ => Ok(Vec::new()),
        }
    }

    /// Write a BM25 index snapshot to CAS, choosing v3 (single blob) or v4
    /// (chunked) format based on the storage backend.
    ///
    /// - Native/file storage → v3 single blob (one CAS write, one read on load)
    /// - S3/object store or memory → v4 chunked (root + posting leaflets for
    ///   selective per-query loading)
    ///
    /// Returns the root `ContentId` — for v4 this is the root blob; leaflet
    /// blobs are separate CAS objects referenced by CID from the root.
    pub(crate) async fn write_bm25_snapshot(
        &self,
        graph_source_id: &str,
        index: &fluree_db_query::bm25::Bm25Index,
    ) -> Result<ContentId> {
        if self.should_use_chunked_format() {
            self.write_bm25_chunked_snapshot(graph_source_id, index)
                .await
        } else {
            self.write_bm25_snapshot_v3(graph_source_id, index).await
        }
    }

    /// Write a single-blob v3 snapshot to CAS. Used for native/file storage.
    async fn write_bm25_snapshot_v3(
        &self,
        graph_source_id: &str,
        index: &fluree_db_query::bm25::Bm25Index,
    ) -> Result<ContentId> {
        use fluree_db_query::bm25::serialize;

        let bytes = serialize(index)?;
        let cs = self.content_store(graph_source_id);
        let snapshot_id = cs
            .put(fluree_db_core::ContentKind::GraphSourceSnapshot, &bytes)
            .await?;
        Ok(snapshot_id)
    }

    /// Write a v4 chunked snapshot: posting leaflets as separate CAS blobs,
    /// then a root blob referencing them by CID.
    async fn write_bm25_chunked_snapshot(
        &self,
        graph_source_id: &str,
        index: &fluree_db_query::bm25::Bm25Index,
    ) -> Result<ContentId> {
        use fluree_db_query::bm25::{finalize_chunked_root, prepare_chunked};
        use futures::stream::{self, StreamExt, TryStreamExt};

        let mut prep = prepare_chunked(index)?;
        let cs = self.content_store(graph_source_id);

        // Drain blobs for parallel writes — finalize_chunked_root only uses
        // prep.root + prep.leaflet_infos, not leaflet_blobs.
        let blobs = std::mem::take(&mut prep.leaflet_blobs);

        // Write leaflets with bounded concurrency, preserving order via enumerate
        let mut cid_results: Vec<(usize, Vec<u8>)> = stream::iter(blobs.into_iter().enumerate())
            .map(|(i, blob)| {
                let cs = cs.clone();
                async move {
                    let cid = cs
                        .put(fluree_db_core::ContentKind::GraphSourceSnapshot, &blob)
                        .await?;
                    Ok::<_, crate::ApiError>((i, cid.to_bytes()))
                }
            })
            .buffer_unordered(BM25_IO_CONCURRENCY)
            .try_collect()
            .await?;

        // Restore order (buffer_unordered may complete out of order)
        cid_results.sort_by_key(|(i, _)| *i);
        let cid_bytes: Vec<Vec<u8>> = cid_results.into_iter().map(|(_, bytes)| bytes).collect();

        // Finalize root with CID references, write to CAS
        let root_bytes = finalize_chunked_root(prep, cid_bytes)?;
        let root_cid = cs
            .put(
                fluree_db_core::ContentKind::GraphSourceSnapshot,
                &root_bytes,
            )
            .await?;
        Ok(root_cid)
    }

    /// Whether this storage backend should use v4 chunked format.
    ///
    /// S3/object stores benefit from selective per-query loading (fetch only
    /// the posting leaflets needed). Local file storage is faster with a
    /// single v3 blob (one read, one decompress). Memory storage uses v4
    /// for test coverage.
    ///
    /// NOTE: that default optimises the **cold read**, which is right for an
    /// index built once and then queried, and wrong for one that is
    /// *maintained* — on v3 every incremental sync rewrites the whole blob, so
    /// write cost tracks corpus size rather than change size. Making this
    /// overridable is a follow-up; it needs a builder knob threaded through
    /// `finalize_with_backend`, which is a wider change than belongs here.
    pub(crate) fn should_use_chunked_format(&self) -> bool {
        let method = self
            .admin_storage()
            .map(fluree_db_core::StorageMethod::storage_method)
            .unwrap_or("unknown");
        matches!(
            method,
            fluree_db_core::STORAGE_METHOD_S3 | fluree_db_core::STORAGE_METHOD_MEMORY
        )
    }

    /// Write a BM25 manifest to CAS and publish the manifest address as
    /// the graph source head pointer in nameservice.
    ///
    /// The manifest is content-addressed (keyed by `index_t`), so each
    /// publish creates a new immutable object in storage.
    pub(crate) async fn publish_bm25_manifest(
        &self,
        graph_source_id: &str,
        manifest: &Bm25Manifest,
        index_t: i64,
    ) -> Result<ContentId> {
        let (name, branch) = split_ledger_id(graph_source_id).map_err(|e| {
            crate::ApiError::config(format!("Invalid graph source ID '{graph_source_id}': {e}"))
        })?;

        let bytes = serde_json::to_vec(manifest)?;

        // Write through the content store so it's stored at the CID-mapped address
        let cs = self.content_store(graph_source_id);
        let index_id = cs
            .put(fluree_db_core::ContentKind::IndexRoot, &bytes)
            .await?;

        self.publisher()?
            .publish_graph_source_index(&name, &branch, &index_id, index_t)
            .await?;

        Ok(index_id)
    }
}

// =============================================================================
// BM25 Manifest Loading (read-only helpers)
// =============================================================================

impl crate::Fluree {
    /// Load the current BM25 manifest from CAS, or create a new empty one.
    ///
    /// Reads the manifest address from the nameservice head pointer,
    /// then loads the manifest JSON from CAS. Returns an empty manifest
    /// if the graph source has no index yet (e.g., during initial create).
    pub(crate) async fn load_or_create_bm25_manifest(
        &self,
        graph_source_id: &str,
    ) -> Result<Bm25Manifest> {
        match self
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await?
        {
            Some(record) if record.index_id.is_some() => {
                let index_cid = record.index_id.as_ref().unwrap();
                let cs = self.content_store(graph_source_id);
                let bytes = cs.get(index_cid).await?;
                let manifest: Bm25Manifest = serde_json::from_slice(&bytes)?;
                Ok(manifest)
            }
            _ => Ok(Bm25Manifest::new(graph_source_id)),
        }
    }

    /// Load the current BM25 manifest from CAS.
    ///
    /// Returns an error if the graph source is not found or has no index.
    pub(crate) async fn load_bm25_manifest(&self, graph_source_id: &str) -> Result<Bm25Manifest> {
        let record = self
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!("Graph source not found: {graph_source_id}"))
            })?;

        let index_cid = record.index_id.ok_or_else(|| {
            crate::ApiError::NotFound(format!("No index for graph source: {graph_source_id}"))
        })?;

        let cs = self.content_store(graph_source_id);
        let bytes = cs.get(&index_cid).await?;
        let manifest: Bm25Manifest = serde_json::from_slice(&bytes)?;
        Ok(manifest)
    }
}

// =============================================================================
// BM25 Index Loading (for queries)
// =============================================================================

impl crate::Fluree {
    /// Select the best BM25 snapshot for a given `as_of_t`.
    ///
    /// Loads the BM25 manifest from CAS and selects the snapshot with the
    /// largest `index_t` that is <= `as_of_t`.
    pub async fn select_bm25_snapshot(
        &self,
        graph_source_id: &str,
        as_of_t: i64,
    ) -> Result<Option<SnapshotSelection>> {
        let manifest = self.load_bm25_manifest(graph_source_id).await?;

        match manifest.select_snapshot(as_of_t) {
            Some(entry) => Ok(Some(SnapshotSelection {
                graph_source_id: graph_source_id.to_string(),
                snapshot_t: entry.index_t,
                snapshot_id: entry.snapshot_id.clone(),
            })),
            None => Ok(None),
        }
    }

    /// Load a BM25 index for a specific `as_of_t` using snapshot selection.
    ///
    /// This is the time-travel aware version of `load_bm25_index`.
    /// Automatically detects v4 chunked format and loads leaflets from CAS.
    pub async fn load_bm25_index_at(
        &self,
        graph_source_id: &str,
        as_of_t: i64,
    ) -> Result<(Arc<fluree_db_query::bm25::Bm25Index>, i64)> {
        let selection = self
            .select_bm25_snapshot(graph_source_id, as_of_t)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!(
                    "No BM25 snapshot available for {graph_source_id} at t={as_of_t}"
                ))
            })?;

        let cs = self.content_store(graph_source_id);
        let bytes = cs.get(&selection.snapshot_id).await?;

        let index = self.load_bm25_from_bytes(graph_source_id, &bytes).await?;
        Ok((Arc::new(index), selection.snapshot_t))
    }

    /// Load a BM25 index from storage (head snapshot).
    ///
    /// Loads the manifest, resolves the head snapshot, and deserializes.
    /// Automatically detects v4 chunked format and loads leaflets from CAS.
    /// For time-travel queries, use `load_bm25_index_at` instead.
    pub async fn load_bm25_index(
        &self,
        graph_source_id: &str,
    ) -> Result<Arc<fluree_db_query::bm25::Bm25Index>> {
        let manifest = self.load_bm25_manifest(graph_source_id).await?;
        let head = manifest.head().ok_or_else(|| {
            crate::ApiError::NotFound(format!("No snapshots in manifest for: {graph_source_id}"))
        })?;

        let cs = self.content_store(graph_source_id);
        let bytes = cs.get(&head.snapshot_id).await?;
        let index = self.load_bm25_from_bytes(graph_source_id, &bytes).await?;
        Ok(Arc::new(index))
    }

    /// Load a BM25 index from raw bytes, auto-detecting v4 chunked format.
    ///
    /// For v1-v3: single-blob deserialization.
    /// For v4: deserialize root, fetch posting leaflets from CAS with caching
    /// and bounded concurrency, then assemble.
    pub(crate) async fn load_bm25_from_bytes(
        &self,
        graph_source_id: &str,
        bytes: &[u8],
    ) -> Result<fluree_db_query::bm25::Bm25Index> {
        use fluree_db_binary_index::LeafletCache;
        use fluree_db_query::bm25::{
            assemble_from_chunked_root, deserialize, deserialize_chunked_root,
            deserialize_posting_leaflet, is_chunked_format, LeafletRef, PostingList,
        };
        use futures::stream::{self, StreamExt, TryStreamExt};

        if is_chunked_format(bytes) {
            let root = deserialize_chunked_root(bytes)?;
            let cs = self.content_store(graph_source_id);
            let cache = self.leaflet_cache();

            let leaflet_refs = root.leaflet_refs();
            let mut posting_lists = vec![PostingList::default(); root.next_term_idx() as usize];

            // Partition leaflet refs into cache hits and misses
            let mut hits: Vec<(LeafletRef, Arc<[u8]>)> = Vec::new();
            let mut misses: Vec<LeafletRef> = Vec::new();

            for lr in &leaflet_refs {
                let key = LeafletCache::cid_cache_key(&lr.cid_bytes);
                if let Some(cached) = cache.get_bm25_leaflet(key) {
                    hits.push((lr.clone(), cached));
                } else {
                    misses.push(lr.clone());
                }
            }

            // Fetch all misses with bounded concurrency
            let fetched: Vec<(LeafletRef, Vec<u8>)> = stream::iter(misses)
                .map(|lr| {
                    let cs = cs.clone();
                    async move {
                        let cid = ContentId::from_bytes(&lr.cid_bytes)?;
                        let data = cs.get(&cid).await?;
                        Ok::<_, crate::ApiError>((lr, data))
                    }
                })
                .buffer_unordered(BM25_IO_CONCURRENCY)
                .try_collect()
                .await?;

            // Cache + deserialize fetched leaflets (zero-copy Vec → Arc)
            for (lr, raw) in fetched {
                let bytes: Arc<[u8]> = raw.into_boxed_slice().into();
                let key = LeafletCache::cid_cache_key(&lr.cid_bytes);
                cache.insert_bm25_leaflet(key, Arc::clone(&bytes));
                let (first_idx, lists) = deserialize_posting_leaflet(&bytes)?;
                for (i, pl) in lists.into_iter().enumerate() {
                    posting_lists[first_idx as usize + i] = pl;
                }
            }

            // Deserialize cache hits
            for (_lr, cached_bytes) in &hits {
                let (first_idx, lists) = deserialize_posting_leaflet(cached_bytes)?;
                for (i, pl) in lists.into_iter().enumerate() {
                    posting_lists[first_idx as usize + i] = pl;
                }
            }

            Ok(assemble_from_chunked_root(root, posting_lists))
        } else {
            Ok(deserialize(bytes)?)
        }
    }

    /// Search a v4 chunked BM25 index with selective leaflet loading.
    ///
    /// Instead of loading the entire index, this:
    /// 1. Deserializes the root blob (terms, doc_meta, routing table)
    /// 2. Analyzes the query to identify needed term indices
    /// 3. Fetches only the posting leaflets containing those terms (with caching
    ///    and bounded concurrency)
    /// 4. Assembles a partial index and scores
    ///
    /// For non-v4 snapshots, falls back to full index load.
    pub(crate) async fn search_bm25_selective(
        &self,
        graph_source_id: &str,
        snapshot_bytes: &[u8],
        query_text: &str,
        limit: usize,
    ) -> Result<fluree_db_query::bm25::Bm25SearchResult> {
        use fluree_db_binary_index::LeafletCache;
        use fluree_db_query::bm25::{
            assemble_from_chunked_root, deserialize_chunked_root, deserialize_posting_leaflet,
            is_chunked_format, Analyzer, Bm25Scorer, Bm25SearchResult, LeafletRef, PostingList,
            SearchHit,
        };
        use futures::stream::{self, StreamExt, TryStreamExt};

        if !is_chunked_format(snapshot_bytes) {
            // Not v4 — fall back to full index load + score
            let index = self
                .load_bm25_from_bytes(graph_source_id, snapshot_bytes)
                .await?;
            let index_t = index.watermark.effective_t();
            let analyzer = Analyzer::english_default();
            let terms = analyzer.analyze_to_strings(query_text);
            if terms.is_empty() {
                return Ok(Bm25SearchResult::empty(index_t));
            }
            let term_refs: Vec<&str> = terms.iter().map(std::string::String::as_str).collect();
            let scorer = Bm25Scorer::new(&index, &term_refs);
            let hits: Vec<SearchHit> = scorer
                .top_k(limit)
                .into_iter()
                .map(|(dk, score)| {
                    SearchHit::new(
                        dk.subject_iri.to_string(),
                        dk.ledger_alias.to_string(),
                        score,
                    )
                })
                .collect();
            return Ok(Bm25SearchResult::new(index_t, hits));
        }

        // V4 selective path
        let root = deserialize_chunked_root(snapshot_bytes)?;

        // Analyze query
        let analyzer = Analyzer::english_default();
        let terms = analyzer.analyze_to_strings(query_text);
        if terms.is_empty() {
            return Ok(Bm25SearchResult::empty(0));
        }

        // Resolve terms → term_idxs
        let term_idxs: Vec<u32> = terms
            .iter()
            .filter_map(|t| root.get_term(t).map(|e| e.idx))
            .collect();

        if term_idxs.is_empty() {
            // No query terms exist in the index
            return Ok(Bm25SearchResult::empty(0));
        }

        // Identify which leaflets contain these term_idxs
        let needed_leaflets = root.leaflet_refs_for_terms(&term_idxs);

        // Fetch needed leaflets with caching + bounded concurrency
        let cs = self.content_store(graph_source_id);
        let cache = self.leaflet_cache();
        let mut posting_lists = vec![PostingList::default(); root.next_term_idx() as usize];

        // Partition into cache hits and misses
        let mut hits: Vec<(LeafletRef, Arc<[u8]>)> = Vec::new();
        let mut misses: Vec<LeafletRef> = Vec::new();

        for lr in &needed_leaflets {
            let key = LeafletCache::cid_cache_key(&lr.cid_bytes);
            if let Some(cached) = cache.get_bm25_leaflet(key) {
                hits.push((lr.clone(), cached));
            } else {
                misses.push(lr.clone());
            }
        }

        // Fetch all misses with bounded concurrency
        let fetched: Vec<(LeafletRef, Vec<u8>)> = stream::iter(misses)
            .map(|lr| {
                let cs = cs.clone();
                async move {
                    let cid = ContentId::from_bytes(&lr.cid_bytes)?;
                    let data = cs.get(&cid).await?;
                    Ok::<_, crate::ApiError>((lr, data))
                }
            })
            .buffer_unordered(BM25_IO_CONCURRENCY)
            .try_collect()
            .await?;

        // Cache + deserialize fetched leaflets (zero-copy Vec → Arc)
        for (lr, raw) in fetched {
            let bytes: Arc<[u8]> = raw.into_boxed_slice().into();
            let key = LeafletCache::cid_cache_key(&lr.cid_bytes);
            cache.insert_bm25_leaflet(key, Arc::clone(&bytes));
            let (first_idx, lists) = deserialize_posting_leaflet(&bytes)?;
            for (i, pl) in lists.into_iter().enumerate() {
                posting_lists[first_idx as usize + i] = pl;
            }
        }

        // Deserialize cache hits
        for (_lr, cached_bytes) in &hits {
            let (first_idx, lists) = deserialize_posting_leaflet(cached_bytes)?;
            for (i, pl) in lists.into_iter().enumerate() {
                posting_lists[first_idx as usize + i] = pl;
            }
        }

        // Assemble partial index and score
        let index = assemble_from_chunked_root(root, posting_lists);
        let effective_t = index.watermark.effective_t();
        let term_refs: Vec<&str> = terms.iter().map(std::string::String::as_str).collect();
        let scorer = Bm25Scorer::new(&index, &term_refs);
        let hits: Vec<SearchHit> = scorer
            .top_k(limit)
            .into_iter()
            .map(|(dk, score)| {
                SearchHit::new(
                    dk.subject_iri.to_string(),
                    dk.ledger_alias.to_string(),
                    score,
                )
            })
            .collect();

        Ok(Bm25SearchResult::new(effective_t, hits))
    }

    /// Check if a BM25 index is stale relative to its source ledger.
    ///
    /// This is a lightweight check that only looks up nameservice records.
    pub async fn check_bm25_staleness(&self, graph_source_id: &str) -> Result<Bm25StalenessCheck> {
        // Look up graph source record
        let record = self
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!("Graph source not found: {graph_source_id}"))
            })?;

        // Get source ledger from dependencies
        let source_ledger = record
            .dependencies
            .first()
            .ok_or_else(|| {
                crate::ApiError::Config("Graph source has no source ledger".to_string())
            })?
            .clone();

        // Check minimum head across all dependencies
        let mut ledger_t: Option<i64> = None;
        for dep in &record.dependencies {
            let ledger_record = self.nameservice().lookup(dep).await?.ok_or_else(|| {
                crate::ApiError::NotFound(format!("Source ledger not found: {dep}"))
            })?;
            ledger_t = Some(match ledger_t {
                Some(cur) => cur.min(ledger_record.commit_t),
                None => ledger_record.commit_t,
            });
        }
        let ledger_t = ledger_t.unwrap_or(0);

        let index_t = record.index_t;
        let is_stale = index_t < ledger_t;
        let lag = ledger_t - index_t;

        Ok(Bm25StalenessCheck {
            graph_source_id: graph_source_id.to_string(),
            source_ledger,
            index_t,
            ledger_t,
            is_stale,
            lag,
        })
    }
}

// =============================================================================
// BM25 Index Sync (Maintenance)
// =============================================================================

impl crate::Fluree {
    /// Look up a graph source that is eligible to be synced.
    ///
    /// A retracted source is refused rather than resurrected: [`Self::drop_full_text_index`]
    /// deletes its snapshots, so writing a fresh one would put a dropped index
    /// back to serving results.
    async fn syncable_graph_source(&self, graph_source_id: &str) -> Result<GraphSourceRecord> {
        let record = self
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!("Graph source not found: {graph_source_id}"))
            })?;

        if record.retracted {
            return Err(crate::ApiError::Drop(format!(
                "Cannot sync retracted graph source: {graph_source_id}"
            )));
        }

        Ok(record)
    }

    /// Sync a BM25 index to catch up with ledger updates.
    ///
    /// This operation performs incremental updates when possible,
    /// falling back to full resync if needed.
    pub async fn sync_bm25_index(&self, graph_source_id: &str) -> Result<Bm25SyncResult> {
        use fluree_db_core::trace_commits_by_id;
        use fluree_db_query::bm25::{CompiledPropertyDeps, IncrementalUpdater};
        use futures::StreamExt;

        info!(graph_source_id = %graph_source_id, "Starting BM25 index sync");

        // 1. Look up graph source record to get config and index address
        let record = self.syncable_graph_source(graph_source_id).await?;

        if record.index_id.is_none() {
            // No index yet - need full resync
            return self.resync_bm25_index(graph_source_id).await;
        }

        // Parse config to get query
        let config: JsonValue = serde_json::from_str(&record.config)?;
        let query = config
            .get("query")
            .cloned()
            .unwrap_or(serde_json::json!({}));

        // Get source ledger alias from dependencies
        let source_ledger_alias = record
            .dependencies
            .first()
            .ok_or_else(|| {
                crate::ApiError::Config("Graph source has no source ledger".to_string())
            })?
            .clone();

        // 2. Load source ledger to get current state
        let ledger = self.ledger(&source_ledger_alias).await?;
        let ledger_t = ledger.t();

        // 3. Load existing index via manifest head
        let manifest = self.load_bm25_manifest(graph_source_id).await?;
        let head = manifest.head().ok_or_else(|| {
            crate::ApiError::NotFound(format!("No snapshots in manifest for: {graph_source_id}"))
        })?;
        let cs = self.content_store(graph_source_id);
        let bytes = cs.get(&head.snapshot_id).await?;
        let mut index = self.load_bm25_from_bytes(graph_source_id, &bytes).await?;
        let old_watermark = index.watermark.get(&source_ledger_alias).unwrap_or(0);

        // Already up to date?
        if ledger_t <= old_watermark {
            info!(graph_source_id = %graph_source_id, ledger_t = ledger_t, "Index already up to date");
            return Ok(Bm25SyncResult {
                graph_source_id: graph_source_id.to_string(),
                upserted: 0,
                removed: 0,
                affected_subjects: 0,
                old_watermark,
                new_watermark: old_watermark,
                was_full_resync: false,
            });
        }

        // 4. Get head commit CID for tracing
        let head_commit_id = ledger
            .ns_record
            .as_ref()
            .and_then(|r| r.commit_head_id.clone())
            .ok_or_else(|| crate::ApiError::NotFound("No commit head for ledger".to_string()))?;

        // 5. Compile property deps for this ledger's namespace
        let compiled_deps = CompiledPropertyDeps::compile(&index.property_deps, |iri: &str| {
            ledger.snapshot.encode_iri(iri)
        });

        // 6. Trace commits and collect affected subjects. Branch-aware
        //    store so the walk can resolve pre-fork ancestors when the
        //    ledger is a branch.
        let mut affected_sids: HashSet<fluree_db_core::Sid> = HashSet::new();
        let store = self
            .content_store_for_record_or_id(ledger.ns_record.as_ref(), &ledger.snapshot.ledger_id)
            .await?;
        let stream = trace_commits_by_id(store, head_commit_id.clone(), old_watermark);
        futures::pin_mut!(stream);

        while let Some(result) = stream.next().await {
            let commit = result?;
            let subjects = compiled_deps.affected_subjects(&commit.flakes);
            affected_sids.extend(subjects);
        }

        // If no subjects affected, fall back to full resync
        if affected_sids.is_empty() {
            warn!(
                graph_source_id = %graph_source_id,
                old_watermark = old_watermark,
                ledger_t = ledger_t,
                "No affected subjects detected, falling back to full resync"
            );
            return self.resync_bm25_index(graph_source_id).await;
        }

        // 7. Convert affected Sids to IRIs
        let affected_iris: HashSet<Arc<str>> = affected_sids
            .into_iter()
            .filter_map(|sid| {
                ledger
                    .snapshot
                    .decode_sid(&sid)
                    .map(|s| Arc::from(s.as_str()))
            })
            .collect();

        info!(
            graph_source_id = %graph_source_id,
            affected_count = affected_iris.len(),
            "Found affected subjects for incremental update"
        );

        // 8. Re-run the indexing query, scoped to the affected subjects where we
        //    can. `scope_indexing_query_to_subjects` declines when the query
        //    shape is not one we can safely narrow, in which case this falls back
        //    to the full scan and the filtering in `apply_update` below does the
        //    work — same results either way, just more of them computed.
        //
        //    Both outcomes log. A decline is correct, not an error, but it is
        //    also invisible: the sync stays O(corpus) and nothing says why. The
        //    reason is the one thing an operator needs to act on it.
        let scoped = scope_indexing_query_to_subjects(&query, &affected_iris);
        match &scoped {
            Ok(_) => debug!(
                graph_source_id = %graph_source_id,
                affected_count = affected_iris.len(),
                "Scoped indexing query to affected subjects"
            ),
            Err(reason) => debug!(
                graph_source_id = %graph_source_id,
                affected_count = affected_iris.len(),
                reason = %reason,
                "Indexing query not narrowed; falling back to the full scan"
            ),
        }
        let scoped_query = scoped.as_ref().unwrap_or(&query);
        let results = self
            .execute_bm25_indexing_query(&ledger, scoped_query)
            .await?;

        // Canonicalise `@id` to full IRIs before touching the index.
        //
        // `create_full_text_index` does this, and the sync paths did not — so the
        // first sync to touch a document silently rewrote its key from the full
        // IRI the build stored to the prefixed form the query returns, leaving one
        // index holding both spellings and lookups by full IRI missing anything
        // re-synced. Doing it here makes the document key the same value whichever
        // path wrote it.
        //
        // It also removes the reason the affected set had to carry prefixed
        // variants: both sides of the match below are now full IRIs.
        let context = query
            .get("@context")
            .cloned()
            .unwrap_or(serde_json::json!({}));
        let prefix_map = extract_prefix_map(&context);
        let results = expand_ids_in_results(results, &prefix_map);

        // 9. Apply incremental update
        let mut updater = IncrementalUpdater::new(source_ledger_alias.as_str(), &mut index);
        let update_result = updater.apply_update(&results, &affected_iris, ledger_t);

        info!(
            graph_source_id = %graph_source_id,
            upserted = update_result.upserted,
            removed = update_result.removed,
            "Applied incremental update"
        );

        // 10. Persist updated index blob
        let new_snapshot_id = self.write_bm25_snapshot(graph_source_id, &index).await?;

        // 11. Update manifest, trim old snapshots, and publish
        let mut manifest = manifest;
        manifest.append(Bm25SnapshotEntry::new(ledger_t, new_snapshot_id.clone()));
        let removed = manifest.trim(snapshot_retention());
        self.publish_bm25_manifest(graph_source_id, &manifest, ledger_t)
            .await?;

        // Best-effort cleanup of old snapshot blobs
        if let Some(storage) = self.admin_storage() {
            delete_old_snapshots(storage, graph_source_id, &removed).await;
        }

        info!(
            graph_source_id = %graph_source_id,
            snapshot_id = %new_snapshot_id,
            trimmed = removed.len(),
            ledger_t = ledger_t,
            "Incremental sync complete"
        );

        Ok(Bm25SyncResult {
            graph_source_id: graph_source_id.to_string(),
            upserted: update_result.upserted,
            removed: update_result.removed,
            affected_subjects: affected_iris.len(),
            old_watermark,
            new_watermark: ledger_t,
            was_full_resync: false,
        })
    }

    /// Force a full resync of a BM25 index.
    ///
    /// Unlike `sync_bm25_index`, this re-runs the entire indexing query
    /// and rebuilds the index from scratch.
    pub async fn resync_bm25_index(&self, graph_source_id: &str) -> Result<Bm25SyncResult> {
        use fluree_db_query::bm25::IncrementalUpdater;

        info!(graph_source_id = %graph_source_id, "Starting BM25 full resync");

        // 1. Look up graph source record
        let record = self.syncable_graph_source(graph_source_id).await?;

        if record.index_id.is_none() {
            return Err(crate::ApiError::NotFound(format!(
                "No index for graph source: {graph_source_id}"
            )));
        }

        let config: JsonValue = serde_json::from_str(&record.config)?;
        let query = config
            .get("query")
            .cloned()
            .unwrap_or(serde_json::json!({}));

        let source_ledger = record
            .dependencies
            .first()
            .ok_or_else(|| {
                crate::ApiError::Config("Graph source has no source ledger".to_string())
            })?
            .clone();

        // 2. Load existing index via manifest head (to preserve config and property deps)
        let manifest = self.load_bm25_manifest(graph_source_id).await?;
        let head = manifest.head().ok_or_else(|| {
            crate::ApiError::NotFound(format!("No snapshots in manifest for: {graph_source_id}"))
        })?;
        let cs = self.content_store(graph_source_id);
        let bytes = cs.get(&head.snapshot_id).await?;
        let mut index = self.load_bm25_from_bytes(graph_source_id, &bytes).await?;
        let old_watermark = index.watermark.get(&source_ledger).unwrap_or(0);

        // 3. Load source ledger
        let ledger = self.ledger(&source_ledger).await?;
        let ledger_t = ledger.t();

        // 4. Re-run indexing query
        let results = self.execute_bm25_indexing_query(&ledger, &query).await?;

        // Canonicalise `@id` to full IRIs, exactly as `create_full_text_index`
        // does. Without this a resync rewrites every document key from the full
        // IRI the build stored to whatever prefixed form the query happens to
        // return — see the note on the same call in the incremental path.
        let prefix_map = extract_prefix_map(
            &query
                .get("@context")
                .cloned()
                .unwrap_or(serde_json::json!({})),
        );
        let results = expand_ids_in_results(results, &prefix_map);

        info!(
            graph_source_id = %graph_source_id,
            result_count = results.len(),
            ledger_t = ledger_t,
            "Executed full indexing query"
        );

        // 5. Apply full sync (replaces all documents)
        let mut updater = IncrementalUpdater::new(source_ledger.as_str(), &mut index);
        let update_result = updater.apply_full_sync(&results, ledger_t);

        // 6. Persist updated index blob
        let new_snapshot_id = self.write_bm25_snapshot(graph_source_id, &index).await?;

        // 7. Update manifest, trim old snapshots, and publish
        let mut manifest = manifest;
        manifest.append(Bm25SnapshotEntry::new(ledger_t, new_snapshot_id.clone()));
        let removed = manifest.trim(snapshot_retention());
        self.publish_bm25_manifest(graph_source_id, &manifest, ledger_t)
            .await?;

        // Best-effort cleanup of old snapshot blobs
        if let Some(storage) = self.admin_storage() {
            delete_old_snapshots(storage, graph_source_id, &removed).await;
        }

        info!(
            graph_source_id = %graph_source_id,
            snapshot_id = %new_snapshot_id,
            trimmed = removed.len(),
            ledger_t = ledger_t,
            "Full resync complete"
        );

        Ok(Bm25SyncResult {
            graph_source_id: graph_source_id.to_string(),
            upserted: update_result.upserted,
            removed: update_result.removed,
            affected_subjects: update_result.upserted + update_result.removed,
            old_watermark,
            new_watermark: ledger_t,
            was_full_resync: true,
        })
    }

    /// Load a BM25 index, optionally syncing if stale.
    ///
    /// This implements the "on-query catch-up" pattern.
    pub async fn load_bm25_index_with_sync(
        &self,
        graph_source_id: &str,
        auto_sync: bool,
    ) -> Result<(
        Arc<fluree_db_query::bm25::Bm25Index>,
        Option<Bm25SyncResult>,
    )> {
        // Look up graph source record
        let record = self
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!("Graph source not found: {graph_source_id}"))
            })?;

        // Get source ledger to check staleness
        let source_ledger = record
            .dependencies
            .first()
            .ok_or_else(|| {
                crate::ApiError::Config("Graph source has no source ledger".to_string())
            })?
            .clone();

        // Look up source ledger record
        let ledger_record = self
            .nameservice()
            .lookup(&source_ledger)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!("Source ledger not found: {source_ledger}"))
            })?;

        let index_t = record.index_t;
        let ledger_t = ledger_record.commit_t;
        let is_stale = index_t < ledger_t;

        // Sync if stale and auto_sync is enabled
        let sync_result = if is_stale && auto_sync {
            info!(
                graph_source_id = %graph_source_id,
                index_t = index_t,
                ledger_t = ledger_t,
                "Index is stale, syncing before load"
            );
            Some(self.sync_bm25_index(graph_source_id).await?)
        } else {
            None
        };

        // Load the (possibly updated) index via manifest head
        let manifest = self.load_bm25_manifest(graph_source_id).await?;
        let head = manifest.head().ok_or_else(|| {
            crate::ApiError::NotFound(format!("No snapshots in manifest for: {graph_source_id}"))
        })?;

        let cs = self.content_store(graph_source_id);
        let bytes = cs.get(&head.snapshot_id).await?;
        let index = self.load_bm25_from_bytes(graph_source_id, &bytes).await?;

        Ok((Arc::new(index), sync_result))
    }

    /// Sync a BM25 index to a specific target time.
    ///
    /// This builds a BM25 snapshot at exactly `target_t` by loading
    /// the source ledger at that historical point.
    pub async fn sync_bm25_index_to(
        &self,
        graph_source_id: &str,
        target_t: i64,
        timeout_ms: Option<u64>,
    ) -> Result<Bm25SyncResult> {
        use fluree_db_query::bm25::{Bm25IndexBuilder, IncrementalUpdater, PropertyDeps};

        info!(
            graph_source_id = %graph_source_id,
            target_t = target_t,
            timeout_ms = ?timeout_ms,
            "Starting BM25 index sync to specific t"
        );

        let _ = timeout_ms; // Reserved for future timeout support

        // 1. Look up graph source record to get config
        let record = self.syncable_graph_source(graph_source_id).await?;

        let config: JsonValue = serde_json::from_str(&record.config)?;
        let query = config
            .get("query")
            .cloned()
            .unwrap_or(serde_json::json!({}));
        let k1 = config
            .get("k1")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(1.2);
        let b = config
            .get("b")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.75);

        let source_ledger = record
            .dependencies
            .first()
            .ok_or_else(|| {
                crate::ApiError::Config("Graph source has no source ledger".to_string())
            })?
            .clone();

        // 2. Check if we already have a snapshot at target_t
        let manifest = self.load_or_create_bm25_manifest(graph_source_id).await?;
        if manifest.has_snapshot_at(target_t) {
            info!(graph_source_id = %graph_source_id, target_t = target_t, "Snapshot already exists");
            return Ok(Bm25SyncResult {
                graph_source_id: graph_source_id.to_string(),
                upserted: 0,
                removed: 0,
                affected_subjects: 0,
                old_watermark: target_t,
                new_watermark: target_t,
                was_full_resync: false,
            });
        }

        // 3. Load source ledger at target_t using time-travel.
        //
        // Use `load_graph_db_at_t` (not `ledger_view_at`) so the historical
        // view comes back fully wired with a `BinaryIndexStore` and
        // `BinaryRangeProvider`. A bare `HistoricalLedgerView` has neither,
        // and any `range()` call against its snapshot would error with
        // "binary-only db has no range_provider attached" once the snapshot
        // is index-backed (which it now is for any `target_t` covered by
        // `base_t..=index_t`).
        let view = self.load_graph_db_at_t(&source_ledger, target_t).await?;

        // 4. Execute indexing query at target_t
        let results = self
            .execute_bm25_indexing_query_historical(&view, &query)
            .await?;

        info!(
            graph_source_id = %graph_source_id,
            target_t = target_t,
            result_count = results.len(),
            "Executed indexing query at historical t"
        );

        // 5. Build BM25 index
        let property_deps = PropertyDeps::from_indexing_query(&query);
        let bm25_config = fluree_db_query::bm25::Bm25Config::new(k1, b);
        let mut builder = Bm25IndexBuilder::new(source_ledger.as_str(), bm25_config)
            .with_property_deps(property_deps)
            .with_watermark(target_t);

        builder.add_results(&results)?;
        let mut index = builder.build();

        // Apply as full sync to set watermarks correctly
        let mut updater = IncrementalUpdater::new(source_ledger.as_str(), &mut index);
        let update_result = updater.apply_full_sync(&results, target_t);

        // 6. Persist versioned snapshot blob
        let snapshot_id = self.write_bm25_snapshot(graph_source_id, &index).await?;

        // 7. Update manifest, trim old snapshots, and publish
        let mut manifest = manifest;
        manifest.append(Bm25SnapshotEntry::new(target_t, snapshot_id));
        let removed = manifest.trim(snapshot_retention());
        let effective_t = manifest.head().map(|h| h.index_t).unwrap_or(target_t);
        self.publish_bm25_manifest(graph_source_id, &manifest, effective_t)
            .await?;

        // Best-effort cleanup of old snapshot blobs
        if let Some(storage) = self.admin_storage() {
            delete_old_snapshots(storage, graph_source_id, &removed).await;
        }

        info!(
            graph_source_id = %graph_source_id,
            target_t = target_t,
            trimmed = removed.len(),
            upserted = update_result.upserted,
            "Sync to specific t complete"
        );

        Ok(Bm25SyncResult {
            graph_source_id: graph_source_id.to_string(),
            upserted: update_result.upserted,
            removed: update_result.removed,
            affected_subjects: update_result.upserted + update_result.removed,
            old_watermark: record.index_t,
            new_watermark: target_t,
            was_full_resync: true,
        })
    }

    /// Sync multiple BM25 indexes.
    pub async fn sync_bm25_indexes(
        &self,
        graph_source_ides: &[&str],
    ) -> Vec<Result<Bm25SyncResult>> {
        let mut results = Vec::with_capacity(graph_source_ides.len());
        for alias in graph_source_ides {
            results.push(self.sync_bm25_index(alias).await);
        }
        results
    }

    /// Check staleness for multiple BM25 indexes.
    pub async fn check_bm25_staleness_batch(
        &self,
        graph_source_ides: &[&str],
    ) -> Vec<Result<Bm25StalenessCheck>> {
        let mut results = Vec::with_capacity(graph_source_ides.len());
        for alias in graph_source_ides {
            results.push(self.check_bm25_staleness(alias).await);
        }
        results
    }

    /// Drop a BM25 full-text index.
    ///
    /// This operation:
    /// 1. Marks the graph source as retracted in nameservice
    /// 2. Deletes all snapshot files from storage
    pub async fn drop_full_text_index(&self, graph_source_id: &str) -> Result<Bm25DropResult>
where {
        info!(graph_source_id = %graph_source_id, "Dropping BM25 full-text index");

        // 1. Look up graph source record to verify it exists
        let record = self
            .nameservice()
            .lookup_graph_source(graph_source_id)
            .await?;

        let record = match record {
            Some(r) => r,
            None => {
                return Err(crate::ApiError::NotFound(format!(
                    "Graph source not found: {graph_source_id}"
                )));
            }
        };

        // If already retracted, return early (idempotent)
        if record.retracted {
            info!(graph_source_id = %graph_source_id, "Graph source already retracted");
            return Ok(Bm25DropResult {
                graph_source_id: graph_source_id.to_string(),
                deleted_snapshots: 0,
                was_already_retracted: true,
            });
        }

        // 2. Load manifest for cleanup (get all snapshot addresses)
        let manifest = self.load_or_create_bm25_manifest(graph_source_id).await?;
        let total = manifest.all_snapshot_ids().len();

        // 3. Retract graph source in nameservice
        self.publisher()?
            .retract_graph_source(&record.name, &record.branch)
            .await?;

        info!(
            graph_source_id = %graph_source_id,
            snapshot_count = manifest.snapshots.len(),
            "Graph source retracted, cleaning up storage"
        );

        // 4. Delete all snapshot files
        let (deleted_snapshots, _warnings) =
            self.delete_bm25_snapshots(graph_source_id, &manifest).await;

        info!(
            graph_source_id = %graph_source_id,
            deleted = deleted_snapshots,
            total = total,
            "Drop complete"
        );

        Ok(Bm25DropResult {
            graph_source_id: graph_source_id.to_string(),
            deleted_snapshots,
            was_already_retracted: false,
        })
    }

    /// Delete every snapshot blob a BM25 manifest references, returning the
    /// number removed alongside a warning per blob that could not be.
    ///
    /// Best-effort per blob: a delete that fails leaves a storage leak, which
    /// is not a reason to abandon the rest of the sweep or to leave the
    /// nameservice record published.
    ///
    /// Takes the manifest rather than loading it so callers control the order
    /// relative to retraction.
    pub(crate) async fn delete_bm25_snapshots(
        &self,
        graph_source_id: &str,
        manifest: &Bm25Manifest,
    ) -> (usize, Vec<String>) {
        let Some(storage) = self.admin_storage() else {
            return (0, Vec::new());
        };

        let method = storage.storage_method().to_string();
        let mut deleted = 0;
        let mut warnings = Vec::new();

        for cid in manifest.all_snapshot_ids() {
            let addr = fluree_db_core::content_address(
                &method,
                fluree_db_core::ContentKind::GraphSourceSnapshot,
                graph_source_id,
                &cid.digest_hex(),
            );
            match storage.delete(&addr).await {
                Ok(()) => deleted += 1,
                Err(e) => {
                    warn!(
                        graph_source_id = %graph_source_id,
                        address = %addr,
                        error = %e,
                        "Failed to delete snapshot file"
                    );
                    warnings.push(format!("Failed to delete snapshot {addr}: {e}"));
                }
            }
        }

        (deleted, warnings)
    }
}

#[cfg(test)]
mod scope_tests {
    use super::*;

    fn iris(v: &[&str]) -> HashSet<Arc<str>> {
        v.iter().map(|s| Arc::from(*s)).collect()
    }

    fn doc_query() -> JsonValue {
        serde_json::json!({
            "@context": { "ex": "http://example.org/" },
            "where": [{ "@id": "?x", "@type": "ex:Doc", "ex:title": "?title" }],
            "select": { "?x": ["@id", "ex:title"] }
        })
    }

    #[test]
    fn binds_the_select_variable_to_the_affected_iris() {
        let scoped = scope_indexing_query_to_subjects(
            &doc_query(),
            &iris(&["http://example.org/doc2", "http://example.org/doc1"]),
        )
        .expect("the documented indexing-query shape should be scopable");

        // Sorted, so the generated query is deterministic across runs.
        assert_eq!(
            scoped["values"],
            serde_json::json!([
                "?x",
                [
                    { "@id": "http://example.org/doc1" },
                    { "@id": "http://example.org/doc2" }
                ]
            ])
        );
        // Everything else is carried through untouched.
        assert_eq!(scoped["where"], doc_query()["where"]);
        assert_eq!(scoped["select"], doc_query()["select"]);
    }

    /// Decline, and say which condition fired. Asserting the reason rather than
    /// just `is_err()` is what stops one guard silently covering for another —
    /// delete the `limit` check and the `limit` case must fail, not fall through
    /// to some other decline and still pass.
    fn assert_declines(q: &JsonValue, expect_reason_contains: &str) {
        let err = scope_indexing_query_to_subjects(q, &iris(&["http://example.org/doc1"]))
            .expect_err("this query shape must not be narrowed");
        assert!(
            err.contains(expect_reason_contains),
            "declined for the wrong reason: wanted something containing \
             {expect_reason_contains:?}, got {err:?}"
        );
    }

    #[test]
    fn declines_a_list_form_select() {
        // A list select does not say which variable is the document, so there is
        // nothing safe to bind.
        let q = serde_json::json!({
            "where": [{ "@id": "?x", "ex:title": "?title" }],
            "select": ["?x", "?title"]
        });
        assert_declines(&q, "not object-form");
    }

    #[test]
    fn declines_a_multi_variable_select() {
        let q = serde_json::json!({
            "select": { "?x": ["@id"], "?y": ["@id"] }
        });
        assert_declines(&q, "more than one variable");
    }

    #[test]
    fn declines_a_query_that_already_has_values() {
        // Injecting here would have to merge with the caller's own bindings.
        let mut q = doc_query();
        q["values"] = serde_json::json!(["?x", [{ "@id": "http://example.org/doc9" }]]);
        assert_declines(&q, "`values`");
    }

    #[test]
    fn declines_an_empty_affected_set() {
        // The caller treats "nothing affected" as a full resync; binding an empty
        // values list would instead silently index nothing.
        let err = scope_indexing_query_to_subjects(&doc_query(), &iris(&[]))
            .expect_err("an empty affected set must not be narrowed");
        assert!(err.contains("affected set is empty"), "got {err:?}");
    }

    #[test]
    fn declines_every_result_shaping_clause_in_both_spellings() {
        // These do not change how rows are FOUND, they change which rows come
        // BACK — and `apply_update` upserts what the query returned while
        // removing what it did not, so a narrowed truncation makes the two paths
        // disagree about the same subject. Concretely for `limit`: unscoped, an
        // affected subject outside the top-N is unseen and gets removed; scoped,
        // it is the only row and gets upserted, leaving one more document indexed
        // than a full resync would produce.
        //
        // Both spellings are checked because the parser accepts both: missing
        // `group-by` or `select-one` would leave exactly that divergence reachable
        // through the alias.
        for (clause, reason) in [
            ("limit", "`limit`"),
            ("offset", "`offset`"),
            ("groupBy", "`groupBy`"),
            ("group-by", "`groupBy`"),
            ("having", "`having`"),
        ] {
            let mut q = doc_query();
            q[clause] = serde_json::json!(2);
            assert_declines(&q, reason);
        }
    }

    #[test]
    fn declines_select_one_in_both_spellings() {
        // `selectOne` returns a single row, so scoping changes WHICH row rather
        // than how it was found. Accepting it "for symmetry with the parser" was
        // the original justification, and symmetry is not a safety argument.
        for clause in ["selectOne", "select-one"] {
            let mut q = serde_json::json!({
                "@context": { "ex": "http://example.org/" },
                "where": [{ "@id": "?x", "@type": "ex:Doc", "ex:title": "?title" }]
            });
            q[clause] = serde_json::json!({ "?x": ["@id", "ex:title"] });
            assert_declines(&q, "`selectOne`");
        }
    }
}
