//! Ledger state combining indexed LedgerSnapshot with novelty overlay
//!
//! This crate provides `LedgerState` which combines:
//! - A persisted `LedgerSnapshot` (the latest indexed state)
//! - A `Novelty` overlay (uncommitted transactions since the last index)
//!
//! Together they provide a consistent view of the ledger at a specific point in time.
//!
//! # Types
//!
//! - [`LedgerState`] - Live ledger state (mutable, has novelty)
//! - [`HistoricalLedgerView`] - Read-only view at a specific time (for time-travel)
//! - [`LedgerView`] - Staged transactions (uncommitted changes)
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_ledger::{LedgerState, HistoricalLedgerView};
//!
//! // Load current ledger state
//! let state = LedgerState::load(&nameservice, "mydb:main", storage).await?;
//! println!("Ledger at t={}", state.t());
//!
//! // Load historical view at t=50
//! let view = HistoricalLedgerView::load_at(&ns, "mydb:main", storage, 50).await?;
//! ```

mod error;
mod historical;
mod staged;

pub use error::{LedgerError, Result};
pub use historical::HistoricalLedgerView;
pub use staged::LedgerView;

use fluree_db_core::{
    BranchedContentStore, ContentId, ContentStore, DictNovelty, Flake, GraphDbRef, GraphId,
    LedgerSnapshot, RuntimeSmallDicts, StorageBackend, TXN_META_GRAPH_ID,
};
use fluree_db_nameservice::{NameService, NsRecord};
use fluree_db_novelty::{
    generate_commit_flakes, stamp_graph_on_commit_flakes, trace_commits_by_id, Commit, Novelty,
};
use futures::StreamExt;
use std::sync::Arc;

/// Type-erased binary index store for query engine access.
///
/// Allows `LedgerState` to carry a `BinaryIndexStore` without
/// depending on `fluree-db-indexer`. The API layer downcasts to
/// the concrete type when building `ContextConfig` for queries.
#[derive(Clone)]
pub struct TypeErasedStore(pub Arc<dyn std::any::Any + Send + Sync>);

impl std::fmt::Debug for TypeErasedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypeErasedStore").finish()
    }
}

/// Configuration for novelty backpressure
#[derive(Clone, Debug)]
pub struct IndexConfig {
    /// Soft threshold - trigger background indexing (default 100KB)
    pub reindex_min_bytes: usize,
    /// Hard threshold - block new commits until indexed (default 1MB)
    pub reindex_max_bytes: usize,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            // Compatibility defaults:
            // - reindex-min-bytes: 100000  (100 kb, decimal)
            // - reindex-max-bytes: 1000000 (1 mb, decimal)
            reindex_min_bytes: 100_000,
            reindex_max_bytes: 1_000_000,
        }
    }
}

/// Ledger state combining indexed LedgerSnapshot with novelty overlay
///
/// Provides a consistent view of the ledger by combining:
/// - The persisted index (LedgerSnapshot)
/// - In-memory uncommitted changes (Novelty)
#[derive(Debug, Clone)]
pub struct LedgerState {
    /// The indexed snapshot
    pub snapshot: LedgerSnapshot,
    /// In-memory overlay of uncommitted transactions
    pub novelty: Arc<Novelty>,
    /// Dictionary novelty layer for subjects and strings.
    ///
    /// Tracks novel dictionary entries introduced since the last index build.
    /// Populated during commit, read during queries, reset at index application.
    pub dict_novelty: Arc<DictNovelty>,
    /// Ledger-scoped runtime IDs for predicates and datatypes.
    ///
    /// Persisted IDs are seeded when a binary index store is attached; novelty-only
    /// predicate/datatype SIDs are appended here during commit so query planning
    /// and runtime stats share one stable identity space.
    pub runtime_small_dicts: Arc<RuntimeSmallDicts>,
    /// Content identifier of the head commit (identity).
    ///
    /// Set during commit (from the computed CID) and during ledger load
    /// (derived from the commit blob hash).
    pub head_commit_id: Option<ContentId>,
    /// Content identifier of the current index root (identity).
    ///
    /// Set when an index is applied (from `IndexResult.root_id`) and during
    /// ledger load (from `NsRecord.index_head_id`).
    pub head_index_id: Option<ContentId>,
    /// Nameservice record (if loaded via nameservice)
    pub ns_record: Option<NsRecord>,
    /// Type-erased binary index store (concrete type: `Arc<BinaryIndexStore>`).
    ///
    /// Set by `Fluree::ledger()` when a binary index is available. Used by
    /// the query engine to enable `BinaryScanOperator` for IRI resolution.
    pub binary_store: Option<TypeErasedStore>,
    /// Default JSON-LD @context for this ledger.
    ///
    /// Captured from turtle @prefix declarations during import and augmented
    /// with built-in namespace prefixes. Applied to queries that don't supply
    /// their own @context. Loaded from CAS via `NsRecord.default_context`.
    pub default_context: Option<serde_json::Value>,
    /// Type-erased spatial index providers, keyed by predicate IRI.
    ///
    /// Each entry is `Arc<dyn SpatialIndexProvider>`. Set by `Fluree::ledger()`
    /// when spatial indexes are available in the binary index root.
    pub spatial_indexes: Option<TypeErasedStore>,
}

impl LedgerState {
    /// Load a ledger from nameservice
    ///
    /// This is resilient to missing index - if the nameservice has commits
    /// but no index yet, it creates a genesis LedgerSnapshot and loads all commits as novelty.
    pub async fn load(
        ns: &dyn NameService,
        ledger_id: &str,
        backend: &StorageBackend,
    ) -> Result<Self> {
        let record = ns
            .lookup(ledger_id)
            .await?
            .ok_or_else(|| LedgerError::not_found(ledger_id))?;

        // For branched ledgers, build a recursive content store that falls
        // back through the branch ancestry DAG. This avoids copying the
        // commit chain when creating a branch — reads fall through to
        // ancestor namespaces for pre-branch-point content.
        if record.source_branch.is_some() {
            let store = Self::build_branched_store(ns, &record, backend).await?;
            return Self::load_with_store(store, record).await;
        }

        let store = backend.content_store(&record.ledger_id);
        Self::load_with_store(store, record).await
    }

    /// Build a recursive `BranchedContentStore` by walking the branch ancestry.
    ///
    /// Delegates to [`fluree_db_nameservice::build_branched_store`] so the
    /// ancestry walk lives in one place — `fluree-db-indexer` reaches the
    /// same logic via the nameservice helpers without taking on a
    /// `fluree-db-ledger` dependency.
    pub async fn build_branched_store(
        ns: &dyn NameService,
        record: &NsRecord,
        backend: &StorageBackend,
    ) -> Result<BranchedContentStore> {
        Ok(fluree_db_nameservice::build_branched_store(backend, ns, record).await?)
    }

    /// Load ledger state using a given content store.
    ///
    /// Shared implementation used by `load` for both regular and branched
    /// ledgers — the only difference is which `ContentStore` is provided.
    async fn load_with_store<C: ContentStore + Clone + 'static>(
        store: C,
        record: NsRecord,
    ) -> Result<Self> {
        // Handle missing index (genesis fallback)
        let (mut snapshot, mut dict_novelty) = match &record.index_head_id {
            Some(index_cid) => {
                let root_bytes = store.get(index_cid).await?;
                let loaded = LedgerSnapshot::from_root_bytes(&root_bytes)?;
                let dn = DictNovelty::with_watermarks(
                    loaded.subject_watermarks.clone(),
                    loaded.string_watermark,
                );
                (loaded, dn)
            }
            None => (
                LedgerSnapshot::genesis(&record.ledger_id),
                DictNovelty::new_genesis(),
            ),
        };

        // Re-stamp canonical ledger_id from the nameservice record.
        //
        // The index root bytes can carry the *source* ledger_id when the index
        // was copied into a new namespace — this happens for branches
        // (create_branch copies the source's index into the branch's namespace)
        // and for pack import/clone into a differently-named destination. If
        // we leave `snapshot.ledger_id` as-is, every subsequent nameservice
        // lookup via `LedgerState::ledger_id()` would target the source's
        // record instead of this ledger's, causing spurious CommitConflict
        // errors on the first write after branching.
        if snapshot.ledger_id != record.ledger_id {
            snapshot.ledger_id = record.ledger_id.clone();
        }

        // Load novelty from commits since index_t
        let head_commit_id = match &record.commit_head_id {
            Some(head_cid) if record.commit_t > snapshot.t => {
                let (novelty_overlay, head_id) = Self::load_novelty(
                    store,
                    head_cid,
                    snapshot.t,
                    &record.ledger_id,
                    &mut snapshot,
                    &mut dict_novelty,
                )
                .await?;
                let head_index_id = record.index_head_id.clone();
                let mut runtime_small_dicts = RuntimeSmallDicts::new();
                runtime_small_dicts.populate_from_flakes_iter(
                    novelty_overlay
                        .iter_index(fluree_db_core::IndexType::Post)
                        .map(|id| novelty_overlay.get_flake(id)),
                );
                return Ok(Self {
                    snapshot,
                    novelty: Arc::new(novelty_overlay),
                    dict_novelty: Arc::new(dict_novelty),
                    runtime_small_dicts: Arc::new(runtime_small_dicts),
                    head_commit_id: head_id,
                    head_index_id,
                    ns_record: Some(record),
                    binary_store: None,
                    default_context: None,
                    spatial_indexes: None,
                });
            }
            _ => record.commit_head_id.clone(),
        };

        let head_index_id = record.index_head_id.clone();
        let novelty_t = snapshot.t;
        Ok(Self {
            snapshot,
            novelty: Arc::new(Novelty::new(novelty_t)),
            dict_novelty: Arc::new(dict_novelty),
            runtime_small_dicts: Arc::new(RuntimeSmallDicts::new()),
            head_commit_id,
            head_index_id,
            ns_record: Some(record),
            binary_store: None,
            default_context: None,
            spatial_indexes: None,
        })
    }

    /// Load novelty from commits since a given index_t.
    ///
    /// Walks the commit chain backwards from `head_cid` using the content store,
    /// collecting flakes for all commits with `t > index_t`.
    ///
    /// Envelope deltas (namespace codes, graph IRIs) are accumulated and applied
    /// to the snapshot via `apply_envelope_deltas()` after the walk completes.
    ///
    /// Returns the novelty overlay and the head commit's ContentId.
    async fn load_novelty<C: ContentStore + Clone + 'static>(
        store: C,
        head_cid: &ContentId,
        index_t: i64,
        ledger_id: &str,
        snapshot: &mut LedgerSnapshot,
        dict_novelty: &mut DictNovelty,
    ) -> Result<(Novelty, Option<ContentId>)> {
        use std::collections::{HashMap, HashSet};

        let mut novelty = Novelty::new(index_t);
        // Accumulate deltas across all commits.
        // IMPORTANT: trace_commits streams HEAD → oldest (newest first).
        // Namespace codes use `or_insert` so newer commits win.
        // Graph IRIs are collected into a set — `apply_delta` dedupes & sorts.
        let mut merged_ns_delta: HashMap<u16, String> = HashMap::new();
        let mut all_graph_iris: HashSet<String> = HashSet::new();

        // Deferred batch approach: collect (flakes, commit_t) batches during
        // the HEAD→oldest walk, then replay oldest→newest after applying deltas.
        // This is required because per-graph novelty routing needs reverse_graph,
        // which depends on namespace_codes from apply_envelope_deltas().
        let mut commit_batches: Vec<(Vec<Flake>, i64)> = Vec::new();

        let stream = trace_commits_by_id(store, head_cid.clone(), index_t);
        futures::pin_mut!(stream);

        while let Some(result) = stream.next().await {
            let commit = result?;

            // Collect flakes for deferred replay
            let meta_flakes = generate_commit_flakes(&commit, ledger_id, commit.t);
            let mut all_flakes = commit.flakes;
            all_flakes.extend(meta_flakes);
            commit_batches.push((all_flakes, commit.t));

            // Accumulate ns_delta (newest wins via or_insert)
            for (code, prefix) in commit.namespace_delta {
                merged_ns_delta.entry(code).or_insert(prefix);
            }

            // Collect graph IRIs from graph_delta values
            for iri in commit.graph_delta.into_values() {
                all_graph_iris.insert(iri);
            }

            // Extract ns_split_mode (immutable after user namespace allocation).
            if let Some(mode) = commit.ns_split_mode {
                snapshot.set_ns_split_mode(mode, commit.t)?;
            }
        }

        // Apply all accumulated deltas to the snapshot in one shot.
        snapshot.apply_envelope_deltas(&merged_ns_delta, &all_graph_iris)?;

        // Stamp commit metadata flakes with txn-meta graph SID now that
        // namespace_codes are complete.
        let txn_meta_iri = fluree_db_core::txn_meta_graph_iri(ledger_id);
        if let Some(g_sid) = snapshot.encode_iri(&txn_meta_iri) {
            for (flakes, _) in &mut commit_batches {
                stamp_graph_on_commit_flakes(flakes, &g_sid);
            }
        }

        // Build reverse_graph now that namespace_codes and graph_registry are complete.
        let mut reverse_graph = snapshot.build_reverse_graph()?;
        // Ensure txn-meta graph is always routable for commit metadata flakes.
        {
            let txn_meta_iri = fluree_db_core::txn_meta_graph_iri(ledger_id);
            if let Some(g_sid) = snapshot.encode_iri(&txn_meta_iri) {
                reverse_graph.entry(g_sid).or_insert(TXN_META_GRAPH_ID);
            }
        }

        // Replay oldest→newest (walk was HEAD→oldest, so reverse)
        commit_batches.reverse();
        for (flakes, commit_t) in commit_batches {
            // Populate dict_novelty with subjects/strings from replayed commits so
            // overlay translation can resolve IDs for unindexed commits.
            dict_novelty.populate_from_flakes(&flakes);
            novelty.apply_commit(flakes, commit_t, &reverse_graph)?;
        }

        Ok((novelty, Some(head_cid.clone())))
    }

    /// Create a new ledger state from components
    pub fn new(snapshot: LedgerSnapshot, novelty: Novelty) -> Self {
        let dict_novelty = DictNovelty::with_watermarks(
            snapshot.subject_watermarks.clone(),
            snapshot.string_watermark,
        );
        let mut runtime_small_dicts = RuntimeSmallDicts::new();
        runtime_small_dicts.populate_from_flakes_iter(
            novelty
                .iter_index(fluree_db_core::IndexType::Post)
                .map(|id| novelty.get_flake(id)),
        );
        Self {
            snapshot,
            novelty: Arc::new(novelty),
            dict_novelty: Arc::new(dict_novelty),
            runtime_small_dicts: Arc::new(runtime_small_dicts),
            head_commit_id: None,
            head_index_id: None,
            ns_record: None,
            binary_store: None,
            default_context: None,
            spatial_indexes: None,
        }
    }

    /// Get the current transaction time (max of index and novelty)
    pub fn t(&self) -> i64 {
        self.novelty.t.max(self.snapshot.t)
    }

    /// Get the indexed transaction time
    pub fn index_t(&self) -> i64 {
        self.snapshot.t
    }

    /// Get the ledger ID
    pub fn ledger_id(&self) -> &str {
        &self.snapshot.ledger_id
    }

    /// Check if novelty is at max capacity (should block new commits)
    pub fn at_max_novelty(&self, config: &IndexConfig) -> bool {
        self.novelty.size >= config.reindex_max_bytes
    }

    /// Check if novelty should trigger background indexing
    pub fn should_reindex(&self, config: &IndexConfig) -> bool {
        self.novelty.size >= config.reindex_min_bytes
    }

    /// Create a `GraphDbRef` bundling snapshot, graph id, overlay, and time.
    ///
    /// `t` is set to `max(novelty.t, snapshot.t)` — the correct upper bound
    /// including all committed flakes.
    pub fn as_graph_db_ref(&self, g_id: GraphId) -> GraphDbRef<'_> {
        GraphDbRef::new(&self.snapshot, g_id, &*self.novelty, self.t())
            .with_runtime_small_dicts(&self.runtime_small_dicts)
    }

    /// Get the novelty size in bytes
    pub fn novelty_size(&self) -> usize {
        self.novelty.size
    }

    /// Get the novelty epoch
    pub fn epoch(&self) -> u64 {
        self.novelty.epoch
    }

    /// Get a reference to the novelty overlay
    pub fn novelty(&self) -> &Arc<Novelty> {
        &self.novelty
    }

    /// Get current stats (indexed + novelty merged)
    ///
    /// Always returns an IndexStats, even for genesis/no-index ledgers.
    /// Falls back to default stats and applies novelty deltas.
    ///
    /// This is the canonical way to get up-to-date statistics for a ledger,
    /// as it includes both the indexed stats and any uncommitted changes
    /// from the novelty layer.
    pub fn current_stats(&self) -> fluree_db_core::IndexStats {
        let indexed = self.snapshot.stats.clone().unwrap_or_default(); // IndexStats::default() for genesis/no-index
        fluree_db_novelty::current_stats(&indexed, self.novelty.as_ref())
    }

    /// Apply a new index, updating LedgerSnapshot and pruning novelty
    ///
    /// # Semantics
    ///
    /// - The loaded LedgerSnapshot's `t` represents `index_t` (time the index is current through)
    /// - Accepts if `new_index_t > current_index_t` (forward progress)
    /// - Allows `new_index_t <= commit_t` (index catching up to commits)
    /// - Equal-t with different CID: ignored (no-op) for now
    /// - Prunes novelty up to `new_index_t`
    ///
    /// # Arguments
    ///
    /// * `index_id` - Content identifier of the new index root
    ///
    /// # Errors
    ///
    /// - `LedgerIdMismatch` if the new index is for a different ledger
    /// - `StaleIndex` if the new index is older than the current index
    /// - `Core` errors from loading the index
    pub async fn apply_index(&mut self, index_id: &ContentId, cs: &dyn ContentStore) -> Result<()> {
        let root_bytes = cs.get(index_id).await?;
        let new_snapshot = LedgerSnapshot::from_root_bytes(&root_bytes)?;

        // Verify ledger ID matches
        if new_snapshot.ledger_id != self.snapshot.ledger_id {
            return Err(LedgerError::ledger_id_mismatch(
                &new_snapshot.ledger_id,
                &self.snapshot.ledger_id,
            ));
        }

        // Verify forward progress on index
        let current_index_t = self.snapshot.t;
        if new_snapshot.t < current_index_t {
            return Err(LedgerError::stale_index(new_snapshot.t, current_index_t));
        }
        if new_snapshot.t == current_index_t {
            // Equal-t: ignore (defer tie-break by hash to multi-indexer phase)
            return Ok(());
        }

        // Clear novelty up to new index_t
        let mut new_novelty = (*self.novelty).clone();
        new_novelty.clear_up_to(new_snapshot.t);

        // Reset dict_novelty with new watermarks from the index root
        let mut new_dict_novelty = DictNovelty::with_watermarks(
            new_snapshot.subject_watermarks.clone(),
            new_snapshot.string_watermark,
        );
        // Re-populate dict_novelty with any remaining novelty flakes (t > index_t)
        // so overlay translation can resolve newly-introduced subject/string IDs.
        if !new_novelty.is_empty() {
            new_dict_novelty.populate_from_flakes_iter(
                new_novelty
                    .iter_index(fluree_db_core::IndexType::Post)
                    .map(|id| new_novelty.get_flake(id)),
            );
        }

        let mut new_runtime_small_dicts = RuntimeSmallDicts::new();
        if !new_novelty.is_empty() {
            new_runtime_small_dicts.populate_from_flakes_iter(
                new_novelty
                    .iter_index(fluree_db_core::IndexType::Post)
                    .map(|id| new_novelty.get_flake(id)),
            );
        }

        // Update state
        self.snapshot = new_snapshot;
        self.novelty = Arc::new(new_novelty);
        self.dict_novelty = Arc::new(new_dict_novelty);
        self.runtime_small_dicts = Arc::new(new_runtime_small_dicts);
        self.head_index_id = Some(index_id.clone());

        // Update ns_record
        if let Some(ref mut record) = self.ns_record {
            record.index_head_id = Some(index_id.clone());
            record.index_t = self.snapshot.t;
        }

        Ok(())
    }

    /// Apply a pre-loaded LedgerSnapshot as the new index.
    ///
    /// Same validation as `apply_index()` but takes an already-loaded LedgerSnapshot,
    /// avoiding the storage I/O call. This enables the API level to:
    /// 1. Read root bytes once
    /// 2. Load `BinaryIndexStore` and attach `BinaryRangeProvider` to the LedgerSnapshot
    /// 3. Apply the enriched LedgerSnapshot here in a brief, non-async swap
    ///
    /// The caller is responsible for ensuring the LedgerSnapshot has `range_provider` set
    /// if it's a binary-only (v2) LedgerSnapshot.
    pub fn apply_loaded_db(
        &mut self,
        new_snapshot: LedgerSnapshot,
        index_id: Option<&ContentId>,
    ) -> Result<()> {
        // Verify ledger ID matches
        if new_snapshot.ledger_id != self.snapshot.ledger_id {
            return Err(LedgerError::ledger_id_mismatch(
                &new_snapshot.ledger_id,
                &self.snapshot.ledger_id,
            ));
        }

        // Verify forward progress on index
        let current_index_t = self.snapshot.t;
        if new_snapshot.t < current_index_t {
            return Err(LedgerError::stale_index(new_snapshot.t, current_index_t));
        }
        if new_snapshot.t == current_index_t {
            return Ok(());
        }

        // Clear novelty up to new index_t
        let mut new_novelty = (*self.novelty).clone();
        new_novelty.clear_up_to(new_snapshot.t);

        // Reset dict_novelty with new watermarks from the index root
        let mut new_dict_novelty = DictNovelty::with_watermarks(
            new_snapshot.subject_watermarks.clone(),
            new_snapshot.string_watermark,
        );
        // Re-populate dict_novelty with any remaining novelty flakes (t > index_t)
        // so overlay translation can resolve newly-introduced subject/string IDs.
        // Note: use `size > 0` not `is_empty()` — after clear_up_to the arena still
        // holds dead flakes, but `size` tracks only active bytes.
        let has_remaining_novelty = new_novelty.size > 0;
        if has_remaining_novelty {
            new_dict_novelty.populate_from_flakes_iter(
                new_novelty
                    .iter_index(fluree_db_core::IndexType::Post)
                    .map(|id| new_novelty.get_flake(id)),
            );
        }

        let mut new_runtime_small_dicts = RuntimeSmallDicts::new();
        if has_remaining_novelty {
            new_runtime_small_dicts.populate_from_flakes_iter(
                new_novelty
                    .iter_index(fluree_db_core::IndexType::Post)
                    .map(|id| new_novelty.get_flake(id)),
            );
        }

        // Preserve namespace codes and graph IRIs from commits still in novelty.
        // The new snapshot from the index root only has codes/IRIs up to index_t.
        // Post-index commits may have introduced new ones that the remaining
        // novelty flakes still reference for encoding/decoding and graph routing.
        let mut merged_snapshot = new_snapshot;
        if has_remaining_novelty {
            // Collect old graph IRIs before moving self.snapshot
            let old_graph_iris: Vec<String> = self
                .snapshot
                .graph_registry
                .iter_entries()
                .map(|(_, iri)| iri.to_string())
                .collect();

            // Merge namespace codes: old entries not in new → carried forward
            for (code, prefix) in self.snapshot.namespaces() {
                merged_snapshot.insert_namespace_code(*code, prefix.clone())?;
            }

            // Merge graph IRIs via apply_delta (idempotent — skips already-registered)
            merged_snapshot.graph_registry.apply_delta(&old_graph_iris);
        }

        // Update state
        self.snapshot = merged_snapshot;
        self.novelty = Arc::new(new_novelty);
        self.dict_novelty = Arc::new(new_dict_novelty);
        self.runtime_small_dicts = Arc::new(new_runtime_small_dicts);
        self.head_index_id = index_id.cloned();

        // Update ns_record
        if let Some(ref mut record) = self.ns_record {
            record.index_head_id = index_id.cloned();
            record.index_t = self.snapshot.t;
        }

        Ok(())
    }

    /// Apply a single commit to the existing ledger state (incremental update).
    ///
    /// This is the fast path for `UpdatePlan::CommitCatchUp` — loads one commit's
    /// flakes and merges them into the existing novelty without re-walking the
    /// entire commit chain.
    ///
    /// Reuses the same delta-accumulation logic as `load_novelty()` but for a
    /// single commit, avoiding the stream/batch machinery.
    pub fn apply_single_commit(&mut self, commit: Commit, ledger_id: &str) -> Result<()> {
        let commit_t = commit.t;
        let current_t = self.t();

        // Guard: commit must have a valid CID
        let commit_id = commit.id.clone().ok_or_else(|| {
            LedgerError::InvalidData(format!(
                "Cannot apply commit at t={commit_t}: missing content ID"
            ))
        })?;

        // Guard: commit.t must be exactly current_t + 1 (monotonic, no gaps)
        if commit_t != current_t + 1 {
            return Err(LedgerError::InvalidData(format!(
                "Cannot apply commit at t={commit_t}: expected t={}, current t={current_t}",
                current_t + 1
            )));
        }

        // Collect graph IRIs from graph_delta
        let graph_iris: std::collections::HashSet<String> =
            commit.graph_delta.values().cloned().collect();

        // Apply ns_split_mode first (immutable after user namespace allocation).
        // Must happen before apply_envelope_deltas so that a genesis commit
        // declaring a non-default mode doesn't fail the immutability check
        // when its namespace codes are inserted under the wrong mode.
        if let Some(mode) = commit.ns_split_mode {
            self.snapshot.set_ns_split_mode(mode, commit_t)?;
        }

        // Apply namespace + graph deltas to snapshot
        self.snapshot
            .apply_envelope_deltas(&commit.namespace_delta, &graph_iris)?;

        // Generate commit metadata flakes
        let mut meta_flakes = generate_commit_flakes(&commit, ledger_id, commit_t);

        // Stamp txn-meta graph SID on metadata flakes
        let txn_meta_iri = fluree_db_core::txn_meta_graph_iri(ledger_id);
        if let Some(g_sid) = self.snapshot.encode_iri(&txn_meta_iri) {
            stamp_graph_on_commit_flakes(&mut meta_flakes, &g_sid);
        }

        // Combine data flakes + metadata flakes
        let mut all_flakes = commit.flakes;
        all_flakes.extend(meta_flakes);

        // Build reverse_graph for per-graph novelty routing
        let mut reverse_graph = self.snapshot.build_reverse_graph()?;
        // Ensure txn-meta graph is always routable
        if let Some(g_sid) = self.snapshot.encode_iri(&txn_meta_iri) {
            reverse_graph.entry(g_sid).or_insert(TXN_META_GRAPH_ID);
        }

        // Clone and extend dict_novelty (existing entries still valid)
        let mut new_dict_novelty = (*self.dict_novelty).clone();
        new_dict_novelty.populate_from_flakes(&all_flakes);

        let mut new_runtime_small_dicts = (*self.runtime_small_dicts).clone();
        new_runtime_small_dicts.populate_from_flakes(&all_flakes);

        // Clone and extend novelty
        let mut new_novelty = (*self.novelty).clone();
        new_novelty.apply_commit(all_flakes, commit_t, &reverse_graph)?;

        // Update state
        self.novelty = Arc::new(new_novelty);
        self.dict_novelty = Arc::new(new_dict_novelty);
        self.runtime_small_dicts = Arc::new(new_runtime_small_dicts);
        self.head_commit_id = Some(commit_id.clone());

        // Update ns_record
        if let Some(ref mut record) = self.ns_record {
            record.commit_head_id = Some(commit_id);
            record.commit_t = commit_t;
        }

        Ok(())
    }

    /// Check nameservice for newer index and apply if available
    ///
    /// Returns `true` if a newer index was applied, `false` otherwise.
    ///
    /// # Errors
    ///
    /// - `NotFound` if the ledger is not in the nameservice
    /// - `MissingIndexAddress` if nameservice has index_t but no index CID
    /// - Other errors from `apply_index`
    pub async fn maybe_apply_newer_index(
        &mut self,
        ns: &dyn NameService,
        cs: &dyn ContentStore,
    ) -> Result<bool> {
        let record = ns
            .lookup(&self.snapshot.ledger_id)
            .await?
            .ok_or_else(|| LedgerError::not_found(&self.snapshot.ledger_id))?;

        // Only apply if there's a newer index AND it has a CID
        if record.index_t > self.snapshot.t {
            let index_id = record.index_head_id.as_ref().ok_or_else(|| {
                LedgerError::missing_index_id(&self.snapshot.ledger_id, record.index_t)
            })?;
            self.apply_index(index_id, cs).await?;
            return Ok(true);
        }

        Ok(false)
    }

    /// Check if indexing should be triggered and return the alias if so
    ///
    /// This is a convenience method for use after committing transactions.
    /// It checks if novelty has exceeded the soft threshold (reindex_min_bytes)
    /// and returns `Some(alias)` if indexing should be triggered.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // After committing a transaction:
    /// if let Some(alias) = ledger.maybe_trigger_index(&index_config) {
    ///     // Trigger background indexing
    ///     indexer_handle.trigger(alias);
    /// }
    /// ```
    ///
    /// For blocking scenarios where indexing must complete before proceeding,
    /// check `at_max_novelty()` instead and wait for indexing to complete.
    pub fn maybe_trigger_index(&self, config: &IndexConfig) -> Option<&str> {
        if self.should_reindex(config) {
            Some(self.ledger_id())
        } else {
            None
        }
    }

    /// Check if at max novelty and return alias for blocking scenarios
    ///
    /// This is for use in blocking scenarios where a commit should wait
    /// for indexing to complete before proceeding.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Before committing when novelty is at max:
    /// if let Some(alias) = ledger.require_index(&index_config) {
    ///     // Trigger and wait for indexing
    ///     let result = indexer_handle.trigger_and_wait(alias).await?;
    ///     ledger.apply_index(&result.root_id, &content_store).await?;
    /// }
    /// ```
    pub fn require_index(&self, config: &IndexConfig) -> Option<&str> {
        if self.at_max_novelty(config) {
            Some(self.ledger_id())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{
        content_store_for, ContentId, ContentKind, Flake, FlakeValue, MemoryStorage, Sid,
    };
    use fluree_db_nameservice::memory::MemoryNameService;
    use std::collections::HashMap;

    fn make_flake(s: u16, p: u16, o: i64, t: i64) -> Flake {
        Flake::new(
            Sid::new(s, format!("s{s}")),
            Sid::new(p, format!("p{p}")),
            FlakeValue::Long(o),
            Sid::new(2, "long"),
            t,
            true,
            None,
        )
    }

    /// Helper: build minimal FIR6 root bytes for testing.
    ///
    /// This must be decodable by `fluree_db_core::LedgerSnapshot::from_root_bytes()`,
    /// which parses FIR6 metadata beyond the header (namespace table, dict refs,
    /// watermarks, routing tables, etc.). We therefore include the full required
    /// FIR6 metadata *skeleton* with empty counts for all variable-length sections.
    fn build_test_fir6(ledger_id: &str, index_t: i64) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);
        buf.extend_from_slice(b"FIR6"); // magic
        buf.push(1); // version
        buf.push(0); // flags (no optional sections)
        buf.extend_from_slice(&0u16.to_le_bytes()); // pad
        buf.extend_from_slice(&index_t.to_le_bytes()); // index_t
        buf.extend_from_slice(&0i64.to_le_bytes()); // base_t
                                                    // Ledger ID (u16 length prefix + UTF-8 bytes)
        let lid = ledger_id.as_bytes();
        buf.extend_from_slice(&(lid.len() as u16).to_le_bytes());
        buf.extend_from_slice(lid);

        // ---- FIR6 metadata skeleton (all empty) ----
        buf.push(0); // subject_id_encoding (u8)

        buf.extend_from_slice(&0u16.to_le_bytes()); // ns_count (u16) = 0

        buf.extend_from_slice(&0u32.to_le_bytes()); // pred_count (u32) = 0

        // graph_iris / datatype_iris / language_tags: string arrays (u16 count + strings)
        buf.extend_from_slice(&0u16.to_le_bytes()); // graph_iris count = 0
        buf.extend_from_slice(&0u16.to_le_bytes()); // datatype_iris count = 0
        buf.extend_from_slice(&0u16.to_le_bytes()); // language_tags count = 0

        // dict pack refs: ns_count (u16) + packs; empty
        buf.extend_from_slice(&0u16.to_le_bytes());
        // dict tree refs: subject reverse, string reverse; each starts with u16 count
        buf.extend_from_slice(&0u16.to_le_bytes()); // subject reverse sp_count = 0
        buf.extend_from_slice(&0u16.to_le_bytes()); // string reverse sp_count = 0

        // per-graph specialty arenas: arena_count (u16) = 0
        buf.extend_from_slice(&0u16.to_le_bytes());

        // watermarks
        buf.extend_from_slice(&0u16.to_le_bytes()); // wm_count = 0
        buf.extend_from_slice(&0u32.to_le_bytes()); // string_watermark = 0

        // cumulative commit stats (3 * u64)
        buf.extend_from_slice(&0u64.to_le_bytes()); // total_commit_size
        buf.extend_from_slice(&0u64.to_le_bytes()); // total_asserts
        buf.extend_from_slice(&0u64.to_le_bytes()); // total_retracts

        // o_type table
        buf.extend_from_slice(&0u32.to_le_bytes()); // otype_count = 0

        // default graph routing
        buf.push(0); // default_order_count = 0

        // named graph routing
        buf.extend_from_slice(&0u16.to_le_bytes()); // named_count = 0

        // Defensive padding: keeps this test helper resilient to small FIR6
        // metadata decode changes (additional trailing fields). All padding
        // bytes are zero so any newly-read counts default to empty.
        buf.extend(std::iter::repeat_n(0u8, 64));

        buf
    }

    /// Helper: store FIR6 root bytes via the content store and return the CID.
    async fn store_index_root(storage: &MemoryStorage, ledger_id: &str, index_t: i64) -> ContentId {
        let store = content_store_for(storage.clone(), ledger_id);
        let bytes = build_test_fir6(ledger_id, index_t);
        store.put(ContentKind::IndexRoot, &bytes).await.unwrap()
    }

    /// Helper: store a commit blob via the content store and return the CID.
    async fn store_commit(
        storage: &MemoryStorage,
        ledger_id: &str,
        commit: &fluree_db_novelty::Commit,
    ) -> ContentId {
        let store = content_store_for(storage.clone(), ledger_id);
        let blob = fluree_db_core::commit::codec::write_commit(commit, false, None).unwrap();
        store.put(ContentKind::Commit, &blob.bytes).await.unwrap()
    }

    #[tokio::test]
    async fn test_ledger_state_new() {
        let snapshot = LedgerSnapshot::genesis("test:main");

        let mut novelty = Novelty::new(0);
        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 1)], 1, &HashMap::new())
            .unwrap();

        let state = LedgerState::new(snapshot, novelty);

        assert_eq!(state.ledger_id(), "test:main");
        assert_eq!(state.index_t(), 0);
        assert_eq!(state.t(), 1);
        assert_eq!(state.epoch(), 1);
    }

    #[tokio::test]
    async fn test_ledger_state_backpressure() {
        let snapshot = LedgerSnapshot::genesis("test:main");

        let mut novelty = Novelty::new(0);
        // Add some flakes to increase size
        for i in 0..100 {
            novelty
                .apply_commit(vec![make_flake(i, 1, i as i64, 1)], 1, &HashMap::new())
                .unwrap();
        }

        let state = LedgerState::new(snapshot, novelty);

        let small_config = IndexConfig {
            reindex_min_bytes: 100,
            reindex_max_bytes: 1000,
        };

        assert!(state.novelty_size() > 0);
        assert!(state.should_reindex(&small_config));
        assert!(state.at_max_novelty(&small_config));
    }

    #[tokio::test]
    async fn test_ledger_state_load_genesis() {
        use fluree_db_nameservice::{CasResult, RefPublisher, RefValue};

        async fn publish_commit(ns: &impl RefPublisher, ledger_id: &str, t: i64, cid: &ContentId) {
            let new = RefValue {
                id: Some(cid.clone()),
                t,
            };
            match ns.fast_forward_commit(ledger_id, &new, 3).await.unwrap() {
                CasResult::Updated => {}
                CasResult::Conflict { actual } => {
                    assert!(
                        actual.as_ref().map(|r| r.t).unwrap_or(0) >= t,
                        "unexpected commit publish conflict: {actual:?}"
                    );
                }
            }
        }

        let ns = MemoryNameService::new();
        let storage = MemoryStorage::new();

        // Create a commit and store it via CAS
        let commit = fluree_db_novelty::Commit::new(1, vec![make_flake(1, 1, 100, 1)]);
        let cid = store_commit(&storage, "test:main", &commit).await;

        // Publish to nameservice (no index)
        publish_commit(&ns, "test:main", 1, &cid).await;

        // Load ledger - should use genesis since no index exists
        let backend = StorageBackend::Managed(std::sync::Arc::new(storage));
        let state = LedgerState::load(&ns, "test:main", &backend).await.unwrap();

        assert_eq!(state.ledger_id(), "test:main");
        assert_eq!(state.index_t(), 0); // Genesis
        assert_eq!(state.t(), 1); // From commit
                                  // 1 data flake + 6 commit metadata flakes (db#address, db#alias, db#t, db#asserts, db#retracts, db#size)
        assert_eq!(state.novelty.len(), 7);
    }

    #[tokio::test]
    async fn test_apply_index_success() {
        use fluree_db_core::IndexType;

        let storage = MemoryStorage::new();
        let snapshot = LedgerSnapshot::genesis("test:main");

        // Create novelty with flakes at t=1 and t=2
        let mut novelty = Novelty::new(0);
        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 1)], 1, &HashMap::new())
            .unwrap();
        novelty
            .apply_commit(vec![make_flake(2, 1, 200, 2)], 2, &HashMap::new())
            .unwrap();

        let mut state = LedgerState::new(snapshot, novelty);
        assert_eq!(state.index_t(), 0);
        // Check active flakes via index iterator (arena has 2, and 2 are active)
        assert_eq!(state.novelty.iter_index(IndexType::Spot).count(), 2);

        // Create an FIR6 index root at t=1 and store via CAS
        let index_cid = store_index_root(&storage, "test:main", 1).await;
        let store = content_store_for(storage.clone(), "test:main");

        // Apply the index
        state.apply_index(&index_cid, &store).await.unwrap();

        // Index should now be at t=1
        assert_eq!(state.index_t(), 1);
        // Novelty at t=1 should be cleared, only t=2 remains in the index vectors
        // (arena still has 2 flakes but only 1 is active)
        assert_eq!(state.novelty.iter_index(IndexType::Spot).count(), 1);
    }

    #[test]
    fn test_apply_loaded_db_repopulates_dict_novelty_for_remaining_overlay_strings() {
        use fluree_db_core::IndexType;

        // Scenario:
        // - Index exists at t=1 with string watermark=1
        // - Two commits happen: t=2 adds string "a" (becomes id=2), t=3 adds string "b" (id=3)
        // - A newer index arrives at t=2 (string watermark=2) but commit t=3 remains in novelty
        // - apply_loaded_db must reset watermarks AND repopulate DictNovelty from remaining novelty
        //   so string id 3 is still resolvable (otherwise decode fails with "string id 3 not found").

        let mut snapshot = LedgerSnapshot::genesis("test:main");
        snapshot.t = 1;
        snapshot.string_watermark = 1;

        let mut state = LedgerState::new(snapshot, Novelty::new(1));
        let reverse_graph = state.snapshot.build_reverse_graph().unwrap_or_default();

        let s = Sid::new(0, "ex:s");
        let p = Sid::new(0, "ex:p");
        let dt = Sid::new(2, "string"); // xsd:string (default namespace codes)

        let flakes_t2 = vec![Flake::new(
            s.clone(),
            p.clone(),
            FlakeValue::String("a".to_string()),
            dt.clone(),
            2,
            true,
            None,
        )];
        Arc::make_mut(&mut state.dict_novelty).populate_from_flakes(&flakes_t2);
        Arc::make_mut(&mut state.novelty)
            .apply_commit(flakes_t2, 2, &reverse_graph)
            .unwrap();

        let flakes_t3 = vec![Flake::new(
            s,
            p,
            FlakeValue::String("b".to_string()),
            dt,
            3,
            true,
            None,
        )];
        Arc::make_mut(&mut state.dict_novelty).populate_from_flakes(&flakes_t3);
        Arc::make_mut(&mut state.novelty)
            .apply_commit(flakes_t3, 3, &reverse_graph)
            .unwrap();

        // Sanity: both novelty strings exist before applying the new index.
        assert_eq!(state.dict_novelty.strings.find_string("a"), Some(2));
        assert_eq!(state.dict_novelty.strings.find_string("b"), Some(3));
        assert_eq!(state.dict_novelty.strings.resolve_string(3), Some("b"));

        // Apply a newer index snapshot at t=2 (string watermark=2), leaving commit t=3 in novelty.
        let mut new_snapshot = LedgerSnapshot::genesis("test:main");
        new_snapshot.t = 2;
        new_snapshot.string_watermark = 2;
        state.apply_loaded_db(new_snapshot, None).unwrap();

        // Novelty at t<=2 cleared; t=3 remains active.
        assert_eq!(state.novelty.iter_index(IndexType::Spot).count(), 1);

        // Regression assertion:
        // With watermark=2, the remaining novelty string must still resolve at id=3.
        // Before the fix, apply_loaded_db reset dict_novelty and lost this mapping.
        assert_eq!(state.dict_novelty.strings.watermark(), 2);
        assert_eq!(state.dict_novelty.strings.resolve_string(3), Some("b"));
    }

    #[tokio::test]
    async fn test_apply_index_address_mismatch() {
        let storage = MemoryStorage::new();
        let snapshot = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);

        let mut state = LedgerState::new(snapshot, novelty);

        // Create an FIR6 root for a different ledger, but store under test:main's CAS space
        let bytes = build_test_fir6("other:ledger", 1);
        let store = content_store_for(storage.clone(), "test:main");
        let index_cid = store.put(ContentKind::IndexRoot, &bytes).await.unwrap();

        // Should fail with ledger ID mismatch
        let result = state.apply_index(&index_cid, &store).await;
        assert!(matches!(result, Err(LedgerError::LedgerIdMismatch { .. })));
    }

    #[tokio::test]
    async fn test_apply_index_stale() {
        let storage = MemoryStorage::new();

        // Create an FIR6 root at t=2
        let index_cid_t2 = store_index_root(&storage, "test:main", 2).await;

        // Load the LedgerSnapshot from CAS for current state
        let store = content_store_for(storage.clone(), "test:main");
        let root_bytes = store.get(&index_cid_t2).await.unwrap();
        let snapshot = LedgerSnapshot::from_root_bytes(&root_bytes).unwrap();
        let novelty = Novelty::new(2);
        let mut state = LedgerState::new(snapshot, novelty);
        assert_eq!(state.index_t(), 2);

        // Create an older FIR6 root at t=1
        let index_cid_t1 = store_index_root(&storage, "test:main", 1).await;

        // Should fail with stale index error
        let cs = content_store_for(storage.clone(), "test:main");
        let result = state.apply_index(&index_cid_t1, &cs).await;
        assert!(matches!(result, Err(LedgerError::StaleIndex { .. })));
    }

    #[tokio::test]
    async fn test_apply_index_equal_t_noop() {
        let storage = MemoryStorage::new();

        // Create an FIR6 root at t=1
        let index_cid = store_index_root(&storage, "test:main", 1).await;

        // Load LedgerSnapshot from CAS
        let store = content_store_for(storage.clone(), "test:main");
        let root_bytes = store.get(&index_cid).await.unwrap();
        let snapshot = LedgerSnapshot::from_root_bytes(&root_bytes).unwrap();
        let novelty = Novelty::new(1);
        let mut state = LedgerState::new(snapshot, novelty);

        // Create another FIR6 root at same t (append extra bytes to produce different CID)
        let store2 = content_store_for(storage.clone(), "test:main");
        let mut bytes2 = build_test_fir6("test:main", 1);
        bytes2.extend_from_slice(b"extra-padding");
        let index_cid_same = store2.put(ContentKind::IndexRoot, &bytes2).await.unwrap();

        // Should succeed as no-op (equal t)
        let cs = content_store_for(storage.clone(), "test:main");
        let result = state.apply_index(&index_cid_same, &cs).await;
        assert!(result.is_ok());
        // Index_t should still be 1
        assert_eq!(state.index_t(), 1);
    }

    #[tokio::test]
    async fn test_maybe_trigger_index_below_threshold() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);

        let state = LedgerState::new(snapshot, novelty);

        let config = IndexConfig {
            reindex_min_bytes: 1000,
            reindex_max_bytes: 10000,
        };

        // No data, so below threshold
        let result = state.maybe_trigger_index(&config);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_maybe_trigger_index_above_threshold() {
        let snapshot = LedgerSnapshot::genesis("test:main");

        // Add some flakes to increase size
        let mut novelty = Novelty::new(0);
        for i in 0..100 {
            novelty
                .apply_commit(vec![make_flake(i, 1, i as i64, 1)], 1, &HashMap::new())
                .unwrap();
        }

        let state = LedgerState::new(snapshot, novelty);

        let config = IndexConfig {
            reindex_min_bytes: 100, // Low threshold to trigger
            reindex_max_bytes: 10000,
        };

        // Above min threshold
        let result = state.maybe_trigger_index(&config);
        assert_eq!(result, Some("test:main"));
    }

    #[tokio::test]
    async fn test_require_index_below_max() {
        let snapshot = LedgerSnapshot::genesis("test:main");

        let mut novelty = Novelty::new(0);
        for i in 0..10 {
            novelty
                .apply_commit(vec![make_flake(i, 1, i as i64, 1)], 1, &HashMap::new())
                .unwrap();
        }

        let state = LedgerState::new(snapshot, novelty);

        let config = IndexConfig {
            reindex_min_bytes: 100,
            reindex_max_bytes: 10000, // High max threshold
        };

        // Below max threshold - should not require
        let result = state.require_index(&config);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_require_index_at_max() {
        let snapshot = LedgerSnapshot::genesis("test:main");

        let mut novelty = Novelty::new(0);
        for i in 0..100 {
            novelty
                .apply_commit(vec![make_flake(i, 1, i as i64, 1)], 1, &HashMap::new())
                .unwrap();
        }

        let state = LedgerState::new(snapshot, novelty);

        let config = IndexConfig {
            reindex_min_bytes: 100,
            reindex_max_bytes: 100, // Low max threshold to trigger
        };

        // Above max threshold - should require
        let result = state.require_index(&config);
        assert_eq!(result, Some("test:main"));
    }

    // ========================================================================
    // apply_single_commit tests
    // ========================================================================

    fn make_test_commit_id(label: &str) -> ContentId {
        ContentId::new(ContentKind::Commit, label.as_bytes())
    }

    #[test]
    fn test_apply_single_commit_happy_path() {
        use fluree_db_core::IndexType;

        let snapshot = LedgerSnapshot::genesis("test:main");
        let mut state = LedgerState::new(snapshot, Novelty::new(0));
        assert_eq!(state.t(), 0);

        // Build a commit at t=1
        let commit = Commit::new(1, vec![make_flake(10, 1, 100, 1)])
            .with_id(make_test_commit_id("commit:1"));

        state.apply_single_commit(commit, "test:main").unwrap();

        assert_eq!(state.t(), 1);
        assert_eq!(state.head_commit_id, Some(make_test_commit_id("commit:1")));
        // Should have at least our data flake plus commit metadata flakes
        assert!(state.novelty.iter_index(IndexType::Spot).count() >= 1);
    }

    #[test]
    fn test_apply_single_commit_multiple_sequential() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let mut state = LedgerState::new(snapshot, Novelty::new(0));

        // Apply t=1
        let c1 = Commit::new(1, vec![make_flake(10, 1, 100, 1)])
            .with_id(make_test_commit_id("commit:1"));
        state.apply_single_commit(c1, "test:main").unwrap();
        assert_eq!(state.t(), 1);

        // Apply t=2
        let c2 = Commit::new(2, vec![make_flake(11, 1, 200, 2)])
            .with_id(make_test_commit_id("commit:2"));
        state.apply_single_commit(c2, "test:main").unwrap();
        assert_eq!(state.t(), 2);
        assert_eq!(state.head_commit_id, Some(make_test_commit_id("commit:2")));
    }

    #[test]
    fn test_apply_single_commit_rejects_missing_cid() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let mut state = LedgerState::new(snapshot, Novelty::new(0));

        // Commit without an id
        let commit = Commit::new(1, vec![make_flake(10, 1, 100, 1)]);
        let result = state.apply_single_commit(commit, "test:main");

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, LedgerError::InvalidData(_)),
            "expected InvalidData, got: {err}"
        );
        assert!(err.to_string().contains("missing content ID"));
        // State should not have changed
        assert_eq!(state.t(), 0);
    }

    #[test]
    fn test_apply_single_commit_rejects_non_monotonic_skip() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let mut state = LedgerState::new(snapshot, Novelty::new(0));

        // Try to apply t=3 when current is t=0 (should be t=1)
        let commit = Commit::new(3, vec![make_flake(10, 1, 100, 3)])
            .with_id(make_test_commit_id("commit:3"));
        let result = state.apply_single_commit(commit, "test:main");

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, LedgerError::InvalidData(_)),
            "expected InvalidData, got: {err}"
        );
        assert!(err.to_string().contains("expected t=1"));
        assert_eq!(state.t(), 0);
    }

    #[test]
    fn test_apply_single_commit_rejects_duplicate_t() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let mut state = LedgerState::new(snapshot, Novelty::new(0));

        // Apply t=1 successfully
        let c1 = Commit::new(1, vec![make_flake(10, 1, 100, 1)])
            .with_id(make_test_commit_id("commit:1"));
        state.apply_single_commit(c1, "test:main").unwrap();

        // Try to apply t=1 again
        let c1_dup = Commit::new(1, vec![make_flake(11, 1, 200, 1)])
            .with_id(make_test_commit_id("commit:1-dup"));
        let result = state.apply_single_commit(c1_dup, "test:main");

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("expected t=2"));
        assert_eq!(state.t(), 1); // unchanged
    }

    #[test]
    fn test_apply_single_commit_updates_ns_record() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let mut state = LedgerState::new(snapshot, Novelty::new(0));

        // Set up an ns_record
        state.ns_record = Some(NsRecord::new("test", "main"));

        let commit = Commit::new(1, vec![make_flake(10, 1, 100, 1)])
            .with_id(make_test_commit_id("commit:1"));
        state.apply_single_commit(commit, "test:main").unwrap();

        let record = state.ns_record.as_ref().unwrap();
        assert_eq!(record.commit_t, 1);
        assert_eq!(record.commit_head_id, Some(make_test_commit_id("commit:1")));
    }

    #[test]
    fn test_apply_single_commit_with_namespace_delta() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let mut state = LedgerState::new(snapshot, Novelty::new(0));

        // Commit that introduces a new namespace code
        let mut ns_delta = HashMap::new();
        ns_delta.insert(100u16, "http://example.org/ns/".to_string());

        let mut commit = Commit::new(1, vec![make_flake(10, 1, 100, 1)])
            .with_id(make_test_commit_id("commit:1"))
            .with_namespace_delta(ns_delta);
        // Also add a graph_delta to test graph routing
        commit
            .graph_delta
            .insert(3, "http://example.org/graph/test".to_string());

        state.apply_single_commit(commit, "test:main").unwrap();

        // Namespace code should be in snapshot
        assert_eq!(
            state.snapshot.namespaces().get(&100),
            Some(&"http://example.org/ns/".to_string())
        );
        // Graph should be registered
        assert!(state
            .snapshot
            .graph_registry
            .iter_entries()
            .any(|(_, iri)| iri == "http://example.org/graph/test"));
    }

    #[test]
    fn test_apply_single_commit_populates_dict_novelty() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let mut state = LedgerState::new(snapshot, Novelty::new(0));

        // Commit with a string flake
        let s = Sid::new(0, "ex:s1");
        let p = Sid::new(0, "ex:p1");
        let dt = Sid::new(2, "string");
        let flake = Flake::new(
            s,
            p,
            FlakeValue::String("hello world".to_string()),
            dt,
            1,
            true,
            None,
        );

        let commit = Commit::new(1, vec![flake]).with_id(make_test_commit_id("commit:1"));
        state.apply_single_commit(commit, "test:main").unwrap();

        // dict_novelty should have the string
        assert!(state
            .dict_novelty
            .strings
            .find_string("hello world")
            .is_some());
    }

    // ========================================================================
    // apply_loaded_db envelope delta preservation tests
    // ========================================================================

    #[test]
    fn test_apply_loaded_db_preserves_namespace_codes_from_remaining_novelty() {
        // Scenario:
        // - Genesis snapshot has default namespace codes
        // - Commit at t=1 introduces namespace code 100 → "http://example.org/ns/"
        // - Commit at t=2 adds data using that namespace
        // - An index arrives at t=1 (new snapshot without code 100)
        // - After apply_loaded_db, code 100 must still be in snapshot for t=2 novelty

        let mut snapshot = LedgerSnapshot::genesis("test:main");
        snapshot.t = 0;
        let mut state = LedgerState::new(snapshot, Novelty::new(0));

        // Simulate commit t=1: add namespace code + flake
        state
            .snapshot
            .insert_namespace_code(100, "http://example.org/ns/".to_string())
            .unwrap();
        let reverse_graph = state.snapshot.build_reverse_graph().unwrap_or_default();
        let flakes_t1 = vec![make_flake(10, 1, 100, 1)];
        Arc::make_mut(&mut state.dict_novelty).populate_from_flakes(&flakes_t1);
        Arc::make_mut(&mut state.novelty)
            .apply_commit(flakes_t1, 1, &reverse_graph)
            .unwrap();

        // Simulate commit t=2: another flake
        let flakes_t2 = vec![make_flake(11, 1, 200, 2)];
        Arc::make_mut(&mut state.dict_novelty).populate_from_flakes(&flakes_t2);
        Arc::make_mut(&mut state.novelty)
            .apply_commit(flakes_t2, 2, &reverse_graph)
            .unwrap();

        assert_eq!(state.t(), 2);

        // Apply index at t=1 — new snapshot won't have code 100
        let mut new_snapshot = LedgerSnapshot::genesis("test:main");
        new_snapshot.t = 1;
        state.apply_loaded_db(new_snapshot, None).unwrap();

        // Novelty at t=1 cleared, t=2 remains
        assert_eq!(state.index_t(), 1);

        // Key assertion: namespace code 100 must be preserved because
        // remaining novelty (t=2) may reference subjects/predicates using it
        assert_eq!(
            state.snapshot.namespaces().get(&100),
            Some(&"http://example.org/ns/".to_string()),
            "namespace code from post-index commit should be preserved"
        );
    }

    #[test]
    fn test_apply_loaded_db_preserves_graph_iris_from_remaining_novelty() {
        use fluree_db_core::IndexType;

        // Scenario:
        // - Commit at t=1 introduces a named graph "http://example.org/graph/test"
        // - Commit at t=2 adds data to that graph
        // - Index arrives at t=1 (new snapshot without the custom graph)
        // - After apply_loaded_db, the graph must still be registered for t=2 routing

        let mut snapshot = LedgerSnapshot::genesis("test:main");
        snapshot.t = 0;
        let mut state = LedgerState::new(snapshot, Novelty::new(0));

        // Register a custom graph in the old snapshot (simulating commit t=1)
        let graph_iri = "http://example.org/graph/test";
        state
            .snapshot
            .graph_registry
            .apply_delta(&[graph_iri.to_string()]);

        // Verify graph was registered
        let has_graph_before = state
            .snapshot
            .graph_registry
            .iter_entries()
            .any(|(_, iri)| iri == graph_iri);
        assert!(has_graph_before, "graph should be registered before index");

        // Add novelty at t=1 and t=2
        let reverse_graph = state.snapshot.build_reverse_graph().unwrap_or_default();
        let flakes_t1 = vec![make_flake(10, 1, 100, 1)];
        Arc::make_mut(&mut state.novelty)
            .apply_commit(flakes_t1, 1, &reverse_graph)
            .unwrap();
        let flakes_t2 = vec![make_flake(11, 1, 200, 2)];
        Arc::make_mut(&mut state.novelty)
            .apply_commit(flakes_t2, 2, &reverse_graph)
            .unwrap();

        // Apply index at t=1 — new snapshot won't have the custom graph
        let mut new_snapshot = LedgerSnapshot::genesis("test:main");
        new_snapshot.t = 1;
        state.apply_loaded_db(new_snapshot, None).unwrap();

        // Remaining novelty at t=2 exists
        assert_eq!(state.novelty.iter_index(IndexType::Spot).count(), 1);

        // Key assertion: graph IRI must survive for t=2 routing
        let has_graph_after = state
            .snapshot
            .graph_registry
            .iter_entries()
            .any(|(_, iri)| iri == graph_iri);
        assert!(
            has_graph_after,
            "graph IRI from post-index commit should be preserved in registry"
        );
    }

    #[test]
    fn test_apply_loaded_db_does_not_merge_when_novelty_empty() {
        use fluree_db_core::IndexType;

        // When all novelty is absorbed by the new index, no merging of old
        // snapshot namespace codes/graph IRIs should occur.

        let mut snapshot = LedgerSnapshot::genesis("test:main");
        snapshot.t = 0;
        let mut state = LedgerState::new(snapshot, Novelty::new(0));

        // Add custom namespace to old snapshot
        state
            .snapshot
            .insert_namespace_code(200, "http://old.example.org/".to_string())
            .unwrap();

        // Add novelty at t=1 only
        let reverse_graph = state.snapshot.build_reverse_graph().unwrap_or_default();
        let flakes_t1 = vec![make_flake(10, 1, 100, 1)];
        Arc::make_mut(&mut state.novelty)
            .apply_commit(flakes_t1, 1, &reverse_graph)
            .unwrap();

        // Apply index at t=1 — absorbs all novelty
        let mut new_snapshot = LedgerSnapshot::genesis("test:main");
        new_snapshot.t = 1;
        state.apply_loaded_db(new_snapshot, None).unwrap();

        // All novelty at t<=1 should be cleared (no active flakes remain)
        assert_eq!(
            state.novelty.iter_index(IndexType::Spot).count(),
            0,
            "all novelty should be absorbed by the index"
        );

        // Old namespace code 200 should NOT be carried forward since
        // there's no remaining novelty that needs it
        assert!(
            !state.snapshot.namespaces().contains_key(&200),
            "old namespace codes should not leak into new snapshot when novelty is empty"
        );
    }
}
