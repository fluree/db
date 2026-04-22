//! Historical ledger view for time-travel queries
//!
//! This module provides `HistoricalLedgerView` for querying a ledger at a specific
//! point in time. Unlike `LedgerState` which represents the live, mutable head,
//! `HistoricalLedgerView` is read-only and time-bounded.
//!
//! # Design
//!
//! A historical view consists of:
//! - A `LedgerSnapshot` loaded from the head index (or genesis if no index exists)
//! - An optional `Novelty` overlay for commits between `index_t` and `target_t`
//! - A `to_t` field that bounds all queries
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_ledger::HistoricalLedgerView;
//!
//! // Load a view at t=50
//! let view = HistoricalLedgerView::load_at(
//!     &ns, "mydb:main", storage, 50
//! ).await?;
//!
//! // Query using the view as an overlay provider
//! execute_pattern_with_overlay(&view.snapshot, view.overlay(), &vars, &pattern, view.to_t()).await?;
//! ```

use crate::error::{LedgerError, Result};
use crate::LedgerState;
use fluree_db_core::{
    ContentId, ContentStore, Flake, FlakeMeta, FlakeValue, GraphDbRef, GraphId, IndexType,
    LedgerSnapshot, OverlayProvider, RuntimeSmallDicts, Sid, StorageBackend, TXN_META_GRAPH_ID,
};
use fluree_db_nameservice::NameService;

use fluree_db_novelty::{
    generate_commit_flakes, stamp_graph_on_commit_flakes, trace_commits_by_id, Novelty,
};
use fluree_vocab::namespaces::{FLUREE_COMMIT, JSON_LD, RDF, XSD};
use fluree_vocab::{rdf_names, xsd_names};
use futures::StreamExt;
use std::sync::Arc;

/// Read-only ledger view for time-bounded historical queries
///
/// This struct provides a consistent view of a ledger at a specific point in time.
/// It combines:
/// - The head index (or genesis if no index exists yet)
/// - An optional novelty overlay (commits between `index_t` and `to_t`)
///
/// Indexes are cumulative and contain all historical data, so the head index
/// is always valid for any query. The `to_t` field bounds query results.
///
/// Unlike `LedgerState`, this is immutable and cannot be updated.
#[derive(Debug)]
pub struct HistoricalLedgerView {
    /// The indexed snapshot (head index or genesis)
    pub snapshot: LedgerSnapshot,
    /// Optional novelty overlay (commits between index_t and to_t)
    overlay: Option<Arc<Novelty>>,
    /// Ledger-scoped runtime IDs for predicates and datatypes when a binary store is attached.
    runtime_small_dicts: Option<Arc<RuntimeSmallDicts>>,
    /// Time bound for all queries
    to_t: i64,
}

impl HistoricalLedgerView {
    /// Load a historical view of a ledger at a specific time
    ///
    /// # Algorithm
    ///
    /// 1. Use head index if available (indexes are cumulative, contain all historical data)
    /// 2. Fall back to genesis only if no index exists
    /// 3. Build novelty overlay from commits in `(index_t, target_t]` if needed
    ///
    /// # Arguments
    ///
    /// * `ns` - Nameservice for ledger discovery
    /// * `ledger_id` - Ledger ID (e.g., "mydb:main")
    /// * `storage` - Storage backend
    /// * `target_t` - The time to load the view at
    ///
    /// # Errors
    ///
    /// - `NotFound` if the ledger doesn't exist
    /// - `FutureTime` if `target_t` is beyond the current head
    pub async fn load_at(
        ns: &dyn NameService,
        alias: &str,
        backend: &StorageBackend,
        target_t: i64,
    ) -> Result<Self> {
        let record = ns
            .lookup(alias)
            .await?
            .ok_or_else(|| LedgerError::not_found(alias))?;

        // Check if target_t is in the future
        if target_t > record.commit_t {
            return Err(LedgerError::future_time(alias, target_t, record.commit_t));
        }

        // For branched ledgers, build a recursive content store that falls
        // back through the branch ancestry DAG.
        if record.source_branch.is_some() {
            let store = LedgerState::build_branched_store(ns, &record, backend).await?;
            return Self::load_at_with_store(store, record, target_t).await;
        }

        let store = backend.content_store(&record.ledger_id);
        Self::load_at_with_store(store, record, target_t).await
    }

    /// Load a historical view using a given content store.
    ///
    /// Shared implementation for both regular and branched ledgers.
    async fn load_at_with_store<C: ContentStore + Clone + 'static>(
        store: C,
        record: fluree_db_nameservice::NsRecord,
        target_t: i64,
    ) -> Result<Self> {
        // The binary index covers `base_t..=index_t` via FIR6 Region 3 history.
        // Use the indexed snapshot whenever it covers `target_t`:
        //   - `target_t >= base_t`: index can serve the query (with overlay replay
        //     for any commits in `(index_t, target_t]` when `target_t > index_t`).
        //   - `target_t <  base_t`: index has been compacted past the target, so
        //     fall back to overlay-only reconstruction from genesis.
        //
        // `base_t` must be read from the index root itself (it isn't in the
        // nameservice record), so we load the root first and then decide.
        let (mut snapshot, index_t) = match record.index_head_id.as_ref() {
            Some(index_cid) => {
                let root_bytes = store.get(index_cid).await?;
                let loaded = LedgerSnapshot::from_root_bytes(&root_bytes)?;
                if target_t < loaded.base_t {
                    tracing::debug!(
                        target_t,
                        base_t = loaded.base_t,
                        index_t = loaded.t,
                        "HistoricalLedgerView: target before index base_t, falling back to overlay-only replay"
                    );
                    (LedgerSnapshot::genesis(&record.ledger_id), 0)
                } else {
                    let t = loaded.t;
                    (loaded, t)
                }
            }
            None => (LedgerSnapshot::genesis(&record.ledger_id), 0),
        };

        // Build novelty from commits between index_t and target_t.
        // When we are in overlay-only mode (use_index=false), this replays *all*
        // commits up to target_t (index_t=0), producing a correct time-travel snapshot
        // without relying on index history coverage.
        let overlay = if let Some(head_cid) = &record.commit_head_id {
            if target_t > index_t {
                tracing::trace!(target_t, index_t, "HistoricalLedgerView: loading novelty");
                let novelty = Self::load_novelty_range(
                    store,
                    head_cid,
                    index_t,
                    target_t,
                    &record.ledger_id,
                    &mut snapshot,
                )
                .await?;

                if novelty.is_empty() {
                    tracing::trace!("HistoricalLedgerView: novelty is empty");
                    None
                } else {
                    tracing::trace!(
                        epoch = novelty.epoch,
                        "HistoricalLedgerView: returning with overlay"
                    );
                    Some(Arc::new(novelty))
                }
            } else {
                tracing::trace!(target_t, index_t, "HistoricalLedgerView: no novelty needed");
                None
            }
        } else {
            tracing::trace!("HistoricalLedgerView: no commit_head_id, no novelty");
            None
        };

        Ok(Self {
            snapshot,
            overlay,
            runtime_small_dicts: None,
            to_t: target_t,
        })
    }

    /// Load novelty from commits within a specific range
    ///
    /// Walks the commit chain backwards from `head_cid` using the content store,
    /// including only commits where `index_t < commit.t <= target_t`.
    ///
    /// Uses a deferred batch approach: collect flakes during the HEAD→oldest walk,
    /// apply namespace/graph deltas, build reverse_graph, then replay oldest→newest.
    async fn load_novelty_range<C: ContentStore + Clone + 'static>(
        store: C,
        head_cid: &ContentId,
        index_t: i64,
        target_t: i64,
        ledger_id: &str,
        snapshot: &mut LedgerSnapshot,
    ) -> Result<Novelty> {
        use std::collections::{HashMap, HashSet};

        tracing::trace!(
            %head_cid,
            index_t,
            target_t,
            "load_novelty_range: starting"
        );

        let mut novelty = Novelty::new(index_t);
        let mut merged_ns_delta: HashMap<u16, String> = HashMap::new();
        let mut all_graph_iris: HashSet<String> = HashSet::new();

        // Deferred txn-meta: raw entries per commit (subject Sid, entries, t).
        // Actual flakes are built after apply_envelope_deltas so that
        // encode_iri can produce the correct txn-meta graph Sid.
        struct DeferredTxnMeta {
            commit_subject: Sid,
            entries: Vec<fluree_db_novelty::TxnMetaEntry>,
            t: i64,
        }

        // Collect (data flakes + commit-meta flakes, deferred txn-meta, t) per commit.
        let mut commit_batches: Vec<(Vec<Flake>, Option<DeferredTxnMeta>, i64)> = Vec::new();

        let stream = trace_commits_by_id(store, head_cid.clone(), index_t);
        futures::pin_mut!(stream);

        let mut commit_count = 0;
        while let Some(result) = stream.next().await {
            let commit = result?;
            commit_count += 1;
            tracing::trace!(
                commit_count,
                t = commit.t,
                flakes = commit.flakes.len(),
                "load_novelty_range: processing commit"
            );

            // Skip commits beyond target_t
            if commit.t > target_t {
                tracing::trace!(
                    t = commit.t,
                    target_t,
                    "load_novelty_range: skipping future commit"
                );
                continue;
            }

            // Defer txn-meta flake construction — the graph Sid depends on
            // namespace_codes which aren't fully applied until after the walk.
            let deferred_txn_meta = if !commit.txn_meta.is_empty() {
                commit.id.as_ref().map(|cid| DeferredTxnMeta {
                    commit_subject: Sid::new(FLUREE_COMMIT, cid.digest_hex()),
                    entries: commit.txn_meta.clone(),
                    t: commit.t,
                })
            } else {
                None
            };

            let meta_flakes = generate_commit_flakes(&commit, ledger_id, commit.t);
            let mut all_flakes = commit.flakes;
            all_flakes.extend(meta_flakes);
            commit_batches.push((all_flakes, deferred_txn_meta, commit.t));

            // Merge namespace deltas (newer wins - trace_commits is newest first)
            for (code, prefix) in commit.namespace_delta {
                merged_ns_delta.entry(code).or_insert(prefix);
            }

            // Collect graph IRIs
            for iri in commit.graph_delta.into_values() {
                all_graph_iris.insert(iri);
            }

            // Extract ns_split_mode (immutable after user namespace allocation).
            if let Some(mode) = commit.ns_split_mode {
                snapshot.set_ns_split_mode(mode, commit.t)?;
            }
        }

        // Apply accumulated deltas to snapshot (ns codes + graph IRIs)
        snapshot.apply_envelope_deltas(&merged_ns_delta, &all_graph_iris)?;

        // Resolve the txn-meta graph Sid now that namespace_codes are complete.
        // This produces the same Sid that build_reverse_graph() will map to g_id=1.
        let txn_meta_iri = fluree_db_core::txn_meta_graph_iri(ledger_id);
        let txn_meta_graph_sid = snapshot.encode_iri(&txn_meta_iri);

        // Stamp commit metadata flakes with txn-meta graph SID
        if let Some(ref g_sid) = txn_meta_graph_sid {
            for (flakes, _, _) in &mut commit_batches {
                stamp_graph_on_commit_flakes(flakes, g_sid);
            }
        }

        // Build reverse_graph now that namespace_codes and graph_registry are complete
        let mut reverse_graph = snapshot.build_reverse_graph()?;
        // Ensure txn-meta graph is always routable for commit metadata flakes.
        let txn_meta_iri = fluree_db_core::txn_meta_graph_iri(ledger_id);
        if let Some(g_sid) = snapshot.encode_iri(&txn_meta_iri) {
            reverse_graph.entry(g_sid).or_insert(TXN_META_GRAPH_ID);
        }

        // Replay oldest→newest (walk was HEAD→oldest)
        commit_batches.reverse();
        for (mut flakes, deferred_txn_meta, commit_t) in commit_batches {
            // Materialize deferred txn-meta flakes with the correct graph Sid
            if let Some(dtm) = deferred_txn_meta {
                if let Some(ref txn_graph) = txn_meta_graph_sid {
                    for entry in &dtm.entries {
                        let p = Sid::new(entry.predicate_ns, &entry.predicate_name);
                        let (o, dt, m) = match &entry.value {
                            fluree_db_novelty::TxnMetaValue::String(s) => (
                                FlakeValue::String(s.clone()),
                                Sid::new(XSD, xsd_names::STRING),
                                None,
                            ),
                            fluree_db_novelty::TxnMetaValue::Long(n) => {
                                (FlakeValue::Long(*n), Sid::new(XSD, xsd_names::LONG), None)
                            }
                            fluree_db_novelty::TxnMetaValue::Double(n) => (
                                FlakeValue::Double(*n),
                                Sid::new(XSD, xsd_names::DOUBLE),
                                None,
                            ),
                            fluree_db_novelty::TxnMetaValue::Boolean(b) => (
                                FlakeValue::Boolean(*b),
                                Sid::new(XSD, xsd_names::BOOLEAN),
                                None,
                            ),
                            fluree_db_novelty::TxnMetaValue::Ref { ns, name } => (
                                FlakeValue::Ref(Sid::new(*ns, name)),
                                Sid::new(JSON_LD, "id"),
                                None,
                            ),
                            fluree_db_novelty::TxnMetaValue::LangString { value, lang } => (
                                FlakeValue::String(value.clone()),
                                Sid::new(RDF, rdf_names::LANG_STRING),
                                Some(FlakeMeta::with_lang(lang.clone())),
                            ),
                            fluree_db_novelty::TxnMetaValue::TypedLiteral {
                                value,
                                dt_ns,
                                dt_name,
                            } => (
                                FlakeValue::String(value.clone()),
                                Sid::new(*dt_ns, dt_name),
                                None,
                            ),
                        };
                        flakes.push(Flake::new_in_graph(
                            txn_graph.clone(),
                            dtm.commit_subject.clone(),
                            p,
                            o,
                            dt,
                            dtm.t,
                            true,
                            m,
                        ));
                    }
                }
            }
            novelty.apply_commit(flakes, commit_t, &reverse_graph)?;
        }

        tracing::trace!(
            commit_count,
            novelty_empty = novelty.is_empty(),
            "load_novelty_range: completed"
        );
        Ok(novelty)
    }

    /// Create a historical view directly from components
    ///
    /// This is useful for testing or when you've already loaded the components.
    /// Runtime predicate/datatype dictionaries default to `None`; callers with an
    /// attached binary store should prefer [`HistoricalLedgerView::new_with_runtime_small_dicts`]
    /// or call [`HistoricalLedgerView::set_runtime_small_dicts`] before query planning.
    pub fn new(snapshot: LedgerSnapshot, overlay: Option<Arc<Novelty>>, to_t: i64) -> Self {
        Self::new_with_runtime_small_dicts(snapshot, overlay, None, to_t)
    }

    /// Create a historical view directly from components, including runtime dicts.
    pub fn new_with_runtime_small_dicts(
        snapshot: LedgerSnapshot,
        overlay: Option<Arc<Novelty>>,
        runtime_small_dicts: Option<Arc<RuntimeSmallDicts>>,
        to_t: i64,
    ) -> Self {
        Self {
            snapshot,
            overlay,
            runtime_small_dicts,
            to_t,
        }
    }

    /// Get the time bound for this view
    pub fn to_t(&self) -> i64 {
        self.to_t
    }

    /// Get the index time (when the snapshot was indexed)
    pub fn index_t(&self) -> i64 {
        self.snapshot.t
    }

    /// Get the ledger ID
    pub fn ledger_id(&self) -> &str {
        &self.snapshot.ledger_id
    }

    /// Get the overlay if present
    pub fn overlay(&self) -> Option<&Arc<Novelty>> {
        self.overlay.as_ref()
    }

    /// Get the runtime predicate/datatype dictionaries if present.
    pub fn runtime_small_dicts(&self) -> Option<&Arc<RuntimeSmallDicts>> {
        self.runtime_small_dicts.as_ref()
    }

    /// Attach runtime predicate/datatype dictionaries after a binary store is loaded.
    pub fn set_runtime_small_dicts(&mut self, runtime_small_dicts: Arc<RuntimeSmallDicts>) {
        self.runtime_small_dicts = Some(runtime_small_dicts);
    }

    /// Get the overlay as an OverlayProvider reference
    ///
    /// Returns the novelty overlay if present, which can be used with
    /// `execute_pattern_with_overlay` and similar functions.
    pub fn overlay_provider(&self) -> Option<&dyn OverlayProvider> {
        self.overlay
            .as_ref()
            .map(|n| n.as_ref() as &dyn OverlayProvider)
    }

    /// Create a `GraphDbRef` for the given graph.
    ///
    /// Uses `self` as the overlay provider (delegates to inner novelty if
    /// present, no-op otherwise). `t` is set to `to_t` (the historical time bound).
    pub fn as_graph_db_ref(&self, g_id: GraphId) -> GraphDbRef<'_> {
        GraphDbRef::new(&self.snapshot, g_id, self, self.to_t)
            .with_runtime_small_dicts_opt(self.runtime_small_dicts.as_deref())
    }
}

/// OverlayProvider implementation for HistoricalLedgerView
///
/// This allows the view to be used directly as an overlay provider in queries.
/// The `to_t` filtering is handled automatically.
impl OverlayProvider for HistoricalLedgerView {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn epoch(&self) -> u64 {
        self.overlay.as_ref().map(|n| n.epoch).unwrap_or(0)
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
        if let Some(novelty) = &self.overlay {
            // Use the minimum of the requested to_t and our view's to_t
            let effective_to_t = to_t.min(self.to_t);
            novelty.for_each_overlay_flake(
                g_id,
                index,
                first,
                rhs,
                leftmost,
                effective_to_t,
                callback,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{
        content_store_for, ContentKind, ContentStore, FlakeValue, MemoryStorage, Sid,
    };
    use fluree_db_nameservice::memory::MemoryNameService;

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

    #[tokio::test]
    async fn test_historical_view_new() {
        let snapshot = LedgerSnapshot::genesis("test:main");

        let view = HistoricalLedgerView::new(snapshot, None, 10);

        assert_eq!(view.ledger_id(), "test:main");
        assert_eq!(view.to_t(), 10);
        assert_eq!(view.index_t(), 0);
        assert!(view.overlay().is_none());
    }

    #[tokio::test]
    async fn test_historical_view_with_overlay() {
        let snapshot = LedgerSnapshot::genesis("test:main");

        let mut novelty = Novelty::new(0);
        let rg = std::collections::HashMap::new();
        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 1)], 1, &rg)
            .unwrap();

        let view = HistoricalLedgerView::new(snapshot, Some(Arc::new(novelty)), 10);

        assert_eq!(view.to_t(), 10);
        assert!(view.overlay().is_some());
        assert_eq!(view.epoch(), 1);
    }

    #[tokio::test]
    async fn test_historical_view_overlay_provider() {
        let snapshot = LedgerSnapshot::genesis("test:main");

        let mut novelty = Novelty::new(0);
        let rg = std::collections::HashMap::new();
        novelty
            .apply_commit(
                vec![
                    make_flake(1, 1, 100, 1),
                    make_flake(2, 1, 200, 5),
                    make_flake(3, 1, 300, 10),
                ],
                10,
                &rg,
            )
            .unwrap();

        // View at t=5 should only see flakes with t <= 5
        let view = HistoricalLedgerView::new(snapshot, Some(Arc::new(novelty)), 5);

        let mut collected = Vec::new();
        view.for_each_overlay_flake(0, IndexType::Spot, None, None, true, 100, &mut |f| {
            collected.push(f.s.namespace_code);
        });

        // Should only see flakes at t=1 and t=5, not t=10
        assert_eq!(collected, vec![1, 2]);
    }

    #[tokio::test]
    async fn test_load_at_not_found() {
        let ns = MemoryNameService::new();
        let storage = MemoryStorage::new();

        let result = HistoricalLedgerView::load_at(
            &ns,
            "nonexistent:main",
            &StorageBackend::Managed(std::sync::Arc::new(storage)),
            10,
        )
        .await;

        assert!(matches!(result, Err(LedgerError::NotFound(_))));
    }

    /// Helper: serialize a commit, store via content store, and publish to nameservice.
    /// Returns the CID of the stored commit.
    async fn store_and_publish_commit(
        storage: &MemoryStorage,
        ns: &MemoryNameService,
        ledger_id: &str,
        commit: &fluree_db_novelty::Commit,
    ) -> ContentId {
        use fluree_db_nameservice::{CasResult, RefPublisher, RefValue};

        let store = content_store_for(storage.clone(), ledger_id);
        let blob = fluree_db_core::commit::codec::write_commit(commit, false, None).unwrap();
        let cid = store.put(ContentKind::Commit, &blob.bytes).await.unwrap();
        let new = RefValue {
            id: Some(cid.clone()),
            t: commit.t,
        };
        match ns.fast_forward_commit(ledger_id, &new, 3).await.unwrap() {
            CasResult::Updated => {}
            CasResult::Conflict { actual } => {
                assert!(
                    actual.as_ref().map(|r| r.t).unwrap_or(0) >= commit.t,
                    "unexpected commit publish conflict: {actual:?}"
                );
            }
        }
        cid
    }

    #[tokio::test]
    async fn test_load_at_future_time() {
        let ns = MemoryNameService::new();
        let storage = MemoryStorage::new();

        // Create a ledger with commits up to t=5
        let commit = fluree_db_novelty::Commit::new(5, vec![make_flake(1, 1, 100, 5)]);
        store_and_publish_commit(&storage, &ns, "test:main", &commit).await;

        // Try to load at t=10 (future)
        let result = HistoricalLedgerView::load_at(
            &ns,
            "test:main",
            &StorageBackend::Managed(std::sync::Arc::new(storage)),
            10,
        )
        .await;

        assert!(matches!(result, Err(LedgerError::FutureTime { .. })));
    }

    #[tokio::test]
    async fn test_load_at_genesis_fallback() {
        let ns = MemoryNameService::new();
        let storage = MemoryStorage::new();

        // Create a ledger with commits but no index
        let commit = fluree_db_novelty::Commit::new(5, vec![make_flake(1, 1, 100, 5)]);
        store_and_publish_commit(&storage, &ns, "test:main", &commit).await;

        // Load at t=5 - should use genesis snapshot since no index exists
        let view = HistoricalLedgerView::load_at(
            &ns,
            "test:main",
            &StorageBackend::Managed(std::sync::Arc::new(storage)),
            5,
        )
        .await
        .unwrap();

        assert_eq!(view.ledger_id(), "test:main");
        assert_eq!(view.to_t(), 5);
        assert_eq!(view.index_t(), 0); // Genesis
        assert!(view.overlay().is_some()); // Should have novelty from commit
    }
}
