//! Detached inputs for the existing indexer. No journal/index publication protocol.
use super::*;

/// An immutable, accepted database input for a trusted independent index worker.
/// It grants read-only committed content, never the coordinator or a transaction
/// callback. Keeping it alive does not prevent newer transactions. Index outputs
/// belong in a separate content store and use the existing IndexPublisher surface.
/// This local embedding seam is not a remote transport or an adoption receipt.
#[derive(Clone, Debug)]
pub struct IndexInput {
    record: NsRecord,
    store: RecoveryStore,
    indexes: Option<Arc<dyn ContentStore>>,
    novelty: Arc<fluree_db_novelty::Novelty>,
}
impl IndexInput {
    /// Resolve the existing indexer's attachment-event contract from this pinned
    /// input, outside transaction locks. Other config/provider choices are retained.
    pub fn configure_indexer(
        &self,
        mut config: fluree_db_indexer::IndexerConfig,
    ) -> fluree_db_indexer::IndexerConfig {
        if config.attachment_events.is_none() {
            let events = self.novelty.attachments.iter_event_pairs().collect();
            config.attachment_events = Some(if self.record.index_t == 0 {
                fluree_db_indexer::AttachmentEventCoverage::Authoritative(events)
            } else {
                fluree_db_indexer::AttachmentEventCoverage::Augment(events)
            });
        }
        config
    }

    pub fn record(&self) -> &NsRecord {
        &self.record
    }

    /// Reads only this accepted prefix and its immutable bootstrap prerequisites.
    /// No reads acquire the owner/cache mutex or trigger transaction recovery.
    pub fn content(&self) -> Arc<dyn ContentStore> {
        Arc::new(InputStore {
            committed: self.store.clone(),
            indexes: self.indexes.clone(),
        })
    }

    /// Connect the standard indexer to independent artifact storage. Commit/raw
    /// reads always use the pinned input. Writes are restricted to derived kinds;
    /// the output store's existing durability policy is preserved without extra syncs.
    /// No deletion, transaction writes, publication or local-path capability escapes.
    pub fn with_index_storage(&self, output: Arc<dyn ContentStore>) -> Arc<dyn ContentStore> {
        Arc::new(IndexStore {
            input: self.content(),
            output,
        })
    }
}
impl JournalLedger {
    /// Capture this adapter's last installed accepted prefix using cheap handles.
    /// An independently opened adapter may have a newer prefix; this does not
    /// refresh/recover the transaction owner on behalf of an index worker.
    /// All subsequent index I/O/building is detached from transaction acceptance.
    pub async fn index_input(&self) -> Result<IndexInput> {
        let cache = self.0.cache.lock().await;
        let record = cache
            .state
            .as_ref()
            .ok_or(JournalError::Poisoned)?
            .ns_record
            .clone()
            .unwrap_or(ns_record(self.0.owner.ledger(), cache.head.as_deref())?);
        if record.commit_head_id.is_none() {
            return Err(JournalError::Invalid("indexing requires an accepted commit").into());
        }
        Ok(IndexInput {
            record,
            store: cache.committed.clone().expect("ready content"),
            indexes: cache.index_storage.clone(),
            novelty: cache
                .state
                .as_ref()
                .ok_or(JournalError::Poisoned)?
                .novelty
                .clone(),
        })
    }
}

#[derive(Debug)]
struct IndexStore {
    input: Arc<dyn ContentStore>,
    output: Arc<dyn ContentStore>,
}
fn derived(id: &ContentId) -> bool {
    id.content_kind().is_some_and(|k| k.is_derived())
}
fn write_error() -> fluree_db_core::Error {
    fluree_db_core::Error::storage("index worker may write only derived artifacts")
}
#[async_trait]
impl ContentStore for IndexStore {
    async fn has(&self, id: &ContentId) -> fluree_db_core::Result<bool> {
        if self.input.has(id).await? {
            return Ok(true);
        }
        if derived(id) {
            self.output.has(id).await
        } else {
            Ok(false)
        }
    }
    async fn get(&self, id: &ContentId) -> fluree_db_core::Result<Vec<u8>> {
        if !derived(id) || self.input.has(id).await? {
            return self.input.get(id).await;
        }
        let bytes = self.output.get(id).await?;
        if !id.verify(&bytes) {
            return Err(fluree_db_core::Error::storage("index output CID mismatch"));
        }
        Ok(bytes)
    }
    async fn put(&self, kind: ContentKind, bytes: &[u8]) -> fluree_db_core::Result<ContentId> {
        if !kind.is_derived() {
            return Err(write_error());
        }
        let expected = ContentId::new(kind, bytes);
        let actual = self.output.put(kind, bytes).await?;
        if actual != expected {
            return Err(fluree_db_core::Error::storage(
                "index output returned wrong CID",
            ));
        }
        Ok(actual)
    }
    async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> fluree_db_core::Result<()> {
        if !derived(id) {
            return Err(write_error());
        }
        if !id.verify(bytes) {
            return Err(fluree_db_core::Error::storage("index output CID mismatch"));
        }
        self.output.put_with_id(id, bytes).await
    }
    async fn release(&self, _: &ContentId) -> fluree_db_core::Result<()> {
        Err(write_error())
    }
}

/// Read-only composition: even when an adopted index store is writable, an input
/// snapshot cannot use it to mutate artifacts or accepted transaction data.
#[derive(Debug)]
struct InputStore {
    committed: RecoveryStore,
    indexes: Option<Arc<dyn ContentStore>>,
}
#[async_trait]
impl ContentStore for InputStore {
    async fn has(&self, id: &ContentId) -> fluree_db_core::Result<bool> {
        if self.committed.has(id).await? {
            return Ok(true);
        }
        if derived(id) {
            if let Some(store) = &self.indexes {
                return store.has(id).await;
            }
        }
        Ok(false)
    }
    async fn get(&self, id: &ContentId) -> fluree_db_core::Result<Vec<u8>> {
        if !derived(id) || self.committed.has(id).await? {
            return self.committed.get(id).await;
        }
        let store = self
            .indexes
            .as_ref()
            .ok_or_else(|| fluree_db_core::Error::storage("missing index prerequisite"))?;
        let bytes = store.get(id).await?;
        if !id.verify(&bytes) {
            return Err(fluree_db_core::Error::storage("index input CID mismatch"));
        }
        Ok(bytes)
    }
    async fn put(&self, _: ContentKind, _: &[u8]) -> fluree_db_core::Result<ContentId> {
        Err(write_error())
    }
    async fn put_with_id(&self, _: &ContentId, _: &[u8]) -> fluree_db_core::Result<()> {
        Err(write_error())
    }
    async fn release(&self, _: &ContentId) -> fluree_db_core::Result<()> {
        Err(write_error())
    }
}

impl JournalLedger {
    /// Background-only adoption of a trusted indexer's completed output. Index
    /// construction/publication remain external. This changes no journal bytes or
    /// durable transaction head. Restart replays from the retained baseline.
    ///
    /// All root/closure reads, replay and dictionary attachment happen outside the
    /// transaction gate. A raced commit/index or a busy gate returns false: the
    /// background caller can retry later; transactions never wait for this work.
    /// As with ordinary IndexPublisher, the worker is trusted for index semantics;
    /// CID checks alone cannot prove equivalence to the input's transaction history.
    pub async fn adopt_index(
        &self,
        id: &ContentId,
        artifacts: Arc<dyn ContentStore>,
    ) -> Result<bool> {
        if id.content_kind() != Some(ContentKind::IndexRoot) {
            return Err(JournalError::Invalid("index adoption requires an index root").into());
        }
        let (input, head, previous_index) = {
            let cache = self.0.cache.lock().await;
            let record = cache
                .state
                .as_ref()
                .ok_or(JournalError::Poisoned)?
                .ns_record
                .clone()
                .ok_or(JournalError::Invalid(
                    "index adoption requires committed state",
                ))?;
            let previous_index = record.index_head_id.clone();
            (
                IndexInput {
                    record,
                    store: cache.committed.clone().expect("ready content"),
                    indexes: cache.index_storage.clone(),
                    novelty: cache
                        .state
                        .as_ref()
                        .ok_or(JournalError::Poisoned)?
                        .novelty
                        .clone(),
                },
                cache.head.clone(),
                previous_index,
            )
        };
        let store = input.with_index_storage(artifacts);
        let bytes = store.get(id).await.map_err(crate::ApiError::from)?;
        let root = fluree_db_binary_index::IndexRoot::decode(&bytes)
            .map_err(|_| JournalError::Invalid("invalid index root"))?;
        if root.ledger_id != self.0.owner.ledger()
            || root.index_t > input.record.commit_t
            || root
                .named_graphs
                .iter()
                .any(|g| g.g_id != fluree_db_core::graph_registry::TXN_META_GRAPH_ID)
            || root
                .graph_arenas
                .iter()
                .any(|g| g.g_id > fluree_db_core::graph_registry::TXN_META_GRAPH_ID)
            || root.graph_iris
                != [
                    fluree_db_core::graph_registry::txn_meta_graph_iri(self.0.owner.ledger()),
                    fluree_db_core::graph_registry::config_graph_iri(self.0.owner.ledger()),
                ]
        {
            return Err(JournalError::Invalid("foreign or future index").into());
        }
        if root.index_t <= input.record.index_t {
            return Ok(false);
        }
        // Force the complete supported artifact closure before installing any state.
        for artifact in crate::pack::compute_missing_index_artifacts(&store, id, None).await? {
            store.get(&artifact).await.map_err(crate::ApiError::from)?;
        }
        let mut record = input.record.clone();
        record.index_head_id = Some(id.clone());
        record.index_t = root.index_t;
        let mut state = LedgerState::load_with_store(store.clone(), record)
            .await
            .map_err(crate::ApiError::from)?;
        crate::ledger_manager::load_and_attach_binary_store_from(
            store.clone(),
            &mut state,
            self.0.cache_dir.path(),
            Some(Arc::clone(self.0.engine.leaflet_cache())),
        )
        .await?;
        // A one-way, in-memory swap only. No owner call, I/O or await under this
        // guard; ready() will still check owner health/frontier before any use.
        let Ok(mut cache) = self.0.cache.try_lock() else {
            return Ok(false);
        };
        if cache.state.is_none()
            || cache.head != head
            || cache
                .state
                .as_ref()
                .and_then(|s| s.ns_record.as_ref())
                .and_then(|r| r.index_head_id.as_ref())
                != previous_index.as_ref()
        {
            return Ok(false);
        }
        let previous_state = cache.state.replace(state);
        let previous_store = cache.index_storage.replace(store);
        drop(cache);
        drop((previous_state, previous_store));
        Ok(true)
    }
}

#[cfg(test)]
mod tests;
