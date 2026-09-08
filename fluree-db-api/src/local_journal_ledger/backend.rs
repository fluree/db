//! Opt-in single-ledger server bridge. Ordinary staging/cache/index workers are reused.
//! Source bytes become authoritative only inside the journal's flushed install hook.
use super::*;
use crate::{ApiError, FlureeBuilder, NameServiceMode};
use fluree_db_core::{FileStorage, StorageBackend, StorageContentStore};
use fluree_db_nameservice::file::FileNameService;
use fluree_db_nameservice::{LedgerEventBus, NameServiceEvent};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    RwLock,
};

mod nameservice;

pub(crate) struct JournalConnection {
    ledger: JournalLedger,
    surface: Arc<Surface>,
    #[cfg(test)]
    install_hook: std::sync::Mutex<Option<InstallHook>>,
}
#[cfg(test)]
type InstallHook = Box<dyn FnOnce() -> fluree_db_core::local_journal::Result<()> + Send>;
#[derive(Clone)]
struct Published {
    record: NsRecord,
    store: RecoveryStore,
}
#[derive(Default)]
struct Pending {
    bytes: usize,
    objects: HashMap<ContentId, Vec<u8>>,
}
struct Surface {
    published: RwLock<Published>,
    pending: std::sync::Mutex<Pending>,
    healthy: AtomicBool,
    outputs: Arc<dyn ContentStore>,
    index_ns: FileNameService,
    index_gate: Mutex<()>,
    events: Arc<LedgerEventBus>,
    _outputs_dir: tempfile::TempDir,
}
impl std::fmt::Debug for Surface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalSurface").finish_non_exhaustive()
    }
}
fn api(e: impl std::fmt::Display) -> ApiError {
    ApiError::internal(e.to_string())
}
impl Surface {
    fn read(&self) -> fluree_db_core::Result<Published> {
        if !self.healthy.load(Ordering::Acquire) {
            return Err(fluree_db_core::Error::storage(
                "journal unavailable; reconcile or reopen",
            ));
        }
        Ok(self.published.read().unwrap().clone())
    }
    fn installed(&self, cache: &Cache, preserve_index: bool) {
        let mut published = self.published.write().unwrap();
        let mut record = cache
            .state
            .as_ref()
            .expect("installed state")
            .ns_record
            .clone()
            .unwrap_or_else(|| published.record.clone());
        if preserve_index && published.record.index_t > record.index_t {
            record.index_t = published.record.index_t;
            record.index_head_id = published.record.index_head_id.clone();
        }
        *published = Published {
            record,
            store: cache.committed.clone().expect("installed content"),
        };
        self.healthy.store(true, Ordering::Release);
    }
}
#[async_trait]
impl ContentStore for Surface {
    async fn has(&self, id: &ContentId) -> fluree_db_core::Result<bool> {
        let published = self.read()?;
        if published.store.has(id).await? {
            return Ok(true);
        }
        if id.content_kind().is_some_and(|k| k.is_derived()) {
            return self.outputs.has(id).await;
        }
        // Provisional raw payloads are deliberately excluded from the read surface.
        Ok(false)
    }
    async fn get(&self, id: &ContentId) -> fluree_db_core::Result<Vec<u8>> {
        let published = self.read()?;
        if id.content_kind().is_some_and(|k| k.is_derived()) && !published.store.has(id).await? {
            let bytes = self.outputs.get(id).await?;
            if !id.verify(&bytes) {
                return Err(fluree_db_core::Error::storage("index CID mismatch"));
            }
            return Ok(bytes);
        }
        published.store.get(id).await
    }
    async fn put(&self, kind: ContentKind, bytes: &[u8]) -> fluree_db_core::Result<ContentId> {
        let id = ContentId::new(kind, bytes);
        self.put_with_id(&id, bytes).await?;
        Ok(id)
    }
    async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> fluree_db_core::Result<()> {
        self.read()?;
        if !id.verify(bytes) {
            return Err(fluree_db_core::Error::storage("CID mismatch"));
        }
        match id.content_kind() {
            Some(ContentKind::Txn) => {
                let mut pending = self.pending.lock().unwrap();
                if !pending.objects.contains_key(id) {
                    if pending.bytes.saturating_add(bytes.len()) > 32 * 1024 * 1024 {
                        return Err(fluree_db_core::Error::storage(
                            "provisional transaction buffer full",
                        ));
                    }
                    pending.bytes += bytes.len();
                    pending.objects.insert(id.clone(), bytes.to_vec());
                }
                Ok(())
            }
            Some(k) if k.is_derived() => self.outputs.put_with_id(id, bytes).await,
            _ => Err(fluree_db_core::Error::storage(
                "journal source writes require transaction acceptance",
            )),
        }
    }
    async fn release(&self, id: &ContentId) -> fluree_db_core::Result<()> {
        if id.content_kind().is_some_and(|k| k.is_derived()) {
            // Only disposable outputs: never release the retained bootstrap.
            return self.outputs.release(id).await;
        }
        Err(fluree_db_core::Error::storage(
            "journal source deletion is unsupported",
        ))
    }
}
impl JournalConnection {
    pub(crate) fn check_health(&self) -> crate::Result<()> {
        self.surface.read().map(|_| ()).map_err(api)
    }
    pub(crate) async fn commit(
        &self,
        view: fluree_db_ledger::StagedLedger,
        ns_registry: fluree_db_transact::NamespaceRegistry,
        config: &IndexConfig,
        mut opts: CommitOpts,
    ) -> crate::Result<(CommitReceipt, LedgerState)> {
        if view.base().ledger_id() != self.ledger.0.owner.ledger()
            || opts.signing_key.is_some()
            || opts.identity.is_some()
            || opts.txn_signature.is_some()
            || !opts.graph_delta.is_empty()
            || !opts.merge_parents.is_empty()
            || opts.namespace_delta.is_some()
            || opts.skip_backpressure
            || view.staged_flakes().iter().any(|f| f.g.is_some())
        {
            return Err(ApiError::config(
                "experimental journal supports unsigned, default-graph linear transactions only",
            ));
        }
        let raw_id = if let Some(pending) = opts.raw_txn_upload.take() {
            pending.finish().await.map_err(api)?
        } else {
            opts.raw_txn_id
                .take()
                .ok_or_else(|| ApiError::config("journal requires raw transaction recording"))?
        };
        if raw_id.content_kind() != Some(ContentKind::Txn) {
            return Err(ApiError::config(
                "raw transaction ID must identify transaction content",
            ));
        }
        let raw = self
            .surface
            .pending
            .lock()
            .unwrap()
            .objects
            .get(&raw_id)
            .cloned();
        let raw = match raw {
            Some(raw) => raw,
            None => self.surface.get(&raw_id).await.map_err(api)?,
        };
        // The ordinary ledger manager owns transaction state. Retain only the
        // accepted byte view/proofs here, so this bridge does not pin a second
        // dictionary snapshot and force a COW clone on every transaction.
        let mut cache = self.ledger.0.cache.clone().lock_owned().await;
        let current = self.ledger.0.owner.accepted_frontier();
        if !current
            .as_ref()
            .is_ok_and(|f| cache.prefix.as_ref().is_some_and(|p| p.matches(f)))
        {
            drop(cache);
            cache = self
                .ledger
                .restore(self.ledger.0.cache.clone().lock_owned().await)
                .await
                .map_err(api)?;
            self.surface.installed(&cache, false);
            cache.state = None;
        }
        let record = ns_record(self.ledger.0.owner.ledger(), cache.head.as_deref()).map_err(api)?;
        if view.base().t() != record.commit_t || view.base().head_commit_id != record.commit_head_id
        {
            return Err(fluree_db_transact::TransactError::PublishLostRace {
                ledger_id: record.ledger_id.clone(),
                attempted_t: view.base().t() + 1,
                attempted_commit_id: "unbuilt".into(),
                published_t: record.commit_t,
                published_commit_id: record
                    .commit_head_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
            }
            .into());
        }
        let expected = record.commit_head_id.as_ref().map(|id| RefValue {
            id: Some(id.clone()),
            t: record.commit_t,
        });
        let staged = fluree_db_transact::build_commit(
            view,
            ns_registry,
            expected,
            Some(raw_id.clone()),
            config,
            opts,
        )
        .await?;
        let surface = self.surface.clone();
        #[cfg(test)]
        let hook = self.install_hook.lock().unwrap().take();
        let result = self
            .ledger
            .accept_built(
                cache,
                staged,
                raw_id.clone(),
                raw,
                move || {
                    #[cfg(test)]
                    if let Some(hook) = hook {
                        return hook();
                    }
                    Ok(())
                },
                move |cache| {
                    surface.installed(cache, true);
                    cache.state = None;
                    let record = surface.published.read().unwrap().record.clone();
                    surface
                        .events
                        .notify(NameServiceEvent::LedgerCommitPublished {
                            ledger_id: record.ledger_id,
                            commit_id: record.commit_head_id.expect("accepted head"),
                            commit_t: record.commit_t,
                        });
                },
            )
            .await;
        match result {
            Ok((accepted, state)) => {
                let mut pending = self.surface.pending.lock().unwrap();
                if let Some(bytes) = pending.objects.remove(&raw_id) {
                    pending.bytes -= bytes.len();
                }
                Ok((accepted.commit, state))
            }
            Err(error) => {
                self.surface.healthy.store(false, Ordering::Release);
                // Reconcile exact bytes, never re-execute the request. The ordinary
                // shielded cache repair then loads this accepted head, even after
                // a lost response or a failed post-flush install.
                if let Ok(mut cache) = self
                    .ledger
                    .restore(self.ledger.0.cache.clone().lock_owned().await)
                    .await
                {
                    self.surface.installed(&cache, false);
                    cache.state = None;
                }
                Err(api(error))
            }
        }
    }
}
impl FlureeBuilder {
    /// Open an existing experimental journal root through ordinary transaction and
    /// query APIs, with the existing independent background index worker. One local
    /// unsigned default-graph ledger; no lifecycle/config changes, Raft or encryption.
    /// `index_directory` must be outside the journal root. Outputs are disposable;
    /// every restart loads the retained bootstrap and replays exact journal commits.
    pub async fn build_local_journal(
        self,
        root: PathBuf,
        index_directory: PathBuf,
    ) -> crate::Result<Fluree> {
        if self.encryption_key.is_some()
            || !self.remote_mounts.is_empty()
            || self.ledger_cache_config.is_none()
            || !matches!(
                self.config.index_storage.storage_type,
                fluree_db_connection::StorageType::File
            )
            || self.config.index_storage.aes256_key.is_some()
            || self.config.commit_storage.is_some()
            || self.config.primary_publisher.is_some()
            || self.config.defaults.is_some()
            || self.config.address_identifiers.is_some()
        {
            return Err(ApiError::config(
                "journal requires local unencrypted storage and ledger caching",
            ));
        }
        let index_directory = std::fs::canonicalize(index_directory).map_err(api)?;
        if index_directory
            .ancestors()
            .any(|p| p.join(".fluree-wal").exists())
        {
            return Err(ApiError::config(
                "index directory must be outside any journal root",
            ));
        }
        let ledger = JournalLedger::open(root).await.map_err(api)?;
        if Arc::strong_count(&ledger.0.owner) != 1 {
            return Err(ApiError::config(
                "journal server root is already open in this process",
            ));
        }
        let outputs_dir = tempfile::Builder::new()
            .prefix("journal-index-")
            .tempdir_in(index_directory)
            .map_err(api)?;
        let mut cache = ledger.ready().await.map_err(api)?;
        let record = ns_record(ledger.0.owner.ledger(), cache.head.as_deref()).map_err(api)?;
        let output = Arc::new(StorageContentStore::new(
            FileStorage::new(outputs_dir.path()),
            record.ledger_id.clone(),
            "file",
        ));
        let events = self.resolve_event_bus();
        let surface = Arc::new(Surface {
            published: RwLock::new(Published {
                record,
                store: cache.committed.clone().unwrap(),
            }),
            pending: Default::default(),
            healthy: AtomicBool::new(true),
            outputs: output,
            index_ns: FileNameService::new(outputs_dir.path().to_string_lossy().as_ref()),
            index_gate: Mutex::new(()),
            events: events.clone(),
            _outputs_dir: outputs_dir,
        });
        cache.state = None;
        drop(cache);
        let backend = StorageBackend::Permanent(surface.clone());
        let notifying =
            fluree_db_nameservice::NotifyingNameService::new(surface.clone(), events.clone());
        let cell = Self::new_attachment_provider_cell();
        let indexing_mode = self.start_background_indexing(&backend, &notifying, &cell);
        let index_config = self.derive_indexing();
        let mut fluree = Self::finalize_with_backend(
            self.ledger_cache_config,
            self.config,
            crate::RuntimeParts {
                backend,
                nameservice: NameServiceMode::ReadWrite(Arc::new(notifying)),
                event_bus: events,
                indexing_mode,
                index_config,
                attachment_provider_cell: cell,
            },
            self.remote_connections,
            self.remote_mounts,
            #[cfg(feature = "iceberg")]
            self.secret_resolver,
        );
        fluree.journal = Some(Arc::new(JournalConnection {
            ledger,
            surface,
            #[cfg(test)]
            install_hook: Default::default(),
        }));
        Ok(fluree)
    }
}

#[cfg(test)]
mod tests;
