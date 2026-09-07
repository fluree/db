//! Bounded experimental WAL adapter for trusted, local JSON-LD and Cypher transactions.
//!
//! One unsigned default-graph ledger per root, initialized empty or bootstrapped
//! from a quiescent local indexed source. The imported index stays fixed; no index
//! publication, lifecycle/configuration changes, credentials, policy context or
//! cluster API. Static imported IRI mappings are preserved. This is an embedded
//! root-authority experiment, not a server backend. No underlying Fluree, cache,
//! staged state or writable storage handle escapes. Raw transaction JSON is always
//! journaled. The 64 MiB journal has no reclamation yet.
use crate::{Fluree, FlureeBuilder, GraphDb, IndexConfig, LedgerState, StageResult, TxnOpts};
use async_trait::async_trait;
use fluree_db_core::local_journal::{
    AcceptanceValidator, Checkpoint, Error as JournalError, LocalRoot, Object, Receipt, Record,
    Transition,
};
use fluree_db_core::{
    content_path, ledger_id::split_ledger_id, ContentId, ContentKind, ContentStore,
};
use fluree_db_nameservice::{NsRecord, RefValue};
use fluree_db_transact::{CommitOpts, CommitReceipt, TxnType};
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::local_journal_acceptance::{validate_linear, LinearCommitValidator};

mod cypher;
#[cfg(test)]
mod fixtures;
mod indexed;
mod prefix;

struct AdapterValidator<'a> {
    proof: Option<&'a indexed::Proof>,
    prefix: Option<&'a prefix::ValidatedPrefix>,
}
impl AcceptanceValidator for AdapterValidator<'_> {
    fn validate_checkpoint(
        &self,
        checkpoint: &Checkpoint,
    ) -> fluree_db_core::local_journal::Result<()> {
        self.proof
            .ok_or(JournalError::Invalid("missing verified baseline proof"))?
            .check(checkpoint)
    }
    fn validate(
        &self,
        view: &fluree_db_core::local_journal::AcceptanceView<'_>,
    ) -> fluree_db_core::local_journal::Result<()> {
        if let Some(prefix) = self.prefix {
            crate::local_journal_acceptance::validate_linear_from(
                view,
                self.proof.is_some(),
                prefix.boundary(view)?,
            )?;
        } else if let Some(proof) = self.proof {
            validate_linear(view, Some(&proof.linear))?;
        } else {
            LinearCommitValidator.validate(view)?;
        }
        let record = ns_record(
            &view.transition.ledger,
            Some(&view.transition.resulting_head),
        )?;
        let id = record.commit_head_id.expect("validated head CID");
        let key = content_path(
            ContentKind::Commit,
            &view.transition.ledger,
            &id.digest_hex(),
        );
        let commit = fluree_db_core::commit::codec::read_commit(
            view.content(&key)
                .ok_or(JournalError::Invalid("missing commit"))?,
        )
        .map_err(|_| JournalError::Invalid("invalid commit"))?;
        if !commit.graph_delta.is_empty() || commit.flakes.iter().any(|f| f.g.is_some()) {
            return Err(JournalError::Invalid(
                "journal adapter supports only default-graph writes",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Api(#[from] crate::ApiError),
    /// A task panic can follow a durable append. Reconcile before retrying.
    #[error("journal task interrupted; reconcile before retrying: {0}")]
    Task(#[from] tokio::task::JoinError),
}
pub type Result<T> = std::result::Result<T, Error>;

/// Returned only after journal durability and installation of queryable state.
#[derive(Debug)]
pub struct AcceptedCommit {
    pub commit: CommitReceipt,
    pub journal: Receipt,
    pub raw_txn_id: ContentId,
}

/// One statement's accepted commit (None for a no-op) and optional Cypher RETURN.
/// Rows are prepared privately but exposed only after durable state installation.
#[derive(Debug)]
pub struct CypherOutcome {
    pub commit: Option<AcceptedCommit>,
    pub result: Option<Value>,
}

struct Cache {
    state: Option<LedgerState>,
    head: Option<Vec<u8>>,
    proof: Option<Arc<indexed::Proof>>,
    prefix: Option<Arc<prefix::ValidatedPrefix>>,
}
struct Inner {
    owner: Arc<LocalRoot>,
    cache: Arc<Mutex<Cache>>,
    // Only the pure staging and explicit-view query entrypoints are used.
    engine: Fluree,
    cache_dir: Arc<tempfile::TempDir>,
}

/// Clones share the write/query gate. Independent opens also check the accepted
/// head before using their cache, so another owner handle cannot leave it stale.
/// Writes are serial; cancelling a submitted flush does not cancel installation.
#[derive(Clone)]
pub struct JournalLedger(Arc<Inner>);

impl JournalLedger {
    /// Initialize an existing empty directory. No populated-root migration.
    pub async fn initialize(root: PathBuf, ledger: String, generation: String) -> Result<Self> {
        split_ledger_id(&ledger).map_err(|_| JournalError::Invalid("invalid journal ledger"))?;
        let owner =
            tokio::task::spawn_blocking(move || LocalRoot::initialize(&root, &ledger, &generation))
                .await??;
        Self::attach(owner).await
    }

    pub async fn open(root: PathBuf) -> Result<Self> {
        let owner = tokio::task::spawn_blocking(move || LocalRoot::open(&root)).await??;
        Self::attach(owner).await
    }

    async fn attach(owner: Arc<LocalRoot>) -> Result<Self> {
        let ledger = Self(Arc::new(Inner {
            owner,
            cache: Arc::new(Mutex::new(Cache {
                state: None,
                head: None,
                proof: None,
                prefix: None,
            })),
            engine: FlureeBuilder::memory().without_indexing().build_memory(),
            cache_dir: Arc::new(
                tempfile::Builder::new()
                    .prefix("fluree-journal-index-")
                    .tempdir()
                    .map_err(JournalError::from)?,
            ),
        }));
        ledger.recover().await?;
        Ok(ledger)
    }

    /// Reconcile complete records and rebuild the real LedgerState before clearing
    /// the root's unavailable flag. This may include a transaction whose response
    /// was lost. It does not automatically resubmit or deduplicate a request.
    pub async fn recover(&self) -> Result<()> {
        let cache = self.0.cache.clone().lock_owned().await;
        self.restore(cache).await.map(|_| ())
    }

    async fn restore(&self, mut cache: OwnedMutexGuard<Cache>) -> Result<OwnedMutexGuard<Cache>> {
        let owner = self.0.owner.clone();
        let runtime = tokio::runtime::Handle::current();
        let cache_dir = self.0.cache_dir.clone();
        let inner = self.0.clone();
        Ok(tokio::task::spawn_blocking(move || {
            // Keep the gate through cancellation and through failed installation.
            cache.state = None;
            cache.proof = None;
            cache.prefix = None;
            owner.recover_with_frontier(|records, checkpoint, frontier| {
                let store = RecoveryStore::new(
                    owner.ledger(),
                    records,
                    checkpoint.clone(),
                    cache_dir.clone(),
                );
                let proof = checkpoint
                    .as_deref()
                    .map(|checkpoint| {
                        runtime
                            .block_on(indexed::Proof::load(checkpoint, &store))
                            .map(Arc::new)
                    })
                    .transpose()?;
                AdapterValidator {
                    proof: proof.as_deref(),
                    prefix: None,
                }
                .validate_recovered_from(records, checkpoint.as_deref())?;
                let (record, head) =
                    recovered_head(owner.ledger(), records, checkpoint.as_deref())?;
                // The store never reenters the root mutex and retains the root/cache leases.
                let mut state = runtime
                    .block_on(LedgerState::load_with_store(store.clone(), record))
                    .map_err(|_| {
                        JournalError::Invalid("journal ledger state reconstruction failed")
                    })?;
                runtime
                    .block_on(crate::ledger_manager::load_and_attach_binary_store_from(
                        Arc::new(store),
                        &mut state,
                        cache_dir.path(),
                        Some(Arc::clone(inner.engine.leaflet_cache())),
                    ))
                    .map_err(|_| {
                        JournalError::Invalid("journal indexed state attachment failed")
                    })?;
                if let Some(context) = proof.as_ref().and_then(|p| p.context.as_ref()) {
                    // Normal Cypher staging resolves context through its Fluree
                    // helper. Seed only derived metadata in the private memory
                    // engine; accepted data/state still comes exclusively from
                    // the owned checkpoint and journal.
                    runtime
                        .block_on(async {
                            if inner
                                .engine
                                .nameservice()
                                .lookup(owner.ledger())
                                .await?
                                .is_none()
                            {
                                inner.engine.create_ledger(owner.ledger()).await?;
                            }
                            inner
                                .engine
                                .set_default_context(owner.ledger(), context)
                                .await?;
                            Ok::<_, crate::ApiError>(())
                        })
                        .map_err(|_| {
                            JournalError::Invalid("private staging context installation failed")
                        })?;
                }
                cache.prefix = Some(Arc::new(prefix::ValidatedPrefix::after_validation(
                    frontier.clone(),
                    owner.ledger(),
                )?));
                cache.proof = proof;
                cache.state = Some(state);
                cache.head = head;
                Ok(())
            })?;
            Ok::<_, JournalError>(cache)
        })
        .await??)
    }

    async fn ready(&self) -> Result<OwnedMutexGuard<Cache>> {
        let cache = self.0.cache.clone().lock_owned().await;
        let owner = self.0.owner.clone();
        let frontier = tokio::task::spawn_blocking(move || owner.accepted_frontier()).await??;
        if cache.state.is_none() || cache.prefix.as_ref().is_none_or(|p| !p.matches(&frontier)) {
            self.restore(cache).await
        } else {
            Ok(cache)
        }
    }

    async fn accepted_head(&self) -> Result<Option<Vec<u8>>> {
        let owner = self.0.owner.clone();
        Ok(tokio::task::spawn_blocking(move || owner.accepted_head()).await??)
    }

    /// Trusted local insert/upsert/WHERE update; no user-supplied commit options.
    /// A no-change update/upsert returns None and creates no journal record.
    pub async fn transact(&self, kind: TxnType, body: &Value) -> Result<Option<AcceptedCommit>> {
        self.transact_with_install_hook(kind, body, || Ok(())).await
    }

    // Private seam for deterministic post-flush failure/cancellation tests.
    async fn transact_with_install_hook(
        &self,
        kind: TxnType,
        body: &Value,
        before_install: impl FnOnce() -> fluree_db_core::local_journal::Result<()> + Send + 'static,
    ) -> Result<Option<AcceptedCommit>> {
        let cache = self.ready().await?;
        let state = cache.state.as_ref().expect("ready state");
        let config = index_config();
        let effective = transaction_context(
            body,
            cache.proof.as_deref().and_then(|p| p.context.as_ref()),
        );
        let staged = self
            .0
            .engine
            .stage_transaction(
                state.clone(),
                kind,
                &effective,
                TxnOpts::default(),
                Some(&config),
            )
            .await?;
        self.accept_staged(cache, staged, kind, body, before_install)
            .await
    }

    async fn accept_staged(
        &self,
        mut cache: OwnedMutexGuard<Cache>,
        staged: StageResult,
        kind: TxnType,
        body: &Value,
        before_install: impl FnOnce() -> fluree_db_core::local_journal::Result<()> + Send + 'static,
    ) -> Result<Option<AcceptedCommit>> {
        let state = cache.state.as_ref().expect("ready state");
        let expected = state.head_commit_id.as_ref().map(|id| RefValue {
            id: Some(id.clone()),
            t: state.t(),
        });
        let config = index_config();
        if !staged.graph_delta.is_empty()
            || staged.view.staged_flakes().iter().any(|f| f.g.is_some())
        {
            return Err(JournalError::Invalid(
                "journal adapter supports only default-graph writes",
            )
            .into());
        }
        if !staged.view.has_staged() && matches!(kind, TxnType::Upsert | TxnType::Update) {
            // A no-op is still a read of accepted state; do not report it through
            // an unresolved acceptance on another handle.
            self.accepted_head().await?;
            return Ok(None);
        }
        let raw = serde_json::to_vec(body).map_err(crate::ApiError::from)?;
        let raw_id = ContentId::new(ContentKind::Txn, &raw);
        let opts = CommitOpts::default().with_txn_meta(staged.txn_meta);
        let staged = fluree_db_transact::build_commit(
            staged.view,
            staged.ns_registry,
            expected,
            Some(raw_id.clone()),
            &config,
            opts,
        )
        .await
        .map_err(crate::ApiError::from)?;
        if !staged.referenced_bytes.is_empty() {
            return Err(JournalError::Invalid("unsupported deferred commit payloads").into());
        }
        let commit_id = staged.commit.id.as_ref().expect("built commit CID");
        let (name, branch) = split_ledger_id(self.0.owner.ledger())
            .map_err(|_| JournalError::Invalid("invalid journal ledger"))?;
        let key = format!("ns@v2/{name}/{branch}.json");
        let mut head: Value = match cache.head.as_deref() {
            Some(bytes) => serde_json::from_slice(bytes).map_err(crate::ApiError::from)?,
            None => json!({"@context":{"f":fluree_vocab::fluree::DB},
                "@id":key,"@type":["f:LedgerSource"],"f:ledger":{"@id":name},
                "f:branch":branch,"f:status":"ready","f:statusV":1,"f:configV":0}),
        };
        head["f:t"] = json!(staged.commit.t);
        head["f:commitCid"] = json!(commit_id.to_string());
        let owner = self.0.owner.clone();
        let transition = Transition {
            ledger: owner.ledger().into(),
            generation: owner.generation().into(),
            head_key: key,
            expected_head: cache.head.clone(),
            resulting_head: serde_json::to_vec(&head).map_err(crate::ApiError::from)?,
            objects: vec![
                Object {
                    key: content_path(ContentKind::Txn, owner.ledger(), &raw_id.digest_hex()),
                    bytes: raw,
                },
                Object {
                    key: content_path(ContentKind::Commit, owner.ledger(), &commit_id.digest_hex()),
                    bytes: staged.commit_bytes.clone(),
                },
            ],
        };
        let runtime = tokio::runtime::Handle::current();
        let cache_dir = self.0.cache_dir.clone();
        let leaflet_cache = Arc::clone(self.0.engine.leaflet_cache());
        // The blocking task owns the cache gate through append, sync and install,
        // even if the awaiting request is dropped or its task is aborted.
        Ok(Some(
            tokio::task::spawn_blocking(move || {
                let mut committed = None;
                let proof = cache.proof.clone();
                let prefix = cache.prefix.clone();
                let journal = owner.accept_with(
                    &transition,
                    &AdapterValidator {
                        proof: proof.as_deref(),
                        prefix: prefix.as_deref(),
                    },
                    |view, journal_receipt| {
                        before_install()?;
                        let (receipt, mut state) = staged.finalize_state().map_err(|_| {
                            JournalError::Invalid("journal state finalization failed")
                        })?;
                        state.ns_record =
                            Some(ns_record(owner.ledger(), Some(&transition.resulting_head))?);
                        // The existing provider's namespace fallback handles
                        // scans but not every bound/join path for a new namespace.
                        // Reattach from our verified store before ACK. This rare
                        // reload is intentionally conservative; optimizing it is
                        // separate from the fixed-index correctness boundary.
                        if crate::ns_helpers::binary_store_missing_snapshot_namespaces(&state) {
                            let store = state.snapshot.content_store.clone().ok_or(
                                JournalError::Invalid("missing owned indexed content store"),
                            )?;
                            runtime
                                .block_on(crate::ledger_manager::load_and_attach_binary_store_from(
                                    store,
                                    &mut state,
                                    cache_dir.path(),
                                    Some(leaflet_cache),
                                ))
                                .map_err(|_| {
                                    JournalError::Invalid(
                                        "post-commit indexed namespace attachment failed",
                                    )
                                })?;
                        }
                        cache.prefix = Some(Arc::new(prefix::ValidatedPrefix::after_validation(
                            view.frontier_after(journal_receipt),
                            owner.ledger(),
                        )?));
                        cache.state = Some(state);
                        cache.head = Some(transition.resulting_head.clone());
                        committed = Some(receipt);
                        Ok(())
                    },
                )?;
                Ok::<_, JournalError>(AcceptedCommit {
                    commit: committed.expect("successful acceptance installed state"),
                    journal,
                    raw_txn_id: raw_id,
                })
            })
            .await??,
        ))
    }

    /// Materialized JSON results only: no cached GraphDb/overlay escapes the gate.
    pub async fn query(&self, query: &Value) -> Result<Value> {
        let cache = self.ready().await?;
        let state = cache.state.as_ref().expect("ready state");
        let view = GraphDb::from_ledger_state(state)
            .with_default_context(cache.proof.as_ref().and_then(|p| p.context.clone()));
        let result = self.0.engine.query(&view, query).await?;
        let value = result
            .to_jsonld(&state.snapshot)
            .map_err(crate::ApiError::from)?;
        self.accepted_head().await?;
        Ok(value)
    }

    /// Current commit identity/time, including a recovered commit whose response
    /// was lost. Walk its parent/raw references to reconcile before resubmitting.
    pub async fn head(&self) -> Result<Option<RefValue>> {
        let cache = self.ready().await?;
        let state = cache.state.as_ref().expect("ready state");
        let head = state.head_commit_id.as_ref().map(|id| RefValue {
            id: Some(id.clone()),
            t: state.t(),
        });
        self.accepted_head().await?;
        Ok(head)
    }

    /// Retrieve exact commit/raw bytes for response-loss reconciliation. Mere
    /// existence is not acceptance: reconcile against the chain rooted at head().
    pub async fn content(&self, id: &ContentId) -> Result<Vec<u8>> {
        let _cache = self.ready().await?;
        let kind = id
            .content_kind()
            .filter(|k| matches!(k, ContentKind::Commit | ContentKind::Txn))
            .ok_or(JournalError::Invalid("unsupported journal content kind"))?;
        let owner = self.0.owner.clone();
        let key = content_path(kind, owner.ledger(), &id.digest_hex());
        let bytes = tokio::task::spawn_blocking(move || owner.read_bytes(&key)).await??;
        if !id.verify(&bytes) {
            return Err(JournalError::Invalid("journal content CID mismatch").into());
        }
        Ok(bytes)
    }
}

#[cfg(test)]
#[path = "local_journal_ledger/tests.rs"]
mod tests;

fn transaction_context<'a>(
    body: &'a Value,
    context: Option<&Value>,
) -> std::borrow::Cow<'a, Value> {
    let Some(context) = context else {
        return std::borrow::Cow::Borrowed(body);
    };
    if body.is_array() {
        return std::borrow::Cow::Owned(json!({"@context": context, "@graph": body}));
    }
    if body
        .as_object()
        .is_some_and(|map| !map.contains_key("@context"))
    {
        let mut effective = body.clone();
        effective["@context"] = context.clone();
        std::borrow::Cow::Owned(effective)
    } else {
        std::borrow::Cow::Borrowed(body)
    }
}

fn index_config() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 1_000_000,
        reindex_max_bytes: 10_000_000,
    }
}

fn ns_record(
    ledger: &str,
    bytes: Option<&[u8]>,
) -> fluree_db_core::local_journal::Result<NsRecord> {
    let (name, branch) =
        split_ledger_id(ledger).map_err(|_| JournalError::Invalid("invalid ledger"))?;
    let mut record = NsRecord::new(name, branch);
    if let Some(bytes) = bytes {
        let head: Value =
            serde_json::from_slice(bytes).map_err(|_| JournalError::Invalid("invalid head"))?;
        record.default_context = head
            .get("f:defaultContextCid")
            .and_then(Value::as_str)
            .map(|s| {
                s.parse()
                    .map_err(|_| JournalError::Invalid("invalid context CID"))
            })
            .transpose()?;
        record.commit_t = head["f:t"]
            .as_i64()
            .ok_or(JournalError::Invalid("invalid head time"))?;
        record.commit_head_id = Some(
            head["f:commitCid"]
                .as_str()
                .ok_or(JournalError::Invalid("missing head CID"))?
                .parse()
                .map_err(|_| JournalError::Invalid("invalid head CID"))?,
        );
    }
    if let Some(bytes) = bytes {
        let head: Value =
            serde_json::from_slice(bytes).map_err(|_| JournalError::Invalid("invalid head"))?;
        if let Some(index) = indexed::parse_index(&head)? {
            record.index_head_id = Some(index.cid);
            record.index_t = index.t;
        }
    }
    Ok(record)
}

fn recovered_head(
    ledger: &str,
    records: &[Record],
    checkpoint: Option<&Checkpoint>,
) -> fluree_db_core::local_journal::Result<(NsRecord, Option<Vec<u8>>)> {
    let head = records
        .last()
        .map(|r| r.transition.resulting_head.clone())
        .or_else(|| checkpoint.map(|c| c.head().bytes.clone()));
    Ok((ns_record(ledger, head.as_deref())?, head))
}

/// Validated journal/checkpoint bytes, used inside the recovery install hook. No raw file
/// paths, writable adapter, asynchronous upload, or arbitrary orphan dependencies.
#[derive(Clone)]
struct RecoveryStore {
    ledger: String,
    objects: Arc<BTreeMap<String, Vec<u8>>>,
    checkpoint: Option<Arc<Checkpoint>>,
    _cache_dir: Arc<tempfile::TempDir>,
}
impl RecoveryStore {
    fn new(
        ledger: &str,
        records: &[Record],
        checkpoint: Option<Arc<Checkpoint>>,
        cache_dir: Arc<tempfile::TempDir>,
    ) -> Self {
        Self {
            ledger: ledger.into(),
            checkpoint,
            _cache_dir: cache_dir,
            objects: Arc::new(
                records
                    .iter()
                    .flat_map(|r| {
                        r.transition
                            .objects
                            .iter()
                            .map(|o| (o.key.clone(), o.bytes.clone()))
                    })
                    .collect(),
            ),
        }
    }
    fn bytes(&self, id: &ContentId) -> fluree_db_core::Result<Vec<u8>> {
        let kind = id
            .content_kind()
            .ok_or_else(|| fluree_db_core::Error::storage("unknown content kind"))?;
        let key = content_path(kind, &self.ledger, &id.digest_hex());
        let bytes = match self.objects.get(&key) {
            Some(bytes) => Some(bytes.clone()),
            None => self
                .checkpoint
                .as_ref()
                .map(|c| c.read(&key))
                .transpose()
                .map_err(|e| fluree_db_core::Error::storage(e.to_string()))?
                .flatten(),
        }
        .ok_or_else(|| {
            fluree_db_core::Error::storage("required journal/checkpoint content absent")
        })?;
        if !id.verify(&bytes) {
            return Err(fluree_db_core::Error::storage(
                "journal/checkpoint CID mismatch",
            ));
        }
        Ok(bytes)
    }
}
impl std::fmt::Debug for RecoveryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveryStore")
            .field("ledger", &self.ledger)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ContentStore for RecoveryStore {
    async fn has(&self, id: &ContentId) -> fluree_db_core::Result<bool> {
        Ok(self.bytes(id).is_ok())
    }
    async fn get(&self, id: &ContentId) -> fluree_db_core::Result<Vec<u8>> {
        self.bytes(id)
    }
    async fn put(&self, _: ContentKind, _: &[u8]) -> fluree_db_core::Result<ContentId> {
        Err(fluree_db_core::Error::storage(
            "journal recovery store is read-only",
        ))
    }
    async fn put_with_id(&self, _: &ContentId, _: &[u8]) -> fluree_db_core::Result<()> {
        Err(fluree_db_core::Error::storage(
            "journal recovery store is read-only",
        ))
    }
    async fn release(&self, _: &ContentId) -> fluree_db_core::Result<()> {
        Err(fluree_db_core::Error::storage(
            "journal recovery store is read-only",
        ))
    }
}
