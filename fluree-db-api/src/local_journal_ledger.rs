//! Bounded experimental WAL adapter for trusted, local JSON-LD and Cypher transactions.
//!
//! One unindexed, unsigned ledger per root; no indexing/import, lifecycle,
//! credentials, policy context, configuration or cluster API. This is an embedded
//! root-authority experiment, not a server backend. No underlying Fluree, cache,
//! staged state or writable storage handle escapes. Raw transaction JSON is always
//! journaled. This adapter rejects core checkpoint roots until indexed baseline
//! loading is connected. The 64 MiB journal has no reclamation yet.
use crate::{Fluree, FlureeBuilder, GraphDb, IndexConfig, LedgerState, StageResult, TxnOpts};
use async_trait::async_trait;
use fluree_db_core::local_journal::{
    AcceptanceValidator, Error as JournalError, LocalRoot, Object, Receipt, Record, Transition,
};
use fluree_db_core::{
    content_path, ledger_id::split_ledger_id, ContentId, ContentKind, ContentStore,
};
use fluree_db_nameservice::{NsRecord, RefValue};
use fluree_db_transact::{CommitOpts, CommitReceipt, TxnType};
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::local_journal_acceptance::LinearCommitValidator;

mod cypher;

struct AdapterValidator;
impl AcceptanceValidator for AdapterValidator {
    fn validate(
        &self,
        view: &fluree_db_core::local_journal::AcceptanceView<'_>,
    ) -> fluree_db_core::local_journal::Result<()> {
        LinearCommitValidator.validate(view)?;
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
}
struct Inner {
    owner: Arc<LocalRoot>,
    cache: Arc<Mutex<Cache>>,
    // Only the pure staging and explicit-view query entrypoints are used.
    engine: Fluree,
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
            })),
            engine: FlureeBuilder::memory().without_indexing().build_memory(),
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
        Ok(tokio::task::spawn_blocking(move || {
            // Keep the gate through cancellation and through failed installation.
            cache.state = None;
            owner.recover_with(|records| {
                AdapterValidator.validate_recovered(records)?;
                let (record, head) = recovered_head(owner.ledger(), records)?;
                let store = RecoveryStore::new(owner.ledger(), records);
                // RecoveryStore is memory-only, never reenters the root mutex.
                let state = runtime
                    .block_on(LedgerState::load_with_store(store, record))
                    .map_err(|_| {
                        JournalError::Invalid("journal ledger state reconstruction failed")
                    })?;
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
        let head = self.accepted_head().await?;
        if cache.state.is_none() || cache.head != head {
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
        let staged = self
            .0
            .engine
            .stage_transaction(state.clone(), kind, body, TxnOpts::default(), Some(&config))
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
        // The blocking task owns the cache gate through append, sync and install,
        // even if the awaiting request is dropped or its task is aborted.
        Ok(Some(
            tokio::task::spawn_blocking(move || {
                let mut committed = None;
                let journal = owner.accept_with(&transition, &AdapterValidator, |_, _| {
                    before_install()?;
                    let (receipt, mut state) = staged
                        .finalize_state()
                        .map_err(|_| JournalError::Invalid("journal state finalization failed"))?;
                    state.ns_record =
                        Some(ns_record(owner.ledger(), Some(&transition.resulting_head))?);
                    cache.state = Some(state);
                    cache.head = Some(transition.resulting_head.clone());
                    committed = Some(receipt);
                    Ok(())
                })?;
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
        let view = GraphDb::from_ledger_state(state);
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
    Ok(record)
}

fn recovered_head(
    ledger: &str,
    records: &[Record],
) -> fluree_db_core::local_journal::Result<(NsRecord, Option<Vec<u8>>)> {
    let head = records.last().map(|r| r.transition.resulting_head.clone());
    Ok((ns_record(ledger, head.as_deref())?, head))
}

/// Only validated journal bytes, used inside the recovery install hook. No file
/// paths, writable adapter, asynchronous upload, or arbitrary orphan dependencies.
#[derive(Clone, Debug)]
struct RecoveryStore {
    ledger: String,
    objects: Arc<BTreeMap<String, Vec<u8>>>,
}
impl RecoveryStore {
    fn new(ledger: &str, records: &[Record]) -> Self {
        Self {
            ledger: ledger.into(),
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
    fn bytes(&self, id: &ContentId) -> fluree_db_core::Result<&Vec<u8>> {
        let kind = id
            .content_kind()
            .ok_or_else(|| fluree_db_core::Error::storage("unknown content kind"))?;
        self.objects
            .get(&content_path(kind, &self.ledger, &id.digest_hex()))
            .filter(|bytes| id.verify(bytes))
            .ok_or_else(|| {
                fluree_db_core::Error::storage("required journal content absent or invalid")
            })
    }
}
#[async_trait]
impl ContentStore for RecoveryStore {
    async fn has(&self, id: &ContentId) -> fluree_db_core::Result<bool> {
        Ok(self.bytes(id).is_ok())
    }
    async fn get(&self, id: &ContentId) -> fluree_db_core::Result<Vec<u8>> {
        self.bytes(id).cloned()
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
