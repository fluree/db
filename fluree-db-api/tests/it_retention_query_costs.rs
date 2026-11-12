//! What retention costs a query. A ledger without a removal reads nothing
//! for it, and one with a removal reads its journal once, not after every
//! commit. An instance sharing its storage with another serves what it
//! learned and learns again in the background, or first once that is
//! stale, so a removal made elsewhere reaches its reads within about two
//! seconds, even one that commits nothing.

use async_trait::async_trait;
use fluree_db_api::{ApiError, Fluree, FlureeBuilder, NameServiceMode};
use fluree_db_core::storage::{
    CasAction, CasOutcome, ContentAddressedWrite, ContentWriteResult, ListResult, MemoryStorage,
    StorageCas, StorageExtError, StorageExtResult, StorageList, StorageMethod, StorageRead,
    StorageWrite,
};
use fluree_db_nameservice::StorageNameService;
use serde_json::{json, Value as JsonValue};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Memory storage that counts reads: under a nameservice, its lookups
/// and configuration reads.
#[derive(Debug, Clone)]
struct ReadCounting {
    inner: MemoryStorage,
    reads: Arc<AtomicUsize>,
}

impl ReadCounting {
    fn new() -> Self {
        Self {
            inner: MemoryStorage::new(),
            reads: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn take(&self) -> usize {
        self.reads.swap(0, Ordering::SeqCst)
    }

    fn count(&self) {
        self.reads.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl StorageRead for ReadCounting {
    fn permits_plaintext_cache(&self) -> bool {
        self.inner.permits_plaintext_cache()
    }

    fn is_remote(&self) -> bool {
        self.inner.is_remote()
    }

    fn encryption_admin(&self) -> Option<Arc<dyn fluree_db_core::EncryptionAdmin>> {
        self.inner.encryption_admin()
    }

    async fn read_bytes(&self, address: &str) -> fluree_db_core::Result<Vec<u8>> {
        self.count();
        self.inner.read_bytes(address).await
    }

    async fn exists(&self, address: &str) -> fluree_db_core::Result<bool> {
        self.count();
        self.inner.exists(address).await
    }

    async fn list_prefix(&self, prefix: &str) -> fluree_db_core::Result<Vec<String>> {
        self.count();
        StorageRead::list_prefix(&self.inner, prefix).await
    }
}

#[async_trait]
impl StorageWrite for ReadCounting {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> fluree_db_core::Result<()> {
        self.inner.write_bytes(address, bytes).await
    }

    async fn delete(&self, address: &str) -> fluree_db_core::Result<()> {
        self.inner.delete(address).await
    }
}

#[async_trait]
impl StorageList for ReadCounting {
    async fn list_prefix(&self, prefix: &str) -> StorageExtResult<Vec<String>> {
        self.count();
        StorageRead::list_prefix(&self.inner, prefix)
            .await
            .map_err(|e| StorageExtError::other(e.to_string()))
    }

    async fn list_prefix_paginated(
        &self,
        prefix: &str,
        _continuation_token: Option<String>,
        _max_keys: usize,
    ) -> StorageExtResult<ListResult> {
        Ok(ListResult {
            keys: StorageList::list_prefix(self, prefix).await?,
            continuation_token: None,
            is_truncated: false,
        })
    }
}

#[async_trait]
impl StorageCas for ReadCounting {
    async fn insert(&self, address: &str, bytes: &[u8]) -> StorageExtResult<bool> {
        StorageCas::insert(&self.inner, address, bytes).await
    }

    async fn compare_and_swap<T, F>(&self, address: &str, f: F) -> StorageExtResult<CasOutcome<T>>
    where
        F: Fn(Option<&[u8]>) -> Result<CasAction<T>, StorageExtError> + Send + Sync + 'static,
        T: Send + 'static,
    {
        self.inner.compare_and_swap(address, f).await
    }
}

#[async_trait]
impl ContentAddressedWrite for ReadCounting {
    async fn content_write_bytes_with_hash(
        &self,
        kind: fluree_db_core::content_kind::ContentKind,
        namespace: &fluree_db_core::StorageNamespace,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> fluree_db_core::Result<ContentWriteResult> {
        self.inner
            .content_write_bytes_with_hash(kind, namespace, content_hash_hex, bytes)
            .await
    }
}

impl StorageMethod for ReadCounting {
    fn storage_method(&self) -> &str {
        self.inner.storage_method()
    }
}

/// An instance over `data`, whose nameservice lives in `ns`. Two built
/// over the same pair share their storage, as processes sharing S3 do.
fn instance(data: &MemoryStorage, ns: &ReadCounting) -> Fluree {
    FlureeBuilder::memory().build_with(
        data.clone(),
        NameServiceMode::ReadWrite(Arc::new(StorageNameService::new(ns.clone(), "ns"))),
    )
}

fn ctx() -> JsonValue {
    json!({"ex": "http://example.org/"})
}

async fn upsert(fluree: &Fluree, ledger_id: &str, doc: JsonValue) -> i64 {
    fluree
        .graph(ledger_id)
        .transact()
        .upsert(&doc)
        .commit()
        .await
        .expect("upsert")
        .receipt
        .t
}

fn email(v: &str) -> JsonValue {
    json!({"@context": ctx(), "@id": "ex:alice", "ex:email": v})
}

fn email_query() -> JsonValue {
    json!({"@context": ctx(), "select": "?o", "where": {"@id": "ex:alice", "ex:email": "?o"}})
}

fn removal_of(value: &str) -> fluree_db_api::retention::RemovalRequest {
    fluree_db_api::retention::RemovalRequest {
        occurrences: vec![],
        matching: vec![fluree_db_api::retention::FactSelector {
            subject: "http://example.org/alice".into(),
            predicate: "http://example.org/email".into(),
            graph: None,
            object: Some(json!(value)),
        }],
    }
}

async fn head_t(fluree: &Fluree, ledger_id: &str) -> i64 {
    fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .unwrap()
        .unwrap()
        .commit_t
}

/// A ledger whose `ex:alice` email was `a@one` at `t1` and is `a@two`.
async fn two_emails(fluree: &Fluree, name: &str) -> (String, i64) {
    fluree.create_ledger(name).await.expect("create");
    let id = format!("{name}:main");
    let t1 = upsert(fluree, &id, email("a@one")).await;
    upsert(fluree, &id, email("a@two")).await;
    (id, t1)
}

/// Queries of a ledger without a removal, current and as of an earlier
/// point, read nothing for retention once its branch is known.
#[tokio::test]
async fn a_ledger_without_a_removal_reads_nothing_for_retention() {
    let (data, ns) = (MemoryStorage::new(), ReadCounting::new());
    let fluree = instance(&data, &ns);
    let (id, t1) = two_emails(&fluree, "it/costs-none").await;
    let now = fluree.db(&id).await.unwrap();
    let past = fluree.db_at_t(&id, t1).await.unwrap();
    for view in [&now, &past] {
        fluree.query(view, &email_query()).await.expect("query");
    }
    fluree.history_suppressed(&id).await.unwrap();

    ns.take();
    let started = Instant::now();
    for _ in 0..20 {
        for view in [&now, &past] {
            fluree.query(view, &email_query()).await.expect("query");
        }
        assert!(!fluree.history_suppressed(&id).await.unwrap());
    }
    // A background read may learn the branch again once a second.
    let allowed = 2 * (started.elapsed().as_secs() as usize + 1);
    let reads = ns.take();
    assert!(reads <= allowed, "{reads} nameservice reads for 60 checks");
}

/// A ledger with a removal reads its journal once: an ordinary commit
/// leaves what was worked out from it standing.
#[tokio::test]
async fn a_removal_is_read_once_not_after_every_commit() {
    let (data, ns) = (MemoryStorage::new(), ReadCounting::new());
    let fluree = instance(&data, &ns);
    let (id, t1) = two_emails(&fluree, "it/costs-removal").await;
    fluree
        .remove_occurrences(&id, removal_of("a@one"))
        .await
        .expect("remove");
    let past = fluree.db_at_t(&id, t1).await.unwrap();
    let refused = |r: Result<fluree_db_api::QueryResult, ApiError>| {
        matches!(r, Err(ApiError::HistoryUnavailable(_)))
    };
    assert!(refused(fluree.query(&past, &email_query()).await));

    let started = Instant::now();
    let mut reads = 0;
    for i in 0..10 {
        upsert(
            &fluree,
            &id,
            json!({"@context": ctx(), "@id": format!("ex:other{i}"), "ex:n": i}),
        )
        .await;
        ns.take();
        assert!(refused(fluree.query(&past, &email_query()).await));
        reads += ns.take();
    }
    let allowed = 2 * (started.elapsed().as_secs() as usize + 1);
    assert!(
        reads <= allowed,
        "{reads} nameservice reads after 10 commits"
    );
}

/// A removal made through another instance on the same storage, of a
/// value no longer current (it commits nothing, so the head stays where it
/// was), reaches this instance's reads without its being told.
#[tokio::test]
async fn a_removal_made_elsewhere_reaches_this_instance() {
    let (data, ns) = (MemoryStorage::new(), ReadCounting::new());
    let here = instance(&data, &ns);
    let (id, t1) = two_emails(&here, "it/costs-elsewhere").await;
    let past = here.db_at_t(&id, t1).await.unwrap();
    let rows = here.query(&past, &email_query()).await.expect("before");
    assert_eq!(rows.to_jsonld(&past.snapshot).unwrap(), json!(["a@one"]));

    let elsewhere = instance(&data, &ns);
    let before = head_t(&elsewhere, &id).await;
    elsewhere
        .remove_occurrences(&id, removal_of("a@one"))
        .await
        .expect("remove");
    assert_eq!(
        head_t(&elsewhere, &id).await,
        before,
        "the removal committed nothing"
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match here.query(&past, &email_query()).await {
            Err(ApiError::HistoryUnavailable(_)) => break,
            Ok(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            other => panic!("the removal never reached this instance: {other:?}"),
        }
    }
}

/// An instance nothing queried for a while has learned nothing in the
/// background meanwhile: its first read after a removal made elsewhere is
/// refused, and its raw storage is audit data from the first ask.
#[tokio::test]
async fn a_removal_made_elsewhere_reaches_the_first_read_after_a_quiet_spell() {
    let (data, ns) = (MemoryStorage::new(), ReadCounting::new());
    let here = instance(&data, &ns);
    let (queried, t1) = two_emails(&here, "it/costs-quiet-query").await;
    let (gated, _) = two_emails(&here, "it/costs-quiet-gate").await;
    let past = here.db_at_t(&queried, t1).await.unwrap();
    here.query(&past, &email_query()).await.expect("before");
    assert!(!here.history_suppressed(&gated).await.unwrap());

    // Longer than what this instance keeps of a branch may grow stale.
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    let elsewhere = instance(&data, &ns);
    for id in [&queried, &gated] {
        elsewhere
            .remove_occurrences(id, removal_of("a@one"))
            .await
            .expect("remove");
    }

    assert!(matches!(
        here.query(&past, &email_query()).await,
        Err(ApiError::HistoryUnavailable(_))
    ));
    assert!(here.history_suppressed(&gated).await.unwrap());
}
