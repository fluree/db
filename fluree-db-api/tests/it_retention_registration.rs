//! When a branch is registered for retention: by the index build that reads
//! its retention input, before the root is published, on every path that
//! builds one; and by a rebase that adopts a registered source's index. A
//! ledger that never uses retention pays no reads for the check.

use crate::support;
use async_trait::async_trait;
use fluree_db_api::{ConflictStrategy, Fluree, FlureeBuilder, NameServiceMode};
use fluree_db_core::storage::{
    ContentAddressedWrite, ContentWriteResult, MemoryStorage, StorageMethod, StorageRead,
    StorageWrite,
};
use fluree_db_core::ContentId;
use fluree_db_nameservice::memory::MemoryNameService;
use serde_json::{json, Value as JsonValue};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn lifetime(id: &str) -> JsonValue {
    json!({
        "@context": {"ex": "http://example.org/"},
        "@id": id,
        "ex:v": {"@value": "private", "@ttl": 60}
    })
}

fn plain(id: &str) -> JsonValue {
    json!({"@context": {"ex": "http://example.org/"}, "@id": id, "ex:v": "public"})
}

async fn insert(fluree: &Fluree, ledger_id: &str, doc: JsonValue) {
    fluree
        .graph(ledger_id)
        .transact()
        .insert(&doc)
        .commit()
        .await
        .expect("insert");
}

async fn registered(fluree: &Fluree, ledger_id: &str) -> bool {
    fluree
        .retention_status(ledger_id)
        .await
        .expect("status")
        .active
}

/// Build `ledger_id`'s index through [`Fluree::build_index`] and publish it.
async fn build_and_publish(fluree: &Fluree, ledger_id: &str) {
    let built = fluree.build_index(ledger_id).await.expect("build");
    support::publish_built_index(fluree, ledger_id, built.index_t, &built.root_id).await;
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// A branch registered from a later commit than its first lifetime, as one
/// whose first index was built with no retention hook is, takes the earlier
/// boundary when a build next reads those commits: the schedule is then
/// discovered again from there.
#[tokio::test]
async fn a_lowered_boundary_schedules_the_commits_it_uncovers() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("it/reg-lowered").await.unwrap();
    let id = "it/reg-lowered:main";
    insert(&fluree, id, lifetime("ex:early")).await;
    let store = fluree.branched_content_store(id).await.unwrap();
    let unhooked = fluree_db_indexer::build_index_for_ledger(
        store,
        fluree.nameservice(),
        id,
        fluree_db_indexer::IndexerConfig::default(),
    )
    .await
    .expect("build");
    support::publish_built_index(&fluree, id, unhooked.index_t, &unhooked.root_id).await;
    assert!(!registered(&fluree, id).await);

    insert(&fluree, id, lifetime("ex:late")).await;
    build_and_publish(&fluree, id).await;
    let pass = fluree.run_retention_at(id, now_ms()).await.expect("pass");
    assert_eq!(
        pass.pending, 1,
        "registered from the later commit: {pass:?}"
    );

    fluree
        .reindex(id, Default::default())
        .await
        .expect("reindex");
    let pass = fluree.run_retention_at(id, now_ms()).await.expect("pass");
    assert_eq!(pass.pending, 2, "{pass:?}");
}

/// A full rebuild (reindex) and an incremental build over the commits since
/// an index both register the branch for the retention input they read,
/// before the caller publishes.
#[tokio::test]
async fn every_index_build_registers_the_retention_input_it_read() {
    let fluree = FlureeBuilder::memory().build_memory();

    fluree.create_ledger("it/reg-reindexed").await.unwrap();
    let rebuilt = "it/reg-reindexed:main";
    insert(&fluree, rebuilt, lifetime("ex:a")).await;
    assert!(!registered(&fluree, rebuilt).await);
    fluree
        .reindex(rebuilt, Default::default())
        .await
        .expect("reindex");
    assert!(registered(&fluree, rebuilt).await, "reindex");

    fluree.create_ledger("it/reg-extended").await.unwrap();
    let extended = "it/reg-extended:main";
    insert(&fluree, extended, plain("ex:a")).await;
    build_and_publish(&fluree, extended).await;
    assert!(
        !registered(&fluree, extended).await,
        "no retention input yet"
    );
    insert(&fluree, extended, lifetime("ex:b")).await;
    fluree.build_index(extended).await.expect("build");
    assert!(registered(&fluree, extended).await, "incremental build");
}

/// A branch rebased onto a registered source takes the source's index, whose
/// commits its own builds never read: it takes the source's registration
/// with it.
#[tokio::test]
async fn a_rebase_onto_a_registered_source_registers_the_branch() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("it/reg-rebase").await.unwrap();
    let main = "it/reg-rebase:main";
    insert(&fluree, main, plain("ex:a")).await;
    fluree
        .create_branch("it/reg-rebase", "dev", None, None)
        .await
        .expect("branch");
    insert(&fluree, main, lifetime("ex:b")).await;
    build_and_publish(&fluree, main).await;
    assert!(registered(&fluree, main).await);
    let dev = "it/reg-rebase:dev";
    assert!(!registered(&fluree, dev).await);

    fluree
        .rebase_branch("it/reg-rebase", "dev", ConflictStrategy::default())
        .await
        .expect("rebase");
    assert!(registered(&fluree, dev).await);
}

/// A scheduled pass over a ledger that is not registered does nothing:
/// it neither walks the commits nor registers the ledger, which is left to
/// the build that first covers its retention input. Once registered, it
/// runs.
#[tokio::test]
async fn a_registered_only_pass_skips_a_ledger_its_builds_have_not_registered() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/reg-scheduled:main";
    fluree.create_ledger("it/reg-scheduled").await.unwrap();
    insert(&fluree, id, lifetime("ex:a")).await;
    assert!(fluree.run_registered_retention(id).await.unwrap().is_none());
    assert!(!registered(&fluree, id).await);

    fluree.build_index(id).await.expect("build");
    assert!(registered(&fluree, id).await);
    let report = fluree.run_registered_retention(id).await.unwrap();
    assert!(report.is_some_and(|r| !r.inactive));
}

/// A ledger created again under a dropped one's name is registered for its
/// own lifetimes.
#[tokio::test]
async fn a_ledger_created_again_under_its_name_is_registered_for_its_own() {
    let fluree = FlureeBuilder::memory().build_memory();
    let name = "it/reg-recreated";
    let id = "it/reg-recreated:main";
    fluree.create_ledger(name).await.unwrap();
    insert(&fluree, id, lifetime("ex:a")).await;
    fluree.build_index(id).await.expect("build");
    assert!(registered(&fluree, id).await);

    fluree
        .drop_ledger(name, fluree_db_api::DropMode::Hard)
        .await
        .unwrap();
    fluree.create_ledger(name).await.unwrap();
    assert!(!registered(&fluree, id).await, "the drop took it");
    insert(&fluree, id, lifetime("ex:a")).await;
    fluree.build_index(id).await.expect("build");
    assert!(registered(&fluree, id).await);
}

/// Memory storage that counts reads.
#[derive(Debug, Clone)]
struct ReadCounting {
    inner: MemoryStorage,
    reads: Arc<AtomicUsize>,
}

impl ReadCounting {
    fn take(&self) -> usize {
        self.reads.swap(0, Ordering::Relaxed)
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
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read_bytes(address).await
    }

    async fn read_byte_range(
        &self,
        address: &str,
        range: std::ops::Range<u64>,
    ) -> fluree_db_core::Result<Vec<u8>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read_byte_range(address, range).await
    }

    async fn exists(&self, address: &str) -> fluree_db_core::Result<bool> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.exists(address).await
    }

    async fn list_prefix(&self, prefix: &str) -> fluree_db_core::Result<Vec<String>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.list_prefix(prefix).await
    }

    fn resolve_cached_bytes(&self, id: &ContentId) -> Option<Arc<[u8]>> {
        self.inner.resolve_cached_bytes(id)
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

/// The background indexer's retention hooks read nothing from storage for
/// a ledger that never used retention: no commit, no registration, no
/// journal, on its first build or any after.
#[tokio::test]
async fn the_build_hooks_read_nothing_for_a_ledger_without_retention() {
    let storage = ReadCounting {
        inner: MemoryStorage::new(),
        reads: Arc::new(AtomicUsize::new(0)),
    };
    let ns = Arc::new(MemoryNameService::new());
    let fluree = Arc::new(FlureeBuilder::memory().build_with(
        storage.clone(),
        NameServiceMode::ReadWrite(ns as Arc<dyn fluree_db_nameservice::NameServicePublisher>),
    ));
    fluree.create_ledger("it/reg-unused").await.unwrap();
    let id = "it/reg-unused:main";
    let observer = fluree.retention_build_observer();
    for round in 0..2 {
        for i in 0..3 {
            insert(&fluree, id, plain(&format!("ex:r{round}-{i}"))).await;
        }
        let record = fluree.nameservice().lookup(id).await.unwrap().unwrap();
        storage.take();
        let directives = observer.before_build(&record).await.expect("before");
        assert!(directives.history_suppression.is_empty());
        assert_eq!(storage.take(), 0, "reads before build {round}");
        let built = fluree.build_index(id).await.expect("build");
        assert_eq!(built.retention_input_from_t, None);
        storage.take();
        observer
            .before_publish(&record, &built)
            .await
            .expect("before publish");
        assert_eq!(storage.take(), 0, "reads before publishing build {round}");
        support::publish_built_index(&fluree, id, built.index_t, &built.root_id).await;
    }
    assert!(!registered(&fluree, id).await);
}
