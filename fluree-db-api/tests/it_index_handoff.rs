//! A build in the query server's process hands over the root it wrote and
//! the objects an install reads (branch manifests, dictionary branches,
//! statistics store objects): the install of that root reads none of them
//! back from storage.

use crate::support;
use async_trait::async_trait;
use fluree_db_api::{FlureeBuilder, NameServiceMode, NsNotify};
use fluree_db_core::storage::{
    ContentAddressedWrite, ContentWriteResult, MemoryStorage, StorageMethod, StorageRead,
    StorageWrite,
};
use fluree_db_core::ContentId;
use fluree_db_indexer::{IndexResult, IndexerConfig, StatsCompactionPolicy};
use fluree_db_nameservice::memory::MemoryNameService;
use serde_json::json;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Memory storage that counts reads of the objects it watches, in two
/// groups. Every read goes through `read_bytes`: it serves nothing from
/// memory directly.
#[derive(Debug, Clone, Default)]
struct WatchedReads {
    inner: MemoryStorage,
    watched: Arc<Mutex<[HashSet<String>; 2]>>,
    reads: Arc<[AtomicUsize; 2]>,
}

impl WatchedReads {
    fn watch(&self, group: usize, ids: impl IntoIterator<Item = ContentId>) {
        let mut watched = self.watched.lock().unwrap();
        watched[group] = ids.into_iter().map(|id| id.address_digest()).collect();
        self.reads[group].store(0, Ordering::Relaxed);
    }

    fn reads(&self, group: usize) -> usize {
        self.reads[group].load(Ordering::Relaxed)
    }

    fn count(&self, address: &str) {
        let watched = self.watched.lock().unwrap();
        for (group, digests) in watched.iter().enumerate() {
            if digests
                .iter()
                .any(|digest| address.contains(digest.as_str()))
            {
                self.reads[group].fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[async_trait]
impl StorageRead for WatchedReads {
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
        self.count(address);
        self.inner.read_bytes(address).await
    }

    async fn read_byte_range(
        &self,
        address: &str,
        range: std::ops::Range<u64>,
    ) -> fluree_db_core::Result<Vec<u8>> {
        self.count(address);
        self.inner.read_byte_range(address, range).await
    }

    async fn exists(&self, address: &str) -> fluree_db_core::Result<bool> {
        self.inner.exists(address).await
    }

    async fn list_prefix(&self, prefix: &str) -> fluree_db_core::Result<Vec<String>> {
        self.inner.list_prefix(prefix).await
    }
}

#[async_trait]
impl StorageWrite for WatchedReads {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> fluree_db_core::Result<()> {
        self.inner.write_bytes(address, bytes).await
    }

    async fn delete(&self, address: &str) -> fluree_db_core::Result<()> {
        self.inner.delete(address).await
    }
}

#[async_trait]
impl ContentAddressedWrite for WatchedReads {
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

impl StorageMethod for WatchedReads {
    fn storage_method(&self) -> &str {
        self.inner.storage_method()
    }
}

/// Stands for a query server in the build's process: a build that has one
/// keeps what an install reads.
#[derive(Debug)]
struct Colocated;

impl fluree_db_indexer::WarmCacheSource for Colocated {
    fn warm_cache(&self) -> Option<Arc<fluree_db_binary_index::LeafletCache>> {
        None
    }
}

async fn insert_people(fluree: &fluree_db_api::Fluree, ledger_id: &str, from: u32, to: u32) {
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let people: Vec<_> = (from..to)
        .map(|i| {
            json!({
                "@id": format!("ex:p{i}"),
                "@type": "ex:Person",
                "ex:name": format!("person {i}"),
                "ex:age": i,
            })
        })
        .collect();
    fluree
        .insert(
            ledger,
            &json!({ "@context": { "ex": "http://example.org/" }, "@graph": people }),
        )
        .await
        .expect("insert");
}

async fn build(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    config: IndexerConfig,
) -> IndexResult {
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("lookup")
        .expect("record");
    fluree_db_indexer::build_index_for_record(
        fluree.branched_content_store(ledger_id).await.unwrap(),
        &record,
        config,
    )
    .await
    .expect("build")
}

async fn notify(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> fluree_db_api::Result<()> {
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("lookup")
        .expect("record");
    fluree
        .ledger_manager()
        .expect("ledger caching")
        .notify(NsNotify {
            ledger_id: record.ledger_id.clone(),
            record: Some(record),
        })
        .await
        .map(|_| ())
}

/// What an install read during publish and install: of what the build
/// handed over, and of the new root's statistics store objects.
struct InstallReads {
    handed_over: usize,
    stats: usize,
}

/// Index a cached ledger once, add to it, build the next root in this
/// process, then install it with the handoff offered or not.
async fn install_reads(policy: StatsCompactionPolicy, offer: bool) -> InstallReads {
    let storage = WatchedReads::default();
    let ns = Arc::new(MemoryNameService::new());
    let fluree = FlureeBuilder::memory().without_indexing().build_with(
        storage.clone(),
        NameServiceMode::ReadWrite(ns as Arc<dyn fluree_db_nameservice::NameServicePublisher>),
    );
    let ledger_id = "it/index-handoff:main";
    support::genesis_ledger_for_fluree(&fluree, ledger_id).await;
    fluree.ledger_cached(ledger_id).await.expect("cache");
    let config = IndexerConfig::default().with_stats_compaction(policy);

    insert_people(&fluree, ledger_id, 0, 40).await;
    let first = build(&fluree, ledger_id, config.clone()).await;
    support::publish_built_index(&fluree, ledger_id, first.index_t, &first.root_id).await;
    notify(&fluree, ledger_id).await.expect("first install");

    insert_people(&fluree, ledger_id, 40, 60).await;
    let colocated = config.with_warm_cache_source(Arc::new(Colocated));
    let mut next = build(&fluree, ledger_id, colocated).await;
    let handoff = next
        .handoff
        .take()
        .expect("a build that wrote a root hands it over");
    let written: Vec<ContentId> = handoff.written.ids().cloned().collect();
    assert!(
        !written.is_empty(),
        "the build must hand over the objects an install reads"
    );
    if let Some(hot) = handoff
        .root
        .stats_store
        .as_ref()
        .and_then(|s| s.hot.as_ref())
    {
        assert!(
            written.contains(hot),
            "the statistics hot object is handed over"
        );
    }
    storage.watch(0, written.into_iter().chain([next.root_id.clone()]));
    let store = handoff
        .root
        .stats_store
        .clone()
        .expect("a statistics store");
    storage.watch(
        1,
        store
            .hot
            .into_iter()
            .chain(store.segments.into_iter().map(|s| s.cid)),
    );
    if offer {
        fluree
            .ledger_manager()
            .expect("ledger caching")
            .offer_index(&next.ledger_id, &next.root_id, handoff);
    }
    support::publish_built_index(&fluree, ledger_id, next.index_t, &next.root_id).await;
    notify(&fluree, ledger_id).await.expect("install");
    let reads = InstallReads {
        handed_over: storage.reads(0),
        stats: storage.reads(1),
    };

    let handle = fluree.ledger_cached(ledger_id).await.expect("cached");
    assert_eq!(
        handle.snapshot().await.snapshot.t,
        next.index_t,
        "the new root is installed"
    );
    let state = fluree.ledger(ledger_id).await.expect("ledger");
    let rows = support::query_jsonld(
        &fluree,
        &state,
        &json!({
            "@context": { "ex": "http://example.org/" },
            "select": ["?name"],
            "where": { "@id": "?p", "@type": "ex:Person", "ex:name": "?name" }
        }),
    )
    .await
    .expect("query")
    .to_jsonld(&state.snapshot)
    .expect("format");
    assert_eq!(
        rows.as_array().map(Vec::len),
        Some(60),
        "every person, read from the index"
    );
    reads
}

fn tiny_store() -> StatsCompactionPolicy {
    StatsCompactionPolicy {
        hot_threshold_entries: 2,
        hot_ceiling_entries: 4,
        normal_steps: 1,
        segment_target_entries: 3,
    }
}

#[tokio::test]
async fn an_install_reads_nothing_a_colocated_build_handed_over() {
    assert_eq!(
        install_reads(StatsCompactionPolicy::default(), true)
            .await
            .handed_over,
        0,
        "per-graph stats inline"
    );
    assert_eq!(
        install_reads(tiny_store(), true).await.handed_over,
        0,
        "per-graph stats in the store"
    );
}

/// The control: without the offer, the same install reads those objects,
/// though not the statistics store when the root carries them inline.
#[tokio::test]
async fn an_install_without_the_handoff_reads_them_from_storage() {
    let inline = install_reads(StatsCompactionPolicy::default(), false).await;
    assert!(inline.handed_over > 0);
    assert_eq!(inline.stats, 0, "inline per-graph stats need no store read");
    let store = install_reads(tiny_store(), false).await;
    assert!(store.handed_over > 0);
    assert!(store.stats > 0, "the install fills them from the store");
}
