//! Loading an indexed ledger reads its index root once. The state load and the
//! binary store both need the root; the second one takes the bytes the first
//! read instead of fetching them again (one more whole-object GET per cold
//! load against a remote store).

use crate::support;
use async_trait::async_trait;
use fluree_db_api::{FlureeBuilder, NameServiceMode};
use fluree_db_core::storage::{
    ContentAddressedWrite, ContentWriteResult, MemoryStorage, StorageMethod, StorageRead,
    StorageWrite,
};
use fluree_db_core::ContentId;
use fluree_db_nameservice::memory::MemoryNameService;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const LEDGER: &str = "it/index-root-reads:main";

/// Memory storage that counts reads of index roots.
#[derive(Debug, Clone)]
struct RootReadCounting {
    inner: MemoryStorage,
    root_reads: Arc<AtomicUsize>,
}

impl RootReadCounting {
    fn count(&self, address: &str) {
        if address.contains("/index/roots/") {
            self.root_reads.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn take(&self) -> usize {
        self.root_reads.swap(0, Ordering::Relaxed)
    }
}

#[async_trait]
impl StorageRead for RootReadCounting {
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

    fn resolve_cached_bytes(&self, id: &ContentId) -> Option<Arc<[u8]>> {
        self.inner.resolve_cached_bytes(id)
    }
}

#[async_trait]
impl StorageWrite for RootReadCounting {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> fluree_db_core::Result<()> {
        self.inner.write_bytes(address, bytes).await
    }

    async fn delete(&self, address: &str) -> fluree_db_core::Result<()> {
        self.inner.delete(address).await
    }
}

#[async_trait]
impl ContentAddressedWrite for RootReadCounting {
    async fn content_write_bytes_with_hash(
        &self,
        kind: fluree_db_core::content_kind::ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> fluree_db_core::Result<ContentWriteResult> {
        self.inner
            .content_write_bytes_with_hash(kind, ledger_id, content_hash_hex, bytes)
            .await
    }
}

impl StorageMethod for RootReadCounting {
    fn storage_method(&self) -> &str {
        self.inner.storage_method()
    }
}

#[tokio::test]
async fn loading_an_indexed_ledger_reads_its_root_once() {
    let storage = RootReadCounting {
        inner: MemoryStorage::new(),
        root_reads: Arc::new(AtomicUsize::new(0)),
    };
    let ns = Arc::new(MemoryNameService::new());
    let mode = || {
        NameServiceMode::ReadWrite(
            ns.clone() as Arc<dyn fluree_db_nameservice::NameServicePublisher>
        )
    };
    let seed = FlureeBuilder::memory().build_with(storage.clone(), mode());
    let ledger = support::genesis_ledger_for_fluree(&seed, LEDGER);
    seed.insert(
        ledger,
        &json!({
            "@context": { "ex": "http://example.org/" },
            "@graph": [{ "@id": "ex:a", "@type": "ex:T", "ex:name": "a" }]
        }),
    )
    .await
    .expect("seed");
    support::rebuild_and_publish_index(&seed, LEDGER).await;

    // Fresh instances, so nothing is cached in-process.
    let fluree = FlureeBuilder::memory().build_with(storage.clone(), mode());
    storage.take();
    let state = fluree.ledger(LEDGER).await.expect("uncached load");
    assert!(state.snapshot.t > 0, "the load must come from the index");
    assert_eq!(storage.take(), 1, "uncached load");

    let fluree = FlureeBuilder::memory().build_with(storage.clone(), mode());
    fluree.ledger_cached(LEDGER).await.expect("cached load");
    assert_eq!(storage.take(), 1, "ledger-manager load");
}
