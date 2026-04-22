//! Rebuild pipeline fetch-count regression guard.
//!
//! Locks in the contract that a full reindex issues:
//!   - exactly **one** envelope-range read per commit (Phase A DAG walk), and
//!   - exactly **one** full-blob read per commit (Phase B resolve).
//!
//! Before issue #156 was fixed, Phase A made two passes over the chain — one
//! to discover parents via `collect_dag_cids`, then a second rescan to harvest
//! `ns_split_mode` — so every reindex fetched each commit three times end to
//! end. On a 6,041-commit Lambda reindex that added ~5 minutes of pure S3
//! round-trip latency to Phase A alone. This test asserts the fetch counts so
//! any future reintroduction of that pattern fails loudly in CI.

#![cfg(feature = "native")]

use async_trait::async_trait;
use fluree_db_api::{FlureeBuilder, IndexConfig, LedgerState, Novelty};
use fluree_db_core::error::Result as StorageResult;
use fluree_db_core::{ContentId, ContentKind, ContentStore, LedgerSnapshot};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Atomic counters for envelope and blob fetch traffic through a
/// [`CountingContentStore`]. Lives in an `Arc` so test code can keep a handle
/// after the store itself has been moved into the rebuild.
#[derive(Default)]
struct FetchCounters {
    get_calls: AtomicU64,
    get_range_calls: AtomicU64,
    get_bytes: AtomicU64,
    get_range_bytes: AtomicU64,
}

/// `ContentStore` wrapper that counts every read through to the inner store.
/// Writes pass through without accounting — only the reindex read traffic
/// (Phase A envelope fetches, Phase B blob fetches) is load-bearing here.
#[derive(Clone, Debug)]
struct CountingContentStore<C> {
    inner: C,
    counters: Arc<FetchCounters>,
}

impl std::fmt::Debug for FetchCounters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FetchCounters")
            .field("get_calls", &self.get_calls.load(Ordering::Relaxed))
            .field(
                "get_range_calls",
                &self.get_range_calls.load(Ordering::Relaxed),
            )
            .field("get_bytes", &self.get_bytes.load(Ordering::Relaxed))
            .field(
                "get_range_bytes",
                &self.get_range_bytes.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl<C: ContentStore> CountingContentStore<C> {
    fn new(inner: C) -> Self {
        Self {
            inner,
            counters: Arc::new(FetchCounters::default()),
        }
    }

    fn counters(&self) -> Arc<FetchCounters> {
        self.counters.clone()
    }
}

#[async_trait]
impl<C: ContentStore + Send + Sync> ContentStore for CountingContentStore<C> {
    async fn has(&self, id: &ContentId) -> StorageResult<bool> {
        self.inner.has(id).await
    }

    async fn get(&self, id: &ContentId) -> StorageResult<Vec<u8>> {
        let bytes = self.inner.get(id).await?;
        self.counters.get_calls.fetch_add(1, Ordering::Relaxed);
        self.counters
            .get_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }

    async fn put(&self, kind: ContentKind, bytes: &[u8]) -> StorageResult<ContentId> {
        self.inner.put(kind, bytes).await
    }

    async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> StorageResult<()> {
        self.inner.put_with_id(id, bytes).await
    }

    async fn release(&self, id: &ContentId) -> StorageResult<()> {
        self.inner.release(id).await
    }

    async fn get_range(
        &self,
        id: &ContentId,
        range: std::ops::Range<u64>,
    ) -> StorageResult<Vec<u8>> {
        let bytes = self.inner.get_range(id, range).await?;
        self.counters
            .get_range_calls
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .get_range_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }
}

async fn seed_commits(fluree: &fluree_db_api::Fluree, ledger_id: &str, n: usize) -> LedgerState {
    let db0 = LedgerSnapshot::genesis(ledger_id);
    let mut ledger = LedgerState::new(db0, Novelty::new(0));
    let idx_cfg = IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 10_000_000,
    };
    for i in 0..n {
        let tx = json!({
            "@context": { "ex": "http://example.org/" },
            "@id": format!("ex:person{i}"),
            "@type": "ex:Person",
            "ex:name": format!("Person {i}"),
        });
        ledger = fluree
            .insert_with_opts(
                ledger,
                &tx,
                Default::default(),
                Default::default(),
                &idx_cfg,
            )
            .await
            .expect("insert_with_opts")
            .ledger;
    }
    ledger
}

#[tokio::test]
async fn rebuild_issues_exactly_one_range_and_one_blob_fetch_per_commit() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/rebuild-fetch-counts:main";

    const N: usize = 5;
    let ledger = seed_commits(&fluree, ledger_id, N).await;
    assert_eq!(ledger.t(), N as i64);

    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("ns lookup")
        .expect("ns record exists");
    assert_eq!(record.commit_t, N as i64);

    let counted = CountingContentStore::new(fluree.content_store(ledger_id));
    let counters = counted.counters();

    let result = fluree_db_indexer::rebuild_index_from_commits_with_store(
        counted,
        ledger_id,
        &record,
        Default::default(),
    )
    .await
    .expect("rebuild_index_from_commits_with_store");
    assert_eq!(result.index_t, N as i64);

    let get_calls = counters.get_calls.load(Ordering::Relaxed);
    let get_range_calls = counters.get_range_calls.load(Ordering::Relaxed);

    // Phase A: one envelope range read per commit.
    // The pre-fix code issued `get_range == 0` and `get == 2*N` (full-blob walk
    // plus full-blob rescan); catching either drift here is the point.
    assert_eq!(
        get_range_calls, N as u64,
        "Phase A must issue exactly {N} envelope range reads, got {get_range_calls}"
    );

    // Phase B: one full-blob read per commit. Upload paths during the rebuild
    // only `put`, so this counter reflects only commit-resolve reads.
    assert_eq!(
        get_calls, N as u64,
        "Phase B must issue exactly {N} full-blob reads, got {get_calls}"
    );
}

/// Concurrency knob honors env override and still preserves the per-commit
/// fetch contract.
#[tokio::test]
async fn rebuild_fetch_concurrency_env_override_preserves_counts() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/rebuild-fetch-counts-serial:main";

    const N: usize = 4;
    let ledger = seed_commits(&fluree, ledger_id, N).await;
    assert_eq!(ledger.t(), N as i64);

    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("ns lookup")
        .expect("ns record exists");

    let counted = CountingContentStore::new(fluree.content_store(ledger_id));
    let counters = counted.counters();

    // K=1 reproduces the previous serial behavior and must still produce
    // identical fetch counts — regression parity for users who pin K=1 while
    // debugging concurrency-sensitive backends.
    std::env::set_var("FLUREE_REBUILD_FETCH_CONCURRENCY", "1");
    let result = fluree_db_indexer::rebuild_index_from_commits_with_store(
        counted,
        ledger_id,
        &record,
        Default::default(),
    )
    .await
    .expect("rebuild with K=1");
    std::env::remove_var("FLUREE_REBUILD_FETCH_CONCURRENCY");
    assert_eq!(result.index_t, N as i64);

    let get_calls = counters.get_calls.load(Ordering::Relaxed);
    let get_range_calls = counters.get_range_calls.load(Ordering::Relaxed);
    assert_eq!(get_range_calls, N as u64);
    assert_eq!(get_calls, N as u64);
}
