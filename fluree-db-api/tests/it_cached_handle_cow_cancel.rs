//! A cancelled commit must never expose the empty cache slot to readers.
//!
//! The cached-handle commit path empties the cache slot for the duration of a
//! commit, leaving a genesis placeholder under the ledger's write lock (see
//! `it_cached_handle_cow`). The lock is what makes the placeholder invisible —
//! so the one interleaving that can expose it is a cancellation, which drops
//! the commit future and releases the lock without the repair having landed.
//!
//! That is a reachable path, not a theoretical one: `LocalCommitter::transact`
//! awaits the commit inline inside the HTTP request future, and axum drops
//! handler futures when the client disconnects. Readers parked behind the
//! commit — the normal condition under load — would acquire the instant the
//! lock released and read an empty ledger at `t = 0`.
//!
//! The commit therefore runs on its own task, so a cancelled caller abandons
//! the *wait* rather than the commit. This test pins that: it parks a reader
//! behind an in-flight commit, cancels the caller, and asserts the reader never
//! sees `t = 0` and that the commit still lands.

use crate::support;
use async_trait::async_trait;
use fluree_db_api::{Fluree, FlureeBuilder, LedgerHandle, NameServiceMode};
use fluree_db_core::content_kind::ContentKind;
use fluree_db_core::storage::ContentWriteResult;
use fluree_db_core::{
    ContentAddressedWrite, MemoryStorage, StorageMethod, StorageRead, StorageWrite,
};
use fluree_db_nameservice::memory::MemoryNameService;
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

fn person(id: &str) -> serde_json::Value {
    json!({
        "@context": { "ex": "http://example.org/" },
        "@id": format!("ex:{id}"),
        "ex:name": id
    })
}

/// Storage that parks the next commit-blob write once armed, until released.
///
/// The commit blob is written inside the detached window, under the ledger's
/// write lock. A parked commit-blob write therefore holds the commit in flight.
#[derive(Debug, Clone)]
struct CommitGate {
    inner: MemoryStorage,
    armed: Arc<AtomicBool>,
    parked: Arc<Notify>,
    release: Arc<Notify>,
}

impl CommitGate {
    fn new() -> Self {
        Self {
            inner: MemoryStorage::new(),
            armed: Arc::new(AtomicBool::new(false)),
            parked: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        }
    }

    /// Park the next commit-blob write.
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    /// Wait until a commit-blob write is parked.
    async fn parked(&self) {
        // `notify_one` stores a permit, so a write that parks before this
        // call is not missed. Bounded so a regression fails instead of
        // hanging until nextest kills the run.
        tokio::time::timeout(Duration::from_secs(60), self.parked.notified())
            .await
            .expect("the armed commit never wrote its commit blob");
    }

    /// Let the parked write proceed.
    fn release(&self) {
        self.release.notify_one();
    }
}

#[async_trait]
impl StorageRead for CommitGate {
    fn permits_plaintext_cache(&self) -> bool {
        self.inner.permits_plaintext_cache()
    }

    fn encryption_admin(&self) -> Option<Arc<dyn fluree_db_core::EncryptionAdmin>> {
        self.inner.encryption_admin()
    }

    async fn read_bytes(&self, address: &str) -> fluree_db_core::Result<Vec<u8>> {
        self.inner.read_bytes(address).await
    }

    async fn exists(&self, address: &str) -> fluree_db_core::Result<bool> {
        self.inner.exists(address).await
    }

    async fn list_prefix(&self, prefix: &str) -> fluree_db_core::Result<Vec<String>> {
        self.inner.list_prefix(prefix).await
    }
}

#[async_trait]
impl StorageWrite for CommitGate {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> fluree_db_core::Result<()> {
        self.inner.write_bytes(address, bytes).await
    }

    async fn delete(&self, address: &str) -> fluree_db_core::Result<()> {
        self.inner.delete(address).await
    }
}

#[async_trait]
impl ContentAddressedWrite for CommitGate {
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> fluree_db_core::Result<ContentWriteResult> {
        if kind == ContentKind::Commit && self.armed.swap(false, Ordering::SeqCst) {
            self.parked.notify_one();
            self.release.notified().await;
        }
        self.inner
            .content_write_bytes_with_hash(kind, ledger_id, content_hash_hex, bytes)
            .await
    }
}

impl StorageMethod for CommitGate {
    fn storage_method(&self) -> &str {
        self.inner.storage_method()
    }
}

async fn wait_for_t(handle: &LedgerHandle, want: i64) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let t = handle.t().await;
        if t == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "cached handle settled at t = {t}, expected {want}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_commit_never_exposes_the_empty_cache_slot() {
    let gate = CommitGate::new();
    let fluree: Fluree = FlureeBuilder::memory().build_with(
        gate.clone(),
        NameServiceMode::ReadWrite(Arc::new(MemoryNameService::new())),
    );

    let ledger_id = "it/cow-cancel:main";
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
    let handle = fluree
        .ledger_cached(ledger_id)
        .await
        .expect("cache the ledger");

    let alice = person("alice");
    fluree
        .stage(&handle)
        .insert(&alice)
        .execute()
        .await
        .expect("first commit");
    assert_eq!(handle.t().await, 1);

    // A second commit, on its own task so the caller can be cancelled the way
    // a client disconnect cancels an axum handler future. The gate holds it
    // inside the detached window until the test releases it.
    gate.arm();
    let committer = {
        let fluree = fluree.clone();
        let handle = handle.clone();
        tokio::spawn(async move {
            let bob = person("bob");
            fluree.stage(&handle).insert(&bob).execute().await
        })
    };
    gate.parked().await;
    assert!(
        handle.is_locked(),
        "the commit blob was written outside the ledger write lock"
    );

    // Park a reader behind the in-flight commit. It is queued before anything
    // the cancellation could schedule, so it is the first thing to observe the
    // cache slot once the write lock is released.
    let reader = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.t().await })
    };
    tokio::time::sleep(Duration::from_millis(5)).await;

    committer.abort();
    // The commit finishes only after its caller is cancelled.
    gate.release();

    let observed = reader.await.expect("reader task");
    assert_ne!(
        observed, 0,
        "a reader parked behind a cancelled commit observed the empty cache slot at t = 0; \
         the commit window is not shielded from cancellation"
    );

    // Cancelling the caller abandons the wait, not the commit: the shielded
    // task runs to completion and installs its state.
    wait_for_t(&handle, 2).await;

    let state = handle.snapshot().await.to_ledger_state();
    for id in ["ex:alice", "ex:bob"] {
        let query = json!({
            "@context": { "ex": "http://example.org/" },
            "select": { id: ["*"] }
        });
        let rows = support::query_jsonld(&fluree, &state, &query)
            .await
            .expect("query after a cancelled commit");
        assert!(
            !rows.is_empty(),
            "{id} must be readable through the handle after a cancelled commit"
        );
    }

    // And the handle still writes.
    let carol = person("carol");
    let after = fluree
        .stage(&handle)
        .insert(&carol)
        .execute()
        .await
        .expect("commit after a cancelled one");
    assert_eq!(after.receipt.t, 3);
}
