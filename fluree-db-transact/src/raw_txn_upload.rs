//! Parallel upload of the original transaction JSON to the content store.
//!
//! When a transaction opts into `store_raw_txn`, the raw JSON bytes must be
//! durably stored so the resulting commit record can reference them by
//! ContentId for provenance. This upload is I/O-heavy and, on remote backends
//! like S3, can dominate commit latency if issued serially.
//!
//! [`PendingRawTxnUpload::spawn`] kicks the upload off on a Tokio task at the
//! moment the raw bytes are known — typically at the top of the transaction
//! pipeline, in parallel with parse / policy / staging work. The commit path
//! awaits [`PendingRawTxnUpload::finish`] just before writing the commit blob,
//! so the upload overlaps CPU work and the commit still blocks on durability.
//!
//! # Failure handling — never delete inline
//!
//! Raw-txn blobs are content-addressed: two transactions with byte-identical
//! bodies (a client retry, an SQS redelivery, an in-process commit-conflict
//! restage) map to the **same** CID and the same storage key, and
//! `ContentStore::release` is an unconditional delete with no reference count.
//! An inline release on a failure path can therefore delete a blob that an
//! already-published commit references, leaving a permanent dangling
//! `commit.txn` pointer (observed in production: a retry's Drop-guard delete
//! landed after the winning attempt's no-op re-put).
//!
//! Where a collector runs (the store has a
//! [`blob_pinner`](fluree_db_core::ContentStore::blob_pinner)), the upload
//! pins the blob before storing it, the commit path holds the pin through
//! publication, and a transaction that fails abandons it: the collector
//! records the blob and deletes it once no commit names it and no writer
//! pins it. Elsewhere a dropped pending upload only cancels the in-flight
//! task, and a blob that already landed stays.

use crate::error::Result;
#[cfg(not(target_arch = "wasm32"))]
use crate::error::TransactError;
use fluree_db_core::{BlobPin, ContentId, ContentKind, ContentStore, HashAlgorithm};
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use tokio::task::JoinHandle;

/// A stored body's id, with its pin where the store pins.
type Uploaded = (ContentId, Option<BlobPin>);

/// A raw-txn upload in flight or completed.
///
/// See module docs for the lifecycle contract.
#[cfg(not(target_arch = "wasm32"))]
pub struct PendingRawTxnUpload {
    handle: Option<JoinHandle<Result<Uploaded>>>,
    /// The store pins, so a failed transaction sees the upload through.
    pinned: bool,
    /// The store's collection intents loaded before the upload stored.
    intents_loaded: Option<u64>,
}

#[cfg(not(target_arch = "wasm32"))]
impl PendingRawTxnUpload {
    /// Spawn the upload on the current Tokio runtime.
    ///
    /// Serialization of `txn_json` happens inside the task so it doesn't add
    /// latency on the caller's path.
    pub fn spawn(
        content_store: Arc<dyn ContentStore>,
        txn_json: serde_json::Value,
        alg: HashAlgorithm,
    ) -> Self {
        let pinner = content_store.blob_pinner();
        let pinned = pinner.is_some();
        let intents_loaded = content_store.intents_loaded();
        let handle = tokio::spawn(async move {
            let bytes = serde_json::to_vec(&txn_json)?;
            let (cid, pin) = match pinner {
                Some(pinner) => {
                    let cid = ContentId::new_with(alg, ContentKind::Txn, &bytes);
                    let pin = pinner.pin(&cid).await;
                    if let Err(e) = content_store.put_minted(&cid, &bytes).await {
                        pin.abandon();
                        return Err(e.into());
                    }
                    (cid, Some(pin))
                }
                None => (
                    content_store
                        .put_hashed(alg, ContentKind::Txn, &bytes)
                        .await?,
                    None,
                ),
            };
            tracing::info!(raw_txn_bytes = bytes.len(), raw_txn_cid = %cid, "raw txn stored");
            Ok::<_, TransactError>((cid, pin))
        });
        Self {
            handle: Some(handle),
            pinned,
            intents_loaded,
        }
    }

    /// How many collection intents the store had loaded before the upload
    /// stored: what a write publishing it presents, at most. See
    /// [`ContentStore::intents_loaded`].
    pub fn intents_loaded(&self) -> Option<u64> {
        self.intents_loaded
    }

    /// Await the upload: the blob's ContentId, and its pin when the store
    /// pins, which the caller holds until a commit naming the blob is
    /// published or abandons.
    pub async fn finish(mut self) -> Result<Uploaded> {
        let handle = self
            .handle
            .take()
            .expect("handle present until finish/abort");
        match handle.await {
            Ok(result) => result,
            Err(join_err) => Err(TransactError::RawTxnUpload(format!(
                "upload task failed: {join_err}"
            ))),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for PendingRawTxnUpload {
    fn drop(&mut self) {
        // The transaction failed. A pinned upload is seen through and its
        // blob abandoned; an unpinned one is cancelled, and a blob that
        // already landed stays — see the module docs.
        let Some(handle) = self.handle.take() else {
            return;
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) if self.pinned => {
                runtime.spawn(async move {
                    if let Ok(Ok((_, Some(pin)))) = handle.await {
                        pin.abandon();
                    }
                });
            }
            _ => handle.abort(),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Debug for PendingRawTxnUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingRawTxnUpload")
            .field("pending", &self.handle.is_some())
            .finish()
    }
}

/// wasm32 twin: single-threaded, no ambient tokio runtime — `tokio::spawn`
/// would panic and there is no latency-overlap to win anyway. The upload is
/// deferred as data and runs inside [`finish`](Self::finish); nothing is
/// stored before that, so `abort` and `Drop` have nothing to release.
#[cfg(target_arch = "wasm32")]
pub struct PendingRawTxnUpload {
    deferred: Option<(Arc<dyn ContentStore>, serde_json::Value, HashAlgorithm)>,
}

#[cfg(target_arch = "wasm32")]
impl PendingRawTxnUpload {
    /// Defer the upload (same signature as the native spawn; see type docs).
    pub fn spawn(
        content_store: Arc<dyn ContentStore>,
        txn_json: serde_json::Value,
        alg: HashAlgorithm,
    ) -> Self {
        Self {
            deferred: Some((content_store, txn_json, alg)),
        }
    }

    /// Nothing collects here.
    pub fn intents_loaded(&self) -> Option<u64> {
        None
    }

    /// Run the upload now and return the resulting ContentId. Nothing
    /// collects here, so there is no pin.
    pub async fn finish(mut self) -> Result<Uploaded> {
        let (store, txn_json, alg) = self
            .deferred
            .take()
            .expect("deferred inputs present until finish/abort");
        let bytes = serde_json::to_vec(&txn_json)?;
        let cid = store.put_hashed(alg, ContentKind::Txn, &bytes).await?;
        tracing::info!(raw_txn_bytes = bytes.len(), "raw txn stored");
        Ok((cid, None))
    }

    /// Nothing has been stored yet; just drop the deferred inputs.
    pub async fn abort(mut self) {
        self.deferred = None;
    }
}

#[cfg(target_arch = "wasm32")]
impl std::fmt::Debug for PendingRawTxnUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingRawTxnUpload")
            .field("pending", &self.deferred.is_some())
            .finish()
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use fluree_db_core::ContentBytes;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Semaphore;

    /// A store that does not pin, whose writes wait for a permit.
    #[derive(Debug)]
    struct Gated {
        permits: Semaphore,
        landed: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl ContentStore for Gated {
        async fn has(&self, _: &ContentId) -> fluree_db_core::Result<bool> {
            Ok(false)
        }
        async fn get(&self, id: &ContentId) -> fluree_db_core::Result<ContentBytes> {
            Err(fluree_db_core::Error::not_found(id.to_string()))
        }
        async fn put(&self, kind: ContentKind, bytes: &[u8]) -> fluree_db_core::Result<ContentId> {
            let _permit = self.permits.acquire().await.unwrap();
            self.landed.fetch_add(1, Ordering::SeqCst);
            Ok(ContentId::new(kind, bytes))
        }
        async fn put_with_id(&self, _: &ContentId, _: &[u8]) -> fluree_db_core::Result<()> {
            unimplemented!()
        }
        async fn release(&self, _: &ContentId) -> fluree_db_core::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_failed_transaction_cancels_an_upload_nothing_collects() {
        let store = Arc::new(Gated {
            permits: Semaphore::new(0),
            landed: AtomicUsize::new(0),
        });
        let pending = PendingRawTxnUpload::spawn(
            store.clone(),
            serde_json::json!({"insert": {}}),
            HashAlgorithm::Sha2_256,
        );
        tokio::task::yield_now().await;
        drop(pending);
        store.permits.add_permits(1);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(store.landed.load(Ordering::SeqCst), 0);
    }
}
