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
//! # Failure handling
//!
//! On any error path that drops a pending upload without calling `finish()`,
//! the [`Drop`] guard aborts the in-flight task and (if the upload completed
//! before the abort landed) spawns a detached release task to reclaim the
//! orphaned content. Callers on known-failure paths may call [`abort`]
//! explicitly to await the release before proceeding.
//!
//! [`abort`]: PendingRawTxnUpload::abort

use crate::error::{Result, TransactError};
use fluree_db_core::{ContentId, ContentKind, ContentStore};
use std::sync::Arc;
use tokio::task::JoinHandle;

/// A raw-txn upload in flight or completed.
///
/// See module docs for the lifecycle contract.
pub struct PendingRawTxnUpload {
    handle: Option<JoinHandle<Result<ContentId>>>,
    content_store: Arc<dyn ContentStore>,
}

impl PendingRawTxnUpload {
    /// Spawn the upload on the current Tokio runtime.
    ///
    /// Serialization of `txn_json` happens inside the task so it doesn't add
    /// latency on the caller's path.
    pub fn spawn(content_store: Arc<dyn ContentStore>, txn_json: serde_json::Value) -> Self {
        let store_for_task = Arc::clone(&content_store);
        let handle = tokio::spawn(async move {
            let bytes = serde_json::to_vec(&txn_json)?;
            let cid = store_for_task.put(ContentKind::Txn, &bytes).await?;
            tracing::info!(raw_txn_bytes = bytes.len(), "raw txn stored");
            Ok::<_, TransactError>(cid)
        });
        Self {
            handle: Some(handle),
            content_store,
        }
    }

    /// Await the upload and return the resulting ContentId.
    ///
    /// On success, consumes self without triggering the Drop-guard release —
    /// the caller is committing to reference this CID from the commit record.
    pub async fn finish(mut self) -> Result<ContentId> {
        let handle = self
            .handle
            .take()
            .expect("handle present until finish/abort");
        match handle.await {
            Ok(Ok(cid)) => Ok(cid),
            Ok(Err(e)) => Err(e),
            Err(join_err) => Err(TransactError::RawTxnUpload(format!(
                "upload task failed: {join_err}"
            ))),
        }
    }

    /// Explicitly abort the upload and release any completed content.
    ///
    /// Awaits the cancellation so callers on known-error paths can be sure
    /// the release has been issued before they return. For implicit failures
    /// (e.g., `?` propagation), the Drop guard performs the same work on a
    /// detached task.
    pub async fn abort(mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
            if let Ok(Ok(cid)) = handle.await {
                let _ = self.content_store.release(&cid).await;
            }
        }
    }
}

impl Drop for PendingRawTxnUpload {
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        handle.abort();
        // Spawn a detached release task only if we're inside a tokio runtime.
        // Outside of one (e.g., synchronous test teardown), we drop the handle
        // and accept that orphaned content may remain for the backend's GC.
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            let store = Arc::clone(&self.content_store);
            rt.spawn(async move {
                if let Ok(Ok(cid)) = handle.await {
                    let _ = store.release(&cid).await;
                }
            });
        }
    }
}

impl std::fmt::Debug for PendingRawTxnUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingRawTxnUpload")
            .field("pending", &self.handle.is_some())
            .finish()
    }
}
