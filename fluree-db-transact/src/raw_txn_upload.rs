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
//! landed after the winning attempt's no-op re-put). So a dropped pending
//! upload only cancels the in-flight task; a blob that already landed is left
//! in place as a harmless orphan for reachability-based GC.

use crate::error::{Result, TransactError};
use fluree_db_core::{ContentId, ContentKind, ContentStore};
use std::sync::Arc;
use tokio::task::JoinHandle;

/// A raw-txn upload in flight or completed.
///
/// See module docs for the lifecycle contract.
pub struct PendingRawTxnUpload {
    handle: Option<JoinHandle<Result<ContentId>>>,
}

impl PendingRawTxnUpload {
    /// Spawn the upload on the current Tokio runtime.
    ///
    /// Serialization of `txn_json` happens inside the task so it doesn't add
    /// latency on the caller's path.
    pub fn spawn(content_store: Arc<dyn ContentStore>, txn_json: serde_json::Value) -> Self {
        let handle = tokio::spawn(async move {
            let bytes = serde_json::to_vec(&txn_json)?;
            let cid = content_store.put(ContentKind::Txn, &bytes).await?;
            tracing::info!(raw_txn_bytes = bytes.len(), raw_txn_cid = %cid, "raw txn stored");
            Ok::<_, TransactError>(cid)
        });
        Self {
            handle: Some(handle),
        }
    }

    /// Await the upload and return the resulting ContentId.
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
}

impl Drop for PendingRawTxnUpload {
    fn drop(&mut self) {
        // Cancel an upload still in flight. A blob that already landed stays
        // in storage — see the module docs for why it must not be deleted.
        if let Some(handle) = self.handle.take() {
            handle.abort();
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
