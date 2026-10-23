//! Commit and transaction files no branch references: the records naming
//! them, the check that decides, their collection, and the reconciliation
//! that finds what no record names.
//!
//! Only an instance holding its storage root's lock collects. Writers pin the
//! blobs they store until a published commit names them
//! ([`fluree_db_core::storage::pins`]), and pins see every writer only where
//! one process writes.

mod check;
mod collect;
mod reconcile;
#[cfg(test)]
mod tests;

pub use collect::GarbageReport;
pub use reconcile::ReconcileReport;

use std::sync::Arc;

use fluree_db_core::{
    content_address, BlobPins, ContentId, ContentKind, Storage, StorageNamespace, StorageRoot,
    CODEC_FLUREE_COMMIT, CODEC_FLUREE_TXN,
};
use serde::{Deserialize, Serialize};

use crate::{ApiError, Fluree, Result};

/// Under each ledger root, beside its branch folders: `@` is not allowed in a
/// branch name.
const DIR: &str = "@garbage";

/// Why a blob was recorded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum GarbageReason {
    /// Its writer failed before publishing a commit naming it.
    Abandoned,
    /// A rebase planned it, or replaced the line naming it.
    Rebase,
    /// Reconciliation found nothing naming it.
    Reconciled,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Record {
    recorded_ms: i64,
    reason: GarbageReason,
}

/// A commit or transaction blob in one branch's folder.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Blob {
    pub(crate) namespace: StorageNamespace,
    pub(crate) id: ContentId,
}

impl Blob {
    pub(crate) fn new(namespace: StorageNamespace, id: ContentId) -> Self {
        Self { namespace, id }
    }

    fn kind(&self) -> Option<(ContentKind, &'static str)> {
        match self.id.content_kind()? {
            ContentKind::Commit => Some((ContentKind::Commit, "commit")),
            ContentKind::Txn => Some((ContentKind::Txn, "txn")),
            _ => None,
        }
    }

    fn branch(&self) -> &str {
        let root = self.namespace.root().as_str();
        &self.namespace.branch_prefix()[root.len() + 1..]
    }

    /// Where the blob is stored.
    pub(crate) fn address(&self, method: &str) -> Option<String> {
        let (kind, _) = self.kind()?;
        Some(content_address(
            method,
            kind,
            &self.namespace,
            &self.id.address_digest(),
        ))
    }

    fn record_address(&self, method: &str) -> Option<String> {
        let (_, name) = self.kind()?;
        Some(format!(
            "{}{}/{name}-{}.json",
            dir_address(method, self.namespace.root()),
            self.branch(),
            self.id.address_digest()
        ))
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn dir_address(method: &str, root: &StorageRoot) -> String {
    format!("fluree:{method}://{}/{DIR}/", root.as_str())
}

/// The blob a record at `address`, under `root`, names.
fn parse_record_address(method: &str, root: &StorageRoot, address: &str) -> Option<Blob> {
    let rest = address.strip_prefix(&dir_address(method, root))?;
    let (branch, file) = rest.rsplit_once('/')?;
    let file = file.strip_suffix(".json")?;
    let (codec, digest) = match file.split_once('-')? {
        ("commit", digest) => (CODEC_FLUREE_COMMIT, digest),
        ("txn", digest) => (CODEC_FLUREE_TXN, digest),
        _ => return None,
    };
    Some(Blob::new(
        StorageNamespace::new(root.clone(), branch),
        ContentId::from_address_digest(codec, digest)?,
    ))
}

/// A record as read back.
struct Recorded {
    blob: Blob,
    record: Record,
    address: String,
}

impl Fluree {
    /// The pins writers to this instance's storage take; `None` where it does
    /// not collect.
    pub(crate) fn blob_pins(&self) -> Option<&Arc<BlobPins>> {
        self.root.pins.as_ref()
    }

    fn garbage_storage(&self) -> Result<&dyn Storage> {
        self.admin_storage()
            .ok_or_else(|| ApiError::internal("garbage collection needs managed storage"))
    }

    /// Record `blobs` as garbage candidates, where this instance collects.
    pub(crate) async fn record_garbage(&self, blobs: &[Blob], reason: GarbageReason) -> Result<()> {
        if self.blob_pins().is_none() || blobs.is_empty() {
            return Ok(());
        }
        let storage = self.garbage_storage()?;
        let method = storage.storage_method().to_string();
        let bytes = serde_json::to_vec(&Record {
            recorded_ms: now_ms(),
            reason,
        })?;
        for blob in blobs {
            if let Some(address) = blob.record_address(&method) {
                storage.write_bytes(&address, &bytes).await?;
            }
        }
        Ok(())
    }

    /// The candidates recorded under `root`.
    async fn garbage_records(&self, root: &StorageRoot) -> Result<Vec<Recorded>> {
        let storage = self.garbage_storage()?;
        let method = storage.storage_method().to_string();
        let mut recorded = Vec::new();
        for address in storage.list_prefix(&dir_address(&method, root)).await? {
            let Some(blob) = parse_record_address(&method, root, &address) else {
                continue;
            };
            let bytes = match storage.read_bytes(&address).await {
                Ok(bytes) => bytes,
                Err(fluree_db_core::Error::NotFound(_)) => continue,
                Err(e) => return Err(e.into()),
            };
            let Ok(record) = serde_json::from_slice(&bytes) else {
                tracing::warn!(%address, "unreadable garbage record");
                continue;
            };
            recorded.push(Recorded {
                blob,
                record,
                address,
            });
        }
        Ok(recorded)
    }
}
