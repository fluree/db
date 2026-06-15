//! In-memory registry for negotiated-upload import jobs.
//!
//! Backs the reference implementation of the negotiated `.flpack` upload flow
//! (mint → upload → complete → poll). A production server fronting real object
//! storage would persist this state externally (DB / S3 tags); the reference
//! server keeps it in process — it exists to exercise the CLI handshake and
//! give implementers something concrete to diff against.

use dashmap::DashMap;
use std::path::PathBuf;
use std::time::Instant;

/// Lifecycle of one negotiated-upload import.
///
/// `AwaitingUpload` covers both "minted, no blob yet" and "blob uploaded,
/// not yet completed" — `complete` verifies the staged file exists rather
/// than tracking an extra state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImportStatus {
    AwaitingUpload,
    Running,
    Succeeded,
    Failed,
}

impl ImportStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            ImportStatus::AwaitingUpload => "awaiting-upload",
            ImportStatus::Running => "running",
            ImportStatus::Succeeded => "succeeded",
            ImportStatus::Failed => "failed",
        }
    }
}

/// One negotiated-upload import job.
pub struct ImportJob {
    /// Target ledger name (carried in the mint request, not the URL path).
    pub ledger_id: String,
    /// Single-use capability token embedded in the upload URL. Modeled on a
    /// presigned URL's signature: knowing it authorizes the blob `PUT`.
    pub token: String,
    /// Where the local backend stages the uploaded archive.
    pub staged_path: PathBuf,
    pub status: ImportStatus,
    /// `RestoreResult` JSON once `status == Succeeded`.
    pub result: Option<serde_json::Value>,
    /// Error message once `status == Failed`.
    pub error: Option<String>,
    pub created_at: Instant,
}

/// Process-local registry of import jobs, keyed by opaque `import_id`.
#[derive(Default)]
pub struct ImportJobs {
    jobs: DashMap<String, ImportJob>,
}

impl ImportJobs {
    pub fn insert(&self, import_id: String, job: ImportJob) {
        self.jobs.insert(import_id, job);
    }

    /// Snapshot the fields needed to authorize and stage a blob upload, without
    /// holding the map lock across the (async) file write.
    pub fn upload_target(&self, import_id: &str) -> Option<(String, PathBuf, ImportStatus)> {
        self.jobs
            .get(import_id)
            .map(|j| (j.token.clone(), j.staged_path.clone(), j.status))
    }

    /// Snapshot `(ledger_id, staged_path, status)` for the `complete` handler.
    pub fn completion_target(&self, import_id: &str) -> Option<(String, PathBuf, ImportStatus)> {
        self.jobs
            .get(import_id)
            .map(|j| (j.ledger_id.clone(), j.staged_path.clone(), j.status))
    }

    pub fn set_status(&self, import_id: &str, status: ImportStatus) {
        if let Some(mut j) = self.jobs.get_mut(import_id) {
            j.status = status;
        }
    }

    pub fn set_succeeded(&self, import_id: &str, result: serde_json::Value) {
        if let Some(mut j) = self.jobs.get_mut(import_id) {
            j.status = ImportStatus::Succeeded;
            j.result = Some(result);
        }
    }

    pub fn set_failed(&self, import_id: &str, error: impl Into<String>) {
        if let Some(mut j) = self.jobs.get_mut(import_id) {
            j.status = ImportStatus::Failed;
            j.error = Some(error.into());
        }
    }

    /// Snapshot status fields for the poll endpoint: `(status, result, error)`.
    pub fn status_snapshot(
        &self,
        import_id: &str,
    ) -> Option<(ImportStatus, Option<serde_json::Value>, Option<String>)> {
        self.jobs
            .get(import_id)
            .map(|j| (j.status, j.result.clone(), j.error.clone()))
    }
}
