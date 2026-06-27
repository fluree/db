//! In-memory registry for negotiated-upload import jobs.
//!
//! Backs the reference implementation of the negotiated `.flpack` upload flow
//! (mint → upload → complete → poll). A production server fronting real object
//! storage would persist this state externally (DB / S3 tags); the reference
//! server keeps it in process — it exists to exercise the CLI handshake and
//! give implementers something concrete to diff against.

use dashmap::DashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// How long a minted upload job (and its staged files) lives before it is
/// reclaimed. The mint response advertises this same window as
/// `expires_at_unix`, so both derive from one source of truth.
pub const IMPORT_JOB_TTL: Duration = Duration::from_secs(3600);

/// Filesystem residue of a swept job that the (async) caller deletes *off* the
/// map lock: the staged archive plus, for a multipart job, the part files that
/// may still be sitting beside it.
pub struct StagedResidue {
    pub staged_path: PathBuf,
    pub num_parts: Option<u32>,
}

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

/// Multipart upload plan for a single import job.
///
/// Present only when the archive exceeded the multipart threshold at mint
/// time. The reference backend stages each part to a sibling of `staged_path`
/// and concatenates them in order on `complete`; a production backend would
/// instead carry the object-store `upload_id` through to
/// `CompleteMultipartUpload`.
#[derive(Clone, Debug)]
pub struct MultipartPlan {
    /// Opaque upload identity (object-store `UploadId`, or a reference token).
    pub upload_id: String,
    /// Byte size of every part except the last.
    pub part_size: u64,
    /// Total number of parts the client must upload.
    pub num_parts: u32,
}

/// One negotiated-upload import job.
pub struct ImportJob {
    /// Target ledger name (carried in the mint request, not the URL path).
    pub ledger_id: String,
    /// Single-use capability token embedded in the upload URL. Modeled on a
    /// presigned URL's signature: knowing it authorizes the blob `PUT`.
    pub token: String,
    /// Where the local backend stages the (assembled) uploaded archive.
    pub staged_path: PathBuf,
    /// Multipart plan when the archive is uploaded in parts; `None` for a
    /// single-PUT upload.
    pub multipart: Option<MultipartPlan>,
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

    /// Snapshot the fields needed to authorize and stage a blob/part upload,
    /// without holding the map lock across the (async) file write. The
    /// `MultipartPlan` is `Some` for multipart jobs (the part `PUT` handler
    /// uses it; the single-blob `PUT` handler rejects it, and vice versa).
    pub fn upload_target(
        &self,
        import_id: &str,
    ) -> Option<(
        String,
        PathBuf,
        ImportStatus,
        Option<MultipartPlan>,
        Instant,
    )> {
        self.jobs.get(import_id).map(|j| {
            (
                j.token.clone(),
                j.staged_path.clone(),
                j.status,
                j.multipart.clone(),
                j.created_at,
            )
        })
    }

    /// Snapshot `(ledger_id, staged_path, status, multipart)` for the
    /// `complete` handler.
    pub fn completion_target(
        &self,
        import_id: &str,
    ) -> Option<(
        String,
        PathBuf,
        ImportStatus,
        Option<MultipartPlan>,
        Instant,
    )> {
        self.jobs.get(import_id).map(|j| {
            (
                j.ledger_id.clone(),
                j.staged_path.clone(),
                j.status,
                j.multipart.clone(),
                j.created_at,
            )
        })
    }

    /// Drop jobs older than `ttl` and return their staged-file residue for the
    /// caller to delete. A `Running` job is spared regardless of age — a
    /// background restore may still be reading its staged archive. Called on
    /// each mint so the registry and staging dir never grow without bound and
    /// the advertised `expires_at_unix` is actually reclaimed.
    pub fn sweep_expired(&self, ttl: Duration) -> Vec<StagedResidue> {
        let mut residue = Vec::new();
        self.jobs.retain(|_id, job| {
            if job.created_at.elapsed() > ttl && job.status != ImportStatus::Running {
                residue.push(StagedResidue {
                    staged_path: job.staged_path.clone(),
                    num_parts: job.multipart.as_ref().map(|p| p.num_parts),
                });
                false
            } else {
                true
            }
        });
        residue
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

#[cfg(test)]
mod tests {
    use super::*;

    fn job(status: ImportStatus) -> ImportJob {
        ImportJob {
            ledger_id: "books:main".into(),
            token: "tok".into(),
            staged_path: PathBuf::from("/tmp/x.flpack"),
            multipart: None,
            status,
            result: None,
            error: None,
            created_at: Instant::now(),
        }
    }

    #[test]
    fn sweep_drops_expired_jobs_but_spares_running() {
        let jobs = ImportJobs::default();
        jobs.insert("await".into(), job(ImportStatus::AwaitingUpload));
        jobs.insert("done".into(), job(ImportStatus::Succeeded));
        jobs.insert("running".into(), job(ImportStatus::Running));

        // A zero TTL means everything minted before this instant is expired.
        std::thread::sleep(Duration::from_millis(5));
        let residue = jobs.sweep_expired(Duration::ZERO);

        // Awaiting + terminal jobs are reaped; the running restore is spared so
        // its staged archive is not pulled out from under it.
        assert_eq!(residue.len(), 2);
        assert!(jobs.upload_target("await").is_none());
        assert!(jobs.upload_target("done").is_none());
        assert!(jobs.upload_target("running").is_some());
    }

    #[test]
    fn sweep_keeps_fresh_jobs() {
        let jobs = ImportJobs::default();
        jobs.insert("fresh".into(), job(ImportStatus::AwaitingUpload));

        let residue = jobs.sweep_expired(IMPORT_JOB_TTL);

        assert!(residue.is_empty());
        assert!(jobs.upload_target("fresh").is_some());
    }
}
