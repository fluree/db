//! Permanent erasure (`DATA_ERASURE.md`, PR 5A): selected occurrences are
//! removed from the commits that hold them, under proof.
//!
//! A request is first a targeted removal on every branch that holds an
//! occurrence (current values retracted, history suppressed), widened so an
//! occurrence takes the one bounding its span: an assertion and the
//! retraction that ends it both carry the value. A control commit then
//! records the request's authorization, and a durable job tracks it.
//!
//! The retention worker proves each affected commit's redaction with the
//! prover (`fluree-prover`, a separate binary), one step at a time, and
//! verifies the receipt in-process against the pinned guest images. Once
//! every affected commit is proved and indexed, the erasure is published:
//! from then on each original reads as its replacement (see [`index`]).
//! Publishing waits for the index so that no erased commit is ever replayed
//! as novelty: an assertion in the index must not lose its retraction. That
//! holds for every branch holding the commit as branches are when it is
//! published, since branch creation waits for acceptance and publication.
//!
//! A request is complete once the published erasure of each affected commit
//! removes every slot it asked for, whichever request proved them: an
//! overlapping request waits for the one ahead of it, and proves what that
//! one does not publish.
//!
//! A purge then deletes the originals and every other copy of what the
//! erasure removed (see [`purge`]).
//! Supported: a single node with file storage, v4 commits with SHA2-256
//! CIDs. Requests elsewhere are refused.

mod activation;
pub(crate) mod index;
mod members;
pub(crate) mod purge;
mod request;
pub(crate) mod transfer;
mod worker;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub use activation::ErasureActivator;
pub use fluree_db_erasure::{
    check_prover, find_prover, install_prover, Prover, ProverConfig, SubprocessProver, Verifier,
    PROVER_BINARY, REDACT_IMAGE_IDS,
};
pub use index::{ErasedCommit, ErasureStep};
pub use members::{MemberProving, ProofStep, ProvingMembers, StepOutcome};
/// Holds on purge passes, for other crates' tests.
#[cfg(feature = "testing")]
pub use purge::hooks as purge_hooks;
pub use purge::{PurgeInspection, PurgePlan, PurgeReport, PurgeScan, PurgeTimings};
pub use request::{BranchRemoval, ErasureReceipt, ErasureRequest};
use serde::{Deserialize, Serialize};
pub use transfer::{AcceptedPack, ErasedObject, ErasurePushes, PackReception};
pub use worker::ErasurePassReport;

/// How this instance proves erasures.
#[derive(Clone, Debug)]
pub struct ErasureConfig {
    pub prover: Arc<dyn Prover>,
    /// Checks receipts; the default accepts only the pinned guest images.
    pub verifier: Arc<Verifier>,
    /// First retry delay after the prover is unavailable (ms); it doubles
    /// up to an hour.
    pub retry_ms: i64,
}

impl ErasureConfig {
    /// Prove with the `fluree-prover` binary.
    pub fn subprocess(prover: ProverConfig) -> Self {
        ErasureConfig {
            prover: Arc::new(SubprocessProver::new(prover)),
            verifier: Arc::new(Verifier::default()),
            retry_ms: 60_000,
        }
    }
}

/// Erasure on a file-backed instance: the index of erased commits (always,
/// so their originals never read) and, when configured, the prover.
#[derive(Debug)]
pub(crate) struct ErasureRuntime {
    pub(crate) index: Arc<index::ErasureIndex>,
    /// What a cluster's collection condemned, checked by this storage's
    /// writes.
    pub(crate) condemned: Arc<crate::collect::Condemned>,
    /// A cluster member's collection, run when it leads.
    pub(crate) collector: std::sync::OnceLock<Arc<dyn crate::collect::Collector>>,
    pub(crate) config: Option<ErasureConfig>,
    /// A step is being proved.
    in_flight: AtomicBool,
    /// A background purge pass is running.
    purge_in_flight: AtomicBool,
    /// Held to accept or publish an erasure, to create a branch, to restore a
    /// ledger, and by a purge pass: the branches an erasure or a pass reaches
    /// are the branches as they are then. Taken before `purging`.
    pub(crate) admission: tokio::sync::Mutex<()>,
    /// Held by a purge pass: one at a time. Shared with key rotation, whose
    /// rewrites a purge's deletions must not interleave with.
    pub(crate) purging: Arc<tokio::sync::Mutex<()>>,
    /// Held to publish a full-text index's head: a purge's rebuild, and a
    /// sync's check that the head it built on is still the head.
    pub(crate) source_heads: tokio::sync::Mutex<()>,
    /// Branches a restore rebuilt over an erased commit, whose replaced
    /// indexes are released once the ledger is visible.
    pub(crate) rebuilt_on_restore: parking_lot::Mutex<std::collections::HashSet<String>>,
    /// Per ledger, which erased commits its history holds, as last found:
    /// see [`transfer::Reached`].
    pub(crate) reached: parking_lot::Mutex<std::collections::HashMap<String, transfer::Reached>>,
    /// Commits a reachability walk has read, for tests.
    #[cfg(test)]
    pub(crate) reach_reads: std::sync::atomic::AtomicUsize,
    /// Per branch: held by a commit from its check until it publishes, and
    /// exclusively by a reconciliation with received erasures.
    fences:
        parking_lot::Mutex<std::collections::HashMap<String, Arc<tokio::sync::RwLock<Reconciled>>>>,
    /// Keys [`Reconciled`]'s hashes, so they do not stand for the values.
    fact_keys: std::collections::hash_map::RandomState,
    /// Set on a cluster member: publications go through it.
    pub(crate) activator: std::sync::OnceLock<Arc<dyn ErasureActivator>>,
    /// Set on a cluster member: as leader, it has steps proved on others.
    pub(crate) members: std::sync::OnceLock<Arc<dyn ProvingMembers>>,
    /// The step this member proves for its leader.
    pub(crate) for_leader: parking_lot::Mutex<Option<members::ForLeader>>,
    /// Held while this host proves: a proof can take most of its memory.
    pub(crate) proving: tokio::sync::Mutex<()>,
    /// Per erased set, whether each branch's head at the last walk reaches
    /// it: a commit's history never changes, so the next walk stops there.
    pub(crate) reached_heads: parking_lot::Mutex<
        std::collections::HashMap<
            String,
            std::collections::HashMap<fluree_db_core::ContentId, bool>,
        >,
    >,
}

/// What reconciling a branch with received erasures took out of its
/// current state, as keyed hashes, and the head it did so at.
#[derive(Debug, Default)]
pub(crate) struct Reconciled {
    pub(crate) head: Option<fluree_db_core::ContentId>,
    pub(crate) facts: std::collections::HashSet<u64>,
}

impl ErasureRuntime {
    pub(crate) fn new(
        index: Arc<index::ErasureIndex>,
        condemned: Arc<crate::collect::Condemned>,
        config: Option<ErasureConfig>,
    ) -> Self {
        ErasureRuntime {
            index,
            condemned,
            collector: Default::default(),
            config,
            in_flight: AtomicBool::new(false),
            purge_in_flight: AtomicBool::new(false),
            admission: tokio::sync::Mutex::new(()),
            purging: Default::default(),
            source_heads: tokio::sync::Mutex::new(()),
            rebuilt_on_restore: Default::default(),
            reached: Default::default(),
            #[cfg(test)]
            reach_reads: Default::default(),
            fences: Default::default(),
            fact_keys: Default::default(),
            activator: Default::default(),
            members: Default::default(),
            for_leader: Default::default(),
            proving: Default::default(),
            reached_heads: Default::default(),
        }
    }

    pub(crate) fn fence(&self, branch: &str) -> Arc<tokio::sync::RwLock<Reconciled>> {
        let mut fences = self.fences.lock();
        if let Some(fence) = fences.get(branch) {
            return fence.clone();
        }
        fences.entry(branch.to_string()).or_default().clone()
    }

    pub(crate) fn fact_hash(&self, flake: &fluree_db_core::Flake) -> u64 {
        use std::hash::BuildHasher;
        self.fact_keys
            .hash_one(crate::retention::state::FactKey::of(flake))
    }
}

/// Whether the prover verifies the original's signatures, or exposes them
/// for the verifier to check (cheaper, but it reveals a signed commit's
/// body hash: see `fluree_db_redaction::Signatures`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SignatureMode {
    #[default]
    Verify,
    Expose,
}

impl From<SignatureMode> for fluree_db_erasure::Signatures {
    fn from(mode: SignatureMode) -> Self {
        match mode {
            SignatureMode::Verify => fluree_db_erasure::Signatures::Verify,
            SignatureMode::Expose => fluree_db_erasure::Signatures::Expose,
        }
    }
}

/// Where an erasure request stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ErasureStage {
    /// Commits are being proved.
    Proving,
    /// Every commit is proved; publishing waits for the index to cover them.
    Indexing,
    /// The replacements stand for the originals.
    Published,
    /// The originals, and the copies of what the request removed, are being
    /// deleted (see the job's `purge`).
    Purging,
    /// No storage the purge inspected holds what the request removed, except
    /// values a surviving occurrence still uses.
    Purged,
    /// A commit cannot be proved (the prover refused it); nothing published.
    Failed,
    /// A stage of a later release, kept as written: this release leaves its
    /// job alone.
    Other(String),
}

impl ErasureStage {
    fn as_str(&self) -> &str {
        match self {
            ErasureStage::Proving => "proving",
            ErasureStage::Indexing => "indexing",
            ErasureStage::Published => "published",
            ErasureStage::Purging => "purging",
            ErasureStage::Purged => "purged",
            ErasureStage::Failed => "failed",
            ErasureStage::Other(stage) => stage,
        }
    }
}

impl Serialize for ErasureStage {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ErasureStage {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let stage = String::deserialize(deserializer)?;
        Ok(match stage.as_str() {
            "proving" => ErasureStage::Proving,
            "indexing" => ErasureStage::Indexing,
            "published" => ErasureStage::Published,
            "purging" => ErasureStage::Purging,
            "purged" => ErasureStage::Purged,
            "failed" => ErasureStage::Failed,
            _ => ErasureStage::Other(stage),
        })
    }
}

/// One affected commit of a request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitErasure {
    pub commit: String,
    pub t: i64,
    /// Original slots this request removes from it, ascending.
    pub slots: Vec<u32>,
    /// The ledger its signatures were made for.
    pub ledger_id: String,
    /// Branches whose history held it when the request was accepted.
    pub branches: Vec<String>,
    /// This request's step, once proved; none when an earlier request's
    /// published step removes every slot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proved: Option<ProvedStep>,
}

/// A proved, verified, stored step.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProvedStep {
    /// Original slots it removes: the requested ones its base still held.
    pub slots: Vec<u32>,
    /// The replacement it redacted further; none for the original commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    pub replacement: String,
    /// SHA-256 (hex) of the receipt.
    pub proof: String,
    pub input_bytes: u64,
    pub replacement_bytes: u64,
    pub user_cycles: u64,
    pub segments: u32,
    pub prove_ms: u64,
}

/// A permanent-erasure request and its progress, kept in the requesting
/// branch's retention registration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErasureJob {
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requester: Option<String>,
    pub accepted_ms: i64,
    /// The control commit that records the request's authorization.
    pub authorized_t: i64,
    /// That commit's id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorized_commit: Option<String>,
    #[serde(default)]
    pub signatures: SignatureMode,
    pub stage: ErasureStage,
    /// Kept outside the registration's record where it can be (see
    /// [`crate::retention::registry`]), which then omits it.
    #[serde(default)]
    pub commits: Vec<CommitErasure>,
    /// The properties (IRIs) of the occurrences it erases, as each branch
    /// that held one decoded it. Empty when one did not decode, or for a job
    /// recorded before they were kept: it may erase any.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub properties: Vec<String>,
    /// Prover attempts that failed and will be retried.
    #[serde(default)]
    pub retries: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_ms: Option<i64>,
    /// Occurrences joined after acceptance: retractions a holder made since,
    /// each ending the span of one of the request's occurrences.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added: Vec<crate::retention::removal::OccurrenceRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purge: Option<PurgeProgress>,
    /// On a cluster: its publication's activation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<ClusterPublication>,
    /// A job in a later release's stage as stored, with the fields this
    /// release does not know: a registration writes it back so.
    #[serde(skip)]
    pub(crate) as_written: Option<std::sync::Arc<AsWritten>>,
}

/// A job's publication on a cluster: published once its manifest's
/// activation applied, effective for serving once every member installed
/// it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterPublication {
    /// The hash of the manifest last prepared for activation.
    pub manifest: String,
    /// Its activation, once applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation: Option<u64>,
    /// When every member had installed it (epoch ms): from then no member
    /// serves the erased history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_ms: Option<i64>,
}

/// A job's stored form, and how this release read it.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct AsWritten {
    stored: serde_json::Value,
    read: serde_json::Value,
}

/// A registration's jobs, each in a later release's stage kept as stored
/// unless this release changed it.
pub(crate) mod jobs_as_written {
    use serde::de::Error as _;
    use serde::ser::{Error as _, SerializeSeq};
    use serde::{Deserialize, Deserializer, Serializer};

    use super::{AsWritten, ErasureJob, ErasureStage};

    pub(crate) fn serialize<S: Serializer>(
        jobs: &[ErasureJob],
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(jobs.len()))?;
        for job in jobs {
            match &job.as_written {
                Some(written)
                    if serde_json::to_value(job).map_err(S::Error::custom)? == written.read =>
                {
                    seq.serialize_element(&written.stored)?;
                }
                _ => seq.serialize_element(job)?,
            }
        }
        seq.end()
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Vec<ErasureJob>, D::Error> {
        Vec::<serde_json::Value>::deserialize(deserializer)?
            .into_iter()
            .map(|stored| {
                let mut job: ErasureJob =
                    serde_json::from_value(stored.clone()).map_err(D::Error::custom)?;
                if matches!(job.stage, ErasureStage::Other(_)) {
                    let read = serde_json::to_value(&job).map_err(D::Error::custom)?;
                    job.as_written = Some(std::sync::Arc::new(AsWritten { stored, read }));
                }
                Ok(job)
            })
            .collect()
    }
}

impl ErasureJob {
    /// Still to be published.
    pub fn pending(&self) -> bool {
        matches!(self.stage, ErasureStage::Proving | ErasureStage::Indexing)
    }

    /// Published, with its storage still to be purged.
    pub fn purge_pending(&self) -> bool {
        matches!(self.stage, ErasureStage::Published | ErasureStage::Purging)
    }

    /// When a cluster's every member had installed its publication: no
    /// member then reads or writes back what it erases. `None` until then,
    /// and off a cluster.
    pub fn effective_ms(&self) -> Option<i64> {
        self.cluster.as_ref().and_then(|c| c.effective_ms)
    }

    /// Published and still to be purged, and on a cluster (`clustered`)
    /// effective: a purge plans only these.
    pub fn purge_ready(&self, clustered: bool) -> bool {
        self.purge_pending() && (!clustered || self.effective_ms().is_some())
    }

    /// Purged or refused: nothing left to do.
    pub fn finished(&self) -> bool {
        matches!(self.stage, ErasureStage::Purged | ErasureStage::Failed)
    }
}

/// How far a job's purge has come. Counts only, never a value.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PurgeProgress {
    /// When its holders' indexes were rebuilt from the replacements; their
    /// older indexes are released once readers have had time to finish.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rebuilt_ms: Option<i64>,
    /// When a scan found nothing unaccounted for, before the originals were
    /// deleted: a resumed purge then only deletes them and checks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scanned_ms: Option<i64>,
    /// On a cluster: when no idempotency record from before the erasure was
    /// effective was left, so the envelopes of the requests it erases were
    /// garbage from then.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envelopes_ms: Option<i64>,
    /// On a cluster: when the shared storage was purged; its members' own
    /// copies are cleaned after.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_ms: Option<i64>,
    /// On a cluster: when, every member having cleaned, the logs stopped
    /// members left were retired; what a replay of one wrote back is
    /// collected from then before the job completes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retired_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purged_ms: Option<i64>,
    /// Files deleted.
    #[serde(default)]
    pub deleted: usize,
    /// Removed values a surviving occurrence still uses, which stay.
    #[serde(default)]
    pub in_use: usize,
    /// Why the purge cannot go on, while it cannot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked: Option<String>,
    /// What a completed purge could not check, and what to do about it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unverified: Vec<String>,
}
