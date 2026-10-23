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
//! Originals are retained in this release; purging them is the next step.
//! Supported: a single node with file storage, v4 commits with SHA2-256
//! CIDs. Requests elsewhere are refused.

pub(crate) mod index;
mod purge;
mod request;
mod worker;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub use fluree_db_erasure::{Prover, ProverConfig, SubprocessProver, Verifier};
pub use index::{ErasedCommit, ErasureStep};
pub use purge::{PurgeInspection, PurgePlan, PurgeScan};
pub use request::{BranchRemoval, ErasureReceipt, ErasureRequest};
use serde::{Deserialize, Serialize};
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
    pub(crate) config: Option<ErasureConfig>,
    /// A step is being proved.
    in_flight: AtomicBool,
    /// Held to accept or publish an erasure, and to create a branch: the
    /// branches an erasure reaches are the branches as they are then.
    pub(crate) admission: tokio::sync::Mutex<()>,
}

impl ErasureRuntime {
    pub(crate) fn new(index: Arc<index::ErasureIndex>, config: Option<ErasureConfig>) -> Self {
        ErasureRuntime {
            index,
            config,
            in_flight: AtomicBool::new(false),
            admission: tokio::sync::Mutex::new(()),
        }
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ErasureStage {
    /// Commits are being proved.
    Proving,
    /// Every commit is proved; publishing waits for the index to cover them.
    Indexing,
    /// The replacements stand for the originals.
    Published,
    /// A commit cannot be proved (the prover refused it); nothing published.
    Failed,
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
    #[serde(default)]
    pub signatures: SignatureMode,
    pub stage: ErasureStage,
    pub commits: Vec<CommitErasure>,
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
}

impl ErasureJob {
    /// Still to be published.
    pub fn pending(&self) -> bool {
        matches!(self.stage, ErasureStage::Proving | ErasureStage::Indexing)
    }
}
