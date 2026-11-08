//! Background retention: TTL expiration from committed policy and metadata.
//!
//! A ledger opts in by declaring `f:retentionDefaults` entries with
//! `f:ttl` in its config graph, by committing per-value TTL
//! instructions (an annotation carrying `f:ttl` or `f:expiresAt`), or
//! by a transaction default in txn-meta. Nothing here runs on a write that
//! carries none of these: the staged-config guard's existing pass over the
//! staged flakes notices TTL instructions (see [`validate`]), and only those
//! writes are validated and marked with `f:retentionControl`.
//!
//! Expiration is a background job owned by the maintenance owner (the
//! catch-up indexer; the Raft leader in a cluster). It derives an
//! expiry-ordered schedule from the commit log, persists it with its
//! discovery watermark, and retracts due assertions through ordinary commits
//! made against a known head. Queries never filter by wall-clock time: an
//! expired value disappears when its retraction commits.

mod engine;
#[cfg(feature = "native")]
pub mod erasure;
pub mod journal;
mod lineage;
mod policy;
pub mod registry;
pub mod removal;
pub mod replicas;
#[cfg(all(test, feature = "native"))]
mod restore_tests;
mod state;
pub(crate) mod suppression;
pub(crate) mod validate;
pub(crate) mod watch;
mod worker;
pub mod writer;

pub use engine::PassReport;
pub use registry::Registration;
pub use removal::{FactSelector, OccurrenceRef, RemovalReceipt, RemovalRequest};
pub use worker::RetentionOptions;
pub(crate) use worker::RetentionRuntime;
pub use writer::{
    outcome_of, RetentionBatch, RetentionWriter, SubmitOutcome, DEFERRED, HEAD_MOVED,
    RETENTION_IDENTITY,
};

use fluree_db_core::Sid;
use serde::Serialize;

use crate::{Fluree, Result};

/// Bounds on one retention pass over one branch.
#[derive(Clone, Debug)]
pub struct RetentionLimits {
    /// Commits discovered per pass; the rest wait for the next.
    pub max_commits_per_pass: usize,
    /// Occurrences expired (and annotations retired) per batch.
    pub max_expire_per_batch: usize,
    /// Retractions per batch.
    pub max_flakes_per_batch: usize,
    /// How often an idle branch's registration records a pass (ms).
    pub heartbeat_ms: i64,
    /// How long a due value left in place (shared by several relationships)
    /// waits before it is checked again (ms).
    pub conflict_backoff_ms: i64,
    /// How often a changing schedule is persisted (ms). What it has not
    /// persisted is rediscovered from the commits after a restart.
    pub checkpoint_ms: i64,
}

impl Default for RetentionLimits {
    fn default() -> Self {
        Self {
            max_commits_per_pass: 500,
            max_expire_per_batch: 1_000,
            max_flakes_per_batch: 10_000,
            heartbeat_ms: 60_000,
            conflict_backoff_ms: 3_600_000,
            checkpoint_ms: 30_000,
        }
    }
}

/// A branch's retention status.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetentionStatus {
    pub ledger_id: String,
    /// The branch is registered: its retention input is processed.
    pub active: bool,
    /// The registration: activation boundary, watermark, pending and
    /// expired counts, next due time, problems.
    pub registration: Option<Registration>,
    /// A retention worker runs in this process. Expiration needs a running
    /// maintenance owner; without one, scheduled values become overdue.
    pub maintenance_running: bool,
    /// Scheduled occurrences already due, when this process holds the
    /// branch's schedule.
    pub overdue: Option<usize>,
}

impl Fluree {
    /// Whether `ledger_id`'s retention suppresses any history: a targeted
    /// removal. Raw commits and transactions of such a ledger hold what its
    /// history no longer serves, so reading them is audit access.
    /// `f:noHistory` alone does not count: it hides nothing.
    pub async fn history_suppressed(&self, ledger_id: &str) -> Result<bool> {
        let ledger_id = fluree_db_core::LedgerId::parse(ledger_id)?.to_string();
        Ok(self
            .suppression_for(&ledger_id)
            .await?
            .is_some_and(|s| !s.journal.removals.is_empty()))
    }

    /// Whether a raw read through `ledger_id` reaches history a removal
    /// journal suppresses: the branch's own, or a source's, since its store
    /// reads the commits it shares with its sources from their folders.
    /// With `every_branch`, any branch of the ledger: a grant over the
    /// ledger's storage reaches every branch's commits.
    pub async fn raw_history_suppressed(
        &self,
        ledger_id: &str,
        every_branch: bool,
    ) -> Result<bool> {
        let id = fluree_db_core::LedgerId::parse(ledger_id)?;
        if self.history_suppressed(id.as_str()).await? {
            return Ok(true);
        }
        let records = self.nameservice().branch_records(id.name()).await?;
        let reached: Vec<&str> = if every_branch {
            records
                .iter()
                .filter(|r| !r.retracted)
                .map(|r| r.ledger_id.as_str())
                .collect()
        } else {
            let mut reached = Vec::new();
            let mut source = records
                .iter()
                .find(|r| r.ledger_id == id)
                .and_then(|r| r.source_branch.as_deref());
            while let Some(branch) = source {
                let Some(record) = records.iter().find(|r| r.branch == branch) else {
                    break;
                };
                if reached.contains(&record.ledger_id.as_str()) {
                    break;
                }
                reached.push(record.ledger_id.as_str());
                source = record.source_branch.as_deref();
            }
            reached
        };
        for branch in reached {
            if branch != id.as_str() && self.history_suppressed(branch).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// What a ledger archive carries of `ledger_id`'s retention (its
    /// activation and removal journal); `None` without a registration.
    pub async fn retention_archive_section(
        &self,
        ledger_id: &str,
    ) -> Result<Option<serde_json::Value>> {
        let ledger_id = fluree_db_core::LedgerId::parse(ledger_id)?.to_string();
        registry::archived(&self.nameservice_mode, self.registry_objects(), &ledger_id).await
    }

    /// The retention status of `ledger_id`.
    pub async fn retention_status(&self, ledger_id: &str) -> Result<RetentionStatus> {
        let ledger_id = fluree_db_core::LedgerId::parse(ledger_id)?.to_string();
        let (_, registration) =
            registry::read(&self.nameservice_mode, self.registry_objects(), &ledger_id).await?;
        let now = worker::now_ms();
        Ok(RetentionStatus {
            active: registration.is_some(),
            overdue: self.retention.cache.peek(&ledger_id, |s| s.overdue(now)),
            registration,
            maintenance_running: self.retention.running(),
            ledger_id,
        })
    }
}
use fluree_vocab::db;
use fluree_vocab::namespaces::FLUREE_DB;

/// `f:ttl`.
pub(crate) fn ttl_sid() -> Sid {
    Sid::new(FLUREE_DB, db::TTL)
}

/// `f:expiresAt`.
pub(crate) fn expires_at_sid() -> Sid {
    Sid::new(FLUREE_DB, db::EXPIRES_AT)
}

/// Whether `p` states a TTL instruction (`f:ttl` or `f:expiresAt`).
pub(crate) fn is_ttl_predicate(p: &Sid) -> bool {
    p.namespace_code == FLUREE_DB && matches!(&*p.name, db::TTL | db::EXPIRES_AT)
}

/// The txn-meta marker on a commit carrying retention control input.
pub(crate) fn retention_control_marker() -> fluree_db_core::TxnMetaEntry {
    fluree_db_core::TxnMetaEntry::new(
        FLUREE_DB,
        db::RETENTION_CONTROL,
        fluree_db_core::TxnMetaValue::Boolean(true),
    )
}
