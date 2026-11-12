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

pub(crate) use engine::carries_retention_input;
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
    /// Removals in the branch's journal. The journal itself, and its
    /// erasures' occurrences and requesters, are audit data
    /// ([`Self::without_audit_data`]).
    pub removals: usize,
}

impl RetentionStatus {
    /// The status as shown to a reader without audit access: what was
    /// removed, where, and who asked for an erasure are left out; counts,
    /// stages and timings stay.
    pub fn without_audit_data(mut self) -> Self {
        if let Some(registration) = &mut self.registration {
            registration.journal.removals.clear();
            #[cfg(feature = "native")]
            for job in &mut registration.erasures {
                job.requester = None;
                job.authorized_commit = None;
                job.commits.clear();
                job.properties.clear();
                job.added.clear();
                job.last_error = None;
            }
        }
        self
    }
}

impl Fluree {
    /// Whether `ledger_id`'s retention suppresses any history: a targeted
    /// removal. Raw commits and transactions of such a ledger hold what its
    /// history no longer serves, so reading them is audit access.
    /// `f:noHistory` alone does not count: it hides nothing.
    pub async fn history_suppressed(&self, ledger_id: &str) -> Result<bool> {
        let ledger_id = fluree_db_core::LedgerId::parse(ledger_id)?.to_string();
        Ok(self.removals_for(&ledger_id).await?.is_some())
    }

    /// The branches whose suppressed history a raw read through `ledger_id`
    /// reaches: its own, and its sources', since its store reads the
    /// commits it shares with them from their folders. With `every_branch`,
    /// every branch of the ledger, dropped ones included: a grant over the
    /// ledger's storage reaches all their folders. Each branch uses this
    /// node's journal and must have installed its locally applied erasures.
    pub async fn raw_history_suppressing(
        &self,
        ledger_id: &str,
        every_branch: bool,
    ) -> Result<Vec<fluree_db_core::LedgerId>> {
        let id = fluree_db_core::LedgerId::parse(ledger_id)?;
        let reached = if every_branch {
            self.nameservice()
                .branch_records(id.name())
                .await?
                .into_iter()
                .map(|record| record.ledger_id)
                .collect()
        } else {
            let mut reached = vec![id.clone()];
            let mut source = self.source_branch_of(&id).await?;
            while let Some(branch) = source {
                let branch = id.with_branch(&branch)?;
                if reached.contains(&branch) {
                    break;
                }
                source = self.source_branch_of(&branch).await?;
                reached.push(branch);
            }
            reached
        };
        let mut suppressing = Vec::new();
        for branch in reached {
            if self.history_suppressed(branch.as_str()).await? {
                suppressing.push(branch);
            }
        }
        Ok(suppressing)
    }

    /// The branch `id` was made from, as this instance last learned it.
    async fn source_branch_of(&self, id: &fluree_db_core::LedgerId) -> Result<Option<String>> {
        let watch = &self.retention.watch;
        if let Some(source) = watch.source_branch(id.as_str()) {
            return Ok(source);
        }
        let token = watch.token();
        let record = self.nameservice().lookup(id.as_str()).await?;
        watch.learn(id.as_str(), record.as_ref(), token);
        Ok(record.and_then(|r| r.source_branch))
    }

    /// [`Self::retention_archive_section`] for the branch `record`
    /// describes, as a storage reader without audit access is given it
    /// (without who asked for each removal), reused while it names the same
    /// configuration revision.
    pub async fn retention_section_for_reader(
        &self,
        record: &fluree_db_nameservice::NsRecord,
    ) -> Result<Option<serde_json::Value>> {
        let ledger_id = record.ledger_id.to_string();
        let key = match record.config_version {
            Some(0) => return Ok(None),
            Some(v) => Some((record.fence, v)),
            None => None,
        };
        let archives = &self.retention.archives;
        if let Some(key) = key {
            if let Some((cached, section)) = archives.lock().get(&ledger_id) {
                if *cached == key {
                    return Ok(section.clone());
                }
            }
        }
        let (config, section) = registry::archived_for_reader_at(
            &self.nameservice_mode,
            self.registry_objects(),
            &ledger_id,
        )
        .await?;
        if let Some(key) = key.filter(|(_, v)| config.as_ref().is_some_and(|c| c.v == *v)) {
            archives.lock().insert(ledger_id, (key, section.clone()));
        }
        Ok(section)
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
            removals: registration
                .as_ref()
                .map_or(0, |r| r.journal.removals.len()),
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
/// The txn-meta a replay of `original` (a rebase) records so its values keep
/// their lifetimes: the original's transaction default, and when the
/// original was written ([`db::LIFETIME_START`]), where it carries retention
/// input or `retention` says the ledger may give its values lifetimes.
pub(crate) fn replayed_lifetimes(
    original: &fluree_db_core::Commit,
    retention: bool,
) -> Vec<fluree_db_core::TxnMetaEntry> {
    let mut entries: Vec<fluree_db_core::TxnMetaEntry> = original
        .txn_meta
        .iter()
        .filter(|e| {
            e.predicate_ns == FLUREE_DB && matches!(&*e.predicate_name, db::TTL | db::EXPIRES_AT)
        })
        .cloned()
        .collect();
    if retention || engine::carries_retention_input(&original.txn_meta) {
        let start = engine::lifetime_start_ms(original)
            .and_then(chrono::DateTime::from_timestamp_millis)
            .map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
        if let Some(start) = start {
            entries.push(fluree_db_core::TxnMetaEntry::new(
                FLUREE_DB,
                db::LIFETIME_START,
                fluree_db_core::TxnMetaValue::TypedLiteral {
                    value: start,
                    dt_ns: fluree_vocab::namespaces::XSD,
                    dt_name: fluree_vocab::xsd_names::DATE_TIME.to_string(),
                },
            ));
        }
    }
    entries
}

pub(crate) fn retention_control_marker() -> fluree_db_core::TxnMetaEntry {
    fluree_db_core::TxnMetaEntry::new(
        FLUREE_DB,
        db::RETENTION_CONTROL,
        fluree_db_core::TxnMetaValue::Boolean(true),
    )
}
