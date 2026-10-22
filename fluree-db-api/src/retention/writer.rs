//! How an expiration batch becomes a commit.
//!
//! A batch is a set of exact retractions verified against one head, and is
//! committed only if that head is still current: anything committed since
//! may renew, cancel or retract-and-reassert what the batch would expire, so
//! a moved head sends the batch back to be recomputed. Committing goes
//! through the deployment's ordered write path: directly here, or through
//! the consensus committer in a cluster (see [`RetentionBatch::encode`]).

use std::collections::HashMap;
use std::sync::Weak;

use fluree_db_core::commit::codec::{decode_flake_batch, encode_flake_batch};
use fluree_db_core::{Flake, LedgerId, Sid, TxnMetaEntry};
use fluree_db_ledger::StagedLedger;
use fluree_db_transact::{CommitOpts, NamespaceRegistry, TransactError};

use crate::{ApiError, Fluree, Result};

/// The `f:identity` recorded on retention commits.
pub const RETENTION_IDENTITY: &str = "urn:fluree:system:retention";

/// How a consensus path reports a batch whose head moved before it staged.
pub const HEAD_MOVED: &str = "retention batch: head moved";

/// How a consensus path reports a batch it did not stage because the
/// ledger's novelty is near its limit.
pub const DEFERRED: &str = "retention batch: deferred";

/// Retractions to commit against `expected_t`.
#[derive(Clone, Debug)]
pub struct RetentionBatch {
    /// The head `t` the retractions were verified against.
    pub expected_t: i64,
    /// Exact retractions of stored flakes.
    pub retractions: Vec<Flake>,
    /// Occurrences the batch expires.
    pub expired: usize,
    /// Superseded TTL annotations the batch retires.
    pub retired: usize,
    /// Control txn-meta the commit records: an erasure's authorization. A
    /// batch with control entries commits even with no retractions.
    pub control: Vec<TxnMetaEntry>,
}

impl RetentionBatch {
    /// An exact encoding, for a batch that crosses a process boundary.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let internal = |e: &dyn std::fmt::Display| ApiError::internal(e.to_string());
        let flakes = encode_flake_batch(&self.retractions).map_err(|e| internal(&e))?;
        let mut out = Vec::new();
        out.extend_from_slice(&self.expected_t.to_le_bytes());
        out.extend_from_slice(&(self.expired as u64).to_le_bytes());
        out.extend_from_slice(&(self.retired as u64).to_le_bytes());
        out.extend_from_slice(&(flakes.len() as u64).to_le_bytes());
        out.extend(flakes);
        out.extend(serde_json::to_vec(&self.control).map_err(|e| internal(&e))?);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let bad = || ApiError::internal("malformed retention batch");
        if bytes.len() < 32 {
            return Err(bad());
        }
        let word = |i: usize| -> [u8; 8] { bytes[i * 8..i * 8 + 8].try_into().unwrap() };
        let flakes_end = usize::try_from(u64::from_le_bytes(word(3)))
            .ok()
            .and_then(|len| len.checked_add(32))
            .filter(|end| *end <= bytes.len())
            .ok_or_else(bad)?;
        Ok(Self {
            expected_t: i64::from_le_bytes(word(0)),
            expired: u64::from_le_bytes(word(1)) as usize,
            retired: u64::from_le_bytes(word(2)) as usize,
            retractions: decode_flake_batch(&bytes[32..flakes_end])
                .map_err(|e| ApiError::internal(e.to_string()))?,
            control: serde_json::from_slice(&bytes[flakes_end..]).map_err(|_| bad())?,
        })
    }
}

/// What submitting a batch did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmitOutcome {
    /// Committed at `t`.
    Committed { t: i64 },
    /// The head is no longer the one the batch was verified against.
    HeadMoved,
    /// Not submitted: the ledger's novelty is near its limit, and ordinary
    /// writes come first.
    Deferred,
}

/// The ordered write path a retention worker commits through.
#[async_trait::async_trait]
pub trait RetentionWriter: Send + Sync {
    async fn submit(&self, ledger_id: &str, batch: RetentionBatch) -> Result<SubmitOutcome>;
}

/// Commits batches through this process's own `Fluree`: a single node, or
/// the transactor that owns the ledger.
pub struct DirectRetentionWriter {
    pub(crate) fluree: Weak<Fluree>,
}

#[async_trait::async_trait]
impl RetentionWriter for DirectRetentionWriter {
    async fn submit(&self, ledger_id: &str, batch: RetentionBatch) -> Result<SubmitOutcome> {
        let Some(fluree) = self.fluree.upgrade() else {
            return Ok(SubmitOutcome::Deferred);
        };
        outcome_of(fluree.commit_retention_batch(ledger_id, batch).await)
    }
}

/// A batch commit's result as a [`SubmitOutcome`]: a moved head and novelty
/// backpressure are outcomes, not failures.
pub fn outcome_of(result: Result<crate::CommitReceipt>) -> Result<SubmitOutcome> {
    match result {
        Ok(receipt) => Ok(SubmitOutcome::Committed { t: receipt.t }),
        Err(e) if e.is_commit_conflict() => Ok(SubmitOutcome::HeadMoved),
        Err(ApiError::Transact(TransactError::NoveltyAtMax)) => Ok(SubmitOutcome::Deferred),
        Err(e) => Err(e),
    }
}

/// Novelty share above which a batch waits: background retractions must not
/// push foreground writes into backpressure.
const NOVELTY_HEADROOM: f64 = 0.5;

impl Fluree {
    /// Commit a retention batch if the branch head is still the one it was
    /// verified against. A moved head is a commit conflict and a ledger near
    /// its novelty limit is `NoveltyAtMax`; see [`outcome_of`].
    pub async fn commit_retention_batch(
        &self,
        ledger_id: &str,
        batch: RetentionBatch,
    ) -> Result<crate::CommitReceipt> {
        let (write_guard, staged) = self.build_retention_commit(ledger_id, batch).await?;
        match write_guard {
            Some(guard) => self.apply_staged_detached(guard, staged).await,
            None => {
                let id = LedgerId::parse(ledger_id)?;
                let record = self
                    .nameservice()
                    .lookup(&id)
                    .await?
                    .ok_or_else(|| ApiError::NotFound(ledger_id.to_string()))?;
                let content_store = self.content_store(&record.storage_namespace());
                staged
                    .apply(&content_store, self.publisher()?)
                    .await
                    .map(|(receipt, _)| receipt)
                    .map_err(ApiError::from)
            }
        }
    }

    /// Stage a retention batch as a commit without publishing it, holding
    /// the ledger write lock from the head check on: a consensus worker
    /// persists and publishes it through its own path.
    pub async fn build_retention_commit(
        &self,
        ledger_id: &str,
        batch: RetentionBatch,
    ) -> Result<(
        Option<crate::LedgerWriteGuard>,
        fluree_db_transact::StagedCommit,
    )> {
        let id = LedgerId::parse(ledger_id)?;
        let record = self
            .nameservice()
            .lookup(&id)
            .await?
            .ok_or_else(|| ApiError::NotFound(ledger_id.to_string()))?;
        let store = self
            .content_store_for_record_or_id(Some(&record), &record.storage_namespace())
            .await?;
        let (write_guard, state) = self.lock_or_load(&id, store.clone(), record).await?;
        if state.t() != batch.expected_t {
            return Err(ApiError::Transact(TransactError::CommitConflict {
                expected_t: batch.expected_t,
                head_t: state.t(),
            }));
        }
        let max = self.index_config.reindex_max_bytes;
        if max > 0 && state.novelty_size() as f64 >= max as f64 * NOVELTY_HEADROOM {
            return Err(ApiError::Transact(TransactError::NoveltyAtMax));
        }
        let staged = self
            .stage_retention_commit(state, batch.retractions, batch.control, store.as_ref())
            .await?;
        Ok((write_guard, staged))
    }

    /// Stage `retractions` against `state` as a retention commit, recording
    /// `control` in its txn-meta.
    pub(crate) async fn stage_retention_commit(
        &self,
        mut state: fluree_db_ledger::LedgerState,
        retractions: Vec<Flake>,
        control: Vec<TxnMetaEntry>,
        store: &dyn fluree_db_core::ContentStore,
    ) -> Result<fluree_db_transact::StagedCommit> {
        if state.head_temporal.is_none() && state.head_commit_id.is_some() {
            state
                .ensure_head_temporal(store)
                .await
                .map_err(TransactError::from)?;
        }
        let reverse_graph: HashMap<Sid, fluree_db_core::GraphId> =
            state.snapshot.build_reverse_graph()?;
        let control_only = retractions.is_empty() && !control.is_empty();
        let view = StagedLedger::new(state, retractions, &reverse_graph)?;
        let ns_registry = NamespaceRegistry::from_db(view.db());
        let mut txn_meta = vec![super::retention_control_marker()];
        txn_meta.extend(control);
        let mut commit_opts = CommitOpts::default()
            .identity(RETENTION_IDENTITY)
            .with_txn_meta(txn_meta);
        if control_only {
            commit_opts = commit_opts.as_control();
        }
        let expected_head_ref =
            view.base()
                .head_commit_id
                .as_ref()
                .map(|cid| fluree_db_nameservice::RefValue {
                    id: Some(cid.clone()),
                    t: view.base().t(),
                });
        Ok(fluree_db_transact::build_commit(
            view,
            ns_registry,
            expected_head_ref,
            None,
            &self.index_config,
            commit_opts.with_content_hash(self.content_hash()),
        )
        .await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::FlakeValue;

    #[test]
    fn batches_round_trip_exactly() {
        let batch = RetentionBatch {
            expected_t: 42,
            retractions: vec![Flake::new(
                Sid::new(100, "s"),
                Sid::new(100, "p"),
                FlakeValue::Double(2.0),
                Sid::new(2, "double"),
                42,
                false,
                None,
            )],
            expired: 1,
            retired: 0,
            control: vec![TxnMetaEntry::new(
                7,
                "erasureRequest",
                fluree_db_core::TxnMetaValue::String("erasure-1".into()),
            )],
        };
        let back = RetentionBatch::decode(&batch.encode().unwrap()).unwrap();
        assert_eq!(back.expected_t, 42);
        assert_eq!(back.expired, 1);
        assert_eq!(back.retractions.len(), 1);
        assert!(matches!(back.retractions[0].o, FlakeValue::Double(_)));
        assert_eq!(back.control, batch.control);
        let encoded = batch.encode().unwrap();
        assert!(RetentionBatch::decode(&encoded[..encoded.len() - 1]).is_err());
    }
}
