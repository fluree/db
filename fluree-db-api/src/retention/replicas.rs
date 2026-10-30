//! Replicas and the removal journal.
//!
//! A cluster replica answers a historical read from its own copy of the
//! journal, which can lag the leader's. Before serving a ledger's history it
//! confirms it has applied every change the cluster had committed when the
//! read arrived, and refuses the read when it
//! cannot (it is partitioned, or no leader is known): an unconfirmed replica
//! is out of serving for that history until it catches up. A journal change
//! waits for every replica to apply it and reports the ones that did not.
//!
//! The confirmation covers every ledger, enabled or not: a replica that has
//! not applied a ledger's first retention registration cannot tell it is
//! enabled.

use std::sync::Arc;
use std::time::Duration;

use crate::{ApiError, Fluree, Result};

/// How long a journal change waits for replicas to acknowledge it.
pub(crate) const ACK_TIMEOUT: Duration = Duration::from_secs(10);

/// A cluster's view of how current each replica is.
#[async_trait::async_trait]
pub trait ReplicaSync: Send + Sync {
    /// Wait until this node has applied every change committed before the
    /// call.
    async fn sync_reads(&self) -> std::result::Result<(), String>;

    /// Wait up to `timeout` for every other replica to apply what this
    /// node has applied; the replicas that did not.
    async fn await_replicas(&self, timeout: Duration) -> Vec<String>;

    /// Wait, briefly, until this node has installed every erasure
    /// activation it has applied that reaches `ledger_id`: until then its
    /// cached state and erasure map may still read what was erased.
    async fn erasures_installed(&self, ledger_id: &str) -> std::result::Result<(), String> {
        let _ = ledger_id;
        Ok(())
    }
}

impl Fluree {
    /// Keep this node's journal reads current with the cluster's.
    pub fn set_retention_replicas(&self, sync: Arc<dyn ReplicaSync>) {
        // Other members write the journals too.
        self.retention.watch.clear_sole_writer();
        let _ = self.retention.replicas.set(sync);
    }

    /// On a cluster replica, wait until the local copy of `ledger_id`'s
    /// journal is current. Whether there was anything to confirm.
    pub(crate) async fn confirm_journal_current(&self, ledger_id: &str) -> Result<bool> {
        self.confirm_current(ledger_id, "retry, or read as of the current head")
            .await
    }

    /// On a cluster replica, refuse to hand out `ledger_id`'s raw history
    /// (a transfer, a commit's content) until its journal is current and it
    /// has installed the erasures reaching the ledger: before, it could
    /// hand out what they erase.
    pub async fn confirm_history_current(&self, ledger_id: &str) -> Result<()> {
        self.confirm_current(ledger_id, "retry").await.map(|_| ())
    }

    async fn confirm_current(&self, ledger_id: &str, advice: &str) -> Result<bool> {
        let Some(replicas) = self.retention.replicas.get() else {
            return Ok(false);
        };
        replicas.sync_reads().await.map_err(|e| {
            ApiError::ReplicaNotCurrent(format!(
                "this replica cannot confirm it holds {ledger_id}'s current removal journal \
                 ({e}); {advice}"
            ))
        })?;
        replicas
            .erasures_installed(ledger_id)
            .await
            .map_err(|e| ApiError::ReplicaNotCurrent(format!("{e}; {advice}")))?;
        Ok(true)
    }

    /// After a journal change: the replicas that have not applied it.
    pub(crate) async fn unacknowledged_replicas(&self, timeout: Duration) -> Vec<String> {
        match self.retention.replicas.get() {
            Some(replicas) => replicas.await_replicas(timeout).await,
            None => Vec::new(),
        }
    }
}
