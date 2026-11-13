//! Replica coordination for retention.
//!
//! Reads use the journal this node has learned through replication. They
//! never wait for a leader, but cannot serve a locally applied erasure until
//! it is installed. Write-side proving still catches up with the leader.
//! A journal change waits for replicas and reports those that did not apply it.

use std::sync::Arc;
use std::time::Duration;

use crate::{ApiError, Fluree, Result};

/// How long a journal change waits for replicas to acknowledge it.
pub(crate) const ACK_TIMEOUT: Duration = Duration::from_secs(10);

/// A cluster's view of how current each replica is.
#[async_trait::async_trait]
pub trait ReplicaSync: Send + Sync {
    /// Every applied config update and snapshot invalidation is published
    /// on this Fluree instance's nameservice event bus before acknowledgement.
    /// Custom backends default to revalidating journals on reads.
    fn config_events_complete(&self) -> bool {
        false
    }

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
    /// Register the cluster's local installation and write coordination.
    pub fn set_retention_replicas(&self, sync: Arc<dyn ReplicaSync>) {
        // Other members write the journals too.
        self.retention
            .watch
            .set_replicated(sync.config_events_complete());
        let _ = self.retention.replicas.set(sync);
    }

    /// Observe a record delivered by a peer's replication stream, including
    /// its reconnect snapshot. Config revisions invalidate journal caches
    /// independently of commit and index heads.
    pub fn observe_nameservice_record(&self, record: &fluree_db_nameservice::NsRecord) {
        self.retention.watch.remote_record(record);
    }

    /// Refuse reads while a locally applied erasure reaching this ledger
    /// has not been installed. This makes no leader round trip.
    pub async fn ensure_read_ready(&self, ledger_id: &str) -> Result<()> {
        if let Some(replicas) = self.retention.replicas.get() {
            replicas
                .erasures_installed(ledger_id)
                .await
                .map_err(|e| ApiError::ReplicaNotCurrent(format!("{e}; retry")))?;
        }
        Ok(())
    }

    /// Catch up before making a write-side decision about erasure proofs.
    #[cfg(feature = "native")]
    pub(crate) async fn confirm_replica_current(&self, ledger_id: &str) -> Result<()> {
        if let Some(replicas) = self.retention.replicas.get() {
            replicas.sync_reads().await.map_err(|e| {
                ApiError::ReplicaNotCurrent(format!(
                    "this replica cannot confirm it holds {ledger_id}'s current removal journal ({e}); retry"
                ))
            })?;
        }
        self.ensure_read_ready(ledger_id).await
    }

    /// After a journal change: the replicas that have not applied it.
    pub(crate) async fn unacknowledged_replicas(&self, timeout: Duration) -> Vec<String> {
        match self.retention.replicas.get() {
            Some(replicas) => replicas.await_replicas(timeout).await,
            None => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn raw_reads_keep_local_installation_gate() {
        struct AppliedButNotInstalled(std::sync::atomic::AtomicUsize);
        #[async_trait::async_trait]
        impl ReplicaSync for AppliedButNotInstalled {
            async fn sync_reads(&self) -> std::result::Result<(), String> {
                panic!("no leader barrier wanted")
            }
            async fn await_replicas(&self, _: Duration) -> Vec<String> {
                vec![]
            }
            async fn erasures_installed(&self, _: &str) -> std::result::Result<(), String> {
                self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Err("applied erasure is not installed".into())
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let fluree = crate::FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .without_indexing()
            .build()
            .unwrap();
        fluree.create_ledger("review/local-install").await.unwrap();
        let replicas = Arc::new(AppliedButNotInstalled(std::sync::atomic::AtomicUsize::new(
            0,
        )));
        fluree.set_retention_replicas(replicas.clone());
        let result = fluree
            .raw_history_suppressing("review/local-install:main", false)
            .await;
        assert!(
            result.is_err(),
            "locally applied erasures must block serving until installed"
        );
        let query = serde_json::json!({
            "from": "review/local-install:main", "select": "?o",
            "where": {"@id": "http://example.org/a", "http://example.org/email": "?o"}
        });
        assert!(matches!(
            fluree.query_connection(&query).await,
            Err(ApiError::ReplicaNotCurrent(_))
        ));
    }
}
