//! Removing compacted log entries from disk, not just from the log.
//!
//! openraft compacts by snapshotting and then purging the log behind the
//! snapshot. A purge is logical: the filesystem backend keeps entries in
//! segments and unlinks only a segment whose every entry is purged, so
//! the segment holding the purge point keeps its bytes — on a quiet
//! group that is often the whole log, and it keeps taking appends.
//! [`RaftGroup::compact_log`] closes that segment first, then snapshots
//! and purges through its last entry, and checks that nothing at or below
//! it is still on disk.

use crate::node::{ClusterNode, NodeId};
use crate::runtime::RaftGroup;
use crate::state_machine::AppStateMachine;
use crate::storage::{RaftLogStore, RaftSnapshotStore, RaftStorage, StorageError};
use async_trait::async_trait;
use openraft::metrics::WaitError;
use openraft::RaftMetrics;
use std::time::Duration;
use thiserror::Error;
use tokio::time::Instant;

/// How long one snapshot wait lasts before the trigger is sent again.
/// A build already in flight swallows a trigger and may stop short of
/// the index compaction needs.
const SNAPSHOT_RETRIGGER: Duration = Duration::from_millis(500);

/// Why [`RaftGroup::compact_log`] could not confirm the log was compacted.
#[derive(Debug, Error)]
pub enum CompactionError {
    #[error("raft storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("raft stopped: {0}")]
    Stopped(String),
    #[error("timed out waiting for {0}")]
    Timeout(&'static str),
    /// The steps completed, yet entries at or below `through` are still
    /// held. Running compaction again normally clears it.
    #[error("log entries from index {first} are still on disk after compacting through {through}")]
    Retained { through: u64, first: u64 },
}

/// What compaction needs from a group's storage, with the backend type
/// erased so [`RaftGroup`] stays generic over the application alone.
#[async_trait]
pub(crate) trait CompactionStorage: Send + Sync + 'static {
    async fn seal_log(&self) -> Result<Option<u64>, StorageError>;
    async fn first_retained_index(&self) -> Result<Option<u64>, StorageError>;
    async fn discard_superseded_snapshots(&self) -> Result<(), StorageError>;
}

#[async_trait]
impl<S: RaftStorage> CompactionStorage for S {
    async fn seal_log(&self) -> Result<Option<u64>, StorageError> {
        self.log().seal().await
    }

    async fn first_retained_index(&self) -> Result<Option<u64>, StorageError> {
        self.log().first_retained_index().await
    }

    async fn discard_superseded_snapshots(&self) -> Result<(), StorageError> {
        self.snapshots().discard_superseded().await
    }
}

impl<A: AppStateMachine> RaftGroup<A> {
    /// Remove from this node's disk every log entry up to the current
    /// tail, and every snapshot but the current one, leaving the state
    /// those entries produced in a fresh snapshot. Returns the index
    /// compacted through; `None` when the log held no entries.
    ///
    /// Succeeds only once storage confirms no entry at or below that
    /// index remains. Entries appended meanwhile are kept.
    ///
    /// Each node holds its own log, so a cluster runs this on every
    /// node. On a leader openraft delays the purge while a replication
    /// task still reads those entries, so `timeout` should allow for a
    /// lagging follower; a follower that falls behind the purge catches
    /// up from the snapshot instead. Nothing persists between calls: if
    /// the process stops before this returns, call it again.
    pub async fn compact_log(&self, timeout: Duration) -> Result<Option<u64>, CompactionError> {
        let deadline = Instant::now() + timeout;
        let Some(through) = self.storage.seal_log().await? else {
            self.storage.discard_superseded_snapshots().await?;
            return Ok(None);
        };

        self.wait_until(
            deadline,
            "entries through the sealed tail to apply",
            move |m| {
                m.last_applied
                    .as_ref()
                    .is_some_and(|id| id.index >= through)
            },
        )
        .await?;
        self.snapshot_through(through, deadline).await?;
        self.raft
            .trigger()
            .purge_log(through)
            .await
            .map_err(|e| CompactionError::Stopped(e.to_string()))?;
        self.wait_until(deadline, "the purge to reach the sealed tail", move |m| {
            m.purged.as_ref().is_some_and(|id| id.index >= through)
        })
        .await?;
        self.storage.discard_superseded_snapshots().await?;

        match self.storage.first_retained_index().await? {
            Some(first) if first <= through => Err(CompactionError::Retained { through, first }),
            _ => Ok(Some(through)),
        }
    }

    async fn snapshot_through(
        &self,
        through: u64,
        deadline: Instant,
    ) -> Result<(), CompactionError> {
        let covers = move |m: &RaftMetrics<NodeId, ClusterNode>| {
            m.snapshot.as_ref().is_some_and(|id| id.index >= through)
        };
        loop {
            if covers(&self.raft.metrics().borrow()) {
                return Ok(());
            }
            self.raft
                .trigger()
                .snapshot()
                .await
                .map_err(|e| CompactionError::Stopped(e.to_string()))?;
            let slice = SNAPSHOT_RETRIGGER.min(deadline.saturating_duration_since(Instant::now()));
            match self
                .raft
                .wait(Some(slice))
                .metrics(covers, "snapshot")
                .await
            {
                Ok(_) => return Ok(()),
                Err(WaitError::Timeout(..)) if Instant::now() < deadline => {}
                Err(e) => return Err(wait_error(e, "a snapshot covering the sealed tail")),
            }
        }
    }

    async fn wait_until(
        &self,
        deadline: Instant,
        what: &'static str,
        condition: impl Fn(&RaftMetrics<NodeId, ClusterNode>) -> bool + Send,
    ) -> Result<(), CompactionError> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        self.raft
            .wait(Some(remaining))
            .metrics(condition, what)
            .await
            .map(|_| ())
            .map_err(|e| wait_error(e, what))
    }
}

fn wait_error(e: WaitError, what: &'static str) -> CompactionError {
    match e {
        WaitError::Timeout(..) => CompactionError::Timeout(what),
        WaitError::ShuttingDown => CompactionError::Stopped(e.to_string()),
    }
}
