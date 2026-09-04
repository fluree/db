//! Attaching a `Fluree` engine to a bootstrapped Raft node.
//!
//! [`RaftIntegration::bootstrap`] gives you the consensus half: the
//! Raft handle, the replicated nameservice, the channels. This module
//! is the other half — wiring an engine to it and starting the
//! per-node tasks — packaged so a process embedding the engine does
//! the same thing `fluree-db-server` does, rather than re-deriving it
//! from the server's source.
//!
//! ## The write seam
//!
//! The engine handle does not know a committer exists: `fluree-db-api`
//! sits below this crate and cannot name the [`Committer`] trait. So
//! `Fluree::transact()` on a Raft-mode engine still writes *locally*,
//! bypassing the queue. Every write the host performs must go through
//! [`EmbeddedRaftNode::committer`] instead. This is the one thing an
//! embedder has to get right; everything else here is wiring.
//!
//! ## What runs where
//!
//! - **Every node**: the worker supervisor (branches are assigned by
//!   rendezvous hash, so a follower routinely stages commits and writes
//!   their blobs to the shared store, ferrying only the head advance to
//!   the leader) and the release task (followers see the same applies
//!   as the leader, so envelope reclamation runs everywhere).
//! - **Leader only**: the idempotency evictor, the liveness monitor, and
//!   whatever the host adds through
//!   [`EmbeddedRaftConfig::extra_leader_tasks`] — the background indexer
//!   lives there, because this crate does not depend on
//!   `fluree-db-indexer`. Omit it and nothing indexes, anywhere.
//!
//! ## Shutdown order is load-bearing
//!
//! [`EmbeddedRaftNode::shutdown`] stops things in a fixed order:
//! workers, then leader tasks, then the Raft core, then the release
//! drain. Workers go first because their final publishes touch the same
//! shared state the leader-only tasks read. The core stops before the
//! release drain because a live core keeps applying entries, and an
//! apply that lands after the drain pushes releases into a closed
//! channel — a leak with no path to GC.

use crate::raft::commit_worker::{
    PublishingChannel, QueuePoisonPublisher, StagingContext, WorkerSupervisor,
};
use crate::raft::eviction_scheduler::EvictionScheduler;
use crate::raft::integration::{
    spawn_leader_watcher, spawn_worker_supervisor, CancellableTaskHandle, RaftIntegration,
};
use crate::raft::liveness_monitor::{LivenessConfig, LivenessMonitor};
use crate::raft::queued_transactor::QueuedTransactor;
use crate::{CachingCommitter, SubmittingCommitter};
use fluree_db_api::{Fluree, IndexConfig};
use fluree_db_core::ContentId;
use fluree_db_nameservice::CommitPublisher;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Bound on how long a wedged Raft core may hold up the rest of
/// teardown — specifically the release drain it is ordered before.
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// Leader-only tasks the host contributes, spawned on each leadership
/// gain and abort-and-awaited on loss. The background indexer goes
/// here.
pub type LeaderTaskFactory = Box<dyn Fn() -> Vec<JoinHandle<()>> + Send + Sync + 'static>;

/// What [`EmbeddedRaftNode::attach`] needs beyond the integration and
/// the engine.
pub struct EmbeddedRaftConfig {
    /// Thresholds the commit workers stage against. Must match the
    /// engine's — take them from
    /// [`Fluree::default_index_config`] rather than constructing
    /// independently, or staging backpressure and novelty backpressure
    /// drift apart.
    pub index_config: IndexConfig,
    pub liveness: LivenessConfig,
    /// See [`LeaderTaskFactory`]. Defaults to none, which means
    /// **nothing indexes**.
    pub extra_leader_tasks: Option<LeaderTaskFactory>,
    /// Per-attempt waiter timeout and attempt count for queued
    /// submissions (`None` = the transactor's defaults, 8 s × 3). The
    /// default is sized for leader transitions on a cluster; a
    /// single-node host whose per-branch queue can run tens of seconds
    /// deep (bulk publishes, sequential upsert chunks) wants a longer
    /// wait, or a submission still queued gets reported stranded and
    /// then commits anyway.
    pub submit_wait: Option<(Duration, usize)>,
}

impl EmbeddedRaftConfig {
    /// Thresholds from the engine, default liveness, no extra tasks.
    pub fn for_engine(fluree: &Fluree) -> Self {
        Self {
            index_config: fluree.default_index_config(),
            liveness: LivenessConfig::default(),
            extra_leader_tasks: None,
            submit_wait: None,
        }
    }

    /// Override the queued transactor's per-attempt wait and attempt
    /// count — see [`Self::submit_wait`].
    pub fn with_submit_wait(mut self, timeout: Duration, max_retries: usize) -> Self {
        self.submit_wait = Some((timeout, max_retries));
        self
    }

    pub fn with_liveness(mut self, liveness: LivenessConfig) -> Self {
        self.liveness = liveness;
        self
    }

    pub fn with_leader_tasks(
        mut self,
        factory: impl Fn() -> Vec<JoinHandle<()>> + Send + Sync + 'static,
    ) -> Self {
        self.extra_leader_tasks = Some(Box::new(factory));
        self
    }
}

/// A Raft node with an engine attached and its tasks running.
pub struct EmbeddedRaftNode {
    pub integration: Arc<RaftIntegration>,
    /// The write seam. Route every write here, never through the
    /// engine handle — see the module docs.
    pub committer: Arc<dyn SubmittingCommitter>,
    worker_supervisor: CancellableTaskHandle,
    leader_watcher: CancellableTaskHandle,
    release_task: Option<(JoinHandle<()>, CancellationToken)>,
}

impl EmbeddedRaftNode {
    /// Wire `fluree` to `integration` and start the per-node tasks.
    ///
    /// `fluree` must have been built with
    /// `FlureeBuilder::with_event_bus(integration.event_bus.clone())`
    /// and `build_client_with_nameservice(NameServiceMode::ReadWrite(
    /// integration.nameservice()))` — one bus, one nameservice — or
    /// the engine's subscribers and the adapter's events are on
    /// different channels and followers never hear about applies.
    pub async fn attach(
        integration: Arc<RaftIntegration>,
        fluree: Arc<Fluree>,
        config: EmbeddedRaftConfig,
    ) -> Self {
        // The adapter reports commit-head advances into the engine's
        // ledger cache synchronously during apply — the lossless path
        // that keeps a cache hit from serving a head the replicated
        // state has moved past. The bus is the lossy reconciler on top.
        if let Some(manager) = fluree.ledger_manager() {
            integration.attach_ledger_manager(Arc::clone(manager));
        }

        // `QueuedTransactor` routes all five `Committer` methods
        // through `EnqueueCommand` plus the per-process waiter and
        // staged-receipt maps; `CachingCommitter` on top dedupes keyed
        // retries before the queue propose.
        let mut queued = QueuedTransactor::new(
            Arc::clone(&integration.raft),
            Arc::clone(&fluree),
            Arc::clone(&integration.waiter_map),
            integration.shared_state.clone(),
        );
        if let Some((timeout, max_retries)) = config.submit_wait {
            queued = queued
                .with_wait_timeout(timeout)
                .with_max_retries(max_retries);
        }
        let committer: Arc<dyn SubmittingCommitter> = Arc::new(CachingCommitter::wrapping(queued));

        let release_task = integration
            .take_release_receiver()
            .await
            .map(|rx| spawn_release_task(Arc::clone(&fluree), rx));

        let nameservice = integration.nameservice();
        let commits: Arc<dyn CommitPublisher> = Arc::clone(&nameservice) as _;
        // Same Arc upcast again so a follower-owned worker can ferry a
        // deterministic poison to the leader rather than spin on
        // `client_write` returning `ForwardToLeader`.
        let poison: Arc<dyn QueuePoisonPublisher> = Arc::clone(&nameservice) as _;
        let worker_supervisor = spawn_worker_supervisor(WorkerSupervisor::new(
            integration.id,
            Arc::clone(&integration.raft),
            integration.shared_state.clone(),
            PublishingChannel {
                commits,
                poison,
                staged_receipts: Arc::clone(&integration.staged_receipts),
            },
            StagingContext {
                fluree: Arc::clone(&fluree),
                index_config: config.index_config,
            },
        ));

        let eviction = EvictionScheduler::new(Arc::clone(&integration.raft));
        let liveness = LivenessMonitor::new(
            Arc::clone(&integration.raft),
            integration.shared_state.clone(),
        )
        .with_config(config.liveness);
        let extra = config.extra_leader_tasks;
        let leader_watcher =
            spawn_leader_watcher(Arc::clone(&integration.raft), integration.id, move || {
                let mut tasks = vec![
                    tokio::spawn(eviction.clone().run()),
                    tokio::spawn(liveness.clone().run()),
                ];
                if let Some(extra) = extra.as_ref() {
                    tasks.extend(extra());
                }
                tasks
            });

        Self {
            integration,
            committer,
            worker_supervisor,
            leader_watcher,
            release_task,
        }
    }

    /// Stop everything, in the order the module docs explain.
    pub async fn shutdown(self) {
        // Workers first: their final publishes go through shared state
        // the leader-only tasks also read. Returns once every
        // per-branch worker has actually stopped.
        self.worker_supervisor.shutdown().await;
        // Then the leader tasks. Resolves only after each in-flight one
        // has been abort-and-awaited, not merely signalled.
        self.leader_watcher.shutdown().await;
        // Then the core, so no further apply can push releases into the
        // channel the drain below is about to close. Bounded: a wedged
        // core must not hold the drain hostage.
        match tokio::time::timeout(SHUTDOWN_GRACE, self.integration.raft.shutdown()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!(error = %e, "raft core shutdown failed"),
            Err(_) => tracing::warn!(
                grace_secs = SHUTDOWN_GRACE.as_secs(),
                "raft core shutdown timed out; proceeding with teardown"
            ),
        }
        // Finally the drain. Cancel, then await: a bare abort would skip
        // the buffered releases and leak them.
        if let Some((task, cancel)) = self.release_task {
            cancel.cancel();
            let _ = task.await;
        }
    }
}

/// The per-node release loop: frees envelope blobs the state machine
/// has finished with, then drains what is still buffered at shutdown.
fn spawn_release_task(
    fluree: Arc<Fluree>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<(String, ContentId)>,
) -> (JoinHandle<()>, CancellationToken) {
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let join = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                () = token.cancelled() => break,
                msg = rx.recv() => match msg {
                    Some((ledger_id, cid)) => release_one(&fluree, &ledger_id, &cid).await,
                    // Sender gone (adapter dropped): nothing more can arrive.
                    None => return,
                },
            }
        }
        // Between the last recv and the cancel, the adapter may have
        // buffered releases for applies that landed moments before
        // stop. Dropping them leaks the envelopes with no path to GC.
        while let Ok((ledger_id, cid)) = rx.try_recv() {
            release_one(&fluree, &ledger_id, &cid).await;
        }
    });
    (join, cancel)
}

/// One release, with bounded retry. `ContentStore::release` is
/// idempotent, so retrying a transient failure is safe — and without
/// it a single blip leaks the blob permanently, since nothing
/// re-proposes a release.
pub async fn release_one(fluree: &Fluree, ledger_id: &str, cid: &ContentId) {
    const ATTEMPTS: u32 = 3;
    const BASE_BACKOFF: Duration = Duration::from_millis(100);

    let store = fluree.content_store(ledger_id);
    for attempt in 1..=ATTEMPTS {
        match store.release(cid).await {
            Ok(()) => return,
            Err(err) if attempt < ATTEMPTS => {
                tracing::debug!(%ledger_id, %cid, attempt, error = %err, "release failed; retrying");
                tokio::time::sleep(BASE_BACKOFF * 2u32.pow(attempt - 1)).await;
            }
            Err(err) => {
                tracing::warn!(
                    %ledger_id, %cid, attempts = ATTEMPTS, error = %err,
                    "failed to release envelope from content store; blob will leak"
                );
            }
        }
    }
}
