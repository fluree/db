//! Raft consensus integration for the HTTP server.
//!
//! Gated on the `raft` feature. Bundles the per-node Raft handle, the
//! shared state-machine handle (for follower-side read paths), and
//! the [`LeaderForwarder`] that the client-facing router uses to
//! redirect leader-only requests when this node is a follower.
//!
//! [`RaftIntegration::bootstrap`] is the one-call assembler:
//!
//! - opens the on-disk Raft storage,
//! - wires the log + state-machine adapters,
//! - builds a shared `reqwest::Client` (used by both the inter-node
//!   RPC factory and the follower-forward middleware so they share a
//!   single connection pool),
//! - validates the raft config and calls `Raft::new`,
//! - returns a clone-cheap [`RaftIntegration`].
//!
//! Bootstrap does **not** call `Raft::initialize` — the operator
//! drives initial cluster formation via
//! [`RaftAdmin`](fluree_db_consensus::raft::admin) on a single node
//! (typically `POST /cluster/initialize` against the private
//! listener). Joining an existing cluster is the same routine on the
//! peer side: bootstrap, then the existing leader's operator hits
//! `POST /cluster/add-learner` referencing this node.

use axum::Router;
use fluree_db_consensus::raft::{
    admin as raft_admin,
    forward::LeaderForwarder,
    log_adapter::LogAdapter,
    network::{self as raft_network, HttpRaftNetworkFactory, NetworkConfig},
    staged_receipt::StagedReceiptMap,
    state_machine_adapter::{SharedState, StateMachineAdapter},
    storage::{fs::FsRaftStorage, StorageError},
    waiter::WaiterMap,
};
use fluree_db_consensus::{NodeId, Raft, RaftConfig, RaftConfigError, RaftFatal, TypeConfig};
use fluree_db_nameservice::LedgerEventBus;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::task::JoinHandle;

/// Per-node Raft integration. Cheap to clone; everything is `Arc`.
#[derive(Clone)]
pub struct RaftIntegration {
    /// Raft handle. Cloned into the network, admin, and forward
    /// routers; also used by
    /// [`QueuedTransactor`](fluree_db_consensus::raft::queued_transactor::QueuedTransactor)
    /// and the [`CommitWorker`](fluree_db_consensus::raft::commit_worker::CommitWorker).
    pub raft: Arc<Raft<TypeConfig>>,
    /// This node's id. Cached so callers (notably the leader-aware
    /// indexer watcher) don't have to dip into `raft.metrics()` just
    /// to ask "is this me?"
    pub self_id: NodeId,
    /// Follower-forward middleware state. Cloned into the
    /// client-facing router's middleware layer.
    pub forwarder: Arc<LeaderForwarder>,
    /// Shared replicated state-machine handle. Hand off to a
    /// `RaftNameService` for follower-side read paths so they observe
    /// committed log state without going through the openraft RPC
    /// surface.
    pub shared_state: SharedState,
    /// In-process broadcast bus the state-machine adapter emits
    /// [`fluree_db_nameservice::NameServiceEvent`]s on after each
    /// successful apply.
    pub event_bus: Arc<LedgerEventBus>,
    /// Per-process map the state-machine adapter resolves after each
    /// queue-related apply. Shared with the
    /// [`QueuedTransactor`](fluree_db_consensus::raft::queued_transactor::QueuedTransactor)
    /// so submission-side registrations meet apply-side resolutions.
    pub waiter_map: Arc<WaiterMap>,
    /// Per-process side channel the
    /// [`CommitWorker`](fluree_db_consensus::raft::commit_worker::CommitWorker)
    /// stashes typed [`AppliedReceipt`](fluree_db_consensus::raft::staged_receipt::AppliedReceipt)
    /// values into before proposing `ApplyHead`; the adapter takes
    /// them during waiter resolution so transactors see staged-time
    /// detail instead of falling back to `Minimal`.
    pub staged_receipts: Arc<StagedReceiptMap>,
}

impl RaftIntegration {
    /// Build the integration from a fully-constructed Raft handle.
    /// The HTTP client is shared with the leader-forward middleware
    /// so a single connection pool serves both inter-node RPC and
    /// follower→leader request relays.
    pub fn new(
        raft: Arc<Raft<TypeConfig>>,
        self_id: NodeId,
        http_client: reqwest::Client,
        shared_state: SharedState,
        event_bus: Arc<LedgerEventBus>,
        waiter_map: Arc<WaiterMap>,
        staged_receipts: Arc<StagedReceiptMap>,
    ) -> Self {
        let forwarder = Arc::new(LeaderForwarder::new(
            Arc::clone(&raft),
            self_id,
            http_client,
        ));
        Self {
            raft,
            self_id,
            forwarder,
            shared_state,
            event_bus,
            waiter_map,
            staged_receipts,
        }
    }

    /// One-call cluster bootstrap: open storage, wire adapters, build
    /// the shared HTTP client, validate the raft config, and call
    /// `Raft::new`. Returns a ready integration.
    ///
    /// The returned node is *not* part of any cluster yet. The
    /// operator then calls either:
    ///
    /// - `POST /cluster/initialize` (this node, once, for a fresh
    ///   cluster) — bootstraps single-node membership; the node
    ///   auto-elects itself leader.
    /// - `POST /cluster/add-learner` (the existing leader, naming
    ///   this node) — joins an existing cluster.
    pub async fn bootstrap(config: RaftBootstrapConfig) -> Result<Self, RaftBootstrapError> {
        let storage = Arc::new(FsRaftStorage::open(config.storage_path).await?);

        let event_bus = Arc::new(LedgerEventBus::new(config.event_bus_capacity));
        let waiter_map = Arc::new(WaiterMap::new());
        let staged_receipts = Arc::new(StagedReceiptMap::new());
        let log = LogAdapter::new(Arc::clone(&storage));
        let sm = StateMachineAdapter::new(Arc::clone(&storage))
            .with_event_bus(Arc::clone(&event_bus))
            .with_waiter_map(Arc::clone(&waiter_map))
            .with_staged_receipts(Arc::clone(&staged_receipts));
        let shared_state = sm.shared_state();

        let raft_cfg = Arc::new(config.raft_config.validate()?);

        let http_client = reqwest::Client::builder()
            .connect_timeout(config.network_config.connect_timeout)
            .pool_idle_timeout(Some(Duration::from_secs(90)))
            .build()?;
        let factory =
            HttpRaftNetworkFactory::with_client(http_client.clone(), config.network_config);

        let raft = Raft::new(config.node_id, raft_cfg, factory, log, sm).await?;

        Ok(Self::new(
            Arc::new(raft),
            config.node_id,
            http_client,
            shared_state,
            event_bus,
            waiter_map,
            staged_receipts,
        ))
    }

    /// Router for the private listener — exposes the inter-node Raft
    /// RPC endpoints under `/raft` and the cluster admin endpoints
    /// under `/cluster`. Mount on a VPC-internal listener; this
    /// router carries no auth of its own.
    pub fn private_router(&self) -> Router {
        Router::new()
            .nest("/raft", raft_network::router(Arc::clone(&self.raft)))
            .nest("/cluster", raft_admin::router(Arc::clone(&self.raft)))
    }
}

// ============================================================================
// Bootstrap config + error
// ============================================================================

/// Inputs to [`RaftIntegration::bootstrap`].
#[derive(Clone, Debug)]
pub struct RaftBootstrapConfig {
    /// This node's id in the cluster. Must be unique and stable
    /// across restarts — the openraft log and snapshots are keyed by
    /// it.
    pub node_id: NodeId,
    /// Root directory for the Raft log, vote, and snapshots. Created
    /// if it doesn't exist; subdirectories carry the constituent
    /// stores. Keep this on durable storage (not tmpfs) — losing the
    /// log loses commits.
    pub storage_path: PathBuf,
    /// openraft tuning (election timeouts, heartbeat interval,
    /// cluster name, snapshot policy). Defaults are sane for LAN
    /// deployments; tighten election timeouts for low-latency
    /// datacenter links or loosen them for high-jitter networks.
    pub raft_config: RaftConfig,
    /// Inter-node HTTP transport tuning (per-request and connect
    /// timeouts). Defaults: 500ms RPC, 30s snapshot, 250ms connect.
    pub network_config: NetworkConfig,
    /// Buffer size of the `LedgerEventBus`. Subscribers that fall
    /// this far behind receive `RecvError::Lagged` and fall back to
    /// a catch-up sweep.
    pub event_bus_capacity: usize,
}

impl RaftBootstrapConfig {
    /// Minimum-input constructor: `node_id`, `storage_path`, all
    /// other knobs defaulted. Suitable for first-pass deployments;
    /// tune `raft_config` / `network_config` as workload patterns
    /// demand.
    pub fn new(node_id: NodeId, storage_path: impl Into<PathBuf>) -> Self {
        Self {
            node_id,
            storage_path: storage_path.into(),
            raft_config: RaftConfig::default(),
            network_config: NetworkConfig::default(),
            event_bus_capacity: 1024,
        }
    }
}

/// Failures that can fall out of [`RaftIntegration::bootstrap`].
#[derive(Debug, Error)]
pub enum RaftBootstrapError {
    /// Opening or initializing the on-disk Raft storage tree failed.
    /// Usually a permissions / disk-full / mount-missing issue.
    #[error("opening raft storage: {0}")]
    Storage(#[from] StorageError),
    /// openraft rejected the supplied `RaftConfig` (timeouts that
    /// don't compose, invalid snapshot policy, etc.).
    #[error("validating raft config: {0}")]
    Config(#[from] RaftConfigError),
    /// `reqwest::Client` builder rejected the network config. Very
    /// rare in practice — usually only triggers on unusual TLS /
    /// proxy environments.
    #[error("building HTTP client: {0}")]
    HttpClient(#[from] reqwest::Error),
    /// openraft refused to start. The variant carries the node id
    /// the failure originated from. Usually a fatal storage error
    /// surfaced during initial state load.
    #[error("raft startup: {0}")]
    RaftStart(#[from] RaftFatal<NodeId>),
}

// ============================================================================
// Leader-aware indexer launcher
// ============================================================================

/// Lifecycle transition emitted by [`LeaderTracker::tick`].
///
/// Driver loop translates these into actual `tokio::spawn` /
/// `JoinHandle::abort` calls. The state machine itself is pure so we
/// can unit-test it without touching tokio.
#[derive(Debug, PartialEq, Eq)]
enum LeadershipTransition {
    /// We just won an election — start the indexer.
    Spawn,
    /// We just lost the lead — abort the currently-running indexer.
    Abort,
    /// No change relevant to the indexer lifecycle.
    None,
}

/// Pure state for the leader-aware indexer driver.
///
/// Tracks the local node's "was leader last tick" so the driver only
/// reacts to *transitions*, not to every metrics update. The watcher
/// loop calls [`tick`](Self::tick) with each new "am I currently
/// leader?" answer and routes the returned transition.
#[derive(Debug, Default)]
struct LeaderTracker {
    was_leader: bool,
}

impl LeaderTracker {
    fn tick(&mut self, is_leader: bool) -> LeadershipTransition {
        let transition = match (self.was_leader, is_leader) {
            (false, true) => LeadershipTransition::Spawn,
            (true, false) => LeadershipTransition::Abort,
            _ => LeadershipTransition::None,
        };
        self.was_leader = is_leader;
        transition
    }
}

/// Spawn a background task that watches `raft.metrics()` and drives
/// the lifecycle of every leader-only task: spawn them when this
/// node becomes leader, abort them when it loses the lead.
///
/// `spawn_leader_tasks` is invoked each time leadership is gained
/// and must return the `JoinHandle`s of the freshly-spawned tasks —
/// the driver owns the handles and aborts every one on leadership
/// loss. Callers bundle as many tasks as belong to the leader
/// (indexer, commit-queue worker, periodic idempotency evictor, …)
/// into the returned `Vec`.
///
/// The returned `JoinHandle` is the watcher itself; aborting it
/// drops the metrics receiver and also aborts any currently-running
/// leader tasks. Shutdown of the server should abort this handle
/// alongside its other maintenance tasks.
///
/// `tokio::sync::watch` is a "latest value" channel, so transient
/// flips that don't cross the leader/not-leader boundary from this
/// node's local view (e.g. Leader → Follower → Leader inside one
/// tick) collapse to "still leader" and are correctly treated as
/// no-ops — the leader tasks keep running and the cluster ends up
/// with us back at the lead, no spawn/abort churn.
pub fn spawn_leader_watcher<F>(
    raft: Arc<Raft<TypeConfig>>,
    self_id: NodeId,
    spawn_leader_tasks: F,
) -> JoinHandle<()>
where
    F: Fn() -> Vec<JoinHandle<()>> + Send + 'static,
{
    tokio::spawn(async move {
        let mut metrics = raft.metrics();
        let mut tracker = LeaderTracker::default();
        let mut current_tasks: Vec<JoinHandle<()>> = Vec::new();

        loop {
            let is_leader = metrics.borrow().current_leader == Some(self_id);
            match tracker.tick(is_leader) {
                LeadershipTransition::Spawn => {
                    current_tasks = spawn_leader_tasks();
                }
                LeadershipTransition::Abort => {
                    abort_all(&mut current_tasks);
                }
                LeadershipTransition::None => {}
            }
            if metrics.changed().await.is_err() {
                // Raft handle was dropped — nothing more to observe.
                break;
            }
        }

        // Watcher shutdown: tear down any in-flight leader tasks.
        abort_all(&mut current_tasks);
    })
}

fn abort_all(handles: &mut Vec<JoinHandle<()>>) {
    for handle in handles.drain(..) {
        handle.abort();
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    //! - Smoke test for the bootstrap assembler. End-to-end cluster
    //!   formation (initialize → leader → state-machine write) is
    //!   covered in the multi-node integration test.
    //! - Pure-logic tests for [`LeaderTracker`] — the watcher's
    //!   transition machine, in isolation.
    //! - One end-to-end watcher test that bootstraps a real
    //!   single-node Raft, lets it self-elect, and verifies the
    //!   indexer spawn closure fired exactly once.

    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    #[tokio::test]
    async fn bootstrap_assembles_integration_on_fresh_storage() {
        let dir = TempDir::new().expect("temp dir");
        let cfg = RaftBootstrapConfig::new(1, dir.path().to_path_buf());

        let integration = RaftIntegration::bootstrap(cfg)
            .await
            .expect("bootstrap should succeed on fresh storage");

        // Pre-initialize: no cluster yet, so no leader and the
        // membership snapshot is empty.
        assert_eq!(integration.raft.current_leader().await, None);
        let metrics = integration.raft.metrics().borrow().clone();
        assert_eq!(metrics.membership_config.nodes().count(), 0);

        // Routers assemble without panicking.
        let _ = integration.private_router();
    }

    // ----------------------------------------------------------------
    // LeaderTracker — pure transition logic
    // ----------------------------------------------------------------

    #[test]
    fn leader_tracker_initial_state_is_not_leader() {
        let mut t = LeaderTracker::default();
        // First tick with `is_leader=false` is a no-op — we weren't
        // leader before, still aren't.
        assert_eq!(t.tick(false), LeadershipTransition::None);
    }

    #[test]
    fn leader_tracker_gains_leadership_yields_spawn() {
        let mut t = LeaderTracker::default();
        assert_eq!(t.tick(true), LeadershipTransition::Spawn);
    }

    #[test]
    fn leader_tracker_staying_leader_is_noop() {
        let mut t = LeaderTracker::default();
        assert_eq!(t.tick(true), LeadershipTransition::Spawn);
        assert_eq!(t.tick(true), LeadershipTransition::None);
        assert_eq!(t.tick(true), LeadershipTransition::None);
    }

    #[test]
    fn leader_tracker_losing_leadership_yields_abort() {
        let mut t = LeaderTracker::default();
        assert_eq!(t.tick(true), LeadershipTransition::Spawn);
        assert_eq!(t.tick(false), LeadershipTransition::Abort);
    }

    #[test]
    fn leader_tracker_flap_cycle_emits_paired_transitions() {
        let mut t = LeaderTracker::default();
        // false → true → false → true → false
        assert_eq!(t.tick(true), LeadershipTransition::Spawn);
        assert_eq!(t.tick(false), LeadershipTransition::Abort);
        assert_eq!(t.tick(true), LeadershipTransition::Spawn);
        assert_eq!(t.tick(false), LeadershipTransition::Abort);
    }

    // ----------------------------------------------------------------
    // spawn_leader_watcher — end-to-end against a real single-node Raft
    // ----------------------------------------------------------------

    #[tokio::test]
    async fn watcher_spawns_leader_tasks_when_node_becomes_leader() {
        let dir = TempDir::new().expect("temp dir");
        let cfg = RaftBootstrapConfig::new(1, dir.path().to_path_buf());
        let integration = RaftIntegration::bootstrap(cfg)
            .await
            .expect("bootstrap");

        // Counter incremented every time the watcher invokes the
        // spawn closure. Wrapped in Arc so the closure can keep a
        // handle to it across multiple invocations.
        let spawn_count = Arc::new(AtomicUsize::new(0));
        let count_for_closure = Arc::clone(&spawn_count);

        let watcher = spawn_leader_watcher(
            Arc::clone(&integration.raft),
            1, // self_id
            move || {
                count_for_closure.fetch_add(1, Ordering::SeqCst);
                // Two parked tasks stand in for the indexer + commit
                // worker pair. We're testing the multi-task lifecycle,
                // not the build pipelines.
                vec![
                    tokio::spawn(async {
                        futures::future::pending::<()>().await;
                    }),
                    tokio::spawn(async {
                        futures::future::pending::<()>().await;
                    }),
                ]
            },
        );

        // Bootstrap as single-voter; node 1 will auto-elect on the
        // next election tick.
        let mut members = std::collections::BTreeMap::new();
        members.insert(
            1u64,
            fluree_db_consensus::raft::ClusterNode::default(),
        );
        integration
            .raft
            .initialize(members)
            .await
            .expect("initialize");

        // Wait for the leader transition to land.
        integration
            .raft
            .wait(Some(Duration::from_secs(5)))
            .state(
                fluree_db_consensus::RaftServerState::Leader,
                "leader after self-election",
            )
            .await
            .expect("becomes leader");

        // The metrics watch channel sends an update for the state
        // transition; the watcher should observe it and invoke the
        // spawn closure. Give the watcher task a tick to react.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            spawn_count.load(Ordering::SeqCst),
            1,
            "leader tasks should spawn exactly once on the leader transition"
        );

        // Tear down: aborting the watcher should also abort every
        // current leader task (no leak), and Raft can shut down
        // cleanly.
        watcher.abort();
        let _ = integration.raft.shutdown().await;
    }
}

