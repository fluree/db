//! Bringing a Raft group up, and running the tasks only its leader
//! should run.
//!
//! A process hosts N groups, each an independent `openraft::Raft` with
//! its own log, elections, and snapshots. [`RaftGroup::bootstrap`] does
//! the assembly for one of them; the rest of this module is the
//! leader-only task lifecycle, which every group needs and which is
//! easy to get subtly wrong (see [`spawn_leader_watcher`]).

use crate::admin::{self, RaftAdmin};
use crate::config::FlureeRaftConfig;
use crate::group::GroupId;
use crate::log_adapter::LogAdapter;
use crate::network::{self, HttpClientConfig, RaftTransportConfig};
use crate::node::NodeId;
use crate::state_machine::{
    AppStateMachine, SharedState, StateMachineAdapter, StateMachineObserver,
};
use crate::storage::RaftStorage;
use openraft::{Config as RaftConfig, Raft};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// How long leader-only tasks get to wind down on their own before
/// they are aborted.
///
/// Generous enough for a task to finish an in-flight propose, short
/// enough that a leader flap doesn't stall the watcher.
pub const DEFAULT_LEADER_TASK_GRACE: Duration = Duration::from_secs(5);

// ============================================================================
// Configuration
// ============================================================================

/// Everything needed to stand up one group in this process.
#[derive(Clone, Debug)]
pub struct RaftGroupConfig {
    /// Names this group's storage subtree and route prefix.
    pub group_id: GroupId,
    /// This node's id. Must be stable across restarts — the log and
    /// snapshots on disk belong to it.
    pub node_id: NodeId,
    /// Parent directory for group storage. The group's own root is
    /// `<storage_root>/<group_id>/`.
    pub storage_root: PathBuf,
    /// openraft tuning. [`default_raft_config`] derives a safe one from
    /// the transport settings.
    pub raft: RaftConfig,
    /// Inter-node HTTP transport tuning: timeouts and body caps applied
    /// per request, so these may differ between co-hosted groups.
    pub transport: RaftTransportConfig,
    /// Settings for the HTTP client this group builds *if it builds its
    /// own*. Ignored when a client is supplied through
    /// [`RaftGroup::bootstrap_with_client`], because those settings are
    /// baked in at client construction and cannot vary per group — which
    /// is exactly why they live on their own struct.
    pub http_client: HttpClientConfig,
}

impl RaftGroupConfig {
    /// Build a config with [`default_raft_config`] applied.
    pub fn new(group_id: GroupId, node_id: NodeId, storage_root: impl Into<PathBuf>) -> Self {
        let transport = RaftTransportConfig::default();
        Self {
            group_id,
            node_id,
            storage_root: storage_root.into(),
            raft: default_raft_config(&transport),
            transport,
            http_client: HttpClientConfig::default(),
        }
    }

    /// This group's own storage directory.
    pub fn group_storage_root(&self) -> PathBuf {
        self.group_id.storage_root(&self.storage_root)
    }
}

/// Election timeouts derived from the transport's RPC timeout, rather
/// than openraft's stock 150–300 ms.
///
/// openraft's defaults assume sub-millisecond, always-reachable peers.
/// Ours don't hold: `rpc_timeout` defaults to 500 ms, and a *dead but
/// not yet evicted* voter costs `connect_timeout` on every vote round.
/// With a 150–300 ms election window a candidate re-elects — bumping
/// its term — before its own vote RPCs from the previous round resolve,
/// so every vote lands on a stale term and no quorum ever forms. On a
/// failover the survivors then livelock: all climb to the same term
/// with no leader, and never converge.
///
/// The fix is the invariant `election_timeout_min > rpc_timeout`: a
/// candidate must give the live quorum time to vote before abandoning
/// the term. Deriving the window from `rpc_timeout` keeps the invariant
/// intact if transport defaults are ever retuned. 2× `rpc_timeout` with
/// a 750 ms floor for `min`, and a 2× spread to `max` so timers
/// desynchronize. Cost is a slightly slower failover (~1–2 s at the
/// defaults).
pub fn default_raft_config(transport: &RaftTransportConfig) -> RaftConfig {
    let rpc_ms = transport.rpc_timeout.as_millis() as u64;
    let election_timeout_min = rpc_ms.saturating_mul(2).max(750);
    RaftConfig {
        election_timeout_min,
        election_timeout_max: election_timeout_min.saturating_mul(2),
        ..RaftConfig::default()
    }
}

/// Warn if `election_timeout_min <= rpc_timeout` — the livelock
/// condition described on [`default_raft_config`].
///
/// openraft's own `validate()` cannot catch this: it does not know the
/// transport timeout. This is the one place that holds both configs.
/// Warns rather than rejects, so an operator who has deliberately tuned
/// for a fast link is not hard-blocked; the shipped defaults are safe.
///
/// Returns whether the invariant holds, so a caller that wants to be
/// stricter than a log line can be.
pub fn check_election_timeout(raft: &RaftConfig, transport: &RaftTransportConfig) -> bool {
    let rpc_timeout_ms = transport.rpc_timeout.as_millis() as u64;
    if raft.election_timeout_min <= rpc_timeout_ms {
        tracing::warn!(
            election_timeout_min = raft.election_timeout_min,
            rpc_timeout_ms,
            "election_timeout_min <= rpc_timeout: candidates may re-elect before their vote \
             RPCs resolve, risking election livelock on leader failover; set \
             election_timeout_min comfortably above rpc_timeout"
        );
        return false;
    }
    true
}

// ============================================================================
// Bootstrap
// ============================================================================

/// Why a group failed to come up.
#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error("raft storage error: {0}")]
    Storage(#[from] crate::storage::StorageError),
    #[error("state machine could not be opened: {0}")]
    StateMachine(#[from] openraft::StorageError<NodeId>),
    #[error("openraft rejected the supplied config: {0}")]
    Config(#[from] openraft::ConfigError),
    #[error("HTTP client could not be built: {0}")]
    HttpClient(#[from] reqwest::Error),
    #[error("openraft refused to start node {node_id}: {source}")]
    Raft {
        node_id: NodeId,
        #[source]
        source: openraft::error::Fatal<NodeId>,
    },
}

/// One running Raft group.
pub struct RaftGroup<A: AppStateMachine> {
    pub group_id: GroupId,
    pub node_id: NodeId,
    pub raft: Arc<Raft<A::Config>>,
    /// Local read model — advisory. See the `state_machine` module docs.
    pub state: SharedState<A>,
    pub admin: RaftAdmin<A::Config>,
    /// Shared across every group in the process; hand the same one to
    /// [`crate::forward::LeaderForwarder`].
    pub client: reqwest::Client,
    transport: RaftTransportConfig,
}

impl<A: AppStateMachine> RaftGroup<A> {
    /// Assemble a group over an already-opened storage backend, building
    /// a dedicated HTTP client from
    /// [`RaftGroupConfig::http_client`](RaftGroupConfig).
    ///
    /// A process hosting several groups should build one client and use
    /// [`Self::bootstrap_with_client`] instead, so the groups share a
    /// connection pool rather than opening one each to every peer.
    ///
    /// The returned node belongs to no cluster yet. An operator then
    /// either initializes it (one node, once, forming a fresh cluster —
    /// see [`RaftAdmin::initialize`]) or adds it as a learner from an
    /// existing leader.
    ///
    /// Single-voter is a first-class starting point, not a degenerate
    /// one: a group that begins as a 1-voter cluster grows purely by
    /// add-learner plus change-membership, so scaling out is never a
    /// data migration.
    pub async fn bootstrap<O, S>(
        config: RaftGroupConfig,
        storage: Arc<S>,
        observer: O,
    ) -> Result<Self, BootstrapError>
    where
        O: StateMachineObserver<A>,
        S: RaftStorage,
    {
        Self::bootstrap_with_client(config, storage, observer, None).await
    }

    /// As [`Self::bootstrap`], reusing an existing HTTP client.
    ///
    /// The client's own settings — connect and pool-idle timeouts —
    /// come from whatever [`HttpClientConfig`] built it;
    /// [`RaftGroupConfig::http_client`](RaftGroupConfig) is unused on
    /// this path. Per-request timeouts and body caps still come from
    /// this group's [`RaftTransportConfig`].
    pub async fn bootstrap_with_client<O, S>(
        config: RaftGroupConfig,
        storage: Arc<S>,
        observer: O,
        client: Option<reqwest::Client>,
    ) -> Result<Self, BootstrapError>
    where
        O: StateMachineObserver<A>,
        S: RaftStorage,
    {
        let raft_config = Arc::new(config.raft.validate()?);
        check_election_timeout(&raft_config, &config.transport);

        let log = LogAdapter::new(Arc::clone(&storage));
        // `open` restores from the current snapshot if there is one.
        // Required on restart: without it the adapter boots at
        // last_applied = None and openraft replays from index 0, but
        // the default snapshot policy has purged those entries, so
        // committed state would be silently lost.
        let sm: StateMachineAdapter<A, O, S> =
            StateMachineAdapter::open(Arc::clone(&storage), observer).await?;
        let state = sm.shared_state();

        let client = match client {
            Some(c) => c,
            None => network::build_client(&config.http_client)?,
        };
        let factory =
            network::HttpRaftNetworkFactory::with_client(client.clone(), config.transport.clone());

        let raft = Raft::new(config.node_id, raft_config, factory, log, sm)
            .await
            .map_err(|source| BootstrapError::Raft {
                node_id: config.node_id,
                source,
            })?;
        let raft = Arc::new(raft);

        Ok(Self {
            group_id: config.group_id,
            node_id: config.node_id,
            admin: RaftAdmin::new(Arc::clone(&raft)),
            raft,
            state,
            client,
            transport: config.transport,
        })
    }

    /// Relative router for this group's inter-node RPCs.
    ///
    /// Peer-trusted and unauthenticated by design — mount it on a
    /// private listener. Nest it where this group's peers expect it:
    /// at the historical `/raft` for a group whose `ClusterNode`
    /// addresses already say so, or at `/raft/<group_id>` for a new
    /// one.
    pub fn raft_router(&self) -> axum::Router {
        network::router(Arc::clone(&self.raft), &self.transport)
    }

    /// Relative router for membership administration.
    ///
    /// Carries no auth of its own: gating membership changes against
    /// operator credentials is the host's job, and the host is the
    /// only layer that knows what its credentials are.
    pub fn admin_router(&self) -> axum::Router {
        admin::router(Arc::clone(&self.raft))
    }

    /// Is this node currently this group's leader?
    ///
    /// Reads the local metrics snapshot, so it is a local opinion, not
    /// a linearizable answer.
    pub fn is_leader(&self) -> bool {
        self.raft.metrics().borrow().current_leader == Some(self.node_id)
    }
}

// ============================================================================
// Leader-only tasks
// ============================================================================

/// A change in this node's leadership for one group.
#[derive(Debug, PartialEq, Eq)]
enum LeadershipTransition {
    /// Just won — start the leader-only tasks.
    Start,
    /// Just lost — stop them.
    Stop,
    /// Nothing relevant changed.
    None,
}

/// Tracks "was leader last tick" so the watcher reacts to transitions
/// rather than to every metrics update. Pure, so it is testable without
/// a runtime.
#[derive(Debug, Default)]
struct LeaderTracker {
    was_leader: bool,
}

impl LeaderTracker {
    fn tick(&mut self, is_leader: bool) -> LeadershipTransition {
        let transition = match (self.was_leader, is_leader) {
            (false, true) => LeadershipTransition::Start,
            (true, false) => LeadershipTransition::Stop,
            _ => LeadershipTransition::None,
        };
        self.was_leader = is_leader;
        transition
    }
}

/// Handle to a cancellable background task.
///
/// The task body receives a [`CancellationToken`] and is expected to
/// return once it fires. Shut down through [`Self::shutdown`], which
/// returns only after the task has actually stopped.
pub struct CancellableTask {
    join: JoinHandle<()>,
    cancel: CancellationToken,
}

impl CancellableTask {
    /// Cancel and wait. The task's loop exits naturally and runs
    /// whatever cleanup its body owns.
    ///
    /// Aborting a `JoinHandle` directly instead would drop the future
    /// mid-await and skip that cleanup entirely.
    pub async fn shutdown(self) {
        self.cancel.cancel();
        // A `JoinError` here means the task panicked; shutdown has no
        // caller to surface that to, so log and move on.
        if let Err(e) = self.join.await {
            if e.is_panic() {
                tracing::error!(error = %e, "background task panicked before shutdown");
            }
        }
    }
}

/// Spawn a task whose body receives a cancellation token.
pub fn spawn_cancellable<F, Fut>(body: F) -> CancellableTask
where
    F: FnOnce(CancellationToken) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let cancel = CancellationToken::new();
    let join = tokio::spawn(body(cancel.clone()));
    CancellableTask { join, cancel }
}

/// Watch a group's leadership and run `spawn_leader_tasks` only while
/// this node holds it.
///
/// `spawn_leader_tasks` is called on each leadership gain with a fresh
/// [`CancellationToken`], and must return the handles of the tasks it
/// spawned. On leadership loss the token is cancelled, the tasks are
/// given `grace` to wind down, and any straggler is aborted — then the
/// watcher waits for all of them before it will start a new set. That
/// wait is the point: without it a rapid leader flap starts a second
/// generation of tasks while the first is still running, and two
/// "leader-only" tasks propose concurrently.
///
/// `tokio::sync::watch` keeps only the latest value, so a flip that
/// does not cross the leader/not-leader boundary within one tick
/// collapses to "no change" and correctly causes no churn.
pub fn spawn_leader_watcher<C, F>(
    raft: Arc<Raft<C>>,
    node_id: NodeId,
    grace: Duration,
    spawn_leader_tasks: F,
) -> CancellableTask
where
    C: FlureeRaftConfig,
    F: Fn(CancellationToken) -> Vec<JoinHandle<()>> + Send + 'static,
{
    spawn_cancellable(move |cancel| async move {
        let mut metrics = raft.metrics();
        let mut tracker = LeaderTracker::default();
        let mut running: Option<(CancellationToken, Vec<JoinHandle<()>>)> = None;

        loop {
            let is_leader = metrics.borrow().current_leader == Some(node_id);
            match tracker.tick(is_leader) {
                LeadershipTransition::Start => {
                    let token = CancellationToken::new();
                    let handles = spawn_leader_tasks(token.clone());
                    running = Some((token, handles));
                }
                LeadershipTransition::Stop => {
                    if let Some((token, handles)) = running.take() {
                        stop_tasks(token, handles, grace).await;
                    }
                }
                LeadershipTransition::None => {}
            }
            tokio::select! {
                changed = metrics.changed() => {
                    if changed.is_err() {
                        // The Raft handle was dropped; nothing left to watch.
                        break;
                    }
                }
                () = cancel.cancelled() => break,
            }
        }

        if let Some((token, handles)) = running.take() {
            stop_tasks(token, handles, grace).await;
        }
    })
}

/// Cancel, wait up to `grace`, then abort whatever is still running.
///
/// Returns only once every handle has resolved, so the caller can rely
/// on "these tasks are stopped."
async fn stop_tasks(token: CancellationToken, handles: Vec<JoinHandle<()>>, grace: Duration) {
    token.cancel();

    // One shared deadline across all tasks, not one each: `grace` is a
    // bound on the whole stop, so a set of slow tasks can't multiply it.
    let deadline = tokio::time::Instant::now() + grace;
    let mut stragglers = Vec::new();
    for mut handle in handles {
        if tokio::time::timeout_at(deadline, &mut handle)
            .await
            .is_err()
        {
            stragglers.push(handle);
        }
    }

    // Whatever ignored the token gets aborted — then awaited, so the
    // caller's "these are stopped" guarantee actually holds.
    for handle in &stragglers {
        handle.abort();
    }
    if !stragglers.is_empty() {
        tracing::warn!(
            count = stragglers.len(),
            grace_ms = grace.as_millis() as u64,
            "leader tasks did not stop within the grace period; aborting"
        );
    }
    for handle in stragglers {
        let _ = handle.await;
    }
}

/// Run `tick` every `interval` until cancelled.
///
/// The shape every leader-only periodic proposer wants: a scheduler
/// that sleeps, does one unit of work, and exits promptly on
/// cancellation rather than at the end of its next sleep. The first
/// tick happens after one interval, not immediately, so a leader flap
/// doesn't produce a burst of proposals.
pub async fn run_periodic<F, Fut>(interval: Duration, cancel: CancellationToken, mut tick: F)
where
    F: FnMut() -> Fut + Send,
    Fut: std::future::Future<Output = ()> + Send,
{
    loop {
        tokio::select! {
            () = tokio::time::sleep(interval) => tick().await,
            () = cancel.cancelled() => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracker_only_reports_transitions() {
        let mut t = LeaderTracker::default();
        assert_eq!(t.tick(false), LeadershipTransition::None);
        assert_eq!(t.tick(true), LeadershipTransition::Start);
        assert_eq!(t.tick(true), LeadershipTransition::None);
        assert_eq!(t.tick(false), LeadershipTransition::Stop);
        assert_eq!(t.tick(false), LeadershipTransition::None);
        assert_eq!(t.tick(true), LeadershipTransition::Start);
    }

    #[test]
    fn default_raft_config_keeps_the_livelock_invariant() {
        let transport = RaftTransportConfig::default();
        let cfg = default_raft_config(&transport);
        assert!(
            check_election_timeout(&cfg, &transport),
            "shipped defaults must satisfy election_timeout_min > rpc_timeout",
        );
        assert!(cfg.election_timeout_max > cfg.election_timeout_min);
    }

    /// The invariant has to survive someone retuning the transport, not
    /// just hold at the shipped numbers.
    #[test]
    fn default_raft_config_tracks_a_retuned_rpc_timeout() {
        let transport = RaftTransportConfig {
            rpc_timeout: Duration::from_secs(3),
            ..RaftTransportConfig::default()
        };
        let cfg = default_raft_config(&transport);
        assert!(check_election_timeout(&cfg, &transport));
        assert_eq!(cfg.election_timeout_min, 6000);
    }

    #[test]
    fn check_election_timeout_flags_a_bad_override() {
        let transport = RaftTransportConfig::default();
        let bad = RaftConfig {
            election_timeout_min: 100,
            election_timeout_max: 200,
            ..RaftConfig::default()
        };
        assert!(
            !check_election_timeout(&bad, &transport),
            "an election window inside the RPC timeout must be reported",
        );
    }

    /// openraft's stock defaults are exactly the trap this exists to
    /// catch — pin that so nobody "simplifies" back to them.
    #[test]
    fn openraft_stock_defaults_would_violate_the_invariant() {
        assert!(!check_election_timeout(
            &RaftConfig::default(),
            &RaftTransportConfig::default()
        ));
    }
}
