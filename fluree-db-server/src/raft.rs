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
    state_machine_adapter::{SharedState, StateMachineAdapter},
    storage::{fs::FsRaftStorage, StorageError},
};
use fluree_db_consensus::{NodeId, Raft, RaftConfig, RaftConfigError, RaftFatal, TypeConfig};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

/// Per-node Raft integration. Cheap to clone; everything is `Arc`.
#[derive(Clone)]
pub struct RaftIntegration {
    /// Raft handle. Cloned into the network, admin, and forward
    /// routers; also used by [`RaftCommitter`](fluree_db_consensus::RaftCommitter).
    pub raft: Arc<Raft<TypeConfig>>,
    /// Follower-forward middleware state. Cloned into the
    /// client-facing router's middleware layer.
    pub forwarder: Arc<LeaderForwarder>,
    /// Shared replicated state-machine handle. Hand off to a
    /// `RaftNameService` for follower-side read paths so they observe
    /// committed log state without going through the openraft RPC
    /// surface.
    pub shared_state: SharedState,
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
    ) -> Self {
        let forwarder = Arc::new(LeaderForwarder::new(
            Arc::clone(&raft),
            self_id,
            http_client,
        ));
        Self {
            raft,
            forwarder,
            shared_state,
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

        let log = LogAdapter::new(Arc::clone(&storage));
        let sm = StateMachineAdapter::new(Arc::clone(&storage));
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
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    //! Smoke test for the bootstrap assembler. End-to-end cluster
    //! formation (initialize → leader → state-machine write) is
    //! covered in the multi-node integration test.

    use super::*;
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
}

