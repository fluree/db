//! Raft consensus integration for the HTTP server.
//!
//! Gated on the `raft` feature. Bundles the per-node Raft handle and
//! the [`LeaderForwarder`] that the client-facing router uses to
//! redirect leader-only requests when this node is a follower.
//!
//! The actual cluster bootstrap (storage, log adapter, state machine,
//! network factory, `Raft::new` call) lives at the embedding edge —
//! see `fluree-db-consensus::raft::{storage, log_adapter,
//! state_machine_adapter, network, admin}` for the building blocks.
//! Once a `Raft<TypeConfig>` handle exists, drop it in here and the
//! server will:
//!
//! - mount [`forward_to_leader`] over leader-only routes
//!   (transact, push, branch admin) so client requests landing on a
//!   follower transparently reach the leader,
//! - expose the inter-node RPC and cluster-admin routers under the
//!   `serve_private` helper for the operator's private listener.
//!
//! Reads stay on every node — no middleware.

use axum::Router;
use fluree_db_consensus::raft::{
    admin as raft_admin, forward::LeaderForwarder, network as raft_network,
};
use fluree_db_consensus::{NodeId, Raft, TypeConfig};
use std::sync::Arc;

/// Per-node Raft integration. Cheap to clone; everything is `Arc`.
#[derive(Clone)]
pub struct RaftIntegration {
    /// Raft handle. Cloned into the network, admin, and forward
    /// routers; also used by [`RaftCommitter`](fluree_db_consensus::RaftCommitter).
    pub raft: Arc<Raft<TypeConfig>>,
    /// Follower-forward middleware state. Cloned into the
    /// client-facing router's middleware layer.
    pub forwarder: Arc<LeaderForwarder>,
}

impl RaftIntegration {
    /// Build the integration from a fully-constructed Raft handle.
    /// The HTTP client is shared with the leader-forward middleware
    /// so a single connection pool serves both inter-node RPC and
    /// follower→leader request relays.
    pub fn new(raft: Arc<Raft<TypeConfig>>, self_id: NodeId, http_client: reqwest::Client) -> Self {
        let forwarder = Arc::new(LeaderForwarder::new(
            Arc::clone(&raft),
            self_id,
            http_client,
        ));
        Self { raft, forwarder }
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
