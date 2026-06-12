//! Cluster bootstrap + membership-change admin surface.
//!
//! Wraps openraft's lifecycle methods (`initialize`, `add_learner`,
//! `change_membership`, `current_leader`, `metrics`) behind a typed
//! [`RaftAdmin`] surface and a matching [`router`] of HTTP endpoints.
//!
//! # Bootstrap flow (3-node cluster)
//!
//! 1. Start the raft processes on all 3 nodes (each has its own
//!    storage, network factory, and state machine adapter).
//! 2. On node 1: [`RaftAdmin::initialize`] with `{1: addr1}` — node 1
//!    becomes a single-node cluster (auto-leader).
//! 3. On node 1: [`RaftAdmin::add_learner`] for node 2 (blocking),
//!    then for node 3 (blocking). The leader replicates the existing
//!    log to each new learner.
//! 4. On node 1: [`RaftAdmin::change_membership`] with `{1, 2, 3}` —
//!    promotes nodes 2 and 3 to voters.
//!
//! After this, the cluster has three voting members. Subsequent
//! node additions / removals reuse `add_learner` →
//! `change_membership`.
//!
//! # Where this lives in deployment
//!
//! The admin endpoints share the same private listener as the
//! inter-node RPC router (see [`super::network`]); operators reach
//! them via SSH tunnel or a control-plane mechanism, never through
//! the public load balancer. v1 trusts the network boundary (VPC
//! ACL); embedders can layer their own auth on top of the returned
//! [`axum::Router`].

use crate::raft::{ClusterNode, NodeId, TypeConfig};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use openraft::{Raft, RaftMetrics};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use thiserror::Error;

// ============================================================================
// Request / response types
// ============================================================================

/// `POST /initialize` body. Used exactly once, on one node, when
/// the cluster is first formed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InitializeRequest {
    /// Initial cluster members. Typically a single-node bootstrap
    /// (`{self_id: self_addrs}`) followed by `add_learner` calls;
    /// a multi-node initialize is supported but less common.
    pub members: BTreeMap<NodeId, NodeAddrs>,
}

/// Address pair for a peer node — both the inter-node Raft RPC URL
/// and the client-facing URL. Stored on the [`ClusterNode`] entries
/// the Raft state machine replicates, so adding a peer at runtime
/// makes both URLs immediately resolvable on every other node.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeAddrs {
    /// Base URL of the peer's Raft RPC endpoint
    /// (e.g. `"http://node-2:9090/raft"`). See the network module
    /// for the exact path layout.
    pub raft_addr: String,
    /// Base URL of the peer's client-facing endpoint
    /// (e.g. `"http://node-2:8080"`). The follower-forward
    /// middleware uses this to redirect leader-only client requests
    /// to the leader.
    pub client_addr: String,
}

impl From<NodeAddrs> for ClusterNode {
    fn from(n: NodeAddrs) -> Self {
        ClusterNode {
            raft_addr: n.raft_addr,
            client_addr: n.client_addr,
        }
    }
}

/// `POST /add-learner` body. Adds a non-voting peer that replicates
/// the log but doesn't participate in elections — the standard
/// prelude to promoting via `change_membership`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddLearnerRequest {
    pub node_id: NodeId,
    #[serde(flatten)]
    pub addrs: NodeAddrs,
    /// Block until the learner catches up to the leader's log before
    /// returning. `true` is the safe default for orchestration
    /// scripts that immediately follow up with `change_membership`.
    pub blocking: bool,
}

/// `POST /change-membership` body. Promotes (or demotes / removes)
/// nodes from the voting set. openraft drives the two-phase joint
/// consensus underneath.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeMembershipRequest {
    /// New set of voter ids.
    pub members: BTreeSet<NodeId>,
    /// If `true`, voters dropped from `members` become learners
    /// (kept in the cluster, non-voting). If `false`, they're
    /// removed entirely.
    pub retain: bool,
}

/// `GET /status` response. Snapshot of cluster state the operator
/// can poll to verify bootstrap or membership change progress.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterStatus {
    /// Currently-known leader id. `None` during election or before
    /// a leader has emerged.
    pub current_leader: Option<NodeId>,
    /// Current Raft term.
    pub current_term: u64,
    /// Last applied log index.
    pub last_applied_index: Option<u64>,
    /// Voting members.
    pub voters: BTreeSet<NodeId>,
    /// Non-voting learners.
    pub learners: BTreeSet<NodeId>,
}

// ============================================================================
// Errors
// ============================================================================

/// Failures from admin operations. Errors from openraft are
/// stringified at the boundary so the admin surface has a stable
/// shape across openraft version bumps.
#[derive(Debug, Error)]
pub enum AdminError {
    /// The cluster has already been bootstrapped (or this node has
    /// at least one log entry). `initialize` is a once-only
    /// operation; subsequent admin work uses `add_learner` /
    /// `change_membership`.
    #[error("already initialized")]
    AlreadyInitialized,
    /// Operation requires the leader. Carries a hint at where the
    /// leader currently is (if known).
    #[error("not leader{}", match leader { Some(id) => format!(" — try node {id}"), None => String::new() })]
    NotLeader { leader: Option<NodeId> },
    /// Raft core failure (storage error, panic, shutdown). The
    /// process is generally not recoverable.
    #[error("fatal raft error: {0}")]
    Fatal(String),
    /// Catch-all for membership/state errors openraft surfaces.
    #[error("admin error: {0}")]
    Other(String),
}

// ============================================================================
// Admin handle
// ============================================================================

/// Admin surface for cluster bootstrap and membership changes.
///
/// Cheap to construct from the same `Arc<Raft<TypeConfig>>` that
/// [`super::RaftCommitter`] holds. Multiple instances can share the
/// underlying handle safely.
#[derive(Clone)]
pub struct RaftAdmin {
    raft: Arc<Raft<TypeConfig>>,
}

impl RaftAdmin {
    pub fn new(raft: Arc<Raft<TypeConfig>>) -> Self {
        Self { raft }
    }

    /// One-shot cluster bootstrap. Call on a single fresh node when
    /// forming a new cluster; that node becomes the initial leader.
    pub async fn initialize(
        &self,
        members: BTreeMap<NodeId, NodeAddrs>,
    ) -> Result<(), AdminError> {
        let members: BTreeMap<NodeId, ClusterNode> = members
            .into_iter()
            .map(|(id, addrs)| (id, addrs.into()))
            .collect();
        self.raft
            .initialize(members)
            .await
            .map_err(map_initialize_err)
    }

    /// Add a non-voting peer. Block until the learner has caught up
    /// to the leader's log (when `blocking == true`) before
    /// returning — so a follow-up `change_membership` can safely
    /// promote it without racing replication.
    pub async fn add_learner(
        &self,
        node_id: NodeId,
        addrs: NodeAddrs,
        blocking: bool,
    ) -> Result<(), AdminError> {
        self.raft
            .add_learner(node_id, addrs.into(), blocking)
            .await
            .map(|_| ())
            .map_err(map_client_write_err)
    }

    /// Change the cluster's voting membership. openraft drives the
    /// underlying two-phase joint consensus.
    pub async fn change_membership(
        &self,
        members: BTreeSet<NodeId>,
        retain: bool,
    ) -> Result<(), AdminError> {
        self.raft
            .change_membership(members, retain)
            .await
            .map(|_| ())
            .map_err(map_client_write_err)
    }

    /// Currently-known leader id. `None` during election.
    pub async fn current_leader(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }

    /// Snapshot of cluster state for status / health endpoints.
    pub fn status(&self) -> ClusterStatus {
        let metrics: RaftMetrics<NodeId, ClusterNode> = self.raft.metrics().borrow().clone();
        let voters: BTreeSet<NodeId> = metrics
            .membership_config
            .membership()
            .voter_ids()
            .collect();
        let learners: BTreeSet<NodeId> = metrics
            .membership_config
            .membership()
            .learner_ids()
            .collect();
        ClusterStatus {
            current_leader: metrics.current_leader,
            current_term: metrics.current_term,
            last_applied_index: metrics.last_applied.map(|id| id.index),
            voters,
            learners,
        }
    }
}

// ============================================================================
// Error mapping
// ============================================================================

fn map_initialize_err(
    err: openraft::error::RaftError<NodeId, openraft::error::InitializeError<NodeId, ClusterNode>>,
) -> AdminError {
    use openraft::error::InitializeError;
    use openraft::error::RaftError;
    match err {
        RaftError::APIError(InitializeError::NotAllowed { .. }) => AdminError::AlreadyInitialized,
        RaftError::APIError(other) => AdminError::Other(other.to_string()),
        RaftError::Fatal(f) => AdminError::Fatal(f.to_string()),
    }
}

fn map_client_write_err(
    err: openraft::error::RaftError<
        NodeId,
        openraft::error::ClientWriteError<NodeId, ClusterNode>,
    >,
) -> AdminError {
    use openraft::error::ClientWriteError;
    use openraft::error::RaftError;
    match err {
        RaftError::APIError(ClientWriteError::ForwardToLeader(fwd)) => AdminError::NotLeader {
            leader: fwd.leader_id,
        },
        RaftError::APIError(ClientWriteError::ChangeMembershipError(e)) => {
            AdminError::Other(e.to_string())
        }
        RaftError::Fatal(f) => AdminError::Fatal(f.to_string()),
    }
}

// ============================================================================
// HTTP router
// ============================================================================

/// Build an `axum::Router` exposing the admin endpoints against the
/// supplied [`Raft`] handle. Mount on the **private** listener
/// (same one [`super::network::router`] is mounted on) — never on
/// the public client-facing port.
///
/// Routes:
/// - `POST <base>/initialize`
/// - `POST <base>/add-learner`
/// - `POST <base>/change-membership`
/// - `GET  <base>/status`
pub fn router(raft: Arc<Raft<TypeConfig>>) -> Router {
    Router::new()
        .route("/initialize", post(handle_initialize))
        .route("/add-learner", post(handle_add_learner))
        .route("/change-membership", post(handle_change_membership))
        .route("/status", get(handle_status))
        .with_state(RaftAdmin::new(raft))
}

fn admin_error_response(err: AdminError) -> Response {
    let (status, body) = match &err {
        AdminError::AlreadyInitialized => (StatusCode::CONFLICT, err.to_string()),
        AdminError::NotLeader { .. } => (StatusCode::MISDIRECTED_REQUEST, err.to_string()),
        AdminError::Other(_) => (StatusCode::UNPROCESSABLE_ENTITY, err.to_string()),
        AdminError::Fatal(_) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    };
    (status, body).into_response()
}

async fn handle_initialize(
    State(admin): State<RaftAdmin>,
    Json(req): Json<InitializeRequest>,
) -> Response {
    match admin.initialize(req.members).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => admin_error_response(e),
    }
}

async fn handle_add_learner(
    State(admin): State<RaftAdmin>,
    Json(req): Json<AddLearnerRequest>,
) -> Response {
    match admin.add_learner(req.node_id, req.addrs, req.blocking).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => admin_error_response(e),
    }
}

async fn handle_change_membership(
    State(admin): State<RaftAdmin>,
    Json(req): Json<ChangeMembershipRequest>,
) -> Response {
    match admin.change_membership(req.members, req.retain).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => admin_error_response(e),
    }
}

async fn handle_status(State(admin): State<RaftAdmin>) -> Response {
    let status = admin.status();
    (StatusCode::OK, Json(status)).into_response()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    //! JSON round-trip + error-mapping tests. The wire shape is the
    //! one downstream operators script against; locking it down here
    //! catches accidental field renames or shape changes early.

    use super::*;
    use serde_json::json;

    #[test]
    fn initialize_request_round_trips() {
        let req = InitializeRequest {
            members: BTreeMap::from([(
                1,
                NodeAddrs {
                    raft_addr: "http://node-1:9090/raft".into(),
                    client_addr: "http://node-1:8080".into(),
                },
            )]),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: InitializeRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.members.len(), 1);
        assert_eq!(parsed.members[&1].raft_addr, "http://node-1:9090/raft");
        assert_eq!(parsed.members[&1].client_addr, "http://node-1:8080");
    }

    #[test]
    fn add_learner_request_round_trips() {
        let req = AddLearnerRequest {
            node_id: 2,
            addrs: NodeAddrs {
                raft_addr: "http://node-2:9090/raft".into(),
                client_addr: "http://node-2:8080".into(),
            },
            blocking: true,
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v,
            json!({
                "node_id": 2,
                "raft_addr": "http://node-2:9090/raft",
                "client_addr": "http://node-2:8080",
                "blocking": true,
            })
        );
        let parsed: AddLearnerRequest = serde_json::from_value(v).unwrap();
        assert_eq!(parsed.node_id, 2);
        assert_eq!(parsed.addrs.client_addr, "http://node-2:8080");
        assert!(parsed.blocking);
    }

    #[test]
    fn change_membership_request_round_trips() {
        let req = ChangeMembershipRequest {
            members: BTreeSet::from([1, 2, 3]),
            retain: false,
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v,
            json!({
                "members": [1, 2, 3],
                "retain": false,
            })
        );
    }

    #[test]
    fn cluster_status_serializes() {
        let s = ClusterStatus {
            current_leader: Some(1),
            current_term: 7,
            last_applied_index: Some(42),
            voters: BTreeSet::from([1, 2, 3]),
            learners: BTreeSet::new(),
        };
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["current_leader"], 1);
        assert_eq!(v["current_term"], 7);
        assert_eq!(v["last_applied_index"], 42);
        assert_eq!(v["voters"], json!([1, 2, 3]));
        assert_eq!(v["learners"], json!([]));
    }

    #[test]
    fn admin_error_messages_carry_leader_hint() {
        let err = AdminError::NotLeader { leader: Some(3) };
        assert!(err.to_string().contains("node 3"));
        let err = AdminError::NotLeader { leader: None };
        assert_eq!(err.to_string(), "not leader");
    }
}
