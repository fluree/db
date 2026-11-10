//! Write-side catch-up and acknowledgements for retention.
//!
//! Erasure acceptance and proving confirm what the cluster has committed
//! through a Raft read index. Ordinary reads use local replication state
//! and only wait for locally applied erasures to be installed. A journal
//! change waits for members to report that they have applied it.

use super::{ForwardingConfig, RaftNameService, POSTCARD_MIME};
use crate::raft::NodeId;
use async_trait::async_trait;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The leader's read index, for a follower's read barrier.
pub const READ_INDEX_PATH: &str = "/read_index";
/// A member's applied log index.
pub const APPLIED_INDEX_PATH: &str = "/applied_index";

const POLL: Duration = Duration::from_millis(50);

/// Routes for [`READ_INDEX_PATH`] and [`APPLIED_INDEX_PATH`], mounted beside
/// the other inter-node RPCs.
pub fn read_index_router(ns: Arc<RaftNameService>) -> Router {
    Router::new()
        .route(READ_INDEX_PATH, post(handle_read_index))
        .route(APPLIED_INDEX_PATH, get(handle_applied_index))
        .with_state(ns)
}

async fn handle_read_index(State(ns): State<Arc<RaftNameService>>) -> Response {
    let outcome: Result<u64, String> = match ns.raft.get_read_log_id().await {
        Ok((read, _)) => Ok(read.map_or(0, |id| id.index)),
        Err(e) => Err(e.to_string()),
    };
    postcard_response(&outcome)
}

async fn handle_applied_index(State(ns): State<Arc<RaftNameService>>) -> Response {
    postcard_response(&ns.applied_index())
}

fn postcard_response<T: serde::Serialize>(value: &T) -> Response {
    match postcard::to_allocvec(value) {
        Ok(bytes) => (
            StatusCode::OK,
            [(reqwest::header::CONTENT_TYPE, POSTCARD_MIME)],
            bytes,
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

impl RaftNameService {
    fn applied_index(&self) -> u64 {
        self.raft
            .metrics()
            .borrow()
            .last_applied
            .map_or(0, |id| id.index)
    }

    fn forwarding(&self) -> Result<&ForwardingConfig, String> {
        self.forwarding
            .as_ref()
            .ok_or_else(|| "cross-node forwarding is not configured".to_string())
    }

    /// Wait until this node has applied everything committed before the
    /// call.
    pub async fn sync_reads(&self) -> Result<(), String> {
        let forwarding = self.forwarding()?;
        let timeout = forwarding.request_timeout;
        let target = if self.is_local_leader(forwarding.id).await {
            let (read, _) = self
                .raft
                .get_read_log_id()
                .await
                .map_err(|e| e.to_string())?;
            read.map_or(0, |id| id.index)
        } else {
            let leader = self
                .lookup_leader_raft_url()
                .await
                .ok_or_else(|| "no leader is known".to_string())?;
            let bytes = forwarding
                .http_client
                .post(format!("{}{READ_INDEX_PATH}", leader.trim_end_matches('/')))
                .timeout(timeout)
                .send()
                .await
                .and_then(reqwest::Response::error_for_status)
                .map_err(|e| format!("read index from the leader: {e}"))?
                .bytes()
                .await
                .map_err(|e| format!("read index from the leader: {e}"))?;
            postcard::from_bytes::<Result<u64, String>>(&bytes)
                .map_err(|e| format!("read index response: {e}"))??
        };
        self.raft
            .wait(Some(timeout))
            .applied_index_at_least(Some(target), "removal journal read barrier")
            .await
            .map(|_| ())
            .map_err(|e| format!("applying through the read index: {e}"))
    }

    /// Wait up to `timeout` for every other member to apply what this node
    /// has applied; the members that did not.
    pub async fn await_replicas(&self, timeout: Duration) -> Vec<String> {
        let target = self.applied_index();
        let members: Vec<(NodeId, String)> = self
            .raft
            .metrics()
            .borrow()
            .membership_config
            .nodes()
            .map(|(id, node)| (*id, node.raft_addr.clone()))
            .collect();
        let Ok(forwarding) = self.forwarding() else {
            return members
                .into_iter()
                .map(|(id, addr)| format!("node {id} ({addr})"))
                .collect();
        };
        let allow_loopback = members
            .iter()
            .find(|(id, _)| *id == forwarding.id)
            .is_some_and(|(_, addr)| crate::raft::forward::self_addr_is_loopback(addr));
        let deadline = Instant::now() + timeout;
        let waits = members
            .into_iter()
            .filter(|(id, _)| *id != forwarding.id)
            .map(|(id, addr)| async move {
                let acked = crate::raft::forward::is_valid_leader_url(&addr, allow_loopback)
                    && member_applied(forwarding, &addr, target, deadline).await;
                (!acked).then(|| format!("node {id} ({addr})"))
            });
        futures::future::join_all(waits)
            .await
            .into_iter()
            .flatten()
            .collect()
    }
}

/// Poll `addr` until it has applied `target`, or `deadline` passes.
async fn member_applied(
    forwarding: &ForwardingConfig,
    addr: &str,
    target: u64,
    deadline: Instant,
) -> bool {
    let url = format!("{}{APPLIED_INDEX_PATH}", addr.trim_end_matches('/'));
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        let applied = async {
            let resp = forwarding
                .http_client
                .get(&url)
                .timeout(remaining)
                .send()
                .await
                .ok()?
                .error_for_status()
                .ok()?;
            postcard::from_bytes::<u64>(&resp.bytes().await.ok()?).ok()
        }
        .await;
        if applied.is_some_and(|applied| applied >= target) {
            return true;
        }
        tokio::time::sleep(POLL.min(deadline.saturating_duration_since(Instant::now()))).await;
    }
}

/// [`fluree_db_api::retention::replicas::ReplicaSync`] over the cluster.
pub struct RaftReplicaSync(pub Arc<RaftNameService>);

#[async_trait]
impl fluree_db_api::retention::replicas::ReplicaSync for RaftReplicaSync {
    fn config_events_complete(&self) -> bool {
        true
    }

    async fn sync_reads(&self) -> Result<(), String> {
        self.0.sync_reads().await
    }

    async fn await_replicas(&self, timeout: Duration) -> Vec<String> {
        self.0.await_replicas(timeout).await
    }

    async fn erasures_installed(&self, ledger_id: &str) -> Result<(), String> {
        self.0
            .wait_erasures_installed(ledger_id, INSTALL_WAIT)
            .await
    }
}

/// How long an affected read waits for this node to install an erasure
/// activation it has applied before refusing the read.
const INSTALL_WAIT: Duration = Duration::from_secs(5);
