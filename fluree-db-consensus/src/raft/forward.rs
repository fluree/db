//! Follower-forwarding middleware for leader-only client requests.
//!
//! Only the Raft leader can accept [`Committer`](crate::Committer)
//! writes (the state machine's `AdvanceRef` proposal has to
//! originate there). When a client load balancer happens to land on
//! a follower for `POST /api/transact` (or any other leader-only
//! route), the follower transparently HTTP-forwards the request to
//! the leader and relays the leader's response back. From the
//! client's perspective it's a single round-trip; the extra hop
//! lives entirely inside the cluster (VPC-internal, fast).
//!
//! This module gives you the primitives:
//!
//! - [`LeaderForwarder`]: per-node state — the Raft handle, this
//!   node's id, the static [`NodeId`] → client-URL map, and a
//!   pooled `reqwest::Client`.
//! - [`forward_to_leader`]: an axum middleware that intercepts a
//!   request, checks leadership, and either calls `next.run(...)`
//!   (this node *is* the leader) or rebuilds the request as an
//!   outbound HTTP call to the leader's client port and returns the
//!   leader's response verbatim.
//!
//! # Configuring the map
//!
//! The forwarder needs the leader's **client-facing** URL, not the
//! raft URL. Operators wire both at deploy time — each node's
//! config carries the cluster topology:
//!
//! ```toml
//! [cluster]
//! node_id = 1
//!
//! [cluster.peers.1]
//! raft_url = "http://node-1:9090/raft"
//! client_url = "http://node-1:8080"
//!
//! [cluster.peers.2]
//! raft_url = "http://node-2:9090/raft"
//! client_url = "http://node-2:8080"
//! ```
//!
//! `BasicNode.addr` (what openraft propagates through membership
//! changes) carries the raft URL only; the client URLs live in this
//! static map. v1 trade-off: the map is fixed at startup. Hot
//! membership changes require restarting peers so they pick up the
//! new node's client URL.

use crate::raft::{NodeId, TypeConfig};
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use openraft::Raft;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Hop-by-hop headers from RFC 7230 §6.1 plus a couple of modern
/// additions. Stripped from both the outbound request and the
/// returned response — they describe the *previous* connection and
/// don't make sense on a forwarded one.
const HOP_BY_HOP_HEADERS: &[&str] = &[
    "connection",
    "proxy-connection",
    "keep-alive",
    "te",
    "trailer",
    "trailers",
    "transfer-encoding",
    "upgrade",
];

// ============================================================================
// State
// ============================================================================

/// Per-node forwarding state, mounted as axum middleware state.
#[derive(Clone)]
pub struct LeaderForwarder {
    raft: Arc<Raft<TypeConfig>>,
    self_id: NodeId,
    peer_client_urls: Arc<BTreeMap<NodeId, String>>,
    client: reqwest::Client,
}

impl LeaderForwarder {
    /// Construct with the cluster topology already resolved into a
    /// `NodeId` → client-URL map. The `self_id` entry can be
    /// omitted (we never forward to ourselves), but including it is
    /// harmless.
    pub fn new(
        raft: Arc<Raft<TypeConfig>>,
        self_id: NodeId,
        peer_client_urls: BTreeMap<NodeId, String>,
        client: reqwest::Client,
    ) -> Self {
        Self {
            raft,
            self_id,
            peer_client_urls: Arc::new(peer_client_urls),
            client,
        }
    }

    /// Decide whether this node should serve the request locally or
    /// forward it. Returned [`ForwardDecision::Forward`] carries the
    /// leader's client URL.
    async fn decide(&self) -> ForwardDecision {
        match self.raft.current_leader().await {
            Some(id) if id == self.self_id => ForwardDecision::Local,
            Some(id) => match self.peer_client_urls.get(&id) {
                Some(url) => ForwardDecision::Forward(url.clone()),
                None => ForwardDecision::UnknownLeader(id),
            },
            None => ForwardDecision::NoLeader,
        }
    }
}

enum ForwardDecision {
    /// This node is the leader — process locally.
    Local,
    /// Forward to the leader at this base client URL.
    Forward(String),
    /// We know the leader's id but have no client URL for it (map
    /// missing the entry). Usually a deployment-config bug.
    UnknownLeader(NodeId),
    /// No leader is currently elected (election in progress).
    NoLeader,
}

// ============================================================================
// Middleware
// ============================================================================

/// Axum middleware: if this node is the leader, fall through to the
/// inner handler; otherwise forward the request to the leader's
/// client port and return its response verbatim. Mount it as a
/// layer over the leader-only routes (transact, branch admin, etc.).
///
/// Example:
/// ```ignore
/// use axum::{middleware, Router};
/// use std::sync::Arc;
///
/// let forwarder = Arc::new(LeaderForwarder::new(raft, self_id, peers, client));
/// let app = Router::new()
///     .route("/api/transact", axum::routing::post(transact_handler))
///     .layer(middleware::from_fn_with_state(forwarder, forward_to_leader));
/// ```
pub async fn forward_to_leader(
    State(forwarder): State<Arc<LeaderForwarder>>,
    request: Request,
    next: Next,
) -> Response {
    match forwarder.decide().await {
        ForwardDecision::Local => next.run(request).await,
        ForwardDecision::Forward(leader_url) => {
            forward_request(&forwarder.client, &leader_url, request)
                .await
                .unwrap_or_else(|e| e.into_response())
        }
        ForwardDecision::UnknownLeader(id) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "no client URL configured for leader node {id}; \
                 check the cluster peer map"
            ),
        )
            .into_response(),
        ForwardDecision::NoLeader => (
            StatusCode::SERVICE_UNAVAILABLE,
            "no leader currently elected; retry shortly",
        )
            .into_response(),
    }
}

// ============================================================================
// HTTP forwarding internals
// ============================================================================

/// Errors that can fall out of forwarding. All map to a 502 Bad
/// Gateway when surfaced to the original client — the request was
/// well-formed, the cluster just couldn't proxy it.
#[derive(Debug, thiserror::Error)]
enum ForwardError {
    #[error("reading request body to forward: {0}")]
    ReadBody(axum::Error),
    #[error("sending forwarded request to leader: {0}")]
    Send(reqwest::Error),
    #[error("reading forwarded response from leader: {0}")]
    ReadResponse(reqwest::Error),
    #[error("building forwarded response: {0}")]
    BuildResponse(axum::http::Error),
}

impl IntoResponse for ForwardError {
    fn into_response(self) -> Response {
        (StatusCode::BAD_GATEWAY, self.to_string()).into_response()
    }
}

/// Rebuild `req` as an outbound HTTP call to `leader_base_url`,
/// preserving the path+query, method, body, and (most) headers.
async fn forward_request(
    client: &reqwest::Client,
    leader_base_url: &str,
    req: Request,
) -> Result<Response, ForwardError> {
    let (parts, body) = req.into_parts();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let leader_url = format!(
        "{}{}",
        leader_base_url.trim_end_matches('/'),
        path_and_query
    );

    let body_bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .map_err(ForwardError::ReadBody)?;

    let upstream = client
        .request(parts.method, &leader_url)
        .headers(strip_hop_by_hop(parts.headers))
        .body(body_bytes)
        .send()
        .await
        .map_err(ForwardError::Send)?;

    response_from_upstream(upstream).await
}

/// Convert a `reqwest::Response` into an `axum::Response`, copying
/// the status, end-to-end headers, and body verbatim.
async fn response_from_upstream(upstream: reqwest::Response) -> Result<Response, ForwardError> {
    let status = upstream.status();
    let upstream_headers = upstream.headers().clone();
    let body_bytes = upstream
        .bytes()
        .await
        .map_err(ForwardError::ReadResponse)?;

    let mut resp = Response::builder()
        .status(status_from_reqwest(status))
        .body(Body::from(body_bytes))
        .map_err(ForwardError::BuildResponse)?;

    let headers = resp.headers_mut();
    for (name, value) in upstream_headers.iter() {
        if is_hop_by_hop(name.as_str()) {
            continue;
        }
        // axum / http types are the same; copy across.
        if let (Ok(n), Ok(v)) = (
            HeaderName::from_bytes(name.as_str().as_bytes()),
            HeaderValue::from_bytes(value.as_bytes()),
        ) {
            headers.insert(n, v);
        }
    }
    Ok(resp)
}

fn strip_hop_by_hop(mut headers: HeaderMap) -> HeaderMap {
    headers.remove("host");
    for name in HOP_BY_HOP_HEADERS {
        headers.remove(*name);
    }
    headers
}

fn is_hop_by_hop(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    HOP_BY_HOP_HEADERS.iter().any(|h| *h == lower)
}

fn status_from_reqwest(s: reqwest::StatusCode) -> StatusCode {
    StatusCode::from_u16(s.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    //! Unit tests cover the header-scrubbing + status-mapping
    //! helpers. The end-to-end leader-forward flow needs a live
    //! Raft cluster; that's exercised in the multi-node integration
    //! test.

    use super::*;

    #[test]
    fn hop_by_hop_headers_are_dropped() {
        let mut h = HeaderMap::new();
        h.insert("host", "node-1:8080".parse().unwrap());
        h.insert("connection", "keep-alive".parse().unwrap());
        h.insert("upgrade", "h2c".parse().unwrap());
        h.insert("authorization", "Bearer abc".parse().unwrap());
        h.insert("x-custom", "value".parse().unwrap());

        let scrubbed = strip_hop_by_hop(h);
        assert!(!scrubbed.contains_key("host"));
        assert!(!scrubbed.contains_key("connection"));
        assert!(!scrubbed.contains_key("upgrade"));
        // End-to-end headers preserved.
        assert_eq!(scrubbed.get("authorization").unwrap(), "Bearer abc");
        assert_eq!(scrubbed.get("x-custom").unwrap(), "value");
    }

    #[test]
    fn hop_by_hop_detection_is_case_insensitive() {
        assert!(is_hop_by_hop("Connection"));
        assert!(is_hop_by_hop("TRANSFER-ENCODING"));
        assert!(!is_hop_by_hop("Content-Type"));
        assert!(!is_hop_by_hop("Authorization"));
    }

    #[test]
    fn status_code_round_trips() {
        assert_eq!(
            status_from_reqwest(reqwest::StatusCode::OK),
            StatusCode::OK
        );
        assert_eq!(
            status_from_reqwest(reqwest::StatusCode::BAD_REQUEST),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status_from_reqwest(reqwest::StatusCode::INTERNAL_SERVER_ERROR),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
