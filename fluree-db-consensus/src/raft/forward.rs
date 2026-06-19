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
//!   node's id, and a pooled `reqwest::Client`.
//! - [`forward_to_leader`]: an axum middleware that intercepts a
//!   request, checks leadership, and either calls `next.run(...)`
//!   (this node *is* the leader) or rebuilds the request as an
//!   outbound HTTP call to the leader's client port and returns the
//!   leader's response verbatim.
//!
//! # Resolving the leader's client URL
//!
//! [`ClusterNode`](crate::raft::ClusterNode) — the type config's
//! `Node` — carries both `raft_addr` (the inter-node RPC URL) and
//! `client_addr` (the client-facing URL). The membership openraft
//! replicates therefore already contains every voter's and
//! learner's client URL; the forwarder reads it from the current
//! membership snapshot on each request, so a peer added at runtime
//! via [`super::admin::RaftAdmin::add_learner`] is immediately
//! reachable for forwarding on every other node — no restart.

use crate::raft::{NodeId, TypeConfig};
use axum::body::Body;
use axum::extract::{OriginalUri, Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use openraft::Raft;
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

/// Upper bound on the request body the follower will buffer before
/// forwarding it to the leader. A follower that's still in catch-up
/// shouldn't be coerced into allocating arbitrary memory by a hostile
/// caller — anything beyond this returns 413 Payload Too Large. 64
/// MiB comfortably covers the bulk-import paths Fluree exposes today;
/// callers running larger imports should split the payload or address
/// the leader directly.
const MAX_FORWARDED_BODY_BYTES: usize = 64 * 1024 * 1024;

// ============================================================================
// State
// ============================================================================

/// Per-node forwarding state, mounted as axum middleware state.
#[derive(Clone)]
pub struct LeaderForwarder {
    raft: Arc<Raft<TypeConfig>>,
    self_id: NodeId,
    client: reqwest::Client,
}

impl LeaderForwarder {
    pub fn new(raft: Arc<Raft<TypeConfig>>, self_id: NodeId, client: reqwest::Client) -> Self {
        Self {
            raft,
            self_id,
            client,
        }
    }

    /// Decide whether this node should serve the request locally or
    /// forward it, resolving the leader's client URL from the
    /// current membership snapshot.
    async fn decide(&self) -> ForwardDecision {
        let Some(leader_id) = self.raft.current_leader().await else {
            return ForwardDecision::NoLeader;
        };
        if leader_id == self.self_id {
            return ForwardDecision::Local;
        }
        let metrics = self.raft.metrics().borrow().clone();
        let leader_node = metrics
            .membership_config
            .nodes()
            .find(|(id, _)| **id == leader_id)
            .map(|(_, node)| node.clone());
        match leader_node {
            Some(node) if !node.client_addr.is_empty() => {
                ForwardDecision::Forward(node.client_addr)
            }
            _ => ForwardDecision::UnknownLeader(leader_id),
        }
    }
}

enum ForwardDecision {
    /// This node is the leader — process locally.
    Local,
    /// Forward to the leader at this base client URL.
    Forward(String),
    /// We know the leader's id but the membership entry has no
    /// `client_addr` (or no entry for this id at all). Indicates a
    /// stale membership snapshot or a misconfigured `add_learner`
    /// call.
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
/// let forwarder = Arc::new(LeaderForwarder::new(raft, self_id, client));
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
                .unwrap_or_else(IntoResponse::into_response)
        }
        ForwardDecision::UnknownLeader(id) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "leader node {id} has no client address in the current \
                 membership; cluster may be reconfiguring"
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
///
/// Path resolution prefers `OriginalUri` over `parts.uri` so the
/// forwarded request carries the full public path the client used,
/// not the prefix-stripped path the inner nested router sees. Axum
/// rewrites `parts.uri` when it dispatches into a `nest`ed sub-router
/// (e.g. `/v1/fluree/create` becomes `/create` once inside the
/// `v1` router), but stashes the original path in the
/// `OriginalUri` extension so middleware can recover it. Without
/// this, a follower mounted under `/v1/fluree` would forward
/// `POST /create` to the leader's root and get a 404 — the leader
/// only mounts the routes under `/v1/fluree`.
async fn forward_request(
    client: &reqwest::Client,
    leader_base_url: &str,
    req: Request,
) -> Result<Response, ForwardError> {
    let (parts, body) = req.into_parts();
    let original_uri = parts.extensions.get::<OriginalUri>().map(|o| &o.0);
    let path_and_query = original_uri
        .and_then(|uri| uri.path_and_query())
        .or_else(|| parts.uri.path_and_query())
        .map_or("/", axum::http::uri::PathAndQuery::as_str);
    let leader_url = format!(
        "{}{}",
        leader_base_url.trim_end_matches('/'),
        path_and_query
    );

    let body_bytes = axum::body::to_bytes(body, MAX_FORWARDED_BODY_BYTES)
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
    let body_bytes = upstream.bytes().await.map_err(ForwardError::ReadResponse)?;

    let mut resp = Response::builder()
        .status(status_from_reqwest(status))
        .body(Body::from(body_bytes))
        .map_err(ForwardError::BuildResponse)?;

    let headers = resp.headers_mut();
    for (name, value) in &upstream_headers {
        if is_hop_by_hop(name.as_str()) {
            continue;
        }
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
    //! helpers. The end-to-end leader-forward flow (membership
    //! lookup → outbound HTTP → response relay) needs a live Raft
    //! cluster; that's exercised in the multi-node integration test.

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
        assert_eq!(status_from_reqwest(reqwest::StatusCode::OK), StatusCode::OK);
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
