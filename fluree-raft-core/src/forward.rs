//! Follower-forwarding middleware for leader-only client requests.
//!
//! Only the leader can propose, so any route that ends in a proposal
//! is leader-only. When a client load balancer lands on a follower for
//! one of those routes, the follower transparently HTTP-forwards the
//! request to the leader and relays the response back. From the
//! client's perspective it is a single round-trip; the extra hop stays
//! inside the cluster.
//!
//! This module gives you the primitives:
//!
//! - [`LeaderView`]: what the routing decision actually needs — who
//!   leads, and the membership addresses. Implemented for
//!   `openraft::Raft<C>`, so production wiring is a no-op.
//! - [`LeaderForwarder`]: per-node state — a [`LeaderView`], this
//!   node's id, and a pooled `reqwest::Client`.
//! - [`forward_to_leader`]: an axum middleware that intercepts a
//!   request, checks leadership, and either calls `next.run(...)`
//!   (this node *is* the leader) or rebuilds the request as an
//!   outbound HTTP call to the leader's client port and returns the
//!   leader's response verbatim.
//!
//! # Resolving the leader's client URL
//!
//! [`ClusterNode`] — the type config's
//! `Node` — carries both `raft_addr` (the inter-node RPC URL) and
//! `client_addr` (the client-facing URL). The membership openraft
//! replicates therefore already contains every voter's and
//! learner's client URL; the forwarder reads it from the current
//! membership snapshot on each request, so a peer added at runtime
//! via [`crate::admin::RaftAdmin::add_learner`] is immediately
//! reachable for forwarding on every other node — no restart.

use crate::config::FlureeRaftConfig;
use crate::http::is_hop_by_hop;
use crate::network::{RaftHttpClient, RaftTransportConfig};
use crate::node::{ClusterNode, NodeId};
use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{OriginalUri, Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use openraft::Raft;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

/// Total per-request timeout for a forwarded call to the leader,
/// from connect through response body read. Without this, a leader
/// that accepts the connection but stalls (long GC pause, fsync
/// stuck, network blackhole) pins the follower's forwarding task
/// indefinitely with the buffered request body still resident —
/// sustained client traffic against a frozen leader exhausts the
/// follower's memory before any failover takes over. 60 s comfortably
/// covers a 64 MiB body at modest throughput while bounding the
/// resource footprint of a stuck leader.
const FORWARD_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Header carrying the count of follower → leader hops a request
/// has already accumulated. Each forwarder increments it; the next
/// forwarder bails if the count is already at [`MAX_FORWARD_HOPS`].
/// Stops a misconfigured cluster (two nodes that each believe the
/// other is leader, e.g. across a membership-update race) from
/// looping a single client request through the whole ring.
const FORWARD_HOPS_HEADER: &str = "x-fluree-raft-forward-hops";

/// Maximum follower → leader hops a request may take. One hop
/// covers the canonical case (client lands on a follower, follower
/// forwards to the leader); the slack absorbs at-most-one stale
/// membership snapshot on the path. Anything beyond that is almost
/// certainly a converging cluster — we'd rather surface 503 + retry
/// than amplify the load.
const MAX_FORWARD_HOPS: u32 = 2;

// ============================================================================
// State
// ============================================================================

/// What [`LeaderForwarder`] needs to know about the cluster.
///
/// Implemented for `openraft::Raft<C>`, which is what every group uses
/// in production. It exists as a trait so forwarding can be exercised
/// without standing up a Raft instance — the decision logic (leader
/// resolution, the SSRF guard, loopback detection) is the part most
/// worth testing, and it has nothing to do with consensus.
#[async_trait]
pub trait LeaderView: Send + Sync + 'static {
    /// Currently-known leader id. `None` during an election.
    async fn current_leader(&self) -> Option<NodeId>;

    /// Every known member with its address pair, from the local
    /// membership snapshot.
    fn membership_nodes(&self) -> Vec<(NodeId, ClusterNode)>;
}

#[async_trait]
impl<C: FlureeRaftConfig> LeaderView for Raft<C> {
    async fn current_leader(&self) -> Option<NodeId> {
        Raft::current_leader(self).await
    }

    fn membership_nodes(&self) -> Vec<(NodeId, ClusterNode)> {
        self.metrics()
            .borrow()
            .membership_config
            .nodes()
            .map(|(id, node)| (*id, node.clone()))
            .collect()
    }
}

/// Per-node forwarding state, mounted as axum middleware state.
pub struct LeaderForwarder<L: LeaderView> {
    raft: Arc<L>,
    id: NodeId,
    client: reqwest::Client,
    /// Upper bound on the request body buffered before relaying —
    /// a follower shouldn't be coerced into allocating arbitrary
    /// memory by a hostile caller. Bodies beyond the cap are
    /// refused with 413 Payload Too Large before any relay. See
    /// [`RaftTransportConfig::forward_max_body_bytes`] for how to size it.
    max_body_bytes: usize,
}

// Hand-written: `#[derive(Clone)]` would demand `L: Clone`, but `L` is
// only ever held behind an `Arc`.
impl<L: LeaderView> Clone for LeaderForwarder<L> {
    fn clone(&self) -> Self {
        Self {
            raft: Arc::clone(&self.raft),
            id: self.id,
            client: self.client.clone(),
            max_body_bytes: self.max_body_bytes,
        }
    }
}

impl<L: LeaderView> LeaderForwarder<L> {
    /// Takes a [`RaftHttpClient`] rather than a bare `reqwest::Client`:
    /// this is the path that dials a membership-supplied URL, so the
    /// no-redirects guarantee is load-bearing here. See
    /// [`is_valid_leader_url`] for what the guard does and does not
    /// cover.
    pub fn new(raft: Arc<L>, id: NodeId, client: RaftHttpClient) -> Self {
        Self {
            raft,
            id,
            client: client.inner().clone(),
            max_body_bytes: RaftTransportConfig::default().forward_max_body_bytes,
        }
    }

    /// Cap the request body buffered before relaying (see
    /// [`RaftTransportConfig::forward_max_body_bytes`]).
    pub fn with_max_body_bytes(mut self, max_body_bytes: usize) -> Self {
        self.max_body_bytes = max_body_bytes;
        self
    }

    /// Decide whether this node should serve the request locally or
    /// forward it, resolving the leader's client URL from the
    /// current membership snapshot.
    ///
    /// `allow_loopback` for the SSRF check is sourced from this
    /// node's own `client_addr` in membership: if self is on
    /// loopback / `localhost`, every peer is too (single-host test
    /// or dev cluster), so loopback peer URLs are accepted. In a
    /// real multi-host deployment self binds to a routable address,
    /// `allow_loopback` is false, and a loopback peer URL is
    /// rejected as SSRF.
    async fn decide(&self) -> ForwardDecision {
        let Some(leader_id) = self.raft.current_leader().await else {
            return ForwardDecision::NoLeader;
        };
        if leader_id == self.id {
            return ForwardDecision::Local;
        }
        let nodes: Vec<(NodeId, ClusterNode)> = self.raft.membership_nodes();
        let allow_loopback = nodes
            .iter()
            .find(|(id, _)| *id == self.id)
            .map(|(_, node)| self_addr_is_loopback(&node.client_addr))
            .unwrap_or(false);
        let leader_node = nodes
            .into_iter()
            .find(|(id, _)| *id == leader_id)
            .map(|(_, node)| node);
        match leader_node {
            Some(node) if is_valid_leader_url(&node.client_addr, allow_loopback) => {
                ForwardDecision::Forward(node.client_addr)
            }
            _ => ForwardDecision::UnknownLeader(leader_id),
        }
    }
}

/// Sanity-check a candidate leader URL before opening an outbound
/// connection to it. Replicated membership data is broadly trusted
/// (proposing a `ChangeMembership` requires Raft consent), but a
/// buggy `add_learner` call or a hand-edited snapshot could leave a
/// `client_addr` that redirects every follower's forwarded write —
/// body, auth headers, and all — at the wrong destination.
///
/// Permit only the two transport schemes the cluster actually uses
/// (http/https) and reject hosts that are obvious SSRF targets:
///
/// - **Loopback** (`127.0.0.0/8`, `::1`) and the literal hostname
///   `"localhost"`: a follower pointing `client_addr` at its own
///   localhost would loop back the write into whatever local service
///   answers that port (Postgres at 5432, an admin endpoint, etc.).
///   Permitted only when `allow_loopback` is true, which the caller
///   sets when this node also reports itself on loopback —
///   single-host test or dev clusters where every peer is on
///   localhost legitimately. In a real multi-host deployment self
///   binds to a routable interface, so `allow_loopback` stays false
///   and loopback peer URLs are rejected.
/// - **Link-local** (`169.254.0.0/16`, `fe80::/10`): notably AWS /
///   GCP / Azure instance metadata services at `169.254.169.254`,
///   which return cloud credentials. Always rejected.
/// - **Unspecified** (`0.0.0.0`, `::`): kernel routes these to a
///   local interface, same effective risk as loopback, but never a
///   valid client target. Always rejected.
///
/// Hostnames are not resolved here — a hostname that resolves to a
/// denied IP at DNS time still passes, and the kernel handles the
/// rest. The intent is to catch the straight-line mistake of putting
/// a literal SSRF address into the membership record; an active
/// adversary controlling DNS for cluster hostnames is out of scope
/// of this check and needs to be addressed at a different layer.
///
/// Public because applications that relay to the leader over their own
/// RPCs (rather than through [`forward_to_leader`]) need the same guard
/// on the same membership-sourced URLs; a second, drifting copy of this
/// check is exactly what should not happen.
pub fn is_valid_leader_url(url: &str, allow_loopback: bool) -> bool {
    match url_host(url) {
        Some(host) => !is_ssrf_host(&host, allow_loopback),
        None => false,
    }
}

/// True iff this node's own reported `client_addr` is on loopback
/// (or the literal `"localhost"`). The forwarder uses this to opt
/// into accepting loopback peer URLs — see [`is_valid_leader_url`].
pub fn self_addr_is_loopback(url: &str) -> bool {
    url_host(url).is_some_and(|h| is_loopback_host(&h))
}

/// Parse an http(s) URL and return its bare host (IPv6 brackets
/// stripped). `None` for empty input, unparseable URLs, non-http(s)
/// schemes, and URLs with no host. `Url::host_str` returns IPv6
/// addresses with the URL-syntax `[...]` brackets in place; the
/// parsers we then feed expect bare addresses.
fn url_host(url: &str) -> Option<String> {
    if url.is_empty() {
        return None;
    }
    let parsed = reqwest::Url::parse(url).ok()?;
    if !matches!(parsed.scheme(), "http" | "https") || !parsed.has_host() {
        return None;
    }
    let host = parsed.host_str()?;
    let bare = host
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host);
    Some(bare.to_string())
}

/// True for loopback IPv4 (`127.0.0.0/8`), IPv6 (`::1`), and the
/// literal hostname `"localhost"`. Used both for the SSRF check and
/// for self-loopback detection.
fn is_loopback_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<IpAddr>()
        .is_ok_and(|ip| ip.to_canonical().is_loopback())
}

/// Returns true for hosts that should never be a legitimate cluster
/// peer in this deployment: link-local / unspecified IPs always; the
/// literal `"localhost"` and loopback IPs unless `allow_loopback` is
/// set (single-host test/dev clusters). See [`is_valid_leader_url`]
/// for the full rationale.
fn is_ssrf_host(host: &str, allow_loopback: bool) -> bool {
    if is_loopback_host(host) {
        return !allow_loopback;
    }
    let Ok(ip) = host.parse::<IpAddr>() else {
        return false;
    };
    // Canonicalize first: an IPv4-mapped IPv6 literal like
    // `::ffff:169.254.169.254` routes to the mapped IPv4 address
    // (here, the cloud metadata service), but its raw v6 form
    // passes every check below — `to_canonical` collapses it to the
    // v4 form so the v4 rules apply.
    let ip = ip.to_canonical();
    if ip.is_unspecified() {
        return true;
    }
    match ip {
        IpAddr::V4(v4) => v4.is_link_local(),
        IpAddr::V6(v6) => {
            // fe80::/10 — link-local unicast. `Ipv6Addr::is_unicast_link_local`
            // exists but is still unstable; the segment check is the
            // stable equivalent.
            (v6.segments()[0] & 0xffc0) == 0xfe80
        }
    }
}

#[derive(Debug)]
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
/// client port and return its response verbatim. Mount it as a layer
/// over the routes that end in a proposal — leave read-only routes off
/// it, since any node can serve those from its local state.
///
/// Example:
/// ```ignore
/// use axum::{middleware, Router};
/// use std::sync::Arc;
///
/// let forwarder = Arc::new(LeaderForwarder::new(raft, id, client));
/// let app = Router::new()
///     .route("/write", axum::routing::post(write_handler))
///     .layer(middleware::from_fn_with_state(forwarder, forward_to_leader));
/// ```
pub async fn forward_to_leader<L: LeaderView>(
    State(forwarder): State<Arc<LeaderForwarder<L>>>,
    request: Request,
    next: Next,
) -> Response {
    match forwarder.decide().await {
        ForwardDecision::Local => next.run(request).await,
        ForwardDecision::Forward(leader_url) => {
            let hops = incoming_hop_count(request.headers());
            if hops >= MAX_FORWARD_HOPS {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!(
                        "forward hop limit ({MAX_FORWARD_HOPS}) reached; \
                         cluster likely converging on a new leader, retry shortly"
                    ),
                )
                    .into_response();
            }
            forward_request(
                &forwarder.client,
                &leader_url,
                request,
                hops + 1,
                forwarder.max_body_bytes,
            )
            .await
            .unwrap_or_else(IntoResponse::into_response)
        }
        ForwardDecision::UnknownLeader(id) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "leader node {id} has no usable client address in the current \
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

/// Read the hop counter from a follower-forward header. Missing /
/// malformed values mean "this is the first hop" — clients that
/// don't set the header at all start at zero, so a fresh public
/// request gets one full follower → leader hop.
fn incoming_hop_count(headers: &HeaderMap) -> u32 {
    headers
        .get(FORWARD_HOPS_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0)
}

// ============================================================================
// HTTP forwarding internals
// ============================================================================

/// Errors that can fall out of forwarding. Each maps to the HTTP
/// status that best describes its failure mode for the original
/// client.
#[derive(Debug, thiserror::Error)]
enum ForwardError {
    #[error("reading request body to forward: {0}")]
    ReadBody(axum::Error),
    #[error("request body exceeds the {limit}-byte forward limit")]
    BodyTooLarge { limit: usize },
    #[error("sending forwarded request to leader: {0}")]
    Send(reqwest::Error),
    #[error("forwarded request to leader timed out after {seconds}s", seconds = FORWARD_REQUEST_TIMEOUT.as_secs())]
    Timeout,
    #[error("reading forwarded response from leader: {0}")]
    ReadResponse(reqwest::Error),
    #[error("building forwarded response: {0}")]
    BuildResponse(axum::http::Error),
}

impl IntoResponse for ForwardError {
    fn into_response(self) -> Response {
        let status = match self {
            // `Timeout` matches HTTP's gateway-timeout semantics — the
            // proxy gave up waiting for the upstream. `ReadResponse`
            // covers the timeout that fires mid-body-read by way of
            // reqwest's per-request deadline.
            ForwardError::Timeout => StatusCode::GATEWAY_TIMEOUT,
            ForwardError::ReadResponse(ref e) if e.is_timeout() => StatusCode::GATEWAY_TIMEOUT,
            // The client's fault, not the leader's — a 5xx here
            // would read as infrastructure failure and invite
            // retries of a request that can never succeed.
            ForwardError::BodyTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            _ => StatusCode::BAD_GATEWAY,
        };
        (status, self.to_string()).into_response()
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
    outgoing_hops: u32,
    max_body_bytes: usize,
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

    let body_bytes = axum::body::to_bytes(body, max_body_bytes)
        .await
        .map_err(|e| classify_body_read_error(e, max_body_bytes))?;

    let mut headers = strip_hop_by_hop(parts.headers);
    // Stamp the outgoing hop count so the next forwarder can bail
    // if we're stuck in a loop. We always insert (rather than
    // merging onto whatever the client sent) so a hostile client
    // can't suppress the guard.
    headers.insert(
        HeaderName::from_static(FORWARD_HOPS_HEADER),
        HeaderValue::from(outgoing_hops),
    );

    let upstream = client
        .request(parts.method, &leader_url)
        .headers(headers)
        .body(body_bytes)
        .timeout(FORWARD_REQUEST_TIMEOUT)
        .send()
        .await
        .map_err(|e| {
            if e.is_timeout() {
                ForwardError::Timeout
            } else {
                ForwardError::Send(e)
            }
        })?;

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

/// Distinguish the body-length cap tripping from a genuine read
/// failure: the cap is the client's fault (413), a failed read is
/// the connection's (502). `axum::body::to_bytes` reports both as
/// `axum::Error`, with the cap identifiable by a
/// `LengthLimitError` in the source chain.
fn classify_body_read_error(err: axum::Error, limit: usize) -> ForwardError {
    let mut source = std::error::Error::source(&err);
    while let Some(current) = source {
        if current.is::<http_body_util::LengthLimitError>() {
            return ForwardError::BodyTooLarge { limit };
        }
        source = current.source();
    }
    ForwardError::ReadBody(err)
}

/// Drop hop-by-hop headers (see [`crate::http::is_hop_by_hop`])
/// plus `host`, which the outbound client rewrites for the leader's
/// address.
fn strip_hop_by_hop(mut headers: HeaderMap) -> HeaderMap {
    headers.remove("host");
    let hop_by_hop: Vec<HeaderName> = headers
        .keys()
        .filter(|name| is_hop_by_hop(name.as_str()))
        .cloned()
        .collect();
    for name in hop_by_hop {
        headers.remove(name);
    }
    headers
}

fn status_from_reqwest(s: reqwest::StatusCode) -> StatusCode {
    StatusCode::from_u16(s.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    //! Unit tests cover the routing decision (via a stub
    //! [`LeaderView`]) plus the header-scrubbing and status-mapping
    //! helpers. Only the outbound HTTP relay itself still needs a live
    //! cluster; that's exercised in the multi-node integration test.

    use super::*;

    /// A [`LeaderView`] with no Raft behind it.
    struct StubView {
        leader: Option<NodeId>,
        nodes: Vec<(NodeId, ClusterNode)>,
    }

    #[async_trait]
    impl LeaderView for StubView {
        async fn current_leader(&self) -> Option<NodeId> {
            self.leader
        }
        fn membership_nodes(&self) -> Vec<(NodeId, ClusterNode)> {
            self.nodes.clone()
        }
    }

    /// `self_id` is node 1 throughout; `nodes` lists `(id, client_addr)`.
    fn forwarder(leader: Option<NodeId>, nodes: &[(NodeId, &str)]) -> LeaderForwarder<StubView> {
        let view = StubView {
            leader,
            nodes: nodes
                .iter()
                .map(|(id, addr)| (*id, ClusterNode::new(format!("{addr}/raft"), *addr)))
                .collect(),
        };
        LeaderForwarder::new(
            Arc::new(view),
            1,
            crate::network::build_client(&crate::network::HttpClientConfig::default())
                .expect("client builds"),
        )
    }

    #[tokio::test]
    async fn no_leader_during_election() {
        let f = forwarder(None, &[(1, "http://10.0.0.1:8080")]);
        assert!(matches!(f.decide().await, ForwardDecision::NoLeader));
    }

    #[tokio::test]
    async fn self_is_leader_runs_locally() {
        let f = forwarder(Some(1), &[(1, "http://10.0.0.1:8080")]);
        assert!(matches!(f.decide().await, ForwardDecision::Local));
    }

    #[tokio::test]
    async fn forwards_to_the_leader_client_addr() {
        let f = forwarder(
            Some(2),
            &[(1, "http://10.0.0.1:8080"), (2, "http://10.0.0.2:8080")],
        );
        match f.decide().await {
            ForwardDecision::Forward(url) => assert_eq!(url, "http://10.0.0.2:8080"),
            other => panic!("expected Forward, got {other:?}"),
        }
    }

    /// A leader id with no membership entry is a stale snapshot, not a
    /// reason to guess an address.
    #[tokio::test]
    async fn leader_missing_from_membership_is_unknown() {
        let f = forwarder(Some(9), &[(1, "http://10.0.0.1:8080")]);
        assert!(matches!(
            f.decide().await,
            ForwardDecision::UnknownLeader(9)
        ));
    }

    /// The SSRF guard: when this node is on a routable address, a
    /// loopback peer URL is refused rather than dialed.
    #[tokio::test]
    async fn loopback_leader_rejected_when_self_is_routable() {
        let f = forwarder(
            Some(2),
            &[(1, "http://10.0.0.1:8080"), (2, "http://127.0.0.1:8080")],
        );
        assert!(matches!(
            f.decide().await,
            ForwardDecision::UnknownLeader(2)
        ));
    }

    /// ...but a single-host dev/test cluster, where this node is itself
    /// on loopback, still forwards.
    #[tokio::test]
    async fn loopback_leader_allowed_when_self_is_loopback() {
        let f = forwarder(
            Some(2),
            &[(1, "http://127.0.0.1:8080"), (2, "http://127.0.0.1:8081")],
        );
        match f.decide().await {
            ForwardDecision::Forward(url) => assert_eq!(url, "http://127.0.0.1:8081"),
            other => panic!("expected Forward, got {other:?}"),
        }
    }

    /// Link-local (cloud instance-metadata) is refused even on a
    /// loopback-permissive node — it is never a valid client target.
    #[tokio::test]
    async fn link_local_leader_always_rejected() {
        let f = forwarder(
            Some(2),
            &[
                (1, "http://127.0.0.1:8080"),
                (2, "http://169.254.169.254:80"),
            ],
        );
        assert!(matches!(
            f.decide().await,
            ForwardDecision::UnknownLeader(2)
        ));
    }

    #[test]
    fn hop_by_hop_headers_are_dropped() {
        let mut h = HeaderMap::new();
        h.insert("host", "node-1:8080".parse().unwrap());
        h.insert("connection", "keep-alive".parse().unwrap());
        h.insert("upgrade", "h2c".parse().unwrap());
        h.insert("proxy-authorization", "Basic xyz".parse().unwrap());
        h.insert("authorization", "Bearer abc".parse().unwrap());
        h.insert("x-custom", "value".parse().unwrap());

        let scrubbed = strip_hop_by_hop(h);
        assert!(!scrubbed.contains_key("host"));
        assert!(!scrubbed.contains_key("connection"));
        assert!(!scrubbed.contains_key("upgrade"));
        // Previous-hop proxy credentials must not reach the leader.
        assert!(!scrubbed.contains_key("proxy-authorization"));
        // End-to-end headers preserved.
        assert_eq!(scrubbed.get("authorization").unwrap(), "Bearer abc");
        assert_eq!(scrubbed.get("x-custom").unwrap(), "value");
    }

    #[test]
    fn timeout_error_maps_to_gateway_timeout() {
        let resp = ForwardError::Timeout.into_response();
        assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);
    }

    /// The body-cap tripping is the client's fault and must read as
    /// 413, not a 502 that looks like infrastructure failure and
    /// invites retries of a request that can never succeed.
    #[tokio::test]
    async fn oversized_body_classifies_as_payload_too_large() {
        let err = axum::body::to_bytes(Body::from(vec![0u8; 64]), 16)
            .await
            .expect_err("body exceeds cap");
        let classified = classify_body_read_error(err, 16);
        assert!(
            matches!(classified, ForwardError::BodyTooLarge { limit: 16 }),
            "expected BodyTooLarge, got {classified:?}"
        );
        let resp = classified.into_response();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn non_limit_body_errors_stay_read_body() {
        let err = axum::Error::new(std::io::Error::other("connection reset"));
        let classified = classify_body_read_error(err, 16);
        assert!(
            matches!(classified, ForwardError::ReadBody(_)),
            "expected ReadBody, got {classified:?}"
        );
        assert_eq!(classified.into_response().status(), StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn non_timeout_errors_map_to_bad_gateway() {
        // `BuildResponse` is the easiest variant to construct in a
        // test — it wraps `axum::http::Error`, which `Response::builder`
        // produces for an out-of-range status. Stand-in for any
        // non-timeout variant; the mapping treats them all the same.
        let axum_err = Response::builder()
            .status(9999_u16)
            .body(Body::empty())
            .unwrap_err();
        let resp = ForwardError::BuildResponse(axum_err).into_response();
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
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

    #[test]
    fn leader_url_validation_accepts_http_and_https() {
        assert!(is_valid_leader_url("http://node-1:8080", false));
        assert!(is_valid_leader_url(
            "https://node-1.cluster.internal:8080",
            false,
        ));
        assert!(is_valid_leader_url("http://10.0.1.5:9090/", false));
    }

    #[test]
    fn leader_url_validation_rejects_other_schemes_and_garbage() {
        // Schemes the forwarder must not honor even if a buggy
        // membership update placed them there.
        assert!(!is_valid_leader_url("file:///etc/passwd", false));
        assert!(!is_valid_leader_url("ftp://node-1:21", false));
        assert!(!is_valid_leader_url("javascript:alert(1)", false));
        assert!(!is_valid_leader_url("data:text/plain,hi", false));
        // Plain garbage.
        assert!(!is_valid_leader_url("", false));
        assert!(!is_valid_leader_url("not a url", false));
    }

    #[test]
    fn leader_url_validation_rejects_ssrf_targets_in_prod_posture() {
        // `allow_loopback = false` — the multi-host posture.
        // Loopback — IPv4 + IPv6 + hostname.
        assert!(!is_valid_leader_url("http://127.0.0.1:8080", false));
        assert!(!is_valid_leader_url("http://127.0.0.5:8080", false));
        assert!(!is_valid_leader_url("http://[::1]:8080", false));
        assert!(!is_valid_leader_url("http://localhost:8080", false));
        assert!(!is_valid_leader_url("http://LOCALHOST:8080", false));

        // Link-local — AWS / GCP / Azure metadata services and any
        // 169.254.x.y peer.
        assert!(!is_valid_leader_url("http://169.254.169.254/", false));
        assert!(!is_valid_leader_url("http://169.254.0.1:8080", false));
        assert!(!is_valid_leader_url("http://[fe80::1]:8080", false));

        // Unspecified — kernel routes 0.0.0.0 / :: to a local
        // interface, same effective risk as loopback.
        assert!(!is_valid_leader_url("http://0.0.0.0:8080", false));
        assert!(!is_valid_leader_url("http://[::]:8080", false));

        // IPv4-mapped IPv6 — the kernel routes these to the mapped
        // IPv4 address, so they must be judged by the v4 rules, not
        // pass through as opaque v6 literals.
        assert!(!is_valid_leader_url(
            "http://[::ffff:169.254.169.254]/",
            false
        ));
        assert!(!is_valid_leader_url(
            "http://[::ffff:127.0.0.1]:8080",
            false
        ));
        assert!(!is_valid_leader_url("http://[::ffff:0.0.0.0]:8080", false));
        // And still rejected even in the single-host posture, where
        // only genuine loopback is meant to be allowed.
        assert!(!is_valid_leader_url(
            "http://[::ffff:169.254.169.254]/",
            true
        ));
    }

    #[test]
    fn leader_url_validation_permits_loopback_on_single_host_clusters() {
        // `allow_loopback = true` — the single-host posture the
        // forwarder enters when self's own client_addr is loopback.
        // Loopback variants pass.
        assert!(is_valid_leader_url("http://127.0.0.1:8080", true));
        assert!(is_valid_leader_url("http://127.0.0.5:8080", true));
        assert!(is_valid_leader_url("http://[::1]:8080", true));
        assert!(is_valid_leader_url("http://localhost:8080", true));
        assert!(is_valid_leader_url("http://LOCALHOST:8080", true));

        // Link-local and unspecified stay rejected — different
        // threat (metadata services / kernel routing), not gated by
        // single-host vs multi-host.
        assert!(!is_valid_leader_url("http://169.254.169.254/", true));
        assert!(!is_valid_leader_url("http://[fe80::1]:8080", true));
        assert!(!is_valid_leader_url("http://0.0.0.0:8080", true));
        assert!(!is_valid_leader_url("http://[::]:8080", true));
    }

    #[test]
    fn leader_url_validation_still_accepts_private_cluster_addresses() {
        // Private RFC1918 ranges are standard for internal clusters;
        // the SSRF deny-list doesn't include them.
        assert!(is_valid_leader_url("http://10.0.1.5:9090/", false));
        assert!(is_valid_leader_url("http://192.168.1.10:8080", false));
        assert!(is_valid_leader_url("http://172.16.0.5:8080", false));
    }

    #[test]
    fn self_addr_loopback_detection_recognizes_local_bindings() {
        // Hosts that mark this node as single-host / dev.
        assert!(self_addr_is_loopback("http://127.0.0.1:8080"));
        assert!(self_addr_is_loopback("http://[::1]:8080"));
        assert!(self_addr_is_loopback("http://localhost:8080"));
        assert!(self_addr_is_loopback("http://LOCALHOST:8080"));

        // Real interface addresses → multi-host posture.
        assert!(!self_addr_is_loopback("http://10.0.1.5:8080"));
        assert!(!self_addr_is_loopback(
            "http://node-1.cluster.internal:8080"
        ));
        assert!(!self_addr_is_loopback("http://192.168.1.10:8080"));

        // Garbage / non-http schemes → conservative false (no opt-in).
        assert!(!self_addr_is_loopback(""));
        assert!(!self_addr_is_loopback("not a url"));
        assert!(!self_addr_is_loopback("file:///etc/passwd"));
    }

    #[test]
    fn incoming_hop_count_defaults_to_zero() {
        let h = HeaderMap::new();
        assert_eq!(incoming_hop_count(&h), 0);
    }

    #[test]
    fn incoming_hop_count_parses_decimal() {
        let mut h = HeaderMap::new();
        h.insert(FORWARD_HOPS_HEADER, "1".parse().unwrap());
        assert_eq!(incoming_hop_count(&h), 1);
        h.insert(FORWARD_HOPS_HEADER, "42".parse().unwrap());
        assert_eq!(incoming_hop_count(&h), 42);
    }

    #[test]
    fn incoming_hop_count_malformed_falls_back_to_zero() {
        // A hostile / buggy client can't bypass the guard by
        // sending unparseable values — we treat them as "fresh."
        let mut h = HeaderMap::new();
        h.insert(FORWARD_HOPS_HEADER, "not-a-number".parse().unwrap());
        assert_eq!(incoming_hop_count(&h), 0);
        h.insert(FORWARD_HOPS_HEADER, "-1".parse().unwrap());
        assert_eq!(incoming_hop_count(&h), 0);
    }
}

// ─── Follower-side propose ───────────────────────────────────────────────

/// Why [`propose_via_leader`] could not land a command.
#[derive(Debug, thiserror::Error)]
pub enum ProposeError {
    /// No leader is known — mid-election, or the group has no quorum.
    #[error("no leader known for this group; retry once one is elected")]
    NoLeader,
    /// The membership-recorded leader address failed [`is_valid_leader_url`].
    #[error("leader address rejected: {0}")]
    BadLeaderAddress(String),
    /// The relay could not reach the leader, or the leader refused.
    #[error("propose relay failed: {0}")]
    Relay(String),
    /// The local or relayed apply failed with an application error.
    #[error("{0}")]
    Apply(String),
}

/// Propose `cmd` to this group from ANY node: a plain `client_write` when
/// this node leads, an HTTP relay of the JSON-encoded command to the
/// leader's network router (`{raft_addr}/propose`) when it does not. One
/// retry after a relayed "leadership moved" answer, with a fresh leader
/// lookup in between.
///
/// The wire is JSON in both directions — commands are constrained to
/// postcard-safe shapes by the log, but RESPONSES never ride the log and
/// may carry `serde_json::Value` state postcard cannot decode.
///
/// The relay dials a membership-supplied URL, so it uses a no-redirect
/// client and [`is_valid_leader_url`] — the same SSRF posture as the
/// request-forwarding middleware above.
pub async fn propose_via_leader<C>(raft: &Raft<C>, cmd: C::D) -> Result<C::R, ProposeError>
where
    C: crate::config::FlureeRaftConfig,
    C::D: Clone + serde::Serialize,
{
    let mut last_relay_error: Option<ProposeError> = None;
    for _attempt in 0..2 {
        match raft.client_write(cmd.clone()).await {
            Ok(resp) => return Ok(resp.data),
            Err(openraft::error::RaftError::APIError(
                openraft::error::ClientWriteError::ForwardToLeader(fwd),
            )) => {
                let Some(node) = fwd.leader_node else {
                    return Err(ProposeError::NoLeader);
                };
                match relay_propose::<C>(&node.raft_addr, &cmd).await {
                    Ok(data) => return Ok(data),
                    Err(e @ ProposeError::Relay(_)) => {
                        // Leadership may have moved mid-relay; loop for
                        // one fresh lookup.
                        last_relay_error = Some(e);
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
            Err(openraft::error::RaftError::APIError(
                openraft::error::ClientWriteError::ChangeMembershipError(e),
            )) => return Err(ProposeError::Apply(format!("membership error: {e}"))),
            Err(openraft::error::RaftError::Fatal(f)) => {
                return Err(ProposeError::Apply(format!("raft fatal: {f}")))
            }
        }
    }
    Err(last_relay_error.unwrap_or(ProposeError::NoLeader))
}

async fn relay_propose<C>(leader_raft_addr: &str, cmd: &C::D) -> Result<C::R, ProposeError>
where
    C: crate::config::FlureeRaftConfig,
    C::D: serde::Serialize,
{
    if !is_valid_leader_url(leader_raft_addr, true) {
        return Err(ProposeError::BadLeaderAddress(leader_raft_addr.to_string()));
    }
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    let client = CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            // Membership-supplied URL: never follow a redirect off it.
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("propose relay client builds")
    });
    let url = format!("{}/propose", leader_raft_addr.trim_end_matches('/'));
    let body = serde_json::to_vec(cmd)
        .map_err(|e| ProposeError::Apply(format!("command encode error: {e}")))?;
    let resp = client
        .post(&url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .map_err(|e| ProposeError::Relay(format!("POST {url}: {e}")))?;
    let status = resp.status();
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| ProposeError::Relay(format!("read {url}: {e}")))?;
    if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
        return Err(ProposeError::Relay(format!(
            "leader at {url} stepped down mid-relay"
        )));
    }
    if !status.is_success() {
        return Err(ProposeError::Apply(format!(
            "{url} answered {status}: {}",
            String::from_utf8_lossy(&bytes)
        )));
    }
    serde_json::from_slice(&bytes)
        .map_err(|e| ProposeError::Apply(format!("response decode error: {e}")))
}
