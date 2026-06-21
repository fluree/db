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
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

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
            Some(node) if is_valid_leader_url(&node.client_addr) => {
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
/// - **Loopback** (`127.0.0.0/8`, `::1`): a follower pointing
///   `client_addr` at its own localhost would loop back the write
///   into whatever local service answers that port (Postgres at
///   5432, an admin endpoint, etc.).
/// - **Link-local** (`169.254.0.0/16`, `fe80::/10`): notably AWS /
///   GCP / Azure instance metadata services at `169.254.169.254`,
///   which return cloud credentials.
/// - **Unspecified** (`0.0.0.0`, `::`): kernel routes these to a
///   local interface, same effective risk as loopback.
/// - The literal hostname `"localhost"`.
///
/// Hostnames are not resolved here — a hostname that resolves to a
/// denied IP at DNS time still passes, and the kernel handles the
/// rest. The intent is to catch the straight-line mistake of putting
/// a literal SSRF address into the membership record; an active
/// adversary controlling DNS for cluster hostnames is out of scope
/// of this check and needs to be addressed at a different layer.
fn is_valid_leader_url(url: &str) -> bool {
    if url.is_empty() {
        return false;
    }
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    if !matches!(parsed.scheme(), "http" | "https") || !parsed.has_host() {
        return false;
    }
    let Some(host) = parsed.host_str() else {
        return false;
    };
    // `Url::host_str` returns IPv6 addresses with the URL-syntax
    // `[...]` brackets in place. Strip them so the IP parser sees
    // a bare address; non-IPv6 hosts pass through unchanged.
    let bare = host
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host);
    !is_ssrf_host(bare)
}

/// Returns true for hosts that should never be a legitimate cluster
/// peer: loopback / link-local / unspecified IPs, or the literal
/// hostname `"localhost"`. See [`is_valid_leader_url`] for the full
/// rationale.
fn is_ssrf_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    let Ok(ip) = host.parse::<IpAddr>() else {
        return false;
    };
    if ip.is_loopback() || ip.is_unspecified() {
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
            forward_request(&forwarder.client, &leader_url, request, hops + 1)
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
    fn timeout_error_maps_to_gateway_timeout() {
        let resp = ForwardError::Timeout.into_response();
        assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);
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
        assert!(is_valid_leader_url("http://node-1:8080"));
        assert!(is_valid_leader_url("https://node-1.cluster.internal:8080"));
        assert!(is_valid_leader_url("http://10.0.1.5:9090/"));
    }

    #[test]
    fn leader_url_validation_rejects_other_schemes_and_garbage() {
        // Schemes the forwarder must not honor even if a buggy
        // membership update placed them there.
        assert!(!is_valid_leader_url("file:///etc/passwd"));
        assert!(!is_valid_leader_url("ftp://node-1:21"));
        assert!(!is_valid_leader_url("javascript:alert(1)"));
        assert!(!is_valid_leader_url("data:text/plain,hi"));
        // Plain garbage.
        assert!(!is_valid_leader_url(""));
        assert!(!is_valid_leader_url("not a url"));
    }

    #[test]
    fn leader_url_validation_rejects_ssrf_targets() {
        // Loopback — IPv4 + IPv6 + hostname.
        assert!(!is_valid_leader_url("http://127.0.0.1:8080"));
        assert!(!is_valid_leader_url("http://127.0.0.5:8080"));
        assert!(!is_valid_leader_url("http://[::1]:8080"));
        assert!(!is_valid_leader_url("http://localhost:8080"));
        assert!(!is_valid_leader_url("http://LOCALHOST:8080"));

        // Link-local — AWS / GCP / Azure metadata services and any
        // 169.254.x.y peer.
        assert!(!is_valid_leader_url("http://169.254.169.254/"));
        assert!(!is_valid_leader_url("http://169.254.0.1:8080"));
        assert!(!is_valid_leader_url("http://[fe80::1]:8080"));

        // Unspecified — kernel routes 0.0.0.0 / :: to a local
        // interface, same effective risk as loopback.
        assert!(!is_valid_leader_url("http://0.0.0.0:8080"));
        assert!(!is_valid_leader_url("http://[::]:8080"));
    }

    #[test]
    fn leader_url_validation_still_accepts_private_cluster_addresses() {
        // Private RFC1918 ranges are standard for internal clusters;
        // the SSRF deny-list doesn't include them.
        assert!(is_valid_leader_url("http://10.0.1.5:9090/"));
        assert!(is_valid_leader_url("http://192.168.1.10:8080"));
        assert!(is_valid_leader_url("http://172.16.0.5:8080"));
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
