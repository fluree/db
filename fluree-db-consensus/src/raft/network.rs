//! HTTP-backed [`RaftNetwork`] for inter-node Raft RPCs.
//!
//! Implements openraft's network surface (append-entries, vote,
//! install-snapshot) over a small HTTP-with-postcard wire protocol.
//! Cluster nodes run [`router`] under a private listener (typically a
//! VPC-only port distinct from the client-facing port) and connect to
//! each other via [`HttpRaftNetworkFactory`].
//!
//! # Wire shape
//!
//! Three POST endpoints, postcard-encoded request and response
//! bodies, content type `application/octet-stream`:
//!
//! - `POST <base>/append-entries`
//! - `POST <base>/vote`
//! - `POST <base>/install-snapshot`
//!
//! Postcard matches the storage layer's encoding, so the inter-node
//! and on-disk formats stay aligned. JSON debuggability isn't a real
//! win for inter-node traffic that humans rarely inspect.
//!
//! # Auth + transport
//!
//! v1 expects the cluster to run behind a VPC / private network. The
//! handlers and client do **no auth checks** of their own — operators
//! enforce trust at the network layer (security-group rule allowing
//! the Raft port only from peer instance IPs). TLS termination, if
//! desired, lives at the embedding HTTP server.
//!
//! # Routing
//!
//! [`ClusterNode::raft_addr`] carries the peer's base URL — for
//! example `http://node-2:9090/raft`. The factory derives endpoint
//! URLs by appending `/append-entries`, `/vote`, `/install-snapshot`.

use crate::raft::{ClusterNode, NodeId, TypeConfig};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{AnyError, Raft};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use std::time::Duration;

const PATH_APPEND_ENTRIES: &str = "/append-entries";
const PATH_VOTE: &str = "/vote";
const PATH_INSTALL_SNAPSHOT: &str = "/install-snapshot";

const POSTCARD_MIME: &str = "application/octet-stream";

// ============================================================================
// Config
// ============================================================================

/// Tuning knobs for the inter-node HTTP transport.
#[derive(Clone, Debug)]
pub struct NetworkConfig {
    /// Per-request timeout for append-entries + vote. openraft's
    /// replication loop drives retry / backoff on top of this.
    pub rpc_timeout: Duration,
    /// Per-request timeout for install-snapshot. Snapshots can be
    /// large; size this larger than `rpc_timeout`.
    pub snapshot_timeout: Duration,
    /// HTTP connect timeout. Independent of the request timeout so a
    /// dead peer fails fast rather than blocking the replication tick.
    pub connect_timeout: Duration,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            rpc_timeout: Duration::from_millis(500),
            snapshot_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_millis(250),
        }
    }
}

// ============================================================================
// Factory + per-peer client
// ============================================================================

/// Factory for per-peer [`HttpRaftNetwork`] instances.
///
/// Holds a shared `reqwest::Client` so all per-peer instances reuse a
/// single connection pool. Cheap to clone (Arc-internals).
#[derive(Clone)]
pub struct HttpRaftNetworkFactory {
    client: reqwest::Client,
    config: NetworkConfig,
}

impl HttpRaftNetworkFactory {
    /// Construct with a fresh `reqwest::Client` configured against
    /// `config`'s timeouts. Errors only if the reqwest builder
    /// rejects the configuration (very rare).
    pub fn new(config: NetworkConfig) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .pool_idle_timeout(Some(Duration::from_secs(90)))
            .build()?;
        Ok(Self { client, config })
    }

    /// Construct from an externally-built client. Use when the
    /// embedder wants to share connection pools / proxy config /
    /// custom TLS roots across raft traffic and other HTTP traffic.
    pub fn with_client(client: reqwest::Client, config: NetworkConfig) -> Self {
        Self { client, config }
    }
}

impl RaftNetworkFactory<TypeConfig> for HttpRaftNetworkFactory {
    type Network = HttpRaftNetwork;

    async fn new_client(&mut self, target: NodeId, node: &ClusterNode) -> Self::Network {
        HttpRaftNetwork {
            client: self.client.clone(),
            config: self.config.clone(),
            target,
            base_url: node.raft_addr.trim_end_matches('/').to_string(),
        }
    }
}

/// Per-peer [`RaftNetwork`]. One instance per `(target, base_url)`
/// tuple; constructed by [`HttpRaftNetworkFactory::new_client`].
pub struct HttpRaftNetwork {
    client: reqwest::Client,
    config: NetworkConfig,
    target: NodeId,
    base_url: String,
}

impl HttpRaftNetwork {
    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    /// Encode the request body, send the POST, decode the response
    /// body. All transport-level failures map to [`RPCError`] in the
    /// shape openraft expects.
    async fn post<Req, Resp, E>(
        &self,
        path: &str,
        req: &Req,
        timeout: Duration,
    ) -> Result<Resp, RPCError<NodeId, ClusterNode, RaftError<NodeId, E>>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        E: std::error::Error + Send + Sync + 'static,
    {
        let body = postcard::to_allocvec(req).map_err(|e| {
            RPCError::Network(NetworkError::new(&AnyError::new(&PostcardError(
                e.to_string(),
            ))))
        })?;

        let resp = self
            .client
            .post(self.url(path))
            .header(reqwest::header::CONTENT_TYPE, POSTCARD_MIME)
            .timeout(timeout)
            .body(body)
            .send()
            .await
            .map_err(|e| classify_reqwest_error(&self.target, e))?;

        let status = resp.status();
        if !status.is_success() {
            let body_bytes = resp.bytes().await.unwrap_or_default();
            return Err(RPCError::Network(NetworkError::new(&AnyError::new(
                &HttpStatusError {
                    status: status.as_u16(),
                    body: String::from_utf8_lossy(&body_bytes).into_owned(),
                },
            ))));
        }

        let bytes = resp.bytes().await.map_err(|e| {
            RPCError::Network(NetworkError::new(&AnyError::new(&HttpReadBodyError(
                e.to_string(),
            ))))
        })?;

        postcard::from_bytes(&bytes).map_err(|e| {
            RPCError::Network(NetworkError::new(&AnyError::new(&PostcardError(
                e.to_string(),
            ))))
        })
    }
}

impl RaftNetwork<TypeConfig> for HttpRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, ClusterNode, RaftError<NodeId>>>
    {
        self.post(PATH_APPEND_ENTRIES, &rpc, self.config.rpc_timeout)
            .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, ClusterNode, RaftError<NodeId>>> {
        self.post(PATH_VOTE, &rpc, self.config.rpc_timeout).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, ClusterNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.post(PATH_INSTALL_SNAPSHOT, &rpc, self.config.snapshot_timeout)
            .await
    }
}

// ============================================================================
// Error wrappers (postcard + raw HTTP errors lifted into AnyError)
// ============================================================================

#[derive(Debug)]
struct PostcardError(String);
impl std::fmt::Display for PostcardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "postcard codec error: {}", self.0)
    }
}
impl std::error::Error for PostcardError {}

#[derive(Debug)]
struct HttpReadBodyError(String);
impl std::fmt::Display for HttpReadBodyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "http read body error: {}", self.0)
    }
}
impl std::error::Error for HttpReadBodyError {}

#[derive(Debug)]
struct HttpStatusError {
    status: u16,
    body: String,
}
impl std::fmt::Display for HttpStatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "http {} from peer: {}", self.status, self.body)
    }
}
impl std::error::Error for HttpStatusError {}

/// Map a `reqwest::Error` to the correct [`RPCError`] variant.
///
/// Connection failures (`is_connect`) and timeouts (`is_timeout`) map
/// to [`Unreachable`] so openraft's backoff kicks in. Everything else
/// is a generic [`NetworkError`].
fn classify_reqwest_error<E>(
    _target: &NodeId,
    err: reqwest::Error,
) -> RPCError<NodeId, ClusterNode, RaftError<NodeId, E>>
where
    E: std::error::Error + Send + Sync + 'static,
{
    let any = AnyError::new(&err);
    if err.is_connect() || err.is_timeout() {
        RPCError::Unreachable(Unreachable::new(&any))
    } else {
        RPCError::Network(NetworkError::new(&any))
    }
}

// ============================================================================
// Server side: axum router
// ============================================================================

/// Build an `axum::Router` exposing the three Raft RPC endpoints
/// against the supplied [`Raft`] handle.
///
/// Mount under whatever prefix you want — `/raft` is the convention.
/// The router has no auth middleware of its own; v1 trusts the
/// network layer (VPC-only port). Embedders can layer their own
/// auth, TLS, metrics, etc. on top.
///
/// Example:
/// ```ignore
/// let raft = Arc::clone(&raft_handle);
/// let app = axum::Router::new()
///     .nest("/raft", fluree_db_consensus::raft::network::router(raft));
/// ```
pub fn router(raft: Arc<Raft<TypeConfig>>) -> Router {
    Router::new()
        .route(PATH_APPEND_ENTRIES, post(handle_append_entries))
        .route(PATH_VOTE, post(handle_vote))
        .route(
            PATH_INSTALL_SNAPSHOT,
            // Axum's default body limit is 2 MiB — well below a
            // realistic state-machine snapshot. The Raft network
            // listens on a private port we trust, so disable the
            // cap entirely and let the per-call snapshot timeout
            // (in `NetworkConfig`) bound the transfer instead.
            post(handle_install_snapshot).layer(DefaultBodyLimit::disable()),
        )
        .with_state(raft)
}

/// Decode a postcard-encoded request body. Failure → 400.
fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T, Response> {
    postcard::from_bytes(body).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("postcard decode error: {e}"),
        )
            .into_response()
    })
}

/// Encode a postcard response body + 200. Encoding failures → 500.
fn encode<T: Serialize>(value: &T) -> Response {
    match postcard::to_allocvec(value) {
        Ok(bytes) => (
            StatusCode::OK,
            [(reqwest::header::CONTENT_TYPE, POSTCARD_MIME)],
            bytes,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("postcard encode error: {e}"),
        )
            .into_response(),
    }
}

async fn handle_append_entries(
    State(raft): State<Arc<Raft<TypeConfig>>>,
    body: axum::body::Bytes,
) -> Response {
    let rpc: AppendEntriesRequest<TypeConfig> = match decode(&body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match raft.append_entries(rpc).await {
        Ok(resp) => encode(&resp),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("append_entries: {e}"),
        )
            .into_response(),
    }
}

async fn handle_vote(
    State(raft): State<Arc<Raft<TypeConfig>>>,
    body: axum::body::Bytes,
) -> Response {
    let rpc: VoteRequest<NodeId> = match decode(&body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match raft.vote(rpc).await {
        Ok(resp) => encode(&resp),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("vote: {e}")).into_response(),
    }
}

async fn handle_install_snapshot(
    State(raft): State<Arc<Raft<TypeConfig>>>,
    body: axum::body::Bytes,
) -> Response {
    let rpc: InstallSnapshotRequest<TypeConfig> = match decode(&body) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match raft.install_snapshot(rpc).await {
        Ok(resp) => encode(&resp),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("install_snapshot: {e}"),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    //! Lock the postcard wire format for every RPC type — round-trip
    //! encode→decode each request and response struct so future
    //! openraft bumps or type-config tweaks can't silently break the
    //! inter-node protocol.
    //!
    //! Multi-node integration tests live alongside cluster bootstrap;
    //! the goal here is just to pin the on-the-wire shape.

    use super::*;
    use openraft::{
        CommittedLeaderId, Entry, EntryPayload, LogId, SnapshotMeta, StoredMembership, Vote,
    };

    /// Encode → decode → re-encode and assert the two encodings
    /// match. This catches codec asymmetry without requiring the
    /// openraft types to implement `PartialEq` (most don't).
    fn round_trip<T: Serialize + DeserializeOwned>(value: &T) {
        let bytes = postcard::to_allocvec(value).expect("encode");
        let decoded: T = postcard::from_bytes(&bytes).expect("decode");
        let reencoded = postcard::to_allocvec(&decoded).expect("re-encode");
        assert_eq!(bytes, reencoded, "round-trip bytes mismatch");
    }

    #[test]
    fn vote_request_round_trips() {
        let v: VoteRequest<NodeId> = VoteRequest {
            vote: Vote::new(7, 42),
            last_log_id: Some(LogId {
                leader_id: CommittedLeaderId::new(7, 42),
                index: 11,
            }),
        };
        round_trip(&v);
    }

    #[test]
    fn vote_response_round_trips() {
        let v: VoteResponse<NodeId> = VoteResponse {
            vote: Vote::new(7, 42),
            vote_granted: true,
            last_log_id: None,
        };
        round_trip(&v);
    }

    #[test]
    fn append_entries_request_round_trips_with_blank_entry() {
        let rpc: AppendEntriesRequest<TypeConfig> = AppendEntriesRequest {
            vote: Vote::new(3, 1),
            prev_log_id: None,
            entries: vec![Entry {
                log_id: LogId {
                    leader_id: CommittedLeaderId::new(3, 1),
                    index: 1,
                },
                payload: EntryPayload::Blank,
            }],
            leader_commit: None,
        };
        round_trip(&rpc);
    }

    #[test]
    fn append_entries_response_round_trips() {
        let resp: AppendEntriesResponse<NodeId> = AppendEntriesResponse::Success;
        round_trip(&resp);
    }

    #[test]
    fn install_snapshot_request_round_trips() {
        let rpc: InstallSnapshotRequest<TypeConfig> = InstallSnapshotRequest {
            vote: Vote::new(5, 9),
            meta: SnapshotMeta {
                last_log_id: Some(LogId {
                    leader_id: CommittedLeaderId::new(5, 9),
                    index: 50,
                }),
                last_membership: StoredMembership::default(),
                snapshot_id: "snap-50-1".into(),
            },
            offset: 0,
            data: vec![1, 2, 3, 4],
            done: true,
        };
        round_trip(&rpc);
    }

    #[test]
    fn install_snapshot_response_round_trips() {
        let resp: InstallSnapshotResponse<NodeId> = InstallSnapshotResponse {
            vote: Vote::new(5, 9),
        };
        round_trip(&resp);
    }

    #[test]
    fn factory_builds_with_default_config() {
        let _ = HttpRaftNetworkFactory::new(NetworkConfig::default())
            .expect("reqwest client builds with default timeouts");
    }
}
