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

use crate::config::FlureeRaftConfig;
use crate::node::{ClusterNode, NodeId};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use openraft::error::{
    Fatal, InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{AnyError, Raft};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
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
///
/// Only fields the generic transport itself reads. Application-specific
/// RPC limits (the nameservice's staged-commit and queue-poison routes,
/// for instance) belong on the application's own config struct, which
/// typically embeds this one.
#[derive(Clone, Debug)]
pub struct RaftTransportConfig {
    /// Per-request timeout for append-entries + vote. openraft's
    /// replication loop drives retry / backoff on top of this.
    pub rpc_timeout: Duration,
    /// Per-request timeout for install-snapshot. Snapshots can be
    /// large; size this larger than `rpc_timeout`.
    pub snapshot_timeout: Duration,
    /// Maximum buffered body size accepted on the `vote` route.
    /// `VoteRequest` is a fixed-size record (a few dozen postcard-
    /// encoded bytes); a tighter cap than the other two RPCs keeps a
    /// compromised peer from flooding the vote endpoint with
    /// max-sized blobs that get decoded as garbage.
    pub vote_max_body_bytes: usize,
    /// Maximum buffered body size accepted on the `append-entries`
    /// route. Bounded by openraft's `max_payload_entries` ×
    /// per-entry size; the default leaves comfortable headroom for
    /// large catch-up batches without letting an oversized RPC OOM
    /// the process.
    pub append_entries_max_body_bytes: usize,
    /// Maximum buffered body size accepted on the `install-snapshot`
    /// route. The snapshot is the full replicated state-machine
    /// image (or a chunk of it under chunked transport), and
    /// realistically grows with cluster lifetime. The default
    /// tolerates a sizable cluster; operators with larger state
    /// raise this knob.
    pub install_snapshot_max_body_bytes: usize,
    /// Maximum request body a follower buffers before relaying a
    /// client request to the leader ([`crate::forward`]).
    /// Bodies beyond the cap are refused with 413 before any relay.
    /// Deployments should align this with the public routes' body
    /// limit — a larger value has the follower buffering bodies the
    /// leader will refuse anyway, a smaller one rejects follower-side
    /// what the leader would accept.
    pub forward_max_body_bytes: usize,
}

impl Default for RaftTransportConfig {
    fn default() -> Self {
        Self {
            rpc_timeout: Duration::from_millis(500),
            snapshot_timeout: Duration::from_secs(30),
            vote_max_body_bytes: 1024 * 1024,
            append_entries_max_body_bytes: 64 * 1024 * 1024,
            install_snapshot_max_body_bytes: 1024 * 1024 * 1024,
            forward_max_body_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Settings baked into the shared `reqwest::Client`.
///
/// Separate from [`RaftTransportConfig`] because these are properties
/// of the *client*, not of a request: once a client exists they cannot
/// vary per group. Keeping them on their own struct means a co-hosted
/// deployment configures them once, rather than each group declaring a
/// connect timeout and silently getting whichever group's value
/// happened to build the shared client.
#[derive(Clone, Debug)]
pub struct HttpClientConfig {
    /// TCP connect timeout. Independent of the per-request timeout so a
    /// dead peer fails fast rather than blocking the replication tick.
    pub connect_timeout: Duration,
    /// How long an idle pooled connection is kept before being closed.
    pub pool_idle_timeout: Duration,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_millis(250),
            pool_idle_timeout: Duration::from_secs(90),
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
pub struct HttpRaftNetworkFactory<C> {
    client: reqwest::Client,
    config: RaftTransportConfig,
    /// `C` appears only in the trait impl, never in a field.
    _config: PhantomData<C>,
}

// Hand-written: `#[derive(Clone)]` would demand `C: Clone` for a tag type.
impl<C> Clone for HttpRaftNetworkFactory<C> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            config: self.config.clone(),
            _config: PhantomData,
        }
    }
}

impl<C> HttpRaftNetworkFactory<C> {
    /// Construct with a fresh `reqwest::Client`. Prefer
    /// [`Self::with_client`] when the process hosts more than one group,
    /// so they share a connection pool.
    ///
    /// Errors only if the reqwest builder rejects the configuration
    /// (very rare).
    pub fn new(
        config: RaftTransportConfig,
        client_config: &HttpClientConfig,
    ) -> Result<Self, reqwest::Error> {
        let client = build_client(client_config)?;
        Ok(Self {
            client,
            config,
            _config: PhantomData,
        })
    }

    /// Construct from an externally-built client. Use when the
    /// embedder wants to share connection pools / proxy config /
    /// custom TLS roots across raft traffic and other HTTP traffic.
    pub fn with_client(client: reqwest::Client, config: RaftTransportConfig) -> Self {
        Self {
            client,
            config,
            _config: PhantomData,
        }
    }
}

/// Build a `reqwest::Client` configured for raft HTTP transport, with
/// redirects disabled — which closes SSRF via a 302 to an internal
/// address such as a cloud instance-metadata endpoint.
///
/// Free rather than an associated function on the factory: the client
/// is independent of the type config, and one client is meant to be
/// shared across every group in the process. Making callers name a `C`
/// just to build it would imply otherwise.
pub fn build_client(config: &HttpClientConfig) -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .connect_timeout(config.connect_timeout)
        .pool_idle_timeout(Some(config.pool_idle_timeout))
        .redirect(reqwest::redirect::Policy::none())
        .build()
}

impl<C: FlureeRaftConfig> RaftNetworkFactory<C> for HttpRaftNetworkFactory<C> {
    type Network = HttpRaftNetwork<C>;

    async fn new_client(&mut self, target: NodeId, node: &ClusterNode) -> Self::Network {
        HttpRaftNetwork {
            client: self.client.clone(),
            config: self.config.clone(),
            target,
            base_url: node.raft_addr.trim_end_matches('/').to_string(),
            _config: PhantomData,
        }
    }
}

/// Per-peer [`RaftNetwork`]. One instance per `(target, base_url)`
/// tuple; constructed by [`HttpRaftNetworkFactory::new_client`].
pub struct HttpRaftNetwork<C> {
    client: reqwest::Client,
    config: RaftTransportConfig,
    target: NodeId,
    base_url: String,
    _config: PhantomData<C>,
}

impl<C> HttpRaftNetwork<C> {
    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    /// Encode the request, send the POST, and decode the response.
    ///
    /// A successful HTTP response carries `postcard(Result<Resp, W>)`:
    /// the peer's *logical* outcome. `Ok` is the response; `Err(w)`
    /// is a remote error, reconstructed into `RaftError` by
    /// `into_raft_error` and surfaced as [`RPCError::RemoteError`] so
    /// openraft's classifier can act on it (retry an `Unreachable`,
    /// restart snapshot streaming on a `SnapshotMismatch`, stop on a
    /// `Fatal`) rather than seeing an opaque transport failure.
    /// Genuine transport problems — connect/timeout, a non-2xx
    /// status (the peer couldn't decode the request), a truncated or
    /// undecodable body — stay [`RPCError::Network`]/`Unreachable`.
    async fn post<Req, Resp, E, W>(
        &self,
        path: &str,
        req: &Req,
        timeout: Duration,
        into_raft_error: impl FnOnce(W) -> RaftError<NodeId, E>,
    ) -> Result<Resp, RPCError<NodeId, ClusterNode, RaftError<NodeId, E>>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        E: std::error::Error + Send + Sync + 'static,
        W: DeserializeOwned,
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

        let outcome: Result<Resp, W> = postcard::from_bytes(&bytes).map_err(|e| {
            RPCError::Network(NetworkError::new(&AnyError::new(&PostcardError(
                e.to_string(),
            ))))
        })?;

        outcome
            .map_err(|w| RPCError::RemoteError(RemoteError::new(self.target, into_raft_error(w))))
    }
}

impl<C: FlureeRaftConfig> RaftNetwork<C> for HttpRaftNetwork<C> {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, ClusterNode, RaftError<NodeId>>>
    {
        // append/vote can only fail with `Fatal` (their API-error
        // slot is `Infallible`), which `serde` can't deserialize, so
        // the wire error is a bare `Fatal` rebuilt into `RaftError`.
        self.post(
            PATH_APPEND_ENTRIES,
            &rpc,
            self.config.rpc_timeout,
            RaftError::Fatal,
        )
        .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, ClusterNode, RaftError<NodeId>>> {
        self.post(PATH_VOTE, &rpc, self.config.rpc_timeout, RaftError::Fatal)
            .await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<C>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, ClusterNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        // install_snapshot's error is inhabited (`SnapshotMismatch`),
        // so the full `RaftError` rides the wire and passes through
        // unchanged.
        self.post(
            PATH_INSTALL_SNAPSHOT,
            &rpc,
            self.config.snapshot_timeout,
            |e| e,
        )
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
/// Each route gets its own body-byte cap from `config` so a single
/// oversized POST can't OOM the process: vote bodies are tiny so
/// they get the tightest cap; append-entries scales with the batch
/// size openraft replicates; install-snapshot tolerates the full
/// state-machine image. Bodies past the cap return 413; the
/// `Bytes` extractor never buffers more than the route allows.
/// Defaults live on [`RaftTransportConfig`].
///
/// The returned router is **relative**: it carries no prefix of its
/// own. The host nests it — at `/raft` for a single group, or at
/// `/raft/<group_id>` when several groups share a process — which is
/// what lets an existing group keep its historical paths (its
/// `ClusterNode::raft_addr` URLs are already in replicated state) while
/// new groups take prefixed ones.
///
/// Example:
/// ```ignore
/// let raft = Arc::clone(&raft_handle);
/// let config = RaftTransportConfig::default();
/// let app = axum::Router::new()
///     .nest("/raft", fluree_raft_core::network::router(raft, &config));
/// ```
pub fn router<C: FlureeRaftConfig>(raft: Arc<Raft<C>>, config: &RaftTransportConfig) -> Router {
    Router::new()
        .route(
            PATH_APPEND_ENTRIES,
            post(handle_append_entries::<C>)
                .layer(DefaultBodyLimit::max(config.append_entries_max_body_bytes)),
        )
        .route(
            PATH_VOTE,
            post(handle_vote::<C>).layer(DefaultBodyLimit::max(config.vote_max_body_bytes)),
        )
        .route(
            PATH_INSTALL_SNAPSHOT,
            post(handle_install_snapshot::<C>).layer(DefaultBodyLimit::max(
                config.install_snapshot_max_body_bytes,
            )),
        )
        .with_state(raft)
}

/// Decode a postcard-encoded request body. Failure → 400.
///
/// The error variant boxes the response so the `Result` itself
/// stays small even though `axum::http::Response<Body>` is
/// ~128 bytes — the happy path of every Raft RPC handler ends up
/// pattern-matching this Result, and the unboxed version pads the
/// good case with the size of the bad case.
fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T, Box<Response>> {
    postcard::from_bytes(body).map_err(|e| {
        Box::new(
            (
                StatusCode::BAD_REQUEST,
                format!("postcard decode error: {e}"),
            )
                .into_response(),
        )
    })
}

/// Encode a logical outcome — `postcard(Result<Resp, E>)` — as a 200
/// response. The `Err` side carries the peer's typed error so the
/// caller can reconstruct it (see [`HttpRaftNetwork::post`]);
/// encoding itself failing is a genuine 500. A non-2xx status is
/// thus reserved for the request never being understood at all
/// (decode failure below), which the caller reads as a transport
/// error rather than a logical one.
fn respond<T, E>(outcome: Result<T, E>) -> Response
where
    T: Serialize,
    E: Serialize,
{
    match postcard::to_allocvec(&outcome) {
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

/// Collapse `RaftError<NodeId>` to its only inhabited variant. The
/// `APIError` slot is `Infallible` for append-entries and vote, so
/// this never hits the unreachable arm; extracting `Fatal` lets it
/// ride the wire (`Infallible` isn't `Deserialize`).
fn into_fatal(err: RaftError<NodeId>) -> Fatal<NodeId> {
    match err {
        RaftError::Fatal(fatal) => fatal,
        RaftError::APIError(infallible) => match infallible {},
    }
}

async fn handle_append_entries<C: FlureeRaftConfig>(
    State(raft): State<Arc<Raft<C>>>,
    body: axum::body::Bytes,
) -> Response {
    let rpc: AppendEntriesRequest<C> = match decode(&body) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };
    respond(raft.append_entries(rpc).await.map_err(into_fatal))
}

async fn handle_vote<C: FlureeRaftConfig>(
    State(raft): State<Arc<Raft<C>>>,
    body: axum::body::Bytes,
) -> Response {
    let rpc: VoteRequest<NodeId> = match decode(&body) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };
    respond(raft.vote(rpc).await.map_err(into_fatal))
}

async fn handle_install_snapshot<C: FlureeRaftConfig>(
    State(raft): State<Arc<Raft<C>>>,
    body: axum::body::Bytes,
) -> Response {
    let rpc: InstallSnapshotRequest<C> = match decode(&body) {
        Ok(v) => v,
        Err(resp) => return *resp,
    };
    respond(raft.install_snapshot(rpc).await)
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
    use std::io::Cursor;

    openraft::declare_raft_types!(
        /// Stands in for a real application config. The envelope shape
        /// pinned here is the part every group shares; each application
        /// is responsible for pinning its own `D`/`R` encoding.
        pub TestConfig:
            D = String,
            R = String,
            NodeId = NodeId,
            Node = ClusterNode,
            Entry = openraft::Entry<TestConfig>,
            SnapshotData = Cursor<Vec<u8>>,
            AsyncRuntime = openraft::TokioRuntime,
    );

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
        let rpc: AppendEntriesRequest<TestConfig> = AppendEntriesRequest {
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
        let rpc: InstallSnapshotRequest<TestConfig> = InstallSnapshotRequest {
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

    /// The response envelope is `Result<Resp, Err>`, not a bare
    /// `Resp` — the `Ok` side must round-trip in that shape (this is
    /// what the client decodes on every successful RPC).
    #[test]
    fn append_response_envelope_ok_round_trips() {
        let outcome: Result<AppendEntriesResponse<NodeId>, Fatal<NodeId>> =
            Ok(AppendEntriesResponse::Success);
        round_trip(&outcome);
    }

    /// The typed error side round-trips too, so a peer's `Fatal`
    /// reaches the caller as a `Fatal` (→ `RPCError::RemoteError`)
    /// rather than an opaque HTTP status.
    #[test]
    fn append_response_envelope_fatal_err_round_trips() {
        let outcome: Result<AppendEntriesResponse<NodeId>, Fatal<NodeId>> = Err(Fatal::Stopped);
        round_trip(&outcome);
        // And the peer's server-side `RaftError<NodeId>` collapses
        // to exactly this `Fatal` on the way onto the wire.
        assert!(matches!(
            into_fatal(RaftError::Fatal(Fatal::Stopped)),
            Fatal::Stopped
        ));
    }

    /// `install_snapshot`'s error is inhabited (`SnapshotMismatch`);
    /// the full `RaftError` rides the wire so openraft's snapshot
    /// sender can restart streaming instead of wedging.
    #[test]
    fn install_snapshot_envelope_mismatch_err_round_trips() {
        use openraft::error::{InstallSnapshotError, SnapshotMismatch};
        use openraft::SnapshotSegmentId;

        let mismatch = SnapshotMismatch {
            expect: SnapshotSegmentId {
                id: "snap-1".to_string(),
                offset: 0,
            },
            got: SnapshotSegmentId {
                id: "snap-1".to_string(),
                offset: 4096,
            },
        };
        let outcome: Result<
            InstallSnapshotResponse<NodeId>,
            RaftError<NodeId, InstallSnapshotError>,
        > = Err(RaftError::APIError(InstallSnapshotError::SnapshotMismatch(
            mismatch,
        )));
        round_trip(&outcome);
    }

    #[test]
    fn factory_builds_with_default_config() {
        let _ = HttpRaftNetworkFactory::<TestConfig>::new(
            RaftTransportConfig::default(),
            &HttpClientConfig::default(),
        )
        .expect("reqwest client builds with default timeouts");
    }

    #[test]
    fn default_body_caps_match_expected_route_profiles() {
        // Pin the per-route caps so a refactor can't quietly widen
        // them past the documented profiles: vote stays tight
        // because the body is fixed-size; append-entries scales
        // with batch size; install-snapshot tolerates the full
        // state-machine image.
        let cfg = RaftTransportConfig::default();
        assert_eq!(cfg.vote_max_body_bytes, 1024 * 1024);
        assert_eq!(cfg.append_entries_max_body_bytes, 64 * 1024 * 1024);
        assert_eq!(cfg.install_snapshot_max_body_bytes, 1024 * 1024 * 1024);
        // Ordering invariant — vote ≤ append-entries ≤ install-snapshot.
        // A future change that flips this is almost certainly a bug.
        assert!(cfg.vote_max_body_bytes <= cfg.append_entries_max_body_bytes);
        assert!(cfg.append_entries_max_body_bytes <= cfg.install_snapshot_max_body_bytes);
    }
}
