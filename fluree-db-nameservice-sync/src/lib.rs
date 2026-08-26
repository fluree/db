//! Git-like remote sync for Fluree DB nameservice
//!
//! This crate provides the sync infrastructure for replicating nameservice
//! state between Fluree instances, modeled after git's remote/fetch/pull/push
//! workflow.
//!
//! # Architecture
//!
//! - [`config`]: Remote and upstream configuration
//! - [`client`]: HTTP client for communicating with remote nameservices
//! - [`origin`]: CAS object fetcher with multi-origin fallback and integrity verification
//! - [`watch`]: Remote watch trait with SSE and polling implementations
//! - [`backoff`]: Exponential backoff utility
//! - [`error`]: Error types for sync operations
//!
//! # Dependencies
//!
//! This crate depends on `fluree-db-nameservice` for core types (`RefPublisher`,
//! `RemoteTrackingStore`, etc.) and `fluree-sse` for SSE parsing. It brings in
//! `reqwest` for HTTP — consumers that don't need sync don't pay this cost.
//!
//! # Targets
//!
//! The proxy clients ([`ProxyStorage`], [`ProxyNameService`]), the
//! [`HttpTransport`] seam, and [`verify_object_integrity`] are
//! runtime-agnostic and build for `wasm32` (a browser peer supplies its own
//! transport). Everything else — sync/pull/clone, SSE watchers, the
//! reqwest-backed transport — is native-only.

#[cfg(not(target_arch = "wasm32"))]
pub mod backoff;
#[cfg(not(target_arch = "wasm32"))]
pub mod client;
#[cfg(not(target_arch = "wasm32"))]
pub mod config;
#[cfg(not(target_arch = "wasm32"))]
pub mod driver;
pub mod error;
pub mod head_stream;
pub mod integrity;
#[cfg(not(target_arch = "wasm32"))]
pub mod origin;
#[cfg(not(target_arch = "wasm32"))]
pub mod pack_client;
pub mod proxy_nameservice;
pub mod proxy_storage;
mod server_sse;
pub mod transport;
#[cfg(all(feature = "aws", not(target_arch = "wasm32")))]
pub mod vended_s3;
pub mod watch;
#[cfg(not(target_arch = "wasm32"))]
pub mod watch_poll;
#[cfg(not(target_arch = "wasm32"))]
pub mod watch_sse;

#[cfg(not(target_arch = "wasm32"))]
pub use client::{HttpRemoteClient, RemoteNameserviceClient, RemoteSnapshot};
#[cfg(not(target_arch = "wasm32"))]
pub use config::{
    MemorySyncConfigStore, OidcLoginFlow, RemoteAuth, RemoteAuthType, RemoteConfig, RemoteEndpoint,
    SyncConfigStore, UpstreamConfig,
};
#[cfg(not(target_arch = "wasm32"))]
pub use driver::{FetchResult, PullResult, PushResult, SyncDriver};
pub use error::{Result, SyncError};
// Re-export LedgerConfig types from fluree-db-nameservice (canonical home)
pub use fluree_db_nameservice::{AuthRequirement, LedgerConfig, Origin, ReplicationDefaults};
pub use head_stream::{
    run_head_stream, BoxChunkStream, HeadSink, HeadStreamConfig, Sleeper, SseChunkSource,
    SseConnectError,
};
#[cfg(not(target_arch = "wasm32"))]
pub use head_stream::{ReqwestSseSource, TokenProvider, TokioSleeper};
pub use integrity::verify_object_integrity;
#[cfg(not(target_arch = "wasm32"))]
pub use origin::{HttpOriginFetcher, MultiOriginFetcher};
#[cfg(not(target_arch = "wasm32"))]
pub use pack_client::{
    fetch_and_ingest_pack, ingest_pack_frame, ingest_pack_stream, ingest_pack_stream_with_header,
    peek_pack_header, PackIngestResult,
};
pub use proxy_nameservice::ProxyNameService;
pub use proxy_storage::{cid_and_ledger_from_address, ProxyReadMode, ProxyStorage};
#[cfg(not(target_arch = "wasm32"))]
pub use transport::ReqwestTransport;
pub use transport::{
    HttpTransport, TransportError, TransportMethod, TransportRequest, TransportResponse,
};
pub use watch::{RemoteEvent, RemoteWatch};
#[cfg(not(target_arch = "wasm32"))]
pub use watch_poll::PollRemoteWatch;
#[cfg(not(target_arch = "wasm32"))]
pub use watch_sse::SseRemoteWatch;
