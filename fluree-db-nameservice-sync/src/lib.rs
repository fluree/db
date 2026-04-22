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

pub mod backoff;
pub mod client;
pub mod config;
pub mod driver;
pub mod error;
pub mod origin;
pub mod pack_client;
mod server_sse;
pub mod watch;
pub mod watch_poll;
pub mod watch_sse;

pub use client::{HttpRemoteClient, RemoteNameserviceClient, RemoteSnapshot};
pub use config::{
    MemorySyncConfigStore, RemoteAuth, RemoteAuthType, RemoteConfig, RemoteEndpoint,
    SyncConfigStore, UpstreamConfig,
};
pub use driver::{FetchResult, PullResult, PushResult, SyncDriver};
pub use error::{Result, SyncError};
// Re-export LedgerConfig types from fluree-db-nameservice (canonical home)
pub use fluree_db_nameservice::{AuthRequirement, LedgerConfig, Origin, ReplicationDefaults};
pub use origin::{verify_object_integrity, HttpOriginFetcher, MultiOriginFetcher};
pub use pack_client::{
    fetch_and_ingest_pack, ingest_pack_frame, ingest_pack_stream, ingest_pack_stream_with_header,
    peek_pack_header, PackIngestResult,
};
pub use watch::{RemoteEvent, RemoteWatch};
pub use watch_poll::PollRemoteWatch;
pub use watch_sse::SseRemoteWatch;
