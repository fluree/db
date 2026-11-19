//! Peer mode support
//!
//! Background SSE subscription and state management for peer mode.
//!
//! ## Storage Access Modes
//!
//! Peers can operate in two storage access modes:
//!
//! - **Shared**: Direct access to storage (shared filesystem/S3 credentials)
//! - **Proxy**: All storage reads proxied through the transaction server's
//!   `/fluree/storage/*` endpoints. This allows peers to operate without
//!   storage credentials.
//!
//! ## Proxy Mode Components
//!
//! - [`ProxyStorage`]: Implements the `Storage` trait by fetching blocks
//!   from the transaction server's `/fluree/storage/block` endpoint.
//! - [`ProxyNameService`]: Implements the `NameService` trait by fetching
//!   records from the transaction server's `/fluree/storage/ns/{alias}` endpoint.

mod forward;
mod proxy_nameservice;
mod proxy_storage;
mod state;
mod subscription;

pub use forward::{ForwardingClient, ForwardingError};
pub use proxy_nameservice::ProxyNameService;
pub use proxy_storage::{ProxyReadMode, ProxyStorage};
pub use state::{
    GraphSourceNeedsRefresh, NeedsRefresh, PeerState, RemoteGraphSourceWatermark,
    RemoteLedgerWatermark,
};
pub use subscription::{PeerSubscriptionError, PeerSubscriptionTask};
