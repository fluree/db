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
pub mod sync_task;

pub use forward::{ForwardingClient, ForwardingError};
pub use proxy_nameservice::ProxyNameService;
pub use proxy_storage::ProxyStorage;
pub use state::{
    GraphSourceNeedsRefresh, NeedsRefresh, PeerState, RemoteGraphSourceWatermark,
    RemoteLedgerWatermark,
};
pub use subscription::{PeerSubscriptionError, PeerSubscriptionTask};
pub use sync_task::PeerSyncTask;

use crate::config::ServerConfig;

/// Build the SSE events URL for peer mode.
///
/// Constructs the full URL with subscription query parameters (same logic as
/// `PeerSubscriptionTask::build_events_url`, extracted for shared use).
pub fn build_peer_events_url(config: &ServerConfig) -> String {
    let mut url = config
        .peer_events_url()
        .expect("peer_events_url should be set in peer mode");

    let sub = config.peer_subscription();
    let mut params = vec![];

    if sub.all {
        params.push("all=true".to_string());
    } else {
        for l in &sub.ledgers {
            params.push(format!("ledger={}", urlencoding::encode(l)));
        }
        for v in &sub.graph_sources {
            params.push(format!("graph-source={}", urlencoding::encode(v)));
        }
    }

    if !params.is_empty() {
        url.push('?');
        url.push_str(&params.join("&"));
    }

    url
}
