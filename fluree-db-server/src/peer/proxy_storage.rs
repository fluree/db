//! Re-export of the proxy storage client.
//!
//! `ProxyStorage` moved to `fluree-db-nameservice-sync` so the CLI and other
//! consumers can build peer-mode/remote-mount Fluree instances without
//! depending on the server crate. This module keeps the historical
//! `fluree_db_server::peer::ProxyStorage` path working.

pub use fluree_db_nameservice_sync::proxy_storage::{ProxyReadMode, ProxyStorage};
