//! Fluree DB HTTP Server
//!
//! A thin HTTP REST API wrapper around `fluree-db-api`, providing endpoints
//! equivalent to the legacy server behavior.
//!
//! # Features
//!
//! - JSON-LD and SPARQL query support
//! - Transaction endpoints (transact, insert, upsert)
//! - History queries
//! - Ledger management (create, drop, info)
//! - Header-based policy injection
//! - CORS support
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_server::{FlureeServer, ServerConfig};
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = ServerConfig::default();
//!     let server = FlureeServer::new(config).await.unwrap();
//!     server.run().await.unwrap();
//! }
//! ```

pub mod config;
pub mod config_file;
pub mod error;
pub mod extract;
#[cfg(feature = "oidc")]
pub mod jwks;
pub mod mcp;
pub mod peer;
pub mod registry;
pub mod routes;
pub mod serde;
pub mod state;
pub mod telemetry;
#[cfg(feature = "oidc")]
pub mod token_verify;

pub use config::{ServerConfig, ServerRole};
pub use error::{Result, ServerError};
pub use peer::{ForwardingClient, PeerState, PeerSubscriptionTask};
pub use state::AppState;
pub use telemetry::{init_logging, shutdown_tracer, TelemetryConfig};

use axum::Router;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

/// Fluree HTTP Server
pub struct FlureeServer {
    /// Application state
    state: Arc<AppState>,
    /// Configured router
    router: Router,
}

impl FlureeServer {
    /// Create a new server with the given configuration
    pub async fn new(config: ServerConfig) -> std::result::Result<Self, fluree_db_api::ApiError> {
        let telemetry_config = TelemetryConfig::with_server_config(&config);
        let state = Arc::new(AppState::new(config, telemetry_config).await?);

        // Warm JWKS cache (async — fetch keys from configured endpoints)
        #[cfg(feature = "oidc")]
        if let Some(jwks_cache) = &state.jwks_cache {
            let warmed = jwks_cache.warm().await;
            let total = jwks_cache.configured_issuer_count();
            if warmed == 0 && total > 0 {
                if state.config.data_auth_mode == crate::config::DataAuthMode::Required {
                    tracing::error!(
                        total_issuers = total,
                        "No JWKS endpoints reachable at startup — \
                         OIDC token verification will FAIL until endpoints become available"
                    );
                } else {
                    tracing::warn!(
                        total_issuers = total,
                        "No JWKS endpoints reachable at startup — \
                         OIDC tokens will be rejected until endpoints become available"
                    );
                }
            }
        }

        // Pre-load all ledgers into the LRU cache so the first query
        // against each ledger doesn't pay the cold-start penalty (loading
        // the binary index root from CAS, deserializing dicts, etc.).
        Self::preload_all_ledgers(&state).await;

        let router = routes::build_router(state.clone());

        Ok(Self { state, router })
    }

    /// Pre-load all non-retracted ledgers into the LRU cache.
    ///
    /// This warms the binary index store cache for every ledger so that the
    /// first query doesn't pay a cold-start penalty. Errors are logged but
    /// do not prevent the server from starting.
    async fn preload_all_ledgers(state: &Arc<AppState>) {
        let start = std::time::Instant::now();

        let records = match state.fluree.nameservice().all_records().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to enumerate ledgers for preloading");
                return;
            }
        };

        let active: Vec<_> = records.into_iter().filter(|r| !r.retracted).collect();
        if active.is_empty() {
            return;
        }

        let total = active.len();
        let mut loaded = 0usize;

        for record in &active {
            let handle = state.fluree.ledger_cached(&record.ledger_id).await;

            match handle {
                Ok(handle) => {
                    loaded += 1;

                    // Warm dict tree leaves into the LeafletCache so the first
                    // query doesn't pay cold-start disk I/O for IRI/string resolution.
                    let snap = handle.snapshot().await;
                    if let Some(store) = &snap.binary_store {
                        match store.preload_dict_leaves() {
                            Ok(leaf_count) => {
                                tracing::debug!(
                                    ledger = %record.ledger_id,
                                    leaf_count,
                                    "Preloaded ledger + dict leaves"
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    ledger = %record.ledger_id,
                                    error = %e,
                                    "Preloaded ledger but dict leaf warming failed"
                                );
                            }
                        }
                    } else {
                        tracing::debug!(ledger = %record.ledger_id, "Preloaded ledger (no binary index)");
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        ledger = %record.ledger_id,
                        error = %e,
                        "Failed to preload ledger"
                    );
                }
            }
        }

        let elapsed = start.elapsed();
        info!(
            loaded,
            total,
            elapsed_ms = elapsed.as_millis() as u64,
            "Ledger preload complete"
        );
    }

    /// Get a reference to the application state
    pub fn state(&self) -> &Arc<AppState> {
        &self.state
    }

    /// Get the router for testing
    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// Run the server
    pub async fn run(self) -> std::result::Result<(), std::io::Error> {
        let addr = self.state.config.listen_addr;
        let listener = TcpListener::bind(addr).await?;

        // Start peer subscription/sync task if in peer mode
        let subscription_task = if self.state.config.is_peer_mode() {
            let peer_state = self
                .state
                .peer_state
                .clone()
                .expect("peer_state should exist in peer mode");

            if !self.state.fluree.nameservice_mode().is_read_only() {
                // Shared storage: PeerSyncTask persists refs into local FileNameService
                let events_url = peer::build_peer_events_url(&self.state.config);
                let auth_token = self.state.config.load_peer_events_token().ok().flatten();
                let watch = fluree_db_nameservice_sync::SseRemoteWatch::new(events_url, auth_token);
                let task = peer::PeerSyncTask::new(
                    Arc::clone(&self.state.fluree),
                    peer_state,
                    watch,
                    self.state.config.clone(),
                );
                Some(task.spawn())
            } else {
                // Proxy storage: existing PeerSubscriptionTask (in-memory watermarks only)
                let task = peer::PeerSubscriptionTask::new(
                    self.state.config.clone(),
                    peer_state,
                    Arc::clone(&self.state.fluree),
                );
                Some(task.spawn())
            }
        } else {
            None
        };

        // Start ledger manager maintenance task for idle eviction
        let ledger_maintenance_task = self.state.fluree.spawn_maintenance();

        info!(
            addr = %addr,
            storage = %self.state.config.storage_type_str(),
            server_role = ?self.state.config.server_role,
            ledger_caching = ledger_maintenance_task.is_some(),
            mcp_enabled = self.state.config.mcp_enabled,
            "Fluree server starting"
        );

        // Run server
        let result = axum::serve(listener, self.router).await;

        // Cancel maintenance tasks on shutdown
        if let Some(task) = subscription_task {
            task.abort();
        }
        if let Some(task) = ledger_maintenance_task {
            task.abort();
        }

        result
    }

    /// Start the registry maintenance task for tracking ledger watermarks.
    ///
    /// This spawns a background task that:
    /// - Listens to nameservice events and updates registry watermarks
    /// - Periodically sweeps idle entries based on the registry's TTL
    ///
    /// Returns a JoinHandle that can be used to await or abort the task.
    /// The task will automatically stop when the nameservice broadcast channel closes.
    pub async fn start_registry_maintenance(
        &self,
        sweep_interval: std::time::Duration,
    ) -> std::result::Result<tokio::task::JoinHandle<()>, fluree_db_api::ApiError> {
        use fluree_db_nameservice::SubscriptionScope;

        let subscription = self
            .state
            .fluree
            .event_bus()
            .subscribe(SubscriptionScope::All);

        let handle = registry::LedgerRegistry::spawn_maintenance_task(
            self.state.registry.clone(),
            subscription.receiver,
            sweep_interval,
        );

        info!("Registry maintenance task started");
        Ok(handle)
    }
}

/// Builder for FlureeServer with fluent API
pub struct FlureeServerBuilder {
    config: ServerConfig,
}

impl FlureeServerBuilder {
    /// Create a new builder with default config (memory storage)
    pub fn new() -> Self {
        Self {
            config: ServerConfig::default(),
        }
    }

    /// Create a builder configured for memory storage
    pub fn memory() -> Self {
        Self::new()
    }

    /// Create a builder configured for file storage
    #[cfg(feature = "native")]
    pub fn file(path: impl Into<std::path::PathBuf>) -> Self {
        let mut builder = Self::new();
        builder.config.storage_path = Some(path.into());
        builder
    }

    /// Set the listen address
    pub fn listen_addr(mut self, addr: impl Into<std::net::SocketAddr>) -> Self {
        self.config.listen_addr = addr.into();
        self
    }

    /// Enable or disable CORS
    pub fn cors_enabled(mut self, enabled: bool) -> Self {
        self.config.cors_enabled = enabled;
        self
    }

    /// Enable or disable background indexing
    pub fn indexing_enabled(mut self, enabled: bool) -> Self {
        self.config.indexing_enabled = enabled;
        self
    }

    /// Set global cache budget in MB
    pub fn cache_max_mb(mut self, max_mb: usize) -> Self {
        self.config.cache_max_mb = Some(max_mb);
        self
    }

    /// Build the server
    pub async fn build(self) -> std::result::Result<FlureeServer, fluree_db_api::ApiError> {
        FlureeServer::new(self.config).await
    }
}

impl Default for FlureeServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}
