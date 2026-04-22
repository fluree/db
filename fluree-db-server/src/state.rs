//! Application state management
//!
//! # Thread Safety Note
//!
//! The HTTP server requires `Send + Sync` for state shared across handlers.
//! This server currently only supports **file-based storage** for production use.
//!
//! Memory storage support would require either:
//! - A single-threaded runtime with `LocalSet`
//! - Or refactoring `MemoryNameService` to use `Arc<RwLock<...>>`
//!
//! # Storage Access Modes
//!
//! Peers can operate in two storage access modes:
//! - **Shared**: Direct access to storage (default, requires storage credentials)
//! - **Proxy**: All storage reads proxied through transaction server (no storage credentials)

use crate::config::{ServerConfig, ServerRole};
use crate::peer::{ForwardingClient, PeerState, ProxyNameService, ProxyStorage};
use crate::registry::LedgerRegistry;
use crate::telemetry::TelemetryConfig;
use fluree_db_api::{Fluree, FlureeBuilder, IndexConfig, NameServiceMode};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Application state shared across all request handlers
///
/// Uses `Arc<AppState>` for sharing across handlers via axum's State extractor.
pub struct AppState {
    /// Fluree instance (storage and nameservice mode selected at startup)
    pub fluree: Arc<Fluree>,

    /// Server configuration
    pub config: ServerConfig,

    /// Telemetry configuration
    pub telemetry_config: TelemetryConfig,

    /// Server start time for uptime tracking
    pub start_time: Instant,

    /// Optional index configuration
    pub index_config: Option<IndexConfig>,

    /// Ledger registry for tracking loaded ledgers and their watermarks
    pub registry: Arc<LedgerRegistry>,

    // === OIDC / JWKS state ===
    /// JWKS cache for OIDC token verification (None if no JWKS issuers configured)
    #[cfg(feature = "oidc")]
    pub jwks_cache: Option<Arc<crate::jwks::JwksCache>>,

    // === Peer mode state ===
    /// Peer state tracking remote watermarks (peer mode only)
    pub peer_state: Option<Arc<PeerState>>,

    /// HTTP client for transaction forwarding (peer mode only)
    pub forwarding_client: Option<Arc<ForwardingClient>>,

    /// Counter for ledger refreshes (for testing/metrics)
    /// Incremented when a ledger is actually reloaded (not for coalesced requests)
    pub refresh_counter: AtomicU64,

    /// Handle for the background leaflet cache stats logger task.
    /// Aborted on drop so the `Arc<LeafletCache>` doesn't outlive the server.
    cache_stats_handle: Option<tokio::task::JoinHandle<()>>,
}

impl AppState {
    fn spawn_leaflet_cache_stats_logger(fluree: &Arc<Fluree>) -> tokio::task::JoinHandle<()> {
        // Keep logging lightweight and periodic: one line per minute.
        let cache = Arc::clone(fluree.leaflet_cache());
        let budget_mb = fluree.cache_budget_mb();
        let budget_bytes = (budget_mb as u64).saturating_mul(1024 * 1024);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let used = cache.weighted_size_bytes();
                let entries = cache.entry_count();

                let used_gib = (used as f64) / (1024.0 * 1024.0 * 1024.0);
                let budget_gib = (budget_bytes as f64) / (1024.0 * 1024.0 * 1024.0);
                let pct = if budget_bytes == 0 {
                    0.0
                } else {
                    (used as f64) * 100.0 / (budget_bytes as f64)
                };

                tracing::info!(
                    cache_entries = entries,
                    cache_weighted_bytes = used,
                    cache_weighted_gib = used_gib,
                    cache_budget_mb = budget_mb,
                    cache_budget_gib = budget_gib,
                    cache_budget_pct = pct,
                    "LeafletCache stats"
                );
            }
        })
    }

    /// Create new application state from config
    ///
    /// Initializes either file-backed or proxy-backed Fluree based on config:
    /// - Transaction server: Always file-backed
    /// - Peer with shared storage: File-backed
    /// - Peer with proxy storage: Proxy-backed (no local storage needed)
    pub async fn new(
        config: ServerConfig,
        telemetry_config: TelemetryConfig,
    ) -> Result<Self, fluree_db_api::ApiError> {
        // Validate configuration at startup
        config.validate().map_err(|e| {
            fluree_db_api::ApiError::internal(format!("Invalid configuration: {e}"))
        })?;

        // Create Fluree instance based on storage access mode
        let (fluree, cache_stats_handle) = if config.is_proxy_storage_mode() {
            // Proxy mode: peer proxies all storage reads through tx server
            Self::create_proxy_fluree(&config)?
        } else {
            // Direct mode: file, S3, DynamoDB, etc. via build_client()
            Self::create_direct_fluree(&config).await?
        };

        // Default idle TTL of 5 minutes for ledger registry
        let registry = Arc::new(LedgerRegistry::new(Duration::from_secs(300)));

        // Initialize peer mode state if in peer role
        let (peer_state, forwarding_client) = if config.server_role == ServerRole::Peer {
            let peer_state = Arc::new(PeerState::new());
            let forwarding_client = Arc::new(ForwardingClient::new(
                config
                    .tx_server_url
                    .clone()
                    .expect("tx_server_url validated in peer mode"),
            ));
            (Some(peer_state), Some(forwarding_client))
        } else {
            (None, None)
        };

        // Create JWKS cache (sync — no fetching yet)
        #[cfg(feature = "oidc")]
        let jwks_cache = {
            match config.jwks_issuer_configs() {
                Ok(configs) if !configs.is_empty() => {
                    let ttl = Some(Duration::from_secs(config.jwks_cache_ttl));
                    Some(Arc::new(crate::jwks::JwksCache::new(configs, ttl)))
                }
                Ok(_) => None,
                Err(e) => {
                    return Err(fluree_db_api::ApiError::internal(format!(
                        "Invalid JWKS configuration: {e}"
                    )));
                }
            }
        };

        // Build IndexConfig from server config (always set, even if indexing is disabled,
        // so that novelty backpressure thresholds are respected for external indexers).
        let index_config = Some(IndexConfig {
            reindex_min_bytes: config.reindex_min_bytes,
            reindex_max_bytes: config.reindex_max_bytes,
        });

        Ok(Self {
            fluree,
            config,
            telemetry_config,
            start_time: Instant::now(),
            index_config,
            registry,
            #[cfg(feature = "oidc")]
            jwks_cache,
            peer_state,
            forwarding_client,
            refresh_counter: AtomicU64::new(0),
            cache_stats_handle: Some(cache_stats_handle),
        })
    }

    /// Create a direct-storage Fluree instance (file, S3, DynamoDB, etc.)
    ///
    /// Uses `FlureeBuilder::build_client()` which returns a type-erased `FlureeClient`
    /// supporting all backend types. Backend selection is driven by:
    /// - `--connection-config`: JSON-LD config file (S3, DynamoDB, split storage, encryption)
    /// - `--storage-path`: local filesystem (default)
    /// - Neither: in-memory storage at `.fluree/storage`
    async fn create_direct_fluree(
        config: &ServerConfig,
    ) -> Result<(Arc<Fluree>, tokio::task::JoinHandle<()>), fluree_db_api::ApiError> {
        let mut builder = if let Some(ref path) = config.connection_config {
            // Connection config: build from JSON-LD (supports S3, DynamoDB, split storage, etc.)
            let json_str = std::fs::read_to_string(path).map_err(|e| {
                fluree_db_api::ApiError::internal(format!(
                    "Failed to read connection config file '{}': {}",
                    path.display(),
                    e
                ))
            })?;
            let json: serde_json::Value = serde_json::from_str(&json_str).map_err(|e| {
                fluree_db_api::ApiError::internal(format!(
                    "Failed to parse connection config file '{}': {}",
                    path.display(),
                    e
                ))
            })?;
            FlureeBuilder::from_json_ld(&json)?
        } else {
            // File-backed: use storage_path or default
            let path = config
                .storage_path
                .clone()
                .unwrap_or_else(|| PathBuf::from(".fluree/storage"));
            let path_str = path.to_string_lossy().to_string();
            FlureeBuilder::file(&path_str)
        };

        // Server-level overrides take precedence over connection config defaults.
        if let Some(max_mb) = config.cache_max_mb {
            builder = builder.cache_max_mb(max_mb);
        }
        if config.indexing_enabled {
            builder = builder
                .with_indexing_thresholds(config.reindex_min_bytes, config.reindex_max_bytes);
        }
        // When indexing is disabled (default), we don't call with_indexing_thresholds,
        // so the builder stays in no-indexing mode regardless of connection config defaults.

        let fluree = Arc::new(builder.build_client().await?);

        tracing::info!(
            storage_type = config.storage_type_str(),
            "Initialized direct backend"
        );

        let handle = Self::spawn_leaflet_cache_stats_logger(&fluree);
        Ok((fluree, handle))
    }

    /// Create a proxy-backed Fluree instance for peer proxy mode
    fn create_proxy_fluree(
        config: &ServerConfig,
    ) -> Result<(Arc<Fluree>, tokio::task::JoinHandle<()>), fluree_db_api::ApiError> {
        let tx_url = config
            .tx_server_url
            .clone()
            .expect("tx_server_url validated in proxy mode");

        let token = config.load_storage_proxy_token().map_err(|e| {
            fluree_db_api::ApiError::internal(format!("Failed to load storage proxy token: {e}"))
        })?;

        let storage = ProxyStorage::new(tx_url.clone(), token.clone());
        let nameservice = ProxyNameService::new(tx_url, token);

        let ns_mode = NameServiceMode::ReadOnly(Arc::new(nameservice));
        let fluree = FlureeBuilder::memory().build_with(storage, ns_mode);

        tracing::info!("Initialized peer with proxy storage mode");
        let fluree = Arc::new(fluree);
        let handle = Self::spawn_leaflet_cache_stats_logger(&fluree);
        Ok((fluree, handle))
    }

    /// Get server uptime in seconds
    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    /// Subscribe to ledger/graph-source change events via the event bus.
    pub fn subscribe_events(
        &self,
        scope: fluree_db_nameservice::SubscriptionScope,
    ) -> fluree_db_nameservice::Subscription {
        self.fluree.event_bus().subscribe(scope)
    }
}

impl Drop for AppState {
    fn drop(&mut self) {
        if let Some(handle) = self.cache_stats_handle.take() {
            handle.abort();
        }
    }
}
