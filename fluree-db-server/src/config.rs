//! Server configuration

use clap::{Parser, ValueEnum};
use fluree_db_api::server_defaults;
use std::net::SocketAddr;
use std::path::PathBuf;

/// Server operating mode
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
pub enum ServerRole {
    /// Write-enabled transaction server (current behavior)
    #[default]
    Transaction,
    /// Read-only query peer with SSE subscription + transaction forwarding
    Peer,
}

/// Storage access mode for peer servers
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
pub enum StorageAccessMode {
    /// Direct storage access (shared filesystem/S3 credentials)
    #[default]
    Shared,
    /// Proxy all storage reads through transaction server
    Proxy,
}

/// Peer subscription configuration
#[derive(Debug, Clone, Default)]
pub struct PeerSubscription {
    /// Subscribe to all ledgers and graph sources
    pub all: bool,
    /// Specific ledger aliases
    pub ledgers: Vec<String>,
    /// Specific graph source aliases
    pub graph_sources: Vec<String>,
}

/// Authentication mode for the events endpoint
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
pub enum EventsAuthMode {
    /// No authentication required (current behavior)
    #[default]
    None,
    /// Accept tokens but don't require them (DEV ONLY - not a security boundary)
    Optional,
    /// Require valid Bearer token (PRODUCTION)
    Required,
}

/// Authentication mode for the data API endpoints (query/update/info/exists).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
pub enum DataAuthMode {
    /// No authentication required (default)
    #[default]
    None,
    /// Accept tokens but don't require them (DEV ONLY - not a security boundary)
    Optional,
    /// Require valid auth (Bearer token or signed request) (PRODUCTION)
    Required,
}

/// Configuration for events endpoint authentication
#[derive(Debug, Clone, Default)]
pub struct EventsAuthConfig {
    /// Authentication mode
    pub mode: EventsAuthMode,
    /// Expected audience claim (optional)
    pub audience: Option<String>,
    /// Trusted issuer did:key identifiers
    pub trusted_issuers: Vec<String>,
    /// DANGEROUS: Accept any valid signature regardless of issuer.
    /// Only for development/testing.
    pub insecure_accept_any_issuer: bool,
    /// Whether JWKS issuers are configured (for validation check)
    pub has_jwks_issuers: bool,
}

/// Configuration for data API endpoint authentication.
#[derive(Debug, Clone, Default)]
pub struct DataAuthConfig {
    /// Authentication mode
    pub mode: DataAuthMode,
    /// Expected audience claim (optional)
    pub audience: Option<String>,
    /// Trusted issuer did:key identifiers for Bearer tokens
    pub trusted_issuers: Vec<String>,
    /// Default policy class IRI (optional). Applied when request does not specify one.
    pub default_policy_class: Option<String>,
    /// DANGEROUS: Accept any valid signature regardless of issuer.
    /// Only for development/testing.
    pub insecure_accept_any_issuer: bool,
    /// Whether JWKS issuers are configured (for validation check)
    pub has_jwks_issuers: bool,
}

impl DataAuthConfig {
    /// Validate configuration at startup
    pub fn validate(&self) -> Result<(), String> {
        if self.mode == DataAuthMode::Required
            && self.trusted_issuers.is_empty()
            && !self.has_jwks_issuers
            && !self.insecure_accept_any_issuer
        {
            return Err(
                "data_auth.mode=required requires --data-auth-trusted-issuer, \
                 --jwks-issuer, or --data-auth-insecure-accept-any-issuer flag"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Check if an issuer is trusted.
    /// When a token is presented, issuer MUST be trusted (unless insecure flag).
    pub fn is_issuer_trusted(&self, issuer: &str) -> bool {
        if self.insecure_accept_any_issuer {
            return true;
        }
        if self.trusted_issuers.is_empty() {
            return false;
        }
        self.trusted_issuers.iter().any(|i| i == issuer)
    }
}

impl EventsAuthConfig {
    /// Validate configuration at startup
    pub fn validate(&self) -> Result<(), String> {
        if self.mode == EventsAuthMode::Required
            && self.trusted_issuers.is_empty()
            && !self.has_jwks_issuers
            && !self.insecure_accept_any_issuer
        {
            return Err(
                "events_auth.mode=required requires --events-auth-trusted-issuer, \
                 --jwks-issuer, or --events-auth-insecure-accept-any-issuer flag"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Check if an issuer is trusted.
    /// When a token is presented, issuer MUST be trusted (unless insecure flag).
    pub fn is_issuer_trusted(&self, issuer: &str) -> bool {
        if self.insecure_accept_any_issuer {
            return true;
        }
        // If trusted_issuers is empty and we're checking, reject
        // (This only happens when a token is presented)
        if self.trusted_issuers.is_empty() {
            return false;
        }
        self.trusted_issuers.iter().any(|i| i == issuer)
    }

    /// Whether identity must be present in token (Required mode)
    pub fn requires_identity(&self) -> bool {
        self.mode == EventsAuthMode::Required
    }
}

/// Configuration for storage proxy endpoints
#[derive(Debug, Clone, Default)]
pub struct StorageProxyConfig {
    /// Enable storage proxy endpoints on transaction server
    pub enabled: bool,

    /// Trusted issuers for storage proxy tokens.
    /// If empty, falls back to events_auth.trusted_issuers.
    pub trusted_issuers: Option<Vec<String>>,

    /// Default identity for policy evaluation (optional).
    /// Used when token has no fluree.identity claim.
    pub default_identity: Option<String>,

    /// Default policy class for peer queries (optional).
    /// Applied in addition to identity-based policy.
    pub default_policy_class: Option<String>,

    /// Emit debug headers (X-Fluree-Ledger-Alias, X-Fluree-Flakes-*) in responses.
    /// Default false - these can leak information about ledger structure.
    pub emit_debug_headers: bool,

    /// DANGEROUS: Accept any valid signature regardless of issuer.
    /// Only for development/testing.
    pub insecure_accept_any_issuer: bool,

    /// Whether any JWKS issuers are configured (enables OIDC trust path).
    pub has_jwks_issuers: bool,
}

impl StorageProxyConfig {
    /// Validate configuration at startup
    pub fn validate(&self, events_auth: &EventsAuthConfig) -> Result<(), String> {
        if self.enabled {
            // Must have some trusted issuers (own, from events_auth, JWKS, or insecure)
            let has_trusted = self.trusted_issuers.as_ref().is_some_and(|v| !v.is_empty())
                || !events_auth.trusted_issuers.is_empty()
                || self.has_jwks_issuers
                || self.insecure_accept_any_issuer;

            if !has_trusted {
                return Err(
                    "storage_proxy.enabled requires --storage-proxy-trusted-issuer, \
                     --events-auth-trusted-issuer, --jwks-issuer, \
                     or --storage-proxy-insecure-accept-any-issuer"
                        .to_string(),
                );
            }
        }
        Ok(())
    }

    /// Get effective trusted issuers (own list or fallback to events_auth)
    pub fn effective_trusted_issuers<'a>(
        &'a self,
        events_auth: &'a EventsAuthConfig,
    ) -> &'a [String] {
        self.trusted_issuers
            .as_deref()
            .filter(|v| !v.is_empty())
            .unwrap_or(&events_auth.trusted_issuers)
    }

    /// Check if an issuer is trusted for storage proxy.
    pub fn is_issuer_trusted(&self, issuer: &str, events_auth: &EventsAuthConfig) -> bool {
        if self.insecure_accept_any_issuer {
            return true;
        }
        let trusted = self.effective_trusted_issuers(events_auth);
        if trusted.is_empty() {
            return false;
        }
        trusted.iter().any(|i| i == issuer)
    }
}

/// Configuration for MCP (Model Context Protocol) endpoint authentication
#[derive(Debug, Clone, Default)]
pub struct McpAuthConfig {
    /// Trusted issuer did:key identifiers for MCP tokens
    pub trusted_issuers: Vec<String>,
    /// DANGEROUS: Accept any valid signature regardless of issuer.
    /// Only for development/testing.
    pub insecure_accept_any_issuer: bool,
}

impl McpAuthConfig {
    /// Validate configuration at startup
    pub fn validate(
        &self,
        mcp_enabled: bool,
        events_auth: &EventsAuthConfig,
    ) -> Result<(), String> {
        if mcp_enabled {
            // Must have some trusted issuers (own or from events_auth)
            let has_trusted = !self.trusted_issuers.is_empty()
                || !events_auth.trusted_issuers.is_empty()
                || self.insecure_accept_any_issuer;

            if !has_trusted {
                return Err("mcp_enabled requires --mcp-auth-trusted-issuer, \
                     --events-auth-trusted-issuer, or --mcp-auth-insecure flag"
                    .to_string());
            }
        }
        Ok(())
    }

    /// Get effective trusted issuers (own list or fallback to events_auth)
    pub fn effective_trusted_issuers<'a>(
        &'a self,
        events_auth: &'a EventsAuthConfig,
    ) -> &'a [String] {
        if !self.trusted_issuers.is_empty() {
            &self.trusted_issuers
        } else {
            &events_auth.trusted_issuers
        }
    }

    /// Check if an issuer is trusted for MCP.
    pub fn is_issuer_trusted(&self, issuer: &str, events_auth: &EventsAuthConfig) -> bool {
        if self.insecure_accept_any_issuer {
            return true;
        }
        let trusted = self.effective_trusted_issuers(events_auth);
        if trusted.is_empty() {
            return false;
        }
        trusted.iter().any(|i| i == issuer)
    }
}

/// Authentication mode for admin endpoints (/fluree/create, /fluree/drop)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
pub enum AdminAuthMode {
    /// No authentication required (open access - development only)
    #[default]
    None,
    /// Require valid Bearer token from trusted issuer (production)
    Required,
}

/// Configuration for admin endpoint authentication (/fluree/create, /fluree/drop)
#[derive(Debug, Clone, Default)]
pub struct AdminAuthConfig {
    /// Authentication mode
    pub mode: AdminAuthMode,
    /// Trusted issuer did:key identifiers for admin tokens
    pub trusted_issuers: Vec<String>,
    /// DANGEROUS: Accept any valid signature regardless of issuer.
    /// Only for development/testing.
    pub insecure_accept_any_issuer: bool,
    /// Whether JWKS issuers are configured (for validation check)
    pub has_jwks_issuers: bool,
}

impl AdminAuthConfig {
    /// Validate configuration at startup
    pub fn validate(&self, events_auth: &EventsAuthConfig) -> Result<(), String> {
        if self.mode == AdminAuthMode::Required {
            // Must have some trusted issuers (own or from events_auth)
            let has_trusted = !self.trusted_issuers.is_empty()
                || !events_auth.trusted_issuers.is_empty()
                || self.has_jwks_issuers
                || self.insecure_accept_any_issuer;

            if !has_trusted {
                return Err(
                    "admin_auth.mode=required requires --admin-auth-trusted-issuer, \
                     --events-auth-trusted-issuer, --jwks-issuer, or --admin-auth-insecure flag"
                        .to_string(),
                );
            }
        }
        Ok(())
    }

    /// Get effective trusted issuers (own list or fallback to events_auth)
    pub fn effective_trusted_issuers<'a>(
        &'a self,
        events_auth: &'a EventsAuthConfig,
    ) -> &'a [String] {
        if !self.trusted_issuers.is_empty() {
            &self.trusted_issuers
        } else {
            &events_auth.trusted_issuers
        }
    }

    /// Check if an issuer is trusted for admin endpoints.
    pub fn is_issuer_trusted(&self, issuer: &str, events_auth: &EventsAuthConfig) -> bool {
        if self.insecure_accept_any_issuer {
            return true;
        }
        let trusted = self.effective_trusted_issuers(events_auth);
        if trusted.is_empty() {
            return false;
        }
        trusted.iter().any(|i| i == issuer)
    }

    /// Whether authentication is required
    pub fn is_required(&self) -> bool {
        self.mode == AdminAuthMode::Required
    }
}

/// Fluree DB HTTP Server configuration
#[derive(Parser, Debug, Clone)]
#[command(name = "fluree-server")]
#[command(about = "Fluree DB HTTP REST API Server")]
pub struct ServerConfig {
    /// Path to configuration file (default: walks up from cwd looking for .fluree/config.toml)
    #[arg(long, env = "FLUREE_CONFIG")]
    pub config_file: Option<PathBuf>,

    /// Configuration profile to activate (merges [profiles.<name>.server] onto [server])
    #[arg(long, env = "FLUREE_PROFILE")]
    pub profile: Option<String>,

    /// Address to listen on
    #[arg(long, env = "FLUREE_LISTEN_ADDR", default_value = server_defaults::DEFAULT_LISTEN_ADDR)]
    pub listen_addr: SocketAddr,

    /// Storage path for file-based storage (enables file storage mode)
    #[arg(long, env = "FLUREE_STORAGE_PATH")]
    pub storage_path: Option<PathBuf>,

    /// Path to a JSON-LD connection configuration file.
    ///
    /// When provided, the server builds the storage and nameservice backend
    /// from this config (using `FlureeBuilder::from_json_ld().build_client()`).
    /// This supports S3, DynamoDB, split commit/index storage, encryption,
    /// and the full connection config surface.
    ///
    /// Overrides `--storage-path` when set. The file format is the same as the
    /// connection JSON-LD used by the Fluree API (see `FlureeBuilder::from_json_ld`).
    #[arg(long, env = "FLUREE_CONNECTION_CONFIG")]
    pub connection_config: Option<PathBuf>,

    /// Enable CORS (Cross-Origin Resource Sharing)
    #[arg(long, env = "FLUREE_CORS_ENABLED", default_value_t = server_defaults::DEFAULT_CORS_ENABLED)]
    pub cors_enabled: bool,

    /// Enable background indexing
    #[arg(long, env = "FLUREE_INDEXING_ENABLED", default_value_t = server_defaults::DEFAULT_INDEXING_ENABLED)]
    pub indexing_enabled: bool,

    /// Novelty size (bytes) that triggers background reindexing (soft threshold)
    #[arg(long, env = "FLUREE_REINDEX_MIN_BYTES", default_value_t = server_defaults::DEFAULT_REINDEX_MIN_BYTES)]
    pub reindex_min_bytes: usize,

    /// Novelty size (bytes) that blocks new commits until reindexing completes (hard threshold)
    ///
    /// Default: 20% of system RAM (256 MB fallback). Set explicitly to override.
    #[arg(long, env = "FLUREE_REINDEX_MAX_BYTES")]
    pub reindex_max_bytes: Option<usize>,

    /// Global cache budget in MB (default: tiered fraction of system RAM — 30% if <4GB, 40% if 4-8GB, 50% if ≥8GB)
    ///
    /// This controls the shared API-level cache budget used for decoded index artifacts.
    #[arg(long, env = "FLUREE_CACHE_MAX_MB")]
    pub cache_max_mb: Option<usize>,

    /// Request body size limit in bytes (default 50MB)
    #[arg(long, env = "FLUREE_BODY_LIMIT", default_value_t = server_defaults::DEFAULT_BODY_LIMIT)]
    pub body_limit: usize,

    /// Query execution timeout in milliseconds (default 15 minutes, 0 disables)
    #[arg(long, env = "FLUREE_QUERY_TIMEOUT_MS", default_value_t = server_defaults::DEFAULT_QUERY_TIMEOUT_MS)]
    pub query_timeout_ms: u64,

    /// Maximum time to wait for HTTP read-after-write min-t freshness checks.
    #[arg(long, env = "FLUREE_QUERY_MIN_T_TIMEOUT_MS", default_value_t = server_defaults::DEFAULT_QUERY_MIN_T_TIMEOUT_MS)]
    pub query_min_t_timeout_ms: u64,

    /// Enable query-time nameservice refresh checks before current-head reads.
    #[arg(long, env = "FLUREE_QUERY_REFRESH_ENABLED", default_value_t = server_defaults::DEFAULT_QUERY_REFRESH_ENABLED)]
    pub query_refresh_enabled: bool,

    /// Minimum milliseconds between query-time refresh checks per ledger per server process.
    #[arg(long, env = "FLUREE_QUERY_REFRESH_TTL_MS", default_value_t = server_defaults::DEFAULT_QUERY_REFRESH_TTL_MS)]
    pub query_refresh_ttl_ms: u64,

    /// Enable the negotiated presigned-upload import path (for clients that
    /// cannot send a large body to `POST /import`, e.g. behind a payload-capped
    /// gateway). Advertised in discovery; the reference impl stages uploads to
    /// `import_staging_dir` and restores from there.
    #[arg(long, env = "FLUREE_IMPORT_PRESIGN_ENABLED", default_value_t = false)]
    pub import_presign_enabled: bool,

    /// Max body size (bytes) accepted on the direct `POST /import` path,
    /// advertised as `import.direct_max_bytes`. Clients with an archive larger
    /// than this use the negotiated upload flow. Only meaningful when presign
    /// is enabled.
    #[arg(
        long,
        env = "FLUREE_IMPORT_DIRECT_MAX_BYTES",
        default_value_t = 6_291_456
    )]
    pub import_direct_max_bytes: usize,

    /// Directory the reference presigned-upload backend stages archives in
    /// before restoring. Defaults to the system temp dir when unset.
    #[arg(long, env = "FLUREE_IMPORT_STAGING_DIR")]
    pub import_staging_dir: Option<std::path::PathBuf>,

    /// Archive size (bytes) at or above which the negotiated upload switches
    /// from a single presigned PUT to a multipart upload. A single S3 PUT caps
    /// at 5 GiB, so archives larger than that MUST use multipart. Default 5 GiB.
    /// Only meaningful when presign is enabled.
    #[arg(
        long,
        env = "FLUREE_IMPORT_MULTIPART_THRESHOLD_BYTES",
        default_value_t = 5_368_709_120
    )]
    pub import_multipart_threshold_bytes: u64,

    /// Target part size (bytes) for multipart uploads. The server adapts this
    /// upward when an archive would otherwise exceed the 10,000-part S3 ceiling.
    /// Default 256 MiB (~84 parts for a 21 GB archive). Only meaningful when
    /// presign is enabled.
    #[arg(
        long,
        env = "FLUREE_IMPORT_MULTIPART_PART_SIZE_BYTES",
        default_value_t = 268_435_456
    )]
    pub import_multipart_part_size_bytes: u64,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, env = "FLUREE_LOG_LEVEL", default_value = server_defaults::DEFAULT_LOG_LEVEL)]
    pub log_level: String,

    // Events authentication options
    /// Authentication mode for /fluree/events endpoint
    #[arg(
        long,
        env = "FLUREE_EVENTS_AUTH_MODE",
        default_value = "none",
        value_enum
    )]
    pub events_auth_mode: EventsAuthMode,

    /// Expected audience claim for events tokens
    #[arg(long, env = "FLUREE_EVENTS_AUTH_AUDIENCE")]
    pub events_auth_audience: Option<String>,

    /// Trusted issuer did:key for events tokens (can be specified multiple times)
    #[arg(
        long = "events-auth-trusted-issuer",
        env = "FLUREE_EVENTS_AUTH_TRUSTED_ISSUERS"
    )]
    pub events_auth_trusted_issuers: Vec<String>,

    /// DANGEROUS: Accept any valid signature regardless of issuer (dev only)
    #[arg(long, env = "FLUREE_EVENTS_AUTH_INSECURE", hide = true)]
    pub events_auth_insecure_accept_any_issuer: bool,

    // === Data API authentication options (query/update/info/exists) ===
    /// Authentication mode for data API endpoints (query/update/info/exists)
    #[arg(
        long,
        env = "FLUREE_DATA_AUTH_MODE",
        default_value = "none",
        value_enum
    )]
    pub data_auth_mode: DataAuthMode,

    /// Expected audience claim for data API Bearer tokens (optional)
    #[arg(long, env = "FLUREE_DATA_AUTH_AUDIENCE")]
    pub data_auth_audience: Option<String>,

    /// Trusted issuer did:key for data API Bearer tokens (can be specified multiple times)
    #[arg(
        long = "data-auth-trusted-issuer",
        env = "FLUREE_DATA_AUTH_TRUSTED_ISSUERS"
    )]
    pub data_auth_trusted_issuers: Vec<String>,

    /// Default policy class IRI for data API requests (optional)
    #[arg(long, env = "FLUREE_DATA_AUTH_DEFAULT_POLICY_CLASS")]
    pub data_auth_default_policy_class: Option<String>,

    /// DANGEROUS: Accept any valid signature regardless of issuer (dev only)
    #[arg(long, env = "FLUREE_DATA_AUTH_INSECURE", hide = true)]
    pub data_auth_insecure_accept_any_issuer: bool,

    // === OIDC / JWKS options (data auth) ===
    /// JWKS issuer mapping: issuer_url=jwks_url (repeatable).
    /// Both the issuer URL and JWKS endpoint URL are required.
    /// Example: --jwks-issuer "https://solo.example.com=https://solo.example.com/.well-known/jwks.json"
    #[cfg(feature = "oidc")]
    #[arg(
        long = "jwks-issuer",
        env = "FLUREE_JWKS_ISSUERS",
        value_delimiter = ','
    )]
    pub jwks_issuers: Vec<String>,

    /// JWKS cache TTL in seconds (default 300 = 5 minutes)
    #[cfg(feature = "oidc")]
    #[arg(long, env = "FLUREE_JWKS_CACHE_TTL", default_value_t = server_defaults::DEFAULT_JWKS_CACHE_TTL)]
    pub jwks_cache_ttl: u64,

    // === Server role (peer mode) ===
    /// Server operating mode: transaction (write-enabled) or peer (read-only with forwarding)
    #[arg(
        long,
        env = "FLUREE_SERVER_ROLE",
        default_value = "transaction",
        value_enum
    )]
    pub server_role: ServerRole,

    /// Transaction server base URL (required in peer mode).
    /// Used for transaction forwarding and default SSE endpoint.
    #[arg(long, env = "FLUREE_TX_SERVER_URL")]
    pub tx_server_url: Option<String>,

    /// Events endpoint URL for peer subscription (defaults to {tx_server_url}/v1/fluree/events)
    #[arg(long, env = "FLUREE_PEER_EVENTS_URL")]
    pub peer_events_url: Option<String>,

    /// Bearer token for peer events authentication (or @filepath to read from file)
    #[arg(long, env = "FLUREE_PEER_EVENTS_TOKEN")]
    pub peer_events_token: Option<String>,

    /// Subscribe to all ledgers and graph sources on transaction server (peer mode)
    #[arg(long)]
    pub peer_subscribe_all: bool,

    /// Subscribe to specific ledgers in peer mode (repeatable)
    #[arg(long = "peer-ledger")]
    pub peer_ledgers: Vec<String>,

    /// Subscribe to specific graph sources in peer mode (repeatable)
    #[arg(long = "peer-graph-source")]
    pub peer_graph_sources: Vec<String>,

    /// Initial reconnect delay in ms for peer SSE subscription
    #[arg(long, default_value_t = server_defaults::DEFAULT_PEER_RECONNECT_INITIAL_MS)]
    pub peer_reconnect_initial_ms: u64,

    /// Maximum reconnect delay in ms for peer SSE subscription
    #[arg(long, default_value_t = server_defaults::DEFAULT_PEER_RECONNECT_MAX_MS)]
    pub peer_reconnect_max_ms: u64,

    /// Reconnect backoff multiplier for peer SSE subscription
    #[arg(long, default_value_t = server_defaults::DEFAULT_PEER_RECONNECT_MULTIPLIER)]
    pub peer_reconnect_multiplier: f64,

    // === Storage proxy options (transaction server) ===
    /// Enable storage proxy endpoints on transaction server
    #[arg(long, env = "FLUREE_STORAGE_PROXY_ENABLED")]
    pub storage_proxy_enabled: bool,

    /// Trusted issuer did:key for storage proxy tokens (can be specified multiple times).
    /// Falls back to events-auth-trusted-issuer if not specified.
    #[arg(
        long = "storage-proxy-trusted-issuer",
        env = "FLUREE_STORAGE_PROXY_TRUSTED_ISSUERS"
    )]
    pub storage_proxy_trusted_issuers: Vec<String>,

    /// Default identity IRI for policy evaluation (when token has no fluree.identity claim)
    #[arg(long, env = "FLUREE_STORAGE_PROXY_DEFAULT_IDENTITY")]
    pub storage_proxy_default_identity: Option<String>,

    /// Default policy class IRI for peer queries
    #[arg(long, env = "FLUREE_STORAGE_PROXY_DEFAULT_POLICY_CLASS")]
    pub storage_proxy_default_policy_class: Option<String>,

    /// Emit debug headers in storage proxy responses (can leak ledger structure info)
    #[arg(long, env = "FLUREE_STORAGE_PROXY_DEBUG_HEADERS")]
    pub storage_proxy_debug_headers: bool,

    /// DANGEROUS: Accept any valid signature for storage proxy (dev only)
    #[arg(long, env = "FLUREE_STORAGE_PROXY_INSECURE", hide = true)]
    pub storage_proxy_insecure_accept_any_issuer: bool,

    // === Peer storage access mode options ===
    /// Storage access mode for peer: shared (direct) or proxy (through tx server)
    #[arg(
        long,
        env = "FLUREE_STORAGE_ACCESS_MODE",
        default_value = "shared",
        value_enum
    )]
    pub storage_access_mode: StorageAccessMode,

    /// Bearer token for storage proxy requests (peer mode + proxy access only).
    /// Supports @filepath syntax for loading from file.
    #[arg(long, env = "FLUREE_STORAGE_PROXY_TOKEN")]
    pub storage_proxy_token: Option<String>,

    /// Path to file containing storage proxy token (alternative to --storage-proxy-token)
    #[arg(long, env = "FLUREE_STORAGE_PROXY_TOKEN_FILE")]
    pub storage_proxy_token_file: Option<PathBuf>,

    // === MCP (Model Context Protocol) options ===
    /// Enable MCP (Model Context Protocol) endpoint at /mcp
    #[arg(long, env = "FLUREE_MCP_ENABLED")]
    pub mcp_enabled: bool,

    /// Trusted issuer did:key for MCP tokens (can be specified multiple times).
    /// Falls back to events-auth-trusted-issuer if not specified.
    #[arg(
        long = "mcp-auth-trusted-issuer",
        env = "FLUREE_MCP_AUTH_TRUSTED_ISSUERS"
    )]
    pub mcp_auth_trusted_issuers: Vec<String>,

    /// DANGEROUS: Accept any valid MCP signature regardless of issuer (dev only)
    #[arg(long, env = "FLUREE_MCP_AUTH_INSECURE", hide = true)]
    pub mcp_auth_insecure_accept_any_issuer: bool,

    /// Byte budget for the MCP `sparql_query` Agent JSON envelope.
    ///
    /// Results are truncated once their serialized size exceeds this limit and the envelope
    /// sets `hasMore: true`. MCP tool calls carry no per-request headers (unlike the HTTP
    /// `Fluree-Max-Bytes` header), so the budget is server-configured.
    #[arg(
        long,
        env = "FLUREE_MCP_AGENT_JSON_MAX_BYTES",
        default_value_t = server_defaults::DEFAULT_MCP_AGENT_JSON_MAX_BYTES
    )]
    pub mcp_agent_json_max_bytes: usize,

    /// Query execution timeout for MCP `sparql_query` in milliseconds (default 5 minutes, 0 disables timeout).
    #[arg(
        long,
        env = "FLUREE_MCP_QUERY_TIMEOUT_MS",
        default_value_t = server_defaults::DEFAULT_MCP_QUERY_TIMEOUT_MS
    )]
    pub mcp_query_timeout_ms: u64,

    // === Admin endpoint authentication options ===
    /// Authentication mode for admin endpoints (/fluree/create, /fluree/drop)
    #[arg(
        long,
        env = "FLUREE_ADMIN_AUTH_MODE",
        default_value = "none",
        value_enum
    )]
    pub admin_auth_mode: AdminAuthMode,

    /// Trusted issuer did:key for admin tokens (can be specified multiple times).
    /// Falls back to events-auth-trusted-issuer if not specified.
    #[arg(
        long = "admin-auth-trusted-issuer",
        env = "FLUREE_ADMIN_AUTH_TRUSTED_ISSUERS"
    )]
    pub admin_auth_trusted_issuers: Vec<String>,

    /// DANGEROUS: Accept any valid admin signature regardless of issuer (dev only)
    #[arg(long, env = "FLUREE_ADMIN_AUTH_INSECURE", hide = true)]
    pub admin_auth_insecure_accept_any_issuer: bool,

    // === Raft cluster options (replicated writes) ===
    //
    // When `raft_enabled` is `true`, the server bootstraps an
    // openraft node, mounts the follower-forward middleware over
    // leader-only routes, and exposes the inter-node RPC + cluster
    // admin routers on `raft_listen_addr` (a separate private
    // listener). All four fields below are required when raft is
    // on; validation rejects partial configs.
    /// Replicate writes through a Raft cluster.
    #[cfg(feature = "raft")]
    #[arg(long, env = "FLUREE_RAFT_ENABLED")]
    pub raft_enabled: bool,

    /// This node's id in the Raft cluster. Must be unique and
    /// stable across restarts — the openraft log + snapshots are
    /// keyed by it.
    #[cfg(feature = "raft")]
    #[arg(long, env = "FLUREE_RAFT_NODE_ID")]
    pub raft_node_id: Option<u64>,

    /// Root directory for the Raft log and snapshots. Distinct
    /// from `--storage-path` — losing this directory loses commits.
    #[cfg(feature = "raft")]
    #[arg(long, env = "FLUREE_RAFT_STORAGE_PATH")]
    pub raft_storage_path: Option<PathBuf>,

    /// VPC-internal address for the inter-node Raft RPC + cluster
    /// admin listener. Distinct from `--listen-addr` (the
    /// client-facing port). No auth — operators enforce trust at
    /// the network layer.
    #[cfg(feature = "raft")]
    #[arg(long, env = "FLUREE_RAFT_LISTEN_ADDR")]
    pub raft_listen_addr: Option<SocketAddr>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            config_file: None,
            profile: None,
            listen_addr: server_defaults::DEFAULT_LISTEN_ADDR.parse().unwrap(),
            storage_path: None,
            connection_config: None,
            cors_enabled: server_defaults::DEFAULT_CORS_ENABLED,
            indexing_enabled: server_defaults::DEFAULT_INDEXING_ENABLED,
            reindex_min_bytes: server_defaults::DEFAULT_REINDEX_MIN_BYTES,
            reindex_max_bytes: None,
            cache_max_mb: None,
            body_limit: server_defaults::DEFAULT_BODY_LIMIT,
            query_timeout_ms: server_defaults::DEFAULT_QUERY_TIMEOUT_MS,
            query_min_t_timeout_ms: server_defaults::DEFAULT_QUERY_MIN_T_TIMEOUT_MS,
            query_refresh_enabled: server_defaults::DEFAULT_QUERY_REFRESH_ENABLED,
            query_refresh_ttl_ms: server_defaults::DEFAULT_QUERY_REFRESH_TTL_MS,
            import_presign_enabled: false,
            import_direct_max_bytes: 6_291_456,
            import_staging_dir: None,
            import_multipart_threshold_bytes: 5_368_709_120,
            import_multipart_part_size_bytes: 268_435_456,
            log_level: server_defaults::DEFAULT_LOG_LEVEL.to_string(),
            events_auth_mode: EventsAuthMode::None,
            events_auth_audience: None,
            events_auth_trusted_issuers: Vec::new(),
            events_auth_insecure_accept_any_issuer: false,
            // Data auth defaults
            data_auth_mode: DataAuthMode::None,
            data_auth_audience: None,
            data_auth_trusted_issuers: Vec::new(),
            data_auth_default_policy_class: None,
            data_auth_insecure_accept_any_issuer: false,
            // JWKS defaults
            #[cfg(feature = "oidc")]
            jwks_issuers: Vec::new(),
            #[cfg(feature = "oidc")]
            jwks_cache_ttl: server_defaults::DEFAULT_JWKS_CACHE_TTL,
            // Peer mode defaults
            server_role: ServerRole::Transaction,
            tx_server_url: None,
            peer_events_url: None,
            peer_events_token: None,
            peer_subscribe_all: false,
            peer_ledgers: Vec::new(),
            peer_graph_sources: Vec::new(),
            peer_reconnect_initial_ms: server_defaults::DEFAULT_PEER_RECONNECT_INITIAL_MS,
            peer_reconnect_max_ms: server_defaults::DEFAULT_PEER_RECONNECT_MAX_MS,
            peer_reconnect_multiplier: server_defaults::DEFAULT_PEER_RECONNECT_MULTIPLIER,
            // Storage proxy defaults
            storage_proxy_enabled: false,
            storage_proxy_trusted_issuers: Vec::new(),
            storage_proxy_default_identity: None,
            storage_proxy_default_policy_class: None,
            storage_proxy_debug_headers: false,
            storage_proxy_insecure_accept_any_issuer: false,
            // Peer storage access mode defaults
            storage_access_mode: StorageAccessMode::Shared,
            storage_proxy_token: None,
            storage_proxy_token_file: None,
            // MCP defaults
            mcp_enabled: false,
            mcp_auth_trusted_issuers: Vec::new(),
            mcp_auth_insecure_accept_any_issuer: false,
            mcp_agent_json_max_bytes: server_defaults::DEFAULT_MCP_AGENT_JSON_MAX_BYTES,
            mcp_query_timeout_ms: server_defaults::DEFAULT_MCP_QUERY_TIMEOUT_MS,
            // Admin auth defaults
            admin_auth_mode: AdminAuthMode::None,
            admin_auth_trusted_issuers: Vec::new(),
            admin_auth_insecure_accept_any_issuer: false,
            // Raft defaults
            #[cfg(feature = "raft")]
            raft_enabled: false,
            #[cfg(feature = "raft")]
            raft_node_id: None,
            #[cfg(feature = "raft")]
            raft_storage_path: None,
            #[cfg(feature = "raft")]
            raft_listen_addr: None,
        }
    }
}

impl ServerConfig {
    /// Create config from CLI args
    pub fn from_args() -> Self {
        Self::parse()
    }

    /// Check if using file storage (vs memory)
    pub fn is_file_storage(&self) -> bool {
        self.storage_path.is_some() && self.connection_config.is_none()
    }

    /// Check if using a connection config file (S3, DynamoDB, etc.)
    pub fn has_connection_config(&self) -> bool {
        self.connection_config.is_some()
    }

    /// Get storage type string for logging
    pub fn storage_type_str(&self) -> &'static str {
        if self.connection_config.is_some() {
            "connection-config"
        } else if self.storage_path.is_some() {
            "file"
        } else {
            "memory"
        }
    }

    /// Get the events authentication configuration
    pub fn events_auth(&self) -> EventsAuthConfig {
        EventsAuthConfig {
            mode: self.events_auth_mode,
            audience: self.events_auth_audience.clone(),
            trusted_issuers: self.events_auth_trusted_issuers.clone(),
            insecure_accept_any_issuer: self.events_auth_insecure_accept_any_issuer,
            has_jwks_issuers: self.has_jwks_issuers(),
        }
    }

    /// Get the data API authentication configuration
    pub fn data_auth(&self) -> DataAuthConfig {
        DataAuthConfig {
            mode: self.data_auth_mode,
            audience: self.data_auth_audience.clone(),
            trusted_issuers: self.data_auth_trusted_issuers.clone(),
            default_policy_class: self.data_auth_default_policy_class.clone(),
            insecure_accept_any_issuer: self.data_auth_insecure_accept_any_issuer,
            has_jwks_issuers: self.has_jwks_issuers(),
        }
    }

    /// Check whether any JWKS issuers are configured.
    pub fn has_jwks_issuers(&self) -> bool {
        #[cfg(feature = "oidc")]
        {
            !self.jwks_issuers.is_empty()
        }
        #[cfg(not(feature = "oidc"))]
        {
            false
        }
    }

    /// Parse JWKS issuer configurations from CLI args.
    ///
    /// Each entry is formatted as `issuer_url=jwks_url`.
    /// Returns parsed configs, or an error if any entry is malformed.
    #[cfg(feature = "oidc")]
    pub fn jwks_issuer_configs(&self) -> Result<Vec<crate::jwks::JwksIssuerConfig>, String> {
        let mut configs = Vec::new();
        for entry in &self.jwks_issuers {
            let (issuer, jwks_url) = entry.split_once('=').ok_or_else(|| {
                format!("Invalid --jwks-issuer format: '{entry}'. Expected 'issuer_url=jwks_url'")
            })?;
            if issuer.is_empty() || jwks_url.is_empty() {
                return Err(format!(
                    "Invalid --jwks-issuer format: '{entry}'. Both issuer_url and jwks_url must be non-empty"
                ));
            }
            configs.push(crate::jwks::JwksIssuerConfig {
                issuer: issuer.to_string(),
                jwks_url: jwks_url.to_string(),
            });
        }
        Ok(configs)
    }

    /// Get the storage proxy configuration
    pub fn storage_proxy(&self) -> StorageProxyConfig {
        StorageProxyConfig {
            enabled: self.storage_proxy_enabled,
            trusted_issuers: if self.storage_proxy_trusted_issuers.is_empty() {
                None
            } else {
                Some(self.storage_proxy_trusted_issuers.clone())
            },
            default_identity: self.storage_proxy_default_identity.clone(),
            default_policy_class: self.storage_proxy_default_policy_class.clone(),
            emit_debug_headers: self.storage_proxy_debug_headers,
            insecure_accept_any_issuer: self.storage_proxy_insecure_accept_any_issuer,
            has_jwks_issuers: self.has_jwks_issuers(),
        }
    }

    /// Get the MCP authentication configuration
    pub fn mcp_auth(&self) -> McpAuthConfig {
        McpAuthConfig {
            trusted_issuers: self.mcp_auth_trusted_issuers.clone(),
            insecure_accept_any_issuer: self.mcp_auth_insecure_accept_any_issuer,
        }
    }

    /// Get the admin authentication configuration
    pub fn admin_auth(&self) -> AdminAuthConfig {
        AdminAuthConfig {
            mode: self.admin_auth_mode,
            trusted_issuers: self.admin_auth_trusted_issuers.clone(),
            insecure_accept_any_issuer: self.admin_auth_insecure_accept_any_issuer,
            has_jwks_issuers: self.has_jwks_issuers(),
        }
    }

    /// Validate all configuration at startup
    pub fn validate(&self) -> Result<(), String> {
        // Validate JWKS issuer configs (parse early to catch format errors)
        #[cfg(feature = "oidc")]
        {
            self.jwks_issuer_configs()?;
        }

        // Validate events auth
        let events_auth = self.events_auth();
        events_auth.validate()?;

        // Validate data auth
        self.data_auth().validate()?;

        // Validate storage proxy
        self.storage_proxy().validate(&events_auth)?;

        // Validate MCP auth
        self.mcp_auth().validate(self.mcp_enabled, &events_auth)?;

        // Validate admin auth
        self.admin_auth().validate(&events_auth)?;

        // Connection config file must exist if specified
        if let Some(ref path) = self.connection_config {
            if !path.exists() {
                return Err(format!(
                    "connection config file not found: {}",
                    path.display()
                ));
            }
        }

        // Warn if both connection_config and storage_path are set
        if self.connection_config.is_some() && self.storage_path.is_some() {
            tracing::warn!("--storage-path is ignored when --connection-config is set");
        }

        // Storage proxy is only intended for transaction servers (peers consume from it)
        if self.storage_proxy_enabled && self.server_role == ServerRole::Peer {
            return Err(
                "storage_proxy.enabled is only valid for server_role=transaction (not peer)"
                    .to_string(),
            );
        }

        // Raft validation: when enabled, node_id + storage_path +
        // listen_addr must all be set, and proxy storage is
        // incompatible with raft (raft replicates writes via the
        // log; proxy mode forwards to a remote tx server).
        #[cfg(feature = "raft")]
        if self.raft_enabled {
            if self.raft_node_id.is_none() {
                return Err("raft.enabled=true requires --raft-node-id".to_string());
            }
            if self.raft_storage_path.is_none() {
                return Err("raft.enabled=true requires --raft-storage-path".to_string());
            }
            if self.raft_listen_addr.is_none() {
                return Err("raft.enabled=true requires --raft-listen-addr".to_string());
            }
            if self.is_proxy_storage_mode() {
                return Err(
                    "raft.enabled=true is incompatible with storage-access-mode=proxy".to_string(),
                );
            }
            // The raft log + snapshot tree (raft_storage_path) and
            // the ledger content store (storage_path) both manage
            // their own directory layouts; overlapping them lets
            // either side blow away the other's files on
            // compaction/eviction, and tends to surface only after a
            // restart corrupts state. Catch the misconfiguration up
            // front rather than mid-recovery. Comparison is lexical
            // (the dirs may not exist yet at validation time, so
            // `canonicalize` would fail); operators using symlink
            // aliasing tricks bypass this knowingly.
            if let (Some(raft_path), Some(storage_path)) =
                (self.raft_storage_path.as_ref(), self.storage_path.as_ref())
            {
                if raft_path == storage_path
                    || raft_path.starts_with(storage_path)
                    || storage_path.starts_with(raft_path)
                {
                    return Err(format!(
                        "raft.storage_path ({}) must not equal or be nested under \
                         storage.path ({}) (or vice versa) — the raft log + state-machine \
                         snapshots and ledger content store need disjoint filesystem subtrees",
                        raft_path.display(),
                        storage_path.display(),
                    ));
                }
            }
        }

        // Peer mode validation
        if self.server_role == ServerRole::Peer {
            // Require transaction server URL
            if self.tx_server_url.is_none() {
                return Err("server_role=peer requires --tx-server-url".to_string());
            }

            // Divergent validation based on storage access mode
            match self.storage_access_mode {
                StorageAccessMode::Shared => {
                    // Shared mode: require either storage path or connection config
                    if self.storage_path.is_none() && self.connection_config.is_none() {
                        return Err(
                            "server_role=peer + storage-access-mode=shared requires --storage-path or --connection-config"
                                .to_string(),
                        );
                    }
                }
                StorageAccessMode::Proxy => {
                    // Proxy mode: require token (inline or file)
                    if self.storage_proxy_token.is_none() && self.storage_proxy_token_file.is_none()
                    {
                        return Err("server_role=peer + storage-access-mode=proxy requires \
                             --storage-proxy-token or --storage-proxy-token-file"
                            .to_string());
                    }
                    // Storage path NOT required in proxy mode (warn if provided)
                    if self.storage_path.is_some() {
                        tracing::warn!("--storage-path is ignored in storage-access-mode=proxy");
                    }
                }
            }

            // Require subscription scope
            if !self.peer_subscribe_all
                && self.peer_ledgers.is_empty()
                && self.peer_graph_sources.is_empty()
            {
                return Err(
                    "server_role=peer requires --peer-subscribe-all or at least one --peer-ledger/--peer-graph-source"
                        .to_string(),
                );
            }

            // Validate reconnect parameters
            if self.peer_reconnect_initial_ms == 0 {
                return Err("peer_reconnect_initial_ms must be > 0".to_string());
            }
            if self.peer_reconnect_max_ms < self.peer_reconnect_initial_ms {
                return Err(
                    "peer_reconnect_max_ms must be >= peer_reconnect_initial_ms".to_string()
                );
            }
            if self.peer_reconnect_multiplier < 1.0 {
                return Err("peer_reconnect_multiplier must be >= 1.0".to_string());
            }
        }

        Ok(())
    }

    /// Get the effective peer events URL
    pub fn peer_events_url(&self) -> Option<String> {
        self.peer_events_url.clone().or_else(|| {
            self.tx_server_url
                .as_ref()
                .map(|base| format!("{base}/v1/fluree/events"))
        })
    }

    /// Load the peer events token, resolving @filepath if needed
    pub fn load_peer_events_token(&self) -> Result<Option<String>, std::io::Error> {
        match &self.peer_events_token {
            Some(token) if token.starts_with('@') => {
                let path = shellexpand(&token[1..]);
                let content = std::fs::read_to_string(path)?;
                Ok(Some(content.trim().to_string()))
            }
            Some(token) => Ok(Some(token.clone())),
            None => Ok(None),
        }
    }

    /// Build peer subscription config
    pub fn peer_subscription(&self) -> PeerSubscription {
        PeerSubscription {
            all: self.peer_subscribe_all,
            ledgers: self.peer_ledgers.clone(),
            graph_sources: self.peer_graph_sources.clone(),
        }
    }

    /// Check if running in peer mode
    pub fn is_peer_mode(&self) -> bool {
        self.server_role == ServerRole::Peer
    }

    /// Check if peer is using proxy storage access mode
    pub fn is_proxy_storage_mode(&self) -> bool {
        self.server_role == ServerRole::Peer && self.storage_access_mode == StorageAccessMode::Proxy
    }

    /// Load the storage proxy token for peer proxy mode.
    ///
    /// Supports:
    /// - Inline token via `--storage-proxy-token`
    /// - Token from file via `--storage-proxy-token-file`
    /// - @filepath syntax in `--storage-proxy-token` (e.g., `@/path/to/token`)
    pub fn load_storage_proxy_token(&self) -> Result<String, std::io::Error> {
        // Try inline token first
        if let Some(token) = &self.storage_proxy_token {
            // Handle @filepath syntax
            if let Some(path) = token.strip_prefix('@') {
                let expanded = shellexpand(path);
                let content = std::fs::read_to_string(&expanded)?;
                return Ok(content.trim().to_string());
            }
            return Ok(token.clone());
        }

        // Try token file
        if let Some(path) = &self.storage_proxy_token_file {
            let content = std::fs::read_to_string(path)?;
            return Ok(content.trim().to_string());
        }

        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "No storage proxy token configured",
        ))
    }
}

/// Simple shell expansion for ~ in paths
fn shellexpand(path: &str) -> String {
    if path.starts_with("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return format!("{}{}", home.to_string_lossy(), &path[1..]);
        }
    }
    path.to_string()
}

#[cfg(all(test, feature = "raft"))]
mod raft_validation_tests {
    use super::*;
    use std::net::SocketAddr;
    use std::path::PathBuf;

    fn raft_enabled_base() -> ServerConfig {
        ServerConfig {
            raft_enabled: true,
            raft_node_id: Some(1),
            raft_listen_addr: Some(SocketAddr::from(([127, 0, 0, 1], 9001))),
            ..Default::default()
        }
    }

    #[test]
    fn rejects_equal_raft_and_storage_paths() {
        let mut cfg = raft_enabled_base();
        cfg.storage_path = Some(PathBuf::from("/var/lib/fluree"));
        cfg.raft_storage_path = Some(PathBuf::from("/var/lib/fluree"));
        let err = cfg.validate().expect_err("must reject identical paths");
        assert!(
            err.contains("raft.storage_path") && err.contains("storage.path"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn rejects_raft_nested_under_storage() {
        let mut cfg = raft_enabled_base();
        cfg.storage_path = Some(PathBuf::from("/var/lib/fluree"));
        cfg.raft_storage_path = Some(PathBuf::from("/var/lib/fluree/raft"));
        let err = cfg.validate().expect_err("must reject nested raft path");
        assert!(err.contains("disjoint"), "unexpected error message: {err}");
    }

    #[test]
    fn rejects_storage_nested_under_raft() {
        let mut cfg = raft_enabled_base();
        cfg.raft_storage_path = Some(PathBuf::from("/srv/raft"));
        cfg.storage_path = Some(PathBuf::from("/srv/raft/data"));
        let err = cfg.validate().expect_err("must reject nested storage path");
        assert!(err.contains("disjoint"), "unexpected error message: {err}");
    }

    #[test]
    fn accepts_disjoint_paths() {
        let mut cfg = raft_enabled_base();
        cfg.storage_path = Some(PathBuf::from("/var/lib/fluree/data"));
        cfg.raft_storage_path = Some(PathBuf::from("/var/lib/fluree/raft"));
        cfg.validate()
            .expect("sibling dirs should validate cleanly");
    }

    #[test]
    fn accepts_raft_without_local_storage_path() {
        // Connection-config-driven deployments don't set
        // `storage_path` at all — the disjoint check should noop.
        let mut cfg = raft_enabled_base();
        cfg.raft_storage_path = Some(PathBuf::from("/var/lib/fluree/raft"));
        cfg.storage_path = None;
        cfg.validate()
            .expect("missing storage_path should skip the disjoint check");
    }
}
