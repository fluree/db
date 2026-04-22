//! Fluree Search HTTP Server
//!
//! A standalone HTTP server that provides search services for Fluree DB.
//! This server can be deployed independently of Fluree DB instances to
//! provide centralized search capabilities.
//!
//! # Endpoints
//!
//! - `POST /v1/search` - Execute a search query
//! - `GET /v1/capabilities` - Get server capabilities
//! - `GET /v1/health` - Health check
//!
//! # Configuration
//!
//! The server requires explicit storage and nameservice paths to access
//! the index data. See `--help` for all configuration options.
//!
//! # Example
//!
//! ```bash
//! fluree-search-httpd \
//!   --storage-root file:///var/fluree/data \
//!   --nameservice-path file:///var/fluree/ns \
//!   --listen 0.0.0.0:9090
//! ```

use async_trait::async_trait;
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use fluree_db_core::{ContentStore, FileStorage};
use fluree_db_nameservice::file::FileNameService;
use fluree_db_nameservice::GraphSourceLookup;
use fluree_db_query::bm25::{deserialize, Bm25Index, Bm25Manifest};
#[cfg(feature = "vector")]
use fluree_db_query::vector::usearch::{deserialize as vector_deserialize, VectorIndex};
use fluree_search_protocol::{Capabilities, SearchError, SearchRequest, SearchResponse};
use fluree_search_service::backend::{Bm25Backend, Bm25BackendConfig, IndexLoader};
use fluree_search_service::backend::{CompositeBackend, SearchBackend};
#[cfg(feature = "vector")]
use fluree_search_service::backend::{VectorBackend, VectorBackendConfig, VectorIndexLoader};
use fluree_search_service::error::{Result as ServiceResult, ServiceError};
use fluree_search_service::sync::SyncConfig;
use serde::Serialize;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tower_http::trace::TraceLayer;
use tracing::{error, info};

/// Fluree Search HTTP Server
#[derive(Parser, Debug)]
#[command(name = "fluree-search-httpd")]
#[command(about = "HTTP server for Fluree DB search service")]
struct Args {
    /// Storage root path (e.g., /var/fluree/data or file:///var/fluree/data)
    #[arg(long, env = "FLUREE_STORAGE_ROOT")]
    storage_root: String,

    /// Nameservice path (e.g., /var/fluree/ns or file:///var/fluree/ns)
    #[arg(long, env = "FLUREE_NAMESERVICE_PATH")]
    nameservice_path: String,

    /// Listen address
    #[arg(long, default_value = "0.0.0.0:9090", env = "FLUREE_SEARCH_LISTEN")]
    listen: SocketAddr,

    /// Maximum number of cached indexes
    #[arg(long, default_value = "100", env = "FLUREE_SEARCH_CACHE_MAX_ENTRIES")]
    cache_max_entries: usize,

    /// Cache TTL in seconds
    #[arg(long, default_value = "300", env = "FLUREE_SEARCH_CACHE_TTL_SECS")]
    cache_ttl_secs: u64,

    /// Maximum search results limit
    #[arg(long, default_value = "1000", env = "FLUREE_SEARCH_MAX_LIMIT")]
    max_limit: usize,

    /// Default request timeout in milliseconds
    #[arg(
        long,
        default_value = "30000",
        env = "FLUREE_SEARCH_DEFAULT_TIMEOUT_MS"
    )]
    default_timeout_ms: u64,

    /// Maximum request timeout in milliseconds
    #[arg(long, default_value = "300000", env = "FLUREE_SEARCH_MAX_TIMEOUT_MS")]
    max_timeout_ms: u64,
}

/// Application state shared across handlers.
struct AppState {
    /// Composite search backend (dispatches to BM25 and/or vector backends).
    backend: CompositeBackend,
    /// Maximum result limit.
    max_limit: usize,
    /// Maximum timeout.
    max_timeout_ms: u64,
}

/// Index loader implementation using file-based storage and nameservice.
///
/// This loader uses:
/// - `FileStorage` for reading BM25 index bytes from storage
/// - `FileNameService` for looking up graph source snapshot history
#[derive(Debug, Clone)]
struct FileIndexLoader {
    storage: FileStorage,
    nameservice: FileNameService,
}

impl FileIndexLoader {
    fn new(storage_root: impl Into<PathBuf>, nameservice_path: impl Into<PathBuf>) -> Self {
        Self {
            storage: FileStorage::new(storage_root),
            nameservice: FileNameService::new(nameservice_path),
        }
    }

    /// Load the BM25 manifest from CAS via the nameservice head pointer.
    ///
    /// Returns an empty manifest if the graph source has no index_address yet.
    async fn load_manifest(&self, graph_source_id: &str) -> ServiceResult<Bm25Manifest> {
        let record = self
            .nameservice
            .lookup_graph_source(graph_source_id)
            .await
            .map_err(|e| ServiceError::Internal {
                message: format!("Nameservice error: {e}"),
            })?;

        let record = match record {
            Some(r) => r,
            None => return Ok(Bm25Manifest::new(graph_source_id)),
        };

        let index_cid = match &record.index_id {
            Some(cid) => cid,
            None => return Ok(Bm25Manifest::new(graph_source_id)),
        };

        let cs = fluree_db_core::content_store_for(self.storage.clone(), graph_source_id);
        let bytes = cs
            .get(index_cid)
            .await
            .map_err(|e| ServiceError::Internal {
                message: format!("Storage error loading manifest: {e}"),
            })?;

        let manifest: Bm25Manifest =
            serde_json::from_slice(&bytes).map_err(|e| ServiceError::Internal {
                message: format!("Manifest deserialize error: {e}"),
            })?;

        Ok(manifest)
    }
}

#[async_trait]
impl IndexLoader for FileIndexLoader {
    async fn load_index(&self, graph_source_id: &str, index_t: i64) -> ServiceResult<Bm25Index> {
        // Load the manifest and find the snapshot for this index_t
        let manifest = self.load_manifest(graph_source_id).await?;

        let entry = manifest
            .snapshots
            .iter()
            .find(|e| e.index_t == index_t)
            .ok_or_else(|| ServiceError::Internal {
                message: format!("No snapshot found for {graph_source_id} at t={index_t}"),
            })?;

        // Load index bytes via content store
        let cs = fluree_db_core::content_store_for(self.storage.clone(), graph_source_id);
        let bytes = cs
            .get(&entry.snapshot_id)
            .await
            .map_err(|e| ServiceError::Internal {
                message: format!("Storage error: {e}"),
            })?;

        // Deserialize the index
        let index = deserialize(&bytes).map_err(|e| ServiceError::Internal {
            message: format!("Deserialize error: {e}"),
        })?;

        Ok(index)
    }

    async fn get_latest_index_t(&self, graph_source_id: &str) -> ServiceResult<Option<i64>> {
        let manifest = self.load_manifest(graph_source_id).await?;
        Ok(manifest.head().map(|e| e.index_t))
    }

    async fn find_snapshot_for_t(
        &self,
        graph_source_id: &str,
        target_t: i64,
    ) -> ServiceResult<Option<i64>> {
        let manifest = self.load_manifest(graph_source_id).await?;
        // select_snapshot returns the newest snapshot <= target_t
        Ok(manifest.select_snapshot(target_t).map(|e| e.index_t))
    }

    async fn get_index_head(&self, graph_source_id: &str) -> ServiceResult<Option<i64>> {
        self.get_latest_index_t(graph_source_id).await
    }
}

/// Vector index loader implementation using file-based storage and nameservice.
///
/// This loader uses the same storage and nameservice infrastructure as
/// [`FileIndexLoader`] but deserializes [`VectorIndex`] snapshots instead
/// of BM25 indexes.
#[cfg(feature = "vector")]
#[derive(Debug, Clone)]
struct FileVectorIndexLoader {
    storage: FileStorage,
    nameservice: FileNameService,
}

#[cfg(feature = "vector")]
impl FileVectorIndexLoader {
    fn new(storage_root: impl Into<PathBuf>, nameservice_path: impl Into<PathBuf>) -> Self {
        Self {
            storage: FileStorage::new(storage_root),
            nameservice: FileNameService::new(nameservice_path),
        }
    }
}

#[cfg(feature = "vector")]
#[async_trait]
impl VectorIndexLoader for FileVectorIndexLoader {
    async fn load_index(&self, graph_source_id: &str, _index_t: i64) -> ServiceResult<VectorIndex> {
        // Vector is head-only: always load from the nameservice head pointer
        let record = self
            .nameservice
            .lookup_graph_source(graph_source_id)
            .await
            .map_err(|e| ServiceError::Internal {
                message: format!("Nameservice error: {e}"),
            })?;

        let record = record.ok_or_else(|| ServiceError::GraphSourceNotFound {
            address: graph_source_id.to_string(),
        })?;

        let index_cid = record.index_id.ok_or_else(|| ServiceError::Internal {
            message: format!("No index CID for vector graph source: {graph_source_id}"),
        })?;

        let cs = fluree_db_core::content_store_for(self.storage.clone(), graph_source_id);
        let bytes = cs
            .get(&index_cid)
            .await
            .map_err(|e| ServiceError::Internal {
                message: format!("Storage error: {e}"),
            })?;

        let index = vector_deserialize(&bytes).map_err(|e| ServiceError::Internal {
            message: format!("Vector index deserialize error: {e}"),
        })?;

        Ok(index)
    }

    async fn get_latest_index_t(&self, graph_source_id: &str) -> ServiceResult<Option<i64>> {
        let record = self
            .nameservice
            .lookup_graph_source(graph_source_id)
            .await
            .map_err(|e| ServiceError::Internal {
                message: format!("Nameservice error: {e}"),
            })?;

        Ok(record.and_then(|r| {
            if r.index_id.is_some() {
                Some(r.index_t)
            } else {
                None
            }
        }))
    }

    async fn get_index_head(&self, graph_source_id: &str) -> ServiceResult<Option<i64>> {
        self.get_latest_index_t(graph_source_id).await
    }
}

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("fluree_search_httpd=info".parse().unwrap())
                .add_directive("fluree_search_service=info".parse().unwrap())
                .add_directive("tower_http=info".parse().unwrap()),
        )
        .init();

    let args = Args::parse();

    info!(
        storage_root = %args.storage_root,
        nameservice_path = %args.nameservice_path,
        listen = %args.listen,
        "Starting Fluree Search HTTP Server"
    );

    // Parse storage and nameservice paths
    let storage_path = parse_path(&args.storage_root);
    let ns_path = parse_path(&args.nameservice_path);

    // Create BM25 backend
    let bm25_loader = FileIndexLoader::new(&storage_path, &ns_path);
    let bm25_config = Bm25BackendConfig {
        cache_max_entries: args.cache_max_entries,
        cache_ttl_secs: args.cache_ttl_secs,
        max_concurrent_loads: 4,
        default_timeout_ms: args.default_timeout_ms,
        sync_config: SyncConfig::default(),
    };
    let bm25_backend = Bm25Backend::new(bm25_loader, bm25_config);

    // Assemble backends into composite
    #[allow(unused_mut)]
    let mut backends: Vec<Box<dyn SearchBackend>> = vec![Box::new(bm25_backend)];

    #[cfg(feature = "vector")]
    {
        let vector_loader = FileVectorIndexLoader::new(&storage_path, &ns_path);
        let vector_config = VectorBackendConfig {
            cache_max_entries: args.cache_max_entries,
            cache_ttl_secs: args.cache_ttl_secs,
            max_concurrent_loads: 4,
            default_timeout_ms: args.default_timeout_ms,
            sync_config: SyncConfig::default(),
        };
        let vector_backend = VectorBackend::new(vector_loader, vector_config);
        backends.push(Box::new(vector_backend));
        info!("Vector search backend enabled");
    }

    let backend = CompositeBackend::new(backends);

    // Create app state
    let state = Arc::new(AppState {
        backend,
        max_limit: args.max_limit,
        max_timeout_ms: args.max_timeout_ms,
    });

    // Build router
    let app = Router::new()
        .route("/v1/search", post(handle_search))
        .route("/v1/capabilities", get(handle_capabilities))
        .route("/v1/health", get(handle_health))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    // Start server
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .expect("Failed to bind address");

    info!(address = %args.listen, "Server listening");

    axum::serve(listener, app).await.expect("Server error");
}

/// Parse a path, stripping file:// prefix if present.
fn parse_path(path: &str) -> PathBuf {
    if let Some(stripped) = path.strip_prefix("file://") {
        PathBuf::from(stripped)
    } else {
        PathBuf::from(path)
    }
}

/// Handle POST /v1/search
async fn handle_search(
    State(state): State<Arc<AppState>>,
    Json(request): Json<SearchRequest>,
) -> impl IntoResponse {
    let start = Instant::now();
    let request_id = request.request_id.clone();

    // Validate limit
    let limit = request.limit.min(state.max_limit);

    // Validate timeout
    let timeout_ms = request
        .timeout_ms
        .map(|t| t.min(state.max_timeout_ms))
        .or(Some(30_000));

    // Execute search
    let result = state
        .backend
        .search(
            &request.graph_source_id,
            &request.query,
            limit,
            request.as_of_t,
            request.sync,
            timeout_ms,
        )
        .await;

    let took_ms = start.elapsed().as_millis() as u64;

    match result {
        Ok((index_t, hits)) => {
            let response = SearchResponse::new(
                fluree_search_protocol::PROTOCOL_VERSION.to_string(),
                request_id,
                index_t,
                hits,
                took_ms,
            );
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            // Map error to HTTP status code
            let status = match &e {
                ServiceError::GraphSourceNotFound { .. }
                | ServiceError::NoSnapshotForAsOfT { .. }
                | ServiceError::IndexNotBuilt { .. } => StatusCode::NOT_FOUND,
                ServiceError::SyncTimeout { .. } | ServiceError::Timeout { .. } => {
                    StatusCode::GATEWAY_TIMEOUT
                }
                ServiceError::InvalidRequest { .. }
                | ServiceError::UnsupportedProtocolVersion { .. } => StatusCode::BAD_REQUEST,
                ServiceError::StorageError { .. }
                | ServiceError::NameserviceError { .. }
                | ServiceError::Internal { .. } => {
                    error!(?e, "Internal error during search");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            };

            let error = SearchError::new(
                fluree_search_protocol::PROTOCOL_VERSION.to_string(),
                request_id,
                e.error_code(),
                e.to_string(),
            );

            (status, Json(error)).into_response()
        }
    }
}

/// Handle GET /v1/capabilities
async fn handle_capabilities(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    #[allow(unused_mut)]
    let mut supported_query_kinds = vec!["bm25".to_string()];

    #[cfg(feature = "vector")]
    supported_query_kinds.push("vector".to_string());

    let capabilities = Capabilities {
        protocol_version: fluree_search_protocol::PROTOCOL_VERSION.to_string(),
        bm25_analyzer_version: fluree_search_protocol::BM25_ANALYZER_VERSION.to_string(),
        supported_query_kinds,
        max_limit: state.max_limit,
        max_timeout_ms: state.max_timeout_ms,
    };

    Json(capabilities)
}

/// Health check response.
#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
}

/// Handle GET /v1/health
async fn handle_health() -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
    })
}
