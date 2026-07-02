//! Caching infrastructure for graph source operations.
//!
//! Provides caches for R2RML compiled mappings and Iceberg table metadata
//! to avoid repeated expensive operations.
//!
//! Uses `moka::sync::Cache` for lock-free concurrent reads. This avoids
//! the write-lock contention that `RwLock<LruCache>` would impose on
//! concurrent queries (LRU `get()` requires `&mut self` to update recency).

use fluree_db_r2rml::mapping::CompiledR2rmlMapping;
use std::sync::Arc;

#[cfg(feature = "iceberg")]
use super::catalog_session::CachedLoadTable;
#[cfg(feature = "iceberg")]
use fluree_db_iceberg::catalog::RestCatalogClient;
#[cfg(feature = "iceberg")]
use fluree_db_iceberg::{io::parquet::ParquetFooterCache, metadata::TableMetadata, DataFile};
#[cfg(feature = "iceberg")]
use std::time::Duration;

#[cfg(feature = "iceberg")]
#[derive(Debug, Clone)]
pub(crate) struct CachedScanFiles {
    pub data_files: Arc<Vec<DataFile>>,
    pub estimated_row_count: i64,
    pub files_selected: usize,
    pub files_pruned: usize,
}

#[cfg(feature = "iceberg")]
const DIRECT_METADATA_LOCATION_TTL: Duration = Duration::from_secs(2);

/// Default cross-query `loadTable`-response cache TTL. A REST `loadTable` GET
/// costs ~1.3–3s against Snowflake Horizon, so caching it across queries lets a
/// burst of queries against the same tables skip the round-trip. The TTL bounds
/// how stale a snapshot a *new* query can observe (an in-flight query pins its
/// own snapshot regardless). Every read is also gated on vended-credential
/// expiry. Override with `FLUREE_ICEBERG_LOADTABLE_TTL_SECS` (`0` disables the
/// cross-query layer, leaving only the per-query pin).
#[cfg(feature = "iceberg")]
const DEFAULT_REST_LOADTABLE_TTL_SECS: u64 = 60;

#[cfg(feature = "iceberg")]
fn rest_loadtable_ttl_secs() -> u64 {
    std::env::var("FLUREE_ICEBERG_LOADTABLE_TTL_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_REST_LOADTABLE_TTL_SECS)
}

/// Default TTL for the process-wide REST catalog client cache. Reusing a client
/// preserves its OAuth `CachedToken` and HTTPS connection pool, but the cache is
/// keyed by a fingerprint of the *raw* config JSON. When a Bearer/OAuth secret is
/// sourced from an env var or secret store, that JSON stores the reference, not
/// the secret, so rotating the secret does not change the fingerprint — without a
/// TTL the stale client (and its cached token) would serve 401s until LRU
/// eviction or a process restart. A bounded TTL lets a rotated secret self-heal:
/// the client rebuilds and re-authenticates within the window. Override with
/// `FLUREE_ICEBERG_REST_CLIENT_TTL_SECS` (`0` rebuilds the client every query).
#[cfg(feature = "iceberg")]
const DEFAULT_REST_CLIENT_TTL_SECS: u64 = 900;

#[cfg(feature = "iceberg")]
fn rest_client_ttl_secs() -> u64 {
    std::env::var("FLUREE_ICEBERG_REST_CLIENT_TTL_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_REST_CLIENT_TTL_SECS)
}

/// Cache for R2RML compiled mappings and Iceberg table metadata.
///
/// This cache is shared across queries to avoid repeated:
/// - R2RML mapping compilation (parsing + validation)
/// - Iceberg catalog calls (load table metadata)
/// - S3 metadata reads
///
/// # Cache Keys
///
/// - **Compiled mappings**: Keyed by `(graph_source_id, mapping_source)` - invalidated when
///   graph source config changes or mapping file is updated.
/// - **Table metadata**: Keyed by `metadata_location` - the S3 path is a content hash,
///   so different snapshots have different keys.
///
/// # Thread Safety
///
/// Uses `moka::sync::Cache` for lock-free concurrent reads.
pub struct R2rmlCache {
    /// Cache for compiled R2RML mappings.
    compiled_mappings: moka::sync::Cache<String, Arc<CompiledR2rmlMapping>>,

    /// Cache for parsed Iceberg table metadata.
    #[cfg(feature = "iceberg")]
    table_metadata: moka::sync::Cache<String, Arc<TableMetadata>>,

    /// Cache for manifest-derived file selections keyed by metadata location.
    #[cfg(feature = "iceberg")]
    scan_files: moka::sync::Cache<String, Arc<CachedScanFiles>>,

    /// Shared Parquet footer cache for repeated scans of the same files.
    /// `Arc` so it can be shared into per-file read workers.
    #[cfg(feature = "iceberg")]
    parquet_footers: Arc<ParquetFooterCache>,

    /// Short-lived cache for direct-catalog `version-hint.text` resolution.
    ///
    /// Uses moka's native TTL (`time_to_live`) so entries auto-expire.
    #[cfg(feature = "iceberg")]
    direct_metadata_locations: moka::sync::Cache<String, String>,

    /// Process-wide REST catalog clients keyed by source config fingerprint.
    /// Reused across queries so the OAuth `CachedToken` and the HTTPS connection
    /// pool survive — one token exchange per ~hour instead of one per query.
    /// The fingerprint is over the raw config JSON, so a secret changed *inline*
    /// in the config invalidates the client, but a secret referenced by env var
    /// / secret store does not (the JSON is unchanged). A TTL
    /// (`DEFAULT_REST_CLIENT_TTL_SECS`) bounds how long such a rotation stays
    /// stale before the client is rebuilt and re-authenticated.
    #[cfg(feature = "iceberg")]
    rest_clients: moka::sync::Cache<String, Arc<RestCatalogClient>>,

    /// Process-wide `loadTable` responses keyed by `(graph_source_id, ns.table)`,
    /// with a short TTL (see [`DEFAULT_REST_LOADTABLE_TTL_SECS`]) and a
    /// credential-expiry gate. Lets a burst of queries against the same table
    /// skip the ~1.3–3s catalog GET.
    #[cfg(feature = "iceberg")]
    rest_load_tables: moka::sync::Cache<String, Arc<CachedLoadTable>>,
}

// moka::sync::Cache is Send+Sync but doesn't implement Debug
impl std::fmt::Debug for R2rmlCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("R2rmlCache")
            .field(
                "compiled_mappings_len",
                &self.compiled_mappings.entry_count(),
            )
            .finish()
    }
}

impl R2rmlCache {
    /// Create a new cache with specified capacities.
    ///
    /// # Arguments
    ///
    /// * `mapping_capacity` - Max compiled mappings to cache (default: 64)
    /// * `metadata_capacity` - Max table metadata entries to cache (default: 128)
    pub fn new(mapping_capacity: usize, metadata_capacity: usize) -> Self {
        let mapping_cap = mapping_capacity.max(1) as u64;
        let metadata_cap = metadata_capacity.max(1) as u64;

        #[cfg(feature = "iceberg")]
        {
            Self {
                compiled_mappings: moka::sync::Cache::new(mapping_cap),
                table_metadata: moka::sync::Cache::new(metadata_cap),
                scan_files: moka::sync::Cache::new(metadata_cap),
                parquet_footers: Arc::new(ParquetFooterCache::new(
                    (metadata_capacity.max(1) / 2).max(32),
                )),
                direct_metadata_locations: moka::sync::Cache::builder()
                    .max_capacity(metadata_cap)
                    .time_to_live(DIRECT_METADATA_LOCATION_TTL)
                    .build(),
                // A process serves few distinct graph sources; a small cap is
                // plenty and bounds retained clients/connection pools. A TTL lets
                // an env-var/secret-store secret rotation self-heal (see
                // `DEFAULT_REST_CLIENT_TTL_SECS`) since the config fingerprint
                // does not change when the referenced secret does.
                rest_clients: moka::sync::Cache::builder()
                    .max_capacity(64)
                    .time_to_live(Duration::from_secs(rest_client_ttl_secs()))
                    .build(),
                rest_load_tables: moka::sync::Cache::builder()
                    .max_capacity(metadata_cap)
                    .time_to_live(Duration::from_secs(rest_loadtable_ttl_secs()))
                    .build(),
            }
        }

        #[cfg(not(feature = "iceberg"))]
        {
            let _ = metadata_cap;
            Self {
                compiled_mappings: moka::sync::Cache::new(mapping_cap),
            }
        }
    }

    /// Create a cache with default capacities.
    pub fn with_defaults() -> Self {
        Self::new(64, 128)
    }

    /// Get a compiled mapping from cache.
    pub async fn get_mapping(&self, cache_key: &str) -> Option<Arc<CompiledR2rmlMapping>> {
        self.compiled_mappings.get(cache_key)
    }

    /// Store a compiled mapping in cache.
    pub async fn put_mapping(&self, cache_key: String, mapping: Arc<CompiledR2rmlMapping>) {
        self.compiled_mappings.insert(cache_key, mapping);
    }

    /// Get table metadata from cache.
    #[cfg(feature = "iceberg")]
    pub async fn get_metadata(&self, metadata_location: &str) -> Option<Arc<TableMetadata>> {
        self.table_metadata.get(metadata_location)
    }

    /// Store table metadata in cache.
    #[cfg(feature = "iceberg")]
    pub async fn put_metadata(&self, metadata_location: String, metadata: Arc<TableMetadata>) {
        self.table_metadata.insert(metadata_location, metadata);
    }

    /// Get cached scan file selections for a metadata location.
    #[cfg(feature = "iceberg")]
    pub(crate) async fn get_scan_files(
        &self,
        metadata_location: &str,
    ) -> Option<Arc<CachedScanFiles>> {
        self.scan_files.get(metadata_location)
    }

    /// Store manifest-derived scan file selections for a metadata location.
    #[cfg(feature = "iceberg")]
    pub(crate) async fn put_scan_files(
        &self,
        metadata_location: String,
        scan_files: Arc<CachedScanFiles>,
    ) {
        self.scan_files.insert(metadata_location, scan_files);
    }

    /// Get the shared Parquet footer cache (clone the `Arc` to share into
    /// per-file read workers).
    #[cfg(feature = "iceberg")]
    pub fn parquet_footers(&self) -> Arc<ParquetFooterCache> {
        Arc::clone(&self.parquet_footers)
    }

    /// Get a recently resolved direct-catalog metadata location.
    ///
    /// Entries auto-expire after `DIRECT_METADATA_LOCATION_TTL` via moka's native TTL.
    #[cfg(feature = "iceberg")]
    pub(crate) async fn get_direct_metadata_location(
        &self,
        table_location: &str,
    ) -> Option<String> {
        self.direct_metadata_locations.get(table_location)
    }

    /// Store a resolved direct-catalog metadata location.
    #[cfg(feature = "iceberg")]
    pub(crate) async fn put_direct_metadata_location(
        &self,
        table_location: String,
        metadata_location: String,
    ) {
        self.direct_metadata_locations
            .insert(table_location, metadata_location);
    }

    /// Get a process-wide REST catalog client for a source config `fingerprint`,
    /// or `None` on miss (or when catalog caching is disabled).
    #[cfg(feature = "iceberg")]
    pub(crate) fn rest_client(&self, fingerprint: &str) -> Option<Arc<RestCatalogClient>> {
        if !super::catalog_session::cache_enabled() {
            return None;
        }
        self.rest_clients.get(fingerprint)
    }

    /// Store a REST catalog client for cross-query reuse (no-op when disabled).
    #[cfg(feature = "iceberg")]
    pub(crate) fn put_rest_client(&self, fingerprint: String, client: Arc<RestCatalogClient>) {
        if !super::catalog_session::cache_enabled() {
            return;
        }
        self.rest_clients.insert(fingerprint, client);
    }

    /// Get a cross-query `loadTable` response if cached, within TTL, and its
    /// vended credentials are not near expiry; otherwise `None` (an expired
    /// entry is invalidated). Returns `None` when caching or the cross-query
    /// layer (TTL=0) is disabled.
    #[cfg(feature = "iceberg")]
    pub(crate) fn get_rest_load_table(&self, key: &str) -> Option<Arc<CachedLoadTable>> {
        if !super::catalog_session::cache_enabled() || rest_loadtable_ttl_secs() == 0 {
            return None;
        }
        let hit = self.rest_load_tables.get(key)?;
        if hit.creds_expired() {
            self.rest_load_tables.invalidate(key);
            return None;
        }
        Some(hit)
    }

    /// Store a `loadTable` response in the cross-query cache (no-op when disabled
    /// or TTL=0).
    #[cfg(feature = "iceberg")]
    pub(crate) fn put_rest_load_table(&self, key: String, value: Arc<CachedLoadTable>) {
        if !super::catalog_session::cache_enabled() || rest_loadtable_ttl_secs() == 0 {
            return;
        }
        self.rest_load_tables.insert(key, value);
    }

    /// Clear all caches.
    pub async fn clear(&self) {
        self.compiled_mappings.invalidate_all();

        #[cfg(feature = "iceberg")]
        {
            self.table_metadata.invalidate_all();
            self.scan_files.invalidate_all();
            self.parquet_footers.clear().await;
            self.direct_metadata_locations.invalidate_all();
            self.rest_clients.invalidate_all();
            self.rest_load_tables.invalidate_all();
        }
    }

    /// Get cache statistics.
    pub async fn stats(&self) -> R2rmlCacheStats {
        R2rmlCacheStats {
            mapping_entries: self.compiled_mappings.entry_count() as usize,
            mapping_capacity: self.compiled_mappings.policy().max_capacity().unwrap_or(0) as usize,
            metadata_entries: {
                #[cfg(feature = "iceberg")]
                {
                    self.table_metadata.entry_count() as usize
                }
                #[cfg(not(feature = "iceberg"))]
                {
                    0
                }
            },
            metadata_capacity: {
                #[cfg(feature = "iceberg")]
                {
                    self.table_metadata.policy().max_capacity().unwrap_or(0) as usize
                }
                #[cfg(not(feature = "iceberg"))]
                {
                    0
                }
            },
        }
    }

    /// Generate a cache key for a compiled mapping.
    ///
    /// Uses `graph_source_id` + hash of `mapping_source` to handle both graph source identity
    /// and mapping file updates.
    ///
    /// The key includes:
    /// - `graph_source_id` - ensures different graph sources don't share mappings
    /// - `mapping_source` - the storage path/address
    /// - `media_type` - distinguishes same source parsed as different formats
    ///
    /// Note: This does NOT detect content changes at the same path.
    /// Use `r2rml_cache().clear()` to invalidate after updating mapping files.
    pub fn mapping_cache_key(
        graph_source_id: &str,
        mapping_source: &str,
        media_type: Option<&str>,
    ) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        mapping_source.hash(&mut hasher);
        media_type.hash(&mut hasher);
        let combined_hash = hasher.finish();

        format!("{graph_source_id}:{combined_hash:016x}")
    }
}

impl Default for R2rmlCache {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Statistics for R2RML cache usage.
#[derive(Debug, Clone)]
pub struct R2rmlCacheStats {
    /// Number of cached compiled mappings
    pub mapping_entries: usize,
    /// Maximum mapping cache capacity
    pub mapping_capacity: usize,
    /// Number of cached table metadata entries
    pub metadata_entries: usize,
    /// Maximum metadata cache capacity
    pub metadata_capacity: usize,
}

#[cfg(all(test, feature = "iceberg"))]
mod iceberg_tests {
    use super::*;
    use crate::graph_source::catalog_session::CachedLoadTable;
    use chrono::{Duration, Utc};
    use fluree_db_iceberg::credential::VendedCredentials;

    fn creds(expires_in_secs: i64) -> VendedCredentials {
        VendedCredentials {
            access_key_id: "AKIA".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: Some("token".to_string()),
            expires_at: Some(Utc::now() + Duration::seconds(expires_in_secs)),
            endpoint: None,
            region: Some("us-east-2".to_string()),
            path_style: false,
        }
    }

    fn entry(loc: &str, creds: Option<VendedCredentials>) -> Arc<CachedLoadTable> {
        Arc::new(CachedLoadTable {
            metadata_location: loc.to_string(),
            credentials: creds,
        })
    }

    #[test]
    fn cross_query_loadtable_put_get() {
        let cache = R2rmlCache::with_defaults();
        assert!(
            cache.get_rest_load_table("k1").is_none(),
            "empty cache misses"
        );
        cache.put_rest_load_table("k1".to_string(), entry("s3://m.json", Some(creds(3600))));
        assert_eq!(
            cache.get_rest_load_table("k1").unwrap().metadata_location,
            "s3://m.json"
        );
        assert!(
            cache.get_rest_load_table("k2").is_none(),
            "different key misses"
        );
    }

    #[test]
    fn cross_query_near_expiry_creds_is_a_miss() {
        let cache = R2rmlCache::with_defaults();
        // Inside the 30s refresh buffer → treated as expired.
        cache.put_rest_load_table("k".to_string(), entry("s3://m.json", Some(creds(10))));
        assert!(
            cache.get_rest_load_table("k").is_none(),
            "about-to-expire vended creds must not be served cross-query"
        );
    }

    #[test]
    fn cross_query_no_creds_never_expires() {
        let cache = R2rmlCache::with_defaults();
        cache.put_rest_load_table("k".to_string(), entry("s3://m.json", None));
        assert!(cache.get_rest_load_table("k").is_some());
    }

    #[tokio::test]
    async fn clear_empties_cross_query_loadtable() {
        let cache = R2rmlCache::with_defaults();
        cache.put_rest_load_table("k".to_string(), entry("s3://m.json", Some(creds(3600))));
        assert!(cache.get_rest_load_table("k").is_some());
        cache.clear().await;
        assert!(
            cache.get_rest_load_table("k").is_none(),
            "clear() drops cross-query entries"
        );
    }
}
