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

// moka's eviction clock reads std::time::Instant at cache construction, which
// aborts on wasm32-unknown-unknown; use core's clock-free LRU stand-in there.
#[cfg(target_arch = "wasm32")]
use fluree_db_core::wasm_cache::Cache as SyncCache;
#[cfg(not(target_arch = "wasm32"))]
use moka::sync::Cache as SyncCache;

#[cfg(feature = "iceberg")]
use super::catalog_session::CachedLoadTable;
#[cfg(feature = "iceberg")]
use fluree_db_iceberg::catalog::SendCatalogClient;
#[cfg(feature = "iceberg")]
use fluree_db_iceberg::{
    io::parquet::ParquetFooterCache, metadata::TableMetadata, DataFile, StorageScope,
};
#[cfg(feature = "sql")]
use fluree_db_sql::TrinoClient;
#[cfg(feature = "iceberg")]
use std::time::Duration;

#[cfg(feature = "iceberg")]
#[derive(Debug, Clone)]
pub(crate) struct CachedScanFiles {
    pub data_files: Arc<Vec<DataFile>>,
    pub estimated_row_count: i64,
    pub files_selected: usize,
    pub files_pruned: usize,
    /// Whether the source snapshot carried merge-on-read delete files. Only ever
    /// `true` when the fail-closed guard was overridden at plan time; carried on
    /// the cache entry so a scan-files cache HIT (in-memory or disk) can re-refuse
    /// if the override is later turned off, instead of silently serving a
    /// delete-bearing file list (audit F-AUD-1, cache-arm follow-up).
    pub has_delete_manifests: bool,
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
pub(crate) fn rest_client_ttl_secs() -> u64 {
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
    compiled_mappings: SyncCache<String, Arc<CompiledR2rmlMapping>>,

    /// Cache for parsed Iceberg table metadata.
    #[cfg(feature = "iceberg")]
    table_metadata: SyncCache<String, Arc<TableMetadata>>,

    /// Cache for manifest-derived file selections keyed by metadata location.
    #[cfg(feature = "iceberg")]
    scan_files: SyncCache<String, Arc<CachedScanFiles>>,

    /// Shared Parquet footer cache for repeated scans of the same files.
    /// `Arc` so it can be shared into per-file read workers.
    #[cfg(feature = "iceberg")]
    parquet_footers: Arc<ParquetFooterCache>,

    /// Short-lived cache for direct-catalog `version-hint.text` resolution.
    ///
    /// Uses moka's native TTL (`time_to_live`) so entries auto-expire.
    #[cfg(feature = "iceberg")]
    direct_metadata_locations: SyncCache<String, String>,

    /// Process-wide catalog clients (REST, AWS Glue, AWS S3 Tables) keyed by
    /// source config fingerprint. Reused across queries so a REST client's OAuth
    /// `CachedToken` and every client's HTTPS connection pool (and an AWS SDK
    /// client's resolved credential chain) survive — one token exchange per ~hour
    /// instead of one per query.
    /// The fingerprint is over the raw config JSON, so a secret changed *inline*
    /// in the config invalidates the client, but a secret referenced by env var
    /// / secret store does not (the JSON is unchanged). A TTL
    /// (`DEFAULT_REST_CLIENT_TTL_SECS`) bounds how long such a rotation stays
    /// stale before the client is rebuilt and re-authenticated.
    #[cfg(feature = "iceberg")]
    catalog_clients: SyncCache<String, Arc<dyn SendCatalogClient>>,

    /// Process-wide `loadTable` responses keyed by `(graph_source_id, ns.table)`,
    /// with a short TTL (see [`DEFAULT_REST_LOADTABLE_TTL_SECS`]) and a
    /// credential-expiry gate. Lets a burst of queries against the same table
    /// skip the ~1.3–3s catalog GET.
    #[cfg(feature = "iceberg")]
    rest_load_tables: SyncCache<String, Arc<CachedLoadTable>>,

    /// Process-wide SQL endpoint clients keyed like `catalog_clients` (id + raw
    /// config fingerprint), sharing its TTL rationale. Each client also holds
    /// the per-table schema probes, so reuse across queries skips the
    /// `LIMIT 0` round trip.
    #[cfg(feature = "sql")]
    sql_clients: SyncCache<String, Arc<TrinoClient>>,
    /// Process-wide Delta table handles keyed by location + io options. A
    /// handle owns the table's store and remembers the last version it read,
    /// which the next query extends rather than replays; the TTL bounds a
    /// stale store configuration.
    #[cfg(feature = "delta")]
    delta_tables: SyncCache<String, fluree_db_delta::DeltaTable>,
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
                compiled_mappings: SyncCache::new(mapping_cap),
                table_metadata: SyncCache::new(metadata_cap),
                scan_files: SyncCache::new(metadata_cap),
                parquet_footers: Arc::new(ParquetFooterCache::new(
                    (metadata_capacity.max(1) / 2).max(32),
                )),
                direct_metadata_locations: SyncCache::builder()
                    .max_capacity(metadata_cap)
                    .time_to_live(DIRECT_METADATA_LOCATION_TTL)
                    .build(),
                // A process serves few distinct graph sources; a small cap is
                // plenty and bounds retained clients/connection pools. A TTL lets
                // an env-var/secret-store secret rotation self-heal (see
                // `DEFAULT_REST_CLIENT_TTL_SECS`) since the config fingerprint
                // does not change when the referenced secret does.
                catalog_clients: SyncCache::builder()
                    .max_capacity(64)
                    .time_to_live(Duration::from_secs(rest_client_ttl_secs()))
                    .build(),
                rest_load_tables: SyncCache::builder()
                    .max_capacity(metadata_cap)
                    .time_to_live(Duration::from_secs(rest_loadtable_ttl_secs()))
                    .build(),
                #[cfg(feature = "sql")]
                sql_clients: SyncCache::builder()
                    .max_capacity(64)
                    .time_to_live(Duration::from_secs(rest_client_ttl_secs()))
                    .build(),
                #[cfg(feature = "delta")]
                delta_tables: SyncCache::builder()
                    .max_capacity(256)
                    .time_to_live(Duration::from_secs(rest_client_ttl_secs()))
                    .build(),
            }
        }

        #[cfg(not(feature = "iceberg"))]
        {
            let _ = metadata_cap;
            Self {
                compiled_mappings: SyncCache::new(mapping_cap),
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

    /// Get table metadata from cache, as read under the storage identity
    /// `scope` (see [`StorageScope`]: an entry is only ever served to the
    /// identity that read it).
    #[cfg(feature = "iceberg")]
    pub async fn get_metadata(
        &self,
        scope: &StorageScope,
        metadata_location: &str,
    ) -> Option<Arc<TableMetadata>> {
        self.table_metadata
            .get(scope.qualify(metadata_location).as_ref())
    }

    /// Store table metadata in cache, as read under `scope`.
    #[cfg(feature = "iceberg")]
    pub async fn put_metadata(
        &self,
        scope: &StorageScope,
        metadata_location: &str,
        metadata: Arc<TableMetadata>,
    ) {
        self.table_metadata
            .insert(scope.qualify(metadata_location).into_owned(), metadata);
    }

    /// Get cached scan file selections for a metadata location, as read under
    /// `scope`.
    #[cfg(feature = "iceberg")]
    pub(crate) async fn get_scan_files(
        &self,
        scope: &StorageScope,
        metadata_location: &str,
    ) -> Option<Arc<CachedScanFiles>> {
        self.scan_files
            .get(scope.qualify(metadata_location).as_ref())
    }

    /// Store manifest-derived scan file selections for a metadata location, as
    /// read under `scope`.
    #[cfg(feature = "iceberg")]
    pub(crate) async fn put_scan_files(
        &self,
        scope: &StorageScope,
        metadata_location: &str,
        scan_files: Arc<CachedScanFiles>,
    ) {
        self.scan_files
            .insert(scope.qualify(metadata_location).into_owned(), scan_files);
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
    /// Keyed by the storage identity `scope` too: the location is read from
    /// storage (`version-hint.text`), so another identity must read it itself.
    #[cfg(feature = "iceberg")]
    pub(crate) async fn get_direct_metadata_location(
        &self,
        scope: &StorageScope,
        table_location: &str,
    ) -> Option<String> {
        self.direct_metadata_locations
            .get(scope.qualify(table_location).as_ref())
    }

    /// Store a resolved direct-catalog metadata location, as read under `scope`.
    #[cfg(feature = "iceberg")]
    pub(crate) async fn put_direct_metadata_location(
        &self,
        scope: &StorageScope,
        table_location: &str,
        metadata_location: String,
    ) {
        self.direct_metadata_locations.insert(
            scope.qualify(table_location).into_owned(),
            metadata_location,
        );
    }

    /// Get a process-wide catalog client for a source config `fingerprint`, or
    /// `None` on miss (or when catalog caching is disabled).
    #[cfg(feature = "iceberg")]
    pub(crate) fn catalog_client(&self, fingerprint: &str) -> Option<Arc<dyn SendCatalogClient>> {
        if !super::catalog_session::cache_enabled() {
            return None;
        }
        self.catalog_clients.get(fingerprint)
    }

    /// Store a catalog client for cross-query reuse (no-op when disabled).
    #[cfg(feature = "iceberg")]
    pub(crate) fn put_catalog_client(
        &self,
        fingerprint: String,
        client: Arc<dyn SendCatalogClient>,
    ) {
        if !super::catalog_session::cache_enabled() {
            return;
        }
        self.catalog_clients.insert(fingerprint, client);
    }

    #[cfg(feature = "sql")]
    pub(crate) fn sql_client(&self, key: &str) -> Option<Arc<TrinoClient>> {
        self.sql_clients.get(key)
    }

    #[cfg(feature = "sql")]
    pub(crate) fn put_sql_client(&self, key: String, client: Arc<TrinoClient>) {
        self.sql_clients.insert(key, client);
    }

    /// The shared handle for source `graph_source_id`'s Delta table at
    /// `placement`, opened on first use. Keyed on the config as stored, so a
    /// rotated secret behind a reference does not re-key the cache; the TTL
    /// bounds how long a handle keeps the old one — and, for a table found
    /// through a catalog, how long one dropped and recreated elsewhere is
    /// still read where it was.
    ///
    /// The config's secrets are resolved before the lookup, on every call: a
    /// caller whose resolver cannot resolve them does not reach a handle
    /// another caller opened with them.
    #[cfg(feature = "delta")]
    pub(crate) async fn delta_table(
        &self,
        graph_source_id: &str,
        table_name: &str,
        placement: &fluree_db_delta::Placement,
        config: &fluree_db_delta::DeltaGsConfig,
        identity: &fluree_db_iceberg::IdentityContext,
    ) -> fluree_db_delta::Result<fluree_db_delta::DeltaTable> {
        use fluree_db_delta::{DeltaTable, Placement};
        let resolver = identity.secret_resolver.as_ref();
        let io = config.io.hydrate(resolver).await?;
        let unity = match (placement, &config.unity) {
            (Placement::Unity(_), Some(unity)) => Some(unity.hydrate(resolver).await?),
            _ => None,
        };
        let key = delta_table_key(graph_source_id, placement, config, identity);
        if let Some(table) = self.delta_tables.get(&key) {
            return Ok(table);
        }
        let table = match (placement, &unity) {
            (Placement::Path(location), _) => {
                DeltaTable::open(table_name, location, &io, identity)?
            }
            (Placement::Unity(full_name), Some(unity)) => {
                DeltaTable::open_in_unity(table_name, unity, full_name, &io, identity).await?
            }
            (Placement::Unity(full_name), None) => {
                return Err(fluree_db_delta::DeltaError::Config(format!(
                    "table '{full_name}' is placed in a catalog the source does not have"
                )))
            }
        };
        self.delta_tables.insert(key, table.clone());
        Ok(table)
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
            self.catalog_clients.invalidate_all();
            self.rest_load_tables.invalidate_all();
        }
        #[cfg(feature = "sql")]
        self.sql_clients.invalidate_all();
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
        VendedCredentials::S3(fluree_db_iceberg::credential::S3Credentials {
            access_key_id: "AKIA".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: Some("token".to_string()),
            expires_at: Some(Utc::now() + Duration::seconds(expires_in_secs)),
            endpoint: None,
            region: Some("us-east-2".to_string()),
            path_style: false,
        })
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

/// Keyed on the source and its config as stored, never on resolved secrets.
/// Two sources never share a handle, even with one config: each is its own
/// registration, and what one source's references resolve to is not
/// evidence of what another's do.
#[cfg(feature = "delta")]
fn delta_table_key(
    graph_source_id: &str,
    placement: &fluree_db_delta::Placement,
    config: &fluree_db_delta::DeltaGsConfig,
    identity: &fluree_db_iceberg::IdentityContext,
) -> String {
    use fluree_db_delta::Placement;
    let io = serde_json::to_string(&config.io).unwrap_or_default();
    // A handle holds clients built under the identity context (Unity's auth,
    // and the storage it reads with); another context must not reuse it.
    let scope = identity.cache_scope();
    match placement {
        Placement::Path(location) => {
            format!("{graph_source_id}\u{1f}{location}\u{1f}{io}\u{1f}{scope}")
        }
        Placement::Unity(full_name) => format!(
            "{graph_source_id}\u{1f}{full_name}\u{1f}{}\u{1f}{io}\u{1f}{scope}",
            serde_json::to_string(&config.unity).unwrap_or_default(),
        ),
    }
}

#[cfg(all(test, feature = "delta"))]
mod delta_key_tests {
    use super::delta_table_key;
    use fluree_db_delta::{DeltaGsConfig, Placement, UnityConfig};
    use fluree_db_iceberg::auth::AuthConfig;
    use fluree_db_iceberg::ConfigValue;
    use fluree_db_iceberg::IdentityContext;

    fn source(token: ConfigValue) -> DeltaGsConfig {
        DeltaGsConfig {
            unity: Some(UnityConfig {
                uri: "https://workspace.example.com".to_string(),
                auth: AuthConfig::Bearer { token },
                catalog: None,
                schema: None,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn a_catalog_tables_handle_belongs_to_the_credentials_that_read_it() {
        let orders = Placement::Unity("main.sales.orders".to_string());
        let items = Placement::Unity("main.sales.items".to_string());
        let alice = source(ConfigValue::literal("alice"));
        let bob = source(ConfigValue::literal("bob"));

        assert_eq!(
            delta_table_key("gs:a", &orders, &alice, &IdentityContext::default()),
            delta_table_key("gs:a", &orders, &alice, &IdentityContext::default())
        );
        assert_ne!(
            delta_table_key("gs:a", &orders, &alice, &IdentityContext::default()),
            delta_table_key("gs:a", &orders, &bob, &IdentityContext::default())
        );
        assert_ne!(
            delta_table_key("gs:a", &orders, &alice, &IdentityContext::default()),
            delta_table_key("gs:a", &items, &alice, &IdentityContext::default())
        );
        // Two sources with one config are two registrations.
        assert_ne!(
            delta_table_key("gs:a", &orders, &alice, &IdentityContext::default()),
            delta_table_key("gs:b", &orders, &alice, &IdentityContext::default())
        );

        let mut elsewhere = alice.clone();
        elsewhere.unity.as_mut().unwrap().uri = "https://other.example.com".to_string();
        assert_ne!(
            delta_table_key("gs:a", &orders, &alice, &IdentityContext::default()),
            delta_table_key("gs:a", &orders, &elsewhere, &IdentityContext::default())
        );

        // A path and a catalog name spelt alike are different tables.
        assert_ne!(
            delta_table_key(
                "gs:a",
                &Placement::Path("main.sales.orders".to_string()),
                &alice,
                &IdentityContext::default()
            ),
            delta_table_key("gs:a", &orders, &alice, &IdentityContext::default())
        );
    }

    #[test]
    fn a_secret_named_by_variable_keys_on_the_name() {
        let orders = Placement::Unity("main.sales.orders".to_string());
        let key = delta_table_key(
            "gs:a",
            &orders,
            &source(ConfigValue::from_env("UNITY_TOKEN_KEY_TEST")),
            &IdentityContext::default(),
        );
        assert!(key.contains("UNITY_TOKEN_KEY_TEST"), "{key}");
    }

    /// The cache is shared by every clone of a `Fluree`: a handle opened under
    /// one identity context (AWS subject, host-identity policy) never serves
    /// another.
    #[test]
    fn the_identity_context_is_part_of_the_key() {
        let orders = Placement::Unity("main.sales.orders".to_string());
        let config = source(ConfigValue::literal("t"));
        let closed = IdentityContext {
            policy: fluree_db_iceberg::HostIdentityPolicy::deny_all(),
            ..IdentityContext::default()
        };
        assert_ne!(
            delta_table_key("gs:a", &orders, &config, &IdentityContext::default()),
            delta_table_key("gs:a", &orders, &config, &closed)
        );
    }

    /// Resolves every reference to one secret, for `tenant` (or for no one in
    /// particular), or refuses every one.
    #[derive(Debug)]
    struct Tenant {
        tenant: Option<&'static str>,
        refuses: bool,
    }

    #[async_trait::async_trait]
    impl fluree_db_iceberg::SecretResolver for Tenant {
        async fn resolve_secret(
            &self,
            secret_ref: &str,
        ) -> Result<String, fluree_db_iceberg::SecretResolveError> {
            if self.refuses {
                return Err(fluree_db_iceberg::SecretResolveError::Denied(
                    secret_ref.to_string(),
                ));
            }
            Ok("one-secret".to_string())
        }

        fn scope_id(&self) -> Option<String> {
            self.tenant.map(str::to_string)
        }
    }

    fn resolving(tenant: Option<&'static str>, refuses: bool) -> IdentityContext {
        IdentityContext {
            secret_resolver: Some(std::sync::Arc::new(Tenant { tenant, refuses })),
            ..IdentityContext::default()
        }
    }

    /// A source on Azure whose service principal's secret is a reference.
    fn azure_source() -> (Placement, DeltaGsConfig) {
        let config = DeltaGsConfig {
            io: fluree_db_delta::DeltaIoConfig {
                azure: Some(fluree_db_delta::AzureAuth::ClientSecret {
                    tenant_id: "00000000-0000-0000-0000-000000000000".to_string(),
                    client_id: "11111111-1111-1111-1111-111111111111".to_string(),
                    client_secret: ConfigValue::SecretRef {
                        secret_ref: "azure-secret".to_string(),
                    },
                }),
                ..Default::default()
            },
            ..Default::default()
        };
        let placement =
            Placement::Path("abfss://lake@account.dfs.core.windows.net/orders".to_string());
        (placement, config)
    }

    /// Two resolvers that resolve the same stored reference for two tenants
    /// key two handles; one tenant's two resolvers share one.
    #[test]
    fn a_resolvers_scope_id_is_part_of_the_key() {
        let (orders, config) = azure_source();
        let key = |ctx: &IdentityContext| delta_table_key("gs:a", &orders, &config, ctx);
        let a = key(&resolving(Some("tenant-a"), false));
        assert_eq!(a, key(&resolving(Some("tenant-a"), false)));
        assert_ne!(a, key(&resolving(Some("tenant-b"), false)));
        assert_ne!(a, key(&resolving(None, false)));
    }

    /// A cached handle is never a way around the resolver: a caller whose
    /// resolver refuses the source's secret gets that refusal, not the handle
    /// a caller that could resolve it opened, even under one key.
    #[tokio::test]
    async fn a_caller_that_cannot_resolve_the_secrets_is_not_served_the_handle() {
        let (orders, config) = azure_source();
        let cache = super::R2rmlCache::new(4, 4);
        let (allowed, refused) = (resolving(None, false), resolving(None, true));
        assert_eq!(
            delta_table_key("gs:a", &orders, &config, &allowed),
            delta_table_key("gs:a", &orders, &config, &refused),
            "the two callers share a key, so only the resolver can tell them apart"
        );
        cache
            .delta_table("gs:a", "orders", &orders, &config, &allowed)
            .await
            .expect("an Azure store is built without a request");
        match cache
            .delta_table("gs:a", "orders", &orders, &config, &refused)
            .await
        {
            Ok(_) => panic!("served a handle opened with a secret this caller cannot resolve"),
            Err(error) => assert!(error.to_string().contains("denied"), "{error}"),
        }
        assert!(cache
            .delta_table("gs:a", "orders", &orders, &config, &allowed)
            .await
            .is_ok());
    }
}
