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
    #[cfg(feature = "iceberg")]
    parquet_footers: ParquetFooterCache,

    /// Short-lived cache for direct-catalog `version-hint.text` resolution.
    ///
    /// Uses moka's native TTL (`time_to_live`) so entries auto-expire.
    #[cfg(feature = "iceberg")]
    direct_metadata_locations: moka::sync::Cache<String, String>,
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
                parquet_footers: ParquetFooterCache::new((metadata_capacity.max(1) / 2).max(32)),
                direct_metadata_locations: moka::sync::Cache::builder()
                    .max_capacity(metadata_cap)
                    .time_to_live(DIRECT_METADATA_LOCATION_TTL)
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

    /// Get the shared Parquet footer cache.
    #[cfg(feature = "iceberg")]
    pub fn parquet_footers(&self) -> &ParquetFooterCache {
        &self.parquet_footers
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

    /// Clear all caches.
    pub async fn clear(&self) {
        self.compiled_mappings.invalidate_all();

        #[cfg(feature = "iceberg")]
        {
            self.table_metadata.invalidate_all();
            self.scan_files.invalidate_all();
            self.parquet_footers.clear().await;
            self.direct_metadata_locations.invalidate_all();
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
