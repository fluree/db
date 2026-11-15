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
    io::parquet::ParquetFooterCache, metadata::TableMetadata, DataFile, ManifestListTotals,
    StorageScope,
};
#[cfg(feature = "sql")]
use fluree_db_sql::TrinoClient;
#[cfg(feature = "iceberg")]
use std::time::Duration;

/// One snapshot's complete live data-file list, in plan order, as the scan
/// planner read it from the manifests. Every scan of the snapshot, filtered or
/// not, plans from it (`SendScanPlanner::plan_from_files`).
#[cfg(feature = "iceberg")]
#[derive(Debug, Clone)]
pub(crate) struct CachedScanFiles {
    pub data_files: Arc<Vec<DataFile>>,
    /// Whether the source snapshot carried merge-on-read delete files. Only ever
    /// `true` when the fail-closed guard was overridden at plan time; carried on
    /// the cache entry so a plan from it (in-memory or disk hit) re-refuses if
    /// the override is later turned off, instead of silently serving a
    /// delete-bearing file list (audit F-AUD-1, cache-arm follow-up).
    pub has_delete_manifests: bool,
    /// The sum of the files' record counts, computed once: an unfiltered
    /// plan's estimate, so a warm scan does not re-sum it.
    record_count: i64,
    /// The entry's weight in the in-memory scan-files cache: the list's
    /// approximate size in KiB (at least 1), computed once.
    weight_kib: u32,
}

#[cfg(feature = "iceberg")]
impl CachedScanFiles {
    pub(crate) fn new(data_files: Vec<DataFile>, has_delete_manifests: bool) -> Self {
        let (mut bytes, mut record_count) = (0usize, 0i64);
        for file in &data_files {
            bytes += file.approx_size_bytes();
            record_count += file.record_count;
        }
        let weight_kib = u32::try_from(bytes.div_ceil(1024))
            .unwrap_or(u32::MAX)
            .max(1);
        Self {
            data_files: Arc::new(data_files),
            has_delete_manifests,
            record_count,
            weight_kib,
        }
    }

    /// The list as the planner plans from it.
    pub(crate) fn kept(&self) -> fluree_db_iceberg::scan::KeptFiles<'_> {
        fluree_db_iceberg::scan::KeptFiles {
            files: &self.data_files,
            record_count: self.record_count,
            has_delete_manifests: self.has_delete_manifests,
        }
    }

    /// The sum of the files' record counts.
    pub(crate) fn record_count(&self) -> i64 {
        self.record_count
    }

    /// Approximate size of the file list in KiB (see [`DataFile::approx_size_bytes`]).
    pub(crate) fn weight_kib(&self) -> u32 {
        self.weight_kib
    }
}

/// Default in-memory budget for the scan-files cache, in MiB.
///
/// The cache holds whole file lists, and a list's size is its file count
/// times a few KiB of statistics per file, so it is bounded by bytes rather
/// than entries: 128 entries of a 100,000-file table would be gigabytes, and
/// 128 entries of 10-file tables a few megabytes. Override with
/// `FLUREE_ICEBERG_SCAN_FILES_CACHE_MB`.
#[cfg(feature = "iceberg")]
const DEFAULT_SCAN_FILES_CACHE_MB: u64 = 512;

/// The scan-files cache: whole file lists weighed by their size in KiB
/// against `budget_kib`.
#[cfg(feature = "iceberg")]
fn scan_files_cache(budget_kib: u64) -> SyncCache<String, Arc<CachedScanFiles>> {
    SyncCache::builder()
        .max_capacity(budget_kib)
        .weigher(|_key: &String, files: &Arc<CachedScanFiles>| files.weight_kib())
        .build()
}

/// The heaviest list the oversize slot keeps, in multiples of the scan-files
/// budget. A list carries statistics for every column of every file, so a wide
/// table with many files can weigh gigabytes; above this the list is not kept
/// in memory (each scan plans from the disk catalog cache or the manifests),
/// and the memory the cache holds is bounded by `(1 + OVERSIZE_SLOT_BUDGETS)`
/// budgets.
#[cfg(feature = "iceberg")]
pub(crate) const OVERSIZE_SLOT_BUDGETS: u64 = 2;

/// Where [`R2rmlCache::put_scan_files`] kept a list.
#[cfg(feature = "iceberg")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScanFilesKept {
    /// In the budgeted cache.
    Budgeted,
    /// In the oversize slot, replacing the list it held.
    Oversize,
    /// Nowhere: the budget is 0, or the list is heavier than the oversize
    /// slot takes. The slot keeps the list it held.
    NotKept,
}

#[cfg(feature = "iceberg")]
fn scan_files_cache_budget_kib() -> u64 {
    std::env::var("FLUREE_ICEBERG_SCAN_FILES_CACHE_MB")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_SCAN_FILES_CACHE_MB)
        .saturating_mul(1024)
}

#[cfg(feature = "iceberg")]
const DIRECT_METADATA_LOCATION_TTL: Duration = Duration::from_secs(2);

/// Entries in the manifest-list totals cache. An entry is a manifest-list path
/// plus three integers (a few hundred bytes), so the bound is generous: a
/// dataset wider than it would re-read some manifest lists on every `/info`.
#[cfg(feature = "iceberg")]
const MANIFEST_LIST_TOTALS_CAPACITY: u64 = 4096;

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

    /// Each snapshot's live data-file list, keyed by metadata location (plus
    /// `#snapshot=<id>` for a pinned read) qualified by the storage scope,
    /// and weighed by approximate size in KiB against a byte budget
    /// (`FLUREE_ICEBERG_SCAN_FILES_CACHE_MB`, default 512).
    #[cfg(feature = "iceberg")]
    scan_files: SyncCache<String, Arc<CachedScanFiles>>,

    /// The most recently kept file list heavier than `scan_files`' whole
    /// budget, under the same key, held beside the budgeted entries: one table
    /// that big is still served warm, and the slot holds one list, so a second
    /// such table replaces the first rather than growing the cache. A list
    /// heavier than [`OVERSIZE_SLOT_BUDGETS`] budgets is not kept at all.
    #[cfg(feature = "iceberg")]
    oversize_scan_files: std::sync::Mutex<Option<(String, Arc<CachedScanFiles>)>>,

    /// Table totals read from a snapshot's manifest list, keyed by the
    /// manifest-list location qualified by the [`StorageScope`] that read it,
    /// as `table_metadata` and `scan_files` are: the totals come from an object
    /// behind the storage gate, so a total read under one identity is never
    /// served to another. A manifest list is immutable once written and its
    /// path names one snapshot, so an entry never goes stale and needs no TTL.
    /// Serves the virtual `/info` row counts and the Schema-tier preview for
    /// tables whose snapshot summary has no `total-records`.
    #[cfg(feature = "iceberg")]
    manifest_list_totals: SyncCache<String, ManifestListTotals>,

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
    ///
    /// The scan-files cache is bounded by bytes instead (see
    /// `FLUREE_ICEBERG_SCAN_FILES_CACHE_MB`, default 512 MiB).
    pub fn new(mapping_capacity: usize, metadata_capacity: usize) -> Self {
        let mapping_cap = mapping_capacity.max(1) as u64;
        let metadata_cap = metadata_capacity.max(1) as u64;

        #[cfg(feature = "iceberg")]
        {
            Self {
                compiled_mappings: SyncCache::new(mapping_cap),
                table_metadata: SyncCache::new(metadata_cap),
                scan_files: scan_files_cache(scan_files_cache_budget_kib()),
                oversize_scan_files: std::sync::Mutex::new(None),
                manifest_list_totals: SyncCache::new(MANIFEST_LIST_TOTALS_CAPACITY),
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

    /// This cache with a scan-files budget of `budget_kib` instead of the
    /// configured one (tests: the env var is read once per process).
    #[cfg(all(test, feature = "iceberg"))]
    pub(crate) fn with_scan_files_budget_kib(mut self, budget_kib: u64) -> Self {
        self.scan_files = scan_files_cache(budget_kib);
        self
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
        let key = scope.qualify(metadata_location);
        if let Some(files) = self.scan_files.get(key.as_ref()) {
            return Some(files);
        }
        self.oversize_scan_files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .filter(|(k, _)| k.as_str() == key.as_ref())
            .map(|(_, files)| Arc::clone(files))
    }

    /// Keep a snapshot's file list in memory, as read under `scope`, and say
    /// where it went.
    ///
    /// A list heavier than the cache's whole budget goes to the one oversize
    /// slot instead, replacing what it held. moka would accept such an insert
    /// and then evict the entry at its next maintenance pass, so for a moment
    /// it would be served and then silently vanish; checking first makes the
    /// outcome deterministic. The slot keeps a large table warm (without it,
    /// every scan of that table would re-read its list from the disk catalog
    /// cache, or from the manifests) while bounding the memory beyond the
    /// budget to one list, of at most [`OVERSIZE_SLOT_BUDGETS`] budgets; a
    /// heavier list is not kept, and leaves the slot as it was. A budget of 0
    /// keeps no list at all.
    #[cfg(feature = "iceberg")]
    pub(crate) async fn put_scan_files(
        &self,
        scope: &StorageScope,
        metadata_location: &str,
        scan_files: Arc<CachedScanFiles>,
    ) -> ScanFilesKept {
        let budget_kib = self.scan_files.policy().max_capacity().unwrap_or(0);
        if budget_kib == 0 {
            // `FLUREE_ICEBERG_SCAN_FILES_CACHE_MB=0`: keep no list in memory.
            return ScanFilesKept::NotKept;
        }
        let weight_kib = u64::from(scan_files.weight_kib());
        if weight_kib > budget_kib.saturating_mul(OVERSIZE_SLOT_BUDGETS) {
            tracing::debug!(
                metadata_location,
                files = scan_files.data_files.len(),
                weight_kib,
                budget_kib,
                "Iceberg scan-files list is heavier than the oversize slot takes; not kept                  in memory"
            );
            return ScanFilesKept::NotKept;
        }
        if weight_kib > budget_kib {
            static ANNOUNCED: std::sync::atomic::AtomicBool =
                std::sync::atomic::AtomicBool::new(false);
            if !ANNOUNCED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                tracing::info!(
                    metadata_location,
                    files = scan_files.data_files.len(),
                    weight_kib = scan_files.weight_kib(),
                    budget_kib,
                    "Iceberg scan-files list exceeds the in-memory cache budget \
                     (FLUREE_ICEBERG_SCAN_FILES_CACHE_MB); keeping the latest such list \
                     in a single oversize slot"
                );
            } else {
                tracing::debug!(
                    metadata_location,
                    files = scan_files.data_files.len(),
                    weight_kib = scan_files.weight_kib(),
                    budget_kib,
                    "Iceberg scan-files list kept in the oversize slot"
                );
            }
            *self
                .oversize_scan_files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some((scope.qualify(metadata_location).into_owned(), scan_files));
            return ScanFilesKept::Oversize;
        }
        self.scan_files
            .insert(scope.qualify(metadata_location).into_owned(), scan_files);
        ScanFilesKept::Budgeted
    }

    /// Get the totals read from the manifest list at `manifest_list_location`,
    /// as read under `scope`.
    #[cfg(feature = "iceberg")]
    pub(crate) fn get_manifest_list_totals(
        &self,
        scope: &StorageScope,
        manifest_list_location: &str,
    ) -> Option<ManifestListTotals> {
        self.manifest_list_totals
            .get(scope.qualify(manifest_list_location).as_ref())
    }

    /// Store the totals read from the manifest list at `manifest_list_location`,
    /// as read under `scope`.
    #[cfg(feature = "iceberg")]
    pub(crate) fn put_manifest_list_totals(
        &self,
        scope: &StorageScope,
        manifest_list_location: &str,
        totals: ManifestListTotals,
    ) {
        self.manifest_list_totals
            .insert(scope.qualify(manifest_list_location).into_owned(), totals);
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
            *self
                .oversize_scan_files
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
            self.manifest_list_totals.invalidate_all();
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

    /// `n` files carrying bounds and counts for 20 columns (~3.5 KiB each).
    fn file_list(n: usize) -> Vec<DataFile> {
        let counts: std::collections::HashMap<i32, i64> = (1..=20).map(|id| (id, 100)).collect();
        let bounds: std::collections::HashMap<i32, Vec<u8>> =
            (1..=20).map(|id| (id, vec![0u8; 8])).collect();
        (0..n)
            .map(|i| DataFile {
                file_path: format!("s3://b/t/data/{i:05}.parquet"),
                file_format: fluree_db_iceberg::manifest::FileFormat::Parquet,
                record_count: 100,
                file_size_in_bytes: 1024,
                partition: fluree_db_iceberg::manifest::PartitionData::default(),
                column_sizes: None,
                value_counts: Some(counts.clone()),
                null_value_counts: Some(counts.clone()),
                nan_value_counts: None,
                lower_bounds: Some(bounds.clone()),
                upper_bounds: Some(bounds.clone()),
                split_offsets: None,
                sort_order_id: None,
            })
            .collect()
    }

    /// The cache is weighed by the lists' size: two 10-file lists (~35 KiB
    /// each) fit a 100 KiB budget. A 40-file list (~140 KiB) is heavier than
    /// the whole budget: it goes to the one oversize slot, is served warm from
    /// there, and leaves the budgeted entries in place; a second oversize list
    /// replaces it.
    #[tokio::test]
    async fn scan_files_are_weighed_by_size_and_one_oversized_list_is_kept_aside() {
        let cache = R2rmlCache::with_defaults().with_scan_files_budget_kib(100);
        let scope = StorageScope::shared();
        let list = |n| Arc::new(CachedScanFiles::new(file_list(n), false));
        let (a, b, big, bigger) = (list(10), list(10), list(40), list(50));
        assert!((30..=40).contains(&a.weight_kib()), "{}", a.weight_kib());
        assert!(big.weight_kib() > 100, "{}", big.weight_kib());

        assert_eq!(
            cache.put_scan_files(&scope, "s3://b/a.json", a).await,
            ScanFilesKept::Budgeted
        );
        assert_eq!(
            cache.put_scan_files(&scope, "s3://b/b.json", b).await,
            ScanFilesKept::Budgeted
        );
        assert_eq!(
            cache.put_scan_files(&scope, "s3://b/big.json", big).await,
            ScanFilesKept::Oversize,
            "a list heavier than the whole budget goes to the oversize slot"
        );
        cache.scan_files.run_pending_tasks();
        let warm = cache
            .get_scan_files(&scope, "s3://b/big.json")
            .await
            .expect("the oversize list is served warm");
        assert_eq!(warm.data_files.len(), 40);
        assert!(cache
            .get_scan_files(&scope, "s3://b/a.json")
            .await
            .is_some());
        assert!(cache
            .get_scan_files(&scope, "s3://b/b.json")
            .await
            .is_some());
        assert!(cache.scan_files.weighted_size() <= 100);
        assert!(
            cache
                .get_scan_files(&StorageScope::of(&[("tenant", "other")]), "s3://b/big.json")
                .await
                .is_none(),
            "the slot is keyed under its scope like the budgeted entries"
        );

        assert_eq!(
            cache
                .put_scan_files(&scope, "s3://b/bigger.json", bigger)
                .await,
            ScanFilesKept::Oversize
        );
        assert!(
            cache
                .get_scan_files(&scope, "s3://b/big.json")
                .await
                .is_none(),
            "a second oversize list replaces the first"
        );
        let warm = cache
            .get_scan_files(&scope, "s3://b/bigger.json")
            .await
            .expect("the latest oversize list is served warm");
        assert_eq!(warm.data_files.len(), 50);

        cache.clear().await;
        assert!(cache
            .get_scan_files(&scope, "s3://b/bigger.json")
            .await
            .is_none());
    }

    /// The oversize slot takes a list of at most `OVERSIZE_SLOT_BUDGETS`
    /// budgets. A heavier one is not kept in memory, and the slot keeps the
    /// list it held: one table too large to hold does not evict a table that
    /// fits.
    #[tokio::test]
    async fn the_oversize_slot_keeps_no_list_heavier_than_its_cap() {
        let budget_kib = 100;
        let cache = R2rmlCache::with_defaults().with_scan_files_budget_kib(budget_kib);
        let scope = StorageScope::shared();
        let list = |n| Arc::new(CachedScanFiles::new(file_list(n), false));
        let cap_kib = budget_kib * OVERSIZE_SLOT_BUDGETS;
        let (fits, too_big) = (list(50), list(100));
        let weight = |l: &Arc<CachedScanFiles>| u64::from(l.weight_kib());
        assert!(
            weight(&fits) > budget_kib && weight(&fits) <= cap_kib,
            "{}",
            fits.weight_kib()
        );
        assert!(weight(&too_big) > cap_kib, "{}", too_big.weight_kib());

        assert_eq!(
            cache.put_scan_files(&scope, "s3://b/fits.json", fits).await,
            ScanFilesKept::Oversize
        );
        assert_eq!(
            cache
                .put_scan_files(&scope, "s3://b/too-big.json", too_big)
                .await,
            ScanFilesKept::NotKept,
            "a list heavier than the slot's cap is not kept"
        );
        assert!(cache
            .get_scan_files(&scope, "s3://b/too-big.json")
            .await
            .is_none());
        let warm = cache
            .get_scan_files(&scope, "s3://b/fits.json")
            .await
            .expect("the slot keeps the list it held");
        assert_eq!(warm.data_files.len(), 50);
        cache.scan_files.run_pending_tasks();
        assert_eq!(cache.scan_files.weighted_size(), 0, "nor is it budgeted");
    }

    /// A budget of 0 keeps no list in memory, the oversize slot included.
    #[tokio::test]
    async fn a_zero_scan_files_budget_keeps_no_list() {
        let cache = R2rmlCache::with_defaults().with_scan_files_budget_kib(0);
        let scope = StorageScope::shared();
        let files = Arc::new(CachedScanFiles::new(file_list(10), false));
        assert_eq!(
            cache.put_scan_files(&scope, "s3://b/a.json", files).await,
            ScanFilesKept::NotKept
        );
        assert!(cache
            .get_scan_files(&scope, "s3://b/a.json")
            .await
            .is_none());
    }

    /// Why `put_scan_files` checks the weight first: moka accepts an entry
    /// heavier than its whole capacity and drops it at the next maintenance
    /// pass, without a trace. (Pins the dependency's behavior: if moka ever
    /// keeps such an entry instead, the cache would overrun its budget.)
    #[test]
    fn moka_silently_drops_an_entry_heavier_than_its_capacity() {
        let cache: moka::sync::Cache<&str, u32> = moka::sync::Cache::builder()
            .max_capacity(100)
            .weigher(|_k: &&str, v: &u32| *v)
            .build();
        cache.insert("fits", 10);
        cache.insert("too-heavy", 101);
        cache.run_pending_tasks();
        assert_eq!(cache.get(&"too-heavy"), None);
        assert_eq!(cache.get(&"fits"), Some(10));
    }

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
