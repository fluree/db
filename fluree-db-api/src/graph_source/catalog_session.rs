//! Query-scoped Iceberg catalog session.
//!
//! A [`crate::graph_source::FlureeR2rmlProvider`] is constructed once per query,
//! so a session held on it is naturally query-scoped. It eliminates the per-scan
//! REST storm that dominates Iceberg/R2RML query latency:
//!
//! - one [`RestCatalogClient`] (carrying its OAuth `CachedToken`) is reused
//!   across every scan of a source, instead of a fresh provider + token exchange
//!   per scan;
//! - one `loadTable` response (metadata location + vended credentials) is cached
//!   per `(source, table)` for the query, instead of a `GET /tables/<t>` REST
//!   round-trip per scan.
//!
//! Per-query scope is also a correctness improvement: every scan in the query
//! reads one pinned Iceberg snapshot. Independent per-scan loads could otherwise
//! observe different snapshots if the table commits mid-query.
//!
//! Cached vended credentials are never served at/after their (30s-buffered)
//! expiry — a late scan transparently reloads. The cache can be disabled with
//! `FLUREE_ICEBERG_LOADTABLE_CACHE=0`, restoring per-scan loads.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use fluree_db_iceberg::catalog::LoadTableResponse;
use fluree_db_iceberg::credential::VendedCredentials;
use fluree_db_iceberg::io::IcebergStorageBackend;
use fluree_db_query::r2rml::TableWatermark;

/// Master switch for all Iceberg catalog caching. Read once from
/// `FLUREE_ICEBERG_LOADTABLE_CACHE` (only `0`/`false`/`off` disable it). When
/// off, every scan builds a fresh REST client and reloads the table (per-scan
/// OAuth + `loadTable` restored).
pub(crate) fn cache_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("FLUREE_ICEBERG_LOADTABLE_CACHE") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off"
        ),
        Err(_) => true,
    })
}

/// The fields a later scan needs to rebuild a [`LoadTableResponse`] without
/// another REST round-trip. Shared by the per-query snapshot pin (this module)
/// and the process-wide cross-query `loadTable` cache (`R2rmlCache`).
#[derive(Clone)]
pub(crate) struct CachedLoadTable {
    pub(crate) metadata_location: String,
    pub(crate) credentials: Option<VendedCredentials>,
}

impl CachedLoadTable {
    pub(crate) fn from_response(resp: &LoadTableResponse) -> Self {
        Self {
            metadata_location: resp.metadata_location.clone(),
            credentials: resp.credentials.clone(),
        }
    }

    /// Rebuild a `LoadTableResponse` (the `config` map is debug-only and dropped;
    /// the inline `metadata` is a preview-only convenience the scan path never
    /// reads, so it is likewise not retained across the cache).
    pub(crate) fn to_response(&self) -> LoadTableResponse {
        LoadTableResponse {
            metadata_location: self.metadata_location.clone(),
            credentials: self.credentials.clone(),
            config: HashMap::default(),
            metadata: None,
        }
    }

    /// True when vended credentials are present and at/after their (30s-buffered)
    /// expiry, so a later scan must reload rather than hand out stale creds.
    pub(crate) fn creds_expired(&self) -> bool {
        self.credentials
            .as_ref()
            .is_some_and(VendedCredentials::is_expired)
    }
}

/// Per-query catalog state: the `loadTable` snapshot pin. `FlureeR2rmlProvider`
/// is built once per query, so this map is naturally query-scoped — every scan
/// in one query reads one pinned Iceberg snapshot. Process-wide client reuse
/// (the OAuth token) and the cross-query `loadTable` cache live in `R2rmlCache`.
#[derive(Default)]
pub(crate) struct IcebergCatalogSession {
    /// Pinned `loadTable` responses keyed by `(graph_source_id, namespace.table)`.
    load_tables: Mutex<HashMap<String, CachedLoadTable>>,
    /// S3 storage clients built from each table's vended credentials, keyed the
    /// same as `load_tables`. The session pins the loadTable RESPONSE above; this
    /// caches the AWS SDK client built FROM those credentials so repeated scans of
    /// one table in a query — a correlated join re-scanning a dim, or the slice-1
    /// prefetch-then-scan — reuse one client instead of rebuilding it
    /// (`aws_config` load + S3 client + HTTP client) per scan. Invalidated by
    /// `store_load_table`: any fresh loadTable (including a creds-expiry reload)
    /// drops the entry, so a client built from stale credentials is never served.
    storages: Mutex<HashMap<String, Arc<IcebergStorageBackend>>>,
    /// Location-only snapshot pins from the loadTable-metadata cache's pointer
    /// rung (`21-loadtable-metadata-cache.md`): that path resolves a snapshot's
    /// `metadata_location` from disk WITHOUT a loadTable GET, so it has NO vended
    /// credentials to store in `load_tables`. Kept in a SEPARATE map so
    /// `cached_load_table` never hands out a credential-less entry (which would
    /// build ambient-cred storage). `pinned_metadata_location` consults it, so a
    /// later touch of the same table this query stays on ONE snapshot even if the
    /// disk pointer's TTL expires between touches — the eager path then reloads
    /// only the credentials and re-pins to this location.
    location_pins: Mutex<HashMap<String, String>>,
    /// Per-build snapshot watermarks (DEC-003 twin builder). Keyed by
    /// `snapshot_key(graph_source_id, table_name)` — the SAME logical table name
    /// the materialize driver passes to `scan_table`, so `pinned_tables` can
    /// report `{table → snapshot}` for one source. Captured at each table's first
    /// touch (first-writer-wins, mirroring `location_pins`), so a table's
    /// watermark is the snapshot the build first read and never moves within the
    /// build. Recorded unconditionally (independent of the loadTable cache toggle):
    /// even with caching off, the build must record the snapshot each scan read.
    snapshots: Mutex<HashMap<String, TableWatermark>>,
    /// MAJOR-2 (#1529 review): tables that yielded a SECOND DISTINCT
    /// `metadata_location` during this build, keyed by `snapshot_key`, with
    /// `(first_location, conflicting_location)`. `snapshots` is first-writer-wins,
    /// so a mid-build snapshot move (a source commit while a Direct source's 2s
    /// metadata cache expired, or a REST source with pinning off) would otherwise be
    /// silently pinned to the first location while later scans read the second — a
    /// twin whose stamped watermark does not describe its contents. Recorded here so
    /// the build can fail loud instead.
    snapshot_conflicts: Mutex<HashMap<String, (String, String)>>,
    /// Warehouse-root child-directory listings, keyed by warehouse root, so a
    /// catalog-less multi-table Direct source LISTs each root exactly ONCE per
    /// build (not once per table). Always cached (independent of the loadTable
    /// cache toggle) — the listing is stable for the build.
    warehouse_listings: Mutex<HashMap<String, Arc<Vec<String>>>>,
    /// Graph sources this session has scanned that are SQL-backed. A SQL source
    /// has no snapshot to pin, so the loadTable-cache precondition in
    /// `verify_build_snapshot_integrity` does not apply to it.
    sql_sources: Mutex<std::collections::HashSet<String>>,
}

impl IcebergCatalogSession {
    pub(crate) fn mark_sql_source(&self, graph_source_id: &str) {
        self.sql_sources
            .lock()
            .unwrap()
            .insert(graph_source_id.to_string());
    }

    pub(crate) fn is_sql_source(&self, graph_source_id: &str) -> bool {
        self.sql_sources.lock().unwrap().contains(graph_source_id)
    }

    /// Cache key for a `loadTable` response: source id + fully-qualified table.
    pub(crate) fn load_table_key(graph_source_id: &str, namespace: &str, table: &str) -> String {
        format!("{graph_source_id}\u{1f}{namespace}.{table}")
    }

    /// Return a cached [`LoadTableResponse`] for `key` if present and its vended
    /// credentials have not expired; otherwise `None` (the caller reloads).
    pub(crate) fn cached_load_table(&self, key: &str) -> Option<LoadTableResponse> {
        if !cache_enabled() {
            return None;
        }
        let lts = self.load_tables.lock().unwrap();
        let hit = lts.get(key)?;
        if hit.creds_expired() {
            return None;
        }
        Some(hit.to_response())
    }

    /// Whether `key` is pinned this query with unexpired credentials — the cheap
    /// (no-clone) predicate `prefetch_tables` uses to skip re-warming a table that
    /// is already resolved. A pinned-but-creds-expired table returns `false` (a
    /// warm would usefully refresh it).
    pub(crate) fn is_pinned(&self, key: &str) -> bool {
        if !cache_enabled() {
            return false;
        }
        self.load_tables
            .lock()
            .unwrap()
            .get(key)
            .is_some_and(|e| !e.creds_expired())
    }

    /// The `metadata_location` pinned for `key` on its first load this query,
    /// regardless of credential freshness. A creds-expiry reload uses this to
    /// keep the query on one Iceberg snapshot even if the table commits mid-query
    /// (the reload refreshes only the credentials). `None` if never loaded.
    pub(crate) fn pinned_metadata_location(&self, key: &str) -> Option<String> {
        if !cache_enabled() {
            return None;
        }
        // The credential-bearing pin (from an actual loadTable) is authoritative;
        // fall back to a location-only pin from the pointer rung. They can only
        // AGREE (the eager path re-pins to the location-pin's value), so the order
        // is a preference, not a correctness choice.
        if let Some(loc) = self
            .load_tables
            .lock()
            .unwrap()
            .get(key)
            .map(|e| e.metadata_location.clone())
        {
            return Some(loc);
        }
        self.location_pins.lock().unwrap().get(key).cloned()
    }

    /// Pin ONLY the `metadata_location` for `key` (no credentials) — for the
    /// loadTable-metadata cache's pointer rung, which serves a snapshot's metadata
    /// from disk WITHOUT a loadTable GET. First-writer-wins (like the loadTable
    /// pin): a later touch of the same table this query resolves the SAME snapshot
    /// even if the disk pointer's TTL expires between touches. Deliberately does
    /// NOT touch `load_tables`, so `cached_load_table` keeps forcing a fresh-creds
    /// GET rather than serving a credential-less entry (ambient-cred storage). The
    /// snapshot pin machinery's whole reason for existing (`cache_enabled` /
    /// snapshot consistency, this module's doc). No-op when the cache is disabled.
    pub(crate) fn pin_metadata_location(&self, key: String, metadata_location: String) {
        if !cache_enabled() {
            return;
        }
        self.location_pins
            .lock()
            .unwrap()
            .entry(key)
            .or_insert(metadata_location);
    }

    /// Cache a `loadTable` response for reuse by later scans of the same
    /// `(source, table)` in this query. The `metadata_location` is pinned on the
    /// first store and never changes; a later store (a creds refresh) updates
    /// only the credentials, so the query stays on one snapshot. No-op when the
    /// cache is disabled.
    pub(crate) fn store_load_table(&self, key: String, resp: &LoadTableResponse) {
        if !cache_enabled() {
            return;
        }
        // Any fresh loadTable invalidates the cached S3 client for this table: a
        // creds-expiry reload changes the vended credentials, so a client built
        // from the previous (now-stale) credentials must be rebuilt (it would
        // otherwise 403). The next `cached_storage` miss triggers the rebuild.
        // On a first load there is nothing to drop; this is a no-op then.
        self.storages.lock().unwrap().remove(&key);
        let mut lts = self.load_tables.lock().unwrap();
        match lts.get_mut(&key) {
            Some(existing) => existing.credentials = resp.credentials.clone(),
            None => {
                lts.insert(key, CachedLoadTable::from_response(resp));
            }
        }
    }

    /// The S3 storage client cached for `key`, if one was built and not since
    /// invalidated by a creds refresh. A hit lets a later scan (or the slice-1
    /// prefetch→scan) skip rebuilding the AWS SDK client. `None` when the cache is
    /// disabled or after a fresh loadTable dropped the entry.
    pub(crate) fn cached_storage(&self, key: &str) -> Option<Arc<IcebergStorageBackend>> {
        if !cache_enabled() {
            return None;
        }
        self.storages.lock().unwrap().get(key).cloned()
    }

    /// Cache the S3 storage client built from `key`'s current pinned credentials.
    /// Paired with `cached_storage`; `store_load_table` invalidates on a creds
    /// refresh, so an entry here always corresponds to the currently pinned creds.
    /// No-op when the cache is disabled.
    pub(crate) fn store_storage(&self, key: String, storage: Arc<IcebergStorageBackend>) {
        if !cache_enabled() {
            return;
        }
        self.storages.lock().unwrap().insert(key, storage);
    }

    /// Cache key for a build snapshot watermark: source id + logical table name.
    /// Distinct from [`Self::load_table_key`] (which parses `namespace.table`):
    /// this keys by the exact `table_name` string the materialize driver passes to
    /// `scan_table` (`TriplesMap::table_name()`), so [`Self::pinned_tables`] and the
    /// driver's `build_watermark` agree on the table key without re-parsing.
    pub(crate) fn snapshot_key(graph_source_id: &str, table_name: &str) -> String {
        format!("{graph_source_id}\u{1f}{table_name}")
    }

    /// Record the pinned snapshot of a table on its first touch this build
    /// (first-writer-wins). Recorded unconditionally — the build must know which
    /// snapshot each scan read even when the loadTable cache is disabled. A later
    /// scan of the same table keeps the first-recorded watermark, matching the
    /// `metadata_location` pin.
    pub(crate) fn record_snapshot(&self, key: String, watermark: TableWatermark) {
        let mut snaps = self.snapshots.lock().unwrap();
        match snaps.entry(key.clone()) {
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(watermark);
            }
            std::collections::hash_map::Entry::Occupied(e) => {
                // MAJOR-2: first-writer-wins keeps the first snapshot, but if a later
                // touch read a DIFFERENT metadata_location the source moved mid-build
                // — record it so the build fails loud rather than baking a twin whose
                // stamp lies. First conflict per table wins (stable).
                if e.get().metadata_location != watermark.metadata_location {
                    self.snapshot_conflicts
                        .lock()
                        .unwrap()
                        .entry(key)
                        .or_insert_with(|| {
                            (
                                e.get().metadata_location.clone(),
                                watermark.metadata_location.clone(),
                            )
                        });
                }
            }
        }
    }

    /// MAJOR-2: tables that yielded a second distinct `metadata_location` this build
    /// for `graph_source_id`, as `(table_name, first_location, conflicting_location)`.
    /// Empty on a clean build (every table stayed on one snapshot).
    pub(crate) fn observed_snapshot_conflicts(
        &self,
        graph_source_id: &str,
    ) -> Vec<(String, String, String)> {
        let prefix = format!("{graph_source_id}\u{1f}");
        self.snapshot_conflicts
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(k, (first, second))| {
                k.strip_prefix(&prefix)
                    .map(|t| (t.to_string(), first.clone(), second.clone()))
            })
            .collect()
    }

    /// The cached child-directory listing for a warehouse `root`, if this build
    /// already LISTed it.
    pub(crate) fn cached_warehouse_listing(&self, root: &str) -> Option<Arc<Vec<String>>> {
        self.warehouse_listings.lock().unwrap().get(root).cloned()
    }

    /// Cache a warehouse root's child-directory listing (first-writer-wins, so a
    /// concurrent double-LIST still keeps one listing), returning the effective
    /// entry.
    pub(crate) fn store_warehouse_listing(
        &self,
        root: String,
        dirs: Vec<String>,
    ) -> Arc<Vec<String>> {
        Arc::clone(
            self.warehouse_listings
                .lock()
                .unwrap()
                .entry(root)
                .or_insert_with(|| Arc::new(dirs)),
        )
    }

    /// All snapshot watermarks captured this build for `graph_source_id`, as
    /// `{table_name → watermark}` — the twin's watermark vector. Feeds
    /// `FlureeR2rmlProvider::build_watermark`.
    pub(crate) fn pinned_tables(&self, graph_source_id: &str) -> HashMap<String, TableWatermark> {
        let prefix = format!("{graph_source_id}\u{1f}");
        self.snapshots
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(k, v)| k.strip_prefix(&prefix).map(|t| (t.to_string(), v.clone())))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use fluree_db_iceberg::io::S3IcebergStorage;

    fn creds(expires_in_secs: Option<i64>) -> VendedCredentials {
        VendedCredentials {
            access_key_id: "AKIA".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: Some("token".to_string()),
            expires_at: expires_in_secs.map(|s| Utc::now() + Duration::seconds(s)),
            endpoint: None,
            region: Some("us-east-2".to_string()),
            path_style: false,
        }
    }

    fn resp(loc: &str, creds: Option<VendedCredentials>) -> LoadTableResponse {
        LoadTableResponse {
            metadata_location: loc.to_string(),
            config: HashMap::default(),
            credentials: creds,
            metadata: None,
        }
    }

    #[test]
    fn cache_hit_returns_stored_response() {
        let s = IcebergCatalogSession::default();
        let key = IcebergCatalogSession::load_table_key("gs:main", "DW", "DIM_STORE");
        assert!(s.cached_load_table(&key).is_none(), "empty cache misses");
        s.store_load_table(
            key.clone(),
            &resp("s3://meta/1.json", Some(creds(Some(3600)))),
        );
        let hit = s.cached_load_table(&key).expect("hit after store");
        assert_eq!(hit.metadata_location, "s3://meta/1.json");
        assert!(hit.credentials.is_some());
    }

    #[test]
    fn record_snapshot_flags_second_distinct_metadata_location() {
        // MAJOR-2: first-writer-wins keeps snap-A, but a later scan reading snap-B
        // means the source moved mid-build — a conflict the build must fail on.
        let wm = |loc: &str| TableWatermark {
            metadata_location: loc.to_string(),
            snapshot_id: None,
            sequence_number: None,
        };
        let s = IcebergCatalogSession::default();
        let key = IcebergCatalogSession::snapshot_key("gs:main", "DW.FACT_ORDER");
        s.record_snapshot(key.clone(), wm("s3://meta/snap-A.json"));
        // Re-reading the SAME snapshot is not a conflict.
        s.record_snapshot(key.clone(), wm("s3://meta/snap-A.json"));
        assert!(
            s.observed_snapshot_conflicts("gs:main").is_empty(),
            "an identical re-read must not flag a conflict"
        );
        // A DISTINCT metadata_location flags the table.
        s.record_snapshot(key, wm("s3://meta/snap-B.json"));
        let conflicts = s.observed_snapshot_conflicts("gs:main");
        assert_eq!(conflicts.len(), 1, "the moved table must be flagged");
        assert_eq!(
            conflicts[0],
            (
                "DW.FACT_ORDER".to_string(),
                "s3://meta/snap-A.json".to_string(),
                "s3://meta/snap-B.json".to_string()
            )
        );
        // A different source's conflicts are scoped out.
        assert!(s.observed_snapshot_conflicts("other:main").is_empty());
    }

    #[test]
    fn expired_creds_entry_is_a_miss() {
        let s = IcebergCatalogSession::default();
        let key = IcebergCatalogSession::load_table_key("gs:main", "DW", "DIM_STORE");
        // Already inside the 30s refresh buffer → treated as expired.
        s.store_load_table(
            key.clone(),
            &resp("s3://meta/1.json", Some(creds(Some(10)))),
        );
        assert!(
            s.cached_load_table(&key).is_none(),
            "about-to-expire vended creds must not be served"
        );
    }

    #[test]
    fn no_creds_entry_never_expires() {
        let s = IcebergCatalogSession::default();
        let key = IcebergCatalogSession::load_table_key("gs:main", "DW", "DIM_STORE");
        s.store_load_table(key.clone(), &resp("s3://meta/1.json", None));
        assert!(
            s.cached_load_table(&key).is_some(),
            "ambient-credential entries have no expiry"
        );
    }

    #[test]
    fn refresh_keeps_pinned_metadata_location() {
        // First load pins the snapshot. A later store (as happens after a
        // creds-expiry reload that observed a NEWER metadata_location because the
        // table committed mid-query) must NOT move the pin — only refresh creds.
        let s = IcebergCatalogSession::default();
        let key = IcebergCatalogSession::load_table_key("gs:main", "DW", "DIM_STORE");
        s.store_load_table(
            key.clone(),
            &resp("s3://snap-A.json", Some(creds(Some(10)))),
        );
        assert_eq!(
            s.pinned_metadata_location(&key).as_deref(),
            Some("s3://snap-A.json")
        );
        // Simulate the reload landing on a newer snapshot with fresh creds.
        s.store_load_table(
            key.clone(),
            &resp("s3://snap-B.json", Some(creds(Some(3600)))),
        );
        assert_eq!(
            s.pinned_metadata_location(&key).as_deref(),
            Some("s3://snap-A.json"),
            "snapshot must stay pinned across a credential refresh"
        );
        let hit = s.cached_load_table(&key).expect("fresh creds now valid");
        assert_eq!(
            hit.metadata_location, "s3://snap-A.json",
            "later scans read the pinned snapshot, not the reloaded one"
        );
    }

    #[test]
    fn location_pin_keeps_query_on_one_snapshot_across_pointer_ttl_expiry() {
        // The loadTable-metadata cache's pointer rung serves a snapshot's metadata
        // from disk (no loadTable GET) and PINS its location. A LATER touch of the
        // same table this query — even after the disk pointer's TTL expired and
        // would resolve a NEWER snapshot — must read the SAME snapshot. This is the
        // exact two-snapshots-in-one-query bug the pin machinery prevents; before
        // the pin was registered on the never-forced metadata path, the pointer rung
        // could violate it.
        let s = IcebergCatalogSession::default();
        let key = IcebergCatalogSession::load_table_key("gs:main", "DW", "FACT_INVENTORY");
        assert_eq!(s.pinned_metadata_location(&key), None);

        // Touch 1: the rung serves + pins snap-A (credential-less — metadata came
        // from the disk caches, no vended creds).
        s.pin_metadata_location(key.clone(), "s3://snap-A.json".to_string());
        assert_eq!(
            s.pinned_metadata_location(&key).as_deref(),
            Some("s3://snap-A.json")
        );
        // A location-only pin must NOT masquerade as a cached loadTable response
        // (that would build ambient-cred storage); the eager path still reloads
        // credentials.
        assert!(
            s.cached_load_table(&key).is_none(),
            "a credential-less location pin never serves as a cached loadTable"
        );

        // Touch 2's disk pointer has expired and advanced to snap-B; first-writer-
        // wins keeps the query on snap-A.
        s.pin_metadata_location(key.clone(), "s3://snap-B.json".to_string());
        assert_eq!(
            s.pinned_metadata_location(&key).as_deref(),
            Some("s3://snap-A.json"),
            "the query stays on the first-pinned snapshot even if the pointer advances"
        );

        // If touch 2 falls to the EAGER path, resolve_rest_load_and_storage reloads
        // credentials and overrides its GET-observed snap-B back to the pinned
        // snap-A (then stores it). Model that store: the query still reads snap-A,
        // now with fresh creds.
        s.store_load_table(
            key.clone(),
            &resp("s3://snap-A.json", Some(creds(Some(3600)))),
        );
        let hit = s.cached_load_table(&key).expect("fresh creds now valid");
        assert_eq!(
            hit.metadata_location, "s3://snap-A.json",
            "later scans read the pinned snapshot with fresh creds"
        );
    }

    #[tokio::test]
    async fn store_load_table_invalidates_cached_storage_on_creds_refresh() {
        // The session caches the S3 client built from a table's vended creds. A
        // fresh loadTable (a creds-expiry reload) must DROP that client so a
        // client built from stale credentials is never reused — otherwise a later
        // scan would 403. `from_default_chain(Some(region), ..)` builds an SDK
        // client offline (region set, ambient creds resolved lazily, no request),
        // which is all this bookkeeping test needs.
        let s = IcebergCatalogSession::default();
        let key = IcebergCatalogSession::load_table_key("gs:main", "DW", "DIM_STORE");
        s.store_load_table(
            key.clone(),
            &resp("s3://snap-A.json", Some(creds(Some(3600)))),
        );
        let storage = Arc::new(IcebergStorageBackend::S3(
            S3IcebergStorage::from_default_chain(Some("us-east-2"), None, false)
                .await
                .expect("offline SDK client construction"),
        ));
        s.store_storage(key.clone(), Arc::clone(&storage));
        assert!(
            s.cached_storage(&key).is_some(),
            "storage client is cached after store"
        );

        // A fresh loadTable with rotated credentials must invalidate it.
        s.store_load_table(
            key.clone(),
            &resp("s3://snap-A.json", Some(creds(Some(3600)))),
        );
        assert!(
            s.cached_storage(&key).is_none(),
            "cached S3 client must be dropped on a credential refresh, forcing a rebuild"
        );
    }

    #[tokio::test]
    async fn cached_storage_persists_without_a_fresh_load_table() {
        // fluree/db#1498: Direct mode caches its S3 client here but NEVER calls
        // `store_load_table` (it has no vended credentials to rotate), so the only
        // thing that would invalidate the client never happens — the client stays
        // cached for the whole query and every repeated scan of the table reuses
        // it. This is the session-layer invariant the direct-branch reuse relies
        // on; the r2rml helper test drives the same contract end-to-end.
        let s = IcebergCatalogSession::default();
        let key = IcebergCatalogSession::load_table_key("gs:main", "DW", "DIM_STORE");
        let storage = Arc::new(IcebergStorageBackend::S3(
            S3IcebergStorage::from_default_chain(Some("us-east-2"), None, false)
                .await
                .expect("offline SDK client construction"),
        ));
        s.store_storage(key.clone(), Arc::clone(&storage));
        // No `store_load_table` in between (Direct mode's flow) — the client must
        // still be served, and it must be the very same Arc.
        let hit = s
            .cached_storage(&key)
            .expect("storage stays cached with no reload");
        assert!(
            Arc::ptr_eq(&hit, &storage),
            "the cached Direct-mode client must be the same Arc across resolutions"
        );
    }

    #[test]
    fn snapshot_capture_is_first_writer_wins_and_scoped_by_source() {
        let s = IcebergCatalogSession::default();
        let wm = |loc: &str, id: i64, seq: i64| TableWatermark {
            metadata_location: loc.to_string(),
            snapshot_id: Some(id),
            sequence_number: Some(seq),
        };
        let k_store = IcebergCatalogSession::snapshot_key("gs:main", "DW.DIM_STORE");
        let k_geo = IcebergCatalogSession::snapshot_key("gs:main", "DW.DIM_GEOGRAPHY");
        let k_other = IcebergCatalogSession::snapshot_key("other:main", "DW.DIM_STORE");

        s.record_snapshot(k_store.clone(), wm("s3://snap-A.json", 1, 10));
        // A later touch must NOT move the pinned snapshot (first-writer-wins).
        s.record_snapshot(k_store.clone(), wm("s3://snap-B.json", 2, 20));
        s.record_snapshot(k_geo, wm("s3://geo-1.json", 5, 50));
        s.record_snapshot(k_other, wm("s3://other-store.json", 9, 90));

        let pinned = s.pinned_tables("gs:main");
        assert_eq!(pinned.len(), 2, "only this source's tables are reported");
        assert_eq!(
            pinned.get("DW.DIM_STORE").unwrap().metadata_location,
            "s3://snap-A.json",
            "first-recorded snapshot wins over a later touch"
        );
        assert_eq!(pinned.get("DW.DIM_STORE").unwrap().snapshot_id, Some(1));
        assert_eq!(
            pinned.get("DW.DIM_STORE").unwrap().sequence_number,
            Some(10)
        );
        assert!(
            pinned.contains_key("DW.DIM_GEOGRAPHY"),
            "the table key is the raw logical table name, not namespace.table split"
        );
        assert_eq!(
            s.pinned_tables("other:main").len(),
            1,
            "a different source's watermarks are isolated"
        );
    }

    #[test]
    fn keys_isolate_by_source_and_table() {
        let s = IcebergCatalogSession::default();
        let k1 = IcebergCatalogSession::load_table_key("gs:main", "DW", "DIM_STORE");
        let k2 = IcebergCatalogSession::load_table_key("gs:main", "DW", "DIM_GEOGRAPHY");
        let k3 = IcebergCatalogSession::load_table_key("other:main", "DW", "DIM_STORE");
        s.store_load_table(k1.clone(), &resp("s3://store.json", None));
        assert!(s.cached_load_table(&k1).is_some());
        assert!(s.cached_load_table(&k2).is_none(), "different table misses");
        assert!(
            s.cached_load_table(&k3).is_none(),
            "different source misses"
        );
    }
}
