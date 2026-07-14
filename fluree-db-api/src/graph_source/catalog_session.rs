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
use fluree_db_iceberg::io::S3IcebergStorage;

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
    storages: Mutex<HashMap<String, Arc<S3IcebergStorage>>>,
}

impl IcebergCatalogSession {
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
        self.load_tables
            .lock()
            .unwrap()
            .get(key)
            .map(|e| e.metadata_location.clone())
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
    pub(crate) fn cached_storage(&self, key: &str) -> Option<Arc<S3IcebergStorage>> {
        if !cache_enabled() {
            return None;
        }
        self.storages.lock().unwrap().get(key).cloned()
    }

    /// Cache the S3 storage client built from `key`'s current pinned credentials.
    /// Paired with `cached_storage`; `store_load_table` invalidates on a creds
    /// refresh, so an entry here always corresponds to the currently pinned creds.
    /// No-op when the cache is disabled.
    pub(crate) fn store_storage(&self, key: String, storage: Arc<S3IcebergStorage>) {
        if !cache_enabled() {
            return;
        }
        self.storages.lock().unwrap().insert(key, storage);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};

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
        let storage = Arc::new(
            S3IcebergStorage::from_default_chain(Some("us-east-2"), None, false)
                .await
                .expect("offline SDK client construction"),
        );
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
