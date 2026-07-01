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

use fluree_db_iceberg::catalog::{LoadTableResponse, RestCatalogClient};
use fluree_db_iceberg::credential::VendedCredentials;

/// Whether the query-scoped catalog cache is enabled. Read once from
/// `FLUREE_ICEBERG_LOADTABLE_CACHE` (only `0`/`false`/`off` disable it).
fn cache_enabled() -> bool {
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
/// another REST round-trip.
#[derive(Clone)]
struct CachedLoadTable {
    metadata_location: String,
    credentials: Option<VendedCredentials>,
}

impl CachedLoadTable {
    /// True when vended credentials are present and at/after their (30s-buffered)
    /// expiry, so a later scan must reload rather than hand out stale creds.
    fn creds_expired(&self) -> bool {
        self.credentials
            .as_ref()
            .is_some_and(VendedCredentials::is_expired)
    }
}

/// Query-scoped catalog state shared across every scan of one query.
#[derive(Default)]
pub(crate) struct IcebergCatalogSession {
    /// Reused REST clients keyed by graph source id. One client per source means
    /// one OAuth token cache, hence one token exchange for the whole query.
    /// `RestCatalogClient` is not `Clone`, so it is shared via `Arc`.
    clients: Mutex<HashMap<String, Arc<RestCatalogClient>>>,
    /// Cached `loadTable` responses keyed by `(graph_source_id, namespace.table)`.
    load_tables: Mutex<HashMap<String, CachedLoadTable>>,
}

impl IcebergCatalogSession {
    /// Cache key for a `loadTable` response: source id + fully-qualified table.
    pub(crate) fn load_table_key(graph_source_id: &str, namespace: &str, table: &str) -> String {
        format!("{graph_source_id}\u{1f}{namespace}.{table}")
    }

    /// Get the cached REST client for `key`, or build one with `build` and cache
    /// it. The build closure is synchronous (client construction performs no
    /// I/O), so the lock is never held across an `await`. With the cache
    /// disabled, every call builds a fresh client (per-scan OAuth restored).
    pub(crate) fn rest_client_or_build<E>(
        &self,
        key: &str,
        build: impl FnOnce() -> Result<RestCatalogClient, E>,
    ) -> Result<Arc<RestCatalogClient>, E> {
        if !cache_enabled() {
            return Ok(Arc::new(build()?));
        }
        let mut clients = self.clients.lock().unwrap();
        if let Some(c) = clients.get(key) {
            return Ok(Arc::clone(c));
        }
        let client = Arc::new(build()?);
        clients.insert(key.to_string(), Arc::clone(&client));
        Ok(client)
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
        Some(LoadTableResponse {
            metadata_location: hit.metadata_location.clone(),
            credentials: hit.credentials.clone(),
            config: HashMap::default(),
        })
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
        let mut lts = self.load_tables.lock().unwrap();
        match lts.get_mut(&key) {
            Some(existing) => existing.credentials = resp.credentials.clone(),
            None => {
                lts.insert(
                    key,
                    CachedLoadTable {
                        metadata_location: resp.metadata_location.clone(),
                        credentials: resp.credentials.clone(),
                    },
                );
            }
        }
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
