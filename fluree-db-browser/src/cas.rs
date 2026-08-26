//! The browser CAS storage: proxy reads + residency tier + persistent cache.
//!
//! [`BrowserCasStorage`] implements the engine's `Storage` traits by
//! wrapping a raw-mode [`ProxyStorage`] (which owns the wire protocol and
//! CID verification) with three cache concerns:
//!
//! - an in-memory [`ResidencyTier`] the synchronous read path can see
//!   through `resolve_cached_bytes`;
//! - the IndexedDB block cache, reached through the driver as `CacheGet` /
//!   `CachePut` jobs (write-behind: a put never delays a read);
//! - request coalescing and a bounded fetch width.
//!
//! Read path for an address: parse its CID → residency hit? return the
//! shared `Arc` → otherwise become the leader (or wait) for that CID →
//! persistent cache hit? make resident → otherwise fetch through the proxy
//! (verified against the CID), make resident, and enqueue the write-behind.
//!
//! ## Copies
//!
//! A fetched block is copied exactly twice: once out of JavaScript memory
//! into the transport's `Bytes` (unavoidable), and once from that `Bytes`
//! into the `Arc<[u8]>` the residency hook's signature requires. The
//! `Vec<u8>`-returning `Storage` methods pay a third copy at their
//! boundary; residency-first callers (prefetchers, the fetch-and-re-run
//! loop) should use [`ensure_resident`](BrowserCasStorage::ensure_resident)
//! or [`prefetch`](BrowserCasStorage::prefetch), which return the resident
//! `Arc` and copy nothing further. Every subsequent synchronous hit is an
//! `Arc` clone.

use crate::bridge::IoHandle;
use crate::coalesce::{InFlight, Ticket};
use crate::config::BrowserIoConfig;
use crate::protocol::IoJob;
use crate::residency::{PinSet, ResidencyError, ResidencyTier};
use async_trait::async_trait;
use bytes::Bytes;
use fluree_db_core::error::{Error as CoreError, Result};
use fluree_db_core::storage::ReadHint;
use fluree_db_core::{
    ContentAddressedWrite, ContentId, ContentKind, ContentWriteResult, StorageMethod, StorageRead,
    StorageWrite,
};
use fluree_db_nameservice_sync::{cid_and_ledger_from_address, ProxyStorage};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{oneshot, Semaphore};

type SharedError = Arc<CoreError>;

/// Clone an error for fan-out to coalesced waiters. `NotFound` is preserved
/// exactly (callers branch on it); everything else keeps its message.
fn clone_error(e: &CoreError) -> CoreError {
    match e {
        CoreError::NotFound(s) => CoreError::NotFound(s.clone()),
        other => CoreError::storage(other.to_string()),
    }
}

fn residency_error(e: ResidencyError) -> CoreError {
    CoreError::storage(e.to_string())
}

#[derive(Default)]
struct Counters {
    residency_hits: AtomicU64,
    cache_hits: AtomicU64,
    fetches: AtomicU64,
    bytes_fetched: AtomicU64,
    coalesced_waits: AtomicU64,
}

/// Point-in-time counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CasStats {
    /// Reads served from the in-memory residency tier.
    pub residency_hits: u64,
    /// Reads served from the persistent (IndexedDB) cache.
    pub cache_hits: u64,
    /// Network fetches performed.
    pub fetches: u64,
    /// Bytes received over the network.
    pub bytes_fetched: u64,
    /// Reads that joined an in-flight fetch instead of starting one.
    pub coalesced_waits: u64,
}

struct Inner {
    proxy: ProxyStorage,
    io: IoHandle,
    residency: Arc<ResidencyTier>,
    inflight: InFlight<ContentId, Arc<[u8]>, SharedError>,
    fetch_slots: Semaphore,
    cache_enabled: bool,
    counters: Counters,
}

/// Engine-facing storage for a browser peer. Cheap to clone; clones share
/// the residency tier, caches, and counters.
#[derive(Clone)]
pub struct BrowserCasStorage {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for BrowserCasStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrowserCasStorage")
            .field("proxy", &self.inner.proxy)
            .field("residency", &self.inner.residency)
            .field("cache_enabled", &self.inner.cache_enabled)
            .finish_non_exhaustive()
    }
}

impl BrowserCasStorage {
    /// Wrap a raw-mode proxy client. `io` reaches the driver that serves
    /// the persistent cache (the proxy's own transport is expected to be a
    /// [`WasmFetchTransport`](crate::WasmFetchTransport) over the same
    /// driver, but nothing here depends on that).
    pub fn new(proxy: ProxyStorage, io: IoHandle, config: &BrowserIoConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                proxy,
                io,
                residency: Arc::new(ResidencyTier::new(config.residency_budget_bytes)),
                inflight: InFlight::default(),
                fetch_slots: Semaphore::new(config.max_concurrent_fetches.max(1)),
                cache_enabled: config.cache.enabled,
                counters: Counters::default(),
            }),
        }
    }

    /// The residency tier (for pinning, stats, or direct sync lookups).
    pub fn residency(&self) -> &Arc<ResidencyTier> {
        &self.inner.residency
    }

    /// A query-duration pin set over the residency tier.
    pub fn pin_set(&self) -> PinSet {
        self.inner.residency.pin_set()
    }

    /// The driver handle this storage enqueues cache jobs on.
    pub fn io(&self) -> &IoHandle {
        &self.inner.io
    }

    /// The underlying proxy client.
    pub fn proxy(&self) -> &ProxyStorage {
        &self.inner.proxy
    }

    /// Current counters.
    pub fn stats(&self) -> CasStats {
        let c = &self.inner.counters;
        CasStats {
            residency_hits: c.residency_hits.load(Ordering::Relaxed),
            cache_hits: c.cache_hits.load(Ordering::Relaxed),
            fetches: c.fetches.load(Ordering::Relaxed),
            bytes_fetched: c.bytes_fetched.load(Ordering::Relaxed),
            coalesced_waits: c.coalesced_waits.load(Ordering::Relaxed),
        }
    }

    /// The CID a canonical storage address refers to, if it parses.
    pub fn cid_for_address(address: &str) -> Option<ContentId> {
        cid_and_ledger_from_address(address).map(|(cid, _)| cid)
    }

    /// Make the object at `address` resident and return the shared bytes.
    /// This is the residency-first entry point: no copy beyond the two the
    /// module docs describe, and a hit costs an `Arc` clone.
    pub async fn ensure_resident(&self, address: &str) -> Result<Arc<[u8]>> {
        self.load(address).await
    }

    /// Make every address resident, at most `max_concurrent_fetches` at a
    /// time. Returns the addresses that failed with their errors; an empty
    /// vector means everything is resident.
    pub async fn prefetch<I>(&self, addresses: I) -> Vec<(String, CoreError)>
    where
        I: IntoIterator<Item = String>,
    {
        let futures = addresses.into_iter().map(|address| async move {
            let result = self.load(&address).await;
            (address, result)
        });
        futures::future::join_all(futures)
            .await
            .into_iter()
            .filter_map(|(address, result)| result.err().map(|e| (address, e)))
            .collect()
    }

    async fn load(&self, address: &str) -> Result<Arc<[u8]>> {
        let Some((cid, _ledger)) = cid_and_ledger_from_address(address) else {
            // Not a canonical CAS address: nothing to key a cache on. The
            // proxy reports the parse failure with its own message.
            return self.inner.proxy.read_bytes(address).await.map(Arc::from);
        };

        if let Some(bytes) = self.inner.residency.resolve(&cid) {
            self.inner
                .counters
                .residency_hits
                .fetch_add(1, Ordering::Relaxed);
            return Ok(bytes);
        }

        match self.inner.inflight.begin(cid.clone()) {
            Ticket::Waiter(rx) => {
                self.inner
                    .counters
                    .coalesced_waits
                    .fetch_add(1, Ordering::Relaxed);
                match rx.await {
                    Ok(Ok(bytes)) => Ok(bytes),
                    Ok(Err(e)) => Err(clone_error(&e)),
                    Err(_) => Err(CoreError::storage(format!(
                        "coalesced fetch for {address} was cancelled"
                    ))),
                }
            }
            Ticket::Leader(guard) => {
                let result = self.fetch_into_residency(&cid, address).await;
                match &result {
                    Ok(bytes) => guard.complete(Ok(Arc::clone(bytes))),
                    Err(e) => guard.complete(Err(Arc::new(clone_error(e)))),
                }
                result
            }
        }
    }

    async fn fetch_into_residency(&self, cid: &ContentId, address: &str) -> Result<Arc<[u8]>> {
        if self.inner.cache_enabled {
            if let Some(bytes) = self.cache_get(cid).await {
                self.inner
                    .counters
                    .cache_hits
                    .fetch_add(1, Ordering::Relaxed);
                let resident: Arc<[u8]> = Arc::from(&bytes[..]);
                drop(bytes);
                return self.make_resident(cid, resident, false);
            }
        }

        let bytes = {
            let _slot = self
                .inner
                .fetch_slots
                .acquire()
                .await
                .map_err(|_| CoreError::storage("browser fetch limiter is closed"))?;
            // Verified against the CID inside the proxy client.
            self.inner.proxy.read_object_bytes(address).await?
        };
        self.inner.counters.fetches.fetch_add(1, Ordering::Relaxed);
        self.inner
            .counters
            .bytes_fetched
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);

        let resident: Arc<[u8]> = Arc::from(&bytes[..]);
        drop(bytes);
        self.make_resident(cid, resident, true)
    }

    /// Insert verified bytes into the residency tier and, for network
    /// fetches, enqueue the write-behind persist. The persist is enqueued
    /// first so a residency-budget failure never loses the bytes for the
    /// next session.
    fn make_resident(
        &self,
        cid: &ContentId,
        bytes: Arc<[u8]>,
        persist: bool,
    ) -> Result<Arc<[u8]>> {
        if persist && self.inner.cache_enabled {
            // Driver gone → nothing to persist to; not a read failure.
            let _ = self.inner.io.send(IoJob::CachePut {
                key: cid.clone(),
                bytes: Arc::clone(&bytes),
            });
        }
        self.inner
            .residency
            .insert(cid.clone(), bytes)
            .map_err(residency_error)
    }

    async fn cache_get(&self, cid: &ContentId) -> Option<Bytes> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .io
            .send(IoJob::CacheGet {
                key: cid.clone(),
                reply,
            })
            .ok()?;
        rx.await.ok().flatten()
    }
}

#[async_trait]
impl StorageRead for BrowserCasStorage {
    async fn read_bytes(&self, address: &str) -> Result<Vec<u8>> {
        Ok(self.load(address).await?.to_vec())
    }

    async fn read_bytes_hint(&self, address: &str, _hint: ReadHint) -> Result<Vec<u8>> {
        // Raw tier only: hints select FLKB representations that never
        // apply to a cache peer.
        self.read_bytes(address).await
    }

    /// Whole-blob policy: the object is made resident (and persisted) and
    /// the slice is served from memory, so repeated windowed reads over
    /// one object cost one fetch. Advertised as a non-native range read so
    /// callers that issue many windows read the whole object once.
    async fn read_byte_range(&self, address: &str, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        if range.start >= range.end {
            return Ok(Vec::new());
        }
        let bytes = self.load(address).await?;
        let start = range.start as usize;
        if start >= bytes.len() {
            return Ok(Vec::new());
        }
        let end = (range.end as usize).min(bytes.len());
        Ok(bytes[start..end].to_vec())
    }

    fn supports_ranged_reads(&self) -> bool {
        false
    }

    async fn exists(&self, address: &str) -> Result<bool> {
        if let Some((cid, _)) = cid_and_ledger_from_address(address) {
            if self.inner.residency.contains(&cid) {
                return Ok(true);
            }
        }
        self.inner.proxy.exists(address).await
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.proxy.list_prefix(prefix).await
    }

    fn resolve_cached_bytes(&self, id: &ContentId) -> Option<Arc<[u8]>> {
        self.inner.residency.resolve(id)
    }
}

#[async_trait]
impl StorageWrite for BrowserCasStorage {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> Result<()> {
        self.inner.proxy.write_bytes(address, bytes).await
    }

    async fn delete(&self, address: &str) -> Result<()> {
        self.inner.proxy.delete(address).await
    }
}

#[async_trait]
impl ContentAddressedWrite for BrowserCasStorage {
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_alias: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        self.inner
            .proxy
            .content_write_bytes_with_hash(kind, ledger_alias, content_hash_hex, bytes)
            .await
    }

    async fn content_write_bytes(
        &self,
        kind: ContentKind,
        ledger_alias: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        self.inner
            .proxy
            .content_write_bytes(kind, ledger_alias, bytes)
            .await
    }
}

impl StorageMethod for BrowserCasStorage {
    fn storage_method(&self) -> &str {
        self.inner.proxy.storage_method()
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod tests {
    use super::*;
    use crate::bridge::{IoReceiver, WasmFetchTransport};
    use crate::config::CacheConfig;
    use fluree_db_nameservice_sync::{ProxyReadMode, TransportResponse};
    use http::{HeaderMap, StatusCode};
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::time::Duration;

    pub(crate) const API_BASE: &str = "http://origin.example/v1/fluree";
    pub(crate) const LEDGER: &str = "mydb:main";

    /// Shared state of the mock driver: canned objects by CID string, the
    /// persistent-cache contents, and observation logs.
    #[derive(Default)]
    pub(crate) struct MockState {
        pub objects: HashMap<String, (u16, Vec<u8>)>,
        pub cache: HashMap<String, Vec<u8>>,
        pub fetch_log: Vec<String>,
        pub url_log: Vec<(String, Vec<(&'static str, String)>)>,
        pub puts: Vec<String>,
        pub in_flight: usize,
        pub max_in_flight: usize,
        pub hold: Option<Duration>,
    }

    fn cid_from_url(url: &str) -> String {
        url.split("/storage/objects/")
            .nth(1)
            .and_then(|rest| rest.split('?').next())
            .unwrap_or_default()
            .to_string()
    }

    /// Drive the job channel like the wasm driver would, from canned state.
    pub(crate) fn spawn_mock_driver(
        mut rx: IoReceiver,
        state: Arc<Mutex<MockState>>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                let state = Arc::clone(&state);
                match job {
                    IoJob::Fetch { req, reply, .. } => {
                        tokio::spawn(async move {
                            let cid = cid_from_url(&req.url);
                            let (hold, canned) = {
                                let mut s = state.lock().unwrap();
                                s.url_log.push((req.url.clone(), req.headers.clone()));
                                if !cid.is_empty() {
                                    s.fetch_log.push(cid.clone());
                                }
                                s.in_flight += 1;
                                s.max_in_flight = s.max_in_flight.max(s.in_flight);
                                (s.hold, s.objects.get(&cid).cloned())
                            };
                            if let Some(hold) = hold {
                                tokio::time::sleep(hold).await;
                            }
                            let (status, body) = canned.unwrap_or((404, Vec::new()));
                            state.lock().unwrap().in_flight -= 1;
                            let _ = reply.send(Ok(TransportResponse {
                                status: StatusCode::from_u16(status).unwrap(),
                                headers: HeaderMap::new(),
                                body: Bytes::from(body),
                            }));
                        });
                    }
                    IoJob::CacheGet { key, reply } => {
                        let hit = state
                            .lock()
                            .unwrap()
                            .cache
                            .get(&key.to_string())
                            .map(|v| Bytes::from(v.clone()));
                        let _ = reply.send(hit);
                    }
                    IoJob::CachePut { key, bytes } => {
                        let mut s = state.lock().unwrap();
                        s.puts.push(key.to_string());
                        s.cache.insert(key.to_string(), bytes.to_vec());
                    }
                    IoJob::Shutdown => break,
                }
            }
        })
    }

    pub(crate) fn object(tag: u8, len: usize) -> (ContentId, String, Vec<u8>) {
        let bytes = vec![tag; len];
        let id = ContentId::new(ContentKind::IndexLeaf, &bytes);
        let address =
            fluree_db_core::content_address("proxy", ContentKind::IndexLeaf, LEDGER, &id.digest_hex());
        (id, address, bytes)
    }

    pub(crate) fn storage_with(
        state: &Arc<Mutex<MockState>>,
        config: &BrowserIoConfig,
    ) -> (BrowserCasStorage, IoHandle, tokio::task::JoinHandle<()>) {
        let (io, rx) = IoHandle::channel();
        let driver = spawn_mock_driver(rx, Arc::clone(state));
        let transport = Arc::new(WasmFetchTransport::new(io.clone(), config.fetch_timeout));
        let proxy = ProxyStorage::from_api_base_with_transport(
            API_BASE.to_string(),
            "tok".to_string(),
            ProxyReadMode::Raw,
            transport,
        );
        let storage = BrowserCasStorage::new(proxy, io.clone(), config);
        (storage, io, driver)
    }

    fn config() -> BrowserIoConfig {
        BrowserIoConfig {
            fetch_timeout: Duration::from_secs(1),
            residency_budget_bytes: 1024,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn fetch_verifies_then_serves_from_residency_and_writes_behind() {
        let (id, address, bytes) = object(1, 16);
        let state = Arc::new(Mutex::new(MockState::default()));
        state
            .lock()
            .unwrap()
            .objects
            .insert(id.to_string(), (200, bytes.clone()));
        let (storage, io, driver) = storage_with(&state, &config());

        let got = storage.read_bytes(&address).await.expect("first read fetches");
        assert_eq!(got, bytes);
        assert_eq!(state.lock().unwrap().fetch_log, vec![id.to_string()]);
        // Bearer header and canonical URL formed by the shared proxy client.
        {
            let s = state.lock().unwrap();
            let (url, headers) = &s.url_log[0];
            assert_eq!(
                url,
                &format!("{API_BASE}/storage/objects/{id}?ledger=mydb%3Amain")
            );
            assert_eq!(headers, &vec![("authorization", "Bearer tok".to_string())]);
        }

        // Second read: residency hit, no network.
        let again = storage.read_bytes(&address).await.unwrap();
        assert_eq!(again, bytes);
        assert_eq!(state.lock().unwrap().fetch_log.len(), 1);
        let stats = storage.stats();
        assert_eq!(stats.fetches, 1);
        assert_eq!(stats.residency_hits, 1);
        assert_eq!(stats.bytes_fetched, 16);

        // Sync hook sees the same allocation the async path produced.
        let resident = storage.resolve_cached_bytes(&id).expect("resident");
        assert_eq!(&resident[..], &bytes[..]);
        let again_arc = storage.ensure_resident(&address).await.unwrap();
        assert!(Arc::ptr_eq(&resident, &again_arc));

        // Write-behind reached the persistent cache with identical bytes.
        io.shutdown();
        driver.await.unwrap();
        let s = state.lock().unwrap();
        assert_eq!(s.puts, vec![id.to_string()]);
        assert_eq!(s.cache.get(&id.to_string()).unwrap(), &bytes);
    }

    #[tokio::test]
    async fn tampered_bytes_are_rejected_and_never_cached() {
        let (id, address, _bytes) = object(2, 16);
        let state = Arc::new(Mutex::new(MockState::default()));
        state
            .lock()
            .unwrap()
            .objects
            .insert(id.to_string(), (200, b"tampered payload".to_vec()));
        let (storage, io, driver) = storage_with(&state, &config());

        let err = storage.read_bytes(&address).await.expect_err("must reject");
        assert!(
            err.to_string().contains("Integrity verification failed"),
            "got: {err}"
        );
        assert!(storage.resolve_cached_bytes(&id).is_none());
        io.shutdown();
        driver.await.unwrap();
        assert!(state.lock().unwrap().puts.is_empty(), "nothing persisted");
    }

    #[tokio::test]
    async fn persistent_cache_hit_skips_the_network() {
        let (id, address, bytes) = object(3, 16);
        let state = Arc::new(Mutex::new(MockState::default()));
        state
            .lock()
            .unwrap()
            .cache
            .insert(id.to_string(), bytes.clone());
        let (storage, io, driver) = storage_with(&state, &config());

        let got = storage.read_bytes(&address).await.unwrap();
        assert_eq!(got, bytes);
        assert!(state.lock().unwrap().fetch_log.is_empty(), "no fetch");
        assert_eq!(storage.stats().cache_hits, 1);
        assert!(storage.resolve_cached_bytes(&id).is_some());
        io.shutdown();
        driver.await.unwrap();
        // A cache hit is not re-persisted.
        assert!(state.lock().unwrap().puts.is_empty());
    }

    #[tokio::test]
    async fn cache_disabled_never_touches_the_driver_cache_jobs() {
        let (id, address, bytes) = object(4, 16);
        let state = Arc::new(Mutex::new(MockState::default()));
        {
            let mut s = state.lock().unwrap();
            s.objects.insert(id.to_string(), (200, bytes.clone()));
            // Even a populated cache is ignored when disabled.
            s.cache.insert(id.to_string(), bytes.clone());
        }
        let cfg = BrowserIoConfig {
            cache: CacheConfig {
                enabled: false,
                ..Default::default()
            },
            ..config()
        };
        let (storage, io, driver) = storage_with(&state, &cfg);
        assert_eq!(storage.read_bytes(&address).await.unwrap(), bytes);
        assert_eq!(state.lock().unwrap().fetch_log.len(), 1);
        assert_eq!(storage.stats().cache_hits, 0);
        io.shutdown();
        driver.await.unwrap();
        assert!(state.lock().unwrap().puts.is_empty());
    }

    #[tokio::test]
    async fn concurrent_misses_coalesce_into_one_fetch() {
        let (id, address, bytes) = object(5, 32);
        let state = Arc::new(Mutex::new(MockState::default()));
        {
            let mut s = state.lock().unwrap();
            s.objects.insert(id.to_string(), (200, bytes.clone()));
            s.hold = Some(Duration::from_millis(20));
        }
        let (storage, io, driver) = storage_with(&state, &config());

        let reads = (0..8).map(|_| storage.read_bytes(&address));
        let results = futures::future::join_all(reads).await;
        for r in results {
            assert_eq!(r.unwrap(), bytes);
        }
        assert_eq!(state.lock().unwrap().fetch_log.len(), 1, "one fetch");
        let stats = storage.stats();
        assert_eq!(stats.fetches, 1);
        assert_eq!(stats.coalesced_waits, 7);
        io.shutdown();
        driver.await.unwrap();
    }

    #[tokio::test]
    async fn prefetch_width_is_bounded_and_makes_everything_resident() {
        let objects: Vec<_> = (10u8..16).map(|t| object(t, 8)).collect();
        let state = Arc::new(Mutex::new(MockState::default()));
        {
            let mut s = state.lock().unwrap();
            for (id, _, bytes) in &objects {
                s.objects.insert(id.to_string(), (200, bytes.clone()));
            }
            s.hold = Some(Duration::from_millis(20));
        }
        let cfg = BrowserIoConfig {
            max_concurrent_fetches: 2,
            ..config()
        };
        let (storage, io, driver) = storage_with(&state, &cfg);

        let failures = storage
            .prefetch(objects.iter().map(|(_, a, _)| a.clone()))
            .await;
        assert!(failures.is_empty(), "{failures:?}");
        {
            let s = state.lock().unwrap();
            assert_eq!(s.fetch_log.len(), 6);
            assert!(
                s.max_in_flight <= 2,
                "fetch width must be bounded, saw {}",
                s.max_in_flight
            );
            assert!(s.max_in_flight >= 2, "fetches must overlap");
        }
        for (id, _, bytes) in &objects {
            assert_eq!(&storage.resolve_cached_bytes(id).unwrap()[..], &bytes[..]);
        }
        io.shutdown();
        driver.await.unwrap();
    }

    #[tokio::test]
    async fn forbidden_maps_to_not_found_and_exists_is_false() {
        let (id, address, _bytes) = object(6, 8);
        let state = Arc::new(Mutex::new(MockState::default()));
        state
            .lock()
            .unwrap()
            .objects
            .insert(id.to_string(), (403, Vec::new()));
        let (storage, io, driver) = storage_with(&state, &config());
        let err = storage.read_bytes(&address).await.expect_err("403");
        assert!(matches!(err, CoreError::NotFound(_)), "got {err:?}");
        assert!(!storage.exists(&address).await.unwrap());
        // Coalesced waiters see the same NotFound class.
        io.shutdown();
        driver.await.unwrap();
    }

    #[tokio::test]
    async fn byte_ranges_are_served_from_the_resident_blob() {
        let (id, address, bytes) = object(7, 32);
        let state = Arc::new(Mutex::new(MockState::default()));
        state
            .lock()
            .unwrap()
            .objects
            .insert(id.to_string(), (200, bytes.clone()));
        let (storage, io, driver) = storage_with(&state, &config());
        assert!(!storage.supports_ranged_reads());
        let slice = storage.read_byte_range(&address, 4..12).await.unwrap();
        assert_eq!(slice, bytes[4..12].to_vec());
        assert!(storage.read_byte_range(&address, 40..50).await.unwrap().is_empty());
        assert!(storage.read_byte_range(&address, 5..5).await.unwrap().is_empty());
        let tail = storage.read_byte_range(&address, 30..u64::MAX).await.unwrap();
        assert_eq!(tail, bytes[30..].to_vec());
        assert_eq!(state.lock().unwrap().fetch_log.len(), 1, "one fetch total");
        assert!(storage.exists(&address).await.unwrap());
        io.shutdown();
        driver.await.unwrap();
    }

    #[tokio::test]
    async fn pinned_working_set_survives_budget_pressure() {
        let a = object(20, 40);
        let b = object(21, 40);
        let c = object(22, 40);
        let state = Arc::new(Mutex::new(MockState::default()));
        {
            let mut s = state.lock().unwrap();
            for (id, _, bytes) in [&a, &b, &c] {
                s.objects.insert(id.to_string(), (200, bytes.clone()));
            }
        }
        let cfg = BrowserIoConfig {
            residency_budget_bytes: 80,
            ..config()
        };
        let (storage, io, driver) = storage_with(&state, &cfg);

        storage.ensure_resident(&a.1).await.unwrap();
        let pins = storage.pin_set();
        assert!(pins.pin(&a.0));
        storage.ensure_resident(&b.1).await.unwrap();
        storage.ensure_resident(&c.1).await.unwrap();
        assert!(storage.resolve_cached_bytes(&a.0).is_some(), "pinned survives");
        assert!(storage.resolve_cached_bytes(&b.0).is_none(), "LRU unpinned evicted");
        assert!(storage.resolve_cached_bytes(&c.0).is_some());

        // Pin c too: the working set (80) fills the budget, so a further
        // insert is a typed working-set failure — not silent thrashing.
        assert!(pins.pin(&c.0));
        let err = storage.ensure_resident(&b.1).await.expect_err("over budget");
        assert!(err.to_string().contains("working set exceeds"), "got {err}");

        drop(pins);
        storage.ensure_resident(&b.1).await.unwrap();
        io.shutdown();
        driver.await.unwrap();
    }
}
