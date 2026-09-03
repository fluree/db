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
use crate::gauge::{WriteBehindGauge, WriteBehindPermit};
use crate::protocol::IoJob;
use crate::residency::{PinSet, QueryGuard, ResidencyError, ResidencyTier};
use async_trait::async_trait;
use bytes::Bytes;
use fluree_db_core::error::{Error as CoreError, Result};
use fluree_db_core::storage::residency::{MissRegister, Want};
use fluree_db_core::storage::ReadHint;
use fluree_db_core::{
    ContentAddressedWrite, ContentId, ContentKind, ContentWriteResult, StorageMethod, StorageRead,
    StorageWrite,
};
use fluree_db_nameservice_sync::{cid_and_ledger_from_address, ProxyStorage};
use futures::StreamExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
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
    cache_rejections: AtomicU64,
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
    /// Cached rows rejected because their bytes did not hash to the CID key
    /// they were stored under (tampered/corrupt); refetched from origin.
    /// A nonzero value is a security signal, not just a performance one.
    pub cache_rejections: u64,
    /// Network fetches performed.
    pub fetches: u64,
    /// Bytes received over the network.
    pub bytes_fetched: u64,
    /// Reads that joined an in-flight fetch instead of starting one.
    pub coalesced_waits: u64,
    /// Bytes currently queued for the IndexedDB write-behind.
    pub write_behind_outstanding: u64,
    /// High-water mark of the write-behind queue.
    pub write_behind_peak: u64,
}

struct Inner {
    proxy: ProxyStorage,
    io: IoHandle,
    residency: Arc<ResidencyTier>,
    inflight: InFlight<ContentId, Arc<[u8]>, SharedError>,
    fetch_slots: Semaphore,
    /// The configured fetch width, mirrored for bounded batch fan-out.
    fetch_width: usize,
    cache_enabled: bool,
    write_behind: Arc<WriteBehindGauge>,
    budget_wait: Duration,
    register: MissRegister,
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
                fetch_width: config.max_concurrent_fetches.max(1),
                cache_enabled: config.cache.enabled,
                write_behind: WriteBehindGauge::new(config.write_behind_budget_bytes),
                budget_wait: config.budget_wait,
                register: MissRegister::new(),
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
            cache_rejections: c.cache_rejections.load(Ordering::Relaxed),
            fetches: c.fetches.load(Ordering::Relaxed),
            bytes_fetched: c.bytes_fetched.load(Ordering::Relaxed),
            coalesced_waits: c.coalesced_waits.load(Ordering::Relaxed),
            write_behind_outstanding: self.inner.write_behind.outstanding(),
            write_behind_peak: self.inner.write_behind.peak(),
        }
    }

    /// Mark a query as in flight for the guard's lifetime; the residency
    /// tier then evicts only entries provably unobservable by any live
    /// query (the epoch-tick rule — see `residency`). The retry loop holds
    /// one across a query's rounds so every observed byte stays resident
    /// (the engine's fetch-pins contract).
    pub fn query_guard(&self) -> QueryGuard {
        self.inner.residency.begin_query()
    }

    /// The canonical storage address for a CID under `ledger`, using this
    /// storage's method. `None` for CIDs whose kind has no distinct address
    /// (the annotation arenas).
    pub fn address_for(&self, ledger: &str, cid: &ContentId) -> Option<String> {
        let kind = cid.content_kind()?;
        Some(fluree_db_core::content_address(
            self.storage_method(),
            kind,
            ledger,
            &cid.digest_hex(),
        ))
    }

    /// Batch-first want fetching: make every CID resident, at most
    /// `max_concurrent_fetches` in flight, coalescing with any concurrent
    /// reads. This is the fetch half of the miss-register drain loop —
    /// hand it the CIDs from the drained wants. Returns per-CID failures;
    /// empty means everything is resident.
    pub async fn fetch_cids<I>(&self, ledger: &str, cids: I) -> Vec<(ContentId, CoreError)>
    where
        I: IntoIterator<Item = ContentId>,
    {
        let futures = cids.into_iter().map(|cid| async move {
            let result = match self.address_for(ledger, &cid) {
                Some(address) => self.load(&address).await.map(|_| ()),
                None => Err(CoreError::storage(format!(
                    "CID {cid} has no addressable storage kind"
                ))),
            };
            (cid, result)
        });
        futures::stream::iter(futures)
            .buffer_unordered(self.inner.fetch_width)
            .filter_map(|(cid, result)| async move { result.err().map(|e| (cid, e)) })
            .collect()
            .await
    }

    /// Fetch a drained want set (the miss register's currency) and pin the
    /// successes — the browser-side twin of the engine's `fetch_wants`
    /// retry primitive, for callers that hold the ledger context.
    pub async fn fetch_wants<I>(
        &self,
        ledger: &str,
        wants: I,
        pins: &PinSet,
    ) -> Vec<(ContentId, CoreError)>
    where
        I: IntoIterator<Item = Want>,
    {
        self.fetch_cids_pinned(ledger, wants.into_iter().map(|w| w.cid), pins)
            .await
    }

    /// [`fetch_cids`](Self::fetch_cids), pinning each successfully fetched
    /// CID into `pins` so it survives until the pin set drops.
    pub async fn fetch_cids_pinned<I>(
        &self,
        ledger: &str,
        cids: I,
        pins: &PinSet,
    ) -> Vec<(ContentId, CoreError)>
    where
        I: IntoIterator<Item = ContentId>,
    {
        let cids: Vec<ContentId> = cids.into_iter().collect();
        let mut failures = self.fetch_cids(ledger, cids.clone()).await;
        for cid in &cids {
            if failures.iter().any(|(failed, _)| failed == cid) {
                continue;
            }
            // A fetched CID can in principle be evicted before the pin
            // lands; reporting it pinned would break the caller's
            // monotone-progress accounting — surface it as a failure.
            if !pins.pin(cid) {
                failures.push((
                    cid.clone(),
                    CoreError::storage(format!("{cid} was evicted before it could be pinned")),
                ));
            }
        }
        failures
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
        // Bounded fan-out: without this, a large warm-up spawns every load
        // at once and the write-behind gauge's admission control cannot
        // propagate to the batch.
        futures::stream::iter(futures)
            .buffer_unordered(self.inner.fetch_width)
            .filter_map(|(address, result)| async move { result.err().map(|e| (address, e)) })
            .collect()
            .await
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

        // A cancelled LEADER (its task dropped, not failed) leaves the
        // block perfectly fetchable — followers re-elect a bounded number
        // of times instead of surfacing a spurious cancellation.
        let mut elections = 0u8;
        loop {
            match self.inner.inflight.begin(cid.clone()) {
                Ticket::Waiter(rx) => {
                    self.inner
                        .counters
                        .coalesced_waits
                        .fetch_add(1, Ordering::Relaxed);
                    match rx.await {
                        Ok(Ok(bytes)) => return Ok(bytes),
                        Ok(Err(e)) => return Err(clone_error(&e)),
                        Err(_) => {
                            // The leader may have completed before it was
                            // cancelled; a residency hit avoids a refetch.
                            if let Some(bytes) = self.inner.residency.resolve(&cid) {
                                return Ok(bytes);
                            }
                            elections += 1;
                            if elections >= 3 {
                                return Err(CoreError::storage(format!(
                                    "coalesced fetch for {address} was cancelled repeatedly"
                                )));
                            }
                        }
                    }
                }
                Ticket::Leader(guard) => {
                    let result = self.fetch_into_residency(&cid, address).await;
                    match &result {
                        Ok(bytes) => guard.complete(Ok(Arc::clone(bytes))),
                        Err(e) => guard.complete(Err(Arc::new(clone_error(e)))),
                    }
                    return result;
                }
            }
        }
    }

    async fn fetch_into_residency(&self, cid: &ContentId, address: &str) -> Result<Arc<[u8]>> {
        if self.inner.cache_enabled {
            if let Some(bytes) = self.cache_get(cid).await {
                // Re-verify a cache hit against its CID before trusting it.
                // The network path is verified inside the proxy client, but
                // IndexedDB is not: a same-origin writer (a second app on the
                // origin, an XSS, a compromised third-party bundle) can plant
                // arbitrary bytes under a well-formed CID key, and because CAS
                // blocks are immutable and never revalidated, an unverified
                // hit would be trusted for the life of the database. This is
                // the one admission path into the engine that the proxy does
                // not already cover.
                if fluree_db_nameservice_sync::verify_object_integrity(cid, &bytes) {
                    self.inner
                        .counters
                        .cache_hits
                        .fetch_add(1, Ordering::Relaxed);
                    let resident: Arc<[u8]> = Arc::from(&bytes[..]);
                    drop(bytes);
                    return self.make_resident(cid, resident, None).await;
                }
                // Tampered or corrupt row: do not serve it. Fall through to
                // the origin fetch, which verifies and (via make_resident's
                // write-behind CachePut) overwrites the poison under the same
                // key, healing the cache. Surfaced as a counter because a
                // nonzero value is a security signal.
                self.inner
                    .counters
                    .cache_rejections
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    cid = %cid,
                    "cached object failed CID verification; discarding and refetching from origin"
                );
            }
        }

        let (bytes, permit) = {
            let _slot = self
                .inner
                .fetch_slots
                .acquire()
                .await
                .map_err(|_| CoreError::storage("browser fetch limiter is closed"))?;
            // Verified against the CID inside the proxy client.
            let bytes = self.inner.proxy.read_object_bytes(address).await?;
            // Write-behind admission happens INSIDE the fetch-slot scope:
            // when IndexedDB lags, a completed fetch parks here still
            // holding its slot, so at most `max_concurrent_fetches` blocks
            // can sit between fetch-complete and gauge admission and no
            // further fetches are admitted. No deadlock: permits are
            // released by IndexedDB writes, which never take fetch slots.
            let permit = if self.inner.cache_enabled {
                Some(self.inner.write_behind.acquire(bytes.len() as u64).await)
            } else {
                None
            };
            (bytes, permit)
        };
        self.inner.counters.fetches.fetch_add(1, Ordering::Relaxed);
        self.inner
            .counters
            .bytes_fetched
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);

        let resident: Arc<[u8]> = Arc::from(&bytes[..]);
        drop(bytes);
        self.make_resident(cid, resident, permit).await
    }

    /// Insert verified bytes into the residency tier and, for network
    /// fetches (signalled by a write-behind permit), enqueue the persist.
    /// The persist is enqueued first so a residency-budget failure never
    /// loses the bytes for the next session. A mid-flight budget overflow
    /// waits one bounded release interval for room before failing typed.
    async fn make_resident(
        &self,
        cid: &ContentId,
        bytes: Arc<[u8]>,
        permit: Option<WriteBehindPermit>,
    ) -> Result<Arc<[u8]>> {
        if let Some(permit) = permit {
            // Driver gone → nothing to persist to (the dropped permit
            // credits the gauge); not a read failure.
            let _ = self.inner.io.send(IoJob::CachePut {
                key: cid.clone(),
                bytes: Arc::clone(&bytes),
                permit: Some(permit),
            });
        }
        match self.inner.residency.insert(cid.clone(), Arc::clone(&bytes)) {
            Err(ResidencyError::EvictionDeferred { .. }) => {
                self.insert_after_release(cid, bytes).await
            }
            other => other.map_err(residency_error),
        }
    }

    /// Retry a deferred insert after a bounded wait for a release event.
    /// The bound (`budget_wait`) is the safety net against waiting while
    /// holding the only query guard.
    ///
    /// Natively, release interest is registered (`Notified::enable`)
    /// BEFORE the re-attempt — the gauge's enable-then-recheck pattern —
    /// so a release landing between the failed insert and the wait cannot
    /// be missed. On wasm the engine side has no timer, so the deadline is
    /// a `Sleep` job served by the driver (keeping this future `Send`);
    /// there is no await point between the failed insert and the wait
    /// there, so no wakeup can be lost.
    async fn insert_after_release(&self, cid: &ContentId, bytes: Arc<[u8]>) -> Result<Arc<[u8]>> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let notified = self.inner.residency.release_notify().notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.inner.residency.insert(cid.clone(), Arc::clone(&bytes)) {
                Err(ResidencyError::EvictionDeferred { .. }) => {
                    let _ = tokio::time::timeout(self.inner.budget_wait, notified).await;
                }
                other => return other.map_err(residency_error),
            }
        }
        #[cfg(target_arch = "wasm32")]
        {
            let (reply, deadline) = oneshot::channel();
            if self
                .inner
                .io
                .send(IoJob::Sleep {
                    duration: self.inner.budget_wait,
                    reply,
                })
                .is_ok()
            {
                futures::future::select(
                    Box::pin(self.inner.residency.released()),
                    Box::pin(deadline),
                )
                .await;
            }
            // Driver gone → no timer: fall through to the final attempt
            // rather than waiting unbounded.
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

    /// Participation signal for the sync residency tier: the binary-index
    /// read path serves exclusively from `resolve_cached_bytes` and records
    /// every miss here for a retry frame to drain. The fetch-pins contract
    /// holds because `read_bytes` inserts into the residency tier and
    /// eviction is frozen while the retry frame's query guard is alive.
    fn miss_register(&self) -> Option<&MissRegister> {
        Some(&self.inner.register)
    }

    /// The engine-facing form of [`BrowserCasStorage::query_guard`]: the
    /// api retry loop holds this across a query's rounds (through
    /// `StorageContentStore`), which freezes eviction so progress is
    /// monotone — the same guard the crate's own callers use, wrapped in
    /// the opaque core handle.
    fn query_guard(&self) -> Option<fluree_db_core::storage::residency::InFlightGuard> {
        Some(fluree_db_core::storage::residency::InFlightGuard::new(
            self.query_guard(),
        ))
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
    #[tokio::test]
    async fn trait_level_query_guard_freezes_the_tier_until_dropped() {
        // The api retry loop only ever sees `dyn StorageRead` — the freeze
        // must survive the opaque InFlightGuard wrapping, not just the
        // concrete QueryGuard path.
        let state = Arc::new(Mutex::new(MockState::default()));
        let (storage, io, driver) = storage_with(&state, &config());
        let dyn_storage: &dyn fluree_db_core::StorageRead = &storage;
        assert_eq!(storage.residency().queries_in_flight(), 0);
        let guard = dyn_storage
            .query_guard()
            .expect("browser storage must participate in query guarding");
        assert_eq!(storage.residency().queries_in_flight(), 1);
        drop(guard);
        assert_eq!(storage.residency().queries_in_flight(), 0);
        io.shutdown();
        driver.await.expect("driver exits");
    }

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
        /// Hold each CachePut's write-behind permit this long before
        /// releasing it (simulates a slow IndexedDB).
        pub put_hold: Option<Duration>,
        /// Never release write-behind permits (simulates a wedged
        /// IndexedDB); held permits park in `held_permits` until the test
        /// clears them.
        pub put_hold_forever: bool,
        pub held_permits: Vec<crate::gauge::WriteBehindPermit>,
        /// Scripted SSE connections: each entry is the frame chunks of one
        /// connect; an exhausted script answers `Fatal` (ends the pump).
        pub sse_script: std::collections::VecDeque<Vec<Vec<u8>>>,
        /// Every SSE connect observed: `(url, headers)`.
        pub sse_log: Vec<(String, Vec<(&'static str, String)>)>,
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
                    IoJob::Sleep { duration, reply } => {
                        tokio::spawn(async move {
                            tokio::time::sleep(duration).await;
                            let _ = reply.send(());
                        });
                    }
                    IoJob::SseOpen {
                        url,
                        headers,
                        ready,
                        chunks,
                    } => {
                        let script = {
                            let mut s = state.lock().unwrap();
                            s.sse_log.push((url, headers.0));
                            s.sse_script.pop_front()
                        };
                        match script {
                            Some(frames) => {
                                let _ = ready.send(Ok(()));
                                tokio::spawn(async move {
                                    for frame in frames {
                                        if chunks.send(Ok(Bytes::from(frame))).await.is_err() {
                                            return;
                                        }
                                    }
                                    // Dropping the sender = clean stream end.
                                });
                            }
                            None => {
                                let _ = ready.send(Err(
                                    fluree_db_nameservice_sync::SseConnectError::Fatal(
                                        "sse script exhausted".to_string(),
                                    ),
                                ));
                            }
                        }
                    }
                    IoJob::CachePut { key, bytes, permit } => {
                        let put_hold = {
                            let mut s = state.lock().unwrap();
                            s.puts.push(key.to_string());
                            s.cache.insert(key.to_string(), bytes.to_vec());
                            if s.put_hold_forever {
                                if let Some(permit) = permit {
                                    s.held_permits.push(permit);
                                }
                                continue;
                            }
                            s.put_hold
                        };
                        tokio::spawn(async move {
                            if let Some(hold) = put_hold {
                                tokio::time::sleep(hold).await;
                            }
                            drop(permit);
                        });
                    }
                    IoJob::Shutdown => break,
                }
            }
        })
    }

    pub(crate) fn object(tag: u8, len: usize) -> (ContentId, String, Vec<u8>) {
        let bytes = vec![tag; len];
        let id = ContentId::new(ContentKind::IndexLeaf, &bytes);
        let address = fluree_db_core::content_address(
            "proxy",
            ContentKind::IndexLeaf,
            LEDGER,
            &id.digest_hex(),
        );
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

        let got = storage
            .read_bytes(&address)
            .await
            .expect("first read fetches");
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

    /// A poisoned IndexedDB row — attacker bytes stored under a VALID CID key
    /// — must not be served. The persistent cache is the one admission path
    /// the proxy client does not verify, and CAS blocks are immutable and
    /// never revalidated, so an unverified hit would be trusted forever. The
    /// read must reject the poison, refetch from origin (which verifies), and
    /// heal the cache by overwriting the bad row.
    #[tokio::test]
    async fn poisoned_cache_row_is_reverified_refetched_and_healed() {
        let (id, address, good) = object(3, 16);
        let state = Arc::new(Mutex::new(MockState::default()));
        {
            let mut s = state.lock().unwrap();
            // Cache holds attacker bytes under the valid CID key...
            s.cache
                .insert(id.to_string(), b"poisoned attacker bytes!!".to_vec());
            // ...while the origin holds the real, CID-matching bytes.
            s.objects.insert(id.to_string(), (200, good.clone()));
        }
        let (storage, io, driver) = storage_with(&state, &config());

        // The read returns the GOOD bytes, not the poison.
        let got = storage
            .read_bytes(&address)
            .await
            .expect("heals via origin");
        assert_eq!(
            got, good,
            "must serve verified origin bytes, not the poison"
        );

        // The poison was rejected and the origin was hit (fall-through).
        assert_eq!(
            state.lock().unwrap().fetch_log,
            vec![id.to_string()],
            "a rejected cache hit must fall through to a network fetch"
        );
        let stats = storage.stats();
        assert_eq!(stats.cache_rejections, 1, "the poison was counted");
        assert_eq!(stats.cache_hits, 0, "a rejected row is not a hit");
        assert_eq!(stats.fetches, 1);

        // Write-behind overwrote the poison under the same key with the
        // verified bytes: the cache is healed.
        io.shutdown();
        driver.await.unwrap();
        let s = state.lock().unwrap();
        assert_eq!(s.puts, vec![id.to_string()]);
        assert_eq!(
            s.cache.get(&id.to_string()).unwrap(),
            &good,
            "the poisoned row was overwritten with verified bytes"
        );
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
        assert!(storage
            .read_byte_range(&address, 40..50)
            .await
            .unwrap()
            .is_empty());
        assert!(storage
            .read_byte_range(&address, 5..5)
            .await
            .unwrap()
            .is_empty());
        let tail = storage
            .read_byte_range(&address, 30..u64::MAX)
            .await
            .unwrap();
        assert_eq!(tail, bytes[30..].to_vec());
        assert_eq!(state.lock().unwrap().fetch_log.len(), 1, "one fetch total");
        assert!(storage.exists(&address).await.unwrap());
        io.shutdown();
        driver.await.unwrap();
    }

    #[tokio::test]
    async fn write_behind_backpressure_bounds_queued_bytes() {
        let objects: Vec<_> = (30u8..34).map(|t| object(t, 16)).collect();
        let state = Arc::new(Mutex::new(MockState::default()));
        {
            let mut s = state.lock().unwrap();
            for (id, _, bytes) in &objects {
                s.objects.insert(id.to_string(), (200, bytes.clone()));
            }
            s.put_hold = Some(Duration::from_millis(20));
        }
        let cfg = BrowserIoConfig {
            // One 16-byte block at a time may sit un-persisted.
            write_behind_budget_bytes: 16,
            ..config()
        };
        let (storage, io, driver) = storage_with(&state, &cfg);

        let failures = storage
            .prefetch(objects.iter().map(|(_, a, _)| a.clone()))
            .await;
        assert!(failures.is_empty(), "{failures:?}");
        let stats = storage.stats();
        assert!(
            stats.write_behind_peak <= 16,
            "write-behind queue must stay within budget, peak {}",
            stats.write_behind_peak
        );
        for (id, _, _) in &objects {
            assert!(storage.resolve_cached_bytes(id).is_some());
        }
        // Let the held permits drain, then confirm every block persisted.
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(state.lock().unwrap().puts.len(), 4);
        assert_eq!(storage.stats().write_behind_outstanding, 0);
        io.shutdown();
        driver.await.unwrap();
    }

    #[tokio::test]
    async fn deferred_insert_waits_for_a_query_release_then_fails_typed() {
        let a = object(40, 16);
        let b = object(41, 16);
        let c = object(42, 16);
        let d = object(43, 16);
        let state = Arc::new(Mutex::new(MockState::default()));
        {
            let mut s = state.lock().unwrap();
            for (id, _, bytes) in [&a, &b, &c, &d] {
                s.objects.insert(id.to_string(), (200, bytes.clone()));
            }
        }
        let cfg = BrowserIoConfig {
            residency_budget_bytes: 32,
            budget_wait: Duration::from_millis(60),
            ..config()
        };
        let (storage, io, driver) = storage_with(&state, &cfg);

        // Everything resident was fetched UNDER the live guard (so it is
        // part of the query's epoch and cannot be evicted); a third block
        // then defers — but the guard drops shortly, so the deferred
        // insert's bounded wait succeeds on the release.
        let guard = storage.query_guard();
        storage.ensure_resident(&a.1).await.unwrap();
        storage.ensure_resident(&b.1).await.unwrap();
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(25)).await;
            drop(guard);
        });
        let resident = storage
            .ensure_resident(&c.1)
            .await
            .expect("waits then evicts");
        assert_eq!(&resident[..], &c.2[..]);
        release.await.unwrap();

        // With a guard held longer than budget_wait and the whole resident
        // set observed under it, the deferred insert fails typed instead of
        // evicting or hanging.
        let _held = storage.query_guard();
        for id in [&b.0, &c.0] {
            // Observation marks the entry as part of the live epoch (only
            // one of a/b survived phase 1; resolving both is harmless).
            let _ = storage.resolve_cached_bytes(id);
        }
        let _ = storage.resolve_cached_bytes(&a.0);
        let err = storage.ensure_resident(&d.1).await.expect_err("deferred");
        assert!(err.to_string().contains("eviction deferred"), "got: {err}");
        io.shutdown();
        driver.await.unwrap();
    }

    /// H-1 regression at the storage level: a tier filled to budget by a
    /// FINISHED query must serve the next query's cold fetch immediately —
    /// no budget_wait stall, no typed failure — by shedding the previous
    /// query's leftovers.
    #[tokio::test]
    async fn next_querys_cold_fetch_succeeds_after_a_full_query_finishes() {
        let a = object(45, 16);
        let b = object(46, 16);
        let c = object(47, 16);
        let state = Arc::new(Mutex::new(MockState::default()));
        {
            let mut s = state.lock().unwrap();
            for (id, _, bytes) in [&a, &b, &c] {
                s.objects.insert(id.to_string(), (200, bytes.clone()));
            }
        }
        let cfg = BrowserIoConfig {
            residency_budget_bytes: 32,
            // Deliberately long: the test proves the cold fetch does NOT
            // wait it out.
            budget_wait: Duration::from_secs(10),
            ..config()
        };
        let (storage, io, driver) = storage_with(&state, &cfg);

        // Query 1 fills the tier to budget and finishes.
        {
            let _guard1 = storage.query_guard();
            storage.ensure_resident(&a.1).await.unwrap();
            storage.ensure_resident(&b.1).await.unwrap();
        }

        // Query 2's cold fetch succeeds promptly by evicting leftovers.
        let _guard2 = storage.query_guard();
        let started = std::time::Instant::now();
        let resident = storage.ensure_resident(&c.1).await.expect("no brick");
        assert_eq!(&resident[..], &c.2[..]);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "cold fetch must not stall on budget_wait, took {:?}",
            started.elapsed()
        );
        assert!(storage.resolve_cached_bytes(&c.0).is_some());
        io.shutdown();
        driver.await.unwrap();
    }

    /// H-2 regression: a stalled IndexedDB write-behind must throttle
    /// FETCH ADMISSION — the gauge permit is acquired inside the fetch-slot
    /// scope, so parked completions hold their slots and the batch paths
    /// are width-bounded.
    #[tokio::test]
    async fn stalled_persist_throttles_fetch_admission() {
        let objects: Vec<_> = (60u8..64).map(|t| object(t, 16)).collect();
        let state = Arc::new(Mutex::new(MockState::default()));
        {
            let mut s = state.lock().unwrap();
            for (id, _, bytes) in &objects {
                s.objects.insert(id.to_string(), (200, bytes.clone()));
            }
            s.put_hold_forever = true;
        }
        let cfg = BrowserIoConfig {
            max_concurrent_fetches: 2,
            // One 16-byte block may sit un-persisted at a time.
            write_behind_budget_bytes: 16,
            ..config()
        };
        let (storage, io, driver) = storage_with(&state, &cfg);

        let prefetch = {
            let storage = storage.clone();
            let addresses: Vec<String> = objects.iter().map(|(_, a, _)| a.clone()).collect();
            tokio::spawn(async move { storage.prefetch(addresses).await })
        };
        tokio::time::sleep(Duration::from_millis(80)).await;

        assert!(!prefetch.is_finished(), "prefetch must stall on the gauge");
        {
            let s = state.lock().unwrap();
            assert_eq!(
                s.fetch_log.len(),
                3,
                "1 admitted + 2 parked holding their fetch slots; the 4th \
                 fetch must NOT be admitted"
            );
        }
        let stats = storage.stats();
        assert_eq!(stats.write_behind_peak, 16, "un-persisted bytes bounded");
        assert_eq!(stats.write_behind_outstanding, 16);

        // Unstick the persist path; everything drains.
        {
            let mut s = state.lock().unwrap();
            s.put_hold_forever = false;
            s.held_permits.clear();
        }
        let failures = tokio::time::timeout(Duration::from_secs(2), prefetch)
            .await
            .expect("prefetch completes once persists drain")
            .unwrap();
        assert!(failures.is_empty(), "{failures:?}");
        assert_eq!(state.lock().unwrap().fetch_log.len(), 4);
        assert_eq!(storage.stats().write_behind_outstanding, 0);
        io.shutdown();
        driver.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_cids_makes_wants_resident_and_pins_them() {
        let objects: Vec<_> = (50u8..53).map(|t| object(t, 8)).collect();
        let missing = object(60, 8); // never served by the mock → 404
        let state = Arc::new(Mutex::new(MockState::default()));
        {
            let mut s = state.lock().unwrap();
            for (id, _, bytes) in &objects {
                s.objects.insert(id.to_string(), (200, bytes.clone()));
            }
        }
        let (storage, io, driver) = storage_with(&state, &config());

        let pins = storage.pin_set();
        let wants: Vec<ContentId> = objects
            .iter()
            .map(|(id, _, _)| id.clone())
            .chain([missing.0.clone()])
            .collect();
        let failures = storage.fetch_cids_pinned(LEDGER, wants, &pins).await;
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].0, missing.0);
        assert!(matches!(failures[0].1, CoreError::NotFound(_)));
        assert_eq!(pins.len(), 3, "every fetched want is pinned");
        for (id, _, bytes) in &objects {
            assert!(pins.contains(id));
            assert_eq!(&storage.resolve_cached_bytes(id).unwrap()[..], &bytes[..]);
        }
        // The formed addresses went through the shared proxy client.
        assert_eq!(state.lock().unwrap().fetch_log.len(), 4);
        io.shutdown();
        driver.await.unwrap();
    }

    /// The load-bearing integration: the engine's LANDED retry primitives
    /// drive this storage through the exact bridge the engine assembles
    /// (`content_store_for` → `StorageContentStore`). A sync miss records
    /// into this storage's register through the bridge; `RetryBudget::
    /// after_error` drains it, fetches through `ContentStore::get` (our
    /// `read_bytes`), verifies residency (fetch-pins contract), and the
    /// re-run hits. Positive markers throughout — no vacuous passes.
    #[tokio::test]
    async fn landed_retry_primitives_drive_this_storage_through_the_engine_bridge() {
        use fluree_db_binary_index::read::need_fetch::RetryBudget;
        use fluree_db_core::storage::residency::{resident_or_need_fetch, FetchKind, NeedFetch};

        let (id, _address, bytes) = object(70, 24);
        let state = Arc::new(Mutex::new(MockState::default()));
        state
            .lock()
            .unwrap()
            .objects
            .insert(id.to_string(), (200, bytes.clone()));
        let (storage, io, driver) = storage_with(&state, &config());
        let bridge = fluree_db_core::storage::content_store_for(storage.clone(), LEDGER);

        // Retry frames hold a query guard; eviction stays frozen throughout.
        let _guard = storage.query_guard();

        // Sync read misses: typed error AND a recorded want, through the bridge.
        let err = resident_or_need_fetch(&bridge, &id, FetchKind::IndexLeaf)
            .expect_err("cold tier must miss");
        let nf = NeedFetch::from_io_error(&err).expect("typed NeedFetch payload");
        assert_eq!(nf.cid, id);
        assert_eq!(storage.miss_register().unwrap().len(), 1, "want recorded");

        // The landed drain/fetch/verify round reports progress.
        let mut budget = RetryBudget::default();
        let rerun = budget
            .after_error(&bridge, 4)
            .await
            .expect("round must make progress");
        assert!(rerun, "wants were fetched — re-run the unit");
        assert_eq!(budget.fetched_total(), 1);
        assert!(storage.miss_register().unwrap().is_empty(), "drained");

        // Re-run hits the resident tier — zero-copy, no new fetch.
        let hit = resident_or_need_fetch(&bridge, &id, FetchKind::IndexLeaf)
            .expect("fetch-pins contract: fetched bytes are resident");
        assert_eq!(&hit[..], &bytes[..]);
        assert_eq!(state.lock().unwrap().fetch_log.len(), 1, "one fetch total");

        // A real (non-miss) error leaves the loop alone: empty register → false.
        let no_rerun = budget.after_error(&bridge, 4).await.expect("no wants");
        assert!(!no_rerun, "empty register means the error was real");

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
        assert!(
            storage.resolve_cached_bytes(&a.0).is_some(),
            "pinned survives"
        );
        assert!(
            storage.resolve_cached_bytes(&b.0).is_none(),
            "LRU unpinned evicted"
        );
        assert!(storage.resolve_cached_bytes(&c.0).is_some());

        // Pin c too: the working set (80) fills the budget, so a further
        // insert is a typed working-set failure — not silent thrashing.
        assert!(pins.pin(&c.0));
        let err = storage
            .ensure_resident(&b.1)
            .await
            .expect_err("over budget");
        assert!(err.to_string().contains("working set exceeds"), "got {err}");

        drop(pins);
        storage.ensure_resident(&b.1).await.unwrap();
        io.shutdown();
        driver.await.unwrap();
    }
}
