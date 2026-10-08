use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use crate::{ContentBytes, ContentId, ContentStore};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::sync::broadcast;

const CACHE_BUDGET_NUMERATOR: u64 = 9;
const CACHE_BUDGET_DENOMINATOR: u64 = 10;
const CACHE_EVICT_NUMERATOR: u64 = 8;
const CACHE_EVICT_DENOMINATOR: u64 = 10;
const DEFAULT_LAMBDA_TMP_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_LAMBDA_TMP_WARN_SLACK_BYTES: u64 = 64 * 1024 * 1024;

static CACHE_REGISTRY: Lazy<Mutex<HashMap<PathBuf, Weak<DiskArtifactCache>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Sentinel for "no configured budget" — fall back to auto-detect.
const BUDGET_UNSET: u64 = u64::MAX;

/// Process-global default disk-cache budget in bytes, set from configuration
/// (e.g. the server's `disk_cache_max_mb`). `BUDGET_UNSET` means auto-detect.
static CONFIGURED_BUDGET_BYTES: AtomicU64 = AtomicU64::new(BUDGET_UNSET);

/// Set the process-global default disk-cache budget (bytes), shared across every
/// on-disk cache (Fluree object storage + Iceberg). Applies to caches created
/// after this call whose `FLUREE_DISK_CACHE_BUDGET_BYTES` env override is unset;
/// `0` disables disk caching. Call before the first cache is created (e.g. at
/// server/builder startup). Mirrors the `disk_cache_max_mb` config knob.
pub fn set_configured_budget_bytes(bytes: u64) {
    CONFIGURED_BUDGET_BYTES.store(bytes, Ordering::Relaxed);
}

/// The configured budget if set, otherwise a fraction of available disk space.
fn configured_or_auto(available: u64) -> u64 {
    match CONFIGURED_BUDGET_BYTES.load(Ordering::Relaxed) {
        BUDGET_UNSET => available
            .saturating_mul(CACHE_BUDGET_NUMERATOR)
            .saturating_div(CACHE_BUDGET_DENOMINATOR),
        bytes => bytes,
    }
}

/// Shared outcome of one in-flight remote fetch. Bytes are shared via `Arc` so
/// coalesced waiters neither re-fetch nor re-allocate the payload; the per-caller
/// `Vec` copy happens only at the API boundary. Errors are shared but never
/// cached — the in-flight entry is removed on completion so the next caller
/// retries (see [`Flights::run`]).
type FlightResult = std::result::Result<Arc<[u8]>, Arc<io::Error>>;

/// A single in-flight fetch that concurrent callers for the same key can wait
/// on instead of issuing their own remote read.
#[derive(Debug)]
struct Flight {
    /// Generation token guarding removal against ABA: a stale guard (from a
    /// cancelled leader) must not evict a newer flight started for the same
    /// key by a different leader.
    generation: u64,
    /// Broadcast handle waiters `subscribe()` to; the leader sends exactly once.
    tx: broadcast::Sender<FlightResult>,
}

/// Single-flight coordination: concurrent callers with one key share one fetch
/// and its outcome, errors included. A key must therefore name everything the
/// outcome depends on, or one caller's failure answers another's read.
///
/// This is process-local (it does not coordinate across containers).
///
/// - the map lock is never held across `.await`;
/// - the slot is cleared on completion *and* on drop, so a cancelled or
///   panicked leader cannot orphan it (waiters then observe a closed channel
///   and retry rather than hang);
/// - errors are propagated to current waiters but never cached — the slot is
///   gone by then, so the next caller retries.
#[derive(Debug)]
pub struct Flights<K> {
    inflight: Mutex<HashMap<K, Flight>>,
    next_generation: AtomicU64,
}

impl<K> Default for Flights<K> {
    fn default() -> Self {
        Self {
            inflight: Mutex::new(HashMap::new()),
            next_generation: AtomicU64::new(0),
        }
    }
}

impl<K: Clone + Eq + std::hash::Hash> Flights<K> {
    /// Run `fetch`, or, while a flight for `key` is already running, wait for
    /// its outcome instead.
    pub async fn run<F, Fut>(self: &Arc<Self>, key: K, fetch: F) -> io::Result<Vec<u8>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = io::Result<Vec<u8>>>,
    {
        loop {
            // Decide leader vs waiter under the lock; release it before awaiting.
            let role = {
                let mut map = self.inflight.lock();
                match map.get(&key) {
                    Some(flight) => FlightRole::Waiter(flight.tx.subscribe()),
                    None => {
                        let (tx, _rx) = broadcast::channel(1);
                        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
                        map.insert(
                            key.clone(),
                            Flight {
                                generation,
                                tx: tx.clone(),
                            },
                        );
                        FlightRole::Leader { generation, tx }
                    }
                }
            };

            match role {
                FlightRole::Waiter(mut rx) => match rx.recv().await {
                    Ok(Ok(bytes)) => return Ok(bytes.to_vec()),
                    Ok(Err(err)) => return Err(io::Error::new(err.kind(), err.to_string())),
                    // Leader finished without publishing (cancelled/panicked).
                    // Its guard has cleared the slot, so retry as a fresh caller
                    // rather than wait on a result that will never arrive.
                    Err(_) => continue,
                },
                FlightRole::Leader { generation, tx } => {
                    // Clears the slot on completion or on early return /
                    // cancellation (drop). Generation-checked, so it never
                    // evicts a newer flight for the same key.
                    let guard = FlightGuard {
                        flights: Arc::clone(self),
                        key: key.clone(),
                        generation,
                    };
                    let outcome = fetch().await;

                    // Clear the slot before waking waiters so callers arriving
                    // after this point start a fresh flight (and hit whatever
                    // the leader wrote) instead of subscribing to a finished one.
                    drop(guard);

                    // Wake waiters that subscribed before removal. Skip the
                    // shared allocation entirely when nobody is waiting.
                    if tx.receiver_count() > 0 {
                        let payload: FlightResult = match &outcome {
                            Ok(bytes) => Ok(Arc::from(bytes.as_slice())),
                            Err(err) => Err(Arc::new(io::Error::new(err.kind(), err.to_string()))),
                        };
                        let _ = tx.send(payload);
                    }
                    return outcome;
                }
            }
        }
    }

    /// Remove the in-flight entry for `key` iff it is still the flight with
    /// `generation`. The generation check keeps removal ABA-safe: a stale guard
    /// from a cancelled leader must not evict a newer flight a different leader
    /// started for the same key.
    fn finish(&self, key: &K, generation: u64) {
        let mut map = self.inflight.lock();
        if map.get(key).is_some_and(|f| f.generation == generation) {
            map.remove(key);
        }
    }
}

/// Whether this caller leads the flight (does the fetch) or waits on a leader.
enum FlightRole {
    Leader {
        generation: u64,
        tx: broadcast::Sender<FlightResult>,
    },
    Waiter(broadcast::Receiver<FlightResult>),
}

/// RAII guard that clears a leader's in-flight slot on completion or on drop,
/// so a cancelled or panicked leader cannot orphan the slot (which would wedge
/// every later waiter on it). Removal is generation-checked, so it never evicts
/// a newer flight for the same key.
struct FlightGuard<K: Clone + Eq + std::hash::Hash> {
    flights: Arc<Flights<K>>,
    key: K,
    generation: u64,
}

impl<K: Clone + Eq + std::hash::Hash> Drop for FlightGuard<K> {
    fn drop(&mut self) {
        self.flights.finish(&self.key, self.generation);
    }
}

#[derive(Debug)]
pub struct DiskArtifactCache {
    root: PathBuf,
    budget_bytes: u64,
    /// Shared with the background scan that sizes the directory.
    state: Arc<Mutex<DiskArtifactCacheState>>,
    /// Coalesces concurrent fetches for one cache target into one remote read
    /// and one tmp-file write, for [`Self::coalesced_fetch`].
    flights: Arc<Flights<PathBuf>>,
}

#[derive(Debug, Default)]
struct DiskArtifactCacheState {
    tracked_bytes: Option<u64>,
    /// A background scan is sizing the directory; see
    /// [`DiskArtifactCache::known_bytes`].
    scanning: bool,
    /// Entries written (positive) and evicted (negative) while that scan
    /// runs, reconciled with what it saw.
    pending: Vec<(PathBuf, i64)>,
    /// Test hook: the background scan waits for this before walking.
    #[cfg(test)]
    scan_hold: Option<std::sync::mpsc::Receiver<()>>,
}

#[derive(Debug)]
struct CacheEntry {
    path: PathBuf,
    bytes: u64,
    modified: std::time::SystemTime,
}

fn storage_to_io_error(e: crate::error::Error) -> io::Error {
    let kind = match &e {
        crate::error::Error::NotFound(_) => io::ErrorKind::NotFound,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, e.to_string())
}

fn is_cache_temp_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with(".cas_") && name.ends_with(".tmp"))
}

fn is_disk_full(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::StorageFull || err.raw_os_error() == Some(28)
}

pub fn try_read_cached_bytes(path: &Path) -> io::Result<Option<Vec<u8>>> {
    read_result_as_cache_outcome(fs::read(path))
}

/// `NotFound` — and `Unsupported`, which is what every `std::fs` call returns
/// on wasm32-unknown-unknown — are cache MISSES that must fall through to the
/// authoritative CAS fetch, not errors: a coalesced fetch's leader checks the
/// entry through this before fetching, and anything mapped to `Err` here
/// would fail the read outright.
fn read_result_as_cache_outcome(res: io::Result<Vec<u8>>) -> io::Result<Option<Vec<u8>>> {
    match res {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err)
            if matches!(
                err.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::Unsupported
            ) =>
        {
            Ok(None)
        }
        Err(err) => Err(err),
    }
}

/// Create the disk-cache directory, treating "this platform has no
/// filesystem" as success.
///
/// Loaders call this once before reading through the cache. On
/// wasm32-unknown-unknown `create_dir_all` returns `Unsupported`, and failing
/// there would abort the whole load before a single CAS fetch was attempted —
/// even though every subsequent cache read already degrades to a miss
/// ([`try_read_cached_bytes`]) and every cache write is already suppressed
/// (`available_space` reports 0). Real filesystem failures — permissions, a
/// full disk, a path that is a file — still surface.
pub fn ensure_cache_dir(dir: &Path) -> io::Result<()> {
    create_dir_result_as_cache_outcome(fs::create_dir_all(dir))
}

fn create_dir_result_as_cache_outcome(res: io::Result<()>) -> io::Result<()> {
    match res {
        Err(err) if err.kind() == io::ErrorKind::Unsupported => Ok(()),
        other => other,
    }
}

#[cfg(test)]
fn directory_bytes(root: &Path) -> io::Result<u64> {
    Ok(scan_cache_entries(root)?
        .into_iter()
        .fold(0u64, |acc, entry| acc.saturating_add(entry.bytes)))
}

fn scan_cache_entries(root: &Path) -> io::Result<Vec<CacheEntry>> {
    let mut stack = vec![root.to_path_buf()];
    let mut entries = Vec::new();

    while let Some(dir) = stack.pop() {
        let read_dir = match fs::read_dir(&dir) {
            Ok(read_dir) => read_dir,
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };

        for child in read_dir {
            let child = child?;
            let path = child.path();
            let file_type = child.file_type()?;
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if !file_type.is_file() || is_cache_temp_file(&path) {
                continue;
            }

            let meta = child.metadata()?;
            entries.push(CacheEntry {
                path,
                bytes: meta.len(),
                modified: meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH),
            });
        }
    }

    Ok(entries)
}

impl DiskArtifactCache {
    pub fn for_dir(cache_dir: &Path) -> Arc<Self> {
        let root = cache_dir.to_path_buf();
        let mut registry = CACHE_REGISTRY.lock();
        if let Some(existing) = registry.get(&root).and_then(Weak::upgrade) {
            return existing;
        }

        let cache = Arc::new(Self::new(root.clone()));
        registry.insert(root, Arc::downgrade(&cache));
        cache
    }

    fn new(root: PathBuf) -> Self {
        if let Err(err) = fs::create_dir_all(&root) {
            tracing::warn!(
                cache_dir = %root.display(),
                error = %err,
                "failed to create disk artifact cache directory; cache writes disabled"
            );
            return Self {
                root,
                budget_bytes: 0,
                state: Arc::new(Mutex::new(DiskArtifactCacheState::default())),
                flights: Arc::default(),
            };
        }

        // No filesystem on wasm32: budget 0 disables cache writes, reads miss.
        #[cfg(target_arch = "wasm32")]
        let available: u64 = 0;
        #[cfg(not(target_arch = "wasm32"))]
        let available = fs2::available_space(&root).unwrap_or_else(|err| {
            tracing::warn!(
                cache_dir = %root.display(),
                error = %err,
                "failed to inspect available disk space; disk cache writes disabled"
            );
            0
        });
        let budget_bytes = match std::env::var("FLUREE_DISK_CACHE_BUDGET_BYTES") {
            Ok(val) => match val.parse::<u64>() {
                Ok(0) => {
                    tracing::debug!(
                        cache_dir = %root.display(),
                        "FLUREE_DISK_CACHE_BUDGET_BYTES=0; disk cache writes disabled"
                    );
                    0
                }
                Ok(bytes) => {
                    tracing::trace!(
                        cache_dir = %root.display(),
                        budget_bytes = bytes,
                        "using FLUREE_DISK_CACHE_BUDGET_BYTES override"
                    );
                    bytes
                }
                Err(err) => {
                    tracing::warn!(
                        cache_dir = %root.display(),
                        value = %val,
                        error = %err,
                        "invalid FLUREE_DISK_CACHE_BUDGET_BYTES; falling back to configured/auto"
                    );
                    configured_or_auto(available)
                }
            },
            Err(_) => configured_or_auto(available),
        };

        if available > 0
            && available
                <= DEFAULT_LAMBDA_TMP_BYTES.saturating_add(DEFAULT_LAMBDA_TMP_WARN_SLACK_BYTES)
        {
            tracing::warn!(
                cache_dir = %root.display(),
                available_tmp_bytes = available,
                cache_budget_bytes = budget_bytes,
                "disk cache is using near-default ephemeral storage; consider increasing Lambda /tmp"
            );
        }

        Self {
            root,
            budget_bytes,
            state: Arc::new(Mutex::new(DiskArtifactCacheState::default())),
            flights: Arc::default(),
        }
    }

    #[cfg(test)]
    fn with_budget(root: PathBuf, budget_bytes: u64) -> Self {
        fs::create_dir_all(&root).expect("create test cache dir");
        Self {
            root,
            budget_bytes,
            state: Arc::new(Mutex::new(DiskArtifactCacheState::default())),
            flights: Arc::default(),
        }
    }

    /// The directory this cache keeps its entries in.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The disk byte budget. `0` means disk caching is disabled (writes are
    /// skipped), so callers can avoid pointless remote fetches into a dead cache.
    pub fn budget_bytes(&self) -> u64 {
        self.budget_bytes
    }

    fn low_water_mark(&self) -> u64 {
        self.budget_bytes
            .saturating_mul(CACHE_EVICT_NUMERATOR)
            .saturating_div(CACHE_EVICT_DENOMINATOR)
    }

    /// The directory's size, scanning it now if it is not yet known.
    #[cfg(test)]
    fn current_bytes(&self) -> io::Result<u64> {
        if let Some(bytes) = self.state.lock().tracked_bytes {
            return Ok(bytes);
        }
        let bytes = directory_bytes(&self.root)?;
        self.set_current_bytes(bytes);
        Ok(bytes)
    }

    /// The directory's size if it is known. If not, starts sizing it on a
    /// background thread and returns `None`.
    ///
    /// Walking a large shared cache takes seconds; done inline, under the
    /// state lock, it stalls the async worker that writes first and every
    /// writer queued on the lock. Writes made meanwhile skip the budget check
    /// and are counted into the scan's total when it lands.
    fn known_bytes(&self) -> Option<u64> {
        let mut state = self.state.lock();
        if state.tracked_bytes.is_some() || state.scanning {
            return state.tracked_bytes;
        }
        state.scanning = true;
        state.pending.clear();
        #[cfg(test)]
        let hold = state.scan_hold.take();
        drop(state);

        let root = self.root.clone();
        let shared = Arc::clone(&self.state);
        let scan = move || {
            #[cfg(test)]
            if let Some(hold) = hold {
                let _ = hold.recv();
            }
            let scanned = scan_cache_entries(&root);
            let mut state = shared.lock();
            match scanned {
                // A synchronous scan or eviction that set the size meanwhile
                // is more recent than this one.
                Ok(entries) if state.tracked_bytes.is_none() => {
                    let seen: std::collections::HashSet<&Path> =
                        entries.iter().map(|e| e.path.as_path()).collect();
                    let mut total: i64 = entries.iter().map(|e| e.bytes as i64).sum();
                    // A write the walk already saw is in `total`; an eviction
                    // counts only against an entry it saw.
                    for (path, delta) in &state.pending {
                        if (*delta > 0) != seen.contains(path.as_path()) {
                            total = total.saturating_add(*delta);
                        }
                    }
                    state.tracked_bytes = Some(total.max(0) as u64);
                }
                Ok(_) => {}
                Err(err) => tracing::debug!(
                    cache_dir = %root.display(),
                    error = %err,
                    "failed to size the disk cache; the next write retries"
                ),
            }
            state.scanning = false;
            state.pending.clear();
        };
        // No threads on wasm32, where the cache never writes (budget 0).
        #[cfg(target_arch = "wasm32")]
        scan();
        #[cfg(not(target_arch = "wasm32"))]
        if let Err(err) = std::thread::Builder::new()
            .name("fluree-cache-size".into())
            .spawn(scan)
        {
            tracing::debug!(error = %err, "failed to start the disk cache size scan");
            self.state.lock().scanning = false;
        }
        None
    }

    fn set_current_bytes(&self, bytes: u64) {
        let mut state = self.state.lock();
        state.tracked_bytes = Some(bytes);
        state.pending.clear();
    }

    fn note_write(&self, path: &Path, bytes: u64) {
        let mut state = self.state.lock();
        match state.tracked_bytes {
            Some(current) => state.tracked_bytes = Some(current.saturating_add(bytes)),
            None if state.scanning => state.pending.push((path.to_path_buf(), bytes as i64)),
            // Untracked: the next capacity check sizes the directory, this
            // write included.
            None => {}
        }
    }

    /// Drop one entry, keeping the byte accounting in step.
    ///
    /// An absent entry is the common case — most released CIDs were never
    /// cached — and is not an error. Untracked totals stay untracked so the
    /// next capacity check rescans rather than trusting a partial figure.
    pub(crate) fn evict_entry(&self, path: &Path) {
        let Ok(metadata) = fs::metadata(path) else {
            return;
        };
        let bytes = metadata.len();

        match fs::remove_file(path) {
            Ok(()) => {
                let mut state = self.state.lock();
                if let Some(tracked) = state.tracked_bytes {
                    state.tracked_bytes = Some(tracked.saturating_sub(bytes));
                } else if state.scanning {
                    state.pending.push((path.to_path_buf(), -(bytes as i64)));
                }
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => tracing::debug!(
                cache_dir = %self.root.display(),
                path = %path.display(),
                error = %err,
                "failed to evict cache entry for a released object"
            ),
        }
    }

    /// Remove entries, oldest first by mtime, until the directory holds no
    /// more than `target_bytes`.
    ///
    /// Reads never touch mtime ([`try_read_cached_bytes`] is a plain
    /// `fs::read`), so this is write order, not access order: the entries
    /// that go first are the ones resident longest, however often they are
    /// read. A bulk writer sharing the directory with the read path — the
    /// index sweep walks and caches every root in a chain — can push out hot
    /// leaves once the budget is reached.
    fn evict_until(&self, target_bytes: u64) -> io::Result<()> {
        let mut entries = scan_cache_entries(&self.root)?;
        let mut current = entries
            .iter()
            .fold(0u64, |acc, entry| acc.saturating_add(entry.bytes));
        if current <= target_bytes {
            self.set_current_bytes(current);
            return Ok(());
        }

        entries.sort_by_key(|entry| entry.modified);
        for entry in entries {
            if current <= target_bytes {
                break;
            }
            match fs::remove_file(&entry.path) {
                Ok(()) => {
                    current = current.saturating_sub(entry.bytes);
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    current = current.saturating_sub(entry.bytes);
                }
                Err(err) => {
                    tracing::debug!(
                        cache_dir = %self.root.display(),
                        path = %entry.path.display(),
                        error = %err,
                        "failed to evict cache file"
                    );
                }
            }
        }

        self.set_current_bytes(current);
        Ok(())
    }

    fn ensure_capacity(&self, incoming_bytes: u64) -> io::Result<()> {
        if self.budget_bytes == 0 {
            return Ok(());
        }

        let Some(current) = self.known_bytes() else {
            return Ok(());
        };
        if current.saturating_add(incoming_bytes) <= self.budget_bytes {
            return Ok(());
        }

        let target = self
            .low_water_mark()
            .min(self.budget_bytes.saturating_sub(incoming_bytes));
        self.evict_until(target)
    }

    fn write_atomic(target: &Path, bytes: &[u8]) -> io::Result<bool> {
        if target.exists() {
            return Ok(false);
        }

        let parent = target
            .parent()
            .ok_or_else(|| io::Error::other("cache target has no parent dir"))?;
        fs::create_dir_all(parent)?;

        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp = parent.join(format!(".cas_{}_{}.tmp", std::process::id(), seq));
        fs::write(&tmp, bytes)?;
        if let Err(_rename_err) = fs::rename(&tmp, target) {
            let _ = fs::remove_file(&tmp);
            if !target.exists() {
                return Err(io::Error::other(format!(
                    "failed to cache bytes to {target:?}"
                )));
            }
            return Ok(false);
        }
        Ok(true)
    }

    pub fn best_effort_write(&self, target: &Path, bytes: &[u8]) {
        if self.budget_bytes == 0 {
            return;
        }

        if let Err(err) = self.ensure_capacity(bytes.len() as u64) {
            tracing::warn!(
                cache_dir = %self.root.display(),
                error = %err,
                "failed to enforce disk cache budget; skipping cache write"
            );
            return;
        }

        match Self::write_atomic(target, bytes) {
            Ok(true) => self.note_write(target, bytes.len() as u64),
            Ok(false) => {}
            Err(err) if is_disk_full(&err) => {
                if let Err(evict_err) = self.evict_until(self.low_water_mark()) {
                    tracing::warn!(
                        cache_dir = %self.root.display(),
                        error = %evict_err,
                        "failed to evict cache files after disk-full error"
                    );
                    return;
                }
                match Self::write_atomic(target, bytes) {
                    Ok(true) => self.note_write(target, bytes.len() as u64),
                    Ok(false) => {}
                    Err(retry_err) => tracing::warn!(
                        cache_dir = %self.root.display(),
                        target = %target.display(),
                        error = %retry_err,
                        "disk cache write failed after eviction; continuing without cache"
                    ),
                }
            }
            Err(err) => tracing::warn!(
                cache_dir = %self.root.display(),
                target = %target.display(),
                error = %err,
                "disk cache write failed; continuing without cache"
            ),
        }
    }

    /// Coalesce concurrent remote fetches that target the same cache path so
    /// only ONE `fetch` runs per `target` at a time; other callers await the
    /// shared result instead of each issuing their own S3 GET and tmp-file
    /// write (see [`Flights`]).
    ///
    /// `fetch` is the leader's remote read (e.g. `cs.get(id)` mapped to io).
    /// Only the leader runs it. After winning the flight the leader double-
    /// checks disk (a just-finished prior flight may have written the file),
    /// then on a miss fetches once, writes the cache atomically, and wakes
    /// waiters with the shared bytes.
    pub async fn coalesced_fetch<F, Fut>(
        self: &Arc<Self>,
        target: PathBuf,
        fetch: F,
    ) -> io::Result<Vec<u8>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = io::Result<Vec<u8>>>,
    {
        self.flights
            .run(target.clone(), || self.fill(&target, fetch))
            .await
    }

    /// The leader's half of a coalesced fetch: `target`'s bytes, fetched and
    /// written unless a prior flight has just written them.
    async fn fill<F, Fut>(&self, target: &Path, fetch: F) -> io::Result<Vec<u8>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = io::Result<Vec<u8>>>,
    {
        // The disk re-check is an OPTIMIZATION only. A cache miss (`Ok(None)`)
        // OR a transient read error (`Err`, e.g. EIO / fd exhaustion on the
        // local file) both fall through to the authoritative remote fetch — we
        // must not let one caller's disk hiccup broadcast a failure to the
        // whole coalesced batch, since the fetch path can satisfy everyone.
        if let Some(bytes) = try_read_cached_bytes(target).ok().flatten() {
            return Ok(bytes);
        }
        let bytes = fetch().await?;
        self.best_effort_write(target, &bytes);
        Ok(bytes)
    }
}

// ============================================================================
// CachedContentStore
// ============================================================================

/// A remote [`ContentStore`] fronted by the disk artifact cache.
///
/// The cache is a property of how a store stack is assembled, not something
/// readers consult: [`crate::StorageBackend::with_disk_cache`] wraps the
/// stores whose reads leave the machine, and every read through the wrapper
/// is served from the inner store's local tier, then this cache, then a
/// fetch that writes the cache. A local store is never wrapped, so it is
/// never copied into the cache.
///
/// Built [`uncached`](Self::uncached) (for a store whose bytes may not sit
/// on disk in plaintext) it still coalesces concurrent fetches of one object,
/// and writes nothing.
///
/// Entries are keyed by CID alone, so every store over one directory shares
/// them. Fetches are not: a waiter takes the leader's outcome without running
/// its own read, so a flight is shared only by stores reading one storage
/// with one key, in one namespace. Otherwise a child branch's miss would fail
/// its parent's concurrent read, and a store that cannot decrypt would receive
/// another's plaintext.
#[derive(Debug, Clone)]
pub struct CachedContentStore {
    inner: Arc<dyn ContentStore>,
    disk: Option<Arc<DiskArtifactCache>>,
    flights: Arc<StoreFlights>,
    /// The namespace `inner` reads, which scopes its flights in `flights`.
    scope: Arc<str>,
}

/// In-flight fetches of the stores one backend hands out, keyed by namespace
/// and object; see [`CachedContentStore`].
pub type StoreFlights = Flights<(Arc<str>, ContentId)>;

impl CachedContentStore {
    /// Front `inner` with the cache directory `cache` serves.
    pub fn new(inner: Arc<dyn ContentStore>, cache: Arc<DiskArtifactCache>) -> Self {
        Self::in_backend(inner, Some(cache), Arc::default(), "")
    }

    /// Coalesce `inner`'s fetches without touching disk.
    pub fn uncached(inner: Arc<dyn ContentStore>) -> Self {
        Self::in_backend(inner, None, Arc::default(), "")
    }

    /// The store a backend hands out for `namespace`: `flights` belongs to
    /// the backend and is shared by every store it hands out.
    pub(crate) fn in_backend(
        inner: Arc<dyn ContentStore>,
        disk: Option<Arc<DiskArtifactCache>>,
        flights: Arc<StoreFlights>,
        namespace: &str,
    ) -> Self {
        Self {
            inner,
            disk,
            flights,
            scope: Arc::from(namespace),
        }
    }

    fn entry(&self, id: &ContentId) -> Option<PathBuf> {
        self.disk
            .as_ref()
            .map(|cache| cache.root.join(id.to_string()))
    }

    /// `id` from the inner store, one fetch per object in flight in this
    /// store's namespace. The leader writes `entry` unless a prior flight has
    /// just written it.
    async fn fetch(&self, id: &ContentId, entry: Option<&Path>) -> crate::error::Result<Vec<u8>> {
        let key = (Arc::clone(&self.scope), id.clone());
        let fetch = || async {
            self.inner
                .get(id)
                .await
                .map(ContentBytes::into_vec)
                .map_err(storage_to_io_error)
        };
        match (&self.disk, entry) {
            (Some(cache), Some(path)) => self.flights.run(key, || cache.fill(path, fetch)).await,
            _ => self.flights.run(key, fetch).await,
        }
        .map_err(io_to_storage_error)
    }

    /// Drop `id`'s cache entry. Called after the inner store released it,
    /// never before: evicting first leaves a window where a concurrent reader
    /// refills the entry from a store that still holds the blob. A fetch
    /// already in flight may still write the entry after this; consumers
    /// that must not act on a released object tolerate one.
    fn evict(&self, id: &ContentId) {
        if let (Some(cache), Some(path)) = (&self.disk, self.entry(id)) {
            cache.evict_entry(&path);
        }
    }
}

/// A cache entry's bytes. Read failures are misses: the entry is a copy, and
/// the inner store is the authority.
#[cfg(not(target_arch = "wasm32"))]
fn read_entry(path: &Path) -> Option<ContentBytes> {
    // SAFETY: entries land by renaming a staged file into place
    // (`write_atomic`) and are only ever unlinked after that.
    match unsafe { ContentBytes::open(path) } {
        Ok(bytes) => bytes,
        Err(err) => {
            tracing::debug!(path = %path.display(), error = %err, "unreadable cache entry; fetching");
            None
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn read_entry(_path: &Path) -> Option<ContentBytes> {
    None
}

/// `range` of a cache entry, read positionally; `None` when the entry is
/// absent or unreadable.
#[cfg(not(target_arch = "wasm32"))]
fn read_entry_range(path: &Path, range: std::ops::Range<u64>) -> Option<Vec<u8>> {
    let file = fs::File::open(path).ok()?;
    let len = range.end.saturating_sub(range.start) as usize;
    let mut buf = vec![0u8; len];
    #[cfg(unix)]
    let n = {
        use std::os::unix::fs::FileExt;
        file.read_at(&mut buf, range.start).ok()?
    };
    #[cfg(not(unix))]
    let n = {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = file;
        file.seek(SeekFrom::Start(range.start)).ok()?;
        file.read(&mut buf).ok()?
    };
    buf.truncate(n);
    Some(buf)
}

#[cfg(target_arch = "wasm32")]
fn read_entry_range(_path: &Path, _range: std::ops::Range<u64>) -> Option<Vec<u8>> {
    None
}

fn io_to_storage_error(e: io::Error) -> crate::error::Error {
    match e.kind() {
        io::ErrorKind::NotFound => crate::error::Error::not_found(e.to_string()),
        _ => crate::error::Error::storage(e.to_string()),
    }
}

#[async_trait::async_trait]
impl ContentStore for CachedContentStore {
    async fn has(&self, id: &ContentId) -> crate::error::Result<bool> {
        self.inner.has(id).await
    }

    async fn get(&self, id: &ContentId) -> crate::error::Result<ContentBytes> {
        if let Some(bytes) = self.inner.get_local(id)? {
            return Ok(bytes);
        }
        let Some(path) = self.entry(id) else {
            return self.fetch(id, None).await.map(Into::into);
        };
        if let Some(bytes) = read_entry(&path) {
            return Ok(bytes);
        }
        let bytes = self.fetch(id, Some(&path)).await?;
        // Read back the entry just written, so a large object is held as a
        // mapping (page cache) rather than heap.
        Ok(read_entry(&path).unwrap_or_else(|| bytes.into()))
    }

    fn get_local(&self, id: &ContentId) -> crate::error::Result<Option<ContentBytes>> {
        if let Some(bytes) = self.inner.get_local(id)? {
            return Ok(Some(bytes));
        }
        Ok(self.entry(id).and_then(|path| read_entry(&path)))
    }

    async fn get_range(
        &self,
        id: &ContentId,
        range: std::ops::Range<u64>,
    ) -> crate::error::Result<Vec<u8>> {
        if let Some(bytes) = self
            .entry(id)
            .and_then(|path| read_entry_range(&path, range.clone()))
        {
            return Ok(bytes);
        }
        self.inner.get_range(id, range).await
    }

    fn supports_ranged_reads(&self) -> bool {
        self.inner.supports_ranged_reads()
    }

    async fn put(&self, kind: crate::ContentKind, bytes: &[u8]) -> crate::error::Result<ContentId> {
        self.inner.put(kind, bytes).await
    }

    async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> crate::error::Result<()> {
        self.inner.put_with_id(id, bytes).await
    }

    async fn prefetch(&self, id: &ContentId) -> crate::error::Result<()> {
        let Some(path) = self.entry(id) else {
            return Ok(());
        };
        if path.exists() || self.inner.get_local(id)?.is_some() {
            return Ok(());
        }
        self.fetch(id, Some(&path)).await.map(drop)
    }

    fn keep_local(&self, id: &ContentId, bytes: &[u8]) {
        let (Some(cache), Some(path)) = (&self.disk, self.entry(id)) else {
            return;
        };
        if matches!(self.inner.get_local(id), Ok(Some(_))) {
            return;
        }
        cache.best_effort_write(&path, bytes);
    }

    async fn release(&self, id: &ContentId) -> crate::error::Result<()> {
        let released = self.inner.release(id).await;
        self.evict(id);
        released
    }

    async fn release_many(&self, ids: &[ContentId]) -> Vec<(ContentId, crate::error::Error)> {
        let failures = self.inner.release_many(ids).await;
        // Every id, failed or not: a failed release may have deleted some of
        // the blob's addresses, and a needless eviction only costs a refetch.
        for id in ids {
            self.evict(id);
        }
        failures
    }

    fn miss_register(&self) -> Option<&crate::storage::residency::MissRegister> {
        self.inner.miss_register()
    }

    fn query_guard(&self) -> Option<crate::storage::residency::InFlightGuard> {
        self.inner.query_guard()
    }

    async fn sync(&self) -> crate::error::Result<()> {
        self.inner.sync().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// wasm32-unknown-unknown returns `Unsupported` from every `std::fs`
    /// call. That MUST read as a cache miss (fall through to CAS fetch), not
    /// an error.
    #[test]
    fn unsupported_read_is_a_miss_not_an_error() {
        let miss = read_result_as_cache_outcome(Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "operation not supported on this platform",
        )));
        assert!(matches!(miss, Ok(None)));

        let not_found =
            read_result_as_cache_outcome(Err(io::Error::new(io::ErrorKind::NotFound, "enoent")));
        assert!(matches!(not_found, Ok(None)));

        let hit = read_result_as_cache_outcome(Ok(vec![1, 2, 3]));
        assert!(matches!(hit, Ok(Some(ref b)) if b == &vec![1, 2, 3]));

        // Real I/O failures (EIO, permissions) still surface as errors.
        let denied = read_result_as_cache_outcome(Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "eacces",
        )));
        assert!(denied.is_err());
    }

    /// The same rule for the loaders' one-time `create_dir_all`: on a
    /// filesystem-less platform there is simply no cache directory to make,
    /// and aborting there kills the load before any CAS fetch is attempted.
    #[test]
    fn unsupported_create_dir_is_not_an_error() {
        let unsupported = create_dir_result_as_cache_outcome(Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "operation not supported on this platform",
        )));
        assert!(unsupported.is_ok());

        assert!(create_dir_result_as_cache_outcome(Ok(())).is_ok());

        // A real filesystem failure still aborts the load.
        let denied = create_dir_result_as_cache_outcome(Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "eacces",
        )));
        assert!(denied.is_err());
    }

    fn temp_cache_dir(label: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "fluree-artifact-cache-test-{}-{}-{}",
            label,
            std::process::id(),
            n
        ));
        let _ = fs::remove_dir_all(&path);
        path
    }

    #[test]
    fn write_and_read_back() {
        let dir = temp_cache_dir("write-read");
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        let target = dir.join("abc123.leaf");
        let data = b"hello world";

        cache.best_effort_write(&target, data);
        assert!(target.exists());
        assert_eq!(fs::read(&target).unwrap(), data);
    }

    #[test]
    fn write_skipped_when_budget_is_zero() {
        let dir = temp_cache_dir("zero-budget");
        let cache = DiskArtifactCache::with_budget(dir.clone(), 0);
        let target = dir.join("should-not-exist.leaf");

        cache.best_effort_write(&target, b"data");
        assert!(!target.exists());
    }

    #[test]
    fn duplicate_write_is_idempotent() {
        let dir = temp_cache_dir("dup-write");
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        let target = dir.join("dup.leaf");
        let data = b"first write";

        cache.best_effort_write(&target, data);
        cache.best_effort_write(&target, b"second write attempt");
        // First write wins — content unchanged.
        assert_eq!(fs::read(&target).unwrap(), data);
    }

    #[test]
    fn tracked_bytes_updated_on_write() {
        let dir = temp_cache_dir("tracked");
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);

        cache.best_effort_write(&dir.join("a.leaf"), &[0u8; 100]);
        cache.best_effort_write(&dir.join("b.leaf"), &[0u8; 200]);

        assert_eq!(cache.current_bytes().unwrap(), 300);
    }

    /// The first write sizes the directory off the writer's thread: a large
    /// shared cache took seconds to walk inline, under the state lock, which
    /// stalled the async worker writing first and every writer behind it.
    #[test]
    fn first_write_does_not_wait_for_the_size_scan() {
        let dir = temp_cache_dir("background-scan");
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        fs::write(dir.join("existing.leaf"), [0u8; 300]).unwrap();
        let (release, hold) = std::sync::mpsc::channel();
        cache.state.lock().scan_hold = Some(hold);

        // Returns with the scan still held: the size is not known yet.
        cache.best_effort_write(&dir.join("new.leaf"), &[0u8; 100]);
        assert!(dir.join("new.leaf").exists());
        assert_eq!(cache.state.lock().tracked_bytes, None);

        release.send(()).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while cache.state.lock().scanning {
            assert!(
                std::time::Instant::now() < deadline,
                "size scan never landed"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        // The walk saw the new entry too; it is counted once.
        assert_eq!(cache.state.lock().tracked_bytes, Some(400));
    }

    /// A released object's entry must go, or a later read sees a blob storage
    /// no longer holds.
    /// Eviction and writes share the byte accounting, so a released entry does
    /// not leave the budget overstated.
    #[test]
    fn tracked_bytes_updated_on_eviction() {
        let dir = temp_cache_dir("evict-tracked");
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        let target = dir.join("tracked.leaf");

        cache.best_effort_write(&target, &[0u8; 100]);
        assert_eq!(cache.current_bytes().unwrap(), 100);

        cache.evict_entry(&target);

        assert_eq!(cache.current_bytes().unwrap(), 0);
    }

    #[test]
    fn eviction_removes_oldest_files() {
        let dir = temp_cache_dir("eviction");
        // Budget: 500 bytes, low water mark = 500 * 8/10 = 400.
        let cache = DiskArtifactCache::with_budget(dir.clone(), 500);

        // Write three 150-byte files (total 450, under budget).
        for name in &["old.leaf", "mid.leaf", "new.leaf"] {
            let target = dir.join(name);
            cache.best_effort_write(&target, &[0u8; 150]);
            // Ensure distinct modification times for deterministic eviction order.
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert_eq!(cache.current_bytes().unwrap(), 450);

        // Writing another 150 bytes pushes total to 600 > 500 budget.
        // ensure_capacity should evict oldest files down to low water mark (400)
        // or budget - incoming (350), whichever is lower → 350.
        cache.best_effort_write(&dir.join("trigger.leaf"), &[0u8; 150]);

        // The oldest file(s) should have been evicted.
        assert!(
            !dir.join("old.leaf").exists(),
            "oldest file should be evicted"
        );
        // The newest files + trigger should survive.
        assert!(dir.join("trigger.leaf").exists());
    }

    #[test]
    fn scan_ignores_temp_files() {
        let dir = temp_cache_dir("scan-temp");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("real.leaf"), [0u8; 10]).unwrap();
        fs::write(dir.join(".cas_123_0.tmp"), [0u8; 20]).unwrap();

        let entries = scan_cache_entries(&dir).unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].path.ends_with("real.leaf"));
    }

    #[test]
    fn scan_walks_subdirectories() {
        let dir = temp_cache_dir("scan-subdir");
        let sub = dir.join("sub");
        fs::create_dir_all(&sub).unwrap();
        fs::write(dir.join("a.leaf"), [0u8; 10]).unwrap();
        fs::write(sub.join("b.leaf"), [0u8; 20]).unwrap();

        let entries = scan_cache_entries(&dir).unwrap();
        assert_eq!(entries.len(), 2);
        let total: u64 = entries.iter().map(|e| e.bytes).sum();
        assert_eq!(total, 30);
    }

    #[test]
    fn for_dir_returns_singleton() {
        let dir = temp_cache_dir("singleton");
        let a = DiskArtifactCache::for_dir(&dir);
        let b = DiskArtifactCache::for_dir(&dir);
        assert!(Arc::ptr_eq(&a, &b), "same dir should return same Arc");
    }

    #[test]
    fn singleton_dropped_when_no_strong_refs() {
        let dir = temp_cache_dir("singleton-drop");
        let a = DiskArtifactCache::for_dir(&dir);
        let ptr1 = Arc::as_ptr(&a);
        drop(a);

        // After dropping the only strong ref, a new call should create a fresh instance.
        let b = DiskArtifactCache::for_dir(&dir);
        let ptr2 = Arc::as_ptr(&b);
        assert_ne!(ptr1, ptr2, "should be a new instance after drop");
    }

    #[test]
    fn current_bytes_scans_on_first_call() {
        let dir = temp_cache_dir("initial-scan");
        fs::create_dir_all(&dir).unwrap();
        // Pre-populate some files before creating the cache.
        fs::write(dir.join("pre1.leaf"), [0u8; 100]).unwrap();
        fs::write(dir.join("pre2.leaf"), [0u8; 200]).unwrap();

        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        assert_eq!(cache.current_bytes().unwrap(), 300);
    }

    #[test]
    fn write_creates_parent_dirs() {
        let dir = temp_cache_dir("nested-write");
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        let target = dir.join("deep").join("nested").join("file.leaf");

        cache.best_effort_write(&target, b"nested data");
        assert_eq!(fs::read(&target).unwrap(), b"nested data");
    }

    // ---- Single-flight coalescing ----

    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coalesced_fetch_runs_leader_once_under_concurrency() {
        let dir = temp_cache_dir("coalesce-once");
        let cache = Arc::new(DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024));
        let target = dir.join("obj.bin");
        let calls = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            let target = target.clone();
            let calls = Arc::clone(&calls);
            handles.push(tokio::spawn(async move {
                cache
                    .coalesced_fetch(target, move || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        // Hold the flight open long enough for the others to join.
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        Ok(b"shared-bytes".to_vec())
                    })
                    .await
            }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap().unwrap(), b"shared-bytes");
        }
        // Only the leader fetched; the other 7 awaited the shared result.
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // And it was written to disk exactly once.
        assert_eq!(fs::read(&target).unwrap(), b"shared-bytes");
    }

    #[tokio::test]
    async fn coalesced_fetch_does_not_cache_errors() {
        let dir = temp_cache_dir("coalesce-err");
        let cache = Arc::new(DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024));
        let target = dir.join("obj.bin");

        let first = cache
            .coalesced_fetch(target.clone(), || async { Err(io::Error::other("boom")) })
            .await;
        assert!(first.is_err());

        // The errored entry must have been removed, not cached: a fresh call
        // runs its own fetch and succeeds.
        let second = cache
            .coalesced_fetch(target.clone(), || async { Ok(b"recovered".to_vec()) })
            .await;
        assert_eq!(second.unwrap(), b"recovered");
    }

    #[tokio::test]
    async fn coalesced_fetch_double_checks_disk_before_fetching() {
        let dir = temp_cache_dir("coalesce-disk");
        let cache = Arc::new(DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024));
        let target = dir.join("obj.bin");
        fs::write(&target, b"already-here").unwrap();

        let fetched = Arc::new(AtomicUsize::new(0));
        let f = Arc::clone(&fetched);
        let bytes = cache
            .coalesced_fetch(target.clone(), move || async move {
                f.fetch_add(1, Ordering::SeqCst);
                Ok(b"from-fetch".to_vec())
            })
            .await
            .unwrap();

        // Leader saw the file on the post-win disk re-check and skipped fetch.
        assert_eq!(bytes, b"already-here");
        assert_eq!(fetched.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn coalesced_fetch_disk_read_error_falls_through_to_fetch() {
        // A transient/non-NotFound error on the optimization-only disk
        // double-check must NOT fail the flight: fall through to the
        // authoritative fetch. We force a non-NotFound read error by placing a
        // *directory* where the cache file would be (`fs::read` on a dir errors).
        let dir = temp_cache_dir("coalesce-diskerr");
        let cache = Arc::new(DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024));
        let target = dir.join("obj.bin");
        fs::create_dir_all(&target).unwrap();

        let fetched = Arc::new(AtomicUsize::new(0));
        let f = Arc::clone(&fetched);
        let bytes = cache
            .coalesced_fetch(target.clone(), move || async move {
                f.fetch_add(1, Ordering::SeqCst);
                Ok(b"from-fetch".to_vec())
            })
            .await
            .unwrap();

        assert_eq!(bytes, b"from-fetch");
        assert_eq!(
            fetched.load(Ordering::SeqCst),
            1,
            "a disk-read error on the double-check must fall through to fetch, not fail"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn coalesced_fetch_cancelled_leader_does_not_orphan_slot() {
        let dir = temp_cache_dir("coalesce-cancel");
        let cache = Arc::new(DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024));
        let target = dir.join("obj.bin");

        // Leader registers the flight then hangs in fetch; we cancel it.
        let leader = tokio::spawn({
            let cache = Arc::clone(&cache);
            let target = target.clone();
            async move {
                cache
                    .coalesced_fetch(target, || async {
                        tokio::time::sleep(Duration::from_secs(60)).await;
                        Ok(b"never".to_vec())
                    })
                    .await
            }
        });
        // Let it register the slot and park in the sleep.
        tokio::time::sleep(Duration::from_millis(50)).await;
        leader.abort();
        let _ = leader.await;

        // The guard must have cleared the slot on drop. A new fetch must proceed
        // (not hang waiting on a result the cancelled leader will never send).
        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        let bytes = tokio::time::timeout(
            Duration::from_secs(5),
            cache.coalesced_fetch(target.clone(), move || async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(b"after-cancel".to_vec())
            }),
        )
        .await
        .expect("must not hang on an orphaned in-flight slot")
        .unwrap();

        assert_eq!(bytes, b"after-cancel");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// A remote store whose fetches count. `local` makes it serve its bytes
    /// locally instead; `watch` records whether that path existed when a
    /// release reached the store.
    #[derive(Debug, Default)]
    struct CountingStore {
        data: Vec<u8>,
        gets: AtomicUsize,
        delay: Duration,
        local: bool,
        watch: Option<PathBuf>,
        watched_at_release: std::sync::atomic::AtomicBool,
    }

    impl CountingStore {
        fn remote(data: &[u8]) -> Self {
            Self {
                data: data.to_vec(),
                ..Self::default()
            }
        }

        fn gets(&self) -> usize {
            self.gets.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl ContentStore for CountingStore {
        async fn has(&self, _id: &ContentId) -> crate::error::Result<bool> {
            Ok(true)
        }
        async fn get(&self, _id: &ContentId) -> crate::error::Result<ContentBytes> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            Ok(self.data.clone().into())
        }
        fn get_local(&self, _id: &ContentId) -> crate::error::Result<Option<ContentBytes>> {
            Ok(self.local.then(|| self.data.clone().into()))
        }
        async fn put(
            &self,
            _kind: crate::ContentKind,
            _bytes: &[u8],
        ) -> crate::error::Result<ContentId> {
            unimplemented!("put not needed for cache tests")
        }
        async fn put_with_id(&self, _id: &ContentId, _bytes: &[u8]) -> crate::error::Result<()> {
            unimplemented!("put_with_id not needed for cache tests")
        }
        async fn release(&self, _id: &ContentId) -> crate::error::Result<()> {
            if let Some(path) = &self.watch {
                self.watched_at_release
                    .store(path.exists(), Ordering::SeqCst);
            }
            Ok(())
        }
    }

    fn regular_files_under(root: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    out.push(path);
                }
            }
        }
        out
    }

    fn cached(store: Arc<CountingStore>, dir: &Path) -> CachedContentStore {
        CachedContentStore::new(store, DiskArtifactCache::for_dir(dir))
    }

    /// A fetch fills the entry, and every later read — whole, local or a
    /// range — is served from it.
    #[tokio::test]
    async fn a_fetched_object_is_served_from_its_entry() {
        let dir = temp_cache_dir("wrapper-entry");
        let data = vec![5u8; 128];
        let id = ContentId::new(crate::ContentKind::IndexLeaf, &data);
        let inner = Arc::new(CountingStore::remote(&data));
        let store = cached(Arc::clone(&inner), &dir);

        assert!(store.get_local(&id).unwrap().is_none(), "nothing local yet");
        assert_eq!(store.get(&id).await.unwrap(), data);
        assert!(dir.join(id.to_string()).exists());
        assert_eq!(store.get(&id).await.unwrap(), data);
        assert_eq!(store.get_local(&id).unwrap().unwrap(), data);
        assert_eq!(store.get_range(&id, 8..16).await.unwrap(), &data[8..16]);
        assert_eq!(inner.gets(), 1, "one fetch serves every read after it");
        let _ = fs::remove_dir_all(&dir);
    }

    /// Bytes the inner store serves locally are never copied: a mixed stack
    /// can be fronted whole without caching its local routes.
    #[tokio::test]
    async fn bytes_the_inner_store_serves_locally_are_not_copied() {
        let dir = temp_cache_dir("wrapper-local");
        let id = ContentId::new(crate::ContentKind::IndexLeaf, b"local");
        let inner = Arc::new(CountingStore {
            local: true,
            ..CountingStore::remote(b"local")
        });
        let store = cached(Arc::clone(&inner), &dir);

        assert_eq!(store.get(&id).await.unwrap(), b"local");
        assert_eq!(store.get_local(&id).unwrap().unwrap(), b"local");
        store.keep_local(&id, b"local");
        store.prefetch(&id).await.unwrap();
        assert_eq!(inner.gets(), 0);
        assert!(regular_files_under(&dir).is_empty(), "nothing was copied");
        let _ = fs::remove_dir_all(&dir);
    }

    /// A release reaches the store before the entry goes: evicting first
    /// leaves a window where a concurrent reader refills the entry from a
    /// store that still holds the blob.
    #[tokio::test]
    async fn release_deletes_then_evicts() {
        let dir = temp_cache_dir("wrapper-release");
        let data = vec![6u8; 64];
        let id = ContentId::new(crate::ContentKind::IndexRoot, &data);
        let entry = dir.join(id.to_string());
        let inner = Arc::new(CountingStore {
            watch: Some(entry.clone()),
            ..CountingStore::remote(&data)
        });
        let store = cached(Arc::clone(&inner), &dir);

        store.get(&id).await.unwrap();
        assert!(entry.exists());
        store.release(&id).await.unwrap();
        assert!(
            inner.watched_at_release.load(Ordering::SeqCst),
            "the entry must still exist when the store releases the blob"
        );
        assert!(
            !entry.exists(),
            "a released blob stays readable from the cache"
        );

        // The same for a batch, and an id never cached is not an error.
        store.get(&id).await.unwrap();
        let never = ContentId::new(crate::ContentKind::IndexRoot, b"never cached");
        assert!(store.release_many(&[id.clone(), never]).await.is_empty());
        assert!(!entry.exists());
        let _ = fs::remove_dir_all(&dir);
    }

    /// Seeding writes the entry a later read is served from; a prefetch
    /// fetches once and leaves an existing entry alone.
    #[tokio::test]
    async fn keep_local_and_prefetch_fill_the_entry() {
        let dir = temp_cache_dir("wrapper-seed");
        let seeded = ContentId::new(crate::ContentKind::IndexLeaf, b"seeded");
        let inner = Arc::new(CountingStore::remote(b"seeded"));
        let store = cached(Arc::clone(&inner), &dir);

        store.keep_local(&seeded, b"seeded");
        assert_eq!(store.get(&seeded).await.unwrap(), b"seeded");
        assert_eq!(inner.gets(), 0, "a seeded object is not fetched");

        let prefetched = ContentId::new(crate::ContentKind::IndexLeaf, b"prefetched");
        store.prefetch(&prefetched).await.unwrap();
        store.prefetch(&prefetched).await.unwrap();
        assert_eq!(inner.gets(), 1);
        assert!(store.get_local(&prefetched).unwrap().is_some());
        let _ = fs::remove_dir_all(&dir);
    }

    /// Without a directory — a store whose bytes may not sit on disk in
    /// plaintext — concurrent readers of one cold object still share one
    /// fetch, nothing is written, an entry left by an earlier run is never
    /// read, and the next read goes back to the store.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_uncached_store_coalesces_without_touching_disk() {
        let dir = temp_cache_dir("wrapper-uncached");
        let data = vec![9u8; 256];
        let id = ContentId::new(crate::ContentKind::IndexRoot, &data);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(id.to_string()), b"stale plaintext").unwrap();
        let inner = Arc::new(CountingStore {
            delay: Duration::from_millis(100),
            ..CountingStore::remote(&data)
        });
        let store = Arc::new(CachedContentStore::uncached(Arc::clone(&inner) as _));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let store = Arc::clone(&store);
            let id = id.clone();
            handles.push(tokio::spawn(async move { store.get(&id).await }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap().unwrap(), data);
        }
        assert_eq!(inner.gets(), 1, "concurrent readers share one fetch");
        assert!(store.get_local(&id).unwrap().is_none());
        store.keep_local(&id, &data);
        store.prefetch(&id).await.unwrap();
        assert_eq!(regular_files_under(&dir).len(), 1, "only the stale entry");
        store.get(&id).await.unwrap();
        assert_eq!(inner.gets(), 2, "nothing is retained");
        let _ = fs::remove_dir_all(&dir);
    }

    /// Concurrent readers of one cold object share one fetch with a cache
    /// directory too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cached_store_coalesces_concurrent_readers() {
        let dir = temp_cache_dir("wrapper-coalesce");
        let data = vec![42u8; 256];
        let id = ContentId::new(crate::ContentKind::IndexRoot, &data);
        let inner = Arc::new(CountingStore {
            delay: Duration::from_millis(100),
            ..CountingStore::remote(&data)
        });
        let store = Arc::new(cached(Arc::clone(&inner), &dir));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let store = Arc::clone(&store);
            let id = id.clone();
            handles.push(tokio::spawn(async move { store.get(&id).await }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap().unwrap(), data);
        }
        assert_eq!(inner.gets(), 1);
        let _ = fs::remove_dir_all(&dir);
    }
}
