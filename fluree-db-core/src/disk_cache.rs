use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use crate::{ContentBytes, ContentId, ContentKind, ContentStore};
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

/// The byte accounting a cache keeps for its directory.
///
/// The directory changes while a scan walks it, so a scan's total is only
/// right once every write and eviction that landed around it is counted
/// exactly once. Each registers here before its file operation and accounts
/// after it ([`Self::begin`], [`Self::end`]), at most one per path at a time,
/// and every scan lands through [`Self::land_scan`], which reconciles them.
#[derive(Debug, Default)]
struct DiskArtifactCacheState {
    /// The directory's size, once a scan has measured it.
    tracked_bytes: Option<u64>,
    /// A background scan is sizing the directory; see
    /// [`DiskArtifactCache::spawn_sizing`].
    sizing: bool,
    /// Scans walking the directory now: the background sizing and
    /// [`DiskArtifactCache::evict_until`]'s rescan.
    scans: usize,
    /// While a scan runs, each write and eviction that lands, with the bytes
    /// it added (positive) or removed (negative).
    landed: Vec<(PathBuf, i64)>,
    /// Writes and evictions registered and not yet accounted.
    in_flight: Vec<InFlight>,
    next_op: u64,
    /// Test hook: named points where an operation waits; see `pause_at`.
    #[cfg(test)]
    pauses: HashMap<&'static str, (std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<bool>)>,
}

/// A write or eviction between its registration and its accounting.
#[derive(Debug)]
struct InFlight {
    id: u64,
    path: PathBuf,
    /// What a scan that landed meanwhile counted for `path`. The walk can't
    /// say whether it read the path before or after this operation, so the
    /// operation corrects the total by what it actually left there.
    counted: Option<u64>,
}

/// The bytes a path holds after an operation that changed it by `delta`: a
/// write leaves its file, an eviction nothing.
fn size_after(delta: i64) -> u64 {
    u64::try_from(delta).unwrap_or(0)
}

fn add_signed(total: u64, delta: i64) -> u64 {
    if delta >= 0 {
        total.saturating_add(delta.unsigned_abs())
    } else {
        total.saturating_sub(delta.unsigned_abs())
    }
}

impl DiskArtifactCacheState {
    /// Register a write or eviction of `path` before its file operation, or
    /// `None` when one of `path` is under way already. One at a time per path
    /// is what lets a scan say what each path holds.
    fn begin(&mut self, path: &Path) -> Option<u64> {
        if self.in_flight.iter().any(|op| op.path == path) {
            return None;
        }
        let id = self.next_op;
        self.next_op += 1;
        self.in_flight.push(InFlight {
            id,
            path: path.to_path_buf(),
            counted: None,
        });
        Some(id)
    }

    /// Account operation `id`, whose file operation changed its path by
    /// `delta` bytes (`+n` a write created it, `-n` an eviction removed it).
    fn end(&mut self, id: u64, delta: i64) {
        let Some(at) = self.in_flight.iter().position(|op| op.id == id) else {
            return;
        };
        let InFlight { path, counted, .. } = self.in_flight.swap_remove(at);
        if delta == 0 {
            return;
        }
        if let Some(tracked) = self.tracked_bytes {
            let change = match counted {
                Some(counted) => size_after(delta) as i64 - counted as i64,
                None => delta,
            };
            self.tracked_bytes = Some(add_signed(tracked, change));
        }
        if self.scans > 0 {
            self.landed.push((path, delta));
        }
    }

    /// Open a scan; returns where its window starts in [`Self::landed`].
    fn begin_scan(&mut self) -> usize {
        self.scans += 1;
        self.landed.len()
    }

    /// Close a scan without installing its total.
    fn end_scan(&mut self) {
        self.scans -= 1;
        if self.scans == 0 {
            self.landed.clear();
        }
    }

    /// Install the size a scan measured: `walked_total` over the entries in
    /// `walked`. The walk read the directory while operations kept landing,
    /// so a path one changed meanwhile holds what its last operation left,
    /// whatever the walk saw; a path one is changing now is counted as found,
    /// for that operation to correct when it lands.
    fn land_scan(&mut self, start: usize, walked_total: u64, walked: &HashMap<&Path, u64>) {
        let mut last: HashMap<&Path, i64> = HashMap::new();
        for (path, delta) in &self.landed[start..] {
            last.insert(path.as_path(), *delta);
        }
        let found = |path: &Path| walked.get(path).copied().unwrap_or(0);
        let mut total = walked_total as i64;
        for (path, delta) in &last {
            total += size_after(*delta) as i64 - found(path) as i64;
        }
        let counted: Vec<u64> = self
            .in_flight
            .iter()
            .map(|op| match last.get(op.path.as_path()) {
                Some(delta) => size_after(*delta),
                None => found(&op.path),
            })
            .collect();
        for (op, counted) in self.in_flight.iter_mut().zip(counted) {
            op.counted = Some(counted);
        }
        self.tracked_bytes = Some(u64::try_from(total).unwrap_or(0));
    }

    /// The directory's size if known. If not, and no background scan is
    /// sizing it yet, `true`: the caller starts one
    /// ([`DiskArtifactCache::spawn_sizing`]).
    fn known_bytes(&mut self) -> (Option<u64>, bool) {
        if self.tracked_bytes.is_some() || self.sizing {
            return (self.tracked_bytes, false);
        }
        self.sizing = true;
        (None, true)
    }
}

/// A scan walking the directory. Writes and evictions that land meanwhile
/// are recorded for it until it lands ([`Self::land`]) or is dropped, which
/// closes it without a total, so an error or a panic mid-walk can't leave
/// it open.
#[must_use]
struct Scan<'a> {
    state: &'a Mutex<DiskArtifactCacheState>,
    /// Where its window starts in [`DiskArtifactCacheState::landed`].
    start: usize,
    /// The background sizing, which closing it ends.
    sizing: bool,
}

impl<'a> Scan<'a> {
    fn begin(state: &'a Mutex<DiskArtifactCacheState>, sizing: bool) -> Self {
        let start = state.lock().begin_scan();
        Self {
            state,
            start,
            sizing,
        }
    }

    /// Install the size the walk found; see
    /// [`DiskArtifactCacheState::land_scan`].
    fn land(self, walked_total: u64, walked: &HashMap<&Path, u64>) {
        self.state
            .lock()
            .land_scan(self.start, walked_total, walked);
    }
}

impl Drop for Scan<'_> {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        state.end_scan();
        if self.sizing {
            state.sizing = false;
        }
    }
}

/// Accounts a registered write or eviction when dropped, so neither an early
/// return nor a panic leaves its path registered for good.
struct Landing<'a> {
    state: &'a Mutex<DiskArtifactCacheState>,
    id: u64,
    delta: i64,
}

impl Drop for Landing<'_> {
    fn drop(&mut self) {
        self.state.lock().end(self.id, self.delta);
    }
}

/// Walk `root` and land `scan` on what it found.
fn walk_and_land(scan: Scan<'_>, root: &Path) -> io::Result<()> {
    let entries = scan_cache_entries(root)?;
    scan.land(walked_bytes(&entries), &walked_paths(&entries));
    Ok(())
}

/// The bytes a walk found.
fn walked_bytes(entries: &[CacheEntry]) -> u64 {
    entries
        .iter()
        .fold(0u64, |acc, entry| acc.saturating_add(entry.bytes))
}

/// Each entry a walk found, with its size, by path.
fn walked_paths(entries: &[CacheEntry]) -> HashMap<&Path, u64> {
    entries
        .iter()
        .map(|entry| (entry.path.as_path(), entry.bytes))
        .collect()
}

/// How long a test pause waits on either side before failing the test.
#[cfg(test)]
const PAUSE_LIMIT: std::time::Duration = std::time::Duration::from_secs(10);

/// Test hook: wait at `point` if a test armed it, after telling the test.
/// The test lets the operation go on, or makes it panic here. A test that
/// fails first does neither, and the operation panics rather than wait for
/// good, so the test reports its failure instead of hanging on the join.
#[cfg(test)]
fn pause_at(state: &Mutex<DiskArtifactCacheState>, point: &'static str) {
    let armed = state.lock().pauses.remove(point);
    if let Some((reached, go)) = armed {
        let _ = reached.send(());
        match go.recv_timeout(PAUSE_LIMIT) {
            Ok(true) => {}
            Ok(false) => panic!("the test panicked `{point}`"),
            Err(_) => panic!("the test never released `{point}`"),
        }
    }
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
///
/// An empty file is a miss too. No object is empty, and an entry renamed into
/// place without an fsync can come back empty after a host crash; serving it
/// would fail every read of the object until the entry is evicted.
fn read_result_as_cache_outcome(res: io::Result<Vec<u8>>) -> io::Result<Option<Vec<u8>>> {
    match res {
        Ok(bytes) if bytes.is_empty() => Ok(None),
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
    Ok(walked_bytes(&scan_cache_entries(root)?))
}

/// Test hook: runs on each file a walk on this thread lists, before the
/// walk reads its size.
#[cfg(test)]
type ListedHook = Box<dyn FnMut(&Path)>;

#[cfg(test)]
thread_local! {
    static ON_LISTED: std::cell::RefCell<Option<ListedHook>> =
        const { std::cell::RefCell::new(None) };
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

        // An entry removed after the listing is gone, not a failed walk: the
        // cache's own evictions and writes run while it walks.
        for child in read_dir {
            let child = child?;
            let path = child.path();
            let file_type = match child.file_type() {
                Ok(file_type) => file_type,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            };
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if !file_type.is_file() || is_cache_temp_file(&path) {
                continue;
            }

            #[cfg(test)]
            ON_LISTED.with(|hook| {
                if let Some(hook) = hook.borrow_mut().as_mut() {
                    hook(&path);
                }
            });
            let meta = match child.metadata() {
                Ok(meta) => meta,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            };
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
        walk_and_land(Scan::begin(&self.state, false), &self.root)?;
        Ok(self.state.lock().tracked_bytes.unwrap_or(0))
    }

    /// Size the directory on a background thread.
    ///
    /// Walking a large shared cache takes seconds; done inline, under the
    /// state lock, it stalls the async worker that writes first and every
    /// writer queued on the lock. Writes made meanwhile skip the budget check,
    /// and the scan reconciles everything that lands while it walks. The
    /// scan opens before the walk starts, so anything that lands before it
    /// opens is on disk for the walk to see.
    fn spawn_sizing(&self) {
        let root = self.root.clone();
        let shared = Arc::clone(&self.state);
        let size = move || {
            let scan = Scan::begin(&shared, true);
            #[cfg(test)]
            pause_at(&shared, "size-scan");
            if let Err(err) = walk_and_land(scan, &root) {
                tracing::debug!(
                    cache_dir = %root.display(),
                    error = %err,
                    "failed to size the disk cache; the next write retries"
                );
            }
        };
        // No threads on wasm32, where the cache never writes (budget 0).
        #[cfg(target_arch = "wasm32")]
        size();
        #[cfg(not(target_arch = "wasm32"))]
        if let Err(err) = std::thread::Builder::new()
            .name("fluree-cache-size".into())
            .spawn(size)
        {
            tracing::debug!(error = %err, "failed to start the disk cache size scan");
            self.state.lock().sizing = false;
        }
    }

    /// Drop one entry, keeping the byte accounting in step.
    ///
    /// An absent entry is the common case — most released CIDs were never
    /// cached — and is not an error. Untracked totals stay untracked so the
    /// next capacity check rescans rather than trusting a partial figure. An
    /// entry another write or eviction is changing is left to it.
    pub(crate) fn evict_entry(&self, path: &Path) {
        let Ok(metadata) = fs::metadata(path) else {
            return;
        };
        let Some(id) = self.state.lock().begin(path) else {
            return;
        };
        let mut landing = Landing {
            state: &self.state,
            id,
            delta: 0,
        };
        match fs::remove_file(path) {
            Ok(()) => landing.delta = -(metadata.len() as i64),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => tracing::debug!(
                cache_dir = %self.root.display(),
                path = %path.display(),
                error = %err,
                "failed to evict cache entry for a released object"
            ),
        }
        #[cfg(test)]
        pause_at(&self.state, "evicted");
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
    ///
    /// It rescans the directory, so it lands as a scan, and each removal is
    /// a registered eviction: entries another write or eviction is changing
    /// are passed over.
    fn evict_until(&self, target_bytes: u64) -> io::Result<()> {
        let scan = Scan::begin(&self.state, false);
        let mut entries = scan_cache_entries(&self.root)?;
        #[cfg(test)]
        pause_at(&self.state, "rescan");
        let walked_total = walked_bytes(&entries);
        let mut current = walked_total;
        if current > target_bytes {
            entries.sort_by_key(|entry| entry.modified);
            for entry in &entries {
                if current <= target_bytes {
                    break;
                }
                let Some(id) = self.state.lock().begin(&entry.path) else {
                    continue;
                };
                let mut landing = Landing {
                    state: &self.state,
                    id,
                    delta: 0,
                };
                match fs::remove_file(&entry.path) {
                    Ok(()) => {
                        landing.delta = -(entry.bytes as i64);
                        current = current.saturating_sub(entry.bytes);
                    }
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {
                        // Gone since the walk, through this cache or around
                        // it: the landing counts it as gone either way.
                        landing.delta = -(entry.bytes as i64);
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
        }
        scan.land(walked_total, &walked_paths(&entries));
        Ok(())
    }

    /// Make room for `incoming_bytes` when the directory's size is `known`
    /// and the write would pass the budget.
    fn ensure_capacity(&self, known: Option<u64>, incoming_bytes: u64) -> io::Result<()> {
        let Some(current) = known else {
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

    fn write_atomic(&self, target: &Path, bytes: &[u8]) -> io::Result<bool> {
        // An empty entry is crash debris that readers treat as a miss; the
        // rename below replaces it.
        if fs::metadata(target).is_ok_and(|meta| meta.len() > 0) {
            return Ok(false);
        }
        #[cfg(test)]
        pause_at(&self.state, "checked");

        let parent = target
            .parent()
            .ok_or_else(|| io::Error::other("cache target has no parent dir"))?;
        fs::create_dir_all(parent)?;

        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp = parent.join(format!(".cas_{}_{}.tmp", std::process::id(), seq));
        if let Err(err) = fs::write(&tmp, bytes) {
            // Scans skip temporary files, so a partial one left here would
            // hold disk space no accounting sees and no eviction removes.
            let _ = fs::remove_file(&tmp);
            return Err(err);
        }
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
        // Only a write that creates the entry (or replaces an empty one)
        // registers: a registered write keeps an eviction of its path away for
        // its whole run. One that races another to create it is still caught
        // by `write_atomic`.
        if self.budget_bytes == 0
            || bytes.is_empty()
            || fs::metadata(target).is_ok_and(|meta| meta.len() > 0)
        {
            return;
        }

        // One lock registers the write and reads the directory's size.
        let (id, known, size) = {
            let mut state = self.state.lock();
            // Another write or an eviction of this entry is under way.
            let Some(id) = state.begin(target) else {
                return;
            };
            let (known, size) = state.known_bytes();
            (id, known, size)
        };
        #[cfg(test)]
        pause_at(&self.state, "registered");
        if size {
            self.spawn_sizing();
        }
        let mut landing = Landing {
            state: &self.state,
            id,
            delta: 0,
        };

        if let Err(err) = self.ensure_capacity(known, bytes.len() as u64) {
            tracing::warn!(
                cache_dir = %self.root.display(),
                error = %err,
                "failed to enforce disk cache budget; skipping cache write"
            );
            return;
        }

        let written = match self.write_atomic(target, bytes) {
            Err(err) if is_disk_full(&err) => {
                if let Err(evict_err) = self.evict_until(self.low_water_mark()) {
                    tracing::warn!(
                        cache_dir = %self.root.display(),
                        error = %evict_err,
                        "failed to evict cache files after disk-full error"
                    );
                    return;
                }
                self.write_atomic(target, bytes).inspect_err(|retry_err| {
                    tracing::warn!(
                        cache_dir = %self.root.display(),
                        target = %target.display(),
                        error = %retry_err,
                        "disk cache write failed after eviction; continuing without cache"
                    );
                })
            }
            Err(err) => {
                tracing::warn!(
                    cache_dir = %self.root.display(),
                    target = %target.display(),
                    error = %err,
                    "disk cache write failed; continuing without cache"
                );
                Err(err)
            }
            written => written,
        };
        if let Ok(true) = written {
            landing.delta = bytes.len() as i64;
        }
        #[cfg(test)]
        pause_at(&self.state, "written");
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
/// never copied into the cache. Only index artifacts are cached; other
/// objects are fetched straight through (see [`is_cached_kind`]).
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

/// Whether objects of `kind` get a disk-cache entry: the index artifacts that
/// queries and the indexer read again and again. Commits, transactions and
/// the other objects a load or fold reads once are fetched (and coalesced)
/// but not written: caching them would push hot leaves out of a cache that
/// evicts in write order, and would put transaction data on local disk.
fn is_cached_kind(kind: &ContentKind) -> bool {
    match kind {
        ContentKind::IndexRoot
        | ContentKind::IndexBranch
        | ContentKind::IndexLeaf
        | ContentKind::DictBlob { .. }
        | ContentKind::HistorySidecar
        | ContentKind::GarbageRecord
        | ContentKind::StatsSketch
        | ContentKind::SpatialIndex
        | ContentKind::AnnotationForwardBranch
        | ContentKind::AnnotationForwardLeaf
        | ContentKind::AnnotationReverseBranch
        | ContentKind::AnnotationReverseLeaf => true,
        ContentKind::Commit
        | ContentKind::Txn
        | ContentKind::LedgerConfig
        | ContentKind::GraphSourceSnapshot
        | ContentKind::GraphSourceMapping => false,
    }
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

    /// Where `id`'s cache entry lives, when this store caches it at all.
    fn entry(&self, id: &ContentId) -> Option<PathBuf> {
        let cache = self.disk.as_ref()?;
        id.content_kind()
            .as_ref()
            .is_some_and(is_cached_kind)
            .then(|| cache.root.join(id.to_string()))
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
/// the inner store is the authority. So is an empty entry; see
/// [`read_result_as_cache_outcome`].
#[cfg(not(target_arch = "wasm32"))]
fn read_entry(path: &Path) -> Option<ContentBytes> {
    // SAFETY: entries land by renaming a staged file into place
    // (`write_atomic`) and are only ever unlinked after that. The one entry
    // replaced in place is an empty one, which is read, never mapped.
    match unsafe { ContentBytes::open(path) } {
        Ok(bytes) => bytes.filter(|bytes| !bytes.is_empty()),
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

/// `range` of a cache entry, read positionally and clamped to the entry, as
/// [`ContentStore::get_range`] requires (`mid..u64::MAX` reads to the end);
/// `None` when the entry is absent, empty or unreadable.
#[cfg(not(target_arch = "wasm32"))]
fn read_entry_range(path: &Path, range: std::ops::Range<u64>) -> Option<Vec<u8>> {
    let file = fs::File::open(path).ok()?;
    let entry_len = file.metadata().ok()?.len();
    if entry_len == 0 {
        return None;
    }
    let start = range.start.min(entry_len);
    let len = (range.end.min(entry_len).saturating_sub(start)) as usize;
    let mut buf = vec![0u8; len];
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(&mut buf, start).ok()?;
    }
    #[cfg(not(unix))]
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = file;
        file.seek(SeekFrom::Start(start)).ok()?;
        file.read_exact(&mut buf).ok()?;
    }
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
            crate::storage::record_read(id, "cache", &bytes);
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
        let bytes = self.entry(id).and_then(|path| read_entry(&path));
        if let Some(bytes) = &bytes {
            crate::storage::record_read(id, "cache", bytes);
        }
        Ok(bytes)
    }

    /// Only the inner store reads by range: entries hold plaintext, which
    /// `get_local` already maps.
    fn get_local_ranged(
        &self,
        id: &ContentId,
    ) -> crate::error::Result<Option<std::sync::Arc<dyn crate::storage::RangeSource>>> {
        self.inner.get_local_ranged(id)
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
            crate::storage::record_read(id, "cache-range", &bytes);
            return Ok(bytes);
        }
        self.inner.get_range(id, range).await
    }

    fn supports_ranged_reads(&self) -> bool {
        self.inner.supports_ranged_reads()
    }

    fn costly_local_reads(&self) -> bool {
        self.inner.costly_local_reads()
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
        if fs::metadata(&path).is_ok_and(|meta| meta.len() > 0)
            || self.inner.get_local(id)?.is_some()
        {
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
        let scan = arm(&cache, "size-scan");

        // Returns with the scan still held: the size is not known yet.
        cache.best_effort_write(&dir.join("new.leaf"), &[0u8; 100]);
        assert!(dir.join("new.leaf").exists());
        assert_eq!(cache.state.lock().tracked_bytes, None);

        scan.release();
        wait_for_sizing(&cache);
        // The walk saw the new entry too; it is counted once.
        assert_eq!(cache.state.lock().tracked_bytes, Some(400));
    }

    /// A pause armed at a named point; see `pause_at`. Arm it inside the
    /// `thread::scope` whose thread it parks: if the test fails, dropping it
    /// fails that thread too, and the scope reports the failure.
    struct Pause {
        reached: std::sync::mpsc::Receiver<()>,
        go: std::sync::mpsc::Sender<bool>,
    }

    impl Pause {
        /// Wait for an operation to park here.
        fn reached(&self) {
            self.reached
                .recv_timeout(PAUSE_LIMIT)
                .expect("no operation reached the pause");
        }

        /// Let the parked operation go on.
        fn release(&self) {
            let _ = self.go.send(true);
        }

        /// Whether an operation has parked here.
        fn is_reached(&self) -> bool {
            self.reached.try_recv().is_ok()
        }

        /// Make the parked operation panic where it is.
        fn panic(&self) {
            let _ = self.go.send(false);
        }
    }

    /// Arm the pause at `point`: the operation that reaches it parks there.
    fn arm(cache: &DiskArtifactCache, point: &'static str) -> Pause {
        let (reached, at) = std::sync::mpsc::channel();
        let (go, wait) = std::sync::mpsc::channel();
        cache.state.lock().pauses.insert(point, (reached, wait));
        Pause { reached: at, go }
    }

    fn wait_for_sizing(cache: &DiskArtifactCache) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while cache.state.lock().sizing {
            assert!(
                std::time::Instant::now() < deadline,
                "size scan never landed"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Start the background size scan without a write.
    fn start_sizing(cache: &DiskArtifactCache) {
        let (_, size) = cache.state.lock().known_bytes();
        assert!(size, "the size is not known yet");
        cache.spawn_sizing();
    }

    /// The accounting matches the directory.
    fn assert_exact(cache: &DiskArtifactCache, dir: &Path, bytes: u64) {
        assert_eq!(directory_bytes(dir).unwrap(), bytes, "the directory");
        assert_eq!(
            cache.state.lock().tracked_bytes,
            Some(bytes),
            "the accounting"
        );
    }

    /// A size scan that walks after a write's file lands, and lands before
    /// the write is accounted, counts the file once.
    #[test]
    fn a_size_scan_landing_inside_a_write_counts_it_once() {
        let dir = temp_cache_dir("size-scan-in-write");
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        std::thread::scope(|scope| {
            let scan = arm(&cache, "size-scan");
            let written = arm(&cache, "written");
            scope.spawn(|| cache.best_effort_write(&dir.join("a.leaf"), &[0u8; 100]));
            scan.reached();
            written.reached();
            scan.release();
            wait_for_sizing(&cache);
            written.release();
        });
        assert_exact(&cache, &dir, 100);
    }

    /// A size scan that walks after an eviction's file is gone, and lands
    /// before the eviction is accounted, counts it gone once.
    #[test]
    fn a_size_scan_landing_inside_an_eviction_counts_it_once() {
        let dir = temp_cache_dir("size-scan-in-evict");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("gone.leaf"), [0u8; 300]).unwrap();
        fs::write(dir.join("kept.leaf"), [0u8; 500]).unwrap();
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        std::thread::scope(|scope| {
            let scan = arm(&cache, "size-scan");
            let evicted = arm(&cache, "evicted");
            start_sizing(&cache);
            scope.spawn(|| cache.evict_entry(&dir.join("gone.leaf")));
            scan.reached();
            evicted.reached();
            scan.release();
            wait_for_sizing(&cache);
            evicted.release();
        });
        assert_exact(&cache, &dir, 500);
    }

    /// A write that lands while `evict_until` rescans is kept: the rescan's
    /// total doesn't overwrite it.
    #[test]
    fn a_write_landing_inside_a_rescan_is_kept() {
        let dir = temp_cache_dir("write-in-rescan");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("kept.leaf"), [0u8; 500]).unwrap();
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        assert_eq!(cache.current_bytes().unwrap(), 500);
        std::thread::scope(|scope| {
            let rescan = arm(&cache, "rescan");
            scope.spawn(|| cache.evict_until(10_000).unwrap());
            rescan.reached();
            cache.best_effort_write(&dir.join("new.leaf"), &[0u8; 100]);
            rescan.release();
        });
        assert_exact(&cache, &dir, 600);
    }

    /// A rescan that walks after a write's file lands, and lands before the
    /// write is accounted, counts the file once.
    #[test]
    fn a_rescan_landing_inside_a_write_counts_it_once() {
        let dir = temp_cache_dir("rescan-in-write");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("kept.leaf"), [0u8; 500]).unwrap();
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        assert_eq!(cache.current_bytes().unwrap(), 500);
        std::thread::scope(|scope| {
            let written = arm(&cache, "written");
            scope.spawn(|| cache.best_effort_write(&dir.join("new.leaf"), &[0u8; 100]));
            written.reached();
            cache.evict_until(10_000).unwrap();
            written.release();
        });
        assert_exact(&cache, &dir, 600);
    }

    /// An eviction that lands while `evict_until` rescans is kept: the
    /// rescan's total doesn't count the entry back.
    #[test]
    fn an_eviction_landing_inside_a_rescan_is_kept() {
        let dir = temp_cache_dir("evict-in-rescan");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("gone.leaf"), [0u8; 300]).unwrap();
        fs::write(dir.join("kept.leaf"), [0u8; 500]).unwrap();
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        assert_eq!(cache.current_bytes().unwrap(), 800);
        std::thread::scope(|scope| {
            let rescan = arm(&cache, "rescan");
            scope.spawn(|| cache.evict_until(10_000).unwrap());
            rescan.reached();
            cache.evict_entry(&dir.join("gone.leaf"));
            rescan.release();
        });
        assert_exact(&cache, &dir, 500);
    }

    /// An entry removed around the cache after `evict_until`'s walk, which
    /// then finds it gone, isn't counted.
    #[test]
    fn an_entry_gone_before_a_rescan_removes_it_is_not_counted() {
        let dir = temp_cache_dir("gone-in-rescan");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("old.leaf"), [0u8; 300]).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        fs::write(dir.join("new.leaf"), [0u8; 500]).unwrap();
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        assert_eq!(cache.current_bytes().unwrap(), 800);
        std::thread::scope(|scope| {
            let rescan = arm(&cache, "rescan");
            scope.spawn(|| cache.evict_until(600).unwrap());
            rescan.reached();
            fs::remove_file(dir.join("old.leaf")).unwrap();
            rescan.release();
        });
        assert!(dir.join("new.leaf").exists());
        assert_exact(&cache, &dir, 500);
    }

    /// A rescan that panics mid-walk closes: operations stop being recorded
    /// for it.
    #[test]
    fn a_rescan_that_panics_is_closed() {
        let dir = temp_cache_dir("rescan-panics");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("kept.leaf"), [0u8; 500]).unwrap();
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        assert_eq!(cache.current_bytes().unwrap(), 500);
        let rescanned = std::thread::scope(|scope| {
            let rescan = arm(&cache, "rescan");
            let evict = scope.spawn(|| cache.evict_until(0));
            rescan.reached();
            rescan.panic();
            evict.join()
        });
        assert!(rescanned.is_err(), "the rescan should have panicked");
        assert_eq!(cache.state.lock().scans, 0, "the rescan is still open");
        cache.best_effort_write(&dir.join("new.leaf"), &[0u8; 100]);
        assert!(cache.state.lock().landed.is_empty());
    }

    /// A size scan that panics ends the sizing: the next write sizes the
    /// directory again, so the budget is enforced.
    #[test]
    fn a_size_scan_that_panics_is_closed() {
        let dir = temp_cache_dir("size-scan-panics");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("kept.leaf"), [0u8; 300]).unwrap();
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        let scan = arm(&cache, "size-scan");
        start_sizing(&cache);
        scan.reached();
        scan.panic();
        wait_for_sizing(&cache);
        assert_eq!(cache.state.lock().scans, 0, "the size scan is still open");
        assert_eq!(cache.state.lock().tracked_bytes, None);

        cache.best_effort_write(&dir.join("new.leaf"), &[0u8; 100]);
        wait_for_sizing(&cache);
        assert_exact(&cache, &dir, 400);
    }

    /// Two writes of one entry at once: the second finds the first under way
    /// and leaves the entry to it, so it is written and counted once.
    #[test]
    fn a_second_write_of_an_entry_under_way_is_left_to_the_first() {
        let dir = temp_cache_dir("write-twice");
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        assert_eq!(cache.current_bytes().unwrap(), 0);
        let target = dir.join("same.leaf");
        std::thread::scope(|scope| {
            let checked = arm(&cache, "checked");
            scope.spawn(|| cache.best_effort_write(&target, &[0u8; 100]));
            checked.reached();
            cache.best_effort_write(&target, &[0u8; 100]);
            checked.release();
        });
        assert_exact(&cache, &dir, 100);
    }

    /// A write of an entry already on disk changes nothing, so it doesn't
    /// hold the entry: an eviction meanwhile removes it.
    #[test]
    fn a_write_of_an_entry_on_disk_leaves_it_to_an_eviction() {
        let dir = temp_cache_dir("write-cached");
        fs::create_dir_all(&dir).unwrap();
        let target = dir.join("cached.leaf");
        fs::write(&target, [0u8; 100]).unwrap();
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        assert_eq!(cache.current_bytes().unwrap(), 100);
        std::thread::scope(|scope| {
            let registered = arm(&cache, "registered");
            let write = scope.spawn(|| cache.best_effort_write(&target, &[0u8; 100]));
            // A write that holds the entry parks once it does; one that
            // doesn't returns.
            let deadline = std::time::Instant::now() + PAUSE_LIMIT;
            while !write.is_finished() && !registered.is_reached() {
                assert!(std::time::Instant::now() < deadline, "the write hung");
                std::thread::sleep(Duration::from_millis(1));
            }
            cache.evict_entry(&target);
            registered.release();
        });
        assert!(
            !target.exists(),
            "the write kept the entry from the eviction"
        );
        assert_exact(&cache, &dir, 0);
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

    /// An entry removed between the walk listing it and reading its size is
    /// gone, not a failed walk.
    #[test]
    fn scan_skips_an_entry_removed_mid_walk() {
        let dir = temp_cache_dir("scan-vanished");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("gone.leaf"), [0u8; 300]).unwrap();
        fs::write(dir.join("kept.leaf"), [0u8; 500]).unwrap();
        ON_LISTED.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(|path: &Path| {
                if path.ends_with("gone.leaf") {
                    fs::remove_file(path).unwrap();
                }
            }));
        });
        let walked = scan_cache_entries(&dir);
        ON_LISTED.with(|hook| hook.borrow_mut().take());
        let entries = walked.expect("the walk failed on the removed entry");
        assert_eq!(walked_bytes(&entries), 500);
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

    /// A range read from an entry is sized from the entry, not the range:
    /// `mid..u64::MAX` reads to the end, and a range past the end is empty.
    #[tokio::test]
    async fn a_range_read_from_an_entry_is_clamped_to_it() {
        let dir = temp_cache_dir("wrapper-range");
        let data: Vec<u8> = (0..128).collect();
        let id = ContentId::new(crate::ContentKind::IndexLeaf, &data);
        let inner = Arc::new(CountingStore::remote(&data));
        let store = cached(Arc::clone(&inner), &dir);

        store.get(&id).await.unwrap();
        assert_eq!(
            store.get_range(&id, 100..u64::MAX).await.unwrap(),
            &data[100..]
        );
        assert!(store.get_range(&id, 200..300).await.unwrap().is_empty());
        assert_eq!(inner.gets(), 1, "both ranges came from the entry");
        let _ = fs::remove_dir_all(&dir);
    }

    /// An empty entry — what a crash can leave of one renamed into place
    /// without an fsync — is a miss, and the fetch it causes replaces it.
    #[tokio::test]
    async fn an_empty_entry_is_a_miss_the_next_fetch_replaces() {
        let dir = temp_cache_dir("wrapper-empty");
        let data = vec![3u8; 96];
        let id = ContentId::new(crate::ContentKind::IndexLeaf, &data);
        let inner = Arc::new(CountingStore::remote(&data));
        let store = cached(Arc::clone(&inner), &dir);
        let entry = dir.join(id.to_string());
        fs::create_dir_all(&dir).unwrap();
        fs::write(&entry, b"").unwrap();

        assert!(store.get_local(&id).unwrap().is_none());
        assert_eq!(store.get_range(&id, 0..8).await.unwrap(), &data[..8]);
        assert_eq!(store.get(&id).await.unwrap(), data);
        assert_eq!(fs::read(&entry).unwrap(), data, "the fetch rewrote it");
        assert_eq!(store.get_local(&id).unwrap().unwrap(), data);
        let _ = fs::remove_dir_all(&dir);
    }

    /// Only index artifacts get an entry. A commit is fetched through every
    /// time and never written, so a fold over a long history leaves the
    /// cache's leaves alone and keeps transaction data off local disk.
    #[tokio::test]
    async fn only_index_artifacts_are_written_to_the_cache() {
        let dir = temp_cache_dir("wrapper-kinds");
        let data = vec![4u8; 64];
        let commit = ContentId::new(crate::ContentKind::Commit, &data);
        let inner = Arc::new(CountingStore::remote(&data));
        let store = cached(Arc::clone(&inner), &dir);

        store.get(&commit).await.unwrap();
        store.keep_local(&commit, &data);
        store.prefetch(&commit).await.unwrap();
        assert!(store.get_local(&commit).unwrap().is_none());
        assert!(regular_files_under(&dir).is_empty(), "no commit entry");
        store.get(&commit).await.unwrap();
        assert_eq!(inner.gets(), 2, "each read fetches");

        let leaf = ContentId::new(crate::ContentKind::IndexLeaf, &data);
        store.get(&leaf).await.unwrap();
        assert_eq!(regular_files_under(&dir), vec![dir.join(leaf.to_string())]);
        let _ = fs::remove_dir_all(&dir);
    }

    /// `with_disk_cache` can't ask a `Permanent` store whether its bytes may
    /// sit on disk in plaintext, so it only coalesces the store's reads;
    /// `cached_permanent` opts one in.
    #[tokio::test]
    async fn a_permanent_store_is_disk_cached_only_by_opt_in() {
        use crate::StorageBackend;
        let dir = temp_cache_dir("wrapper-permanent");
        let data = vec![3u8; 64];
        let leaf = ContentId::new(crate::ContentKind::IndexLeaf, &data);
        let inner = Arc::new(CountingStore::remote(&data));
        let ns = crate::StorageNamespace::parse_legacy("ns:main").unwrap();

        let wrapped = StorageBackend::Permanent(Arc::clone(&inner) as _).with_disk_cache(&dir);
        wrapped.content_store(&ns).get(&leaf).await.unwrap();
        assert!(
            regular_files_under(&dir).is_empty(),
            "no disk copy by default"
        );

        let opted = StorageBackend::cached_permanent(Arc::clone(&inner) as _, &dir);
        opted.content_store(&ns).get(&leaf).await.unwrap();
        assert_eq!(regular_files_under(&dir), vec![dir.join(leaf.to_string())]);
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
