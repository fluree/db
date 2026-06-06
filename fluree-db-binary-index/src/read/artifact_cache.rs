use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use fluree_db_core::{ContentId, ContentStore};
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

/// Shared outcome of one in-flight remote fetch. Bytes are shared via `Arc` so
/// coalesced waiters neither re-fetch nor re-allocate the payload; the per-caller
/// `Vec` copy happens only at the API boundary. Errors are shared but never
/// cached — the in-flight entry is removed on completion so the next caller
/// retries (see [`DiskArtifactCache::coalesced_fetch`]).
type FlightResult = std::result::Result<Arc<[u8]>, Arc<io::Error>>;

/// A single in-flight fetch that concurrent callers for the same cache target
/// can wait on instead of issuing their own remote read.
#[derive(Debug)]
struct Flight {
    /// Generation token guarding removal against ABA: a stale guard (from a
    /// cancelled leader) must not evict a newer flight started for the same
    /// target by a different leader.
    generation: u64,
    /// Broadcast handle waiters `subscribe()` to; the leader sends exactly once.
    tx: broadcast::Sender<FlightResult>,
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
/// a newer flight for the same target.
struct FlightGuard {
    cache: Arc<DiskArtifactCache>,
    target: PathBuf,
    generation: u64,
}

impl Drop for FlightGuard {
    fn drop(&mut self) {
        self.cache.finish_flight(&self.target, self.generation);
    }
}

#[derive(Debug)]
pub(crate) struct DiskArtifactCache {
    root: PathBuf,
    budget_bytes: u64,
    state: Mutex<DiskArtifactCacheState>,
    /// Per-target single-flight coordination: coalesces concurrent remote
    /// fetches for the same cache target into one `cs.get` + one tmp-file write.
    /// Keyed by the resolved cache-target path (narrow: same content + same
    /// destination). Never held across `.await`.
    inflight: Mutex<HashMap<PathBuf, Flight>>,
    next_flight_generation: AtomicU64,
}

#[derive(Debug, Default)]
struct DiskArtifactCacheState {
    tracked_bytes: Option<u64>,
}

#[derive(Debug)]
struct CacheEntry {
    path: PathBuf,
    bytes: u64,
    modified: std::time::SystemTime,
}

fn storage_to_io_error(e: fluree_db_core::error::Error) -> io::Error {
    let kind = match &e {
        fluree_db_core::error::Error::NotFound(_) => io::ErrorKind::NotFound,
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

fn try_read_cached_bytes(path: &Path) -> io::Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
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
    pub(crate) fn for_dir(cache_dir: &Path) -> Arc<Self> {
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
                state: Mutex::new(DiskArtifactCacheState::default()),
                inflight: Mutex::new(HashMap::new()),
                next_flight_generation: AtomicU64::new(0),
            };
        }

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
                        "invalid FLUREE_DISK_CACHE_BUDGET_BYTES; falling back to auto-detect"
                    );
                    available
                        .saturating_mul(CACHE_BUDGET_NUMERATOR)
                        .saturating_div(CACHE_BUDGET_DENOMINATOR)
                }
            },
            Err(_) => available
                .saturating_mul(CACHE_BUDGET_NUMERATOR)
                .saturating_div(CACHE_BUDGET_DENOMINATOR),
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
            state: Mutex::new(DiskArtifactCacheState::default()),
            inflight: Mutex::new(HashMap::new()),
            next_flight_generation: AtomicU64::new(0),
        }
    }

    #[cfg(test)]
    fn with_budget(root: PathBuf, budget_bytes: u64) -> Self {
        fs::create_dir_all(&root).expect("create test cache dir");
        Self {
            root,
            budget_bytes,
            state: Mutex::new(DiskArtifactCacheState::default()),
            inflight: Mutex::new(HashMap::new()),
            next_flight_generation: AtomicU64::new(0),
        }
    }

    fn low_water_mark(&self) -> u64 {
        self.budget_bytes
            .saturating_mul(CACHE_EVICT_NUMERATOR)
            .saturating_div(CACHE_EVICT_DENOMINATOR)
    }

    fn current_bytes(&self) -> io::Result<u64> {
        let mut state = self.state.lock();
        if let Some(bytes) = state.tracked_bytes {
            return Ok(bytes);
        }
        let bytes = scan_cache_entries(&self.root)?
            .into_iter()
            .fold(0u64, |acc, entry| acc.saturating_add(entry.bytes));
        state.tracked_bytes = Some(bytes);
        Ok(bytes)
    }

    fn set_current_bytes(&self, bytes: u64) {
        self.state.lock().tracked_bytes = Some(bytes);
    }

    fn note_write(&self, bytes: u64) {
        let mut state = self.state.lock();
        let current = state.tracked_bytes.unwrap_or(0);
        state.tracked_bytes = Some(current.saturating_add(bytes));
    }

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

        let current = self.current_bytes()?;
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

    pub(crate) fn best_effort_write(&self, target: &Path, bytes: &[u8]) {
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
            Ok(true) => self.note_write(bytes.len() as u64),
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
                    Ok(true) => self.note_write(bytes.len() as u64),
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

    /// Remove the in-flight entry for `target` iff it is still the flight with
    /// `generation`. The generation check keeps removal ABA-safe: a stale guard
    /// from a cancelled leader must not evict a newer flight a different leader
    /// started for the same target.
    fn finish_flight(&self, target: &Path, generation: u64) {
        let mut map = self.inflight.lock();
        if map.get(target).is_some_and(|f| f.generation == generation) {
            map.remove(target);
        }
    }

    /// Coalesce concurrent remote fetches that target the same cache path so
    /// only ONE `fetch` runs per `target` at a time; other callers await the
    /// shared result instead of each issuing their own S3 GET and tmp-file
    /// write. This is process-local (it does not coordinate across containers).
    ///
    /// `fetch` is the leader's remote read (e.g. `cs.get(id)` mapped to io).
    /// Only the leader runs it. After winning the flight the leader double-
    /// checks disk (a just-finished prior flight may have written the file),
    /// then on a miss fetches once, writes the cache atomically, and wakes
    /// waiters with the shared bytes.
    ///
    /// Safety properties:
    /// - the in-flight map lock is never held across `.await`;
    /// - the slot is cleared on completion *and* on drop, so a cancelled or
    ///   panicked leader cannot orphan it (waiters then observe a closed channel
    ///   and retry rather than hang);
    /// - errors are propagated to current waiters but never cached — the slot is
    ///   gone by then, so the next caller retries.
    async fn coalesced_fetch<F, Fut>(
        self: &Arc<Self>,
        target: PathBuf,
        fetch: F,
    ) -> io::Result<Vec<u8>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = io::Result<Vec<u8>>>,
    {
        loop {
            // Decide leader vs waiter under the lock; release it before awaiting.
            let role = {
                let mut map = self.inflight.lock();
                match map.get(&target) {
                    Some(flight) => FlightRole::Waiter(flight.tx.subscribe()),
                    None => {
                        let (tx, _rx) = broadcast::channel(1);
                        let generation =
                            self.next_flight_generation.fetch_add(1, Ordering::Relaxed);
                        map.insert(
                            target.clone(),
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
                    // evicts a newer flight for the same target.
                    let guard = FlightGuard {
                        cache: Arc::clone(self),
                        target: target.clone(),
                        generation,
                    };

                    // Double-check disk after winning: a prior flight for the
                    // same target may have just completed its atomic write.
                    //
                    // The disk re-check is an OPTIMIZATION only. A cache miss
                    // (`Ok(None)`) OR a transient read error (`Err`, e.g. EIO /
                    // fd exhaustion on the local file) both fall through to the
                    // authoritative remote fetch — we must not let one caller's
                    // disk hiccup broadcast a failure to the whole coalesced
                    // batch, since the fetch path can satisfy everyone.
                    let outcome: io::Result<Vec<u8>> = match try_read_cached_bytes(&target) {
                        Ok(Some(bytes)) => Ok(bytes),
                        Ok(None) | Err(_) => match fetch().await {
                            Ok(bytes) => {
                                self.best_effort_write(&target, &bytes);
                                Ok(bytes)
                            }
                            Err(err) => Err(err),
                        },
                    };

                    // Clear the slot before waking waiters so callers arriving
                    // after this point start a fresh flight (and hit the now-
                    // written cache) instead of subscribing to a finished one.
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
}

pub fn best_effort_cache_bytes_to_path(cache_dir: &Path, target: &Path, bytes: &[u8]) {
    DiskArtifactCache::for_dir(cache_dir).best_effort_write(target, bytes);
}

pub async fn fetch_cached_bytes(
    cs: &dyn ContentStore,
    id: &ContentId,
    cache_dir: &Path,
    ext: &str,
) -> io::Result<Vec<u8>> {
    let cache = DiskArtifactCache::for_dir(cache_dir);
    let cached = cache_dir.join(format!("{}.{}", id.digest_hex(), ext));

    if let Some(local_path) = cs.resolve_local_path(id) {
        if let Some(bytes) = try_read_cached_bytes(&local_path)? {
            return Ok(bytes);
        }
        tracing::debug!(
            path = %local_path.display(),
            "local artifact path disappeared during read; falling back to remote fetch"
        );
        return cache
            .coalesced_fetch(cached, || async {
                cs.get(id).await.map_err(storage_to_io_error)
            })
            .await;
    }

    if let Some(bytes) = try_read_cached_bytes(&cached)? {
        return Ok(bytes);
    }
    cache
        .coalesced_fetch(cached, || async {
            cs.get(id).await.map_err(storage_to_io_error)
        })
        .await
}

pub async fn fetch_cached_bytes_cid(
    cs: &dyn ContentStore,
    id: &ContentId,
    cache_dir: &Path,
) -> io::Result<Vec<u8>> {
    let cache = DiskArtifactCache::for_dir(cache_dir);
    let cached = cache_dir.join(id.to_string());

    if let Some(local_path) = cs.resolve_local_path(id) {
        if let Some(bytes) = try_read_cached_bytes(&local_path)? {
            return Ok(bytes);
        }
        tracing::debug!(
            path = %local_path.display(),
            "local artifact path disappeared during read; falling back to remote fetch"
        );
        return cache
            .coalesced_fetch(cached, || async {
                cs.get(id).await.map_err(storage_to_io_error)
            })
            .await;
    }

    if let Some(bytes) = try_read_cached_bytes(&cached)? {
        return Ok(bytes);
    }
    cache
        .coalesced_fetch(cached, || async {
            cs.get(id).await.map_err(storage_to_io_error)
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

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

    /// Counting content store: records `get` calls and can delay to widen the
    /// concurrency window.
    #[derive(Debug)]
    struct CountingStore {
        data: Vec<u8>,
        gets: Arc<AtomicUsize>,
        delay: Duration,
    }

    #[async_trait::async_trait]
    impl ContentStore for CountingStore {
        async fn has(&self, _id: &ContentId) -> fluree_db_core::error::Result<bool> {
            Ok(true)
        }
        async fn get(&self, _id: &ContentId) -> fluree_db_core::error::Result<Vec<u8>> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            Ok(self.data.clone())
        }
        async fn put(
            &self,
            _kind: fluree_db_core::ContentKind,
            _bytes: &[u8],
        ) -> fluree_db_core::error::Result<ContentId> {
            unimplemented!("put not needed for cache tests")
        }
        async fn put_with_id(
            &self,
            _id: &ContentId,
            _bytes: &[u8],
        ) -> fluree_db_core::error::Result<()> {
            unimplemented!("put_with_id not needed for cache tests")
        }
        async fn release(&self, _id: &ContentId) -> fluree_db_core::error::Result<()> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fetch_cached_bytes_cid_coalesces_concurrent_callers() {
        let dir = temp_cache_dir("e2e-coalesce");
        let data = vec![42u8; 256];
        let id = ContentId::new(fluree_db_core::ContentKind::IndexRoot, &data);
        let gets = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(CountingStore {
            data: data.clone(),
            gets: Arc::clone(&gets),
            delay: Duration::from_millis(100),
        });

        let mut handles = Vec::new();
        for _ in 0..8 {
            let store = Arc::clone(&store);
            let dir = dir.clone();
            let id = id.clone();
            handles.push(tokio::spawn(async move {
                fetch_cached_bytes_cid(store.as_ref(), &id, &dir).await
            }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap().unwrap(), data);
        }
        // End to end: eight concurrent callers, one underlying S3 GET.
        assert_eq!(
            gets.load(Ordering::SeqCst),
            1,
            "concurrent callers for the same CID should coalesce into one get"
        );
        // Subsequent calls hit the disk cache (still one get total).
        assert_eq!(
            fetch_cached_bytes_cid(store.as_ref(), &id, &dir)
                .await
                .unwrap(),
            data
        );
        assert_eq!(gets.load(Ordering::SeqCst), 1);
    }
}
