//! Multi-pack reader for forward dictionary packs.
//!
//! `ForwardPackReader` manages one or more `FPK1` packs and routes lookups
//! to the correct pack via binary search on ID ranges.
//!
//! ## Loading
//!
//! - **`from_pack_refs`**: Async constructor. Resolves locally available packs
//!   immediately; defers remote packs to lazy fetch on first lookup.
//! - **`from_memory`**: In-memory constructor for testing.

#[cfg(target_arch = "wasm32")]
use crate::wasm_compat::memmap2;
use std::io;
use std::path::{Path, PathBuf};
#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use once_cell::sync::OnceCell;

use super::forward_pack::{lookup_in_pack, parse_pack_meta, ParsedPackMeta};
use crate::format::wire_helpers::PackBranchEntry;
use fluree_db_core::{ContentId, ContentStore};

/// Global atomic counter for unique temp file names (avoids collisions
/// across concurrent pack fetches within the same process).
#[cfg(not(target_arch = "wasm32"))]
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

// ============================================================================
// PackHandle — owns routing info + backing store for a single pack
// ============================================================================

struct PackHandle {
    /// ID range from the root routing table (always known, even for lazy packs).
    first_id: u64,
    last_id: u64,
    inner: PackInner,
}

enum PackInner {
    /// Pack is fully loaded: metadata parsed, backing bytes available.
    Loaded {
        meta: ParsedPackMeta,
        backing: LoadedBacking,
    },
    /// Pack deferred: will be fetched from CAS on first lookup.
    Lazy {
        pack_cid: ContentId,
        cache_path: PathBuf,
        loaded: OnceCell<LazyLoaded>,
    },
}

enum LoadedBacking {
    Mmap(memmap2::Mmap),
    InMemory(Arc<[u8]>),
}

struct LazyLoaded {
    meta: ParsedPackMeta,
    backing: LoadedBacking,
}

impl LoadedBacking {
    fn bytes(&self) -> &[u8] {
        match self {
            LoadedBacking::Mmap(mmap) => mmap.as_ref(),
            LoadedBacking::InMemory(bytes) => bytes.as_ref(),
        }
    }
}

impl PackHandle {
    /// Get pre-parsed metadata and raw bytes. For lazy packs, triggers
    /// fetch + cache + load + parse on first call (subsequent calls return cached).
    fn ensure_loaded(&self, ctx: Option<&LoadContext>) -> io::Result<(&ParsedPackMeta, &[u8])> {
        match &self.inner {
            PackInner::Loaded { meta, backing } => Ok((meta, backing.bytes())),
            PackInner::Lazy {
                pack_cid,
                cache_path,
                loaded,
            } => {
                let ctx = ctx.ok_or_else(|| io::Error::other("lazy pack without load context"))?;
                let lazy = loaded.get_or_try_init(|| {
                    fetch_and_load(self.first_id, self.last_id, pack_cid, cache_path, ctx)
                })?;
                Ok((&lazy.meta, lazy.backing.bytes()))
            }
        }
    }
}

/// Shared loading context. Stored once on `ForwardPackReader`.
struct LoadContext {
    cs: Arc<dyn ContentStore>,
    expected_kind: u8,
    expected_ns_code: u16,
}

// ============================================================================
// ForwardPackReader
// ============================================================================

/// Multi-pack reader for forward dictionary lookups.
///
/// Manages one or more `FPK1` packs, sorted by ID range. Lookups binary-search
/// the packs, then use pre-parsed metadata for zero-alloc page navigation.
///
/// Locally available packs are eagerly loaded at construction. Remote packs
/// are lazily fetched on first lookup.
pub struct ForwardPackReader {
    packs: Vec<PackHandle>,
    /// Shared loading context. `None` for in-memory readers.
    load_ctx: Option<LoadContext>,
}

impl ForwardPackReader {
    /// Load packs from CAS. Does not perform remote fetches.
    ///
    /// For each `PackBranchEntry`:
    /// 1. If `cs.resolve_local_path(&cid)` returns a path → load + validate → `Loaded`.
    /// 2. Else if cache file exists → load + validate → `Loaded`.
    /// 3. Else → `Lazy` handle (fetched + cached + loaded on first lookup).
    ///
    /// "Load" is `read()` for small packs and `mmap` for large ones — see
    /// [`DEFAULT_MMAP_MIN_BYTES`]. This path is eager over EVERY pack in the
    /// routing table, so it is where a mapping-per-pack becomes a hard cap on
    /// how many ledgers a process can hold open at once.
    ///
    /// Loaded packs are validated: ID range must match the routing entry, and
    /// `kind`/`ns_code` must match `expected_kind`/`expected_ns_code`. Lazy packs
    /// are validated on first fetch.
    pub async fn from_pack_refs(
        cs: Arc<dyn ContentStore>,
        cache_dir: &Path,
        refs: &[PackBranchEntry],
        expected_kind: u8,
        expected_ns_code: u16,
    ) -> io::Result<Self> {
        // Pre-create cache directory once.
        if !refs.is_empty() {
            std::fs::create_dir_all(cache_dir).map_err(|e| {
                io::Error::other(format!("create cache dir {}: {}", cache_dir.display(), e))
            })?;
        }

        let mut packs = Vec::with_capacity(refs.len());

        for entry in refs {
            let cache_name = format!("{}.fpk", entry.pack_cid.digest_hex());
            let cache_path = cache_dir.join(&cache_name);

            let local_path = cs.resolve_local_path(&entry.pack_cid);

            if let Some(path) = local_path {
                // Local CAS path.
                let backing = load_pack_backing(&path)?;
                let meta = parse_pack_meta(backing.bytes())?;
                validate_meta(&meta, entry, expected_kind, expected_ns_code)?;
                packs.push(PackHandle {
                    first_id: entry.first_id,
                    last_id: entry.last_id,
                    inner: PackInner::Loaded { meta, backing },
                });
            } else if cache_path.exists() {
                // Cached on disk.
                let backing = load_pack_backing(&cache_path)?;
                let meta = parse_pack_meta(backing.bytes())?;
                validate_meta(&meta, entry, expected_kind, expected_ns_code)?;
                packs.push(PackHandle {
                    first_id: entry.first_id,
                    last_id: entry.last_id,
                    inner: PackInner::Loaded { meta, backing },
                });
            } else {
                // Remote — defer to lazy fetch on first lookup.
                packs.push(PackHandle {
                    first_id: entry.first_id,
                    last_id: entry.last_id,
                    inner: PackInner::Lazy {
                        pack_cid: entry.pack_cid.clone(),
                        cache_path,
                        loaded: OnceCell::new(),
                    },
                });
            }
        }

        // Sort by first_id (should already be sorted, but enforce).
        packs.sort_by_key(|p| p.first_id);
        validate_pack_routing(&packs)?;

        Ok(Self {
            packs,
            load_ctx: Some(LoadContext {
                cs,
                expected_kind,
                expected_ns_code,
            }),
        })
    }

    /// Create from pre-built in-memory pack bytes (for testing).
    pub fn from_memory(pack_bytes_list: Vec<Arc<[u8]>>) -> io::Result<Self> {
        let mut packs = Vec::with_capacity(pack_bytes_list.len());

        for bytes in pack_bytes_list {
            let meta = parse_pack_meta(&bytes)?;
            packs.push(PackHandle {
                first_id: meta.first_id,
                last_id: meta.last_id,
                inner: PackInner::Loaded {
                    meta,
                    backing: LoadedBacking::InMemory(bytes),
                },
            });
        }

        packs.sort_by_key(|p| p.first_id);
        validate_pack_routing(&packs)?;

        Ok(Self {
            packs,
            load_ctx: None,
        })
    }

    /// Create an empty reader (no packs).
    pub fn empty() -> Self {
        Self {
            packs: Vec::new(),
            load_ctx: None,
        }
    }

    /// Number of packs in this reader.
    pub fn pack_count(&self) -> usize {
        self.packs.len()
    }

    /// Pre-warm forward-dict pack pages into the OS page cache, up to
    /// `budget_bytes` total across this reader's packs. Returns the number of
    /// bytes touched.
    ///
    /// Locally-mmapped packs fault their pages in lazily (on first query); this
    /// touches one byte per page so the first dictionary lookups after startup
    /// don't pay that cold-fault I/O. Lazy (remote, not-yet-fetched) packs are
    /// fetched + cached + mmapped first. A pack larger than the remaining budget
    /// is partially warmed; warming stops once the budget is exhausted.
    ///
    /// This blocks on page faults / sequential reads — call it from a blocking
    /// context (e.g. `tokio::task::spawn_blocking`), never on the hot async path.
    /// Warming is best-effort: a pack that fails to load is skipped, never fatal.
    pub fn prewarm(&self, budget_bytes: u64) -> u64 {
        let mut warmed: u64 = 0;
        for pack in &self.packs {
            if warmed >= budget_bytes {
                break;
            }
            let bytes = match pack.ensure_loaded(self.load_ctx.as_ref()) {
                Ok((_meta, bytes)) => bytes,
                Err(_) => continue,
            };
            let remaining = budget_bytes - warmed;
            let take = (bytes.len() as u64).min(remaining) as usize;
            warmed += touch_pages(&bytes[..take]);
        }
        warmed
    }

    /// Hot-path: append value bytes to `out`. Returns `true` if the ID was found.
    ///
    /// Zero-alloc steady state: uses pre-parsed page directory for binary search,
    /// then O(1) offset indexing within the page.
    pub fn forward_lookup_into(&self, id: u64, out: &mut Vec<u8>) -> io::Result<bool> {
        let Some(handle) = self.find_pack(id) else {
            return Ok(false);
        };
        let (meta, bytes) = handle.ensure_loaded(self.load_ctx.as_ref())?;
        match lookup_in_pack(bytes, meta, id) {
            Some(value) => {
                out.extend_from_slice(value);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Convenience: look up and return as a `String`.
    pub fn forward_lookup_str(&self, id: u64) -> io::Result<Option<String>> {
        let Some(handle) = self.find_pack(id) else {
            return Ok(None);
        };
        let (meta, bytes) = handle.ensure_loaded(self.load_ctx.as_ref())?;
        match lookup_in_pack(bytes, meta, id) {
            Some(value) => {
                let s = std::str::from_utf8(value)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(Some(s.to_string()))
            }
            None => Ok(None),
        }
    }

    /// Binary search packs by ID to find the one containing `id`.
    fn find_pack(&self, id: u64) -> Option<&PackHandle> {
        let idx = self.packs.partition_point(|p| p.first_id <= id);
        if idx == 0 {
            return None;
        }
        let candidate = &self.packs[idx - 1];
        if id <= candidate.last_id {
            Some(candidate)
        } else {
            None
        }
    }
}

impl std::fmt::Debug for ForwardPackReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForwardPackReader")
            .field("pack_count", &self.packs.len())
            .field("has_load_ctx", &self.load_ctx.is_some())
            .finish()
    }
}

// ============================================================================
// Validation
// ============================================================================

/// Validate that pack handles have strictly increasing, non-overlapping ID ranges.
fn validate_pack_routing(packs: &[PackHandle]) -> io::Result<()> {
    for i in 1..packs.len() {
        let prev_last = packs[i - 1].last_id;
        let curr_first = packs[i].first_id;
        if curr_first <= prev_last {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "pack routing: pack {i} first_id {curr_first} overlaps with previous last_id {prev_last}"
                ),
            ));
        }
    }
    Ok(())
}

/// Validate parsed pack metadata against the root routing entry and expected kind/ns_code.
fn validate_meta(
    meta: &ParsedPackMeta,
    entry: &PackBranchEntry,
    expected_kind: u8,
    expected_ns_code: u16,
) -> io::Result<()> {
    if meta.first_id != entry.first_id || meta.last_id != entry.last_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "pack header range [{}, {}] doesn't match root routing entry [{}, {}]",
                meta.first_id, meta.last_id, entry.first_id, entry.last_id,
            ),
        ));
    }
    if meta.kind != expected_kind {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "pack kind {} doesn't match expected {}",
                meta.kind, expected_kind,
            ),
        ));
    }
    if meta.ns_code != expected_ns_code {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "pack ns_code {} doesn't match expected {}",
                meta.ns_code, expected_ns_code,
            ),
        ));
    }
    Ok(())
}

// ============================================================================
// Lazy fetch
// ============================================================================

/// Fetch a pack from CAS, write to cache, mmap, parse, and validate.
///
/// Uses `thread::spawn` + `Handle::block_on` to bridge the sync lookup API
/// with the async `ContentStore::get`. This pattern works on both single-thread
/// and multi-thread Tokio runtimes (unlike `block_in_place` which requires
/// multi-thread).
/// wasm32 lazy pack load: no filesystem and no sync→async bridge — serve the
/// content store's residency tier (`resolve_cached_bytes`) with shared,
/// zero-copy backing, surfacing a typed
/// [`crate::read::need_fetch::NeedFetch`] miss for an async caller to fetch
/// and retry. The surrounding `OnceCell` makes a successful load one-time,
/// exactly as on native.
#[cfg(target_arch = "wasm32")]
fn fetch_and_load(
    expected_first_id: u64,
    expected_last_id: u64,
    pack_cid: &ContentId,
    cache_path: &Path,
    ctx: &LoadContext,
) -> io::Result<LazyLoaded> {
    let _ = cache_path;
    let bytes = crate::read::need_fetch::resident_or_need_fetch(
        ctx.cs.as_ref(),
        pack_cid,
        crate::read::need_fetch::FetchKind::ForwardPack,
    )?;
    let backing = LoadedBacking::InMemory(bytes);
    let meta = parse_pack_meta(backing.bytes())?;
    validate_lazy_meta(&meta, expected_first_id, expected_last_id, ctx)?;
    Ok(LazyLoaded { meta, backing })
}

#[cfg(not(target_arch = "wasm32"))]
fn fetch_and_load(
    expected_first_id: u64,
    expected_last_id: u64,
    pack_cid: &ContentId,
    cache_path: &Path,
    ctx: &LoadContext,
) -> io::Result<LazyLoaded> {
    // Fast paths: check if something appeared since construction.
    if let Some(path) = ctx.cs.resolve_local_path(pack_cid) {
        let backing = load_pack_backing(&path)?;
        let meta = parse_pack_meta(backing.bytes())?;
        validate_lazy_meta(&meta, expected_first_id, expected_last_id, ctx)?;
        return Ok(LazyLoaded { meta, backing });
    }
    if cache_path.exists() {
        let backing = load_pack_backing(cache_path)?;
        let meta = parse_pack_meta(backing.bytes())?;
        validate_lazy_meta(&meta, expected_first_id, expected_last_id, ctx)?;
        return Ok(LazyLoaded { meta, backing });
    }

    // Remote fetch: bridge the sync lookup to the async CAS get via the shared
    // `run_sync_on_runtime` helper. It uses `block_in_place(handle.block_on)`
    // on a multi-thread runtime (a replacement worker keeps driving the
    // reactor while this thread blocks) and a process-wide helper runtime when
    // needed. The previous `thread::spawn` + outer-`Handle::block_on`
    // + `.join()` re-injected the fetch onto the OUTER runtime with no
    // `block_in_place`, which can wedge a small (e.g. 2-worker) runtime under
    // query fan-out (every worker parked with no thread driving the reactor).
    let cs = Arc::clone(&ctx.cs);
    let cid = pack_cid.clone();
    let timeout = crate::read::binary_index_store::cas_sync_timeout();
    let bytes = crate::read::binary_index_store::run_sync_on_runtime(async move {
        let fetch = async {
            cs.get(&cid)
                .await
                .map_err(|e| io::Error::other(e.to_string()))
        };
        // Optional per-fetch ceiling (FLUREE_CAS_SYNC_TIMEOUT_MS): a stalled
        // pack fetch self-aborts instead of blocking.
        match timeout {
            Some(dur) => tokio::time::timeout(dur, fetch).await.map_err(|_| {
                io::Error::other(format!(
                    "forward pack CAS fetch timed out after {}ms",
                    dur.as_millis()
                ))
            })?,
            None => fetch.await,
        }
    })
    .map_err(|e| {
        tracing::debug!(
            cid = %pack_cid,
            cache_path = %cache_path.display(),
            first_id = expected_first_id,
            last_id = expected_last_id,
            error = %e,
            "remote lazy fetch for forward pack failed"
        );
        io::Error::other(format!("lazy pack fetch: {e}"))
    })?;

    // Write to cache, then re-open it. Re-opening rather than keeping `bytes`
    // is deliberate: `load_pack_backing` is the single place that decides
    // read-vs-mmap, so a large pack still ends up mapped (no heap duplication)
    // and a small one still ends up on the heap, without duplicating the
    // threshold logic here.
    atomic_write_to_cache(cache_path, &bytes)?;
    drop(bytes);

    let backing = load_pack_backing(cache_path)?;
    let meta = parse_pack_meta(backing.bytes())?;
    validate_lazy_meta(&meta, expected_first_id, expected_last_id, ctx)?;
    Ok(LazyLoaded { meta, backing })
}

/// Validate metadata for a lazily loaded pack (same checks as eager, but using
/// the expected range stored on the handle rather than a `PackBranchEntry`).
fn validate_lazy_meta(
    meta: &ParsedPackMeta,
    expected_first: u64,
    expected_last: u64,
    ctx: &LoadContext,
) -> io::Result<()> {
    if meta.first_id != expected_first || meta.last_id != expected_last {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "pack header range [{}, {}] doesn't match root routing [{}, {}]",
                meta.first_id, meta.last_id, expected_first, expected_last,
            ),
        ));
    }
    if meta.kind != ctx.expected_kind {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "pack kind {} doesn't match expected {}",
                meta.kind, ctx.expected_kind,
            ),
        ));
    }
    if meta.ns_code != ctx.expected_ns_code {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "pack ns_code {} doesn't match expected {}",
                meta.ns_code, ctx.expected_ns_code,
            ),
        ));
    }
    Ok(())
}

// ============================================================================
// Helpers
// ============================================================================

/// Packs at or below this many bytes are `read()` into the heap instead of
/// being mapped. Override with `FLUREE_DICT_PACK_MMAP_MIN_BYTES`; 0 restores
/// the old always-mmap behaviour.
///
/// **A mapping is a scarcer resource than the bytes it exposes.** Every mmap
/// costs a VMA, and a process is hard-capped at `vm.max_map_count` (65,530 by
/// default) *regardless of how much memory is free* — past it `mmap` returns
/// ENOMEM, which surfaces here as "failed to load binary index: Cannot allocate
/// memory (os error 12)" on a host with gigabytes idle. That is not a
/// theoretical limit: dict packs are per-ID-range and never compacted, so a
/// ledger's routing table grows without bound. Measured on one deployment,
/// 23 ledgers held **103,426 packs** and the process carried **47,336
/// mappings** against the 65,530 cap — every ledger load pushing it closer, and
/// raising the container's memory limit doing nothing at all because bytes were
/// never the constraint.
///
/// The size split works because pack sizes are extremely skewed: on that same
/// deployment **91% of packs were under 4 KiB and 99.8% of the mapped ones were
/// under 64 KiB, holding 30 MB between them.** Mapping a 113-byte file (the
/// median!) spends a VMA and a whole page of address space to expose less than
/// a cache line's worth of useful data. So this trades ~30 MB of heap for
/// ~47,000 mappings, and the large packs that actually justify demand paging —
/// 5,728 files holding 12.1 of the 12.5 GiB — still get mapped.
const DEFAULT_MMAP_MIN_BYTES: u64 = 64 * 1024;

fn mmap_min_bytes() -> u64 {
    static CACHED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("FLUREE_DICT_PACK_MMAP_MIN_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_MMAP_MIN_BYTES)
    })
}

/// Open a pack: heap-read when small, mmap when large. See
/// [`DEFAULT_MMAP_MIN_BYTES`] for why the small case is the important one.
fn load_pack_backing(path: &Path) -> io::Result<LoadedBacking> {
    let file = std::fs::File::open(path).map_err(|e| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("open pack file {}: {}", path.display(), e),
        )
    })?;

    // A failed `metadata()` falls through to mmap rather than erroring: the
    // threshold is an optimisation, so losing the size must not lose the read.
    let small = match file.metadata() {
        Ok(meta) => meta.len() <= mmap_min_bytes(),
        Err(_) => false,
    };

    if small {
        // `read_to_end` on a fresh Vec, not `fs::read`, so the already-open
        // handle is reused and the path is not resolved twice (a GC/promotion
        // unlink between the two would turn a live pack into NotFound).
        let mut bytes = Vec::new();
        {
            use std::io::Read;
            let mut file = file;
            file.read_to_end(&mut bytes)?;
        }
        return Ok(LoadedBacking::InMemory(Arc::from(bytes)));
    }

    // SAFETY: The file is an immutable CAS artifact, not concurrently modified.
    Ok(LoadedBacking::Mmap(unsafe { memmap2::Mmap::map(&file)? }))
}

/// Page size used to stride `touch_pages`. 4 KiB is the smallest common page
/// size; reading one byte per 4 KiB faults every page on hosts with larger
/// pages too (just with redundant in-page reads), so warming stays correct
/// without querying the OS page size.
const WARM_PAGE_STRIDE: usize = 4096;

/// Fault `bytes` resident by reading one byte per page, returning the number of
/// bytes covered (i.e. `bytes.len()`).
///
/// For an mmap-backed slice this pulls each page into the OS page cache; for an
/// already-resident in-memory slice it is a cheap strided read. The accumulator
/// is fed to [`std::hint::black_box`] so the reads are not optimized away.
fn touch_pages(bytes: &[u8]) -> u64 {
    let mut acc: u8 = 0;
    let mut i = 0;
    while i < bytes.len() {
        acc ^= bytes[i];
        i += WARM_PAGE_STRIDE;
    }
    std::hint::black_box(acc);
    bytes.len() as u64
}

/// Write bytes to a cache file atomically (temp file + rename).
///
/// Ensures the parent directory exists so lazy fetches succeed even if the
/// cache directory was removed between construction and first lookup.
#[cfg(not(target_arch = "wasm32"))]
fn atomic_write_to_cache(cache_path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = cache_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = cache_path.with_extension(format!(
        "tmp.{}.{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&tmp, bytes)?;
    match std::fs::rename(&tmp, cache_path) {
        Ok(()) => Ok(()),
        Err(_) if cache_path.exists() => {
            // Another process won the race — discard our tmp and use theirs.
            let _ = std::fs::remove_file(&tmp);
            Ok(())
        }
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            Err(e)
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dict::forward_pack::{encode_forward_pack, KIND_STRING_FWD};
    use crate::format::wire_helpers::PackBranchEntry;
    use fluree_db_core::content_kind::DictKind;
    use fluree_db_core::{ContentKind, MemoryContentStore};

    fn make_pack_bytes(first: u64, count: usize) -> Vec<u8> {
        let entries: Vec<(u64, Vec<u8>)> = (0..count)
            .map(|i| {
                let id = first + i as u64;
                (id, format!("val_{id}").into_bytes())
            })
            .collect();
        let refs: Vec<(u64, &[u8])> = entries.iter().map(|(id, v)| (*id, v.as_slice())).collect();
        encode_forward_pack(&refs, KIND_STRING_FWD, 0, 256 * 1024).unwrap()
    }

    #[test]
    fn test_single_pack_memory() {
        let bytes = make_pack_bytes(0, 100);
        let reader =
            ForwardPackReader::from_memory(vec![Arc::from(bytes.into_boxed_slice())]).unwrap();

        assert_eq!(reader.pack_count(), 1);

        // Hit
        assert_eq!(
            reader.forward_lookup_str(50).unwrap(),
            Some("val_50".to_string())
        );

        // Miss
        assert_eq!(reader.forward_lookup_str(100).unwrap(), None);
    }

    #[test]
    fn test_multi_pack_memory() {
        let pack1 = make_pack_bytes(0, 100);
        let pack2 = make_pack_bytes(100, 100);
        let pack3 = make_pack_bytes(200, 50);

        let reader = ForwardPackReader::from_memory(vec![
            Arc::from(pack1.into_boxed_slice()),
            Arc::from(pack2.into_boxed_slice()),
            Arc::from(pack3.into_boxed_slice()),
        ])
        .unwrap();

        assert_eq!(reader.pack_count(), 3);

        // First pack
        assert_eq!(
            reader.forward_lookup_str(0).unwrap(),
            Some("val_0".to_string())
        );
        assert_eq!(
            reader.forward_lookup_str(99).unwrap(),
            Some("val_99".to_string())
        );

        // Second pack
        assert_eq!(
            reader.forward_lookup_str(100).unwrap(),
            Some("val_100".to_string())
        );

        // Third pack
        assert_eq!(
            reader.forward_lookup_str(249).unwrap(),
            Some("val_249".to_string())
        );

        // Out of range
        assert_eq!(reader.forward_lookup_str(250).unwrap(), None);
    }

    #[test]
    fn test_lookup_into() {
        let bytes = make_pack_bytes(0, 10);
        let reader =
            ForwardPackReader::from_memory(vec![Arc::from(bytes.into_boxed_slice())]).unwrap();

        let mut out = Vec::new();
        assert!(reader.forward_lookup_into(5, &mut out).unwrap());
        assert_eq!(out, b"val_5");

        assert!(!reader.forward_lookup_into(999, &mut out).unwrap());
        assert_eq!(out.len(), 5); // unchanged
    }

    #[test]
    fn test_empty_reader() {
        let reader = ForwardPackReader::empty();
        assert_eq!(reader.pack_count(), 0);
        assert_eq!(reader.forward_lookup_str(0).unwrap(), None);
    }

    #[test]
    fn test_gap_between_packs() {
        // Packs covering [0..99] and [200..299] — gap at [100..199]
        let pack1 = make_pack_bytes(0, 100);
        let pack2 = make_pack_bytes(200, 100);

        let reader = ForwardPackReader::from_memory(vec![
            Arc::from(pack1.into_boxed_slice()),
            Arc::from(pack2.into_boxed_slice()),
        ])
        .unwrap();

        assert_eq!(
            reader.forward_lookup_str(50).unwrap(),
            Some("val_50".to_string())
        );
        assert_eq!(reader.forward_lookup_str(150).unwrap(), None); // in the gap
        assert_eq!(
            reader.forward_lookup_str(250).unwrap(),
            Some("val_250".to_string())
        );
    }

    #[test]
    fn test_prewarm_respects_budget() {
        let p1 = make_pack_bytes(0, 100);
        let p2 = make_pack_bytes(100, 100);
        let len1 = p1.len() as u64;
        let len2 = p2.len() as u64;
        let total = len1 + len2;
        assert!(
            len1 > 2,
            "pack must be large enough to exercise partial warming"
        );

        let reader = ForwardPackReader::from_memory(vec![
            Arc::from(p1.into_boxed_slice()),
            Arc::from(p2.into_boxed_slice()),
        ])
        .unwrap();

        // Unbounded budget warms every pack fully.
        assert_eq!(reader.prewarm(u64::MAX), total);

        // Zero budget warms nothing (and never touches a pack).
        assert_eq!(reader.prewarm(0), 0);

        // Budget below the first pack partially warms just it.
        let partial = len1 / 2;
        assert_eq!(reader.prewarm(partial), partial);

        // Budget exactly the first pack warms only the first pack (the loop
        // breaks before the second since `warmed >= budget`).
        assert_eq!(reader.prewarm(len1), len1);

        // Budget spanning pack1 + part of pack2 stops mid-second-pack.
        let mid = len1 + len2 / 2;
        assert_eq!(reader.prewarm(mid), mid);
    }

    #[test]
    fn test_overlapping_packs_rejected() {
        // Packs [0..99] and [50..149] overlap — should be rejected.
        let pack1 = make_pack_bytes(0, 100);
        let pack2 = make_pack_bytes(50, 100);

        let result = ForwardPackReader::from_memory(vec![
            Arc::from(pack1.into_boxed_slice()),
            Arc::from(pack2.into_boxed_slice()),
        ]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("overlaps"));
    }

    /// Forces the lazy fetch path under a **current-thread** Tokio runtime.
    ///
    /// `MemoryContentStore` always returns `None` from `resolve_local_path`,
    /// so all packs become `Lazy`. The lookup triggers `fetch_and_load` which
    /// uses `thread::spawn` + `Handle::block_on` — this test verifies that
    /// pattern works on the single-threaded `#[tokio::test]` runtime.
    #[tokio::test]
    async fn test_lazy_fetch_current_thread_runtime() {
        let pack_bytes = make_pack_bytes(0, 50);
        let cs = MemoryContentStore::new();

        // Store the pack in the content store.
        let cid = cs
            .put(
                ContentKind::DictBlob {
                    dict: DictKind::StringForward,
                },
                &pack_bytes,
            )
            .await
            .unwrap();

        let refs = vec![PackBranchEntry {
            first_id: 0,
            last_id: 49,
            pack_cid: cid,
        }];

        let cache_dir =
            std::env::temp_dir().join(format!("fluree_test_lazy_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&cache_dir);

        let reader =
            ForwardPackReader::from_pack_refs(Arc::new(cs), &cache_dir, &refs, KIND_STRING_FWD, 0)
                .await
                .unwrap();

        // All packs should be Lazy (MemoryContentStore has no local path).
        assert_eq!(reader.pack_count(), 1);

        // This triggers the lazy fetch via thread::spawn + block_on.
        assert_eq!(
            reader.forward_lookup_str(0).unwrap(),
            Some("val_0".to_string())
        );
        assert_eq!(
            reader.forward_lookup_str(25).unwrap(),
            Some("val_25".to_string())
        );
        assert_eq!(
            reader.forward_lookup_str(49).unwrap(),
            Some("val_49".to_string())
        );
        assert_eq!(reader.forward_lookup_str(50).unwrap(), None);

        // Cleanup.
        let _ = std::fs::remove_dir_all(&cache_dir);
    }

    /// A small pack must be read onto the heap, not mapped — the whole point of
    /// [`DEFAULT_MMAP_MIN_BYTES`]. Asserting on the backing VARIANT rather than
    /// on lookups is deliberate: lookups pass either way, which is exactly why
    /// the mapping leak went unnoticed for months. This is the only assertion
    /// that can fail if someone reverts to always-mmap.
    #[test]
    fn small_packs_are_read_not_mapped() {
        let bytes = make_pack_bytes(0, 10);
        assert!(
            (bytes.len() as u64) <= DEFAULT_MMAP_MIN_BYTES,
            "fixture must be under the threshold to exercise the read path, got {} bytes",
            bytes.len()
        );

        let dir =
            std::env::temp_dir().join(format!("fluree_test_small_pack_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("small.fpk");
        std::fs::write(&path, &bytes).unwrap();

        let backing = load_pack_backing(&path).unwrap();
        assert!(
            matches!(backing, LoadedBacking::InMemory(_)),
            "a {}-byte pack must not consume a VMA",
            bytes.len()
        );
        // The bytes must survive the trip, or we have traded a mapping for a bug.
        assert_eq!(backing.bytes(), bytes.as_slice());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Above the threshold we still map: large packs are where demand paging
    /// actually pays, and this pins that the split is a split and not a
    /// wholesale move to heap reads (which would pull GiB-sized packs into RAM).
    #[test]
    fn large_packs_are_still_mapped() {
        let dir =
            std::env::temp_dir().join(format!("fluree_test_large_pack_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("large.fpk");
        // Content is irrelevant here — load_pack_backing does not parse.
        std::fs::write(&path, vec![0u8; (DEFAULT_MMAP_MIN_BYTES + 1) as usize]).unwrap();

        let backing = load_pack_backing(&path).unwrap();
        assert!(
            matches!(backing, LoadedBacking::Mmap(_)),
            "packs over the threshold should still be mapped"
        );
        assert_eq!(backing.bytes().len(), (DEFAULT_MMAP_MIN_BYTES + 1) as usize);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The boundary is inclusive (`<=`), so a pack exactly at the threshold is
    /// read. Pinned because an off-by-one here silently changes which side of
    /// the split the most common pack size lands on.
    #[test]
    fn threshold_boundary_is_inclusive() {
        let dir =
            std::env::temp_dir().join(format!("fluree_test_edge_pack_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("edge.fpk");
        std::fs::write(&path, vec![0u8; DEFAULT_MMAP_MIN_BYTES as usize]).unwrap();

        assert!(matches!(
            load_pack_backing(&path).unwrap(),
            LoadedBacking::InMemory(_)
        ));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
