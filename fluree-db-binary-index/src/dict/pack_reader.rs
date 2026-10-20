//! Multi-pack reader for forward dictionary packs.
//!
//! `ForwardPackReader` manages one or more `FPK1` packs and routes lookups
//! to the correct pack via binary search on ID ranges.
//!
//! ## Loading
//!
//! - **`from_pack_refs`**: Builds routing handles without opening pack files.
//!   Local and remote packs load and validate on first lookup or background warming.
//! - **`from_memory`**: In-memory constructor for testing.

use std::io;
use std::sync::Arc;

use once_cell::sync::OnceCell;

use super::allowance::{reject, DictionaryAllowance, PackSlot, Promotion};
use super::forward_pack::{
    find_page_in_dir, lookup_in_pack, page_dir_range, parse_pack_meta, parse_pack_meta_parts,
    PackHeader, PageDirEntry, ParsedPackMeta, PACK_HEADER_SIZE,
};
use crate::format::wire_helpers::PackBranchEntry;
use crate::read::cached_units::{CachedUnits, MissMeter};
use crate::read::leaflet_cache::LeafletCache;
use fluree_db_core::{ContentBytes, ContentId, ContentStore, RangeSource};
use std::sync::atomic::{AtomicBool, Ordering};

// ============================================================================
// PackHandle — owns routing info + backing store for a single pack
// ============================================================================

struct PackHandle {
    /// Content id from the root routing table; `None` for in-memory packs.
    /// Together with the id range this is what a later load matches on to
    /// reuse the handle instead of reopening the file.
    cid: Option<ContentId>,
    /// ID range from the root routing table (always known, even for lazy packs).
    first_id: u64,
    last_id: u64,
    inner: PackInner,
}

enum PackInner {
    /// Pack is fully loaded: metadata parsed, backing bytes available.
    Loaded {
        meta: ParsedPackMeta,
        backing: PackBacking,
    },
    /// Pack deferred: resolve locally or fetch from CAS on first lookup.
    Lazy {
        pack_cid: ContentId,
        loaded: OnceCell<LazyLoaded>,
    },
}

struct LazyLoaded {
    meta: ParsedPackMeta,
    backing: PackBacking,
}

/// Where a loaded pack's pages come from.
enum PackBacking {
    /// The whole pack: mapped, shared from the store, or on the heap.
    Whole(ContentBytes),
    /// A local pack whose store decrypts on read: whole while the cache's
    /// dictionary allowance holds it, and otherwise read by range, a lookup
    /// reading only its entry's two offsets and its value through the
    /// leaflet cache's decrypted units.
    Adaptive(Box<AdaptivePack>),
}

/// A pack read by range until its ranged reads have decrypted, on cache
/// misses since it was last whole, as many bytes as it holds; then it is
/// promoted into the dictionary allowance, which may later evict it.
struct AdaptivePack {
    units: CachedUnits,
    /// Per page: whether its own entry count and final offset have been
    /// checked against its directory entry.
    checked: Box<[AtomicBool]>,
    allowance: Arc<DictionaryAllowance>,
    meter: Arc<MissMeter>,
    /// Where the allowance's copy is, while it holds one.
    slot: Arc<PackSlot>,
    /// Bytes of distinct units a rental must cover before promotion.
    min_covered: u64,
}

impl AdaptivePack {
    fn new(
        units: CachedUnits,
        checked: Box<[AtomicBool]>,
        allowance: Arc<DictionaryAllowance>,
        coverage: f64,
        kind: u8,
    ) -> Self {
        let (meter, slot) =
            allowance.tenancy(units.identity(), units.unit_count(), units.len(), kind);
        let min_covered = (units.len() as f64 * coverage.clamp(0.0, 1.0)).ceil() as u64;
        Self {
            units: units.with_meter(Arc::clone(&meter)),
            checked,
            allowance,
            meter,
            slot,
            min_covered,
        }
    }

    fn lookup_into(&self, meta: &ParsedPackMeta, id: u64, out: &mut Vec<u8>) -> io::Result<bool> {
        if let Some(whole) = &*self.slot.load() {
            whole.mark_used();
            return Ok(lookup_whole(whole, meta, id, out));
        }
        if id < meta.first_id || id > meta.last_id {
            return Ok(false);
        }
        let Some(page_idx) = find_page_in_dir(&meta.directory, id) else {
            return Ok(false);
        };
        let found = ranged_lookup(
            &self.units,
            &self.checked[page_idx],
            &meta.directory[page_idx],
            id,
            out,
        )?;
        self.consider_promotion()?;
        Ok(found)
    }

    /// Promote once its recent rent reaches its size and its rental has
    /// covered enough of it.
    fn consider_promotion(&self) -> io::Result<()> {
        let len = self.units.len();
        if len > self.allowance.capacity() {
            self.meter.set_reason(reject::TOO_LARGE);
        } else if self.meter.recent_rent() < len {
            self.meter.set_reason(reject::RENT);
        } else {
            let covered = self.meter.covered();
            if self.meter.first_crossing(len) {
                self.allowance.note_rent_reached(covered, len);
            }
            if covered < self.min_covered {
                self.meter.set_reason(reject::COVERAGE);
            } else {
                let promotion =
                    self.allowance
                        .promote(self.units.identity(), len, covered, |buf| {
                            self.units.assemble_into(buf)
                        })?;
                self.settle(promotion);
            }
        }
        Ok(())
    }

    /// Once the pack is held whole, its rent starts again from nothing.
    fn settle(&self, promotion: Promotion) {
        self.meter.set_reason(promotion.reason());
        match promotion {
            Promotion::Promoted(pack) => {
                if self.meter.note_promotion() > 0 {
                    self.allowance.note_repromotion(pack.len() as u64);
                }
                self.meter.reset();
                self.units.forget_cached();
                fluree_db_core::io_stats::record_work("pack promoted whole", pack.len());
            }
            Promotion::Resident(_) => self.meter.reset(),
            Promotion::Busy | Promotion::NoRoom | Promotion::TooLarge => {}
        }
    }
}

/// Exact byte ranges of a ranged pack.
trait UnitRead {
    /// Append exactly the bytes of `range`, which must lie in the pack.
    fn extend_into(&self, range: std::ops::Range<u64>, out: &mut Vec<u8>) -> io::Result<()>;

    /// The `n` little-endian u32s at `at`, read through `scratch`.
    fn u32s<const N: usize>(&self, at: u64, scratch: &mut Vec<u8>) -> io::Result<[u32; N]> {
        let mark = scratch.len();
        self.extend_into(at..at + 4 * N as u64, scratch)?;
        let mut out = [0u32; N];
        for (i, slot) in out.iter_mut().enumerate() {
            let b = &scratch[mark + 4 * i..mark + 4 * i + 4];
            *slot = u32::from_le_bytes(b.try_into().expect("four bytes"));
        }
        scratch.truncate(mark);
        Ok(out)
    }
}

/// Fail a read that came back short of `want` bytes appended since `mark`.
fn exact(out: &mut Vec<u8>, mark: usize, want: usize) -> io::Result<()> {
    if out.len() - mark != want {
        out.truncate(mark);
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "forward pack: read past the end of the pack",
        ));
    }
    Ok(())
}

impl UnitRead for CachedUnits {
    fn extend_into(&self, range: std::ops::Range<u64>, out: &mut Vec<u8>) -> io::Result<()> {
        let (want, mark) = ((range.end - range.start) as usize, out.len());
        CachedUnits::extend_into(self, range, out)?;
        exact(out, mark, want)
    }
}

/// Look `id` up in page `dir` of a ranged pack, appending its value to
/// `out`. Reads the page's two offsets for `id` and then the value; on the
/// page's first visit also its entry count and final offset, which must
/// agree with the directory. Offsets are bounds-checked on every lookup.
fn ranged_lookup(
    units: &impl UnitRead,
    checked: &AtomicBool,
    dir: &PageDirEntry,
    id: u64,
    out: &mut Vec<u8>,
) -> io::Result<bool> {
    let invalid = |what: &str| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("forward pack page: {what}"),
        )
    };
    let base = u64::from(dir.page_offset);
    let count = u64::from(dir.entry_count);
    // `parse_pack_meta_parts` checked the page is at least this long.
    let data_start = 4 + (count + 1) * 4;
    let data_len = u64::from(dir.page_len) - data_start;
    if !checked.load(Ordering::Acquire) {
        let [entries] = units.u32s::<1>(base, out)?;
        let [last] = units.u32s::<1>(base + 4 + count * 4, out)?;
        if u64::from(entries) != count {
            return Err(invalid("entry count does not match its directory entry"));
        }
        if u64::from(last) != data_len {
            return Err(invalid("final offset does not match its length"));
        }
        checked.store(true, Ordering::Release);
    }
    let local = id - dir.page_first_id;
    let [lo, hi] = units.u32s::<2>(base + 4 + local * 4, out)?;
    if lo > hi || u64::from(hi) > data_len {
        return Err(invalid("value offsets out of order or out of bounds"));
    }
    let from = base + data_start + u64::from(lo);
    units.extend_into(from..from + u64::from(hi - lo), out)?;
    Ok(true)
}

fn lookup_whole(bytes: &[u8], meta: &ParsedPackMeta, id: u64, out: &mut Vec<u8>) -> bool {
    match lookup_in_pack(bytes, meta, id) {
        Some(value) => {
            out.extend_from_slice(value);
            true
        }
        None => false,
    }
}

impl PackBacking {
    /// Append `id`'s value bytes to `out`; `false` when the pack lacks it.
    fn lookup_into(&self, meta: &ParsedPackMeta, id: u64, out: &mut Vec<u8>) -> io::Result<bool> {
        match self {
            PackBacking::Whole(bytes) => Ok(lookup_whole(bytes, meta, id, out)),
            PackBacking::Adaptive(pack) => pack.lookup_into(meta, id, out),
        }
    }
}

impl PackHandle {
    /// Get pre-parsed metadata and the pack's backing. For lazy packs,
    /// triggers fetch + load + parse on first call (subsequent calls return
    /// cached).
    fn ensure_loaded(
        &self,
        ctx: Option<&LoadContext>,
    ) -> io::Result<(&ParsedPackMeta, &PackBacking)> {
        match &self.inner {
            PackInner::Loaded { meta, backing } => Ok((meta, backing)),
            PackInner::Lazy { pack_cid, loaded } => {
                let ctx = ctx.ok_or_else(|| io::Error::other("lazy pack without load context"))?;
                let lazy = loaded.get_or_try_init(|| {
                    fetch_and_load(self.first_id, self.last_id, pack_cid, ctx)
                })?;
                Ok((&lazy.meta, &lazy.backing))
            }
        }
    }
}

/// Shared loading context. Stored once on `ForwardPackReader`.
struct LoadContext {
    cs: Arc<dyn ContentStore>,
    expected_kind: u8,
    expected_ns_code: u16,
    /// Where the decrypted units of packs read by range are kept, and the
    /// dictionary allowance that holds promoted ones. Without one, a pack of
    /// a store that decrypts on read is decrypted whole.
    page_cache: Option<Arc<LeafletCache>>,
    /// The share of a pack its ranged misses must have covered, besides
    /// renting its size, before it is promoted.
    promote_coverage: f64,
}

/// The share of a pack a rental must have covered before it is promoted: a
/// pack whose misses keep landing on the same small region rents its size
/// without being worth holding whole.
const PROMOTE_COVERAGE: f64 = 0.5;

// ============================================================================
// ForwardPackReader
// ============================================================================

/// Multi-pack reader for forward dictionary lookups.
///
/// Manages one or more `FPK1` packs, sorted by ID range. Lookups binary-search
/// the packs, then use pre-parsed metadata for zero-alloc page navigation.
///
/// Local and remote packs are loaded and validated on first lookup. Concurrent
/// readers and background warming share the same one-time initialization.
pub struct ForwardPackReader {
    /// Shared so a reload of the same routing table can carry a handle (and
    /// its open backing) over from the previous reader.
    packs: Vec<Arc<PackHandle>>,
    /// Shared loading context. `None` for in-memory readers.
    load_ctx: Option<LoadContext>,
}

impl ForwardPackReader {
    /// Build routing handles without probing or opening pack files.
    ///
    /// The root's ID ranges are checked immediately. On first lookup or warming,
    /// each pack is resolved locally before trying remote CAS, opened (heap-read
    /// for small packs, mmap for large ones), and validated against its routing
    /// range, kind, and namespace, so a pack that fails those checks errors on
    /// use rather than here. Ledger open still reads the reverse dictionary
    /// trees eagerly, so a missing dictionary still fails at open.
    pub async fn from_pack_refs(
        cs: Arc<dyn ContentStore>,
        refs: &[PackBranchEntry],
        expected_kind: u8,
        expected_ns_code: u16,
    ) -> io::Result<Self> {
        Self::from_pack_refs_reusing(cs, refs, expected_kind, expected_ns_code, None).await
    }

    /// [`Self::from_pack_refs`], carrying over every handle of `prev` whose
    /// content id and id range match a routing entry.
    ///
    /// A pack is an immutable content-addressed blob, so a handle that was
    /// valid for the same cid in the previous reader is valid here without
    /// another open, size probe, or header parse — an incremental index
    /// publish keeps nearly every pack of the previous root, and this path
    /// used to reopen all of them. `prev` is ignored unless it was loaded for
    /// the same kind and namespace code.
    pub async fn from_pack_refs_reusing(
        cs: Arc<dyn ContentStore>,
        refs: &[PackBranchEntry],
        expected_kind: u8,
        expected_ns_code: u16,
        prev: Option<&ForwardPackReader>,
    ) -> io::Result<Self> {
        let prev = prev.filter(|p| {
            p.load_ctx.as_ref().is_some_and(|ctx| {
                ctx.expected_kind == expected_kind && ctx.expected_ns_code == expected_ns_code
            })
        });
        // The routing table of a stream that received nothing since the
        // previous root is the previous table entry for entry — the case
        // for nearly every namespace of a ledger with many — and needs no
        // matching at all. Compared in order rather than through a map so a
        // ledger with tens of thousands of namespaces pays no allocation
        // per stream.
        if let Some(p) = prev {
            let unchanged = p.packs.len() == refs.len()
                && p.packs.iter().zip(refs).all(|(h, entry)| {
                    h.cid.as_ref() == Some(&entry.pack_cid)
                        && h.first_id == entry.first_id
                        && h.last_id == entry.last_id
                });
            if unchanged {
                return Ok(Self {
                    packs: p.packs.clone(),
                    load_ctx: Some(LoadContext {
                        cs,
                        expected_kind,
                        expected_ns_code,
                        page_cache: None,
                        promote_coverage: PROMOTE_COVERAGE,
                    }),
                });
            }
        }
        let reusable: std::collections::HashMap<&ContentId, &Arc<PackHandle>> = prev
            .map(|p| {
                p.packs
                    .iter()
                    .filter_map(|h| h.cid.as_ref().map(|cid| (cid, h)))
                    .collect()
            })
            .unwrap_or_default();

        let mut packs = Vec::with_capacity(refs.len());
        let mut reused = 0usize;

        for entry in refs {
            if let Some(handle) = reusable.get(&entry.pack_cid) {
                if handle.first_id == entry.first_id && handle.last_id == entry.last_id {
                    packs.push(Arc::clone(handle));
                    reused += 1;
                    continue;
                }
            }

            // Use the same lazy path for local and remote packs. It asks the
            // store for local bytes before fetching, so opening a ledger
            // needs only the routing table, not every dictionary file.
            let inner = PackInner::Lazy {
                pack_cid: entry.pack_cid.clone(),
                loaded: OnceCell::new(),
            };
            packs.push(Arc::new(PackHandle {
                cid: Some(entry.pack_cid.clone()),
                first_id: entry.first_id,
                last_id: entry.last_id,
                inner,
            }));
        }

        // Sort by first_id (should already be sorted, but enforce).
        packs.sort_by_key(|p| p.first_id);
        validate_pack_routing(&packs)?;

        if reused > 0 {
            tracing::trace!(
                kind = expected_kind,
                ns_code = expected_ns_code,
                reused,
                total = packs.len(),
                "forward packs carried over from the previous reader"
            );
        }

        Ok(Self {
            packs,
            load_ctx: Some(LoadContext {
                cs,
                expected_kind,
                expected_ns_code,
                page_cache: None,
                promote_coverage: PROMOTE_COVERAGE,
            }),
        })
    }

    #[cfg(test)]
    fn with_promote_coverage(mut self, coverage: f64) -> Self {
        if let Some(ctx) = &mut self.load_ctx {
            ctx.promote_coverage = coverage;
        }
        self
    }

    /// Read packs of a store that decrypts on read by range through
    /// `cache`, promoting them into its dictionary allowance. Without it
    /// they are decrypted whole.
    pub fn with_page_cache(mut self, cache: Option<Arc<LeafletCache>>) -> Self {
        if let Some(ctx) = &mut self.load_ctx {
            ctx.page_cache = cache;
        }
        self
    }

    /// Create from pre-built in-memory pack bytes (for testing).
    pub fn from_memory(pack_bytes_list: Vec<Arc<[u8]>>) -> io::Result<Self> {
        let mut packs = Vec::with_capacity(pack_bytes_list.len());

        for bytes in pack_bytes_list {
            let meta = parse_pack_meta(&bytes)?;
            packs.push(Arc::new(PackHandle {
                cid: None,
                first_id: meta.first_id,
                last_id: meta.last_id,
                inner: PackInner::Loaded {
                    meta,
                    backing: PackBacking::Whole(ContentBytes::Shared(bytes)),
                },
            }));
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

    /// Number of packs whose handle (and open backing) this reader shares
    /// with `other`. Diagnostic for the reload path: after an incremental
    /// publish this should equal the number of packs the two roots have in
    /// common.
    pub fn packs_shared_with(&self, other: &ForwardPackReader) -> usize {
        self.packs
            .iter()
            .filter(|h| other.packs.iter().any(|o| Arc::ptr_eq(h, o)))
            .count()
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
        let mut failed = 0usize;
        let mut first_failure = None;
        for pack in &self.packs {
            if warmed >= budget_bytes {
                break;
            }
            let bytes: &[u8] = match pack.ensure_loaded(self.load_ctx.as_ref()) {
                Ok((_, PackBacking::Whole(bytes))) => bytes,
                // Values decrypt on use; opening read the header and
                // directory, which is what counts against the budget.
                Ok((meta, PackBacking::Adaptive(_))) => {
                    warmed += (PACK_HEADER_SIZE + meta.directory.len() * 20) as u64;
                    continue;
                }
                Err(error) => {
                    failed += 1;
                    first_failure.get_or_insert((
                        pack.cid.as_ref(),
                        pack.first_id,
                        pack.last_id,
                        error,
                    ));
                    continue;
                }
            };
            let remaining = budget_bytes - warmed;
            let take = (bytes.len() as u64).min(remaining) as usize;
            warmed += touch_pages(&bytes[..take]);
        }
        // Skipped packs fail again on first lookup, but a query layer may turn
        // that into a placeholder rather than an error; leave one trace here.
        if let Some((cid, first_id, last_id, error)) = first_failure {
            tracing::warn!(
                failed,
                cid = ?cid.map(ToString::to_string),
                first_id,
                last_id,
                error = %error,
                "forward dictionary packs failed to load during warming"
            );
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
        let (meta, backing) = handle.ensure_loaded(self.load_ctx.as_ref())?;
        backing.lookup_into(meta, id, out)
    }

    /// Convenience: look up and return as a `String`.
    pub fn forward_lookup_str(&self, id: u64) -> io::Result<Option<String>> {
        let Some(handle) = self.find_pack(id) else {
            return Ok(None);
        };
        let (meta, backing) = handle.ensure_loaded(self.load_ctx.as_ref())?;
        let mut value = Vec::new();
        if !backing.lookup_into(meta, id, &mut value)? {
            return Ok(None);
        }
        String::from_utf8(value)
            .map(Some)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
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
fn validate_pack_routing(packs: &[Arc<PackHandle>]) -> io::Result<()> {
    for pack in packs {
        if pack.first_id > pack.last_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "pack routing: first_id exceeds last_id",
            ));
        }
    }
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

// ============================================================================
// Lazy fetch
// ============================================================================

/// Fetch a pack from CAS, write to cache, mmap, parse, and validate.
///
/// Uses `thread::spawn` + `Handle::block_on` to bridge the sync lookup API
/// with the async `ContentStore::get`. This pattern works on both single-thread
/// and multi-thread Tokio runtimes (unlike `block_in_place` which requires
/// multi-thread).
fn fetch_and_load(
    expected_first_id: u64,
    expected_last_id: u64,
    pack_cid: &ContentId,
    ctx: &LoadContext,
) -> io::Result<LazyLoaded> {
    // Residency-mode stores: serve the residency tier with shared zero-copy
    // backing; a miss surfaces as `NeedFetch` (recorded in the miss
    // register). The surrounding `OnceCell` makes a successful load
    // one-time, exactly as on native. Checked before any filesystem probe.
    #[cfg(any(target_arch = "wasm32", feature = "residency"))]
    if ctx.cs.miss_register().is_some() {
        let backing = crate::read::need_fetch::resident_or_need_fetch(
            ctx.cs.as_ref(),
            pack_cid,
            crate::read::need_fetch::FetchKind::ForwardPack,
        )?;
        return loaded(backing, expected_first_id, expected_last_id, ctx);
    }

    if let (true, Some(cache)) = (ctx.cs.costly_local_reads(), &ctx.page_cache) {
        if let Some(source) = ctx
            .cs
            .get_local_ranged(pack_cid)
            .map_err(|e| io::Error::other(format!("forward pack {pack_cid}: {e}")))?
        {
            return loaded_adaptive(
                source,
                cache,
                pack_cid,
                expected_first_id,
                expected_last_id,
                ctx,
            );
        }
        cache.note_unrangeable_pack();
    }
    if let Some(backing) = ctx
        .cs
        .get_local(pack_cid)
        .map_err(|e| io::Error::other(format!("forward pack {pack_cid}: {e}")))?
    {
        return loaded(backing, expected_first_id, expected_last_id, ctx);
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
            first_id = expected_first_id,
            last_id = expected_last_id,
            error = %e,
            "remote lazy fetch for forward pack failed"
        );
        io::Error::other(format!("lazy pack fetch: {e}"))
    })?;

    loaded(bytes, expected_first_id, expected_last_id, ctx)
}

/// Parse and validate a pack's bytes.
fn loaded(
    backing: ContentBytes,
    expected_first_id: u64,
    expected_last_id: u64,
    ctx: &LoadContext,
) -> io::Result<LazyLoaded> {
    let meta = parse_pack_meta(&backing)?;
    validate_lazy_meta(&meta, expected_first_id, expected_last_id, ctx)?;
    fluree_db_core::io_stats::record_work(
        match backing {
            ContentBytes::Owned(_) => "pack held whole (heap)",
            ContentBytes::Shared(_) => "pack held whole (shared)",
            #[cfg(not(target_arch = "wasm32"))]
            ContentBytes::Mapped(_) => "pack held whole (mapped)",
        },
        backing.len(),
    );
    Ok(LazyLoaded {
        meta,
        backing: PackBacking::Whole(backing),
    })
}

/// Open a pack read by range: its header and page directory are read and
/// validated now, its values on use.
fn loaded_adaptive(
    source: Arc<dyn RangeSource>,
    cache: &Arc<LeafletCache>,
    pack_cid: &ContentId,
    expected_first_id: u64,
    expected_last_id: u64,
    ctx: &LoadContext,
) -> io::Result<LazyLoaded> {
    let read = |range: std::ops::Range<u64>| {
        source
            .read_range(range)
            .map_err(|e| io::Error::other(format!("forward pack {pack_cid}: {e}")))
    };
    let header_bytes = read(0..PACK_HEADER_SIZE as u64)?;
    let header = PackHeader::decode(&header_bytes)?;
    let dir_bytes = read(page_dir_range(&header, source.len())?)?;
    let meta = parse_pack_meta_parts(&header, &dir_bytes, source.len())?;
    validate_lazy_meta(&meta, expected_first_id, expected_last_id, ctx)?;
    fluree_db_core::io_stats::record_work(
        "pack opened paged",
        header_bytes.len() + dir_bytes.len(),
    );
    let checked = meta
        .directory
        .iter()
        .map(|_| AtomicBool::new(false))
        .collect();
    let units = CachedUnits::new(source, Arc::clone(cache));
    cache.note_promotable_pack(units.identity(), units.len());
    let backing = PackBacking::Adaptive(Box::new(AdaptivePack::new(
        units,
        checked,
        Arc::clone(cache.dictionary_allowance()),
        ctx.promote_coverage,
        ctx.expected_kind,
    )));
    Ok(LazyLoaded { meta, backing })
}

/// Validate a loaded pack against the expected range and dictionary stream.
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Unique temp directory names across tests in this process.
    static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);
    use super::*;
    use crate::dict::forward_pack::{encode_forward_pack, KIND_STRING_FWD, KIND_SUBJECT_FWD};
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
    /// A simulated remote store serves nothing locally, so the lookup triggers
    /// `fetch_and_load`'s bridged fetch — this test verifies that bridge works
    /// on the single-threaded `#[tokio::test]` runtime.
    #[tokio::test]
    async fn test_lazy_fetch_current_thread_runtime() {
        let pack_bytes = make_pack_bytes(0, 50);
        let cs = fluree_db_core::content_store_for(
            fluree_db_core::MemoryStorage::new().simulating_remote(),
            &fluree_db_core::StorageNamespace::parse_legacy("lazy:main").unwrap(),
        );

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

        let reader = ForwardPackReader::from_pack_refs(Arc::new(cs), &refs, KIND_STRING_FWD, 0)
            .await
            .unwrap();
        assert_eq!(reader.pack_count(), 1);

        // This triggers the lazy fetch over the sync→async bridge.
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
    }

    /// A reload of a routing table that still names a pack must share the
    /// previous reader's handle for it, and only load what is new. Matching is
    /// by cid AND id range, and never across a reader built for another
    /// dictionary stream.
    #[tokio::test]
    async fn a_reload_carries_over_matching_pack_handles() {
        let cs: Arc<dyn ContentStore> = Arc::new(MemoryContentStore::new());
        let kind = ContentKind::DictBlob {
            dict: DictKind::StringForward,
        };
        let p1 = cs.put(kind, &make_pack_bytes(0, 50)).await.unwrap();
        let p2 = cs.put(kind, &make_pack_bytes(50, 50)).await.unwrap();
        let entry = |first: u64, last: u64, cid: &ContentId| PackBranchEntry {
            first_id: first,
            last_id: last,
            pack_cid: cid.clone(),
        };

        let first = ForwardPackReader::from_pack_refs(
            Arc::clone(&cs),
            &[entry(0, 49, &p1)],
            KIND_STRING_FWD,
            0,
        )
        .await
        .unwrap();
        // Resolve through the first reader so its lazy handle is loaded; the
        // second reader must see that load, not repeat it.
        assert_eq!(
            first.forward_lookup_str(7).unwrap().as_deref(),
            Some("val_7")
        );

        let second = ForwardPackReader::from_pack_refs_reusing(
            Arc::clone(&cs),
            &[entry(0, 49, &p1), entry(50, 99, &p2)],
            KIND_STRING_FWD,
            0,
            Some(&first),
        )
        .await
        .unwrap();
        assert_eq!(second.pack_count(), 2);
        assert_eq!(
            second.packs_shared_with(&first),
            1,
            "the pack both tables name must be the same handle"
        );
        assert!(
            Arc::ptr_eq(&second.packs[0], &first.packs[0]),
            "the shared handle is the one for the common pack"
        );
        assert!(matches!(
            &second.packs[0].inner,
            PackInner::Lazy { loaded, .. } if loaded.get().is_some()
        ));
        assert_eq!(
            second.forward_lookup_str(7).unwrap().as_deref(),
            Some("val_7")
        );
        assert_eq!(
            second.forward_lookup_str(75).unwrap().as_deref(),
            Some("val_75")
        );

        // Same cid under a different id range is a different routing entry
        // (the mismatch is checked when the new handle is used).
        let shifted = ForwardPackReader::from_pack_refs_reusing(
            Arc::clone(&cs),
            &[entry(100, 149, &p1)],
            KIND_STRING_FWD,
            0,
            Some(&first),
        )
        .await
        .unwrap();
        assert_eq!(shifted.packs_shared_with(&first), 0);

        // A reader for another stream never lends its handles.
        let other_stream = ForwardPackReader::from_pack_refs_reusing(
            Arc::clone(&cs),
            &[entry(0, 49, &p1)],
            KIND_SUBJECT_FWD,
            3,
            Some(&first),
        )
        .await
        .unwrap();
        assert_eq!(other_stream.packs_shared_with(&first), 0);
    }

    /// Test store whose local-path probes are observable independently of CAS
    /// fetches. The same files can also stand in for a populated disk cache.
    #[derive(Debug)]
    struct FilePackStore {
        dir: PathBuf,
        local: bool,
        resolves: AtomicU64,
        gets: AtomicU64,
        fallback: MemoryContentStore,
    }

    impl FilePackStore {
        fn new(local: bool) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "fluree_lazy_local_{}_{}",
                std::process::id(),
                TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).unwrap();
            Self {
                dir,
                local,
                resolves: AtomicU64::new(0),
                gets: AtomicU64::new(0),
                fallback: MemoryContentStore::new(),
            }
        }

        fn path(&self, id: &ContentId) -> PathBuf {
            self.dir.join(format!("{}.fpk", id.digest_hex()))
        }
    }

    impl Drop for FilePackStore {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[async_trait::async_trait]
    impl ContentStore for FilePackStore {
        async fn has(&self, id: &ContentId) -> fluree_db_core::Result<bool> {
            self.fallback.has(id).await
        }
        async fn get(
            &self,
            id: &ContentId,
        ) -> fluree_db_core::Result<fluree_db_core::ContentBytes> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            if self.local {
                // Local files are the authority, as for file storage.
                return std::fs::read(self.path(id))
                    .map(Into::into)
                    .map_err(|e| fluree_db_core::Error::not_found(e.to_string()));
            }
            self.fallback.get(id).await
        }
        async fn put(&self, kind: ContentKind, bytes: &[u8]) -> fluree_db_core::Result<ContentId> {
            let id = self.fallback.put(kind, bytes).await?;
            std::fs::write(self.path(&id), bytes).unwrap();
            Ok(id)
        }
        async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> fluree_db_core::Result<()> {
            self.fallback.put_with_id(id, bytes).await?;
            std::fs::write(self.path(id), bytes).unwrap();
            Ok(())
        }
        async fn release(&self, id: &ContentId) -> fluree_db_core::Result<()> {
            self.fallback.release(id).await
        }
        fn get_local(
            &self,
            id: &ContentId,
        ) -> fluree_db_core::Result<Option<fluree_db_core::ContentBytes>> {
            self.resolves.fetch_add(1, Ordering::Relaxed);
            if !self.local {
                return Ok(None);
            }
            // SAFETY: the test never rewrites a file it has stored.
            unsafe { fluree_db_core::ContentBytes::open(&self.path(id)) }
                .map_err(|e| fluree_db_core::Error::io(e.to_string()))
        }
    }

    async fn file_pack(cs: &FilePackStore, first: u64, count: usize) -> PackBranchEntry {
        let pack_cid = cs
            .put(
                ContentKind::DictBlob {
                    dict: DictKind::StringForward,
                },
                &make_pack_bytes(first, count),
            )
            .await
            .unwrap();
        PackBranchEntry {
            first_id: first,
            last_id: first + count as u64 - 1,
            pack_cid,
        }
    }

    #[tokio::test]
    async fn local_and_cached_packs_open_only_on_use() {
        // A local store, then a remote one whose disk cache already holds the
        // packs.
        for local in [true, false] {
            let cs = Arc::new(FilePackStore::new(local));
            let refs = vec![file_pack(&cs, 0, 50).await, file_pack(&cs, 100, 50).await];
            let store: Arc<dyn ContentStore> = if local {
                cs.clone()
            } else {
                let cache =
                    fluree_db_core::disk_cache::DiskArtifactCache::for_dir(&cs.dir.join("cache"));
                let cached = fluree_db_core::disk_cache::CachedContentStore::new(cs.clone(), cache);
                for entry in &refs {
                    let bytes = std::fs::read(cs.path(&entry.pack_cid)).unwrap();
                    cached.keep_local(&entry.pack_cid, &bytes);
                }
                cs.resolves.store(0, Ordering::Relaxed);
                Arc::new(cached)
            };
            let reader =
                ForwardPackReader::from_pack_refs(store.clone(), &refs, KIND_STRING_FWD, 0)
                    .await
                    .unwrap();
            assert_eq!(
                cs.resolves.load(Ordering::Relaxed),
                0,
                "constructor must not probe files"
            );
            assert_eq!(cs.gets.load(Ordering::Relaxed), 0);
            assert_eq!(reader.prewarm(0), 0);
            assert_eq!(
                reader.forward_lookup_str(75).unwrap(),
                None,
                "gap needs no pack"
            );
            assert_eq!(cs.resolves.load(Ordering::Relaxed), 0);

            // Reusing an unopened handle must remain lazy and share its first load.
            let reused = ForwardPackReader::from_pack_refs_reusing(
                store.clone(),
                &refs,
                KIND_STRING_FWD,
                0,
                Some(&reader),
            )
            .await
            .unwrap();
            assert_eq!(reused.packs_shared_with(&reader), 2);
            assert_eq!(
                reused.forward_lookup_str(7).unwrap().as_deref(),
                Some("val_7")
            );
            assert_eq!(
                reader.forward_lookup_str(7).unwrap().as_deref(),
                Some("val_7")
            );
            assert_eq!(cs.resolves.load(Ordering::Relaxed), 1);
            assert_eq!(
                cs.gets.load(Ordering::Relaxed),
                0,
                "local/cache hit must not fetch CAS"
            );
            assert!(
                matches!(&reader.packs[1].inner, PackInner::Lazy {loaded, ..} if loaded.get().is_none())
            );
        }
    }

    /// Store holding its packs in memory, handed out shared as memory
    /// storage does through the bridge. Fetches count.
    #[derive(Debug)]
    struct ResidentPackStore {
        inner: MemoryContentStore,
        resident: std::sync::RwLock<std::collections::HashMap<ContentId, Arc<[u8]>>>,
        gets: AtomicU64,
    }

    #[async_trait::async_trait]
    impl ContentStore for ResidentPackStore {
        async fn has(&self, id: &ContentId) -> fluree_db_core::Result<bool> {
            self.inner.has(id).await
        }
        async fn get(
            &self,
            id: &ContentId,
        ) -> fluree_db_core::Result<fluree_db_core::ContentBytes> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            self.inner.get(id).await
        }
        async fn put(&self, kind: ContentKind, bytes: &[u8]) -> fluree_db_core::Result<ContentId> {
            let id = self.inner.put(kind, bytes).await?;
            self.resident
                .write()
                .unwrap()
                .insert(id.clone(), Arc::from(bytes));
            Ok(id)
        }
        async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> fluree_db_core::Result<()> {
            self.inner.put_with_id(id, bytes).await?;
            self.resident
                .write()
                .unwrap()
                .insert(id.clone(), Arc::from(bytes));
            Ok(())
        }
        async fn release(&self, id: &ContentId) -> fluree_db_core::Result<()> {
            self.inner.release(id).await
        }
        fn get_local(
            &self,
            id: &ContentId,
        ) -> fluree_db_core::Result<Option<fluree_db_core::ContentBytes>> {
            Ok(self
                .resident
                .read()
                .unwrap()
                .get(id)
                .cloned()
                .map(fluree_db_core::ContentBytes::Shared))
        }
    }

    /// A store holding its packs in memory hands them out shared: the lazy
    /// load borrows the pack and never fetches it.
    #[tokio::test]
    async fn resident_packs_load_without_fetching_or_copying() {
        let cs = Arc::new(ResidentPackStore {
            inner: MemoryContentStore::new(),
            resident: Default::default(),
            gets: AtomicU64::new(0),
        });
        let pack_cid = cs
            .put(
                ContentKind::DictBlob {
                    dict: DictKind::StringForward,
                },
                &make_pack_bytes(0, 50),
            )
            .await
            .unwrap();
        let refs = [PackBranchEntry {
            first_id: 0,
            last_id: 49,
            pack_cid: pack_cid.clone(),
        }];
        let reader = ForwardPackReader::from_pack_refs(cs.clone(), &refs, KIND_STRING_FWD, 0)
            .await
            .unwrap();

        assert_eq!(
            reader.forward_lookup_str(7).unwrap().as_deref(),
            Some("val_7")
        );
        assert_eq!(
            cs.gets.load(Ordering::Relaxed),
            0,
            "a resident pack must not be fetched"
        );
        let resident = cs.get_local(&pack_cid).unwrap().unwrap();
        assert!(
            matches!(
                &reader.packs[0].inner,
                PackInner::Lazy { loaded, .. }
                    if matches!(
                        loaded.get(),
                        Some(LazyLoaded { backing: PackBacking::Whole(backing), .. })
                            if backing.ptr_eq(&resident)
                    )
            ),
            "the loaded pack must be the store's own allocation"
        );
    }

    #[tokio::test]
    async fn local_first_reads_and_warming_share_one_load() {
        let cs = Arc::new(FilePackStore::new(true));
        let refs = [file_pack(&cs, 0, 50).await];
        let reader = Arc::new(
            ForwardPackReader::from_pack_refs(cs.clone(), &refs, KIND_STRING_FWD, 0)
                .await
                .unwrap(),
        );
        let barrier = Arc::new(std::sync::Barrier::new(9));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let reader = &reader;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    for _ in 0..100 {
                        assert_eq!(
                            reader.forward_lookup_str(7).unwrap().as_deref(),
                            Some("val_7")
                        );
                    }
                });
            }
            barrier.wait();
            assert_eq!(reader.prewarm(1), 1);
        });
        assert_eq!(cs.resolves.load(Ordering::Relaxed), 1);
        assert_eq!(cs.gets.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn deferred_local_validation_and_retry_preserve_errors() {
        let cs = Arc::new(FilePackStore::new(true));
        let entry = file_pack(&cs, 0, 50).await;
        for (first, last, kind, ns, message) in [
            (1, 49, KIND_STRING_FWD, 0, "pack header range"),
            (0, 49, KIND_SUBJECT_FWD, 0, "pack kind"),
            (0, 49, KIND_STRING_FWD, 3, "pack ns_code"),
        ] {
            let refs = [PackBranchEntry {
                first_id: first,
                last_id: last,
                pack_cid: entry.pack_cid.clone(),
            }];
            let reader = ForwardPackReader::from_pack_refs(cs.clone(), &refs, kind, ns)
                .await
                .unwrap();
            let err = reader.forward_lookup_str(7).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
            assert!(err.to_string().contains(message), "{err}");
        }

        let reader = ForwardPackReader::from_pack_refs(
            cs.clone(),
            std::slice::from_ref(&entry),
            KIND_STRING_FWD,
            0,
        )
        .await
        .unwrap();
        let path = cs.path(&entry.pack_cid);
        let original = std::fs::read(&path).unwrap();
        std::fs::write(&path, b"broken header").unwrap();
        assert!(reader.forward_lookup_str(7).is_err());
        // Failed initialization must not poison the cell; this also models a
        // temporarily missing local artifact being restored by storage.
        std::fs::remove_file(&path).unwrap();
        assert!(reader.forward_lookup_str(7).is_err());
        std::fs::write(&path, original).unwrap();
        assert_eq!(
            reader.forward_lookup_str(7).unwrap().as_deref(),
            Some("val_7")
        );
    }

    #[tokio::test]
    async fn root_routing_is_validated_without_opening_packs() {
        let cs = Arc::new(FilePackStore::new(true));
        let entry = file_pack(&cs, 0, 50).await;
        let reversed = PackBranchEntry {
            first_id: 50,
            last_id: 49,
            pack_cid: entry.pack_cid.clone(),
        };
        for refs in [vec![reversed], vec![entry.clone(), entry]] {
            let err = ForwardPackReader::from_pack_refs(cs.clone(), &refs, KIND_STRING_FWD, 0)
                .await
                .unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        }
        assert_eq!(cs.resolves.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    #[ignore = "local opening probe; DICT_OPEN_PACKS sets fixture size"]
    async fn local_dictionary_open_probe() {
        let n: usize = std::env::var("DICT_OPEN_PACKS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5000);
        let cs = Arc::new(FilePackStore::new(true));
        let mut refs = Vec::with_capacity(n);
        for i in 0..n {
            refs.push(file_pack(&cs, i as u64 * 100, 100).await);
        }
        for run in 0..5 {
            let start = std::time::Instant::now();
            let reader = ForwardPackReader::from_pack_refs(cs.clone(), &refs, KIND_STRING_FWD, 0)
                .await
                .unwrap();
            let open = start.elapsed();
            let first = std::time::Instant::now();
            assert_eq!(
                reader.forward_lookup_str(7).unwrap().as_deref(),
                Some("val_7")
            );
            let first = first.elapsed();
            let all = std::time::Instant::now();
            for i in 0..n {
                let id = i as u64 * 100 + 7;
                assert_eq!(
                    reader.forward_lookup_str(id).unwrap(),
                    Some(format!("val_{id}"))
                );
            }
            let all = all.elapsed();
            let hot = std::time::Instant::now();
            let mut out = Vec::new();
            for _ in 0..20 {
                for i in 0..n {
                    out.clear();
                    assert!(reader
                        .forward_lookup_into(i as u64 * 100 + 7, &mut out)
                        .unwrap());
                    std::hint::black_box(&out);
                }
            }
            let hot = hot.elapsed();
            eprintln!(
                "DICT_HOT packs={n} run={run} lookups={} hot_us={}",
                n * 20,
                hot.as_micros()
            );
            eprintln!(
                "DICT_OPEN packs={n} run={run} open_us={} first_us={} all_us={}",
                open.as_micros(),
                first.as_micros(),
                all.as_micros()
            );
        }
    }

    /// Packs of a store whose whole local reads decrypt everything: read by
    /// range through the leaflet cache's units, and promoted whole into its
    /// dictionary allowance once their recent misses rent their size.
    mod ranged {
        use super::*;
        use crate::read::binary_index_store::tests::ranged_local::SegmentedContentStore;
        use std::sync::atomic::Ordering as AtomicOrdering;

        const FIRST: u64 = 1_000;
        const COUNT: usize = 2_000;
        const HALF: u64 = COUNT as u64 / 2;

        fn value(id: u64) -> Vec<u8> {
            format!("value-{id:08}").into_bytes()
        }

        fn pack_with_pages(first: u64, page_bytes: usize) -> Vec<u8> {
            let entries: Vec<(u64, Vec<u8>)> = (first..first + COUNT as u64)
                .map(|id| (id, value(id)))
                .collect();
            let refs: Vec<(u64, &[u8])> =
                entries.iter().map(|(id, v)| (*id, v.as_slice())).collect();
            encode_forward_pack(&refs, KIND_STRING_FWD, 0, page_bytes).unwrap()
        }

        /// A pack of a few large pages, so a page spans many read units.
        fn pack_from(first: u64) -> Vec<u8> {
            pack_with_pages(first, 32 * 1024)
        }

        fn reader_of(
            cs: &SegmentedContentStore,
            pack: &[u8],
            first: u64,
            cache: Option<Arc<LeafletCache>>,
            kind: u8,
        ) -> ForwardPackReader {
            let pack_cid = cs.put_sync(
                ContentKind::DictBlob {
                    dict: DictKind::StringForward,
                },
                pack,
            );
            let refs = vec![PackBranchEntry {
                first_id: first,
                last_id: first + COUNT as u64 - 1,
                pack_cid,
            }];
            let cs: Arc<dyn ContentStore> = Arc::new(cs.clone());
            crate::read::binary_index_store::run_sync_on_runtime(async move {
                ForwardPackReader::from_pack_refs(cs, &refs, kind, 0).await
            })
            .unwrap()
            .with_page_cache(cache)
        }

        fn adaptive(
            cs: &SegmentedContentStore,
            first: u64,
            cache: &Arc<LeafletCache>,
        ) -> ForwardPackReader {
            reader_of(
                cs,
                &pack_from(first),
                first,
                Some(Arc::clone(cache)),
                KIND_STRING_FWD,
            )
        }

        /// A cache whose allowance holds nothing: packs stay ranged.
        fn ranged_cache() -> Arc<LeafletCache> {
            Arc::new(LeafletCache::with_max_mb(16))
        }

        fn allowance_cache(bytes: u64) -> Arc<LeafletCache> {
            Arc::new(LeafletCache::with_max_mb(16).with_dictionary_allowance(bytes))
        }

        /// An allowance whose rent never ages: for eviction and sharing, with
        /// room for a pack or two, where a pack's own misses would otherwise
        /// age its rent before it reaches its size.
        fn unaging_cache(bytes: u64) -> Arc<LeafletCache> {
            Arc::new(LeafletCache::with_max_mb(16).with_unaging_dictionary_allowance(bytes))
        }

        fn lookup(reader: &ForwardPackReader, id: u64) -> Option<Vec<u8>> {
            let mut out = Vec::new();
            reader
                .forward_lookup_into(id, &mut out)
                .unwrap()
                .then_some(out)
        }

        /// Read the values of the pack at `first` in `ids` (offsets from
        /// it), checking each.
        fn read_ids(reader: &ForwardPackReader, first: u64, ids: std::ops::Range<u64>) {
            for id in first + ids.start..first + ids.end {
                assert_eq!(lookup(reader, id), Some(value(id)), "id {id}");
            }
        }

        fn read_all(reader: &ForwardPackReader, first: u64) {
            read_ids(reader, first, 0..COUNT as u64);
        }

        fn has_units(cache: &LeafletCache) -> bool {
            cache
                .occupancy()
                .iter()
                .any(|(kind, _, _)| *kind == "source-unit")
        }

        fn identity(cs: &SegmentedContentStore, first: u64) -> u128 {
            let cid = cs.put_sync(
                ContentKind::DictBlob {
                    dict: DictKind::StringForward,
                },
                &pack_from(first),
            );
            xxhash_rust::xxh3::xxh3_128(&cid.to_bytes())
        }

        fn reason_of(cache: &LeafletCache) -> u8 {
            let report = cache.dictionary_allowance().pack_report();
            assert_eq!(report.len(), 1);
            report[0].reason
        }

        /// A ranged lookup reads the units holding its offsets and value,
        /// not the page: the page check on first visit, the offsets and the
        /// value are each a unit or two of a page that spans many.
        #[test]
        fn ranged_lookups_read_a_few_units_of_a_page() {
            let cs = SegmentedContentStore::new(true);
            let pack = pack_from(FIRST);
            let meta = parse_pack_meta(&pack).unwrap();
            assert!(
                meta.directory[0].page_len > 6 * 4096,
                "a page spans many units"
            );
            let reader = reader_of(&cs, &pack, FIRST, Some(ranged_cache()), KIND_STRING_FWD);

            let opened = {
                assert_eq!(lookup(&reader, FIRST + 7), Some(value(FIRST + 7)));
                cs.reads()
            };
            // Header and directory, then whole units through the cache.
            let units: Vec<_> = opened[2..].to_vec();
            assert!(units.len() <= 4, "{units:?}");
            assert!(units
                .iter()
                .all(|r| r.start % 4096 == 0 && r.end - r.start == 4096));

            assert_eq!(lookup(&reader, FIRST + 8), Some(value(FIRST + 8)));
            assert_eq!(
                cs.reads().len(),
                opened.len(),
                "a neighbour reads nothing new"
            );
            assert_eq!(cs.whole_reads.load(AtomicOrdering::SeqCst), 0);
        }

        #[test]
        fn every_value_reads_back_by_range() {
            for pack in [pack_with_pages(FIRST, 512), pack_from(FIRST)] {
                let cs = SegmentedContentStore::new(true);
                let reader = reader_of(&cs, &pack, FIRST, Some(ranged_cache()), KIND_STRING_FWD);
                read_all(&reader, FIRST);
                assert_eq!(lookup(&reader, FIRST + COUNT as u64), None);
                assert_eq!(lookup(&reader, FIRST - 1), None);
                assert_eq!(
                    reader.forward_lookup_str(FIRST).unwrap(),
                    Some(String::from_utf8(value(FIRST)).unwrap())
                );
                assert_eq!(cs.whole_reads.load(AtomicOrdering::SeqCst), 0);
            }
        }

        /// Offsets out of order, a final offset or an entry count that
        /// disagree with the directory: each is an error for lookups on that
        /// page, and other pages still read.
        #[test]
        fn ranged_lookups_check_the_page_structure() {
            let pack = pack_from(FIRST);
            let meta = parse_pack_meta(&pack).unwrap();
            let page = meta.directory[0];
            let base = page.page_offset as usize;
            let count = page.entry_count as usize;
            let last = FIRST + COUNT as u64 - 1;
            let corruptions: [(&str, usize, u32); 3] = [
                ("entry count", base, page.entry_count + 1),
                ("final offset", base + 4 + count * 4, 1),
                ("offsets out of order", base + 4 + 4, u32::MAX),
            ];
            for (what, at, bytes) in corruptions {
                let mut bad = pack.clone();
                bad[at..at + 4].copy_from_slice(&bytes.to_le_bytes());
                let cs = SegmentedContentStore::new(true);
                let reader = reader_of(&cs, &bad, FIRST, Some(ranged_cache()), KIND_STRING_FWD);
                let mut out = Vec::new();
                let id = if what == "offsets out of order" {
                    FIRST + 1
                } else {
                    FIRST
                };
                assert!(reader.forward_lookup_into(id, &mut out).is_err(), "{what}");
                assert!(
                    reader.forward_lookup_into(id, &mut out).is_err(),
                    "{what} again"
                );
                assert_eq!(
                    lookup(&reader, last),
                    Some(value(last)),
                    "{what}: other pages"
                );
            }
        }

        /// Warming a ranged pack opens it and decrypts no value.
        #[test]
        fn prewarm_reads_only_the_directory() {
            let cs = SegmentedContentStore::new(true);
            let reader = adaptive(&cs, FIRST, &allowance_cache(u64::MAX));
            assert!(reader.prewarm(u64::MAX) > 0);
            assert_eq!(cs.reads().len(), 2, "header and directory only");
        }

        /// A reader without a cache to keep units in, or a store without
        /// ranged reads, decrypts the pack whole, once.
        #[test]
        fn without_a_cache_or_ranged_reads_a_pack_is_read_whole() {
            for (ranged, cache) in [(true, None), (false, Some(ranged_cache()))] {
                let cs = SegmentedContentStore::new(ranged);
                let reader = reader_of(&cs, &pack_from(FIRST), FIRST, cache, KIND_STRING_FWD);
                read_all(&reader, FIRST);
                assert_eq!(cs.whole_reads.load(AtomicOrdering::SeqCst), 1, "{ranged}");
                assert!(cs.reads().is_empty(), "no range reads: {ranged}");
            }
        }

        /// A pack is read by range until its cache misses have decrypted as
        /// many bytes as it holds — cache hits do not count — then it is
        /// promoted whole, reusing its cached units, its units are dropped
        /// from the cache, and lookups read nothing more.
        #[test]
        fn adaptive_packs_promote_once_misses_rent_their_size() {
            let len = pack_from(FIRST).len() as u64;
            let cs = SegmentedContentStore::new(true);
            let cache = allowance_cache(2 * len);
            let reader = adaptive(&cs, FIRST, &cache);
            read_ids(&reader, FIRST, 0..HALF);
            read_ids(&reader, FIRST, 0..HALF);
            read_ids(&reader, FIRST, 0..HALF);
            assert_eq!(
                cache.dictionary_allowance().stats().promoted,
                0,
                "hits are free"
            );
            assert!(has_units(&cache));
            read_ids(&reader, FIRST, HALF..COUNT as u64);
            let stats = cache.dictionary_allowance().stats();
            assert_eq!((stats.promoted, stats.resident_bytes), (1, len));
            assert_eq!(stats.assembly_decrypted + stats.assembly_reused, len);
            assert!(stats.assembly_reused > 0, "cached units are reused");
            assert_eq!(stats.rent_covered_bytes, len, "every unit missed once");
            assert_eq!(
                (stats.crossing_coverage[9], stats.promotion_coverage[9]),
                (1, 1)
            );
            let reads = cs.reads().len();
            read_all(&reader, FIRST);
            assert_eq!(cs.reads().len(), reads, "served whole");
            assert!(!has_units(&cache), "its units are dropped");
            let report = cache.dictionary_allowance().pack_report();
            assert_eq!(report.len(), 1);
            assert!(report[0].resident && report[0].promotions == 1 && report[0].rent == 0);
            assert_eq!(cs.whole_reads.load(AtomicOrdering::SeqCst), 0);
        }

        /// Promoting a pack that needs the room evicts the other; the evicted
        /// pack reads by range again and rents from nothing, so reading half
        /// of it does not promote it straight back.
        #[test]
        fn an_evicted_pack_reads_by_range_and_rents_again() {
            let len = pack_from(FIRST).len() as u64;
            let other = FIRST + 10 * COUNT as u64;
            let cs = SegmentedContentStore::new(true);
            let cache = unaging_cache(len + len / 2);
            let a = adaptive(&cs, FIRST, &cache);
            let b = adaptive(&cs, other, &cache);
            read_all(&a, FIRST);
            read_ids(&a, FIRST, 0..HALF);
            read_all(&b, other);
            let stats = cache.dictionary_allowance().stats();
            assert_eq!(
                (stats.promoted, stats.evicted, stats.resident_bytes),
                (2, 1, len)
            );
            let reads = cs.reads().len();
            read_ids(&a, FIRST, 0..HALF);
            assert!(cs.reads().len() > reads, "the evicted pack reads by range");
            assert_eq!(
                cache.dictionary_allowance().stats().promoted,
                2,
                "rent restarted"
            );
            assert_eq!(cache.dictionary_allowance().stats().repromoted, 0);
            read_ids(&a, FIRST, HALF..COUNT as u64);
            let stats = cache.dictionary_allowance().stats();
            assert_eq!(
                (stats.promoted, stats.repromoted, stats.repromoted_bytes),
                (3, 1, len),
                "promoted again once it rents its size again"
            );
            read_all(&b, other);
            let stats = cache.dictionary_allowance().stats();
            assert_eq!(
                (stats.evicted, stats.repromoted),
                (3, 2),
                "b rents its size again and evicts a"
            );
        }

        /// An evicted pack a reader still holds stays charged, so a promotion
        /// needing its bytes waits; the pack's own handle stops using it at
        /// once and reads by range.
        #[test]
        fn an_evicted_pack_is_charged_until_released_and_used_no_more() {
            let len = pack_from(FIRST).len() as u64;
            let other = FIRST + 10 * COUNT as u64;
            let cs = SegmentedContentStore::new(true);
            let cache = unaging_cache(len + len / 2);
            let a = adaptive(&cs, FIRST, &cache);
            let b = adaptive(&cs, other, &cache);
            read_all(&a, FIRST);
            let allowance = Arc::clone(cache.dictionary_allowance());
            let pin = allowance.resident(identity(&cs, FIRST)).expect("promoted");
            read_all(&b, other);
            let stats = allowance.stats();
            assert_eq!((stats.promoted, stats.evicted, stats.used), (1, 1, len));
            assert!(stats.no_room > 0, "b waits for the pinned bytes");
            let reads = cs.reads().len();
            assert_eq!(lookup(&a, FIRST + 1), Some(value(FIRST + 1)));
            assert!(cs.reads().len() > reads, "the evicted copy is not used");
            drop(pin);
            assert_eq!(allowance.stats().used, 0);
            assert_eq!(lookup(&b, other + 1), Some(value(other + 1)));
            assert_eq!(
                allowance.stats().promoted,
                2,
                "b is promoted once there is room"
            );
        }

        fn guarded(
            cs: &SegmentedContentStore,
            cache: &Arc<LeafletCache>,
            coverage: f64,
        ) -> ForwardPackReader {
            adaptive(cs, FIRST, cache).with_promote_coverage(coverage)
        }

        /// A pack whose misses keep landing on the same small region — its
        /// units falling out of the cache — rents its size without covering
        /// it: the coverage guard turns it away for that until a rental
        /// covers half of it. Without the guard the same rental promotes.
        #[test]
        fn the_coverage_guard_waits_for_a_rental_to_cover_the_pack() {
            let len = pack_from(FIRST).len() as u64;
            for coverage in [PROMOTE_COVERAGE, 0.0] {
                let cs = SegmentedContentStore::new(true);
                let cache = allowance_cache(2 * len);
                let reader = guarded(&cs, &cache, coverage);
                for _ in 0..12 {
                    read_ids(&reader, FIRST, 0..COUNT as u64 / 10);
                    cache.invalidate_all();
                }
                let stats = cache.dictionary_allowance().stats();
                if coverage > 0.0 {
                    assert_eq!(stats.promoted, 0, "a small region re-missed");
                    assert_eq!(reason_of(&cache), reject::COVERAGE);
                    assert_eq!(
                        stats.crossing_coverage.iter().sum::<u64>(),
                        1,
                        "rent reached once"
                    );
                    assert!(
                        stats.crossing_coverage[..5].iter().sum::<u64>() == 1,
                        "below half"
                    );
                    read_ids(&reader, FIRST, COUNT as u64 / 10..COUNT as u64 * 6 / 10);
                    let stats = cache.dictionary_allowance().stats();
                    assert_eq!(stats.promoted, 1, "half covered");
                    assert!(stats.rent_covered_bytes * 2 >= len);
                } else {
                    assert_eq!(stats.promoted, 1, "no guard");
                }
                read_all(&reader, FIRST);
            }
        }

        /// Under drifting misses — each pack missed a little at a time while
        /// the others' misses run the rent clock — recent rent never reaches
        /// a pack's size, so nothing is bought and the room is left unused.
        #[test]
        fn drifting_misses_buy_nothing() {
            let len = pack_from(FIRST).len() as u64;
            let others: Vec<u64> = (1..=8).map(|k| FIRST + 10 * k * COUNT as u64).collect();
            let cs = SegmentedContentStore::new(true);
            let cache = allowance_cache(len + len / 4);
            let a = adaptive(&cs, FIRST, &cache);
            let rest: Vec<_> = others.iter().map(|&f| adaptive(&cs, f, &cache)).collect();
            for round in 0..40 {
                read_ids(&a, FIRST, 0..COUNT as u64 / 40);
                let k = round % 8;
                read_ids(&rest[k], others[k], 0..COUNT as u64 * 4 / 10);
                cache.invalidate_all();
            }
            let stats = cache.dictionary_allowance().stats();
            assert_eq!((stats.promoted, stats.used), (0, 0), "room left unused");
            assert!(stats.rent_epoch >= 8, "{} rent epochs", stats.rent_epoch);
            let report = cache.dictionary_allowance().pack_report();
            assert!(report
                .iter()
                .all(|p| p.reason == reject::RENT && p.recent_rent < len && p.rent >= len / 2));
        }

        /// A pack now in use replaces one the workload has left as soon as
        /// its recent rent reaches its size. (With room for one and a half
        /// packs, rent halves every pack and a half missed, so that takes b
        /// two passes.)
        #[test]
        fn recent_rent_follows_a_switch() {
            let len = pack_from(FIRST).len() as u64;
            let other = FIRST + 10 * COUNT as u64;
            let cs = SegmentedContentStore::new(true);
            let cache = allowance_cache(len + len / 2);
            let a = adaptive(&cs, FIRST, &cache);
            let b = adaptive(&cs, other, &cache);
            for _ in 0..3 {
                read_all(&a, FIRST);
            }
            read_all(&b, other);
            assert_eq!(cache.dictionary_allowance().stats().promoted, 1);
            cache.invalidate_all();
            read_all(&b, other);
            let stats = cache.dictionary_allowance().stats();
            assert_eq!((stats.promoted, stats.evicted), (2, 1), "b replaces a");
            let reads = cs.reads().len();
            read_all(&b, other);
            assert_eq!(cs.reads().len(), reads, "b served whole");
        }

        /// The rent clock counts the bytes forward-pack lookups miss, and
        /// nothing else: not cache hits, and not what a promotion decrypts
        /// to assemble the pack.
        #[test]
        fn the_rent_clock_counts_only_range_misses() {
            let len = pack_from(FIRST).len() as u64;
            let cs = SegmentedContentStore::new(true);
            let cache = allowance_cache(2 * len);
            let reader = adaptive(&cs, FIRST, &cache);
            // Unit reads: misses and assembly, not the pack's opening reads.
            let read_bytes = |cs: &SegmentedContentStore| -> u64 {
                cs.reads()
                    .iter()
                    .filter(|r| r.start % 4096 == 0 && r.end - r.start == 4096)
                    .map(|r| r.end.min(len) - r.start)
                    .sum()
            };
            read_ids(&reader, FIRST, 0..HALF);
            let missed = read_bytes(&cs);
            let stats = cache.dictionary_allowance().stats();
            assert_eq!(stats.rent_counted, missed);
            read_ids(&reader, FIRST, 0..HALF);
            assert_eq!(
                cache.dictionary_allowance().stats().rent_counted,
                missed,
                "hits"
            );
            cache.invalidate_all();
            read_ids(&reader, FIRST, HALF..COUNT as u64);
            let stats = cache.dictionary_allowance().stats();
            assert_eq!(stats.promoted, 1);
            assert!(
                stats.assembly_decrypted > 0,
                "the first half decrypted again"
            );
            assert_eq!(
                stats.rent_counted,
                read_bytes(&cs) - stats.assembly_decrypted
            );
        }

        /// Promoted packs live in anonymous mappings of their own.
        #[cfg(not(target_arch = "wasm32"))]
        #[test]
        fn promotions_are_mapped() {
            let len = pack_from(FIRST).len() as u64;
            let cs = SegmentedContentStore::new(true);
            let cache = unaging_cache(len);
            let reader = adaptive(&cs, FIRST, &cache);
            read_all(&reader, FIRST);
            let reads = cs.reads().len();
            read_all(&reader, FIRST);
            assert_eq!(cs.reads().len(), reads, "served whole");
            let pack = cache
                .dictionary_allowance()
                .resident(identity(&cs, FIRST))
                .expect("promoted");
            assert!(pack.is_mapped());
            assert_eq!(pack.len() as u64, len);
        }

        /// Readers of the same pack share its promotion: the other reader
        /// adopts the allowance's copy without reading.
        #[test]
        fn adaptive_readers_share_a_promotion() {
            let len = pack_from(FIRST).len() as u64;
            let cs = SegmentedContentStore::new(true);
            let cache = unaging_cache(len);
            let first = adaptive(&cs, FIRST, &cache);
            let second = adaptive(&cs, FIRST, &cache);
            assert_eq!(lookup(&second, FIRST), Some(value(FIRST)));
            read_all(&first, FIRST);
            let reads = cs.reads().len();
            read_all(&second, FIRST);
            assert_eq!(cs.reads().len(), reads, "adopted, not read");
            assert_eq!(cache.dictionary_allowance().stats().promoted, 1);
        }

        /// A pack larger than the whole allowance stays ranged however much
        /// it rents.
        #[test]
        fn an_adaptive_pack_larger_than_the_allowance_stays_ranged() {
            let len = pack_from(FIRST).len() as u64;
            let cs = SegmentedContentStore::new(true);
            let cache = allowance_cache(len - 1);
            let reader = adaptive(&cs, FIRST, &cache);
            for _ in 0..3 {
                read_all(&reader, FIRST);
                cache.invalidate_all();
            }
            let stats = cache.dictionary_allowance().stats();
            assert_eq!((stats.promoted, stats.used), (0, 0));
            let report = cache.dictionary_allowance().pack_report();
            assert_eq!(report.len(), 1);
            let pack = report[0];
            assert_eq!(
                (pack.kind, pack.len, pack.resident, pack.reason),
                (KIND_STRING_FWD, len, false, reject::TOO_LARGE)
            );
            assert!(pack.rent >= len && pack.covered <= len && pack.promotions == 0);
        }

        /// Readers on many threads — some sharing a reader, some reading the
        /// same packs through readers of their own — promote and evict packs
        /// under one another while the leaflet cache is dropped beneath them:
        /// every value reads back, charges never pass the allowance, and once
        /// the readers stop nothing evicted is still held.
        #[test]
        fn concurrent_readers_promote_and_evict_within_the_allowance() {
            let len = pack_from(FIRST).len() as u64;
            let firsts: Vec<u64> = (0..4).map(|k| FIRST + 10 * k * COUNT as u64).collect();
            let cs = SegmentedContentStore::new(true);
            let cache = unaging_cache(len + len / 2);
            let shared: Vec<_> = firsts.iter().map(|&f| adaptive(&cs, f, &cache)).collect();
            std::thread::scope(|scope| {
                for t in 0..8u64 {
                    let (cs, cache, shared, firsts) = (&cs, &cache, &shared, &firsts);
                    scope.spawn(move || {
                        let own: Vec<_> = firsts.iter().map(|&f| adaptive(cs, f, cache)).collect();
                        for round in 0..30u64 {
                            let k = ((t + round) % 4) as usize;
                            let reader = if t % 2 == 0 { &shared[k] } else { &own[k] };
                            let start = (t * 131 + round * 17) % (COUNT as u64 - 300);
                            read_ids(reader, firsts[k], start..start + 300);
                            if round % 7 == t % 7 {
                                cache.invalidate_all();
                            }
                            let stats = cache.dictionary_allowance().stats();
                            assert!(stats.used <= stats.capacity, "{stats:?}");
                        }
                    });
                }
            });
            let stats = cache.dictionary_allowance().stats();
            assert!(
                stats.promoted > 4 && stats.evicted > 0,
                "churned: {stats:?}"
            );
            assert!(stats.peak <= stats.capacity);
            assert_eq!(stats.outstanding, 0, "{stats:?}");
            assert_eq!(stats.used, stats.resident_bytes);
            for (reader, &first) in shared.iter().zip(&firsts) {
                read_all(reader, first);
            }
        }

        /// A reload over a root that keeps a pack carries its handle over:
        /// the promoted copy serves the new reader with no read and no second
        /// promotion, and outlives the old reader.
        #[test]
        fn a_reload_carries_a_promoted_pack_over() {
            let len = pack_from(FIRST).len() as u64;
            let cs = SegmentedContentStore::new(true);
            let cache = allowance_cache(2 * len);
            let pack_cid = cs.put_sync(
                ContentKind::DictBlob {
                    dict: DictKind::StringForward,
                },
                &pack_from(FIRST),
            );
            let refs = vec![PackBranchEntry {
                first_id: FIRST,
                last_id: FIRST + COUNT as u64 - 1,
                pack_cid,
            }];
            let store: Arc<dyn ContentStore> = Arc::new(cs.clone());
            let open = |prev: Option<&ForwardPackReader>| {
                let (store, refs) = (Arc::clone(&store), refs.clone());
                // An owned stand-in for `prev`, which the runtime cannot borrow.
                let prev = prev.map(|p| ForwardPackReader {
                    packs: p.packs.clone(),
                    load_ctx: Some(LoadContext {
                        cs: Arc::clone(&store),
                        expected_kind: KIND_STRING_FWD,
                        expected_ns_code: 0,
                        page_cache: None,
                        promote_coverage: PROMOTE_COVERAGE,
                    }),
                });
                crate::read::binary_index_store::run_sync_on_runtime(async move {
                    ForwardPackReader::from_pack_refs_reusing(
                        store,
                        &refs,
                        KIND_STRING_FWD,
                        0,
                        prev.as_ref(),
                    )
                    .await
                })
                .unwrap()
                .with_page_cache(Some(Arc::clone(&cache)))
            };
            let old = open(None);
            read_all(&old, FIRST);
            assert_eq!(cache.dictionary_allowance().stats().promoted, 1);
            let new = open(Some(&old));
            assert_eq!(new.packs_shared_with(&old), 1, "the handle carried over");
            drop(old);
            let reads = cs.reads().len();
            read_all(&new, FIRST);
            assert_eq!(cs.reads().len(), reads, "served whole");
            assert_eq!(cache.dictionary_allowance().stats().promoted, 1);
        }

        /// Once no reader holds a pack, nothing keeps it described: its meter
        /// leaves the report, and its promoted copy is given up to make room
        /// for a pack in use, returning its charge.
        #[test]
        fn a_retired_pack_leaves_no_meter_and_gives_up_its_room() {
            let len = pack_from(FIRST).len() as u64;
            let other = FIRST + 10 * COUNT as u64;
            let cs = SegmentedContentStore::new(true);
            let cache = unaging_cache(len + len / 2);
            let retired = adaptive(&cs, FIRST, &cache);
            read_all(&retired, FIRST);
            drop(retired);
            let allowance = Arc::clone(cache.dictionary_allowance());
            assert!(allowance.pack_report().is_empty(), "no meter left");
            assert_eq!(allowance.stats().resident_bytes, len);
            let live = adaptive(&cs, other, &cache);
            read_all(&live, other);
            let stats = allowance.stats();
            assert_eq!((stats.promoted, stats.evicted), (2, 1));
            assert_eq!((stats.used, stats.outstanding), (len, 0));
            assert!(allowance.resident(identity(&cs, other)).is_some());
            assert!(allowance.resident(identity(&cs, FIRST)).is_none());
        }

        /// A pack that fails validation is an error, and takes nothing from
        /// the allowance.
        #[test]
        fn a_pack_that_fails_validation_is_an_error() {
            let cs = SegmentedContentStore::new(true);
            let cache = allowance_cache(u64::MAX);
            let reader = reader_of(
                &cs,
                &pack_from(FIRST),
                FIRST,
                Some(Arc::clone(&cache)),
                KIND_SUBJECT_FWD,
            );
            let err = reader.forward_lookup_str(FIRST).unwrap_err();
            assert!(err.to_string().contains("pack kind"), "{err}");
            assert_eq!(cache.dictionary_allowance().stats().used, 0);
        }

        /// Time per lookup on the slowest of `threads` threads each looking
        /// up `n` ids in `reader`, started together once all have warmed up;
        /// the fastest of three runs.
        fn lookup_ns(reader: &ForwardPackReader, threads: usize, n: u64) -> f64 {
            let run = || {
                let start = std::sync::Barrier::new(threads);
                let slowest = std::thread::scope(|scope| {
                    let workers: Vec<_> = (0..threads as u64)
                        .map(|t| {
                            let start = &start;
                            scope.spawn(move || {
                                // Large enough to share no cache line with the pack.
                                let mut out = Vec::with_capacity(16 * 1024);
                                let mut look = |i: u64| {
                                    out.clear();
                                    let id = FIRST + (i * 7 + t * 13) % COUNT as u64;
                                    assert!(reader.forward_lookup_into(id, &mut out).unwrap());
                                };
                                (0..COUNT as u64).for_each(&mut look);
                                start.wait();
                                let began = std::time::Instant::now();
                                (0..n).for_each(&mut look);
                                began.elapsed()
                            })
                        })
                        .collect();
                    workers.into_iter().map(|w| w.join().unwrap()).max()
                });
                slowest.expect("a worker").as_nanos() as f64 / n as f64
            };
            (0..3).map(|_| run()).fold(f64::INFINITY, f64::min)
        }

        /// Lookups in a promoted pack on several threads at once each run
        /// about as fast as on one: they find the pack without a lock or a
        /// count on it, so, like lookups in a pack held whole from the start,
        /// they write nothing they share.
        #[test]
        fn lookups_in_a_promoted_pack_scale_across_threads() {
            // Eight threads catch a regression in a debug build, where a
            // lookup's own work hides most of what sharing costs: CI's eight
            // cores are the gate, and fewer only check that lookups keep
            // pace. Run alone (`.config/nextest.toml`).
            let threads = std::thread::available_parallelism().map_or(1, |n| n.get().min(8));
            if threads < 2 {
                eprintln!("SKIPPED: needs two cores, found {threads}");
                return;
            }
            let len = pack_from(FIRST).len() as u64;
            let cs = SegmentedContentStore::new(true);
            let cache = unaging_cache(len);
            let reader = adaptive(&cs, FIRST, &cache);
            read_all(&reader, FIRST);
            assert_eq!(cache.dictionary_allowance().stats().promoted, 1);
            let reads = cs.reads().len();
            let n = 500_000;
            let one = lookup_ns(&reader, 1, n);
            let many = lookup_ns(&reader, threads, n);
            assert_eq!(cs.reads().len(), reads, "served whole");
            assert!(
                many < 3.0 * one,
                "{threads} threads: {many:.0} ns a lookup against {one:.0} on one"
            );
        }
    }
}
