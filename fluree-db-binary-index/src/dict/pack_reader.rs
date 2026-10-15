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

use super::allowance::{reject, Admission, DictionaryAllowance, HeldPack, Promotion};
use super::forward_pack::{
    find_page_in_dir, lookup_in_pack, page_dir_range, parse_pack_meta, parse_pack_meta_parts,
    ForwardPage, PackHeader, PageDirEntry, ParsedPackMeta, PACK_HEADER_SIZE,
};
use crate::format::wire_helpers::PackBranchEntry;
use crate::read::cached_units::{CachedUnits, MissMeter};
use crate::read::leaflet_cache::{ForwardPageKey, LeafletCache};
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
    /// A local pack whose store decrypts on read, decrypted whole within the
    /// cache's dictionary allowance.
    Held(Arc<HeldPack>),
    /// A local pack whose store decrypts on read, read by range: a page is
    /// decrypted on first use into the leaflet cache, and the pack is never
    /// held decrypted whole.
    Paged {
        source: Arc<dyn RangeSource>,
        /// `xxh3_128(pack CID bytes)`, the pack half of a page's cache key.
        pack_key: u128,
    },
    /// A local pack whose store decrypts on read, where a lookup reads only
    /// its entry's two offsets and its value, through the leaflet cache's
    /// decrypted units: a page is never assembled.
    Ranged {
        units: PackUnits,
        /// Per page: whether its own entry count and final offset have been
        /// checked against its directory entry.
        checked: Box<[AtomicBool]>,
    },
    /// A local pack whose store decrypts on read, whole while the cache's
    /// dictionary allowance holds it and ranged otherwise.
    Adaptive(Box<AdaptivePack>),
}

/// A pack read by range until its ranged reads have decrypted, on cache
/// misses since it was last whole, as many bytes as it holds; then it is
/// promoted into the dictionary allowance, which may later evict it.
struct AdaptivePack {
    units: CachedUnits,
    checked: Box<[AtomicBool]>,
    allowance: Arc<DictionaryAllowance>,
    meter: Arc<MissMeter>,
    /// The allowance's copy as last seen, used while it lives and is not
    /// evicted. Readers hold it only for a lookup.
    seen: parking_lot::RwLock<std::sync::Weak<HeldPack>>,
    /// The allowance generation at which this pack was last found absent.
    absent_at: std::sync::atomic::AtomicU64,
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
        let meter = allowance.meter(units.identity(), units.unit_count(), units.len(), kind);
        let min_covered = (units.len() as f64 * coverage.clamp(0.0, 1.0)).ceil() as u64;
        Self {
            units: units.with_meter(Arc::clone(&meter)),
            checked,
            allowance,
            meter,
            seen: parking_lot::RwLock::new(std::sync::Weak::new()),
            absent_at: std::sync::atomic::AtomicU64::new(u64::MAX),
            min_covered,
        }
    }

    fn lookup_into(&self, meta: &ParsedPackMeta, id: u64, out: &mut Vec<u8>) -> io::Result<bool> {
        if self.allowance.counts_lookups() {
            self.meter.note_lookup();
        }
        if let Some(whole) = self.whole() {
            let mark = out.len();
            let found = lookup_whole(&whole, meta, id, out);
            whole.note_served(out.len() - mark);
            return Ok(found);
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
        match self.allowance.admission() {
            Admission::Demand => self.by_demand()?,
            _ => self.by_coverage()?,
        }
        Ok(found)
    }

    /// Promote once its rent — since it was last whole, or recent — reaches
    /// its size and has covered enough of it.
    fn by_coverage(&self) -> io::Result<()> {
        let len = self.units.len();
        let admission = self.allowance.admission();
        let rent = match admission {
            Admission::Coverage => self.meter.bytes(),
            _ => self.meter.recent_rent(),
        };
        if len > self.allowance.capacity() {
            self.meter.set_reason(reject::TOO_LARGE);
        } else if rent < len {
            self.meter.set_reason(reject::RENT);
        } else {
            let covered = self.meter.covered();
            if self.meter.first_crossing(len) {
                self.allowance.note_rent_reached(covered, len);
            }
            if covered < self.min_covered {
                self.meter.set_reason(reject::COVERAGE);
            } else {
                let identity = self.units.identity();
                let assemble = |buf: &mut [u8]| self.units.assemble_into(buf);
                let promotion = if admission == Admission::RecentCompared {
                    self.allowance.promote_compared(
                        identity,
                        len,
                        covered,
                        &self.meter,
                        assemble,
                    )?
                } else {
                    self.allowance.promote(identity, len, covered, assemble)?
                };
                self.settle(promotion);
            }
        }
        Ok(())
    }

    /// Weigh it for admission, at most once a lookup epoch, once its recent
    /// rent reaches its size.
    fn by_demand(&self) -> io::Result<()> {
        if self.meter.recent_rent() < self.units.len() {
            self.meter.set_reason(reject::RENT);
        } else if self.meter.weigh_now() {
            let promotion = self.allowance.admit(
                self.units.identity(),
                self.units.len(),
                &self.meter,
                |buf| self.units.assemble_into(buf),
            )?;
            self.settle(promotion);
        }
        Ok(())
    }

    /// The allowance's copy, if it holds one.
    fn whole(&self) -> Option<Arc<HeldPack>> {
        if let Some(pack) = self.seen.read().upgrade() {
            if !pack.is_evicted() {
                pack.mark_used();
                return Some(pack);
            }
        }
        let generation = self.allowance.generation();
        if self.absent_at.swap(generation, Ordering::Relaxed) == generation {
            return None;
        }
        let pack = self.allowance.resident(self.units.identity())?;
        self.adopt(&pack);
        pack.mark_used();
        Some(pack)
    }

    /// Use `pack` from now on; rent starts again from nothing.
    fn adopt(&self, pack: &Arc<HeldPack>) {
        *self.seen.write() = Arc::downgrade(pack);
        self.meter.reset();
    }

    fn settle(&self, promotion: Promotion) {
        self.meter.set_reason(promotion.reason());
        match promotion {
            Promotion::Promoted(pack) => {
                if self.meter.note_promotion() > 0 {
                    self.allowance.note_repromotion(pack.len() as u64);
                }
                self.adopt(&pack);
                self.units.forget_cached();
                fluree_db_core::io_stats::record_work("pack promoted whole", pack.len());
            }
            Promotion::Resident(pack) => self.adopt(&pack),
            Promotion::Busy | Promotion::NoRoom | Promotion::Outweighed | Promotion::TooLarge => {}
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

/// How a ranged pack's bytes are read: through the leaflet cache's kept
/// units, or straight from the source when the reader has no cache.
enum PackUnits {
    Cached(CachedUnits),
    Direct(Arc<dyn RangeSource>),
}

impl UnitRead for PackUnits {
    fn extend_into(&self, range: std::ops::Range<u64>, out: &mut Vec<u8>) -> io::Result<()> {
        let (want, mark) = ((range.end - range.start) as usize, out.len());
        match self {
            PackUnits::Cached(units) => units.extend_into(range, out)?,
            PackUnits::Direct(source) => out.extend_from_slice(
                &source
                    .read_range(range)
                    .map_err(|e| io::Error::other(format!("forward pack: {e}")))?,
            ),
        }
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
    fn lookup_into(
        &self,
        meta: &ParsedPackMeta,
        ctx: Option<&LoadContext>,
        id: u64,
        out: &mut Vec<u8>,
    ) -> io::Result<bool> {
        match self {
            PackBacking::Whole(bytes) => Ok(lookup_whole(bytes, meta, id, out)),
            PackBacking::Held(bytes) => Ok(lookup_whole(bytes, meta, id, out)),
            PackBacking::Paged { source, pack_key } => {
                if id < meta.first_id || id > meta.last_id {
                    return Ok(false);
                }
                let Some(page_idx) = find_page_in_dir(&meta.directory, id) else {
                    return Ok(false);
                };
                let dir = &meta.directory[page_idx];
                let page = load_page(source, *pack_key, page_idx, dir, ctx)?;
                Ok(
                    ForwardPage::from_bytes_unchecked(&page, dir.page_first_id)
                        .lookup_into(id, out),
                )
            }
            PackBacking::Adaptive(pack) => pack.lookup_into(meta, id, out),
            PackBacking::Ranged { units, checked } => {
                if id < meta.first_id || id > meta.last_id {
                    return Ok(false);
                }
                let Some(page_idx) = find_page_in_dir(&meta.directory, id) else {
                    return Ok(false);
                };
                ranged_lookup(
                    units,
                    &checked[page_idx],
                    &meta.directory[page_idx],
                    id,
                    out,
                )
            }
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
    /// Where pages of paged packs are kept; without one, each lookup reads
    /// its page again.
    page_cache: Option<Arc<LeafletCache>>,
    /// How packs a store reads by range are held.
    encrypted_packs: EncryptedPacks,
    /// Adaptive mode: the share of a pack its ranged misses must have
    /// covered, besides renting its size, before it is promoted.
    promote_coverage: f64,
}

/// Experiment control: `FLUREE_DICT_PROMOTE_COVERAGE`, a fraction; off (0)
/// when unset.
fn promote_coverage_from_env() -> f64 {
    static COVERAGE: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *COVERAGE.get_or_init(|| {
        std::env::var("FLUREE_DICT_PROMOTE_COVERAGE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0)
    })
}

/// How a local pack whose store decrypts on read is held. Experiment
/// control for the segmented-envelope A/B, not a supported configuration:
/// `FLUREE_ENCRYPTED_PACKS=whole|pages|ranged|bounded`, default `ranged`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EncryptedPacks {
    /// Decrypted whole and held for the reader's lifetime.
    Whole,
    /// Whole pages decrypted into the leaflet cache.
    Pages,
    /// Offsets and values read through the leaflet cache's decrypted units.
    Ranged,
    /// Decrypted whole when the cache's dictionary allowance can hold it,
    /// otherwise ranged.
    Bounded,
    /// Ranged, promoted whole into the dictionary allowance once ranged
    /// misses have decrypted its size; evictable.
    Adaptive,
}

impl EncryptedPacks {
    fn from_env() -> Self {
        static MODE: std::sync::OnceLock<EncryptedPacks> = std::sync::OnceLock::new();
        *MODE.get_or_init(
            || match std::env::var("FLUREE_ENCRYPTED_PACKS").as_deref() {
                Ok("whole") => EncryptedPacks::Whole,
                Ok("pages") => EncryptedPacks::Pages,
                Ok("bounded") => EncryptedPacks::Bounded,
                Ok("adaptive") => EncryptedPacks::Adaptive,
                _ => EncryptedPacks::Ranged,
            },
        )
    }
}

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
                        encrypted_packs: EncryptedPacks::from_env(),
                        promote_coverage: promote_coverage_from_env(),
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
                encrypted_packs: EncryptedPacks::from_env(),
                promote_coverage: promote_coverage_from_env(),
            }),
        })
    }

    #[cfg(test)]
    fn with_encrypted_packs(mut self, mode: EncryptedPacks) -> Self {
        if let Some(ctx) = &mut self.load_ctx {
            ctx.encrypted_packs = mode;
        }
        self
    }

    #[cfg(test)]
    fn with_promote_coverage(mut self, coverage: f64) -> Self {
        if let Some(ctx) = &mut self.load_ctx {
            ctx.promote_coverage = coverage;
        }
        self
    }

    /// Keep pages of packs read by range (encrypted local packs) in `cache`,
    /// within its budget. Without it a lookup reads its page every time.
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
                Ok((_, PackBacking::Held(bytes))) => bytes,
                // Pages decrypt on use; opening read the header and
                // directory, which is what counts against the budget.
                Ok((
                    meta,
                    PackBacking::Paged { .. }
                    | PackBacking::Ranged { .. }
                    | PackBacking::Adaptive(_),
                )) => {
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
        backing.lookup_into(meta, self.load_ctx.as_ref(), id, out)
    }

    /// Convenience: look up and return as a `String`.
    pub fn forward_lookup_str(&self, id: u64) -> io::Result<Option<String>> {
        let Some(handle) = self.find_pack(id) else {
            return Ok(None);
        };
        let (meta, backing) = handle.ensure_loaded(self.load_ctx.as_ref())?;
        let mut value = Vec::new();
        if !backing.lookup_into(meta, self.load_ctx.as_ref(), id, &mut value)? {
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

    if ctx.cs.costly_local_reads() && ctx.encrypted_packs != EncryptedPacks::Whole {
        if let Some(source) = ctx
            .cs
            .get_local_ranged(pack_cid)
            .map_err(|e| io::Error::other(format!("forward pack {pack_cid}: {e}")))?
        {
            if let Some(held) = hold_whole(&source, pack_cid, ctx)? {
                return loaded_held(held, expected_first_id, expected_last_id, ctx);
            }
            return loaded_paged(source, pack_cid, expected_first_id, expected_last_id, ctx);
        }
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

/// In bounded mode, the pack decrypted whole if the cache's dictionary
/// allowance can hold it beside what it already holds.
fn hold_whole(
    source: &Arc<dyn RangeSource>,
    pack_cid: &ContentId,
    ctx: &LoadContext,
) -> io::Result<Option<Arc<HeldPack>>> {
    let (EncryptedPacks::Bounded, Some(cache)) = (ctx.encrypted_packs, &ctx.page_cache) else {
        return Ok(None);
    };
    let len = source.len();
    cache
        .dictionary_allowance()
        .hold(source.cache_identity(), len, || {
            source
                .read_range(0..len)
                .map_err(|e| io::Error::other(format!("forward pack {pack_cid}: {e}")))
        })
}

fn loaded_held(
    held: Arc<HeldPack>,
    expected_first_id: u64,
    expected_last_id: u64,
    ctx: &LoadContext,
) -> io::Result<LazyLoaded> {
    let meta = parse_pack_meta(&held)?;
    validate_lazy_meta(&meta, expected_first_id, expected_last_id, ctx)?;
    fluree_db_core::io_stats::record_work("pack held whole (allowance)", held.len());
    Ok(LazyLoaded {
        meta,
        backing: PackBacking::Held(held),
    })
}

/// Open a pack read by range: its header and page directory are read and
/// validated now, its pages on use.
fn loaded_paged(
    source: Arc<dyn RangeSource>,
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
    let checked = || {
        meta.directory
            .iter()
            .map(|_| AtomicBool::new(false))
            .collect()
    };
    let backing = match (ctx.encrypted_packs, &ctx.page_cache) {
        (EncryptedPacks::Pages, _) => PackBacking::Paged {
            source,
            pack_key: xxhash_rust::xxh3::xxh3_128(pack_cid.to_bytes().as_ref()),
        },
        (EncryptedPacks::Adaptive, Some(cache)) if crate::read::cached_units::enabled() => {
            PackBacking::Adaptive(Box::new(AdaptivePack::new(
                CachedUnits::new(source, Arc::clone(cache)),
                checked(),
                Arc::clone(cache.dictionary_allowance()),
                ctx.promote_coverage,
                ctx.expected_kind,
            )))
        }
        _ => PackBacking::Ranged {
            checked: checked(),
            units: match &ctx.page_cache {
                Some(cache) if crate::read::cached_units::enabled() => {
                    PackUnits::Cached(CachedUnits::new(source, Arc::clone(cache)))
                }
                _ => PackUnits::Direct(source),
            },
        },
    };
    Ok(LazyLoaded { meta, backing })
}

/// A paged pack's page, from the cache or read and validated: a page that
/// fails validation is an error and is not cached.
fn load_page(
    source: &Arc<dyn RangeSource>,
    pack_key: u128,
    page_idx: usize,
    dir: &PageDirEntry,
    ctx: Option<&LoadContext>,
) -> io::Result<Arc<[u8]>> {
    let read = || -> io::Result<Arc<[u8]>> {
        let start = u64::from(dir.page_offset);
        let bytes = source
            .read_range(start..start + u64::from(dir.page_len))
            .map_err(|e| io::Error::other(format!("forward pack page: {e}")))?;
        if bytes.len() != dir.page_len as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "forward pack page: shorter than its directory entry",
            ));
        }
        let page = ForwardPage::from_bytes(&bytes, dir.page_first_id)?;
        if page.entry_count() != dir.entry_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "forward pack page: entry count does not match its directory entry",
            ));
        }
        fluree_db_core::io_stats::record_work("pack page decoded", bytes.len());
        Ok(Arc::from(bytes))
    };
    match ctx.and_then(|ctx| ctx.page_cache.as_ref()) {
        Some(cache) => cache.try_get_or_load_forward_page(
            ForwardPageKey {
                pack: pack_key,
                page: u32::try_from(page_idx).map_err(|_| io::Error::other("page index"))?,
            },
            read,
        ),
        None => read(),
    }
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

    /// Packs of a store whose whole local reads decrypt everything: pages
    /// are read by range on use and kept in the leaflet cache.
    mod paged {
        use super::*;
        use crate::read::binary_index_store::tests::ranged_local::SegmentedContentStore;
        use std::sync::atomic::Ordering as AtomicOrdering;

        const FIRST: u64 = 1_000;
        const COUNT: usize = 2_000;

        fn value(id: u64) -> Vec<u8> {
            format!("value-{id:08}").into_bytes()
        }

        /// A pack of many small pages.
        fn many_page_pack() -> Vec<u8> {
            let entries: Vec<(u64, Vec<u8>)> = (0..COUNT as u64)
                .map(|i| (FIRST + i, value(FIRST + i)))
                .collect();
            let refs: Vec<(u64, &[u8])> =
                entries.iter().map(|(id, v)| (*id, v.as_slice())).collect();
            encode_forward_pack(&refs, KIND_STRING_FWD, 0, 512).unwrap()
        }

        fn reader(
            cs: &SegmentedContentStore,
            pack: &[u8],
            cache: Option<Arc<LeafletCache>>,
        ) -> ForwardPackReader {
            reader_in(cs, pack, cache, EncryptedPacks::Pages)
        }

        fn reader_in(
            cs: &SegmentedContentStore,
            pack: &[u8],
            cache: Option<Arc<LeafletCache>>,
            mode: EncryptedPacks,
        ) -> ForwardPackReader {
            let pack_cid = cs.put_sync(
                ContentKind::DictBlob {
                    dict: DictKind::StringForward,
                },
                pack,
            );
            let refs = vec![PackBranchEntry {
                first_id: FIRST,
                last_id: FIRST + COUNT as u64 - 1,
                pack_cid,
            }];
            let cs: Arc<dyn ContentStore> = Arc::new(cs.clone());
            crate::read::binary_index_store::run_sync_on_runtime(async move {
                ForwardPackReader::from_pack_refs(cs, &refs, KIND_STRING_FWD, 0).await
            })
            .unwrap()
            .with_page_cache(cache)
            .with_encrypted_packs(mode)
        }

        fn lookup(reader: &ForwardPackReader, id: u64) -> Option<Vec<u8>> {
            let mut out = Vec::new();
            reader
                .forward_lookup_into(id, &mut out)
                .unwrap()
                .then_some(out)
        }

        #[test]
        fn lookups_read_the_page_they_need_once() {
            let cs = SegmentedContentStore::new(true);
            let pack = many_page_pack();
            let cache = Arc::new(LeafletCache::with_max_mb(16));
            let reader = reader(&cs, &pack, Some(Arc::clone(&cache)));

            assert_eq!(lookup(&reader, FIRST + 7), Some(value(FIRST + 7)));
            let opened = cs.reads();
            assert_eq!(
                opened.len(),
                3,
                "header, directory and one page: {opened:?}"
            );
            let page_bytes = opened[2].end - opened[2].start;
            assert!(page_bytes < pack.len() as u64 / 20);

            assert_eq!(lookup(&reader, FIRST + 8), Some(value(FIRST + 8)));
            assert_eq!(cs.reads().len(), 3, "the page is cached");
            let last = FIRST + COUNT as u64 - 1;
            assert_eq!(lookup(&reader, last), Some(value(last)));
            assert_eq!(cs.reads().len(), 4, "another page is one more read");
            assert_eq!(
                reader.forward_lookup_str(FIRST + 9).unwrap(),
                Some(String::from_utf8(value(FIRST + 9)).unwrap())
            );
            assert_eq!(lookup(&reader, FIRST + COUNT as u64), None);
            assert_eq!(lookup(&reader, FIRST - 1), None);
            assert_eq!(
                cs.whole_reads.load(AtomicOrdering::SeqCst),
                0,
                "never whole"
            );

            let pages = cache
                .occupancy()
                .into_iter()
                .find(|(kind, _, _)| *kind == "forward-page")
                .expect("pages are in the leaflet cache");
            assert_eq!(pages.1, 2);
        }

        #[test]
        fn every_value_reads_back_through_pages() {
            let cs = SegmentedContentStore::new(true);
            let reader = reader(
                &cs,
                &many_page_pack(),
                Some(Arc::new(LeafletCache::with_max_mb(16))),
            );
            for id in FIRST..FIRST + COUNT as u64 {
                assert_eq!(lookup(&reader, id), Some(value(id)), "id {id}");
            }
            assert_eq!(cs.whole_reads.load(AtomicOrdering::SeqCst), 0);
        }

        /// A page that fails validation is an error every time it is read,
        /// never a cached success.
        #[test]
        fn a_corrupt_page_errors_and_is_not_cached() {
            let mut pack = many_page_pack();
            let meta = parse_pack_meta(&pack).unwrap();
            let page = &meta.directory[0];
            // Offsets must not decrease: make the second one smaller than the first.
            let offsets = page.page_offset as usize + 4;
            pack[offsets..offsets + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            let cs = SegmentedContentStore::new(true);
            let reader = reader(&cs, &pack, Some(Arc::new(LeafletCache::with_max_mb(16))));
            let mut out = Vec::new();
            assert!(reader.forward_lookup_into(FIRST, &mut out).is_err());
            assert!(reader.forward_lookup_into(FIRST, &mut out).is_err());
            let last = FIRST + COUNT as u64 - 1;
            assert_eq!(lookup(&reader, last), Some(value(last)), "other pages read");
        }

        /// Warming a paged pack opens it and decrypts no page.
        #[test]
        fn prewarm_reads_no_pages() {
            let cs = SegmentedContentStore::new(true);
            let reader = reader(
                &cs,
                &many_page_pack(),
                Some(Arc::new(LeafletCache::with_max_mb(16))),
            );
            assert!(reader.prewarm(u64::MAX) > 0);
            assert_eq!(cs.reads().len(), 2, "header and directory only");
        }

        /// A pack of a few large pages, so a page spans many read units.
        fn large_page_pack() -> Vec<u8> {
            let entries: Vec<(u64, Vec<u8>)> = (0..COUNT as u64)
                .map(|i| (FIRST + i, value(FIRST + i)))
                .collect();
            let refs: Vec<(u64, &[u8])> =
                entries.iter().map(|(id, v)| (*id, v.as_slice())).collect();
            encode_forward_pack(&refs, KIND_STRING_FWD, 0, 32 * 1024).unwrap()
        }

        /// A ranged lookup reads the units holding its offsets and value,
        /// not the page: the page check on first visit, the offsets and the
        /// value are each a unit or two of a page that spans many.
        #[test]
        fn ranged_lookups_read_a_few_units_of_a_page() {
            let cs = SegmentedContentStore::new(true);
            let pack = large_page_pack();
            let meta = parse_pack_meta(&pack).unwrap();
            assert!(
                meta.directory[0].page_len > 6 * 4096,
                "a page spans many units"
            );
            let cache = Arc::new(LeafletCache::with_max_mb(16));
            let reader = reader_in(&cs, &pack, Some(Arc::clone(&cache)), EncryptedPacks::Ranged);

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
            assert!(cache
                .occupancy()
                .iter()
                .all(|(kind, _, _)| *kind != "forward-page"));
        }

        #[test]
        fn every_value_reads_back_by_range() {
            for (cache, pack) in [
                (
                    Some(Arc::new(LeafletCache::with_max_mb(16))),
                    many_page_pack(),
                ),
                (None, large_page_pack()),
            ] {
                let cs = SegmentedContentStore::new(true);
                let reader = reader_in(&cs, &pack, cache, EncryptedPacks::Ranged);
                for id in FIRST..FIRST + COUNT as u64 {
                    assert_eq!(lookup(&reader, id), Some(value(id)), "id {id}");
                }
                assert_eq!(lookup(&reader, FIRST + COUNT as u64), None);
                assert_eq!(
                    reader.forward_lookup_str(FIRST).unwrap(),
                    Some(String::from_utf8(value(FIRST)).unwrap())
                );
            }
        }

        /// Offsets out of order, a final offset or an entry count that
        /// disagree with the directory: each is an error for lookups on that
        /// page, and other pages still read.
        #[test]
        fn ranged_lookups_check_the_page_structure() {
            let pack = large_page_pack();
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
                let reader = reader_in(
                    &cs,
                    &bad,
                    Some(Arc::new(LeafletCache::with_max_mb(16))),
                    EncryptedPacks::Ranged,
                );
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

        fn pack_from(first: u64) -> Vec<u8> {
            let entries: Vec<(u64, Vec<u8>)> = (first..first + COUNT as u64)
                .map(|id| (id, value(id)))
                .collect();
            let refs: Vec<(u64, &[u8])> =
                entries.iter().map(|(id, v)| (*id, v.as_slice())).collect();
            encode_forward_pack(&refs, KIND_STRING_FWD, 0, 32 * 1024).unwrap()
        }

        fn bounded_reader(
            cs: &SegmentedContentStore,
            first: u64,
            cache: &Arc<LeafletCache>,
            kind: u8,
        ) -> ForwardPackReader {
            reader_over(cs, first, cache, kind, EncryptedPacks::Bounded)
        }

        fn reader_over(
            cs: &SegmentedContentStore,
            first: u64,
            cache: &Arc<LeafletCache>,
            kind: u8,
            mode: EncryptedPacks,
        ) -> ForwardPackReader {
            let pack_cid = cs.put_sync(
                ContentKind::DictBlob {
                    dict: DictKind::StringForward,
                },
                &pack_from(first),
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
            .with_page_cache(Some(Arc::clone(cache)))
            .with_encrypted_packs(mode)
        }

        fn allowance_cache(bytes: u64) -> Arc<LeafletCache> {
            Arc::new(LeafletCache::with_max_mb(16).with_dictionary_allowance(bytes))
        }

        fn has_units(cache: &LeafletCache) -> bool {
            cache
                .occupancy()
                .iter()
                .any(|(kind, _, _)| *kind == "source-unit")
        }

        /// Within the allowance a pack is decrypted whole, in one read, and
        /// none of it is kept as units; beyond it, it reads by range.
        #[test]
        fn bounded_packs_are_held_whole_only_within_the_allowance() {
            let len = pack_from(FIRST).len() as u64;
            for (allowance, held) in [(len, true), (len - 1, false)] {
                let cs = SegmentedContentStore::new(true);
                let cache = allowance_cache(allowance);
                let reader = bounded_reader(&cs, FIRST, &cache, KIND_STRING_FWD);
                for id in FIRST..FIRST + COUNT as u64 {
                    assert_eq!(lookup(&reader, id), Some(value(id)), "id {id}");
                }
                let stats = cache.dictionary_allowance().stats();
                if held {
                    let reads = cs.reads();
                    assert_eq!(reads.first(), Some(&(0..len)), "one whole read");
                    assert_eq!(reads.len(), 1);
                    assert_eq!((stats.used, stats.admitted, stats.declined), (len, 1, 0));
                    assert!(!has_units(&cache));
                } else {
                    assert!(cs.reads().iter().all(|r| r.end - r.start < len));
                    assert_eq!((stats.used, stats.admitted, stats.declined), (0, 0, 1));
                    assert!(has_units(&cache));
                }
                assert_eq!(cs.whole_reads.load(AtomicOrdering::SeqCst), 0);
            }
        }

        /// The allowance is shared: once one reader's pack fills it, another
        /// reader's pack reads by range, and both read every value.
        #[test]
        fn readers_draw_on_one_allowance() {
            let len = pack_from(FIRST).len() as u64;
            let cs = SegmentedContentStore::new(true);
            let cache = allowance_cache(len + len / 2);
            let first = bounded_reader(&cs, FIRST, &cache, KIND_STRING_FWD);
            let other = FIRST + 10 * COUNT as u64;
            let second = bounded_reader(&cs, other, &cache, KIND_STRING_FWD);
            assert_eq!(lookup(&first, FIRST + 3), Some(value(FIRST + 3)));
            for id in other..other + COUNT as u64 {
                assert_eq!(lookup(&second, id), Some(value(id)), "id {id}");
            }
            let stats = cache.dictionary_allowance().stats();
            assert_eq!((stats.used, stats.admitted, stats.declined), (len, 1, 1));
            assert!(has_units(&cache), "the second pack is read by range");
        }

        /// Readers of the same pack share one held copy, charged once, and
        /// the allowance gets it back when the last of them is dropped.
        #[test]
        fn readers_share_a_held_pack_until_the_last_is_dropped() {
            let len = pack_from(FIRST).len() as u64;
            let cs = SegmentedContentStore::new(true);
            let cache = allowance_cache(len);
            let first = bounded_reader(&cs, FIRST, &cache, KIND_STRING_FWD);
            let second = bounded_reader(&cs, FIRST, &cache, KIND_STRING_FWD);
            assert_eq!(second.packs_shared_with(&first), 0, "separate handles");
            assert_eq!(lookup(&first, FIRST), Some(value(FIRST)));
            assert_eq!(lookup(&second, FIRST + 1), Some(value(FIRST + 1)));
            let stats = cache.dictionary_allowance().stats();
            assert_eq!((stats.used, stats.admitted, stats.shared), (len, 1, 1));
            assert_eq!(cs.reads().len(), 1, "decrypted once");
            drop(first);
            assert_eq!(cache.dictionary_allowance().stats().used, len);
            drop(second);
            let stats = cache.dictionary_allowance().stats();
            assert_eq!((stats.used, stats.packs, stats.released), (0, 0, 1));
        }

        /// A held pack that fails validation is an error and gives its
        /// reservation back.
        #[test]
        fn a_held_pack_that_fails_validation_is_released() {
            let len = pack_from(FIRST).len() as u64;
            let cs = SegmentedContentStore::new(true);
            let cache = allowance_cache(len);
            let reader = bounded_reader(&cs, FIRST, &cache, KIND_SUBJECT_FWD);
            let err = reader.forward_lookup_str(FIRST).unwrap_err();
            assert!(err.to_string().contains("pack kind"), "{err}");
            let stats = cache.dictionary_allowance().stats();
            assert_eq!((stats.used, stats.admitted, stats.released), (0, 1, 1));
        }

        fn adaptive(
            cs: &SegmentedContentStore,
            first: u64,
            cache: &Arc<LeafletCache>,
        ) -> ForwardPackReader {
            reader_over(cs, first, cache, KIND_STRING_FWD, EncryptedPacks::Adaptive)
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

        const HALF: u64 = COUNT as u64 / 2;

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
            let cache = allowance_cache(len + len / 2);
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
            let cache = allowance_cache(len + len / 2);
            let a = adaptive(&cs, FIRST, &cache);
            let b = adaptive(&cs, other, &cache);
            read_all(&a, FIRST);
            let cid = cs.put_sync(
                ContentKind::DictBlob {
                    dict: DictKind::StringForward,
                },
                &pack_from(FIRST),
            );
            let allowance = Arc::clone(cache.dictionary_allowance());
            let pin = allowance
                .resident(xxhash_rust::xxh3::xxh3_128(&cid.to_bytes()))
                .expect("promoted");
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

        /// With the coverage guard, re-missing the same small region rents
        /// the pack's size without promoting it; covering half of it does.
        /// Without the guard the same rental promotes.
        #[test]
        fn the_coverage_guard_waits_for_a_rental_to_cover_the_pack() {
            let len = pack_from(FIRST).len() as u64;
            for coverage in [0.5, 0.0] {
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

        fn policy_cache(bytes: u64, admission: Admission) -> Arc<LeafletCache> {
            Arc::new(LeafletCache::with_max_mb(16).with_admission(
                bytes,
                admission,
                2 * COUNT as u64,
            ))
        }

        fn reason_of(cache: &LeafletCache) -> u8 {
            let report = cache.dictionary_allowance().pack_report();
            assert_eq!(report.len(), 1);
            report[0].reason
        }

        /// A pack whose misses keep landing on the same small region — its
        /// units falling out of the cache — rents its size without covering
        /// it: the coverage guard turns it away for that, whether its rent is
        /// cumulative or recent; demand admission, without the guard, buys it.
        #[test]
        fn a_narrow_hot_pack_is_refused_by_the_coverage_guard() {
            let len = pack_from(FIRST).len() as u64;
            for (admission, bought) in [
                (Admission::Coverage, false),
                (Admission::Recent, false),
                (Admission::RecentCompared, false),
                (Admission::Demand, true),
            ] {
                let cs = SegmentedContentStore::new(true);
                let cache = policy_cache(2 * len, admission);
                let reader = guarded(&cs, &cache, 0.5);
                for _ in 0..12 {
                    read_ids(&reader, FIRST, 0..COUNT as u64 / 10);
                    cache.invalidate_all();
                }
                let stats = cache.dictionary_allowance().stats();
                if bought {
                    assert_eq!(stats.promoted, 1, "{admission:?}");
                    assert_eq!(reason_of(&cache), reject::NONE);
                } else {
                    assert_eq!(stats.promoted, 0, "{admission:?}");
                    assert_eq!(reason_of(&cache), reject::COVERAGE, "{admission:?}");
                }
                read_all(&reader, FIRST);
            }
        }

        /// Under drifting misses — each pack missed a little at a time while
        /// the others' misses run the rent clock — recent rent never reaches
        /// a pack's size, so admission by recent rent buys nothing and leaves
        /// the room unused; cumulative rent buys the first pack it fills.
        #[test]
        fn drifting_misses_buy_nothing_by_recent_rent() {
            let len = pack_from(FIRST).len() as u64;
            let others: Vec<u64> = (1..=8).map(|k| FIRST + 10 * k * COUNT as u64).collect();
            for admission in [
                Admission::Demand,
                Admission::Recent,
                Admission::RecentCompared,
                Admission::Coverage,
            ] {
                let recent = admission != Admission::Coverage;
                let cs = SegmentedContentStore::new(true);
                let cache = policy_cache(len + len / 4, admission);
                let a = adaptive(&cs, FIRST, &cache);
                let rest: Vec<_> = others.iter().map(|&f| adaptive(&cs, f, &cache)).collect();
                for round in 0..40 {
                    read_ids(&a, FIRST, 0..COUNT as u64 / 40);
                    let k = round % 8;
                    read_ids(&rest[k], others[k], 0..COUNT as u64 * 4 / 10);
                    cache.invalidate_all();
                }
                let stats = cache.dictionary_allowance().stats();
                if recent {
                    assert_eq!(
                        (stats.promoted, stats.used),
                        (0, 0),
                        "{admission:?}: room left unused"
                    );
                    assert!(stats.rent_epoch >= 8, "{} rent epochs", stats.rent_epoch);
                    let report = cache.dictionary_allowance().pack_report();
                    assert!(report
                        .iter()
                        .all(|p| p.reason == reject::RENT && p.recent_rent < len));
                } else {
                    assert!(stats.promoted > 0, "cumulative rent fills the room");
                }
            }
        }

        /// Weighing lookups, with the room taken by a pack the workload has
        /// left, a pack now in use is outweighed at first; as the old pack's
        /// lookups age and the new one's grow, the new one replaces it.
        #[test]
        fn weighed_admission_follows_a_switch_once_demand_shifts() {
            for admission in [Admission::Demand, Admission::RecentCompared] {
                weighed_switch(admission);
            }
        }

        fn weighed_switch(admission: Admission) {
            let len = pack_from(FIRST).len() as u64;
            let other = FIRST + 10 * COUNT as u64;
            let cs = SegmentedContentStore::new(true);
            let cache = policy_cache(len + len / 2, admission);
            let a = adaptive(&cs, FIRST, &cache);
            let b = adaptive(&cs, other, &cache);
            for _ in 0..3 {
                read_all(&a, FIRST);
            }
            assert_eq!(cache.dictionary_allowance().stats().promoted, 1);
            read_all(&b, other);
            cache.invalidate_all();
            read_all(&b, other);
            let stats = cache.dictionary_allowance().stats();
            assert_eq!(stats.promoted, 1);
            assert!(stats.outweighed > 0, "a's lookups still outweigh b's");
            for _ in 0..4 {
                cache.invalidate_all();
                read_all(&b, other);
            }
            let stats = cache.dictionary_allowance().stats();
            assert_eq!((stats.promoted, stats.evicted), (2, 1), "b replaces a");
            let reads = cs.reads().len();
            read_all(&b, other);
            assert_eq!(cs.reads().len(), reads, "b served whole");
        }

        /// By recent rent and the clock alone, a pack now in use replaces
        /// one the workload has left as soon as its recent rent reaches its
        /// size: no demand to wait out. (With room for one and a half packs,
        /// rent halves every pack and a half missed, so that takes b two
        /// passes.)
        #[test]
        fn recent_rent_follows_a_switch_at_once() {
            let len = pack_from(FIRST).len() as u64;
            let other = FIRST + 10 * COUNT as u64;
            let cs = SegmentedContentStore::new(true);
            let cache = policy_cache(len + len / 2, Admission::Recent);
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
            assert_eq!(stats.outweighed, 0);
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
            let cache = policy_cache(2 * len, Admission::Recent);
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

        /// Lookups a whole pack serves are counted, and recorded with their
        /// value bytes when it is evicted.
        #[test]
        fn lookups_served_whole_are_recorded_at_eviction() {
            let len = pack_from(FIRST).len() as u64;
            let other = FIRST + 10 * COUNT as u64;
            let cs = SegmentedContentStore::new(true);
            let cache = allowance_cache(len + len / 2);
            let a = adaptive(&cs, FIRST, &cache);
            read_all(&a, FIRST);
            let cid = cs.put_sync(
                ContentKind::DictBlob {
                    dict: DictKind::StringForward,
                },
                &pack_from(FIRST),
            );
            let (before, before_bytes) = cache
                .dictionary_allowance()
                .resident(xxhash_rust::xxh3::xxh3_128(&cid.to_bytes()))
                .expect("promoted")
                .served();
            read_ids(&a, FIRST, 0..100);
            let served: u64 = (0..100).map(|i| value(FIRST + i).len() as u64).sum();
            read_all(&adaptive(&cs, other, &cache), other);
            let stats = cache.dictionary_allowance().stats();
            assert_eq!(stats.evicted, 1);
            assert_eq!(
                (stats.evicted_served_lookups, stats.evicted_served_bytes),
                (before + 100, before_bytes + served)
            );
            assert_eq!(stats.evicted_served.iter().sum::<u64>(), 1);
        }

        /// Packs promoted into anonymous mappings serve the same values, and
        /// return their charge when dropped.
        #[test]
        fn mapped_promotions_serve_the_same_values() {
            let len = pack_from(FIRST).len() as u64;
            let cs = SegmentedContentStore::new(true);
            let cache =
                Arc::new(LeafletCache::with_max_mb(16).with_mapped_dictionary_allowance(len));
            let reader = adaptive(&cs, FIRST, &cache);
            read_all(&reader, FIRST);
            let reads = cs.reads().len();
            read_all(&reader, FIRST);
            assert_eq!(cs.reads().len(), reads, "served whole");
            let cid = cs.put_sync(
                ContentKind::DictBlob {
                    dict: DictKind::StringForward,
                },
                &pack_from(FIRST),
            );
            let pack = cache
                .dictionary_allowance()
                .resident(xxhash_rust::xxh3::xxh3_128(&cid.to_bytes()))
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
            let cache = allowance_cache(len);
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
                (pack.kind, pack.len, pack.resident),
                (KIND_STRING_FWD, len, false)
            );
            assert!(pack.rent >= len && pack.covered <= len && pack.promotions == 0);
        }

        /// Without a ranged source the pack is held whole, as before.
        #[test]
        fn a_pack_without_a_ranged_source_is_held_whole() {
            let cs = SegmentedContentStore::new(false);
            let reader = reader(
                &cs,
                &many_page_pack(),
                Some(Arc::new(LeafletCache::with_max_mb(16))),
            );
            assert_eq!(lookup(&reader, FIRST + 3), Some(value(FIRST + 3)));
            assert_eq!(lookup(&reader, FIRST + 1_500), Some(value(FIRST + 1_500)));
            assert_eq!(cs.whole_reads.load(AtomicOrdering::SeqCst), 1);
        }
    }
}
