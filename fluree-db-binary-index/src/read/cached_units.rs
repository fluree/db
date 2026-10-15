//! A ranged local source read through the leaflet cache one read unit at a
//! time.
//!
//! The units of an encrypted source are its authenticated, decrypted
//! segments. Keeping them in the leaflet cache means a reader that comes
//! back to the same bytes — a scan that loads columns without the decoded
//! cache, a dictionary lookup — copies them instead of decrypting them
//! again, while what is kept stays inside the cache's one budget and is
//! evicted with everything else.

use std::cell::RefCell;
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use fluree_db_core::RangeSource;

use super::leaflet_cache::{LeafletCache, SourceUnitKey};

/// Experiment control for the segmented-envelope A/B, not a supported
/// configuration: `FLUREE_ENCRYPTED_SEGMENT_CACHE=0` reads ranged sources
/// without keeping their units.
pub(crate) fn enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("FLUREE_ENCRYPTED_SEGMENT_CACHE").map_or(true, |v| v != "0"))
}

/// Units one open [`UnitMemoScope`] keeps on its thread.
const MEMO_UNITS: usize = 8;

thread_local! {
    static MEMO_HITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static MEMO: RefCell<Memo> = const {
        RefCell::new(Memo {
            depth: 0,
            slots: Vec::new(),
            next: 0,
        })
    };
}

/// The units served on this thread while a scope is open, most recent
/// overwriting the oldest.
struct Memo {
    depth: u32,
    slots: Vec<(SourceUnitKey, Arc<[u8]>)>,
    next: usize,
}

/// While open, the units this thread reads through [`CachedUnits`] are kept
/// beside the shared cache, so a run of lookups landing in the same few
/// units — formatting a batch of rows, whose ids mostly ascend — takes them
/// from here rather than asking the cache each time. Each unit is still
/// read through the cache the first time the scope meets it.
///
/// At most `MEMO_UNITS` units are held per open scope, and they are
/// released when the outermost scope on the thread closes. Not `Send`, so a
/// scope cannot be held across an await that may resume on another thread.
pub struct UnitMemoScope {
    _thread_bound: PhantomData<*const ()>,
}

impl UnitMemoScope {
    pub fn enter() -> Self {
        MEMO.with(|m| m.borrow_mut().depth += 1);
        Self {
            _thread_bound: PhantomData,
        }
    }
}

impl Drop for UnitMemoScope {
    fn drop(&mut self) {
        MEMO.with(|m| {
            let mut m = m.borrow_mut();
            m.depth -= 1;
            if m.depth == 0 {
                m.slots.clear();
                m.next = 0;
            }
        });
    }
}

/// Units this thread has taken from a scope's memo, for diagnostics.
pub fn unit_memo_hits() -> u64 {
    MEMO_HITS.with(std::cell::Cell::get)
}

fn memo_get(key: &SourceUnitKey) -> Option<Arc<[u8]>> {
    let hit = MEMO.with(|m| {
        let m = m.borrow();
        if m.depth == 0 {
            return None;
        }
        m.slots
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, unit)| Arc::clone(unit))
    });
    if hit.is_some() {
        MEMO_HITS.with(|h| h.set(h.get() + 1));
    }
    hit
}

fn memo_put(key: SourceUnitKey, unit: &Arc<[u8]>) {
    MEMO.with(|m| {
        let mut m = m.borrow_mut();
        if m.depth == 0 {
            return;
        }
        let entry = (key, Arc::clone(unit));
        if m.slots.len() < MEMO_UNITS {
            m.slots.push(entry);
        } else {
            let at = m.next;
            m.slots[at] = entry;
            m.next = (at + 1) % MEMO_UNITS;
        }
    });
}

/// What reading a source cost on cache misses since the meter was last
/// reset: bytes read from the source, and the distinct units they covered.
/// Units served from the cache or a scope's memo are not counted.
pub struct MissMeter {
    bytes: AtomicU64,
    units: Box<[AtomicU64]>,
    /// Times the metered source has been promoted whole.
    promotions: AtomicU64,
}

impl MissMeter {
    pub fn new(units: u64) -> Self {
        Self {
            bytes: AtomicU64::new(0),
            units: (0..units.div_ceil(64)).map(|_| AtomicU64::new(0)).collect(),
            promotions: AtomicU64::new(0),
        }
    }

    fn record(&self, unit: u64, bytes: usize) {
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        if let Some(word) = self.units.get((unit / 64) as usize) {
            word.fetch_or(1 << (unit % 64), Ordering::Relaxed);
        }
    }

    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    pub fn distinct_units(&self) -> u64 {
        self.units
            .iter()
            .map(|w| u64::from(w.load(Ordering::Relaxed).count_ones()))
            .sum()
    }

    /// Count a promotion of the source; returns how many came before it.
    pub fn note_promotion(&self) -> u64 {
        self.promotions.fetch_add(1, Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.bytes.store(0, Ordering::Relaxed);
        for word in &*self.units {
            word.store(0, Ordering::Relaxed);
        }
    }
}

/// `source` with its read units kept in `cache`, keyed by the source's
/// identity, so only readers of the same storage and the same envelope
/// share them.
pub struct CachedUnits {
    source: Arc<dyn RangeSource>,
    cache: Arc<LeafletCache>,
    // The source's constants, read once rather than per unit.
    identity: u128,
    unit_size: u64,
    len: u64,
    meter: Option<Arc<MissMeter>>,
}

impl CachedUnits {
    pub fn new(source: Arc<dyn RangeSource>, cache: Arc<LeafletCache>) -> Self {
        Self {
            identity: source.cache_identity(),
            unit_size: source.read_unit(),
            len: source.len(),
            source,
            cache,
            meter: None,
        }
    }

    /// Count what this reader's cache misses read from the source.
    pub fn with_meter(mut self, meter: Arc<MissMeter>) -> Self {
        self.meter = Some(meter);
        self
    }

    pub fn identity(&self) -> u128 {
        self.identity
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The source's units: at least one, since even an empty source has
    /// an end to authenticate.
    pub fn unit_count(&self) -> u64 {
        self.len.div_ceil(self.unit_size).max(1)
    }

    /// The whole source: cached units copied, the rest read from the source
    /// without caching them. Returns the bytes and how many were read.
    pub fn assemble(&self) -> io::Result<(Vec<u8>, u64)> {
        let mut out = Vec::with_capacity(self.len as usize);
        let mut read = 0u64;
        for unit in 0..self.unit_count() {
            let key = SourceUnitKey {
                source: self.identity,
                unit,
            };
            let start = unit * self.unit_size;
            let expected = self.unit_size.min(self.len.saturating_sub(start)) as usize;
            match self.cache.peek_source_unit(key) {
                Some(bytes) if bytes.len() == expected => out.extend_from_slice(&bytes),
                _ => {
                    let bytes = self
                        .source
                        .read_range(start..start + self.unit_size)
                        .map_err(|e| io::Error::other(e.to_string()))?;
                    if bytes.len() != expected {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "read unit shorter than the source declares",
                        ));
                    }
                    read += bytes.len() as u64;
                    out.extend_from_slice(&bytes);
                }
            }
        }
        Ok((out, read))
    }

    /// Drop this source's units from the cache.
    pub fn forget_cached(&self) {
        for unit in 0..self.unit_count() {
            self.cache.invalidate_source_unit(SourceUnitKey {
                source: self.identity,
                unit,
            });
        }
    }

    /// Unit `index`'s bytes, from the scope's memo, the cache, or read once
    /// from the source.
    fn unit(&self, index: u64) -> io::Result<Arc<[u8]>> {
        let key = SourceUnitKey {
            source: self.identity,
            unit: index,
        };
        if let Some(unit) = memo_get(&key) {
            return Ok(unit);
        }
        let unit = self.load_unit(key)?;
        memo_put(key, &unit);
        Ok(unit)
    }

    fn load_unit(&self, key: SourceUnitKey) -> io::Result<Arc<[u8]>> {
        let size = self.unit_size;
        let start = key.unit * size;
        let expected = size.min(self.len.saturating_sub(start)) as usize;
        self.cache.try_get_or_load_source_unit(key, || {
            let bytes = self
                .source
                .read_range(start..start + size)
                .map_err(|e| io::Error::other(e.to_string()))?;
            if bytes.len() != expected {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "read unit shorter than the source declares",
                ));
            }
            fluree_db_core::io_stats::record_work("source unit cached", bytes.len());
            if let Some(meter) = &self.meter {
                meter.record(key.unit, bytes.len());
            }
            Ok(Arc::from(bytes))
        })
    }

    /// Append the bytes of `range`, clamped to the source, to `out`. A
    /// range starting at or past the end appends nothing, after the source
    /// has authenticated its end.
    pub fn extend_into(&self, range: Range<u64>, out: &mut Vec<u8>) -> io::Result<()> {
        if range.start >= range.end {
            return Ok(());
        }
        let len = self.len;
        let (start, end) = (range.start.min(len), range.end.min(len));
        if start >= end {
            // Nothing to copy, but the source vouches for where its end is.
            self.source
                .read_range(range)
                .map_err(|e| io::Error::other(e.to_string()))?;
            return Ok(());
        }
        let size = self.unit_size;
        out.reserve((end - start) as usize);
        for index in start / size..=(end - 1) / size {
            let unit = self.unit(index)?;
            let base = index * size;
            let from = (start.max(base) - base) as usize;
            let to = (end.min(base + size) - base) as usize;
            out.extend_from_slice(&unit[from..to]);
        }
        Ok(())
    }
}

impl fmt::Debug for CachedUnits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedUnits")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl RangeSource for CachedUnits {
    fn len(&self) -> u64 {
        self.source.len()
    }

    fn read_range(&self, range: Range<u64>) -> fluree_db_core::Result<Vec<u8>> {
        let mut out = Vec::new();
        self.extend_into(range, &mut out)
            .map_err(|e| fluree_db_core::Error::io(e.to_string()))?;
        Ok(out)
    }

    fn read_unit(&self) -> u64 {
        self.source.read_unit()
    }

    /// The cached units are charged as entries of their own.
    fn cache_weight(&self) -> usize {
        self.source.cache_weight()
    }

    fn cache_identity(&self) -> u128 {
        self.source.cache_identity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    #[derive(Debug)]
    struct Source {
        bytes: Vec<u8>,
        identity: u128,
        reads: Mutex<Vec<Range<u64>>>,
        fail: AtomicBool,
    }

    impl Source {
        fn new(len: usize, identity: u128) -> Arc<Self> {
            Arc::new(Self {
                bytes: (0..len).map(|i| (i % 251) as u8).collect(),
                identity,
                reads: Mutex::new(Vec::new()),
                fail: AtomicBool::new(false),
            })
        }

        fn reads(&self) -> usize {
            self.reads.lock().unwrap().len()
        }
    }

    impl RangeSource for Source {
        fn len(&self) -> u64 {
            self.bytes.len() as u64
        }

        fn read_range(&self, range: Range<u64>) -> fluree_db_core::Result<Vec<u8>> {
            self.reads.lock().unwrap().push(range.clone());
            if self.fail.load(Ordering::SeqCst) {
                return Err(fluree_db_core::Error::storage("authentication failed"));
            }
            let len = self.bytes.len() as u64;
            let (start, end) = (
                range.start.min(len),
                range.end.min(len).max(range.start.min(len)),
            );
            Ok(self.bytes[start as usize..end as usize].to_vec())
        }

        fn read_unit(&self) -> u64 {
            1000
        }

        fn cache_weight(&self) -> usize {
            0
        }

        fn cache_identity(&self) -> u128 {
            self.identity
        }
    }

    fn cached(source: &Arc<Source>, cache: &Arc<LeafletCache>) -> CachedUnits {
        CachedUnits::new(
            Arc::clone(source) as Arc<dyn RangeSource>,
            Arc::clone(cache),
        )
    }

    #[test]
    fn ranges_read_across_units_match_the_source() {
        let source = Source::new(4_500, 1);
        let units = cached(&source, &Arc::new(LeafletCache::with_max_mb(4)));
        for range in [
            0..1,
            0..1000,
            999..1001,
            1500..3999,
            4000..4500,
            4499..9999,
            10..u64::MAX,
        ] {
            let end = range.end.min(4_500) as usize;
            assert_eq!(
                units.read_range(range.clone()).unwrap(),
                source.bytes[range.start as usize..end],
                "{range:?}"
            );
        }
        assert!(units.read_range(5_000..5_010).unwrap().is_empty());
        assert!(units.read_range(7..7).unwrap().is_empty());
    }

    /// A unit read once is served from the cache after, whatever range
    /// asks for it; only whole units are read from the source.
    #[test]
    fn each_unit_is_read_once() {
        let source = Source::new(4_500, 1);
        let units = cached(&source, &Arc::new(LeafletCache::with_max_mb(4)));
        units.read_range(100..2_100).unwrap();
        assert_eq!(source.reads(), 3);
        units.read_range(0..3_000).unwrap();
        units.read_range(2_500..2_501).unwrap();
        assert_eq!(source.reads(), 3, "units 0-2 are cached");
        units.read_range(4_100..4_200).unwrap();
        assert_eq!(source.reads(), 4);
        assert!(source
            .reads
            .lock()
            .unwrap()
            .iter()
            .all(|r| r.start % 1000 == 0));
    }

    /// Units are keyed by source identity: another storage, or the same
    /// object encrypted again, reads its own.
    #[test]
    fn units_are_not_shared_across_identities() {
        let cache = Arc::new(LeafletCache::with_max_mb(4));
        let a = Source::new(2_000, 1);
        let b = Source::new(2_000, 2);
        cached(&a, &cache).read_range(0..10).unwrap();
        cached(&b, &cache).read_range(0..10).unwrap();
        assert_eq!((a.reads(), b.reads()), (1, 1));
        cached(&a, &cache).read_range(0..10).unwrap();
        assert_eq!(a.reads(), 1, "the same identity shares");
    }

    /// A unit that fails to authenticate is an error each time, never a
    /// cached success.
    #[test]
    fn a_failed_unit_is_not_cached() {
        let source = Source::new(2_000, 1);
        let units = cached(&source, &Arc::new(LeafletCache::with_max_mb(4)));
        source.fail.store(true, Ordering::SeqCst);
        assert!(units.read_range(0..10).is_err());
        assert!(units.read_range(0..10).is_err());
        assert_eq!(source.reads(), 2);
        source.fail.store(false, Ordering::SeqCst);
        assert_eq!(units.read_range(0..3).unwrap(), [0, 1, 2]);
    }

    /// Inside a scope, units served once come from the thread's memo, even
    /// after the shared cache drops them; when the outermost scope closes
    /// the memo is released and reads go back to the cache and the source.
    #[test]
    fn a_scope_keeps_its_units_until_the_outermost_scope_closes() {
        let cache = Arc::new(LeafletCache::with_max_mb(4));
        let source = Source::new(4_000, 1);
        let units = cached(&source, &cache);
        {
            let _outer = UnitMemoScope::enter();
            units.read_range(10..20).unwrap();
            {
                let _inner = UnitMemoScope::enter();
                units.read_range(1_010..1_020).unwrap();
            }
            cache.invalidate_all();
            assert_eq!(units.read_range(15..25).unwrap(), source.bytes[15..25]);
            assert_eq!(
                units.read_range(1_500..1_501).unwrap(),
                source.bytes[1_500..1_501]
            );
            assert_eq!(source.reads(), 2, "served from the memo");
        }
        cache.invalidate_all();
        units.read_range(15..25).unwrap();
        assert_eq!(source.reads(), 3, "released with the scope");
        cache.invalidate_all();
        let _next = UnitMemoScope::enter();
        units.read_range(1_010..1_020).unwrap();
        assert_eq!(source.reads(), 4, "a later scope starts empty");
    }

    /// Without a scope nothing is memoized; a scope holds a bounded number
    /// of units, oldest replaced first, and never mixes identities.
    #[test]
    fn the_memo_is_bounded_and_scoped() {
        let cache = Arc::new(LeafletCache::with_max_mb(4));
        let source = Source::new(20_000, 1);
        let units = cached(&source, &cache);
        units.read_range(0..1).unwrap();
        cache.invalidate_all();
        units.read_range(0..1).unwrap();
        assert_eq!(source.reads(), 2, "no scope, no memo");

        let _scope = UnitMemoScope::enter();
        for unit in 0..=MEMO_UNITS as u64 {
            units.read_range(unit * 1000..unit * 1000 + 1).unwrap();
        }
        cache.invalidate_all();
        let reads = source.reads();
        units
            .read_range(MEMO_UNITS as u64 * 1000..MEMO_UNITS as u64 * 1000 + 1)
            .unwrap();
        assert_eq!(source.reads(), reads, "the newest unit is held");
        units.read_range(0..1).unwrap();
        assert_eq!(source.reads(), reads + 1, "the oldest was replaced");

        let other = Source::new(20_000, 2);
        cached(&other, &cache).read_range(3000..3001).unwrap();
        assert_eq!(
            other.reads(),
            1,
            "another identity is not served from the memo"
        );
    }

    /// The meter counts what cache misses read, once per miss: not cache
    /// hits, not memo hits, and a unit read again after eviction again.
    #[test]
    fn the_meter_counts_cache_misses_only() {
        let cache = Arc::new(LeafletCache::with_max_mb(4));
        let source = Source::new(4_500, 1);
        let meter = Arc::new(MissMeter::new(5));
        let units = cached(&source, &cache).with_meter(Arc::clone(&meter));
        units.read_range(0..10).unwrap();
        units.read_range(5..2_500).unwrap();
        assert_eq!((meter.bytes(), meter.distinct_units()), (3_000, 3));
        {
            let _scope = UnitMemoScope::enter();
            units.read_range(4_100..4_200).unwrap();
            cache.invalidate_all();
            units.read_range(4_100..4_200).unwrap();
        }
        assert_eq!(
            (meter.bytes(), meter.distinct_units()),
            (3_500, 4),
            "memo hit"
        );
        units.read_range(0..10).unwrap();
        assert_eq!(
            (meter.bytes(), meter.distinct_units()),
            (4_500, 4),
            "re-read"
        );
        meter.reset();
        assert_eq!((meter.bytes(), meter.distinct_units()), (0, 0));
    }

    /// Assembly copies the units the cache holds and reads only the rest;
    /// forgetting drops every unit from the cache.
    #[test]
    fn assembly_reuses_cached_units() {
        let cache = Arc::new(LeafletCache::with_max_mb(4));
        let source = Source::new(4_500, 1);
        let units = cached(&source, &cache);
        units.read_range(1_000..3_000).unwrap();
        let reads = source.reads();
        let (bytes, read) = units.assemble().unwrap();
        assert_eq!(bytes, source.bytes);
        assert_eq!(read, 2_500, "units 0, 3 and 4");
        assert_eq!(source.reads(), reads + 3);
        units.forget_cached();
        let (_, read) = units.assemble().unwrap();
        assert_eq!(read, 4_500, "nothing left cached");
        source.fail.store(true, Ordering::SeqCst);
        assert!(
            units.assemble().is_err(),
            "a unit failing to authenticate fails it"
        );
    }

    /// An end-of-object answer comes from the source, which authenticates
    /// it, rather than from the declared length alone.
    #[test]
    fn past_the_end_asks_the_source() {
        let source = Source::new(2_000, 1);
        let units = cached(&source, &Arc::new(LeafletCache::with_max_mb(4)));
        source.fail.store(true, Ordering::SeqCst);
        assert!(units.read_range(3_000..3_010).is_err());
    }
}
