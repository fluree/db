//! A ranged local source read through the leaflet cache one read unit at a
//! time.
//!
//! The units of an encrypted source are its authenticated, decrypted
//! segments. Keeping them in the leaflet cache means a reader that comes
//! back to the same bytes — a scan that loads columns without the decoded
//! cache, a dictionary lookup — copies them instead of decrypting them
//! again, while what is kept stays inside the cache's one budget and is
//! evicted with everything else.

use std::fmt;
use std::io;
use std::ops::Range;
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

/// `source` with its read units kept in `cache`, keyed by the source's
/// identity, so only readers of the same storage and the same envelope
/// share them.
pub struct CachedUnits {
    source: Arc<dyn RangeSource>,
    cache: Arc<LeafletCache>,
}

impl CachedUnits {
    pub fn new(source: Arc<dyn RangeSource>, cache: Arc<LeafletCache>) -> Self {
        Self { source, cache }
    }

    /// Unit `index`'s bytes, from the cache or read once from the source.
    fn unit(&self, index: u64) -> io::Result<Arc<[u8]>> {
        let size = self.source.read_unit();
        let start = index * size;
        let expected = size.min(self.source.len().saturating_sub(start)) as usize;
        self.cache.try_get_or_load_source_unit(
            SourceUnitKey {
                source: self.source.cache_identity(),
                unit: index,
            },
            || {
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
                Ok(Arc::from(bytes))
            },
        )
    }

    /// Append the bytes of `range`, clamped to the source, to `out`. A
    /// range starting at or past the end appends nothing, after the source
    /// has authenticated its end.
    pub fn extend_into(&self, range: Range<u64>, out: &mut Vec<u8>) -> io::Result<()> {
        if range.start >= range.end {
            return Ok(());
        }
        let len = self.source.len();
        let (start, end) = (range.start.min(len), range.end.min(len));
        if start >= end {
            // Nothing to copy, but the source vouches for where its end is.
            self.source
                .read_range(range)
                .map_err(|e| io::Error::other(e.to_string()))?;
            return Ok(());
        }
        let size = self.source.read_unit();
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
