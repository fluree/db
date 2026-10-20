//! Objects on this machine read by range, for stores whose whole-object
//! local read is costly.

use std::fmt;
use std::ops::Range;

use crate::error::Result;

/// An object on this machine whose bytes are read by range.
///
/// For a store whose [`get_local`](super::ContentStore::get_local) does work
/// in proportion to the object — decrypting all of it — so a reader that
/// needs a few ranges of a large object pays for those ranges only.
///
/// A source reads the object as it was when the source was opened: a store
/// that later replaces the object at the same address (re-encrypting it
/// under a new key) does not change what an open source reads.
pub trait RangeSource: fmt::Debug + Send + Sync {
    /// The object's length in bytes.
    fn len(&self) -> u64;

    /// Whether the object is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The bytes of `range`, clamped to the object like
    /// [`StorageRead::read_byte_range`](super::StorageRead::read_byte_range):
    /// an empty range, or one starting at or past the end, reads nothing.
    /// Every byte returned has been authenticated; a failure to authenticate
    /// is an error, never a short read.
    fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>>;

    /// The granularity reads cost in: a range touching any byte of an
    /// aligned unit of this size pays for the whole unit. Readers merge
    /// requests that fall within one unit of each other.
    fn read_unit(&self) -> u64;

    /// Bytes this source keeps alive, as a cache budget should count them.
    fn cache_weight(&self) -> usize;

    /// Identifies what this source reads, for a cache of what it returns.
    /// Two sources share an identity only when they read the same stored
    /// bytes for the same reader: an object written again at its address
    /// (re-encrypted) gets a new one, and so does a reader holding other
    /// keys, so neither is served what was cached for another.
    fn cache_identity(&self) -> u128;
}
