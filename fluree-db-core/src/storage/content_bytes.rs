//! Read-only bytes handed out by a [`ContentStore`](super::ContentStore).

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

/// Read-only bytes of one stored object.
///
/// Every variant is reference-counted, so a clone is a refcount bump and a
/// reader's cache can hold one directly.
#[derive(Clone)]
pub enum ContentBytes {
    /// Heap bytes from a read, a fetch or a decrypt. An `Arc<Vec<u8>>`, so
    /// wrapping a freshly read buffer does not copy it.
    Owned(Arc<Vec<u8>>),
    /// Bytes a store already holds shared, such as memory storage or a
    /// residency tier.
    Shared(Arc<[u8]>),
    /// A file mapping: pages fault in when touched, and the memory is the
    /// OS page cache rather than the heap.
    #[cfg(not(target_arch = "wasm32"))]
    Mapped(Arc<memmap2::Mmap>),
}

impl ContentBytes {
    /// Bytes this handle charges against a cache budget.
    ///
    /// Heap bytes weigh their length. A mapping is reclaimable page cache, so
    /// it weighs far less than its length, but stays length-proportional so
    /// the mapped address space a budget admits stays bounded (× 64).
    pub fn cache_weight(&self) -> usize {
        match self {
            ContentBytes::Owned(bytes) => bytes.len(),
            ContentBytes::Shared(bytes) => bytes.len(),
            #[cfg(not(target_arch = "wasm32"))]
            ContentBytes::Mapped(mmap) => (mmap.len() / 64).max(16 * 1024),
        }
    }

    /// The bytes as an owned buffer: moved out when this is the only handle
    /// to heap bytes, copied otherwise.
    pub fn into_vec(self) -> Vec<u8> {
        match self {
            ContentBytes::Owned(bytes) => Arc::try_unwrap(bytes).unwrap_or_else(|b| (*b).clone()),
            ContentBytes::Shared(bytes) => bytes.to_vec(),
            #[cfg(not(target_arch = "wasm32"))]
            ContentBytes::Mapped(mmap) => mmap.to_vec(),
        }
    }

    /// The bytes as a shared slice: moved out when already shared, copied
    /// otherwise.
    pub fn into_shared(self) -> Arc<[u8]> {
        match self {
            ContentBytes::Shared(bytes) => bytes,
            other => Arc::from(&other[..]),
        }
    }

    /// Whether two handles share one backing allocation or mapping.
    pub fn ptr_eq(&self, other: &ContentBytes) -> bool {
        std::ptr::eq(self.as_ref().as_ptr(), other.as_ref().as_ptr()) && self.len() == other.len()
    }
}

/// Blobs at or below this many bytes are `read()` into the heap by
/// [`ContentBytes::from_file`] instead of being mapped. Override with
/// `FLUREE_MMAP_MIN_BYTES`; 0 maps every blob.
///
/// **A mapping is a scarcer resource than the bytes it exposes.** Every mmap
/// costs a VMA, and a process is hard-capped at `vm.max_map_count` (65,530 by
/// default) *regardless of how much memory is free* — past it `mmap` returns
/// ENOMEM, which surfaces as "failed to load binary index: Cannot allocate
/// memory (os error 12)" on a host with gigabytes idle. That is not a
/// theoretical limit: dict packs are per-ID-range, so a ledger's routing table
/// grows with it. Measured on one deployment, 23 ledgers held **103,426
/// packs** and the process carried **47,336 mappings** against the 65,530 cap
/// — every ledger load pushing it closer, and raising the container's memory
/// limit doing nothing at all because bytes were never the constraint.
///
/// The size split works because blob sizes are extremely skewed: on that same
/// deployment **91% of packs were under 4 KiB and 99.8% of the mapped ones
/// were under 64 KiB, holding 30 MB between them.** Mapping a 113-byte file
/// (the median!) spends a VMA and a whole page of address space to expose less
/// than a cache line's worth of useful data. So this trades ~30 MB of heap for
/// ~47,000 mappings, and the large blobs that actually justify demand paging —
/// 5,728 files holding 12.1 of the 12.5 GiB — still get mapped.
#[cfg(not(target_arch = "wasm32"))]
pub const DEFAULT_MMAP_MIN_BYTES: u64 = 64 * 1024;

#[cfg(not(target_arch = "wasm32"))]
fn mmap_min_bytes() -> u64 {
    static CACHED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("FLUREE_MMAP_MIN_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_MMAP_MIN_BYTES)
    })
}

#[cfg(not(target_arch = "wasm32"))]
impl ContentBytes {
    /// The bytes of an open file of `len` bytes: read into the heap at or
    /// below [`DEFAULT_MMAP_MIN_BYTES`], mapped above it.
    ///
    /// Reading goes through the handle already open, so a GC unlink between
    /// two path resolutions cannot turn a live blob into `NotFound`.
    ///
    /// # Safety
    ///
    /// The file must not be written to or truncated while the returned bytes,
    /// or any clone of them, are alive: a mapping would observe the change,
    /// and reading past a truncation faults. Unlinking it, or replacing it by
    /// renaming another file over its path, is fine.
    pub unsafe fn from_file(file: std::fs::File, len: u64) -> std::io::Result<Self> {
        if len <= mmap_min_bytes() {
            use std::io::Read;
            let mut bytes = Vec::with_capacity(len as usize);
            let mut file = file;
            file.read_to_end(&mut bytes)?;
            return Ok(bytes.into());
        }
        // SAFETY: upheld by the caller.
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        Ok(ContentBytes::Mapped(Arc::new(mmap)))
    }

    /// [`Self::from_file`] by path: `Ok(None)` when nothing is there.
    ///
    /// # Safety
    ///
    /// As for [`Self::from_file`], for the file at `path`.
    pub unsafe fn open(path: &std::path::Path) -> std::io::Result<Option<Self>> {
        let file = match std::fs::File::open(path) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err),
        };
        let len = file.metadata()?.len();
        // SAFETY: upheld by the caller.
        unsafe { Self::from_file(file, len) }.map(Some)
    }
}

impl Deref for ContentBytes {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        match self {
            ContentBytes::Owned(bytes) => bytes,
            ContentBytes::Shared(bytes) => bytes,
            #[cfg(not(target_arch = "wasm32"))]
            ContentBytes::Mapped(mmap) => mmap,
        }
    }
}

impl AsRef<[u8]> for ContentBytes {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl std::borrow::Borrow<[u8]> for ContentBytes {
    fn borrow(&self) -> &[u8] {
        self
    }
}

impl From<Vec<u8>> for ContentBytes {
    fn from(bytes: Vec<u8>) -> Self {
        ContentBytes::Owned(Arc::new(bytes))
    }
}

impl From<Arc<[u8]>> for ContentBytes {
    fn from(bytes: Arc<[u8]>) -> Self {
        ContentBytes::Shared(bytes)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<Arc<memmap2::Mmap>> for ContentBytes {
    fn from(mmap: Arc<memmap2::Mmap>) -> Self {
        ContentBytes::Mapped(mmap)
    }
}

impl From<ContentBytes> for Vec<u8> {
    fn from(bytes: ContentBytes) -> Self {
        bytes.into_vec()
    }
}

impl<T: AsRef<[u8]> + ?Sized> PartialEq<T> for ContentBytes {
    fn eq(&self, other: &T) -> bool {
        **self == *other.as_ref()
    }
}

impl Eq for ContentBytes {}

impl fmt::Debug for ContentBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            ContentBytes::Owned(_) => "Owned",
            ContentBytes::Shared(_) => "Shared",
            #[cfg(not(target_arch = "wasm32"))]
            ContentBytes::Mapped(_) => "Mapped",
        };
        f.debug_struct("ContentBytes")
            .field("kind", &kind)
            .field("len", &self.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_moves_out_when_unique_and_copies_when_shared() {
        let bytes = vec![1u8, 2, 3];
        let ptr = bytes.as_ptr();
        let unique = ContentBytes::from(bytes);
        let moved = unique.into_vec();
        assert_eq!(moved.as_ptr(), ptr, "unique owned bytes move");

        let shared = ContentBytes::from(vec![4u8, 5]);
        let other = shared.clone();
        assert!(shared.ptr_eq(&other), "a clone shares the allocation");
        assert_eq!(shared.into_vec(), vec![4u8, 5]);
        assert_eq!(other, [4u8, 5]);
    }

    #[test]
    fn heap_bytes_weigh_their_length_and_mappings_are_discounted() {
        assert_eq!(ContentBytes::from(vec![0u8; 1000]).cache_weight(), 1000);
        let arc: Arc<[u8]> = Arc::from(vec![0u8; 500]);
        assert_eq!(ContentBytes::from(arc).cache_weight(), 500);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blob");
        std::fs::write(&path, vec![7u8; 4 << 20]).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        // SAFETY: the test owns the file and never modifies it.
        let mmap = Arc::new(unsafe { memmap2::Mmap::map(&file).unwrap() });
        assert_eq!(ContentBytes::from(mmap).cache_weight(), (4 << 20) / 64);
    }

    fn open(path: &std::path::Path) -> Option<ContentBytes> {
        // SAFETY: the tests never modify a file after writing it.
        unsafe { ContentBytes::open(path) }.unwrap()
    }

    /// A small file must be read onto the heap, not mapped — the whole point
    /// of [`DEFAULT_MMAP_MIN_BYTES`]. Asserting on the backing VARIANT rather
    /// than on reads is deliberate: reads pass either way, which is exactly
    /// why the mapping leak went unnoticed for months. This is the only
    /// assertion that can fail if someone reverts to always-mmap.
    #[test]
    fn small_files_are_read_not_mapped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("small");
        let bytes = vec![3u8; 113];
        std::fs::write(&path, &bytes).unwrap();

        let read = open(&path).unwrap();
        assert!(
            matches!(read, ContentBytes::Owned(_)),
            "a {}-byte file must not consume a VMA",
            bytes.len()
        );
        // The bytes must survive the trip, or we have traded a mapping for a bug.
        assert_eq!(read, bytes);
    }

    /// Above the threshold we still map: large blobs are where demand paging
    /// actually pays, and this pins that the split is a split and not a
    /// wholesale move to heap reads (which would pull GiB-sized packs into RAM).
    #[test]
    fn large_files_are_still_mapped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large");
        std::fs::write(&path, vec![0u8; (DEFAULT_MMAP_MIN_BYTES + 1) as usize]).unwrap();

        let mapped = open(&path).unwrap();
        assert!(matches!(mapped, ContentBytes::Mapped(_)), "{mapped:?}");
        assert_eq!(mapped.len(), (DEFAULT_MMAP_MIN_BYTES + 1) as usize);
    }

    /// The boundary is inclusive (`<=`), so a file exactly at the threshold
    /// is read. Pinned because an off-by-one here silently changes which side
    /// of the split the most common pack size lands on.
    #[test]
    fn threshold_boundary_is_inclusive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("edge");
        std::fs::write(&path, vec![0u8; DEFAULT_MMAP_MIN_BYTES as usize]).unwrap();

        assert!(matches!(open(&path).unwrap(), ContentBytes::Owned(_)));
        assert!(open(&dir.path().join("absent")).is_none());
    }
}
