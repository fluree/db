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
}
