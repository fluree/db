//! Range-backed ChunkReader for large Parquet files.
//!
//! This module provides `RangeBackedChunkReader`, a parquet `ChunkReader` implementation
//! that fetches byte ranges on-demand instead of loading the entire file into memory.
//!
//! # Design
//!
//! For files larger than `MAX_SPARSE_BUFFER_SIZE` (64MB), allocating a sparse buffer
//! becomes impractical in memory-constrained environments like AWS Lambda. This reader:
//!
//! 1. Stores the file path and storage reference
//! 2. Fetches byte ranges on-demand when parquet-rs requests them
//! 3. Caches fetched ranges to avoid redundant network calls
//! 4. Uses `tokio::runtime::Handle::block_on` to bridge sync/async
//!
//! # Sync/Async Bridge
//!
//! The parquet `ChunkReader` trait is synchronous, but our storage is async. We use
//! `Handle::block_on` which is safe when called from a blocking context. Callers
//! should use `tokio::task::spawn_blocking` or similar when processing large files.
//!
//! # Cache Strategy
//!
//! The cache is intentionally simple - a vector of (offset, Bytes) pairs. When
//! `get_bytes(start, len)` is called:
//!
//! 1. Check if any cached range fully contains [start, start+len)
//! 2. If found, return a slice of the cached Bytes
//! 3. If not found, fetch via read_range and cache the result
//!
//! For typical Parquet access patterns (column chunks, dictionary pages), this
//! provides good hit rates without complex interval tree logic.

use std::io::{Cursor, Read};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use parquet::file::reader::{ChunkReader, Length};
use tokio::runtime::Handle;

use crate::io::SendIcebergStorage;

/// A cached byte range.
#[derive(Debug, Clone)]
struct CachedRange {
    /// Start offset in the file.
    start: u64,
    /// The fetched bytes.
    data: Bytes,
}

impl CachedRange {
    /// Check if this cached range fully contains the requested range.
    fn contains(&self, start: u64, len: usize) -> bool {
        let end = start + len as u64;
        start >= self.start && end <= self.start + self.data.len() as u64
    }

    /// Extract a slice from this cached range.
    fn slice(&self, start: u64, len: usize) -> Bytes {
        let offset = (start - self.start) as usize;
        self.data.slice(offset..offset + len)
    }
}

/// Range-backed ChunkReader for large Parquet files.
///
/// Instead of loading the entire file into memory, this reader fetches
/// byte ranges on-demand using async storage, bridging to sync via
/// `Handle::block_on`.
///
/// # Example
///
/// ```ignore
/// let reader = RangeBackedChunkReader::new(
///     storage.clone(),
///     "s3://bucket/large-file.parquet",
///     file_size,
///     Handle::current(),
/// );
/// let parquet_reader = SerializedFileReader::new(reader)?;
/// ```
pub struct RangeBackedChunkReader<S: SendIcebergStorage + 'static> {
    /// Storage implementation.
    storage: Arc<S>,
    /// File path.
    path: String,
    /// File size in bytes.
    file_size: u64,
    /// Tokio runtime handle for blocking on async calls.
    runtime: Handle,
    /// Cache of fetched byte ranges.
    cache: Mutex<Vec<CachedRange>>,
    /// Maximum cache size (number of ranges).
    max_cache_entries: usize,
}

impl<S: SendIcebergStorage + 'static> std::fmt::Debug for RangeBackedChunkReader<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangeBackedChunkReader")
            .field("path", &self.path)
            .field("file_size", &self.file_size)
            .field(
                "cache_entries",
                &self.cache.lock().map(|c| c.len()).unwrap_or(0),
            )
            .finish()
    }
}

impl<S: SendIcebergStorage + 'static> RangeBackedChunkReader<S> {
    /// Create a new range-backed chunk reader.
    ///
    /// # Arguments
    ///
    /// * `storage` - Storage implementation (must be Send + Sync)
    /// * `path` - File path in storage
    /// * `file_size` - Total file size in bytes
    /// * `runtime` - Tokio runtime handle for async bridging
    pub fn new(storage: Arc<S>, path: String, file_size: u64, runtime: Handle) -> Self {
        Self {
            storage,
            path,
            file_size,
            runtime,
            cache: Mutex::new(Vec::with_capacity(16)),
            max_cache_entries: 64,
        }
    }

    /// Set the maximum number of cached ranges.
    pub fn with_max_cache_entries(mut self, max: usize) -> Self {
        self.max_cache_entries = max;
        self
    }

    /// Fetch bytes from storage, checking cache first.
    fn fetch_bytes(
        &self,
        start: u64,
        len: usize,
    ) -> std::result::Result<Bytes, parquet::errors::ParquetError> {
        // Check cache first
        {
            let cache = self.cache.lock().map_err(|e| {
                parquet::errors::ParquetError::General(format!("Cache lock poisoned: {e}"))
            })?;

            for cached in cache.iter() {
                if cached.contains(start, len) {
                    tracing::trace!(
                        path = %self.path,
                        start,
                        len,
                        "Cache hit for byte range"
                    );
                    return Ok(cached.slice(start, len));
                }
            }
        }

        // Cache miss - fetch from storage
        //
        // Align fetch to MIN_FETCH_SIZE boundaries to improve cache hit rates.
        // S3 GET-range calls are expensive; fetching slightly more data reduces
        // the number of calls when parquet does many small reads nearby.
        const MIN_FETCH_SIZE: u64 = 256 * 1024; // 256KB minimum fetch
        const ALIGNMENT: u64 = 64 * 1024; // 64KB alignment

        let aligned_start = (start / ALIGNMENT) * ALIGNMENT;
        let end = start + len as u64;
        let aligned_end = ((end + MIN_FETCH_SIZE - 1) / ALIGNMENT * ALIGNMENT).min(self.file_size); // Don't read past EOF

        tracing::debug!(
            path = %self.path,
            requested_start = start,
            requested_len = len,
            aligned_start,
            aligned_end,
            fetch_size = aligned_end - aligned_start,
            "Fetching aligned byte range from storage"
        );

        let range = aligned_start..aligned_end;

        // Block on async read_range
        let storage = Arc::clone(&self.storage);
        let path_for_fetch = self.path.clone();
        let path_for_err = self.path.clone();
        let data = self
            .runtime
            .block_on(async move { storage.read_range(&path_for_fetch, range).await })
            .map_err(|e| {
                parquet::errors::ParquetError::General(format!(
                    "Failed to read range [{aligned_start}, {aligned_end}) from {path_for_err}: {e}"
                ))
            })?;

        // Cache the aligned result
        {
            let mut cache = self.cache.lock().map_err(|e| {
                parquet::errors::ParquetError::General(format!("Cache lock poisoned: {e}"))
            })?;

            // Evict oldest entry if cache is full
            if cache.len() >= self.max_cache_entries {
                cache.remove(0);
            }

            cache.push(CachedRange {
                start: aligned_start,
                data: data.clone(),
            });
        }

        // Return the requested slice from the aligned fetch
        let offset_in_fetch = (start - aligned_start) as usize;
        Ok(data.slice(offset_in_fetch..offset_in_fetch + len))
    }
}

impl<S: SendIcebergStorage + 'static> Length for RangeBackedChunkReader<S> {
    fn len(&self) -> u64 {
        self.file_size
    }
}

impl<S: SendIcebergStorage + 'static> ChunkReader for RangeBackedChunkReader<S> {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> std::result::Result<Self::T, parquet::errors::ParquetError> {
        // For get_read, we need to return a reader starting at `start`.
        // We read to the end of the file (or a reasonable chunk).
        // Parquet typically uses get_bytes for column chunks, so this is less common.
        let len = (self.file_size - start) as usize;

        // Cap at 8MB to avoid huge reads for sequential access
        let len = len.min(8 * 1024 * 1024);

        let bytes = self.fetch_bytes(start, len)?;
        Ok(Cursor::new(bytes))
    }

    fn get_bytes(
        &self,
        start: u64,
        length: usize,
    ) -> std::result::Result<Bytes, parquet::errors::ParquetError> {
        self.fetch_bytes(start, length)
    }
}

/// A reader implementation that wraps Bytes.
/// This provides Read + Seek for parquet's requirements.
#[derive(Debug)]
pub struct BytesReader {
    bytes: Bytes,
    position: usize,
}

impl BytesReader {
    pub fn new(bytes: Bytes) -> Self {
        Self { bytes, position: 0 }
    }
}

impl Read for BytesReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = self.bytes.len() - self.position;
        let to_read = buf.len().min(remaining);

        if to_read == 0 {
            return Ok(0);
        }

        buf[..to_read].copy_from_slice(&self.bytes[self.position..self.position + to_read]);
        self.position += to_read;
        Ok(to_read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use async_trait::async_trait;
    use std::ops::Range;

    /// Mock storage for testing.
    #[derive(Debug, Clone)]
    struct MockStorage {
        data: Bytes,
    }

    #[async_trait]
    impl SendIcebergStorage for MockStorage {
        async fn read(&self, _path: &str) -> Result<Bytes> {
            Ok(self.data.clone())
        }

        async fn read_range(&self, _path: &str, range: Range<u64>) -> Result<Bytes> {
            let start = range.start as usize;
            let end = range.end as usize;
            Ok(self.data.slice(start..end))
        }

        async fn file_size(&self, _path: &str) -> Result<u64> {
            Ok(self.data.len() as u64)
        }
    }

    /// Test get_bytes using spawn_blocking (mirrors real usage pattern).
    ///
    /// The ChunkReader methods are sync and use Handle::block_on internally,
    /// so they must be called from a blocking context (not directly in async).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_chunk_reader_get_bytes() {
        let data = Bytes::from(vec![0u8; 1000]);
        let storage = Arc::new(MockStorage { data: data.clone() });
        let handle = Handle::current();

        // Run sync operations in blocking context (same as read_task_large_file)
        let result = tokio::task::spawn_blocking(move || {
            let reader =
                RangeBackedChunkReader::new(storage, "test.parquet".to_string(), 1000, handle);

            // Test get_bytes
            let bytes = reader.get_bytes(100, 200).unwrap();
            assert_eq!(bytes.len(), 200);

            // Test cache hit (subset of previous fetch)
            let bytes2 = reader.get_bytes(150, 50).unwrap();
            assert_eq!(bytes2.len(), 50);

            "ok"
        })
        .await
        .unwrap();

        assert_eq!(result, "ok");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_chunk_reader_length() {
        let data = Bytes::from(vec![0u8; 12345]);
        let storage = Arc::new(MockStorage { data });
        let handle = Handle::current();

        let result = tokio::task::spawn_blocking(move || {
            let reader =
                RangeBackedChunkReader::new(storage, "test.parquet".to_string(), 12345, handle);

            reader.len()
        })
        .await
        .unwrap();

        assert_eq!(result, 12345);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_chunk_reader_get_read() {
        let data = Bytes::from(b"Hello, World!".to_vec());
        let storage = Arc::new(MockStorage { data });
        let handle = Handle::current();

        let result = tokio::task::spawn_blocking(move || {
            let reader =
                RangeBackedChunkReader::new(storage, "test.parquet".to_string(), 13, handle);

            let mut cursor = reader.get_read(7).unwrap();
            let mut buf = String::new();
            cursor.read_to_string(&mut buf).unwrap();
            buf
        })
        .await
        .unwrap();

        assert_eq!(result, "World!");
    }
}
