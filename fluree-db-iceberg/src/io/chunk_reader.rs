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
//! The parquet `ChunkReader` trait is synchronous, but our storage is async. The
//! bridge (`block_on_storage_fetch`) is flavor-aware: on a multi-thread runtime
//! it blocks via `block_in_place(handle.block_on(..))` so the calling worker is
//! released and a replacement keeps driving the IO reactor (correct on both
//! worker and `spawn_blocking` threads); on a current-thread runtime it drives
//! the future on a self-contained helper runtime in a separate thread to avoid
//! panicking or deadlocking the single thread; with no ambient runtime it blocks
//! on the handle captured at construction. Callers should still prefer
//! `tokio::task::spawn_blocking` for large files so the whole decode does not
//! occupy a worker for its full duration.
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
//!
//! `get_read(start)` is how parquet's page reader reads each page header (the
//! page body then comes through `get_bytes`). It is served from the newest
//! cached window that covers `start` with at least [`GET_READ_MIN_TAIL`] bytes
//! to spare, or that runs to the end of the file; otherwise it fetches a fresh
//! [`GET_READ_WINDOW`] from `start`. So a column chunk read page by page costs
//! about one fetch per window, not one per page. The reader it returns reads
//! up to [`GET_READ_WINDOW`] from `start`, as the old 8 MiB cursor did,
//! fetching past a cached window's end only if read that far.

use std::io::{Cursor, Read};
use std::ops::Range;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use parquet::errors::ParquetError;
use parquet::file::reader::{ChunkReader, Length};
use tokio::runtime::{Handle, RuntimeFlavor};

use crate::io::SendIcebergStorage;

/// How much `get_read` fetches from its offset when no cached window serves it.
/// Parquet reads one page header from the start of it, then that page's body
/// and the pages after it from the same window.
const GET_READ_WINDOW: u64 = 8 * 1024 * 1024;

/// The least a cached window must hold past a `get_read` offset to serve it,
/// unless it runs to the end of the file. A page header needs a few bytes; the
/// rest is room for the page bodies that follow, so a window this close to its
/// end is passed over for a fresh one rather than served and soon missed.
const GET_READ_MIN_TAIL: u64 = 1024 * 1024;

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

    /// The offset just past this range's last byte.
    fn end(&self) -> u64 {
        self.start + self.data.len() as u64
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
    /// File path, shared with the readers `get_read` returns.
    path: Arc<str>,
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

/// Drive a storage future to completion from parquet's synchronous
/// `ChunkReader` context, choosing a strategy that is safe for the current
/// runtime flavor. Mirrors the canonical `run_sync_on_runtime` bridge in
/// fluree-db-binary-index.
///
/// - **Multi-thread runtime:** `block_in_place(handle.block_on(..))` so the
///   calling worker is released and Tokio keeps a thread driving the IO reactor
///   while we block. Correct on both worker threads (a future direct caller)
///   and `spawn_blocking` threads (today's sole caller, where `block_in_place`
///   is a harmless no-op).
/// - **Current-thread / unknown runtime:** `block_in_place` would panic and
///   `block_on` would deadlock the single thread, so the future is driven on a
///   self-contained current-thread runtime in a helper OS thread.
/// - **No ambient runtime:** block on the caller-provided fallback handle.
fn block_on_storage_fetch<F>(fallback: &Handle, fut: F) -> F::Output
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    match Handle::try_current() {
        Ok(handle) => match handle.runtime_flavor() {
            RuntimeFlavor::MultiThread => tokio::task::block_in_place(|| handle.block_on(fut)),
            // CurrentThread or any future/unknown flavor.
            _ => run_on_helper_runtime(fut),
        },
        Err(_) => fallback.block_on(fut),
    }
}

/// Run `fut` to completion on a dedicated current-thread runtime in a freshly
/// spawned OS thread, returning its output. Bridges sync→async from a
/// current-thread runtime without blocking that runtime's only thread on its
/// own progress.
fn run_on_helper_runtime<F>(fut: F) -> F::Output
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build helper runtime for iceberg chunk read");
        let _ = tx.send(rt.block_on(fut));
    });
    rx.recv()
        .expect("iceberg chunk-read helper runtime thread panicked")
}

/// Read `range` of `path` from `storage`, from parquet's synchronous context.
fn fetch_range<S: SendIcebergStorage + 'static>(
    storage: &Arc<S>,
    path: &Arc<str>,
    runtime: &Handle,
    range: Range<u64>,
) -> std::result::Result<Bytes, ParquetError> {
    let (start, end) = (range.start, range.end);
    let storage = Arc::clone(storage);
    let path_for_fetch = Arc::clone(path);
    let fetch = async move { storage.read_range(&path_for_fetch, range).await };
    block_on_storage_fetch(runtime, fetch).map_err(|e| {
        ParquetError::General(format!(
            "Failed to read range [{start}, {end}) from {path}: {e}"
        ))
    })
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
            path: path.into(),
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

    /// The cached bytes from `start` to the end of the newest cached window
    /// that covers `start` and either holds at least [`GET_READ_MIN_TAIL`]
    /// bytes past it or runs to the end of the file.
    fn cached_tail(&self, start: u64) -> std::result::Result<Option<Bytes>, ParquetError> {
        let cache = self
            .cache
            .lock()
            .map_err(|e| ParquetError::General(format!("Cache lock poisoned: {e}")))?;
        Ok(cache
            .iter()
            .rev()
            .find(|cached| {
                let end = cached.end();
                start >= cached.start
                    && start < end
                    && (end - start >= GET_READ_MIN_TAIL || end == self.file_size)
            })
            .map(|cached| cached.data.slice((start - cached.start) as usize..)))
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

        // Bridge parquet's sync ChunkReader to our async storage with a
        // flavor-aware blocking strategy (see `block_on_storage_fetch`).
        let data = fetch_range(
            &self.storage,
            &self.path,
            &self.runtime,
            aligned_start..aligned_end,
        )?;

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
    type T = RangeRead<S>;

    /// Parquet's page reader calls this once per page, to read the page's
    /// header; the body then comes through `get_bytes`. Serving it from a
    /// window already fetched keeps a page-by-page read of a column chunk at
    /// about one fetch per window. Requiring a cached window to hold a whole
    /// `GET_READ_WINDOW` past the offset instead meant a fresh 8 MiB fetch for
    /// nearly every page: 8 MiB moved per 1 MiB page.
    fn get_read(&self, start: u64) -> std::result::Result<Self::T, parquet::errors::ParquetError> {
        if start > self.file_size {
            return Err(ParquetError::EOF(format!(
                "Expected to read at offset {start}, while {} has length {}",
                self.path, self.file_size
            )));
        }
        let limit = (start + GET_READ_WINDOW).min(self.file_size);
        let head = match self.cached_tail(start)? {
            Some(tail) => tail.slice(..tail.len().min((limit - start) as usize)),
            None => self.fetch_bytes(start, (limit - start) as usize)?,
        };
        Ok(RangeRead {
            next: start + head.len() as u64,
            head: Cursor::new(head),
            limit,
            storage: Arc::clone(&self.storage),
            path: Arc::clone(&self.path),
            runtime: self.runtime.clone(),
        })
    }

    fn get_bytes(
        &self,
        start: u64,
        length: usize,
    ) -> std::result::Result<Bytes, parquet::errors::ParquetError> {
        self.fetch_bytes(start, length)
    }
}

/// The reader [`RangeBackedChunkReader::get_read`] returns: the bytes from its
/// offset that a cached or fresh window holds, then, only if read past them,
/// the rest of [`GET_READ_WINDOW`] from that offset, fetched uncached. That is
/// the old 8 MiB cursor's reach: a page header longer than a served window's
/// remainder still reads whole, while a corrupt header that declares a longer
/// field than that runs into the end of the reader, as it did before, instead
/// of pulling the rest of the file through it.
pub struct RangeRead<S: SendIcebergStorage + 'static> {
    /// The bytes not yet read from the current window.
    head: Cursor<Bytes>,
    /// The file offset just past the current window.
    next: u64,
    /// The file offset this reader stops at: `GET_READ_WINDOW` past where it
    /// started, or the end of the file.
    limit: u64,
    storage: Arc<S>,
    path: Arc<str>,
    runtime: Handle,
}

impl<S: SendIcebergStorage + 'static> Read for RangeRead<S> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.head.read(buf)?;
        if n > 0 || buf.is_empty() || self.next >= self.limit {
            return Ok(n);
        }
        let bytes = fetch_range(
            &self.storage,
            &self.path,
            &self.runtime,
            self.next..self.limit,
        )
        .map_err(std::io::Error::other)?;
        if bytes.is_empty() {
            // A store that returns nothing for a range inside the file would
            // otherwise loop here; report it as the end of what can be read.
            return Ok(0);
        }
        self.next += bytes.len() as u64;
        self.head = Cursor::new(bytes);
        self.head.read(buf)
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

    /// The sync ChunkReader called *directly* from a current-thread runtime
    /// (no `spawn_blocking`) must not panic or deadlock: the bridge drives the
    /// fetch on a helper runtime instead of blocking the single runtime thread
    /// on its own progress. Default `#[tokio::test]` is a current-thread runtime.
    #[tokio::test]
    async fn test_chunk_reader_current_thread_runtime_does_not_panic() {
        let data = Bytes::from(vec![7u8; 1000]);
        let storage = Arc::new(MockStorage { data });
        let handle = Handle::current();
        let reader = RangeBackedChunkReader::new(storage, "test.parquet".to_string(), 1000, handle);

        // Direct call on the current-thread runtime thread.
        let bytes = reader
            .get_bytes(100, 200)
            .expect("current-thread bridge should read without panicking");
        assert_eq!(bytes.len(), 200);
        assert!(bytes.iter().all(|&b| b == 7));

        // Cache hit path (no second fetch) still works.
        let bytes2 = reader.get_bytes(150, 50).expect("cache-hit read");
        assert_eq!(bytes2.len(), 50);
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

    const MIB: u64 = 1024 * 1024;

    /// A store over `data` that records every range it is asked for.
    #[derive(Debug)]
    struct CountingStorage {
        data: Bytes,
        fetches: Mutex<Vec<Range<u64>>>,
    }

    impl CountingStorage {
        /// `len` bytes, each a function of its offset, so a misplaced slice
        /// shows up as a mismatch.
        fn patterned(len: u64) -> Arc<Self> {
            let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            Self::over(Bytes::from(data))
        }

        fn over(data: Bytes) -> Arc<Self> {
            Arc::new(Self {
                data,
                fetches: Mutex::new(Vec::new()),
            })
        }

        fn fetches(&self) -> Vec<Range<u64>> {
            self.fetches.lock().unwrap().clone()
        }

        fn bytes_fetched(&self) -> u64 {
            self.fetches().iter().map(|r| r.end - r.start).sum()
        }
    }

    #[async_trait]
    impl SendIcebergStorage for CountingStorage {
        async fn read(&self, _path: &str) -> Result<Bytes> {
            panic!("a range-backed reader never reads a whole file");
        }

        async fn read_range(&self, _path: &str, range: Range<u64>) -> Result<Bytes> {
            self.fetches.lock().unwrap().push(range.clone());
            Ok(self.data.slice(range.start as usize..range.end as usize))
        }

        async fn file_size(&self, _path: &str) -> Result<u64> {
            Ok(self.data.len() as u64)
        }
    }

    fn reader_over(storage: &Arc<CountingStorage>) -> RangeBackedChunkReader<CountingStorage> {
        RangeBackedChunkReader::new(
            Arc::clone(storage),
            "test.parquet".to_string(),
            storage.data.len() as u64,
            Handle::current(),
        )
    }

    fn read_n(read: &mut impl Read, n: usize) -> Vec<u8> {
        let mut buf = vec![0u8; n];
        read.read_exact(&mut buf).unwrap();
        buf
    }

    /// Parquet's page reader reads a column chunk page by page: `get_read` at
    /// each page for its header, then `get_bytes` for the body. A header used
    /// to fetch a fresh 8 MiB window unless a cached one held 8 MiB past it, so
    /// these 16 MiB of 64 KiB pages took 64 fetches and about 520 MiB. A window
    /// already fetched now serves the pages it covers.
    #[tokio::test(flavor = "multi_thread")]
    async fn page_by_page_read_fetches_each_window_once() {
        const PAGE: u64 = 64 * 1024;
        const HEADER: u64 = 32;
        let storage = CountingStorage::patterned(24 * MIB);
        let data = storage.data.clone();
        let check = Arc::clone(&storage);
        tokio::task::spawn_blocking(move || {
            let reader = reader_over(&storage);
            let mut offset = 0;
            while offset + PAGE <= 16 * MIB {
                let mut read = reader.get_read(offset).unwrap();
                let header = read_n(&mut read, HEADER as usize);
                assert_eq!(header, data[offset as usize..(offset + HEADER) as usize]);
                let body = reader
                    .get_bytes(offset + HEADER, (PAGE - HEADER) as usize)
                    .unwrap();
                assert_eq!(
                    body,
                    data[(offset + HEADER) as usize..(offset + PAGE) as usize]
                );
                offset += PAGE;
            }
        })
        .await
        .unwrap();

        // Each window serves pages until fewer than GET_READ_MIN_TAIL bytes
        // are left in it, so 16 MiB of pages take three windows.
        let fetches = check.fetches();
        assert_eq!(fetches.len(), 3, "fetches: {fetches:?}");
        assert!(
            check.bytes_fetched() < 26 * MIB,
            "fetched {} bytes for 16 MiB of pages",
            check.bytes_fetched()
        );
    }

    /// A window with less than GET_READ_MIN_TAIL left past the offset doesn't
    /// serve it: a page body read from it would soon run off its end, so a
    /// fresh window is fetched from the offset instead.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_read_near_a_window_end_fetches_a_fresh_window() {
        let storage = CountingStorage::patterned(32 * MIB);
        let data = storage.data.clone();
        let check = Arc::clone(&storage);
        tokio::task::spawn_blocking(move || {
            let reader = reader_over(&storage);
            reader.get_read(0).unwrap();
            assert_eq!(storage.fetches().len(), 1);
            let first_end = storage.fetches()[0].end;

            // More than GET_READ_MIN_TAIL left: served from the first window.
            let served = first_end - GET_READ_MIN_TAIL;
            let mut read = reader.get_read(served).unwrap();
            assert_eq!(
                read_n(&mut read, 16),
                data[served as usize..served as usize + 16]
            );
            assert_eq!(storage.fetches().len(), 1);

            // Less than that left: a fresh window from the offset.
            let refetched = served + 1;
            let mut read = reader.get_read(refetched).unwrap();
            assert_eq!(
                read_n(&mut read, 16),
                data[refetched as usize..refetched as usize + 16]
            );
        })
        .await
        .unwrap();

        let fetches = check.fetches();
        assert_eq!(fetches.len(), 2, "fetches: {fetches:?}");
        let second = &fetches[1];
        assert!(
            second.start <= fetches[0].end - GET_READ_MIN_TAIL + 1
                && second.end - second.start >= GET_READ_WINDOW,
            "the second fetch is a full window from the offset: {second:?}"
        );
    }

    /// A window that runs to the end of the file serves any offset in it, however
    /// little is left: there is nothing past it to fetch.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_read_serves_a_window_that_runs_to_eof() {
        let storage = CountingStorage::patterned(9 * MIB);
        let data = storage.data.clone();
        let check = Arc::clone(&storage);
        tokio::task::spawn_blocking(move || {
            let reader = reader_over(&storage);
            reader.get_read(MIB).unwrap();
            assert_eq!(storage.fetches(), vec![MIB..9 * MIB]);

            let mut tail = Vec::new();
            reader
                .get_read(9 * MIB - 100)
                .unwrap()
                .read_to_end(&mut tail)
                .unwrap();
            assert_eq!(tail, data[(9 * MIB - 100) as usize..]);

            let mut at_eof = Vec::new();
            reader
                .get_read(9 * MIB)
                .unwrap()
                .read_to_end(&mut at_eof)
                .unwrap();
            assert!(at_eof.is_empty());
        })
        .await
        .unwrap();

        assert_eq!(check.fetches().len(), 1, "fetches: {:?}", check.fetches());
    }

    /// The reader `get_read` returns reaches `GET_READ_WINDOW` from its offset,
    /// as the old 8 MiB cursor did, and no further. Served from a cached window
    /// with less than that left, a read past the window's end fetches the rest
    /// of the reach in one uncached range (a page header longer than what the
    /// window has left still reads whole); a read past the reach ends there, so
    /// a corrupt header that declares a longer field can't pull the rest of the
    /// file through the reader.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_read_reads_up_to_its_reach_and_no_further() {
        let storage = CountingStorage::patterned(32 * MIB);
        let data = storage.data.clone();
        let check = Arc::clone(&storage);
        let (first_end, served, from_miss) = tokio::task::spawn_blocking(move || {
            let reader = reader_over(&storage);
            reader.get_read(0).unwrap();
            let first_end = storage.fetches()[0].end;

            // Served from the first window, 2 MiB before its end.
            let start = first_end - 2 * MIB;
            let mut served = Vec::new();
            reader
                .get_read(start)
                .unwrap()
                .read_to_end(&mut served)
                .unwrap();
            assert_eq!(
                served,
                data[start as usize..(start + GET_READ_WINDOW) as usize]
            );

            // A miss: the fresh window is the whole reach.
            let start = 20 * MIB + 1000;
            let mut from_miss = Vec::new();
            reader
                .get_read(start)
                .unwrap()
                .read_to_end(&mut from_miss)
                .unwrap();
            assert_eq!(
                from_miss,
                data[start as usize..(start + GET_READ_WINDOW) as usize]
            );
            (first_end, served.len(), from_miss.len())
        })
        .await
        .unwrap();

        assert_eq!(served as u64, GET_READ_WINDOW);
        assert_eq!(from_miss as u64, GET_READ_WINDOW);
        let fetches = check.fetches();
        assert_eq!(fetches.len(), 3, "fetches: {fetches:?}");
        // The served reader fetched only what its window lacked of the reach.
        assert_eq!(
            fetches[1],
            first_end..first_end - 2 * MIB + GET_READ_WINDOW,
            "fetches: {fetches:?}"
        );
    }

    /// An offset past the end of the file is an EOF error, as parquet's own
    /// `Bytes` reader gives, not a fetch of an inverted range.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_read_past_the_end_of_the_file_is_an_eof_error() {
        let storage = CountingStorage::patterned(MIB);
        let check = Arc::clone(&storage);
        let err = tokio::task::spawn_blocking(move || {
            reader_over(&storage)
                .get_read(MIB + 1)
                .err()
                .expect("an offset past the end is an error")
        })
        .await
        .unwrap();

        assert!(matches!(err, ParquetError::EOF(_)), "{err}");
        assert!(check.fetches().is_empty());
    }

    /// A page longer than what its header's window has left past the header
    /// (Iceberg writes 1 MiB pages by default, and real pages vary) costs one
    /// extra fetch at that window's edge: the body is fetched on its own, and
    /// the next header, with less than `GET_READ_MIN_TAIL` left in that body's
    /// range, starts a fresh window. Ten 1.75 MiB pages take four fetches,
    /// against ten 8 MiB fetches before.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_page_longer_than_its_windows_remainder_costs_one_extra_fetch() {
        const PAGE: u64 = 1792 * 1024;
        const HEADER: u64 = 32;
        let storage = CountingStorage::patterned(24 * MIB);
        let data = storage.data.clone();
        let check = Arc::clone(&storage);
        tokio::task::spawn_blocking(move || {
            let reader = reader_over(&storage);
            for page in 0..10 {
                let offset = page * PAGE;
                let mut read = reader.get_read(offset).unwrap();
                let header = read_n(&mut read, HEADER as usize);
                assert_eq!(header, data[offset as usize..(offset + HEADER) as usize]);
                let body = reader
                    .get_bytes(offset + HEADER, (PAGE - HEADER) as usize)
                    .unwrap();
                assert_eq!(
                    body,
                    data[(offset + HEADER) as usize..(offset + PAGE) as usize]
                );
            }
        })
        .await
        .unwrap();

        // A window from the first header; the fifth page's body, which runs
        // past it; a window from the sixth header; the tenth page's body.
        let fetches = check.fetches();
        let starts: Vec<u64> = fetches.iter().map(|r| r.start).collect();
        assert_eq!(
            starts,
            vec![0, 4 * PAGE, 5 * PAGE, 9 * PAGE],
            "fetches: {fetches:?}"
        );
    }

    /// End to end through the Arrow decoder: a file of small pages decodes to
    /// the same rows through the range-backed reader as from memory, in five
    /// fetches and little more than the file's bytes. Before page headers were
    /// served from cached windows, this 19 MB file took 43 fetches and 363 MB.
    #[tokio::test(flavor = "multi_thread")]
    async fn small_page_parquet_decodes_with_about_one_pass_over_the_file() {
        use arrow::array::{ArrayRef, Int64Array, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::collections::HashMap;

        const ROWS: i64 = 1_200_000;
        let field = |name: &str, id: &str| {
            Field::new(name, DataType::Int64, false).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_owned(),
                id.to_owned(),
            )]))
        };
        let schema = Arc::new(ArrowSchema::new(vec![field("a", "1"), field("b", "2")]));
        let a: ArrayRef = Arc::new(Int64Array::from_iter_values(
            (0..ROWS).map(|i| i.wrapping_mul(2_654_435_761)),
        ));
        let b: ArrayRef = Arc::new(Int64Array::from_iter_values(
            (0..ROWS).map(|i| i.wrapping_mul(40_503) ^ 0x5555),
        ));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![a, b]).unwrap();
        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_data_page_size_limit(64 * 1024)
            .build();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file = Bytes::from(buf);
        let file_size = file.len() as u64;
        assert!(file_size > 2 * GET_READ_WINDOW, "file is {file_size} bytes");

        let storage = CountingStorage::over(file.clone());
        let check = Arc::clone(&storage);
        let (ranged, in_memory) = tokio::task::spawn_blocking(move || {
            let ranged = crate::io::arrow_reader::decode_batches_arrow(
                reader_over(&storage),
                &[1, 2],
                None,
                None,
                None,
                None,
            )
            .unwrap();
            let in_memory = crate::io::arrow_reader::decode_batches_arrow(
                file,
                &[1, 2],
                None,
                None,
                None,
                None,
            )
            .unwrap();
            (ranged, in_memory)
        })
        .await
        .unwrap();

        let column = |batches: &[crate::io::ColumnBatch], i: usize| -> Vec<Option<i64>> {
            batches
                .iter()
                .flat_map(|b| match &b.columns[i] {
                    crate::io::Column::Int64(v) => v.clone(),
                    other => panic!("expected an Int64 column, got {other:?}"),
                })
                .collect()
        };
        for i in 0..2 {
            let got = column(&ranged, i);
            assert_eq!(got.len(), ROWS as usize);
            assert_eq!(got, column(&in_memory, i), "column {i}");
        }
        assert!(
            check.bytes_fetched() < 2 * file_size,
            "fetched {} bytes in {} reads for a {file_size}-byte file",
            check.bytes_fetched(),
            check.fetches().len()
        );
        // The footer, then a window per ~7 MiB of the two column chunks.
        assert_eq!(check.fetches().len(), 5, "fetches: {:?}", check.fetches());
    }
}
