//! `LazyS3Storage` — a [`SendIcebergStorage`] that DEFERS the loadTable GET + S3
//! client build until the first actual byte read (the PR-8 loadTable-metadata
//! cache: `21-loadtable-metadata-cache.md`).
//!
//! When a query's metadata (slice-2 disk cache), scan files (slice-2), and every
//! Parquet file ([`fluree_db_iceberg::DiskArtifactCache`]) are all disk-resident,
//! the reader never issues a read to the storage, so the deferred builder is never
//! invoked — no loadTable REST GET, no OAuth token exchange, no S3. A single disk
//! miss forces the builder ONCE (a `tokio::sync::OnceCell` dedups concurrent
//! Parquet reads racing to force), and it reuses the query session's existing
//! storage-build path (the slice-1 credential machinery), never a duplicate.
//!
//! A force-time build failure surfaces as an [`IcebergError`] carrying the same
//! "Failed to load table from catalog" cause the eager path produces — no
//! panic/unwrap inside the trait methods (a fallible `get_or_try_init`).

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use fluree_db_iceberg::error::{IcebergError, Result as IcebergResult};
use fluree_db_iceberg::io::{S3IcebergStorage, SendIcebergStorage};
use tokio::sync::OnceCell;

/// A boxed, `Send + Sync` async builder that performs the deferred loadTable GET
/// and returns the concrete storage — or an [`IcebergError`] mapped from the build
/// failure (so the trait methods surface the eager path's error shape).
pub(crate) type StorageBuilder<'a, S> = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = IcebergResult<Arc<S>>> + Send + 'a>> + Send + Sync + 'a,
>;

/// A storage handle whose concrete backend is built lazily, at most once. Generic
/// over the backend `S` for testability; production uses [`LazyS3Storage`].
///
/// `Clone` (via `Arc` fields) because [`fluree_db_iceberg::io::SendParquetReader`]
/// requires `S: Clone` — it clones the storage into its per-range read tasks. All
/// clones SHARE the one `cell`, so a reader that clones the handle into N range
/// reads still forces exactly ONE build (one loadTable GET).
pub(crate) struct LazyStorage<'a, S> {
    cell: Arc<OnceCell<Arc<S>>>,
    builder: StorageBuilder<'a, S>,
}

impl<S> Clone for LazyStorage<'_, S> {
    fn clone(&self) -> Self {
        Self {
            cell: Arc::clone(&self.cell),
            builder: Arc::clone(&self.builder),
        }
    }
}

/// The production alias: a lazily-built [`S3IcebergStorage`].
pub(crate) type LazyS3Storage<'a> = LazyStorage<'a, S3IcebergStorage>;

impl<'a, S> LazyStorage<'a, S> {
    /// Deferred: `builder` runs at most once, on the first read (if any).
    pub(crate) fn deferred(builder: StorageBuilder<'a, S>) -> Self {
        Self {
            cell: Arc::new(OnceCell::new()),
            builder,
        }
    }

    /// Pre-resolved: the GET already happened (a pointer/metadata miss forced it,
    /// or the pointer cache is off), so the builder is never called. Total (no
    /// unwrap): were the cell ever cleared by a future refactor, a read errors
    /// rather than panics.
    pub(crate) fn ready(storage: Arc<S>) -> Self {
        let cell = OnceCell::new();
        let _ = cell.set(storage); // infallible on a fresh cell
        Self {
            cell: Arc::new(cell),
            builder: Arc::new(|| {
                Box::pin(async {
                    Err(IcebergError::Storage(
                        "LazyStorage::ready builder invoked (cell was cleared)".to_string(),
                    ))
                })
            }),
        }
    }

    /// Resolve the concrete storage, forcing the deferred build on the first call.
    /// Concurrent callers dedup to ONE build (`OnceCell::get_or_try_init`); on a
    /// build error the cell stays empty and a later read retries.
    async fn resolve(&self) -> IcebergResult<&Arc<S>> {
        self.cell.get_or_try_init(|| (self.builder)()).await
    }

    /// Whether the backend has been built (a real S3 read has happened). Used by
    /// the caller to attribute the loadTable span only when forced.
    #[cfg(test)]
    pub(crate) fn is_forced(&self) -> bool {
        self.cell.initialized()
    }
}

impl<S> fmt::Debug for LazyStorage<'_, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazyStorage")
            .field("forced", &self.cell.initialized())
            .finish()
    }
}

#[async_trait]
impl<S: SendIcebergStorage> SendIcebergStorage for LazyStorage<'_, S> {
    async fn read(&self, path: &str) -> IcebergResult<Bytes> {
        self.resolve().await?.read(path).await
    }

    async fn read_range(&self, path: &str, range: std::ops::Range<u64>) -> IcebergResult<Bytes> {
        self.resolve().await?.read_range(path, range).await
    }

    async fn file_size(&self, path: &str) -> IcebergResult<u64> {
        self.resolve().await?.file_size(path).await
    }

    async fn list_dir(&self, prefix: &str) -> IcebergResult<Vec<String>> {
        // Warehouse-root resolution genuinely needs the object store, so this
        // forces the deferred build (the one case the lazy optimization does not
        // apply — a build-time LIST, not a per-file read).
        self.resolve().await?.list_dir(prefix).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A trivial in-memory backend that counts reads — stands in for the real S3
    /// client so the lazy/dedup logic is testable without AWS.
    #[derive(Debug)]
    struct CountingStorage {
        reads: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SendIcebergStorage for CountingStorage {
        async fn read(&self, _path: &str) -> IcebergResult<Bytes> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(Bytes::new())
        }
        async fn read_range(&self, _p: &str, _r: std::ops::Range<u64>) -> IcebergResult<Bytes> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(Bytes::new())
        }
        async fn file_size(&self, _p: &str) -> IcebergResult<u64> {
            Ok(0)
        }
    }

    // The whole point: when nothing reads, the builder never runs — no loadTable
    // GET for a fully disk-cached query.
    #[tokio::test]
    async fn deferred_builder_not_invoked_without_a_read() {
        let builds = Arc::new(AtomicUsize::new(0));
        let b = Arc::clone(&builds);
        let reads = Arc::new(AtomicUsize::new(0));
        let r = Arc::clone(&reads);
        let lazy: LazyStorage<'_, CountingStorage> = LazyStorage::deferred(Arc::new(move || {
            let b = Arc::clone(&b);
            let r = Arc::clone(&r);
            Box::pin(async move {
                b.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(CountingStorage { reads: r }))
            })
        }));
        assert!(!lazy.is_forced());
        assert_eq!(
            builds.load(Ordering::SeqCst),
            0,
            "no read ⇒ no build ⇒ no GET"
        );
    }

    // The lead's expectation (b): N Parquet reads racing to force dedup to ONE
    // build (one loadTable GET), and every read still resolves.
    #[tokio::test]
    async fn concurrent_forces_dedup_to_one_build() {
        let builds = Arc::new(AtomicUsize::new(0));
        let reads = Arc::new(AtomicUsize::new(0));
        let b = Arc::clone(&builds);
        let r = Arc::clone(&reads);
        let lazy: LazyStorage<'_, CountingStorage> = LazyStorage::deferred(Arc::new(move || {
            let b = Arc::clone(&b);
            let r = Arc::clone(&r);
            Box::pin(async move {
                b.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(CountingStorage { reads: r }))
            })
        }));
        // 16 concurrent reads on the SAME handle (join, not spawn — no 'static
        // needed; join_all polls them concurrently so they race the OnceCell).
        let futs = (0..16).map(|_| lazy.read("s3://b/f.parquet"));
        let results = futures::future::join_all(futs).await;
        assert!(results.iter().all(Result::is_ok), "every read resolves");
        assert_eq!(
            builds.load(Ordering::SeqCst),
            1,
            "concurrent forces dedup to ONE build"
        );
        assert_eq!(
            reads.load(Ordering::SeqCst),
            16,
            "all 16 reads reach the backend"
        );
        assert!(lazy.is_forced());
    }

    // A force-time build failure surfaces as an error (no panic), and the cell
    // stays empty so a later read can retry (matching get_or_try_init semantics).
    #[tokio::test]
    async fn build_failure_surfaces_as_error_no_panic() {
        let lazy: LazyStorage<'_, CountingStorage> = LazyStorage::deferred(Arc::new(|| {
            Box::pin(async {
                Err(IcebergError::Catalog(
                    "Failed to load table from catalog: boom".to_string(),
                ))
            })
        }));
        let err = lazy.read("s3://b/f").await.unwrap_err();
        assert!(
            err.to_string()
                .contains("Failed to load table from catalog"),
            "the eager path's cause is preserved: {err}"
        );
        assert!(
            !lazy.is_forced(),
            "a failed build leaves the cell empty for a retry"
        );
    }
}
