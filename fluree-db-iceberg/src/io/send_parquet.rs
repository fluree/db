//! Send-safe Parquet file reader.
//!
//! This module provides `SendParquetReader` which mirrors `ParquetReader` but uses
//! `SendIcebergStorage` for AWS SDK integration where futures must be `Send`.
//!
//! This is required because the query engine's `Operator` trait requires Send futures,
//! but the standard `IcebergStorage` trait uses `?Send` for WASM compatibility.
//!
//! # Large File Support
//!
//! For files larger than `MAX_SPARSE_BUFFER_SIZE` (64MB), this reader uses
//! `RangeBackedChunkReader` instead of loading the entire file into memory.
//! This enables processing of large Parquet files in memory-constrained
//! environments like AWS Lambda.

use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use fluree_db_core::disk_cache::DiskArtifactCache;
use tokio::runtime::Handle;
// PR-2 phase-1 (measurement-only): `.instrument()` for the per-file cost
// decomposition sub-spans in `read_task_small_file`. Additive/debug-level;
// no behavior change.
use tracing::Instrument as _;

use crate::error::{IcebergError, Result};
use crate::io::batch::ColumnBatch;
use crate::io::chunk_reader::RangeBackedChunkReader;
use crate::io::parquet::{
    build_batch_schema, build_batch_schema_with_iceberg, calculate_column_chunk_ranges,
    parse_parquet_metadata_from_bytes, ParquetFooterCache, NULL_COLUMN_SENTINEL,
};
use crate::io::SendIcebergStorage;
use crate::metadata::Schema;
use crate::scan::predicate::Expression;
use crate::scan::pruning::row_group_can_contain;
use crate::scan::FileScanTask;

use parquet::file::metadata::ParquetMetaData;
use std::collections::HashMap;

/// Parquet magic bytes (footer ends with "PAR1").
const PARQUET_MAGIC: [u8; 4] = *b"PAR1";

/// Whether row-group / row-level predicate pushdown is enabled. Read once from
/// `FLUREE_ICEBERG_PREDICATE_PUSHDOWN` (only `0`/`false`/`off` disable it).
pub(crate) fn predicate_pushdown_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(
        || match std::env::var("FLUREE_ICEBERG_PREDICATE_PUSHDOWN") {
            Ok(v) => !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "off"
            ),
            Err(_) => true,
        },
    )
}

/// The row groups to decode: those whose column statistics do not rule out
/// `residual`. Returns every row group (no pruning) when pushdown is disabled or
/// there is no residual filter. Pruning is conservative — a row group is dropped
/// only when its min/max bounds prove no row can match, so results are unchanged.
pub(crate) fn surviving_row_groups(
    metadata: &ParquetMetaData,
    residual: Option<&Expression>,
    field_id_to_leaf: &HashMap<i32, usize>,
) -> Vec<usize> {
    let n = metadata.num_row_groups();
    let Some(expr) = residual.filter(|_| predicate_pushdown_enabled()) else {
        return (0..n).collect();
    };
    let mut keep = Vec::with_capacity(n);
    for rg in 0..n {
        if row_group_can_contain(expr, metadata.row_group(rg), field_id_to_leaf) {
            keep.push(rg);
        }
    }
    let pruned = n - keep.len();
    if pruned > 0 {
        tracing::debug!(
            row_groups_total = n,
            row_groups_pruned = pruned,
            "Row-group pruning skipped non-matching row groups"
        );
    }
    keep
}

/// Maximum file size for sparse buffer allocation (64MB).
///
/// Files smaller than this use sparse buffer approach (efficient range reads
/// assembled into a single buffer). Files larger than this use on-demand
/// range reads via `RangeBackedChunkReader` to avoid excessive memory allocation.
const MAX_SPARSE_BUFFER_SIZE: u64 = 64 * 1024 * 1024;

/// Disk-cache policy (rule 2): a file at or below this size is "obviously cheap"
/// to cache whole rather than range-read.
const WHOLE_FILE_MAX_BYTES: u64 = 32 * 1024 * 1024;

/// Disk-cache policy (rule 2): when the projection touches at least this percent
/// of a file, the read is "broad" enough to cache the whole file.
const WHOLE_FILE_MIN_SHARE_PCT: u64 = 50;

/// Correctness floor: files below this size are always read whole (a sparse
/// buffer can omit chunks the reader dereferences). Applies even without a
/// disk cache.
const MIN_SPARSE_FILE_BYTES: u64 = 1024 * 1024;

/// Ceiling on large-file broad admission. Filling caches the whole file, which
/// transiently holds it in memory once; this bounds that spike (and the per-file
/// cache footprint). Larger broad files keep range-reading from source uncached.
const LARGE_FILE_ADMIT_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// Borrowed handle to the shared on-disk cache plus its directory, used to map an
/// immutable source path to a stable local cache file.
#[derive(Clone, Copy)]
struct DiskCacheRef<'a> {
    cache: &'a Arc<DiskArtifactCache>,
    dir: &'a Path,
}

impl DiskCacheRef<'_> {
    /// Stable local path for an immutable source file. The expected size is part
    /// of the key, so a cached file whose length differs is never even named.
    fn local_path(&self, source_path: &str, expected_size: u64) -> PathBuf {
        let h = xxhash_rust::xxh64::xxh64(source_path.as_bytes(), 0);
        self.dir
            .join(format!("iceberg-{h:016x}-{expected_size}.parquet"))
    }

    /// The local cache path iff a file of exactly `expected_size` bytes exists.
    fn valid_local(&self, source_path: &str, expected_size: u64) -> Option<PathBuf> {
        let p = self.local_path(source_path, expected_size);
        match std::fs::metadata(&p) {
            Ok(m) if m.len() == expected_size => Some(p),
            _ => None,
        }
    }
}

/// Admission decision (policy rule 2): cache + read the whole file when the read
/// is cheap (file at/under [`WHOLE_FILE_MAX_BYTES`]) or broad (projection touches
/// at least [`WHOLE_FILE_MIN_SHARE_PCT`] percent of the file by bytes). A narrow
/// projection of a large file returns `false` and keeps range-reading.
fn admit_whole_file(file_size: u64, projected_bytes: u64) -> bool {
    let cheap = file_size <= WHOLE_FILE_MAX_BYTES;
    let broad =
        projected_bytes.saturating_mul(100) >= file_size.saturating_mul(WHOLE_FILE_MIN_SHARE_PCT);
    cheap || broad
}

/// Read a whole cached file from local disk, returning `None` (so the caller
/// re-fetches from source) unless the byte length matches `expected_size` —
/// guarding against a truncated or otherwise wrong cached file.
async fn read_whole_local(path: &Path, expected_size: u64) -> Option<Bytes> {
    let owned = path.to_path_buf();
    match tokio::task::spawn_blocking(move || std::fs::read(&owned)).await {
        Ok(Ok(bytes)) if bytes.len() as u64 == expected_size => Some(Bytes::from(bytes)),
        _ => None,
    }
}

/// Lever A kill switch (`FLUREE_ICEBERG_FOOTER_FROM_CACHE`, default **on**). When
/// on, a file that is read whole anyway — a disk-cache hit, or a cheap/small file
/// the policy fetches whole — parses its Parquet footer from those in-memory
/// bytes instead of issuing a separate footer round-trip (measured ~190ms of two
/// serial S3 range GETs per file, see `docs/audit/2026-07-virtual-dataset-perf/06-per-file-cost.md`).
/// Off (`0`/`false`/`off`/`no`, trimmed + case-insensitive per the R2RML switch
/// family) restores the byte-identical footer-first path. Read once per process
/// (`OnceLock`, the family idiom — e.g. `catalog_session::cache_enabled` in
/// `fluree-db-api`): set it at startup, not per query.
fn footer_from_cache_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("FLUREE_ICEBERG_FOOTER_FROM_CACHE") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        ),
        Err(_) => true,
    })
}

/// Parse Parquet metadata from a whole-file byte buffer already resident in
/// memory — the footer is sliced out of `bytes`, so this does **no I/O**. Mirrors
/// the validation and parse in [`SendParquetReader::read_metadata`] exactly (same
/// footer slice `file_size - 8 - footer_len .. file_size`, same parser), differing
/// only in that the bytes are already local.
fn metadata_from_whole_bytes(bytes: &Bytes) -> Result<Arc<ParquetMetaData>> {
    let file_size = bytes.len() as u64;
    if file_size < 12 {
        return Err(IcebergError::Storage(format!(
            "File too small to be Parquet: {file_size} bytes"
        )));
    }
    let len = bytes.len();
    if bytes[len - 4..len] != PARQUET_MAGIC {
        return Err(IcebergError::Storage(
            "Invalid Parquet file: missing magic bytes".to_string(),
        ));
    }
    let footer_len = u32::from_le_bytes([
        bytes[len - 8],
        bytes[len - 7],
        bytes[len - 6],
        bytes[len - 5],
    ]) as u64;
    let footer_start = file_size.saturating_sub(8 + footer_len) as usize;
    let footer_bytes = bytes.slice(footer_start..len);
    let metadata = parse_parquet_metadata_from_bytes(&footer_bytes, file_size)?;
    Ok(Arc::new(metadata))
}

/// A [`SendIcebergStorage`] backed by a single local cache file, serving a large
/// file's byte ranges from local disk instead of the remote store. Reads run
/// synchronously; the chunk reader only drives this from a blocking context.
///
/// Because the shared disk cache can evict this file concurrently (eviction is
/// mtime-ordered with no in-use pinning), a local read can fail *after* the file
/// was validated. Rather than failing the query, a failed local read falls back
/// to the `source` store using the original source `path` — turning a mid-read
/// eviction into a (rare) slow read instead of an `IcebergError`.
#[derive(Debug)]
struct LocalFileStorage<S: SendIcebergStorage> {
    path: PathBuf,
    size: u64,
    source: Arc<S>,
}

impl<S: SendIcebergStorage> LocalFileStorage<S> {
    fn read_local(&self) -> std::io::Result<Bytes> {
        std::fs::read(&self.path).map(Bytes::from)
    }

    fn read_range_local(&self, range: &Range<u64>) -> std::io::Result<Bytes> {
        let mut f = std::fs::File::open(&self.path)?;
        f.seek(SeekFrom::Start(range.start))?;
        let len = range.end.saturating_sub(range.start) as usize;
        let mut buf = vec![0u8; len];
        f.read_exact(&mut buf)?;
        Ok(Bytes::from(buf))
    }
}

#[async_trait]
impl<S: SendIcebergStorage + 'static> SendIcebergStorage for LocalFileStorage<S> {
    async fn read(&self, path: &str) -> Result<Bytes> {
        match self.read_local() {
            Ok(bytes) => Ok(bytes),
            Err(e) => {
                tracing::debug!(path, error = %e, "local cache read failed; falling back to source");
                self.source.read(path).await
            }
        }
    }

    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        match self.read_range_local(&range) {
            Ok(bytes) => Ok(bytes),
            Err(e) => {
                tracing::debug!(path, error = %e, "local cache range read failed; falling back to source");
                self.source.read_range(path, range).await
            }
        }
    }

    async fn file_size(&self, _path: &str) -> Result<u64> {
        Ok(self.size)
    }
}

/// Total bytes the projection reads (sum of projected column chunk ranges),
/// used to decide broad-read admission for large files.
fn projected_chunk_bytes(task: &FileScanTask, metadata: &Arc<ParquetMetaData>) -> Result<u64> {
    let (_, column_indices) = if let Some(ref iceberg_schema) = task.iceberg_schema {
        build_batch_schema_with_iceberg(metadata, iceberg_schema, &task.projected_field_ids)?
    } else {
        build_batch_schema(metadata, &task.projected_field_ids)?
    };
    let real: Vec<usize> = column_indices
        .into_iter()
        .filter(|&idx| idx != NULL_COLUMN_SENTINEL)
        .collect();
    Ok(calculate_column_chunk_ranges(metadata, &real)
        .iter()
        .map(|(s, e)| e - s)
        .sum())
}

/// Send-safe Parquet reader with range-read support.
///
/// This is identical to `ParquetReader` but uses `SendIcebergStorage` instead of
/// `IcebergStorage`, producing `Send` futures for use with tokio::spawn and
/// async_trait without ?Send.
pub struct SendParquetReader<'a, S: SendIcebergStorage> {
    storage: &'a S,
    footer_cache: Option<&'a ParquetFooterCache>,
    disk_cache: Option<DiskCacheRef<'a>>,
}

impl<'a, S: SendIcebergStorage> SendParquetReader<'a, S> {
    /// Create a new Send-safe Parquet reader.
    pub fn new(storage: &'a S) -> Self {
        Self {
            storage,
            footer_cache: None,
            disk_cache: None,
        }
    }

    /// Create a reader with footer caching.
    pub fn with_cache(storage: &'a S, cache: &'a ParquetFooterCache) -> Self {
        Self {
            storage,
            footer_cache: Some(cache),
            disk_cache: None,
        }
    }

    /// Create a reader with footer caching and the shared on-disk data-file cache.
    ///
    /// The disk cache participates in the read path's whole-file-vs-range policy:
    /// a whole file already cached is served from disk; otherwise the file is
    /// cached whole only when the read is cheap (small file) or broad (touches a
    /// large share). Narrow projections keep range-reading from source.
    pub fn with_caches(
        storage: &'a S,
        footer_cache: &'a ParquetFooterCache,
        disk_cache: &'a Arc<DiskArtifactCache>,
        cache_dir: &'a Path,
    ) -> Self {
        Self {
            storage,
            footer_cache: Some(footer_cache),
            disk_cache: Some(DiskCacheRef {
                cache: disk_cache,
                dir: cache_dir,
            }),
        }
    }

    /// Read the Parquet file metadata (footer) using range reads.
    pub async fn read_metadata(&self, path: &str) -> Result<Arc<ParquetMetaData>> {
        let file_size = self.storage.file_size(path).await?;

        // Check cache
        if let Some(cache) = self.footer_cache {
            if let Some(cached) = cache.get(path, file_size).await {
                tracing::debug!(path, "Using cached Parquet footer");
                return Ok(cached);
            }
        }

        // Read footer length (last 8 bytes: 4-byte length + 4-byte magic)
        if file_size < 12 {
            return Err(IcebergError::Storage(format!(
                "File too small to be Parquet: {file_size} bytes"
            )));
        }

        let footer_size_range = (file_size - 8)..file_size;
        let footer_size_bytes = self.storage.read_range(path, footer_size_range).await?;

        // Verify magic bytes
        if footer_size_bytes[4..8] != PARQUET_MAGIC {
            return Err(IcebergError::Storage(
                "Invalid Parquet file: missing magic bytes".to_string(),
            ));
        }

        // Parse footer length
        let footer_len = u32::from_le_bytes([
            footer_size_bytes[0],
            footer_size_bytes[1],
            footer_size_bytes[2],
            footer_size_bytes[3],
        ]) as u64;

        // Read entire footer + magic
        let footer_start = file_size.saturating_sub(8 + footer_len);
        let footer_range = footer_start..file_size;
        let footer_bytes = self.storage.read_range(path, footer_range).await?;

        // Parse using parquet-rs
        let metadata = parse_parquet_metadata_from_bytes(&footer_bytes, file_size)?;
        let metadata = Arc::new(metadata);

        // Cache the footer
        if let Some(cache) = self.footer_cache {
            cache
                .put(path.to_string(), file_size, Arc::clone(&metadata))
                .await;
        }

        Ok(metadata)
    }

    /// Read a file scan task into column batches.
    ///
    /// Decodes via the Arrow reader ([`crate::io::arrow_reader`]) with:
    /// - projection pushdown (only the requested columns are read),
    /// - row-group pruning + exact row filtering from `task.residual_filter`.
    ///
    /// For files larger than 64MB, uses `RangeBackedChunkReader` for on-demand
    /// range reads instead of loading the entire file into memory.
    pub async fn read_task(&self, task: &FileScanTask) -> Result<Vec<ColumnBatch>>
    where
        S: Clone + 'static,
    {
        let file_size = task.data_file.file_size_in_bytes as u64;

        // Large files take the on-demand range-read path, which itself serves
        // from / fills the disk cache (rule 1/2) before falling back to source.
        if file_size > MAX_SPARSE_BUFFER_SIZE {
            return self.read_task_large_file(task).await;
        }

        self.read_task_small_file(task).await
    }

    /// Lever A: obtain the whole-file bytes for the cases where the disk-cache /
    /// small-file policy reads the file whole regardless of projection, WITHOUT a
    /// separate footer read. Returns `None` for files that take the sparse-range
    /// path (they keep the footer-first policy in [`Self::read_task_small_file`]).
    ///
    /// - **A2** (disk-cache hit, any size in the small-file range): the whole file
    ///   — and thus its footer — is served from the cached local bytes.
    /// - **A1** (a miss on the cheap tier `<= WHOLE_FILE_MAX_BYTES`, admitted whole
    ///   unconditionally by `admit_whole_file`'s `cheap` branch; or a
    ///   sub-[`MIN_SPARSE_FILE_BYTES`] file with no disk cache): the file is
    ///   fetched whole in one GET (single-flight, cached when a disk cache is
    ///   present), then its footer is parsed from those bytes.
    ///
    /// A larger file with a narrow projection returns `None` — that is the
    /// range-read tier, whose footer path is intentionally unchanged.
    async fn try_whole_bytes_no_footer(&self, path: &str, file_size: u64) -> Result<Option<Bytes>> {
        if let Some(dc) = self.disk_cache.filter(|dc| dc.cache.budget_bytes() > 0) {
            let local = dc.local_path(path, file_size);
            // A2: whole file already local — serve footer + data from disk.
            if let Some(bytes) = read_whole_local(&local, file_size).await {
                tracing::debug!(
                    path,
                    file_size,
                    "Lever A: footer+data from disk cache (whole file)"
                );
                return Ok(Some(bytes));
            }
            // A1 (cheap tier): admitted whole unconditionally, so fetch whole
            // without the footer. Single-flight fill mirrors `read_file_for_task`.
            if file_size <= WHOLE_FILE_MAX_BYTES {
                let data = dc
                    .cache
                    .coalesced_fetch(local, || async {
                        self.storage
                            .read(path)
                            .await
                            .map(|b| b.to_vec())
                            .map_err(|e| std::io::Error::other(e.to_string()))
                    })
                    .await
                    .map_err(|e| IcebergError::Storage(format!("disk-cache fill: {e}")))?;
                tracing::debug!(
                    path,
                    file_size,
                    "Lever A: footer+data from whole-file fetch (cheap, cached)"
                );
                return Ok(Some(Bytes::from(data)));
            }
            // Larger file, narrow projection: range-read tier (unchanged).
            return Ok(None);
        }
        // No disk cache: only the sub-`MIN_SPARSE_FILE_BYTES` correctness-floor
        // files are read whole (matches `read_file_for_task`).
        if file_size < MIN_SPARSE_FILE_BYTES {
            tracing::debug!(
                path,
                file_size,
                "Lever A: footer+data from whole small file (no disk cache)"
            );
            return Ok(Some(self.storage.read(path).await?));
        }
        Ok(None)
    }

    /// Read a small file (`<= MAX_SPARSE_BUFFER_SIZE`).
    //
    // PR-2: Lever A (footer-from-cache) fast path first — when the file is read
    // whole anyway, parse the footer from the fetched/cached bytes instead of a
    // separate 2-round-trip footer read. Sparse-range files (larger, narrow
    // projection) and the kill-switch-off case fall through to the unchanged
    // footer-first path. The four `iceberg.*` sub-spans (PR-2 phase-1 counters)
    // decompose the per-file wall; in the fast path `iceberg.read_footer` times
    // the in-memory footer parse — the ~190ms round-trip collapsed to a parse.
    async fn read_task_small_file(&self, task: &FileScanTask) -> Result<Vec<ColumnBatch>> {
        let path = &task.data_file.file_path;
        let file_size = task.data_file.file_size_in_bytes as u64;

        // Lever A fast path (default on; FLUREE_ICEBERG_FOOTER_FROM_CACHE=0 skips).
        if footer_from_cache_enabled() {
            let maybe_whole = self
                .try_whole_bytes_no_footer(path, file_size)
                .instrument(tracing::debug_span!("iceberg.fetch_bytes"))
                .await?;
            if let Some(file_bytes) = maybe_whole {
                // Parse+validate the footer from the in-memory bytes (no I/O).
                // `decode_batches_arrow` re-derives it from the same bytes; this
                // parse is the early validation and the footer-cost counter.
                {
                    let _footer_guard = tracing::debug_span!("iceberg.read_footer").entered();
                    metadata_from_whole_bytes(&file_bytes)?;
                }
                let _decode_guard = tracing::debug_span!("iceberg.decode").entered();
                return crate::io::arrow_reader::decode_batches_arrow(
                    file_bytes,
                    &task.projected_field_ids,
                    task.residual_filter.as_ref(),
                    task.iceberg_schema.as_deref(),
                    None,
                );
            }
        }

        // Unchanged path: footer first (range reads), then the policy-driven fetch
        // of the projected column chunks (sparse) or the whole file.
        let metadata = self
            .read_metadata(path)
            .instrument(tracing::debug_span!("iceberg.read_footer"))
            .await?;

        // Resolve the projected Parquet column indices so the sparse-range read
        // fetches exactly the column chunks the Arrow reader will decode.
        let (_, column_indices) = {
            let _plan_guard = tracing::debug_span!("iceberg.plan_columns").entered();
            if let Some(ref iceberg_schema) = task.iceberg_schema {
                build_batch_schema_with_iceberg(
                    &metadata,
                    iceberg_schema,
                    &task.projected_field_ids,
                )?
            } else {
                build_batch_schema(&metadata, &task.projected_field_ids)?
            }
        };

        let real_column_indices: Vec<usize> = column_indices
            .iter()
            .copied()
            .filter(|&idx| idx != NULL_COLUMN_SENTINEL)
            .collect();

        // Read the file bytes via range reads for the needed column chunks.
        let file_bytes = self
            .read_file_for_task(path, task, &real_column_indices, &metadata)
            .instrument(tracing::debug_span!("iceberg.fetch_bytes"))
            .await?;

        // Decode the range-read bytes with native projection + row-group pruning
        // + exact row filtering. `Bytes` is a `ChunkReader`, so the Arrow reader
        // reuses the exact bytes fetched above.
        let _decode_guard = tracing::debug_span!("iceberg.decode").entered();
        crate::io::arrow_reader::decode_batches_arrow(
            file_bytes,
            &task.projected_field_ids,
            task.residual_filter.as_ref(),
            task.iceberg_schema.as_deref(),
            None,
        )
    }

    /// Read a large file, applying the disk-cache policy before falling back to
    /// on-demand range reads from source:
    ///   1. a validly cached whole file → range-read lazily from local disk;
    ///   2. a broad read of a large file within the fill ceiling → fill once
    ///      (coalesced), then range-read from local;
    ///   3. otherwise → range-read from source on demand, uncached.
    async fn read_task_large_file(&self, task: &FileScanTask) -> Result<Vec<ColumnBatch>>
    where
        S: Clone + 'static,
    {
        let path = task.data_file.file_path.clone();
        let file_size = task.data_file.file_size_in_bytes as u64;
        let projected_field_ids = task.projected_field_ids.clone();
        let residual_filter = task.residual_filter.clone();
        let iceberg_schema = task.iceberg_schema.clone();
        let runtime = Handle::current();

        if let Some(dc) = self.disk_cache.filter(|dc| dc.cache.budget_bytes() > 0) {
            // Rule 1: serve a validly cached whole file as lazy local ranges.
            if let Some(local) = dc.valid_local(&path, file_size) {
                tracing::debug!(path = %path, file_size, "Iceberg disk-cache hit (large)");
                let storage = Arc::new(LocalFileStorage {
                    path: local,
                    size: file_size,
                    source: Arc::new(self.storage.clone()),
                });
                return Self::decode_large_file(
                    storage,
                    path,
                    file_size,
                    projected_field_ids,
                    residual_filter,
                    iceberg_schema,
                    runtime,
                    None,
                )
                .await;
            }
            // Rule 2: admit a broad read of a large file within the fill ceiling.
            // Fill once (coalesced) then serve from local; narrow large reads
            // skip this and range-read from source.
            if file_size <= LARGE_FILE_ADMIT_MAX_BYTES {
                let metadata = self.read_metadata(&path).await?;
                let projected_bytes = projected_chunk_bytes(task, &metadata)?;
                if admit_whole_file(file_size, projected_bytes) {
                    let local = dc.local_path(&path, file_size);
                    dc.cache
                        .coalesced_fetch(local.clone(), || async {
                            self.storage
                                .read(&path)
                                .await
                                .map(|b| b.to_vec())
                                .map_err(|e| std::io::Error::other(e.to_string()))
                        })
                        .await
                        .map_err(|e| IcebergError::Storage(format!("disk-cache fill: {e}")))?;
                    tracing::debug!(path = %path, file_size, "Iceberg disk-cache fill (large, broad)");
                    let storage = Arc::new(LocalFileStorage {
                        path: local,
                        size: file_size,
                        source: Arc::new(self.storage.clone()),
                    });
                    return Self::decode_large_file(
                        storage,
                        path,
                        file_size,
                        projected_field_ids,
                        residual_filter,
                        iceberg_schema,
                        runtime,
                        None,
                    )
                    .await;
                }
            }
        }

        tracing::info!(file_size, path = %path, "Using range-backed chunk reader (source)");
        let storage = Arc::new(self.storage.clone());
        Self::decode_large_file(
            storage,
            path,
            file_size,
            projected_field_ids,
            residual_filter,
            iceberg_schema,
            runtime,
            None,
        )
        .await
    }

    /// Decode a large Parquet file by streaming byte ranges from `storage` (the
    /// source store or a local cache file) via `RangeBackedChunkReader`,
    /// projecting only the requested columns. Runs the sync decode on a blocking
    /// thread.
    ///
    /// `max_rows` bounds the decode to a cheap first-row-group "peek" (see
    /// [`crate::io::arrow_reader::decode_batches_arrow`]); `None` decodes the
    /// whole projected file.
    // Params mirror the `FileScanTask` fields already destructured for the
    // `spawn_blocking` move plus the storage/runtime/budget; bundling them would
    // not clarify this internal helper.
    #[expect(clippy::too_many_arguments)]
    async fn decode_large_file<St: SendIcebergStorage + 'static>(
        storage: Arc<St>,
        path: String,
        file_size: u64,
        projected_field_ids: Vec<i32>,
        residual_filter: Option<Expression>,
        iceberg_schema: Option<Arc<Schema>>,
        runtime: Handle,
        max_rows: Option<usize>,
    ) -> Result<Vec<ColumnBatch>> {
        // Run the sync Arrow decode in a blocking context: native projection +
        // row-group pruning + row filtering over the range-backed reader
        // (skipped row groups' column chunks are never fetched).
        let result = tokio::task::spawn_blocking(move || {
            let chunk_reader = RangeBackedChunkReader::new(storage, path, file_size, runtime);
            crate::io::arrow_reader::decode_batches_arrow(
                chunk_reader,
                &projected_field_ids,
                residual_filter.as_ref(),
                iceberg_schema.as_deref(),
                max_rows,
            )
        })
        .await
        .map_err(|e| IcebergError::Storage(format!("Blocking task failed: {e}")))?;

        result
    }

    /// Read at most `max_rows` rows from the **first row group** of the task's
    /// file — a bounded, cheap "peek" for row previews and the LLM data sampler.
    ///
    /// Unlike [`Self::read_task`], this never reads the whole file: it drives the
    /// range-backed chunk reader against the source store, and the row-group-0
    /// restriction + row budget in
    /// [`decode_batches_arrow`](crate::io::arrow_reader::decode_batches_arrow)
    /// mean only the footer plus the first row group's projected column chunks are
    /// fetched, whatever the file size. It therefore returns at most
    /// `min(max_rows, rows-in-first-row-group)` rows and does **not** fill the
    /// disk cache (a peek should not pull a whole file into it).
    pub async fn read_task_sample(
        &self,
        task: &FileScanTask,
        max_rows: usize,
    ) -> Result<Vec<ColumnBatch>>
    where
        S: Clone + 'static,
    {
        if max_rows == 0 {
            return Ok(Vec::new());
        }
        let path = task.data_file.file_path.clone();
        let file_size = task.data_file.file_size_in_bytes as u64;
        let storage = Arc::new(self.storage.clone());
        Self::decode_large_file(
            storage,
            path,
            file_size,
            task.projected_field_ids.clone(),
            task.residual_filter.clone(),
            task.iceberg_schema.clone(),
            Handle::current(),
            Some(max_rows),
        )
        .await
    }

    /// Read file bytes needed for the task using range reads (small files only).
    async fn read_file_for_task(
        &self,
        path: &str,
        task: &FileScanTask,
        real_column_indices: &[usize],
        metadata: &Arc<ParquetMetaData>,
    ) -> Result<Bytes> {
        let file_size = task.data_file.file_size_in_bytes as u64;

        // Column chunk ranges for the projection (metadata-only, no I/O). These
        // drive the disk-cache whole-file-vs-range decision below.
        let column_ranges = calculate_column_chunk_ranges(metadata, real_column_indices);

        // Disk-cache policy, decided here because the footer metadata now tells us
        // how much of the file this query actually fetches:
        //   1. serve a whole file already cached on disk;
        //   2. otherwise cache + read the whole file when the read is cheap (small
        //      file) or broad (projection touches a large share);
        //   3. otherwise range-read the projected chunks and do not cache.
        if let Some(dc) = self.disk_cache.filter(|dc| dc.cache.budget_bytes() > 0) {
            let local = dc.local_path(path, file_size);
            if let Some(bytes) = read_whole_local(&local, file_size).await {
                tracing::debug!(path, file_size, "Iceberg disk-cache hit (whole file)");
                return Ok(bytes);
            }
            let projected_bytes: u64 = column_ranges.iter().map(|(s, e)| e - s).sum();
            if admit_whole_file(file_size, projected_bytes) {
                tracing::debug!(
                    path,
                    file_size,
                    projected_bytes,
                    "Iceberg disk-cache fill (whole file)"
                );
                // Single-flight: concurrent queries touching the same file share
                // one S3 GET + one cache write instead of each fetching the whole
                // file. `coalesced_fetch` writes the bytes to `local` on success.
                let data = dc
                    .cache
                    .coalesced_fetch(local, || async {
                        self.storage
                            .read(path)
                            .await
                            .map(|b| b.to_vec())
                            .map_err(|e| std::io::Error::other(e.to_string()))
                    })
                    .await
                    .map_err(|e| IcebergError::Storage(format!("disk-cache fill: {e}")))?;
                return Ok(Bytes::from(data));
            }
        } else if file_size < MIN_SPARSE_FILE_BYTES {
            // No disk cache: keep the small-file correctness behavior (read whole
            // to avoid a sparse buffer missing chunks the reader needs).
            tracing::debug!(path, file_size, "Reading entire small Parquet file");
            return self.storage.read(path).await;
        }

        // Rule 3 / sparse path: range-read the projected column chunks + footer.
        // Calculate footer range
        let footer_and_size = self
            .storage
            .read_range(path, (file_size - 8)..file_size)
            .await?;
        let footer_len = u32::from_le_bytes([
            footer_and_size[0],
            footer_and_size[1],
            footer_and_size[2],
            footer_and_size[3],
        ]) as u64;
        let footer_start = file_size.saturating_sub(8 + footer_len);

        // Collect all ranges: column chunks + footer
        let mut all_ranges: Vec<(u64, u64)> = column_ranges;
        all_ranges.push((footer_start, file_size));

        // Coalesce nearby ranges
        let coalesced = coalesce_ranges(&mut all_ranges, 64 * 1024);

        // Calculate total bytes to fetch
        let total_fetch: u64 = coalesced.iter().map(|(s, e)| e - s).sum();

        tracing::debug!(
            file_size,
            num_ranges = coalesced.len(),
            total_fetch_bytes = total_fetch,
            savings_pct = ((file_size - total_fetch) * 100 / file_size.max(1)),
            "Range-reading Parquet file"
        );

        // Fetch all ranges
        let mut range_data: Vec<(u64, Bytes)> = Vec::with_capacity(coalesced.len());
        for (start, end) in &coalesced {
            let data = self.storage.read_range(path, *start..*end).await?;
            range_data.push((*start, data));
        }

        // Assemble into sparse buffer
        let sparse_buffer = assemble_sparse_buffer(file_size as usize, range_data);

        Ok(Bytes::from(sparse_buffer))
    }
}

/// Coalesce byte ranges that are within `gap_threshold` of each other.
fn coalesce_ranges(ranges: &mut [(u64, u64)], gap_threshold: u64) -> Vec<(u64, u64)> {
    if ranges.is_empty() {
        return Vec::new();
    }

    ranges.sort_by_key(|(start, _)| *start);

    let mut coalesced = Vec::new();
    let mut current_start = ranges[0].0;
    let mut current_end = ranges[0].1;

    for &(start, end) in ranges.iter().skip(1) {
        if start <= current_end + gap_threshold {
            current_end = current_end.max(end);
        } else {
            coalesced.push((current_start, current_end));
            current_start = start;
            current_end = end;
        }
    }
    coalesced.push((current_start, current_end));

    coalesced
}

/// Assemble fetched ranges into a sparse buffer at correct file offsets.
fn assemble_sparse_buffer(file_size: usize, ranges: Vec<(u64, Bytes)>) -> Vec<u8> {
    let mut buffer = vec![0u8; file_size];

    for (offset, data) in ranges {
        let start = offset as usize;
        let end = start + data.len();
        if end <= file_size {
            buffer[start..end].copy_from_slice(&data);
        }
    }

    buffer
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    /// In-memory source store standing in for S3, used to prove the local-cache
    /// fallback reads from source when the cache file is gone.
    #[derive(Debug, Clone)]
    struct InMemorySource {
        bytes: Bytes,
    }

    #[async_trait]
    impl SendIcebergStorage for InMemorySource {
        async fn read(&self, _path: &str) -> Result<Bytes> {
            Ok(self.bytes.clone())
        }

        async fn read_range(&self, _path: &str, range: Range<u64>) -> Result<Bytes> {
            let start = (range.start as usize).min(self.bytes.len());
            let end = (range.end as usize).min(self.bytes.len());
            Ok(self.bytes.slice(start..end))
        }

        async fn file_size(&self, _path: &str) -> Result<u64> {
            Ok(self.bytes.len() as u64)
        }
    }

    #[test]
    fn admit_caches_cheap_small_files_for_any_projection() {
        // Rule 2a: a small file is cached whole even for a 1-byte projection.
        assert!(admit_whole_file(8 * MIB, 1));
        assert!(admit_whole_file(WHOLE_FILE_MAX_BYTES, 0));
    }

    #[test]
    fn admit_caches_large_files_only_when_broad() {
        let big = 100 * MIB;
        // Narrow projection of a large file: keep range-reading, do not cache.
        assert!(!admit_whole_file(big, 10 * MIB));
        // Broad projection (>= 50% by bytes): cache the whole file.
        assert!(admit_whole_file(big, 50 * MIB));
        assert!(admit_whole_file(big, 90 * MIB));
    }

    #[test]
    fn admit_share_boundary_is_inclusive() {
        let n = 64 * MIB;
        let at_threshold = n * WHOLE_FILE_MIN_SHARE_PCT / 100;
        assert!(admit_whole_file(n, at_threshold));
        assert!(!admit_whole_file(n, at_threshold - 1));
    }

    fn fresh_cache_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("fluree_iceberg_cache_test_{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn local_path_keys_on_size() {
        let dir = fresh_cache_dir("key");
        let cache = DiskArtifactCache::for_dir(&dir);
        let dc = DiskCacheRef {
            cache: &cache,
            dir: &dir,
        };
        // The same source path at a different size maps to a different cache
        // file, so a hit can never serve bytes of the wrong length.
        assert_ne!(
            dc.local_path("s3://b/f.parquet", 100),
            dc.local_path("s3://b/f.parquet", 200)
        );
    }

    #[test]
    fn valid_local_requires_a_matching_size_on_disk() {
        let dir = fresh_cache_dir("valid");
        let cache = DiskArtifactCache::for_dir(&dir);
        let dc = DiskCacheRef {
            cache: &cache,
            dir: &dir,
        };
        let src = "s3://b/f.parquet";
        assert!(dc.valid_local(src, 4).is_none(), "absent file is not valid");
        std::fs::write(dc.local_path(src, 4), b"abcd").unwrap();
        assert!(dc.valid_local(src, 4).is_some(), "exact size is a hit");
        assert!(
            dc.valid_local(src, 5).is_none(),
            "a different expected size names a different (absent) path"
        );
    }

    #[tokio::test]
    async fn read_whole_local_rejects_a_wrong_size_file() {
        let dir = fresh_cache_dir("read");
        let path = dir.join("blob.parquet");
        std::fs::write(&path, b"abcd").unwrap();
        assert!(read_whole_local(&path, 4).await.is_some());
        assert!(read_whole_local(&path, 5).await.is_none());
        assert!(read_whole_local(&dir.join("missing"), 4).await.is_none());
    }

    #[tokio::test]
    async fn local_file_storage_serves_positioned_ranges() {
        let dir = fresh_cache_dir("localfs");
        let path = dir.join("blob");
        std::fs::write(&path, b"0123456789").unwrap();
        let source = Arc::new(InMemorySource {
            bytes: Bytes::from_static(b"SOURCE----"),
        });
        let storage = LocalFileStorage {
            path,
            size: 10,
            source,
        };
        // While the cache file exists, reads target it (never the source).
        assert_eq!(storage.file_size("ignored").await.unwrap(), 10);
        assert_eq!(
            &storage.read_range("ignored", 2..5).await.unwrap()[..],
            b"234"
        );
        assert_eq!(
            &storage.read_range("ignored", 7..10).await.unwrap()[..],
            b"789"
        );
        assert_eq!(&storage.read("ignored").await.unwrap()[..], b"0123456789");
    }

    #[tokio::test]
    async fn local_file_storage_falls_back_to_source_when_evicted() {
        // Simulate a concurrent cache eviction: the local file is removed after
        // validation but before the deferred range reads run. Reads must fall
        // back to the source store rather than failing the query.
        let dir = fresh_cache_dir("localfs_evict");
        let path = dir.join("blob");
        std::fs::write(&path, b"0123456789").unwrap();
        let source = Arc::new(InMemorySource {
            bytes: Bytes::from_static(b"SOURCEDATA"),
        });
        let storage = LocalFileStorage {
            path: path.clone(),
            size: 10,
            source,
        };
        // Evict the cache file mid-read.
        std::fs::remove_file(&path).unwrap();
        // Range and whole-file reads now resolve from source, not an error.
        assert_eq!(
            &storage.read_range("ignored", 2..5).await.unwrap()[..],
            b"URC"
        );
        assert_eq!(&storage.read("ignored").await.unwrap()[..], b"SOURCEDATA");
    }

    /// A multi-type, multi-row-group Parquet file with nulls. Non-Iceberg (no
    /// field-id metadata), so `build_batch_schema` assigns field_id = column
    /// index: id=0, name=1, age=2, active=3, bday=4.
    fn multitype_parquet() -> Bytes {
        use parquet::data_type::{BoolType, ByteArray, ByteArrayType, Int32Type, Int64Type};
        use parquet::file::properties::WriterProperties;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::parser::parse_message_type;

        let message = "
            message schema {
              OPTIONAL INT64 id;
              OPTIONAL BYTE_ARRAY name (UTF8);
              OPTIONAL INT32 age;
              OPTIONAL BOOLEAN active;
              OPTIONAL INT32 bday (DATE);
            }";
        let schema = Arc::new(parse_message_type(message).unwrap());
        let props = Arc::new(WriterProperties::builder().build());
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = SerializedFileWriter::new(&mut buf, schema, props).unwrap();

            // Row group 1: two rows; name is null in row 2.
            {
                let mut rg = writer.next_row_group().unwrap();
                let mut c = rg.next_column().unwrap().unwrap();
                c.typed::<Int64Type>()
                    .write_batch(&[1, 2], Some(&[1, 1]), None)
                    .unwrap();
                c.close().unwrap();
                let mut c = rg.next_column().unwrap().unwrap();
                c.typed::<ByteArrayType>()
                    .write_batch(&[ByteArray::from("alice")], Some(&[1, 0]), None)
                    .unwrap();
                c.close().unwrap();
                let mut c = rg.next_column().unwrap().unwrap();
                c.typed::<Int32Type>()
                    .write_batch(&[10, 20], Some(&[1, 1]), None)
                    .unwrap();
                c.close().unwrap();
                let mut c = rg.next_column().unwrap().unwrap();
                c.typed::<BoolType>()
                    .write_batch(&[true, false], Some(&[1, 1]), None)
                    .unwrap();
                c.close().unwrap();
                let mut c = rg.next_column().unwrap().unwrap();
                c.typed::<Int32Type>()
                    .write_batch(&[100, 200], Some(&[1, 1]), None)
                    .unwrap();
                c.close().unwrap();
                rg.close().unwrap();
            }

            // Row group 2: one row; age is null.
            {
                let mut rg = writer.next_row_group().unwrap();
                let mut c = rg.next_column().unwrap().unwrap();
                c.typed::<Int64Type>()
                    .write_batch(&[3], Some(&[1]), None)
                    .unwrap();
                c.close().unwrap();
                let mut c = rg.next_column().unwrap().unwrap();
                c.typed::<ByteArrayType>()
                    .write_batch(&[ByteArray::from("carol")], Some(&[1]), None)
                    .unwrap();
                c.close().unwrap();
                let mut c = rg.next_column().unwrap().unwrap();
                c.typed::<Int32Type>()
                    .write_batch(&[], Some(&[0]), None)
                    .unwrap();
                c.close().unwrap();
                let mut c = rg.next_column().unwrap().unwrap();
                c.typed::<BoolType>()
                    .write_batch(&[true], Some(&[1]), None)
                    .unwrap();
                c.close().unwrap();
                let mut c = rg.next_column().unwrap().unwrap();
                c.typed::<Int32Type>()
                    .write_batch(&[300], Some(&[1]), None)
                    .unwrap();
                c.close().unwrap();
                rg.close().unwrap();
            }

            writer.close().unwrap();
        }
        Bytes::from(buf)
    }

    /// Flatten a decoded column across all emitted batches into a single
    /// row-ordered vector (batch boundaries are not asserted).
    fn flatten<'a, T: Clone + 'a>(
        batches: &'a [ColumnBatch],
        field_id: i32,
        extract: impl Fn(&'a crate::io::batch::Column) -> &'a [Option<T>],
    ) -> Vec<Option<T>> {
        let mut out = Vec::new();
        for b in batches {
            let col = b
                .column_by_id(field_id)
                .unwrap_or_else(|| panic!("missing field {field_id}"));
            out.extend(extract(col).iter().cloned());
        }
        out
    }

    /// End-to-end decode round-trip: asserts the exact decoded values across all
    /// supported types, nulls, and multiple row groups.
    #[tokio::test]
    async fn read_task_decodes_all_types_with_nulls_across_row_groups() {
        use crate::io::batch::Column;
        use crate::scan::planner::FileScanTask;

        let bytes = multitype_parquet();
        let source = InMemorySource {
            bytes: bytes.clone(),
        };
        let mut data_file = crate::manifest::DataFile {
            file_path: "mem://multitype.parquet".to_string(),
            file_format: crate::manifest::FileFormat::Parquet,
            record_count: 3,
            file_size_in_bytes: bytes.len() as i64,
            partition: crate::manifest::PartitionData::default(),
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            split_offsets: None,
            sort_order_id: None,
        };
        data_file.file_size_in_bytes = bytes.len() as i64;

        // Empty projection => all columns.
        let task = FileScanTask::for_whole_file(data_file, vec![], None);

        let reader = SendParquetReader::new(&source);
        let batches = reader.read_task(&task).await.expect("decode");

        let total: usize = batches.iter().map(|b| b.num_rows).sum();
        assert_eq!(total, 3, "expected 3 rows across all batches");

        let ids = flatten(&batches, 0, |c| match c {
            Column::Int64(v) => v.as_slice(),
            _ => panic!("id not Int64"),
        });
        assert_eq!(ids, vec![Some(1), Some(2), Some(3)]);

        let names = flatten(&batches, 1, |c| match c {
            Column::String(v) => v.as_slice(),
            _ => panic!("name not String"),
        });
        assert_eq!(
            names,
            vec![Some("alice".to_string()), None, Some("carol".to_string())]
        );

        let ages = flatten(&batches, 2, |c| match c {
            Column::Int32(v) => v.as_slice(),
            _ => panic!("age not Int32"),
        });
        assert_eq!(ages, vec![Some(10), Some(20), None]);

        let active = flatten(&batches, 3, |c| match c {
            Column::Boolean(v) => v.as_slice(),
            _ => panic!("active not Boolean"),
        });
        assert_eq!(active, vec![Some(true), Some(false), Some(true)]);

        // INT32 + (DATE) logical annotation decodes as Date (days since epoch).
        let bday = flatten(&batches, 4, |c| match c {
            Column::Date(v) => v.as_slice(),
            _ => panic!("bday not Date"),
        });
        assert_eq!(bday, vec![Some(100), Some(200), Some(300)]);
    }

    /// A whole-file scan task for an in-memory Parquet blob (empty projection =>
    /// all columns), with an optional residual pushdown predicate and optional
    /// Iceberg schema (needed for field_id → column resolution when the fixture
    /// carries no embedded Parquet field IDs).
    fn whole_file_task(
        bytes: &Bytes,
        residual: Option<Expression>,
        schema: Option<Arc<Schema>>,
    ) -> FileScanTask {
        let data_file = crate::manifest::DataFile {
            file_path: "mem://fixture.parquet".to_string(),
            file_format: crate::manifest::FileFormat::Parquet,
            record_count: 0,
            file_size_in_bytes: bytes.len() as i64,
            partition: crate::manifest::PartitionData::default(),
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            split_offsets: None,
            sort_order_id: None,
        };
        match schema {
            Some(s) => FileScanTask::for_whole_file_with_schema(data_file, vec![], residual, s),
            None => FileScanTask::for_whole_file(data_file, vec![], residual),
        }
    }

    /// Build a minimal Iceberg schema from `(name, type)` fields, ids assigned by
    /// position (matching the fixtures' column order).
    fn schema_of(fields: &[(&str, &str)]) -> Arc<Schema> {
        use crate::metadata::SchemaField;
        Arc::new(Schema {
            schema_id: 0,
            identifier_field_ids: vec![],
            fields: fields
                .iter()
                .enumerate()
                .map(|(i, (name, ty))| SchemaField {
                    id: i as i32,
                    name: name.to_string(),
                    required: false,
                    field_type: serde_json::json!(ty),
                    doc: None,
                })
                .collect(),
        })
    }

    /// A single-row-group Parquet with an `xsd:integer`-style column physically
    /// stored as `DECIMAL(9,0)` (INT32-backed) — how Iceberg materializes small
    /// integers. Columns: id=0, year=1. Rows: (1,2020) (2,2024) (3,null).
    fn decimal_backed_int_parquet() -> Bytes {
        use parquet::data_type::{Int32Type, Int64Type};
        use parquet::file::properties::WriterProperties;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::parser::parse_message_type;

        let message = "
            message schema {
              OPTIONAL INT64 id;
              OPTIONAL INT32 year (DECIMAL(9,0));
            }";
        let schema = Arc::new(parse_message_type(message).unwrap());
        let props = Arc::new(WriterProperties::builder().build());
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = SerializedFileWriter::new(&mut buf, schema, props).unwrap();
            let mut rg = writer.next_row_group().unwrap();
            let mut c = rg.next_column().unwrap().unwrap();
            c.typed::<Int64Type>()
                .write_batch(&[1, 2, 3], Some(&[1, 1, 1]), None)
                .unwrap();
            c.close().unwrap();
            let mut c = rg.next_column().unwrap().unwrap();
            c.typed::<Int32Type>()
                .write_batch(&[2020, 2024], Some(&[1, 1, 0]), None)
                .unwrap();
            c.close().unwrap();
            rg.close().unwrap();
            writer.close().unwrap();
        }
        Bytes::from(buf)
    }

    /// Arrow row filter drops rows that fail the predicate and rows whose filter
    /// column is null (an R2RML null column produces no triple). Arrow-only: the
    /// (Row-group pruning alone would not drop individual rows within a group.)
    #[tokio::test]
    async fn read_task_row_filter_drops_nonmatching_and_null_rows() {
        use crate::io::batch::Column;

        let bytes = multitype_parquet();
        let source = InMemorySource {
            bytes: bytes.clone(),
        };
        // age (field_id 2) >= 20
        let residual = Expression::Comparison {
            field_id: 2,
            column: "age".to_string(),
            op: crate::scan::predicate::ComparisonOp::GtEq,
            value: crate::scan::predicate::LiteralValue::Int64(20),
        };
        let schema = schema_of(&[
            ("id", "long"),
            ("name", "string"),
            ("age", "int"),
            ("active", "boolean"),
            ("bday", "date"),
        ]);
        let task = whole_file_task(&bytes, Some(residual), Some(schema));
        let batches = SendParquetReader::new(&source)
            .read_task(&task)
            .await
            .expect("decode");

        let ids = flatten(&batches, 0, |c| match c {
            Column::Int64(v) => v.as_slice(),
            _ => panic!("id not Int64"),
        });
        // age 10 dropped, age 20 kept, age null dropped.
        assert_eq!(ids, vec![Some(2)]);
    }

    /// String-equality row filter: `name = "alice"` keeps only the matching row
    /// (and drops the null-name row). Exercises the Arrow string comparison path.
    #[tokio::test]
    async fn read_task_row_filter_string_equality() {
        use crate::io::batch::Column;

        let bytes = multitype_parquet();
        let source = InMemorySource {
            bytes: bytes.clone(),
        };
        // name (field_id 1) = "alice"
        let residual = Expression::Comparison {
            field_id: 1,
            column: "name".to_string(),
            op: crate::scan::predicate::ComparisonOp::Eq,
            value: crate::scan::predicate::LiteralValue::String("alice".to_string()),
        };
        let schema = schema_of(&[
            ("id", "long"),
            ("name", "string"),
            ("age", "int"),
            ("active", "boolean"),
            ("bday", "date"),
        ]);
        let task = whole_file_task(&bytes, Some(residual), Some(schema));
        let batches = SendParquetReader::new(&source)
            .read_task(&task)
            .await
            .expect("decode");

        let ids = flatten(&batches, 0, |c| match c {
            Column::Int64(v) => v.as_slice(),
            _ => panic!("id not Int64"),
        });
        let names = flatten(&batches, 1, |c| match c {
            Column::String(v) => v.as_slice(),
            _ => panic!("name not String"),
        });
        assert_eq!(ids, vec![Some(1)], "only alice's row survives");
        assert_eq!(names, vec![Some("alice".to_string())]);
    }

    /// The Decimal landmine: an `xsd:integer` column is physically `DECIMAL`, so
    /// an `Int64` literal must be cast to the column's decimal type before
    /// comparison. A naive raw compare dropped every row; this asserts the one
    /// matching row survives. Arrow-only.
    #[tokio::test]
    async fn read_task_row_filter_handles_decimal_backed_integer() {
        use crate::io::batch::Column;

        let bytes = decimal_backed_int_parquet();
        let source = InMemorySource {
            bytes: bytes.clone(),
        };
        // year (field_id 1) >= 2024, literal is a plain integer.
        let residual = Expression::Comparison {
            field_id: 1,
            column: "year".to_string(),
            op: crate::scan::predicate::ComparisonOp::GtEq,
            value: crate::scan::predicate::LiteralValue::Int64(2024),
        };
        let schema = schema_of(&[("id", "long"), ("year", "decimal(9, 0)")]);
        let task = whole_file_task(&bytes, Some(residual), Some(schema));
        let batches = SendParquetReader::new(&source)
            .read_task(&task)
            .await
            .expect("decode");

        let ids = flatten(&batches, 0, |c| match c {
            Column::Int64(v) => v.as_slice(),
            _ => panic!("id not Int64"),
        });
        // year 2020 dropped, year 2024 kept, year null dropped.
        assert_eq!(ids, vec![Some(2)], "decimal-backed integer filter mismatch");
    }

    /// Bounded first-row-group "peek": `read_task_sample` returns at most `n`
    /// rows, reads only the FIRST row group, and honors projection — exercising
    /// the range-backed reader path end to end over the 2-row-group fixture
    /// (row group 0 = 2 rows, row group 1 = 1 row).
    #[tokio::test(flavor = "multi_thread")]
    async fn read_task_sample_bounds_rows_to_first_row_group() {
        use crate::io::batch::Column;

        let bytes = multitype_parquet();
        let source = InMemorySource {
            bytes: bytes.clone(),
        };
        let reader = SendParquetReader::new(&source);

        // Whole-file task (empty projection => all columns).
        let task = whole_file_task(&bytes, None, None);

        // n = 0 => no rows, no read.
        let none = reader.read_task_sample(&task, 0).await.expect("sample 0");
        assert_eq!(none.iter().map(|b| b.num_rows).sum::<usize>(), 0);

        // n = 1 => exactly the first row of row group 0.
        let one = reader.read_task_sample(&task, 1).await.expect("sample 1");
        let ids = flatten(&one, 0, |c| match c {
            Column::Int64(v) => v.as_slice(),
            _ => panic!("id not Int64"),
        });
        assert_eq!(ids, vec![Some(1)]);

        // n = 2 => all of row group 0.
        let two = reader.read_task_sample(&task, 2).await.expect("sample 2");
        assert_eq!(two.iter().map(|b| b.num_rows).sum::<usize>(), 2);

        // n = 10 => still only row group 0's 2 rows (row group 1 is never read),
        // proving the peek does not scan the whole file.
        let ten = reader.read_task_sample(&task, 10).await.expect("sample 10");
        let ids = flatten(&ten, 0, |c| match c {
            Column::Int64(v) => v.as_slice(),
            _ => panic!("id not Int64"),
        });
        assert_eq!(
            ids,
            vec![Some(1), Some(2)],
            "only the first row group is read"
        );

        // Projection: sampling a single column yields only that column.
        let projected = FileScanTask::for_whole_file(task.data_file.clone(), vec![0], None);
        let sampled = reader
            .read_task_sample(&projected, 1)
            .await
            .expect("sample projected");
        assert!(
            sampled[0].column_by_id(0).is_some(),
            "projected id column present"
        );
        assert!(
            sampled[0].column_by_id(1).is_none(),
            "name column not projected"
        );
    }
}
