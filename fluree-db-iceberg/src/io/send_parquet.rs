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

use std::sync::Arc;

use bytes::Bytes;
use tokio::runtime::Handle;

use crate::error::{IcebergError, Result};
use crate::io::batch::ColumnBatch;
use crate::io::chunk_reader::RangeBackedChunkReader;
use crate::io::parquet::{
    build_batch_schema, build_batch_schema_with_iceberg, build_columns_from_values,
    build_projected_schema, calculate_column_chunk_ranges, convert_field_to_column_value,
    parse_parquet_metadata_from_bytes, ColumnValue, ParquetFooterCache, NULL_COLUMN_SENTINEL,
};
use crate::io::SendIcebergStorage;
use crate::scan::FileScanTask;

use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;

/// Parquet magic bytes (footer ends with "PAR1").
const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];

/// Maximum file size for sparse buffer allocation (64MB).
///
/// Files smaller than this use sparse buffer approach (efficient range reads
/// assembled into a single buffer). Files larger than this use on-demand
/// range reads via `RangeBackedChunkReader` to avoid excessive memory allocation.
const MAX_SPARSE_BUFFER_SIZE: u64 = 64 * 1024 * 1024;

/// Send-safe Parquet reader with range-read support.
///
/// This is identical to `ParquetReader` but uses `SendIcebergStorage` instead of
/// `IcebergStorage`, producing `Send` futures for use with tokio::spawn and
/// async_trait without ?Send.
pub struct SendParquetReader<'a, S: SendIcebergStorage> {
    storage: &'a S,
    footer_cache: Option<&'a ParquetFooterCache>,
}

impl<'a, S: SendIcebergStorage> SendParquetReader<'a, S> {
    /// Create a new Send-safe Parquet reader.
    pub fn new(storage: &'a S) -> Self {
        Self {
            storage,
            footer_cache: None,
        }
    }

    /// Create a reader with footer caching.
    pub fn with_cache(storage: &'a S, cache: &'a ParquetFooterCache) -> Self {
        Self {
            storage,
            footer_cache: Some(cache),
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
    /// Uses parquet-rs's row iterator API for reliable decoding.
    /// Optimizations:
    /// - Projection pushdown: Only decodes projected columns
    /// - O(1) field lookup: Uses iterator position instead of name lookup
    /// - Per-row-group batches: Emits one batch per row group for streaming
    ///
    /// For files larger than 64MB, uses `RangeBackedChunkReader` for on-demand
    /// range reads instead of loading the entire file into memory.
    pub async fn read_task(&self, task: &FileScanTask) -> Result<Vec<ColumnBatch>>
    where
        S: Clone + 'static,
    {
        let file_size = task.data_file.file_size_in_bytes as u64;

        // For large files, use range-backed chunk reader
        if file_size > MAX_SPARSE_BUFFER_SIZE {
            return self.read_task_large_file(task).await;
        }

        self.read_task_small_file(task).await
    }

    /// Read a small file using sparse buffer approach.
    async fn read_task_small_file(&self, task: &FileScanTask) -> Result<Vec<ColumnBatch>> {
        use parquet::record::reader::RowIter;

        let path = &task.data_file.file_path;
        let metadata = self.read_metadata(path).await?;

        // Resolve the exact Parquet column indices first so sparse-range reads
        // and row-iterator projection stay in lock-step.
        let (batch_schema, column_indices) = if let Some(ref iceberg_schema) = task.iceberg_schema {
            build_batch_schema_with_iceberg(&metadata, iceberg_schema, &task.projected_field_ids)?
        } else {
            build_batch_schema(&metadata, &task.projected_field_ids)?
        };
        let batch_schema = Arc::new(batch_schema);

        let real_column_indices: Vec<usize> = column_indices
            .iter()
            .copied()
            .filter(|&idx| idx != NULL_COLUMN_SENTINEL)
            .collect();

        // Read the file bytes via range reads for the needed column chunks
        let file_bytes = self
            .read_file_for_task(path, task, &real_column_indices, &metadata)
            .await?;

        // Parse using parquet-rs
        let reader = SerializedFileReader::new(file_bytes)
            .map_err(|e| IcebergError::Storage(format!("Failed to read Parquet file: {e}")))?;

        let metadata = reader.metadata();

        // Build mapping from batch position to row position (or None for NULL columns)
        let batch_to_row_mapping: Vec<Option<usize>> = column_indices
            .iter()
            .scan(0usize, |row_idx, &col_idx| {
                if col_idx == NULL_COLUMN_SENTINEL {
                    Some(None) // NULL column - no row data
                } else {
                    let current = *row_idx;
                    *row_idx += 1;
                    Some(Some(current)) // Real column - maps to this row position
                }
            })
            .collect();

        // Build a projected schema for parquet-rs to only decode needed columns
        let projected_schema =
            build_projected_schema(metadata.file_metadata().schema(), &real_column_indices)?;

        let mut batches = Vec::new();

        // Process each row group separately to emit streaming batches
        for rg_idx in 0..metadata.num_row_groups() {
            let row_group_reader = reader.get_row_group(rg_idx).map_err(|e| {
                IcebergError::Storage(format!("Failed to get row group {rg_idx}: {e}"))
            })?;

            // Create row iterator for this row group with projection
            let row_iter =
                RowIter::from_row_group(Some(projected_schema.clone()), row_group_reader.as_ref())
                    .map_err(|e| {
                        IcebergError::Storage(format!(
                            "Failed to create row iterator for row group {rg_idx}: {e}"
                        ))
                    })?;

            // Collect rows into columnar format
            let num_fields = batch_schema.fields.len();
            let estimated_rows = metadata.row_group(rg_idx).num_rows() as usize;
            let mut column_data: Vec<Vec<Option<ColumnValue>>> = (0..num_fields)
                .map(|_| Vec::with_capacity(estimated_rows))
                .collect();

            for row_result in row_iter {
                let row = row_result
                    .map_err(|e| IcebergError::Storage(format!("Failed to read row: {e}")))?;

                // With projection, row columns come in the same order as projected schema.
                let row_fields: Vec<_> = row.get_column_iter().map(|(_, f)| f).collect();

                // Map row columns to batch positions, inserting NULLs for missing columns
                for (batch_idx, field_info) in batch_schema.fields.iter().enumerate() {
                    let value = match batch_to_row_mapping[batch_idx] {
                        Some(row_idx) => {
                            // Real column - get value from row
                            row_fields.get(row_idx).and_then(|field| {
                                convert_field_to_column_value(field, &field_info.field_type)
                            })
                        }
                        None => {
                            // NULL column (schema evolution) - always NULL
                            None
                        }
                    };
                    column_data[batch_idx].push(value);
                }
            }

            // Convert to Column format and create batch for this row group
            let columns = build_columns_from_values(column_data, &batch_schema)?;
            let batch = ColumnBatch::new(Arc::clone(&batch_schema), columns)?;

            if !batch.is_empty() {
                batches.push(batch);
            }
        }

        Ok(batches)
    }

    /// Read a large file using range-backed chunk reader.
    ///
    /// This method uses `RangeBackedChunkReader` to fetch byte ranges on-demand
    /// instead of loading the entire file into memory. The decoding runs in a
    /// blocking context via `spawn_blocking`.
    async fn read_task_large_file(&self, task: &FileScanTask) -> Result<Vec<ColumnBatch>>
    where
        S: Clone + 'static,
    {
        use parquet::record::reader::RowIter;

        let path = task.data_file.file_path.clone();
        let file_size = task.data_file.file_size_in_bytes as u64;
        let projected_field_ids = task.projected_field_ids.clone();
        let iceberg_schema = task.iceberg_schema.clone();

        // Clone storage for use in blocking task
        let storage = Arc::new(self.storage.clone());
        let runtime = Handle::current();

        tracing::info!(
            file_size,
            path = %path,
            "Using range-backed chunk reader for large file"
        );

        // Run the sync parquet decoding in a blocking context
        let result = tokio::task::spawn_blocking(move || {
            // Create range-backed chunk reader
            let chunk_reader =
                RangeBackedChunkReader::new(storage, path.clone(), file_size, runtime);

            // Parse using parquet-rs with our chunk reader
            let reader = SerializedFileReader::new(chunk_reader)
                .map_err(|e| IcebergError::Storage(format!("Failed to read Parquet file: {e}")))?;

            let metadata = reader.metadata();

            // Build schema for batch and get column indices for projected columns
            let (batch_schema, column_indices) = if let Some(ref iceberg_schema) = iceberg_schema {
                build_batch_schema_with_iceberg(metadata, iceberg_schema, &projected_field_ids)?
            } else {
                build_batch_schema(metadata, &projected_field_ids)?
            };
            let batch_schema = Arc::new(batch_schema);

            // Separate real columns from NULL columns (schema evolution)
            let real_column_indices: Vec<usize> = column_indices
                .iter()
                .copied()
                .filter(|&idx| idx != NULL_COLUMN_SENTINEL)
                .collect();

            // Build mapping from batch position to row position
            let batch_to_row_mapping: Vec<Option<usize>> = column_indices
                .iter()
                .scan(0usize, |row_idx, &col_idx| {
                    if col_idx == NULL_COLUMN_SENTINEL {
                        Some(None)
                    } else {
                        let current = *row_idx;
                        *row_idx += 1;
                        Some(Some(current))
                    }
                })
                .collect();

            // Build a projected schema for parquet-rs
            let projected_schema =
                build_projected_schema(metadata.file_metadata().schema(), &real_column_indices)?;

            let mut batches = Vec::new();

            // Process each row group
            for rg_idx in 0..metadata.num_row_groups() {
                let row_group_reader = reader.get_row_group(rg_idx).map_err(|e| {
                    IcebergError::Storage(format!("Failed to get row group {rg_idx}: {e}"))
                })?;

                let row_iter = RowIter::from_row_group(
                    Some(projected_schema.clone()),
                    row_group_reader.as_ref(),
                )
                .map_err(|e| {
                    IcebergError::Storage(format!("Failed to create row iterator: {e}"))
                })?;

                let num_fields = batch_schema.fields.len();
                let estimated_rows = metadata.row_group(rg_idx).num_rows() as usize;
                let mut column_data: Vec<Vec<Option<ColumnValue>>> = (0..num_fields)
                    .map(|_| Vec::with_capacity(estimated_rows))
                    .collect();

                for row_result in row_iter {
                    let row = row_result
                        .map_err(|e| IcebergError::Storage(format!("Failed to read row: {e}")))?;

                    let row_fields: Vec<_> = row.get_column_iter().map(|(_, f)| f).collect();

                    for (batch_idx, field_info) in batch_schema.fields.iter().enumerate() {
                        let value = match batch_to_row_mapping[batch_idx] {
                            Some(row_idx) => row_fields.get(row_idx).and_then(|field| {
                                convert_field_to_column_value(field, &field_info.field_type)
                            }),
                            None => None,
                        };
                        column_data[batch_idx].push(value);
                    }
                }

                let columns = build_columns_from_values(column_data, &batch_schema)?;
                let batch = ColumnBatch::new(Arc::clone(&batch_schema), columns)?;

                if !batch.is_empty() {
                    batches.push(batch);
                }
            }

            Ok::<Vec<ColumnBatch>, IcebergError>(batches)
        })
        .await
        .map_err(|e| IcebergError::Storage(format!("Blocking task failed: {e}")))?;

        result
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

        // For small files (< 1MB), read the entire file to avoid sparse buffer issues
        // where the row iterator may need column chunks not included in the projection.
        if file_size < 1_024 * 1_024 {
            tracing::debug!(path, file_size, "Reading entire small Parquet file");
            let data = self.storage.read(path).await?;
            return Ok(data);
        }

        // Calculate column chunk ranges using the exact resolved Parquet indices.
        let column_ranges = calculate_column_chunk_ranges(metadata, real_column_indices);

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
