//! Range-read Parquet file reader.
//!
//! This module provides efficient Parquet reading by:
//! 1. Range-reading the footer (last 8 bytes for length, then footer bytes)
//! 2. Parsing FileMetaData to get row group and column chunk info
//! 3. Range-reading only the column chunks needed for projection
//! 4. Handling dictionary pages (read from earliest offset)
//! 5. Decoding column data into `ColumnBatch` format
//!
//! # Footer Cache
//!
//! Parquet footers are small (~KB) and reused across scans. A simple LRU cache
//! is included to avoid repeated footer reads.

use std::sync::Arc;

use bytes::Bytes;
use lru::LruCache;
use parquet::basic::Type as PhysicalType;
use parquet::column::reader::ColumnReader;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::RowGroupReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::schema::types::Type as SchemaType;
use tokio::sync::Mutex;

use std::collections::HashMap;

use crate::error::{IcebergError, Result};
use crate::io::batch::{BatchSchema, Column, ColumnBatch, FieldInfo, FieldType};
use crate::io::IcebergStorage;
use crate::metadata::Schema;
use crate::scan::FileScanTask;

/// Parquet magic bytes (footer ends with "PAR1").
const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];

/// Maximum file size for sparse buffer allocation (64MB).
///
/// For files larger than this, the sparse buffer approach allocates significant
/// memory even when fetching only a small fraction of the file. Files exceeding
/// this threshold will fall back to reading the whole file.
///
/// # Lambda Compatibility Note
///
/// This threshold prevents OOM from sparse buffer allocation (e.g., allocating 1GB
/// of zeros for a large file when only fetching 10MB of columns). However, the
/// fallback to whole-file read does NOT make large files Lambda-safe—it just avoids
/// the *additional* memory cost of sparse allocation.
///
/// For true Lambda support with large Parquet files (>64MB), a **seekable
/// range-backed reader** is required. This would implement `Read + Seek` over
/// `IcebergStorage::read_range()`, allowing parquet-rs to seek to column chunks
/// on-demand without loading the entire file into memory.
///
/// Current behavior by file size:
/// - **≤64MB**: Sparse buffer (network-efficient, O(file_size) memory)
/// - **>64MB**: Whole file read (same memory as sparse, but simpler)
/// - **Very large**: May OOM on Lambda—requires seekable reader (TODO)
const MAX_SPARSE_BUFFER_SIZE: u64 = 64 * 1024 * 1024; // 64MB

/// Footer cache for Parquet files.
#[derive(Debug)]
pub struct ParquetFooterCache {
    cache: Mutex<LruCache<String, CachedFooter>>,
}

#[derive(Debug, Clone)]
struct CachedFooter {
    file_size: u64,
    metadata: Arc<ParquetMetaData>,
}

impl ParquetFooterCache {
    /// Create a new footer cache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(capacity)
                    .unwrap_or(std::num::NonZeroUsize::new(64).unwrap()),
            )),
        }
    }

    /// Get cached footer, or None if not cached or file size changed.
    pub async fn get(&self, path: &str, file_size: u64) -> Option<Arc<ParquetMetaData>> {
        let mut cache = self.cache.lock().await;
        if let Some(cached) = cache.get(path) {
            if cached.file_size == file_size {
                return Some(Arc::clone(&cached.metadata));
            }
        }
        None
    }

    /// Cache a footer.
    pub async fn put(&self, path: String, file_size: u64, metadata: Arc<ParquetMetaData>) {
        let mut cache = self.cache.lock().await;
        cache.put(
            path,
            CachedFooter {
                file_size,
                metadata,
            },
        );
    }

    /// Clear all cached Parquet footers.
    pub async fn clear(&self) {
        let mut cache = self.cache.lock().await;
        cache.clear();
    }
}

impl Default for ParquetFooterCache {
    fn default() -> Self {
        Self::new(64)
    }
}

/// Parquet reader with range-read support.
pub struct ParquetReader<'a, S: IcebergStorage> {
    storage: &'a S,
    footer_cache: Option<&'a ParquetFooterCache>,
}

impl<'a, S: IcebergStorage> ParquetReader<'a, S> {
    /// Create a new Parquet reader.
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

        // Read entire footer + magic (need enough context for parquet parser)
        let footer_start = file_size.saturating_sub(8 + footer_len);
        let footer_range = footer_start..file_size;
        let footer_bytes = self.storage.read_range(path, footer_range).await?;

        // Parse using parquet-rs by creating an in-memory reader
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
    pub async fn read_task(&self, task: &FileScanTask) -> Result<Vec<ColumnBatch>> {
        use parquet::file::reader::FileReader;
        use parquet::record::reader::RowIter;

        let path = &task.data_file.file_path;
        let metadata = self.read_metadata(path).await?;

        // Resolve the exact Parquet column indices first so sparse-range reads
        // and row-iterator projection use the same source of truth.
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

        // Read the file bytes
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

    /// Read file bytes needed for the task using range reads.
    ///
    /// This fetches only the footer and projected column chunks, assembling them
    /// into a sparse buffer at correct file offsets. Unread regions are filled
    /// with zeros (they won't be accessed by parquet-rs for projected columns).
    ///
    /// # Memory Considerations
    ///
    /// The sparse buffer approach allocates `file_size` bytes regardless of how
    /// much data is actually fetched. For files larger than `MAX_SPARSE_BUFFER_SIZE`,
    /// we fall back to reading the whole file to avoid excessive memory allocation
    /// in memory-constrained environments.
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
            return self.storage.read(path).await;
        }

        // For large files, fall back to reading the whole file to avoid
        // allocating a huge sparse buffer (O(file_size) memory)
        if file_size > MAX_SPARSE_BUFFER_SIZE {
            tracing::warn!(
                file_size,
                max_sparse_buffer = MAX_SPARSE_BUFFER_SIZE,
                "File exceeds sparse buffer threshold, reading whole file"
            );
            return self.storage.read(path).await;
        }

        // Calculate column chunk ranges using the exact resolved Parquet indices.
        let column_ranges = calculate_column_chunk_ranges(metadata, real_column_indices);

        // Calculate footer range (last 8 bytes + footer content)
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

        // Coalesce nearby ranges (within 64KB gap) to reduce HTTP requests
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

        // Fetch all ranges (could be parallelized with bounded concurrency)
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
/// This reduces the number of HTTP requests at the cost of fetching some extra bytes.
fn coalesce_ranges(ranges: &mut [(u64, u64)], gap_threshold: u64) -> Vec<(u64, u64)> {
    if ranges.is_empty() {
        return Vec::new();
    }

    // Sort by start offset
    ranges.sort_by_key(|(start, _)| *start);

    let mut coalesced = Vec::new();
    let mut current_start = ranges[0].0;
    let mut current_end = ranges[0].1;

    for &(start, end) in ranges.iter().skip(1) {
        if start <= current_end + gap_threshold {
            // Ranges overlap or are close enough - merge
            current_end = current_end.max(end);
        } else {
            // Gap too large - emit current range and start new one
            coalesced.push((current_start, current_end));
            current_start = start;
            current_end = end;
        }
    }
    coalesced.push((current_start, current_end));

    coalesced
}

/// Assemble fetched ranges into a sparse buffer at correct file offsets.
/// Unfetched regions are filled with zeros.
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

/// Calculate byte ranges for column chunks needed for projection.
///
/// IMPORTANT: callers must pass the exact resolved Parquet column indices that
/// will be included in the projected schema for `RowIter::from_row_group()`.
/// Re-deriving them from Iceberg field IDs can diverge for name-based fallback
/// or schema-evolution cases and produce sparse buffers with missing bytes.
pub(crate) fn calculate_column_chunk_ranges(
    metadata: &ParquetMetaData,
    projected_column_indices: &[usize],
) -> Vec<(u64, u64)> {
    let mut ranges = Vec::new();

    for rg_idx in 0..metadata.num_row_groups() {
        let row_group = metadata.row_group(rg_idx);

        for &col_idx in projected_column_indices {
            // Get the byte range for this column chunk
            let col_chunk = row_group.column(col_idx);

            // Handle dictionary page offset - use earliest offset
            // Dictionary pages (if present) come before data pages
            let dict_offset = col_chunk.dictionary_page_offset();
            let data_offset = col_chunk.data_page_offset();

            let start_offset = match dict_offset {
                Some(dict) => dict.min(data_offset) as u64,
                None => data_offset as u64,
            };

            // compressed_size is total for entire column chunk (dict + data pages)
            let compressed_size = col_chunk.compressed_size() as u64;
            let end_offset = start_offset + compressed_size;

            ranges.push((start_offset, end_offset));
        }
    }

    ranges
}

/// Intermediate column value for row-to-columnar conversion.
///
/// These values are collected row-by-row during Parquet iteration,
/// then assembled into typed `Column` vectors.
#[derive(Debug, Clone)]
pub enum ColumnValue {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Boolean(bool),
    String(String),
    Bytes(Vec<u8>),
    /// Date: days since 1970-01-01
    Date(i32),
    /// Timestamp: microseconds since epoch (local time, no timezone)
    Timestamp(i64),
    /// TimestampTz: microseconds since epoch (UTC)
    TimestampTz(i64),
    /// Decimal: unscaled i128 value (precision/scale from schema)
    Decimal(i128),
}

/// Convert a parquet Field to our ColumnValue.
///
/// Uses `field_type` to determine the correct output type, especially for:
/// - Dates: Always converted to days since epoch
/// - Timestamps: Converted to microseconds since epoch (normalizing from millis if needed)
/// - TimestampTz: Same as Timestamp but tagged for UTC interpretation
/// - Decimals: Converted to i128 unscaled value
pub fn convert_field_to_column_value(
    field: &parquet::record::Field,
    field_type: &FieldType,
) -> Option<ColumnValue> {
    use parquet::record::Field;

    match field {
        Field::Null => None,
        Field::Bool(v) => Some(ColumnValue::Boolean(*v)),
        Field::Byte(v) => Some(ColumnValue::Int32(*v as i32)),
        Field::Short(v) => Some(ColumnValue::Int32(*v as i32)),
        Field::Int(v) => {
            // Int could be a Date in Iceberg (days since epoch)
            match field_type {
                FieldType::Date => Some(ColumnValue::Date(*v)),
                _ => Some(ColumnValue::Int32(*v)),
            }
        }
        Field::Long(v) => {
            // Plain Long is always Int64. Timestamps come through as
            // Field::TimestampMillis or Field::TimestampMicros which have
            // explicit time unit information. Don't treat Long as timestamp
            // because we can't know if it's millis or micros.
            Some(ColumnValue::Int64(*v))
        }
        Field::UByte(v) => Some(ColumnValue::Int32(*v as i32)),
        Field::UShort(v) => Some(ColumnValue::Int32(*v as i32)),
        Field::UInt(v) => Some(ColumnValue::Int64(*v as i64)),
        Field::ULong(v) => Some(ColumnValue::Int64(*v as i64)),
        Field::Float(v) => Some(ColumnValue::Float32(*v)),
        Field::Double(v) => Some(ColumnValue::Float64(*v)),
        Field::Str(v) => Some(ColumnValue::String(v.clone())),
        Field::Bytes(v) => Some(ColumnValue::Bytes(v.data().to_vec())),
        Field::Date(v) => Some(ColumnValue::Date(*v)),
        Field::TimestampMillis(v) => {
            // Convert milliseconds to microseconds for consistent storage
            let micros = *v * 1000;
            match field_type {
                FieldType::TimestampTz => Some(ColumnValue::TimestampTz(micros)),
                _ => Some(ColumnValue::Timestamp(micros)),
            }
        }
        Field::TimestampMicros(v) => match field_type {
            FieldType::TimestampTz => Some(ColumnValue::TimestampTz(*v)),
            _ => Some(ColumnValue::Timestamp(*v)),
        },
        Field::Decimal(d) => {
            // Convert decimal bytes to i128 unscaled value
            let bytes = d.data();
            let unscaled = decimal_bytes_to_i128(bytes);
            Some(ColumnValue::Decimal(unscaled))
        }
        Field::Float16(v) => Some(ColumnValue::Float32(v.to_f32())),
        Field::Group(_) | Field::ListInternal(_) | Field::MapInternal(_) => {
            // Nested types not fully supported yet
            None
        }
    }
}

/// Convert big-endian decimal bytes to i128.
///
/// Parquet stores decimals as big-endian two's complement integers.
/// The byte array is sign-extended to 16 bytes for i128 conversion.
///
/// # Overflow Handling
///
/// If bytes.len() > 16, this takes only the last 16 bytes (least significant),
/// which will produce incorrect results for values that overflow i128. This is
/// defensive against malformed files; valid Parquet decimals (precision ≤ 38)
/// always fit in 16 bytes.
fn decimal_bytes_to_i128(bytes: &[u8]) -> i128 {
    if bytes.is_empty() {
        return 0;
    }

    // Handle overflow: if more than 16 bytes, take the last 16 (truncate high bytes)
    // This produces incorrect results but avoids panic on malformed data
    let bytes = if bytes.len() > 16 {
        tracing::warn!(
            byte_len = bytes.len(),
            "Decimal bytes exceed 16, truncating (value will be incorrect)"
        );
        &bytes[bytes.len() - 16..]
    } else {
        bytes
    };

    // Check sign bit of most significant byte
    let is_negative = (bytes[0] & 0x80) != 0;

    // Sign-extend to 16 bytes
    let fill_byte = if is_negative { 0xFF } else { 0x00 };
    let mut arr = [fill_byte; 16];

    // Copy bytes to the end (big-endian)
    let start = 16 - bytes.len();
    arr[start..].copy_from_slice(bytes);

    i128::from_be_bytes(arr)
}

/// Build Column vectors from row-collected values.
pub fn build_columns_from_values(
    column_data: Vec<Vec<Option<ColumnValue>>>,
    schema: &BatchSchema,
) -> Result<Vec<Column>> {
    let mut columns = Vec::with_capacity(schema.fields.len());

    for (col_idx, field) in schema.fields.iter().enumerate() {
        let values = &column_data[col_idx];
        let column = match field.field_type {
            FieldType::Boolean => {
                let data: Vec<Option<bool>> = values
                    .iter()
                    .map(|v| {
                        v.as_ref().and_then(|cv| match cv {
                            ColumnValue::Boolean(b) => Some(*b),
                            _ => None,
                        })
                    })
                    .collect();
                Column::Boolean(data)
            }
            FieldType::Int32 => {
                let data: Vec<Option<i32>> = values
                    .iter()
                    .map(|v| {
                        v.as_ref().and_then(|cv| match cv {
                            ColumnValue::Int32(i) => Some(*i),
                            ColumnValue::Int64(i) => Some(*i as i32),
                            _ => None,
                        })
                    })
                    .collect();
                Column::Int32(data)
            }
            FieldType::Int64 => {
                let data: Vec<Option<i64>> = values
                    .iter()
                    .map(|v| {
                        v.as_ref().and_then(|cv| match cv {
                            ColumnValue::Int64(i) => Some(*i),
                            ColumnValue::Int32(i) => Some(*i as i64),
                            _ => None,
                        })
                    })
                    .collect();
                Column::Int64(data)
            }
            FieldType::Float32 => {
                let data: Vec<Option<f32>> = values
                    .iter()
                    .map(|v| {
                        v.as_ref().and_then(|cv| match cv {
                            ColumnValue::Float32(f) => Some(*f),
                            ColumnValue::Float64(f) => Some(*f as f32),
                            _ => None,
                        })
                    })
                    .collect();
                Column::Float32(data)
            }
            FieldType::Float64 => {
                let data: Vec<Option<f64>> = values
                    .iter()
                    .map(|v| {
                        v.as_ref().and_then(|cv| match cv {
                            ColumnValue::Float64(f) => Some(*f),
                            ColumnValue::Float32(f) => Some(*f as f64),
                            _ => None,
                        })
                    })
                    .collect();
                Column::Float64(data)
            }
            FieldType::String => {
                let data: Vec<Option<String>> = values
                    .iter()
                    .map(|v| {
                        v.as_ref().and_then(|cv| match cv {
                            ColumnValue::String(s) => Some(s.clone()),
                            ColumnValue::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
                            _ => None,
                        })
                    })
                    .collect();
                Column::String(data)
            }
            FieldType::Bytes => {
                let data: Vec<Option<Vec<u8>>> = values
                    .iter()
                    .map(|v| {
                        v.as_ref().and_then(|cv| match cv {
                            ColumnValue::Bytes(b) => Some(b.clone()),
                            ColumnValue::String(s) => Some(s.as_bytes().to_vec()),
                            _ => None,
                        })
                    })
                    .collect();
                Column::Bytes(data)
            }
            FieldType::Date => {
                let data: Vec<Option<i32>> = values
                    .iter()
                    .map(|v| {
                        v.as_ref().and_then(|cv| match cv {
                            ColumnValue::Date(i) => Some(*i),
                            // Fallback for Int32 if source didn't properly tag as Date
                            ColumnValue::Int32(i) => Some(*i),
                            _ => None,
                        })
                    })
                    .collect();
                Column::Date(data)
            }
            FieldType::Timestamp => {
                let data: Vec<Option<i64>> = values
                    .iter()
                    .map(|v| {
                        v.as_ref().and_then(|cv| match cv {
                            ColumnValue::Timestamp(i) => Some(*i),
                            ColumnValue::TimestampTz(i) => Some(*i),
                            // Fallback for Int64 if source didn't properly tag
                            ColumnValue::Int64(i) => Some(*i),
                            ColumnValue::Int32(i) => Some(*i as i64),
                            _ => None,
                        })
                    })
                    .collect();
                Column::Timestamp(data)
            }
            FieldType::TimestampTz => {
                let data: Vec<Option<i64>> = values
                    .iter()
                    .map(|v| {
                        v.as_ref().and_then(|cv| match cv {
                            ColumnValue::TimestampTz(i) => Some(*i),
                            ColumnValue::Timestamp(i) => Some(*i),
                            // Fallback for Int64 if source didn't properly tag
                            ColumnValue::Int64(i) => Some(*i),
                            ColumnValue::Int32(i) => Some(*i as i64),
                            _ => None,
                        })
                    })
                    .collect();
                Column::TimestampTz(data)
            }
            FieldType::Decimal { precision, scale } => {
                let data: Vec<Option<i128>> = values
                    .iter()
                    .map(|v| {
                        v.as_ref().and_then(|cv| match cv {
                            ColumnValue::Decimal(i) => Some(*i),
                            // Fallback for Int32/Int64: Parquet can encode small-precision
                            // decimals as INT32/INT64 physical types. These are already
                            // unscaled values (e.g., decimal(5,2) value 123.45 stored as 12345).
                            ColumnValue::Int64(i) => Some(*i as i128),
                            ColumnValue::Int32(i) => Some(*i as i128),
                            _ => None,
                        })
                    })
                    .collect();
                Column::Decimal {
                    values: data,
                    precision,
                    scale,
                }
            }
        };
        columns.push(column);
    }

    Ok(columns)
}

/// Decode columns from a row group reader.
pub fn decode_row_group_columns(
    row_group_reader: &dyn RowGroupReader,
    schema: &BatchSchema,
    column_indices: &[usize],
    num_rows: usize,
) -> Result<Vec<Column>> {
    let mut columns = Vec::with_capacity(schema.fields.len());

    for (field_idx, &col_idx) in column_indices.iter().enumerate() {
        let field = &schema.fields[field_idx];
        let column = decode_column(row_group_reader, col_idx, field, num_rows)?;
        columns.push(column);
    }

    Ok(columns)
}

/// Decode a single column from a row group.
///
/// # Limitations
///
/// - **Nested/repeated columns not supported**: The decode functions allocate `rep_levels`
///   but ignore them. This works for flat, non-repeated columns only. Attempting to decode
///   LIST, MAP, or repeated fields will produce incorrect results.
/// - The schema resolution in `build_batch_schema` filters out nested types (groups),
///   so this should not be hit in practice for Phase 2.
fn decode_column(
    row_group_reader: &dyn RowGroupReader,
    col_idx: usize,
    field: &FieldInfo,
    num_rows: usize,
) -> Result<Column> {
    let col_reader = row_group_reader.get_column_reader(col_idx).map_err(|e| {
        IcebergError::Storage(format!(
            "Failed to get column reader for {}: {}",
            field.name, e
        ))
    })?;

    match col_reader {
        ColumnReader::BoolColumnReader(mut reader) => decode_bool_column(&mut reader, num_rows),
        ColumnReader::Int32ColumnReader(mut reader) => {
            decode_int32_column(&mut reader, field, num_rows)
        }
        ColumnReader::Int64ColumnReader(mut reader) => {
            decode_int64_column(&mut reader, field, num_rows)
        }
        ColumnReader::Int96ColumnReader(mut reader) => decode_int96_column(&mut reader, num_rows),
        ColumnReader::FloatColumnReader(mut reader) => decode_float_column(&mut reader, num_rows),
        ColumnReader::DoubleColumnReader(mut reader) => decode_double_column(&mut reader, num_rows),
        ColumnReader::ByteArrayColumnReader(mut reader) => {
            decode_byte_array_column(&mut reader, field, num_rows)
        }
        ColumnReader::FixedLenByteArrayColumnReader(mut reader) => {
            decode_fixed_len_byte_array_column(&mut reader, num_rows)
        }
    }
}

/// Helper to decode values with proper null handling based on definition levels.
///
/// Parquet definition levels work as follows:
/// - `max_def_level = 0`: Column is REQUIRED, all values present, def_levels all 0
/// - `max_def_level > 0`: Column is OPTIONAL, def_level == max_def_level means present
///
/// The key insight is that `values_read == records_read` means ALL values are present
/// (either required column or optional with no nulls), so we can skip def_level checks.
///
/// For the slow path with nulls: `def_levels[i] > 0` means value present for optional
/// columns (max_def_level = 1, which is the common case for flat schemas). For deeply
/// nested schemas this might need adjustment, but flat Iceberg columns work correctly.
fn decode_with_nulls<T, V, F>(
    records_read: usize,
    values_read: usize,
    def_levels: &[i16],
    values: &[V],
    convert: F,
) -> Vec<Option<T>>
where
    F: Fn(&V) -> T,
{
    let mut result = Vec::with_capacity(records_read);

    // Fast path: all values present (required column or optional with no nulls)
    // This correctly handles required columns where max_def_level = 0
    if values_read == records_read {
        for v in values.iter().take(records_read) {
            result.push(Some(convert(v)));
        }
        return result;
    }

    // Slow path: has nulls, check def_levels
    // For optional columns (max_def_level = 1), def_levels[i] > 0 means value present
    let mut value_idx = 0;
    for &def_level in def_levels.iter().take(records_read) {
        if def_level > 0 && value_idx < values_read {
            result.push(Some(convert(&values[value_idx])));
            value_idx += 1;
        } else {
            result.push(None);
        }
    }

    result
}

/// Decode boolean column.
fn decode_bool_column(
    reader: &mut parquet::column::reader::ColumnReaderImpl<parquet::data_type::BoolType>,
    num_rows: usize,
) -> Result<Column> {
    let mut values = vec![false; num_rows];
    let mut def_levels = vec![0i16; num_rows];
    let mut rep_levels = vec![0i16; num_rows];

    let (records_read, values_read, _) = reader
        .read_records(
            num_rows,
            Some(&mut def_levels),
            Some(&mut rep_levels),
            &mut values,
        )
        .map_err(|e| IcebergError::Storage(format!("Failed to read bool column: {e}")))?;

    let result = decode_with_nulls(records_read, values_read, &def_levels, &values, |v| *v);

    Ok(Column::Boolean(result))
}

/// Decode int32 column (may be int32 or date depending on field type).
fn decode_int32_column(
    reader: &mut parquet::column::reader::ColumnReaderImpl<parquet::data_type::Int32Type>,
    field: &FieldInfo,
    num_rows: usize,
) -> Result<Column> {
    let mut values = vec![0i32; num_rows];
    let mut def_levels = vec![0i16; num_rows];
    let mut rep_levels = vec![0i16; num_rows];

    let (records_read, values_read, _) = reader
        .read_records(
            num_rows,
            Some(&mut def_levels),
            Some(&mut rep_levels),
            &mut values,
        )
        .map_err(|e| IcebergError::Storage(format!("Failed to read int32 column: {e}")))?;

    let result = decode_with_nulls(records_read, values_read, &def_levels, &values, |v| *v);

    // Return appropriate column type based on field
    match field.field_type {
        FieldType::Date => Ok(Column::Date(result)),
        _ => Ok(Column::Int32(result)),
    }
}

/// Decode int64 column (may be int64 or timestamp depending on field type).
fn decode_int64_column(
    reader: &mut parquet::column::reader::ColumnReaderImpl<parquet::data_type::Int64Type>,
    field: &FieldInfo,
    num_rows: usize,
) -> Result<Column> {
    let mut values = vec![0i64; num_rows];
    let mut def_levels = vec![0i16; num_rows];
    let mut rep_levels = vec![0i16; num_rows];

    let (records_read, values_read, _) = reader
        .read_records(
            num_rows,
            Some(&mut def_levels),
            Some(&mut rep_levels),
            &mut values,
        )
        .map_err(|e| IcebergError::Storage(format!("Failed to read int64 column: {e}")))?;

    let result = decode_with_nulls(records_read, values_read, &def_levels, &values, |v| *v);

    // Return appropriate column type based on field
    match field.field_type {
        FieldType::Timestamp | FieldType::TimestampTz => Ok(Column::Timestamp(result)),
        _ => Ok(Column::Int64(result)),
    }
}

/// Decode int96 column (legacy timestamp format).
fn decode_int96_column(
    reader: &mut parquet::column::reader::ColumnReaderImpl<parquet::data_type::Int96Type>,
    num_rows: usize,
) -> Result<Column> {
    use parquet::data_type::Int96;

    let mut values = vec![Int96::new(); num_rows];
    let mut def_levels = vec![0i16; num_rows];
    let mut rep_levels = vec![0i16; num_rows];

    let (records_read, values_read, _) = reader
        .read_records(
            num_rows,
            Some(&mut def_levels),
            Some(&mut rep_levels),
            &mut values,
        )
        .map_err(|e| IcebergError::Storage(format!("Failed to read int96 column: {e}")))?;

    // Convert INT96 to microseconds since epoch
    let result = decode_with_nulls(records_read, values_read, &def_levels, &values, |int96| {
        int96_to_nanos(int96) / 1000
    });

    Ok(Column::Timestamp(result))
}

/// Convert INT96 to nanoseconds since Unix epoch.
fn int96_to_nanos(int96: &parquet::data_type::Int96) -> i64 {
    let data = int96.data();
    // INT96 format: first 8 bytes are nanoseconds within the day,
    // last 4 bytes are Julian day number
    let nanos_in_day = (data[0] as i64) | ((data[1] as i64) << 32);
    let julian_day = data[2] as i64;

    // Convert Julian day to Unix epoch (Julian day 2440588 = 1970-01-01)
    const JULIAN_UNIX_EPOCH: i64 = 2_440_588;
    const NANOS_PER_DAY: i64 = 86_400_000_000_000;

    let days_since_epoch = julian_day - JULIAN_UNIX_EPOCH;
    days_since_epoch * NANOS_PER_DAY + nanos_in_day
}

/// Decode float column.
fn decode_float_column(
    reader: &mut parquet::column::reader::ColumnReaderImpl<parquet::data_type::FloatType>,
    num_rows: usize,
) -> Result<Column> {
    let mut values = vec![0.0f32; num_rows];
    let mut def_levels = vec![0i16; num_rows];
    let mut rep_levels = vec![0i16; num_rows];

    let (records_read, values_read, _) = reader
        .read_records(
            num_rows,
            Some(&mut def_levels),
            Some(&mut rep_levels),
            &mut values,
        )
        .map_err(|e| IcebergError::Storage(format!("Failed to read float column: {e}")))?;

    let result = decode_with_nulls(records_read, values_read, &def_levels, &values, |v| *v);

    Ok(Column::Float32(result))
}

/// Decode double column.
fn decode_double_column(
    reader: &mut parquet::column::reader::ColumnReaderImpl<parquet::data_type::DoubleType>,
    num_rows: usize,
) -> Result<Column> {
    let mut values = vec![0.0f64; num_rows];
    let mut def_levels = vec![0i16; num_rows];
    let mut rep_levels = vec![0i16; num_rows];

    let (records_read, values_read, _) = reader
        .read_records(
            num_rows,
            Some(&mut def_levels),
            Some(&mut rep_levels),
            &mut values,
        )
        .map_err(|e| IcebergError::Storage(format!("Failed to read double column: {e}")))?;

    let result = decode_with_nulls(records_read, values_read, &def_levels, &values, |v| *v);

    Ok(Column::Float64(result))
}

/// Decode byte array column (may be string or bytes depending on field type).
///
/// Uses ByteArray::new() and only accesses values that were actually filled.
fn decode_byte_array_column(
    reader: &mut parquet::column::reader::ColumnReaderImpl<parquet::data_type::ByteArrayType>,
    field: &FieldInfo,
    num_rows: usize,
) -> Result<Column> {
    use parquet::data_type::ByteArray;

    let mut all_values: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut all_bytes: Vec<Option<Vec<u8>>> = Vec::with_capacity(num_rows);
    let is_string = matches!(field.field_type, FieldType::String);

    let mut total_records = 0;
    while total_records < num_rows {
        let batch_size = (num_rows - total_records).min(1024);
        let mut values: Vec<ByteArray> = vec![ByteArray::new(); batch_size];
        let mut def_levels = vec![0i16; batch_size];
        let mut rep_levels = vec![0i16; batch_size];

        let (records_read, values_read, _) = reader
            .read_records(
                batch_size,
                Some(&mut def_levels),
                Some(&mut rep_levels),
                &mut values,
            )
            .map_err(|e| IcebergError::Storage(format!("Failed to read byte array column: {e}")))?;

        if records_read == 0 {
            break;
        }

        // IMPORTANT: parquet-rs fills values[0..values_read] with actual data.
        // The def_levels array maps which records have values.
        // For optional columns: def_level > 0 means value is present
        // For required columns (values_read == records_read): all values present

        let mut value_idx = 0;
        for (i, &def_level) in def_levels.iter().enumerate().take(records_read) {
            // Determine if this record position has a value
            let has_value = if values_read == records_read {
                // All records have values (required column or no nulls)
                true
            } else {
                // Optional column with some nulls - check definition level
                def_level > 0
            };

            if has_value {
                if value_idx < values_read {
                    // Access the next value in the values array
                    let data = values[value_idx].data();
                    if is_string {
                        let s = String::from_utf8_lossy(data).into_owned();
                        all_values.push(Some(s));
                    } else {
                        all_bytes.push(Some(data.to_vec()));
                    }
                    value_idx += 1;
                } else {
                    // This shouldn't happen if parquet-rs is working correctly
                    tracing::warn!(
                        field = %field.name,
                        record_idx = i,
                        value_idx,
                        values_read,
                        "Value index exceeded values_read"
                    );
                    if is_string {
                        all_values.push(None);
                    } else {
                        all_bytes.push(None);
                    }
                }
            } else {
                // Null value
                if is_string {
                    all_values.push(None);
                } else {
                    all_bytes.push(None);
                }
            }
        }

        total_records += records_read;
    }

    if is_string {
        Ok(Column::String(all_values))
    } else {
        Ok(Column::Bytes(all_bytes))
    }
}

/// Decode fixed length byte array column.
fn decode_fixed_len_byte_array_column(
    reader: &mut parquet::column::reader::ColumnReaderImpl<
        parquet::data_type::FixedLenByteArrayType,
    >,
    num_rows: usize,
) -> Result<Column> {
    use parquet::data_type::FixedLenByteArray;

    let mut all_bytes: Vec<Option<Vec<u8>>> = Vec::with_capacity(num_rows);
    let mut total_records = 0;

    while total_records < num_rows {
        let batch_size = (num_rows - total_records).min(1024);
        // Pre-initialize with empty Vec to avoid panic
        let mut values: Vec<FixedLenByteArray> = (0..batch_size)
            .map(|_| FixedLenByteArray::from(Vec::<u8>::new()))
            .collect();
        let mut def_levels = vec![0i16; batch_size];
        let mut rep_levels = vec![0i16; batch_size];

        let (records_read, values_read, _) = reader
            .read_records(
                batch_size,
                Some(&mut def_levels),
                Some(&mut rep_levels),
                &mut values,
            )
            .map_err(|e| {
                IcebergError::Storage(format!("Failed to read fixed byte array column: {e}"))
            })?;

        if records_read == 0 {
            break;
        }

        let mut value_idx = 0;
        for &def_level in def_levels.iter().take(records_read) {
            let has_value = if values_read == records_read {
                true
            } else {
                def_level > 0
            };

            if has_value && value_idx < values_read {
                all_bytes.push(Some(values[value_idx].data().to_vec()));
                value_idx += 1;
            } else {
                all_bytes.push(None);
            }
        }

        total_records += records_read;
    }

    Ok(Column::Bytes(all_bytes))
}

/// Parse Parquet metadata from footer bytes.
pub fn parse_parquet_metadata_from_bytes(
    footer_bytes: &Bytes,
    _file_size: u64,
) -> Result<ParquetMetaData> {
    // Use parquet's metadata reader
    use parquet::file::metadata::ParquetMetaDataReader;

    let reader = ParquetMetaDataReader::new();

    // The reader expects bytes in a specific format, we have the footer + magic
    // For simplicity, try parsing with the full footer
    let metadata = reader
        .parse_and_finish(footer_bytes)
        .map_err(|e| IcebergError::Storage(format!("Failed to parse Parquet metadata: {e}")))?;

    Ok(metadata)
}

/// Build a mapping from Iceberg field ID to Parquet column index.
///
/// This function is the source of truth for matching Iceberg schema fields to
/// Parquet columns. It handles both:
/// - **Iceberg Parquet files**: Uses embedded field IDs (`SchemaElement.field_id`)
/// - **Non-Iceberg files**: Falls back to name-based matching
///
/// # Arguments
///
/// * `parquet_schema` - The Parquet file's schema
/// * `iceberg_schema` - The Iceberg table's schema (optional, for name-based fallback)
///
/// # Returns
///
/// A HashMap mapping Iceberg field ID → Parquet column index.
///
/// # Schema Evolution Safety
///
/// When Iceberg evolves a schema (adding/removing/reordering columns), the field IDs
/// remain stable. This mapping uses field IDs (not column positions) to ensure
/// projection and predicates work correctly across schema evolution.
pub fn build_field_id_to_column_mapping(
    parquet_schema: &SchemaType,
    iceberg_schema: Option<&Schema>,
) -> HashMap<i32, usize> {
    let mut mapping = HashMap::new();

    // First pass: collect field IDs from Parquet metadata
    for (col_idx, parquet_field) in parquet_schema.get_fields().iter().enumerate() {
        let basic_info = parquet_field.get_basic_info();

        if basic_info.has_id() {
            // Iceberg Parquet file: use embedded field ID
            let field_id = basic_info.id();
            mapping.insert(field_id, col_idx);
        } else if let Some(schema) = iceberg_schema {
            // Non-Iceberg file: fall back to name-based matching
            let col_name = parquet_field.name();
            if let Some(iceberg_field) = schema.field_by_name(col_name) {
                tracing::debug!(
                    col_idx,
                    name = col_name,
                    iceberg_field_id = iceberg_field.id,
                    "Matched Parquet column to Iceberg field by name"
                );
                mapping.insert(iceberg_field.id, col_idx);
            } else {
                tracing::warn!(
                    col_idx,
                    name = col_name,
                    "Parquet column has no field_id and no matching Iceberg field"
                );
            }
        } else {
            // No Iceberg schema available - log warning
            tracing::warn!(
                col_idx,
                name = parquet_field.name(),
                "Parquet column has no field_id and no Iceberg schema for fallback"
            );
        }
    }

    mapping
}

/// Build batch schema from Parquet metadata, Iceberg schema, and projected field IDs.
///
/// This function uses the Iceberg schema as the source of truth for field metadata
/// (id, name, required), mapping projected fields to Parquet columns using the
/// `build_field_id_to_column_mapping` function.
///
/// # Arguments
///
/// * `metadata` - Parquet file metadata
/// * `iceberg_schema` - Iceberg table schema (source of truth for field metadata)
/// * `projected_field_ids` - Field IDs to project (empty means all fields)
///
/// # Returns
///
/// Tuple of (BatchSchema, Vec<column_indices>) where column_indices maps each
/// BatchSchema field to its Parquet column index.
///
/// # Schema Evolution Safety
///
/// This implementation correctly handles schema evolution because:
/// 1. Field IDs (not column positions) are used to match Iceberg → Parquet
/// 2. Field metadata (name, required) comes from Iceberg schema, not Parquet
/// 3. Missing fields are reported as errors, not silently skipped
pub fn build_batch_schema_with_iceberg(
    metadata: &ParquetMetaData,
    iceberg_schema: &Schema,
    projected_field_ids: &[i32],
) -> Result<(BatchSchema, Vec<usize>)> {
    let parquet_schema = metadata.file_metadata().schema();

    // Build field ID → column index mapping
    let field_mapping = build_field_id_to_column_mapping(parquet_schema, Some(iceberg_schema));

    // Determine which fields to project
    let fields_to_project: Vec<&crate::metadata::SchemaField> = if projected_field_ids.is_empty() {
        // Project all non-nested fields
        iceberg_schema
            .fields
            .iter()
            .filter(|f| !f.is_nested())
            .collect()
    } else {
        // Project specific fields by ID
        projected_field_ids
            .iter()
            .filter_map(|&id| iceberg_schema.field(id))
            .filter(|f| !f.is_nested())
            .collect()
    };

    let mut fields = Vec::with_capacity(fields_to_project.len());
    let mut column_indices = Vec::with_capacity(fields_to_project.len());

    for iceberg_field in fields_to_project {
        // Find the Parquet column for this Iceberg field
        let col_idx = field_mapping.get(&iceberg_field.id).copied();

        // Determine field type - from Parquet if available, otherwise infer from Iceberg type
        let field_type = if let Some(idx) = col_idx {
            let parquet_field = &parquet_schema.get_fields()[idx];
            parquet_type_to_field_type(parquet_field)
        } else {
            // Field not in Parquet file (schema evolution case) - infer type from Iceberg schema
            // Per Iceberg spec, missing columns should be read as NULL
            tracing::debug!(
                field_id = iceberg_field.id,
                name = %iceberg_field.name,
                "Iceberg field not found in Parquet file, will materialize as NULLs"
            );
            iceberg_type_to_field_type(&iceberg_field.field_type)
        };

        let field_info = FieldInfo {
            name: iceberg_field.name.clone(),
            field_type,
            // Missing fields are always nullable (will be all NULLs)
            nullable: col_idx.is_none() || !iceberg_field.required,
            field_id: iceberg_field.id,
        };

        fields.push(field_info);
        // Use sentinel value (usize::MAX) to indicate "materialize as NULL"
        column_indices.push(col_idx.unwrap_or(usize::MAX));
    }

    Ok((BatchSchema::new(fields), column_indices))
}

/// Sentinel value indicating a column should be materialized as all NULLs.
///
/// Used when a projected field exists in the Iceberg schema but not in the
/// Parquet file (schema evolution case where column was added after file was written).
pub const NULL_COLUMN_SENTINEL: usize = usize::MAX;

/// Convert Iceberg type string to FieldType.
///
/// Used for schema evolution when a field exists in Iceberg schema but not in Parquet file.
fn iceberg_type_to_field_type(iceberg_type: &serde_json::Value) -> FieldType {
    match iceberg_type.as_str() {
        Some("boolean") => FieldType::Boolean,
        Some("int") => FieldType::Int32,
        Some("long") => FieldType::Int64,
        Some("float") => FieldType::Float32,
        Some("double") => FieldType::Float64,
        Some("string") => FieldType::String,
        Some("binary") => FieldType::Bytes,
        Some("date") => FieldType::Date,
        Some("time") => FieldType::Int64, // Microseconds (no separate Time type yet)
        Some("timestamp") => FieldType::Timestamp,
        Some("timestamptz") => FieldType::TimestampTz,
        Some("uuid") => FieldType::Bytes, // 16-byte fixed
        Some(s) if s.starts_with("decimal") => {
            // Parse decimal(precision, scale) if possible
            if let Some(inner) = s.strip_prefix("decimal(").and_then(|s| s.strip_suffix(')')) {
                let parts: Vec<&str> = inner.split(',').collect();
                if parts.len() == 2 {
                    if let (Ok(precision), Ok(scale)) =
                        (parts[0].trim().parse::<u8>(), parts[1].trim().parse::<i8>())
                    {
                        return FieldType::Decimal { precision, scale };
                    }
                }
            }
            // Fallback to bytes if parsing fails
            FieldType::Bytes
        }
        Some(s) if s.starts_with("fixed") => FieldType::Bytes, // Fixed-length binary
        _ => {
            // Nested types or unknown - default to bytes
            tracing::warn!(iceberg_type = ?iceberg_type, "Unknown Iceberg type, defaulting to Bytes");
            FieldType::Bytes
        }
    }
}

/// Build batch schema from Parquet metadata and projected field IDs.
///
/// This is the legacy API that extracts field IDs from Parquet metadata.
/// For correct schema evolution support, use `build_batch_schema_with_iceberg`
/// which uses the Iceberg schema as the source of truth.
///
/// # Note on Field ID Extraction
///
/// Iceberg writes field_id into the Parquet schema's `SchemaElement.field_id`.
/// This function extracts those IDs using parquet-rs's `BasicTypeInfo::id()`.
/// If a column has no field_id (non-Iceberg file), it falls back to column index,
/// which may produce incorrect results after schema evolution.
pub fn build_batch_schema(
    metadata: &ParquetMetaData,
    projected_field_ids: &[i32],
) -> Result<(BatchSchema, Vec<usize>)> {
    let parquet_schema = metadata.file_metadata().schema();
    let mut fields = Vec::new();
    let mut column_indices = Vec::new();

    // Map Parquet columns to projected fields using actual Iceberg field IDs
    for (col_idx, parquet_field) in parquet_schema.get_fields().iter().enumerate() {
        let basic_info = parquet_field.get_basic_info();

        // Extract the actual Iceberg field_id from the Parquet schema.
        // Iceberg writers store field_id in the Parquet SchemaElement.
        // Fall back to column index only for non-Iceberg Parquet files.
        let field_id = if basic_info.has_id() {
            basic_info.id()
        } else {
            // Fallback for Parquet files not written by Iceberg
            tracing::debug!(
                col_idx,
                name = parquet_field.name(),
                "Parquet column has no field_id, using column index as fallback"
            );
            col_idx as i32
        };

        // Check if this field is projected
        if !projected_field_ids.is_empty() && !projected_field_ids.contains(&field_id) {
            continue;
        }

        let field_type = parquet_type_to_field_type(parquet_field);
        let is_optional = !matches!(
            basic_info.repetition(),
            parquet::basic::Repetition::REQUIRED
        );

        let field_info = FieldInfo {
            name: parquet_field.name().to_string(),
            field_type,
            nullable: is_optional,
            field_id,
        };

        fields.push(field_info);
        column_indices.push(col_idx);
    }

    Ok((BatchSchema::new(fields), column_indices))
}

/// Build a projected Parquet schema containing only the specified column indices.
///
/// This creates a new schema that can be passed to `get_row_iter()` or
/// `RowIter::from_row_group()` for projection pushdown - parquet-rs will
/// only decode the specified columns.
pub fn build_projected_schema(
    full_schema: &SchemaType,
    column_indices: &[usize],
) -> Result<SchemaType> {
    let fields = full_schema.get_fields();

    // Collect only the projected fields
    let projected_fields: Vec<Arc<SchemaType>> = column_indices
        .iter()
        .filter_map(|&idx| fields.get(idx).cloned())
        .collect();

    // Build a new group type with the projected fields.
    // Use the original schema name - parquet-rs requires this for projection matching.
    SchemaType::group_type_builder(full_schema.name())
        .with_fields(projected_fields)
        .build()
        .map_err(|e| IcebergError::Storage(format!("Failed to build projected schema: {e}")))
}

/// Convert Parquet type to our FieldType.
fn parquet_type_to_field_type(parquet_type: &Arc<SchemaType>) -> FieldType {
    // Check if it's a primitive type
    if parquet_type.is_primitive() {
        let physical_type = parquet_type.get_physical_type();
        // Check logical/converted type annotations
        let basic_info = parquet_type.get_basic_info();
        let converted_type = basic_info.converted_type();

        // Check for string annotation
        if converted_type == parquet::basic::ConvertedType::UTF8 {
            return FieldType::String;
        }

        // Check for date annotation
        if converted_type == parquet::basic::ConvertedType::DATE {
            return FieldType::Date;
        }

        // Check for timestamp annotations
        // Parquet 2.0+ uses LogicalType for timezone info, converted_type doesn't distinguish
        if converted_type == parquet::basic::ConvertedType::TIMESTAMP_MILLIS
            || converted_type == parquet::basic::ConvertedType::TIMESTAMP_MICROS
        {
            // Check logical type for UTC info if available
            if let Some(parquet::basic::LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                ..
            }) = basic_info.logical_type()
            {
                return FieldType::TimestampTz;
            }
            return FieldType::Timestamp;
        }

        // Check for decimal annotation
        if converted_type == parquet::basic::ConvertedType::DECIMAL {
            // Try to get precision/scale from logical type
            if let Some(parquet::basic::LogicalType::Decimal { precision, scale }) =
                basic_info.logical_type()
            {
                return FieldType::Decimal {
                    precision: precision as u8,
                    scale: scale as i8,
                };
            }
            // Fallback: use default precision/scale if not available
            return FieldType::Decimal {
                precision: 38,
                scale: 0,
            };
        }

        // Fall back to physical type
        match physical_type {
            PhysicalType::BOOLEAN => FieldType::Boolean,
            PhysicalType::INT32 => FieldType::Int32,
            PhysicalType::INT64 => FieldType::Int64,
            PhysicalType::FLOAT => FieldType::Float32,
            PhysicalType::DOUBLE => FieldType::Float64,
            PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => FieldType::Bytes,
            PhysicalType::INT96 => FieldType::Timestamp, // Legacy timestamp (always local)
        }
    } else {
        // Group type - treat as bytes for now
        FieldType::Bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_footer_cache() {
        let cache = ParquetFooterCache::new(2);

        // Cache miss
        assert!(cache.get("file1.parquet", 1000).await.is_none());
    }

    #[test]
    fn test_parquet_magic() {
        assert_eq!(&PARQUET_MAGIC, b"PAR1");
    }

    #[test]
    fn test_int96_to_nanos() {
        // Test epoch (Julian day 2440588)
        let int96 = parquet::data_type::Int96::from(vec![0, 0, 2_440_588_u32]);
        let nanos = int96_to_nanos(&int96);
        assert_eq!(nanos, 0);
    }

    #[test]
    fn test_coalesce_ranges_no_overlap() {
        // Ranges far apart - should not merge
        let mut ranges = vec![(0, 100), (500, 600), (1000, 1100)];
        let coalesced = coalesce_ranges(&mut ranges, 50);
        assert_eq!(coalesced, vec![(0, 100), (500, 600), (1000, 1100)]);
    }

    #[test]
    fn test_coalesce_ranges_overlap() {
        // Overlapping ranges - should merge
        let mut ranges = vec![(0, 100), (50, 150), (140, 200)];
        let coalesced = coalesce_ranges(&mut ranges, 50);
        assert_eq!(coalesced, vec![(0, 200)]);
    }

    #[test]
    fn test_coalesce_ranges_small_gap() {
        // Small gap within threshold - should merge
        let mut ranges = vec![(0, 100), (120, 200)];
        let coalesced = coalesce_ranges(&mut ranges, 50);
        assert_eq!(coalesced, vec![(0, 200)]);
    }

    #[test]
    fn test_coalesce_ranges_large_gap() {
        // Large gap - should not merge
        let mut ranges = vec![(0, 100), (200, 300)];
        let coalesced = coalesce_ranges(&mut ranges, 50);
        assert_eq!(coalesced, vec![(0, 100), (200, 300)]);
    }

    #[test]
    fn test_coalesce_ranges_unsorted() {
        // Unsorted input - should sort and merge correctly
        let mut ranges = vec![(500, 600), (0, 100), (200, 300)];
        let coalesced = coalesce_ranges(&mut ranges, 50);
        assert_eq!(coalesced, vec![(0, 100), (200, 300), (500, 600)]);
    }

    #[test]
    fn test_coalesce_ranges_empty() {
        let mut ranges: Vec<(u64, u64)> = vec![];
        let coalesced = coalesce_ranges(&mut ranges, 50);
        assert!(coalesced.is_empty());
    }

    #[test]
    fn test_assemble_sparse_buffer() {
        // Create a 100-byte file with data at specific offsets
        let ranges = vec![
            (0u64, Bytes::from_static(b"HEADER")),
            (50u64, Bytes::from_static(b"MIDDLE")),
            (94u64, Bytes::from_static(b"FOOTER")),
        ];

        let buffer = assemble_sparse_buffer(100, ranges);

        assert_eq!(buffer.len(), 100);
        assert_eq!(&buffer[0..6], b"HEADER");
        assert_eq!(&buffer[6..50], vec![0u8; 44].as_slice()); // Gap filled with zeros
        assert_eq!(&buffer[50..56], b"MIDDLE");
        assert_eq!(&buffer[94..100], b"FOOTER");
    }

    #[test]
    fn test_assemble_sparse_buffer_overlapping() {
        // Later data should overwrite earlier (last write wins)
        let ranges = vec![
            (0u64, Bytes::from_static(b"AAAA")),
            (2u64, Bytes::from_static(b"BB")),
        ];

        let buffer = assemble_sparse_buffer(10, ranges);

        assert_eq!(&buffer[0..4], b"AABB");
    }

    // ==========================================================================
    // Field ID Mapping Tests
    // ==========================================================================

    #[test]
    fn test_build_field_id_to_column_mapping_with_iceberg_schema() {
        // Create a mock Iceberg schema for testing name-based fallback
        use crate::metadata::SchemaField;

        let schema = Schema {
            schema_id: 1,
            identifier_field_ids: vec![],
            fields: vec![
                SchemaField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: serde_json::Value::String("long".to_string()),
                    doc: None,
                },
                SchemaField {
                    id: 2,
                    name: "name".to_string(),
                    required: false,
                    field_type: serde_json::Value::String("string".to_string()),
                    doc: None,
                },
                SchemaField {
                    id: 3,
                    name: "value".to_string(),
                    required: false,
                    field_type: serde_json::Value::String("double".to_string()),
                    doc: None,
                },
            ],
        };

        // Create a simple Parquet schema without field IDs (non-Iceberg file)
        let parquet_schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![
                Arc::new(
                    parquet::schema::types::Type::primitive_type_builder("id", PhysicalType::INT64)
                        .build()
                        .unwrap(),
                ),
                Arc::new(
                    parquet::schema::types::Type::primitive_type_builder(
                        "name",
                        PhysicalType::BYTE_ARRAY,
                    )
                    .build()
                    .unwrap(),
                ),
                Arc::new(
                    parquet::schema::types::Type::primitive_type_builder(
                        "value",
                        PhysicalType::DOUBLE,
                    )
                    .build()
                    .unwrap(),
                ),
            ])
            .build()
            .unwrap();

        // Build mapping with Iceberg schema (name-based fallback)
        let mapping = build_field_id_to_column_mapping(&parquet_schema, Some(&schema));

        // Verify name-based mapping works
        assert_eq!(mapping.get(&1), Some(&0)); // id -> col 0
        assert_eq!(mapping.get(&2), Some(&1)); // name -> col 1
        assert_eq!(mapping.get(&3), Some(&2)); // value -> col 2
    }

    #[test]
    fn test_build_field_id_to_column_mapping_without_schema() {
        // Create a simple Parquet schema without field IDs and no Iceberg schema
        let parquet_schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![
                Arc::new(
                    parquet::schema::types::Type::primitive_type_builder("id", PhysicalType::INT64)
                        .build()
                        .unwrap(),
                ),
                Arc::new(
                    parquet::schema::types::Type::primitive_type_builder(
                        "name",
                        PhysicalType::BYTE_ARRAY,
                    )
                    .build()
                    .unwrap(),
                ),
            ])
            .build()
            .unwrap();

        // Build mapping without Iceberg schema - no entries should be created
        let mapping = build_field_id_to_column_mapping(&parquet_schema, None);

        // Without Iceberg schema and no field IDs in Parquet, mapping should be empty
        assert!(
            mapping.is_empty(),
            "Mapping should be empty without field IDs or schema"
        );
    }

    #[test]
    fn test_schema_field_is_nested() {
        use crate::metadata::SchemaField;

        // Primitive type (string)
        let primitive_field = SchemaField {
            id: 1,
            name: "name".to_string(),
            required: false,
            field_type: serde_json::Value::String("string".to_string()),
            doc: None,
        };
        assert!(!primitive_field.is_nested());

        // Nested type (struct)
        let nested_field = SchemaField {
            id: 2,
            name: "address".to_string(),
            required: false,
            field_type: serde_json::json!({
                "type": "struct",
                "fields": [
                    {"id": 3, "name": "street", "required": false, "type": "string"}
                ]
            }),
            doc: None,
        };
        assert!(nested_field.is_nested());
    }

    // ==========================================================================
    // Decimal Byte Conversion Tests
    // ==========================================================================

    #[test]
    fn test_decimal_bytes_to_i128_positive() {
        // Positive value: 12345 in big-endian
        // 12345 = 0x3039
        let bytes = vec![0x30, 0x39];
        assert_eq!(decimal_bytes_to_i128(&bytes), 12345);

        // Larger positive: 1234567890
        // 1234567890 = 0x499602D2
        let bytes = vec![0x49, 0x96, 0x02, 0xD2];
        assert_eq!(decimal_bytes_to_i128(&bytes), 1_234_567_890);
    }

    #[test]
    fn test_decimal_bytes_to_i128_negative() {
        // Negative value: -1 in two's complement
        // -1 = 0xFF (single byte)
        let bytes = vec![0xFF];
        assert_eq!(decimal_bytes_to_i128(&bytes), -1);

        // -12345 in two's complement big-endian
        // -12345 = 0xFFFFCFC7 (4 bytes) or 0xCFC7 (2 bytes)
        let bytes = vec![0xCF, 0xC7];
        assert_eq!(decimal_bytes_to_i128(&bytes), -12345);

        // -128 in single byte
        let bytes = vec![0x80];
        assert_eq!(decimal_bytes_to_i128(&bytes), -128);
    }

    #[test]
    fn test_decimal_bytes_to_i128_odd_length() {
        // 3 bytes: 0x01 0x00 0x00 = 65536
        let bytes = vec![0x01, 0x00, 0x00];
        assert_eq!(decimal_bytes_to_i128(&bytes), 65536);

        // 3 bytes negative: 0xFF 0x00 0x00 = -65536 (sign extended)
        let bytes = vec![0xFF, 0x00, 0x00];
        assert_eq!(decimal_bytes_to_i128(&bytes), -65536);

        // 5 bytes positive
        let bytes = vec![0x01, 0x02, 0x03, 0x04, 0x05];
        // = 0x0102030405 = 4328719365
        assert_eq!(decimal_bytes_to_i128(&bytes), 4_328_719_365);
    }

    #[test]
    fn test_decimal_bytes_to_i128_edge_cases() {
        // Empty bytes
        assert_eq!(decimal_bytes_to_i128(&[]), 0);

        // Single zero byte
        assert_eq!(decimal_bytes_to_i128(&[0x00]), 0);

        // Maximum positive for 1 byte (127)
        assert_eq!(decimal_bytes_to_i128(&[0x7F]), 127);

        // Minimum negative for 1 byte (-128)
        assert_eq!(decimal_bytes_to_i128(&[0x80]), -128);
    }
}
