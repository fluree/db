//! Arrow-based Parquet decode path.
//!
//! An alternative to the row-based `RowIter` decode in [`super::send_parquet`],
//! compiled in when the `arrow` feature is enabled. It uses
//! `ParquetRecordBatchReaderBuilder` for native columnar decode with:
//!
//! - **projection** via `ProjectionMask` (only the requested leaves are read),
//! - **row-group pruning** via `with_row_groups` (skipped groups' column chunks
//!   are never fetched — the same statistic-based pruning as the RowIter path),
//! - and, in a later commit, **exact row filtering** via `with_row_filter`.
//!
//! To stay byte-identical to the RowIter path, each Arrow cell is converted to
//! the same intermediate [`ColumnValue`] the row path produces, then assembled
//! by the shared [`build_columns_from_values`]. Only the "Arrow cell →
//! `ColumnValue`" step is new; column construction is unchanged.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    FixedSizeBinaryArray, Float16Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::ChunkReader;

use crate::error::{IcebergError, Result};
use crate::io::batch::{ColumnBatch, FieldType};
use crate::io::parquet::{
    build_batch_schema, build_batch_schema_with_iceberg, build_columns_from_values, ColumnValue,
    NULL_COLUMN_SENTINEL,
};
use crate::metadata::Schema;
use crate::scan::predicate::Expression;

/// Rows per emitted `RecordBatch`. Batch boundaries do not affect query results
/// (the R2RML operator streams batches), so this only tunes chunk granularity —
/// smaller batches give finer LIMIT early-termination once the row budget is
/// wired through, at the cost of more per-batch overhead.
const ARROW_BATCH_ROWS: usize = 8192;

/// Decode a Parquet file to [`ColumnBatch`]es using the Arrow reader.
///
/// `chunk_reader` is either the in-memory `Bytes` of a small file (already range
/// read) or a `RangeBackedChunkReader` for a large file — both implement
/// `ChunkReader`, so the same fetched bytes / on-demand ranges are reused.
///
/// The batch schema and projected column indices are recomputed from the
/// reader's own footer (identical to the RowIter path, which derives them from
/// the same file), so callers only pass the projection request.
pub(crate) fn decode_batches_arrow<R: ChunkReader + 'static>(
    chunk_reader: R,
    projected_field_ids: &[i32],
    residual_filter: Option<&Expression>,
    iceberg_schema: Option<&Schema>,
) -> Result<Vec<ColumnBatch>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(chunk_reader)
        .map_err(|e| IcebergError::Storage(format!("Failed to open Parquet (arrow): {e}")))?;

    let metadata = builder.metadata().clone();
    let md: &ParquetMetaData = &metadata;

    // Same schema/projection resolution as the RowIter path.
    let (batch_schema, column_indices) = if let Some(schema) = iceberg_schema {
        build_batch_schema_with_iceberg(md, schema, projected_field_ids)?
    } else {
        build_batch_schema(md, projected_field_ids)?
    };
    let batch_schema = Arc::new(batch_schema);

    // Real (non-null-sentinel) parquet leaf indices this projection needs.
    let real: Vec<usize> = column_indices
        .iter()
        .copied()
        .filter(|&idx| idx != NULL_COLUMN_SENTINEL)
        .collect();

    let mask = ProjectionMask::leaves(builder.parquet_schema(), real.iter().copied());

    // A projection selects leaves in ascending file order regardless of request
    // order, so map each parquet leaf index → its RecordBatch column position.
    let mut sorted = real.clone();
    sorted.sort_unstable();
    sorted.dedup();
    let leaf_to_pos: HashMap<usize, usize> = sorted
        .iter()
        .enumerate()
        .map(|(pos, &c)| (c, pos))
        .collect();

    // batch field → Some(RecordBatch column position) or None (schema-evolution
    // column absent from this file → always null).
    let field_to_pos: Vec<Option<usize>> = column_indices
        .iter()
        .map(|&idx| {
            if idx == NULL_COLUMN_SENTINEL {
                None
            } else {
                leaf_to_pos.get(&idx).copied()
            }
        })
        .collect();

    let surviving =
        crate::io::send_parquet::surviving_row_groups(md, residual_filter, iceberg_schema);

    let reader = builder
        .with_projection(mask)
        .with_row_groups(surviving)
        .with_batch_size(ARROW_BATCH_ROWS)
        .build()
        .map_err(|e| {
            IcebergError::Storage(format!("Failed to build Parquet reader (arrow): {e}"))
        })?;

    let num_fields = batch_schema.fields.len();
    let mut batches = Vec::new();

    for record_batch in reader {
        let record_batch =
            record_batch.map_err(|e| IcebergError::Storage(format!("Arrow decode error: {e}")))?;
        let num_rows = record_batch.num_rows();
        if num_rows == 0 {
            continue;
        }

        let mut column_data: Vec<Vec<Option<ColumnValue>>> = (0..num_fields)
            .map(|_| Vec::with_capacity(num_rows))
            .collect();

        for (batch_idx, field_info) in batch_schema.fields.iter().enumerate() {
            match field_to_pos[batch_idx] {
                Some(pos) => {
                    let array = record_batch.column(pos).as_ref();
                    for row in 0..num_rows {
                        column_data[batch_idx].push(arrow_cell_to_column_value(
                            array,
                            row,
                            &field_info.field_type,
                        ));
                    }
                }
                None => column_data[batch_idx].extend(std::iter::repeat_n(None, num_rows)),
            }
        }

        let columns = build_columns_from_values(column_data, &batch_schema)?;
        let batch = ColumnBatch::new(Arc::clone(&batch_schema), columns)?;
        if !batch.is_empty() {
            batches.push(batch);
        }
    }

    Ok(batches)
}

/// Convert a single Arrow array cell to the intermediate [`ColumnValue`],
/// mirroring `convert_field_to_column_value` for the RowIter path so downstream
/// column assembly is identical. Returns `None` for nulls and unsupported types.
fn arrow_cell_to_column_value(
    array: &dyn Array,
    row: usize,
    field_type: &FieldType,
) -> Option<ColumnValue> {
    if array.is_null(row) {
        return None;
    }

    macro_rules! cell {
        ($ty:ty) => {
            array.as_any().downcast_ref::<$ty>().map(|a| a.value(row))
        };
    }

    match array.data_type() {
        DataType::Boolean => cell!(BooleanArray).map(ColumnValue::Boolean),
        DataType::Int8 => cell!(Int8Array).map(|v| ColumnValue::Int32(v as i32)),
        DataType::Int16 => cell!(Int16Array).map(|v| ColumnValue::Int32(v as i32)),
        DataType::Int32 => cell!(Int32Array).map(|v| match field_type {
            FieldType::Date => ColumnValue::Date(v),
            _ => ColumnValue::Int32(v),
        }),
        DataType::Int64 => cell!(Int64Array).map(ColumnValue::Int64),
        DataType::UInt8 => cell!(UInt8Array).map(|v| ColumnValue::Int32(v as i32)),
        DataType::UInt16 => cell!(UInt16Array).map(|v| ColumnValue::Int32(v as i32)),
        DataType::UInt32 => cell!(UInt32Array).map(|v| ColumnValue::Int64(v as i64)),
        DataType::UInt64 => cell!(UInt64Array).map(|v| ColumnValue::Int64(v as i64)),
        DataType::Float16 => cell!(Float16Array).map(|v| ColumnValue::Float32(v.to_f32())),
        DataType::Float32 => cell!(Float32Array).map(ColumnValue::Float32),
        DataType::Float64 => cell!(Float64Array).map(ColumnValue::Float64),
        DataType::Utf8 => cell!(StringArray).map(|s| ColumnValue::String(s.to_string())),
        DataType::LargeUtf8 => cell!(LargeStringArray).map(|s| ColumnValue::String(s.to_string())),
        DataType::Binary => cell!(BinaryArray).map(|b| ColumnValue::Bytes(b.to_vec())),
        DataType::LargeBinary => cell!(LargeBinaryArray).map(|b| ColumnValue::Bytes(b.to_vec())),
        DataType::FixedSizeBinary(_) => {
            cell!(FixedSizeBinaryArray).map(|b| ColumnValue::Bytes(b.to_vec()))
        }
        DataType::Date32 => cell!(Date32Array).map(ColumnValue::Date),
        DataType::Date64 => {
            cell!(Date64Array).map(|ms| ColumnValue::Date((ms / 86_400_000) as i32))
        }
        DataType::Decimal128(_, _) => cell!(Decimal128Array).map(ColumnValue::Decimal),
        DataType::Timestamp(unit, _tz) => {
            let micros = match unit {
                TimeUnit::Second => cell!(TimestampSecondArray).map(|v| v * 1_000_000),
                TimeUnit::Millisecond => cell!(TimestampMillisecondArray).map(|v| v * 1_000),
                TimeUnit::Microsecond => cell!(TimestampMicrosecondArray),
                TimeUnit::Nanosecond => cell!(TimestampNanosecondArray).map(|v| v / 1_000),
            };
            micros.map(|m| match field_type {
                FieldType::TimestampTz => ColumnValue::TimestampTz(m),
                _ => ColumnValue::Timestamp(m),
            })
        }
        _ => None,
    }
}
