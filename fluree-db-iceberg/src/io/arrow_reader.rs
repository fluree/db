//! Arrow-based Parquet decode path.
//!
//! An alternative to the row-based `RowIter` decode in [`super::send_parquet`],
//! compiled in when the `arrow` feature is enabled. It uses
//! `ParquetRecordBatchReaderBuilder` for native columnar decode with:
//!
//! - **projection** via `ProjectionMask` (only the requested leaves are read),
//! - **row-group pruning** via `with_row_groups` (skipped groups' column chunks
//!   are never fetched — the same statistic-based pruning as the RowIter path),
//! - **exact row filtering** via `with_row_filter` (the pushed predicate is
//!   evaluated during decode, dropping non-matching rows before materialization).
//!
//! To stay byte-identical to the RowIter path, each Arrow cell is converted to
//! the same intermediate [`ColumnValue`] the row path produces, then assembled
//! by the shared [`build_columns_from_values`]. Only the "Arrow cell →
//! `ColumnValue`" step is new; column construction is unchanged.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    FixedSizeBinaryArray, Float16Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, Scalar, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute::kernels::{boolean::and, cast::cast, cmp};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::ChunkReader;
use parquet::schema::types::SchemaDescriptor;

use crate::error::{IcebergError, Result};
use crate::io::batch::{ColumnBatch, FieldType};
use crate::io::parquet::{
    build_batch_schema, build_batch_schema_with_iceberg, build_columns_from_values,
    build_field_id_to_column_mapping, ColumnValue, NULL_COLUMN_SENTINEL,
};
use crate::io::send_parquet::predicate_pushdown_enabled;
use crate::metadata::Schema;
use crate::scan::predicate::{ComparisonOp, Expression, LiteralValue};

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

    // Exact row-level filtering: Arrow evaluates the pushed predicate during
    // decode and drops non-matching rows before they are materialized. Native
    // type handling (Int→Decimal casts, null semantics) sidesteps the manual
    // type reconciliation that a hand-rolled evaluator needs. Conservative
    // relative to the in-engine FILTER, which stays the authority: a row this
    // keeps is re-checked, and a row it drops (predicate false, or a null cell
    // that yields no R2RML triple) would also be excluded downstream.
    let row_filter = residual_filter
        .filter(|_| predicate_pushdown_enabled())
        .and_then(|r| build_row_filter(r, builder.parquet_schema(), iceberg_schema));

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

    let mut builder = builder
        .with_projection(mask)
        .with_row_groups(surviving)
        .with_batch_size(ARROW_BATCH_ROWS);
    if let Some(filter) = row_filter {
        builder = builder.with_row_filter(filter);
    }
    let reader = builder.build().map_err(|e| {
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

/// One resolved comparison in the row filter: the column's position within the
/// predicate `RecordBatch` (predicate columns arrive in ascending leaf order),
/// the operator, and the literal to compare against.
type ResolvedComparison = (usize, ComparisonOp, LiteralValue);

/// Build an Arrow `RowFilter` from a residual predicate, or `None` when the
/// predicate is not a plain conjunction of column comparisons (the only shape
/// the R2RML → Iceberg bridge emits) or references no mappable column.
///
/// Dropping an unmappable conjunct only weakens the filter (keeps more rows),
/// which is safe: the in-engine FILTER remains the authority.
fn build_row_filter(
    residual: &Expression,
    parquet_schema: &SchemaDescriptor,
    iceberg_schema: Option<&Schema>,
) -> Option<RowFilter> {
    let mut comparisons = Vec::new();
    if !collect_and_comparisons(residual, &mut comparisons) || comparisons.is_empty() {
        return None;
    }

    let field_to_col =
        build_field_id_to_column_mapping(parquet_schema.root_schema(), iceberg_schema);

    // Resolve each comparison to a parquet column index; drop any we cannot map.
    let mut resolved: Vec<(usize, ComparisonOp, LiteralValue)> = Vec::new();
    for (field_id, op, value) in comparisons {
        if let Some(&col_idx) = field_to_col.get(&field_id) {
            resolved.push((col_idx, op, value));
        }
    }
    if resolved.is_empty() {
        return None;
    }

    // Predicate columns are projected in ascending leaf order; map col → position.
    let mut cols: Vec<usize> = resolved.iter().map(|(c, _, _)| *c).collect();
    cols.sort_unstable();
    cols.dedup();
    let pos_of: HashMap<usize, usize> = cols.iter().enumerate().map(|(pos, &c)| (c, pos)).collect();

    let plan: Vec<ResolvedComparison> = resolved
        .into_iter()
        .map(|(col, op, value)| (pos_of[&col], op, value))
        .collect();

    let mask = ProjectionMask::leaves(parquet_schema, cols.iter().copied());
    let predicate = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
        eval_conjunction(&batch, &plan)
    });
    Some(RowFilter::new(vec![Box::new(predicate)]))
}

/// Flatten a predicate into a list of `(field_id, op, literal)` comparisons.
/// Returns `false` if any node is not a `Comparison` or `And` — i.e. the
/// predicate cannot be represented as a simple conjunction and no row filter
/// should be built (row-group pruning + the in-engine FILTER still apply).
fn collect_and_comparisons(
    expr: &Expression,
    out: &mut Vec<(i32, ComparisonOp, LiteralValue)>,
) -> bool {
    match expr {
        Expression::Comparison {
            field_id,
            op,
            value,
            ..
        } => {
            out.push((*field_id, *op, value.clone()));
            true
        }
        Expression::And(children) => children.iter().all(|c| collect_and_comparisons(c, out)),
        _ => false,
    }
}

/// Evaluate the conjunction over a predicate `RecordBatch`, ANDing the per-
/// comparison masks. A null cell yields a null mask entry, which Arrow's
/// `RowFilter` treats as "drop" — correct for R2RML, where a null column
/// produces no triple.
fn eval_conjunction(
    batch: &RecordBatch,
    plan: &[ResolvedComparison],
) -> std::result::Result<BooleanArray, ArrowError> {
    let mut acc: Option<BooleanArray> = None;
    for (pos, op, value) in plan {
        let mask = eval_comparison(batch.column(*pos), op, value)?;
        acc = Some(match acc {
            Some(prev) => and(&prev, &mask)?,
            None => mask,
        });
    }
    acc.ok_or_else(|| ArrowError::ComputeError("empty row-filter conjunction".to_string()))
}

/// Evaluate a single comparison against a column, returning a boolean mask.
///
/// The literal is cast into the column's own Arrow type before comparison, so
/// an `xsd:integer` literal compared against a physically `Decimal` Iceberg
/// column is scaled correctly (the exact case a manual evaluator mishandled).
/// If the literal cannot be cast to the column type the comparison is treated
/// as all-true (keep every row) so the in-engine FILTER decides.
fn eval_comparison(
    column: &ArrayRef,
    op: &ComparisonOp,
    value: &LiteralValue,
) -> std::result::Result<BooleanArray, ArrowError> {
    let literal = literal_to_array(value);
    let casted = match cast(&literal, column.data_type()) {
        Ok(c) => c,
        Err(_) => return Ok(BooleanArray::from(vec![true; column.len()])),
    };
    let scalar = Scalar::new(casted);
    match op {
        ComparisonOp::Eq => cmp::eq(column, &scalar),
        ComparisonOp::NotEq => cmp::neq(column, &scalar),
        ComparisonOp::Lt => cmp::lt(column, &scalar),
        ComparisonOp::LtEq => cmp::lt_eq(column, &scalar),
        ComparisonOp::Gt => cmp::gt(column, &scalar),
        ComparisonOp::GtEq => cmp::gt_eq(column, &scalar),
    }
}

/// A single-element Arrow array holding the literal, in a natural type that
/// `cast` can convert to the target column type.
fn literal_to_array(value: &LiteralValue) -> ArrayRef {
    match value {
        LiteralValue::Boolean(b) => Arc::new(BooleanArray::from(vec![*b])),
        LiteralValue::Int32(i) => Arc::new(Int32Array::from(vec![*i])),
        LiteralValue::Int64(i) => Arc::new(Int64Array::from(vec![*i])),
        LiteralValue::Float32(f) => Arc::new(Float32Array::from(vec![*f])),
        LiteralValue::Float64(f) => Arc::new(Float64Array::from(vec![*f])),
        LiteralValue::String(s) => Arc::new(StringArray::from(vec![s.clone()])),
        LiteralValue::Bytes(b) => Arc::new(BinaryArray::from(vec![b.as_slice()])),
        LiteralValue::Date(d) => Arc::new(Date32Array::from(vec![*d])),
        LiteralValue::Timestamp(t) => Arc::new(TimestampMicrosecondArray::from(vec![*t])),
        LiteralValue::Decimal {
            unscaled,
            precision,
            scale,
        } => Arc::new(
            Decimal128Array::from(vec![*unscaled])
                .with_precision_and_scale(*precision, *scale)
                .expect("valid decimal precision/scale"),
        ),
    }
}
