//! Arrow-based Parquet decode path — the single decode path for the Send reader
//! ([`super::send_parquet`]). It uses `ParquetRecordBatchReaderBuilder` for
//! native columnar decode with:
//!
//! - **projection** via `ProjectionMask` (only the requested leaves are read),
//! - **row-group pruning** via `with_row_groups` (skipped groups' column chunks
//!   are never fetched),
//! - **exact row filtering** by evaluating the pushed predicate on each decoded
//!   `RecordBatch` and dropping non-matching rows with `filter_record_batch`.
//!   Arrow's `with_row_filter` is deliberately NOT used: its RowSelection calls
//!   `skip_records`, which panics in parquet-rs 54 on Snowflake's
//!   DELTA_BINARY_PACKED integer columns.
//!
//! Each Arrow cell is converted to the intermediate [`ColumnValue`] and
//! assembled by the shared [`build_columns_from_values`], so the output
//! `ColumnBatch` format matches the rest of the crate.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    FixedSizeBinaryArray, Float16Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, Scalar, StringArray,
    Time32MillisecondArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::{boolean::and, cast::cast, cmp};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::ChunkReader;

use crate::error::{IcebergError, Result};
use crate::io::batch::{Column, ColumnBatch, FieldType};
use crate::io::parquet::{
    build_batch_schema, build_batch_schema_with_iceberg, build_columns_from_values,
    build_field_id_to_leaf_mapping, build_root_to_leaf_map, column_from_values, ColumnValue,
    NULL_COLUMN_SENTINEL,
};
use crate::io::send_parquet::predicate_pushdown_enabled;
use crate::metadata::Schema;
use crate::scan::predicate::{ComparisonOp, Expression, LiteralValue};

/// Rows per emitted `RecordBatch` for an unbounded scan. Batch boundaries do not
/// affect query results (the R2RML operator streams batches), so this only tunes
/// chunk granularity, at the cost of more per-batch overhead. A bounded read (see
/// `max_rows` on [`decode_batches_arrow`]) instead sizes the decode batch to the
/// row budget so the first batch already satisfies it.
const ARROW_BATCH_ROWS: usize = 8192;

/// Number of row groups in a Parquet file, read from its footer only (no column
/// chunks are fetched). Used by the T2.3 row-group parallel decode to size the fan
/// and honor the single-row-group decline before spawning the per-slice tasks.
pub(crate) fn read_num_row_groups<R: ChunkReader + 'static>(chunk_reader: R) -> Result<usize> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(chunk_reader)
        .map_err(|e| IcebergError::Storage(format!("Failed to open Parquet (arrow): {e}")))?;
    Ok(builder.metadata().num_row_groups())
}

/// Decode a Parquet file to [`ColumnBatch`]es using the Arrow reader.
///
/// `chunk_reader` is either the in-memory `Bytes` of a small file (already range
/// read) or a `RangeBackedChunkReader` for a large file — both implement
/// `ChunkReader`, so the same fetched bytes / on-demand ranges are reused.
///
/// The batch schema and projected column indices are recomputed from the
/// reader's own footer, so callers only pass the projection request.
///
/// `max_rows` bounds the read for a cheap "peek" (row preview / data sampler):
/// when `Some(n)` the decode is restricted to the **first surviving row group**
/// and stops after `n` rows, so a small `n` fetches only that group's projected
/// column chunks (plus the footer) via the range-backed reader — it never scans
/// the whole file. At most `min(n, rows-in-first-row-group)` rows are returned.
/// `None` is an unbounded scan and is behavior-identical to before this budget
/// existed.
pub(crate) fn decode_batches_arrow<R: ChunkReader + 'static>(
    chunk_reader: R,
    projected_field_ids: &[i32],
    residual_filter: Option<&Expression>,
    iceberg_schema: Option<&Schema>,
    max_rows: Option<usize>,
    row_group_subset: Option<&[usize]>,
) -> Result<Vec<ColumnBatch>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(chunk_reader)
        .map_err(|e| IcebergError::Storage(format!("Failed to open Parquet (arrow): {e}")))?;

    let metadata = builder.metadata().clone();
    let md: &ParquetMetaData = &metadata;

    // Resolve the batch schema and projected column indices from the footer.
    let (batch_schema, column_indices) = if let Some(schema) = iceberg_schema {
        build_batch_schema_with_iceberg(md, schema, projected_field_ids)?
    } else {
        build_batch_schema(md, projected_field_ids)?
    };
    let batch_schema = Arc::new(batch_schema);

    // `column_indices` are ROOT field indices; the projection and row-group
    // statistics APIs below index the flat LEAF-column space, which diverges
    // under nested columns. Translate through the root→leaf map once and reuse
    // it (also avoids rebuilding the field-id mapping per consumer).
    let root_to_leaf = build_root_to_leaf_map(builder.parquet_schema());

    // batch field → its parquet leaf index, or None (schema-evolution column
    // absent from this file → always null).
    let field_to_leaf: Vec<Option<usize>> = column_indices
        .iter()
        .map(|&idx| {
            (idx != NULL_COLUMN_SENTINEL)
                .then(|| root_to_leaf.get(&idx).copied())
                .flatten()
        })
        .collect();

    // Real (non-null-sentinel) parquet leaf indices this projection needs.
    let real: Vec<usize> = field_to_leaf.iter().flatten().copied().collect();

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

    // batch field → Some(RecordBatch column position) or None (absent column).
    let field_to_pos: Vec<Option<usize>> = field_to_leaf
        .iter()
        .map(|&leaf| leaf.and_then(|l| leaf_to_pos.get(&l).copied()))
        .collect();

    // Iceberg field ID → leaf column index, shared by the row filter and
    // row-group pruning (built once here instead of once per consumer).
    let field_id_to_leaf = build_field_id_to_leaf_mapping(md, iceberg_schema);

    // Exact row-level filtering, applied to each decoded RecordBatch via
    // `filter_record_batch`. We deliberately do NOT use Arrow's `with_row_filter`:
    // it builds a RowSelection that calls `skip_records`, which panics on
    // Snowflake's DELTA_BINARY_PACKED integer columns (parquet-rs 54
    // `DeltaBitPackDecoder::skip`). Filtering post-decode sidesteps that while
    // keeping native type handling (Int→Decimal casts). Row-group pruning
    // (below) still runs, and the in-engine FILTER stays the authority — a kept
    // row is re-checked; a dropped row (predicate false, or a null cell that
    // yields no R2RML triple) would be excluded downstream anyway.
    let predicate_plan = residual_filter
        .filter(|_| predicate_pushdown_enabled())
        .and_then(|r| build_predicate_plan(r, &field_id_to_leaf, &leaf_to_pos));

    let mut surviving =
        crate::io::send_parquet::surviving_row_groups(md, residual_filter, &field_id_to_leaf);

    // T2.3: a parallel decode assigns each blocking task a contiguous slice of the
    // file's row groups; keep only the surviving groups in this task's slice so the
    // tasks partition the file with no overlap (their union == a full sequential
    // decode; row groups are independently decodable). No-op for the sequential
    // path (`None`). Does not co-occur with a `max_rows` peek (a parallel decode is
    // never a bounded peek).
    if let Some(subset) = row_group_subset {
        surviving.retain(|rg| subset.contains(rg));
    }

    // Bounded "peek": restrict to the first surviving row group so a small
    // `max_rows` fetches only that group's projected column chunks (+ footer) via
    // the range-backed reader — the whole point of a cheap sample. Unbounded
    // reads (`None`) keep every surviving row group.
    if max_rows.is_some() {
        surviving.truncate(1);
    }

    // A bounded peek only needs `max_rows` rows, so size the decode batch to the
    // budget instead of decoding a full `ARROW_BATCH_ROWS` batch we would
    // immediately slice back down.
    let batch_rows = match max_rows {
        Some(n) => n.clamp(1, ARROW_BATCH_ROWS),
        None => ARROW_BATCH_ROWS,
    };

    let reader = builder
        .with_projection(mask)
        .with_row_groups(surviving)
        .with_batch_size(batch_rows)
        .build()
        .map_err(|e| {
            IcebergError::Storage(format!("Failed to build Parquet reader (arrow): {e}"))
        })?;

    let mut batches = Vec::new();
    let mut produced = 0usize;
    // The row filter's literals, converted into the decoded column types once per
    // file (a column's type cannot change between batches of one file).
    let mut prepared: Option<Vec<PreparedComparison>> = None;

    for record_batch in reader {
        // Stop before decoding another batch once the row budget is met.
        if max_rows.is_some_and(|budget| produced >= budget) {
            break;
        }
        let mut record_batch =
            record_batch.map_err(|e| IcebergError::Storage(format!("Arrow decode error: {e}")))?;
        if let Some(plan) = &predicate_plan {
            let prepared =
                prepared.get_or_insert_with(|| prepare_comparisons(plan, &record_batch.schema()));
            let mask = eval_conjunction(&record_batch, prepared)
                .map_err(|e| IcebergError::Storage(format!("Row filter eval error: {e}")))?;
            if let Some(mask) = mask {
                record_batch = filter_record_batch(&record_batch, &mask)
                    .map_err(|e| IcebergError::Storage(format!("Row filter apply error: {e}")))?;
            }
        }
        // Trim the final batch so we never emit more than the row budget. Safe:
        // the guard above guarantees `produced < budget` here.
        if let Some(budget) = max_rows {
            let remaining = budget - produced;
            if record_batch.num_rows() > remaining {
                record_batch = record_batch.slice(0, remaining);
            }
        }
        let num_rows = record_batch.num_rows();
        if num_rows == 0 {
            continue;
        }

        // N2 (Gap-4 P1): build `Column`s DIRECTLY from the Arrow arrays, skipping
        // the per-cell `Vec<Option<ColumnValue>>` intermediate. Off restores the
        // byte-identical two-hop path.
        let columns = if direct_decode_enabled() {
            batch_schema
                .fields
                .iter()
                .enumerate()
                .map(|(batch_idx, field_info)| match field_to_pos[batch_idx] {
                    Some(pos) => arrow_column_to_column(
                        record_batch.column(pos).as_ref(),
                        &field_info.field_type,
                        num_rows,
                    ),
                    // schema-evolution column absent from this file → all null.
                    None => column_from_values(&vec![None; num_rows], &field_info.field_type),
                })
                .collect()
        } else {
            let column_data: Vec<Vec<Option<ColumnValue>>> = batch_schema
                .fields
                .iter()
                .enumerate()
                .map(|(batch_idx, field_info)| match field_to_pos[batch_idx] {
                    Some(pos) => arrow_column_to_values(
                        record_batch.column(pos).as_ref(),
                        &field_info.field_type,
                        num_rows,
                    ),
                    // schema-evolution column absent from this file → all null.
                    None => vec![None; num_rows],
                })
                .collect();
            build_columns_from_values(column_data, &batch_schema)
        };
        let batch = ColumnBatch::new(Arc::clone(&batch_schema), columns)?;
        if !batch.is_empty() {
            batches.push(batch);
        }
        produced += num_rows;
    }

    Ok(batches)
}

/// Convert a whole Arrow column to a `Vec<Option<ColumnValue>>` for the shared
/// column assembly. The array type is resolved (`downcast_ref` + `data_type`
/// match) ONCE per column, then the row loop runs over the concrete typed array
/// — instead of re-dispatching on every cell. Nulls and unsupported types map to
/// `None`.
fn arrow_column_to_values(
    array: &dyn Array,
    field_type: &FieldType,
    num_rows: usize,
) -> Vec<Option<ColumnValue>> {
    // Downcast to the concrete array once, then map each row through `$f`,
    // preserving nulls.
    macro_rules! column {
        ($ty:ty, $f:expr) => {
            match array.as_any().downcast_ref::<$ty>() {
                Some(a) => (0..num_rows)
                    .map(|i| (!a.is_null(i)).then(|| $f(a.value(i))))
                    .collect(),
                None => vec![None; num_rows],
            }
        };
    }

    match array.data_type() {
        DataType::Boolean => column!(BooleanArray, ColumnValue::Boolean),
        DataType::Int8 => column!(Int8Array, |v| ColumnValue::Int32(v as i32)),
        DataType::Int16 => column!(Int16Array, |v| ColumnValue::Int32(v as i32)),
        DataType::Int32 => column!(Int32Array, |v| match field_type {
            FieldType::Date => ColumnValue::Date(v),
            _ => ColumnValue::Int32(v),
        }),
        DataType::Int64 => column!(Int64Array, ColumnValue::Int64),
        DataType::UInt8 => column!(UInt8Array, |v| ColumnValue::Int32(v as i32)),
        DataType::UInt16 => column!(UInt16Array, |v| ColumnValue::Int32(v as i32)),
        DataType::UInt32 => column!(UInt32Array, |v| ColumnValue::Int64(v as i64)),
        DataType::UInt64 => column!(UInt64Array, |v| ColumnValue::Int64(v as i64)),
        // Inline (not via `column!`) so `a.value(i)`'s `half::f16` type is known
        // for `.to_f32()` without naming the `half` crate.
        DataType::Float16 => match array.as_any().downcast_ref::<Float16Array>() {
            Some(a) => (0..num_rows)
                .map(|i| (!a.is_null(i)).then(|| ColumnValue::Float32(a.value(i).to_f32())))
                .collect(),
            None => vec![None; num_rows],
        },
        DataType::Float32 => column!(Float32Array, ColumnValue::Float32),
        DataType::Float64 => column!(Float64Array, ColumnValue::Float64),
        DataType::Utf8 => column!(StringArray, |s: &str| ColumnValue::String(s.to_string())),
        DataType::LargeUtf8 => {
            column!(LargeStringArray, |s: &str| ColumnValue::String(
                s.to_string()
            ))
        }
        DataType::Binary => column!(BinaryArray, |b: &[u8]| ColumnValue::Bytes(b.to_vec())),
        DataType::LargeBinary => {
            column!(LargeBinaryArray, |b: &[u8]| ColumnValue::Bytes(b.to_vec()))
        }
        DataType::FixedSizeBinary(_) => {
            column!(FixedSizeBinaryArray, |b: &[u8]| ColumnValue::Bytes(
                b.to_vec()
            ))
        }
        DataType::Date32 => column!(Date32Array, ColumnValue::Date),
        DataType::Time32(TimeUnit::Millisecond) => {
            column!(Time32MillisecondArray, |v| ColumnValue::Int64(
                i64::from(v) * 1000
            ))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            column!(Time64MicrosecondArray, ColumnValue::Int64)
        }
        DataType::Date64 => column!(Date64Array, |ms| ColumnValue::Date(
            (ms / 86_400_000) as i32
        )),
        DataType::Decimal128(_, _) => column!(Decimal128Array, ColumnValue::Decimal),
        DataType::Timestamp(unit, _tz) => {
            let to_value = |m: i64| match field_type {
                FieldType::TimestampTz => ColumnValue::TimestampTz(m),
                _ => ColumnValue::Timestamp(m),
            };
            match unit {
                TimeUnit::Second => column!(TimestampSecondArray, |v| to_value(v * 1_000_000)),
                TimeUnit::Millisecond => {
                    column!(TimestampMillisecondArray, |v| to_value(v * 1_000))
                }
                TimeUnit::Microsecond => column!(TimestampMicrosecondArray, to_value),
                TimeUnit::Nanosecond => column!(TimestampNanosecondArray, |v| to_value(
                    fluree_db_core::temporal::micros_from_epoch_nanos(v)
                )),
            }
        }
        _ => vec![None; num_rows],
    }
}

/// N2 kill switch (`FLUREE_ARROW_DIRECT_DECODE`, default **on**). Off
/// (`0`/`false`/`off`/`no`, trimmed + case-insensitive per the switch family)
/// restores the byte-identical two-hop `arrow_column_to_values` +
/// `build_columns_from_values` path. Read once per process (`OnceLock`, the family
/// idiom): set it at startup, not per query. id=3717339924: the falsy-spelling parse
/// is the shared crate-local `env_switch_enabled` helper, not a hand-rolled match.
fn direct_decode_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| crate::env_switch_enabled("FLUREE_ARROW_DIRECT_DECODE"))
}

/// N2 (Gap-4 P1): build a [`Column`] DIRECTLY from an Arrow array, skipping the
/// per-cell `Vec<Option<ColumnValue>>` intermediate that the two-hop
/// `arrow_column_to_values` then `build_columns_from_values` allocate — the
/// "two-hop decode copy" the P0 profile measured at ~33–46% of on-CPU wall on the
/// fused GROUP BY family (`audit-2026-07/gap4-adjudication/P0-PROFILE.md`). For
/// string keys this also drops one of the two per-cell `String` allocations.
///
/// The fast arms cover the case where the Arrow physical type MATCHES the target
/// `field_type`; each is value-identical to the two-hop composition for that case
/// (`Arrow → ColumnValue → Column`), which the `direct_decode_matches_two_hop`
/// differential asserts cell-for-cell. ANY other (arrow-type, field_type) pair —
/// cross-type coercions (e.g. a `Decimal`-typed field physically stored INT64, a
/// `Date` field stored plain INT32), timestamps (unit-scaled), or unhandled types
/// — falls back to the exact old mapping via `column_from_values`, so behavior is
/// unchanged there.
fn arrow_column_to_column(array: &dyn Array, field_type: &FieldType, num_rows: usize) -> Column {
    // Downcast to the concrete typed array once, then map each row through `$conv`,
    // preserving nulls — producing the target `Column` variant with no intermediate.
    macro_rules! direct {
        ($ty:ty, $variant:path, $conv:expr) => {
            array.as_any().downcast_ref::<$ty>().map(|a| {
                $variant(
                    (0..num_rows)
                        .map(|i| (!a.is_null(i)).then(|| $conv(a.value(i))))
                        .collect(),
                )
            })
        };
    }

    let built: Option<Column> = match (field_type, array.data_type()) {
        (FieldType::Boolean, DataType::Boolean) => direct!(BooleanArray, Column::Boolean, |v| v),
        (FieldType::Int32, DataType::Int32) => direct!(Int32Array, Column::Int32, |v| v),
        (FieldType::Int64, DataType::Int64) => direct!(Int64Array, Column::Int64, |v| v),
        (FieldType::Float32, DataType::Float32) => direct!(Float32Array, Column::Float32, |v| v),
        (FieldType::Float64, DataType::Float64) => direct!(Float64Array, Column::Float64, |v| v),
        (FieldType::String, DataType::Utf8) => {
            direct!(StringArray, Column::String, |s: &str| s.to_string())
        }
        (FieldType::String, DataType::LargeUtf8) => {
            direct!(LargeStringArray, Column::String, |s: &str| s.to_string())
        }
        (FieldType::Bytes, DataType::Binary) => {
            direct!(BinaryArray, Column::Bytes, |b: &[u8]| b.to_vec())
        }
        (FieldType::Bytes, DataType::LargeBinary) => {
            direct!(LargeBinaryArray, Column::Bytes, |b: &[u8]| b.to_vec())
        }
        (FieldType::Bytes, DataType::FixedSizeBinary(_)) => {
            direct!(FixedSizeBinaryArray, Column::Bytes, |b: &[u8]| b.to_vec())
        }
        (FieldType::Date, DataType::Date32) => direct!(Date32Array, Column::Date, |v| v),
        (FieldType::Decimal { precision, scale }, DataType::Decimal128(_, _)) => array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .map(|a| Column::Decimal {
                values: (0..num_rows)
                    .map(|i| (!a.is_null(i)).then(|| a.value(i)))
                    .collect(),
                precision: *precision,
                scale: *scale,
            }),
        // Cross-type coercions, timestamps (unit-scaled), unhandled types: the
        // exact old two-hop mapping, so the direct path never diverges.
        _ => None,
    };
    built.unwrap_or_else(|| {
        column_from_values(
            &arrow_column_to_values(array, field_type, num_rows),
            field_type,
        )
    })
}

/// One resolved comparison in the row filter: the column's position within the
/// decoded `RecordBatch`, the operator, and the literal to compare against.
type ResolvedComparison = (usize, ComparisonOp, LiteralValue);

/// Resolve a residual predicate into a plan evaluated against each decoded
/// `RecordBatch` (of the main projection). Returns `None` when the predicate is
/// not a plain conjunction of column comparisons (the only shape the R2RML →
/// Iceberg bridge emits) or references no projected, mappable column.
///
/// A comparison on a column that is not projected is dropped, which only
/// weakens the filter (keeps more rows) — safe, since the in-engine FILTER
/// remains the authority. `field_id_to_leaf` maps an Iceberg field ID to its
/// parquet leaf index; `leaf_to_pos` maps that leaf index to its position in
/// the decoded `RecordBatch`.
fn build_predicate_plan(
    residual: &Expression,
    field_id_to_leaf: &HashMap<i32, usize>,
    leaf_to_pos: &HashMap<usize, usize>,
) -> Option<Vec<ResolvedComparison>> {
    let mut comparisons = Vec::new();
    if !collect_and_comparisons(residual, &mut comparisons) || comparisons.is_empty() {
        return None;
    }

    // field_id → parquet leaf → RecordBatch position; drop any we cannot resolve
    // to a projected column.
    let plan: Vec<ResolvedComparison> = comparisons
        .into_iter()
        .filter_map(|(field_id, op, value)| {
            let leaf = field_id_to_leaf.get(&field_id)?;
            let pos = leaf_to_pos.get(leaf)?;
            Some((*pos, op, value))
        })
        .collect();

    (!plan.is_empty()).then_some(plan)
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
        // An `In`/`NotIn` set filter has no row-level Arrow representation here.
        // Treat it as transparent (don't push, don't abort) so a sibling
        // comparison in the same `And` still builds its row filter. Sound: the row
        // filter may only ever keep MORE rows than the true predicate — file /
        // row-group pruning (which DO evaluate `In`) and the in-engine FILTER stay
        // the authority. A lone `In` yields an empty plan ⇒ no row filter, exactly
        // as before this arm existed.
        Expression::In { .. } | Expression::NotIn { .. } => true,
        _ => false,
    }
}

/// One row-filter comparison prepared against its decoded column: the
/// column's position, the operator, and the literal converted EXACTLY into the
/// column's comparison type — `None` when no exact conversion exists, in which
/// case the comparison keeps every row and the in-engine FILTER decides.
struct PreparedComparison {
    pos: usize,
    op: ComparisonOp,
    literal: Option<Scalar<ArrayRef>>,
}

/// Prepare `plan` against the schema of a decoded batch (see
/// [`PreparedComparison`]).
fn prepare_comparisons(
    plan: &[ResolvedComparison],
    schema: &arrow::datatypes::Schema,
) -> Vec<PreparedComparison> {
    plan.iter()
        .map(|(pos, op, value)| {
            let target = comparison_type(schema.field(*pos).data_type());
            let literal = exact_literal(value, &target);
            if literal.is_none() {
                tracing::debug!(
                    literal = ?value,
                    column_type = ?target,
                    "row filter: the literal has no exact value in the column type, so this \
                     comparison keeps every row and the in-engine FILTER decides"
                );
            }
            PreparedComparison {
                pos: *pos,
                op: *op,
                literal: literal.map(Scalar::new),
            }
        })
        .collect()
}

/// Evaluate the conjunction over a decoded `RecordBatch`, ANDing the per-
/// comparison masks into a keep mask for `filter_record_batch`. A null cell
/// yields a null mask entry, which `filter_record_batch` treats as "drop" —
/// correct for R2RML, where a null column produces no triple. `None` means no
/// comparison could narrow the batch: keep every row.
fn eval_conjunction(
    batch: &RecordBatch,
    plan: &[PreparedComparison],
) -> std::result::Result<Option<BooleanArray>, ArrowError> {
    let mut acc: Option<BooleanArray> = None;
    for comparison in plan {
        let Some(literal) = &comparison.literal else {
            continue;
        };
        let column = normalize_column(batch.column(comparison.pos))?;
        let mask = match comparison.op {
            ComparisonOp::Eq => cmp::eq(&column, literal),
            ComparisonOp::NotEq => cmp::neq(&column, literal),
            ComparisonOp::Lt => cmp::lt(&column, literal),
            ComparisonOp::LtEq => cmp::lt_eq(&column, literal),
            ComparisonOp::Gt => cmp::gt(&column, literal),
            ComparisonOp::GtEq => cmp::gt_eq(&column, literal),
        }?;
        acc = Some(match acc {
            Some(prev) => and(&prev, &mask)?,
            None => mask,
        });
    }
    Ok(acc)
}

/// The type a column is compared in. Our tabular time representation is an
/// integer count of microseconds, so time columns compare as `Int64` micros —
/// a microsecond literal must never be read as milliseconds by a cast to
/// `Time32`. Every other column compares in its own type.
fn comparison_type(column_type: &DataType) -> DataType {
    match column_type {
        DataType::Time32(TimeUnit::Millisecond) | DataType::Time64(TimeUnit::Microsecond) => {
            DataType::Int64
        }
        other => other.clone(),
    }
}

/// `column` in its [`comparison_type`].
fn normalize_column(column: &ArrayRef) -> std::result::Result<ArrayRef, ArrowError> {
    match column.data_type() {
        DataType::Time32(TimeUnit::Millisecond) => cast(
            &cast(column, &DataType::Time64(TimeUnit::Microsecond))?,
            &DataType::Int64,
        ),
        DataType::Time64(TimeUnit::Microsecond) => cast(column, &DataType::Int64),
        _ => Ok(Arc::clone(column)),
    }
}

/// The literal as a single-element array of `target`, or `None` when it has
/// no exact value there.
///
/// Arrow's cast cannot be trusted to say so: reducing a decimal's scale
/// ROUNDS (half away from zero), and a value outside the target's range comes
/// back NULL rather than as an error. Either would make the row filter drop
/// rows that truly match (`?d < 1.0500000001` on `decimal(38,9)` losing
/// `1.050000000`; `?d < 1000` on `decimal(5,2)` losing every row), and a
/// dropped row never reaches the in-engine FILTER. So a conversion is accepted
/// only when it is non-null and casts back to the original literal unchanged.
fn exact_literal(value: &LiteralValue, target: &DataType) -> Option<ArrayRef> {
    let literal = literal_to_array(value)?;
    let converted = cast(&literal, target).ok()?;
    if converted.is_null(0) {
        return None;
    }
    let back = cast(&converted, literal.data_type()).ok()?;
    let same = cmp::eq(&back, &literal).ok()?;
    (same.is_valid(0) && same.value(0)).then_some(converted)
}

/// A single-element Arrow array holding the literal, in a natural type that
/// `cast` can convert to the target column type. `None` for a decimal that no
/// `decimal128` can hold.
fn literal_to_array(value: &LiteralValue) -> Option<ArrayRef> {
    Some(match value {
        LiteralValue::Boolean(b) => Arc::new(BooleanArray::from(vec![*b])),
        LiteralValue::Int32(i) => Arc::new(Int32Array::from(vec![*i])),
        LiteralValue::Int64(i) => Arc::new(Int64Array::from(vec![*i])),
        LiteralValue::Float32(f) => Arc::new(Float32Array::from(vec![*f])),
        LiteralValue::Float64(f) => Arc::new(Float64Array::from(vec![*f])),
        LiteralValue::String(s) => Arc::new(StringArray::from(vec![s.clone()])),
        LiteralValue::Bytes(b) => Arc::new(BinaryArray::from(vec![b.as_slice()])),
        LiteralValue::Date(d) => Arc::new(Date32Array::from(vec![*d])),
        // Both timestamp flavors carry micros-since-epoch; `cast` adapts to the
        // column's own (tz-tagged or not) timestamp type, and `exact_literal`
        // refuses an adaptation that loses anything.
        LiteralValue::Timestamp(t) | LiteralValue::TimestampTz(t) => {
            Arc::new(TimestampMicrosecondArray::from(vec![*t]))
        }
        LiteralValue::Decimal { unscaled, scale } => decimal_literal(*unscaled, *scale)?,
    })
}

/// `unscaled × 10^-scale` as a `decimal128(38, s)`. A negative scale (`4E+2`
/// normalizes to unscaled 4, scale -2) is rescaled exactly to scale 0. `None`
/// when the value needs more than 38 digits or a scale above 38.
fn decimal_literal(unscaled: i128, scale: i8) -> Option<ArrayRef> {
    use arrow::datatypes::{DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE};
    let (unscaled, scale) = match u32::try_from(scale) {
        Ok(_) => (unscaled, scale),
        Err(_) => (
            unscaled.checked_mul(10i128.checked_pow(u32::from(scale.unsigned_abs()))?)?,
            0,
        ),
    };
    if scale > DECIMAL128_MAX_SCALE {
        return None;
    }
    let array = Decimal128Array::from(vec![unscaled])
        .with_precision_and_scale(DECIMAL128_MAX_PRECISION, scale)
        .ok()?;
    array
        .validate_decimal_precision(DECIMAL128_MAX_PRECISION)
        .ok()?;
    Some(Arc::new(array))
}

#[cfg(test)]
mod direct_decode_tests {
    use super::*;

    /// A nanosecond timestamp is held to the microsecond at or before it, as
    /// a dateTime is, before 1970 too.
    #[test]
    fn nanosecond_timestamps_round_down_to_the_microsecond() {
        let nanos = TimestampNanosecondArray::from(vec![1_500, -1_500, -1_000, 0, -1]);
        let values = arrow_column_to_values(&nanos, &FieldType::Timestamp, 5);
        let micros: Vec<i64> = values
            .into_iter()
            .map(|v| match v {
                Some(ColumnValue::Timestamp(m)) => m,
                other => panic!("not a timestamp: {other:?}"),
            })
            .collect();
        assert_eq!(micros, [1, -2, -1, 0, -1]);
    }

    /// Exercise the production decoder with compressed, delta-packed integer
    /// pages. Millisecond time values must be normalized before BOTH filtering
    /// and conversion; comparing raw millisecond statistics to a microsecond
    /// literal would incorrectly discard the matching row groups.
    #[test]
    fn compressed_time_columns_filter_in_microseconds() {
        use arrow::datatypes::{Field, Schema as ArrowSchema};
        use bytes::Bytes;
        use parquet::arrow::ArrowWriter;
        use parquet::basic::{Compression, Encoding, GzipLevel, ZstdLevel};
        use parquet::file::properties::WriterProperties;
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let times: Vec<ArrayRef> = vec![
            Arc::new(Time32MillisecondArray::from(vec![
                Some(500),
                None,
                Some(1500),
                Some(2500),
                Some(3500),
            ])),
            Arc::new(Time64MicrosecondArray::from(vec![
                Some(500_000),
                None,
                Some(1_500_000),
                Some(2_500_000),
                Some(3_500_000),
            ])),
        ];
        for compression in [
            Compression::GZIP(GzipLevel::default()),
            Compression::SNAPPY,
            Compression::ZSTD(ZstdLevel::default()),
        ] {
            for time in &times {
                let schema = Arc::new(ArrowSchema::new(vec![
                    Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([(
                        "PARQUET:field_id".to_owned(),
                        "42".to_owned(),
                    )])),
                    Field::new("time", time.data_type().clone(), true).with_metadata(
                        HashMap::from([("PARQUET:field_id".to_owned(), "87".to_owned())]),
                    ),
                ]));
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                        time.clone(),
                    ],
                )
                .unwrap();
                let props = WriterProperties::builder()
                    .set_compression(compression)
                    .set_dictionary_enabled(false)
                    .set_encoding(Encoding::DELTA_BINARY_PACKED)
                    .set_max_row_group_row_count(Some(2))
                    .build();
                let mut buf = Vec::new();
                let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
                writer.write(&batch).unwrap();
                writer.close().unwrap();
                let bytes = Bytes::from(buf);
                let reader = SerializedFileReader::new(bytes.clone()).unwrap();
                assert_eq!(reader.metadata().num_row_groups(), 3);
                assert_eq!(
                    reader.metadata().row_group(0).column(1).compression(),
                    compression
                );
                let row_times: Vec<_> = reader
                    .get_row_iter(None)
                    .unwrap()
                    .map(|row| {
                        let row = row.unwrap();
                        let field = row.get_column_iter().nth(1).unwrap().1;
                        match crate::io::parquet::convert_field_to_column_value(
                            field,
                            &FieldType::Int64,
                        ) {
                            Some(ColumnValue::Int64(value)) => Some(value),
                            None => None,
                            other => panic!("unexpected time value: {other:?}"),
                        }
                    })
                    .collect();
                assert_eq!(
                    row_times,
                    vec![
                        Some(500_000),
                        None,
                        Some(1_500_000),
                        Some(2_500_000),
                        Some(3_500_000)
                    ]
                );

                let filter = Expression::Comparison {
                    field_id: 87,
                    column: "time".to_owned(),
                    op: ComparisonOp::GtEq,
                    value: LiteralValue::Int64(1_750_000),
                };
                let batches =
                    decode_batches_arrow(bytes, &[42, 87], Some(&filter), None, None, None)
                        .unwrap();
                let mut actual = Vec::new();
                for batch in batches {
                    assert_eq!(
                        batch.schema.field_by_id(87).unwrap().field_type,
                        FieldType::Int64
                    );
                    for row in 0..batch.num_rows {
                        actual.push((
                            batch.column_by_id(42).unwrap().get_i64(row),
                            batch.column_by_id(87).unwrap().get_i64(row),
                        ));
                    }
                }
                assert_eq!(
                    actual,
                    vec![(Some(4), Some(2_500_000)), (Some(5), Some(3_500_000))],
                    "{compression:?} / {:?}",
                    time.data_type()
                );
            }
        }
    }

    /// N2 differential: the direct Arrow->Column decoder must produce a `Column`
    /// value-identical to the two-hop `arrow_column_to_values` +
    /// `column_from_values` path. `Column` has no `PartialEq`, so we compare its
    /// `Debug` form (a faithful value rendering for these primitive-typed columns).
    fn assert_direct_matches(array: &dyn Array, ft: FieldType) {
        let n = array.len();
        let direct = arrow_column_to_column(array, &ft, n);
        let two_hop = column_from_values(&arrow_column_to_values(array, &ft, n), &ft);
        assert_eq!(
            format!("{direct:?}"),
            format!("{two_hop:?}"),
            "direct vs two-hop diverge for field_type {ft:?}"
        );
    }

    #[test]
    fn direct_decode_matches_two_hop() {
        // Fast-arm cases (Arrow physical type matches the field type), each with a
        // null and a boundary value.
        assert_direct_matches(
            &BooleanArray::from(vec![Some(true), None, Some(false)]),
            FieldType::Boolean,
        );
        assert_direct_matches(
            &Int32Array::from(vec![Some(1), None, Some(i32::MIN)]),
            FieldType::Int32,
        );
        assert_direct_matches(
            &Int64Array::from(vec![Some(1_000_000_000_000i64), None, Some(i64::MAX)]),
            FieldType::Int64,
        );
        assert_direct_matches(
            &Float32Array::from(vec![Some(1.5f32), None]),
            FieldType::Float32,
        );
        assert_direct_matches(
            &Float64Array::from(vec![Some(2.5f64), None, Some(f64::NAN)]),
            FieldType::Float64,
        );
        assert_direct_matches(
            &StringArray::from(vec![Some("web"), None, Some("mobile")]),
            FieldType::String,
        );
        assert_direct_matches(
            &LargeStringArray::from(vec![Some("x"), None]),
            FieldType::String,
        );
        assert_direct_matches(
            &Date32Array::from(vec![Some(19_000), None]),
            FieldType::Date,
        );
        let dec = Decimal128Array::from(vec![Some(12_345i128), None])
            .with_precision_and_scale(10, 2)
            .unwrap();
        assert_direct_matches(
            &dec,
            FieldType::Decimal {
                precision: 10,
                scale: 2,
            },
        );

        // Cross-type coercions must hit the fallback and stay identical: an
        // Int64-typed field physically stored INT32, and a small-precision Decimal
        // field physically stored INT64.
        assert_direct_matches(&Int32Array::from(vec![Some(5), None]), FieldType::Int64);
        assert_direct_matches(
            &Int64Array::from(vec![Some(9_999i64), None]),
            FieldType::Decimal {
                precision: 6,
                scale: 2,
            },
        );
        // A Date field physically stored as plain INT32 (fallback, tags as Date).
        assert_direct_matches(&Int32Array::from(vec![Some(19_001), None]), FieldType::Date);
    }
}

#[cfg(test)]
mod row_filter_tests {
    use super::*;
    use arrow::datatypes::{Field, Schema as ArrowSchema};

    const OPS: [ComparisonOp; 6] = [
        ComparisonOp::Eq,
        ComparisonOp::NotEq,
        ComparisonOp::Lt,
        ComparisonOp::LtEq,
        ComparisonOp::Gt,
        ComparisonOp::GtEq,
    ];

    /// The row positions the row filter keeps for `column op literal`.
    fn kept(column: ArrayRef, op: ComparisonOp, literal: LiteralValue) -> Vec<usize> {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "c",
            column.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![column]).unwrap();
        let plan = vec![(0, op, literal)];
        let prepared = prepare_comparisons(&plan, &batch.schema());
        match eval_conjunction(&batch, &prepared).unwrap() {
            None => (0..batch.num_rows()).collect(),
            Some(mask) => (0..mask.len())
                .filter(|&i| mask.is_valid(i) && mask.value(i))
                .collect(),
        }
    }

    fn holds(op: ComparisonOp, ord: std::cmp::Ordering) -> bool {
        use std::cmp::Ordering::{Equal, Greater, Less};
        match op {
            ComparisonOp::Eq => ord == Equal,
            ComparisonOp::NotEq => ord != Equal,
            ComparisonOp::Lt => ord == Less,
            ComparisonOp::LtEq => ord != Greater,
            ComparisonOp::Gt => ord == Greater,
            ComparisonOp::GtEq => ord != Less,
        }
    }

    /// `a × 10^-sa` against `b × 10^-sb`, in exact integer arithmetic.
    fn decimal_ord(a: i128, sa: i8, b: i128, sb: i8) -> std::cmp::Ordering {
        let scale = sa.max(sb);
        let up = |v: i128, s: i8| v * 10i128.pow(u32::try_from(scale - s).unwrap());
        up(a, sa).cmp(&up(b, sb))
    }

    fn decimal_column(values: &[i128], precision: u8, scale: i8) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(values.to_vec())
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        )
    }

    /// Every op over a `decimal(38,9)` column — BigQuery's `NUMERIC` — against
    /// literals that sit on, between and beside its grid. A literal the column
    /// can hold exactly must keep exactly the true matches; one it cannot must
    /// keep every row (never drop a true match: the in-engine FILTER cannot
    /// restore a row the row filter dropped).
    #[test]
    fn decimal_literals_keep_exactly_the_true_matches_or_everything() {
        // 0.049999999, 0.050000000, 0.050000001, 1.050000000, -0.050000000
        let values = [
            49_999_999,
            50_000_000,
            50_000_001,
            1_050_000_000,
            -50_000_000,
        ];
        let literals: [(i128, i8, bool); 7] = [
            // (unscaled, scale, holds exactly in decimal(38,9))
            (5, 2, true),                // 0.05: below one — panicked before
            (-5, 2, true),               // -0.05
            (500_000_001, 10, false),    // 0.0500000001: rounds to 0.050000000
            (499_999_999, 10, false),    // 0.0499999999: rounds to 0.050000000
            (10_500_000_001, 10, false), // 1.0500000001: valid precision, still rounds
            (4, -2, true),               // 4E+2, a negative scale
            (1, 50, false),              // 1E-50: no decimal128 holds it
        ];
        for (unscaled, scale, exact) in literals {
            for op in OPS {
                let got = kept(
                    decimal_column(&values, 38, 9),
                    op,
                    LiteralValue::Decimal { unscaled, scale },
                );
                // An exact literal keeps exactly the true matches; one the column
                // cannot hold keeps every row, whatever the truth.
                let want: Vec<usize> = if exact {
                    (0..values.len())
                        .filter(|&i| holds(op, decimal_ord(values[i], 9, unscaled, scale)))
                        .collect()
                } else {
                    (0..values.len()).collect()
                };
                assert_eq!(got, want, "{op:?} {unscaled}e-{scale} (exact: {exact})");
            }
        }
    }

    /// A literal outside the column's range is not representable there. Arrow
    /// casts it to NULL rather than failing, and a NULL comparison drops the
    /// row — so before this fix `?d < 1000` on `decimal(5,2)` returned nothing.
    #[test]
    fn an_out_of_range_literal_keeps_every_row() {
        let column = || decimal_column(&[99_999, 100, -99_999], 5, 2);
        for literal in [
            LiteralValue::Decimal {
                unscaled: 1000,
                scale: 0,
            },
            LiteralValue::Int64(1000),
        ] {
            for op in OPS {
                assert_eq!(
                    kept(column(), op, literal.clone()),
                    [0, 1, 2],
                    "{op:?} {literal:?}"
                );
            }
        }
        // An integer too wide for an `int` column, the same way.
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        assert_eq!(
            kept(ints, ComparisonOp::Lt, LiteralValue::Int64(3_000_000_000)),
            [0, 1]
        );
    }

    /// The exactness check is not decimal-specific: any cast that loses
    /// information is refused, e.g. a sub-millisecond instant against a
    /// millisecond timestamp column (the cast truncates toward the epoch).
    #[test]
    fn a_lossy_conversion_of_any_type_is_refused() {
        let millis: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1_000]));
        // 1.0005 s as micros. `< 1.0005 s` holds for the 1.000 s row.
        assert_eq!(
            kept(millis, ComparisonOp::Lt, LiteralValue::Timestamp(1_000_500)),
            [0]
        );
        assert!(exact_literal(
            &LiteralValue::Timestamp(1_000_500),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        )
        .is_none());
        assert!(exact_literal(
            &LiteralValue::Timestamp(1_000_000),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        )
        .is_some());
    }

    /// Exact literals still narrow: the conversion is not a blanket keep-all.
    #[test]
    fn an_exact_literal_still_filters() {
        let column = decimal_column(&[99_999, 100, 5], 5, 2);
        let five_cents = LiteralValue::Decimal {
            unscaled: 5,
            scale: 2,
        };
        assert_eq!(kept(column, ComparisonOp::Gt, five_cents), [0, 1]);
    }
}
