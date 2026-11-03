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
/// column's position and what to check there.
struct PreparedComparison {
    pos: usize,
    check: Check,
}

/// What a prepared comparison does to a batch.
enum Check {
    /// Compare each cell with a literal of the column's comparison type.
    Compare(ComparisonOp, Scalar<ArrayRef>),
    /// No literal compares exactly with the column: keep every row, and the
    /// in-engine FILTER decides.
    KeepAll,
    /// No cell can satisfy the comparison (an equality with a value the column
    /// cannot hold).
    DropAll,
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
            let check = match column_kind(&target) {
                Some(column) if comparable(*op, literal_kind(value), column) => {
                    match decimal_check(*op, value, &target) {
                        Some(check) => check,
                        None => exact_literal(value, &target)
                            .and_then(|literal| total_order_zero(*op, literal))
                            .map_or(Check::KeepAll, |literal| {
                                Check::Compare(*op, Scalar::new(literal))
                            }),
                    }
                }
                _ => Check::KeepAll,
            };
            if matches!(check, Check::KeepAll) {
                tracing::debug!(
                    literal = ?value,
                    column_type = ?target,
                    "row filter: the literal does not compare exactly with the column, so \
                     this comparison keeps every row and the in-engine FILTER decides"
                );
            }
            PreparedComparison { pos: *pos, check }
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
        let mask = match &comparison.check {
            Check::KeepAll => continue,
            Check::DropAll => BooleanArray::new(
                arrow::buffer::BooleanBuffer::new_unset(batch.num_rows()),
                None,
            ),
            Check::Compare(op, literal) => {
                let column = normalize_column(batch.column(comparison.pos))?;
                match op {
                    ComparisonOp::Eq => cmp::eq(&column, literal),
                    ComparisonOp::NotEq => cmp::neq(&column, literal),
                    ComparisonOp::Lt => cmp::lt(&column, literal),
                    ComparisonOp::LtEq => cmp::lt_eq(&column, literal),
                    ComparisonOp::Gt => cmp::gt(&column, literal),
                    ComparisonOp::GtEq => cmp::gt_eq(&column, literal),
                }?
            }
        };
        acc = Some(match acc {
            Some(prev) => and(&prev, &mask)?,
            None => mask,
        });
    }
    Ok(acc)
}

/// The kind of value a literal holds or a column compares as, for deciding
/// whether the row filter may compare the two at all (see [`comparable`]).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ValueKind {
    Boolean,
    Integer,
    Decimal,
    Float,
    Text,
    Binary,
    Temporal,
}

fn literal_kind(value: &LiteralValue) -> ValueKind {
    match value {
        LiteralValue::Boolean(_) => ValueKind::Boolean,
        LiteralValue::Int32(_) | LiteralValue::Int64(_) => ValueKind::Integer,
        LiteralValue::Decimal { .. } => ValueKind::Decimal,
        LiteralValue::Float32(_) | LiteralValue::Float64(_) => ValueKind::Float,
        LiteralValue::String(_) => ValueKind::Text,
        LiteralValue::Bytes(_) => ValueKind::Binary,
        LiteralValue::Date(_) | LiteralValue::Timestamp(_) | LiteralValue::TimestampTz(_) => {
            ValueKind::Temporal
        }
    }
}

/// The kind a column of comparison type `t` compares as; `None` for a type the
/// row filter does not compare (`Float16` among them: no Iceberg type decodes
/// to it, and [`total_order_zero`] does not cover it).
fn column_kind(t: &DataType) -> Option<ValueKind> {
    use DataType as D;
    Some(match t {
        D::Boolean => ValueKind::Boolean,
        D::Int8 | D::Int16 | D::Int32 | D::Int64 | D::UInt8 | D::UInt16 | D::UInt32 | D::UInt64 => {
            ValueKind::Integer
        }
        D::Decimal128(..) | D::Decimal256(..) => ValueKind::Decimal,
        D::Float32 | D::Float64 => ValueKind::Float,
        D::Utf8 | D::LargeUtf8 | D::Utf8View => ValueKind::Text,
        D::Binary | D::LargeBinary | D::BinaryView | D::FixedSizeBinary(_) => ValueKind::Binary,
        D::Date32 | D::Date64 | D::Timestamp(..) => ValueKind::Temporal,
        _ => return None,
    })
}

/// Whether the row filter may compare a `literal` with a `column` by `op`:
/// whether the column's own comparison keeps every row the engine's FILTER
/// does.
///
/// An exact conversion is not enough. An integer literal converts exactly to a
/// string column's `"1000"`, and the column then compares as text, where the
/// engine reads an `xsd:integer` map over that column as numbers: `"999" <
/// "1000"` is false, so the row filter would drop the 999 the FILTER keeps.
/// Likewise `true` against a column holding `"1"`. So a literal compares only
/// with a column of its own kind, or across kinds where both sides compare by
/// value: integers with decimals and with floats (`exact_literal` refuses a
/// value the column type cannot hold). A decimal and a float do not: a decimal
/// round-trips through a float by rounding back (0.1), while the engine
/// compares the two exactly.
///
/// A string compares with a date or timestamp column by any operator: the
/// engine orders a plain string against a date or dateTime by parsing it as
/// that type, which is what the cast does, and `exact_literal` accepts only
/// the column's own rendering of a value. Against any other column a string
/// compares only by `=`, where the round trip makes it the rendering of the
/// one value it matches (`"10"`, never `"010"`): a cell that renders as the
/// string is that value. Ordering it would compare values where the engine
/// compares the string, or nothing at all.
fn comparable(op: ComparisonOp, literal: ValueKind, column: ValueKind) -> bool {
    use ValueKind as K;
    literal == column
        || matches!(
            (literal, column),
            (K::Integer, K::Decimal | K::Float)
                | (K::Decimal | K::Float, K::Integer)
                | (K::Text, K::Temporal)
        )
        || (literal == K::Text && op == ComparisonOp::Eq && column != K::Binary)
}

/// A float column compared with a zero literal, as the zero whose comparison
/// the column's kernels agree on with the engine; `None` to keep every row.
///
/// arrow-ord's float comparisons follow IEEE 754 totalOrder, which puts -0.0
/// below 0.0, where the engine (like SPARQL) holds the two equal: `?d >= 0`
/// keeps a -0.0 cell in the engine, and `cmp::gt_eq(-0.0, 0.0)` is false. For
/// the ordering operators one signed zero agrees: `< -0.0` and `>= -0.0` split
/// the negatives from both zeros, `<= 0.0` and `> 0.0` both zeros from the
/// positives. Equality would need both zeros, so it keeps every row. Any other
/// literal orders against both zeros alike, and against NaN cells totalOrder
/// can only keep extra rows (the engine compares nothing with NaN).
fn total_order_zero(op: ComparisonOp, literal: ArrayRef) -> Option<ArrayRef> {
    use arrow::array::AsArray;
    use arrow::datatypes::{Float32Type, Float64Type};
    let negative = match op {
        ComparisonOp::Lt | ComparisonOp::GtEq => true,
        ComparisonOp::LtEq | ComparisonOp::Gt => false,
        ComparisonOp::Eq | ComparisonOp::NotEq => {
            let zero = match literal.data_type() {
                DataType::Float64 => literal.as_primitive::<Float64Type>().value(0) == 0.0,
                DataType::Float32 => literal.as_primitive::<Float32Type>().value(0) == 0.0,
                _ => false,
            };
            return (!zero).then_some(literal);
        }
    };
    Some(match literal.data_type() {
        DataType::Float64 if literal.as_primitive::<Float64Type>().value(0) == 0.0 => {
            Arc::new(Float64Array::from(vec![if negative { -0.0 } else { 0.0 }]))
        }
        DataType::Float32 if literal.as_primitive::<Float32Type>().value(0) == 0.0 => {
            Arc::new(Float32Array::from(vec![if negative { -0.0 } else { 0.0 }]))
        }
        _ => literal,
    })
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

/// An integer or decimal literal against a `decimal128(p, s)` column, as the
/// check on the column's grid that keeps exactly the rows the comparison
/// holds for; `None` for any other literal or column type.
///
/// Computed in integer arithmetic on the column's scale, never by a cast. A
/// literal on the grid compares as itself, even beyond the column's
/// precision (the kernels compare the unscaled values, so `?d < 1000` on
/// `decimal(5,2)` keeps every row and `> 1000` none). A literal between two
/// grid points `lo < x < hi` holds for exactly the cells at or below `lo`
/// (`<`, `<=`) or at or above `hi` (`>`, `>=`): `< 1.0500000001` on
/// `decimal(38,9)` is `<= 1.050000000`, and `> 1.0499999999` is
/// `>= 1.050000000`. No cell equals it, so `=` drops every row and `!=` keeps
/// every row. A grid point beyond `i128` saturates, which can only keep a cell
/// it should not.
fn decimal_check(op: ComparisonOp, value: &LiteralValue, target: &DataType) -> Option<Check> {
    let DataType::Decimal128(precision, column_scale) = *target else {
        return None;
    };
    let (unscaled, scale) = match *value {
        LiteralValue::Int32(n) => (i128::from(n), 0),
        LiteralValue::Int64(n) => (i128::from(n), 0),
        LiteralValue::Decimal { unscaled, scale } => (unscaled, scale),
        _ => return None,
    };
    let (lo, hi, exact) = grid_neighbors(unscaled, scale, column_scale);
    let (op, units) = if exact {
        (op, lo)
    } else {
        match op {
            ComparisonOp::Lt | ComparisonOp::LtEq => (ComparisonOp::LtEq, lo),
            ComparisonOp::Gt | ComparisonOp::GtEq => (ComparisonOp::GtEq, hi),
            ComparisonOp::Eq => return Some(Check::DropAll),
            ComparisonOp::NotEq => return Some(Check::KeepAll),
        }
    };
    let literal = Decimal128Array::from(vec![units])
        .with_precision_and_scale(precision, column_scale)
        .ok()?;
    Some(Check::Compare(op, Scalar::new(Arc::new(literal))))
}

/// `unscaled × 10^-scale` on the grid of `column_scale`, in its units: the
/// grid points just at or below and at or above it, and whether it lies on
/// the grid. A value beyond `i128` is off the grid, with both neighbors
/// saturated at that end.
fn grid_neighbors(unscaled: i128, scale: i8, column_scale: i8) -> (i128, i128, bool) {
    let shift = i32::from(column_scale) - i32::from(scale);
    let saturated = if unscaled < 0 { i128::MIN } else { i128::MAX };
    if shift >= 0 {
        let units = 10i128
            .checked_pow(shift.unsigned_abs())
            .and_then(|f| unscaled.checked_mul(f));
        return match units {
            Some(u) => (u, u, true),
            None => (saturated, saturated, false),
        };
    }
    match 10i128.checked_pow(shift.unsigned_abs()) {
        Some(divisor) => {
            let lo = unscaled.div_euclid(divisor);
            let exact = unscaled.rem_euclid(divisor) == 0;
            (lo, if exact { lo } else { lo + 1 }, exact)
        }
        // A divisor beyond i128 exceeds every unscaled value: the literal lies
        // strictly between -1 and 1 grid units (or is 0).
        None => match unscaled.signum() {
            0 => (0, 0, true),
            1 => (0, 1, false),
            _ => (-1, 0, false),
        },
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

    /// `a × 10^-sa` against `b × 10^-sb`, exactly: in `i128` at the common
    /// scale, or by the digits when that overflows.
    fn decimal_ord(a: i128, sa: i8, b: i128, sb: i8) -> std::cmp::Ordering {
        let scale = sa.max(sb);
        let shift = |s: i8| u32::try_from(i32::from(scale) - i32::from(s)).unwrap();
        let up = |v: i128, s: i8| 10i128.checked_pow(shift(s)).and_then(|f| v.checked_mul(f));
        if let (Some(x), Some(y)) = (up(a, sa), up(b, sb)) {
            return x.cmp(&y);
        }
        if a.signum() != b.signum() {
            return a.signum().cmp(&b.signum());
        }
        let digits = |v: i128, s: i8| {
            let mut d = v.unsigned_abs().to_string();
            d.extend(std::iter::repeat_n('0', shift(s) as usize));
            d.trim_start_matches('0').to_string()
        };
        let (x, y) = (digits(a, sa), digits(b, sb));
        let magnitude = x.len().cmp(&y.len()).then_with(|| x.cmp(&y));
        if a < 0 {
            magnitude.reverse()
        } else {
            magnitude
        }
    }

    fn decimal_column(values: &[i128], precision: u8, scale: i8) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(values.to_vec())
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        )
    }

    /// Every op over a `decimal(38,9)` column — BigQuery's `NUMERIC` — against
    /// literals that sit on, between and beside its grid keeps exactly the true
    /// matches: one on the grid compares as itself, one between grid points as
    /// its neighbor on the safe side (never drop a true match: the in-engine
    /// FILTER cannot restore a row the row filter dropped).
    #[test]
    fn decimal_literals_keep_exactly_the_true_matches() {
        // 0.049999999, 0.050000000, 0.050000001, 1.050000000, -0.050000000
        let values = [
            49_999_999,
            50_000_000,
            50_000_001,
            1_050_000_000,
            -50_000_000,
        ];
        let literals: [(i128, i8); 9] = [
            (5, 2),               // 0.05: below one — panicked before
            (-5, 2),              // -0.05
            (500_000_001, 10),    // 0.0500000001: between grid points
            (499_999_999, 10),    // 0.0499999999
            (-500_000_001, 10),   // -0.0500000001
            (10_500_000_001, 10), // 1.0500000001: valid precision, still off the grid
            (4, -2),              // 4E+2, a negative scale
            (1, 50),              // 1E-50: no decimal128 holds it
            (-1, 50),             // -1E-50
        ];
        for (unscaled, scale) in literals {
            for op in OPS {
                let got = kept(
                    decimal_column(&values, 38, 9),
                    op,
                    LiteralValue::Decimal { unscaled, scale },
                );
                let want: Vec<usize> = (0..values.len())
                    .filter(|&i| holds(op, decimal_ord(values[i], 9, unscaled, scale)))
                    .collect();
                assert_eq!(got, want, "{op:?} {unscaled}e-{scale}");
            }
        }
    }

    /// A literal outside the column's range is not representable there. Arrow
    /// casts it to NULL rather than failing, and a NULL comparison drops the
    /// row — so before this fix `?d < 1000` on `decimal(5,2)` returned nothing.
    /// It now compares as itself: below it every row, above it none.
    #[test]
    fn an_out_of_range_literal_keeps_exactly_the_true_matches() {
        let column = || decimal_column(&[99_999, 100, -99_999], 5, 2);
        for literal in [
            LiteralValue::Decimal {
                unscaled: 1000,
                scale: 0,
            },
            LiteralValue::Int64(1000),
            LiteralValue::Int32(1000),
        ] {
            for (op, want) in [
                (ComparisonOp::Eq, vec![]),
                (ComparisonOp::NotEq, vec![0, 1, 2]),
                (ComparisonOp::Lt, vec![0, 1, 2]),
                (ComparisonOp::LtEq, vec![0, 1, 2]),
                (ComparisonOp::Gt, vec![]),
                (ComparisonOp::GtEq, vec![]),
            ] {
                assert_eq!(
                    kept(column(), op, literal.clone()),
                    want,
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

    /// A literal converts exactly to a column whose own order is not the
    /// engine's: an integer to a string column's "1000", which then compares
    /// as text ("999" < "1000" is false), a boolean to "true" (where the cell
    /// holds "1"), a decimal to a double (0.1 rounds back to itself). Those
    /// keep every row; a string against a date column still compares as a
    /// date, which is how the engine orders a plain string against one.
    /// (Equality of an integer with a string cell would be sound for "1000",
    /// but not for "01000", which the engine reads as 1000 too.)
    #[test]
    fn a_literal_compares_only_with_a_column_that_orders_as_the_engine() {
        let text = || -> ArrayRef { Arc::new(StringArray::from(vec!["999", "1000", "1"])) };
        for op in OPS {
            assert_eq!(
                kept(text(), op, LiteralValue::Int64(1000)),
                [0, 1, 2],
                "{op:?}"
            );
            assert_eq!(
                kept(text(), op, LiteralValue::Int32(1000)),
                [0, 1, 2],
                "{op:?}"
            );
            assert_eq!(
                kept(text(), op, LiteralValue::Boolean(true)),
                [0, 1, 2],
                "{op:?}"
            );
            let doubles: ArrayRef = Arc::new(Float64Array::from(vec![0.1, 0.2]));
            let tenth = LiteralValue::Decimal {
                unscaled: 1,
                scale: 1,
            };
            assert_eq!(kept(doubles, op, tenth), [0, 1], "{op:?}");
            let decimals = decimal_column(&[1, 2], 38, 1);
            assert_eq!(
                kept(decimals, op, LiteralValue::Float64(0.1)),
                [0, 1],
                "{op:?}"
            );
            // A string orders against a number nowhere but in a string column;
            // by `=` it matches the one value it renders.
            let ints: ArrayRef = Arc::new(Int64Array::from(vec![1, 1000]));
            let want: &[usize] = if op == ComparisonOp::Eq {
                &[1]
            } else {
                &[0, 1]
            };
            assert_eq!(
                kept(ints, op, LiteralValue::String("1000".into())),
                want,
                "{op:?}"
            );
        }
        // Same kind, or integers against floats and decimals: still compared.
        assert_eq!(
            kept(text(), ComparisonOp::Lt, LiteralValue::String("2".into())),
            [1, 2]
        );
        let doubles: ArrayRef = Arc::new(Float64Array::from(vec![0.5, 2.0]));
        assert_eq!(kept(doubles, ComparisonOp::Gt, LiteralValue::Int64(1)), [1]);
        let days: ArrayRef = Arc::new(Date32Array::from(vec![19_723, 19_724]));
        assert_eq!(
            kept(
                days,
                ComparisonOp::Gt,
                LiteralValue::String("2024-01-01".into())
            ),
            [1]
        );
    }

    /// arrow-ord orders floats by IEEE totalOrder (-0.0 < 0.0); the engine
    /// holds -0.0 = 0. Against every spelling of a zero literal, the ordering
    /// operators keep exactly the IEEE matches (and, as for any literal, a NaN
    /// cell totalOrder puts past it), and equality keeps every row.
    #[test]
    fn a_zero_literal_against_a_float_column_keeps_both_zeros() {
        // -0.0, 0.0, 1.5, -1.5, NaN
        let f64s =
            || -> ArrayRef { Arc::new(Float64Array::from(vec![-0.0, 0.0, 1.5, -1.5, f64::NAN])) };
        let f32s =
            || -> ArrayRef { Arc::new(Float32Array::from(vec![-0.0, 0.0, 1.5, -1.5, f32::NAN])) };
        let zeros = [
            LiteralValue::Int64(0),
            LiteralValue::Int32(0),
            LiteralValue::Float64(0.0),
            LiteralValue::Float64(-0.0),
        ];
        for column in [f64s, f32s] {
            for zero in &zeros {
                for (op, want) in [
                    (ComparisonOp::Lt, vec![3]),
                    (ComparisonOp::LtEq, vec![0, 1, 3]),
                    (ComparisonOp::Gt, vec![2, 4]),
                    (ComparisonOp::GtEq, vec![0, 1, 2, 4]),
                    (ComparisonOp::Eq, vec![0, 1, 2, 3, 4]),
                    (ComparisonOp::NotEq, vec![0, 1, 2, 3, 4]),
                ] {
                    assert_eq!(kept(column(), op, zero.clone()), want, "{op:?} {zero:?}");
                }
            }
            // A non-zero literal orders against both zeros alike.
            assert_eq!(
                kept(column(), ComparisonOp::Gt, LiteralValue::Int64(-1)),
                [0, 1, 2, 4]
            );
            assert_eq!(
                kept(column(), ComparisonOp::Eq, LiteralValue::Int64(-1)),
                Vec::<usize>::new()
            );
        }
    }

    /// Exhaustively, every grid value of a `decimal(2,1)` column against
    /// literals of scale -2 to 4 on, between and around its grid points, its
    /// limits and zero, and beyond `i128`: each op keeps exactly the rows the
    /// comparison holds for, computed in integer arithmetic.
    #[test]
    fn decimal_rounding_is_exact_at_every_boundary() {
        let values: Vec<i128> = (-99..=99).collect();
        let column = decimal_column(&values, 2, 1);
        let mut literals: Vec<(i128, i8)> = Vec::new();
        for scale in -2i8..=4 {
            // One grid unit of the column (0.1) in the literal's units, when it
            // is at least one.
            let unit = 10i128.pow(u32::try_from((scale - 1).max(0)).unwrap());
            let max = 99 * unit; // 9.9
            for centre in [0, max, -max, max / 2, -max / 3] {
                for d in -25..=25 {
                    literals.push((centre + d, scale));
                }
            }
            for k in (-120..=120).step_by(3) {
                literals.push((k * unit, scale));
                literals.push((k * unit + unit / 2, scale));
            }
        }
        for (unscaled, scale) in [
            (1, 50),
            (-1, 50),
            (i128::MAX, 0),
            (i128::MIN + 1, 0),
            (i128::MAX, 38),
            (10i128.pow(30), -20),
            (-(10i128.pow(30)), -20),
        ] {
            literals.push((unscaled, scale));
        }
        let oracle = |v: i128, unscaled: i128, scale: i8| decimal_ord(v, 1, unscaled, scale);
        let mut checked = 0usize;
        let mut check = |literal: LiteralValue, unscaled: i128, scale: i8| {
            let ords: Vec<_> = values.iter().map(|&v| oracle(v, unscaled, scale)).collect();
            for op in OPS {
                let got = kept(Arc::clone(&column), op, literal.clone());
                let want: Vec<usize> = (0..values.len()).filter(|&i| holds(op, ords[i])).collect();
                assert_eq!(got, want, "{op:?} {literal:?}");
                checked += 1;
            }
        };
        for &(unscaled, scale) in &literals {
            check(LiteralValue::Decimal { unscaled, scale }, unscaled, scale);
        }
        // Integer literals take the same path at scale 0.
        for n in -120i64..=120 {
            check(LiteralValue::Int64(n), i128::from(n), 0);
        }
        assert!(checked > 15_000, "{checked}");
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
