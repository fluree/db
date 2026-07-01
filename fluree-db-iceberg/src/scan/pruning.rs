//! Partition and file pruning using statistics.
//!
//! This module provides functions to evaluate filter expressions against:
//! - Partition field summaries (manifest-level pruning)
//! - Data file column bounds (file-level pruning)
//!
//! # Pruning Semantics
//!
//! Pruning functions return `true` if the data MIGHT contain matching rows.
//! They are conservative: returning `true` is always safe (may include extra files),
//! but returning `false` means we can definitively skip this manifest/file.

use crate::manifest::value_codec::TypedValue;
use crate::manifest::{decode_by_type_string, DataFile, PartitionFieldSummary};
use crate::metadata::Schema;
use crate::scan::predicate::{ComparisonOp, Expression, LiteralValue};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use std::collections::HashMap;

/// Evaluate expression against manifest partition summary.
///
/// Returns `true` if the manifest MIGHT contain matching data.
/// Returns `false` only if we can definitively exclude this manifest.
///
/// # Arguments
///
/// * `expr` - The filter expression
/// * `summaries` - Partition field summaries from the manifest
/// * `schema` - Table schema for type information
/// * `partition_spec_fields` - Mapping from partition field index to source field ID
pub fn can_contain_partition(
    expr: &Expression,
    _summaries: &[PartitionFieldSummary],
    _schema: &Schema,
    _partition_spec_fields: &[(i32, i32)], // (partition_field_idx, source_field_id)
) -> bool {
    // For now, we only do basic pruning based on bounds
    // Full partition pruning requires understanding partition transforms
    // which is deferred to a later optimization

    match expr {
        Expression::AlwaysTrue => true,
        Expression::AlwaysFalse => false,
        Expression::Not(inner) => {
            // NOT can't be used for pruning without full evaluation
            // Be conservative and include
            !matches!(inner.as_ref(), Expression::AlwaysTrue)
        }
        Expression::And(exprs) => {
            // All must be able to contain
            exprs
                .iter()
                .all(|e| can_contain_partition(e, _summaries, _schema, _partition_spec_fields))
        }
        Expression::Or(exprs) => {
            // Any might contain
            exprs
                .iter()
                .any(|e| can_contain_partition(e, _summaries, _schema, _partition_spec_fields))
        }
        // For column predicates, we can't easily map to partition summaries without
        // understanding partition transforms. Be conservative.
        _ => true,
    }
}

/// Evaluate expression against data file bounds.
///
/// Returns `true` if the file MIGHT contain matching data.
/// Returns `false` only if we can definitively exclude this file.
///
/// # Arguments
///
/// * `expr` - The filter expression
/// * `data_file` - The data file with column statistics
/// * `schema` - Table schema for type information
pub fn can_contain_file(expr: &Expression, data_file: &DataFile, schema: &Schema) -> bool {
    match expr {
        Expression::AlwaysTrue => true,
        Expression::AlwaysFalse => false,
        Expression::Not(inner) => {
            // NOT requires careful handling
            match inner.as_ref() {
                Expression::AlwaysTrue => false,
                Expression::AlwaysFalse => true,
                Expression::IsNull { field_id, .. } => {
                    // NOT IS NULL = there exists non-null values
                    // If the file might contain non-null values, include it
                    data_file.might_contain_values(*field_id)
                }
                Expression::IsNotNull { field_id, .. } => {
                    // NOT IS NOT NULL = all values are null
                    // Can't easily determine this, be conservative
                    let null_count = data_file.null_count(*field_id).unwrap_or(0);
                    null_count > 0
                }
                _ => true, // Conservative for complex negations
            }
        }
        Expression::And(exprs) => {
            // All conditions must be satisfiable
            exprs.iter().all(|e| can_contain_file(e, data_file, schema))
        }
        Expression::Or(exprs) => {
            // Any condition might be satisfiable
            exprs.iter().any(|e| can_contain_file(e, data_file, schema))
        }
        Expression::IsNull { field_id, .. } => {
            // File might contain null if null_count > 0 or unknown
            let null_count = data_file.null_count(*field_id).unwrap_or(1);
            null_count > 0
        }
        Expression::IsNotNull { field_id, .. } => {
            // File might contain non-null values
            data_file.might_contain_values(*field_id)
        }
        Expression::Comparison {
            field_id,
            op,
            value,
            ..
        } => can_contain_comparison(*field_id, *op, value, data_file, schema),
        Expression::In {
            field_id, values, ..
        } => {
            // Any value in the list might be in range
            values
                .iter()
                .any(|v| can_contain_comparison(*field_id, ComparisonOp::Eq, v, data_file, schema))
        }
        Expression::NotIn { .. } => {
            // NOT IN is hard to prune with just min/max bounds
            // Be conservative
            true
        }
    }
}

/// Check if a comparison predicate might be satisfied by a file.
fn can_contain_comparison(
    field_id: i32,
    op: ComparisonOp,
    value: &LiteralValue,
    data_file: &DataFile,
    schema: &Schema,
) -> bool {
    // Get the field type for decoding
    let field = match schema.field(field_id) {
        Some(f) => f,
        None => return true, // Unknown field, be conservative
    };

    // Get bounds
    let lower = data_file.lower_bound(field_id);
    let upper = data_file.upper_bound(field_id);

    // If no bounds, can't prune
    if lower.is_none() && upper.is_none() {
        return true;
    }

    // Decode bounds
    let lower_typed = lower.and_then(|b| decode_by_type_string(b, field.type_string()).ok());
    let upper_typed = upper.and_then(|b| decode_by_type_string(b, field.type_string()).ok());

    // Convert literal to typed value
    let lit_typed = value.to_typed_value();

    bounds_can_contain(op, &lit_typed, lower_typed, upper_typed)
}

/// Shared min/max reasoning for a `column <op> value` predicate. `lower`/`upper`
/// are the column's decoded bounds (from an Iceberg `DataFile` or a Parquet
/// row-group `Statistics`); a missing bound is treated conservatively (cannot
/// prune). Returns `true` if a value satisfying the predicate could lie within
/// the bounds.
fn bounds_can_contain(
    op: ComparisonOp,
    lit: &TypedValue,
    lower: Option<TypedValue>,
    upper: Option<TypedValue>,
) -> bool {
    match op {
        // value ∈ [lower, upper]
        ComparisonOp::Eq => match (&lower, &upper) {
            (Some(l), Some(u)) => lit.ge(l).unwrap_or(true) && lit.le(u).unwrap_or(true),
            (Some(l), None) => lit.ge(l).unwrap_or(true),
            (None, Some(u)) => lit.le(u).unwrap_or(true),
            (None, None) => true,
        },
        // Prunable only when every value equals the excluded one (lower==upper==value).
        ComparisonOp::NotEq => match (&lower, &upper) {
            (Some(l), Some(u)) if l == u => l != lit,
            _ => true,
        },
        // column < value can occur iff lower < value.
        ComparisonOp::Lt => lower.as_ref().is_none_or(|l| lit.gt(l).unwrap_or(true)),
        ComparisonOp::LtEq => lower.as_ref().is_none_or(|l| lit.ge(l).unwrap_or(true)),
        // column > value can occur iff upper > value.
        ComparisonOp::Gt => upper.as_ref().is_none_or(|u| lit.lt(u).unwrap_or(true)),
        ComparisonOp::GtEq => upper.as_ref().is_none_or(|u| lit.le(u).unwrap_or(true)),
    }
}

/// Row-group-level pruning: can this Parquet row group contain a row matching
/// `expr`? Conservative — returns `true` unless the row group's column
/// statistics prove no row can match. `field_to_col` maps an Iceberg field id to
/// the Parquet column index in this file.
pub fn row_group_can_contain(
    expr: &Expression,
    row_group: &RowGroupMetaData,
    field_to_col: &HashMap<i32, usize>,
) -> bool {
    match expr {
        Expression::AlwaysTrue => true,
        Expression::AlwaysFalse => false,
        Expression::And(exprs) => exprs
            .iter()
            .all(|e| row_group_can_contain(e, row_group, field_to_col)),
        Expression::Or(exprs) => exprs
            .iter()
            .any(|e| row_group_can_contain(e, row_group, field_to_col)),
        Expression::Comparison {
            field_id,
            op,
            value,
            ..
        } => {
            let Some(&col_idx) = field_to_col.get(field_id) else {
                return true;
            };
            let Some(stats) = row_group.column(col_idx).statistics() else {
                return true;
            };
            let lit = value.to_typed_value();
            let (lower, upper) = stat_bounds(stats, &lit);
            bounds_can_contain(*op, &lit, lower, upper)
        }
        Expression::In {
            field_id, values, ..
        } => {
            let Some(&col_idx) = field_to_col.get(field_id) else {
                return true;
            };
            let Some(stats) = row_group.column(col_idx).statistics() else {
                return true;
            };
            values.iter().any(|v| {
                let lit = v.to_typed_value();
                let (lower, upper) = stat_bounds(stats, &lit);
                bounds_can_contain(ComparisonOp::Eq, &lit, lower, upper)
            })
        }
        // Null predicates and negations keep the row group (conservative).
        _ => true,
    }
}

/// Extract a Parquet row-group column's min/max as `TypedValue`s coerced to the
/// same variant as `like` (the predicate literal). Only the pushdown-supported
/// physical types are read (bool / int32 / int64, including int32-backed dates);
/// anything else yields `(None, None)` so pruning stays conservative.
fn stat_bounds(stats: &Statistics, like: &TypedValue) -> (Option<TypedValue>, Option<TypedValue>) {
    match (stats, like) {
        (Statistics::Boolean(s), TypedValue::Boolean(_)) => (
            s.min_opt().map(|&v| TypedValue::Boolean(v)),
            s.max_opt().map(|&v| TypedValue::Boolean(v)),
        ),
        (Statistics::Int32(s), TypedValue::Int32(_)) => (
            s.min_opt().map(|&v| TypedValue::Int32(v)),
            s.max_opt().map(|&v| TypedValue::Int32(v)),
        ),
        // Iceberg dates are physically int32 (days since 1970-01-01).
        (Statistics::Int32(s), TypedValue::Date(_)) => (
            s.min_opt().map(|&v| TypedValue::Date(v)),
            s.max_opt().map(|&v| TypedValue::Date(v)),
        ),
        (Statistics::Int64(s), TypedValue::Int64(_)) => (
            s.min_opt().map(|&v| TypedValue::Int64(v)),
            s.max_opt().map(|&v| TypedValue::Int64(v)),
        ),
        // UTF-8 string min/max. Parquet stats are valid bounds even when the
        // writer truncates them (min truncated down, max up), so lexicographic
        // pruning stays conservative. Non-UTF-8 bytes fall through to no bound.
        (Statistics::ByteArray(s), TypedValue::String(_)) => {
            let to_str = |b: &parquet::data_type::ByteArray| {
                std::str::from_utf8(b.data())
                    .ok()
                    .map(|v| TypedValue::String(v.to_string()))
            };
            (s.min_opt().and_then(to_str), s.max_opt().and_then(to_str))
        }
        _ => (None, None),
    }
}

/// Evaluate expression against column batch rows.
///
/// Returns indices of rows that match the predicate.
///
/// # Arguments
///
/// * `expr` - The filter expression
/// * `batch` - The column batch to evaluate
pub fn evaluate_batch(expr: &Expression, batch: &crate::io::ColumnBatch) -> Vec<usize> {
    match expr {
        Expression::AlwaysTrue => (0..batch.num_rows).collect(),
        Expression::AlwaysFalse => Vec::new(),
        Expression::Not(inner) => {
            let matching = evaluate_batch(inner, batch);
            let matching_set: std::collections::HashSet<usize> = matching.into_iter().collect();
            (0..batch.num_rows)
                .filter(|i| !matching_set.contains(i))
                .collect()
        }
        Expression::And(exprs) => {
            let mut result: Vec<usize> = (0..batch.num_rows).collect();
            for expr in exprs {
                if result.is_empty() {
                    break;
                }
                let matching = evaluate_batch(expr, batch);
                let matching_set: std::collections::HashSet<usize> = matching.into_iter().collect();
                result.retain(|i| matching_set.contains(i));
            }
            result
        }
        Expression::Or(exprs) => {
            let mut result_set = std::collections::HashSet::new();
            for expr in exprs {
                let matching = evaluate_batch(expr, batch);
                result_set.extend(matching);
            }
            let mut result: Vec<usize> = result_set.into_iter().collect();
            result.sort();
            result
        }
        Expression::IsNull { field_id, .. } => {
            let Some(col) = batch.column_by_id(*field_id) else {
                return Vec::new();
            };
            (0..batch.num_rows).filter(|&i| col.is_null(i)).collect()
        }
        Expression::IsNotNull { field_id, .. } => {
            let Some(col) = batch.column_by_id(*field_id) else {
                return Vec::new();
            };
            (0..batch.num_rows).filter(|&i| !col.is_null(i)).collect()
        }
        Expression::Comparison {
            field_id,
            op,
            value,
            ..
        } => evaluate_comparison(*field_id, *op, value, batch),
        Expression::In {
            field_id, values, ..
        } => {
            let mut result_set = std::collections::HashSet::new();
            for value in values {
                let matching = evaluate_comparison(*field_id, ComparisonOp::Eq, value, batch);
                result_set.extend(matching);
            }
            let mut result: Vec<usize> = result_set.into_iter().collect();
            result.sort();
            result
        }
        Expression::NotIn {
            field_id, values, ..
        } => {
            // Start with all rows, remove those matching any value
            let mut excluded = std::collections::HashSet::new();
            for value in values {
                let matching = evaluate_comparison(*field_id, ComparisonOp::Eq, value, batch);
                excluded.extend(matching);
            }
            (0..batch.num_rows)
                .filter(|i| !excluded.contains(i))
                .collect()
        }
    }
}

/// Evaluate a comparison predicate against batch rows.
fn evaluate_comparison(
    field_id: i32,
    op: ComparisonOp,
    value: &LiteralValue,
    batch: &crate::io::ColumnBatch,
) -> Vec<usize> {
    use crate::io::Column;

    let Some(col) = batch.column_by_id(field_id) else {
        return Vec::new();
    };

    let mut result = Vec::new();

    for i in 0..batch.num_rows {
        let matches = match (col, value) {
            (Column::Boolean(vals), LiteralValue::Boolean(lit)) => vals
                .get(i)
                .and_then(|v| *v)
                .is_some_and(|v| compare_op(v, *lit, op)),
            (Column::Int32(vals), LiteralValue::Int32(lit)) => vals
                .get(i)
                .and_then(|v| *v)
                .is_some_and(|v| compare_op(v, *lit, op)),
            (Column::Int64(vals), LiteralValue::Int64(lit)) => vals
                .get(i)
                .and_then(|v| *v)
                .is_some_and(|v| compare_op(v, *lit, op)),
            (Column::Float32(vals), LiteralValue::Float32(lit)) => vals
                .get(i)
                .and_then(|v| *v)
                .is_some_and(|v| compare_op_f32(v, *lit, op)),
            (Column::Float64(vals), LiteralValue::Float64(lit)) => vals
                .get(i)
                .and_then(|v| *v)
                .is_some_and(|v| compare_op_f64(v, *lit, op)),
            (Column::String(vals), LiteralValue::String(lit)) => vals
                .get(i)
                .and_then(|v| v.as_ref())
                .is_some_and(|v| compare_op(v.as_str(), lit.as_str(), op)),
            (Column::Date(vals), LiteralValue::Date(lit)) => vals
                .get(i)
                .and_then(|v| *v)
                .is_some_and(|v| compare_op(v, *lit, op)),
            (Column::Timestamp(vals) | Column::TimestampTz(vals), LiteralValue::Timestamp(lit)) => {
                vals.get(i)
                    .and_then(|v| *v)
                    .is_some_and(|v| compare_op(v, *lit, op))
            }
            _ => false, // Type mismatch
        };

        if matches {
            result.push(i);
        }
    }

    result
}

/// Compare two values with the given operator.
fn compare_op<T: PartialOrd + PartialEq>(a: T, b: T, op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Eq => a == b,
        ComparisonOp::NotEq => a != b,
        ComparisonOp::Lt => a < b,
        ComparisonOp::LtEq => a <= b,
        ComparisonOp::Gt => a > b,
        ComparisonOp::GtEq => a >= b,
    }
}

/// Compare two f32 values with the given operator (handling NaN).
fn compare_op_f32(a: f32, b: f32, op: ComparisonOp) -> bool {
    if a.is_nan() || b.is_nan() {
        return op == ComparisonOp::NotEq;
    }
    compare_op(a, b, op)
}

/// Compare two f64 values with the given operator (handling NaN).
fn compare_op_f64(a: f64, b: f64, op: ComparisonOp) -> bool {
    if a.is_nan() || b.is_nan() {
        return op == ComparisonOp::NotEq;
    }
    compare_op(a, b, op)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{BatchSchema, Column, ColumnBatch, FieldInfo, FieldType};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_data_file(lower: i64, upper: i64) -> DataFile {
        let mut lower_bounds = HashMap::new();
        lower_bounds.insert(1, lower.to_le_bytes().to_vec());

        let mut upper_bounds = HashMap::new();
        upper_bounds.insert(1, upper.to_le_bytes().to_vec());

        DataFile {
            file_path: "test.parquet".to_string(),
            file_format: crate::manifest::FileFormat::Parquet,
            record_count: 100,
            file_size_in_bytes: 1024,
            partition: crate::manifest::PartitionData::default(),
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: Some(lower_bounds),
            upper_bounds: Some(upper_bounds),
            split_offsets: None,
            sort_order_id: None,
        }
    }

    fn create_test_schema() -> Schema {
        Schema {
            schema_id: 0,
            identifier_field_ids: vec![],
            fields: vec![crate::metadata::SchemaField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: serde_json::json!("long"),
                doc: None,
            }],
        }
    }

    #[test]
    fn test_can_contain_file_eq() {
        let schema = create_test_schema();

        // File with id in [10, 100]
        let data_file = create_test_data_file(10, 100);

        // id = 50 (in range) -> should be true
        let expr = Expression::eq(1, "id", LiteralValue::Int64(50));
        assert!(can_contain_file(&expr, &data_file, &schema));

        // id = 5 (below range) -> should be false
        let expr = Expression::eq(1, "id", LiteralValue::Int64(5));
        assert!(!can_contain_file(&expr, &data_file, &schema));

        // id = 150 (above range) -> should be false
        let expr = Expression::eq(1, "id", LiteralValue::Int64(150));
        assert!(!can_contain_file(&expr, &data_file, &schema));

        // id = 10 (at lower bound) -> should be true
        let expr = Expression::eq(1, "id", LiteralValue::Int64(10));
        assert!(can_contain_file(&expr, &data_file, &schema));

        // id = 100 (at upper bound) -> should be true
        let expr = Expression::eq(1, "id", LiteralValue::Int64(100));
        assert!(can_contain_file(&expr, &data_file, &schema));
    }

    #[test]
    fn test_can_contain_file_lt() {
        let schema = create_test_schema();
        let data_file = create_test_data_file(10, 100);

        // id < 50 -> file might contain (lower < 50)
        let expr = Expression::lt(1, "id", LiteralValue::Int64(50));
        assert!(can_contain_file(&expr, &data_file, &schema));

        // id < 5 -> file cannot contain (lower >= 5)
        let expr = Expression::lt(1, "id", LiteralValue::Int64(5));
        assert!(!can_contain_file(&expr, &data_file, &schema));

        // id < 10 -> file cannot contain (lower >= 10)
        let expr = Expression::lt(1, "id", LiteralValue::Int64(10));
        assert!(!can_contain_file(&expr, &data_file, &schema));
    }

    #[test]
    fn test_can_contain_file_gt() {
        let schema = create_test_schema();
        let data_file = create_test_data_file(10, 100);

        // id > 50 -> file might contain (upper > 50)
        let expr = Expression::gt(1, "id", LiteralValue::Int64(50));
        assert!(can_contain_file(&expr, &data_file, &schema));

        // id > 150 -> file cannot contain (upper <= 150)
        let expr = Expression::gt(1, "id", LiteralValue::Int64(150));
        assert!(!can_contain_file(&expr, &data_file, &schema));

        // id > 100 -> file cannot contain (upper <= 100)
        let expr = Expression::gt(1, "id", LiteralValue::Int64(100));
        assert!(!can_contain_file(&expr, &data_file, &schema));
    }

    #[test]
    fn test_can_contain_file_and() {
        let schema = create_test_schema();
        let data_file = create_test_data_file(10, 100);

        // id >= 20 AND id <= 80 -> should match
        let expr = Expression::and(vec![
            Expression::gt_eq(1, "id", LiteralValue::Int64(20)),
            Expression::lt_eq(1, "id", LiteralValue::Int64(80)),
        ]);
        assert!(can_contain_file(&expr, &data_file, &schema));

        // id >= 200 AND id <= 300 -> should not match
        let expr = Expression::and(vec![
            Expression::gt_eq(1, "id", LiteralValue::Int64(200)),
            Expression::lt_eq(1, "id", LiteralValue::Int64(300)),
        ]);
        assert!(!can_contain_file(&expr, &data_file, &schema));
    }

    #[test]
    fn test_evaluate_batch() {
        let schema = Arc::new(BatchSchema::new(vec![FieldInfo {
            name: "id".to_string(),
            field_type: FieldType::Int64,
            nullable: false,
            field_id: 1,
        }]));

        let columns = vec![Column::Int64(vec![
            Some(10),
            Some(20),
            Some(30),
            Some(40),
            Some(50),
        ])];

        let batch = ColumnBatch::new(schema, columns).unwrap();

        // id > 25 -> rows 2, 3, 4
        let expr = Expression::gt(1, "id", LiteralValue::Int64(25));
        let result = evaluate_batch(&expr, &batch);
        assert_eq!(result, vec![2, 3, 4]);

        // id = 30 -> row 2
        let expr = Expression::eq(1, "id", LiteralValue::Int64(30));
        let result = evaluate_batch(&expr, &batch);
        assert_eq!(result, vec![2]);

        // id IN (10, 30, 50) -> rows 0, 2, 4
        let expr = Expression::in_list(
            1,
            "id",
            vec![
                LiteralValue::Int64(10),
                LiteralValue::Int64(30),
                LiteralValue::Int64(50),
            ],
        );
        let result = evaluate_batch(&expr, &batch);
        assert_eq!(result, vec![0, 2, 4]);
    }

    /// Write a Parquet file with two row groups over one INT64 column `v`:
    /// row group 0 holds 0..=4, row group 1 holds 100..=104. The default writer
    /// properties emit chunk statistics, so each row group carries real min/max.
    fn two_row_group_parquet() -> bytes::Bytes {
        use parquet::data_type::Int64Type;
        use parquet::file::properties::WriterProperties;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::parser::parse_message_type;

        let schema = Arc::new(parse_message_type("message s { REQUIRED INT64 v; }").unwrap());
        let props = Arc::new(WriterProperties::builder().build());
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = SerializedFileWriter::new(&mut buf, schema, props).unwrap();
            for vals in [[0i64, 1, 2, 3, 4], [100, 101, 102, 103, 104]] {
                let mut rg = writer.next_row_group().unwrap();
                let mut col = rg.next_column().unwrap().unwrap();
                col.typed::<Int64Type>()
                    .write_batch(&vals, None, None)
                    .unwrap();
                col.close().unwrap();
                rg.close().unwrap();
            }
            writer.close().unwrap();
        }
        bytes::Bytes::from(buf)
    }

    #[test]
    fn row_group_pruning_uses_real_parquet_stats() {
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let reader = SerializedFileReader::new(two_row_group_parquet()).unwrap();
        let meta = reader.metadata();
        assert_eq!(meta.num_row_groups(), 2);

        // Iceberg field id 1 maps to Parquet column 0 (the sole column).
        let field_to_col = HashMap::from([(1i32, 0usize)]);
        let cmp = |op, v| Expression::Comparison {
            field_id: 1,
            column: "v".to_string(),
            op,
            value: LiteralValue::Int64(v),
        };

        // v >= 50: rg0 (max 4) pruned, rg1 (min 100) kept.
        let ge = cmp(ComparisonOp::GtEq, 50);
        assert!(!row_group_can_contain(
            &ge,
            meta.row_group(0),
            &field_to_col
        ));
        assert!(row_group_can_contain(&ge, meta.row_group(1), &field_to_col));

        // v < 50: rg0 kept, rg1 pruned.
        let lt = cmp(ComparisonOp::Lt, 50);
        assert!(row_group_can_contain(&lt, meta.row_group(0), &field_to_col));
        assert!(!row_group_can_contain(
            &lt,
            meta.row_group(1),
            &field_to_col
        ));

        // v == 2: only rg0 can contain it.
        let eq = cmp(ComparisonOp::Eq, 2);
        assert!(row_group_can_contain(&eq, meta.row_group(0), &field_to_col));
        assert!(!row_group_can_contain(
            &eq,
            meta.row_group(1),
            &field_to_col
        ));

        // A field the query does not map is conservative — keep the row group.
        let unmapped = HashMap::from([(2i32, 0usize)]);
        assert!(row_group_can_contain(&ge, meta.row_group(0), &unmapped));
    }

    /// Two row groups with disjoint UTF-8 string ranges, for `ByteArray`-stats
    /// pruning: rg0 = [apple, cherry], rg1 = [mango, peach].
    fn two_row_group_string_parquet() -> bytes::Bytes {
        use parquet::data_type::{ByteArray, ByteArrayType};
        use parquet::file::properties::WriterProperties;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::parser::parse_message_type;

        let schema =
            Arc::new(parse_message_type("message s { REQUIRED BYTE_ARRAY v (UTF8); }").unwrap());
        let props = Arc::new(WriterProperties::builder().build());
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = SerializedFileWriter::new(&mut buf, schema, props).unwrap();
            for vals in [["apple", "banana", "cherry"], ["mango", "orange", "peach"]] {
                let arr: Vec<ByteArray> = vals.iter().map(|s| ByteArray::from(*s)).collect();
                let mut rg = writer.next_row_group().unwrap();
                let mut col = rg.next_column().unwrap().unwrap();
                col.typed::<ByteArrayType>()
                    .write_batch(&arr, None, None)
                    .unwrap();
                col.close().unwrap();
                rg.close().unwrap();
            }
            writer.close().unwrap();
        }
        bytes::Bytes::from(buf)
    }

    #[test]
    fn row_group_pruning_uses_string_stats() {
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let reader = SerializedFileReader::new(two_row_group_string_parquet()).unwrap();
        let meta = reader.metadata();
        assert_eq!(meta.num_row_groups(), 2);
        let field_to_col = HashMap::from([(1i32, 0usize)]);
        let cmp = |op, v: &str| Expression::Comparison {
            field_id: 1,
            column: "v".to_string(),
            op,
            value: LiteralValue::String(v.to_string()),
        };

        // = "banana": only rg0 (in [apple, cherry]); rg1 (min mango) pruned.
        let eq_b = cmp(ComparisonOp::Eq, "banana");
        assert!(row_group_can_contain(
            &eq_b,
            meta.row_group(0),
            &field_to_col
        ));
        assert!(!row_group_can_contain(
            &eq_b,
            meta.row_group(1),
            &field_to_col
        ));

        // = "orange": rg0 (max cherry) pruned; only rg1 can contain it.
        let eq_o = cmp(ComparisonOp::Eq, "orange");
        assert!(!row_group_can_contain(
            &eq_o,
            meta.row_group(0),
            &field_to_col
        ));
        assert!(row_group_can_contain(
            &eq_o,
            meta.row_group(1),
            &field_to_col
        ));

        // >= "m": rg0 (max cherry < m) pruned; rg1 kept.
        let ge_m = cmp(ComparisonOp::GtEq, "m");
        assert!(!row_group_can_contain(
            &ge_m,
            meta.row_group(0),
            &field_to_col
        ));
        assert!(row_group_can_contain(
            &ge_m,
            meta.row_group(1),
            &field_to_col
        ));

        // < "m": rg0 kept; rg1 (min mango > m) pruned.
        let lt_m = cmp(ComparisonOp::Lt, "m");
        assert!(row_group_can_contain(
            &lt_m,
            meta.row_group(0),
            &field_to_col
        ));
        assert!(!row_group_can_contain(
            &lt_m,
            meta.row_group(1),
            &field_to_col
        ));
    }
}
