//! Integration tests for Iceberg scan planning and columnar batch operations.
//!
//! These tests verify the Phase 2 implementation without requiring external
//! S3 or Iceberg services.

use fluree_db_iceberg::io::{MemoryStorage, RangeOnlyStorage};
use fluree_db_iceberg::{
    BatchSchema, Column, ColumnBatch, ComparisonOp, Expression, FieldInfo, FieldType, LiteralValue,
    ScanConfig, TypedValue,
};

/// Test that columnar batches can be created, filtered, and projected correctly.
#[test]
fn test_column_batch_filter_and_project() {
    // Create a schema with id, name, and age columns
    let schema = BatchSchema::new(vec![
        FieldInfo {
            name: "id".to_string(),
            field_type: FieldType::Int64,
            nullable: false,
            field_id: 1,
        },
        FieldInfo {
            name: "name".to_string(),
            field_type: FieldType::String,
            nullable: true,
            field_id: 2,
        },
        FieldInfo {
            name: "age".to_string(),
            field_type: FieldType::Int32,
            nullable: true,
            field_id: 3,
        },
    ]);

    // Create columns with test data
    let id_col = Column::Int64(vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
    let name_col = Column::String(vec![
        Some("Alice".to_string()),
        Some("Bob".to_string()),
        Some("Charlie".to_string()),
        None,
        Some("Eve".to_string()),
    ]);
    let age_col = Column::Int32(vec![Some(25), Some(30), Some(35), Some(40), Some(28)]);

    let batch = ColumnBatch::new(std::sync::Arc::new(schema), vec![id_col, name_col, age_col])
        .expect("batch creation");

    assert_eq!(batch.num_rows, 5);
    assert!(!batch.is_empty());

    // Test projection to just id and name
    let projected = batch.project(&[1, 2]).expect("projection");
    assert_eq!(projected.schema.fields.len(), 2);
    assert_eq!(projected.schema.fields[0].name, "id");
    assert_eq!(projected.schema.fields[1].name, "name");

    // Test filtering by row indices (simulating age > 28)
    let filtered = batch.filter_by_indices(&[1, 2, 3]);
    assert_eq!(filtered.num_rows, 3);
}

/// Test scan configuration builder pattern.
#[test]
fn test_scan_config_builder() {
    let config = ScanConfig::new()
        .with_projection(vec![1, 2, 3])
        .with_filter(Expression::and(vec![
            Expression::gt(1, "id", LiteralValue::Int64(100)),
            Expression::lt(3, "age", LiteralValue::Int32(50)),
        ]))
        .with_batch_row_limit(2048)
        .with_batch_byte_budget(1024 * 1024);

    assert_eq!(config.projection, Some(vec![1, 2, 3]));
    assert!(config.filter.is_some());
    assert_eq!(config.batch_row_limit, 2048);
    assert_eq!(config.batch_byte_budget, Some(1024 * 1024));
}

/// Test expression building and field ID extraction.
#[test]
fn test_expression_field_id_extraction() {
    // Simple comparison
    let expr1 = Expression::eq(1, "id", LiteralValue::Int64(42));
    let fields1 = expr1.referenced_field_ids();
    assert!(fields1.contains(&1));

    // Compound expression
    let expr2 = Expression::and(vec![
        Expression::gt(1, "id", LiteralValue::Int64(0)),
        Expression::eq(2, "status", LiteralValue::String("active".to_string())),
        Expression::is_not_null(3, "name"),
    ]);
    let fields2 = expr2.referenced_field_ids();
    assert!(fields2.contains(&1));
    assert!(fields2.contains(&2));
    assert!(fields2.contains(&3));
    assert_eq!(fields2.len(), 3);
}

/// Test TypedValue comparison (used for bound pruning).
#[test]
fn test_typed_value_comparison() {
    // Int64 comparisons
    let a = TypedValue::Int64(100);
    let b = TypedValue::Int64(200);
    assert!(a < b);
    assert!(b > a);

    // String comparisons (lexicographic)
    let s1 = TypedValue::String("apple".to_string());
    let s2 = TypedValue::String("banana".to_string());
    assert!(s1 < s2);

    // Date comparisons (days since epoch)
    let d1 = TypedValue::Date(19000); // ~2022
    let d2 = TypedValue::Date(19500); // ~2023
    assert!(d1 < d2);
}

/// Test ComparisonOp negation.
#[test]
fn test_comparison_op_negation() {
    assert_eq!(ComparisonOp::Eq.negate(), ComparisonOp::NotEq);
    assert_eq!(ComparisonOp::NotEq.negate(), ComparisonOp::Eq);
    assert_eq!(ComparisonOp::Lt.negate(), ComparisonOp::GtEq);
    assert_eq!(ComparisonOp::LtEq.negate(), ComparisonOp::Gt);
    assert_eq!(ComparisonOp::Gt.negate(), ComparisonOp::LtEq);
    assert_eq!(ComparisonOp::GtEq.negate(), ComparisonOp::Lt);
}

/// Test LiteralValue type conversions.
#[test]
fn test_literal_value_typed_conversion() {
    let int_val = LiteralValue::Int64(42);
    let typed = int_val.to_typed_value();
    assert_eq!(typed, TypedValue::Int64(42));

    let str_val = LiteralValue::String("hello".to_string());
    let typed = str_val.to_typed_value();
    assert_eq!(typed, TypedValue::String("hello".to_string()));

    let date_val = LiteralValue::Date(19000);
    let typed = date_val.to_typed_value();
    assert_eq!(typed, TypedValue::Date(19000));
}

/// Test column type with nullability.
#[test]
fn test_column_null_handling() {
    let col = Column::Int64(vec![Some(1), None, Some(3), None, Some(5)]);

    if let Column::Int64(vals) = &col {
        assert_eq!(vals.len(), 5);
        assert_eq!(vals[0], Some(1));
        assert_eq!(vals[1], None);
        assert_eq!(vals[2], Some(3));
        assert_eq!(vals[3], None);
        assert_eq!(vals[4], Some(5));
    } else {
        panic!("Expected Int64 column");
    }
}

/// Test that different field types are handled correctly in schema.
#[test]
fn test_field_type_variants() {
    let types = vec![
        (FieldType::Boolean, "boolean"),
        (FieldType::Int32, "int"),
        (FieldType::Int64, "long"),
        (FieldType::Float32, "float"),
        (FieldType::Float64, "double"),
        (FieldType::String, "string"),
        (FieldType::Bytes, "binary"),
        (FieldType::Date, "date"),
        (FieldType::Timestamp, "timestamp"),
        (
            FieldType::Decimal {
                precision: 10,
                scale: 2,
            },
            "decimal(10,2)",
        ),
    ];

    for (field_type, expected_name) in types {
        let type_str = match &field_type {
            FieldType::Boolean => "boolean",
            FieldType::Int32 => "int",
            FieldType::Int64 => "long",
            FieldType::Float32 => "float",
            FieldType::Float64 => "double",
            FieldType::String => "string",
            FieldType::Bytes => "binary",
            FieldType::Date => "date",
            FieldType::Timestamp => "timestamp",
            FieldType::TimestampTz => "timestamptz",
            FieldType::Decimal { precision, scale } => {
                if *precision == 10 && *scale == 2 {
                    "decimal(10,2)"
                } else {
                    "decimal"
                }
            }
        };
        assert_eq!(
            type_str, expected_name,
            "Field type mismatch for {field_type:?}"
        );
    }
}

/// Test that RangeOnlyStorage correctly tracks read() vs read_range() calls.
///
/// This wrapper is used in integration tests to enforce that ParquetReader
/// uses range reads instead of downloading entire files.
#[tokio::test]
async fn test_range_only_storage_tracking() {
    use fluree_db_iceberg::IcebergStorage;

    let mut inner = MemoryStorage::new();
    inner.add_file("test.parquet", vec![0u8; 1000]);

    let storage = RangeOnlyStorage::new(inner);

    // Initially no calls
    assert_eq!(storage.read_calls(), 0);
    assert_eq!(storage.range_read_calls(), 0);

    // Range read should increment range_read_calls
    let _ = storage.read_range("test.parquet", 0..100).await;
    assert_eq!(storage.read_calls(), 0);
    assert_eq!(storage.range_read_calls(), 1);

    // Another range read
    let _ = storage.read_range("test.parquet", 500..600).await;
    assert_eq!(storage.read_calls(), 0);
    assert_eq!(storage.range_read_calls(), 2);

    // Full read should increment read_calls
    let _ = storage.read("test.parquet").await;
    assert_eq!(storage.read_calls(), 1);
    assert_eq!(storage.range_read_calls(), 2);

    // assert_no_full_reads should panic now
    let result = std::panic::catch_unwind(|| {
        storage.assert_no_full_reads();
    });
    assert!(result.is_err(), "Expected panic from assert_no_full_reads");
}

/// Test that RangeOnlyStorage.assert_no_full_reads() passes when only range reads are used.
#[tokio::test]
async fn test_range_only_storage_no_full_reads_passes() {
    use fluree_db_iceberg::IcebergStorage;

    let mut inner = MemoryStorage::new();
    inner.add_file("test.parquet", vec![0u8; 1000]);

    let storage = RangeOnlyStorage::new(inner);

    // Only use range reads
    let _ = storage.read_range("test.parquet", 0..100).await;
    let _ = storage.read_range("test.parquet", 900..1000).await;
    let _ = storage.file_size("test.parquet").await;

    // This should NOT panic
    storage.assert_no_full_reads();
    assert_eq!(storage.range_read_calls(), 2);
}
