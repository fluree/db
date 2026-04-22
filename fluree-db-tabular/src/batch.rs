//! Columnar batch format for tabular data.
//!
//! This module provides a columnar batch format that is efficient for filtering
//! and joins without requiring Arrow as a dependency. The `ColumnBatch` type
//! stores data in typed column vectors with schema information.
//!
//! # Design
//!
//! - **Columnar storage**: Data is stored in typed `Vec` per column, not per-row
//! - **Strongly typed**: All column access is through the `Column` enum, no `dyn Any`
//! - **Field ID canonical**: Field IDs are the canonical identifier for columns
//! - **Lambda-friendly**: No Arrow dependency by default (small binary size)

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{Result, TabularError};

/// Tabular field types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    Boolean,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Bytes,
    Date,
    Timestamp,
    TimestampTz,
    Decimal { precision: u8, scale: i8 },
}

/// Field information for a column in a batch.
#[derive(Debug, Clone)]
pub struct FieldInfo {
    /// Column name (for debug/UX, not canonical lookup).
    pub name: String,
    /// Field type.
    pub field_type: FieldType,
    /// Whether the field allows nulls.
    pub nullable: bool,
    /// Field ID - CANONICAL identifier for lookups.
    pub field_id: i32,
}

/// Schema for a column batch.
#[derive(Debug, Clone)]
pub struct BatchSchema {
    /// Field definitions in column order.
    pub fields: Vec<FieldInfo>,
    /// Canonical lookup by field_id.
    field_id_to_index: HashMap<i32, usize>,
    /// Convenience lookup by name (debug/UX).
    name_to_index: HashMap<String, usize>,
}

impl BatchSchema {
    /// Create a new batch schema from field definitions.
    pub fn new(fields: Vec<FieldInfo>) -> Self {
        let field_id_to_index = fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.field_id, i))
            .collect();
        let name_to_index = fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name.clone(), i))
            .collect();

        Self {
            fields,
            field_id_to_index,
            name_to_index,
        }
    }

    /// Get field index by field_id (canonical).
    #[inline]
    pub fn index_by_id(&self, field_id: i32) -> Option<usize> {
        self.field_id_to_index.get(&field_id).copied()
    }

    /// Get field index by name (convenience).
    #[inline]
    pub fn index_by_name(&self, name: &str) -> Option<usize> {
        self.name_to_index.get(name).copied()
    }

    /// Get field info by field_id.
    pub fn field_by_id(&self, field_id: i32) -> Option<&FieldInfo> {
        self.index_by_id(field_id).map(|i| &self.fields[i])
    }

    /// Get field info by name.
    pub fn field_by_name(&self, name: &str) -> Option<&FieldInfo> {
        self.index_by_name(name).map(|i| &self.fields[i])
    }

    /// Number of fields in the schema.
    #[inline]
    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    /// Get all field IDs.
    pub fn field_ids(&self) -> impl Iterator<Item = i32> + '_ {
        self.fields.iter().map(|f| f.field_id)
    }
}

/// Column storage - typed arrays with optional values (nullable).
#[derive(Debug, Clone)]
pub enum Column {
    Boolean(Vec<Option<bool>>),
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    Float32(Vec<Option<f32>>),
    Float64(Vec<Option<f64>>),
    String(Vec<Option<String>>),
    Bytes(Vec<Option<Vec<u8>>>),
    /// Date: days since 1970-01-01
    Date(Vec<Option<i32>>),
    /// Timestamp: microseconds since epoch (UTC)
    Timestamp(Vec<Option<i64>>),
    /// TimestampTz: microseconds since epoch with timezone
    TimestampTz(Vec<Option<i64>>),
    /// Decimal: (unscaled_value, precision, scale)
    Decimal {
        values: Vec<Option<i128>>,
        precision: u8,
        scale: i8,
    },
}

impl Column {
    /// Create an empty column of the given type.
    pub fn empty(field_type: FieldType) -> Self {
        match field_type {
            FieldType::Boolean => Self::Boolean(Vec::new()),
            FieldType::Int32 => Self::Int32(Vec::new()),
            FieldType::Int64 => Self::Int64(Vec::new()),
            FieldType::Float32 => Self::Float32(Vec::new()),
            FieldType::Float64 => Self::Float64(Vec::new()),
            FieldType::String => Self::String(Vec::new()),
            FieldType::Bytes => Self::Bytes(Vec::new()),
            FieldType::Date => Self::Date(Vec::new()),
            FieldType::Timestamp => Self::Timestamp(Vec::new()),
            FieldType::TimestampTz => Self::TimestampTz(Vec::new()),
            FieldType::Decimal { precision, scale } => Self::Decimal {
                values: Vec::new(),
                precision,
                scale,
            },
        }
    }

    /// Create an empty column with pre-allocated capacity.
    pub fn with_capacity(field_type: FieldType, capacity: usize) -> Self {
        match field_type {
            FieldType::Boolean => Self::Boolean(Vec::with_capacity(capacity)),
            FieldType::Int32 => Self::Int32(Vec::with_capacity(capacity)),
            FieldType::Int64 => Self::Int64(Vec::with_capacity(capacity)),
            FieldType::Float32 => Self::Float32(Vec::with_capacity(capacity)),
            FieldType::Float64 => Self::Float64(Vec::with_capacity(capacity)),
            FieldType::String => Self::String(Vec::with_capacity(capacity)),
            FieldType::Bytes => Self::Bytes(Vec::with_capacity(capacity)),
            FieldType::Date => Self::Date(Vec::with_capacity(capacity)),
            FieldType::Timestamp => Self::Timestamp(Vec::with_capacity(capacity)),
            FieldType::TimestampTz => Self::TimestampTz(Vec::with_capacity(capacity)),
            FieldType::Decimal { precision, scale } => Self::Decimal {
                values: Vec::with_capacity(capacity),
                precision,
                scale,
            },
        }
    }

    /// Get the number of rows in this column.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Boolean(v) => v.len(),
            Self::Int32(v) => v.len(),
            Self::Int64(v) => v.len(),
            Self::Float32(v) => v.len(),
            Self::Float64(v) => v.len(),
            Self::String(v) => v.len(),
            Self::Bytes(v) => v.len(),
            Self::Date(v) => v.len(),
            Self::Timestamp(v) => v.len(),
            Self::TimestampTz(v) => v.len(),
            Self::Decimal { values, .. } => values.len(),
        }
    }

    /// Check if the column is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if value at index is null.
    #[inline]
    pub fn is_null(&self, idx: usize) -> bool {
        match self {
            Self::Boolean(v) => v.get(idx).is_none_or(std::option::Option::is_none),
            Self::Int32(v) => v.get(idx).is_none_or(std::option::Option::is_none),
            Self::Int64(v) => v.get(idx).is_none_or(std::option::Option::is_none),
            Self::Float32(v) => v.get(idx).is_none_or(std::option::Option::is_none),
            Self::Float64(v) => v.get(idx).is_none_or(std::option::Option::is_none),
            Self::String(v) => v.get(idx).is_none_or(std::option::Option::is_none),
            Self::Bytes(v) => v.get(idx).is_none_or(std::option::Option::is_none),
            Self::Date(v) => v.get(idx).is_none_or(std::option::Option::is_none),
            Self::Timestamp(v) => v.get(idx).is_none_or(std::option::Option::is_none),
            Self::TimestampTz(v) => v.get(idx).is_none_or(std::option::Option::is_none),
            Self::Decimal { values, .. } => {
                values.get(idx).is_none_or(std::option::Option::is_none)
            }
        }
    }

    /// Get the field type of this column.
    pub fn field_type(&self) -> FieldType {
        match self {
            Self::Boolean(_) => FieldType::Boolean,
            Self::Int32(_) => FieldType::Int32,
            Self::Int64(_) => FieldType::Int64,
            Self::Float32(_) => FieldType::Float32,
            Self::Float64(_) => FieldType::Float64,
            Self::String(_) => FieldType::String,
            Self::Bytes(_) => FieldType::Bytes,
            Self::Date(_) => FieldType::Date,
            Self::Timestamp(_) => FieldType::Timestamp,
            Self::TimestampTz(_) => FieldType::TimestampTz,
            Self::Decimal {
                precision, scale, ..
            } => FieldType::Decimal {
                precision: *precision,
                scale: *scale,
            },
        }
    }

    /// Get boolean value at index (returns None if wrong type or null).
    #[inline]
    pub fn get_bool(&self, idx: usize) -> Option<bool> {
        match self {
            Self::Boolean(v) => v.get(idx).and_then(|v| *v),
            _ => None,
        }
    }

    /// Get i32 value at index (returns None if wrong type or null).
    #[inline]
    pub fn get_i32(&self, idx: usize) -> Option<i32> {
        match self {
            Self::Int32(v) => v.get(idx).and_then(|v| *v),
            _ => None,
        }
    }

    /// Get i64 value at index (returns None if wrong type or null).
    #[inline]
    pub fn get_i64(&self, idx: usize) -> Option<i64> {
        match self {
            Self::Int64(v) => v.get(idx).and_then(|v| *v),
            _ => None,
        }
    }

    /// Get f32 value at index (returns None if wrong type or null).
    #[inline]
    pub fn get_f32(&self, idx: usize) -> Option<f32> {
        match self {
            Self::Float32(v) => v.get(idx).and_then(|v| *v),
            _ => None,
        }
    }

    /// Get f64 value at index (returns None if wrong type or null).
    #[inline]
    pub fn get_f64(&self, idx: usize) -> Option<f64> {
        match self {
            Self::Float64(v) => v.get(idx).and_then(|v| *v),
            _ => None,
        }
    }

    /// Get string value at index (returns None if wrong type or null).
    #[inline]
    pub fn get_string(&self, idx: usize) -> Option<&str> {
        match self {
            Self::String(v) => v.get(idx).and_then(|v| v.as_deref()),
            _ => None,
        }
    }

    /// Get bytes value at index (returns None if wrong type or null).
    #[inline]
    pub fn get_bytes(&self, idx: usize) -> Option<&[u8]> {
        match self {
            Self::Bytes(v) => v.get(idx).and_then(|v| v.as_deref()),
            _ => None,
        }
    }

    /// Get date value at index (days since epoch, returns None if wrong type or null).
    #[inline]
    pub fn get_date(&self, idx: usize) -> Option<i32> {
        match self {
            Self::Date(v) => v.get(idx).and_then(|v| *v),
            _ => None,
        }
    }

    /// Get timestamp value at index (microseconds since epoch, returns None if wrong type or null).
    #[inline]
    pub fn get_timestamp(&self, idx: usize) -> Option<i64> {
        match self {
            Self::Timestamp(v) | Self::TimestampTz(v) => v.get(idx).and_then(|v| *v),
            _ => None,
        }
    }

    /// Filter column by row indices, returning a new column with only those rows.
    pub fn filter_by_indices(&self, indices: &[usize]) -> Self {
        match self {
            Self::Boolean(v) => Self::Boolean(indices.iter().map(|&i| v[i]).collect()),
            Self::Int32(v) => Self::Int32(indices.iter().map(|&i| v[i]).collect()),
            Self::Int64(v) => Self::Int64(indices.iter().map(|&i| v[i]).collect()),
            Self::Float32(v) => Self::Float32(indices.iter().map(|&i| v[i]).collect()),
            Self::Float64(v) => Self::Float64(indices.iter().map(|&i| v[i]).collect()),
            Self::String(v) => Self::String(indices.iter().map(|&i| v[i].clone()).collect()),
            Self::Bytes(v) => Self::Bytes(indices.iter().map(|&i| v[i].clone()).collect()),
            Self::Date(v) => Self::Date(indices.iter().map(|&i| v[i]).collect()),
            Self::Timestamp(v) => Self::Timestamp(indices.iter().map(|&i| v[i]).collect()),
            Self::TimestampTz(v) => Self::TimestampTz(indices.iter().map(|&i| v[i]).collect()),
            Self::Decimal {
                values,
                precision,
                scale,
            } => Self::Decimal {
                values: indices.iter().map(|&i| values[i]).collect(),
                precision: *precision,
                scale: *scale,
            },
        }
    }

    /// Approximate byte size of this column (for budget tracking).
    pub fn byte_size(&self) -> usize {
        match self {
            Self::Boolean(v) => v.len() * 2, // Option<bool> is 2 bytes
            Self::Int32(v) | Self::Date(v) => v.len() * 8, // Option<i32> with alignment
            Self::Int64(v) | Self::Timestamp(v) | Self::TimestampTz(v) => v.len() * 16,
            Self::Float32(v) => v.len() * 8,
            Self::Float64(v) => v.len() * 16,
            Self::String(v) => v
                .iter()
                .map(|s| s.as_ref().map_or(0, |s| s.len() + 24))
                .sum(),
            Self::Bytes(v) => v
                .iter()
                .map(|b| b.as_ref().map_or(0, |b| b.len() + 24))
                .sum(),
            Self::Decimal { values, .. } => values.len() * 24, // Option<i128>
        }
    }
}

/// Columnar batch - efficient for filtering and joins.
#[derive(Debug, Clone)]
pub struct ColumnBatch {
    /// Schema for this batch.
    pub schema: Arc<BatchSchema>,
    /// Column data in schema order.
    pub columns: Vec<Column>,
    /// Number of rows in the batch.
    pub num_rows: usize,
}

impl ColumnBatch {
    /// Create a new column batch.
    pub fn new(schema: Arc<BatchSchema>, columns: Vec<Column>) -> Result<Self> {
        if columns.len() != schema.num_fields() {
            return Err(TabularError::Schema(format!(
                "Column count mismatch: schema has {} fields, got {} columns",
                schema.num_fields(),
                columns.len()
            )));
        }

        let num_rows = columns.first().map_or(0, Column::len);

        // Verify all columns have the same row count
        for (i, col) in columns.iter().enumerate() {
            if col.len() != num_rows {
                return Err(TabularError::Schema(format!(
                    "Row count mismatch: column {} has {} rows, expected {}",
                    i,
                    col.len(),
                    num_rows
                )));
            }
        }

        Ok(Self {
            schema,
            columns,
            num_rows,
        })
    }

    /// Create an empty batch with the given schema.
    pub fn empty(schema: Arc<BatchSchema>) -> Self {
        let columns = schema
            .fields
            .iter()
            .map(|f| Column::empty(f.field_type))
            .collect();
        Self {
            schema,
            columns,
            num_rows: 0,
        }
    }

    /// Get column by field_id (CANONICAL - use this for filters/comparisons).
    #[inline]
    pub fn column_by_id(&self, field_id: i32) -> Option<&Column> {
        self.schema.index_by_id(field_id).map(|i| &self.columns[i])
    }

    /// Get column by name (convenience for debug/UX).
    #[inline]
    pub fn column_by_name(&self, name: &str) -> Option<&Column> {
        self.schema.index_by_name(name).map(|i| &self.columns[i])
    }

    /// Get column by index.
    #[inline]
    pub fn column(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }

    /// Check if the batch is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    /// Filter batch by row indices, returning a new batch with only those rows.
    pub fn filter_by_indices(&self, indices: &[usize]) -> Self {
        let columns = self
            .columns
            .iter()
            .map(|c| c.filter_by_indices(indices))
            .collect();
        Self {
            schema: Arc::clone(&self.schema),
            columns,
            num_rows: indices.len(),
        }
    }

    /// Project to subset of columns by field_id.
    pub fn project(&self, field_ids: &[i32]) -> Result<Self> {
        let mut new_fields = Vec::with_capacity(field_ids.len());
        let mut new_columns = Vec::with_capacity(field_ids.len());

        for &field_id in field_ids {
            let idx = self.schema.index_by_id(field_id).ok_or_else(|| {
                TabularError::Schema(format!("Field ID {field_id} not found in schema"))
            })?;

            new_fields.push(self.schema.fields[idx].clone());
            new_columns.push(self.columns[idx].clone());
        }

        let new_schema = Arc::new(BatchSchema::new(new_fields));
        Ok(Self {
            schema: new_schema,
            columns: new_columns,
            num_rows: self.num_rows,
        })
    }

    /// Approximate byte size of this batch (for budget tracking).
    pub fn byte_size(&self) -> usize {
        self.columns.iter().map(Column::byte_size).sum()
    }

    /// Iterator over row indices.
    pub fn row_indices(&self) -> impl Iterator<Item = usize> {
        0..self.num_rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_schema() -> Arc<BatchSchema> {
        Arc::new(BatchSchema::new(vec![
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
                name: "active".to_string(),
                field_type: FieldType::Boolean,
                nullable: true,
                field_id: 3,
            },
        ]))
    }

    #[test]
    fn test_schema_lookup() {
        let schema = sample_schema();

        // Lookup by field_id (canonical)
        assert_eq!(schema.index_by_id(1), Some(0));
        assert_eq!(schema.index_by_id(2), Some(1));
        assert_eq!(schema.index_by_id(3), Some(2));
        assert_eq!(schema.index_by_id(99), None);

        // Lookup by name (convenience)
        assert_eq!(schema.index_by_name("id"), Some(0));
        assert_eq!(schema.index_by_name("name"), Some(1));
        assert_eq!(schema.index_by_name("unknown"), None);
    }

    #[test]
    fn test_column_batch_creation() {
        let schema = sample_schema();

        let columns = vec![
            Column::Int64(vec![Some(1), Some(2), Some(3)]),
            Column::String(vec![
                Some("Alice".to_string()),
                Some("Bob".to_string()),
                None,
            ]),
            Column::Boolean(vec![Some(true), Some(false), Some(true)]),
        ];

        let batch = ColumnBatch::new(schema, columns).unwrap();
        assert_eq!(batch.num_rows, 3);

        // Access by field_id
        let id_col = batch.column_by_id(1).unwrap();
        assert_eq!(id_col.get_i64(0), Some(1));
        assert_eq!(id_col.get_i64(1), Some(2));

        // Access by name
        let name_col = batch.column_by_name("name").unwrap();
        assert_eq!(name_col.get_string(0), Some("Alice"));
        assert_eq!(name_col.get_string(2), None); // null

        // Access by index
        let active_col = batch.column(2).unwrap();
        assert_eq!(active_col.get_bool(0), Some(true));
    }

    #[test]
    fn test_filter_by_indices() {
        let schema = sample_schema();

        let columns = vec![
            Column::Int64(vec![Some(1), Some(2), Some(3), Some(4)]),
            Column::String(vec![
                Some("A".to_string()),
                Some("B".to_string()),
                Some("C".to_string()),
                Some("D".to_string()),
            ]),
            Column::Boolean(vec![Some(true), Some(false), Some(true), Some(false)]),
        ];

        let batch = ColumnBatch::new(schema, columns).unwrap();
        let filtered = batch.filter_by_indices(&[0, 2]); // Keep rows 0 and 2

        assert_eq!(filtered.num_rows, 2);
        assert_eq!(filtered.column_by_id(1).unwrap().get_i64(0), Some(1));
        assert_eq!(filtered.column_by_id(1).unwrap().get_i64(1), Some(3));
        assert_eq!(
            filtered.column_by_name("name").unwrap().get_string(0),
            Some("A")
        );
        assert_eq!(
            filtered.column_by_name("name").unwrap().get_string(1),
            Some("C")
        );
    }

    #[test]
    fn test_project() {
        let schema = sample_schema();

        let columns = vec![
            Column::Int64(vec![Some(1), Some(2)]),
            Column::String(vec![Some("A".to_string()), Some("B".to_string())]),
            Column::Boolean(vec![Some(true), Some(false)]),
        ];

        let batch = ColumnBatch::new(schema, columns).unwrap();
        let projected = batch.project(&[1, 3]).unwrap(); // Only id and active

        assert_eq!(projected.schema.num_fields(), 2);
        assert!(projected.column_by_id(1).is_some());
        assert!(projected.column_by_id(2).is_none()); // name was not projected
        assert!(projected.column_by_id(3).is_some());
    }
}
