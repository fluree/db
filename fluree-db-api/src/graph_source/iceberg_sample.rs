//! Bounded, cheap row/column **sampler** over an Iceberg table.
//!
//! This is the data-peek complement to the metadata-only
//! [`preview_iceberg_table`](super::iceberg_catalog::preview_iceberg_table): it
//! actually reads a handful of *values*. Two entry points:
//!
//! - [`sample_iceberg_rows`] — the first `n` rows, projected to some (or all)
//!   columns, each row as a JSON object keyed by column name.
//! - [`sample_column_values`] — the first `n` values of a single column, as a
//!   JSON array.
//!
//! It powers the LLM agent's "look at the data" tool and solo #756 (row
//! preview). **Bounded by construction**: it mirrors `preview_iceberg_table`'s
//! catalog/storage/snapshot handling, plans a *projected* single-table scan, and
//! reads only the **first row group** of the first data file, bounded to `n`
//! rows (see [`SendParquetReader::read_task_sample`]) — never a full-table (or
//! even full-file) scan. The unit of cost is the row group, not the row: the
//! read fetches the footer plus the first row group's chunk for EVERY projected
//! column, so a narrow projection costs a few column chunks while an all-columns
//! sample (`projection: None`) of a wide table decodes the entire first row
//! group across all columns regardless of how small `n` is. There is
//! deliberately no column/byte cap — silently dropping columns from an
//! "all columns" peek would be worse than the bounded extra read.
//! Consequently it returns at most `min(n, rows-in-first-row-group)` rows.
//!
//! Values are rendered to JSON through the same typed path the stats preview
//! uses ([`fluree_db_iceberg::typed_value_to_json`]): temporals become ISO-8601
//! strings, decimals their scaled string, bytes hex — nulls become JSON `null`.

use serde_json::{Map, Value};

use fluree_db_iceberg::catalog::SendCatalogClient;
use fluree_db_iceberg::metadata::Schema;
use fluree_db_iceberg::{
    typed_value_to_json, Column, ColumnBatch, ScanConfig, SendParquetReader, SendScanPlanner,
    TypedValue,
};

use crate::graph_source::config::IcebergConnectionConfig;
use crate::graph_source::iceberg_catalog::{
    build_preview_storage, rest_catalog_client, TableIdentifier,
};
use crate::{ApiError, Result};

/// Sample the first `n` rows of `table`, projected to `projection` (column names;
/// `None` or an empty list means all non-nested columns). Each row is a JSON
/// object keyed by column name; nulls are JSON `null`.
///
/// Cheap peek — see the [module docs](self). Returns at most
/// `min(n, rows-in-first-row-group)` rows; an empty table (no data files) yields
/// an empty vector.
pub async fn sample_iceberg_rows(
    conn: IcebergConnectionConfig,
    table: TableIdentifier,
    projection: Option<Vec<String>>,
    n: usize,
) -> Result<Vec<Value>> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let batches = sample_table_batches(&conn, &table, n, |schema| {
        resolve_projection(schema, projection.as_deref(), &table)
    })
    .await?;
    Ok(rows_from_batches(&batches, n))
}

/// Sample the first `n` values of a single `column` of `table`, as a JSON array
/// (nulls are JSON `null`).
///
/// Cheap peek — see the [module docs](self). Returns at most
/// `min(n, rows-in-first-row-group)` values.
pub async fn sample_column_values(
    conn: IcebergConnectionConfig,
    table: TableIdentifier,
    column: String,
    n: usize,
) -> Result<Vec<Value>> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let batches = sample_table_batches(&conn, &table, n, |schema| {
        let field = schema.field_by_name(&column).ok_or_else(|| {
            ApiError::config(format!(
                "Column {column:?} not found in table {}. Available: {:?}",
                table.qualified(),
                schema.field_names()
            ))
        })?;
        Ok(vec![field.id])
    })
    .await?;
    Ok(column_values_from_batches(&batches, n))
}

/// Resolve `table`, plan a projected scan, and read up to `n` rows from the first
/// data file's first row group. `resolve_field_ids` maps the table's current
/// schema to the projected field IDs (so projection errors reference the real
/// schema). Returns the decoded batches (≤ `n` rows total).
///
/// Mirrors [`preview_iceberg_table`](super::iceberg_catalog::preview_iceberg_table):
/// same REST catalog client, inline `loadTable` metadata, and vended-vs-ambient
/// storage policy.
async fn sample_table_batches<F>(
    conn: &IcebergConnectionConfig,
    table: &TableIdentifier,
    n: usize,
    resolve_field_ids: F,
) -> Result<Vec<ColumnBatch>>
where
    F: FnOnce(&Schema) -> Result<Vec<i32>>,
{
    let (catalog, _uri, _wh) = rest_catalog_client(conn, "table sample")?;
    let table_id = table.to_catalog();

    let load = SendCatalogClient::load_table(&catalog, &table_id, conn.io.vended_credentials)
        .await
        .map_err(|e| {
            ApiError::config(format!("Failed to load table {}: {e}", table.qualified()))
        })?;

    let metadata = load.metadata.as_ref().ok_or_else(|| {
        ApiError::config(format!(
            "Catalog did not return inline table metadata for {} — sampling requires a REST \
             catalog whose loadTable response includes the `metadata` object.",
            table.qualified()
        ))
    })?;

    let schema = metadata
        .current_schema()
        .ok_or_else(|| ApiError::config("Table metadata has no current schema"))?;

    let field_ids = resolve_field_ids(schema)?;

    // Build S3 storage the same way the preview/scan paths do (vended creds when
    // the catalog delegated them, else the ambient AWS chain).
    let storage = build_preview_storage(conn, load.credentials.as_ref()).await?;

    // Plan a projected scan over the current snapshot. A table with no current
    // snapshot / no data files yields no tasks → an empty sample.
    let scan_config = ScanConfig::new().with_projection(field_ids);
    let planner = SendScanPlanner::new(&storage, metadata, scan_config);
    let plan = planner.plan_scan().await.map_err(|e| {
        ApiError::config(format!(
            "Failed to plan scan for {}: {e}",
            table.qualified()
        ))
    })?;

    let Some(task) = plan.tasks.first() else {
        return Ok(Vec::new());
    };

    // Bounded, cheap read of the first row group only.
    let reader = SendParquetReader::new(&storage);
    reader.read_task_sample(task, n).await.map_err(|e| {
        ApiError::config(format!(
            "Failed to sample rows from {}: {e}",
            table.qualified()
        ))
    })
}

/// Resolve requested column names to field IDs against the table schema. `None`
/// or an empty list projects all non-nested columns (matching the R2RML scan
/// default). Named columns that do not exist are an error referencing the real
/// schema.
fn resolve_projection(
    schema: &Schema,
    projection: Option<&[String]>,
    table: &TableIdentifier,
) -> Result<Vec<i32>> {
    match projection {
        Some(cols) if !cols.is_empty() => {
            let ids: Vec<i32> = cols
                .iter()
                .filter_map(|c| schema.field_by_name(c).map(|f| f.id))
                .collect();
            if ids.is_empty() {
                return Err(ApiError::config(format!(
                    "None of the requested columns {:?} exist in table {}. Available: {:?}",
                    cols,
                    table.qualified(),
                    schema.field_names()
                )));
            }
            Ok(ids)
        }
        _ => Ok(schema
            .fields
            .iter()
            .filter(|f| !f.is_nested())
            .map(|f| f.id)
            .collect()),
    }
}

/// Assemble up to `n` rows from the decoded batches as JSON objects keyed by
/// column name. Column order follows each batch's schema (the projection order).
fn rows_from_batches(batches: &[ColumnBatch], n: usize) -> Vec<Value> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows {
            let mut obj = Map::with_capacity(batch.columns.len());
            for (field, col) in batch.schema.fields.iter().zip(batch.columns.iter()) {
                obj.insert(field.name.clone(), column_cell_to_json(col, row));
            }
            rows.push(Value::Object(obj));
            if rows.len() >= n {
                return rows;
            }
        }
    }
    rows
}

/// Assemble up to `n` values from the decoded batches (single-column projection)
/// as a JSON array. Reads the batch's first (only) column.
fn column_values_from_batches(batches: &[ColumnBatch], n: usize) -> Vec<Value> {
    let mut values = Vec::new();
    for batch in batches {
        let Some(col) = batch.column(0) else {
            continue;
        };
        for row in 0..batch.num_rows {
            values.push(column_cell_to_json(col, row));
            if values.len() >= n {
                return values;
            }
        }
    }
    values
}

/// Render one column cell to JSON, reusing the stats preview's typed→JSON path
/// ([`typed_value_to_json`]) so temporals/decimals/bytes format identically
/// across the two surfaces. A null (or out-of-range index) is JSON `null`.
fn column_cell_to_json(col: &Column, idx: usize) -> Value {
    let typed: Option<TypedValue> = match col {
        Column::Boolean(v) => v.get(idx).copied().flatten().map(TypedValue::Boolean),
        Column::Int32(v) => v.get(idx).copied().flatten().map(TypedValue::Int32),
        Column::Int64(v) => v.get(idx).copied().flatten().map(TypedValue::Int64),
        Column::Float32(v) => v.get(idx).copied().flatten().map(TypedValue::Float32),
        Column::Float64(v) => v.get(idx).copied().flatten().map(TypedValue::Float64),
        Column::String(v) => v.get(idx).cloned().flatten().map(TypedValue::String),
        Column::Bytes(v) => v.get(idx).cloned().flatten().map(TypedValue::Bytes),
        Column::Date(v) => v.get(idx).copied().flatten().map(TypedValue::Date),
        Column::Timestamp(v) => v.get(idx).copied().flatten().map(TypedValue::Timestamp),
        Column::TimestampTz(v) => v.get(idx).copied().flatten().map(TypedValue::TimestampTz),
        Column::Decimal {
            values,
            precision,
            scale,
        } => values
            .get(idx)
            .copied()
            .flatten()
            .map(|unscaled| TypedValue::Decimal {
                unscaled,
                precision: *precision,
                scale: *scale,
            }),
    };
    typed.as_ref().map_or(Value::Null, typed_value_to_json)
}

impl crate::Fluree {
    /// Sample the first `n` rows of an Iceberg table as JSON objects. Convenience
    /// wrapper over the stateless [`sample_iceberg_rows`] free function.
    pub async fn sample_iceberg_rows(
        &self,
        conn: IcebergConnectionConfig,
        table: TableIdentifier,
        projection: Option<Vec<String>>,
        n: usize,
    ) -> Result<Vec<Value>> {
        let conn = self.hydrate_conn(conn).await?;
        sample_iceberg_rows(conn, table, projection, n).await
    }

    /// Sample the first `n` values of a single Iceberg column as a JSON array.
    /// Convenience wrapper over the stateless [`sample_column_values`] free
    /// function.
    pub async fn sample_column_values(
        &self,
        conn: IcebergConnectionConfig,
        table: TableIdentifier,
        column: String,
        n: usize,
    ) -> Result<Vec<Value>> {
        let conn = self.hydrate_conn(conn).await?;
        sample_column_values(conn, table, column, n).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_iceberg::{BatchSchema, FieldInfo, FieldType};
    use std::sync::Arc;

    fn field(name: &str, field_type: FieldType, field_id: i32) -> FieldInfo {
        FieldInfo {
            name: name.to_string(),
            field_type,
            nullable: true,
            field_id,
        }
    }

    /// A 3-row batch: id (Int64), name (String, one null), created (Date).
    fn sample_batch() -> ColumnBatch {
        let schema = Arc::new(BatchSchema::new(vec![
            field("id", FieldType::Int64, 1),
            field("name", FieldType::String, 2),
            field("created", FieldType::Date, 3),
        ]));
        let columns = vec![
            Column::Int64(vec![Some(1), Some(2), Some(3)]),
            Column::String(vec![Some("alice".into()), None, Some("carol".into())]),
            // 2020-01-01, 2020-01-02, 2020-01-03 (days since epoch).
            Column::Date(vec![Some(18262), Some(18263), Some(18264)]),
        ];
        ColumnBatch::new(schema, columns).unwrap()
    }

    #[test]
    fn cell_to_json_covers_types_and_nulls() {
        let batch = sample_batch();
        let id = batch.column_by_id(1).unwrap();
        let name = batch.column_by_id(2).unwrap();
        let created = batch.column_by_id(3).unwrap();

        assert_eq!(column_cell_to_json(id, 0), Value::from(1));
        assert_eq!(column_cell_to_json(name, 0), Value::from("alice"));
        // Null cell → JSON null.
        assert_eq!(column_cell_to_json(name, 1), Value::Null);
        // Date renders as an ISO-8601 string via the typed→JSON path.
        assert_eq!(column_cell_to_json(created, 0), Value::from("2020-01-01"));
    }

    #[test]
    fn cell_to_json_renders_decimal_as_scaled_string() {
        // unscaled 12345, scale 2 → "123.45".
        let col = Column::Decimal {
            values: vec![Some(12345)],
            precision: 5,
            scale: 2,
        };
        assert_eq!(column_cell_to_json(&col, 0), Value::from("123.45"));
    }

    #[test]
    fn rows_are_objects_keyed_by_column_and_bounded_by_n() {
        let batches = vec![sample_batch()];
        // n smaller than the batch → truncated to n rows.
        let rows = rows_from_batches(&batches, 2);
        assert_eq!(rows.len(), 2, "bounded to n rows");

        let first = rows[0].as_object().expect("row is a JSON object");
        assert_eq!(first.get("id"), Some(&Value::from(1)));
        assert_eq!(first.get("name"), Some(&Value::from("alice")));
        assert_eq!(first.get("created"), Some(&Value::from("2020-01-01")));

        // A null cell is present as JSON null (not omitted).
        let second = rows[1].as_object().unwrap();
        assert_eq!(second.get("name"), Some(&Value::Null));
    }

    #[test]
    fn rows_honor_projection_column_set() {
        // A batch projected to a single column yields single-key objects.
        let schema = Arc::new(BatchSchema::new(vec![field("id", FieldType::Int64, 1)]));
        let batch = ColumnBatch::new(schema, vec![Column::Int64(vec![Some(7), Some(8)])]).unwrap();
        let rows = rows_from_batches(&[batch], 10);
        assert_eq!(rows.len(), 2);
        let obj = rows[0].as_object().unwrap();
        assert_eq!(obj.len(), 1, "only the projected column is present");
        assert_eq!(obj.get("id"), Some(&Value::from(7)));
    }

    #[test]
    fn column_values_are_scalars_bounded_by_n() {
        // Single-column batch (as produced by a single-column projection).
        let schema = Arc::new(BatchSchema::new(vec![field("id", FieldType::Int64, 1)]));
        let batch = ColumnBatch::new(
            schema,
            vec![Column::Int64(vec![Some(10), Some(20), Some(30)])],
        )
        .unwrap();
        let values = column_values_from_batches(&[batch], 2);
        assert_eq!(values, vec![Value::from(10), Value::from(20)]);
    }
}
