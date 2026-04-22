//! Example: Read Iceberg table via REST catalog.
//!
//! This demonstrates reading from an Iceberg REST catalog (Tabular/Polaris)
//! with data stored in MinIO.
//!
//! Run with:
//! ```
//! cargo run --example read_rest_catalog -p fluree-db-iceberg --features aws
//! ```
//!
//! Prerequisites:
//! - REST catalog running on localhost:8181
//! - MinIO running on localhost:9000 with warehouse bucket

use fluree_db_iceberg::auth::NoAuth;
use fluree_db_iceberg::catalog::{
    parse_table_identifier, RestCatalogClient, RestCatalogConfig, SendCatalogClient,
};
use fluree_db_iceberg::io::send_parquet::SendParquetReader;
use fluree_db_iceberg::io::{S3IcebergStorage, SendIcebergStorage};
use fluree_db_iceberg::metadata::TableMetadata;
use fluree_db_iceberg::scan::{ScanConfig, SendScanPlanner};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configuration
    let catalog_uri = std::env::var("ICEBERG_CATALOG_URI")
        .unwrap_or_else(|_| "http://localhost:8181".to_string());
    let minio_endpoint =
        std::env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());
    let minio_region = std::env::var("MINIO_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let table_name =
        std::env::var("ICEBERG_TABLE").unwrap_or_else(|_| "openflights.airlines".to_string());

    println!("=== Iceberg REST Catalog Reader ===");
    println!("Catalog URI: {catalog_uri}");
    println!("MinIO endpoint: {minio_endpoint}");
    println!("Table: {table_name}");

    // Create REST catalog client (no auth for local Tabular catalog)
    let catalog_config = RestCatalogConfig {
        uri: catalog_uri,
        warehouse: None,
        ..Default::default()
    };

    let auth = Arc::new(NoAuth);
    let catalog = RestCatalogClient::new(catalog_config, auth)?;

    // Parse table identifier
    let table_id = parse_table_identifier(&table_name)?;
    println!("\n=== Loading Table ===");
    println!("Namespace: {}", table_id.namespace);
    println!("Table: {}", table_id.table);

    // Load table metadata (no vended credentials from this catalog)
    let load_response = catalog.load_table(&table_id, false).await?;

    println!("Metadata location: {}", load_response.metadata_location);
    println!(
        "Has vended credentials: {}",
        load_response.credentials.is_some()
    );

    // Create S3 storage with MinIO settings (ambient credentials)
    // MinIO uses path-style URLs and we set AWS credentials via environment
    println!("\n=== Creating S3 Storage (MinIO) ===");
    let storage = S3IcebergStorage::from_default_chain(
        Some(&minio_region),
        Some(&minio_endpoint),
        true, // path_style for MinIO
    )
    .await?;

    // Read and parse table metadata
    println!("\n=== Reading Table Metadata ===");
    let metadata_bytes = storage.read(&load_response.metadata_location).await?;
    let metadata = TableMetadata::from_json(&metadata_bytes)?;

    println!("Format version: {}", metadata.format_version);
    println!("Table UUID: {:?}", metadata.table_uuid);

    if let Some(schema) = metadata.current_schema() {
        println!("\n=== Schema ===");
        for field in &schema.fields {
            println!(
                "  {} (id={}): {:?}{}",
                field.name,
                field.id,
                field.type_string(),
                if field.required { " NOT NULL" } else { "" }
            );
        }
    }

    // Plan scan
    println!("\n=== Planning Scan ===");
    let scan_config = ScanConfig::new();
    let planner = SendScanPlanner::new(&storage, &metadata, scan_config);
    let plan = planner.plan_scan().await?;

    println!("Files selected: {}", plan.files_selected);
    println!("Files pruned: {}", plan.files_pruned);
    println!("Estimated rows: {}", plan.estimated_row_count);

    if plan.is_empty() {
        println!("No data files to read");
        return Ok(());
    }

    // Read data
    println!("\n=== Reading Data ===");
    let reader = SendParquetReader::new(&storage);
    let mut total_rows = 0;
    let mut first_batch = true;

    for task in &plan.tasks {
        println!("Reading: {}", task.data_file.file_path);
        let batches = reader.read_task(task).await?;

        for batch in &batches {
            total_rows += batch.num_rows;

            // Print first few rows of first batch
            if first_batch && batch.num_rows > 0 {
                first_batch = false;
                println!("\n=== Sample Data (first 5 rows) ===");
                let col_names: Vec<_> = batch.schema.fields.iter().map(|f| &f.name).collect();
                println!("Columns: {col_names:?}");
                println!(
                    "Field IDs: {:?}",
                    batch.schema.field_ids().collect::<Vec<_>>()
                );

                // Debug: print column types and lengths
                use fluree_db_iceberg::io::batch::Column;
                println!("Column details:");
                for (i, col) in batch.columns.iter().enumerate() {
                    let (col_type, len) = match col {
                        Column::Int32(v) => ("Int32", v.len()),
                        Column::Int64(v) => ("Int64", v.len()),
                        Column::String(v) => ("String", v.len()),
                        Column::Boolean(v) => ("Boolean", v.len()),
                        Column::Float64(v) => ("Float64", v.len()),
                        Column::Float32(v) => ("Float32", v.len()),
                        Column::Bytes(v) => ("Bytes", v.len()),
                        Column::Date(v) => ("Date", v.len()),
                        Column::Timestamp(v) => ("Timestamp", v.len()),
                        Column::TimestampTz(v) => ("TimestampTz", v.len()),
                        Column::Decimal { values, .. } => ("Decimal", values.len()),
                    };
                    println!("  Col {i}: {col_type} (len={len})");
                }

                for row_idx in 0..batch.num_rows.min(5) {
                    let mut row_values = Vec::new();
                    for col in &batch.columns {
                        // Vec<Option<T>>[idx] returns Option<T> reference
                        let val = match col {
                            fluree_db_iceberg::io::batch::Column::Int32(v) => {
                                format!("{:?}", v[row_idx])
                            }
                            fluree_db_iceberg::io::batch::Column::Int64(v) => {
                                format!("{:?}", v[row_idx])
                            }
                            fluree_db_iceberg::io::batch::Column::String(v) => {
                                format!("{:?}", v[row_idx])
                            }
                            fluree_db_iceberg::io::batch::Column::Boolean(v) => {
                                format!("{:?}", v[row_idx])
                            }
                            fluree_db_iceberg::io::batch::Column::Float64(v) => {
                                format!("{:?}", v[row_idx])
                            }
                            _ => "...".to_string(),
                        };
                        row_values.push(val);
                    }
                    println!("  Row {row_idx}: {row_values:?}");
                }
            }
        }
    }

    println!("\n=== Summary ===");
    println!("Total rows read: {total_rows}");

    Ok(())
}
