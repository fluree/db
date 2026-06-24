//! Smoke test: read a pyiceberg-written table off MinIO via the Direct catalog.
//!
//! Validates the real S3 path end-to-end: version-hint.text resolution,
//! manifest parsing, scan planning, and Parquet reading on tables produced by
//! pyiceberg (not our own writer).
//!
//! Run (MinIO must be up, table generated):
//! ```
//! AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
//! ICEBERG_ENDPOINT=http://localhost:9000 \
//! ICEBERG_TABLE_LOC=s3://warehouse/iceberg/tpch/region \
//!   cargo run -p fluree-db-iceberg --features aws --example smoke_minio
//! ```
use std::sync::Arc;

use fluree_db_iceberg::catalog::{CatalogClient, DirectCatalogClient, TableIdentifier};
use fluree_db_iceberg::io::parquet::ParquetReader;
use fluree_db_iceberg::io::IcebergStorage;
use fluree_db_iceberg::metadata::TableMetadata;
use fluree_db_iceberg::scan::{ScanConfig, ScanPlanner};
use fluree_db_iceberg::S3IcebergStorage;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("ICEBERG_ENDPOINT").unwrap_or("http://localhost:9000".into());
    let table_loc =
        std::env::var("ICEBERG_TABLE_LOC").unwrap_or("s3://warehouse/iceberg/tpch/region".into());
    let region = std::env::var("AWS_REGION").unwrap_or("us-east-1".into());

    println!("endpoint={endpoint}  table={table_loc}");

    let storage =
        Arc::new(S3IcebergStorage::from_default_chain(Some(&region), Some(&endpoint), true).await?);

    // Resolve current metadata via version-hint.text.
    let catalog = DirectCatalogClient::new(table_loc.clone(), Arc::clone(&storage));
    let resp = catalog
        .load_table(&TableIdentifier::new("tpch", "t"), false)
        .await?;
    println!("metadata_location = {}", resp.metadata_location);

    let meta_bytes = storage.read(&resp.metadata_location).await?;
    let metadata = TableMetadata::from_json(&meta_bytes)?;
    println!(
        "format_version={} location={}",
        metadata.format_version, metadata.location
    );
    if let Some(s) = metadata.current_schema() {
        println!("schema fields: {}", s.fields.len());
    }

    let planner = ScanPlanner::new(storage.as_ref(), &metadata, ScanConfig::new());
    let plan = planner.plan_scan().await?;
    println!(
        "scan: files_selected={} files_pruned={} est_rows={}",
        plan.files_selected, plan.files_pruned, plan.estimated_row_count
    );

    let reader = ParquetReader::new(storage.as_ref());
    let mut total = 0usize;
    for task in &plan.tasks {
        let batches = reader.read_task(task).await?;
        for b in &batches {
            total += b.num_rows;
        }
    }
    println!("TOTAL ROWS READ = {total}");
    Ok(())
}
