//! Example: read an Iceberg table from AWS Glue Data Catalog or AWS S3 Tables via
//! the native AWS SDK (no REST catalog, no SigV4 signing code — the SDK signs).
//!
//! Exercises `GlueSdkCatalogClient` / `S3TablesSdkCatalogClient` end-to-end:
//! catalog `load_table` -> metadata_location -> ambient S3 read -> scan plan ->
//! Parquet decode.
//!
//! Glue:
//! ```
//! MODE=glue GLUE_DB=enterprise_dw GLUE_TABLE=dim_geography \
//!   AWS_PROFILE=aj-sandbox AWS_REGION=us-east-1 \
//!   cargo run --example read_glue -p fluree-db-iceberg --features aws
//! ```
//! S3 Tables:
//! ```
//! MODE=s3tables S3TABLES_ARN=arn:aws:s3tables:us-east-1:ACCT:bucket/NAME \
//!   NS=enterprise_dw TABLE=dim_geography \
//!   AWS_PROFILE=aj-sandbox AWS_REGION=us-east-1 \
//!   cargo run --example read_glue -p fluree-db-iceberg --features aws
//! ```

use fluree_db_iceberg::catalog::{
    GlueSdkCatalogClient, S3TablesSdkCatalogClient, SendCatalogClient, TableIdentifier,
};
use fluree_db_iceberg::io::send_parquet::SendParquetReader;
use fluree_db_iceberg::io::{S3IcebergStorage, SendIcebergStorage};
use fluree_db_iceberg::metadata::TableMetadata;
use fluree_db_iceberg::scan::{ScanConfig, SendScanPlanner};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = std::env::var("MODE").unwrap_or_else(|_| "glue".into());
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());

    let (catalog, table_id): (Box<dyn SendCatalogClient>, TableIdentifier) = if mode == "s3tables" {
        let arn = std::env::var("S3TABLES_ARN").expect("S3TABLES_ARN required for MODE=s3tables");
        let namespace = std::env::var("NS").unwrap_or_else(|_| "enterprise_dw".into());
        let table = std::env::var("TABLE").unwrap_or_else(|_| "dim_geography".into());
        println!("=== AWS S3 Tables: {namespace}.{table}\n    bucket: {arn}");
        (
            Box::new(S3TablesSdkCatalogClient::new(Some(&region), arn).await?),
            TableIdentifier { namespace, table },
        )
    } else {
        let namespace = std::env::var("GLUE_DB").unwrap_or_else(|_| "enterprise_dw".into());
        let table = std::env::var("GLUE_TABLE").unwrap_or_else(|_| "dim_geography".into());
        println!("=== AWS Glue Data Catalog: {namespace}.{table}");
        (
            Box::new(GlueSdkCatalogClient::new(Some(&region), None).await?),
            TableIdentifier { namespace, table },
        )
    };

    // 1. Resolve the metadata location via the SDK catalog client.
    let load = catalog.load_table(&table_id, false).await?;
    println!("metadata_location : {}", load.metadata_location);
    println!("vended credentials: {}", load.credentials.is_some());

    // 2. Read metadata from S3 with the ambient credential chain.
    let storage = S3IcebergStorage::from_default_chain(Some(&region), None, false).await?;
    let metadata_bytes = storage.read(&load.metadata_location).await?;
    let metadata = TableMetadata::from_json(&metadata_bytes)?;
    println!("format-version    : {}", metadata.format_version);
    if let Some(schema) = metadata.current_schema() {
        println!(
            "schema fields     : {} ({})",
            schema.fields.len(),
            schema
                .fields
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    // 3. Plan the scan.
    let planner = SendScanPlanner::new(&storage, &metadata, ScanConfig::new());
    let plan = planner.plan_scan().await?;
    println!(
        "files selected    : {} (pruned {}), est. rows {}",
        plan.files_selected, plan.files_pruned, plan.estimated_row_count
    );

    // 4. Read the data files.
    let reader = SendParquetReader::new(&storage);
    let mut total_rows = 0;
    for task in &plan.tasks {
        for batch in &reader.read_task(task).await? {
            total_rows += batch.num_rows;
        }
    }
    println!("TOTAL ROWS READ   : {total_rows}");
    println!("OK");
    Ok(())
}
