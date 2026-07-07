//! Env-gated live integration tests: AWS Glue Data Catalog + S3 Tables Iceberg
//! reads via the native AWS SDK. These need a live catalog + ambient AWS
//! credentials, so — like `it_gcs_sdk_reads.rs` — they SKIP unless the relevant
//! env vars are set (no live dependency in CI).
//!
//! Glue (matches the sandbox harness):
//! ```
//! AWS_PROFILE=aj-sandbox AWS_REGION=us-east-1 \
//!   FLUREE_GLUE_IT_DATABASE=enterprise_dw FLUREE_GLUE_IT_TABLE=dim_store \
//!   FLUREE_GLUE_IT_EXPECT_ROWS=300 \
//!   cargo test -p fluree-db-iceberg --features aws --test it_glue_sdk_reads -- --nocapture
//! ```
//! S3 Tables:
//! ```
//! FLUREE_S3TABLES_IT_ARN=arn:aws:s3tables:us-east-1:ACCT:bucket/NAME \
//!   FLUREE_S3TABLES_IT_TABLE=dim_geography FLUREE_S3TABLES_IT_EXPECT_ROWS=200 ...
//! ```
#![cfg(feature = "aws")]

use fluree_db_iceberg::catalog::{
    GlueSdkCatalogClient, S3TablesSdkCatalogClient, SendCatalogClient, TableIdentifier,
};
use fluree_db_iceberg::io::send_parquet::SendParquetReader;
use fluree_db_iceberg::io::{S3IcebergStorage, SendIcebergStorage};
use fluree_db_iceberg::metadata::TableMetadata;
use fluree_db_iceberg::scan::{ScanConfig, SendScanPlanner};

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|s| !s.is_empty())
}
fn region() -> Option<String> {
    env("FLUREE_GLUE_IT_REGION").or_else(|| env("AWS_REGION"))
}

/// Resolve `metadata_location` via the SDK catalog, then read the table's rows
/// with the ambient credential chain — the full SDK-native read path.
async fn read_rows(catalog: Box<dyn SendCatalogClient>, table_id: TableIdentifier) -> usize {
    let load = catalog
        .load_table(&table_id, false)
        .await
        .expect("load_table");
    assert!(
        load.metadata_location.contains("://"),
        "metadata_location should be a URI, got {}",
        load.metadata_location
    );
    assert!(
        load.credentials.is_none(),
        "the AWS SDK path reads S3 with ambient creds (no vending)"
    );

    let storage = S3IcebergStorage::from_default_chain(region().as_deref(), None, false)
        .await
        .expect("storage");
    let bytes = storage
        .read(&load.metadata_location)
        .await
        .expect("read metadata");
    let metadata = TableMetadata::from_json(&bytes).expect("parse metadata");

    let planner = SendScanPlanner::new(&storage, &metadata, ScanConfig::new());
    let plan = planner.plan_scan().await.expect("plan_scan");

    let reader = SendParquetReader::new(&storage);
    let mut total = 0;
    for task in &plan.tasks {
        for batch in &reader.read_task(task).await.expect("read_task") {
            total += batch.num_rows;
        }
    }
    total
}

#[tokio::test]
async fn glue_catalog_reads_rows() {
    let Some(namespace) = env("FLUREE_GLUE_IT_DATABASE") else {
        eprintln!(
            "skip glue_catalog_reads_rows: set FLUREE_GLUE_IT_DATABASE + FLUREE_GLUE_IT_TABLE"
        );
        return;
    };
    let table =
        env("FLUREE_GLUE_IT_TABLE").expect("FLUREE_GLUE_IT_TABLE required when DATABASE set");
    let catalog = Box::new(
        GlueSdkCatalogClient::new(region().as_deref(), env("FLUREE_GLUE_IT_CATALOG_ID"))
            .await
            .expect("glue client"),
    );
    let total = read_rows(catalog, TableIdentifier { namespace, table }).await;
    match env("FLUREE_GLUE_IT_EXPECT_ROWS").and_then(|v| v.parse::<usize>().ok()) {
        Some(expect) => assert_eq!(total, expect, "row-count mismatch"),
        None => assert!(total > 0, "expected a non-empty table"),
    }
    eprintln!("glue read {total} rows OK");
}

#[tokio::test]
async fn s3tables_catalog_reads_rows() {
    let Some(arn) = env("FLUREE_S3TABLES_IT_ARN") else {
        eprintln!("skip s3tables_catalog_reads_rows: set FLUREE_S3TABLES_IT_ARN");
        return;
    };
    let namespace = env("FLUREE_S3TABLES_IT_NS").unwrap_or_else(|| "enterprise_dw".into());
    let table = env("FLUREE_S3TABLES_IT_TABLE").unwrap_or_else(|| "dim_geography".into());
    let catalog = Box::new(
        S3TablesSdkCatalogClient::new(region().as_deref(), arn)
            .await
            .expect("s3tables client"),
    );
    let total = read_rows(catalog, TableIdentifier { namespace, table }).await;
    match env("FLUREE_S3TABLES_IT_EXPECT_ROWS").and_then(|v| v.parse::<usize>().ok()) {
        Some(expect) => assert_eq!(total, expect, "row-count mismatch"),
        None => assert!(total > 0, "expected a non-empty table"),
    }
    eprintln!("s3tables read {total} rows OK");
}
