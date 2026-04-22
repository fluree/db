//! Integration tests for Iceberg Direct catalog mode using LocalStack.
//!
//! These tests verify the full Direct catalog flow:
//!   1. Write a minimal Iceberg table to local S3 (metadata.json + version-hint.text)
//!   2. Resolve metadata via DirectCatalogClient / SendDirectCatalogClient
//!   3. Verify version-hint.text resolution and metadata location
//!   4. Simulate an append (new metadata + updated version-hint.text)
//!   5. Verify the new metadata is discovered
//!
//! Run (requires Docker):
//!   cargo test -p fluree-db-api --features iceberg,aws-testcontainers \
//!     --test it_iceberg_direct -- --nocapture

#![cfg(all(feature = "iceberg", feature = "aws-testcontainers"))]

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::primitives::ByteStream;
use fluree_db_iceberg::catalog::SendCatalogClient;
use fluree_db_iceberg::catalog::TableIdentifier;
use fluree_db_iceberg::metadata::TableMetadata;
use fluree_db_iceberg::{S3IcebergStorage, SendDirectCatalogClient, SendIcebergStorage};
use fs2::FileExt;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{runners::AsyncRunner, GenericImage, ImageExt};

const LOCALSTACK_EDGE_PORT: u16 = 4566;
const REGION: &str = "us-east-1";
const BUCKET: &str = "iceberg-test";
const TABLE_PREFIX: &str = "warehouse/ns/test_table";

struct LocalstackTestLock {
    _file: std::fs::File,
}

async fn acquire_localstack_test_lock() -> LocalstackTestLock {
    tokio::task::spawn_blocking(|| {
        let lock_path = std::env::temp_dir().join("fluree-localstack-tests.lock");
        let lock_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap_or_else(|e| panic!("open LocalStack test lock {}: {e}", lock_path.display()));

        lock_file
            .lock_exclusive()
            .unwrap_or_else(|e| panic!("lock LocalStack test lock {}: {e}", lock_path.display()));

        LocalstackTestLock { _file: lock_file }
    })
    .await
    .expect("join LocalStack test lock task")
}

async fn wait_for_localstack_host_port(
    container: &testcontainers::ContainerAsync<GenericImage>,
) -> u16 {
    let mut last_err = None;

    for _ in 0..40 {
        match container.get_host_port_ipv4(LOCALSTACK_EDGE_PORT).await {
            Ok(host_port) => return host_port,
            Err(err) => {
                last_err = Some(err);
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }

    panic!(
        "LocalStack edge port mapped after retries: {:?}",
        last_err.expect("at least one port lookup attempt")
    );
}

async fn start_localstack(
    services: &str,
) -> (
    LocalstackTestLock,
    testcontainers::ContainerAsync<GenericImage>,
    String,
) {
    let lock = acquire_localstack_test_lock().await;
    let image = GenericImage::new("localstack/localstack", "4.4")
        .with_exposed_port(LOCALSTACK_EDGE_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready."))
        .with_env_var("SERVICES", services)
        .with_env_var("DEFAULT_REGION", REGION)
        .with_env_var("SKIP_SSL_CERT_DOWNLOAD", "1")
        .with_startup_timeout(Duration::from_secs(300));

    let container = image
        .start()
        .await
        .expect("LocalStack started (Docker must be running)");
    let host_port = wait_for_localstack_host_port(&container).await;
    let endpoint = format!("http://127.0.0.1:{host_port}");
    (lock, container, endpoint)
}

/// Minimal Iceberg v2 metadata JSON with one snapshot.
const METADATA_V1: &str = r#"{
    "format-version": 2,
    "table-uuid": "direct-test-uuid",
    "location": "s3://iceberg-test/warehouse/ns/test_table",
    "last-sequence-number": 1,
    "last-updated-ms": 1700000000000,
    "last-column-id": 2,
    "current-schema-id": 0,
    "schemas": [{
        "schema-id": 0,
        "fields": [
            {"id": 1, "name": "id", "required": true, "type": "long"},
            {"id": 2, "name": "name", "required": false, "type": "string"}
        ]
    }],
    "current-snapshot-id": 1,
    "snapshots": [
        {"snapshot-id": 1, "timestamp-ms": 1700000000000, "summary": {"total-records": "10"}}
    ],
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "sort-orders": [{"order-id": 0, "fields": []}],
    "properties": {}
}"#;

/// Updated metadata JSON with two snapshots (simulating an append).
const METADATA_V2: &str = r#"{
    "format-version": 2,
    "table-uuid": "direct-test-uuid",
    "location": "s3://iceberg-test/warehouse/ns/test_table",
    "last-sequence-number": 2,
    "last-updated-ms": 1700001000000,
    "last-column-id": 2,
    "current-schema-id": 0,
    "schemas": [{
        "schema-id": 0,
        "fields": [
            {"id": 1, "name": "id", "required": true, "type": "long"},
            {"id": 2, "name": "name", "required": false, "type": "string"}
        ]
    }],
    "current-snapshot-id": 2,
    "snapshots": [
        {"snapshot-id": 1, "timestamp-ms": 1700000000000, "summary": {"total-records": "10"}},
        {"snapshot-id": 2, "timestamp-ms": 1700001000000, "summary": {"total-records": "25"}}
    ],
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "sort-orders": [{"order-id": 0, "fields": []}],
    "properties": {}
}"#;

fn set_test_aws_env() {
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_REGION", REGION);
    std::env::set_var("AWS_DEFAULT_REGION", REGION);
    std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
}

async fn sdk_config_for_localstack(endpoint: &str) -> aws_config::SdkConfig {
    set_test_aws_env();
    let region_provider = RegionProviderChain::default_provider().or_else(REGION);
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region_provider)
        .endpoint_url(endpoint)
        .load()
        .await
}

async fn wait_for_localstack(sdk_config: &aws_config::SdkConfig) {
    let s3 = aws_sdk_s3::Client::new(sdk_config);
    for _ in 0..60 {
        if s3.list_buckets().send().await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("LocalStack did not become ready in time");
}

async fn ensure_bucket(sdk_config: &aws_config::SdkConfig, bucket: &str) {
    let s3 = aws_sdk_s3::Client::new(sdk_config);
    let _ = s3.create_bucket().bucket(bucket).send().await;
    for _ in 0..30 {
        if s3.head_bucket().bucket(bucket).send().await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("S3 bucket was not available: {bucket}");
}

/// Write a string to an S3 key.
async fn put_s3_object(sdk_config: &aws_config::SdkConfig, bucket: &str, key: &str, body: &str) {
    let s3 = aws_sdk_s3::Client::new(sdk_config);
    s3.put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body.as_bytes().to_vec()))
        .send()
        .await
        .unwrap_or_else(|e| panic!("Failed to PUT s3://{bucket}/{key}: {e}"));
}

fn table_location() -> String {
    format!("s3://{BUCKET}/{TABLE_PREFIX}")
}

fn table_id() -> TableIdentifier {
    TableIdentifier {
        namespace: "ns".to_string(),
        table: "test_table".to_string(),
    }
}

// =============================================================================
// Tests
// =============================================================================

/// Full lifecycle test:
///   1. Write v1 metadata + version-hint.text pointing to metadata filename
///   2. Resolve via SendDirectCatalogClient → get v1 metadata location
///   3. Read and parse metadata → verify snapshot 1
///   4. Simulate append: write v2 metadata + update version-hint.text to new filename
///   5. Re-resolve → get v2 metadata location
///   6. Read and parse → verify snapshot 2 is current
#[tokio::test]
async fn direct_catalog_full_lifecycle() {
    // Boot LocalStack
    let (_lock, _container, endpoint) = start_localstack("s3").await;

    let sdk_config = sdk_config_for_localstack(&endpoint).await;
    wait_for_localstack(&sdk_config).await;
    ensure_bucket(&sdk_config, BUCKET).await;

    // -- Step 1: Write initial Iceberg table (v1) --
    put_s3_object(
        &sdk_config,
        BUCKET,
        &format!("{TABLE_PREFIX}/metadata/00001-abc1-0001.metadata.json"),
        METADATA_V1,
    )
    .await;
    put_s3_object(
        &sdk_config,
        BUCKET,
        &format!("{TABLE_PREFIX}/metadata/version-hint.text"),
        "00001-abc1-0001.metadata.json",
    )
    .await;

    // -- Step 2: Create storage + DirectCatalogClient and resolve --
    let storage = S3IcebergStorage::from_default_chain(Some(REGION), Some(&endpoint), true)
        .await
        .expect("S3IcebergStorage created");
    let storage = Arc::new(storage);

    let catalog = SendDirectCatalogClient::new(table_location(), Arc::clone(&storage));
    let response = catalog
        .load_table(&table_id(), false)
        .await
        .expect("load_table should succeed");

    assert_eq!(
        response.metadata_location,
        format!("s3://{BUCKET}/{TABLE_PREFIX}/metadata/00001-abc1-0001.metadata.json"),
        "Should resolve to v1 metadata"
    );
    assert!(
        response.credentials.is_none(),
        "Direct mode should not vend credentials"
    );

    // -- Step 3: Read and parse metadata --
    let metadata_bytes = storage
        .read(&response.metadata_location)
        .await
        .expect("metadata read");
    let metadata = TableMetadata::from_json(&metadata_bytes).expect("metadata parse");

    assert_eq!(metadata.format_version, 2);
    assert_eq!(metadata.current_snapshot_id, Some(1));
    assert_eq!(metadata.snapshots.len(), 1);

    // -- Step 4: Simulate append — write v2 metadata + update hint --
    put_s3_object(
        &sdk_config,
        BUCKET,
        &format!("{TABLE_PREFIX}/metadata/00002-def2-0002.metadata.json"),
        METADATA_V2,
    )
    .await;
    put_s3_object(
        &sdk_config,
        BUCKET,
        &format!("{TABLE_PREFIX}/metadata/version-hint.text"),
        "00002-def2-0002.metadata.json",
    )
    .await;

    // -- Step 5: Re-resolve — should now point to v2 --
    let catalog2 = SendDirectCatalogClient::new(table_location(), Arc::clone(&storage));
    let response2 = catalog2
        .load_table(&table_id(), false)
        .await
        .expect("load_table v2 should succeed");

    assert_eq!(
        response2.metadata_location,
        format!("s3://{BUCKET}/{TABLE_PREFIX}/metadata/00002-def2-0002.metadata.json"),
        "Should resolve to v2 metadata after append"
    );

    // -- Step 6: Parse v2 metadata --
    let metadata_bytes2 = storage
        .read(&response2.metadata_location)
        .await
        .expect("metadata v2 read");
    let metadata2 = TableMetadata::from_json(&metadata_bytes2).expect("metadata v2 parse");

    assert_eq!(metadata2.current_snapshot_id, Some(2));
    assert_eq!(metadata2.snapshots.len(), 2);
    assert_eq!(metadata2.last_sequence_number, 2);
}

/// Test that missing version-hint.text produces a clear error.
#[tokio::test]
async fn direct_catalog_missing_version_hint() {
    let (_lock, _container, endpoint) = start_localstack("s3").await;

    let sdk_config = sdk_config_for_localstack(&endpoint).await;
    wait_for_localstack(&sdk_config).await;
    ensure_bucket(&sdk_config, BUCKET).await;

    // No version-hint.text written — table location exists but is empty
    let storage = S3IcebergStorage::from_default_chain(Some(REGION), Some(&endpoint), true)
        .await
        .expect("storage");
    let catalog = SendDirectCatalogClient::new(table_location(), Arc::new(storage));

    let result = catalog.load_table(&table_id(), false).await;
    assert!(
        result.is_err(),
        "Should fail when version-hint.text is missing"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("version-hint.text"),
        "Error should mention version-hint.text: {err_msg}"
    );
}

/// Test IcebergCreateConfig::new_direct() → to_iceberg_gs_config() roundtrip
/// and validate() for direct mode.
#[test]
fn direct_create_config_roundtrip() {
    use fluree_db_api::IcebergCreateConfig;
    use fluree_db_iceberg::IcebergGsConfig;

    let config = IcebergCreateConfig::new_direct("my-direct-gs", "s3://bucket/warehouse/ns/table")
        .with_s3_region("us-east-1")
        .with_s3_path_style(true);

    // Validate
    assert!(config.validate().is_ok());
    assert!(config.is_direct());
    assert!(!config.is_rest());
    assert_eq!(
        config.catalog_uri_or_location(),
        "s3://bucket/warehouse/ns/table"
    );
    assert_eq!(config.table_identifier_display(), "ns.table");
    assert_eq!(config.graph_source_id(), "my-direct-gs:main");

    // Roundtrip through IcebergGsConfig (nameservice storage format)
    let gs_config = config.to_iceberg_gs_config();
    let json = gs_config.to_json().expect("serialize");
    let parsed = IcebergGsConfig::from_json(&json).expect("parse");

    // Validate the parsed config
    assert!(parsed.validate().is_ok());
    match &parsed.catalog {
        fluree_db_iceberg::CatalogConfig::Direct { table_location } => {
            assert_eq!(table_location, "s3://bucket/warehouse/ns/table");
        }
        other => panic!("Expected Direct, got {other:?}"),
    }
    assert!(!parsed.io.vended_credentials);
    assert_eq!(parsed.io.s3_region, Some("us-east-1".to_string()));
}
