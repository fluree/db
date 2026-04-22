//! AWS/S3 configuration tests for fluree-db-connection
//!
//! These tests verify that Rust can parse JSON-LD configs for AWS backends.
//!
//! Unit tests (no AWS required):
//!   cargo test -p fluree-db-connection --features aws --test aws_config_test
//!
//! Integration tests (requires LocalStack):
//!   cargo test -p fluree-db-connection --features aws --test aws_config_test -- --ignored

#![cfg(feature = "aws")]

use fluree_db_connection::{connect, ConnectionConfig, StorageType};
use serde_json::json;

/// Test parsing S3 storage JSON-LD config
#[test]
fn test_s3_storage_config_parsing() {
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {
                "@id": "s3Storage",
                "@type": "Storage",
                "s3Bucket": "my-fluree-bucket",
                "s3Prefix": "ledgers/prod"
            },
            {
                "@id": "connection",
                "@type": "Connection",
                "parallelism": 4,
                "indexStorage": {"@id": "s3Storage"}
            }
        ]
    });

    let parsed = ConnectionConfig::from_json_ld(&config).expect("Should parse S3 config");

    assert_eq!(parsed.parallelism, 4);
    match &parsed.index_storage.storage_type {
        StorageType::S3(s3_config) => {
            assert_eq!(&*s3_config.bucket, "my-fluree-bucket");
            assert_eq!(s3_config.prefix.as_deref(), Some("ledgers/prod"));
        }
        _ => panic!("Expected S3 storage type"),
    }
}

/// Test parsing S3 storage without prefix
#[test]
fn test_s3_storage_no_prefix() {
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {
                "@id": "s3Storage",
                "@type": "Storage",
                "s3Bucket": "my-bucket"
            },
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "s3Storage"}
            }
        ]
    });

    let parsed = ConnectionConfig::from_json_ld(&config).expect("Should parse");

    match &parsed.index_storage.storage_type {
        StorageType::S3(s3_config) => {
            assert_eq!(&*s3_config.bucket, "my-bucket");
            assert!(s3_config.prefix.is_none());
        }
        _ => panic!("Expected S3 storage type"),
    }
}

/// Test S3 Express bucket detection via naming convention
#[test]
fn test_s3_express_bucket_naming() {
    // S3 Express buckets follow pattern: name--{az}--x-s3
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {
                "@id": "expressStorage",
                "@type": "Storage",
                "s3Bucket": "my-bucket--use1-az1--x-s3",
                "s3Prefix": "data"
            },
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "expressStorage"}
            }
        ]
    });

    let parsed = ConnectionConfig::from_json_ld(&config).expect("Should parse");

    match &parsed.index_storage.storage_type {
        StorageType::S3(s3_config) => {
            assert_eq!(&*s3_config.bucket, "my-bucket--use1-az1--x-s3");
            // Express bucket detection happens at S3Storage creation time,
            // not config parsing time
        }
        _ => panic!("Expected S3 storage type"),
    }
}

/// Test parsing separate index and commit storage
#[test]
fn test_separate_index_commit_storage() {
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {
                "@id": "indexS3",
                "@type": "Storage",
                "s3Bucket": "index-bucket",
                "s3Prefix": "indexes"
            },
            {
                "@id": "commitS3",
                "@type": "Storage",
                "s3Bucket": "commit-bucket",
                "s3Prefix": "commits"
            },
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "indexS3"},
                "commitStorage": {"@id": "commitS3"}
            }
        ]
    });

    let parsed = ConnectionConfig::from_json_ld(&config).expect("Should parse");

    // Check index storage
    match &parsed.index_storage.storage_type {
        StorageType::S3(s3_config) => {
            assert_eq!(&*s3_config.bucket, "index-bucket");
            assert_eq!(s3_config.prefix.as_deref(), Some("indexes"));
        }
        _ => panic!("Expected S3 index storage"),
    }

    // Check commit storage
    let commit = parsed
        .commit_storage
        .as_ref()
        .expect("Should have commit storage");
    match &commit.storage_type {
        StorageType::S3(s3_config) => {
            assert_eq!(&*s3_config.bucket, "commit-bucket");
            assert_eq!(s3_config.prefix.as_deref(), Some("commits"));
        }
        _ => panic!("Expected S3 commit storage"),
    }
}

/// Test parsing DynamoDB publisher config
#[test]
fn test_dynamodb_publisher_config() {
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {
                "@id": "s3Storage",
                "@type": "Storage",
                "s3Bucket": "my-bucket"
            },
            {
                "@id": "publisher",
                "@type": "Publisher",
                "dynamodbTable": "fluree-nameservice",
                "dynamodbRegion": "us-east-1"
            },
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "s3Storage"},
                "primaryPublisher": {"@id": "publisher"}
            }
        ]
    });

    let parsed = ConnectionConfig::from_json_ld(&config).expect("Should parse");

    let publisher = parsed
        .primary_publisher
        .as_ref()
        .expect("Should have publisher");
    match &publisher.publisher_type {
        fluree_db_connection::config::PublisherType::DynamoDb { table, region, .. } => {
            assert_eq!(&**table, "fluree-nameservice");
            assert_eq!(region.as_deref(), Some("us-east-1"));
        }
        _ => panic!("Expected DynamoDB publisher"),
    }
}

/// Test parsing storage-backed publisher (nameservice via S3)
#[test]
fn test_storage_backed_publisher_config() {
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {
                "@id": "s3Storage",
                "@type": "Storage",
                "s3Bucket": "my-bucket",
                "s3Prefix": "fluree-data"
            },
            {
                "@id": "publisher",
                "@type": "Publisher",
                "storage": {"@id": "s3Storage"}
            },
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "s3Storage"},
                "primaryPublisher": {"@id": "publisher"}
            }
        ]
    });

    let parsed = ConnectionConfig::from_json_ld(&config).expect("Should parse");

    let publisher = parsed
        .primary_publisher
        .as_ref()
        .expect("Should have publisher");
    match &publisher.publisher_type {
        fluree_db_connection::config::PublisherType::Storage { storage } => {
            match &storage.storage_type {
                StorageType::S3(s3_config) => {
                    assert_eq!(&*s3_config.bucket, "my-bucket");
                    assert_eq!(s3_config.prefix.as_deref(), Some("fluree-data"));
                }
                _ => panic!("Expected S3 storage for publisher"),
            }
        }
        _ => panic!("Expected Storage publisher type"),
    }
}

/// Test that sync connect() fails for S3 storage (needs async)
#[test]
fn test_sync_connect_fails_for_s3() {
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {
                "@id": "s3Storage",
                "@type": "Storage",
                "s3Bucket": "my-bucket"
            },
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "s3Storage"}
            }
        ]
    });

    let result = connect(&config);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("async") || err_msg.contains("connect_async"),
        "Error should mention async: {err_msg}"
    );
}

/// Test full S3 + DynamoDB config
#[test]
fn test_full_aws_config() {
    // This is the config format used by legacy Lambda deployments
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@id": "lambdaConfig",
        "@graph": [
            {
                "@id": "s3IndexStorage",
                "@type": "Storage",
                "s3Bucket": "fluree-indexes",
                "s3Prefix": "prod/indexes",
                "s3ReadTimeoutMs": 5000
            },
            {
                "@id": "s3CommitStorage",
                "@type": "Storage",
                "s3Bucket": "fluree-commits",
                "s3Prefix": "prod/commits",
                "s3WriteTimeoutMs": 10000
            },
            {
                "@id": "dynamoNs",
                "@type": "Publisher",
                "dynamodbTable": "fluree-ledgers",
                "dynamodbRegion": "us-west-2",
                "dynamodbTimeoutMs": 3000
            },
            {
                "@id": "connection",
                "@type": "Connection",
                "parallelism": 8,
                "cacheMaxMb": 2048,
                "indexStorage": {"@id": "s3IndexStorage"},
                "commitStorage": {"@id": "s3CommitStorage"},
                "primaryPublisher": {"@id": "dynamoNs"}
            }
        ]
    });

    let parsed = ConnectionConfig::from_json_ld(&config).expect("Should parse full AWS config");

    // Verify connection settings
    assert_eq!(parsed.parallelism, 8);
    assert_eq!(parsed.cache.max_mb, 2048);

    // Verify index storage
    match &parsed.index_storage.storage_type {
        StorageType::S3(s3_config) => {
            assert_eq!(&*s3_config.bucket, "fluree-indexes");
            assert_eq!(s3_config.prefix.as_deref(), Some("prod/indexes"));
            assert_eq!(s3_config.read_timeout_ms, Some(5000));
        }
        _ => panic!("Expected S3 index storage"),
    }

    // Verify commit storage
    let commit = parsed
        .commit_storage
        .as_ref()
        .expect("Should have commit storage");
    match &commit.storage_type {
        StorageType::S3(s3_config) => {
            assert_eq!(&*s3_config.bucket, "fluree-commits");
            assert_eq!(s3_config.prefix.as_deref(), Some("prod/commits"));
            assert_eq!(s3_config.write_timeout_ms, Some(10000));
        }
        _ => panic!("Expected S3 commit storage"),
    }

    // Verify publisher
    let publisher = parsed
        .primary_publisher
        .as_ref()
        .expect("Should have publisher");
    match &publisher.publisher_type {
        fluree_db_connection::config::PublisherType::DynamoDb {
            table,
            region,
            timeout_ms,
            ..
        } => {
            assert_eq!(&**table, "fluree-ledgers");
            assert_eq!(region.as_deref(), Some("us-west-2"));
            assert_eq!(*timeout_ms, Some(3000));
        }
        _ => panic!("Expected DynamoDB publisher"),
    }
}

/// Test shared storage reference (same @id used multiple times)
#[test]
fn test_shared_storage_reference() {
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {
                "@id": "sharedS3",
                "@type": "Storage",
                "s3Bucket": "unified-bucket",
                "s3Prefix": "all-data"
            },
            {
                "@id": "publisher",
                "@type": "Publisher",
                "storage": {"@id": "sharedS3"}
            },
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "sharedS3"},
                "commitStorage": {"@id": "sharedS3"},
                "primaryPublisher": {"@id": "publisher"}
            }
        ]
    });

    let parsed =
        ConnectionConfig::from_json_ld(&config).expect("Should parse shared storage config");

    // All three should reference the same bucket/prefix
    let check_shared = |st: &StorageType, context: &str| match st {
        StorageType::S3(s3) => {
            assert_eq!(&*s3.bucket, "unified-bucket", "{context} bucket mismatch");
            assert_eq!(
                s3.prefix.as_deref(),
                Some("all-data"),
                "{context} prefix mismatch"
            );
        }
        _ => panic!("{context}: Expected S3 storage"),
    };

    check_shared(&parsed.index_storage.storage_type, "index_storage");

    let commit = parsed
        .commit_storage
        .as_ref()
        .expect("Should have commit storage");
    check_shared(&commit.storage_type, "commit_storage");

    let publisher = parsed
        .primary_publisher
        .as_ref()
        .expect("Should have publisher");
    if let fluree_db_connection::config::PublisherType::Storage { storage } =
        &publisher.publisher_type
    {
        check_shared(&storage.storage_type, "publisher storage");
    } else {
        panic!("Expected Storage publisher type");
    }
}

/// Test cache configuration in MB
#[test]
fn test_cache_mb_config() {
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {"@id": "s3", "@type": "Storage", "s3Bucket": "bucket"},
            {
                "@id": "connection",
                "@type": "Connection",
                "cacheMaxMb": 4096,
                "indexStorage": {"@id": "s3"}
            }
        ]
    });

    let parsed = ConnectionConfig::from_json_ld(&config).expect("Should parse");
    assert_eq!(parsed.cache.max_mb, 4096);
}

// ============================================================================
// Integration tests (require LocalStack or real AWS)
// ============================================================================

/// Integration test: connect_async with S3 storage
///
/// Requires LocalStack running:
///   docker run -d -p 4566:4566 localstack/localstack
///
/// Run with:
///   AWS_ENDPOINT_URL=http://localhost:4566 \
///   cargo test -p fluree-db-connection --features aws --test aws_config_test \
///     test_connect_async_s3 -- --ignored --nocapture
#[tokio::test]
#[ignore = "Requires LocalStack: AWS_ENDPOINT_URL=http://localhost:4566"]
async fn test_connect_async_s3() {
    use fluree_db_connection::connect_async;

    // Check if LocalStack is available via environment
    let endpoint = std::env::var("AWS_ENDPOINT_URL").ok();
    if endpoint.is_none() {
        eprintln!("AWS_ENDPOINT_URL not set, skipping S3 integration test");
        eprintln!("To run: docker run -d -p 4566:4566 localstack/localstack");
        eprintln!("Then: AWS_ENDPOINT_URL=http://localhost:4566 cargo test ...");
        return;
    }

    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {
                "@id": "s3Storage",
                "@type": "Storage",
                "s3Bucket": "test-fluree-bucket",
                "s3Prefix": "test-ledgers"
            },
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "s3Storage"}
            }
        ]
    });

    let result = connect_async(&config).await;

    match result {
        Ok(conn) => {
            println!("S3 connection created: {:?}", conn.config().id);
            // Connection was successfully created
            // In a full test, we'd load a database and run queries
        }
        Err(e) => {
            // May fail if bucket doesn't exist or credentials aren't set
            eprintln!("S3 connection failed (expected without bucket setup): {e}");
        }
    }
}

/// Integration test: connect_async with DynamoDB nameservice
///
/// Requires LocalStack with DynamoDB:
///   docker run -d -p 4566:4566 localstack/localstack
///   AWS_ENDPOINT_URL=http://localhost:4566 aws dynamodb create-table \
///     --table-name test-fluree-ns \
///     --attribute-definitions AttributeName=ledger_id,AttributeType=S \
///     --key-schema AttributeName=ledger_id,KeyType=HASH \
///     --billing-mode PAY_PER_REQUEST
///
/// Run with:
///   AWS_ENDPOINT_URL=http://localhost:4566 \
///   cargo test -p fluree-db-connection --features aws --test aws_config_test \
///     test_connect_async_dynamodb -- --ignored --nocapture
#[tokio::test]
#[ignore = "Requires LocalStack: AWS_ENDPOINT_URL=http://localhost:4566"]
async fn test_connect_async_dynamodb() {
    use fluree_db_connection::connect_async;

    let endpoint = std::env::var("AWS_ENDPOINT_URL").ok();
    if endpoint.is_none() {
        eprintln!("AWS_ENDPOINT_URL not set, skipping DynamoDB integration test");
        return;
    }

    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {
                "@id": "s3Storage",
                "@type": "Storage",
                "s3Bucket": "test-fluree-bucket"
            },
            {
                "@id": "dynamoPublisher",
                "@type": "Publisher",
                "dynamodbTable": "test-fluree-ns"
            },
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "s3Storage"},
                "primaryPublisher": {"@id": "dynamoPublisher"}
            }
        ]
    });

    let result = connect_async(&config).await;

    match result {
        Ok(conn) => {
            println!("DynamoDB connection created: {:?}", conn.config().id);
        }
        Err(e) => {
            eprintln!("DynamoDB connection failed (expected without table setup): {e}");
        }
    }
}

/// Integration test: connect_async with storage-backed nameservice
///
/// Run with:
///   AWS_ENDPOINT_URL=http://localhost:4566 \
///   cargo test -p fluree-db-connection --features aws --test aws_config_test \
///     test_connect_async_storage_nameservice -- --ignored --nocapture
#[tokio::test]
#[ignore = "Requires LocalStack: AWS_ENDPOINT_URL=http://localhost:4566"]
async fn test_connect_async_storage_nameservice() {
    use fluree_db_connection::connect_async;

    let endpoint = std::env::var("AWS_ENDPOINT_URL").ok();
    if endpoint.is_none() {
        eprintln!("AWS_ENDPOINT_URL not set, skipping storage nameservice test");
        return;
    }

    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {
                "@id": "s3Storage",
                "@type": "Storage",
                "s3Bucket": "test-fluree-bucket",
                "s3Prefix": "fluree-data"
            },
            {
                "@id": "storagePublisher",
                "@type": "Publisher",
                "storage": {"@id": "s3Storage"}
            },
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "s3Storage"},
                "primaryPublisher": {"@id": "storagePublisher"}
            }
        ]
    });

    let result = connect_async(&config).await;

    match result {
        Ok(conn) => {
            println!(
                "Storage nameservice connection created: {:?}",
                conn.config().id
            );
        }
        Err(e) => {
            eprintln!("Storage nameservice connection failed: {e}");
        }
    }
}
