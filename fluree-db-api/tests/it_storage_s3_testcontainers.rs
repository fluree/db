//! S3 storage integration tests using testcontainers + LocalStack.
//!
//! Run (requires Docker):
//!   cargo test -p fluree-db-api --features aws-testcontainers --test it_storage_s3_testcontainers -- --nocapture

#![cfg(feature = "aws-testcontainers")]

mod support;

use aws_config::meta::region::RegionProviderChain;
use fluree_db_api::{tx, Fluree};
use fluree_db_connection::ConnectionConfig;
use fluree_db_indexer::IndexerConfig;
use fluree_db_storage_aws::{DynamoDbConfig, DynamoDbNameService, S3Config, S3Storage};
use fs2::FileExt;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{runners::AsyncRunner, GenericImage, ImageExt};

const LOCALSTACK_EDGE_PORT: u16 = 4566;
const REGION: &str = "us-east-1";

struct LocalstackTestLock {
    _file: std::fs::File,
}

fn set_test_aws_env() {
    // Dummy credentials accepted by LocalStack.
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_REGION", REGION);
    std::env::set_var("AWS_DEFAULT_REGION", REGION);
    // Avoid IMDS lookups that can hang tests.
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

async fn ensure_dynamodb_table(sdk_config: &aws_config::SdkConfig, table_name: &str) {
    let client = aws_sdk_dynamodb::Client::new(sdk_config);
    let ns = DynamoDbNameService::from_client(client, table_name.to_string());
    ns.ensure_table()
        .await
        .expect("DynamoDB table creation failed");
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

fn build_fluree(storage: S3Storage, nameservice: DynamoDbNameService) -> Fluree {
    Fluree::new(
        ConnectionConfig::default(),
        storage,
        fluree_db_api::NameServiceMode::ReadWrite(Arc::new(nameservice)),
    )
}

async fn list_object_keys(sdk_config: &aws_config::SdkConfig, bucket: &str) -> Vec<String> {
    let s3 = aws_sdk_s3::Client::new(sdk_config);
    let resp = s3
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .expect("list_objects_v2");
    resp.contents()
        .iter()
        .filter_map(|o| o.key().map(std::string::ToString::to_string))
        .collect()
}

#[tokio::test]
async fn s3_testcontainers_basic_test() {
    // Boot LocalStack (edge port 4566)
    let (_lock, _container, endpoint) = start_localstack("s3,dynamodb").await;

    let sdk_config = sdk_config_for_localstack(&endpoint).await;

    // Provision infra
    let bucket = "fluree-test";
    let table = "fluree-test-ns";
    ensure_bucket(&sdk_config, bucket).await;
    ensure_dynamodb_table(&sdk_config, table).await;

    // Create Fluree over S3 + DynamoDB nameservice
    let storage = S3Storage::new(
        &sdk_config,
        S3Config {
            bucket: bucket.to_string(),
            prefix: Some("test".to_string()),
            endpoint: None,
            force_path_style: None,
            timeout_ms: Some(30_000),
            max_retries: None,
            retry_base_delay_ms: None,
            retry_max_delay_ms: None,
            max_concurrent_requests: None,
        },
    )
    .await
    .expect("S3Storage::new");

    let nameservice = DynamoDbNameService::new(
        &sdk_config,
        DynamoDbConfig {
            table_name: table.to_string(),
            region: None,
            endpoint: None,
            timeout_ms: Some(30_000),
        },
    )
    .await
    .expect("DynamoDbNameService::new");

    let fluree = build_fluree(storage.clone(), nameservice.clone());

    // Create ledger + insert data + query
    let ledger_id = "testcontainers-test:main";
    let ledger0 = fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");

    let tx = json!({
        "@context": [support::default_context(), {"ex": "http://example.org/ns/"}],
        "insert": [
            {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice"},
            {"@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob"}
        ]
    });
    let ledger1 = fluree.update(ledger0, &tx).await.expect("update").ledger;

    let q = json!({
        "@context": [support::default_context(), {"ex": "http://example.org/ns/"}],
        "select": ["?s", "?name"],
        "where": {"@id": "?s", "@type": "ex:Person", "ex:name": "?name"}
    });
    let results = support::query_jsonld(&fluree, &ledger1, &q)
        .await
        .expect("query")
        .to_jsonld_async(ledger1.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    assert_eq!(results.as_array().unwrap().len(), 2);
    assert_eq!(results, json!([["ex:alice", "Alice"], ["ex:bob", "Bob"]]));

    // Reload from a "fresh connection" (new cache) and re-query
    let fluree2 = build_fluree(storage.clone(), nameservice.clone());
    let reloaded = fluree2.ledger(ledger_id).await.expect("ledger reload");
    let reload_results = support::query_jsonld(&fluree2, &reloaded, &q)
        .await
        .expect("query reload")
        .to_jsonld_async(reloaded.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    assert_eq!(results, reload_results);

    // Verify no double slashes in stored S3 keys
    let keys = list_object_keys(&sdk_config, bucket).await;
    assert!(!keys.is_empty(), "expected objects in bucket after commit");
    assert!(
        keys.iter().all(|k| !k.contains("//")),
        "paths should not contain double slashes: {keys:?}"
    );
}

#[tokio::test]
#[cfg(feature = "native")]
async fn s3_testcontainers_indexing_test() {
    use fluree_db_transact::{CommitOpts, TxnOpts};

    // Boot LocalStack
    let (_lock, _container, endpoint) = start_localstack("s3,dynamodb").await;

    let sdk_config = sdk_config_for_localstack(&endpoint).await;

    let bucket = "fluree-indexing-test";
    let table = "fluree-indexing-test-ns";
    ensure_bucket(&sdk_config, bucket).await;
    ensure_dynamodb_table(&sdk_config, table).await;

    let storage = S3Storage::new(
        &sdk_config,
        S3Config {
            bucket: bucket.to_string(),
            prefix: Some("indexing".to_string()),
            endpoint: None,
            force_path_style: None,
            timeout_ms: Some(30_000),
            max_retries: None,
            retry_base_delay_ms: None,
            retry_max_delay_ms: None,
            max_concurrent_requests: None,
        },
    )
    .await
    .expect("S3Storage::new");

    let nameservice = DynamoDbNameService::new(
        &sdk_config,
        DynamoDbConfig {
            table_name: table.to_string(),
            region: None,
            endpoint: None,
            timeout_ms: Some(30_000),
        },
    )
    .await
    .expect("DynamoDbNameService::new");

    let mut fluree = build_fluree(storage.clone(), nameservice.clone());

    // Start background indexing worker + handle (LocalSet since worker may be !Send)
    let (local, handle) = support::start_background_indexer_local(
        fluree.backend().clone(),
        std::sync::Arc::new(nameservice.clone()),
        IndexerConfig::small(),
    );
    fluree.set_indexing_mode(tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "indexing-test:main";
            let ledger0 = fluree
                .create_ledger(ledger_id)
                .await
                .expect("create ledger");

            // Insert enough data to justify indexing and force indexing_needed=true.
            let tx = json!({
                "@context": [support::default_context(), {"ex": "http://example.org/ns/"}],
                "insert": (0..50).map(|i| json!({
                    "@id": format!("ex:person{}", i),
                    "@type": "ex:Person",
                    "ex:name": format!("Person {}", i),
                    "ex:age": i
                })).collect::<Vec<_>>()
            });

            let index_cfg = fluree_db_api::IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 1_000_000,
            };

            let result = fluree
                .insert_with_opts(
                    ledger0,
                    &tx,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert_with_opts");

            // Trigger indexing and wait
            let completion = handle
                .trigger(result.ledger.ledger_id(), result.receipt.t)
                .await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed {
                    index_t, root_id, ..
                } => {
                    assert!(index_t >= result.receipt.t);
                    assert!(root_id.is_some(), "expected root_id after indexing");
                }
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            // Verify index address got published to nameservice
            let rec = fluree
                .nameservice()
                .lookup(result.ledger.ledger_id())
                .await
                .expect("nameservice lookup")
                .expect("record exists");
            assert!(rec.index_head_id.is_some(), "expected published index id");

            // Verify bucket contains index artifacts and no double slashes
            let keys = list_object_keys(&sdk_config, bucket).await;
            assert!(!keys.is_empty());
            assert!(
                keys.iter().all(|k| !k.contains("//")),
                "paths should not contain double slashes: {keys:?}"
            );
            assert!(
                keys.iter().any(|k| k.contains("index")),
                "expected some index files in S3: {keys:?}"
            );
        })
        .await;
}

/// End-to-end hard drop on S3 + DynamoDB.
///
/// Covers the fixes that were needed for Hard drop to work on AWS:
/// 1. `DynamoDbNameService::purge` deletes every row under the alias (not just
///    flipping `meta.retracted = true`), so the alias can be re-initialized.
/// 2. `S3Storage::list_prefix` parses the `fluree:s3://...` scheme like
///    reads/writes/deletes, so the enumeration actually finds artifacts under
///    the configured bucket prefix.
/// 3. `drop_artifacts` enumerates per-subprefix (`commit/`, `txn/`, `index/`,
///    `config/`, and optionally `@shared/dicts/`) so `TieredStorage` routes
///    each list to the right tier and no artifact class is missed.
#[tokio::test]
#[cfg(feature = "native")]
async fn s3_testcontainers_hard_drop_clears_ledger() {
    use fluree_db_api::{DropMode, DropStatus};

    let (_lock, _container, endpoint) = start_localstack("s3,dynamodb").await;
    let sdk_config = sdk_config_for_localstack(&endpoint).await;

    let bucket = "fluree-drop-test";
    let table = "fluree-drop-test-ns";
    ensure_bucket(&sdk_config, bucket).await;
    ensure_dynamodb_table(&sdk_config, table).await;

    let storage = S3Storage::new(
        &sdk_config,
        S3Config {
            bucket: bucket.to_string(),
            prefix: Some("fluree-data".to_string()),
            endpoint: None,
            force_path_style: None,
            timeout_ms: Some(30_000),
            max_retries: None,
            retry_base_delay_ms: None,
            retry_max_delay_ms: None,
            max_concurrent_requests: None,
        },
    )
    .await
    .expect("S3Storage::new");

    let nameservice = DynamoDbNameService::new(
        &sdk_config,
        DynamoDbConfig {
            table_name: table.to_string(),
            region: None,
            endpoint: None,
            timeout_ms: Some(30_000),
        },
    )
    .await
    .expect("DynamoDbNameService::new");

    let fluree = build_fluree(storage.clone(), nameservice.clone());

    let ledger_id = "drop-test:main";
    let ledger_name = "drop-test";
    let ledger0 = fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");

    let tx = json!({
        "@context": [support::default_context(), {"ex": "http://example.org/ns/"}],
        "insert": (0..10).map(|i| json!({
            "@id": format!("ex:person{}", i),
            "@type": "ex:Person",
            "ex:name": format!("Person {}", i)
        })).collect::<Vec<_>>()
    });
    let _ledger1 = fluree.update(ledger0, &tx).await.expect("update").ledger;

    // Write a default context so the bucket also has `config/` artifacts
    // (both `ConfigPayload` and `default_context` blobs are stored under
    // `ContentKind::LedgerConfig` → `{ledger}/{branch}/config/`).
    fluree
        .set_default_context(
            ledger_id,
            &json!({"ex": "http://example.org/ns/", "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"}),
        )
        .await
        .expect("set_default_context");

    // Sanity: commits and a config blob landed in the bucket.
    let keys_before = list_object_keys(&sdk_config, bucket).await;
    assert!(
        keys_before.iter().any(|k| k.contains("/commit/")),
        "expected commit artifacts before drop: {keys_before:?}"
    );
    assert!(
        keys_before
            .iter()
            .any(|k| k.contains("/drop-test/main/config/")),
        "expected a config artifact before drop: {keys_before:?}"
    );

    // Hard drop.
    let report = fluree
        .drop_ledger(ledger_name, DropMode::Hard)
        .await
        .expect("drop_ledger");
    assert_eq!(report.status, DropStatus::Dropped);
    assert!(
        report.artifacts_deleted > 0,
        "expected hard drop to remove artifacts, got 0 with warnings: {:?}",
        report.warnings
    );

    // Nameservice purged: lookup returns None and we can re-init the alias.
    let after = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("lookup after drop");
    assert!(
        after.is_none(),
        "hard drop must purge the NS record entirely, got: {after:?}"
    );

    // Bucket cleared of every per-ledger artifact class we enumerate:
    // commit/, txn/, index/, config/, and (since this is the only branch)
    // @shared/dicts/. We don't assert on the bucket prefix itself.
    let keys_after = list_object_keys(&sdk_config, bucket).await;
    let stragglers: Vec<_> = keys_after
        .iter()
        .filter(|k| {
            k.contains("/drop-test/main/commit/")
                || k.contains("/drop-test/main/txn/")
                || k.contains("/drop-test/main/index/")
                || k.contains("/drop-test/main/config/")
                || k.contains("/drop-test/@shared/dicts/")
        })
        .cloned()
        .collect();
    assert!(
        stragglers.is_empty(),
        "expected no per-ledger artifacts after hard drop, found: {stragglers:?}"
    );

    // Re-initialization works because purge removed every row under the alias.
    let _reinit = fluree
        .create_ledger(ledger_id)
        .await
        .expect("re-create ledger after hard drop");
}

/// The collector and a fork drop, end to end on S3 + DynamoDB with the worker
/// running. Exercises what differs from the file backends: batch releases as
/// `DeleteObjects` against the shared dictionary prefix, sibling discovery
/// through DynamoDB's eventually consistent listing index, and each sibling's
/// head through a consistent `lookup`.
// Multi-threaded, as a server is: an insert after the first build probes the
// persisted dictionaries synchronously, and on remote storage that read needs
// a runtime thread other than the one blocked on it. On a current-thread
// runtime the probe times out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(feature = "native")]
async fn s3_testcontainers_collector_and_fork_drop_keep_every_referenced_dictionary() {
    let (_lock, _container, endpoint) = start_localstack("s3,dynamodb").await;
    let sdk_config = sdk_config_for_localstack(&endpoint).await;
    let bucket = "fluree-gc-e2e";
    let table = "fluree-gc-e2e-ns";
    ensure_bucket(&sdk_config, bucket).await;
    ensure_dynamodb_table(&sdk_config, table).await;

    let storage = S3Storage::new(
        &sdk_config,
        S3Config {
            bucket: bucket.to_string(),
            prefix: Some("gc".to_string()),
            endpoint: None,
            force_path_style: None,
            timeout_ms: Some(30_000),
            max_retries: None,
            retry_base_delay_ms: None,
            retry_max_delay_ms: None,
            max_concurrent_requests: None,
        },
    )
    .await
    .expect("S3Storage::new");
    let nameservice = DynamoDbNameService::new(
        &sdk_config,
        DynamoDbConfig {
            table_name: table.to_string(),
            region: None,
            endpoint: None,
            timeout_ms: Some(30_000),
        },
    )
    .await
    .expect("DynamoDbNameService::new");

    let mut fluree = build_fluree(storage, nameservice.clone());
    // Spawned, as the server does, rather than on a `LocalSet`: the blocking
    // region a remote dictionary read enters is not allowed inside one.
    let (worker, handle) = fluree_db_api::BackgroundIndexerWorker::new(
        fluree.backend().clone(),
        Arc::new(nameservice),
        support::collecting_indexer_config(),
    );
    let worker = tokio::spawn(worker.run());
    fluree.set_indexing_mode(tx::IndexingMode::Background(handle.clone()));

    support::run_collector_and_fork_drop_scenario(&fluree, &handle, "gc-e2e").await;
    worker.abort();
}

/// `delete_many` issues one `DeleteObjects` per thousand keys: every key in a
/// batch spanning two requests goes, a key that never existed is not a
/// failure, and nothing outside the batch is touched.
#[tokio::test]
async fn s3_testcontainers_delete_many_batches_across_requests() {
    use fluree_db_core::{StorageRead, StorageWrite};
    use futures::StreamExt;

    let (_lock, _container, endpoint) = start_localstack("s3").await;
    let sdk_config = sdk_config_for_localstack(&endpoint).await;
    let bucket = "fluree-delete-many";
    ensure_bucket(&sdk_config, bucket).await;

    let storage = S3Storage::new(
        &sdk_config,
        S3Config {
            bucket: bucket.to_string(),
            prefix: Some("test".to_string()),
            endpoint: None,
            force_path_style: None,
            timeout_ms: Some(30_000),
            max_retries: None,
            retry_base_delay_ms: None,
            retry_max_delay_ms: None,
            max_concurrent_requests: None,
        },
    )
    .await
    .expect("S3Storage::new");

    let address = |i: usize| format!("fluree:s3://batch/main/index/objects/leaves/{i:05}.fli");
    let doomed: Vec<String> = (0..1003).map(address).collect();
    let survivor = address(9000);
    let writes: Vec<_> = doomed
        .iter()
        .chain(std::iter::once(&survivor))
        .map(|addr| storage.write_bytes(addr, b"x"))
        .collect();
    let written: Vec<_> = futures::stream::iter(writes)
        .buffer_unordered(16)
        .collect()
        .await;
    assert!(written.iter().all(Result::is_ok), "seed writes succeed");
    assert_eq!(list_all_keys(&sdk_config, bucket).await, 1004);

    let mut batch = doomed.clone();
    batch.push(address(9999));
    let failures = storage.delete_many(&batch).await;
    assert!(
        failures.is_empty(),
        "a missing key is not a failure: {failures:?}"
    );
    assert_eq!(
        list_all_keys(&sdk_config, bucket).await,
        1,
        "both DeleteObjects requests landed"
    );
    assert_eq!(storage.read_bytes(&survivor).await.expect("survivor"), b"x");
}

/// Object count in `bucket`, paginated: `list_object_keys` reads one page.
async fn list_all_keys(sdk_config: &aws_config::SdkConfig, bucket: &str) -> usize {
    let s3 = aws_sdk_s3::Client::new(sdk_config);
    let mut pages = s3.list_objects_v2().bucket(bucket).into_paginator().send();
    let mut count = 0;
    while let Some(page) = pages.next().await {
        count += page.expect("list_objects_v2").contents().len();
    }
    count
}

/// `build_client()` from a JSON-LD S3 config with `AES256Key`: every object
/// the ledger writes to the bucket carries the encryption envelope, and a
/// second client built from the same config reads it back.
#[tokio::test]
async fn s3_testcontainers_build_client_honours_aes256_key() {
    use fluree_db_api::FlureeBuilder;

    let (_lock, _container, endpoint) = start_localstack("s3,dynamodb").await;
    let sdk_config = sdk_config_for_localstack(&endpoint).await;

    let bucket = "fluree-encrypted-test";
    let table = "fluree-encrypted-test-ns";
    ensure_bucket(&sdk_config, bucket).await;
    ensure_dynamodb_table(&sdk_config, table).await;

    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {
                "@id": "storage",
                "@type": "Storage",
                "s3Bucket": bucket,
                "s3Prefix": "enc",
                "s3Endpoint": endpoint,
                "s3ForcePathStyle": true,
                "AES256Key": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
            },
            {
                "@id": "publisher",
                "@type": "Publisher",
                "dynamodbTable": table,
                "dynamodbEndpoint": endpoint
            },
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "storage"},
                "primaryPublisher": {"@id": "publisher"}
            }
        ]
    });
    let build = || async {
        let builder = FlureeBuilder::from_json_ld(&config).expect("config");
        assert!(builder.has_encryption_key());
        builder.build_client().await.expect("build_client")
    };

    let fluree = build().await;
    let ledger_id = "encrypted-s3:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.expect("create");
    let tx = json!({
        "@context": [support::default_context(), {"ex": "http://example.org/ns/"}],
        "insert": [
            {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice"},
            {"@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob"}
        ]
    });
    fluree.update(ledger0, &tx).await.expect("update");

    let s3 = aws_sdk_s3::Client::new(&sdk_config);
    let keys = list_object_keys(&sdk_config, bucket).await;
    assert!(!keys.is_empty(), "expected objects in bucket after commit");
    for key in &keys {
        let body = s3
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("get_object")
            .body
            .collect()
            .await
            .expect("body")
            .into_bytes();
        assert!(
            body.starts_with(b"FLU\x00"),
            "plaintext object in bucket: {key}"
        );
    }

    let fluree2 = build().await;
    let reloaded = fluree2.ledger(ledger_id).await.expect("ledger reload");
    let q = json!({
        "@context": [support::default_context(), {"ex": "http://example.org/ns/"}],
        "select": ["?s", "?name"],
        "where": {"@id": "?s", "@type": "ex:Person", "ex:name": "?name"}
    });
    let results = support::query_jsonld(&fluree2, &reloaded, &q)
        .await
        .expect("query")
        .to_jsonld_async(reloaded.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    assert_eq!(results, json!([["ex:alice", "Alice"], ["ex:bob", "Bob"]]));
}
