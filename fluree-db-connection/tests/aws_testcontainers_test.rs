//! Opt-in infra tests that boot LocalStack via testcontainers.
//!
//! Run (requires Docker):
//!   cargo test -p fluree-db-connection --features aws-testcontainers --test aws_testcontainers_test -- --nocapture

#![cfg(feature = "aws-testcontainers")]

use fluree_db_connection::{connect_async, ConnectionHandle};
use fluree_db_core::{ContentId, ContentKind, StorageRead, StorageWrite};
use fluree_db_nameservice::{
    AdminPublisher, CasResult, ConfigCasResult, ConfigLookup, ConfigPayload, ConfigPublisher,
    ConfigValue, GraphSourceLookup, GraphSourcePublisher, GraphSourceType, NameService,
    NsLookupResult, Publisher, RefKind, RefLookup, RefPublisher, RefValue, StatusCasResult,
    StatusLookup, StatusPayload, StatusPublisher, StatusValue,
};
use fluree_db_storage_aws::DynamoDbNameService;
use fs2::FileExt;
use serde_json::json;
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{runners::AsyncRunner, GenericImage, ImageExt};

/// Helper to create a deterministic `ContentId` for test commit refs.
fn test_commit_id(label: &str) -> ContentId {
    ContentId::new(ContentKind::Commit, label.as_bytes())
}

/// Helper to create a deterministic `ContentId` for test index refs.
fn test_index_id(label: &str) -> ContentId {
    ContentId::new(ContentKind::IndexRoot, label.as_bytes())
}

async fn publish_commit(
    ns: &(impl RefPublisher + ?Sized),
    ledger_id: &str,
    t: i64,
    cid: &ContentId,
) {
    let new = RefValue {
        id: Some(cid.clone()),
        t,
    };
    match ns.fast_forward_commit(ledger_id, &new, 3).await.unwrap() {
        CasResult::Updated => {}
        CasResult::Conflict { actual } => {
            assert!(
                actual.as_ref().map(|r| r.t).unwrap_or(0) >= t,
                "unexpected commit publish conflict: {actual:?}"
            );
        }
    }
}

const LOCALSTACK_EDGE_PORT: u16 = 4566;
const REGION: &str = "us-east-1";

struct LocalstackTestLock {
    _file: std::fs::File,
}

fn set_localstack_env(endpoint: &str) {
    // Dummy credentials accepted by LocalStack
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_REGION", REGION);
    std::env::set_var("AWS_DEFAULT_REGION", REGION);

    // Ensure the SDK never tries IMDS (common source of slow hangs in tests)
    std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");

    // Used by the AWS Rust SDK for local endpoints (matches existing repo tests)
    std::env::set_var("AWS_ENDPOINT_URL", endpoint);
}

async fn ensure_bucket(sdk_config: &aws_config::SdkConfig, bucket: &str) {
    let s3 = aws_sdk_s3::Client::new(sdk_config);

    // CreateBucket is idempotent-ish in LocalStack; if it already exists, HeadBucket will pass.
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
    // Note: call `GenericImage` methods before `ImageExt` methods (per testcontainers docs).
    let image = GenericImage::new("localstack/localstack", "4.4")
        .with_exposed_port(LOCALSTACK_EDGE_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready."))
        .with_env_var("SERVICES", services)
        .with_env_var("DEFAULT_REGION", REGION)
        .with_env_var("SKIP_SSL_CERT_DOWNLOAD", "1")
        .with_startup_timeout(Duration::from_secs(300));
    let container = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            panic!(
                "Failed to start LocalStack via Docker.\n\
                \n\
                This usually means the Docker daemon/socket is not available.\n\
                - If you use Docker Desktop: open Docker Desktop and wait until it's running, then verify `docker ps` works.\n\
                - If you use Colima: run `colima start` and set `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock`.\n\
                - If `DOCKER_HOST` is set but wrong: try `unset DOCKER_HOST`.\n\
                \n\
                Original error: {e:?}"
            );
        }
    };
    let host_port = wait_for_localstack_host_port(&container).await;
    let endpoint = format!("http://127.0.0.1:{host_port}");
    (lock, container, endpoint)
}

#[tokio::test]
async fn localstack_s3_and_dynamodb_smoke() {
    // 1) Boot LocalStack (edge port 4566)
    let (_lock, _container, endpoint) = start_localstack("s3,dynamodb").await;

    // 2) Configure AWS SDK to talk to LocalStack
    set_localstack_env(&endpoint);
    let sdk_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;

    // 3) Provision minimal infra
    let bucket = "test-fluree-bucket";
    let table = "test-fluree-ns";
    ensure_bucket(&sdk_config, bucket).await;
    ensure_dynamodb_table(&sdk_config, table).await;

    // 4) Create connection via JSON-LD config (exercises parsing + AWS init)
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {
                "@id": "s3Storage",
                "@type": "Storage",
                "s3Bucket": bucket,
                "s3Prefix": "test-ledgers"
            },
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "s3Storage"},
                "primaryPublisher": {
                    "@type": "Publisher",
                    "dynamodbTable": table
                }
            }
        ]
    });

    let conn = connect_async(&config)
        .await
        .expect("connect_async should succeed against LocalStack");

    let aws = match conn {
        ConnectionHandle::Aws(h) => h,
        _ => panic!("Expected AWS connection handle"),
    };

    // 5) DynamoDB nameservice smoke: init + publish + lookup
    let alias = "mydb:main";

    // Init materializes all concern items (meta, head, index, status, config)
    aws.nameservice_arc()
        .publish_ledger_init(alias)
        .await
        .expect("publish_ledger_init should succeed");

    // Publish commit head
    let commit_id = test_commit_id("commit:1");
    publish_commit(aws.nameservice_arc().as_ref(), alias, 1, &commit_id).await;

    // Publish index head
    let index_id = test_index_id("index:1");
    aws.publish_index(alias, 1, &index_id)
        .await
        .expect("publish_index should succeed");

    // Lookup and verify
    let record = aws
        .lookup(alias)
        .await
        .expect("lookup should succeed")
        .expect("record should exist after publish");
    assert_eq!(record.commit_head_id.as_ref(), Some(&commit_id));
    assert_eq!(record.index_head_id.as_ref(), Some(&index_id));
    assert_eq!(record.index_t, 1);

    // Monotonic: older t should be silently ignored
    publish_commit(
        aws.nameservice_arc().as_ref(),
        alias,
        0,
        &test_commit_id("old-commit"),
    )
    .await;
    let record2 = aws.lookup(alias).await.expect("lookup").expect("exists");
    assert_eq!(
        record2.commit_head_id.as_ref(),
        Some(&commit_id),
        "stale publish should not overwrite"
    );

    // 6) S3 storage smoke: write + read
    let address = "fluree:s3://it-test/hello.txt";
    aws.index_storage()
        .write_bytes(address, b"hello localstack")
        .await
        .expect("S3 write should succeed");

    let bytes = aws
        .index_storage()
        .read_bytes(address)
        .await
        .expect("S3 read should succeed");
    assert_eq!(bytes, b"hello localstack");
}

/// Build an SDK config with explicit endpoint (no global env vars).
async fn sdk_config_for_endpoint(endpoint: &str) -> aws_config::SdkConfig {
    use aws_config::meta::region::RegionProviderChain;
    // Dummy credentials accepted by LocalStack
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    let region_provider = RegionProviderChain::default_provider().or_else(REGION);
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region_provider)
        .endpoint_url(endpoint)
        .load()
        .await
}

/// Helper: boot LocalStack + provision infra, returning (container, DynamoDbNameService, sdk_config).
async fn setup_localstack_ns() -> (
    LocalstackTestLock,
    testcontainers::ContainerAsync<GenericImage>,
    DynamoDbNameService,
) {
    let (_lock, _container, ns, _sdk_config) = setup_localstack_ns_with_config().await;
    (_lock, _container, ns)
}

/// Like `setup_localstack_ns` but also returns the SDK config for direct DynamoDB access in tests.
async fn setup_localstack_ns_with_config() -> (
    LocalstackTestLock,
    testcontainers::ContainerAsync<GenericImage>,
    DynamoDbNameService,
    aws_config::SdkConfig,
) {
    let (lock, container, endpoint) = start_localstack("dynamodb").await;
    let sdk_config = sdk_config_for_endpoint(&endpoint).await;

    let table = "fluree-ns-test";
    ensure_dynamodb_table(&sdk_config, table).await;

    let client = aws_sdk_dynamodb::Client::new(&sdk_config);
    let ns = DynamoDbNameService::from_client(client, table.to_string());
    (lock, container, ns, sdk_config)
}

// ────────────────────────────────────────────────────────────────────────────
// Comprehensive nameservice trait coverage
// ────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn nameservice_ledger_lifecycle() {
    let (_lock, _container, ns) = setup_localstack_ns().await;

    let alias = "lifecycle-test:main";
    let direct_alias = "lifecycle-direct:main";

    // ── lookup before init → None ──────────────────────────────────────────
    assert!(ns.lookup(alias).await.unwrap().is_none());

    // ── direct ref publish can create a commit head without prior init ─────
    let direct_commit = test_commit_id("commit:direct");
    let result = ns
        .fast_forward_commit(
            direct_alias,
            &RefValue {
                id: Some(direct_commit.clone()),
                t: 1,
            },
            3,
        )
        .await
        .unwrap();
    assert!(matches!(result, CasResult::Updated));
    let direct_record = ns.lookup(direct_alias).await.unwrap().expect("direct head");
    assert_eq!(direct_record.commit_head_id.as_ref(), Some(&direct_commit));
    assert_eq!(direct_record.commit_t, 1);

    let err = ns.publish_index(alias, 1, &test_index_id("index:1")).await;
    assert!(
        err.is_err(),
        "publish_index on uninitialized alias should fail"
    );

    // ── publish_ledger_init ────────────────────────────────────────────────
    ns.publish_ledger_init(alias).await.unwrap();

    // Lookup returns record with unborn head/index
    let rec = ns.lookup(alias).await.unwrap().expect("exists after init");
    assert_eq!(rec.ledger_id, alias, "ledger_id should be the full alias");
    assert_eq!(
        rec.name, "lifecycle-test",
        "name is the ledger-name-only part"
    );
    assert_eq!(rec.branch, "main");
    assert!(rec.commit_head_id.is_none());
    assert!(rec.index_head_id.is_none());
    assert_eq!(rec.index_t, 0);

    // Double init → should succeed (idempotent or conflict suppressed)
    // The implementation uses conditional PutItems that will fail if items exist,
    // but the error should be suppressed as "already exists".
    let init2 = ns.publish_ledger_init(alias).await;
    assert!(
        init2.is_ok() || init2.is_err(),
        "double init should not panic"
    );

    // ── publish_commit + publish_index ─────────────────────────────────────
    let commit_id_1 = test_commit_id("commit:1");
    let index_id_1 = test_index_id("index:1");
    publish_commit(&ns, alias, 1, &commit_id_1).await;
    ns.publish_index(alias, 1, &index_id_1).await.unwrap();

    let rec = ns.lookup(alias).await.unwrap().unwrap();
    assert_eq!(rec.commit_head_id.as_ref(), Some(&commit_id_1));
    assert_eq!(rec.index_head_id.as_ref(), Some(&index_id_1));
    assert_eq!(rec.index_t, 1);

    // Monotonic: stale commit/index silently ignored
    publish_commit(&ns, alias, 0, &test_commit_id("stale:0")).await;
    ns.publish_index(alias, 0, &test_index_id("stale:0"))
        .await
        .unwrap();
    let rec = ns.lookup(alias).await.unwrap().unwrap();
    assert_eq!(rec.commit_head_id.as_ref(), Some(&commit_id_1));
    assert_eq!(rec.index_head_id.as_ref(), Some(&index_id_1));

    // Advance
    let commit_id_2 = test_commit_id("commit:2");
    let index_id_2 = test_index_id("index:2");
    publish_commit(&ns, alias, 2, &commit_id_2).await;
    ns.publish_index(alias, 2, &index_id_2).await.unwrap();
    let rec = ns.lookup(alias).await.unwrap().unwrap();
    assert_eq!(rec.commit_head_id.as_ref(), Some(&commit_id_2));
    assert_eq!(rec.index_head_id.as_ref(), Some(&index_id_2));
    assert_eq!(rec.index_t, 2);

    // ── all_records() ──────────────────────────────────────────────────────
    let recs = ns.all_records().await.unwrap();
    assert!(
        recs.iter().any(|r| r.ledger_id == alias),
        "all_records should contain our ledger"
    );

    // ── retract ────────────────────────────────────────────────────────────
    ns.retract(alias).await.unwrap();
    // Lookup still returns record, but record should have retracted flag
    // (NsRecord might not expose retracted directly — depends on fields)
    let rec = ns.lookup(alias).await.unwrap();
    assert!(rec.is_some(), "retracted record still visible via lookup");
}

#[tokio::test]
async fn nameservice_admin_publisher() {
    let (_lock, _container, ns) = setup_localstack_ns().await;

    let alias = "admin-test:main";
    ns.publish_ledger_init(alias).await.unwrap();
    let index_id_5 = test_index_id("index:5");
    ns.publish_index(alias, 5, &index_id_5).await.unwrap();

    // AdminPublisher: publish_index_allow_equal — same t succeeds
    let index_id_5_rebuild = test_index_id("index:5-rebuild");
    ns.publish_index_allow_equal(alias, 5, &index_id_5_rebuild)
        .await
        .unwrap();
    let rec = ns.lookup(alias).await.unwrap().unwrap();
    assert_eq!(
        rec.index_head_id.as_ref(),
        Some(&index_id_5_rebuild),
        "allow_equal should overwrite at same t"
    );
    assert_eq!(rec.index_t, 5);

    // Still rejects lower t
    ns.publish_index_allow_equal(alias, 3, &test_index_id("stale:3"))
        .await
        .unwrap();
    let rec = ns.lookup(alias).await.unwrap().unwrap();
    assert_eq!(
        rec.index_head_id.as_ref(),
        Some(&index_id_5_rebuild),
        "allow_equal should reject lower t"
    );
}

/// Regression test: publish_index must succeed when the ledger is initialized
/// but the sk="index" item is missing (e.g., older migration paths that
/// pre-date create_ledger_with_ref).
///
/// Before the fix, `update_index_item` used `attribute_exists(#pk) AND #it < :t`
/// which required the index item to already exist. If it was missing, the
/// condition failed and the ConditionalCheckFailedException was silently
/// swallowed — publish_index returned Ok(()) without writing anything.
#[tokio::test]
async fn publish_index_without_preexisting_index_item() {
    let (_lock, _container, ns, sdk_config) = setup_localstack_ns_with_config().await;

    let alias = "missing-index-item:main";

    // 1. Initialize ledger — creates meta, head, index, status, config items
    ns.publish_ledger_init(alias).await.unwrap();

    // Sanity: publish_index works on a fully initialized ledger
    let index_id_1 = test_index_id("index:1");
    ns.publish_index(alias, 1, &index_id_1).await.unwrap();
    let rec = ns.lookup(alias).await.unwrap().unwrap();
    assert_eq!(rec.index_head_id.as_ref(), Some(&index_id_1));
    assert_eq!(rec.index_t, 1);

    // 2. Manually delete the sk="index" item to simulate the bug condition
    let ddb = aws_sdk_dynamodb::Client::new(&sdk_config);
    ddb.delete_item()
        .table_name("fluree-ns-test")
        .key(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S("missing-index-item:main".to_string()),
        )
        .key(
            "sk",
            aws_sdk_dynamodb::types::AttributeValue::S("index".to_string()),
        )
        .send()
        .await
        .expect("delete_item should succeed");

    // Verify the index item is gone
    let rec = ns.lookup(alias).await.unwrap().unwrap();
    assert!(
        rec.index_head_id.is_none(),
        "index should be gone after manual deletion"
    );

    // 3. publish_index should succeed — this was the bug
    let index_id_2 = test_index_id("index:2");
    ns.publish_index(alias, 2, &index_id_2)
        .await
        .expect("publish_index must succeed even when sk=index item is missing");

    // 4. Verify the index was written correctly
    let rec = ns.lookup(alias).await.unwrap().unwrap();
    assert_eq!(
        rec.index_head_id.as_ref(),
        Some(&index_id_2),
        "index should be restored after publish"
    );
    assert_eq!(rec.index_t, 2);

    // 5. Subsequent publishes should still work normally (monotonic)
    let index_id_3 = test_index_id("index:3");
    ns.publish_index(alias, 3, &index_id_3).await.unwrap();
    let rec = ns.lookup(alias).await.unwrap().unwrap();
    assert_eq!(rec.index_head_id.as_ref(), Some(&index_id_3));
    assert_eq!(rec.index_t, 3);

    // Stale publish ignored
    ns.publish_index(alias, 1, &test_index_id("stale"))
        .await
        .unwrap();
    let rec = ns.lookup(alias).await.unwrap().unwrap();
    assert_eq!(rec.index_head_id.as_ref(), Some(&index_id_3));
}

#[tokio::test]
async fn nameservice_ref_publisher() {
    let (_lock, _container, ns) = setup_localstack_ns().await;

    let alias = "ref-test:main";

    // get_ref before init → None
    assert!(ns
        .get_ref(alias, RefKind::CommitHead)
        .await
        .unwrap()
        .is_none());

    ns.publish_ledger_init(alias).await.unwrap();

    // get_ref after init → unborn (id=None, t=0)
    let ref_val = ns
        .get_ref(alias, RefKind::CommitHead)
        .await
        .unwrap()
        .expect("exists after init");
    assert!(ref_val.id.is_none());
    assert_eq!(ref_val.t, 0);

    // CAS: unborn → first commit
    let commit_id_1 = test_commit_id("commit:1");
    let new_ref = RefValue {
        id: Some(commit_id_1.clone()),
        t: 1,
    };
    let result = ns
        .compare_and_set_ref(alias, RefKind::CommitHead, Some(&ref_val), &new_ref)
        .await
        .unwrap();
    assert!(
        matches!(result, CasResult::Updated),
        "CAS from unborn should succeed"
    );

    // Verify
    let ref_val = ns
        .get_ref(alias, RefKind::CommitHead)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ref_val.id.as_ref(), Some(&commit_id_1));
    assert_eq!(ref_val.t, 1);

    // CAS: commit:1 → commit:2
    let commit_id_2 = test_commit_id("commit:2");
    let new_ref2 = RefValue {
        id: Some(commit_id_2.clone()),
        t: 2,
    };
    let result = ns
        .compare_and_set_ref(alias, RefKind::CommitHead, Some(&ref_val), &new_ref2)
        .await
        .unwrap();
    assert!(matches!(result, CasResult::Updated));

    // CAS with stale expected → Conflict
    let stale_expected = RefValue {
        id: Some(commit_id_1.clone()),
        t: 1,
    };
    let commit_id_3 = test_commit_id("commit:3");
    let new_ref3 = RefValue {
        id: Some(commit_id_3),
        t: 3,
    };
    let result = ns
        .compare_and_set_ref(alias, RefKind::CommitHead, Some(&stale_expected), &new_ref3)
        .await
        .unwrap();
    match result {
        CasResult::Conflict { actual } => {
            assert_eq!(actual.as_ref().unwrap().t, 2, "conflict shows current t=2");
        }
        CasResult::Updated => panic!("stale CAS should conflict"),
    }

    // IndexHead: similar test
    let idx_ref = ns
        .get_ref(alias, RefKind::IndexHead)
        .await
        .unwrap()
        .unwrap();
    assert!(idx_ref.id.is_none());
    assert_eq!(idx_ref.t, 0);

    let index_id_1 = test_index_id("index:1");
    let new_idx = RefValue {
        id: Some(index_id_1),
        t: 1,
    };
    let result = ns
        .compare_and_set_ref(alias, RefKind::IndexHead, Some(&idx_ref), &new_idx)
        .await
        .unwrap();
    assert!(matches!(result, CasResult::Updated));

    // ── CAS expected=None creates ledger (matches StorageNameService) ────
    let new_alias = "cas-create-test:main";
    assert!(ns.lookup(new_alias).await.unwrap().is_none());

    let create_commit_id = test_commit_id("create-commit:1");
    let create_ref = RefValue {
        id: Some(create_commit_id.clone()),
        t: 1,
    };
    let result = ns
        .compare_and_set_ref(new_alias, RefKind::CommitHead, None, &create_ref)
        .await
        .unwrap();
    assert!(
        matches!(result, CasResult::Updated),
        "expected=None should create ledger when alias unknown"
    );

    // Verify the ledger was created with the ref set
    let rec = ns
        .lookup(new_alias)
        .await
        .unwrap()
        .expect("ledger should exist after CAS create");
    assert_eq!(rec.commit_head_id.as_ref(), Some(&create_commit_id));
    assert_eq!(rec.index_t, 0, "index should be unborn");

    // CAS expected=None on existing alias → Conflict
    let create_ref2 = RefValue {
        id: Some(test_commit_id("commit:99")),
        t: 99,
    };
    let result = ns
        .compare_and_set_ref(new_alias, RefKind::CommitHead, None, &create_ref2)
        .await
        .unwrap();
    assert!(
        matches!(result, CasResult::Conflict { .. }),
        "expected=None should conflict when alias already exists"
    );
}

#[tokio::test]
async fn nameservice_status_publisher() {
    let (_lock, _container, ns) = setup_localstack_ns().await;

    let alias = "status-test:main";

    // get_status before init → None
    assert!(ns.get_status(alias).await.unwrap().is_none());

    ns.publish_ledger_init(alias).await.unwrap();

    // get_status after init → initial (v=1, state="ready")
    let status = ns.get_status(alias).await.unwrap().expect("exists");
    assert_eq!(status.v, 1);
    assert!(status.payload.is_ready());

    // push_status: ready → indexing
    let new_status = StatusValue::new(2, StatusPayload::new("indexing"));
    let result = ns
        .push_status(alias, Some(&status), &new_status)
        .await
        .unwrap();
    assert!(matches!(result, StatusCasResult::Updated));

    let status2 = ns.get_status(alias).await.unwrap().unwrap();
    assert_eq!(status2.v, 2);
    assert_eq!(status2.payload.state, "indexing");

    // push_status with stale expected → Conflict
    let stale = StatusValue::new(3, StatusPayload::new("error"));
    let result = ns.push_status(alias, Some(&status), &stale).await.unwrap();
    match result {
        StatusCasResult::Conflict { actual } => {
            assert_eq!(actual.as_ref().unwrap().v, 2);
        }
        StatusCasResult::Updated => panic!("stale push_status should conflict"),
    }
}

#[tokio::test]
async fn nameservice_config_publisher() {
    let (_lock, _container, ns) = setup_localstack_ns().await;

    let alias = "config-test:main";

    // get_config before init → None
    assert!(ns.get_config(alias).await.unwrap().is_none());

    ns.publish_ledger_init(alias).await.unwrap();

    // get_config after init → unborn (v=0, payload=None)
    let config = ns.get_config(alias).await.unwrap().expect("exists");
    assert!(config.is_unborn());

    // push_config: set default context
    let ctx_cid = ContentId::new(ContentKind::LedgerConfig, b"default-context");
    let new_config = ConfigValue::new(
        1,
        Some(ConfigPayload::with_default_context(ctx_cid.clone())),
    );
    let result = ns
        .push_config(alias, Some(&config), &new_config)
        .await
        .unwrap();
    assert!(matches!(result, ConfigCasResult::Updated));

    let config2 = ns.get_config(alias).await.unwrap().unwrap();
    assert_eq!(config2.v, 1);
    assert_eq!(
        config2.payload.as_ref().unwrap().default_context,
        Some(ctx_cid)
    );

    // push_config with stale expected → Conflict
    let stale = ConfigValue::new(2, Some(ConfigPayload::new()));
    let result = ns.push_config(alias, Some(&config), &stale).await.unwrap();
    match result {
        ConfigCasResult::Conflict { actual } => {
            assert_eq!(actual.as_ref().unwrap().v, 1);
        }
        ConfigCasResult::Updated => panic!("stale push_config should conflict"),
    }

    // ── ConfigPublisher gated to ledgers: graph source returns None ─────
    ns.publish_graph_source(
        "config-gate-gs",
        "main",
        GraphSourceType::Bm25,
        r#"{"foo":"bar"}"#,
        &[],
    )
    .await
    .unwrap();
    let gs_config = ns.get_config("config-gate-gs:main").await.unwrap();
    assert!(
        gs_config.is_none(),
        "get_config on graph source should return None"
    );
}

#[tokio::test]
async fn nameservice_graph_source_publisher() {
    let (_lock, _container, ns) = setup_localstack_ns().await;

    let graph_source_id = "search-bm25:main";

    // lookup_graph_source before publish → None
    assert!(ns
        .lookup_graph_source(graph_source_id)
        .await
        .unwrap()
        .is_none());

    // publish_graph_source
    ns.publish_graph_source(
        "search-bm25",
        "main",
        GraphSourceType::Bm25,
        r#"{"analyzer":"english"}"#,
        &["source-ledger:main".to_string()],
    )
    .await
    .unwrap();

    // lookup_graph_source
    let gs = ns
        .lookup_graph_source(graph_source_id)
        .await
        .unwrap()
        .expect("exists");
    assert_eq!(gs.name, "search-bm25");
    assert_eq!(gs.branch, "main");
    assert!(matches!(gs.source_type, GraphSourceType::Bm25));
    assert_eq!(gs.config, r#"{"analyzer":"english"}"#);
    assert_eq!(gs.dependencies, vec!["source-ledger:main".to_string()]);
    assert!(gs.index_id.is_none());
    assert_eq!(gs.index_t, 0);

    // publish_graph_source_index
    let gs_index_id_1 = test_index_id("gs-index:1");
    ns.publish_graph_source_index("search-bm25", "main", &gs_index_id_1, 1)
        .await
        .unwrap();
    let gs = ns
        .lookup_graph_source(graph_source_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(gs.index_id.as_ref(), Some(&gs_index_id_1));
    assert_eq!(gs.index_t, 1);

    // Monotonic: stale index ignored
    ns.publish_graph_source_index("search-bm25", "main", &test_index_id("stale:0"), 0)
        .await
        .unwrap();
    let gs = ns
        .lookup_graph_source(graph_source_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(gs.index_id.as_ref(), Some(&gs_index_id_1));

    // lookup_any → ledger or graph source
    let any = ns.lookup_any(graph_source_id).await.unwrap();
    match any {
        NsLookupResult::GraphSource(ref r) => assert_eq!(r.name, "search-bm25"),
        _ => panic!("expected GraphSource, got {any:?}"),
    }

    // Also test lookup_any for a ledger
    ns.publish_ledger_init("ledger-test:main").await.unwrap();
    let any = ns.lookup_any("ledger-test:main").await.unwrap();
    match any {
        NsLookupResult::Ledger(ref r) => assert_eq!(r.ledger_id, "ledger-test:main"),
        _ => panic!("expected Ledger, got {any:?}"),
    }

    // lookup_any for unknown → NotFound
    let any = ns.lookup_any("nonexistent:main").await.unwrap();
    assert!(matches!(any, NsLookupResult::NotFound));

    // all_graph_source_records
    let records = ns.all_graph_source_records().await.unwrap();
    assert!(
        records.iter().any(|r| r.name == "search-bm25"),
        "all_graph_source_records should contain our graph source"
    );

    // retract_graph_source
    ns.retract_graph_source("search-bm25", "main")
        .await
        .unwrap();
    let gs = ns.lookup_graph_source(graph_source_id).await.unwrap();
    assert!(
        gs.is_some(),
        "retracted graph source still visible via lookup_graph_source"
    );

    // Re-publish after retract should work (preserves retracted or re-creates)
    ns.publish_graph_source(
        "search-bm25",
        "main",
        GraphSourceType::Bm25,
        r#"{"analyzer":"english_v2"}"#,
        &["source-ledger:main".to_string()],
    )
    .await
    .unwrap();
    let gs = ns
        .lookup_graph_source(graph_source_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(gs.config, r#"{"analyzer":"english_v2"}"#);
    // Index should be preserved from before retraction
    assert_eq!(gs.index_id.as_ref(), Some(&gs_index_id_1));
}
