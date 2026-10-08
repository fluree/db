//! Vended-credential integration test using testcontainers + LocalStack.
//!
//! Round-trips the full grant flow: mint STS credentials scoped to a
//! ledger's prefix, build an S3 reader from the grant, and read a CAS
//! object with it.
//!
//! Note: LocalStack community edition does not enforce IAM session
//! policies, so this validates minting and grant usability — the
//! prefix-scoping *policy shape* is covered by unit tests in
//! `fluree_db_api::vended_credentials`.
//!
//! Run (requires Docker):
//!   cargo test -p fluree-db-api --features aws,aws-testcontainers \
//!     --test it_vended_credentials_testcontainers -- --nocapture

#![cfg(all(feature = "aws", feature = "aws-testcontainers"))]

use aws_config::meta::region::RegionProviderChain;
use fluree_db_api::vended_credentials::{mint_scoped_credentials, S3VendScope};
use fluree_db_core::StorageRead;
use fluree_db_storage_aws::{S3Config, S3Storage};
use fs2::FileExt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{runners::AsyncRunner, GenericImage, ImageExt};

const LOCALSTACK_EDGE_PORT: u16 = 4566;
const REGION: &str = "us-east-1";

struct LocalstackTestLock {
    _file: std::fs::File,
}

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
            .expect("acquire LocalStack test lock");
        LocalstackTestLock { _file: lock_file }
    })
    .await
    .expect("lock task")
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
    let host_port = container
        .get_host_port_ipv4(LOCALSTACK_EDGE_PORT)
        .await
        .expect("LocalStack edge port");
    let endpoint = format!("http://127.0.0.1:{host_port}");
    (lock, container, endpoint)
}

#[tokio::test]
async fn vended_grant_mints_and_reads_ledger_objects() {
    let (_lock, _container, endpoint) = start_localstack("s3,sts,iam").await;
    let sdk_config = sdk_config_for_localstack(&endpoint).await;

    // Seed a bucket with one CAS object under the ledger's prefix.
    let bucket = "vend-test-bucket";
    let s3_admin = aws_sdk_s3::Client::new(&sdk_config);
    s3_admin
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("create bucket");
    let object_bytes = b"canonical commit bytes".to_vec();
    s3_admin
        .put_object()
        .bucket(bucket)
        .key("ledgers/inv/main/commit/test.fc")
        .body(object_bytes.clone().into())
        .send()
        .await
        .expect("seed object");

    // Mint a grant scoped to the ledger's name-level prefix.
    let sts = aws_sdk_sts::Client::new(&sdk_config);
    let scope = S3VendScope {
        bucket: bucket.to_string(),
        root_prefix: Some("ledgers".to_string()),
        region: Some(REGION.to_string()),
        endpoint: Some(endpoint.clone()),
        // LocalStack accepts virtual-hosted addressing; leaving this unset
        // keeps the SDK default, as before.
        force_path_style: None,
    };
    let grant = mint_scoped_credentials(
        &sts,
        "arn:aws:iam::000000000000:role/fluree-vend-test",
        &scope,
        REGION,
        "inv:main",
        900,
        Some("did:key:z6MkTest"),
    )
    .await
    .expect("mint grant");

    assert_eq!(grant.scoped_prefix, "ledgers/inv");
    assert_eq!(grant.bucket, bucket);
    assert!(!grant.session_token.is_empty(), "STS session token present");
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    assert!(
        grant.expires_at_epoch_secs > now_secs + 600,
        "grant expiry should be well in the future: {} vs {now_secs}",
        grant.expires_at_epoch_secs
    );

    // Build a consumer S3 reader from the grant (the same construction
    // `fluree-db-nameservice-sync::vended_s3` performs) and read the object
    // through the fluree address layer.
    let creds = aws_sdk_s3::config::Credentials::new(
        grant.access_key_id.clone(),
        grant.secret_access_key.clone(),
        Some(grant.session_token.clone()),
        None,
        "vended-test",
    );
    let consumer_sdk = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .credentials_provider(creds)
        .region(aws_config::Region::new(REGION))
        .endpoint_url(&endpoint)
        .load()
        .await;
    let consumer = S3Storage::new(
        &consumer_sdk,
        S3Config {
            bucket: grant.bucket.clone(),
            prefix: grant.key_prefix.clone(),
            endpoint: grant.endpoint.clone(),
            ..Default::default()
        },
    )
    .await
    .expect("consumer storage");

    let bytes = consumer
        .read_bytes("fluree:s3://inv/main/commit/test.fc")
        .await
        .expect("vended read");
    assert_eq!(bytes, object_bytes);
}
