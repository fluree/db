//! Live GCS read verification for the HTTP/1.1-pinned S3 SDK path.
//!
//! GCS-backed Iceberg tables are read through `S3IcebergStorage` pointed at the
//! Cloud Storage S3-interoperability endpoint, with the SDK transport pinned to
//! HTTP/1.1 (see `io::storage::http1_only_http_client`). These tests exercise
//! the two failure modes that motivated the change:
//!
//!   1. Range GETs used to fail over HTTP/2 (the smithy-rs partial-response
//!      bug); they must now succeed over HTTP/1.1.
//!   2. Partitioned paths (`field=value/`) used to 403 under a bespoke SigV4
//!      signer that double-encoded `=` to `%3D`; the AWS SDK signs the path
//!      consistently, so a missing partition key returns 404 (NoSuchKey), never
//!      403 (SignatureDoesNotMatch).
//!
//! They hit a real bucket and are `#[ignore]`d so normal `cargo test` / CI runs
//! skip them. Run explicitly with HMAC interop creds + probe URIs in the env:
//!
//! ```bash
//! AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... FLUREE_GCS_REGION=europe-west1 \
//! FLUREE_GCS_DATA_URI='gs://bucket/iceberg/t/data/part-0.parquet' \
//! FLUREE_GCS_MISSING_PARTITION_URI='gs://bucket/iceberg/t/data/event_date=2024-01-01/missing.bin' \
//!   cargo test -p fluree-db-iceberg --features aws --test it_gcs_sdk_reads -- --ignored --nocapture
//! ```

#![cfg(feature = "aws")]

use fluree_db_iceberg::io::{S3IcebergStorage, SendIcebergStorage};

const ENDPOINT: &str = "https://storage.googleapis.com";

async fn gcs_storage() -> S3IcebergStorage {
    let region = std::env::var("FLUREE_GCS_REGION").unwrap_or_else(|_| "europe-west1".to_string());
    S3IcebergStorage::from_default_chain(Some(&region), Some(ENDPOINT), true)
        .await
        .expect("build S3IcebergStorage for the GCS S3-interop endpoint")
}

/// A range GET against a real GCS-backed Parquet data file must succeed over
/// HTTP/1.1 — the exact operation the AWS-SDK HTTP/2 range bug used to break.
#[tokio::test]
#[ignore = "requires live GCS creds + FLUREE_GCS_DATA_URI"]
async fn gcs_range_read_succeeds_over_http1() {
    let uri =
        std::env::var("FLUREE_GCS_DATA_URI").expect("set FLUREE_GCS_DATA_URI to a gs:// data file");
    let storage = gcs_storage().await;

    let size = storage.file_size(&uri).await.expect("HEAD (file_size)");
    assert!(
        size > 8,
        "data file should be non-trivial, got {size} bytes"
    );

    let head = storage
        .read_range(&uri, 0..16)
        .await
        .expect("range GET must succeed over HTTP/1.1");
    assert_eq!(head.len(), 16, "requested exactly 16 bytes");
    assert_eq!(
        &head[0..4],
        b"PAR1",
        "expected the Parquet magic at offset 0"
    );

    // A second, larger range confirms partial reads keep working past the first
    // network round-trip (the h2 bug surfaced on partial-content streaming).
    let tail = storage
        .read_range(&uri, (size - 4)..size)
        .await
        .expect("trailing range GET must succeed");
    assert_eq!(&tail[..], b"PAR1", "expected the Parquet magic at EOF");
}

/// A range GET on a *missing* `field=value/` key must fail as NoSuchKey (404),
/// not SignatureDoesNotMatch (403): the SDK signs the `=` in the path exactly as
/// it is sent on the wire, so authentication succeeds and only the object lookup
/// fails. The old bespoke signer 403'd here on every partitioned table.
#[tokio::test]
#[ignore = "requires live GCS creds + FLUREE_GCS_MISSING_PARTITION_URI"]
async fn gcs_partition_path_signs_correctly() {
    let uri = std::env::var("FLUREE_GCS_MISSING_PARTITION_URI")
        .expect("set FLUREE_GCS_MISSING_PARTITION_URI to a missing gs://.../field=value/ key");
    assert!(
        uri.contains('='),
        "the probe URI must contain a partition '=' segment, got: {uri}"
    );

    let storage = gcs_storage().await;
    let err = storage
        .read_range(&uri, 0..16)
        .await
        .expect_err("a missing key must return an error");
    let msg = err.to_string();
    eprintln!("missing-partition-key error: {msg}");

    assert!(
        !msg.contains("SignatureDoesNotMatch") && !msg.contains("403"),
        "partition '=' path produced a signature mismatch (403); signing is wrong: {msg}"
    );
    assert!(
        msg.contains("NoSuchKey")
            || msg.contains("404")
            || msg.to_lowercase().contains("does not exist")
            || msg.to_lowercase().contains("no such key"),
        "expected a 404/NoSuchKey for the missing partition key, got: {msg}"
    );
}
