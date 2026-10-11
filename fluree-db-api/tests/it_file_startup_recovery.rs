//! Building a file-backed instance replays the WAL a crash left behind.

#![cfg(feature = "native")]

use fluree_db_api::FlureeBuilder;
use fluree_db_core::FileStorage;
use std::path::{Path, PathBuf};

async fn crash_with_a_logged_write(root: &Path) -> PathBuf {
    FileStorage::crash_with_a_logged_write_for_test(root, "fluree:file://a.bin", b"logged")
        .await
        .unwrap()
}

#[tokio::test]
async fn build_async_recovers_the_wal() {
    let dir = tempfile::tempdir().unwrap();
    let path = crash_with_a_logged_write(dir.path()).await;

    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build_async()
        .await
        .unwrap();

    assert_eq!(std::fs::read(&path).unwrap(), b"logged");
    fluree
        .create_ledger("startup/recovery:main")
        .await
        .expect("the recovered instance writes");
}

#[tokio::test]
async fn build_client_recovers_the_wal() {
    let dir = tempfile::tempdir().unwrap();
    let path = crash_with_a_logged_write(dir.path()).await;

    let _client = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build_client()
        .await
        .unwrap();

    assert_eq!(std::fs::read(&path).unwrap(), b"logged");
}
