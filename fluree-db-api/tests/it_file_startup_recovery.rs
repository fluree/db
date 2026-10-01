//! Building a file-backed instance replays the WAL a crash left behind.

#![cfg(feature = "native")]

use fluree_db_api::FlureeBuilder;
use fluree_db_core::{Durability, FileStorage, StorageWrite};
use std::path::{Path, PathBuf};

/// Leaves a root whose WAL holds a write the crash kept off disk.
/// Returns the path of the missing file.
async fn crash_with_a_logged_write(root: &Path) -> PathBuf {
    let storage = FileStorage::new(root).with_durability(Durability::Wal);
    storage.recover_wal().unwrap();
    storage.hold_wal_segments_for_test().unwrap();
    storage
        .write_bytes("fluree:file://ledger/a.bin", b"logged")
        .await
        .unwrap();
    storage.sync().await.unwrap();
    storage.simulate_crash_for_test();
    let path = root.join("ledger/a.bin");
    std::fs::remove_file(&path).unwrap();
    path
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
