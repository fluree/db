use super::*;
use tempfile::TempDir;

fn entry(term: u64, index: u64) -> LogEntry {
    LogEntry {
        log_id: LogId::new(term, index),
        payload: vec![index as u8],
    }
}

#[tokio::test]
async fn zero_based_openraft_log_survives_restart() {
    let dir = TempDir::new().unwrap();
    let store = FsRaftLogStore::open(dir.path()).await.unwrap();
    let entries = [entry(1, 0), entry(1, 1), entry(2, 2)];
    store.append(&entries).await.unwrap();
    store.save_committed(Some(LogId::new(2, 2))).await.unwrap();
    drop(store);
    let reopened = FsRaftLogStore::open(dir.path()).await.unwrap();
    assert_eq!(
        reopened.log_state().await.unwrap().last_log,
        Some(LogId::new(2, 2))
    );
    assert_eq!(reopened.read_range(0..10).await.unwrap(), entries);
}

#[tokio::test]
async fn reads_hide_purged_files_and_orphans_past_a_gap() {
    let dir = TempDir::new().unwrap();
    let store = FsRaftLogStore::open(dir.path()).await.unwrap();
    store
        .append(&(0..6).map(|i| entry(1, i)).collect::<Vec<_>>())
        .await
        .unwrap();
    // Crash image after the purge marker, before old-file deletion; later
    // incomplete append lost entry 4's rename but left entry 5 visible.
    atomic_write(
        &store.last_purged_path(),
        &postcard::to_allocvec(&LogId::new(1, 1)).unwrap(),
    )
    .await
    .unwrap();
    fs::remove_file(store.entry_path(4)).await.unwrap();
    assert_eq!(
        store.read_range(0..10).await.unwrap(),
        [entry(1, 2), entry(1, 3)]
    );
    assert!(store.read_range(5..10).await.unwrap().is_empty());
}

#[tokio::test]
async fn repaired_tail_cannot_resurrect_when_a_new_leader_fills_the_gap() {
    let dir = TempDir::new().unwrap();
    let store = FsRaftLogStore::open(dir.path()).await.unwrap();
    store
        .append(&(0..6).map(|i| entry(1, i)).collect::<Vec<_>>())
        .await
        .unwrap();
    store.save_committed(Some(LogId::new(1, 2))).await.unwrap();
    fs::remove_file(store.entry_path(3)).await.unwrap();
    drop(store);
    let reopened = FsRaftLogStore::open(dir.path()).await.unwrap();
    assert!(!reopened.entry_path(4).exists());
    assert!(!reopened.entry_path(5).exists());
    reopened.append(&[entry(2, 3)]).await.unwrap();
    drop(reopened);
    let again = FsRaftLogStore::open(dir.path()).await.unwrap();
    assert_eq!(
        again.log_state().await.unwrap().last_log,
        Some(LogId::new(2, 3))
    );
    assert_eq!(again.read_range(3..10).await.unwrap(), [entry(2, 3)]);
}

#[tokio::test]
async fn missing_committed_history_fails_before_any_repair() {
    for missing in [0, 2, 4] {
        let dir = TempDir::new().unwrap();
        let store = FsRaftLogStore::open(dir.path()).await.unwrap();
        store
            .append(&(0..6).map(|i| entry(1, i)).collect::<Vec<_>>())
            .await
            .unwrap();
        store.save_committed(Some(LogId::new(1, 4))).await.unwrap();
        fs::remove_file(store.entry_path(missing)).await.unwrap();
        drop(store);
        assert!(matches!(
            FsRaftLogStore::open(dir.path()).await,
            Err(StorageError::Corruption(_))
        ));
        assert!(dir.path().join("log").join(entry_filename(5)).exists());
    }
}

#[tokio::test]
async fn filename_and_committed_term_mismatches_are_rejected() {
    let dir = TempDir::new().unwrap();
    let store = FsRaftLogStore::open(dir.path()).await.unwrap();
    store.append(&[entry(1, 0), entry(1, 1)]).await.unwrap();
    atomic_write(
        &store.entry_path(1),
        &postcard::to_allocvec(&entry(1, 9)).unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(
        store.read_range(1..2).await,
        Err(StorageError::Corruption(_))
    ));
    store.append(&[entry(1, 1)]).await.unwrap();
    store.save_committed(Some(LogId::new(2, 1))).await.unwrap();
    drop(store);
    assert!(matches!(
        FsRaftLogStore::open(dir.path()).await,
        Err(StorageError::Corruption(_))
    ));
}

#[tokio::test]
async fn every_incomplete_tail_image_preserves_the_acknowledged_prefix() {
    for missing in 0u32..16 {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("new").join("nested").join("raft");
        let store = FsRaftLogStore::open(&path).await.unwrap();
        store
            .append(&(0..6).map(|i| entry(1, i)).collect::<Vec<_>>())
            .await
            .unwrap();
        store.save_committed(Some(LogId::new(1, 1))).await.unwrap();
        let mut first_gap = 6;
        for offset in 0..4 {
            if missing & (1 << offset) != 0 {
                let index = offset + 2;
                first_gap = first_gap.min(index);
                fs::remove_file(store.entry_path(index)).await.unwrap();
            }
        }
        drop(store);
        let reopened = FsRaftLogStore::open(&path).await.unwrap();
        assert_eq!(
            reopened.read_range(0..10).await.unwrap(),
            (0..first_gap).map(|i| entry(1, i)).collect::<Vec<_>>()
        );
        reopened.append(&[entry(2, first_gap)]).await.unwrap();
        drop(reopened);
        let again = FsRaftLogStore::open(&path).await.unwrap();
        assert_eq!(
            again.log_state().await.unwrap().last_log,
            Some(LogId::new(2, first_gap))
        );
        assert_eq!(
            again.read_range(0..2).await.unwrap(),
            [entry(1, 0), entry(1, 1)]
        );
    }
}

#[tokio::test]
async fn legacy_zero_and_one_based_roots_gain_a_durable_origin() {
    for start in [0, 1] {
        let dir = TempDir::new().unwrap();
        let store = FsRaftLogStore::open(dir.path()).await.unwrap();
        store
            .append(&(start..4).map(|i| entry(1, i)).collect::<Vec<_>>())
            .await
            .unwrap();
        fs::remove_file(dir.path().join("log_start")).await.unwrap();
        drop(store);
        let reopened = FsRaftLogStore::open(dir.path()).await.unwrap();
        assert_eq!(
            reopened.read_range(0..10).await.unwrap(),
            (start..4).map(|i| entry(1, i)).collect::<Vec<_>>()
        );
        let marker: u64 =
            postcard::from_bytes(&fs::read(dir.path().join("log_start")).await.unwrap()).unwrap();
        assert_eq!(marker, start);
    }
}
