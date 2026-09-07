use super::*;
use fluree_db_core::commit::codec::read_commit;

fn body(id: &str, value: i64) -> Value {
    json!({"@context":{"ex":"http://example.org/wal/"},"@id":format!("ex:{id}"),"ex:value":value})
}
fn query() -> Value {
    json!({"@context":{"ex":"http://example.org/wal/"},"select":["?id","?value"],
        "where":{"@id":"?id","ex:value":"?value"},"orderBy":"?id"})
}
async fn initialized() -> (tempfile::TempDir, JournalLedger) {
    let dir = tempfile::tempdir().unwrap();
    let ledger =
        JournalLedger::initialize(dir.path().into(), "adapter:main".into(), "test-1".into())
            .await
            .unwrap();
    (dir, ledger)
}

#[tokio::test]
async fn real_staging_query_and_reopen_preserve_chain_raw_bytes_and_noops() {
    let (dir, ledger) = initialized().await;
    assert_eq!(ledger.query(&query()).await.unwrap(), json!([]));
    let one = body("one", 10);
    let first = ledger
        .transact(TxnType::Upsert, &one)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.commit.t, 1);
    assert_eq!(
        ledger.query(&query()).await.unwrap(),
        json!([["ex:one", 10]])
    );
    assert_eq!(
        ledger.content(&first.raw_txn_id).await.unwrap(),
        serde_json::to_vec(&one).unwrap()
    );
    let commit = read_commit(&ledger.content(&first.commit.commit_id).await.unwrap()).unwrap();
    assert!(commit.parents.is_empty());
    assert_eq!(commit.txn, Some(first.raw_txn_id));
    let size = std::fs::metadata(dir.path().join(".fluree-wal/journal"))
        .unwrap()
        .len();
    assert!(ledger
        .transact(TxnType::Upsert, &one)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        std::fs::metadata(dir.path().join(".fluree-wal/journal"))
            .unwrap()
            .len(),
        size
    );
    // A WHERE-based update reads its accepted base through the real staging path.
    let update = json!({"@context":{"ex":"http://example.org/wal/"},
        "where":{"@id":"ex:one","ex:value":"?old"},
        "delete":{"@id":"ex:one","ex:value":"?old"},
        "insert":{"@id":"ex:one","ex:value":11}});
    let second = ledger
        .transact(TxnType::Update, &update)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.commit.t, 2);
    assert_eq!(
        ledger.query(&query()).await.unwrap(),
        json!([["ex:one", 11]])
    );
    let second_bytes = ledger.content(&second.commit.commit_id).await.unwrap();
    let commit = read_commit(&second_bytes).unwrap();
    assert_eq!(commit.parents, vec![first.commit.commit_id.clone()]);
    assert_eq!(commit.txn, Some(second.raw_txn_id.clone()));
    drop(ledger);
    // Delete ALL materialized bytes: restart must reconstruct from the journal,
    // not from file caches or a fixture exported into ordinary storage.
    std::fs::remove_dir_all(dir.path().join(".fluree-wal/data")).unwrap();
    std::fs::create_dir(dir.path().join(".fluree-wal/data")).unwrap();
    let ledger = JournalLedger::open(dir.path().into()).await.unwrap();
    assert_eq!(
        ledger.query(&query()).await.unwrap(),
        json!([["ex:one", 11]])
    );
    assert_eq!(
        ledger.content(&second.commit.commit_id).await.unwrap(),
        second_bytes
    );
    assert_eq!(
        ledger.content(&second.raw_txn_id).await.unwrap(),
        serde_json::to_vec(&update).unwrap()
    );
    let third = ledger
        .transact(TxnType::Insert, &body("two", 20))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(third.commit.t, 3);
    assert_eq!(
        read_commit(&ledger.content(&third.commit.commit_id).await.unwrap())
            .unwrap()
            .parents,
        vec![second.commit.commit_id]
    );
    assert_eq!(
        ledger.query(&query()).await.unwrap(),
        json!([["ex:one", 11], ["ex:two", 20]])
    );
}

#[tokio::test]
async fn independently_opened_caches_refresh_and_cloned_writers_serialize() {
    let (dir, ledger) = initialized().await;
    let other = JournalLedger::open(dir.path().into()).await.unwrap();
    ledger
        .transact(TxnType::Insert, &body("first", 1))
        .await
        .unwrap();
    assert_eq!(
        other.query(&query()).await.unwrap(),
        json!([["ex:first", 1]])
    );
    let second = other
        .transact(TxnType::Insert, &body("second", 2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.commit.t, 2);
    assert_eq!(
        ledger
            .query(&query())
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let mut jobs = Vec::new();
    for i in 0..8 {
        let clone = ledger.clone();
        jobs.push(tokio::spawn(async move {
            clone
                .transact(TxnType::Insert, &body(&format!("concurrent-{i}"), i))
                .await
                .unwrap()
                .unwrap()
        }));
    }
    let mut times = Vec::new();
    for job in jobs {
        times.push(job.await.unwrap().commit.t);
    }
    times.sort();
    assert_eq!(times, (3..=10).collect::<Vec<_>>());
    assert_eq!(
        other
            .query(&query())
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        10
    );
}

#[tokio::test]
async fn failed_install_blocks_cached_reads_and_recovery_installs_unacknowledged_commit() {
    let (dir, ledger) = initialized().await;
    ledger
        .transact(TxnType::Insert, &body("ack", 1))
        .await
        .unwrap();
    let other = JournalLedger::open(dir.path().into()).await.unwrap();
    let error = ledger
        .transact_with_install_hook(TxnType::Insert, &body("lost-response", 2), || {
            Err(JournalError::Invalid("injected installation failure"))
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Journal(JournalError::AcceptanceUnresolved {
            durable: Some(_),
            ..
        })
    ));
    for handle in [&ledger, &other] {
        assert!(matches!(
            handle.query(&query()).await,
            Err(Error::Journal(JournalError::Poisoned))
        ));
        assert!(handle
            .transact(TxnType::Insert, &body("blocked", 3))
            .await
            .is_err());
    }
    ledger.recover().await.unwrap();
    assert_eq!(
        other.query(&query()).await.unwrap(),
        json!([["ex:ack", 1], ["ex:lost-response", 2]])
    );
    let next = ledger
        .transact(TxnType::Insert, &body("after", 3))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.commit.t, 3);
}

#[tokio::test]
async fn cancellation_during_durable_install_keeps_gate_until_state_is_installed() {
    let (_dir, ledger) = initialized().await;
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let writer = ledger.clone();
    let request = tokio::spawn(async move {
        writer
            .transact_with_install_hook(TxnType::Insert, &body("cancelled", 7), move || {
                entered_tx.send(()).unwrap();
                release_rx
                    .recv_timeout(std::time::Duration::from_secs(20))
                    .unwrap();
                Ok(())
            })
            .await
    });
    entered_rx.await.unwrap(); // journal flush has completed, before installation
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    assert!(ledger.0.cache.try_lock().is_err());
    release_tx.send(()).unwrap();
    assert_eq!(
        ledger.query(&query()).await.unwrap(),
        json!([["ex:cancelled", 7]])
    );
    let next = ledger
        .transact(TxnType::Insert, &body("next", 8))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.commit.t, 2);
}

#[tokio::test]
async fn unsupported_named_graph_writes_do_not_advance_the_journal() {
    let (dir, ledger) = initialized().await;
    let before = std::fs::read(dir.path().join(".fluree-wal/journal")).unwrap();
    let named = json!({"@context":{"ex":"http://example.org/wal/"},
        "insert":{"@id":"ex:g","@graph":[{"@id":"ex:one","ex:value":10}]}});
    assert!(ledger.transact(TxnType::Update, &named).await.is_err());
    assert_eq!(
        std::fs::read(dir.path().join(".fluree-wal/journal")).unwrap(),
        before
    );
    assert!(ledger.head().await.unwrap().is_none());
    assert!(ledger
        .transact(TxnType::Insert, &body("valid", 1))
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn recovery_rejects_semantically_unsupported_but_checksummed_journal() {
    use fluree_db_core::local_journal::{FileIo, Journal};
    let (dir, ledger) = initialized().await;
    ledger
        .transact(TxnType::Insert, &body("one", 1))
        .await
        .unwrap();
    drop(ledger);
    let (journal, records) =
        Journal::open(FileIo::open(&dir.path().join(".fluree-wal/journal")).unwrap()).unwrap();
    drop(journal);
    let target = tempfile::tempdir().unwrap();
    drop(LocalRoot::initialize(target.path(), "adapter:main", "test-1").unwrap());
    // Offline invalid-semantic fixture; framing/hash and root generation remain
    // valid. The core deliberately cannot interpret this database-specific head.
    let (mut journal, _) =
        Journal::open(FileIo::open(&target.path().join(".fluree-wal/journal")).unwrap()).unwrap();
    let mut transition = records[0].transition.clone();
    let mut head: Value = serde_json::from_slice(&transition.resulting_head).unwrap();
    head["f:configV"] = json!(9);
    transition.resulting_head = serde_json::to_vec(&head).unwrap();
    journal.append_and_sync(&transition).unwrap();
    drop(journal);
    assert!(matches!(
        JournalLedger::open(target.path().into()).await,
        Err(Error::Journal(JournalError::Invalid(_)))
    ));
    assert!(
        FlureeBuilder::file(target.path().to_string_lossy().to_string())
            .without_indexing()
            .build()
            .is_err()
    );
}

// Re-executed in a separate process. No action in the ordinary test harness.
#[test]
fn process_writer() {
    use std::io::Write;
    let Some(root) = std::env::var_os("FLUREE_WAL_ADAPTER_TEST_ROOT") else {
        return;
    };
    let controller = PathBuf::from(std::env::var_os("FLUREE_WAL_ADAPTER_TEST_CONTROLLER").unwrap());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let ledger = JournalLedger::initialize(root.into(), "adapter:main".into(), "process-test".into()).await.unwrap();
        let ack = ledger.transact(TxnType::Insert, &body("process-ack", 42)).await.unwrap().unwrap();
        let receipt = json!({"t":ack.commit.t,"commit":ack.commit.commit_id.to_string(),
            "raw":ack.raw_txn_id.to_string(),"bytes":ledger.content(&ack.commit.commit_id).await.unwrap()});
        let mut file = std::fs::File::create(controller.join("ack.json")).unwrap();
        file.write_all(&serde_json::to_vec(&receipt).unwrap()).unwrap();
        file.sync_all().unwrap();
        std::fs::File::open(&controller).unwrap().sync_all().unwrap();
        std::fs::write(controller.join("ready"), b"ready").unwrap();
        // Parent kills us while the live adapter still owns the root/cache.
        std::thread::sleep(std::time::Duration::from_secs(30));
        drop(ledger);
    });
}

#[tokio::test]
async fn process_death_reopens_acknowledged_real_transaction() {
    let root = tempfile::tempdir().unwrap();
    let controller = tempfile::tempdir().unwrap();
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut child = Child(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "local_journal_ledger::tests::process_writer",
                "--nocapture",
            ])
            .env("FLUREE_WAL_ADAPTER_TEST_ROOT", root.path())
            .env("FLUREE_WAL_ADAPTER_TEST_CONTROLLER", controller.path())
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        while !controller.path().join("ready").exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "writer exited before acknowledgment"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // A competing process must not acquire the live writer's root.
    assert!(JournalLedger::open(root.path().into()).await.is_err());
    child.0.kill().unwrap();
    assert!(!child.0.wait().unwrap().success());
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(controller.path().join("ack.json")).unwrap())
            .unwrap();
    let ledger = JournalLedger::open(root.path().into()).await.unwrap();
    let head = ledger.head().await.unwrap().unwrap();
    assert_eq!(head.t, oracle["t"].as_i64().unwrap());
    let id: ContentId = oracle["commit"].as_str().unwrap().parse().unwrap();
    assert_eq!(head.id, Some(id.clone()));
    assert_eq!(json!(ledger.content(&id).await.unwrap()), oracle["bytes"]);
    let raw: ContentId = oracle["raw"].as_str().unwrap().parse().unwrap();
    assert_eq!(
        ledger.content(&raw).await.unwrap(),
        serde_json::to_vec(&body("process-ack", 42)).unwrap()
    );
    assert_eq!(
        ledger.query(&query()).await.unwrap(),
        json!([["ex:process-ack", 42]])
    );
    // SIGKILL leaves kernel page cache intact; the separate erased-data/oracle
    // tests and core fault model exercise lost materialization and torn writes.
}

#[tokio::test]
async fn replay_preserves_generated_subjects_without_reexecuting_input() {
    let (dir, ledger) = initialized().await;
    let input = json!({"@context":{"ex":"http://example.org/wal/"},"ex:value":99});
    let first = ledger
        .transact(TxnType::Insert, &input)
        .await
        .unwrap()
        .unwrap();
    let second = ledger
        .transact(TxnType::Insert, &input)
        .await
        .unwrap()
        .unwrap();
    // Same raw body is shared, but two successful insert calls create distinct
    // subjects/commits. Replay must preserve both without generating a third ID.
    assert_eq!(first.raw_txn_id, second.raw_txn_id);
    assert_ne!(first.commit.commit_id, second.commit.commit_id);
    let rows = ledger.query(&query()).await.unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 2);
    assert_ne!(rows[0][0], rows[1][0]);
    drop(ledger);
    std::fs::remove_dir_all(dir.path().join(".fluree-wal/data")).unwrap();
    std::fs::create_dir(dir.path().join(".fluree-wal/data")).unwrap();
    let ledger = JournalLedger::open(dir.path().into()).await.unwrap();
    assert_eq!(ledger.query(&query()).await.unwrap(), rows);
    assert_eq!(
        ledger.head().await.unwrap().unwrap().id,
        Some(second.commit.commit_id)
    );
}

#[tokio::test]
async fn opaque_checkpoint_roots_cannot_open_as_empty_ledgers() {
    use fluree_db_core::local_journal::{CheckpointEntry, CheckpointSpec};
    let dir = tempfile::tempdir().unwrap();
    let baseline = CheckpointSpec {
        head: Object {
            key: "ns/head".into(),
            bytes: b"existing-baseline".to_vec(),
        },
        objects: vec![CheckpointEntry {
            key: "baseline/object".into(),
            length: 0,
            // SHA-256 of the empty byte string.
            sha256: [
                0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f,
                0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b,
                0x78, 0x52, 0xb8, 0x55,
            ],
        }],
    };
    let owner = LocalRoot::bootstrap(
        dir.path(),
        "adapter:main",
        "test-1",
        baseline,
        |_| Ok(std::io::empty()),
        |_| Ok(()),
    )
    .unwrap();
    assert!(matches!(
        JournalLedger::open(dir.path().into()).await,
        Err(Error::Journal(JournalError::Invalid(
            "missing source head provenance"
        )))
    ));
    assert!(matches!(owner.accepted_head(), Err(JournalError::Poisoned)));
    assert!(matches!(
        JournalLedger::open(dir.path().into()).await,
        Err(Error::Journal(JournalError::Poisoned))
    ));
    drop(owner);
    assert!(matches!(
        JournalLedger::open(dir.path().into()).await,
        Err(Error::Journal(JournalError::Invalid(
            "missing source head provenance"
        )))
    ));
}
