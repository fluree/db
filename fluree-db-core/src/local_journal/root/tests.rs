use super::*;
use crate::local_journal::Transition;
use crate::{FileStorage, StorageRead, StorageWrite};

fn append_fixture(root: &Path) {
    let path = root.join(JOURNAL_DIR).join(JOURNAL);
    let (mut journal, _) = Journal::open(FileIo::open(&path).unwrap()).unwrap();
    for n in 1..=2 {
        journal
            .append_and_sync(&Transition {
                ledger: "test:main".into(),
                generation: "g1".into(),
                head_key: "ns/head".into(),
                expected_head: if n == 1 { None } else { Some(vec![n - 1]) },
                resulting_head: vec![n],
                objects: vec![Object {
                    key: format!("objects/{n}"),
                    bytes: vec![n; 23],
                }],
            })
            .unwrap();
    }
}

struct SyntheticValidator;
impl super::super::AcceptanceValidator for SyntheticValidator {
    fn validate(&self, _: &super::super::AcceptanceView<'_>) -> Result<()> {
        Ok(())
    }
}
fn candidate(n: u8) -> Transition {
    Transition {
        ledger: "test:main".into(),
        generation: "g1".into(),
        head_key: "ns/head".into(),
        expected_head: None,
        resulting_head: vec![n],
        objects: vec![Object {
            key: format!("objects/{n}"),
            bytes: vec![n],
        }],
    }
}

#[test]
fn concurrent_acceptance_has_one_cas_winner_and_no_loser_bytes() {
    let root = tempfile::tempdir().unwrap();
    let owner = LocalRoot::initialize(root.path(), "test:main", "g1").unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let joins: Vec<_> = (1..=2)
        .map(|n| {
            let owner = owner.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                owner.accept_with(&candidate(n), &SyntheticValidator, |_, _| Ok(()))
            })
        })
        .collect();
    let results: Vec<_> = joins.into_iter().map(|j| j.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(Error::Conflict)))
            .count(),
        1
    );
    let winner = owner.read_bytes("ns/head").unwrap()[0];
    assert!(owner
        .read_bytes(&format!("objects/{}", 3 - winner))
        .is_err());
    drop(owner);
    let (_, records) =
        Journal::open(FileIo::open(&root.path().join(JOURNAL_DIR).join(JOURNAL)).unwrap()).unwrap();
    assert_eq!(records.len(), 1);
}

#[test]
fn installation_panic_blocks_shared_reads_until_explicit_reconciliation() {
    let root = tempfile::tempdir().unwrap();
    let owner = LocalRoot::initialize(root.path(), "test:main", "g1").unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        owner.accept_with(&candidate(1), &SyntheticValidator, |_, _| {
            panic!("injected install panic")
        })
    }));
    assert!(panic.is_err());
    assert!(matches!(owner.read_bytes("ns/head"), Err(Error::Poisoned)));
    assert!(matches!(LocalRoot::open(root.path()), Err(Error::Poisoned)));
    assert!(owner
        .recover_with(|_| Err(Error::Invalid("reinstall failed")))
        .is_err());
    assert!(matches!(owner.read_bytes("ns/head"), Err(Error::Poisoned)));
    let journal_path = root.path().join(JOURNAL_DIR).join(JOURNAL);
    let bytes = std::fs::read(&journal_path).unwrap();
    let mut wrong_generation = bytes.clone();
    wrong_generation[8] ^= 1;
    std::fs::write(&journal_path, wrong_generation).unwrap();
    assert!(owner
        .recover_with(|_| panic!("wrong journal generation installed"))
        .is_err());
    std::fs::write(&journal_path, bytes).unwrap();
    owner
        .recover_with(|records| {
            assert_eq!(records.len(), 1);
            Ok(())
        })
        .unwrap();
    assert_eq!(owner.read_bytes("ns/head").unwrap(), vec![1]);
    // Re-transacting the stale candidate would be a conflict, not a duplicate.
    assert!(matches!(
        owner.accept_with(&candidate(1), &SyntheticValidator, |_, _| Ok(())),
        Err(Error::Conflict)
    ));
}

#[tokio::test]
async fn root_owner_recovers_before_reads_and_fences_every_ordinary_file_surface() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("db");
    std::fs::create_dir(&root).unwrap();
    let dormant = FileStorage::new(&root);
    drop(LocalRoot::initialize(&root, "test:main", "g1").unwrap());
    append_fixture(&root);
    let owner = LocalRoot::open(&root).unwrap();
    assert_eq!(owner.read_bytes("ns/head").unwrap(), vec![2]);
    assert_eq!(owner.read_bytes("objects/1").unwrap(), vec![1; 23]);
    assert_eq!(owner.read_bytes("objects/2").unwrap(), vec![2; 23]);
    let again = LocalRoot::open(&root).unwrap();
    assert!(Arc::ptr_eq(&owner, &again));
    assert!(dormant.read_bytes("ns/head").await.is_err());
    assert!(dormant.read_byte_range("ns/head", 0..1).await.is_err());
    assert!(dormant.exists("ns/head").await.is_err());
    assert!(dormant.list_prefix("").await.is_err());
    assert!(dormant.write_bytes("new", b"bad").await.is_err());
    assert!(dormant.delete("ns/head").await.is_err());
    assert!(dormant.resolve_local_path("ns/head").is_none());
    assert!(dormant.sweep_orphaned_staging().is_none());
    assert!(FileStorage::new(root.join("missing/child"))
        .write_bytes("bad", b"bad")
        .await
        .is_err());
    assert!(!root.join("missing").exists());
    // A wider ordinary storage root cannot address or discover the reserved data.
    let wider = FileStorage::new(parent.path());
    assert!(wider
        .read_bytes("fluree:file://db/.fluree-wal/data/ns/head")
        .await
        .is_err());
    assert!(wider.list_prefix("db").await.unwrap().is_empty());
    drop((owner, again));
    assert!(dormant.read_bytes("ns/head").await.is_err()); // marker survives owner
    let reopened = LocalRoot::open(&root).unwrap();
    assert_eq!(reopened.read_bytes("ns/head").unwrap(), vec![2]);
}

#[test]
fn active_ordinary_handles_block_initialization_until_the_last_clone_drops() {
    let root = tempfile::tempdir().unwrap();
    let storage = FileStorage::new(root.path());
    storage.ensure_ordinary_access().unwrap();
    let clone = storage.clone();
    drop(storage);
    assert!(LocalRoot::initialize(root.path(), "test:main", "g1").is_err());
    assert!(!root.path().join(JOURNAL_DIR).exists());
    drop(clone);
    let owner = LocalRoot::initialize(root.path(), "test:main", "g1").unwrap();
    assert_eq!(owner.ledger(), "test:main");
    assert_eq!(owner.generation(), "g1");
}

#[test]
fn aliases_share_one_owner_and_nonempty_migration_is_refused() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("db");
    let alias = parent.path().join("alias");
    std::fs::create_dir(&root).unwrap();
    std::os::unix::fs::symlink(&root, &alias).unwrap();
    let owner = LocalRoot::initialize(&root, "test:main", "g1").unwrap();
    assert!(Arc::ptr_eq(&owner, &LocalRoot::open(&alias).unwrap()));
    assert!(LocalRoot::initialize(&root, "test:main", "g1").is_err());
    let populated = tempfile::tempdir().unwrap();
    std::fs::write(populated.path().join("existing"), b"data").unwrap();
    assert!(LocalRoot::initialize(populated.path(), "test:main", "g1").is_err());
    assert!(!populated.path().join(JOURNAL_DIR).exists());
}

#[test]
fn failed_initialization_and_replay_never_expose_a_ready_owner() {
    let root = tempfile::tempdir().unwrap();
    let control = root.path().join(JOURNAL_DIR);
    std::fs::create_dir(&control).unwrap(); // crash just after fence creation
    assert!(LocalRoot::open(root.path()).is_err());
    assert!(FileStorage::new(root.path())
        .ensure_ordinary_access()
        .is_err());
    assert!(LocalRoot::initialize(root.path(), "test:main", "g1").is_err());

    let root = tempfile::tempdir().unwrap();
    drop(LocalRoot::initialize(root.path(), "test:main", "g1").unwrap());
    append_fixture(root.path());
    let data = root.path().join(JOURNAL_DIR).join(DATA);
    std::fs::create_dir(data.join("objects")).unwrap();
    std::fs::write(data.join("objects/2"), b"corrupt").unwrap();
    assert!(LocalRoot::open(root.path()).is_err());
    assert!(!data.join("ns/head").exists());
    // First object was materialized before the failure. Restore the missing-file
    // crash shape and retry: exact replay completes, without re-executing a txn.
    std::fs::remove_file(data.join("objects/2")).unwrap();
    let owner = LocalRoot::open(root.path()).unwrap();
    assert_eq!(owner.read_bytes("ns/head").unwrap(), vec![2]);
    drop(owner);
    let path = root.path().join(JOURNAL_DIR).join(JOURNAL);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[8] ^= 1;
    std::fs::write(path, bytes).unwrap();
    assert!(LocalRoot::open(root.path()).is_err());
}

#[test]
fn replay_rejects_symlink_parents_without_writing_outside_the_root() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    drop(LocalRoot::initialize(root.path(), "test:main", "g1").unwrap());
    append_fixture(root.path());
    std::os::unix::fs::symlink(
        outside.path(),
        root.path().join(JOURNAL_DIR).join(DATA).join("objects"),
    )
    .unwrap();
    assert!(LocalRoot::open(root.path()).is_err());
    assert!(std::fs::read_dir(outside.path()).unwrap().next().is_none());
}

// Run by the parent tests in a separate test process; no global environment edits.
#[test]
fn child_root_access() {
    let Ok(root) = std::env::var("FLUREE_ROOT_TEST_PATH") else {
        return;
    };
    match std::env::var("FLUREE_ROOT_TEST_MODE").unwrap().as_str() {
        "blocked" => {
            assert!(LocalRoot::open(Path::new(&root)).is_err());
            assert!(FileStorage::new(root).ensure_ordinary_access().is_err());
        }
        "hold" => {
            let _owner = LocalRoot::open(Path::new(&root)).unwrap();
            std::fs::write(std::env::var("FLUREE_ROOT_TEST_READY").unwrap(), b"ready").unwrap();
            loop {
                std::thread::park();
            }
        }
        _ => panic!("unexpected child mode"),
    }
}

#[test]
fn cross_process_exclusion_and_owner_death_release_the_root() {
    use std::process::{Command, Stdio};
    let root = tempfile::tempdir().unwrap();
    let controller = tempfile::tempdir().unwrap();
    let owner = LocalRoot::initialize(root.path(), "test:main", "g1").unwrap();
    let command = || {
        let mut c = Command::new(std::env::current_exe().unwrap());
        c.args([
            "--exact",
            "local_journal::root::tests::child_root_access",
            "--nocapture",
        ])
        .env("FLUREE_ROOT_TEST_PATH", root.path())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
        c
    };
    assert!(command()
        .env("FLUREE_ROOT_TEST_MODE", "blocked")
        .status()
        .unwrap()
        .success());
    drop(owner);
    let ready = controller.path().join("ready");
    let mut child = command()
        .env("FLUREE_ROOT_TEST_MODE", "hold")
        .env("FLUREE_ROOT_TEST_READY", &ready)
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !ready.exists() && std::time::Instant::now() < deadline {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let started = ready.exists();
    let blocked = LocalRoot::open(root.path()).is_err();
    let _ = child.kill();
    child.wait().unwrap();
    assert!(started, "child did not acquire root");
    assert!(blocked, "second process acquired the live owner's root");
    assert!(LocalRoot::open(root.path()).is_ok());
}
