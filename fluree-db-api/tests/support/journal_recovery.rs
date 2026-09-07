//! Real journal I/O + external database ACK oracle. Ordinary transactions supply
//! exact test payloads only: their file snapshots are NOT assumed durable in our
//! crash image. Reconstructed databases start empty and get all bytes via replay.
use super::*;
use fluree_db_core::local_journal::{
    replay_chain, Error, FileIo, Journal, JournalIo, Object, Record, ReplayTarget, Transition,
};
use std::cell::RefCell;
use std::io;
use std::rc::Rc;

#[derive(Default)]
struct Tape {
    barriers: Vec<Vec<u8>>,
    writes: Vec<(u64, Vec<u8>)>,
    fail_next_flush: bool,
}

struct TracedFile {
    file: FileIo,
    path: PathBuf,
    tape: Rc<RefCell<Tape>>,
}

impl JournalIo for TracedFile {
    fn len(&mut self) -> io::Result<u64> {
        self.file.len()
    }
    fn read_at(&mut self, offset: u64, out: &mut [u8]) -> io::Result<usize> {
        self.file.read_at(offset, out)
    }
    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<usize> {
        // Force short writes through the same production retry loop.
        let n = self.file.write_at(offset, &bytes[..bytes.len().min(113)])?;
        self.tape
            .borrow_mut()
            .writes
            .push((offset, bytes[..n].to_vec()));
        Ok(n)
    }
    fn sync_all(&mut self) -> io::Result<()> {
        if std::mem::take(&mut self.tape.borrow_mut().fail_next_flush) {
            return Err(io::ErrorKind::Other.into());
        }
        self.file.sync_all()?;
        self.tape
            .borrow_mut()
            .barriers
            .push(std::fs::read(&self.path)?);
        Ok(())
    }
}

/// Only used in private fresh temp directories; this is not the future production
/// path/symlink/ownership/checkpoint implementation. Atomic head rename is last.
struct Target<'a> {
    root: &'a Path,
    remaining_operations: Option<usize>,
}
impl Target<'_> {
    fn cut(&mut self) -> fluree_db_core::local_journal::Result<()> {
        if let Some(remaining) = &mut self.remaining_operations {
            if *remaining == 0 {
                return Err(Error::Io(io::ErrorKind::Other.into()));
            }
            *remaining -= 1;
        }
        Ok(())
    }
}
impl ReplayTarget for Target<'_> {
    fn read_head(&mut self, key: &str) -> fluree_db_core::local_journal::Result<Option<Vec<u8>>> {
        match std::fs::read(self.root.join(key)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    fn put_immutable(&mut self, object: &Object) -> fluree_db_core::local_journal::Result<()> {
        self.cut()?;
        let path = self.root.join(&object.key);
        match std::fs::read(&path) {
            Ok(bytes) if bytes == object.bytes => return Ok(()),
            Ok(_) => return Err(Error::Invalid("existing immutable object differs")),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        std::fs::create_dir_all(path.parent().unwrap())?;
        let staging = path.with_extension("journal-test-tmp");
        std::fs::write(&staging, &object.bytes)?;
        std::fs::rename(staging, path)?;
        Ok(())
    }
    fn publish_head(
        &mut self,
        key: &str,
        expected: Option<&[u8]>,
        bytes: &[u8],
    ) -> fluree_db_core::local_journal::Result<()> {
        self.cut()?;
        if self.read_head(key)?.as_deref() != expected {
            return Err(Error::Invalid("unexpected head during test replay"));
        }
        let path = self.root.join(key);
        std::fs::create_dir_all(path.parent().unwrap())?;
        let staging = path.with_extension("journal-test-tmp");
        std::fs::write(&staging, bytes)?;
        std::fs::rename(staging, path)?;
        Ok(())
    }
}

fn records_from_image(image: &[u8]) -> fluree_db_core::local_journal::Result<Vec<Record>> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("journal");
    std::fs::write(&path, image).unwrap();
    Journal::open(FileIo::open(&path)?).map(|(_, records)| records)
}

#[tokio::test]
async fn root_startup_replays_real_commits_and_refuses_ordinary_api_and_nameservice_access() {
    use fluree_db_core::local_journal::LocalRoot;
    use fluree_db_nameservice::{BranchLifecycle, NameServiceLookup, StatusLookup};
    let fixture = fixture().await;
    let root = tempfile::tempdir().unwrap();
    drop(LocalRoot::initialize(root.path(), LEDGER, "fixture-generation-1").unwrap());
    // Offline fixture injection, not a supported transaction API. The owner has
    // been dropped; all replay data is placed only in the durable journal.
    let (mut journal, _) =
        Journal::open(FileIo::open(&root.path().join(".fluree-wal/journal")).unwrap()).unwrap();
    let first = transition(None, &fixture.first_frontier);
    let second = transition(Some(&fixture.first_frontier), &fixture.frontier);
    journal.append_and_sync(&first).unwrap();
    journal.append_and_sync(&second).unwrap();
    drop(journal);
    let owner = LocalRoot::open(root.path()).unwrap();
    assert!(
        FlureeBuilder::file(root.path().to_string_lossy().to_string())
            .without_indexing()
            .build()
            .is_err()
    );
    let ns = fluree_db_nameservice::file::FileNameService::new(root.path());
    assert!(ns.lookup(LEDGER).await.is_err());
    assert!(ns.all_records().await.is_err());
    assert!(ns.get_status(LEDGER).await.is_err());
    assert!(ns.prune_commit_index(LEDGER, 1).await.is_err());
    assert!(ns.drop_branch(LEDGER).await.is_err());

    // No normal Fluree/WAL adapter is exposed yet. Export ONLY the recovered
    // owner's bytes into a fresh ordinary test image to exercise the independent
    // full query oracle. Never bypass the managed root's fence to query it.
    let image = tempfile::tempdir().unwrap();
    for key in fixture.frontier.0.keys() {
        let bytes = owner.read_bytes(key.to_str().unwrap()).unwrap();
        let path = image.path().join(key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }
    fixture.oracle.check(image.path()).await.unwrap();
    drop(owner);
    let reopened = LocalRoot::open(root.path()).unwrap();
    assert_eq!(
        reopened.read_bytes(HEAD_PATH).unwrap(),
        second.resulting_head
    );
}

fn transition(before: Option<&DeclaredFrontier>, after: &DeclaredFrontier) -> Transition {
    Transition {
        ledger: LEDGER.into(),
        generation: "fixture-generation-1".into(),
        head_key: HEAD_PATH.into(),
        expected_head: before.map(|s| s.0[Path::new(HEAD_PATH)].clone()),
        resulting_head: after.0[Path::new(HEAD_PATH)].clone(),
        objects: after
            .0
            .iter()
            .filter(|(key, bytes)| {
                key.as_path() != Path::new(HEAD_PATH)
                    && before.and_then(|s| s.0.get(*key)) != Some(*bytes)
            })
            .map(|(key, bytes)| Object {
                key: key.to_str().unwrap().into(),
                bytes: bytes.clone(),
            })
            .collect(),
    }
}

#[tokio::test]
async fn accepted_commits_validate_closure_before_flush_and_reconcile_failed_installation() {
    use fluree_db_api::local_journal_acceptance::LinearCommitValidator;
    use fluree_db_core::local_journal::LocalRoot;
    let fixture = fixture().await;
    let root = tempfile::tempdir().unwrap();
    let owner = LocalRoot::initialize(root.path(), LEDGER, "fixture-generation-1").unwrap();
    let first = transition(None, &fixture.first_frontier);
    let mut second = transition(Some(&fixture.first_frontier), &fixture.frontier);
    // The original fixture intentionally retains a CAS-loser object. This scope
    // permits only required dependencies, so remove it from the valid candidate.
    let loser_key = content_path(ContentKind::Commit, LEDGER, &fixture.loser_id.digest_hex());
    second.objects.retain(|object| object.key != loser_key);
    let journal_path = root.path().join(".fluree-wal/journal");
    let data = root.path().join(".fluree-wal/data");
    owner
        .accept_with(&first, &LinearCommitValidator, |_, receipt| {
            assert_eq!(std::fs::metadata(&journal_path).unwrap().len(), receipt.end);
            assert_eq!(
                std::fs::read(data.join(HEAD_PATH)).unwrap(),
                first.resulting_head
            );
            Ok(())
        })
        .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&first.resulting_head)));
    let before = std::fs::read(&journal_path).unwrap();
    let raw_key = content_path(
        ContentKind::Txn,
        LEDGER,
        &fixture.oracle.commits[1].raw_id.digest_hex(),
    );
    let mut missing = second.clone();
    missing.objects.retain(|object| object.key != raw_key);
    // Even an identical readable orphan is not an accepted durable prerequisite.
    let orphan_raw = data.join(&raw_key);
    std::fs::create_dir_all(orphan_raw.parent().unwrap()).unwrap();
    std::fs::write(&orphan_raw, &fixture.oracle.commits[1].raw_bytes).unwrap();
    assert!(owner
        .accept_with(&missing, &LinearCommitValidator, |_, _| panic!(
            "missing raw accepted"
        ))
        .is_err());
    let mut corrupt = second.clone();
    let commit_key = content_path(
        ContentKind::Commit,
        LEDGER,
        &fixture.oracle.commits[1].commit_id.digest_hex(),
    );
    corrupt
        .objects
        .iter_mut()
        .find(|o| o.key == commit_key)
        .unwrap()
        .bytes[10] ^= 1;
    assert!(owner
        .accept_with(&corrupt, &LinearCommitValidator, |_, _| panic!(
            "bad CID accepted"
        ))
        .is_err());
    let mut unsupported = second.clone();
    let mut head: Value = serde_json::from_slice(&unsupported.resulting_head).unwrap();
    head["f:status"] = json!("retracted");
    unsupported.resulting_head = serde_json::to_vec(&head).unwrap();
    assert!(owner
        .accept_with(&unsupported, &LinearCommitValidator, |_, _| panic!(
            "lifecycle accepted"
        ))
        .is_err());
    let unfiltered = transition(Some(&fixture.first_frontier), &fixture.frontier);
    assert!(owner
        .accept_with(&unfiltered, &LinearCommitValidator, |_, _| panic!(
            "CAS loser object accepted"
        ))
        .is_err());
    let mut duplicate = second.clone();
    duplicate.resulting_head = [b"{\"f:t\":123,".as_slice(), &second.resulting_head[1..]].concat();
    assert!(owner
        .accept_with(&duplicate, &LinearCommitValidator, |_, _| panic!(
            "duplicate field accepted"
        ))
        .is_err());
    assert_eq!(std::fs::read(&journal_path).unwrap(), before);
    assert_eq!(owner.read_bytes(HEAD_PATH).unwrap(), first.resulting_head);
    owner
        .accept_with(&second, &LinearCommitValidator, |_, _| Ok(()))
        .unwrap();
    // Persist independent expected outcomes only after successful installation.
    let controller = tempfile::tempdir().unwrap();
    fixture.oracle.persist(&controller.path().join("acks.json"));
    let oracle: AckOracle =
        serde_json::from_slice(&std::fs::read(controller.path().join("acks.json")).unwrap())
            .unwrap();
    let image = tempfile::tempdir().unwrap();
    for key in fixture
        .frontier
        .0
        .keys()
        .filter(|key| key.to_str().unwrap() != loser_key)
    {
        let bytes = owner.read_bytes(key.to_str().unwrap()).unwrap();
        let path = image.path().join(key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }
    oracle.check(image.path()).await.unwrap();

    let mut third_head: Value = serde_json::from_slice(&second.resulting_head).unwrap();
    third_head["f:t"] = json!(3);
    third_head["f:commitCid"] = json!(fixture.orphan_id.to_string());
    let third = Transition {
        ledger: LEDGER.into(),
        generation: "fixture-generation-1".into(),
        head_key: HEAD_PATH.into(),
        expected_head: Some(second.resulting_head.clone()),
        resulting_head: serde_json::to_vec(&third_head).unwrap(),
        objects: vec![Object {
            key: content_path(ContentKind::Commit, LEDGER, &fixture.orphan_id.digest_hex()),
            bytes: fixture.orphan_bytes.clone(),
        }],
    };
    assert!(matches!(
        owner.accept_with(&third, &LinearCommitValidator, |_, _| Err(Error::Invalid(
            "injected state installation failure"
        ))),
        Err(Error::AcceptanceUnresolved {
            durable: Some(_),
            ..
        })
    ));
    assert!(matches!(owner.read_bytes(HEAD_PATH), Err(Error::Poisoned)));
    owner
        .recover_with(|records| {
            assert_eq!(records.len(), 3);
            Ok(())
        })
        .unwrap();
    assert_eq!(owner.read_bytes(HEAD_PATH).unwrap(), third.resulting_head);
    for ack in &oracle.commits {
        assert_eq!(
            owner
                .read_bytes(&content_path(
                    ContentKind::Commit,
                    LEDGER,
                    &ack.commit_id.digest_hex()
                ))
                .unwrap(),
            ack.commit_bytes
        );
        assert_eq!(
            owner
                .read_bytes(&content_path(
                    ContentKind::Txn,
                    LEDGER,
                    &ack.raw_id.digest_hex()
                ))
                .unwrap(),
            ack.raw_bytes
        );
    }
    assert!(matches!(
        owner.accept_with(&third, &LinearCommitValidator, |_, _| Ok(())),
        Err(Error::Conflict)
    ));
}

#[tokio::test]
async fn journal_barriers_recover_external_acknowledgments_and_interrupted_replay() {
    let fixture = fixture().await;
    let controller = tempfile::tempdir().unwrap();
    let journal_dir = tempfile::tempdir().unwrap();
    let path = journal_dir.path().join("journal");
    let tape = Rc::new(RefCell::new(Tape::default()));
    let file = TracedFile {
        file: FileIo::create_new(&path).unwrap(),
        path: path.clone(),
        tape: tape.clone(),
    };
    let mut journal = Journal::create(file, [42; 16]).unwrap();
    let first = transition(None, &fixture.first_frontier);
    let second = transition(Some(&fixture.first_frontier), &fixture.frontier);
    let ack1 = journal.append_and_sync(&first).unwrap();
    let ack2 = journal.append_and_sync(&second).unwrap();
    // Record the test's expected acknowledged outcomes outside the fault image
    // only after both actual journal barriers returned successfully.
    fixture.oracle.persist(&controller.path().join("acks.json"));
    assert_eq!(tape.borrow().barriers.len(), 3);
    let durable = tape.borrow().barriers.last().unwrap().clone();
    assert_eq!(durable.len() as u64, ack2.end);
    let records = records_from_image(&durable).unwrap();
    assert_eq!(records[0].receipt, ack1);
    assert_eq!(records[1].receipt, ack2);
    let oracle: AckOracle =
        serde_json::from_slice(&std::fs::read(controller.path().join("acks.json")).unwrap())
            .unwrap();

    // Cut before each materialization operation, then replay to completion twice.
    let operations = records
        .iter()
        .map(|r| r.transition.objects.len())
        .sum::<usize>()
        + 1;
    for cut in 0..=operations {
        let root = tempfile::tempdir().unwrap();
        let mut target = Target {
            root: root.path(),
            remaining_operations: Some(cut),
        };
        let result = replay_chain(&records, LEDGER, "fixture-generation-1", &mut target);
        assert_eq!(result.is_ok(), cut == operations);
        target.remaining_operations = None;
        replay_chain(&records, LEDGER, "fixture-generation-1", &mut target).unwrap();
        oracle.check(root.path()).await.unwrap();
        let before = DeclaredFrontier::capture(root.path());
        replay_chain(&records, LEDGER, "fixture-generation-1", &mut target).unwrap();
        oracle.check(root.path()).await.unwrap();
        assert_eq!(before.0, DeclaredFrontier::capture(root.path()).0);
    }

    // A later append fails to flush. Synthesize loss/tears from that actual write
    // stream, retaining the last completed barrier. Partial tails must fail closed.
    let mut pending = second.clone();
    pending.expected_head = Some(second.resulting_head.clone());
    pending.resulting_head = b"unacknowledged-test-head".to_vec();
    pending.objects.clear();
    tape.borrow_mut().fail_next_flush = true;
    assert!(journal.append_and_sync(&pending).is_err());
    assert!(matches!(
        journal.append_and_sync(&pending),
        Err(Error::Poisoned)
    ));
    let volatile = std::fs::read(&path).unwrap();
    assert_eq!(records_from_image(&durable).unwrap(), records);
    for cut in [
        durable.len() + 1,
        durable.len().midpoint(volatile.len()),
        volatile.len() - 1,
    ] {
        assert!(records_from_image(&volatile[..cut]).is_err());
    }
    // Complete uncertain outcomes are retained, not silently treated as absent.
    assert_eq!(records_from_image(&volatile).unwrap().len(), 3);

    // The external oracle catches a missing required raw object even if a caller
    // incorrectly supplies an otherwise valid journal bundle. Closure validation
    // remains mandatory at the future transaction acceptance boundary.
    let mut incomplete = records.clone();
    let raw = &oracle.commits.last().unwrap().raw_id;
    let raw_key = content_path(ContentKind::Txn, LEDGER, &raw.digest_hex());
    let mut removed = 0;
    for r in &mut incomplete {
        r.transition.objects.retain(|o| {
            if o.key == raw_key {
                removed += 1;
                false
            } else {
                true
            }
        });
    }
    assert_eq!(removed, 1);
    let root = tempfile::tempdir().unwrap();
    replay_chain(
        &incomplete,
        LEDGER,
        "fixture-generation-1",
        &mut Target {
            root: root.path(),
            remaining_operations: None,
        },
    )
    .unwrap();
    assert!(oracle.check(root.path()).await.is_err());

    // Checksums alone cannot detect loss of a whole valid suffix. Such loss
    // violates the successful-sync contract and is caught by the external ACK
    // oracle, not inferred from a still-valid surviving journal prefix.
    let truncated = records_from_image(&tape.borrow().barriers[1]).unwrap();
    let root = tempfile::tempdir().unwrap();
    replay_chain(
        &truncated,
        LEDGER,
        "fixture-generation-1",
        &mut Target {
            root: root.path(),
            remaining_operations: None,
        },
    )
    .unwrap();
    assert!(oracle.check(root.path()).await.is_err());

    // Resume from an intermediate published head, then restore a missing object
    // even when the final head was already installed.
    let root = tempfile::tempdir().unwrap();
    let mut target = Target {
        root: root.path(),
        remaining_operations: None,
    };
    replay_chain(&records[..1], LEDGER, "fixture-generation-1", &mut target).unwrap();
    replay_chain(&records, LEDGER, "fixture-generation-1", &mut target).unwrap();
    oracle.check(root.path()).await.unwrap();
    std::fs::remove_file(root.path().join(&raw_key)).unwrap();
    replay_chain(&records, LEDGER, "fixture-generation-1", &mut target).unwrap();
    oracle.check(root.path()).await.unwrap();
    std::fs::write(root.path().join(&raw_key), b"corrupted required bytes").unwrap();
    assert!(replay_chain(&records, LEDGER, "fixture-generation-1", &mut target).is_err());

    // Reject wrong generation or unrelated head before creating any objects.
    let root = tempfile::tempdir().unwrap();
    let mut target = Target {
        root: root.path(),
        remaining_operations: None,
    };
    assert!(replay_chain(&records, LEDGER, "different-generation", &mut target).is_err());
    assert!(DeclaredFrontier::capture(root.path()).0.is_empty());
    let mut broken_chain = records.clone();
    broken_chain[1].transition.expected_head = Some(b"CAS loser".to_vec());
    assert!(replay_chain(&broken_chain, LEDGER, "fixture-generation-1", &mut target).is_err());
    assert!(DeclaredFrontier::capture(root.path()).0.is_empty());
    target
        .publish_head(HEAD_PATH, None, b"stale-or-foreign-head")
        .unwrap();
    let before = DeclaredFrontier::capture(root.path());
    assert!(replay_chain(&records, LEDGER, "fixture-generation-1", &mut target).is_err());
    assert_eq!(before.0, DeclaredFrontier::capture(root.path()).0);
}
