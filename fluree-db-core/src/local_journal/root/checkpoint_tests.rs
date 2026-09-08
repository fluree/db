use super::*;
use std::io::Cursor;

fn spec() -> CheckpointSpec {
    CheckpointSpec {
        head: Object {
            key: "ns/head".into(),
            bytes: b"baseline-7".to_vec(),
        },
        objects: [
            "commits/base",
            "index/dictionaries/subjects",
            "index/leaves/1",
        ]
        .into_iter()
        .map(|key| CheckpointEntry {
            key: key.into(),
            length: key.len() as u64,
            sha256: super::super::digest(key.as_bytes()),
        })
        .collect(),
    }
}
fn source(entry: &CheckpointEntry) -> Result<Cursor<Vec<u8>>> {
    Ok(Cursor::new(entry.key.as_bytes().to_vec()))
}
struct Validator;
impl AcceptanceValidator for Validator {
    fn validate_checkpoint(&self, checkpoint: &Checkpoint) -> Result<()> {
        if checkpoint.head() != &spec().head
            || checkpoint.entries() != spec().objects
            || checkpoint.ledger() != "test:main"
            || checkpoint.generation() != "g1"
        {
            return Err(Error::Invalid("synthetic baseline semantics"));
        }
        for entry in checkpoint.entries() {
            assert_eq!(
                checkpoint.read(&entry.key)?.as_deref(),
                Some(entry.key.as_bytes())
            );
        }
        Ok(())
    }
    fn validate(&self, view: &AcceptanceView<'_>) -> Result<()> {
        assert!(view.checkpoint().is_some());
        if !view
            .transition
            .objects
            .iter()
            .any(|o| o.key == "commits/base")
        {
            assert!(view.content("commits/base").is_none());
        }
        assert_eq!(
            view.read_content("commits/base")?.as_deref(),
            Some(b"commits/base".as_slice())
        );
        assert!(view.read_content("unlisted-readable-file")?.is_none());
        Ok(())
    }
}
fn bootstrap(path: &Path) -> Arc<LocalRoot> {
    LocalRoot::bootstrap(path, "test:main", "g1", spec(), source, |c| {
        Validator.validate_checkpoint(c)
    })
    .unwrap()
}
fn transition(n: u8) -> Transition {
    Transition {
        ledger: "test:main".into(),
        generation: "g1".into(),
        head_key: "ns/head".into(),
        expected_head: Some(if n == 1 {
            spec().head.bytes
        } else {
            vec![n - 1]
        }),
        resulting_head: vec![n],
        objects: vec![Object {
            key: format!("tail/{n}"),
            bytes: vec![n; 13],
        }],
    }
}
fn erase_materialization(path: &Path) {
    let data = path.join(JOURNAL_DIR).join(DATA);
    std::fs::remove_dir_all(&data).unwrap();
    std::fs::create_dir(data).unwrap();
}

#[test]
fn baseline_plus_external_acknowledgments_recovers_without_materialization() {
    let root = tempfile::tempdir().unwrap();
    let owner = bootstrap(root.path());
    assert_eq!(owner.accepted_head().unwrap(), Some(spec().head.bytes));
    assert_eq!(
        std::fs::metadata(root.path().join(JOURNAL_DIR).join(JOURNAL))
            .unwrap()
            .len(),
        super::super::HEADER as u64
    );
    std::fs::write(
        root.path()
            .join(JOURNAL_DIR)
            .join(checkpoint::DIRECTORY)
            .join("objects/unlisted-readable-file"),
        b"readable but not a prerequisite",
    )
    .unwrap();
    // This controller-owned vector is independent of the erased recovery image.
    let mut acknowledged = Vec::new();
    for n in 1..=4 {
        let txn = transition(n);
        let receipt = owner.accept_with(&txn, &Validator, |_, _| Ok(())).unwrap();
        acknowledged.push((txn, receipt));
    }
    drop(owner);
    for _ in 0..2 {
        erase_materialization(root.path());
        let owner = LocalRoot::open(root.path()).unwrap();
        assert_eq!(owner.accepted_head().unwrap(), Some(vec![4]));
        owner
            .recover_with_checkpoint(|records, checkpoint| {
                Validator.validate_recovered_from(records, checkpoint.as_deref())?;
                assert_eq!(records.len(), acknowledged.len());
                for (record, (txn, receipt)) in records.iter().zip(&acknowledged) {
                    assert_eq!(&record.transition, txn);
                    assert_eq!(&record.receipt, receipt);
                }
                Ok(())
            })
            .unwrap();
        for (txn, _) in &acknowledged {
            for object in &txn.objects {
                assert_eq!(owner.read_bytes(&object.key).unwrap(), object.bytes);
            }
        }
        for entry in spec().objects {
            assert_eq!(owner.read_bytes(&entry.key).unwrap(), entry.key.as_bytes());
        }
    }
}

#[test]
fn empty_tail_recovers_baseline_and_rejects_genesis_or_foreign_head() {
    let root = tempfile::tempdir().unwrap();
    let owner = bootstrap(root.path());
    let mut genesis = transition(1);
    genesis.expected_head = None;
    assert!(matches!(
        owner.accept_with(&genesis, &Validator, |_, _| Ok(())),
        Err(Error::Conflict)
    ));
    drop(owner);
    erase_materialization(root.path());
    let owner = LocalRoot::open(root.path()).unwrap();
    assert_eq!(owner.read_bytes("ns/head").unwrap(), spec().head.bytes);
    drop(owner);
    std::fs::write(
        root.path().join(JOURNAL_DIR).join(DATA).join("ns/head"),
        b"foreign",
    )
    .unwrap();
    assert!(LocalRoot::open(root.path()).is_err());
}

#[test]
fn checkpoint_requires_explicit_validator_and_recovery_hook_support() {
    struct LegacyValidator;
    impl AcceptanceValidator for LegacyValidator {
        fn validate(&self, _: &AcceptanceView<'_>) -> Result<()> {
            Ok(())
        }
    }
    let root = tempfile::tempdir().unwrap();
    let owner = bootstrap(root.path());
    assert!(owner
        .recover_with(|_| panic!("legacy hook must not receive empty records"))
        .is_err());
    assert!(owner
        .accept_with(&transition(1), &LegacyValidator, |_, _| panic!(
            "legacy validator accepted baseline"
        ))
        .is_err());
    assert_eq!(owner.accepted_head().unwrap(), Some(spec().head.bytes));
    owner
        .accept_with(&transition(1), &Validator, |_, _| Ok(()))
        .unwrap();
    assert!(owner
        .recover_with_checkpoint(|_, _| Err(Error::Invalid("failed indexed installation")))
        .is_err());
    assert!(matches!(owner.accepted_head(), Err(Error::Poisoned)));
    owner
        .recover_with_checkpoint(|records, checkpoint| {
            Validator.validate_recovered_from(records, checkpoint.as_deref())
        })
        .unwrap();
    assert_eq!(owner.accepted_head().unwrap(), Some(vec![1]));
}

#[test]
fn immutable_checkpoint_keys_cannot_be_overwritten_or_repurposed() {
    let root = tempfile::tempdir().unwrap();
    let owner = bootstrap(root.path());
    for key in [
        "commits/base",
        "index",
        "index/leaves/1/child",
        "ns",
        "ns/head/child",
    ] {
        let mut txn = transition(1);
        txn.objects[0].key = key.into();
        assert!(
            owner
                .accept_with(&txn, &Validator, |_, _| panic!("collision installed"))
                .is_err(),
            "{key}"
        );
    }
    assert_eq!(owner.accepted_head().unwrap(), Some(spec().head.bytes));
    let mut txn = transition(1);
    // Identical content can also be journaled; this never mutates the checkpoint.
    txn.objects.push(Object {
        key: "commits/base".into(),
        bytes: b"commits/base".to_vec(),
    });
    owner.accept_with(&txn, &Validator, |_, _| Ok(())).unwrap();
}

#[test]
fn cuts_at_every_bootstrap_boundary_never_open_an_empty_ready_database() {
    let mut steps = Vec::new();
    let root = tempfile::tempdir().unwrap();
    drop(
        LocalRoot::bootstrap_steps(
            root.path(),
            "test:main",
            "g1",
            spec(),
            source,
            |c| Validator.validate_checkpoint(c),
            &mut |event| {
                steps.push(event);
                Ok(())
            },
        )
        .unwrap(),
    );
    assert_eq!(steps.iter().filter(|s| **s == "object synced").count(), 3);
    let validated = steps
        .iter()
        .position(|s| *s == "baseline validated")
        .unwrap();
    let journal = steps
        .iter()
        .position(|s| *s == "journal initialized")
        .unwrap();
    let ready = steps
        .iter()
        .rposition(|s| *s == "manifest published")
        .unwrap();
    assert!(validated < journal && journal < ready);
    assert!(steps[..validated].contains(&"checkpoint entry synced"));
    for cut in 0..steps.len() {
        let root = tempfile::tempdir().unwrap();
        let mut cursor = 0;
        let result = LocalRoot::bootstrap_steps(
            root.path(),
            "test:main",
            "g1",
            spec(),
            source,
            |c| Validator.validate_checkpoint(c),
            &mut |event| {
                assert_eq!(event, steps[cursor]);
                let fail = cursor == cut;
                cursor += 1;
                if fail {
                    Err(Error::Io(io::ErrorKind::Other.into()))
                } else {
                    Ok(())
                }
            },
        );
        assert!(result.is_err());
        assert!(crate::FileStorage::new(root.path())
            .ensure_ordinary_access()
            .is_err());
        for _ in 0..2 {
            match LocalRoot::open(root.path()) {
                Ok(owner) => {
                    assert!(cut >= ready, "premature ready at {}", steps[cut]);
                    assert_eq!(owner.accepted_head().unwrap(), Some(spec().head.bytes));
                    for entry in spec().objects {
                        assert_eq!(owner.read_bytes(&entry.key).unwrap(), entry.key.as_bytes());
                    }
                }
                Err(_) => assert!(cut < ready, "complete ready image failed at {}", steps[cut]),
            }
        }
        assert!(LocalRoot::initialize(root.path(), "test:main", "g1").is_err());
    }
}

#[test]
fn missing_corrupt_and_symlinked_checkpoint_dependencies_fail_closed() {
    for entry in spec().objects {
        for failure in ["missing", "corrupt", "short", "extra", "symlink"] {
            let root = tempfile::tempdir().unwrap();
            drop(bootstrap(root.path()));
            let path = root
                .path()
                .join(JOURNAL_DIR)
                .join(checkpoint::DIRECTORY)
                .join("objects")
                .join(&entry.key);
            match failure {
                "missing" => std::fs::remove_file(&path).unwrap(),
                "corrupt" => std::fs::write(&path, vec![0; entry.length as usize]).unwrap(),
                "short" => std::fs::write(&path, b"").unwrap(),
                "extra" => std::fs::write(&path, vec![0; entry.length as usize + 1]).unwrap(),
                "symlink" => {
                    let target = root.path().join("outside");
                    std::fs::write(&target, entry.key.as_bytes()).unwrap();
                    std::fs::remove_file(&path).unwrap();
                    std::os::unix::fs::symlink(target, &path).unwrap();
                }
                _ => unreachable!(),
            }
            for _ in 0..2 {
                assert!(
                    LocalRoot::open(root.path()).is_err(),
                    "{failure}: {}",
                    entry.key
                );
            }
        }
    }
}

#[test]
fn source_mismatch_and_semantic_rejection_preserve_fenced_unready_root() {
    for failure in ["short", "extra", "hash", "read", "semantics"] {
        let root = tempfile::tempdir().unwrap();
        let result = LocalRoot::bootstrap(
            root.path(),
            "test:main",
            "g1",
            spec(),
            |entry| {
                let mut bytes = entry.key.as_bytes().to_vec();
                match failure {
                    "short" => {
                        bytes.pop();
                    }
                    "extra" => bytes.push(0),
                    "hash" => bytes[0] ^= 1,
                    "read" => return Err(Error::Io(io::ErrorKind::UnexpectedEof.into())),
                    _ => {}
                }
                Ok(Cursor::new(bytes))
            },
            |_| Err(Error::Invalid("source changed or unsupported baseline")),
        );
        assert!(result.is_err());
        assert!(!root.path().join(JOURNAL_DIR).join(FORMAT).exists());
        assert!(crate::FileStorage::new(root.path())
            .ensure_ordinary_access()
            .is_err());
        for _ in 0..2 {
            assert!(LocalRoot::open(root.path()).is_err());
        }
    }
}

#[test]
fn manifest_and_journal_bind_the_exact_baseline_and_generation() {
    for failure in [
        "descriptor",
        "generation",
        "checkpoint_binding",
        "foreign_journal",
        "version",
    ] {
        let root = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        drop(bootstrap(root.path()));
        drop(bootstrap(other.path()));
        let control = root.path().join(JOURNAL_DIR);
        let format = control.join(FORMAT);
        match failure {
            "descriptor" => {
                let path = control
                    .join(checkpoint::DIRECTORY)
                    .join(checkpoint::MANIFEST);
                let mut bytes = std::fs::read(&path).unwrap();
                bytes.push(b' ');
                std::fs::write(path, bytes).unwrap();
            }
            "foreign_journal" => {
                std::fs::copy(
                    other.path().join(JOURNAL_DIR).join(JOURNAL),
                    control.join(JOURNAL),
                )
                .unwrap();
            }
            "checkpoint_binding" => {
                // Even a locally checksummed replacement descriptor cannot be
                // silently paired with the journal of the prior baseline.
                let path = control
                    .join(checkpoint::DIRECTORY)
                    .join(checkpoint::MANIFEST);
                let mut value: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                value["baseline"]["head"]["bytes"] = serde_json::json!([42]);
                let bytes = serde_json::to_vec(&value).unwrap();
                std::fs::write(path, &bytes).unwrap();
                let mut manifest: Manifest =
                    serde_json::from_slice(&std::fs::read(&format).unwrap()).unwrap();
                manifest.checkpoint = Some(super::super::digest(&bytes));
                std::fs::write(format, serde_json::to_vec(&manifest).unwrap()).unwrap();
            }
            _ => {
                let mut manifest: Manifest =
                    serde_json::from_slice(&std::fs::read(&format).unwrap()).unwrap();
                if failure == "generation" {
                    manifest.generation = "wrong".into();
                } else {
                    manifest.version = 1;
                }
                std::fs::write(format, serde_json::to_vec(&manifest).unwrap()).unwrap();
            }
        }
        for _ in 0..2 {
            assert!(LocalRoot::open(root.path()).is_err(), "{failure}");
        }
    }
}

#[test]
fn retained_checkpoint_lease_prevents_new_owner_and_reads_verify_bytes() {
    let root = tempfile::tempdir().unwrap();
    let owner = bootstrap(root.path());
    let mut checkpoint = None;
    owner
        .recover_with_checkpoint(|_, c| {
            checkpoint = c;
            Ok(())
        })
        .unwrap();
    let checkpoint = checkpoint.unwrap();
    drop(owner);
    assert!(LocalRoot::open(root.path()).is_err());
    assert_eq!(
        checkpoint.read("commits/base").unwrap().unwrap(),
        b"commits/base"
    );
    let object = root
        .path()
        .join(JOURNAL_DIR)
        .join(checkpoint::DIRECTORY)
        .join("objects/commits/base");
    std::fs::write(&object, b"corrupt/base").unwrap();
    assert!(checkpoint.read("commits/base").is_err());
    std::fs::write(object, b"commits/base").unwrap();
    drop(checkpoint);
    assert!(LocalRoot::open(root.path()).is_ok());
}

#[test]
fn unsafe_duplicate_unsorted_colliding_and_oversized_inventory_rejected_before_fence() {
    let mut invalid = Vec::new();
    for key in [
        "../escape",
        "/absolute",
        "a//b",
        "a/./b",
        "a\\b",
        "a:b",
        "a/.fluree-wal/b",
        "ns/head",
        "ns",
        "ns/head/x",
        "index",
        "index/leaves/1/x",
    ] {
        let mut input = spec();
        input.objects.push(CheckpointEntry {
            key: key.into(),
            length: 0,
            sha256: super::super::digest(b""),
        });
        input.objects.sort_by(|a, b| a.key.cmp(&b.key));
        invalid.push(input);
    }
    let mut input = spec();
    input.objects.reverse();
    invalid.push(input);
    let mut input = spec();
    input.objects[0].length = checkpoint::MAX_CHECKPOINT_OBJECT_BYTES + 1;
    invalid.push(input);
    for input in invalid {
        let root = tempfile::tempdir().unwrap();
        assert!(
            LocalRoot::bootstrap(root.path(), "test:main", "g1", input, source, |_| Ok(()))
                .is_err()
        );
        assert!(!root.path().join(JOURNAL_DIR).exists());
    }
}

#[test]
fn large_source_is_streamed_outside_the_journal_cap() {
    // Synthesize >64 MiB without retaining it in the source or coordinator.
    let root = tempfile::tempdir().unwrap();
    let length = super::super::MAX_JOURNAL_BYTES + 17;
    let mut hash = sha2::Sha256::new();
    use sha2::Digest;
    let block = [b'x'; 65536];
    for _ in 0..length / block.len() as u64 {
        hash.update(block);
    }
    hash.update(&block[..(length % block.len() as u64) as usize]);
    let mut input = spec();
    input.objects = vec![CheckpointEntry {
        key: "large".into(),
        length,
        sha256: hash.finalize().into(),
    }];
    let owner = LocalRoot::bootstrap(
        root.path(),
        "test:main",
        "g1",
        input,
        |_| Ok(io::repeat(b'x').take(length)),
        |_| Ok(()),
    )
    .unwrap();
    assert_eq!(
        std::fs::metadata(root.path().join(JOURNAL_DIR).join(JOURNAL))
            .unwrap()
            .len(),
        super::super::HEADER as u64
    );
    drop(owner);
    assert_eq!(
        LocalRoot::open(root.path())
            .unwrap()
            .accepted_head()
            .unwrap(),
        Some(spec().head.bytes)
    );
}

#[test]
fn post_flush_failure_reconciles_tail_and_damaged_baseline_keeps_owner_blocked() {
    let root = tempfile::tempdir().unwrap();
    let owner = bootstrap(root.path());
    let receipt = match owner.accept_with(&transition(1), &Validator, |_, _| {
        Err(Error::Invalid("interrupted application installation"))
    }) {
        Err(Error::AcceptanceUnresolved {
            durable: Some(receipt),
            ..
        }) => receipt,
        _ => panic!("expected a durable unresolved tail"),
    };
    assert!(matches!(owner.accepted_head(), Err(Error::Poisoned)));
    erase_materialization(root.path());
    let object = root
        .path()
        .join(JOURNAL_DIR)
        .join(checkpoint::DIRECTORY)
        .join("objects/commits/base");
    std::fs::remove_file(&object).unwrap();
    assert!(owner
        .recover_with_checkpoint(|_, _| panic!("damaged baseline installed"))
        .is_err());
    assert!(matches!(owner.read_bytes("tail/1"), Err(Error::Poisoned)));
    assert!(matches!(LocalRoot::open(root.path()), Err(Error::Poisoned)));
    std::fs::write(object, b"commits/base").unwrap();
    owner
        .recover_with_checkpoint(|records, checkpoint| {
            Validator.validate_recovered_from(records, checkpoint.as_deref())?;
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].receipt, receipt);
            Ok(())
        })
        .unwrap();
    assert_eq!(owner.accepted_head().unwrap(), Some(vec![1]));
    assert!(matches!(
        owner.accept_with(&transition(1), &Validator, |_, _| Ok(())),
        Err(Error::Conflict)
    ));
}
