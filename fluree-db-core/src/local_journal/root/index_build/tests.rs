use super::*;
use crate::local_journal::{AcceptanceView, MAX_JOURNAL_BYTES};
use std::io::Cursor;

struct Validate;
impl AcceptanceValidator for Validate {
    fn validate(&self, _: &AcceptanceView<'_>) -> Result<()> {
        Ok(())
    }
}
fn commit(owner: &Arc<LocalRoot>, n: u8) -> Result<Receipt> {
    owner.accept_with(&transition(owner, n), &Validate, |_, _| Ok(()))
}
fn transition(owner: &Arc<LocalRoot>, n: u8) -> Transition {
    Transition {
        index_publication: None,
        ledger: "index:main".into(),
        generation: "g1".into(),
        head_key: "ns/head".into(),
        expected_head: owner.accepted_head().unwrap(),
        resulting_head: vec![n],
        objects: vec![Object {
            key: format!("commits/{n}"),
            bytes: vec![n],
        }],
    }
}
fn owner() -> (tempfile::TempDir, Arc<LocalRoot>) {
    let d = tempfile::tempdir().unwrap();
    let o = LocalRoot::initialize(d.path(), "index:main", "g1").unwrap();
    commit(&o, 1).unwrap();
    (d, o)
}
fn objects() -> Vec<CheckpointEntry> {
    ["index/dict/a", "index/leaf/a", "index/root"]
        .into_iter()
        .map(|key| CheckpointEntry {
            key: key.into(),
            length: key.len() as u64,
            sha256: crate::local_journal::digest(key.as_bytes()),
        })
        .collect()
}
fn source(e: &CheckpointEntry) -> Result<Cursor<Vec<u8>>> {
    Ok(Cursor::new(e.key.as_bytes().to_vec()))
}
fn validate(c: &Checkpoint) -> Result<()> {
    for e in c.entries() {
        assert_eq!(c.read(&e.key)?.as_deref(), Some(e.key.as_bytes()));
    }
    Ok(())
}
fn prepare(o: &Arc<LocalRoot>) -> PreparedIndex {
    o.pin_index_build()
        .unwrap()
        .prepare(objects(), source, validate)
        .unwrap()
}
fn journal(d: &Path) -> Vec<u8> {
    std::fs::read(d.join(JOURNAL_DIR).join(JOURNAL)).unwrap()
}

#[test]
fn preparation_does_not_publish_and_newer_commits_proceed_during_copy() {
    let (d, o) = owner();
    let pin = o.pin_index_build().unwrap();
    let base = pin.frontier().clone();
    let mut advanced = false;
    let prepared = pin
        .prepare(
            objects(),
            |e| {
                if !advanced {
                    commit(&o, 2)?;
                    commit(&o, 3)?;
                    advanced = true;
                }
                source(e)
            },
            validate,
        )
        .unwrap();
    assert_eq!(prepared.head().bytes, vec![1]);
    assert_eq!(prepared.frontier(), &base);
    assert_eq!(o.accepted_head().unwrap(), Some(vec![3]));
    let acknowledged = journal(d.path());
    prepared.verify_for(&o).unwrap();
    assert_eq!(
        prepared.read("index/root").unwrap(),
        Some(b"index/root".to_vec())
    );
    assert!(o.read_bytes("index/root").is_err());
    assert_eq!(journal(d.path()), acknowledged);
    let (_foreign_dir, foreign) = owner();
    assert!(matches!(
        prepared.verify_for(&foreign),
        Err(Error::Invalid("foreign prepared index owner"))
    ));
    // The opaque, retained handle keeps BOTH owner and journal ownership alive.
    let weak = Arc::downgrade(&o);
    drop(o);
    assert!(weak.upgrade().is_some());
    let o = weak.upgrade().unwrap();
    drop(prepared);
    drop(o);
    assert!(weak.upgrade().is_none());
    for _ in 0..2 {
        std::fs::remove_dir_all(d.path().join(JOURNAL_DIR).join(DATA)).unwrap();
        std::fs::create_dir(d.path().join(JOURNAL_DIR).join(DATA)).unwrap();
        let reopened = LocalRoot::open(d.path()).unwrap();
        assert_eq!(reopened.accepted_head().unwrap(), Some(vec![3]));
        assert!(reopened.read_bytes("index/root").is_err());
        assert_eq!(journal(d.path()), acknowledged);
    }
}

#[test]
fn every_completed_staging_cut_leaves_accepted_database_and_retry_intact() {
    let (_d, o) = owner();
    let mut steps = Vec::new();
    o.pin_index_build()
        .unwrap()
        .prepare_steps(objects(), source, validate, &mut |s| {
            steps.push(s);
            Ok(())
        })
        .unwrap();
    for cut in 0..steps.len() {
        let (d, o) = owner();
        let before = journal(d.path());
        let mut i = 0;
        let result =
            o.pin_index_build()
                .unwrap()
                .prepare_steps(objects(), source, validate, &mut |_| {
                    let fail = i == cut;
                    i += 1;
                    if fail {
                        Err(Error::Invalid("staging cut"))
                    } else {
                        Ok(())
                    }
                });
        assert!(result.is_err(), "cut {cut}: {}", steps[cut]);
        assert_eq!(journal(d.path()), before);
        assert_eq!(o.accepted_head().unwrap(), Some(vec![1]));
        drop(o);
        for _ in 0..2 {
            let reopened = LocalRoot::open(d.path()).unwrap();
            assert_eq!(reopened.accepted_head().unwrap(), Some(vec![1]));
            assert!(reopened.read_bytes("index/root").is_err());
        }
        let reopened = LocalRoot::open(d.path()).unwrap();
        prepare(&reopened).verify_for(&reopened).unwrap();
        assert_eq!(journal(d.path()), before);
        commit(&reopened, 2).unwrap();
    }
}

#[test]
fn wrong_sources_semantic_rejection_and_bad_inventory_never_change_acceptance() {
    let (d, o) = owner();
    let before = journal(d.path());
    for bytes in [
        b"short".to_vec(),
        b"index/dict/ax".to_vec(),
        b"xxxxxxxxxxxx".to_vec(),
    ] {
        assert!(o
            .pin_index_build()
            .unwrap()
            .prepare(objects(), |_| Ok(Cursor::new(bytes.clone())), |_| Ok(()))
            .is_err());
    }
    assert!(o
        .pin_index_build()
        .unwrap()
        .prepare(objects(), source, |_| Err(Error::Invalid(
            "bad index semantics"
        )))
        .is_err());
    for invalid in [
        vec![],
        vec![CheckpointEntry {
            key: "../escape".into(),
            ..objects()[0].clone()
        }],
        vec![objects()[0].clone(); 2],
        objects().into_iter().rev().collect(),
    ] {
        assert!(o
            .pin_index_build()
            .unwrap()
            .prepare(invalid, source, |_| panic!(
                "bad inventory reached validator"
            ))
            .is_err());
    }
    assert_eq!(journal(d.path()), before);
    prepare(&o).verify_for(&o).unwrap();
}

#[test]
fn manifest_binds_exact_prefix_and_rechecks_corruption_and_owner_health() {
    let (d, o) = owner();
    let first = prepare(&o);
    commit(&o, 2).unwrap();
    commit(&o, 1).unwrap(); // synthetic core ABA; API rejects this.
    let second = prepare(&o);
    assert_eq!(first.head(), second.head());
    assert_ne!(first.frontier(), second.frontier());
    assert_ne!(first.manifest_digest(), second.manifest_digest());
    first.verify_for(&o).unwrap();
    second.verify_for(&o).unwrap();
    let t = transition(&o, 3);
    assert!(o
        .accept_with(&t, &Validate, |_, _| Err(Error::Invalid("install failure")))
        .is_err());
    assert!(matches!(first.read("index/root"), Err(Error::Poisoned)));
    assert!(matches!(first.verify_for(&o), Err(Error::Poisoned)));
    assert!(matches!(o.pin_index_build(), Err(Error::Poisoned)));
    o.recover_with(|rs| Validate.validate_recovered(rs))
        .unwrap();
    first.verify_for(&o).unwrap();
    assert_eq!(o.accepted_head().unwrap(), Some(vec![3]));
    let manifest = first.build_path.join("checkpoint/manifest.json");
    let bytes = std::fs::read(&manifest).unwrap();
    std::fs::write(&manifest, b"bad").unwrap();
    assert!(first.verify_for(&o).is_err());
    std::fs::write(&manifest, bytes).unwrap();
    let artifact = first.build_path.join("checkpoint/objects/index/root");
    std::fs::write(&artifact, b"bad bytes!").unwrap();
    assert!(first.read("index/root").is_err());
    assert!(first.verify_for(&o).is_err());
    std::fs::remove_file(&artifact).unwrap();
    assert!(first.verify_for(&o).is_err());
    std::os::unix::fs::symlink(d.path().join(JOURNAL_DIR).join(JOURNAL), &artifact).unwrap();
    assert!(first.verify_for(&o).is_err());
    assert_eq!(o.accepted_head().unwrap(), Some(vec![3]));
    // Unpublished corrupt files never become recovery prerequisites.
    drop((first, second, o));
    let reopened = LocalRoot::open(d.path()).unwrap();
    assert_eq!(reopened.accepted_head().unwrap(), Some(vec![3]));
}

#[test]
fn large_index_streams_outside_the_journal_limit() {
    use sha2::{Digest, Sha256};
    let (d, o) = owner();
    let before = journal(d.path());
    let length = MAX_JOURNAL_BYTES + 1;
    let mut hash = Sha256::new();
    let block = [7u8; 65536];
    let mut remaining = length;
    while remaining > 0 {
        let n = remaining.min(block.len() as u64) as usize;
        hash.update(&block[..n]);
        remaining -= n as u64;
    }
    struct Bounded {
        remaining: u64,
    }
    impl Read for Bounded {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            assert!(
                out.len() <= 65536,
                "copy allocated an oversized read buffer"
            );
            let n = (self.remaining as usize).min(out.len());
            out[..n].fill(7);
            self.remaining -= n as u64;
            Ok(n)
        }
    }
    let prepared = o
        .pin_index_build()
        .unwrap()
        .prepare(
            vec![CheckpointEntry {
                key: "index/large".into(),
                length,
                sha256: hash.finalize().into(),
            }],
            |_| Ok(Bounded { remaining: length }),
            |_| Ok(()),
        )
        .unwrap();
    prepared.verify_for(&o).unwrap();
    assert_eq!(journal(d.path()), before);
    assert_eq!(prepared.entries()[0].length, length);
}

#[test]
fn staging_is_not_a_bootstrap_checkpoint_and_managed_symlinks_are_rejected() {
    let (d, o) = owner();
    let prepared = prepare(&o);
    // Identical root/ledger/head/hash cannot erase the staging-purpose binding.
    assert!(Checkpoint::open(
        &prepared.build_path,
        checkpoint::Binding {
            identity: o.manifest.identity,
            ledger: o.ledger(),
            generation: o.generation(),
            index_build: None
        },
        prepared.manifest_digest(),
        o._directory_lock.clone()
    )
    .is_err());
    for generation in ["foreign-generation", o.generation()] {
        assert!(Checkpoint::open(
            &prepared.build_path,
            checkpoint::Binding {
                identity: o.manifest.identity,
                ledger: o.ledger(),
                generation,
                index_build: Some([0; 32])
            },
            prepared.manifest_digest(),
            o._directory_lock.clone()
        )
        .is_err());
    }
    let cp = prepared.build_path.join(checkpoint::DIRECTORY);
    let moved = prepared.build_path.join("moved");
    std::fs::rename(&cp, &moved).unwrap();
    std::os::unix::fs::symlink(&moved, &cp).unwrap();
    assert!(prepared.read("index/root").is_err());
    assert!(prepared.verify_for(&o).is_err());
    std::fs::remove_file(&cp).unwrap();
    std::fs::rename(&moved, &cp).unwrap();
    prepared.verify_for(&o).unwrap();
    let builds = d.path().join(JOURNAL_DIR).join(BUILDS);
    let moved = d.path().join(JOURNAL_DIR).join("moved-builds");
    std::fs::rename(&builds, &moved).unwrap();
    std::os::unix::fs::symlink(&moved, &builds).unwrap();
    assert!(prepared.read("index/root").is_err());
    assert!(prepared.verify_for(&o).is_err());
    assert!(o
        .pin_index_build()
        .unwrap()
        .prepare(objects(), source, validate)
        .is_err());
    assert_eq!(o.accepted_head().unwrap(), Some(vec![1]));
}

#[test]
fn preparation_fails_if_writer_becomes_unresolved_and_empty_roots_cannot_pin() {
    let d = tempfile::tempdir().unwrap();
    let o = LocalRoot::initialize(d.path(), "index:main", "g1").unwrap();
    assert!(o.pin_index_build().is_err());
    assert!(!d.path().join(JOURNAL_DIR).join(BUILDS).exists());
    commit(&o, 1).unwrap();
    let pin = o.pin_index_build().unwrap();
    let t = transition(&o, 2);
    let result = pin.prepare(objects(), source, |_| {
        assert!(o
            .accept_with(&t, &Validate, |_, _| Err(Error::Invalid(
                "install cut during build"
            )))
            .is_err());
        Ok(())
    });
    assert!(matches!(result, Err(Error::Poisoned)));
    o.recover_with(|rs| Validate.validate_recovered(rs))
        .unwrap();
    assert_eq!(o.accepted_head().unwrap(), Some(vec![2]));
    assert!(o.read_bytes("index/root").is_err());
    prepare(&o).verify_for(&o).unwrap();
}

#[test]
fn concurrent_builds_use_distinct_directories_and_preserve_their_inputs() {
    let (_d, o) = owner();
    let first = o.pin_index_build().unwrap();
    commit(&o, 2).unwrap();
    let second = o.pin_index_build().unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let spawn = |pin: IndexBuildPin| {
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            let mut first = true;
            pin.prepare(
                objects(),
                |e| {
                    if first {
                        barrier.wait();
                        first = false;
                    }
                    source(e)
                },
                validate,
            )
            .unwrap()
        })
    };
    let a = spawn(first);
    let b = spawn(second);
    let a = a.join().unwrap();
    let b = b.join().unwrap();
    assert_ne!(a.build_path, b.build_path);
    assert_eq!(a.head().bytes, vec![1]);
    assert_eq!(b.head().bytes, vec![2]);
    a.verify_for(&o).unwrap();
    b.verify_for(&o).unwrap();
    assert_eq!(o.accepted_head().unwrap(), Some(vec![2]));
}
