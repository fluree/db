use super::*;
use crate::local_journal::{tests::FaultIo, Journal};
use std::io::Cursor;

// Deliberately opaque core fixture: [commit, built-through index]. The real
// Fluree embedding must supply its own CID/closure and nameservice validator.
struct Validate;
impl AcceptanceValidator for Validate {
    fn validate(&self, v: &AcceptanceView<'_>) -> Result<()> {
        let t = &v.transition;
        let old = t.expected_head.as_deref().unwrap_or(&[0, 0]);
        if t.resulting_head != [old[0] + 1, old[1]] {
            return Err(Error::Invalid("synthetic commit semantics"));
        }
        if old[1] > 0 {
            let key = format!("index/{}/root", old[1]);
            if v.read_content(&key)?.as_deref() != Some(key.as_bytes()) {
                return Err(Error::Invalid("missing accepted index"));
            }
        }
        // A later publication must never supply this earlier commit's inputs.
        if old[1] == 0 && v.read_content("index/1/root")?.is_some() {
            return Err(Error::Invalid("future prerequisite leaked"));
        }
        Ok(())
    }
    fn validate_checkpoint(&self, c: &Checkpoint) -> Result<()> {
        if c.head().bytes != [1, 0] {
            return Err(Error::Invalid("baseline"));
        }
        Ok(())
    }
    fn validate_index_publication(&self, v: &AcceptanceView<'_>, c: &Checkpoint) -> Result<()> {
        let old = v.transition.expected_head.as_deref().unwrap();
        let new = &v.transition.resulting_head;
        let built = c.head().bytes[0];
        if new != &[old[0], built] || built <= old[1] || built > old[0] {
            return Err(Error::Invalid("synthetic publication semantics"));
        }
        for suffix in ["dict", "leaf", "root"] {
            let key = format!("index/{built}/{suffix}");
            if v.read_content(&key)?.as_deref() != Some(key.as_bytes()) {
                return Err(Error::Invalid("synthetic index closure"));
            }
        }
        Ok(())
    }
}
struct Legacy;
impl AcceptanceValidator for Legacy {
    fn validate(&self, _: &AcceptanceView<'_>) -> Result<()> {
        Ok(())
    }
}
fn owner() -> (tempfile::TempDir, Arc<LocalRoot>) {
    let d = tempfile::tempdir().unwrap();
    let o = LocalRoot::initialize(d.path(), "pub:main", "g1").unwrap();
    commit(&o).unwrap();
    (d, o)
}
fn commit(o: &Arc<LocalRoot>) -> Result<Receipt> {
    let old = o.accepted_head()?;
    let h = old.as_deref().unwrap_or(&[0, 0]);
    o.accept_with(
        &Transition {
            ledger: o.ledger().into(),
            generation: o.generation().into(),
            head_key: "ns/head".into(),
            resulting_head: vec![h[0] + 1, h[1]],
            objects: vec![Object {
                key: format!("commits/{}", h[0] + 1),
                bytes: vec![h[0] + 1],
            }],
            expected_head: old,
            index_publication: None,
        },
        &Validate,
        |_, _| Ok(()),
    )
}
fn prepare_keys(o: &Arc<LocalRoot>, keys: Vec<String>) -> PreparedIndex {
    try_prepare_keys(o, keys).unwrap()
}
fn try_prepare_keys(o: &Arc<LocalRoot>, mut keys: Vec<String>) -> Result<PreparedIndex> {
    keys.sort();
    let entries = keys
        .into_iter()
        .map(|key| CheckpointEntry {
            length: key.len() as u64,
            sha256: crate::local_journal::digest(key.as_bytes()),
            key,
        })
        .collect();
    o.pin_index_build().unwrap().prepare(
        entries,
        |e| Ok(Cursor::new(e.key.as_bytes().to_vec())),
        |_| Ok(()),
    )
}
fn prepare(o: &Arc<LocalRoot>) -> PreparedIndex {
    let n = o.accepted_head().unwrap().unwrap()[0];
    prepare_keys(
        o,
        ["dict", "leaf", "root"]
            .map(|s| format!("index/{n}/{s}"))
            .to_vec(),
    )
}
fn publish(o: &Arc<LocalRoot>, p: &PreparedIndex) -> Result<Receipt> {
    o.publish_index(
        p,
        |h| Ok(vec![h.bytes[0], p.head().bytes[0]]),
        &Validate,
        |v, r| {
            assert_eq!(
                v.frontier_after(r).head(),
                Some(v.transition.resulting_head.as_slice())
            );
            Ok(())
        },
    )
}
fn journal(d: &Path) -> Vec<u8> {
    std::fs::read(d.join(JOURNAL_DIR).join(JOURNAL)).unwrap()
}
fn recover(o: &LocalRoot) -> Result<()> {
    o.recover_with_indexes(|rs, base, indexes, _| {
        Validate.validate_recovered_with_indexes(rs, base.as_deref(), indexes)
    })
}
fn erase(d: &Path) {
    let data = d.join(JOURNAL_DIR).join(DATA);
    std::fs::remove_dir_all(&data).unwrap();
    std::fs::create_dir(data).unwrap();
}

#[test]
fn old_build_preserves_new_commits_and_recovers_twice_with_exact_receipts() {
    let (d, o) = owner();
    let p = prepare(&o);
    let mut receipts = vec![commit(&o).unwrap(), commit(&o).unwrap()];
    assert!(o.read_bytes("index/1/root").is_err());
    receipts.push(publish(&o, &p).unwrap());
    assert_eq!(o.accepted_head().unwrap(), Some(vec![3, 1]));
    assert_eq!(o.read_bytes("index/1/root").unwrap(), b"index/1/root");
    receipts.push(commit(&o).unwrap());
    let acknowledged = journal(d.path());
    drop(p);
    drop(o);
    for _ in 0..2 {
        erase(d.path());
        let o = LocalRoot::open(d.path()).unwrap();
        o.recover_with_indexes(|rs, base, indexes, frontier| {
            Validate.validate_recovered_with_indexes(rs, base.as_deref(), indexes)?;
            assert_eq!(indexes.len(), 1);
            assert_eq!(
                rs[1..]
                    .iter()
                    .map(|r| r.receipt.clone())
                    .collect::<Vec<_>>(),
                receipts
            );
            assert_eq!(frontier.head(), Some([4, 1].as_slice()));
            Ok(())
        })
        .unwrap();
        assert_eq!(o.read_bytes("commits/4").unwrap(), [4]);
        assert_eq!(o.read_bytes("index/1/root").unwrap(), b"index/1/root");
        assert_eq!(journal(d.path()), acknowledged);
    }
}

#[test]
fn legacy_raw_forged_foreign_and_semantic_rejections_do_not_append() {
    let (d, o) = owner();
    let p = prepare(&o);
    let before = journal(d.path());
    assert!(o
        .publish_index(&p, |_| Ok(vec![1, 1]), &Legacy, |_, _| panic!("installed"))
        .is_err());
    assert!(o
        .publish_index(
            &p,
            |_| Ok(vec![2, 1]),
            &Validate,
            |_, _| panic!("installed")
        )
        .is_err());
    let (_other, foreign) = owner();
    assert!(publish(&foreign, &p).is_err());
    let t = Transition {
        ledger: o.ledger().into(),
        generation: o.generation().into(),
        head_key: "ns/head".into(),
        expected_head: Some(vec![1, 0]),
        resulting_head: vec![1, 1],
        objects: vec![],
        index_publication: Some(IndexPublication {
            build: p.build_path.file_name().unwrap().to_str().unwrap().into(),
            manifest: p.manifest_digest(),
            input_prefix: p.frontier().prefix_digest(),
            input_head: p.head().clone(),
        }),
    };
    // Raw acceptance cannot bypass the verified prepared-handle capability.
    assert!(o
        .accept_with(&t, &Legacy, |_, _| panic!("installed"))
        .is_err());
    for altered in 0..4 {
        let mut forged = t.clone();
        let r = forged.index_publication.as_mut().unwrap();
        match altered {
            0 => r.input_prefix = [0; 32],
            1 => r.input_head.bytes = vec![9, 0],
            2 => r.manifest = [0; 32],
            _ => r.build = "../escape".into(),
        }
        let candidate = p.verified_checkpoint(&o).unwrap();
        let mut state = o.coordinator.lock();
        assert!(state
            .accept_index(
                &forged,
                &Validate,
                &mut FileTarget {
                    root: &d.path().join(JOURNAL_DIR).join(DATA)
                },
                |_, _| panic!("installed"),
                Some(candidate)
            )
            .is_err());
    }
    assert_eq!(journal(d.path()), before);
    assert_eq!(o.accepted_head().unwrap(), Some(vec![1, 0]));
    publish(&o, &p).unwrap();
}

#[test]
fn duplicate_and_regressing_indexes_reject_but_next_build_advances() {
    let (d, o) = owner();
    let p1 = prepare(&o);
    let other1 = prepare(&o);
    commit(&o).unwrap();
    let p2 = prepare(&o);
    publish(&o, &p2).unwrap();
    let before = journal(d.path());
    for p in [&p2, &p1, &other1] {
        assert!(publish(&o, p).is_err());
    }
    assert_eq!(journal(d.path()), before);
    commit(&o).unwrap();
    let p3 = prepare(&o);
    publish(&o, &p3).unwrap();
    recover(&o).unwrap();
    assert_eq!(o.accepted_head().unwrap(), Some(vec![3, 3]));
    assert_eq!(o.read_bytes("index/2/root").unwrap(), b"index/2/root");
}

#[test]
fn installation_error_and_panic_poison_until_publication_aware_recovery() {
    for panic in [false, true] {
        let (_d, o) = owner();
        let p = prepare(&o);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            o.publish_index(
                &p,
                |_| Ok(vec![1, 1]),
                &Validate,
                |_, _| {
                    if panic {
                        panic!("install crash");
                    }
                    Err(Error::Invalid("install failure"))
                },
            )
        }));
        if panic {
            assert!(result.is_err());
        } else {
            assert!(matches!(
                result.unwrap(),
                Err(Error::AcceptanceUnresolved {
                    durable: Some(_),
                    ..
                })
            ));
        }
        assert!(matches!(o.accepted_head(), Err(Error::Poisoned)));
        assert!(matches!(o.read_bytes("index/1/root"), Err(Error::Poisoned)));
        assert!(matches!(publish(&o, &p), Err(Error::Poisoned)));
        assert!(o
            .recover_with_frontier(|_, _, _| panic!("legacy install"))
            .is_err());
        assert!(matches!(o.accepted_head(), Err(Error::Poisoned)));
        recover(&o).unwrap();
        assert_eq!(o.accepted_head().unwrap(), Some(vec![1, 1]));
        commit(&o).unwrap();
    }
}

#[test]
fn referenced_artifact_loss_corruption_and_symlinks_fail_closed() {
    for relative in [
        "checkpoint/manifest.json",
        "checkpoint/objects/index/1/dict",
        "checkpoint/objects/index/1/leaf",
        "checkpoint/objects/index/1/root",
    ] {
        for mode in 0..3 {
            let (d, o) = owner();
            let p = prepare(&o);
            publish(&o, &p).unwrap();
            let path = p.build_path.join(relative);
            match mode {
                0 => std::fs::remove_file(&path).unwrap(),
                1 => std::fs::write(&path, b"corrupt").unwrap(),
                _ => {
                    std::fs::remove_file(&path).unwrap();
                    std::os::unix::fs::symlink(d.path().join(JOURNAL_DIR).join(JOURNAL), path)
                        .unwrap();
                }
            }
            assert!(recover(&o).is_err(), "{relative} mode {mode}");
            assert!(matches!(o.accepted_head(), Err(Error::Poisoned)));
            drop(p);
            drop(o);
            assert!(LocalRoot::open(d.path()).is_err(), "{relative} mode {mode}");
        }
    }
}

#[test]
fn unpublished_corruption_is_ignored_and_published_parent_symlinks_reject_live_reads() {
    let (d, o) = owner();
    let orphan = prepare(&o);
    let p = prepare(&o);
    publish(&o, &p).unwrap();
    std::fs::write(
        orphan.build_path.join("checkpoint/manifest.json"),
        b"bad orphan",
    )
    .unwrap();
    recover(&o).unwrap();
    let saved = d.path().join("saved-build");
    std::fs::rename(&p.build_path, &saved).unwrap();
    std::os::unix::fs::symlink(&saved, &p.build_path).unwrap();
    assert!(o.read_bytes("index/1/root").is_err());
    assert!(recover(&o).is_err());
    std::fs::remove_file(&p.build_path).unwrap();
    std::fs::rename(saved, &p.build_path).unwrap();
    recover(&o).unwrap();
    drop(orphan);
    drop(p);
    drop(o);
    let o = LocalRoot::open(d.path()).unwrap();
    recover(&o).unwrap();
}

#[test]
fn immutable_and_directory_collisions_reject_in_both_directions() {
    let (d, o) = owner();
    let p = prepare(&o);
    publish(&o, &p).unwrap();
    commit(&o).unwrap();
    let before = journal(d.path());
    for key in [
        "commits/1",
        "commits",
        "commits/1/child",
        "ns/head",
        "ns",
        "ns/head/child",
        "index/1/root/child",
        "index/1",
    ] {
        let Ok(bad) = try_prepare_keys(&o, vec![key.into()]) else {
            // A build cannot contain its own pinned head or its ancestors.
            assert!(key.starts_with("ns"));
            continue;
        };
        assert!(publish(&o, &bad).is_err(), "{key}");
        // Use a permissive publication validator so structural checks are tested.
        struct Structural;
        impl AcceptanceValidator for Structural {
            fn validate(&self, _: &AcceptanceView<'_>) -> Result<()> {
                Ok(())
            }
            fn validate_index_publication(
                &self,
                _: &AcceptanceView<'_>,
                _: &Checkpoint,
            ) -> Result<()> {
                Ok(())
            }
        }
        assert!(
            o.publish_index(
                &bad,
                |_| Ok(vec![2, 2]),
                &Structural,
                |_, _| panic!("installed")
            )
            .is_err(),
            "{key}"
        );
    }
    for key in ["index/1/root", "index/1", "index/1/root/child"] {
        let t = Transition {
            ledger: o.ledger().into(),
            generation: o.generation().into(),
            head_key: "ns/head".into(),
            expected_head: Some(vec![2, 1]),
            resulting_head: vec![3, 1],
            objects: vec![Object {
                key: key.into(),
                bytes: b"overwrite".to_vec(),
            }],
            index_publication: None,
        };
        assert!(
            o.accept_with(&t, &Legacy, |_, _| panic!("installed"))
                .is_err(),
            "{key}"
        );
    }
    assert_eq!(journal(d.path()), before);
}

#[test]
fn empty_journal_checkpoint_prefix_can_authorize_publication() {
    let d = tempfile::tempdir().unwrap();
    let key = "commits/base";
    let o = LocalRoot::bootstrap(
        d.path(),
        "pub:main",
        "g1",
        CheckpointSpec {
            head: Object {
                key: "ns/head".into(),
                bytes: vec![1, 0],
            },
            objects: vec![CheckpointEntry {
                key: key.into(),
                length: key.len() as u64,
                sha256: crate::local_journal::digest(key.as_bytes()),
            }],
        },
        |e| Ok(Cursor::new(e.key.as_bytes().to_vec())),
        |c| Validate.validate_checkpoint(c),
    )
    .unwrap();
    let p = prepare(&o);
    publish(&o, &p).unwrap();
    drop(p);
    drop(o);
    for _ in 0..2 {
        erase(d.path());
        let o = LocalRoot::open(d.path()).unwrap();
        recover(&o).unwrap();
        assert_eq!(o.accepted_head().unwrap(), Some(vec![1, 1]));
    }
}

#[test]
fn publication_flush_error_reconciles_both_durable_outcomes_without_installing() {
    for persists in [false, true] {
        let (d, o) = owner();
        let p = prepare(&o);
        let before = journal(d.path());
        let index = p.verified_checkpoint(&o).unwrap();
        let receipt = publish(&o, &p).unwrap();
        let mut transition = None;
        o.recover_with_indexes(|rs, _, _, _| {
            transition = Some(rs.last().unwrap().transition.clone());
            Ok(())
        })
        .unwrap();
        let t = transition.unwrap();
        let io = FaultIo::from_image(before);
        let (journal, rs) = Journal::open(io.clone()).unwrap();
        let acknowledged = rs[0].receipt.clone();
        let mut state = Coordinator::restored(journal, &rs, o.ledger(), o.generation()).unwrap();
        // Restore only the pre-publication materialization for the model writer.
        std::fs::write(
            d.path().join(JOURNAL_DIR).join(DATA).join("ns/head"),
            [1, 0],
        )
        .unwrap();
        io.fail_next_flush(persists);
        assert!(matches!(
            state.accept_index(
                &t,
                &Validate,
                &mut FileTarget {
                    root: &d.path().join(JOURNAL_DIR).join(DATA)
                },
                |_, _| panic!("failed flush installed"),
                Some(index.clone())
            ),
            Err(Error::AcceptanceUnresolved { durable: None, .. })
        ));
        assert!(state.poisoned);
        for _ in 0..2 {
            let (journal, rs) = Journal::open(io.crash()).unwrap();
            assert_eq!(rs[0].receipt, acknowledged);
            let indexes = if persists {
                vec![index.clone()]
            } else {
                vec![]
            };
            let restored = Coordinator::restored_with_indexes(
                journal,
                &rs,
                o.ledger(),
                o.generation(),
                None,
                &indexes,
            )
            .unwrap();
            Validate
                .validate_recovered_with_indexes(&rs, None, &indexes)
                .unwrap();
            assert_eq!(
                restored.frontier().unwrap().head(),
                Some([1, u8::from(persists)].as_slice())
            );
            if persists {
                assert_eq!(rs.last().unwrap().receipt, receipt);
            }
        }
    }
}

#[test]
fn correctly_hashed_manifest_with_unknown_input_prefix_cannot_recover() {
    let (d, o) = owner();
    let build_id = "f".repeat(32);
    let build = d.path().join(JOURNAL_DIR).join(BUILDS).join(&build_id);
    std::fs::create_dir_all(&build).unwrap();
    let input = Object {
        key: "ns/head".into(),
        bytes: vec![1, 0],
    };
    let (_, manifest) = Checkpoint::create(
        &build,
        checkpoint::Binding {
            identity: o.manifest.identity,
            ledger: o.ledger(),
            generation: o.generation(),
            index_build: Some([0; 32]),
        },
        CheckpointSpec {
            head: input.clone(),
            objects: vec![CheckpointEntry {
                key: "index/fake".into(),
                length: 1,
                sha256: crate::local_journal::digest(b"x"),
            }],
        },
        o._directory_lock.clone(),
        |_| Ok(Cursor::new(b"x")),
        &mut |_| Ok(()),
    )
    .unwrap();
    let t = Transition {
        ledger: o.ledger().into(),
        generation: o.generation().into(),
        head_key: "ns/head".into(),
        expected_head: Some(vec![1, 0]),
        resulting_head: vec![1, 1],
        objects: vec![],
        index_publication: Some(IndexPublication {
            build: build_id,
            manifest,
            input_prefix: [0; 32],
            input_head: input,
        }),
    };
    // Intentionally bypass acceptance to construct a hash-valid malicious image.
    o.coordinator
        .lock()
        .journal
        .as_mut()
        .unwrap()
        .append_and_sync(&t)
        .unwrap();
    assert!(recover(&o).is_err());
    assert!(matches!(o.accepted_head(), Err(Error::Poisoned)));
    drop(o);
    assert!(LocalRoot::open(d.path()).is_err());
}

#[test]
fn every_publication_frame_cut_preserves_prior_ack_or_fails_closed() {
    let (d, o) = owner();
    let p = prepare(&o);
    let before = journal(d.path());
    let index = p.verified_checkpoint(&o).unwrap();
    let acknowledged = publish(&o, &p).unwrap();
    let full = journal(d.path());
    for cut in before.len()..=full.len() {
        match Journal::open(FaultIo::from_image(full[..cut].to_vec())) {
            Ok((journal, rs)) => {
                assert_eq!(rs.len(), if cut == full.len() { 2 } else { 1 });
                let indexes = if rs.len() == 2 {
                    vec![index.clone()]
                } else {
                    vec![]
                };
                let restored = Coordinator::restored_with_indexes(
                    journal,
                    &rs,
                    o.ledger(),
                    o.generation(),
                    None,
                    &indexes,
                )
                .unwrap();
                Validate
                    .validate_recovered_with_indexes(&rs, None, &indexes)
                    .unwrap();
                if cut == full.len() {
                    assert_eq!(restored.last, Some(acknowledged.clone()));
                }
            }
            Err(_) => assert!(cut < full.len()),
        }
    }
}
