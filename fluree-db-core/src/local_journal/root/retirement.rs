//! Offline generation switch. No new index is required or adopted.
use super::*;

impl LocalRoot {
    /// Replace the retained baseline and reset the journal under exclusive offline
    /// ownership. All database/index/cache handles must be closed first. The trusted
    /// embedding must validate the old accepted state and the new complete dependency
    /// closure; the new head MUST equal the recovered accepted head exactly.
    ///
    /// The prepare callback reads only journal/checkpoint bytes, never materialized
    /// caches. Copy and validate before switching one durable manifest. Retire old
    /// inputs only after that switch is synced. A failure may leave either generation
    /// selected, with the same accepted head; reopen to reconcile, never retransact.
    /// Call on a blocking thread. This is maintenance, not an online transaction hook.
    pub fn checkpoint_offline<R: Read, F: FnMut(&CheckpointEntry) -> Result<R>>(
        root: &Path,
        prepare: impl FnOnce(&[Record], Option<Arc<Checkpoint>>, Object) -> Result<(CheckpointSpec, F)>,
        validate: impl FnOnce(&Checkpoint) -> Result<()>,
    ) -> Result<()> {
        Self::checkpoint_offline_steps(root, prepare, validate, &mut |_| Ok(()))
    }

    fn checkpoint_offline_steps<R: Read, F: FnMut(&CheckpointEntry) -> Result<R>>(
        root: &Path,
        prepare: impl FnOnce(&[Record], Option<Arc<Checkpoint>>, Object) -> Result<(CheckpointSpec, F)>,
        validate: impl FnOnce(&Checkpoint) -> Result<()>,
        step: &mut impl FnMut(&'static str) -> Result<()>,
    ) -> Result<()> {
        let owner = Self::open(root)?;
        let mut owner = Arc::try_unwrap(owner)
            .map_err(|_| Error::Invalid("checkpoint requires all root handles closed"))?;
        let control = owner.path.join(JOURNAL_DIR);
        let old = active_control(&control, &owner.manifest)?;
        let state = owner.coordinator.get_mut();
        let (key, bytes) = state
            .head
            .clone()
            .ok_or(Error::Invalid("checkpoint requires an accepted head"))?;
        drop(state.journal.take());
        let (old_journal, records) = open_journal(&old, &journal_identity(&owner.manifest))?;
        let head = Object { key, bytes };
        let (spec, source) = prepare(&records, owner.checkpoint.clone(), head.clone())?;
        if spec.head != head {
            return Err(Error::Invalid("checkpoint cannot change accepted head"));
        }
        checkpoint::validate_spec(&spec)?;
        retire(&control, owner.manifest.epoch, step)?;
        let epochs = checked_path(&control, "epochs", true)?;
        if !epochs.exists() {
            std::fs::create_dir(&epochs)?;
        }
        File::open(&control)?.sync_all()?;
        step("epochs directory synced")?;
        let epoch: [u8; 16] = rand::random();
        let next = epochs.join(hex::encode(epoch));
        std::fs::create_dir(&next)?;
        File::open(&epochs)?.sync_all()?;
        step("epoch directory synced")?;
        let manifest = Manifest {
            version: 3,
            identity: rand::random(),
            ledger: owner.manifest.ledger.clone(),
            generation: owner.manifest.generation.clone(),
            checkpoint: None,
            epoch: Some(epoch),
        };
        let (checkpoint, hash) = Checkpoint::create(
            &next,
            checkpoint::Binding {
                identity: manifest.identity,
                ledger: &manifest.ledger,
                generation: &manifest.generation,
            },
            spec,
            owner._directory_lock.clone(),
            source,
            step,
        )?;
        validate(&checkpoint)?;
        step("replacement validated")?;
        let manifest = Manifest {
            checkpoint: Some(hash),
            ..manifest
        };
        let journal = Journal::create(
            FileIo::create_new(&next.join(JOURNAL))?,
            journal_identity(&manifest),
        )?;
        step("replacement journal synced")?;
        std::fs::create_dir(next.join(DATA))?;
        replay_from(
            &[],
            &manifest,
            Some(&checkpoint),
            &mut FileTarget {
                root: &next.join(DATA),
            },
        )?;
        File::open(&next)?.sync_all()?;
        step("replacement directory synced")?;
        let bytes =
            serde_json::to_vec(&manifest).map_err(|_| Error::Invalid("manifest encoding"))?;
        atomic_write_steps(&control.join(FORMAT), &bytes, step)?;
        step("replacement selected durably")?;
        // No live database handle exists. Drop every old file/checkpoint handle
        // before unlinking; the stable root directory lease remains held.
        drop(old_journal);
        drop(journal);
        drop(checkpoint);
        drop(owner.checkpoint.take());
        owner
            .coordinator
            .get_mut()
            .clear_checkpoint_for_retirement();
        retire(&control, Some(epoch), step)?;
        Ok(())
    }
}

fn retire(
    control: &Path,
    active: Option<[u8; 16]>,
    step: &mut impl FnMut(&'static str) -> Result<()>,
) -> Result<()> {
    // Only known root-format paths and our strictly named epoch directories.
    // Never follow symlinks or remove unrelated files.
    if active.is_some() {
        for name in [JOURNAL, checkpoint::DIRECTORY, DATA] {
            remove(&control.join(name), step)?;
        }
    }
    // A failed root-manifest write can leave an unselected temporary file.
    // Its name is reserved by atomic_write_steps; never inspect/remove FORMAT.
    for entry in std::fs::read_dir(control)? {
        let entry = entry?;
        let name = entry.file_name();
        if name
            .to_str()
            .and_then(|s| s.strip_prefix(".wal-bootstrap-"))
            .is_some_and(epoch_name)
        {
            remove(&entry.path(), step)?;
        }
    }
    File::open(control)?.sync_all()?;
    let epochs = checked_path(control, "epochs", false)?;
    if !epochs.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(&epochs)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if active.is_some_and(|id| name == hex::encode(id)) {
            continue;
        }
        if epoch_name(name) {
            remove(&entry.path(), step)?;
        }
    }
    File::open(&epochs)?.sync_all()?;
    File::open(control)?.sync_all()?;
    step("retirement synced")?;
    Ok(())
}
fn epoch_name(name: &str) -> bool {
    name.len() == 32
        && name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn remove(path: &Path, step: &mut impl FnMut(&'static str) -> Result<()>) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            return Err(Error::Invalid("retirement refuses symlink"))
        }
        Ok(meta) if meta.is_dir() => std::fs::remove_dir_all(path)?,
        Ok(_) => std::fs::remove_file(path)?,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    }
    step("retired path removed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    struct Validator;
    impl AcceptanceValidator for Validator {
        fn validate(&self, _: &AcceptanceView<'_>) -> Result<()> {
            Ok(())
        }
        fn validate_checkpoint(&self, _: &Checkpoint) -> Result<()> {
            Ok(())
        }
    }
    fn txn(n: u8) -> Transition {
        Transition {
            ledger: "test:main".into(),
            generation: "g".into(),
            head_key: "ns/head".into(),
            expected_head: (n > 1).then(|| vec![n - 1]),
            resulting_head: vec![n],
            objects: vec![Object {
                key: format!("commit/{n}"),
                bytes: vec![n; 113],
            }],
        }
    }
    pub(super) fn seed(path: &Path) {
        let owner = LocalRoot::initialize(path, "test:main", "g").unwrap();
        for n in 1..=2 {
            owner
                .accept_with(&txn(n), &Validator, |_, _| Ok(()))
                .unwrap();
        }
    }
    pub(super) fn prepare(
        records: &[Record],
        checkpoint: Option<Arc<Checkpoint>>,
        head: Object,
    ) -> Result<(
        CheckpointSpec,
        impl FnMut(&CheckpointEntry) -> Result<Cursor<Vec<u8>>>,
    )> {
        let mut bytes = BTreeMap::new();
        if let Some(checkpoint) = checkpoint {
            for entry in checkpoint.entries() {
                bytes.insert(entry.key.clone(), checkpoint.read(&entry.key)?.unwrap());
            }
        }
        for record in records {
            for o in &record.transition.objects {
                bytes.insert(o.key.clone(), o.bytes.clone());
            }
        }
        let objects = bytes
            .iter()
            .map(|(key, bytes)| CheckpointEntry {
                key: key.clone(),
                length: bytes.len() as u64,
                sha256: super::super::super::digest(bytes),
            })
            .collect();
        Ok((
            CheckpointSpec { head, objects },
            move |e: &CheckpointEntry| Ok(Cursor::new(bytes[&e.key].clone())),
        ))
    }
    fn erase_caches(path: &Path) {
        let control = path.join(JOURNAL_DIR);
        let mut data = vec![control.join(DATA)];
        if let Ok(epochs) = std::fs::read_dir(control.join("epochs")) {
            data.extend(epochs.map(|e| e.unwrap().path().join(DATA)));
        }
        for dir in data {
            if dir.exists() {
                std::fs::remove_dir_all(&dir).unwrap();
                std::fs::create_dir(dir).unwrap();
            }
        }
    }
    pub(super) fn verify(path: &Path, n: u8) {
        erase_caches(path);
        let owner = LocalRoot::open(path).unwrap();
        assert_eq!(owner.accepted_head().unwrap(), Some(vec![n]));
        for t in 1..=n {
            assert_eq!(
                owner.read_bytes(&format!("commit/{t}")).unwrap(),
                vec![t; 113]
            );
        }
    }
    #[test]
    fn repeated_retirement_preserves_every_ack_and_restarts_empty_journal() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path());
        for n in 2..=8 {
            LocalRoot::checkpoint_offline(dir.path(), prepare, |_| Ok(())).unwrap();
            verify(dir.path(), n);
            let control = dir.path().join(JOURNAL_DIR);
            assert!(!control.join(JOURNAL).exists());
            assert!(!control.join(checkpoint::DIRECTORY).exists());
            assert_eq!(
                std::fs::read_dir(control.join("epochs")).unwrap().count(),
                1
            );
            let owner = LocalRoot::open(dir.path()).unwrap();
            let active = active_control(&control, &owner.manifest).unwrap();
            assert_eq!(
                std::fs::metadata(active.join(JOURNAL)).unwrap().len(),
                super::super::super::HEADER as u64
            );
            if n < 8 {
                owner
                    .accept_with(&txn(n + 1), &Validator, |_, _| Ok(()))
                    .unwrap();
            }
        }
    }
    #[test]
    fn full_journal_rejects_definitely_then_offline_checkpoint_restores_capacity() {
        let dir = tempfile::tempdir().unwrap();
        let mut owner = LocalRoot::initialize(dir.path(), "test:main", "g").unwrap();
        let mut accepted = 0u8;
        let mut total_encoded = 0;
        for _ in 0..2 {
            loop {
                let mut candidate = txn(accepted + 1);
                candidate.objects[0].bytes = vec![123; 1024 * 1024];
                let encoded = serde_json::to_vec(&candidate).unwrap().len();
                match owner.accept_with(&candidate, &Validator, |_, _| Ok(())) {
                    Ok(_) => {
                        accepted += 1;
                        total_encoded += encoded;
                    }
                    Err(Error::Capacity) => break,
                    Err(error) => panic!("unexpected acceptance failure: {error}"),
                }
            }
            let control = dir.path().join(JOURNAL_DIR);
            let active = active_control(&control, &owner.manifest).unwrap();
            assert!(
                std::fs::metadata(active.join(JOURNAL)).unwrap().len()
                    <= super::super::super::MAX_JOURNAL_BYTES
            );
            assert_eq!(owner.accepted_head().unwrap(), Some(vec![accepted]));
            drop(owner);
            LocalRoot::checkpoint_offline(dir.path(), prepare, |_| Ok(())).unwrap();
            erase_caches(dir.path());
            owner = LocalRoot::open(dir.path()).unwrap();
            assert_eq!(owner.accepted_head().unwrap(), Some(vec![accepted]));
            for n in 1..=accepted {
                assert_eq!(
                    owner.read_bytes(&format!("commit/{n}")).unwrap(),
                    vec![123; 1024 * 1024]
                );
            }
            assert_eq!(
                std::fs::read_dir(control.join("epochs")).unwrap().count(),
                1
            );
        }
        assert!(total_encoded as u64 > super::super::super::MAX_JOURNAL_BYTES);
    }

    #[test]
    fn every_checkpoint_io_cut_recovers_and_retry_removes_orphans() {
        for already_checkpointed in [false, true] {
            let setup = |path: &Path| {
                seed(path);
                if already_checkpointed {
                    LocalRoot::checkpoint_offline(path, prepare, |_| Ok(())).unwrap();
                }
            };
            let baseline = tempfile::tempdir().unwrap();
            setup(baseline.path());
            let mut steps = Vec::new();
            LocalRoot::checkpoint_offline_steps(
                baseline.path(),
                prepare,
                |_| Ok(()),
                &mut |name| {
                    steps.push(name);
                    Ok(())
                },
            )
            .unwrap();
            assert!(steps.contains(&"replacement selected durably"));
            assert!(steps.contains(&"retired path removed"));
            for cut in 0..steps.len() {
                let dir = tempfile::tempdir().unwrap();
                setup(dir.path());
                let mut at = 0;
                let result = LocalRoot::checkpoint_offline_steps(
                    dir.path(),
                    prepare,
                    |_| Ok(()),
                    &mut |_| {
                        let fail = at == cut;
                        at += 1;
                        if fail {
                            Err(Error::Invalid("injected checkpoint I/O interruption"))
                        } else {
                            Ok(())
                        }
                    },
                );
                assert!(result.is_err(), "cut {cut} {}", steps[cut]);
                verify(dir.path(), 2);
                verify(dir.path(), 2);
                LocalRoot::checkpoint_offline(dir.path(), prepare, |_| Ok(())).unwrap();
                verify(dir.path(), 2);
                assert_eq!(
                    std::fs::read_dir(dir.path().join(JOURNAL_DIR).join("epochs"))
                        .unwrap()
                        .count(),
                    1
                );
                assert!(std::fs::read_dir(dir.path().join(JOURNAL_DIR))
                    .unwrap()
                    .all(|e| !e
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".wal-bootstrap-")));
            }
        }
    }
    #[test]
    fn unsynced_manifest_switch_can_select_either_complete_generation() {
        for already_checkpointed in [false, true] {
            for keep_new in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                seed(dir.path());
                if already_checkpointed {
                    LocalRoot::checkpoint_offline(dir.path(), prepare, |_| Ok(())).unwrap();
                }
                let format = dir.path().join(JOURNAL_DIR).join(FORMAT);
                let old = std::fs::read(&format).unwrap();
                let result = LocalRoot::checkpoint_offline_steps(
                    dir.path(),
                    prepare,
                    |_| Ok(()),
                    &mut |step| {
                        let now: Manifest =
                            serde_json::from_slice(&std::fs::read(&format)?).unwrap();
                        if step == "manifest published" && serde_json::to_vec(&now).unwrap() != old
                        {
                            Err(Error::Invalid("lost directory flush"))
                        } else {
                            Ok(())
                        }
                    },
                );
                assert!(result.is_err());
                if !keep_new {
                    std::fs::write(&format, &old).unwrap();
                }
                verify(dir.path(), 2);
            }
        }
    }
    #[test]
    fn live_owners_and_changed_heads_are_refused_before_retirement() {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path());
        let owner = LocalRoot::open(dir.path()).unwrap();
        assert!(LocalRoot::checkpoint_offline(dir.path(), prepare, |_| Ok(())).is_err());
        assert_eq!(owner.accepted_head().unwrap(), Some(vec![2]));
        drop(owner);
        assert!(LocalRoot::checkpoint_offline(
            dir.path(),
            |r, c, mut h| {
                h.bytes = vec![99];
                prepare(r, c, h)
            },
            |_| Ok(())
        )
        .is_err());
        assert!(
            LocalRoot::checkpoint_offline(dir.path(), prepare, |_| Err(Error::Invalid(
                "bad semantic closure"
            )))
            .is_err()
        );
        verify(dir.path(), 2);
        LocalRoot::checkpoint_offline(dir.path(), prepare, |_| Ok(())).unwrap();
        let owner = LocalRoot::open(dir.path()).unwrap();
        let retained = owner.checkpoint.clone().unwrap();
        drop(owner);
        assert!(LocalRoot::checkpoint_offline(dir.path(), prepare, |_| Ok(())).is_err());
        drop(retained);
        verify(dir.path(), 2);
    }
}

#[cfg(test)]
mod process_tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;
    #[test]
    fn child_checkpoint_cut() {
        let Ok(root) = std::env::var("FLUREE_CHECKPOINT_CUT_ROOT") else {
            return;
        };
        let cut = std::env::var("FLUREE_CHECKPOINT_CUT_STEP").unwrap();
        LocalRoot::checkpoint_offline_steps(
            Path::new(&root),
            tests::prepare,
            |_| Ok(()),
            &mut |step| {
                let format: Manifest = serde_json::from_slice(&std::fs::read(
                    Path::new(&root).join(JOURNAL_DIR).join(FORMAT),
                )?)
                .unwrap();
                if step == cut && (step != "manifest published" || format.version == 3) {
                    // Kill this child without running destructors; its parent owns the oracle.
                    unsafe {
                        libc::raise(libc::SIGKILL);
                    }
                    panic!("SIGKILL unexpectedly returned");
                }
                Ok(())
            },
        )
        .unwrap();
        panic!("checkpoint cut was never reached: {cut}");
    }
    #[test]
    fn killed_checkpoint_process_recovers_exact_head_and_releases_lease() {
        for cut in [
            "epoch directory synced",
            "object written",
            "object synced",
            "replacement journal synced",
            "manifest published",
            "replacement selected durably",
            "retired path removed",
            "retirement synced",
        ] {
            let dir = tempfile::tempdir().unwrap();
            tests::seed(dir.path());
            let result = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "local_journal::root::retirement::process_tests::child_checkpoint_cut",
                    "--nocapture",
                ])
                .env("FLUREE_CHECKPOINT_CUT_ROOT", dir.path())
                .env("FLUREE_CHECKPOINT_CUT_STEP", cut)
                .output()
                .unwrap();
            assert_eq!(
                result.status.signal(),
                Some(libc::SIGKILL),
                "{cut}: {}",
                String::from_utf8_lossy(&result.stdout)
            );
            tests::verify(dir.path(), 2);
            tests::verify(dir.path(), 2);
            LocalRoot::checkpoint_offline(dir.path(), tests::prepare, |_| Ok(())).unwrap();
            tests::verify(dir.path(), 2);
        }
    }
}
