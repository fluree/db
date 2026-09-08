//! Durable but unpublished index prerequisites. Never a database visibility path.
use super::*;
use crate::local_journal::AcceptanceFrontier;

const BUILDS: &str = "index-builds";

/// Exact accepted input for an index build. Obtain this while the embedding holds
/// its database operation gate and checks that its LedgerState matches this head.
/// The pin retains root ownership but does not hold the acceptance mutex: newer
/// commits may proceed during construction. This value is not caller-constructible.
pub struct IndexBuildPin {
    owner: Arc<LocalRoot>,
    frontier: AcceptanceFrontier,
    head: Object,
}

/// Synced, validated staging files bound to a pinned accepted input and root.
/// This is NOT an accepted index, a checkpoint publication or a semantic proof
/// that can bypass publication validation. The active head and journal are untouched.
/// There is deliberately no reopen-by-directory or automatic orphan promotion API.
/// Losing this handle requires a fresh build; staged files remain for inspection.
pub struct PreparedIndex {
    pin: IndexBuildPin,
    checkpoint: Arc<Checkpoint>,
    build_path: PathBuf,
}

impl LocalRoot {
    /// Capture the exact healthy accepted head for an embedding's private index
    /// build. Empty roots must accept a baseline first. Blocking like other root
    /// methods; the database layer must coordinate this with its LedgerState gate.
    pub fn pin_index_build(self: &Arc<Self>) -> Result<IndexBuildPin> {
        let state = self.coordinator.lock();
        let frontier = state.frontier()?;
        let (key, bytes) = state
            .head
            .as_ref()
            .ok_or(Error::Invalid("index build requires an accepted head"))?;
        Ok(IndexBuildPin {
            owner: self.clone(),
            frontier,
            head: Object {
                key: key.clone(),
                bytes: bytes.clone(),
            },
        })
    }
}

impl IndexBuildPin {
    pub fn head(&self) -> &Object {
        &self.head
    }
    pub fn frontier(&self) -> &AcceptanceFrontier {
        &self.frontier
    }

    /// Copy a strictly sorted, bounded inventory using the checkpoint format's
    /// 64 KiB streaming buffer, hash/length checks and file/directory syncs.
    /// The trusted callback must verify the intended index's CIDs, complete
    /// dependency closure and built-through input against head(). A hash inventory
    /// alone does not establish index semantics. Publication must validate again.
    ///
    /// Failure affects only this private build; it never poisons a healthy writer
    /// or changes accepted data. No destructive cleanup or retry-in-place occurs.
    pub fn prepare<R: Read>(
        self,
        objects: Vec<CheckpointEntry>,
        source: impl FnMut(&CheckpointEntry) -> Result<R>,
        validate: impl FnOnce(&Checkpoint) -> Result<()>,
    ) -> Result<PreparedIndex> {
        self.prepare_steps(objects, source, validate, &mut |_| Ok(()))
    }

    fn prepare_steps<R: Read>(
        self,
        objects: Vec<CheckpointEntry>,
        source: impl FnMut(&CheckpointEntry) -> Result<R>,
        validate: impl FnOnce(&Checkpoint) -> Result<()>,
        step: &mut impl FnMut(&'static str) -> Result<()>,
    ) -> Result<PreparedIndex> {
        let spec = CheckpointSpec {
            head: self.head.clone(),
            objects,
        };
        checkpoint::validate_spec(&spec)?;
        self.owner.accepted_frontier()?;
        let control = checked_path(&self.owner.path, JOURNAL_DIR, false)?;
        let builds = checked_path(&control, BUILDS, false)?;
        match std::fs::create_dir(&builds) {
            Ok(()) => step("index builds directory created")?,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists && builds.is_dir() => {}
            Err(e) => return Err(e.into()),
        }
        // Also repairs reachability left by an earlier interrupted preparation.
        File::open(&control)?.sync_all()?;
        step("index builds entry synced")?;
        let build_path = builds.join(format!("{:032x}", rand::random::<u128>()));
        std::fs::create_dir(&build_path)?;
        step("private index build created")?;
        File::open(&builds)?.sync_all()?;
        step("private index build entry synced")?;
        let (checkpoint, _) = Checkpoint::create(
            &build_path,
            checkpoint::Binding {
                identity: self.owner.manifest.identity,
                ledger: self.owner.ledger(),
                generation: self.owner.generation(),
                index_build: Some(self.frontier.prefix_digest()),
            },
            spec,
            self.owner._directory_lock.clone(),
            source,
            step,
        )?;
        validate(&checkpoint)?;
        step("index build semantics validated")?;
        // Acceptance may have advanced while copying, which is allowed. An
        // unresolved writer must recover before this handle can be used.
        self.owner.accepted_frontier()?;
        Ok(PreparedIndex {
            pin: self,
            checkpoint,
            build_path,
        })
    }
}

impl PreparedIndex {
    pub fn head(&self) -> &Object {
        &self.pin.head
    }
    pub fn frontier(&self) -> &AcceptanceFrontier {
        &self.pin.frontier
    }
    pub fn manifest_digest(&self) -> [u8; 32] {
        self.checkpoint.digest()
    }
    pub fn entries(&self) -> &[CheckpointEntry] {
        self.checkpoint.entries()
    }

    fn validate_location(&self) -> Result<()> {
        let control = checked_path(&self.pin.owner.path, JOURNAL_DIR, false)?;
        let builds = checked_path(&control, BUILDS, false)?;
        let name = self
            .build_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or(Error::Invalid("invalid private index build path"))?;
        let build = checked_path(&builds, name, false)?;
        checked_path(&build, checkpoint::DIRECTORY, false)?;
        Ok(())
    }

    /// Read only listed private build bytes, rechecking their length/hash and
    /// owner health. This does not expose them through the accepted database.
    pub fn read(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.pin.owner.accepted_frontier()?;
        self.validate_location()?;
        self.checkpoint.read(key)
    }

    /// Recheck physical prerequisites and root ownership. Newer accepted commits
    /// are allowed; the input frontier remains fixed. This grants no publication
    /// authority: publication must revalidate under its own acceptance gate.
    pub fn verify_for(&self, owner: &Arc<LocalRoot>) -> Result<()> {
        if !Arc::ptr_eq(owner, &self.pin.owner) {
            return Err(Error::Invalid("foreign prepared index owner"));
        }
        owner.accepted_frontier()?;
        self.validate_location()?;
        Checkpoint::open(
            &self.build_path,
            checkpoint::Binding {
                identity: owner.manifest.identity,
                ledger: owner.ledger(),
                generation: owner.generation(),
                index_build: Some(self.pin.frontier.prefix_digest()),
            },
            self.checkpoint.digest(),
            owner._directory_lock.clone(),
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
