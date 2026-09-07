//! Exclusive root with serialized journal acceptance and recovery.
use super::acceptance::{AcceptanceTarget, Coordinator};
use super::{
    replay_chain, AcceptanceValidator, AcceptanceView, Error, FileIo, Journal, Object, Receipt,
    Record, ReplayTarget, Result, Transition,
};
use crate::root_access::{reject_journal_ancestor, JOURNAL_DIR};
use fs2::FileExt;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, LazyLock, Weak};

static OWNERS: LazyLock<Mutex<BTreeMap<PathBuf, Weak<LocalRoot>>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));
const FORMAT: &str = "format.json";
const JOURNAL: &str = "journal";
const DATA: &str = "data";
const MAX_MANIFEST: u64 = 4096;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    identity: [u8; 16],
    ledger: String,
    generation: String,
}

/// A recovered local root with serialized acceptance. All opens for a canonical root in this
/// process share this owner. A competing process (including ordinary file mode)
/// is refused. The final Arc holds both the root and journal locks through reads.
///
/// This is NOT a Fluree transaction backend. Acceptance requires a trusted semantic
/// validator and state-installation hook. There is no general StorageWrite, delete,
/// indexing, raw local-path, encryption, or custom-backend adapter.
/// Call these blocking open methods on a blocking thread in async applications.
/// Only local Unix filesystems with working advisory directory locks are in scope.
/// Never rename/replace roots, ancestors, journal files, or lock inodes while open;
/// old binaries and arbitrary direct filesystem access cannot be fenced by this API.
pub struct LocalRoot {
    path: PathBuf,
    manifest: Manifest,
    coordinator: Mutex<Coordinator<FileIo>>,
    _directory_lock: File,
}

impl LocalRoot {
    /// Initialize an EXISTING EMPTY directory. In-place conversion of populated
    /// databases is rejected. Any interrupted initialization leaves a permanent
    /// ordinary-access fence and requires inspection; no automatic deletion occurs.
    pub fn initialize(root: &Path, ledger: &str, generation: &str) -> Result<Arc<Self>> {
        if ledger.is_empty() || generation.is_empty() || ledger.len() + generation.len() > 1024 {
            return Err(Error::Invalid("invalid root ledger/generation"));
        }
        let path = std::fs::canonicalize(root)?;
        let mut registry = OWNERS.lock();
        let directory = lock_root(&path)?;
        reject_journal_ancestor(&path)?;
        if std::fs::read_dir(&path)?.next().is_some() {
            return Err(Error::Invalid(
                "journal initialization requires an empty root",
            ));
        }
        // Sync every existing ancestor, deepest first, so a caller's newly made
        // root/parents are durably reachable before installing the mode fence.
        for ancestor in path.ancestors() {
            File::open(ancestor)?.sync_all()?;
        }
        let control = path.join(JOURNAL_DIR);
        std::fs::create_dir(&control)?;
        directory.sync_all()?; // presence itself fences ordinary mode
        let manifest = Manifest {
            version: 1,
            identity: rand::random(),
            ledger: ledger.into(),
            generation: generation.into(),
        };
        let journal = Journal::create(
            FileIo::create_new(&control.join(JOURNAL))?,
            manifest.identity,
        )?;
        std::fs::create_dir(control.join(DATA))?;
        File::open(&control)?.sync_all()?;
        let encoded =
            serde_json::to_vec(&manifest).map_err(|_| Error::Invalid("manifest encoding"))?;
        atomic_write(&control.join(FORMAT), &encoded, true)?;
        let owner = Arc::new(Self {
            path: path.clone(),
            manifest,
            coordinator: Mutex::new(Coordinator::restored(journal, &[], ledger, generation)?),
            _directory_lock: directory,
        });
        registry.retain(|_, owner| owner.strong_count() > 0);
        registry.insert(path, Arc::downgrade(&owner));
        Ok(owner)
    }

    /// Acquire ownership, validate the format/journal, and replay before returning
    /// any usable reader. A failed recovery exposes no owner and retains all journal
    /// bytes for retry/inspection. Same-process opens share only a fully ready owner.
    pub fn open(root: &Path) -> Result<Arc<Self>> {
        let path = std::fs::canonicalize(root)?;
        let mut registry = OWNERS.lock();
        if let Some(owner) = registry.get(&path).and_then(Weak::upgrade) {
            if owner.coordinator.lock().poisoned {
                return Err(Error::Poisoned);
            }
            return Ok(owner);
        }
        let directory = lock_root(&path)?;
        // Nested roots must not bypass another journal owner's boundary.
        if let Some(parent) = path.parent() {
            reject_journal_ancestor(parent)?;
        }
        let control = checked_path(&path, JOURNAL_DIR, false)?;
        if !std::fs::metadata(&control)?.is_dir() {
            return Err(Error::Invalid("journal control is not a directory"));
        }
        let format_path = checked_path(&control, FORMAT, false)?;
        let mut bytes = Vec::new();
        File::open(format_path)?
            .take(MAX_MANIFEST + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_MANIFEST {
            return Err(Error::Invalid("oversized journal manifest"));
        }
        let manifest: Manifest = serde_json::from_slice(&bytes)
            .map_err(|_| Error::Invalid("missing/invalid journal manifest"))?;
        if manifest.version != 1 || manifest.ledger.is_empty() || manifest.generation.is_empty() {
            return Err(Error::Invalid("unsupported journal root format"));
        }
        let (journal, records) = open_journal(&control, &manifest.identity)?;
        let data = checked_path(&control, DATA, false)?;
        if !std::fs::metadata(&data)?.is_dir() {
            return Err(Error::Invalid("missing journal data directory"));
        }
        let coordinator =
            Coordinator::restored(journal, &records, &manifest.ledger, &manifest.generation)?;
        let mut target = FileTarget { root: &data };
        replay_chain(
            &records,
            &manifest.ledger,
            &manifest.generation,
            &mut target,
        )?;
        let owner = Arc::new(Self {
            path: path.clone(),
            manifest,
            coordinator: Mutex::new(coordinator),
            _directory_lock: directory,
        });
        registry.retain(|_, owner| owner.strong_count() > 0);
        registry.insert(path, Arc::downgrade(&owner));
        Ok(owner)
    }

    /// Serialize CAS validation, semantic validation, flush, materialization and
    /// the trusted state-installation hook. A receipt returns only after all steps
    /// succeed. Neither validator nor hook may reenter this root or publish an external response.
    /// This is an embedding seam, not a raw HTTP/Fluree transaction entry point.
    pub fn accept_with(
        &self,
        transition: &Transition,
        validator: &impl AcceptanceValidator,
        install: impl FnOnce(&AcceptanceView<'_>, &Receipt) -> Result<()>,
    ) -> Result<Receipt> {
        let mut state = self.coordinator.lock();
        let data = self.path.join(JOURNAL_DIR).join(DATA);
        state.accept(
            transition,
            validator,
            &mut FileTarget { root: &data },
            install,
        )
    }

    /// Reconcile an unknown outcome under the same root lock. Complete accepted
    /// records may recover even when the prior call returned an error. Install
    /// restored application state before making reads/acceptance available again.
    /// The hook must not reenter this root. Torn/corrupt journals still fail closed.
    pub fn recover_with(&self, install: impl FnOnce(&[Record]) -> Result<()>) -> Result<()> {
        let mut state = self.coordinator.lock();
        state.poisoned = true;
        drop(state.journal.take());
        let control = self.path.join(JOURNAL_DIR);
        let (journal, records) = open_journal(&control, &self.manifest.identity)?;
        let restored = Coordinator::restored(
            journal,
            &records,
            &self.manifest.ledger,
            &self.manifest.generation,
        )?;
        let data = control.join(DATA);
        replay_chain(
            &records,
            &self.manifest.ledger,
            &self.manifest.generation,
            &mut FileTarget { root: &data },
        )?;
        install(&records)?;
        *state = restored;
        Ok(())
    }

    pub fn ledger(&self) -> &str {
        &self.manifest.ledger
    }
    pub fn generation(&self) -> &str {
        &self.manifest.generation
    }

    /// Read after completed recovery. The returned bytes do not outlive an mmap or
    /// a raw-path capability; ordinary storage handles remain fenced even now.
    pub fn read_bytes(&self, key: &str) -> Result<Vec<u8>> {
        let state = self.coordinator.lock();
        if state.poisoned {
            return Err(Error::Poisoned);
        }
        data_key(key)?;
        Ok(std::fs::read(checked_path(
            &self.path.join(JOURNAL_DIR).join(DATA),
            key,
            false,
        )?)?)
    }
}

fn open_journal(control: &Path, identity: &[u8; 16]) -> Result<(Journal<FileIo>, Vec<Record>)> {
    let path = checked_path(control, JOURNAL, false)?;
    let mut file = File::open(&path)?;
    let mut header = [0; super::HEADER];
    file.read_exact(&mut header)?;
    if &header[8..24] != identity {
        return Err(Error::Invalid("root/journal generation mismatch"));
    }
    Journal::open(FileIo::open(&path)?)
}

fn lock_root(path: &Path) -> Result<File> {
    let directory = File::open(path)?;
    if !directory.metadata()?.is_dir() {
        return Err(Error::Invalid("root is not a directory"));
    }
    FileExt::try_lock_exclusive(&directory)?;
    Ok(directory)
}

fn data_key(key: &str) -> Result<()> {
    if key.split('/').any(|part| part == JOURNAL_DIR) {
        return Err(Error::Invalid("journal control path is reserved"));
    }
    Ok(())
}

/// Refuse traversal and symlinks, including final-component symlinks. The owner
/// excludes cooperative writers; hostile external filesystem mutations are outside
/// this prototype's contract (there is no descriptor-relative openat sandbox).
fn checked_path(root: &Path, key: &str, create_parents: bool) -> Result<PathBuf> {
    let key = Path::new(key);
    if key.as_os_str().is_empty() || key.components().any(|c| !matches!(c, Component::Normal(_))) {
        return Err(Error::Invalid("invalid materialization key"));
    }
    let mut path = root.to_path_buf();
    let count = key.components().count();
    for (i, component) in key.components().enumerate() {
        path.push(component);
        match std::fs::symlink_metadata(&path) {
            Ok(m) if m.file_type().is_symlink() => {
                return Err(Error::Invalid("symlink in managed root"))
            }
            Ok(m) if i + 1 < count && !m.is_dir() => {
                return Err(Error::Invalid("non-directory materialization parent"))
            }
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if create_parents && i + 1 < count {
                    std::fs::create_dir(&path)?;
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(path)
}

fn atomic_write(path: &Path, bytes: &[u8], durable: bool) -> Result<()> {
    let temporary = path
        .parent()
        .unwrap()
        .join(format!(".wal-materialize-{:032x}", rand::random::<u128>()));
    // Do not unlink a pre-existing filename if exclusive creation fails.
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    let result = (|| -> Result<()> {
        file.write_all(bytes)?;
        if durable {
            file.sync_all()?;
        }
        std::fs::rename(&temporary, path)?;
        if durable {
            File::open(path.parent().unwrap())?.sync_all()?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

struct FileTarget<'a> {
    root: &'a Path,
}

impl AcceptanceTarget for FileTarget<'_> {
    fn preflight(&mut self, transition: &Transition) -> Result<()> {
        data_key(&transition.head_key)?;
        checked_path(self.root, &transition.head_key, false)?;
        for object in &transition.objects {
            data_key(&object.key)?;
            let path = checked_path(self.root, &object.key, false)?;
            match std::fs::read(path) {
                Ok(bytes) if bytes == object.bytes => {}
                Ok(_) => {
                    return Err(Error::Invalid(
                        "immutable materialized bytes differ before acceptance",
                    ))
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
impl ReplayTarget for FileTarget<'_> {
    fn read_head(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        data_key(key)?;
        match std::fs::read(checked_path(self.root, key, false)?) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    fn put_immutable(&mut self, object: &Object) -> Result<()> {
        data_key(&object.key)?;
        let path = checked_path(self.root, &object.key, true)?;
        match std::fs::read(&path) {
            Ok(bytes) if bytes == object.bytes => return Ok(()),
            Ok(_) => return Err(Error::Invalid("immutable materialized bytes differ")),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        atomic_write(&path, &object.bytes, false)
    }
    fn publish_head(&mut self, key: &str, expected: Option<&[u8]>, bytes: &[u8]) -> Result<()> {
        if self.read_head(key)?.as_deref() != expected {
            return Err(Error::Invalid("materialized head changed during recovery"));
        }
        atomic_write(&checked_path(self.root, key, true)?, bytes, false)
    }
}
