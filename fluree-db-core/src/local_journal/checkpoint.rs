//! Immutable bootstrap prerequisites, kept outside the bounded journal.
use super::root::{checked_path, data_key};
use super::{digest, Error, Object, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const MAX_CHECKPOINT_MANIFEST_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_CHECKPOINT_OBJECTS: usize = 100_000;
pub const MAX_CHECKPOINT_OBJECT_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_TOTAL_BYTES: u64 = 1024 * 1024 * 1024 * 1024;
const MAX_HEAD_BYTES: usize = 64 * 1024;
const BUFFER_BYTES: usize = 64 * 1024;
pub(super) const DIRECTORY: &str = "checkpoint";
pub(super) const MANIFEST: &str = "manifest.json";
const OBJECTS: &str = "objects";

/// An exact immutable prerequisite. The trusted embedding must also verify its
/// content identity and dependency semantics; a SHA-256 inventory is not that proof.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointEntry {
    pub key: String,
    pub length: u64,
    pub sha256: [u8; 32],
}

/// A pinned baseline for one ledger. Object entries must be strictly key-sorted.
/// Includes every required immutable object, never an ordinary mutable pathname.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointSpec {
    pub head: Object,
    pub objects: Vec<CheckpointEntry>,
}

pub(super) struct Binding<'a> {
    pub identity: [u8; 16],
    pub ledger: &'a str,
    pub generation: &'a str,
    pub index_build: Option<[u8; 32]>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    version: u32,
    identity: [u8; 16],
    ledger: String,
    generation: String,
    baseline: CheckpointSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    index_build: Option<[u8; 32]>,
}

/// Verified, read-only prerequisite store for a trusted database embedding. No
/// write, raw-path, mmap or ordinary storage capability is exposed. A retained
/// handle keeps the exclusive root lease alive, even after LocalRoot is dropped.
/// Reads recheck exact length/hash. Holding this does NOT assert coordinator health;
/// the embedding must retain its operation gate for all queries using cached data.
pub struct Checkpoint {
    path: PathBuf,
    descriptor: Descriptor,
    entries: BTreeMap<String, usize>,
    digest: [u8; 32],
    _directory_lock: Arc<File>,
}

impl Checkpoint {
    /// Exact manifest identity, including the unique root and baseline binding.
    /// Trusted embeddings may bind cached semantic validation to this digest.
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn head(&self) -> &Object {
        &self.descriptor.baseline.head
    }
    pub fn ledger(&self) -> &str {
        &self.descriptor.ledger
    }
    pub fn generation(&self) -> &str {
        &self.descriptor.generation
    }
    pub fn entries(&self) -> &[CheckpointEntry] {
        &self.descriptor.baseline.objects
    }
    pub(super) fn entry(&self, key: &str) -> Option<&CheckpointEntry> {
        self.entries.get(key).map(|i| &self.entries()[*i])
    }
    pub(super) fn has_descendant(&self, key: &str) -> bool {
        let prefix = format!("{key}/");
        self.entries
            .range(prefix.clone()..)
            .next()
            .is_some_and(|(k, _)| k.starts_with(&prefix))
    }
    /// Read a single inventory object, verifying it again before returning bytes.
    /// Unknown keys return None; missing/corrupt listed objects are errors.
    pub fn read(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let Some(entry) = self.entry(key) else {
            return Ok(None);
        };
        let mut file = self.open_object(entry)?;
        let mut bytes = Vec::new();
        (&mut file).take(entry.length + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 != entry.length || digest(&bytes) != entry.sha256 {
            return Err(Error::Invalid("checkpoint object length/checksum"));
        }
        Ok(Some(bytes))
    }
    fn open_object(&self, entry: &CheckpointEntry) -> Result<File> {
        // Recheck the managed directory as well as all relative key components.
        let objects = checked_path(&self.path, OBJECTS, false)?;
        let file = File::open(checked_path(&objects, &entry.key, false)?)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() != entry.length {
            return Err(Error::Invalid("checkpoint object type/length"));
        }
        Ok(file)
    }
    fn verify_objects(&self) -> Result<()> {
        for entry in self.entries() {
            let mut file = self.open_object(entry)?;
            verify_stream(&mut file, entry, |_: &[u8]| Ok(()))?;
        }
        Ok(())
    }
    pub(super) fn create<R: Read>(
        control: &Path,
        binding: Binding<'_>,
        spec: CheckpointSpec,
        lease: Arc<File>,
        mut source: impl FnMut(&CheckpointEntry) -> Result<R>,
        step: &mut impl FnMut(&'static str) -> Result<()>,
    ) -> Result<(Arc<Self>, [u8; 32])> {
        validate_spec(&spec)?;
        let descriptor = Descriptor {
            version: 1,
            identity: binding.identity,
            ledger: binding.ledger.into(),
            generation: binding.generation.into(),
            baseline: spec,
            index_build: binding.index_build,
        };
        // Cap encoding too: no unbounded serialized inventory allocation.
        let mut encoded = BoundedManifest(Vec::new());
        serde_json::to_writer(&mut encoded, &descriptor).map_err(|_| Error::Capacity)?;
        let encoded = encoded.0;
        let path = control.join(DIRECTORY);
        std::fs::create_dir(&path)?;
        step("checkpoint directory created")?;
        let objects = path.join(OBJECTS);
        std::fs::create_dir(&objects)?;
        let mut directories = BTreeSet::from([objects.clone(), path.clone()]);
        for entry in &descriptor.baseline.objects {
            let destination = checked_path(&objects, &entry.key, true)?;
            for parent in destination.parent().unwrap().ancestors() {
                if !parent.starts_with(&path) {
                    break;
                }
                directories.insert(parent.to_path_buf());
            }
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(destination)?;
            step("object created")?;
            verify_stream(&mut source(entry)?, entry, |bytes| {
                file.write_all(bytes)?;
                step("object written")
            })?;
            file.sync_all()?;
            step("object synced")?;
        }
        // All descendant entries must be durable before publishing the descriptor.
        let mut directories: Vec<_> = directories.into_iter().collect();
        directories.sort_by_key(|p| std::cmp::Reverse(p.components().count()));
        for directory in directories {
            File::open(directory)?.sync_all()?;
            step("checkpoint directory synced")?;
        }
        super::root::atomic_write_steps(&path.join(MANIFEST), &encoded, step)?;
        File::open(control)?.sync_all()?;
        step("checkpoint entry synced")?;
        let entries = inventory(&descriptor);
        Ok((
            Arc::new(Self {
                path,
                descriptor,
                entries,
                digest: digest(&encoded),
                _directory_lock: lease,
            }),
            digest(&encoded),
        ))
    }

    pub(super) fn open(
        control: &Path,
        binding: Binding<'_>,
        hash: [u8; 32],
        lease: Arc<File>,
    ) -> Result<Arc<Self>> {
        let path = checked_path(control, DIRECTORY, false)?;
        let file = File::open(checked_path(&path, MANIFEST, false)?)?;
        let mut bytes = Vec::new();
        file.take(MAX_CHECKPOINT_MANIFEST_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_CHECKPOINT_MANIFEST_BYTES || digest(&bytes) != hash {
            return Err(Error::Invalid("checkpoint manifest size/checksum"));
        }
        let descriptor: Descriptor = serde_json::from_slice(&bytes)
            .map_err(|_| Error::Invalid("checkpoint manifest encoding"))?;
        if descriptor.version != 1
            || descriptor.identity != binding.identity
            || descriptor.ledger != binding.ledger
            || descriptor.generation != binding.generation
            || descriptor.index_build != binding.index_build
        {
            return Err(Error::Invalid("checkpoint generation binding"));
        }
        validate_spec(&descriptor.baseline)?;
        let checkpoint = Arc::new(Self {
            path,
            entries: inventory(&descriptor),
            digest: hash,
            descriptor,
            _directory_lock: lease,
        });
        checkpoint.verify_objects()?;
        Ok(checkpoint)
    }
}

fn inventory(descriptor: &Descriptor) -> BTreeMap<String, usize> {
    descriptor
        .baseline
        .objects
        .iter()
        .enumerate()
        .map(|(i, entry)| (entry.key.clone(), i))
        .collect()
}

pub(super) fn validate_spec(spec: &CheckpointSpec) -> Result<()> {
    if spec.head.bytes.is_empty()
        || spec.head.bytes.len() > MAX_HEAD_BYTES
        || spec.objects.is_empty()
        || spec.objects.len() > MAX_CHECKPOINT_OBJECTS
    {
        return Err(Error::Invalid("checkpoint head/inventory bounds"));
    }
    let mut keys = BTreeSet::new();
    let mut total = 0u64;
    let mut previous: Option<&str> = None;
    for key in
        std::iter::once(spec.head.key.as_str()).chain(spec.objects.iter().map(|e| e.key.as_str()))
    {
        super::validate_key(key)?;
        data_key(key)?;
        if key.len() > 1024 || !keys.insert(key) {
            return Err(Error::Invalid("checkpoint key bounds/duplicate"));
        }
    }
    for key in &keys {
        for (i, _) in key.match_indices('/') {
            if keys.contains(&key[..i]) {
                return Err(Error::Invalid("checkpoint file/directory collision"));
            }
        }
    }
    for entry in &spec.objects {
        if previous.is_some_and(|key| key >= entry.key.as_str())
            || entry.length > MAX_CHECKPOINT_OBJECT_BYTES
        {
            return Err(Error::Invalid("checkpoint inventory order/object bounds"));
        }
        total = total.checked_add(entry.length).ok_or(Error::Capacity)?;
        previous = Some(&entry.key);
    }
    if total > MAX_TOTAL_BYTES {
        return Err(Error::Capacity);
    }
    Ok(())
}

/// Copy/hash at most one fixed-size buffer; reject truncated AND extra source bytes.
fn verify_stream(
    source: &mut impl Read,
    entry: &CheckpointEntry,
    mut consume: impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let mut buffer = [0u8; BUFFER_BYTES];
    let mut remaining = entry.length;
    let mut hash = Sha256::new();
    while remaining > 0 {
        let n = remaining.min(BUFFER_BYTES as u64) as usize;
        source.read_exact(&mut buffer[..n])?;
        hash.update(&buffer[..n]);
        consume(&buffer[..n])?;
        remaining -= n as u64;
    }
    let mut extra = Vec::new();
    if source.take(1).read_to_end(&mut extra)? != 0
        || <[u8; 32]>::from(hash.finalize()) != entry.sha256
    {
        return Err(Error::Invalid("checkpoint object length/checksum"));
    }
    Ok(())
}

struct BoundedManifest(Vec<u8>);
impl Write for BoundedManifest {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > MAX_CHECKPOINT_MANIFEST_BYTES - self.0.len() {
            return Err(std::io::ErrorKind::OutOfMemory.into());
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
