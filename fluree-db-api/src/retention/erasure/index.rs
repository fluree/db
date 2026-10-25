//! Which commits are erased, and the storage that reads their replacements.
//!
//! Each published erasure is an immutable map object under
//! `@retention/erasure/map/`, outside every ledger's folder and so outside
//! backup rollback and ledger drops; the index is their union. An erased
//! commit's address then reads, through [`ErasureStorage`], as its verified
//! replacement on every path: loading, replay, indexing, history, raw
//! retrieval. Nothing reads the original once its erasure is published.
//!
//! Another instance on the same storage (a query peer) publishes nothing
//! and is told nothing: it lists the map objects again on a commit read once
//! its last listing is [`REFRESH`] old, and a publication is reported only
//! after that.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use fluree_db_core::storage::{
    BlobPins, ContentAddressedWrite, ReadHint, RemoteObject, StorageMethod,
};
use fluree_db_core::{
    content_address, ContentBytes, ContentId, ContentKind, ContentWriteResult, EncryptionAdmin,
    RangeSource, Storage, StorageNamespace, StorageRead, StorageWrite,
};
use fluree_db_erasure::Verifier;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{ApiError, Result};

const DIR: &str = "@retention/erasure";

/// Map objects a publish finds before it replaces them with their union.
const COMPACT_AT: usize = 64;

/// How long a listing of the map objects stands before a commit read lists
/// them again.
pub(crate) const REFRESH: Duration = Duration::from_secs(1);

/// One redaction step of an erased commit, as published.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErasureStep {
    /// The request that authorized it.
    pub request_id: String,
    /// Original slots it removed, ascending.
    pub slots: Vec<u32>,
    /// The replacement it produced.
    pub replacement: String,
    /// SHA-256 (hex) of its receipt.
    pub proof: String,
}

/// A commit's published erasure: every step, oldest first.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErasedCommit {
    /// The original commit's CID.
    pub commit: String,
    /// The ledger id its signatures were made for.
    pub ledger_id: String,
    /// The digest of its transaction document, which holds what its steps
    /// removed: nothing writes that document again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub txn: Option<String>,
    pub steps: Vec<ErasureStep>,
    /// Addresses of its original that hold no file but whose folders read
    /// it, as its replacement: those a purge deleted, and those written
    /// since publication, which are refused (a restored copy's).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replaced_at: Vec<String>,
}

impl ErasedCommit {
    /// Every slot its steps removed, ascending.
    pub fn redacted_slots(&self) -> Vec<u32> {
        let mut slots: Vec<u32> = self.steps.iter().flat_map(|s| s.slots.clone()).collect();
        slots.sort_unstable();
        slots
    }

    /// The chain's identity: the SHA-256 (hex) of its commit, signing
    /// ledger and steps.
    pub fn digest(&self) -> String {
        self.digest_of(self.steps.len())
    }

    /// The digest of its first `steps` steps: the chain's, when it had them.
    pub(crate) fn digest_of(&self, steps: usize) -> String {
        let canonical = serde_json::to_vec(&(&self.commit, &self.ledger_id, &self.steps[..steps]))
            .expect("an erasure chain serializes");
        sha256_hex(&canonical)
    }
}

/// What an artifact is.
#[derive(Clone, Copy)]
pub(crate) enum Artifact {
    Replacement,
    Proof,
    /// An erasure request's control commit, kept for transfers to carry.
    Control,
}

impl Artifact {
    fn path(self, hex: &str) -> String {
        match self {
            Artifact::Replacement => format!("replacement/{hex}.fcr"),
            Artifact::Proof => format!("proof/{hex}.receipt"),
            Artifact::Control => format!("control/{hex}.fcv2"),
        }
    }
}

struct Entry {
    erased: ErasedCommit,
    /// The replacement, once its proofs have verified in this process.
    verified: tokio::sync::OnceCell<Arc<[u8]>>,
}

/// The erased commits of one storage, by the original's address digest.
pub(crate) struct ErasureIndex {
    storage: Arc<dyn Storage>,
    verifier: Arc<Verifier>,
    /// When the last complete listing of the map objects started.
    listed_at: parking_lot::RwLock<Option<Instant>>,
    /// The map objects read, as of that listing; held while listing.
    read: tokio::sync::Mutex<HashSet<String>>,
    /// Some commit is erased.
    any: AtomicBool,
    /// The digests of erased commits' transaction documents.
    txns: parking_lot::RwLock<HashSet<String>>,
    entries: parking_lot::RwLock<HashMap<String, Arc<Entry>>>,
}

impl std::fmt::Debug for ErasureIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErasureIndex")
            .field("listed_at", &*self.listed_at.read())
            .field("erased", &self.entries.read().len())
            .finish()
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// The address digest of the commit `address` names, if it names one.
fn commit_digest(address: &str) -> Option<&str> {
    let (_, file) = address.rsplit_once("/commit/")?;
    let digest = file.strip_suffix(".fcv2")?;
    (!digest.is_empty() && !digest.contains('/')).then_some(digest)
}

/// The digest of the transaction document `address` names, if it names one.
fn txn_digest(address: &str) -> Option<&str> {
    let (_, file) = address.rsplit_once("/txn/")?;
    let digest = file.strip_suffix(".json")?;
    (!digest.is_empty() && !digest.contains('/')).then_some(digest)
}

/// What `incoming` makes of the erasure held for its commit: the longer of
/// the two chains, with the addresses either names it replaced at; `None`
/// when it changes nothing, or diverges from the held chain.
fn merged(held: Option<&ErasedCommit>, incoming: ErasedCommit) -> Option<ErasedCommit> {
    let Some(current) = held else {
        return Some(incoming);
    };
    let shared = current.steps.len().min(incoming.steps.len());
    if current.steps[..shared] != incoming.steps[..shared] {
        tracing::warn!(commit = %incoming.commit, "divergent erasures; keeping the first");
        return None;
    }
    let mut replaced_at = current.replaced_at.clone();
    for address in &incoming.replaced_at {
        if !replaced_at.contains(address) {
            replaced_at.push(address.clone());
        }
    }
    let longer = incoming.steps.len() > current.steps.len();
    if !longer && replaced_at.len() == current.replaced_at.len() {
        return None;
    }
    let txn = incoming.txn.clone().or_else(|| current.txn.clone());
    let base = if longer { incoming } else { current.clone() };
    Some(ErasedCommit {
        txn,
        replaced_at,
        ..base
    })
}

fn merge(held: &mut HashMap<String, ErasedCommit>, incoming: ErasedCommit) {
    if let Some(next) = merged(held.get(&incoming.commit), incoming) {
        held.insert(next.commit.clone(), next);
    }
}

impl ErasureIndex {
    pub(crate) fn new(storage: Arc<dyn Storage>, verifier: Arc<Verifier>) -> Self {
        ErasureIndex {
            storage,
            verifier,
            listed_at: parking_lot::RwLock::new(None),
            read: tokio::sync::Mutex::new(HashSet::new()),
            any: AtomicBool::new(false),
            txns: parking_lot::RwLock::new(HashSet::new()),
            entries: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    fn address(&self, path: &str) -> String {
        format!("fluree:{}://{DIR}/{path}", self.storage.storage_method())
    }

    /// List the map objects again before the next read: another instance
    /// published since.
    pub(crate) fn expire(&self) {
        *self.listed_at.write() = None;
    }

    /// The published erasures, as a hash: every chain, and where its
    /// originals are read without a file.
    pub(crate) async fn generation(&self) -> Result<String> {
        self.load().await?;
        let mut chains: Vec<String> = self
            .entries
            .read()
            .values()
            .map(|e| {
                let mut at = e.erased.replaced_at.clone();
                at.sort();
                format!("{} {}", e.erased.digest(), at.join(" "))
            })
            .collect();
        chains.sort();
        Ok(sha256_hex(chains.join("\n").as_bytes()))
    }

    /// Whether the last listing of the map objects still stands.
    fn fresh(&self) -> bool {
        self.listed_at
            .read()
            .is_some_and(|at| at.elapsed() < REFRESH)
    }

    /// Read the map objects not read yet, unless the last listing still
    /// stands. An object that cannot be read or parsed is an error: what it
    /// holds may be an erasure.
    async fn load(&self) -> Result<()> {
        if self.fresh() {
            return Ok(());
        }
        let mut read = self.read.lock().await;
        if self.fresh() {
            return Ok(());
        }
        // An object compacted away between listing and reading is in the
        // union, which a listing after that holds.
        for _ in 0..3 {
            let started = Instant::now();
            let objects = self.storage.list_prefix(&self.address("map/")).await?;
            let mut held: HashMap<String, ErasedCommit> = HashMap::new();
            let mut gone = false;
            for object in objects.iter().filter(|o| !read.contains(*o)) {
                let bytes = match self.storage.read_bytes(object).await {
                    Ok(bytes) => bytes,
                    Err(fluree_db_core::Error::NotFound(_)) => {
                        gone = true;
                        break;
                    }
                    Err(e) => return Err(e.into()),
                };
                let published: Vec<ErasedCommit> = serde_json::from_slice(&bytes)
                    .map_err(|e| ApiError::internal(format!("erasure map {object}: {e}")))?;
                for erased in published {
                    merge(&mut held, erased);
                }
            }
            self.install(held.into_values());
            if !gone {
                *read = objects.into_iter().collect();
                *self.listed_at.write() = Some(started);
                return Ok(());
            }
        }
        Err(ApiError::internal(
            "the erasure maps kept changing while being read",
        ))
    }

    fn install(&self, erased: impl IntoIterator<Item = ErasedCommit>) {
        let mut entries = self.entries.write();
        for erased in erased {
            let Ok(cid) = erased.commit.parse::<ContentId>() else {
                tracing::warn!(commit = %erased.commit, "erasure of an unparsable commit id");
                continue;
            };
            let key = cid.address_digest();
            if let Some(next) = merged(entries.get(&key).map(|e| &e.erased), erased) {
                if let Some(txn) = &next.txn {
                    self.txns.write().insert(txn.clone());
                }
                entries.insert(
                    key,
                    Arc::new(Entry {
                        erased: next,
                        verified: tokio::sync::OnceCell::new(),
                    }),
                );
            }
        }
        self.any.store(!entries.is_empty(), Ordering::Release);
    }

    /// Whether the commit at `digest` may be erased, without waiting: true
    /// while the map objects are to be listed again.
    fn may_be_erased(&self, digest: &str) -> bool {
        !self.fresh()
            || (self.any.load(Ordering::Acquire) && self.entries.read().contains_key(digest))
    }

    /// Whether the transaction document at `digest` is an erased commit's.
    pub(crate) async fn is_erased_txn(&self, digest: &str) -> Result<bool> {
        if self.fresh() && !self.any.load(Ordering::Acquire) {
            return Ok(false);
        }
        self.load().await?;
        Ok(self.txns.read().contains(digest))
    }

    /// Whether the commit at `digest` is erased.
    async fn is_erased(&self, digest: &str) -> Result<bool> {
        self.load().await?;
        Ok(self.entries.read().contains_key(digest))
    }

    /// Where an artifact is stored.
    pub(crate) fn artifact_address(&self, kind: Artifact, hex: &str) -> String {
        self.address(&kind.path(hex))
    }

    /// The bytes stored at `address`, past the substitution: an erased
    /// commit's original, not its replacement. `None` when nothing is there.
    pub(crate) async fn read_raw(&self, address: &str) -> Result<Option<Vec<u8>>> {
        match self.storage.read_bytes(address).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(fluree_db_core::Error::NotFound(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Whether anything is stored at `address`, past the substitution.
    pub(crate) async fn exists_raw(&self, address: &str) -> Result<bool> {
        Ok(self.storage.exists(address).await?)
    }

    /// Delete what is stored at `address`, past the substitution. Whether
    /// anything was there.
    pub(crate) async fn delete_raw(&self, address: &str) -> Result<bool> {
        if !self.storage.exists(address).await? {
            return Ok(false);
        }
        self.storage.delete(address).await?;
        Ok(true)
    }

    /// Every address under the storage, past the substitution: what the
    /// files are, not what reads of them return.
    pub(crate) async fn list_all(&self) -> Result<Vec<String>> {
        Ok(self.storage.list_prefix(&self.address_prefix()).await?)
    }

    /// Every address under `prefix`, past the substitution.
    pub(crate) async fn list_raw(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self.storage.list_prefix(prefix).await?)
    }

    /// What every address of the storage starts with.
    pub(crate) fn address_prefix(&self) -> String {
        format!("fluree:{}://", self.storage.storage_method())
    }

    /// Whether any commit is erased.
    pub(crate) async fn any_erased(&self) -> Result<bool> {
        self.load().await?;
        Ok(self.any.load(Ordering::Acquire))
    }

    /// Whether `namespace` holds `original`'s own file, whatever its
    /// erasure, or reads it as its replacement without one: the file a
    /// purge deleted, or one refused since publication.
    pub(crate) async fn holds_original(
        &self,
        namespace: &StorageNamespace,
        original: &ContentId,
    ) -> Result<bool> {
        let address = self.commit_address(namespace, original);
        if self.storage.exists(&address).await? {
            return Ok(true);
        }
        Ok(self
            .erased(original)
            .await?
            .is_some_and(|e| e.replaced_at.contains(&address)))
    }

    /// Where `namespace` keeps commit `cid`.
    pub(crate) fn commit_address(&self, namespace: &StorageNamespace, cid: &ContentId) -> String {
        content_address(
            self.storage.storage_method(),
            ContentKind::Commit,
            namespace,
            &cid.address_digest(),
        )
    }

    /// Record that the erased originals at `addresses` are read there as
    /// their replacements without a file: before a purge deletes them, or
    /// when a write of one is refused. [`Self::holds_original`] goes on
    /// finding them.
    pub(crate) async fn record_replaced_at(&self, addresses: &[String]) -> Result<()> {
        self.load().await?;
        let mut changed: HashMap<String, ErasedCommit> = HashMap::new();
        {
            let entries = self.entries.read();
            for address in addresses {
                let Some(entry) = commit_digest(address).and_then(|d| entries.get(d)) else {
                    continue;
                };
                if entry.erased.replaced_at.contains(address) {
                    continue;
                }
                changed
                    .entry(entry.erased.commit.clone())
                    .or_insert_with(|| entry.erased.clone())
                    .replaced_at
                    .push(address.clone());
            }
        }
        if changed.is_empty() {
            return Ok(());
        }
        self.publish(changed.into_values().collect()).await
    }

    /// The digest of every erasure chain this storage holds.
    pub(crate) async fn digests(&self) -> Result<Vec<String>> {
        self.load().await?;
        Ok(self
            .entries
            .read()
            .values()
            .map(|e| e.erased.digest())
            .collect())
    }

    /// `original`'s published erasure, if it has one.
    pub(crate) async fn erased(&self, original: &ContentId) -> Result<Option<ErasedCommit>> {
        self.load().await?;
        Ok(self
            .entries
            .read()
            .get(&original.address_digest())
            .map(|e| e.erased.clone()))
    }

    /// The verified replacement read in place of the commit at `digest`,
    /// `None` when it is not erased. Its proofs verify once per process; a
    /// proof or replacement that does not verify is an error, never the
    /// original.
    pub(crate) async fn replacement(&self, digest: &str) -> Option<Result<Arc<[u8]>>> {
        if let Err(e) = self.load().await {
            return Some(Err(e));
        }
        let entry = self.entries.read().get(digest).cloned()?;
        Some(
            entry
                .verified
                .get_or_try_init(|| self.verify(&entry.erased))
                .await
                .cloned(),
        )
    }

    /// `erased`'s latest replacement, once its stored receipts verify it
    /// against the pinned guest images, bound to the original and its slots.
    pub(crate) async fn verify(&self, erased: &ErasedCommit) -> Result<Arc<[u8]>> {
        let last = erased.steps.last().ok_or_else(|| {
            ApiError::internal(format!("erasure of {} has no steps", erased.commit))
        })?;
        let replacement_cid: ContentId = last
            .replacement
            .parse()
            .map_err(|_| ApiError::internal("erasure names an unparsable replacement"))?;
        let replacement = self.read_replacement(&replacement_cid).await?;
        let mut receipts = Vec::with_capacity(erased.steps.len());
        for step in &erased.steps {
            receipts.push(self.artifact(Artifact::Proof, &step.proof).await?);
        }
        let original: ContentId = erased
            .commit
            .parse()
            .map_err(|_| ApiError::internal("erasure of an unparsable commit id"))?;
        let authorized: Vec<Vec<u32>> = erased.steps.iter().map(|s| s.slots.clone()).collect();
        let verifier = self.verifier.clone();
        let ledger_id = erased.ledger_id.clone();
        let checked = replacement.clone();
        tokio::task::spawn_blocking(move || {
            verifier.verify_chain(
                &receipts,
                &checked,
                &original.to_bytes(),
                &ledger_id,
                &authorized,
            )
        })
        .await
        .map_err(|e| ApiError::internal(format!("proof verification task: {e}")))?
        .map_err(|e| {
            ApiError::internal(format!(
                "the erasure of {} does not verify: {e}",
                erased.commit
            ))
        })?;
        Ok(replacement.into())
    }

    /// A replacement's bytes, checked against its CID.
    pub(crate) async fn read_replacement(&self, cid: &ContentId) -> Result<Vec<u8>> {
        let hex = cid.address_digest();
        let bytes = self.artifact(Artifact::Replacement, &hex).await?;
        if !cid.verify(&bytes) {
            return Err(ApiError::internal(format!(
                "stored replacement {cid} does not match its id"
            )));
        }
        Ok(bytes)
    }

    /// Store an artifact under the SHA-256 of its bytes; returns the hex.
    pub(crate) async fn put_artifact(&self, kind: Artifact, bytes: &[u8]) -> Result<String> {
        let hex = sha256_hex(bytes);
        self.storage
            .write_bytes(&self.address(&kind.path(&hex)), bytes)
            .await?;
        Ok(hex)
    }

    /// An artifact's bytes, checked against the hash that names them.
    pub(crate) async fn artifact(&self, kind: Artifact, hex: &str) -> Result<Vec<u8>> {
        let bytes = self
            .storage
            .read_bytes(&self.address(&kind.path(hex)))
            .await?;
        if sha256_hex(&bytes) != hex {
            return Err(ApiError::internal(format!(
                "erasure artifact {hex} does not match its hash"
            )));
        }
        Ok(bytes)
    }

    /// What authorized erasure request `request_id`, as stored when it was
    /// published here or received.
    pub(crate) async fn authorization(
        &self,
        request_id: &str,
    ) -> Result<Option<super::transfer::Authorization>> {
        let Some(bytes) = self
            .read_raw(&self.authorization_address(request_id))
            .await?
        else {
            return Ok(None);
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| ApiError::internal(format!("erasure authorization {request_id}: {e}")))
    }

    /// Store what authorized an erasure request. One held already must be
    /// the same.
    pub(crate) async fn put_authorization(
        &self,
        authorization: &super::transfer::Authorization,
    ) -> Result<()> {
        if let Some(held) = self.authorization(&authorization.request_id).await? {
            if held == *authorization {
                return Ok(());
            }
            return Err(ApiError::http(
                409,
                format!(
                    "the authorization of erasure request {} differs from the one this storage \
                     holds",
                    authorization.request_id
                ),
            ));
        }
        let bytes =
            serde_json::to_vec(authorization).map_err(|e| ApiError::internal(e.to_string()))?;
        let address = self.authorization_address(&authorization.request_id);
        self.storage.write_bytes(&address, &bytes).await?;
        Ok(())
    }

    fn authorization_address(&self, request_id: &str) -> String {
        self.address(&format!("authorization/{request_id}.json"))
    }

    /// Record an activation of received erasures, before the first ledger
    /// here changes for it, under `id`.
    pub(crate) async fn put_pending(&self, id: &str, bytes: &[u8]) -> Result<()> {
        self.storage
            .write_bytes(&self.address(&format!("pending/{id}.json")), bytes)
            .await?;
        Ok(())
    }

    /// Every activation recorded and not yet completed: (id, bytes).
    pub(crate) async fn pending(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let mut pending = Vec::new();
        for address in self.storage.list_prefix(&self.address("pending/")).await? {
            let Some(id) = address
                .rsplit_once('/')
                .and_then(|(_, file)| file.strip_suffix(".json"))
            else {
                continue;
            };
            if let Some(bytes) = self.read_raw(&address).await? {
                pending.push((id.to_string(), bytes));
            }
        }
        Ok(pending)
    }

    /// Forget a completed activation.
    pub(crate) async fn complete_pending(&self, id: &str) -> Result<()> {
        self.delete_raw(&self.address(&format!("pending/{id}.json")))
            .await
            .map(|_| ())
    }

    /// Publish erasures: one immutable map object, then the index. From
    /// here on, this instance reads the originals as their replacements,
    /// and another on the same storage does within [`REFRESH`].
    pub(crate) async fn publish(&self, erased: Vec<ErasedCommit>) -> Result<()> {
        self.load().await?;
        let mut read = self.read.lock().await;
        read.insert(self.write_map(&erased).await?);
        self.install(erased);
        // Past a bound, one object holding everything replaces the objects
        // listed: what they hold is in the index, which loaded them all.
        let objects = self.storage.list_prefix(&self.address("map/")).await?;
        if objects.len() >= COMPACT_AT {
            let all: Vec<ErasedCommit> = self
                .entries
                .read()
                .values()
                .map(|e| e.erased.clone())
                .collect();
            let union = self.write_map(&all).await?;
            read.insert(union.clone());
            let stale: Vec<String> = objects.into_iter().filter(|o| *o != union).collect();
            for (address, e) in self.storage.delete_many(&stale).await {
                tracing::warn!(%address, error = %e, "erasure: compacted map object not deleted");
            }
        }
        Ok(())
    }

    /// Write a map object named by its hash; returns its address.
    async fn write_map(&self, erased: &[ErasedCommit]) -> Result<String> {
        let bytes = serde_json::to_vec(erased).map_err(|e| ApiError::internal(e.to_string()))?;
        let address = self.address(&format!("map/{}.json", sha256_hex(&bytes)));
        self.storage.write_bytes(&address, &bytes).await?;
        Ok(address)
    }
}

/// Storage that reads each erased commit as its verified replacement, and
/// drops writes of an erased original so nothing writes it back.
#[derive(Debug)]
pub(crate) struct ErasureStorage {
    inner: Arc<dyn Storage>,
    index: Arc<ErasureIndex>,
    pins: Option<Arc<BlobPins>>,
}

impl ErasureStorage {
    /// A write of an erased original at `address` is refused: record that
    /// its folder reads the commit all the same.
    async fn refused(&self, address: &str) -> fluree_db_core::Result<()> {
        if commit_digest(address).is_none() {
            return Ok(());
        }
        self.index
            .record_replaced_at(&[address.to_string()])
            .await
            .map_err(|e| fluree_db_core::Error::storage(e.to_string()))
    }

    pub(crate) fn new(
        inner: Arc<dyn Storage>,
        index: Arc<ErasureIndex>,
        pins: Option<Arc<BlobPins>>,
    ) -> Self {
        ErasureStorage { inner, index, pins }
    }

    async fn substitute(&self, address: &str) -> Option<fluree_db_core::Result<Arc<[u8]>>> {
        let digest = commit_digest(address)?;
        if !self.index.may_be_erased(digest) {
            return None;
        }
        self.index
            .replacement(digest)
            .await
            .map(|r| r.map_err(|e| fluree_db_core::Error::storage(e.to_string())))
    }

    /// Whether `address` is an erased commit's, or its transaction
    /// document's, which may not be written back.
    async fn erased_address(&self, address: &str) -> fluree_db_core::Result<bool> {
        if let Some(digest) = txn_digest(address) {
            return self
                .index
                .is_erased_txn(digest)
                .await
                .map_err(|e| fluree_db_core::Error::storage(e.to_string()));
        }
        match commit_digest(address) {
            Some(digest) if self.index.may_be_erased(digest) => self
                .index
                .is_erased(digest)
                .await
                .map_err(|e| fluree_db_core::Error::storage(e.to_string())),
            _ => Ok(false),
        }
    }

    /// Whether a sync read of `address` must go the async way, through
    /// [`Self::substitute`].
    fn defer_sync(&self, address: &str) -> bool {
        commit_digest(address).is_some_and(|digest| self.index.may_be_erased(digest))
    }
}

#[async_trait]
impl StorageRead for ErasureStorage {
    async fn read_bytes(&self, address: &str) -> fluree_db_core::Result<Vec<u8>> {
        match self.substitute(address).await {
            Some(r) => r.map(|b| b.to_vec()),
            None => self.inner.read_bytes(address).await,
        }
    }

    async fn read_bytes_hint(
        &self,
        address: &str,
        hint: ReadHint,
    ) -> fluree_db_core::Result<Vec<u8>> {
        match self.substitute(address).await {
            Some(r) => r.map(|b| b.to_vec()),
            None => self.inner.read_bytes_hint(address, hint).await,
        }
    }

    async fn read_byte_range(
        &self,
        address: &str,
        range: std::ops::Range<u64>,
    ) -> fluree_db_core::Result<Vec<u8>> {
        match self.substitute(address).await {
            Some(r) => r.map(|b| {
                let end = (range.end as usize).min(b.len());
                let start = (range.start as usize).min(end);
                b[start..end].to_vec()
            }),
            None => self.inner.read_byte_range(address, range).await,
        }
    }

    fn supports_ranged_reads(&self) -> bool {
        self.inner.supports_ranged_reads()
    }

    async fn exists(&self, address: &str) -> fluree_db_core::Result<bool> {
        match self.substitute(address).await {
            Some(r) => r.map(|_| true),
            None => self.inner.exists(address).await,
        }
    }

    async fn list_prefix(&self, prefix: &str) -> fluree_db_core::Result<Vec<String>> {
        self.inner.list_prefix(prefix).await
    }

    async fn list_prefix_with_metadata(
        &self,
        prefix: &str,
    ) -> fluree_db_core::Result<Vec<RemoteObject>> {
        self.inner.list_prefix_with_metadata(prefix).await
    }

    fn get_local(&self, address: &str) -> fluree_db_core::Result<Option<ContentBytes>> {
        if self.defer_sync(address) {
            return Ok(None);
        }
        self.inner.get_local(address)
    }

    fn get_local_ranged(
        &self,
        address: &str,
    ) -> fluree_db_core::Result<Option<Arc<dyn RangeSource>>> {
        if self.defer_sync(address) {
            return Ok(None);
        }
        self.inner.get_local_ranged(address)
    }

    fn permits_plaintext_cache(&self) -> bool {
        self.inner.permits_plaintext_cache()
    }

    fn is_remote(&self) -> bool {
        self.inner.is_remote()
    }

    fn encryption_admin(&self) -> Option<Arc<dyn EncryptionAdmin>> {
        self.inner.encryption_admin()
    }

    fn resolve_cached_bytes(&self, id: &ContentId) -> Option<Arc<[u8]>> {
        if id.content_kind() == Some(ContentKind::Commit)
            && self.index.may_be_erased(&id.address_digest())
        {
            return None;
        }
        self.inner.resolve_cached_bytes(id)
    }

    fn miss_register(&self) -> Option<&fluree_db_core::storage::residency::MissRegister> {
        self.inner.miss_register()
    }

    fn query_guard(&self) -> Option<fluree_db_core::storage::residency::InFlightGuard> {
        self.inner.query_guard()
    }

    fn blob_pins(&self) -> Option<Arc<BlobPins>> {
        self.pins.clone()
    }
}

#[async_trait]
impl StorageWrite for ErasureStorage {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> fluree_db_core::Result<()> {
        // The original is gone; the replacement stands for it.
        if self.erased_address(address).await? {
            return self.refused(address).await;
        }
        self.inner.write_bytes(address, bytes).await
    }

    async fn delete(&self, address: &str) -> fluree_db_core::Result<()> {
        self.inner.delete(address).await
    }

    async fn delete_many(&self, addresses: &[String]) -> Vec<(String, fluree_db_core::Error)> {
        self.inner.delete_many(addresses).await
    }

    async fn sync(&self) -> fluree_db_core::Result<()> {
        self.inner.sync().await
    }
}

#[async_trait]
impl ContentAddressedWrite for ErasureStorage {
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        namespace: &StorageNamespace,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> fluree_db_core::Result<ContentWriteResult> {
        if matches!(kind, ContentKind::Commit | ContentKind::Txn) {
            let address = content_address(
                self.inner.storage_method(),
                kind,
                namespace,
                content_hash_hex,
            );
            if self.erased_address(&address).await? {
                self.refused(&address).await?;
                return Ok(ContentWriteResult {
                    address,
                    content_hash: content_hash_hex.to_string(),
                    size_bytes: bytes.len(),
                });
            }
        }
        self.inner
            .content_write_bytes_with_hash(kind, namespace, content_hash_hex, bytes)
            .await
    }
}

impl StorageMethod for ErasureStorage {
    fn storage_method(&self) -> &str {
        self.inner.storage_method()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_commit_address_names_its_digest() {
        assert_eq!(
            commit_digest("fluree:file://acme/main/commit/abc123.fcv2"),
            Some("abc123")
        );
        assert_eq!(
            commit_digest("fluree:file://acme/@i/main/commit/sha2-512-ff.fcv2"),
            Some("sha2-512-ff")
        );
        assert_eq!(commit_digest("fluree:file://acme/main/txn/abc.json"), None);
        assert_eq!(
            commit_digest("fluree:file://acme/main/index/commit/x/y.fcv2"),
            None
        );
    }

    fn step(id: &str) -> ErasureStep {
        ErasureStep {
            request_id: id.into(),
            slots: vec![1],
            replacement: format!("r-{id}"),
            proof: format!("p-{id}"),
        }
    }

    fn erased(n: usize) -> ErasedCommit {
        ErasedCommit {
            commit: ContentId::new(ContentKind::Commit, format!("commit {n}").as_bytes())
                .to_string(),
            ledger_id: "l:main".into(),
            txn: None,
            steps: vec![step(&format!("r{n}"))],
            replaced_at: Vec::new(),
        }
    }

    #[tokio::test]
    async fn published_maps_compact_and_load_whole() {
        let storage: Arc<dyn Storage> = Arc::new(fluree_db_core::MemoryStorage::new());
        let index = ErasureIndex::new(storage.clone(), Arc::new(Verifier::default()));
        for n in 0..COMPACT_AT + 5 {
            index.publish(vec![erased(n)]).await.unwrap();
        }
        let objects = storage.list_prefix(&index.address("map/")).await.unwrap();
        assert!(objects.len() < COMPACT_AT, "{} objects", objects.len());
        let fresh = ErasureIndex::new(storage, Arc::new(Verifier::default()));
        for n in 0..COMPACT_AT + 5 {
            let cid: ContentId = erased(n).commit.parse().unwrap();
            assert_eq!(fresh.erased(&cid).await.unwrap(), Some(erased(n)));
        }
    }

    #[test]
    fn a_longer_chain_replaces_the_one_it_extends_and_nothing_else() {
        let erased = |steps: Vec<ErasureStep>| ErasedCommit {
            commit: "c".into(),
            ledger_id: "l:main".into(),
            txn: None,
            steps,
            replaced_at: Vec::new(),
        };
        let mut held = HashMap::new();
        merge(&mut held, erased(vec![step("a")]));
        merge(&mut held, erased(vec![step("a"), step("b")]));
        assert_eq!(held["c"].steps.len(), 2);
        merge(&mut held, erased(vec![step("a")]));
        assert_eq!(held["c"].steps.len(), 2, "a prefix does not shorten it");
        merge(&mut held, erased(vec![step("x"), step("y"), step("z")]));
        assert_eq!(
            held["c"].steps[1].request_id, "b",
            "a divergent chain is not taken"
        );
    }

    #[test]
    fn replaced_addresses_accumulate_whichever_chain_is_longer() {
        let erased = |steps: &[&str], at: &[&str]| ErasedCommit {
            commit: "c".into(),
            ledger_id: "l:main".into(),
            txn: None,
            steps: steps.iter().map(|s| step(s)).collect(),
            replaced_at: at.iter().map(ToString::to_string).collect(),
        };
        let mut held = HashMap::new();
        merge(&mut held, erased(&["a"], &[]));
        merge(&mut held, erased(&["a"], &["f1"]));
        merge(&mut held, erased(&["a", "b"], &[]));
        merge(&mut held, erased(&["a"], &["f2"]));
        assert_eq!(held["c"].steps.len(), 2);
        assert_eq!(held["c"].replaced_at, ["f1", "f2"]);
    }
}
