//! Fixed imported index bootstrap. No index publication or checkpoint retirement.
use super::*;
use crate::local_journal_acceptance::{indexed_head, LinearBaseline};
use fluree_db_core::local_journal::{
    Checkpoint, CheckpointEntry, CheckpointSpec, Result as JResult,
};
use fluree_db_core::{FileStorage, StorageBackend, StorageRead};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Cursor;

const SOURCE_MAIN: &str = "bootstrap/source-main.json";
const SOURCE_INDEX: &str = "bootstrap/source-index.json";

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IndexPointer {
    #[serde(rename = "f:cid")]
    pub cid: ContentId,
    #[serde(rename = "f:t")]
    pub t: i64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IndexFile {
    #[serde(rename = "@context")]
    context: Value,
    #[serde(rename = "f:ledgerIndex")]
    index: IndexPointer,
}
#[derive(PartialEq, Eq)]
struct Pin {
    main: Vec<u8>,
    index: Option<Vec<u8>>,
}
impl Pin {
    async fn read(storage: &FileStorage, ledger: &str) -> JResult<Self> {
        let (name, branch) =
            split_ledger_id(ledger).map_err(|_| JournalError::Invalid("invalid ledger"))?;
        let main = format!("fluree:file://ns@v2/{name}/{branch}.json");
        let index = format!("fluree:file://ns@v2/{name}/{branch}.index.json");
        let main = storage
            .read_bytes(&main)
            .await
            .map_err(|_| JournalError::Invalid("source main head read failed"))?;
        let index = if storage
            .exists(&index)
            .await
            .map_err(|_| JournalError::Invalid("source index head lookup failed"))?
        {
            Some(
                storage
                    .read_bytes(&index)
                    .await
                    .map_err(|_| JournalError::Invalid("source index head read failed"))?,
            )
        } else {
            None
        };
        if main.len() > 65536 || index.as_ref().is_some_and(|b| b.len() > 65536) {
            return Err(JournalError::Invalid("oversized source head"));
        }
        Ok(Self { main, index })
    }
    fn head(&self, ledger: &str) -> JResult<Object> {
        let (name, branch) =
            split_ledger_id(ledger).map_err(|_| JournalError::Invalid("invalid ledger"))?;
        let key = format!("ns@v2/{name}/{branch}.json");
        indexed_head(&self.main, ledger, &key)?; // closed shape, before Value parsing
        let mut head: Value = serde_json::from_slice(&self.main)
            .map_err(|_| JournalError::Invalid("source head JSON"))?;
        let mut index = parse_index(&head)?;
        if let Some(bytes) = &self.index {
            let separate: IndexFile = serde_json::from_slice(bytes)
                .map_err(|_| JournalError::Invalid("unsupported source index head"))?;
            if separate.context != json!({"f":fluree_vocab::fluree::DB}) {
                return Err(JournalError::Invalid("unsupported source index context"));
            }
            // Same effective-head merge rule as FileNameService::lookup.
            if index.as_ref().is_none_or(|old| separate.index.t >= old.t) {
                index = Some(separate.index);
            }
        }
        let index = index.ok_or(JournalError::Invalid(
            "bootstrap requires an indexed source",
        ))?;
        if index.cid.content_kind() != Some(ContentKind::IndexRoot)
            || index.t <= 0
            || index.t > head["f:t"].as_i64().unwrap_or(-1)
        {
            return Err(JournalError::Invalid("invalid source index time/kind"));
        }
        head["f:ledgerIndex"] = serde_json::to_value(index)
            .map_err(|_| JournalError::Invalid("index head encoding"))?;
        Ok(Object {
            key,
            bytes: serde_json::to_vec(&head).map_err(|_| JournalError::Invalid("head encoding"))?,
        })
    }
    fn metadata(&self) -> Vec<Object> {
        let mut objects = vec![Object {
            key: SOURCE_MAIN.into(),
            bytes: self.main.clone(),
        }];
        if let Some(bytes) = &self.index {
            objects.push(Object {
                key: SOURCE_INDEX.into(),
                bytes: bytes.clone(),
            });
        }
        objects
    }
}

pub(super) fn parse_index(head: &Value) -> JResult<Option<IndexPointer>> {
    head.get("f:ledgerIndex")
        .filter(|v| !v.is_null())
        .map(|value| {
            serde_json::from_value(value.clone())
                .map_err(|_| JournalError::Invalid("invalid fixed index pointer"))
        })
        .transpose()
}

fn prefix_context(bytes: &[u8]) -> JResult<Value> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|_| JournalError::Invalid("invalid imported prefix context"))?;
    let map = value
        .as_object()
        .ok_or(JournalError::Invalid("context must be a static IRI map"))?;
    if map.iter().any(|(key, value)| {
        (key.starts_with('@') && key != "@vocab")
            || !value.as_str().is_some_and(|iri| {
                iri.starts_with("https://") || iri.starts_with("http://") || iri.starts_with("urn:")
            })
    }) {
        return Err(JournalError::Invalid(
            "only static imported IRI mappings are supported",
        ));
    }
    Ok(value)
}

struct Artifact {
    id: ContentId,
    entry: CheckpointEntry,
}
fn entry(key: String, bytes: &[u8]) -> CheckpointEntry {
    CheckpointEntry {
        key,
        length: bytes.len() as u64,
        sha256: Sha256::digest(bytes).into(),
    }
}
async fn fetch<C: ContentStore>(
    store: &C,
    ledger: &str,
    id: &ContentId,
    inventory: &mut BTreeMap<String, Artifact>,
) -> JResult<Vec<u8>> {
    let kind = id
        .content_kind()
        .ok_or(JournalError::Invalid("unknown baseline content kind"))?;
    let bytes = store
        .get(id)
        .await
        .map_err(|_| JournalError::Invalid("missing baseline dependency"))?;
    if bytes.len() as u64 > 1024 * 1024 * 1024 || !id.verify(&bytes) {
        return Err(JournalError::Invalid("baseline content size/CID mismatch"));
    }
    let key = content_path(kind, ledger, &id.digest_hex());
    inventory.insert(
        key.clone(),
        Artifact {
            id: id.clone(),
            entry: entry(key, &bytes),
        },
    );
    if inventory.len() > 99_998 {
        return Err(JournalError::Invalid("baseline inventory capacity"));
    }
    Ok(bytes)
}

/// Verify the full supported baseline once. Full commit decoding is deliberately
/// retained in this first integration; optimizing large-import validation is later.
async fn verify_baseline<C: ContentStore>(
    store: &C,
    ledger: &str,
    head: &Object,
) -> JResult<(LinearBaseline, BTreeMap<String, Artifact>)> {
    indexed_head(&head.bytes, ledger, &head.key)?;
    let record = ns_record(ledger, Some(&head.bytes))?;
    let index_id = record
        .index_head_id
        .as_ref()
        .ok_or(JournalError::Invalid("missing baseline index"))?;
    if index_id.content_kind() != Some(ContentKind::IndexRoot)
        || record.index_t <= 0
        || record.index_t > record.commit_t
    {
        return Err(JournalError::Invalid("baseline index time/kind"));
    }
    let mut inventory = BTreeMap::new();
    if let Some(id) = &record.default_context {
        if id.content_kind() != Some(ContentKind::LedgerConfig) {
            return Err(JournalError::Invalid("baseline context content kind"));
        }
        prefix_context(&fetch(store, ledger, id, &mut inventory).await?)?;
    }
    let mut next = Some((index_id.clone(), record.index_t));
    let mut index_roots = BTreeSet::new();
    while let Some((id, expected_t)) = next {
        if id.content_kind() != Some(ContentKind::IndexRoot)
            || !index_roots.insert(id.clone())
            || index_roots.len() > 4096
        {
            return Err(JournalError::Invalid(
                "invalid or oversized baseline index history",
            ));
        }
        let root = fluree_db_binary_index::IndexRoot::decode(
            &fetch(store, ledger, &id, &mut inventory).await?,
        )
        .map_err(|_| JournalError::Invalid("invalid baseline index root"))?;
        if root.ledger_id != ledger
            || root.index_t != expected_t
            || root.index_t < 0
            || root
                .named_graphs
                .iter()
                .any(|g| g.g_id != fluree_db_core::graph_registry::TXN_META_GRAPH_ID)
            || root
                .graph_arenas
                .iter()
                .any(|g| g.g_id > fluree_db_core::graph_registry::TXN_META_GRAPH_ID)
            || root.graph_iris
                != [
                    fluree_db_core::graph_registry::txn_meta_graph_iri(ledger),
                    fluree_db_core::graph_registry::config_graph_iri(ledger),
                ]
        {
            return Err(JournalError::Invalid(
                "unsupported baseline index identity/graphs",
            ));
        }
        let artifacts = crate::pack::compute_missing_index_artifacts(store, &id, None)
            .await
            .map_err(|_| JournalError::Invalid("incomplete baseline index closure"))?;
        for id in artifacts {
            fetch(store, ledger, &id, &mut inventory).await?;
        }
        if let Some(garbage) = root.garbage {
            if garbage.id.content_kind() != Some(ContentKind::GarbageRecord) {
                return Err(JournalError::Invalid("baseline garbage manifest kind"));
            }
            // Preserve the manifest, but never run GC or resurrect objects merely
            // because they are listed as garbage. Live dependencies of every
            // retained historical root are included by the strict walker above.
            fetch(store, ledger, &garbage.id, &mut inventory).await?;
        }
        next = match root.prev_index {
            Some(previous) if previous.t <= root.index_t => Some((previous.id, previous.t)),
            Some(_) => {
                return Err(JournalError::Invalid(
                    "baseline index history advances time",
                ))
            }
            None => None,
        };
    }
    let head_id = record
        .commit_head_id
        .ok_or(JournalError::Invalid("missing baseline commit"))?;
    let mut id = head_id.clone();
    let mut t = record.commit_t;
    let mut visited = BTreeSet::new();
    loop {
        if id.content_kind() != Some(ContentKind::Commit) || !visited.insert(id.clone()) {
            return Err(JournalError::Invalid("invalid baseline commit ancestry"));
        }
        let bytes = fetch(store, ledger, &id, &mut inventory).await?;
        if bytes.get(4) != Some(&4) {
            return Err(JournalError::Invalid("baseline requires v4 commits"));
        }
        let commit = fluree_db_core::commit::codec::read_commit(&bytes)
            .map_err(|_| JournalError::Invalid("invalid baseline commit"))?;
        if commit.t != t
            || t < 0
            || commit.parents.len() > 1
            || commit.txn_signature.is_some()
            || !commit.commit_signatures.is_empty()
            || !commit.graph_delta.is_empty()
            || commit.flakes.iter().any(|f| f.g.is_some())
        {
            return Err(JournalError::Invalid(
                "unsupported baseline commit time/signatures/graphs",
            ));
        }
        if let Some(raw) = commit.txn {
            if raw.content_kind() != Some(ContentKind::Txn) {
                return Err(JournalError::Invalid("baseline raw content kind"));
            }
            fetch(store, ledger, &raw, &mut inventory).await?;
        }
        match commit.parents.into_iter().next() {
            Some(parent) => {
                id = parent;
                t = t
                    .checked_sub(1)
                    .ok_or(JournalError::Invalid("baseline ancestor time"))?;
            }
            None if t <= 1 => break,
            None => return Err(JournalError::Invalid("baseline genesis missing")),
        }
    }
    Ok((
        LinearBaseline {
            id: head_id,
            t: record.commit_t,
        },
        inventory,
    ))
}

pub(super) struct Proof {
    pub linear: LinearBaseline,
    pub context: Option<Value>,
    digest: [u8; 32],
}
impl Proof {
    pub fn check(&self, checkpoint: &Checkpoint) -> JResult<()> {
        if self.digest != checkpoint.digest() {
            return Err(JournalError::Invalid("baseline proof binding mismatch"));
        }
        Ok(())
    }
    pub async fn load(checkpoint: &Checkpoint, store: &RecoveryStore) -> JResult<Self> {
        let pin = Pin {
            main: checkpoint
                .read(SOURCE_MAIN)?
                .ok_or(JournalError::Invalid("missing source head provenance"))?,
            index: checkpoint.read(SOURCE_INDEX)?,
        };
        if pin.head(checkpoint.ledger())? != *checkpoint.head() {
            return Err(JournalError::Invalid(
                "checkpoint head differs from pinned source",
            ));
        }
        let (linear, inventory) =
            verify_baseline(store, checkpoint.ledger(), checkpoint.head()).await?;
        let mut expected: Vec<_> = inventory
            .into_values()
            .map(|a| a.entry)
            .chain(
                pin.metadata()
                    .iter()
                    .map(|o| entry(o.key.clone(), &o.bytes)),
            )
            .collect();
        expected.sort_by(|a, b| a.key.cmp(&b.key));
        if expected != checkpoint.entries() {
            return Err(JournalError::Invalid(
                "checkpoint inventory differs from complete baseline closure",
            ));
        }
        let record = ns_record(checkpoint.ledger(), Some(&checkpoint.head().bytes))?;
        let context = match record.default_context {
            Some(id) => {
                Some(prefix_context(&store.bytes(&id).map_err(|_| {
                    JournalError::Invalid("missing baseline context")
                })?)?)
            }
            None => None,
        };
        Ok(Self {
            linear,
            context,
            digest: checkpoint.digest(),
        })
    }
}

impl JournalLedger {
    /// Copy a quiescent private ordinary FILE source into a fresh empty owned root.
    /// Only unsigned default-graph ledgers with fully retained index history and
    /// optional static imported IRI mappings are supported. Preserve the source;
    /// this is not live migration, index advancement or an ordinary server backend.
    pub async fn bootstrap(
        root: PathBuf,
        source_root: PathBuf,
        ledger: String,
        generation: String,
    ) -> Result<Self> {
        Self::bootstrap_with_copy_hook(root, source_root, ledger, generation, || Ok(())).await
    }

    // Deterministic source-change injection after every object was copied.
    async fn bootstrap_with_copy_hook(
        root: PathBuf,
        source_root: PathBuf,
        ledger: String,
        generation: String,
        after_copy: impl FnOnce() -> JResult<()> + Send + 'static,
    ) -> Result<Self> {
        let runtime = tokio::runtime::Handle::current();
        let owner = tokio::task::spawn_blocking(move || -> JResult<Arc<LocalRoot>> {
            let source_root = std::fs::canonicalize(source_root)?;
            let root = std::fs::canonicalize(root)?;
            if source_root.starts_with(&root) || root.starts_with(&source_root) {
                return Err(JournalError::Invalid(
                    "source and checkpoint roots must be disjoint",
                ));
            }
            let storage = FileStorage::new(source_root);
            let store = StorageBackend::Managed(Arc::new(storage.clone())).content_store(&ledger);
            let pin = runtime.block_on(Pin::read(&storage, &ledger))?;
            let head = pin.head(&ledger)?;
            let (_, inventory) = runtime.block_on(verify_baseline(&store, &ledger, &head))?;
            let metadata: BTreeMap<_, _> = pin
                .metadata()
                .into_iter()
                .map(|o| (o.key, o.bytes))
                .collect();
            let mut entries: Vec<_> = inventory
                .values()
                .map(|a| a.entry.clone())
                .chain(
                    metadata
                        .iter()
                        .map(|(key, bytes)| entry(key.clone(), bytes)),
                )
                .collect();
            entries.sort_by(|a, b| a.key.cmp(&b.key));
            LocalRoot::bootstrap(
                &root,
                &ledger,
                &generation,
                CheckpointSpec {
                    head,
                    objects: entries,
                },
                |entry| {
                    let bytes = match metadata.get(&entry.key) {
                        Some(bytes) => bytes.clone(),
                        None => runtime
                            .block_on(store.get(&inventory[&entry.key].id))
                            .map_err(|_| JournalError::Invalid("baseline copy read failed"))?,
                    };
                    Ok(Cursor::new(bytes))
                },
                |_| {
                    // The copied inventory came from the verified exact CID closure.
                    // Core verified those same lengths/hashes while copying.
                    after_copy()?;
                    if runtime.block_on(Pin::read(&storage, &ledger))? != pin {
                        return Err(JournalError::Invalid(
                            "source heads changed during bootstrap",
                        ));
                    }
                    Ok(())
                },
            )
        })
        .await??;
        Self::attach(owner).await
    }
}

#[cfg(test)]
mod tests;
