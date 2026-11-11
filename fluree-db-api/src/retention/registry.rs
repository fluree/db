//! A branch's retention registration: whether the retention worker runs for
//! it, from which commit its retention input is honored, and where its
//! persisted schedule is.
//!
//! Kept in the branch's nameservice config record (`extra.retention`), which
//! every nameservice backend persists, Raft replicates (`PushConfig`), and a
//! ledger backup does not roll back. Written with compare-and-set under the
//! branch fence, so a stale owner cannot overwrite a newer checkpoint.
//!
//! The record holds what is bounded. The lists that grow, the journal's
//! removals and each erasure job's commits and joined occurrences, are kept
//! outside it where the instance has managed storage: split by position into
//! chunks of [`CHUNK`] items, each an immutable object under
//! `@retention/registration/` named by the hash of its bytes, which the
//! record names. A write stores only the chunks that changed, and a list
//! equal to the one read is not serialized again. Without managed storage the
//! lists stay inline, as a record written before chunks still has them.

use std::any::Any;
use std::sync::Arc;

use fluree_db_core::Storage;
use fluree_db_nameservice::{ConfigCasResult, ConfigPayload, ConfigValue, NameServicePublisher};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::journal::{Journal, Removal};
#[cfg(feature = "native")]
use super::removal::OccurrenceRef;
use crate::{ApiError, Result};

/// Items in a chunk of a list kept outside the record.
pub(crate) const CHUNK: usize = 512;

/// Chunks a process keeps read.
const CACHED_CHUNKS: usize = 4096;

const OBJECTS_DIR: &str = "@retention/registration";

/// Key of the registration in the config record's `extra` map.
pub const EXTRA_KEY: &str = "retention";

/// How many times a registration update retries a lost compare-and-set.
const CAS_ATTEMPTS: usize = 8;

/// A branch's retention registration.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Registration {
    /// The activation boundary: the first commit whose retention input
    /// (TTL instructions, transaction defaults, retention config) is honored.
    pub active_since_t: i64,
    /// Commits with `t <= watermark_t` are in the persisted schedule.
    #[serde(default)]
    pub watermark_t: i64,
    /// Content id of the persisted schedule blob.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,
    /// When the earliest scheduled occurrence is due (epoch ms).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_due_ms: Option<i64>,
    /// Scheduled occurrences not yet expired.
    #[serde(default)]
    pub pending: u64,
    /// When the worker last finished a pass (epoch ms).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_pass_ms: Option<i64>,
    /// Occurrences expired since activation.
    #[serde(default)]
    pub expired_total: u64,
    /// Expiration batches that had to be recomputed (head moved, conflict).
    #[serde(default)]
    pub retries: u64,
    /// What blocks or degrades processing, for an operator.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub problems: Vec<String>,
    /// The removal journal: what the branch's history no longer serves.
    #[serde(default, skip_serializing_if = "super::journal::Journal::is_empty")]
    pub journal: super::journal::Journal,
    /// The CID of the ledger's genesis commit, which every copy of it
    /// (restored, cloned, branched) shares; keys the journal checkpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<String>,
    /// The journal generation last published to the lineage's checkpoint;
    /// a pass publishes a journal that has moved past it.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub checkpointed: u64,
    /// Permanent-erasure requests made on this branch, and their progress.
    #[cfg(feature = "native")]
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        with = "super::erasure::jobs_as_written"
    )]
    pub erasures: Vec<super::erasure::ErasureJob>,
    /// The chunks the record names, as read.
    #[serde(skip)]
    pub(crate) named: Named,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

/// The chunks a record names, with the lists as read from them, so a write
/// of a list unchanged names them again without serializing it. Never part
/// of a registration's equality.
#[derive(Clone, Debug, Default)]
pub(crate) struct Named(Option<Arc<Chunks>>);

impl PartialEq for Named {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

#[derive(Debug, Default)]
struct Chunks {
    removals: Option<Listed<Removal>>,
    /// By erasure request id.
    #[cfg(feature = "native")]
    jobs: std::collections::HashMap<String, JobLists>,
}

#[derive(Debug)]
struct Listed<T> {
    chunks: Vec<String>,
    read: Arc<Vec<T>>,
}

#[cfg(feature = "native")]
#[derive(Debug, Default)]
struct JobLists {
    commits: Option<Listed<super::erasure::CommitErasure>>,
    added: Option<Listed<OccurrenceRef>>,
}

/// Chunks read in this process, by hash. Objects never change, so an entry
/// is never stale.
pub(crate) struct ChunkCache(parking_lot::Mutex<lru::LruCache<String, Arc<dyn Any + Send + Sync>>>);

impl Default for ChunkCache {
    fn default() -> Self {
        ChunkCache(parking_lot::Mutex::new(lru::LruCache::new(
            std::num::NonZeroUsize::new(CACHED_CHUNKS).expect("nonzero"),
        )))
    }
}

#[cfg(test)]
impl ChunkCache {
    pub(crate) fn clear(&self) {
        self.0.lock().clear();
    }
}

/// Where chunks are stored, and those read.
#[derive(Clone, Copy)]
pub(crate) struct Objects<'a> {
    storage: &'a dyn Storage,
    cache: &'a ChunkCache,
    /// Held to write a registration, from its chunks to its record, and
    /// exclusively by a sweep from reading names to deleting.
    writes: Option<&'a tokio::sync::RwLock<()>>,
    /// On a cluster member, what collection condemned: each write runs in
    /// a scope, so its chunks are checked and its record presents what it
    /// saw.
    #[cfg(feature = "native")]
    condemned: Option<&'a crate::collect::Condemned>,
}

impl<'a> Objects<'a> {
    pub(crate) fn new(storage: &'a dyn Storage, cache: &'a ChunkCache) -> Self {
        Objects {
            storage,
            cache,
            writes: None,
            #[cfg(feature = "native")]
            condemned: None,
        }
    }

    fn address(&self, hash: &str) -> String {
        format!(
            "fluree:{}://{OBJECTS_DIR}/{hash}.json",
            self.storage.storage_method()
        )
    }

    async fn chunk<T>(&self, hash: &str) -> Result<Arc<Vec<T>>>
    where
        T: DeserializeOwned + Send + Sync + 'static,
    {
        if let Some(cached) = self.cache.0.lock().get(hash).cloned() {
            if let Ok(chunk) = cached.downcast::<Vec<T>>() {
                return Ok(chunk);
            }
        }
        let bytes = self.storage.read_bytes(&self.address(hash)).await?;
        if fluree_db_core::sha256_hex(&bytes) != hash {
            return Err(ApiError::internal(format!(
                "retention registration chunk {hash} does not match its hash"
            )));
        }
        let chunk: Arc<Vec<T>> = Arc::new(serde_json::from_slice(&bytes).map_err(|e| {
            ApiError::internal(format!("retention registration chunk {hash}: {e}"))
        })?);
        self.cache.0.lock().put(
            hash.to_string(),
            chunk.clone() as Arc<dyn Any + Send + Sync>,
        );
        Ok(chunk)
    }

    async fn read_list<T>(&self, chunks: Vec<String>) -> Result<Listed<T>>
    where
        T: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        let mut read = Vec::new();
        for hash in &chunks {
            read.extend(self.chunk::<T>(hash).await?.iter().cloned());
        }
        Ok(Listed {
            chunks,
            read: Arc::new(read),
        })
    }

    /// Store `items` as chunks, naming again each chunk equal to the one
    /// `before` read at its position.
    async fn write_list<T>(&self, items: &[T], before: Option<&Listed<T>>) -> Result<Vec<String>>
    where
        T: Serialize + PartialEq,
    {
        let mut out = Vec::with_capacity(items.len().div_ceil(CHUNK));
        for (k, chunk) in items.chunks(CHUNK).enumerate() {
            let same = before.and_then(|b| {
                let old = b.read.chunks(CHUNK).nth(k)?;
                (old == chunk).then(|| b.chunks[k].clone())
            });
            let hash = match same {
                Some(hash) => hash,
                None => {
                    let bytes = serde_json::to_vec(chunk).map_err(|e| {
                        ApiError::internal(format!("retention registration chunk: {e}"))
                    })?;
                    let hash = fluree_db_core::sha256_hex(&bytes);
                    self.storage
                        .write_bytes(&self.address(&hash), &bytes)
                        .await?;
                    hash
                }
            };
            out.push(hash);
        }
        Ok(out)
    }
}

/// How long a chunk no registration names is kept at least. One written for
/// a write not yet applied is named by then, and a replica reading a record
/// that named a superseded one has caught up.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) const CHUNK_GRACE: std::time::Duration = std::time::Duration::from_secs(3600);

/// Every chunk `stored` names, into `out`.
#[cfg(not(target_arch = "wasm32"))]
fn named_in(stored: &serde_json::Value, out: &mut std::collections::HashSet<String>) {
    match stored {
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                match value.as_array() {
                    Some(hashes) if key.ends_with("Chunks") => {
                        out.extend(hashes.iter().filter_map(|h| h.as_str().map(str::to_string)));
                    }
                    _ => named_in(value, out),
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                named_in(value, out);
            }
        }
        _ => {}
    }
}

impl crate::Fluree {
    /// What a reader that cannot read this storage's registry objects (a
    /// query peer through the storage proxy) needs of `ledger_id`'s config:
    /// its retention activation and journal, the journal's chunked removals
    /// inlined, so the reader refuses the history this ledger refuses. None
    /// of the registration's other state, such as erasure requests.
    pub async fn config_for_reader(&self, ledger_id: &str) -> Result<Option<ConfigValue>> {
        let (config, stored) = read_stored(&self.nameservice_mode, ledger_id).await?;
        let Some(config) = config else {
            return Ok(None);
        };
        let Some(stored) = stored else {
            // Even an empty journal carries the revision read with it. A
            // peer may have learned a newer revision from another replica
            // and must not cache an older empty answer indefinitely.
            return Ok(Some(ConfigValue::new(config.v, None)));
        };
        let journal = resolve_journal(&stored, self.registry_objects()).await?;
        let reader = serde_json::json!({
            "activeSinceT": stored.get("activeSinceT").cloned().unwrap_or_default(),
            "journal": serde_json::to_value(&journal)
                .map_err(|e| ApiError::internal(format!("retention journal: {e}")))?,
        });
        let mut payload = fluree_db_nameservice::ConfigPayload::new();
        payload.extra.insert(EXTRA_KEY.to_string(), reader);
        Ok(Some(ConfigValue::new(config.v, Some(payload))))
    }

    /// Where this instance keeps registrations' chunks; `None` without
    /// managed storage, which keeps them inline.
    pub(crate) fn registry_objects(&self) -> Option<Objects<'_>> {
        self.admin_storage().map(|storage| Objects {
            writes: Some(&self.retention.registry_writes),
            #[cfg(feature = "native")]
            condemned: self
                .retention
                .erasure
                .get()
                .map(|runtime| &*runtime.condemned),
            ..Objects::new(storage, &self.retention.chunks)
        })
    }

    /// Every chunk a registration names: a branch's, visible or not (one a
    /// create or restore under way, or stopped, has written), or one a
    /// dropped ledger keeps for its restore.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn named_chunks(&self) -> Result<std::collections::HashSet<String>> {
        let ns = self.publisher()?;
        let mut named = std::collections::HashSet::new();
        for record in ns.all_raw_records().await? {
            if let (_, Some(stored)) = read_stored(ns, record.ledger_id.as_str()).await? {
                named_in(&stored, &mut named);
            }
        }
        for dropped in ns.list_dropped().await? {
            for config in dropped.value.configs.iter().flat_map(|c| c.values()) {
                if let Some(stored) = config.payload.as_ref().and_then(|p| p.extra.get(EXTRA_KEY)) {
                    named_in(stored, &mut named);
                }
            }
        }
        Ok(named)
    }

    /// The addresses of the chunks no registration names, in this listing
    /// and in one at least `grace` before; the others unnamed now are kept
    /// for a later call.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn unnamed_chunks(
        &self,
        objects: &Objects<'_>,
        grace: std::time::Duration,
    ) -> Result<Vec<String>> {
        let prefix = chunk_prefix(objects.storage.storage_method());
        let listed = objects.storage.list_prefix(&prefix).await?;
        let named = self.named_chunks().await?;
        let now = std::time::Instant::now();
        let mut doomed = Vec::new();
        let mut unnamed = self.retention.unnamed_chunks.lock();
        let mut still = std::collections::HashMap::new();
        for address in listed {
            let Some(hash) = chunk_hash(&prefix, &address) else {
                continue;
            };
            if named.contains(hash) {
                continue;
            }
            let since = unnamed.get(hash).copied().unwrap_or(now);
            if now.duration_since(since) >= grace {
                doomed.push(address.clone());
            } else {
                still.insert(hash.to_string(), since);
            }
        }
        *unnamed = still;
        Ok(doomed)
    }

    /// Delete the chunks no registration names that a sweep at least
    /// `grace` earlier also found unnamed; the count deleted. No
    /// registration is written meanwhile. Only an instance that holds its
    /// storage's lock, and is no cluster member, sweeps: another process's
    /// write, or a member's, could name a chunk again between the names
    /// being read and the delete. A cluster's leader collects its chunks
    /// through intents instead (`garbage::cluster`).
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn sweep_registration_chunks(
        &self,
        grace: std::time::Duration,
    ) -> Result<usize> {
        if self.retention.replicas.get().is_some() || self.blob_pins().is_none() {
            return Ok(0);
        }
        let Some(objects) = self.registry_objects() else {
            return Ok(0);
        };
        let _sweeping = self.retention.registry_writes.write().await;
        let doomed = self.unnamed_chunks(&objects, grace).await?;
        #[cfg(test)]
        {
            let hold = self.retention.sweep_hold.lock().clone();
            if let Some((reached, proceed)) = hold {
                reached.notify_one();
                proceed.notified().await;
            }
        }
        let mut deleted = doomed.len();
        for (address, e) in objects.storage.delete_many(&doomed).await {
            deleted -= 1;
            tracing::warn!(%address, error = %e, "retention: registration chunk not deleted");
        }
        Ok(deleted)
    }
}

/// Where registration chunks live, for a storage method.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn chunk_prefix(method: &str) -> String {
    format!("fluree:{method}://{OBJECTS_DIR}/")
}

/// The hash a chunk's address under `prefix` names.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn chunk_hash<'a>(prefix: &str, address: &'a str) -> Option<&'a str> {
    address
        .strip_prefix(prefix)
        .and_then(|file| file.strip_suffix(".json"))
}

fn unreadable() -> ApiError {
    ApiError::internal(
        "the retention registration names chunks this instance cannot read: it has no managed \
         storage",
    )
}

fn chunk_refs(value: Option<&serde_json::Value>) -> Result<Option<Vec<String>>> {
    value
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()
        .map_err(|e| ApiError::internal(format!("retention registration chunks: {e}")))
}

/// The journal's identity as `stored` names it: equal for equal journals,
/// without reading its chunks.
pub(crate) fn journal_identity(stored: &serde_json::Value) -> String {
    let journal = stored.get("journal").cloned().unwrap_or_default();
    fluree_db_core::sha256_hex(&serde_json::to_vec(&journal).unwrap_or_default())
}

/// Whether `stored`'s journal suppresses nothing, without reading its chunks.
pub(crate) fn journal_empty(stored: &serde_json::Value) -> bool {
    let empty = |key: &str| {
        stored
            .pointer(&format!("/journal/{key}"))
            .and_then(|v| v.as_array())
            .is_none_or(Vec::is_empty)
    };
    empty("historyRules") && empty("removals") && empty("removalChunks")
}

/// The journal `stored` holds, its removals read from their chunks.
pub(crate) async fn resolve_journal(
    stored: &serde_json::Value,
    objects: Option<Objects<'_>>,
) -> Result<Journal> {
    let mut journal: Journal = match stored.get("journal") {
        Some(journal) => serde_json::from_value(journal.clone())
            .map_err(|e| ApiError::internal(format!("retention journal: {e}")))?,
        None => Journal::default(),
    };
    if let Some(refs) = chunk_refs(stored.pointer("/journal/removalChunks"))? {
        let listed = objects
            .ok_or_else(unreadable)?
            .read_list::<Removal>(refs)
            .await?;
        journal.removals = listed.read.as_ref().clone();
    }
    Ok(journal)
}

/// The registration `stored` holds without its chunked lists: its scalars,
/// journal generation and rules, and job summaries, for a check that needs
/// no more.
pub(crate) fn shell(stored: &serde_json::Value) -> Result<Registration> {
    serde_json::from_value(stored.clone())
        .map_err(|e| ApiError::internal(format!("retention registration: {e}")))
}

/// The registration `stored` holds, its lists read from the chunks it names.
pub(crate) async fn resolve(
    stored: &serde_json::Value,
    objects: Option<Objects<'_>>,
) -> Result<Registration> {
    let mut registration: Registration = serde_json::from_value(stored.clone())
        .map_err(|e| ApiError::internal(format!("retention registration: {e}")))?;
    let mut chunks = Chunks::default();
    if let Some(refs) = chunk_refs(stored.pointer("/journal/removalChunks"))? {
        let listed = objects.ok_or_else(unreadable)?.read_list(refs).await?;
        registration.journal.removals = listed.read.as_ref().clone();
        chunks.removals = Some(listed);
    }
    #[cfg(feature = "native")]
    if let Some(jobs) = stored.get("erasures").and_then(|e| e.as_array()) {
        for (stored, job) in jobs.iter().zip(registration.erasures.iter_mut()) {
            // A later release's, kept as written.
            if job.as_written.is_some() {
                continue;
            }
            let mut lists = JobLists::default();
            if let Some(refs) = chunk_refs(stored.get("commitChunks"))? {
                let listed = objects.ok_or_else(unreadable)?.read_list(refs).await?;
                job.commits = listed.read.as_ref().clone();
                lists.commits = Some(listed);
            }
            if let Some(refs) = chunk_refs(stored.get("addedChunks"))? {
                let listed = objects.ok_or_else(unreadable)?.read_list(refs).await?;
                job.added = listed.read.as_ref().clone();
                lists.added = Some(listed);
            }
            chunks.jobs.insert(job.request_id.clone(), lists);
        }
    }
    registration.named = Named(Some(Arc::new(chunks)));
    Ok(registration)
}

/// `registration`'s stored form: inline without `objects`, else with its
/// lists as chunks, those equal to what `current`, the record it replaces,
/// names named again. Only that record's names are reused: it is the one the
/// write's compare-and-set expects, so its chunks are not yet swept.
async fn store(
    registration: &mut Registration,
    current: Option<&Registration>,
    objects: Option<Objects<'_>>,
) -> Result<serde_json::Value> {
    let to_value = |r: &Registration| {
        serde_json::to_value(r)
            .map_err(|e| ApiError::internal(format!("retention registration: {e}")))
    };
    let Some(objects) = objects else {
        return to_value(registration);
    };
    let named = current.and_then(|c| c.named.0.clone()).unwrap_or_default();
    let removals = std::mem::take(&mut registration.journal.removals);
    #[cfg(feature = "native")]
    let lists: Vec<Option<(Vec<super::erasure::CommitErasure>, Vec<OccurrenceRef>)>> = registration
        .erasures
        .iter_mut()
        .map(|job| {
            job.as_written.is_none().then(|| {
                (
                    std::mem::take(&mut job.commits),
                    std::mem::take(&mut job.added),
                )
            })
        })
        .collect();
    let shell = to_value(registration);
    let written = async {
        let removal_chunks = objects
            .write_list(&removals, named.removals.as_ref())
            .await?;
        #[cfg(feature = "native")]
        let mut job_chunks = Vec::with_capacity(lists.len());
        #[cfg(feature = "native")]
        for (job, lists) in registration.erasures.iter().zip(&lists) {
            let Some((commits, added)) = lists else {
                job_chunks.push(None);
                continue;
            };
            let before = named.jobs.get(&job.request_id);
            let commits = objects
                .write_list(commits, before.and_then(|b| b.commits.as_ref()))
                .await?;
            let added = objects
                .write_list(added, before.and_then(|b| b.added.as_ref()))
                .await?;
            job_chunks.push(Some((commits, added)));
        }
        #[cfg(not(feature = "native"))]
        let job_chunks: Vec<Option<(Vec<String>, Vec<String>)>> = Vec::new();
        Ok::<_, ApiError>((removal_chunks, job_chunks))
    }
    .await;
    // Restored before any error returns.
    registration.journal.removals = removals;
    #[cfg(feature = "native")]
    for (job, lists) in registration.erasures.iter_mut().zip(lists) {
        if let Some((commits, added)) = lists {
            job.commits = commits;
            job.added = added;
        }
    }
    let (removal_chunks, job_chunks) = written?;
    let mut value = shell?;
    if !removal_chunks.is_empty() {
        // Without its removals the journal may read as empty, which
        // serializing skips; its generation and rules are kept all the same.
        let mut journal = serde_json::to_value(Journal {
            generation: registration.journal.generation,
            history_rules: registration.journal.history_rules.clone(),
            removals: Vec::new(),
        })
        .map_err(|e| ApiError::internal(format!("retention registration: {e}")))?;
        journal["removalChunks"] = serde_json::json!(removal_chunks);
        value["journal"] = journal;
    }
    if let Some(jobs) = value.get_mut("erasures").and_then(|e| e.as_array_mut()) {
        for (stored, chunks) in jobs.iter_mut().zip(job_chunks) {
            let Some((commits, added)) = chunks else {
                continue;
            };
            if !commits.is_empty() {
                stored["commitChunks"] = serde_json::json!(commits);
            }
            if !added.is_empty() {
                stored["addedChunks"] = serde_json::json!(added);
            }
        }
    }
    Ok(value)
}

/// The branch's stored registration, as its config record holds it, with
/// that record.
pub(crate) async fn read_stored<N: fluree_db_nameservice::ConfigLookup + ?Sized>(
    ns: &N,
    ledger_id: &str,
) -> Result<(Option<ConfigValue>, Option<serde_json::Value>)> {
    let config = ns.get_config(ledger_id).await?;
    let stored = config
        .as_ref()
        .and_then(|c| c.payload.as_ref())
        .and_then(|p| p.extra.get(EXTRA_KEY))
        .cloned();
    Ok((config, stored))
}

/// The branch's registration, if any, with the config value it was read from.
pub(crate) async fn read<N: fluree_db_nameservice::ConfigLookup + ?Sized>(
    ns: &N,
    objects: Option<Objects<'_>>,
    ledger_id: &str,
) -> Result<(Option<ConfigValue>, Option<Registration>)> {
    let (config, stored) = read_stored(ns, ledger_id).await?;
    let registration = match stored {
        Some(stored) => Some(resolve(&stored, objects).await?),
        None => None,
    };
    Ok((config, registration))
}

/// Update the branch's registration with `update`, which sees the current
/// one and returns the new one (`None` leaves the record unchanged).
/// Retries a lost compare-and-set against the record as it then is.
pub(crate) async fn update(
    ns: &dyn NameServicePublisher,
    objects: Option<Objects<'_>>,
    ledger_id: &str,
    update: impl FnMut(Option<&Registration>) -> Option<Registration>,
) -> Result<Option<Registration>> {
    let Some(record) = ns.lookup(ledger_id).await? else {
        return Err(ApiError::NotFound(ledger_id.to_string()));
    };
    update_fenced(ns, objects, ledger_id, record.fence, update).await
}

/// [`update`] under `fence`, for a record not yet visible to lookups (a
/// ledger being restored).
pub(crate) async fn update_fenced(
    ns: &dyn NameServicePublisher,
    objects: Option<Objects<'_>>,
    ledger_id: &str,
    fence: Option<fluree_db_nameservice::Fence>,
    mut update: impl FnMut(Option<&Registration>) -> Option<Registration>,
) -> Result<Option<Registration>> {
    for _ in 0..CAS_ATTEMPTS {
        let attempt = async {
            let (config, current) = read(ns, objects, ledger_id).await?;
            let Some(mut next) = update(current.as_ref()) else {
                return Result::Ok(Some(current));
            };
            // No sweep deletes a chunk between this write storing or naming
            // it and its record taking effect.
            let _writing = match objects.and_then(|o| o.writes) {
                Some(writes) => Some(writes.read().await),
                None => None,
            };
            let value = store(&mut next, current.as_ref(), objects).await?;
            let (v, mut payload) = match &config {
                Some(c) => (c.v, c.payload.clone().unwrap_or_default()),
                None => (0, ConfigPayload::default()),
            };
            payload.extra.insert(EXTRA_KEY.to_string(), value);
            let new = ConfigValue::new(v + 1, Some(payload));
            match ns
                .push_config_fenced(ledger_id, fence, config.as_ref(), &new)
                .await
            {
                Ok(ConfigCasResult::Updated) => Ok(Some(Some(next))),
                Ok(ConfigCasResult::Conflict { .. }) => Ok(None),
                // Its chunks were stored before collection deleted what it
                // condemned: written again, in a new scope.
                Err(fluree_db_nameservice::NameServiceError::ApplyLagged(_)) => Ok(None),
                Err(e) => Err(e.into()),
            }
        };
        #[cfg(feature = "native")]
        let outcome = match objects.and_then(|o| o.condemned) {
            Some(condemned) => condemned.scoped(attempt).await,
            None => attempt.await,
        };
        #[cfg(not(feature = "native"))]
        let outcome = attempt.await;
        if let Some(done) = outcome? {
            return Ok(done);
        }
    }
    Err(ApiError::internal(format!(
        "retention registration for {ledger_id}: compare-and-set kept losing"
    )))
}

/// Give a new branch its source's retention: its activation boundary and its
/// journal, which the history the branch shares still needs. Nothing when
/// the source has no registration.
pub(crate) async fn inherit(
    ns: &dyn NameServicePublisher,
    objects: Option<Objects<'_>>,
    source_id: &str,
    branch_id: &str,
    fence: Option<fluree_db_nameservice::Fence>,
) -> Result<()> {
    let Some(source) = read(ns, objects, source_id).await?.1 else {
        return Ok(());
    };
    let mut conflict = None;
    update_fenced(ns, objects, branch_id, fence, |current| {
        let mut next = current.cloned().unwrap_or_else(|| Registration {
            active_since_t: source.active_since_t,
            watermark_t: source.active_since_t - 1,
            ..Default::default()
        });
        if let Err(e) = next.journal.merge(&source.journal) {
            conflict = Some(e);
            return None;
        }
        next.lineage = next.lineage.clone().or_else(|| source.lineage.clone());
        Some(next)
    })
    .await?;
    conflict.map_or(Ok(()), |e| Err(e.into()))
}

/// Register the branch as active from `since_t`, keeping an earlier boundary
/// and everything else an existing registration holds.
pub(crate) async fn activate(
    ns: &dyn NameServicePublisher,
    objects: Option<Objects<'_>>,
    ledger_id: &str,
    since_t: i64,
) -> Result<()> {
    update(ns, objects, ledger_id, |current| match current {
        Some(r) if r.active_since_t <= since_t => None,
        Some(r) => Some(Registration {
            active_since_t: since_t,
            // An earlier boundary means commits the schedule never saw.
            watermark_t: r.watermark_t.min(since_t - 1),
            ..r.clone()
        }),
        None => Some(Registration {
            active_since_t: since_t,
            watermark_t: since_t - 1,
            ..Default::default()
        }),
    })
    .await
    .map(|_| ())
}

/// What a ledger archive carries of the source's retention: its activation
/// boundary and removal journal, so a restored copy serves no history the
/// source no longer served.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ArchivedRetention {
    pub(crate) active_since_t: i64,
    #[serde(default)]
    pub(crate) journal: super::journal::Journal,
}

impl ArchivedRetention {
    pub(crate) fn parse(archived: &serde_json::Value) -> Result<Self> {
        serde_json::from_value(archived.clone())
            .map_err(|e| ApiError::http(400, format!(".flpack retention section: {e}")))
    }
}

/// The archive's retention section for `ledger_id`, if it is registered.
pub(crate) async fn archived<N: fluree_db_nameservice::ConfigLookup + ?Sized>(
    ns: &N,
    objects: Option<Objects<'_>>,
    ledger_id: &str,
) -> Result<Option<serde_json::Value>> {
    let Some(registration) = read(ns, objects, ledger_id).await?.1 else {
        return Ok(None);
    };
    let archived = ArchivedRetention {
        active_since_t: registration.active_since_t,
        journal: registration.journal,
    };
    serde_json::to_value(archived)
        .map(Some)
        .map_err(|e| ApiError::internal(format!("retention registration: {e}")))
}

/// Register a restored ledger: its archive's retention section (if any),
/// with the journal the lineage's checkpoint holds merged in, so a copy
/// restored from an archive made before a removal still refuses it. Its
/// expiry schedule is rebuilt from its commits, and its index history
/// compacted again. Nothing is registered when neither has anything. A
/// registration the restore already reinstated (a soft-dropped ledger's)
/// is kept, and takes what the journal adds to its own.
pub(crate) async fn restore(
    ns: &dyn NameServicePublisher,
    objects: Option<Objects<'_>>,
    record: &fluree_db_nameservice::NsRecord,
    archived: Option<ArchivedRetention>,
    lineage: Option<&str>,
    checkpoint: Option<super::journal::Journal>,
) -> Result<Option<Registration>> {
    let mut journal = archived
        .as_ref()
        .map(|a| a.journal.clone())
        .unwrap_or_default();
    if let Some(checkpoint) = &checkpoint {
        journal.merge(checkpoint)?;
    }
    let active_since_t = archived.as_ref().map_or(1, |a| a.active_since_t);
    let fresh = (archived.is_some() || !journal.is_empty()).then(|| Registration {
        active_since_t,
        watermark_t: active_since_t - 1,
        journal: journal.clone(),
        lineage: lineage.map(str::to_string),
        ..Default::default()
    });
    let mut conflict = None;
    let registration = update_fenced(
        ns,
        objects,
        record.ledger_id.as_str(),
        record.fence,
        |current| match current {
            Some(current) => {
                let mut next = current.clone();
                let merged = match next.journal.merge(&journal) {
                    Ok(merged) => merged,
                    Err(e) => {
                        conflict = Some(e);
                        return None;
                    }
                };
                let adopted = next.lineage.is_none() && lineage.is_some();
                if adopted {
                    next.lineage = lineage.map(str::to_string);
                }
                (merged || adopted).then_some(next)
            }
            None => fresh.clone(),
        },
    )
    .await?;
    conflict.map_or(Ok(registration), |e| Err(e.into()))
}

#[cfg(all(test, feature = "native"))]
mod tests {
    use super::*;
    use crate::retention::erasure::ErasureJob;
    use crate::{Fluree, FlureeBuilder};

    fn removal(n: usize) -> Removal {
        Removal {
            request_id: "removal-1".into(),
            commit: format!("commit-{n}"),
            slot: n as u32,
            predicate: "http://example.org/email".into(),
            from_t: 1,
            until_t: Some(2),
            scope_t: 2,
            subject: None,
        }
    }

    fn job() -> ErasureJob {
        let commits: Vec<_> = (0..3)
            .map(|n| {
                serde_json::json!({
                    "commit": format!("commit-{n}"), "t": 1, "slots": [0],
                    "ledgerId": "t/chunks:main", "branches": ["t/chunks:main"],
                })
            })
            .collect();
        serde_json::from_value(serde_json::json!({
            "requestId": "erasure-1", "acceptedMs": 1, "authorizedT": 1,
            "stage": "proving", "commits": commits,
            "added": [{"commit": "commit-0", "slot": 1}],
        }))
        .unwrap()
    }

    /// A reader without audit access sees no removed occurrence and no
    /// requester: neither the journal's removals nor an erasure job's
    /// commits, slots, properties or requester.
    #[test]
    fn a_status_without_audit_data_keeps_counts_and_stages() {
        let mut job = job();
        job.requester = Some("did:key:alice".into());
        job.properties = vec!["http://example.org/email".into()];
        let mut registration = Registration::default();
        registration.journal.removals = vec![removal(0), removal(1)];
        registration.erasures = vec![job];
        let status = crate::retention::RetentionStatus {
            ledger_id: "t/chunks:main".into(),
            active: true,
            registration: Some(registration),
            maintenance_running: false,
            overdue: None,
            removals: 2,
        }
        .without_audit_data();
        let shown = serde_json::to_value(&status).unwrap();
        let job = &shown["registration"]["erasures"][0];
        assert_eq!(shown["removals"], 2);
        assert!(shown["registration"]["journal"].is_null(), "{shown}");
        assert_eq!(job["stage"], "proving");
        for audit in ["requester", "properties", "added", "authorizedCommit"] {
            assert!(job[audit].is_null(), "{audit}: {shown}");
        }
        assert_eq!(job["commits"], serde_json::json!([]), "{shown}");
    }

    async fn ledger(name: &str) -> (tempfile::TempDir, Fluree, String) {
        let dir = tempfile::tempdir().unwrap();
        let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .without_indexing()
            .build()
            .unwrap();
        fluree.create_ledger(name).await.unwrap();
        (dir, fluree, format!("{name}:main"))
    }

    async fn chunk_objects(fluree: &Fluree) -> Vec<String> {
        let storage = fluree.admin_storage().unwrap();
        let mut objects = storage
            .list_prefix(&format!(
                "fluree:{}://{OBJECTS_DIR}/",
                storage.storage_method()
            ))
            .await
            .unwrap();
        objects.sort();
        objects
    }

    /// A registration's journal and erasure jobs are kept as chunks its
    /// record names. They read back as written; a write changing neither
    /// stores no chunk and leaves the journal's identity; one appending a
    /// removal stores only its last chunk again.
    #[tokio::test]
    async fn growing_lists_are_kept_as_chunks_the_record_names() {
        let (_dir, fluree, id) = ledger("t/chunks").await;
        let (ns, objects) = (fluree.publisher().unwrap(), fluree.registry_objects());
        let removals: Vec<Removal> = (0..CHUNK + 10).map(removal).collect();
        let written = Registration {
            active_since_t: 1,
            journal: Journal {
                generation: 1,
                history_rules: Vec::new(),
                removals: removals.clone(),
            },
            erasures: vec![job()],
            ..Default::default()
        };
        update(ns, objects, &id, |_| Some(written.clone()))
            .await
            .unwrap();

        let stored = read_stored(ns, &id).await.unwrap().1.unwrap();
        let journal_chunks = |stored: &serde_json::Value| -> Vec<String> {
            serde_json::from_value(stored["journal"]["removalChunks"].clone()).unwrap()
        };
        assert_eq!(journal_chunks(&stored).len(), 2);
        assert!(stored["journal"].get("removals").is_none(), "{stored}");
        assert_eq!(stored["journal"]["generation"], 1);
        assert_eq!(stored["erasures"][0]["commits"], serde_json::json!([]));
        assert_eq!(
            stored["erasures"][0]["commitChunks"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            stored["erasures"][0]["addedChunks"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let read_back = read(ns, objects, &id).await.unwrap().1.unwrap();
        assert_eq!(read_back, written);

        let chunks = chunk_objects(&fluree).await;
        let identity = journal_identity(&stored);
        update(ns, objects, &id, |current| {
            Some(Registration {
                pending: 7,
                ..current.unwrap().clone()
            })
        })
        .await
        .unwrap();
        assert_eq!(chunk_objects(&fluree).await, chunks);
        let unchanged = read_stored(ns, &id).await.unwrap().1.unwrap();
        assert_eq!(journal_identity(&unchanged), identity);

        update(ns, objects, &id, |current| {
            let mut next = current.unwrap().clone();
            next.journal.removals.push(removal(9_999));
            next.journal.generation += 1;
            Some(next)
        })
        .await
        .unwrap();
        assert_eq!(chunk_objects(&fluree).await.len(), chunks.len() + 1);
        let appended = read_stored(ns, &id).await.unwrap().1.unwrap();
        let (before, after) = (journal_chunks(&unchanged), journal_chunks(&appended));
        assert_eq!(before[0], after[0], "the first chunk is named again");
        assert_ne!(before[1], after[1]);
        assert_ne!(journal_identity(&appended), identity);
        let read_back = read(ns, objects, &id).await.unwrap().1.unwrap();
        assert_eq!(read_back.journal.removals.len(), CHUNK + 11);
    }

    /// A registration stored inline, as without managed storage, reads as
    /// before and is stored in chunks on its next change.
    #[tokio::test]
    async fn an_inline_registration_is_chunked_on_its_next_change() {
        let (_dir, fluree, id) = ledger("t/inline").await;
        let (ns, objects) = (fluree.publisher().unwrap(), fluree.registry_objects());
        let written = Registration {
            active_since_t: 1,
            journal: Journal {
                generation: 1,
                history_rules: Vec::new(),
                removals: vec![removal(0)],
            },
            erasures: vec![job()],
            ..Default::default()
        };
        update(ns, None, &id, |_| Some(written.clone()))
            .await
            .unwrap();
        let stored = read_stored(ns, &id).await.unwrap().1.unwrap();
        assert_eq!(stored["journal"]["removals"].as_array().unwrap().len(), 1);
        assert!(chunk_objects(&fluree).await.is_empty());
        assert_eq!(read(ns, objects, &id).await.unwrap().1.unwrap(), written);

        update(ns, objects, &id, |current| {
            Some(Registration {
                pending: 1,
                ..current.unwrap().clone()
            })
        })
        .await
        .unwrap();
        let stored = read_stored(ns, &id).await.unwrap().1.unwrap();
        assert!(stored["journal"].get("removals").is_none(), "{stored}");
        assert_eq!(chunk_objects(&fluree).await.len(), 3);
        let read_back = read(ns, objects, &id).await.unwrap().1.unwrap();
        assert_eq!(read_back.journal, written.journal);
        assert_eq!(read_back.erasures, written.erasures);
    }

    /// A chunk equal to the one read at its position is named again, not
    /// serialized or stored; a changed one is stored.
    #[tokio::test]
    async fn an_unchanged_chunk_is_named_again_without_being_stored() {
        let storage = fluree_db_core::MemoryStorage::new();
        let cache = ChunkCache::default();
        let objects = Objects::new(&storage, &cache);
        let items: Vec<Removal> = (0..=CHUNK).map(removal).collect();
        let before = Listed {
            chunks: vec!["as-read-0".into(), "as-read-1".into()],
            read: Arc::new(items.clone()),
        };
        let mut next = items;
        next[CHUNK].slot += 1;
        let chunks = objects.write_list(&next, Some(&before)).await.unwrap();
        assert_eq!(chunks[0], "as-read-0");
        assert_ne!(chunks[1], "as-read-1");
        let storage: &dyn Storage = &storage;
        let stored = storage
            .list_prefix(&format!(
                "fluree:{}://{OBJECTS_DIR}/",
                storage.storage_method()
            ))
            .await
            .unwrap();
        assert_eq!(stored.len(), 1, "{stored:?}");
    }

    /// A chunk no registration names is deleted by a sweep at least the
    /// grace after the first that found it unnamed. Those a branch's or a
    /// dropped ledger's registration names stay.
    #[tokio::test]
    async fn superseded_chunks_are_swept_after_the_grace() {
        let (_dir, fluree, id) = ledger("t/swept").await;
        let (ns, objects) = (fluree.publisher().unwrap(), fluree.registry_objects());
        let registered = |removals: Vec<Removal>| Registration {
            active_since_t: 1,
            journal: Journal {
                generation: 1,
                history_rules: Vec::new(),
                removals,
            },
            ..Default::default()
        };
        fluree.create_ledger("t/kept").await.unwrap();
        let kept_id = "t/kept:main";
        update(ns, objects, kept_id, |_| Some(registered(vec![removal(1)])))
            .await
            .unwrap();
        let kept = read_stored(ns, kept_id).await.unwrap().1.unwrap();
        let kept_chunk = kept["journal"]["removalChunks"][0]
            .as_str()
            .unwrap()
            .to_string();
        fluree
            .drop_ledger("t/kept", crate::DropMode::Soft)
            .await
            .unwrap();
        update(ns, objects, &id, |_| Some(registered(vec![removal(0)])))
            .await
            .unwrap();
        update(ns, objects, &id, |current| {
            let mut next = current.unwrap().clone();
            next.journal.removals.push(removal(2));
            Some(next)
        })
        .await
        .unwrap();
        assert_eq!(chunk_objects(&fluree).await.len(), 3);

        let grace = std::time::Duration::from_millis(50);
        assert_eq!(fluree.sweep_registration_chunks(grace).await.unwrap(), 0);
        tokio::time::sleep(grace).await;
        assert_eq!(fluree.sweep_registration_chunks(grace).await.unwrap(), 1);
        let left = chunk_objects(&fluree).await;
        assert_eq!(left.len(), 2);
        assert!(left.iter().any(|a| a.contains(&kept_chunk)), "{left:?}");
        let read_back = read(ns, objects, &id).await.unwrap().1.unwrap();
        assert_eq!(read_back.journal.removals, vec![removal(0), removal(2)]);
    }

    /// On a cluster the leader's collection condemns a chunk no registration
    /// has named across the grace, and deletes it a pass later unless a
    /// registration names it again meanwhile.
    #[tokio::test]
    async fn a_cluster_collects_superseded_chunks_through_intents() {
        use crate::garbage::tests::cluster::{pass, Cluster};

        let (_dir, fluree, id) = ledger("t/clustered").await;
        let cluster = Arc::new(Cluster::default());
        fluree.set_collector(cluster.clone());
        let (ns, objects) = (fluree.publisher().unwrap(), fluree.registry_objects());
        let registered = |removals: Vec<Removal>| Registration {
            active_since_t: 1,
            journal: Journal {
                generation: 1,
                history_rules: Vec::new(),
                removals,
            },
            ..Default::default()
        };
        update(ns, objects, &id, |_| Some(registered(vec![removal(0)])))
            .await
            .unwrap();
        update(ns, objects, &id, |current| {
            let mut next = current.unwrap().clone();
            next.journal.removals.push(removal(2));
            Some(next)
        })
        .await
        .unwrap();
        assert_eq!(chunk_objects(&fluree).await.len(), 2);

        let first = pass(&fluree, &cluster).await;
        assert_eq!(first.condemned, 1, "{first:?}");
        // Named again before deletion begins: kept.
        update(ns, objects, &id, |_| Some(registered(vec![removal(0)])))
            .await
            .unwrap();
        let second = pass(&fluree, &cluster).await;
        assert_eq!((second.deleted, second.resolved), (0, 1), "{second:?}");
        assert_eq!(chunk_objects(&fluree).await.len(), 2);
        let read_back = read(ns, objects, &id).await.unwrap().1.unwrap();
        assert_eq!(read_back.journal.removals, vec![removal(0)]);

        // The longer one, superseded in turn, was condemned by that pass.
        assert_eq!(second.condemned, 1, "{second:?}");
        let third = pass(&fluree, &cluster).await;
        assert_eq!(third.deleted, 1, "{third:?}");
        assert_eq!(chunk_objects(&fluree).await.len(), 1);
    }

    /// A registration read before its chunks were superseded and swept, then
    /// written back, stores them again: a write names again only the chunks
    /// of the record it replaces.
    #[tokio::test]
    async fn a_registration_written_back_after_a_sweep_stores_its_chunks_again() {
        let (_dir, fluree, id) = ledger("t/stale").await;
        let (ns, objects) = (fluree.publisher().unwrap(), fluree.registry_objects());
        update(ns, objects, &id, |_| {
            Some(Registration {
                active_since_t: 1,
                journal: Journal {
                    generation: 1,
                    history_rules: Vec::new(),
                    removals: vec![removal(0)],
                },
                ..Default::default()
            })
        })
        .await
        .unwrap();
        let stale = read(ns, objects, &id).await.unwrap().1.unwrap();
        update(ns, objects, &id, |current| {
            let mut next = current.unwrap().clone();
            next.journal.removals = vec![removal(1)];
            Some(next)
        })
        .await
        .unwrap();
        let grace = std::time::Duration::from_millis(20);
        fluree.sweep_registration_chunks(grace).await.unwrap();
        tokio::time::sleep(grace).await;
        assert_eq!(fluree.sweep_registration_chunks(grace).await.unwrap(), 1);

        update(ns, objects, &id, |_| Some(stale.clone()))
            .await
            .unwrap();
        // Read as another process would, without the chunks this one holds.
        let fresh = ChunkCache::default();
        let elsewhere = Some(Objects::new(fluree.admin_storage().unwrap(), &fresh));
        let read_back = read(ns, elsewhere, &id).await.unwrap().1.unwrap();
        assert_eq!(read_back.journal, stale.journal);
    }

    /// A sweep and a registration write exclude each other: a write naming
    /// again a chunk the sweep is about to delete waits for the delete, then
    /// stores the chunk again.
    #[tokio::test]
    async fn a_write_naming_a_chunk_being_swept_waits_and_stores_it_again() {
        let (_dir, fluree, id) = ledger("t/race").await;
        let fluree = Arc::new(fluree);
        let write = |removals: Vec<Removal>| {
            let fluree = Arc::clone(&fluree);
            let id = id.clone();
            async move {
                update(
                    fluree.publisher().unwrap(),
                    fluree.registry_objects(),
                    &id,
                    |current| {
                        let mut next = current.cloned().unwrap_or_default();
                        next.journal.removals = removals.clone();
                        Some(next)
                    },
                )
                .await
                .map(|_| ())
            }
        };
        write(vec![removal(0)]).await.unwrap();
        write(vec![removal(1)]).await.unwrap();
        let grace = std::time::Duration::from_millis(20);
        assert_eq!(fluree.sweep_registration_chunks(grace).await.unwrap(), 0);
        tokio::time::sleep(grace).await;

        let (reached, proceed) = (
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(tokio::sync::Notify::new()),
        );
        *fluree.retention.sweep_hold.lock() = Some((Arc::clone(&reached), Arc::clone(&proceed)));
        let sweep = {
            let fluree = Arc::clone(&fluree);
            tokio::spawn(async move { fluree.sweep_registration_chunks(grace).await })
        };
        reached.notified().await;
        let named_again = tokio::spawn(write(vec![removal(0)]));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            !named_again.is_finished(),
            "a write named the chunk while the sweep was about to delete it"
        );
        *fluree.retention.sweep_hold.lock() = None;
        proceed.notify_one();
        assert_eq!(sweep.await.unwrap().unwrap(), 1);
        named_again.await.unwrap().unwrap();

        let fresh = ChunkCache::default();
        let elsewhere = Some(Objects::new(fluree.admin_storage().unwrap(), &fresh));
        let ns = fluree.publisher().unwrap();
        let read_back = read(ns, elsewhere, &id).await.unwrap().1.unwrap();
        assert_eq!(read_back.journal.removals, vec![removal(0)]);
    }

    /// A pass with nothing to do reads no chunk: its registration's scalars
    /// and journal identity decide it.
    #[tokio::test]
    async fn an_idle_retention_pass_reads_no_chunk() {
        let (_dir, fluree, id) = ledger("t/idle").await;
        fluree
            .graph(&id)
            .transact()
            .insert(&serde_json::json!({
                "@id": "http://example.org/alice",
                "http://example.org/email": "a@secret"
            }))
            .commit()
            .await
            .unwrap();
        fluree
            .remove_occurrences(
                &id,
                super::super::removal::RemovalRequest {
                    matching: vec![super::super::FactSelector {
                        subject: "http://example.org/alice".into(),
                        predicate: "http://example.org/email".into(),
                        graph: None,
                        object: Some(serde_json::json!("a@secret")),
                    }],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        fluree.run_retention(&id).await.unwrap();
        fluree.run_retention(&id).await.unwrap();
        let ns = fluree.publisher().unwrap();
        let stored = read_stored(ns, &id).await.unwrap().1.unwrap();
        assert!(
            stored.pointer("/journal/removalChunks").is_some(),
            "{stored}"
        );

        // Gone from storage and from this process: a pass reading them fails.
        let storage = fluree.admin_storage().unwrap();
        for address in chunk_objects(&fluree).await {
            storage.delete(&address).await.unwrap();
        }
        fluree.retention.chunks.clear();
        fluree.run_retention(&id).await.unwrap();
    }

    struct Member;

    #[async_trait::async_trait]
    impl crate::retention::replicas::ReplicaSync for Member {
        async fn sync_reads(&self) -> std::result::Result<(), String> {
            Ok(())
        }

        async fn await_replicas(&self, _: std::time::Duration) -> Vec<String> {
            Vec::new()
        }
    }

    /// A cluster member does not sweep: another member's write is not
    /// ordered with its delete yet.
    #[tokio::test]
    async fn a_cluster_member_does_not_sweep() {
        let (_dir, fluree, id) = ledger("t/member").await;
        let (ns, objects) = (fluree.publisher().unwrap(), fluree.registry_objects());
        for n in 0..2 {
            update(ns, objects, &id, |current| {
                let mut next = current.cloned().unwrap_or_default();
                next.journal.removals = vec![removal(n)];
                Some(next)
            })
            .await
            .unwrap();
        }
        fluree.set_retention_replicas(Arc::new(Member));
        let grace = std::time::Duration::from_millis(1);
        fluree.sweep_registration_chunks(grace).await.unwrap();
        tokio::time::sleep(grace).await;
        assert_eq!(fluree.sweep_registration_chunks(grace).await.unwrap(), 0);
        assert_eq!(chunk_objects(&fluree).await.len(), 2);
    }
}
