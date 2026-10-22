//! The journal checkpoint each lineage keeps outside its ledgers.
//!
//! A ledger's journal lives in its nameservice record, which a drop or purge
//! removes and which an archive copies as of when it was made. So that a
//! copy restored from an older archive, or after its source is gone, still
//! refuses what was suppressed, every journal change is also published to a
//! checkpoint keyed by the lineage — the genesis commit every copy of the
//! ledger shares — under `@retention/journal/<lineage>/`, outside any
//! ledger's folder: drops, purges and the orphan sweep leave it, and ledger
//! backups do not hold it. A restore merges it in before the copy serves.
//!
//! The checkpoint is the union of immutable objects, each named by the hash
//! of its bytes: a write adds what the union lacks of its ledger's journal
//! as a new object, so copies writing at once cannot lose each other's
//! entries. A write that finds many objects replaces them with their union,
//! deleting only objects it has read.

use std::collections::HashMap;

use fluree_db_core::{
    collect_first_parent_cids, load_commit_by_id, ContentId, ContentStore, Flake, Storage,
};
use fluree_db_nameservice::NsRecord;
use sha2::{Digest, Sha256};

use super::journal::{Journal, Removal};
use super::registry::{self, Registration};
use crate::{ApiError, Fluree, Result};

const CHECKPOINT_DIR: &str = "@retention/journal";

/// Objects a write finds before it replaces them with their union.
const COMPACT_AT: usize = 16;

/// Listings a read retries when an object it listed is compacted away
/// before it is read.
const READ_ATTEMPTS: usize = 4;

fn checkpoint_prefix(storage: &dyn Storage, lineage: &ContentId) -> String {
    format!(
        "fluree:{}://{CHECKPOINT_DIR}/{}/",
        storage.storage_method(),
        lineage.address_digest()
    )
}

impl Fluree {
    fn checkpoint_storage(&self) -> Result<&dyn Storage> {
        self.admin_storage().ok_or_else(|| {
            ApiError::config("the retention journal checkpoint needs a managed storage backend")
        })
    }

    /// The checkpoint for `lineage`, `None` when it has none. An object that
    /// cannot be read or parsed is an error, not an absence.
    pub(crate) async fn journal_checkpoint(&self, lineage: &ContentId) -> Result<Option<Journal>> {
        Ok(self
            .read_checkpoint(lineage)
            .await?
            .map(|(journal, _)| journal))
    }

    /// The checkpoint's union and the objects it was read from.
    async fn read_checkpoint(&self, lineage: &ContentId) -> Result<Option<(Journal, Vec<String>)>> {
        let storage = self.checkpoint_storage()?;
        let prefix = checkpoint_prefix(storage, lineage);
        let unreadable = |address: &str, e: String| {
            ApiError::internal(format!("journal checkpoint {address}: {e}"))
        };
        'listing: for _ in 0..READ_ATTEMPTS {
            let addresses = storage.list_prefix(&prefix).await?;
            if addresses.is_empty() {
                return Ok(None);
            }
            let mut union = Journal::default();
            for address in &addresses {
                let bytes = match storage.read_bytes(address).await {
                    Ok(bytes) => bytes,
                    Err(fluree_db_core::Error::NotFound(_)) => continue 'listing,
                    Err(e) => return Err(unreadable(address, e.to_string())),
                };
                let part: Journal = serde_json::from_slice(&bytes)
                    .map_err(|e| unreadable(address, e.to_string()))?;
                union.merge(&part);
            }
            return Ok(Some((union, addresses)));
        }
        Err(ApiError::internal(format!(
            "journal checkpoint {prefix} kept changing while it was read"
        )))
    }

    /// The genesis commit of the ledger `record` holds: its registration's
    /// record of it, or found by walking the first-parent line.
    async fn ledger_lineage(
        &self,
        record: &NsRecord,
        registration: &Registration,
    ) -> Result<Option<ContentId>> {
        if let Some(lineage) = registration.lineage.as_deref() {
            return lineage
                .parse()
                .map(Some)
                .map_err(|_| ApiError::internal(format!("retention lineage {lineage}")));
        }
        let Some(head) = record.commit_head_id.clone() else {
            return Ok(None);
        };
        let store = self
            .content_store_for_record_or_id(Some(record), &record.storage_namespace())
            .await?;
        let line = collect_first_parent_cids(store.as_ref(), &head, 0).await?;
        Ok(line.into_iter().min_by_key(|(t, _)| *t).map(|(_, id)| id))
    }

    /// Publish `ledger_id`'s journal to its lineage's checkpoint. A removal
    /// or policy change is complete once this succeeds.
    pub(crate) async fn checkpoint_journal(&self, ledger_id: &str) -> Result<()> {
        let record = self
            .publisher()?
            .lookup(ledger_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(ledger_id.to_string()))?;
        self.publish_checkpoint(&record).await
    }

    /// Publish the journal of the ledger `record` holds, which need not be
    /// visible yet, and record the lineage and the generation published on
    /// its registration.
    async fn publish_checkpoint(&self, record: &NsRecord) -> Result<()> {
        let ns = self.publisher()?;
        let ledger_id = record.ledger_id.as_str();
        let Some(registration) = registry::read(ns, ledger_id).await?.1 else {
            return Ok(());
        };
        let journal = &registration.journal;
        if journal.is_empty()
            || (registration.checkpointed == journal.generation && registration.lineage.is_some())
        {
            return Ok(());
        }
        let Some(lineage) = self.ledger_lineage(record, &registration).await? else {
            return Ok(());
        };
        let storage = self.checkpoint_storage()?;
        let prefix = checkpoint_prefix(storage, &lineage);
        let (held, listed) = self.read_checkpoint(&lineage).await?.unwrap_or_default();
        let missing = journal.missing_from(&held);
        if listed.len() >= COMPACT_AT {
            let mut union = held;
            union.merge(&missing);
            let written = write_object(storage, &prefix, &union).await?;
            let stale: Vec<String> = listed.into_iter().filter(|a| *a != written).collect();
            for (address, e) in storage.delete_many(&stale).await {
                tracing::warn!(%address, error = %e, "retention: compacted checkpoint object not deleted");
            }
        } else if !missing.is_empty() {
            write_object(storage, &prefix, &missing).await?;
        }

        let (generation, lineage) = (journal.generation, lineage.to_string());
        registry::update_fenced(ns, ledger_id, record.fence, |current| {
            let current = current?;
            (current.checkpointed < generation || current.lineage.is_none()).then(|| Registration {
                checkpointed: current.checkpointed.max(generation),
                lineage: Some(lineage.clone()),
                ..current.clone()
            })
        })
        .await?;
        Ok(())
    }

    /// Ready a restored ledger, not yet visible, to serve: merge its
    /// lineage's checkpoint into the archive's retention, then retract every
    /// journaled occurrence it holds as current state — an archive made
    /// before a removal holds the removed value, which history suppression
    /// does not cover — and publish the result to the checkpoint. Returns
    /// the `t` of the retraction, if one was needed.
    pub(crate) async fn restore_retention(
        &self,
        record: &NsRecord,
        archived: Option<registry::ArchivedRetention>,
        lineage: Option<&ContentId>,
    ) -> Result<Option<i64>> {
        let checkpoint = match lineage {
            Some(lineage) => self.journal_checkpoint(lineage).await?,
            None => None,
        };
        let lineage = lineage.map(ToString::to_string);
        let Some(registration) = registry::restore(
            self.publisher()?,
            record,
            archived,
            lineage.as_deref(),
            checkpoint,
        )
        .await?
        else {
            return Ok(None);
        };
        let ns = self.publisher()?;
        let ledger_id = record.ledger_id.as_str();
        let pending = record;
        let record = self.restored_record(pending).await?;
        let retraction = self
            .retract_removed(&record, &registration.journal.removals)
            .await?;
        if let Some((_, removals)) = &retraction {
            registry::update_fenced(ns, ledger_id, record.fence, |current| {
                let mut next = current?.clone();
                next.journal.removals.extend(removals.iter().cloned());
                next.journal.generation += 1;
                Some(next)
            })
            .await?;
        }
        let record = self.restored_record(pending).await?;
        self.publish_checkpoint(&record).await?;
        Ok(retraction.map(|(t, _)| t))
    }

    /// The record of `pending`, a ledger not yet visible, as the restore has
    /// left it: heads in place, under the storage root its claim holds.
    async fn restored_record(&self, pending: &NsRecord) -> Result<NsRecord> {
        let ledger_id = pending.ledger_id.as_str();
        let stored = self
            .publisher()?
            .raw_record(ledger_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(ledger_id.to_string()))?;
        Ok(NsRecord {
            storage_root: pending.storage_root.clone(),
            ..stored
        })
    }

    /// Commit, to the ledger `record` holds, a retraction of every one of
    /// `removals` it holds as current state. Returns the commit's `t` and the
    /// journal entries for its retractions, which are occurrences of the
    /// removed facts too.
    async fn retract_removed(
        &self,
        record: &NsRecord,
        removals: &[Removal],
    ) -> Result<Option<(i64, Vec<Removal>)>> {
        if removals.is_empty() {
            return Ok(None);
        }
        let store = self
            .content_store_for_record_or_id(Some(record), &record.storage_namespace())
            .await?;
        let state = self
            .load_queryable_state_with_store(store.clone(), record.clone())
            .await?;
        let mut commits: HashMap<ContentId, fluree_db_core::Commit> = HashMap::new();
        let mut current: Vec<(Flake, &Removal)> = Vec::new();
        for removal in removals {
            let Ok(cid) = removal.commit.parse::<ContentId>() else {
                continue;
            };
            if !commits.contains_key(&cid) {
                if !store.has(&cid).await? {
                    continue;
                }
                let commit = load_commit_by_id(store.as_ref(), &cid).await?;
                commits.insert(cid.clone(), commit);
            }
            let Some(flake) = commits[&cid].at_slot(removal.slot).filter(|f| f.op) else {
                continue;
            };
            // Current as this occurrence, not as a later reassertion.
            for stored in super::removal::stored_terms(&state, flake).await? {
                if stored.t == flake.t && !current.iter().any(|(f, _)| *f == stored) {
                    current.push((stored, removal));
                }
            }
        }
        if current.is_empty() {
            return Ok(None);
        }
        let retractions = current.iter().map(|(f, _)| f.clone()).collect();
        let staged = self
            .stage_retention_commit(state, retractions, store.as_ref())
            .await?;
        let content_store = self.content_store(&record.storage_namespace());
        let (receipt, _) = staged
            .apply(&content_store, self.publisher()?)
            .await
            .map_err(ApiError::from)?;
        let cid = receipt.commit_id;
        let commit = load_commit_by_id(store.as_ref(), &cid).await?;
        let entries = commit
            .occurrences()
            .filter_map(|(slot, flake)| {
                let key = super::state::FactKey::of(flake);
                let (asserted, removal) = current
                    .iter()
                    .find(|(f, _)| super::state::FactKey::of(f) == key)?;
                Some(Removal {
                    request_id: removal.request_id.clone(),
                    commit: cid.to_string(),
                    slot,
                    predicate: removal.predicate.clone(),
                    from_t: asserted.t,
                    until_t: Some(receipt.t),
                    scope_t: receipt.t - 1,
                })
            })
            .collect();
        Ok(Some((receipt.t, entries)))
    }
}

/// Write `journal` as a checkpoint object under `prefix`, named by the hash
/// of its bytes. Returns its address.
async fn write_object(storage: &dyn Storage, prefix: &str, journal: &Journal) -> Result<String> {
    let bytes = serde_json::to_vec(journal)
        .map_err(|e| ApiError::internal(format!("journal checkpoint: {e}")))?;
    let address = format!("{prefix}{}.json", hex::encode(Sha256::digest(&bytes)));
    storage.write_bytes(&address, &bytes).await?;
    Ok(address)
}
