//! Whether any branch of a ledger references a commit or transaction blob in
//! a given folder.
//!
//! A branch's state to its head (index plus novelty) holds a txn-meta record
//! of every commit in its history: the commit's subject, its parents, its
//! transaction. Commits a merge copied in from another branch appear there
//! only as a merge's parent, so they and their transactions are found by
//! walking their envelopes from there, as are commits published after the
//! branches were loaded. A branch reads a blob from the first folder holding
//! it along its own and its ancestors' folders, so it references that copy
//! only.
//!
//! Collection asks about one blob at a time, with point lookups. A
//! reconciliation asks about every listed copy in a slice of the digest
//! space at once: each branch's txn-meta is streamed once, matched by
//! dictionary id, and which folder a branch reads is taken from the
//! listing.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_binary_index::{
    BinaryCursor, BinaryFilter, BinaryIndexStore, ColumnProjection, RunSortOrder,
};
use fluree_db_core::commit_subject::{commit_id_of_sid, commit_sid};
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::{
    content_address, load_commit_envelope_by_id, range_with_overlay, ContentId, ContentKind,
    ContentStore, FlakeValue, IndexType, LedgerName, OType, RangeMatch, RangeOptions, RangeTest,
    Sid, StorageNamespace, TXN_META_GRAPH_ID,
};
use fluree_db_ledger::LedgerState;
use fluree_db_nameservice::{BindingState, NameBinding, NsRecord};
use fluree_vocab::db;
use fluree_vocab::namespaces::FLUREE_DB;

use super::Blob;
use crate::{Fluree, Result};

struct Branch {
    record: NsRecord,
    state: LedgerState,
    store: Arc<dyn ContentStore>,
    /// The folders it reads, its own first.
    folders: Vec<StorageNamespace>,
    /// Commits in its history its txn-meta lacks: copied in by a merge, or
    /// published since it was loaded.
    commits: HashSet<ContentId>,
    /// The transactions those commits name.
    txns: HashSet<ContentId>,
}

/// The branches of one ledger, loaded to decide what they reference.
pub(crate) struct Holders {
    name: String,
    branches: Vec<Branch>,
}

impl Fluree {
    /// The branches of ledger `name`, each loaded to its head; `None` unless
    /// its binding is settled: active, no branch being created, and every
    /// listed branch's record current and with a head.
    pub(crate) async fn load_holders(
        &self,
        name: &str,
        binding: &NameBinding,
    ) -> Result<Option<Holders>> {
        let Some(records) = self.settled_records(name, binding).await? else {
            return Ok(None);
        };
        let mut branches = Vec::new();
        for record in &records {
            let store = self
                .content_store_for_record_or_id(Some(record), &record.storage_namespace())
                .await?;
            let (state, head) = match self.cached_on_line(record, &store).await? {
                Some(cached) => cached,
                None => (
                    self.load_queryable_state_with_store(store.clone(), record.clone())
                        .await?,
                    record
                        .commit_head_id
                        .clone()
                        .expect("settled records have heads"),
                ),
            };
            // A history whose head has no commit record decides nothing.
            let loaded = state.head_commit_id.clone().unwrap_or_else(|| head.clone());
            if !has_commit(&state, &loaded).await? {
                tracing::warn!(ledger = %record.ledger_id, "txn-meta lacks the head commit; garbage left alone");
                return Ok(None);
            }
            // Until a build replaces it, an index of a line a rebase
            // replaced holds that line's commits and misses the head's.
            if !indexes_line(&state, store.as_ref(), &loaded).await? {
                tracing::warn!(ledger = %record.ledger_id, "the index is of another line; garbage left alone");
                return Ok(None);
            }
            let mut branch = Branch {
                folders: folders(record, &records),
                record: record.clone(),
                state,
                store,
                commits: HashSet::new(),
                txns: HashSet::new(),
            };
            let merged = merged_parents(&branch.state)?;
            branch.walk(merged).await?;
            // The commits a cached state had yet to take in.
            if loaded != head {
                branch.walk(vec![head]).await?;
            }
            branches.push(branch);
        }
        Ok(Some(Holders {
            name: name.to_string(),
            branches,
        }))
    }

    /// The state this instance holds for `record`'s branch, and the head to
    /// catch it up from, when the branch's head, read after the state, has
    /// it on its first-parent line within [`MAX_GAP`] commits. A state is
    /// cached once its commit is published, and heads only move to a higher
    /// `t`, so the head read after it is at or past it; a history rewritten
    /// under it, or a placeholder, gives `None`.
    async fn cached_on_line(
        &self,
        record: &NsRecord,
        store: &Arc<dyn ContentStore>,
    ) -> Result<Option<(LedgerState, ContentId)>> {
        let Some(manager) = &self.ledger_manager else {
            return Ok(None);
        };
        let Some(handle) = manager
            .get_loaded_handle(&fluree_db_core::LedgerId::parse(&record.ledger_id)?)
            .await
        else {
            return Ok(None);
        };
        let state = handle.clone_state().await;
        let Some(cached) = state.head_commit_id.clone() else {
            return Ok(None);
        };
        let Some(now) = self.publisher()?.raw_record(&record.ledger_id).await? else {
            return Ok(None);
        };
        let Some(head) = now.commit_head_id.clone() else {
            return Ok(None);
        };
        if now.fence != record.fence
            || state.t() > now.commit_t
            || !on_line(store, &head, &cached, state.t()).await?
        {
            return Ok(None);
        }
        Ok(Some((state, head)))
    }

    /// The records of `binding`'s branches, when it is settled.
    async fn settled_records(
        &self,
        name: &str,
        binding: &NameBinding,
    ) -> Result<Option<Vec<NsRecord>>> {
        if !matches!(binding.state, BindingState::Active)
            || binding.branches.iter().any(|b| b.creating)
        {
            return Ok(None);
        }
        let ledger = LedgerName::parse(name)?;
        let mut records = Vec::new();
        for listing in &binding.branches {
            let ledger_id = ledger.with_branch(&listing.branch)?;
            let Some(record) = self.publisher()?.raw_record(ledger_id.as_str()).await? else {
                return Ok(None);
            };
            // A branch with no head yet is being filled by a clone.
            if record.fence != Some(listing.fence) || record.commit_head_id.is_none() {
                return Ok(None);
            }
            // A stored record leaves its folder to the binding.
            records.push(NsRecord {
                storage_root: Some(binding.root.clone()),
                ..record
            });
        }
        Ok(Some(records))
    }
}

/// How many commits a cached state may be behind its branch's head.
const MAX_GAP: i64 = 10_000;

/// Whether `to`, at `to_t`, is on `from`'s first-parent line within
/// [`MAX_GAP`] commits.
async fn on_line(
    store: &Arc<dyn ContentStore>,
    from: &ContentId,
    to: &ContentId,
    to_t: i64,
) -> Result<bool> {
    let mut next = Some(from.clone());
    while let Some(id) = next {
        if &id == to {
            return Ok(true);
        }
        let envelope = match load_commit_envelope_by_id(store.as_ref(), &id).await {
            Ok(envelope) => envelope,
            Err(fluree_db_core::Error::NotFound(_)) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        if envelope.t <= to_t || envelope.t - to_t > MAX_GAP {
            return Ok(false);
        }
        next = envelope.parents.into_iter().next();
    }
    Ok(false)
}

/// The folders `record`'s branch reads, its own first, then its ancestors'.
fn folders(record: &NsRecord, records: &[NsRecord]) -> Vec<StorageNamespace> {
    let mut folders = vec![record.storage_namespace()];
    let mut source = record.source_branch.as_deref();
    while let Some(parent) = source.and_then(|s| records.iter().find(|r| r.branch == s)) {
        if folders.contains(&parent.storage_namespace()) {
            break;
        }
        folders.push(parent.storage_namespace());
        source = parent.source_branch.as_deref();
    }
    folders
}

/// The commit and transaction copies listed in the branches' folders, in
/// one slice of the digest space.
pub(crate) struct Listed {
    pub(crate) blobs: Vec<Blob>,
    /// The listed copies of each id, as indexes into `blobs`.
    copies: HashMap<ContentId, Vec<usize>>,
}

impl Listed {
    pub(crate) fn new(blobs: Vec<Blob>) -> Self {
        let mut copies: HashMap<ContentId, Vec<usize>> = HashMap::new();
        for (i, blob) in blobs.iter().enumerate() {
            copies.entry(blob.id.clone()).or_default().push(i);
        }
        Self { blobs, copies }
    }
}

impl Holders {
    /// Which of `listed` a branch references: has in its history, and reads
    /// from that copy's folder, the first of its folders the listing shows
    /// holding it.
    pub(crate) async fn read(&self, listed: &Listed) -> Result<Vec<bool>> {
        let mut read = vec![false; listed.blobs.len()];
        for branch in &self.branches {
            let readable: Vec<ContentId> = listed
                .copies
                .iter()
                .filter(|(_, copies)| {
                    copies
                        .iter()
                        .any(|&i| branch.folders.contains(&listed.blobs[i].namespace))
                })
                .map(|(id, _)| id.clone())
                .collect();
            for id in branch.reached(readable).await? {
                let copies = &listed.copies[&id];
                let first = branch.folders.iter().find_map(|folder| {
                    copies
                        .iter()
                        .copied()
                        .find(|&i| listed.blobs[i].namespace == *folder)
                });
                if let Some(i) = first {
                    read[i] = true;
                }
            }
        }
        Ok(read)
    }

    /// Account for commits published since the branches were loaded, from
    /// the ledger's records as they are now. `false` when it is no longer
    /// settled, or has a branch that was not loaded.
    pub(crate) async fn catch_up(&mut self, fluree: &Fluree) -> Result<bool> {
        let Some(binding) = fluree.publisher()?.get_binding(&self.name).await? else {
            return Ok(false);
        };
        let Some(records) = fluree.settled_records(&self.name, &binding.value).await? else {
            return Ok(false);
        };
        for record in records {
            let Some(branch) = self
                .branches
                .iter_mut()
                .find(|b| b.record.branch == record.branch)
            else {
                return Ok(false);
            };
            if branch.record.commit_head_id != record.commit_head_id {
                branch.store = fluree
                    .content_store_for_record_or_id(Some(&record), &record.storage_namespace())
                    .await?;
                branch
                    .walk(record.commit_head_id.into_iter().collect())
                    .await?;
            }
        }
        Ok(true)
    }

    /// Whether a branch references `blob`: has it in its history, and reads
    /// it from `blob`'s folder.
    pub(crate) async fn references(&self, fluree: &Fluree, blob: &Blob) -> Result<bool> {
        let kind = blob.id.content_kind();
        for branch in &self.branches {
            if !branch.folders.contains(&blob.namespace) {
                continue;
            }
            let reached = match kind {
                Some(ContentKind::Commit) => {
                    branch.commits.contains(&blob.id) || has_commit(&branch.state, &blob.id).await?
                }
                Some(ContentKind::Txn) => {
                    branch.txns.contains(&blob.id) || names_txn(&branch.state, &blob.id).await?
                }
                _ => return Ok(true),
            };
            if reached && branch.reads_from(fluree, blob).await? {
                return Ok(true);
            }
        }
        if kind == Some(ContentKind::Commit) {
            return self.lacks_records(fluree, blob).await;
        }
        Ok(false)
    }

    /// Whether a branch whose history reaches the commit's `t` has no commit
    /// record there: its txn-meta lacks records, and decides nothing.
    pub(crate) async fn lacks_records(&self, fluree: &Fluree, blob: &Blob) -> Result<bool> {
        let store = fluree.backend().content_store(&blob.namespace);
        let t = match load_commit_envelope_by_id(store.as_ref(), &blob.id).await {
            Ok(envelope) => envelope.t,
            Err(fluree_db_core::Error::NotFound(_)) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        for branch in self.branches.iter().filter(|b| b.state.t() >= t) {
            if !committed_at(&branch.state, t).await? {
                tracing::warn!(ledger = %branch.record.ledger_id, t, "txn-meta lacks a commit; garbage left alone");
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl Branch {
    /// Whether the first of this branch's folders holding `blob`'s id is
    /// `blob`'s.
    async fn reads_from(&self, fluree: &Fluree, blob: &Blob) -> Result<bool> {
        let storage = fluree.garbage_storage()?;
        let method = storage.storage_method();
        let Some(kind) = blob.id.content_kind() else {
            return Ok(true);
        };
        for folder in &self.folders {
            let address = content_address(method, kind, folder, &blob.id.address_digest());
            if storage.exists(&address).await? {
                return Ok(folder == &blob.namespace);
            }
        }
        Ok(false)
    }

    /// Which of `ids` this branch's history holds.
    async fn reached(&self, ids: Vec<ContentId>) -> Result<Vec<ContentId>> {
        let (mut reached, rest): (Vec<_>, Vec<_>) = ids
            .into_iter()
            .partition(|id| self.commits.contains(id) || self.txns.contains(id));
        // Above the index.
        let txn = Sid::new(FLUREE_DB, db::TXN);
        let mut above: HashSet<ContentId> = HashSet::new();
        for flake in
            self.state
                .novelty
                .range_flakes(TXN_META_GRAPH_ID, IndexType::Spot, None, None, true)
        {
            if !flake.op || flake.t > self.state.t() {
                continue;
            }
            above.extend(commit_id_of_sid(&flake.s));
            if let (true, FlakeValue::String(value)) = (flake.p == txn, &flake.o) {
                above.extend(value.parse::<ContentId>().ok());
            }
        }
        let (found, rest): (Vec<_>, Vec<_>) = rest.into_iter().partition(|id| above.contains(id));
        reached.extend(found);
        if let (Some(index), false) = (binary_index(&self.state), rest.is_empty()) {
            let to_t = self.state.index_t();
            let found = tokio::task::spawn_blocking(move || indexed(&index, to_t, rest))
                .await
                .map_err(|e| crate::ApiError::internal(format!("txn-meta scan: {e}")))?
                .map_err(|e| crate::ApiError::internal(format!("txn-meta scan: {e}")))?;
            reached.extend(found);
        }
        Ok(reached)
    }

    /// Add the commits reached from `roots` down to the first this branch's
    /// txn-meta records, and their transactions.
    async fn walk(&mut self, roots: Vec<ContentId>) -> Result<()> {
        let mut next = roots;
        while let Some(id) = next.pop() {
            if self.commits.contains(&id) {
                continue;
            }
            let envelope = match load_commit_envelope_by_id(self.store.as_ref(), &id).await {
                Ok(envelope) => Some(envelope),
                // Nothing stored to delete, and nothing more to reach.
                Err(fluree_db_core::Error::NotFound(_)) => None,
                Err(e) => return Err(e.into()),
            };
            // A commit past the loaded state cannot be in its txn-meta.
            let past = envelope.as_ref().is_some_and(|e| e.t > self.state.t());
            if !past && has_commit(&self.state, &id).await? {
                continue;
            }
            self.commits.insert(id);
            if let Some(envelope) = envelope {
                self.txns.extend(envelope.txn);
                next.extend(envelope.parents);
            }
        }
        Ok(())
    }
}

/// The parents of the merges in `state`'s history: streamed from its
/// index's txn-meta, and read from its novelty's above the index.
fn merged_parents(state: &LedgerState) -> Result<Vec<ContentId>> {
    let mut merged = Vec::new();
    if let Some(index) = binary_index(state) {
        merged.extend(
            indexed_merge_parents(&index, state.index_t())
                .map_err(|e| crate::ApiError::internal(format!("txn-meta scan: {e}")))?,
        );
    }
    let previous = Sid::new(FLUREE_DB, db::PREVIOUS);
    let mut parents: HashMap<&Sid, Vec<&Sid>> = HashMap::new();
    for flake in state
        .novelty
        .range_flakes(TXN_META_GRAPH_ID, IndexType::Spot, None, None, true)
    {
        if let (true, true, FlakeValue::Ref(parent)) = (
            flake.op && flake.t <= state.t(),
            flake.p == previous,
            &flake.o,
        ) {
            parents.entry(&flake.s).or_default().push(parent);
        }
    }
    for parents in parents.into_values().filter(|p| p.len() > 1) {
        merged.extend(parents.into_iter().filter_map(commit_id_of_sid));
    }
    Ok(merged)
}

fn binary_index(state: &LedgerState) -> Option<Arc<BinaryIndexStore>> {
    state
        .binary_store
        .as_ref()
        .and_then(|s| Arc::downcast::<BinaryIndexStore>(Arc::clone(&s.0)).ok())
}

/// Each `(s_id, o_type, o_key)` row of predicate `p` in `index`'s txn-meta
/// as of `to_t`, in subject order, read without decoding the rest.
fn for_each_row(
    index: &Arc<BinaryIndexStore>,
    p: &Sid,
    to_t: i64,
    mut row: impl FnMut(u64, u16, u64) -> std::io::Result<()>,
) -> std::io::Result<()> {
    let Some(p_id) = index.sid_to_p_id(p) else {
        return Ok(());
    };
    let Some(branch) = index.dedicated_branch(TXN_META_GRAPH_ID, RunSortOrder::Psot) else {
        return Ok(());
    };
    let bound = |s_id: u64, o_key: u64, t: u32, o_i: u64, o_type: u16| RunRecordV2 {
        s_id: SubjectId(s_id),
        o_key,
        p_id,
        t,
        o_i,
        o_type,
        g_id: TXN_META_GRAPH_ID,
    };
    let mut cursor = BinaryCursor::new(
        Arc::clone(index),
        RunSortOrder::Psot,
        Arc::clone(branch),
        &bound(0, 0, 0, 0, 0),
        &bound(u64::MAX, u64::MAX, u32::MAX, u64::MAX, u16::MAX),
        BinaryFilter {
            p_id: Some(p_id),
            ..Default::default()
        },
        ColumnProjection::all(),
    );
    cursor.set_to_t(to_t);
    while let Some(batch) = cursor.next_batch()? {
        for i in 0..batch.row_count {
            row(batch.s_id.get(i), batch.o_type.get(i), batch.o_key.get(i))?;
        }
    }
    Ok(())
}

/// The parents of every commit with more than one in `index`'s txn-meta.
fn indexed_merge_parents(
    index: &Arc<BinaryIndexStore>,
    to_t: i64,
) -> std::io::Result<Vec<ContentId>> {
    let mut merged = Vec::new();
    let mut subject = None;
    let mut parents: Vec<u64> = Vec::new();
    let mut flush = |parents: &mut Vec<u64>| -> std::io::Result<()> {
        if parents.len() > 1 {
            for parent in parents.iter() {
                let (ns, name) = index.resolve_subject_parts(*parent)?;
                merged.extend(commit_id_of_sid(&Sid::new(ns, name)));
            }
        }
        parents.clear();
        Ok(())
    };
    for_each_row(
        index,
        &Sid::new(FLUREE_DB, db::PREVIOUS),
        to_t,
        |s_id, _, parent| {
            if subject != Some(s_id) {
                flush(&mut parents)?;
                subject = Some(s_id);
            }
            parents.push(parent);
            Ok(())
        },
    )?;
    flush(&mut parents)?;
    Ok(merged)
}

/// Which of `ids` `index`'s txn-meta records as of `to_t`: commits by
/// their `db#t`, transactions by the `db#txn` naming them. The ids are
/// looked up in its dictionaries once, and each predicate streamed once.
fn indexed(
    index: &Arc<BinaryIndexStore>,
    to_t: i64,
    ids: Vec<ContentId>,
) -> std::io::Result<Vec<ContentId>> {
    let (commits, txns): (Vec<_>, Vec<_>) = ids
        .into_iter()
        .partition(|id| id.content_kind() == Some(ContentKind::Commit));
    let mut reached = Vec::new();
    let subjects: Vec<Sid> = commits.iter().map(commit_sid).collect();
    let parts: Vec<(u16, &str)> = subjects
        .iter()
        .map(|s| (s.namespace_code, s.name.as_ref()))
        .collect();
    let mut by_subject: HashMap<u64, ContentId> = index
        .find_subject_ids_by_parts(&parts)?
        .into_iter()
        .zip(commits)
        .filter_map(|(s_id, id)| Some((s_id?, id)))
        .collect();
    if !by_subject.is_empty() {
        for_each_row(index, &Sid::new(FLUREE_DB, db::T), to_t, |s_id, _, _| {
            reached.extend(by_subject.remove(&s_id));
            Ok(())
        })?;
    }
    let names: Vec<String> = txns.iter().map(ToString::to_string).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let mut by_string: HashMap<u64, ContentId> = index
        .find_string_ids(&names)?
        .into_iter()
        .zip(txns)
        .filter_map(|(str_id, id)| Some((u64::from(str_id?), id)))
        .collect();
    if !by_string.is_empty() {
        let string = OType::XSD_STRING.as_u16();
        for_each_row(
            index,
            &Sid::new(FLUREE_DB, db::TXN),
            to_t,
            |_, o_type, o_key| {
                if o_type == string {
                    reached.extend(by_string.remove(&o_key));
                }
                Ok(())
            },
        )?;
    }
    Ok(reached)
}

fn current(state: &LedgerState) -> RangeOptions {
    RangeOptions::default()
        .with_to_t(state.t())
        .with_flake_limit(1)
}

/// Whether `state`'s index is of the line through `head`: it holds the
/// commit that line has at the index's `t`.
async fn indexes_line(
    state: &LedgerState,
    store: &dyn fluree_db_core::ContentStore,
    head: &ContentId,
) -> Result<bool> {
    if state.index_t() <= 0 {
        return Ok(true);
    }
    let above = fluree_db_core::collect_first_parent_cids(store, head, state.index_t()).await?;
    let base = match above.last() {
        Some((_, oldest)) => {
            let envelope = fluree_db_core::load_commit_envelope_by_id(store, oldest).await?;
            match envelope.parents.into_iter().next() {
                Some(parent) => parent,
                None => return Ok(true),
            }
        }
        None => head.clone(),
    };
    has_commit(state, &base).await
}

async fn has_commit(state: &LedgerState, id: &ContentId) -> Result<bool> {
    let found = range_with_overlay(
        &state.snapshot,
        TXN_META_GRAPH_ID,
        state.novelty.as_ref(),
        IndexType::Spot,
        RangeTest::Eq,
        RangeMatch::subject(commit_sid(id)),
        current(state),
    )
    .await?;
    Ok(!found.is_empty())
}

async fn names_txn(state: &LedgerState, id: &ContentId) -> Result<bool> {
    let found = range_with_overlay(
        &state.snapshot,
        TXN_META_GRAPH_ID,
        state.novelty.as_ref(),
        IndexType::Post,
        RangeTest::Eq,
        RangeMatch::predicate_object(
            Sid::new(FLUREE_DB, db::TXN),
            FlakeValue::String(id.to_string()),
        ),
        current(state),
    )
    .await?;
    Ok(!found.is_empty())
}

async fn committed_at(state: &LedgerState, t: i64) -> Result<bool> {
    let found = range_with_overlay(
        &state.snapshot,
        TXN_META_GRAPH_ID,
        state.novelty.as_ref(),
        IndexType::Post,
        RangeTest::Eq,
        RangeMatch::predicate_object(Sid::new(FLUREE_DB, db::T), FlakeValue::Long(t)),
        current(state),
    )
    .await?;
    Ok(!found.is_empty())
}
