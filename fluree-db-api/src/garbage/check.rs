//! Whether any branch of a ledger references a commit or transaction blob.
//!
//! A branch's state to its head (index plus novelty) holds a txn-meta record
//! of every commit in its history: the commit's subject, its parents, its
//! transaction. Commits a merge copied in from another branch appear there
//! only as a merge's parent, so they and their transactions are found by
//! walking their envelopes from there, as are commits published after the
//! branches were loaded.

use std::collections::HashSet;
use std::sync::Arc;

use fluree_db_core::commit_subject::{commit_id_of_sid, commit_sid};
use fluree_db_core::{
    load_commit_envelope_by_id, range_bounded_with_overlay, range_with_overlay, ContentId,
    ContentKind, ContentStore, Flake, FlakeValue, IndexType, LedgerName, RangeMatch, RangeOptions,
    RangeTest, Sid, TXN_META_GRAPH_ID,
};
use fluree_db_ledger::LedgerState;
use fluree_db_nameservice::{BindingState, NameBinding, NsRecord};
use fluree_vocab::db;
use fluree_vocab::namespaces::FLUREE_DB;

use super::Blob;
use crate::{Fluree, Result};

/// Flakes read per page of a txn-meta scan.
const PAGE: usize = 10_000;

struct Branch {
    record: NsRecord,
    state: LedgerState,
    store: Arc<dyn ContentStore>,
}

/// The branches of one ledger, loaded to decide what they reference.
pub(crate) struct Holders {
    name: String,
    branches: Vec<Branch>,
    /// Commits reached outside every branch's txn-meta: copied in by a
    /// merge, or published since the branches were loaded.
    commits: HashSet<ContentId>,
    /// The transactions those commits name.
    txns: HashSet<ContentId>,
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
        let mut holders = Holders {
            name: name.to_string(),
            branches: Vec::new(),
            commits: HashSet::new(),
            txns: HashSet::new(),
        };
        for record in records {
            let store = self
                .content_store_for_record_or_id(Some(&record), &record.storage_namespace())
                .await?;
            let state = self
                .load_queryable_state_with_store(store.clone(), record.clone())
                .await?;
            let head = record
                .commit_head_id
                .clone()
                .expect("settled records have heads");
            // A history whose head has no commit record decides nothing.
            if !has_commit(&state, &head).await? {
                tracing::warn!(ledger = %record.ledger_id, "txn-meta lacks the head commit; garbage left alone");
                return Ok(None);
            }
            holders.branches.push(Branch {
                record,
                state,
                store,
            });
        }
        for i in 0..holders.branches.len() {
            let merged = merged_parents(&holders.branches[i].state).await?;
            let store = holders.branches[i].store.clone();
            holders.walk(&store, merged).await?;
        }
        Ok(Some(holders))
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

impl Holders {
    /// Account for commits published since the branches were loaded, from
    /// the ledger's records as they are now. `false` when it is no longer
    /// settled.
    pub(crate) async fn catch_up(&mut self, fluree: &Fluree) -> Result<bool> {
        let Some(binding) = fluree.publisher()?.get_binding(&self.name).await? else {
            return Ok(false);
        };
        let Some(records) = fluree.settled_records(&self.name, &binding.value).await? else {
            return Ok(false);
        };
        for record in records {
            let loaded = self.branches.iter().any(|b| {
                b.record.branch == record.branch && b.record.commit_head_id == record.commit_head_id
            });
            if loaded {
                continue;
            }
            let store = fluree
                .content_store_for_record_or_id(Some(&record), &record.storage_namespace())
                .await?;
            self.walk(&store, record.commit_head_id.into_iter().collect())
                .await?;
        }
        Ok(true)
    }

    /// Whether a branch references `blob`.
    pub(crate) async fn references(&self, fluree: &Fluree, blob: &Blob) -> Result<bool> {
        match blob.id.content_kind() {
            Some(ContentKind::Commit) => {
                if self.commits.contains(&blob.id) || self.known(&blob.id).await? {
                    return Ok(true);
                }
                // A branch whose history reaches the commit's t without a
                // commit record there lacks records, and decides nothing.
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
            Some(ContentKind::Txn) => {
                if self.txns.contains(&blob.id) {
                    return Ok(true);
                }
                for branch in &self.branches {
                    if names_txn(&branch.state, &blob.id).await? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            _ => Ok(true),
        }
    }

    /// Whether a branch's txn-meta records commit `id`.
    async fn known(&self, id: &ContentId) -> Result<bool> {
        for branch in &self.branches {
            if has_commit(&branch.state, id).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Add the commits reached from `roots`, read from `store`, down to the
    /// first a branch's txn-meta records, and their transactions.
    async fn walk(&mut self, store: &Arc<dyn ContentStore>, roots: Vec<ContentId>) -> Result<()> {
        let mut next = roots;
        while let Some(id) = next.pop() {
            if self.commits.contains(&id) || self.known(&id).await? {
                continue;
            }
            self.commits.insert(id.clone());
            match load_commit_envelope_by_id(store.as_ref(), &id).await {
                Ok(envelope) => {
                    self.txns.extend(envelope.txn);
                    next.extend(envelope.parents);
                }
                // Nothing stored to delete, and nothing more to reach.
                Err(fluree_db_core::Error::NotFound(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

/// The parents of the merges in `state`'s txn-meta.
async fn merged_parents(state: &LedgerState) -> Result<Vec<ContentId>> {
    let previous = Sid::new(FLUREE_DB, db::PREVIOUS);
    let end = Flake::max_for_predicate(previous.clone());
    let cmp = IndexType::Psot.comparator();
    let mut start = Flake::min_for_predicate(previous);
    let mut last: Option<Flake> = None;
    let mut merged = Vec::new();
    let mut subject: Option<Sid> = None;
    let mut parents: Vec<ContentId> = Vec::new();
    loop {
        let page = range_bounded_with_overlay(
            &state.snapshot,
            TXN_META_GRAPH_ID,
            state.novelty.as_ref(),
            IndexType::Psot,
            start,
            end.clone(),
            RangeOptions::default()
                .with_to_t(state.t())
                .with_flake_limit(PAGE),
        )
        .await?;
        let full = page.len() >= PAGE;
        for flake in &page {
            // Pages start at the last flake read.
            if last.as_ref().is_some_and(|l| cmp(flake, l).is_le()) {
                continue;
            }
            if subject.as_ref() != Some(&flake.s) {
                if parents.len() > 1 {
                    merged.append(&mut parents);
                }
                parents.clear();
                subject = Some(flake.s.clone());
            }
            if let FlakeValue::Ref(parent) = &flake.o {
                parents.extend(commit_id_of_sid(parent));
            }
        }
        match page.last() {
            Some(tail) if full && last.as_ref().is_none_or(|l| cmp(tail, l).is_gt()) => {
                last = Some(tail.clone());
                start = tail.clone();
            }
            _ => break,
        }
    }
    if parents.len() > 1 {
        merged.append(&mut parents);
    }
    Ok(merged)
}

fn current(state: &LedgerState) -> RangeOptions {
    RangeOptions::default()
        .with_to_t(state.t())
        .with_flake_limit(1)
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
