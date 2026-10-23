//! Where the captured facts still are in the storage a purge inspects: the
//! folders of every holding branch, live or dropped, and the indexes of the
//! live ones.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::commit::codec::read_commit;
use fluree_db_core::{
    load_commit_envelope_by_id, range, ContentId, ContentStore, FlakeValue, IndexType, RangeMatch,
    RangeOptions, RangeTest,
};
use fluree_db_nameservice::NsRecord;
use serde::{Deserialize, Serialize};

use super::{Captured, PurgePlan};
use crate::retention::state::FactKey;
use crate::{ApiError, Fluree, Result};

/// What a scan found. It names files and branches, never a value.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PurgeScan {
    /// Commit files in a holder's folder that no holder's history reaches,
    /// holding a captured value: left by a rebase or an abandoned write.
    pub unreferenced: Vec<String>,
    /// Holders whose index still replays an erased assertion.
    pub indexed: Vec<String>,
    /// Holders whose dictionaries hold a captured value that no surviving
    /// occurrence uses.
    pub dictionary: Vec<String>,
    /// Captured values a surviving occurrence uses, which may stay.
    pub in_use: usize,
}

impl PurgeScan {
    /// Nothing holds a captured value that the purge has not accounted for.
    pub fn clean(&self) -> bool {
        self.unreferenced.is_empty() && self.indexed.is_empty() && self.dictionary.is_empty()
    }
}

impl Fluree {
    /// Scan the holders of `plan`'s commits for `captured`.
    pub(crate) async fn scan_purge(
        &self,
        plan: &PurgePlan,
        captured: &[Captured],
    ) -> Result<PurgeScan> {
        let runtime = self.erasure_runtime()?;
        let mut holders: Vec<(NsRecord, Arc<dyn ContentStore>, bool)> = Vec::new();
        for id in &plan.rebuild {
            if let Some(record) = self.nameservice().lookup(id).await? {
                let store = self
                    .content_store_for_record_or_id(Some(&record), &record.storage_namespace())
                    .await?;
                holders.push((record, store, true));
            }
        }
        let commits: Vec<&str> = plan.commits.iter().map(String::as_str).collect();
        for (record, store) in self.dropped_holders(&commits).await? {
            holders.push((record, store, false));
        }
        let erased: HashSet<String> = plan
            .commits
            .iter()
            .filter_map(|c| c.parse::<ContentId>().ok())
            .map(|c| c.address_digest())
            .collect();
        let reachable = reachable_commits(&holders).await?;

        // Every commit file in the holders' folders, past the substitution.
        let prefix = runtime.index.address_prefix();
        let folders: Vec<String> = holders
            .iter()
            .map(|(r, _, _)| format!("{prefix}{}/commit/", r.storage_namespace().branch_prefix()))
            .collect();
        let mut in_use: Vec<&FlakeValue> = Vec::new();
        let mut unreferenced = BTreeSet::new();
        for address in runtime.index.list_all().await? {
            let Some(digest) = folders
                .iter()
                .find_map(|f| address.strip_prefix(f.as_str()))
                .and_then(|file| file.strip_suffix(".fcv2"))
            else {
                continue;
            };
            if erased.contains(digest) {
                continue;
            }
            let reached = reachable.contains(digest);
            let Some(bytes) = runtime.index.read_raw(&address).await? else {
                continue;
            };
            let commit = read_commit(&bytes)
                .map_err(|e| ApiError::internal(format!("commit file {address}: {e}")))?;
            for (_, flake) in commit.occurrences() {
                let Some(c) = captured.iter().find(|c| c.flake.o == flake.o) else {
                    continue;
                };
                if !reached {
                    unreferenced.insert(address.clone());
                } else if !in_use.contains(&&c.flake.o) {
                    in_use.push(&c.flake.o);
                }
            }
        }

        // Each live holder's index: no erased occurrence, and in its
        // dictionaries no captured value that nothing still uses.
        let mut indexed = BTreeSet::new();
        let mut dictionary = BTreeSet::new();
        for (record, store, live) in &holders {
            if !live || record.index_head_id.is_none() {
                continue;
            }
            let state = self
                .load_queryable_state_with_store(store.clone(), record.clone())
                .await?;
            let snapshot = &state.snapshot;
            // Each erased assertion, as of when it was made: an index holding
            // its history replays it there.
            for c in captured.iter().filter(|c| c.flake.op) {
                let g_id = match &c.flake.g {
                    None => 0,
                    Some(g) => match snapshot
                        .decode_sid(g)
                        .and_then(|iri| snapshot.graph_registry.graph_id_for_iri(&iri))
                    {
                        Some(g_id) => g_id,
                        None => continue,
                    },
                };
                let found = range(
                    snapshot,
                    g_id,
                    IndexType::Spot,
                    RangeTest::Eq,
                    RangeMatch::subject_predicate(c.flake.s.clone(), c.flake.p.clone()),
                    RangeOptions {
                        to_t: Some(c.flake.t),
                        ..Default::default()
                    },
                )
                .await?;
                let key = FactKey::of(&c.flake);
                if found.iter().any(|f| FactKey::of(f) == key) {
                    indexed.insert(record.ledger_id.to_string());
                }
            }
            let Some(store) = state
                .binary_store
                .as_ref()
                .and_then(|te| te.0.downcast_ref::<BinaryIndexStore>())
            else {
                continue;
            };
            for c in captured {
                if let FlakeValue::String(value) = &c.flake.o {
                    let held = store
                        .find_string_id(value)
                        .map_err(|e| ApiError::internal(format!("dictionary lookup: {e}")))?;
                    if held.is_some() && !in_use.contains(&&c.flake.o) {
                        dictionary.insert(record.ledger_id.to_string());
                    }
                }
            }
        }
        Ok(PurgeScan {
            unreferenced: unreferenced.into_iter().collect(),
            indexed: indexed.into_iter().collect(),
            dictionary: dictionary.into_iter().collect(),
            in_use: in_use.len(),
        })
    }
}

/// The digests of every commit a holder's history reaches, through every
/// parent of a merge.
async fn reachable_commits(
    holders: &[(NsRecord, Arc<dyn ContentStore>, bool)],
) -> Result<HashSet<String>> {
    let mut seen = HashSet::new();
    for (record, store, _) in holders {
        let mut next: Vec<ContentId> = record.commit_head_id.iter().cloned().collect();
        while let Some(cid) = next.pop() {
            if !seen.insert(cid.address_digest()) {
                continue;
            }
            next.extend(
                load_commit_envelope_by_id(store.as_ref(), &cid)
                    .await?
                    .parents,
            );
        }
    }
    Ok(seen)
}
