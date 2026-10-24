//! Where the captured facts still are in what a purge inspects: the commits
//! and TTL schedules in the folders of every holding branch, live or
//! dropped, the indexes of the live ones, and the files nothing references
//! that are recorded as garbage under the live ones' ledgers.

use std::borrow::Cow;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::{
    load_commit_by_id, range, ContentId, ContentStore, FlakeValue, IndexType, RangeMatch,
    RangeOptions, RangeTest,
};
use fluree_db_nameservice::NsRecord;
use serde::{Deserialize, Serialize};

use super::{Captured, PurgePlan};
use crate::retention::state::{FactKey, Schedule};
use crate::{ApiError, Fluree, Result};

/// What a scan found. It names files and branches, never a value.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PurgeScan {
    /// Files under a live holder's ledger that nothing references, recorded
    /// as garbage by its last reconciliation or before, and not yet
    /// collected: commits a rebase or a lost race left, transaction
    /// documents of transactions that failed.
    pub unreferenced: Vec<String>,
    /// TTL schedules that still hold an erased fact.
    pub schedules: Vec<String>,
    /// Holders whose index still replays an erased assertion.
    pub indexed: Vec<String>,
    /// Holders whose dictionaries hold a captured value that no surviving
    /// occurrence uses.
    pub dictionary: Vec<String>,
    /// Index files and dictionaries a dropped holder still has, soft-dropped
    /// or retracted, and full-text or vector index files their heads no
    /// longer use, which the purge releases.
    #[serde(default)]
    pub unreleased: Vec<String>,
    /// Captured values a surviving occurrence uses, which may stay.
    pub in_use: usize,
}

impl PurgeScan {
    /// Nothing holds a captured value that the purge has not accounted for.
    pub fn clean(&self) -> bool {
        self.unreferenced.is_empty()
            && self.schedules.is_empty()
            && self.indexed.is_empty()
            && self.dictionary.is_empty()
            && self.unreleased.is_empty()
    }
}

/// The string `value` is interned as in the string dictionary, if any:
/// strings, JSON and generic durations, as the binary index encodes them.
fn dictionary_string(value: &FlakeValue) -> Option<Cow<'_, str>> {
    match value {
        FlakeValue::String(s) | FlakeValue::Json(s) => Some(Cow::Borrowed(s)),
        FlakeValue::Duration(d) => Some(Cow::Owned(d.to_canonical_string())),
        _ => None,
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
        // What survives in the holders' histories, an erased commit read as
        // its replacement, whether or not its original's file is still
        // stored. What no history reaches is the garbage collector's.
        let mut in_use: Vec<&FlakeValue> = Vec::new();
        let mut facts_in_use: Vec<FactKey> = Vec::new();
        // The dictionary strings of erased values, and those a surviving
        // occurrence interns too, whatever its type.
        let erased_strings: HashSet<String> = captured
            .iter()
            .filter_map(|c| dictionary_string(&c.flake.o))
            .map(Cow::into_owned)
            .collect();
        let mut strings_in_use: HashSet<String> = HashSet::new();
        let mut seen = HashSet::new();
        for (record, store, _) in &holders {
            let mut next: Vec<ContentId> = record.commit_head_id.iter().cloned().collect();
            while let Some(cid) = next.pop() {
                if !seen.insert(cid.address_digest()) {
                    continue;
                }
                let commit = load_commit_by_id(store.as_ref(), &cid).await?;
                next.extend(commit.parents.iter().cloned());
                for (_, flake) in commit.occurrences() {
                    if let Some(string) = dictionary_string(&flake.o) {
                        if erased_strings.contains(string.as_ref()) {
                            strings_in_use.insert(string.into_owned());
                        }
                    }
                    let Some(c) = captured.iter().find(|c| c.flake.o == flake.o) else {
                        continue;
                    };
                    if !in_use.contains(&&c.flake.o) {
                        in_use.push(&c.flake.o);
                    }
                    let key = FactKey::of(flake);
                    if captured.iter().any(|c| FactKey::of(&c.flake) == key)
                        && !facts_in_use.contains(&key)
                    {
                        facts_in_use.push(key);
                    }
                }
            }
        }

        // The holders' schedules, past the substitution.
        let prefix = runtime.index.address_prefix();
        let storage = self
            .admin_storage()
            .ok_or_else(|| ApiError::internal("a purge needs managed storage"))?;
        let mut scheduled: Vec<(String, &Captured)> = Vec::new();
        for (record, _, _) in &holders {
            let folder = format!("{prefix}{}/", record.storage_namespace().branch_prefix());
            for address in storage.list_prefix(&format!("{folder}retention/")).await? {
                if !address.ends_with(".frs") {
                    continue;
                }
                let Some(bytes) = runtime.index.read_raw(&address).await? else {
                    continue;
                };
                let schedule = Schedule::decode(&bytes)
                    .map_err(|e| ApiError::internal(format!("schedule {address}: {e}")))?;
                for (key, _) in schedule.entries() {
                    if let Some(c) = captured.iter().find(|c| FactKey::of(&c.flake) == *key) {
                        scheduled.push((address.clone(), c));
                    }
                }
            }
        }
        let mut unreferenced = BTreeSet::new();
        let roots: BTreeSet<_> = holders
            .iter()
            .filter(|(_, _, live)| *live)
            .map(|(r, _, _)| r.storage_namespace().root().clone())
            .collect();
        for root in &roots {
            unreferenced.extend(self.unresolved_garbage(root).await?);
        }
        // A schedule may hold a fact a surviving occurrence asserts again.
        let schedules: BTreeSet<String> = scheduled
            .into_iter()
            .filter(|(_, c)| !facts_in_use.contains(&FactKey::of(&c.flake)))
            .map(|(address, _)| address)
            .collect();

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
            for string in erased_strings.difference(&strings_in_use) {
                let held = store
                    .find_string_id(string)
                    .map_err(|e| ApiError::internal(format!("dictionary lookup: {e}")))?;
                if held.is_some() {
                    dictionary.insert(record.ledger_id.to_string());
                }
            }
        }
        Ok(PurgeScan {
            unreferenced: unreferenced.into_iter().collect(),
            schedules: schedules.into_iter().collect(),
            indexed: indexed.into_iter().collect(),
            dictionary: dictionary.into_iter().collect(),
            unreleased: {
                let mut files = self.dropped_index_files(&plan.commits).await?;
                files.extend(self.retracted_index_files(&plan.retracted).await?);
                files.extend(self.source_leftovers(plan).await?);
                files
            },
            in_use: in_use.len(),
        })
    }
}
