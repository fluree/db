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
use crate::retention::state::{FactKey, Schedule};
use crate::{ApiError, Fluree, Result};

/// What a scan found. It names files and branches, never a value.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PurgeScan {
    /// Files in a holder's folder that no holder's history reaches, holding
    /// a captured value: commits a rebase left, transaction documents of
    /// transactions that failed.
    pub unreferenced: Vec<String>,
    /// TTL schedules that still hold an erased fact.
    pub schedules: Vec<String>,
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
        self.unreferenced.is_empty()
            && self.schedules.is_empty()
            && self.indexed.is_empty()
            && self.dictionary.is_empty()
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

        // The files in the holders' folders, past the substitution.
        let prefix = runtime.index.address_prefix();
        let folders: Vec<String> = holders
            .iter()
            .map(|(r, _, _)| format!("{prefix}{}/", r.storage_namespace().branch_prefix()))
            .collect();
        let planned: HashSet<&str> = plan.transactions.iter().map(String::as_str).collect();
        let lexical: Vec<String> = captured
            .iter()
            .filter_map(|c| lexical(&c.flake.o))
            .collect();
        let mut in_use: Vec<&FlakeValue> = Vec::new();
        let mut facts_in_use: Vec<FactKey> = Vec::new();
        let mut unreferenced = BTreeSet::new();
        let mut scheduled: Vec<(String, &Captured)> = Vec::new();
        for address in runtime.index.list_all().await? {
            let Some(file) = folders
                .iter()
                .find_map(|f| address.strip_prefix(f.as_str()))
            else {
                continue;
            };
            if let Some(digest) = file
                .strip_prefix("commit/")
                .and_then(|f| f.strip_suffix(".fcv2"))
            {
                if erased.contains(digest) {
                    continue;
                }
                let reached = reachable.commits.contains(digest);
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
                        continue;
                    }
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
            } else if let Some(digest) = file
                .strip_prefix("txn/")
                .and_then(|f| f.strip_suffix(".json"))
            {
                // A reachable commit's document holds what the commit does.
                if planned.contains(address.as_str()) || reachable.transactions.contains(digest) {
                    continue;
                }
                let Some(bytes) = runtime.index.read_raw(&address).await? else {
                    continue;
                };
                if document_holds(&bytes, &lexical) {
                    unreferenced.insert(address.clone());
                }
            } else if file.starts_with("retention/") && file.ends_with(".frs") {
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
            schedules: schedules.into_iter().collect(),
            indexed: indexed.into_iter().collect(),
            dictionary: dictionary.into_iter().collect(),
            in_use: in_use.len(),
        })
    }
}

/// What the holders' histories reach.
#[derive(Default)]
struct Reachable {
    /// Commit digests, through every parent of a merge.
    commits: HashSet<String>,
    /// The digests of the transaction documents those commits name.
    transactions: HashSet<String>,
}

async fn reachable_commits(
    holders: &[(NsRecord, Arc<dyn ContentStore>, bool)],
) -> Result<Reachable> {
    let mut reachable = Reachable::default();
    for (record, store, _) in holders {
        let mut next: Vec<ContentId> = record.commit_head_id.iter().cloned().collect();
        while let Some(cid) = next.pop() {
            if !reachable.commits.insert(cid.address_digest()) {
                continue;
            }
            let envelope = load_commit_envelope_by_id(store.as_ref(), &cid).await?;
            reachable
                .transactions
                .extend(envelope.txn.map(|t| t.address_digest()));
            next.extend(envelope.parents);
        }
    }
    Ok(reachable)
}

/// `value` as a document writes it; `None` for a reference, which names a
/// subject rather than holding a value.
fn lexical(value: &FlakeValue) -> Option<String> {
    match value {
        FlakeValue::Ref(_) | FlakeValue::Null | FlakeValue::TripleTerm(_) => None,
        FlakeValue::String(s) | FlakeValue::Json(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// Whether a stored transaction document holds one of `values`: as a JSON
/// value, as a literal in the text of a query or RDF body, or inside a
/// signed or base64 payload.
fn document_holds(bytes: &[u8], values: &[String]) -> bool {
    match serde_json::from_slice::<serde_json::Value>(bytes) {
        Ok(json) => json_holds(&json, values),
        Err(_) => text_holds(&String::from_utf8_lossy(bytes), values),
    }
}

fn json_holds(json: &serde_json::Value, values: &[String]) -> bool {
    use base64::Engine as _;
    use serde_json::Value;
    match json {
        Value::String(s) => {
            values.iter().any(|v| v == s)
                || text_holds(s, values)
                || s.strip_prefix("base64:")
                    .and_then(|b| base64::engine::general_purpose::STANDARD.decode(b).ok())
                    .is_some_and(|b| document_holds(&b, values))
                || jws_payload(s).is_some_and(|b| document_holds(&b, values))
        }
        Value::Number(n) => values.iter().any(|v| {
            *v == n.to_string() || (n.as_f64().is_some() && v.parse::<f64>().ok() == n.as_f64())
        }),
        Value::Bool(b) => values.iter().any(|v| *v == b.to_string()),
        Value::Array(items) => items.iter().any(|i| json_holds(i, values)),
        Value::Object(map) => map.values().any(|i| json_holds(i, values)),
        Value::Null => false,
    }
}

/// Whether `text` writes one of `values` as a quoted literal.
fn text_holds(text: &str, values: &[String]) -> bool {
    values
        .iter()
        .any(|v| text.contains(&format!("\"{v}\"")) || text.contains(&format!("'{v}'")))
}

/// The payload of a compact JWS: its middle segment, base64url.
fn jws_payload(s: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    let mut parts = s.split('.');
    let (Some(_), Some(payload), Some(_), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return None;
    };
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()
}
