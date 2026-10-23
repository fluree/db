//! Physical purge: what holds the values of a ledger's published erasures,
//! inventoried before anything is deleted (DATA_ERASURE.md, 5B).

use std::collections::{BTreeMap, BTreeSet};

use fluree_db_core::commit::codec::read_commit_envelope;
use fluree_db_core::{ContentId, LedgerId};
use serde::{Deserialize, Serialize};

use super::ErasureStage;
use crate::retention::registry;
use crate::{ApiError, Fluree, Result};

/// What a purge of a ledger's published erasures deletes or rebuilds. It
/// names requests, files and branches, never a value.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PurgePlan {
    /// The published requests it covers.
    pub requests: Vec<String>,
    /// Every copy of an erased commit's original, in any folder.
    pub originals: Vec<String>,
    /// The transaction documents of those commits, in each folder that
    /// holds a copy of a commit naming one.
    pub transactions: Vec<String>,
    /// Branches whose indexes are rebuilt from the replacements.
    pub rebuild: Vec<String>,
    /// Branches of soft-dropped ledgers, whose index artifacts are released.
    pub release: Vec<String>,
}

impl Fluree {
    /// Plan the purge of `ledger`'s published erasures, to inspect before
    /// anything is deleted.
    pub async fn plan_purge(&self, ledger: &str) -> Result<PurgePlan> {
        let runtime = self.erasure_runtime()?;
        let name = LedgerId::parse(ledger)?;
        let ns = self.publisher()?;
        let mut plan = PurgePlan::default();
        let mut erased = BTreeSet::new();
        let mut rebuild = BTreeSet::new();
        for record in self.nameservice().list_branches(name.name()).await? {
            if record.retracted {
                continue;
            }
            let branch = record.ledger_id.to_string();
            let Some(registration) = registry::read(ns, &branch).await?.1 else {
                continue;
            };
            for job in registration
                .erasures
                .iter()
                .filter(|j| j.stage == ErasureStage::Published)
            {
                plan.requests.push(job.request_id.clone());
                erased.extend(job.commits.iter().map(|c| c.commit.clone()));
                if let Some(holders) = self.holders(&branch, job).await? {
                    rebuild.extend(holders.iter().map(|r| r.ledger_id.to_string()));
                }
            }
        }
        let commits: Vec<&str> = erased.iter().map(String::as_str).collect();
        let release: BTreeSet<String> = self
            .dropped_holders(&commits)
            .await?
            .into_iter()
            .map(|(r, _)| r.ledger_id.to_string())
            .collect();

        // Each erased original's digest, with its transaction document's,
        // which the replacement keeps in its envelope.
        let mut txn_of: BTreeMap<String, Option<String>> = BTreeMap::new();
        for commit in &erased {
            let cid: ContentId = commit
                .parse()
                .map_err(|_| ApiError::internal(format!("not a commit id: {commit}")))?;
            let digest = cid.address_digest();
            let Some(bytes) = runtime.index.replacement(&digest).await else {
                continue;
            };
            let envelope = read_commit_envelope(&bytes?)
                .map_err(|e| ApiError::internal(format!("replacement of {commit}: {e}")))?;
            txn_of.insert(digest, envelope.txn.map(|t| t.address_digest()));
        }

        let listing = runtime.index.list_all().await?;
        let mut named: BTreeSet<(&str, &str)> = BTreeSet::new();
        for address in &listing {
            let Some((folder, file)) = address.rsplit_once("/commit/") else {
                continue;
            };
            let Some(txn) = file.strip_suffix(".fcv2").and_then(|d| txn_of.get(d)) else {
                continue;
            };
            plan.originals.push(address.clone());
            if let Some(txn) = txn {
                named.insert((folder, txn));
            }
        }
        for address in &listing {
            let Some((folder, file)) = address.rsplit_once("/txn/") else {
                continue;
            };
            if file
                .strip_suffix(".json")
                .is_some_and(|d| named.contains(&(folder, d)))
            {
                plan.transactions.push(address.clone());
            }
        }
        plan.originals.sort();
        plan.transactions.sort();
        plan.rebuild = rebuild.into_iter().collect();
        plan.release = release.into_iter().collect();
        Ok(plan)
    }
}
