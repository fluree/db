//! Physical purge: what holds the values of a ledger's published erasures,
//! inventoried before anything is deleted (DATA_ERASURE.md, 5B).

use std::collections::{BTreeMap, BTreeSet};

use fluree_db_core::commit::codec::{read_commit, read_commit_envelope};
use fluree_db_core::{collect_first_parent_cids, ContentId, ContentKind, Flake, LedgerId};
use fluree_db_nameservice::GraphSourceType;
use serde::{Deserialize, Serialize};

use super::index::{Artifact, ErasedCommit};
use crate::retention::registry;
use crate::{ApiError, Fluree, Result};

/// What a purge of a ledger's published erasures deletes or rebuilds. It
/// names requests, files and branches, never a value.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PurgePlan {
    /// The published requests it covers.
    pub requests: Vec<String>,
    /// The commits they erased.
    pub commits: Vec<String>,
    /// Every copy of an erased commit's original, in any folder.
    pub originals: Vec<String>,
    /// The transaction documents of those commits, in each folder that
    /// holds a copy of a commit naming one.
    pub transactions: Vec<String>,
    /// Replacements a later step superseded, which still hold what it
    /// removed: earlier steps of a chain, and steps proved on one.
    pub superseded: Vec<String>,
    /// Branches whose indexes are rebuilt from the replacements.
    pub rebuild: Vec<String>,
    /// Branches of soft-dropped ledgers, whose index artifacts are released.
    pub release: Vec<String>,
    /// Branches dropped while others still fork from them, frozen, whose
    /// history holds an erased commit: their index heads are cleared and
    /// their indexes released.
    pub retracted: Vec<String>,
    /// Full-text, vector and geospatial indexes over a holder, which keep
    /// their own copies of what they index.
    pub sources: Vec<String>,
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
        let mut jobs = Vec::new();
        for record in self.nameservice().list_branches(name.name()).await? {
            if record.retracted {
                continue;
            }
            let branch = record.ledger_id.to_string();
            let Some(registration) = registry::read(ns, &branch).await?.1 else {
                continue;
            };
            jobs.extend(registration.erasures.iter().cloned());
            for job in registration.erasures.iter().filter(|j| j.purge_pending()) {
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
        let mut superseded = BTreeSet::new();
        for commit in &erased {
            let cid = commit_id(commit)?;
            if let Some(held) = runtime.index.erased(&cid).await? {
                let latest = held.steps.last().map(|s| s.replacement.as_str());
                let proved = jobs
                    .iter()
                    .flat_map(|j| &j.commits)
                    .filter(|t| t.commit == *commit)
                    .filter_map(|t| t.proved.as_ref())
                    .filter(|p| {
                        Some(p.replacement.as_str()) != latest && p.base.as_deref() != latest
                    })
                    .map(|p| p.replacement.as_str());
                let earlier = held
                    .steps
                    .iter()
                    .rev()
                    .skip(1)
                    .map(|s| s.replacement.as_str());
                for replacement in earlier.chain(proved) {
                    let address = runtime.index.artifact_address(
                        Artifact::Replacement,
                        &commit_id(replacement)?.address_digest(),
                    );
                    if runtime.index.exists_raw(&address).await? {
                        superseded.insert(address);
                    }
                }
            }
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
        plan.commits = erased.into_iter().collect();
        plan.superseded = superseded.into_iter().collect();
        plan.rebuild = rebuild.into_iter().collect();
        plan.release = release.into_iter().collect();
        plan.retracted = self
            .retracted_holders(&name, &plan.rebuild, &txn_of)
            .await?;
        let holders: BTreeSet<&String> = plan.rebuild.iter().chain(&plan.release).collect();
        plan.sources = self
            .nameservice()
            .all_graph_source_records()
            .await?
            .into_iter()
            .filter(|g| {
                !g.retracted
                    && matches!(
                        g.source_type,
                        GraphSourceType::Bm25 | GraphSourceType::Vector | GraphSourceType::Geo
                    )
                    // As the creator named it: a ledger alone is its main
                    // branch.
                    && g.dependencies.iter().any(|d| {
                        LedgerId::parse(d).is_ok_and(|id| holders.contains(&id.to_string()))
                    })
            })
            .map(|g| g.graph_source_id.to_string())
            .collect();
        Ok(plan)
    }

    /// The retracted branches, of `name`'s ledger and of the ledgers of
    /// `holders`, whose history holds a commit of `erased` (by digest).
    async fn retracted_holders<T>(
        &self,
        name: &LedgerId,
        holders: &[String],
        erased: &BTreeMap<String, T>,
    ) -> Result<Vec<String>> {
        let mut names: BTreeSet<String> = holders
            .iter()
            .filter_map(|b| LedgerId::parse(b).ok())
            .map(|id| id.name().to_string())
            .collect();
        names.insert(name.name().to_string());
        let mut retracted = Vec::new();
        // Listings of a ledger's branches leave its retracted ones out.
        for record in self.nameservice().all_records().await? {
            let (true, Some(head)) = (record.retracted, record.commit_head_id.as_ref()) else {
                continue;
            };
            if !names.contains(record.ledger_id.name()) {
                continue;
            }
            // A history that cannot be read may hold one.
            let holds = match self
                .content_store_for_record_or_id(Some(&record), &record.storage_namespace())
                .await
            {
                Ok(store) => match collect_first_parent_cids(store.as_ref(), head, 0).await {
                    Ok(cids) => cids
                        .iter()
                        .any(|(_, cid)| erased.contains_key(&cid.address_digest())),
                    Err(_) => true,
                },
                Err(_) => true,
            };
            if holds {
                retracted.push(record.ledger_id.to_string());
            }
        }
        Ok(retracted)
    }

    /// What a purge of `ledger`'s published erasures would delete or
    /// rebuild, and what it finds still holding an erased value that the
    /// plan does not account for. Deletes nothing.
    pub async fn inspect_purge(&self, ledger: &str) -> Result<PurgeInspection> {
        let plan = self.plan_purge(ledger).await?;
        let captured = self.capture_erased(&plan).await?;
        let scan = self.scan_purge(&plan, &captured).await?;
        Ok(PurgeInspection { plan, scan })
    }

    /// The facts `plan`'s erasures removed, read from the oldest bytes of
    /// each commit still stored: an original, or a replacement a later step
    /// superseded. They stay in memory for one pass and are never stored.
    pub(crate) async fn capture_erased(&self, plan: &PurgePlan) -> Result<Vec<Captured>> {
        let runtime = self.erasure_runtime()?;
        let mut captured = Vec::new();
        for commit in &plan.commits {
            let cid = commit_id(commit)?;
            let Some(held) = runtime.index.erased(&cid).await? else {
                continue;
            };
            let Some(bytes) = self.oldest_bytes(plan, &cid, &held).await? else {
                continue;
            };
            let decoded = read_commit(&bytes)
                .map_err(|e| ApiError::internal(format!("stored bytes of {commit}: {e}")))?;
            for slot in held.redacted_slots() {
                if let Some(flake) = decoded.at_slot(slot) {
                    captured.push(Captured {
                        flake: flake.clone(),
                    });
                }
            }
        }
        Ok(captured)
    }

    /// The oldest bytes of `cid` still stored: a copy of its original, or
    /// the earliest replacement of its chain.
    async fn oldest_bytes(
        &self,
        plan: &PurgePlan,
        cid: &ContentId,
        held: &ErasedCommit,
    ) -> Result<Option<Vec<u8>>> {
        let runtime = self.erasure_runtime()?;
        let file = format!("/commit/{}.fcv2", cid.address_digest());
        for address in plan.originals.iter().filter(|a| a.ends_with(&file)) {
            if let Some(bytes) = runtime.index.read_raw(address).await? {
                if ContentId::new(ContentKind::Commit, &bytes) == *cid {
                    return Ok(Some(bytes));
                }
            }
        }
        for step in &held.steps {
            let hex = commit_id(&step.replacement)?.address_digest();
            let address = runtime.index.artifact_address(Artifact::Replacement, &hex);
            if let Some(bytes) = runtime.index.read_raw(&address).await? {
                return Ok(Some(bytes));
            }
        }
        Ok(None)
    }
}

/// A purge's plan, and what its scan found.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PurgeInspection {
    pub plan: PurgePlan,
    pub scan: PurgeScan,
}

/// An erased occurrence, read from bytes that still hold it.
#[derive(Clone, Debug)]
pub(crate) struct Captured {
    pub(crate) flake: Flake,
}

fn commit_id(commit: &str) -> Result<ContentId> {
    commit
        .parse()
        .map_err(|_| ApiError::internal(format!("not a commit id: {commit}")))
}

mod erase;
pub(crate) mod release;
mod run;
mod scan;
mod sources;
#[cfg(test)]
mod tests;

pub use run::{PurgeReport, PurgeTimings};
pub use scan::PurgeScan;
