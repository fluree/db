//! Recording the commit and transaction files that no branch references and
//! no record names yet: those a writer left when it crashed before
//! abandoning them, those a history rewrite left, and those from before
//! anything collected.

use std::collections::HashSet;
use std::time::Duration;

use fluree_db_core::{
    ContentId, StorageNamespace, StorageRoot, CODEC_FLUREE_COMMIT, CODEC_FLUREE_TXN,
};
use fluree_db_nameservice::BindingState;
use serde::{Deserialize, Serialize};

use super::{dir_address, now_ms, Blob, GarbageReason};
use crate::{Fluree, Result};

/// What reconciling a ledger found.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconcileReport {
    /// Commit and transaction files checked.
    pub listed: usize,
    /// Of those, recorded as garbage.
    pub recorded: usize,
    /// Every file is checked.
    pub complete: bool,
}

/// A ledger's last completed reconciliation, beside its records.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Reconciled {
    started_ms: i64,
    completed_ms: i64,
}

/// A reconciliation in progress: when it started, and the last file it
/// checked.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Reconciling {
    started_ms: i64,
    after: String,
}

const RECONCILED: &str = "reconciled.json";
const RECONCILING: &str = "reconciling.json";

/// Files one reconciliation step checks at most.
const STEP: usize = 50_000;

impl Fluree {
    /// Record the commit and transaction files in ledger `name`'s branch
    /// folders that no branch references. `None` when the ledger is not
    /// settled, or this instance does not collect.
    pub async fn reconcile_garbage(&self, name: &str) -> Result<Option<ReconcileReport>> {
        let mut total = ReconcileReport::default();
        loop {
            let Some(step) = self.reconcile_step(name, STEP, None).await? else {
                return Ok(None);
            };
            total.listed += step.listed;
            total.recorded += step.recorded;
            if step.complete {
                total.complete = true;
                return Ok(Some(total));
            }
        }
    }

    /// Check up to `budget` more of ledger `name`'s commit and transaction
    /// files, resuming the reconciliation in progress unless it started
    /// before `not_before`.
    pub(super) async fn reconcile_step(
        &self,
        name: &str,
        budget: usize,
        not_before: Option<i64>,
    ) -> Result<Option<ReconcileReport>> {
        if self.blob_pins().is_none() {
            return Ok(None);
        }
        let Some(binding) = self.publisher()?.get_binding(name).await? else {
            return Ok(None);
        };
        let binding = binding.value;
        let storage = self.garbage_storage()?;
        let method = storage.storage_method().to_string();
        let progress_address = format!("{}{RECONCILING}", dir_address(&method, &binding.root));
        let progress = match storage.read_bytes(&progress_address).await {
            Ok(bytes) => serde_json::from_slice::<Reconciling>(&bytes).ok(),
            Err(fluree_db_core::Error::NotFound(_)) => None,
            Err(e) => return Err(e.into()),
        }
        .filter(|p| not_before.is_none_or(|ms| p.started_ms >= ms));
        let started_ms = progress.as_ref().map_or_else(now_ms, |p| p.started_ms);
        let Some(holders) = self.load_holders(name, &binding).await? else {
            return Ok(None);
        };
        let recorded: HashSet<Blob> = self
            .garbage_records(&binding.root)
            .await?
            .into_iter()
            .map(|r| r.blob)
            .collect();
        // Every file in one order, so a step resumes after the last checked.
        let mut files = Vec::new();
        for listing in &binding.branches {
            let namespace = StorageNamespace::new(binding.root.clone(), &listing.branch);
            for (folder, codec, suffix) in [
                ("commit", CODEC_FLUREE_COMMIT, ".fcv2"),
                ("txn", CODEC_FLUREE_TXN, ".json"),
            ] {
                let prefix = format!("fluree:{method}://{}/{folder}/", namespace.branch_prefix());
                for address in storage.list_prefix(&prefix).await? {
                    let id = address
                        .strip_prefix(&prefix)
                        .and_then(|f| f.strip_suffix(suffix))
                        .and_then(|digest| ContentId::from_address_digest(codec, digest));
                    if let Some(id) = id {
                        files.push((address, Blob::new(namespace.clone(), id)));
                    }
                }
            }
        }
        files.sort_by(|a, b| a.0.cmp(&b.0));
        let start = progress.as_ref().map_or(0, |p| {
            files.partition_point(|(a, _)| a.as_str() <= p.after.as_str())
        });
        let end = files.len().min(start + budget);
        let mut found = Vec::new();
        for (_, blob) in &files[start..end] {
            if !recorded.contains(blob) && !holders.references(self, blob).await? {
                found.push(blob.clone());
            }
        }
        self.record_garbage(&found, GarbageReason::Reconciled)
            .await?;
        let report = ReconcileReport {
            listed: end - start,
            recorded: found.len(),
            complete: end == files.len(),
        };
        if report.complete {
            let done = Reconciled {
                started_ms,
                completed_ms: now_ms(),
            };
            storage
                .write_bytes(
                    &reconciled_address(&method, &binding.root),
                    &serde_json::to_vec(&done)?,
                )
                .await?;
            if progress.is_some() {
                storage.delete(&progress_address).await?;
            }
        } else {
            let progress = Reconciling {
                started_ms,
                after: files[end - 1].0.clone(),
            };
            storage
                .write_bytes(&progress_address, &serde_json::to_vec(&progress)?)
                .await?;
        }
        Ok(Some(report))
    }

    /// Reconcile the settled ledger least recently reconciled, unless every
    /// one was within `every`. Returns its name and report.
    pub(crate) async fn reconcile_next(
        &self,
        every: Duration,
    ) -> Result<Option<(String, ReconcileReport)>> {
        if self.blob_pins().is_none() {
            return Ok(None);
        }
        let due_before = now_ms() - i64::try_from(every.as_millis()).unwrap_or(i64::MAX);
        let mut oldest: Option<(Option<i64>, String)> = None;
        for (name, binding) in self.publisher()?.list_bindings().await? {
            if !matches!(binding.value.state, BindingState::Active) {
                continue;
            }
            let from = self
                .reconciled(&binding.value.root)
                .await?
                .map(|r| r.started_ms);
            if from.is_some_and(|ms| ms > due_before) {
                continue;
            }
            if oldest.as_ref().is_none_or(|(o, _)| from < *o) {
                oldest = Some((from, name));
            }
        }
        let Some((_, name)) = oldest else {
            return Ok(None);
        };
        Ok(self
            .reconcile_step(&name, STEP, None)
            .await?
            .map(|report| (name, report)))
    }

    /// The last completed reconciliation of the ledger at `root`.
    async fn reconciled(&self, root: &StorageRoot) -> Result<Option<Reconciled>> {
        let storage = self.garbage_storage()?;
        let address = reconciled_address(storage.storage_method(), root);
        match storage.read_bytes(&address).await {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
            Err(fluree_db_core::Error::NotFound(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Collect, without a grace period, every file of ledger `name` that no
    /// branch references, as of a reconciliation started at or after
    /// `since_ms`, taking a step of one if there is none. `false` while that
    /// cannot be established: the reconciliation is not complete, the ledger
    /// is unsettled, or a writer pins a file.
    pub(crate) async fn resolve_garbage(&self, name: &str, since_ms: i64) -> Result<bool> {
        let Some(binding) = self.publisher()?.get_binding(name).await? else {
            return Ok(false);
        };
        let root = &binding.value.root;
        let covered = |r: Option<Reconciled>| r.is_some_and(|r| r.started_ms >= since_ms);
        if !covered(self.reconciled(root).await?) {
            let step = self.reconcile_step(name, STEP, Some(since_ms)).await?;
            if !step.is_some_and(|s| s.complete) || !covered(self.reconciled(root).await?) {
                return Ok(false);
            }
        }
        let mut report = super::GarbageReport::default();
        self.collect_ledger(name, &binding.value, now_ms(), &mut report)
            .await?;
        Ok(self.unresolved_garbage(root).await?.is_empty())
    }

    /// The files recorded as garbage under `root`, by the last completed
    /// reconciliation or before it, and not yet collected.
    pub(crate) async fn unresolved_garbage(&self, root: &StorageRoot) -> Result<Vec<String>> {
        let Some(reconciled) = self.reconciled(root).await? else {
            return Ok(Vec::new());
        };
        let method = self.garbage_storage()?.storage_method().to_string();
        Ok(self
            .garbage_records(root)
            .await?
            .into_iter()
            .filter(|r| r.record.recorded_ms <= reconciled.completed_ms)
            .filter_map(|r| r.blob.address(&method))
            .collect())
    }
}

fn reconciled_address(method: &str, root: &StorageRoot) -> String {
    format!("{}{RECONCILED}", dir_address(method, root))
}
