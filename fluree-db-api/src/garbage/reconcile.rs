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
    /// Commit and transaction files listed.
    pub listed: usize,
    /// Of those, recorded as garbage.
    pub recorded: usize,
}

/// A ledger's last completed reconciliation, beside its records.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Reconciled {
    started_ms: i64,
    completed_ms: i64,
}

const RECONCILED: &str = "reconciled.json";

impl Fluree {
    /// Record the commit and transaction files in ledger `name`'s branch
    /// folders that no branch references. `None` when the ledger is not
    /// settled, or this instance does not collect.
    pub async fn reconcile_garbage(&self, name: &str) -> Result<Option<ReconcileReport>> {
        if self.blob_pins().is_none() {
            return Ok(None);
        }
        let Some(binding) = self.publisher()?.get_binding(name).await? else {
            return Ok(None);
        };
        let binding = binding.value;
        let started_ms = now_ms();
        let Some(holders) = self.load_holders(name, &binding).await? else {
            return Ok(None);
        };
        let storage = self.garbage_storage()?;
        let method = storage.storage_method().to_string();
        let recorded: HashSet<Blob> = self
            .garbage_records(&binding.root)
            .await?
            .into_iter()
            .map(|r| r.blob)
            .collect();
        let mut report = ReconcileReport::default();
        let mut found = Vec::new();
        for listing in &binding.branches {
            let namespace = StorageNamespace::new(binding.root.clone(), &listing.branch);
            for (folder, codec, suffix) in [
                ("commit", CODEC_FLUREE_COMMIT, ".fcv2"),
                ("txn", CODEC_FLUREE_TXN, ".json"),
            ] {
                let prefix = format!("fluree:{method}://{}/{folder}/", namespace.branch_prefix());
                for address in storage.list_prefix(&prefix).await? {
                    let Some(id) = address
                        .strip_prefix(&prefix)
                        .and_then(|f| f.strip_suffix(suffix))
                        .and_then(|digest| ContentId::from_address_digest(codec, digest))
                    else {
                        continue;
                    };
                    report.listed += 1;
                    let blob = Blob::new(namespace.clone(), id);
                    if !recorded.contains(&blob) && !holders.references(self, &blob).await? {
                        found.push(blob);
                    }
                }
            }
        }
        self.record_garbage(&found, GarbageReason::Reconciled)
            .await?;
        report.recorded = found.len();
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
            .reconcile_garbage(&name)
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
    /// `since_ms`, running one if there is none. `false` while that cannot
    /// be established: the ledger is unsettled, or a writer pins a file.
    pub(crate) async fn resolve_garbage(&self, name: &str, since_ms: i64) -> Result<bool> {
        let Some(binding) = self.publisher()?.get_binding(name).await? else {
            return Ok(false);
        };
        let root = &binding.value.root;
        let covered = |r: &Option<Reconciled>| r.is_some_and(|r| r.started_ms >= since_ms);
        let mut reconciled = self.reconciled(root).await?;
        if !covered(&reconciled) {
            if self.reconcile_garbage(name).await?.is_none() {
                return Ok(false);
            }
            reconciled = self.reconciled(root).await?;
        }
        if !covered(&reconciled) {
            return Ok(false);
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
