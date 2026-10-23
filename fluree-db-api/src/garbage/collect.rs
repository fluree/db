//! Deleting recorded blobs that no branch references and no writer pins.

use std::time::Duration;

use fluree_db_core::BlobClaim;
use fluree_db_nameservice::NameBinding;
use serde::Serialize;

use super::{now_ms, Blob, GarbageReason, Recorded};
use crate::{Fluree, Result};

/// What a collection pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GarbageReport {
    /// Records written for blobs their writers abandoned.
    pub recorded: usize,
    /// Blobs deleted.
    pub deleted: usize,
    /// Records dropped because a branch references the blob, or it is gone.
    pub resolved: usize,
    /// Records past their grace period left for a later pass: pinned, or
    /// their ledger unsettled.
    pub deferred: usize,
}

impl Fluree {
    /// Record the blobs writers abandoned, then delete every recorded blob
    /// older than `grace` that no branch references and no writer pins. Does
    /// nothing where this instance does not collect.
    pub async fn collect_garbage(&self, grace: Duration) -> Result<GarbageReport> {
        let Some(pins) = self.blob_pins().cloned() else {
            return Ok(GarbageReport::default());
        };
        let mut report = GarbageReport::default();
        let abandoned: Vec<Blob> = pins
            .take_abandoned()
            .into_iter()
            .map(|a| Blob::new(a.namespace, a.id))
            .collect();
        report.recorded = abandoned.len();
        self.record_garbage(&abandoned, GarbageReason::Abandoned)
            .await?;
        let cutoff = now_ms() - i64::try_from(grace.as_millis()).unwrap_or(i64::MAX);
        for (name, binding) in self.publisher()?.list_bindings().await? {
            if let Err(e) = self
                .collect_ledger(&name, &binding.value, cutoff, &mut report)
                .await
            {
                tracing::warn!(ledger = %name, error = %e, "garbage collection failed");
            }
        }
        Ok(report)
    }

    async fn collect_ledger(
        &self,
        name: &str,
        binding: &NameBinding,
        cutoff: i64,
        report: &mut GarbageReport,
    ) -> Result<()> {
        let pins = self.blob_pins().expect("collecting").clone();
        let due: Vec<Recorded> = self
            .garbage_records(&binding.root)
            .await?
            .into_iter()
            .filter(|r| r.record.recorded_ms <= cutoff)
            .collect();
        if due.is_empty() {
            return Ok(());
        }
        let Some(mut holders) = self.load_holders(name, binding).await? else {
            report.deferred += due.len();
            return Ok(());
        };
        // Claimed before the last check: a writer storing one of these again
        // waits for the pass, and anything published before the claims is
        // caught up on.
        let mut claimed: Vec<(Recorded, BlobClaim)> = Vec::new();
        for recorded in due {
            match pins.try_claim(&recorded.blob.id) {
                Some(claim) => claimed.push((recorded, claim)),
                None => report.deferred += 1,
            }
        }
        if !holders.catch_up(self).await? {
            report.deferred += claimed.len();
            return Ok(());
        }
        let storage = self.garbage_storage()?;
        let method = storage.storage_method().to_string();
        for (recorded, _claim) in claimed {
            let Some(address) = recorded.blob.address(&method) else {
                continue;
            };
            if !storage.exists(&address).await? || holders.references(self, &recorded.blob).await? {
                report.resolved += 1;
            } else {
                storage.delete(&address).await?;
                report.deleted += 1;
            }
            storage.delete(&recorded.address).await?;
        }
        Ok(())
    }
}
