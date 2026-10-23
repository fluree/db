//! Deleting recorded blobs that no branch references and no writer pins.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use fluree_db_core::BlobClaim;
use fluree_db_nameservice::NameBinding;
use serde::Serialize;

use super::{now_ms, Blob, GarbageReason, Recorded};
use crate::{Fluree, Result};

/// Blobs claimed together, so a writer storing one again waits for no more
/// than one batch's checks.
const CLAIM_BATCH: usize = 32;

/// What a collection pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GarbageReport {
    /// Records written for blobs their writers abandoned.
    pub recorded: usize,
    /// Blobs deleted.
    pub deleted: usize,
    /// Of those, by ledger.
    pub deleted_in: BTreeMap<String, usize>,
    /// Records dropped because a branch references the blob, or it is gone.
    pub resolved: usize,
    /// Records past their grace period left for a later pass: pinned, or
    /// their ledger unsettled.
    pub deferred: usize,
    /// Records past their grace period the pass ran out of time for.
    pub left: usize,
    pub timings: CollectTimings,
}

/// Where a collection pass's time went.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectTimings {
    /// Loading the branches to their heads.
    pub load: Duration,
    /// Accounting for commits published since.
    pub catch_up: Duration,
    /// Checking and deleting the claimed blobs.
    pub check: Duration,
}

impl Fluree {
    /// Record the blobs writers abandoned, then delete every recorded blob
    /// older than `grace` that no branch references and no writer pins. Does
    /// nothing where this instance does not collect.
    pub async fn collect_garbage(&self, grace: Duration) -> Result<GarbageReport> {
        self.collect_garbage_within(grace, None).await
    }

    /// [`Self::collect_garbage`], claiming no more blobs once `budget` is
    /// spent.
    pub(crate) async fn collect_garbage_within(
        &self,
        grace: Duration,
        budget: Option<Duration>,
    ) -> Result<GarbageReport> {
        let deadline = budget.and_then(|b| Instant::now().checked_add(b));
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
                .collect_ledger(&name, &binding.value, cutoff, deadline, &mut report)
                .await
            {
                tracing::warn!(ledger = %name, error = %e, "garbage collection failed");
            }
        }
        Ok(report)
    }

    pub(super) async fn collect_ledger(
        &self,
        name: &str,
        binding: &NameBinding,
        cutoff: i64,
        deadline: Option<Instant>,
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
        let Some(_pass) = pins.try_collect_root(&binding.root) else {
            report.deferred += due.len();
            return Ok(());
        };
        let clock = Instant::now();
        let holders = self.load_holders(name, binding).await?;
        report.timings.load += clock.elapsed();
        let Some(mut holders) = holders else {
            report.deferred += due.len();
            return Ok(());
        };
        let storage = self.garbage_storage()?;
        let method = storage.storage_method().to_string();
        // Claimed a batch at a time, before the last check: a writer storing
        // one of these again waits for its batch only, and anything published
        // before the claims is caught up on.
        let mut due = due.into_iter().peekable();
        while due.peek().is_some() {
            if deadline.is_some_and(|d| Instant::now() >= d) {
                report.left += due.count();
                return Ok(());
            }
            let mut claimed: Vec<(Recorded, BlobClaim)> = Vec::new();
            for recorded in due.by_ref().take(CLAIM_BATCH) {
                match pins.try_claim(&recorded.blob.id) {
                    Some(claim) => claimed.push((recorded, claim)),
                    None => report.deferred += 1,
                }
            }
            let clock = Instant::now();
            let caught_up = holders.catch_up(self).await?;
            report.timings.catch_up += clock.elapsed();
            if !caught_up {
                report.deferred += claimed.len() + due.count();
                return Ok(());
            }
            let clock = Instant::now();
            for (recorded, _claim) in claimed {
                let Some(address) = recorded.blob.address(&method) else {
                    continue;
                };
                if !storage.exists(&address).await?
                    || holders.references(self, &recorded.blob).await?
                {
                    report.resolved += 1;
                } else {
                    storage.delete(&address).await?;
                    report.deleted += 1;
                    *report.deleted_in.entry(name.to_string()).or_default() += 1;
                }
                storage.delete(&recorded.address).await?;
            }
            report.timings.check += clock.elapsed();
        }
        Ok(())
    }
}
