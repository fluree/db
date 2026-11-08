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
    /// On a cluster: blobs condemned for the next pass to delete.
    pub condemned: usize,
    /// On a cluster: condemned blobs a writer stored again.
    pub spared: usize,
    /// Records past their grace period the pass ran out of time for.
    pub left: usize,
    /// Ledgers the pass ran out of time before reaching.
    pub ledgers_left: usize,
    /// Records in the ledgers reached still within their grace period.
    pub waiting: usize,
    /// When the oldest record in the ledgers reached was made.
    pub oldest_ms: Option<i64>,
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

    /// A cluster member's collection pass with intents settling for `settle`
    /// rather than [`SETTLE`](super::cluster::SETTLE); for tests.
    #[cfg(feature = "testing")]
    pub async fn collect_garbage_settled(
        &self,
        grace: Duration,
        settle: Duration,
    ) -> Result<GarbageReport> {
        let collector = self
            .collector()
            .ok_or_else(|| crate::ApiError::internal("not a cluster member"))?;
        self.collect_in_cluster(collector, grace, settle, None)
            .await
    }

    /// [`Self::collect_garbage`], starting nothing more once `budget` is
    /// spent: a soft limit, since a ledger's records are read and its
    /// branches loaded whole. The next pass starts with the first ledger
    /// this one ran out of time for.
    pub async fn collect_garbage_within(
        &self,
        grace: Duration,
        budget: Option<Duration>,
    ) -> Result<GarbageReport> {
        let deadline = budget.and_then(|b| Instant::now().checked_add(b));
        if let Some(collector) = self.collector() {
            return self
                .collect_in_cluster(collector, grace, super::cluster::SETTLE, deadline)
                .await;
        }
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
        let from = self.root.collect_from.lock().clone();
        let mut next_from = None;
        for (name, binding) in resume_order(self.publisher()?.list_bindings().await?, from) {
            match self
                .collect_ledger(&name, &binding.value, cutoff, deadline, &mut report)
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    next_from.get_or_insert(name);
                }
                Err(e) => {
                    tracing::warn!(ledger = %name, error = %e, "garbage collection failed");
                }
            }
        }
        *self.root.collect_from.lock() = next_from;
        Ok(report)
    }

    /// Collect ledger `name`'s due records, claiming none once `deadline`
    /// passes, as checked before each costly phase. `false` when it ran out
    /// of time.
    pub(super) async fn collect_ledger(
        &self,
        name: &str,
        binding: &NameBinding,
        cutoff: i64,
        deadline: Option<Instant>,
        report: &mut GarbageReport,
    ) -> Result<bool> {
        let out_of_time = || deadline.is_some_and(|d| Instant::now() >= d);
        if out_of_time() {
            report.ledgers_left += 1;
            return Ok(false);
        }
        let pins = self.blob_pins().expect("collecting").clone();
        let (due, waiting): (Vec<Recorded>, Vec<Recorded>) = self
            .garbage_records(&binding.root)
            .await?
            .into_iter()
            .partition(|r| r.record.recorded_ms <= cutoff);
        report.waiting += waiting.len();
        let oldest = due
            .iter()
            .chain(&waiting)
            .map(|r| r.record.recorded_ms)
            .min();
        report.oldest_ms = report.oldest_ms.into_iter().chain(oldest).min();
        if due.is_empty() {
            return Ok(true);
        }
        let Some(_pass) = pins.try_collect_root(&binding.root) else {
            report.deferred += due.len();
            return Ok(true);
        };
        if out_of_time() {
            report.left += due.len();
            return Ok(false);
        }
        let clock = Instant::now();
        let holders = self.load_holders(name, binding).await?;
        report.timings.load += clock.elapsed();
        let Some(mut holders) = holders else {
            report.deferred += due.len();
            return Ok(true);
        };
        let storage = self.garbage_storage()?;
        let method = storage.storage_method().to_string();
        // Claimed a batch at a time, before the last check: a writer storing
        // one of these again waits for its batch only, and anything published
        // before the claims is caught up on.
        let mut due = due.into_iter().peekable();
        while due.peek().is_some() {
            if out_of_time() {
                report.left += due.count();
                return Ok(false);
            }
            let batch = Instant::now();
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
                return Ok(true);
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
            // A pass with a budget runs in the background: it pauses as long
            // as each batch took, its claims released, so its deletes take
            // at most half the device the commits share it with. Not after
            // the last: the pause would only hold the root's collection.
            if let Some(deadline) = deadline.filter(|_| due.peek().is_some()) {
                let left = deadline.saturating_duration_since(Instant::now());
                tokio::time::sleep(batch.elapsed().min(left)).await;
            }
        }
        Ok(true)
    }
}

/// `ledgers` by name, from `from` on and then the rest, so a pass resumes
/// with the ledger the last ran out of time for.
pub(super) fn resume_order<T>(
    mut ledgers: Vec<(String, T)>,
    from: Option<String>,
) -> Vec<(String, T)> {
    ledgers.sort_by(|a, b| a.0.cmp(&b.0));
    if let Some(from) = from {
        let at = ledgers.partition_point(|(name, _)| *name < from);
        ledgers.rotate_left(at);
    }
    ledgers
}

#[cfg(test)]
mod tests {
    use super::resume_order;

    #[test]
    fn a_pass_resumes_with_the_ledger_the_last_did_not_reach() {
        let names = |from: Option<&str>| -> Vec<String> {
            let ledgers = ["c", "a", "b"].map(|n| (n.to_string(), ())).to_vec();
            resume_order(ledgers, from.map(str::to_string))
                .into_iter()
                .map(|(n, ())| n)
                .collect()
        };
        assert_eq!(names(None), ["a", "b", "c"]);
        assert_eq!(names(Some("b")), ["b", "c", "a"]);
        // One dropped since: the next after it.
        assert_eq!(names(Some("bb")), ["c", "a", "b"]);
    }
}
