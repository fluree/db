//! Collection on a cluster, run by its leader through intents the cluster
//! condemns (see [`crate::collect`]): no member's write can name what is
//! deleted, and no replayed log can delete what is written again.
//!
//! A pass first carries on the open intents. One condemned a settle period
//! ago or more begins deleting; what writers spared stays, the rest is
//! checked against the ledger's branches and its queued requests once more,
//! a check no later write can invalidate, and what is still unreferenced is
//! deleted, each unlink within the leader's lease. Then the pass condemns
//! the recorded blobs past their grace that nothing references, and the
//! retention registration chunks no registration has named across a grace,
//! as one new intent, which the next pass deletes.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{now_ms, parse_record_address, GarbageReason, GarbageReport, Recorded};
use crate::collect::{Collector, IntentEntry};
use crate::{Fluree, Result};

/// How long an intent stands before its deletion begins: members load it
/// meanwhile, so few writes are refused for having begun before it.
pub(crate) const SETTLE: Duration = Duration::from_secs(10);

/// The most addresses one intent condemns.
const MAX_INTENT: usize = 10_000;

/// Deletes between pauses, in a pass with a budget.
const DELETE_BATCH: usize = 32;

/// Why a registration chunk is condemned: no registration names it.
const UNNAMED_CHUNK: &str = "registration";

impl GarbageReason {
    fn as_str(self) -> &'static str {
        match self {
            GarbageReason::Abandoned => "abandoned",
            GarbageReason::Rebase => "rebase",
            GarbageReason::Reconciled => "reconciled",
        }
    }
}

impl Fluree {
    /// A cluster collection pass through `collector`; see the module docs.
    pub(crate) async fn collect_in_cluster(
        &self,
        collector: &Arc<dyn Collector>,
        grace: Duration,
        settle: Duration,
        deadline: Option<Instant>,
    ) -> Result<GarbageReport> {
        self.pass_in_cluster(collector, None, grace, settle, deadline)
            .await
    }

    /// [`Self::collect_in_cluster`], condemning only ledger `name`'s
    /// recorded blobs, whatever their age, and no registration chunk: as a
    /// purge resolves the garbage of a ledger it purges. The open intents
    /// are carried on as in any pass.
    pub(crate) async fn collect_ledger_in_cluster(
        &self,
        collector: &Arc<dyn Collector>,
        name: &str,
        settle: Duration,
        deadline: Option<Instant>,
    ) -> Result<GarbageReport> {
        self.pass_in_cluster(collector, Some(name), Duration::ZERO, settle, deadline)
            .await
    }

    async fn pass_in_cluster(
        &self,
        collector: &Arc<dyn Collector>,
        only: Option<&str>,
        grace: Duration,
        settle: Duration,
        deadline: Option<Instant>,
    ) -> Result<GarbageReport> {
        let mut report = GarbageReport::default();
        // One pass at a time: another would complete an intent this one
        // still deletes.
        let _pass = self.root.cluster_pass.lock().await;
        // A member that does not lead writes no intent it cannot condemn.
        let Some(first) = collector.lease().await else {
            return Ok(report);
        };
        if !collector.enabled().await {
            return Ok(report);
        }
        // Its deletes are this term's: led again later, what it found may
        // not hold.
        let term = first.term;
        let mut condemned = HashSet::new();
        for (number, hash, deleting) in collector.open().await {
            let first_seen = *self
                .root
                .intents_seen
                .lock()
                .entry(number)
                .or_insert_with(Instant::now);
            if deleting || first_seen.elapsed() >= settle {
                if deadline.is_some_and(|d| Instant::now() >= d)
                    || !self
                        .delete_intent(collector, term, number, &hash, deadline, &mut report)
                        .await?
                {
                    return Ok(report);
                }
            } else {
                // Open still: what it condemns is not condemned again.
                condemned.extend(
                    self.intent(&hash)
                        .await?
                        .entries
                        .into_iter()
                        .map(|e| e.address),
                );
            }
        }
        let mut entries = self
            .unreferenced_due(collector, only, grace, deadline, &condemned, &mut report)
            .await?;
        if let (None, Some(objects)) = (only, self.registry_objects()) {
            for address in self.unnamed_chunks(&objects, grace).await? {
                if entries.len() < MAX_INTENT && !condemned.contains(&address) {
                    entries.push(IntentEntry {
                        address,
                        reason: UNNAMED_CHUNK.to_string(),
                        ledger: None,
                        record: None,
                    });
                }
            }
        }
        if !entries.is_empty() {
            report.condemned = entries.len();
            let hash = self.put_intent(entries).await?;
            let number = collector.condemn(&hash).await?;
            self.root
                .intents_seen
                .lock()
                .entry(number)
                .or_insert_with(Instant::now);
        }
        Ok(report)
    }

    /// Delete what intent `number` condemns and is still unreferenced, then
    /// close it. `false` when it stopped short: this member no longer leads,
    /// or the pass ran out of time; the intent stays open for the next.
    async fn delete_intent(
        &self,
        collector: &Arc<dyn Collector>,
        term: u64,
        number: u64,
        hash: &str,
        deadline: Option<Instant>,
        report: &mut GarbageReport,
    ) -> Result<bool> {
        let Some(spared) = collector.delete(number).await? else {
            self.root.intents_seen.lock().remove(&number);
            return Ok(true);
        };
        let spared: HashSet<String> = spared.into_iter().collect();
        let storage = self.garbage_storage()?;
        let method = storage.storage_method().to_string();
        let mut by_ledger: BTreeMap<String, Vec<IntentEntry>> = BTreeMap::new();
        let mut chunks = Vec::new();
        for entry in self.intent(hash).await?.entries {
            if spared.contains(&entry.address) {
                // Its writer names it or abandons it; reconciliation finds
                // it again in the second case.
                if let Some(record) = &entry.record {
                    storage.delete(record).await?;
                }
                report.spared += 1;
            } else if let Some(ledger) = entry.ledger.clone() {
                by_ledger.entry(ledger).or_default().push(entry);
            } else if entry.reason == UNNAMED_CHUNK {
                chunks.push(entry.address);
            }
        }
        let mut lease = None;
        if !chunks.is_empty() {
            // Named again by a registration written before deletion began,
            // or not at all: no later write can name these.
            let named = self.named_chunks().await?;
            let prefix = crate::retention::registry::chunk_prefix(&method);
            for address in chunks {
                if crate::retention::registry::chunk_hash(&prefix, &address)
                    .is_none_or(|hash| named.contains(hash))
                {
                    report.resolved += 1;
                    continue;
                }
                if !delete_leased(storage, collector, term, &mut lease, &address).await? {
                    return Ok(false);
                }
                report.deleted += 1;
            }
        }
        for (ledger, entries) in by_ledger {
            // Decided against the ledger's branches as they are now that
            // deletion has begun: no write begun before can name these.
            let Some(binding) = self.publisher()?.get_binding(&ledger).await? else {
                report.deferred += entries.len();
                continue;
            };
            let binding = binding.value;
            let Some(holders) = self.load_holders(&ledger, &binding).await? else {
                report.deferred += entries.len();
                continue;
            };
            let queued = collector.queued(self, &ledger).await?;
            let mut batch = Instant::now();
            for (n, entry) in entries.into_iter().enumerate() {
                let (Some(record), Some(blob)) = (
                    entry.record.as_deref(),
                    entry
                        .record
                        .as_deref()
                        .and_then(|r| parse_record_address(&method, &binding.root, r)),
                ) else {
                    // Another instance of the ledger, or no record: left.
                    report.deferred += 1;
                    continue;
                };
                if queued.contains(&blob.id) || holders.references(self, &blob).await? {
                    report.resolved += 1;
                } else {
                    if !delete_leased(storage, collector, term, &mut lease, &entry.address).await? {
                        return Ok(false);
                    }
                    report.deleted += 1;
                    *report.deleted_in.entry(ledger.clone()).or_default() += 1;
                }
                storage.delete(record).await?;
                if let Some(deadline) = deadline {
                    if (n + 1) % DELETE_BATCH == 0 {
                        if Instant::now() >= deadline {
                            return Ok(false);
                        }
                        // Paced as on a single node: deletes share the device
                        // with every commit.
                        let left = deadline.saturating_duration_since(Instant::now());
                        tokio::time::sleep(batch.elapsed().min(left)).await;
                        batch = Instant::now();
                    }
                }
            }
        }
        collector.complete(number).await?;
        self.root.intents_seen.lock().remove(&number);
        let _ = storage.delete(&self.intent_address(&method, hash)).await;
        Ok(true)
    }

    /// The recorded blobs past `grace` that no branch of their ledger and no
    /// queued request names, not condemned already, as intent entries;
    /// records of what is gone or referenced are dropped.
    async fn unreferenced_due(
        &self,
        collector: &Arc<dyn Collector>,
        only: Option<&str>,
        grace: Duration,
        deadline: Option<Instant>,
        condemned: &HashSet<String>,
        report: &mut GarbageReport,
    ) -> Result<Vec<IntentEntry>> {
        let cutoff = now_ms() - i64::try_from(grace.as_millis()).unwrap_or(i64::MAX);
        let storage = self.garbage_storage()?;
        let method = storage.storage_method().to_string();
        let mut entries = Vec::new();
        let from = self.root.collect_from.lock().clone();
        let mut next_from = None;
        let mut bindings = self.publisher()?.list_bindings().await?;
        if let Some(only) = only {
            bindings.retain(|(name, _)| name == only);
        }
        for (name, binding) in super::collect::resume_order(bindings, from) {
            if entries.len() >= MAX_INTENT || deadline.is_some_and(|d| Instant::now() >= d) {
                next_from.get_or_insert(name);
                report.ledgers_left += 1;
                continue;
            }
            let binding = binding.value;
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
            let due: Vec<(Recorded, String)> = due
                .into_iter()
                .filter_map(|r| Some((r.blob.address(&method)?, r)))
                .filter(|(address, _)| !condemned.contains(address))
                .map(|(address, r)| (r, address))
                .collect();
            if due.is_empty() {
                continue;
            }
            let Some(holders) = self.load_holders(&name, &binding).await? else {
                report.deferred += due.len();
                continue;
            };
            let queued = collector.queued(self, &name).await?;
            for (recorded, address) in due {
                if !storage.exists(&address).await?
                    || queued.contains(&recorded.blob.id)
                    || holders.references(self, &recorded.blob).await?
                {
                    storage.delete(&recorded.address).await?;
                    report.resolved += 1;
                } else {
                    entries.push(IntentEntry {
                        address,
                        reason: recorded.record.reason.as_str().to_string(),
                        ledger: Some(name.clone()),
                        record: Some(recorded.address),
                    });
                }
            }
        }
        // A pass over one ledger leaves where the next full one starts.
        if only.is_none() {
            *self.root.collect_from.lock() = next_from;
        }
        Ok(entries)
    }
}

/// Delete `address` within this member's lease as leader in `term`, taking
/// a new one once it has run out: `false`, with nothing deleted, when the
/// member no longer leads in that term or the lease ran out before the
/// unlink could begin.
async fn delete_leased(
    storage: &dyn fluree_db_core::Storage,
    collector: &Arc<dyn Collector>,
    term: u64,
    lease: &mut Option<crate::collect::Lease>,
    address: &str,
) -> Result<bool> {
    *lease = crate::collect::Lease::renewed(*lease, term, collector.as_ref()).await;
    let Some(held) = *lease else {
        return Ok(false);
    };
    Ok(storage.delete_unlogged(address, held.until).await?)
}
