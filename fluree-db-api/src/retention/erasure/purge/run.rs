//! A purge pass: rebuild the holders' indexes from the replacements,
//! release the older ones, collect the holders' ledgers' garbage, scan for
//! the erased values, delete what the plan and the scan name — the
//! originals last — and check that it is gone.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fluree_db_core::{content_path, ContentId, ContentKind, ContentStore, LedgerId, LedgerName};
use fluree_db_indexer::{plan_garbage, release_garbage_plan, siblings_of, CleanGarbageConfig};
use serde::Serialize;

use super::super::{ErasureJob, ErasureStage, PurgeProgress};
use super::erase::Erased;
use super::release::Released;
use super::{Captured, PurgePlan};
use crate::retention::registry;
use crate::retention::state::{FactKey, Schedule};
use crate::retention::worker::now_ms;
use crate::{ApiError, Fluree, IndexingMode, ReindexOptions, Result};

/// Another process reading this storage may still hold a purged value in
/// memory: this one cannot see into it.
const UNVERIFIED_MEMORY: &str = "the memory of other processes that read this storage, \
     such as query peers: restart them";

/// What a purge pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PurgeReport {
    /// The requests it worked on.
    pub requests: Vec<String>,
    /// Requests it finished.
    pub purged: usize,
    /// Files it deleted.
    pub deleted: usize,
    /// Holders whose indexes it rewrote leaf by leaf.
    pub leafwise: usize,
    /// Holders whose indexes it rebuilt, a value living outside the leaves
    /// and the string dictionary.
    pub rebuilt: usize,
    /// Soft-dropped ledgers whose indexes it released, to be rebuilt on
    /// restore.
    pub released: usize,
    /// Full-text and vector indexes over the holders it rebuilt.
    pub sources: usize,
    /// Waiting for readers of the older indexes to finish before releasing
    /// them.
    pub waiting: bool,
    /// Why it cannot go on, if it cannot.
    pub blocked: Option<String>,
    /// Where its time went.
    pub timings: PurgeTimings,
}

/// Where a purge pass's time went.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PurgeTimings {
    /// Planning, and reading the erased facts.
    pub plan: Duration,
    /// Erasing the holders' indexes, or rebuilding them, and their
    /// full-text and vector indexes.
    pub index: Duration,
    /// Releasing the older indexes and collecting the holders' garbage.
    pub release: Duration,
    /// Both scans.
    pub scan: Duration,
    /// Deleting, and making the deletes durable.
    pub delete: Duration,
}

impl Fluree {
    /// Purge `ledger`'s published erasures. Rewrites every holding branch's
    /// index without the erased events, leaf by leaf (rebuilding it from the
    /// replacements where a value lives outside the leaves and the string
    /// dictionary), releases the older indexes once `release_after` has
    /// passed since, scans for the erased
    /// values, deletes what the plan and the scan name with the originals
    /// last, and checks that they are gone. A pass interrupted anywhere
    /// resumes from where it stopped.
    pub async fn run_purge_pass(
        &self,
        ledger: &str,
        release_after: Duration,
    ) -> Result<PurgeReport> {
        // On a cluster every delete is the leader's, within its lease: none
        // lands once another leader may be writing an index it would remove.
        match self.collector() {
            Some(collector) => {
                let Some(first) = collector.lease().await else {
                    return Err(ApiError::http(409, "a cluster purges on its leader"));
                };
                // Bound to the term it begins in: led again later, its plan
                // and holds may no longer hold.
                let lease = crate::collect::LeaderLease::new(Arc::clone(collector), first);
                fluree_db_core::FileStorage::within_lease(
                    lease,
                    Box::pin(self.purge_pass(ledger, release_after)),
                )
                .await
            }
            None => Box::pin(self.purge_pass(ledger, release_after)).await,
        }
    }

    async fn purge_pass(&self, ledger: &str, release_after: Duration) -> Result<PurgeReport> {
        let runtime = self.erasure_runtime()?;
        // On a cluster the leader purges the shared storage, for jobs every
        // member has installed: none reads or writes back what they erase.
        let clustered = self.in_cluster();
        // The holders stay those the pass planned for: a branch made from
        // one would copy an index not yet erased, and a restore would
        // rebuild into folders the release deletes.
        let _admission = runtime.admission.write().await;
        let _purging = runtime.purging.lock().await;
        let name = LedgerId::parse(ledger)?.ledger_name();
        let jobs = self.purge_jobs(&name).await?;
        let mut report = PurgeReport {
            requests: jobs.iter().map(|(_, j)| j.request_id.clone()).collect(),
            ..Default::default()
        };
        if jobs.is_empty() {
            return Ok(report);
        }
        let progress = |f: fn(&PurgeProgress) -> Option<i64>| {
            jobs.iter()
                .map(|(_, j)| j.purge.as_ref().and_then(f))
                .min()
                .flatten()
        };
        let waiting = |rebuilt_ms: i64| now_ms() - rebuilt_ms < release_after.as_millis() as i64;
        // The shared storage is purged: what is left is its members'.
        if progress(|p| p.storage_ms).is_some() {
            return self.complete_in_cluster(ledger, &jobs, report).await;
        }
        if progress(|p| p.scanned_ms).is_none() && progress(|p| p.rebuilt_ms).is_some_and(waiting) {
            report.waiting = true;
            return Ok(report);
        }
        // What a stopped member's log would write back is back before the
        // plan lists what to delete.
        if clustered {
            self.retire_stopped_logs().await?;
        }
        let clock = Instant::now();
        let plan = self.plan_purge(ledger).await?;
        report.timings.plan += clock.elapsed();
        #[cfg(any(test, feature = "testing"))]
        super::hooks::hold(super::hooks::Hold::Planned, name.as_str()).await;
        let stuck = self.unrebuildable_sources(&plan.rebuilt_sources).await?;
        let blocked = (!stuck.is_empty()).then_some(
            "full-text or vector indexes over a dropped ledger, or of a kind this build \
             cannot rebuild, keep their own copies: drop them first",
        );
        let blocked = blocked.or_else(|| {
            (self.blob_pins().is_none() && !clustered).then_some(
                "this instance does not collect garbage: it does not hold its storage's lock",
            )
        });
        let blocked = match blocked {
            Some(why) => Some(why),
            // Members retire their own logs after the purge, and stopped
            // owners' logs are retired before it deletes.
            None if clustered => None,
            None => (self.foreign_wal_segments().await? > 0).then_some(
                "another process's write-ahead log under this storage may hold erased bytes, \
                 and an open after it stops would replay them",
            ),
        };
        if let Some(why) = blocked {
            return self.block(&jobs, report, why.to_string()).await;
        }

        if progress(|p| p.scanned_ms).is_none() {
            // Soft-dropped holders' indexes, marked for a rebuild on restore.
            if !plan.release.is_empty() {
                match self.release_dropped(&plan.commits).await? {
                    Released::Done { ledgers, deleted } => {
                        report.released += ledgers;
                        report.deleted += deleted;
                    }
                    Released::Changed => {
                        report.waiting = true;
                        return Ok(report);
                    }
                }
            }
            // Retracted holders' index heads, so the sweep releases their
            // indexes: nothing loads a frozen branch.
            for branch in &plan.retracted {
                if self.publisher()?.clear_frozen_index(branch).await? {
                    report.released += 1;
                } else if self.frozen_index_head(branch).await? {
                    let why = format!("{branch} is dropped but not yet frozen");
                    return self.block(&jobs, report, why).await;
                }
            }
            let clock = Instant::now();
            let captured = self.capture_erased(&plan).await?;
            report.timings.plan += clock.elapsed();
            let rebuilt_ms = match progress(|p| p.rebuilt_ms) {
                Some(ms) => ms,
                None => {
                    let clock = Instant::now();
                    for branch in &plan.rebuild {
                        let Some(record) = self.nameservice().lookup(branch).await? else {
                            continue;
                        };
                        match self.erase_from_index(&record, &captured).await? {
                            Erased::Leafwise => report.leafwise += 1,
                            Erased::NeedsRebuild => {
                                self.reindex(branch, ReindexOptions::default()).await?;
                                // A new root at the same `t`, as leaf by leaf.
                                self.announce_refresh(&record.ledger_id).await;
                                report.rebuilt += 1;
                            }
                            Erased::Nothing => {}
                        }
                    }
                    report.sources += self.rebuild_sources(&plan.rebuilt_sources).await?;
                    report.timings.index += clock.elapsed();
                    let ms = now_ms();
                    self.update_purge(&jobs, |p| p.rebuilt_ms = Some(ms))
                        .await?;
                    #[cfg(any(test, feature = "testing"))]
                    super::hooks::hold(super::hooks::Hold::Rebuilt, name.as_str()).await;
                    ms
                }
            };
            if waiting(rebuilt_ms) {
                report.waiting = true;
                return Ok(report);
            }
            let ledgers: BTreeSet<LedgerName> = plan
                .rebuild
                .iter()
                .chain(&plan.retracted)
                .filter_map(|b| LedgerId::parse(b).ok())
                .map(|id| id.ledger_name())
                .collect();
            // On a cluster the envelopes of the requests erased are garbage
            // once no idempotency record from before names them.
            let since_ms = match self.collector() {
                None => rebuilt_ms,
                Some(collector) => match progress(|p| p.envelopes_ms) {
                    Some(ms) => ms.max(rebuilt_ms),
                    None => {
                        let effective = jobs.iter().filter_map(|(_, j)| j.effective_ms()).max();
                        for ledger in &ledgers {
                            if collector
                                .requests_recorded_by(
                                    ledger.as_str(),
                                    effective.unwrap_or(i64::MAX),
                                )
                                .await
                            {
                                report.waiting = true;
                                return Ok(report);
                            }
                        }
                        let ms = now_ms();
                        self.update_purge(&jobs, |p| p.envelopes_ms = Some(ms))
                            .await?;
                        ms.max(rebuilt_ms)
                    }
                },
            };
            let clock = Instant::now();
            for branch in &plan.rebuild {
                self.collapse_index(branch).await?;
            }
            report.deleted += self.release_source_leftovers(&plan).await?;
            for ledger in &ledgers {
                self.sweep_index_storage(ledger.as_str()).await?;
                let orphans = self.plan_index_sweep(ledger.as_str()).await?.orphans;
                if !orphans.is_empty() {
                    let why = format!(
                        "{} index files of {ledger} could not be released",
                        orphans.len()
                    );
                    return self.block(&jobs, report, why).await;
                }
                // Every file nothing references is garbage once the ledger
                // is reconciled: collected, none can hold an erased value.
                if !self.resolve_garbage(ledger.as_str(), since_ms).await? {
                    report.waiting = true;
                    return Ok(report);
                }
            }
            report.timings.release += clock.elapsed();

            let clock = Instant::now();
            let scan = self.scan_purge(&plan, &captured).await?;
            report.timings.scan += clock.elapsed();
            if !scan.indexed.is_empty() || !scan.dictionary.is_empty() {
                let why = format!(
                    "rebuilt indexes still hold erased values: {:?}",
                    scan.indexed
                        .iter()
                        .chain(&scan.dictionary)
                        .collect::<BTreeSet<_>>()
                );
                return self.block(&jobs, report, why).await;
            }
            if !scan.unreleased.is_empty() {
                let why = format!(
                    "{} index files the purge releases are still stored",
                    scan.unreleased.len()
                );
                return self.block(&jobs, report, why).await;
            }
            let clock = Instant::now();
            if clustered {
                self.retire_stopped_logs().await?;
            }
            report.deleted += self
                .rewrite_schedules(&plan, &scan.schedules, &captured)
                .await?;
            let files: Vec<&String> = plan.transactions.iter().chain(&plan.superseded).collect();
            report.deleted += self.delete_files(&files).await?;
            report.timings.delete += clock.elapsed();
            let clock = Instant::now();
            let scan = self.scan_purge(&plan, &captured).await?;
            report.timings.scan += clock.elapsed();
            if !scan.clean() {
                let why = "a scan after deleting still found erased values".to_string();
                return self.block(&jobs, report, why).await;
            }
            // The originals are the only source of the values: the scan is
            // recorded before they go, so a resumed pass need not repeat it.
            let (deleted, in_use, ms) = (report.deleted, scan.in_use, now_ms());
            self.update_purge(&jobs, |p| {
                p.scanned_ms = Some(ms);
                p.deleted += deleted;
                p.in_use = in_use;
            })
            .await?;
        }

        let clock = Instant::now();
        // A stopped member's log could write an original back after it went.
        if clustered {
            self.retire_stopped_logs().await?;
        }
        // A document a replay wrote back since the scan goes too: the scan
        // was taken before the originals, the only other source, went.
        let others: Vec<&String> = plan.transactions.iter().chain(&plan.superseded).collect();
        let mut deleted = self.delete_files(&others).await?;
        // Their folders' histories still read them: a later erasure of the
        // same commit must find them.
        self.erasure_runtime()?
            .index
            .record_replaced_at(&plan.originals)
            .await?;
        let originals: Vec<&String> = plan.originals.iter().collect();
        deleted += self.delete_files(&originals).await?;
        report.deleted += deleted;
        let runtime = self.erasure_runtime()?;
        for address in plan
            .originals
            .iter()
            .chain(&plan.transactions)
            .chain(&plan.superseded)
        {
            if runtime.index.exists_raw(address).await? {
                return Err(ApiError::internal(format!(
                    "{address} is still stored after its purge"
                )));
            }
        }
        // On a cluster other members write here: a staging file is a crashed
        // write's only if untouched since every member installed the erasure.
        let staging_age = clustered.then(|| {
            let effective = jobs
                .iter()
                .filter_map(|(_, j)| j.effective_ms())
                .min()
                .unwrap_or_else(now_ms);
            std::time::Duration::from_millis(u64::try_from(now_ms() - effective).unwrap_or(0))
        });
        report.deleted += self.make_deletes_durable(&plan, staging_age).await?;
        report.timings.delete += clock.elapsed();
        for branch in &plan.rebuild {
            if let Ok(id) = LedgerId::parse(branch) {
                self.announce_refresh(&id).await;
            }
        }
        // What this process decoded from the released indexes. Other
        // processes' memory is beyond it: reported as unverified.
        self.clear_decoded_caches();
        if clustered {
            let ms = now_ms();
            self.update_purge(&jobs, |p| {
                p.storage_ms = Some(ms);
                p.deleted += deleted;
            })
            .await?;
            report.waiting = true;
            return Ok(report);
        }
        self.mark_purged(&jobs, deleted).await?;
        report.purged = jobs.len();
        Ok(report)
    }

    /// Finish `jobs` on a cluster, their shared storage purged: once every
    /// member has cleaned its own copies, retire the logs stopped members
    /// left and delete again whatever a replay of one wrote back, then mark
    /// them purged.
    async fn complete_in_cluster(
        &self,
        ledger: &str,
        jobs: &[(String, ErasureJob)],
        mut report: PurgeReport,
    ) -> Result<PurgeReport> {
        let runtime = self.erasure_runtime()?;
        let activator = runtime
            .activator
            .get()
            .ok_or_else(|| ApiError::internal("a cluster purge needs its activator"))?;
        let mut index = 0;
        for (_, job) in jobs {
            if let Some(activation) = job.cluster.as_ref().and_then(|c| c.activation) {
                index = index.max(activator.record_purged(activation).await?);
            }
        }
        let pending = activator.not_cleaned(index).await;
        if !pending.is_empty() {
            let why = format!("members {pending:?} have not cleaned their own copies yet");
            self.update_purge(jobs, |p| p.blocked = Some(why.clone()))
                .await?;
            report.waiting = true;
            return Ok(report);
        }
        self.retire_stopped_logs().await?;
        let retired_ms = match jobs
            .iter()
            .filter_map(|(_, j)| j.purge.as_ref().and_then(|p| p.retired_ms))
            .min()
        {
            Some(ms) => ms,
            None => {
                let ms = now_ms();
                self.update_purge(jobs, |p| p.retired_ms = Some(ms)).await?;
                ms
            }
        };
        let plan = self.plan_purge(ledger).await?;
        // What a replay wrote back, a request's envelope among it, is
        // garbage: collected before the job completes.
        let ledgers: BTreeSet<LedgerName> = plan
            .rebuild
            .iter()
            .chain(&plan.retracted)
            .filter_map(|b| LedgerId::parse(b).ok())
            .map(|id| id.ledger_name())
            .collect();
        for ledger in &ledgers {
            if !self.resolve_garbage(ledger.as_str(), retired_ms).await? {
                report.waiting = true;
                return Ok(report);
            }
        }
        let mut deleted = 0;
        for address in plan
            .originals
            .iter()
            .chain(&plan.transactions)
            .chain(&plan.superseded)
        {
            if runtime.index.delete_raw(address).await? {
                deleted += 1;
            }
        }
        self.mark_purged(jobs, deleted).await?;
        report.deleted = deleted;
        report.purged = jobs.len();
        Ok(report)
    }

    /// Mark `jobs` purged, `deleted` more files deleted.
    async fn mark_purged(&self, jobs: &[(String, ErasureJob)], deleted: usize) -> Result<()> {
        let ms = now_ms();
        for (branch, job) in jobs {
            self.update_job(branch, &job.request_id, |j| {
                j.stage = ErasureStage::Purged;
                j.retry_at_ms = None;
                let p = j.purge.get_or_insert_with(Default::default);
                p.purged_ms = Some(ms);
                p.deleted += deleted;
                p.blocked = None;
                p.unverified = vec![UNVERIFIED_MEMORY.to_string()];
            })
            .await?;
        }
        Ok(())
    }

    /// Drop what this process decoded from indexes a purge released.
    pub(crate) fn clear_decoded_caches(&self) {
        self.leaflet_cache().clear();
        crate::ontology_imports::global_schema_bundle_cache().clear();
        crate::ontology_imports::global_schema_bundle_flakes_cache().clear();
    }

    /// The published, unpurged jobs of `name`'s branches, with the branch
    /// whose registration holds each.
    pub(super) async fn purge_jobs(&self, name: &LedgerName) -> Result<Vec<(String, ErasureJob)>> {
        let ns = self.publisher()?;
        let clustered = self.in_cluster();
        let mut jobs = Vec::new();
        for record in self.nameservice().branch_records(name.as_str()).await? {
            if record.retracted {
                continue;
            }
            let branch = record.ledger_id.to_string();
            if let Some(registration) = registry::read(ns, self.registry_objects(), &branch)
                .await?
                .1
            {
                jobs.extend(
                    registration
                        .erasures
                        .into_iter()
                        .filter(|j| j.purge_ready(clustered))
                        .map(|j| (branch.clone(), j)),
                );
            }
        }
        Ok(jobs)
    }

    async fn update_purge(
        &self,
        jobs: &[(String, ErasureJob)],
        change: impl Fn(&mut PurgeProgress),
    ) -> Result<()> {
        for (branch, job) in jobs {
            self.update_job(branch, &job.request_id, |j| {
                j.stage = ErasureStage::Purging;
                let p = j.purge.get_or_insert_with(Default::default);
                p.blocked = None;
                change(p);
            })
            .await?;
        }
        Ok(())
    }

    async fn block(
        &self,
        jobs: &[(String, ErasureJob)],
        mut report: PurgeReport,
        why: String,
    ) -> Result<PurgeReport> {
        for (branch, job) in jobs {
            // The background worker tries again in an hour.
            let retry_at = now_ms() + 3_600_000;
            self.update_job(branch, &job.request_id, |j| {
                j.stage = ErasureStage::Purging;
                j.retry_at_ms = Some(retry_at);
                j.purge.get_or_insert_with(Default::default).blocked = Some(why.clone());
            })
            .await?;
        }
        report.blocked = Some(why);
        Ok(report)
    }

    /// Remove `captured`'s facts from the TTL schedules at `addresses`: a
    /// holder's current schedule is rewritten without them, and any other is
    /// deleted, as stale. Returns how many files it deleted.
    async fn rewrite_schedules(
        &self,
        plan: &PurgePlan,
        addresses: &[String],
        captured: &[Captured],
    ) -> Result<usize> {
        let runtime = self.erasure_runtime()?;
        let ns = self.publisher()?;
        let prefix = runtime.index.address_prefix();
        let erased: Vec<FactKey> = captured.iter().map(|c| FactKey::of(&c.flake)).collect();
        let mut deleted = 0;
        for address in addresses {
            let mut current = None;
            for branch in &plan.rebuild {
                let Some(record) = self.nameservice().lookup(branch).await? else {
                    continue;
                };
                let Some(blob) = registry::read(ns, self.registry_objects(), branch)
                    .await?
                    .1
                    .and_then(|r| r.schedule)
                else {
                    continue;
                };
                let Ok(cid) = blob.parse::<ContentId>() else {
                    continue;
                };
                let held = format!(
                    "{prefix}{}",
                    content_path(
                        ContentKind::RetentionState,
                        &record.storage_namespace(),
                        &cid.address_digest()
                    )
                );
                if held == *address {
                    current = Some((branch, record, blob));
                    break;
                }
            }
            if let Some((branch, record, blob)) = current {
                let Some(bytes) = runtime.index.read_raw(address).await? else {
                    continue;
                };
                let mut schedule = Schedule::decode(&bytes).map_err(ApiError::internal)?;
                for key in &erased {
                    schedule.remove(key);
                }
                let encoded = schedule.encode().map_err(ApiError::internal)?;
                let store = self.content_store(&record.storage_namespace());
                let rewritten = store
                    .put(ContentKind::RetentionState, &encoded)
                    .await?
                    .to_string();
                let (pending, next_due_ms) = (schedule.len() as u64, schedule.next_due_ms());
                registry::update(ns, self.registry_objects(), branch, |r| {
                    let r = r?;
                    (r.schedule.as_deref() == Some(blob.as_str())).then(|| registry::Registration {
                        schedule: Some(rewritten.clone()),
                        pending,
                        next_due_ms,
                        ..r.clone()
                    })
                })
                .await?;
            }
            if runtime.index.delete_raw(address).await? {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// Delete `files` past the substitution; one already gone counts as
    /// done. Returns how many it deleted.
    async fn delete_files(&self, files: &[&String]) -> Result<usize> {
        let runtime = self.erasure_runtime()?;
        let mut deleted = 0;
        for address in files {
            if runtime.index.delete_raw(address).await? {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// Make the purge's deletes survive a crash: retire the write-ahead log,
    /// which would otherwise replay the writes they undid and still holds
    /// their bytes, then flush the directories they were in. Staging files
    /// crashed writes left in the originals' ledgers go too: one may hold an
    /// original's bytes.
    async fn make_deletes_durable(
        &self,
        plan: &PurgePlan,
        staging_age: Option<std::time::Duration>,
    ) -> Result<usize> {
        let Some(root) = self.config.index_storage.path.clone() else {
            return Ok(0);
        };
        let prefix = self.erasure_runtime()?.index.address_prefix();
        let keys: Vec<String> = plan
            .originals
            .iter()
            .chain(&plan.transactions)
            .chain(&plan.superseded)
            .filter_map(|a| a.strip_prefix(&prefix).map(str::to_string))
            .collect();
        // Every tree the pass deleted from: each original's ledger folder
        // (`{name}/@{instance}/{branch}/commit/…` up to its root, or a legacy
        // name root), every holder's, live, retracted or dropped, and the
        // folders of the indexes over them.
        let mut trees: BTreeSet<String> = keys
            .iter()
            .filter_map(|k| k.rsplit_once("/commit/").map(|(folder, _)| folder))
            .filter_map(|folder| folder.rsplit_once('/').map(|(root, _)| root.to_string()))
            .collect();
        for branch in plan.rebuild.iter().chain(&plan.retracted) {
            if let Some(record) = self.nameservice().lookup(branch).await? {
                trees.insert(record.storage_namespace().root().as_str().to_string());
            }
        }
        let commits: Vec<&str> = plan.commits.iter().map(String::as_str).collect();
        for (record, _) in self.dropped_holders(&commits).await? {
            trees.insert(record.storage_namespace().root().as_str().to_string());
        }
        for source in plan.sources.iter().chain(&plan.dropped_sources) {
            if let Ok(namespace) = fluree_db_core::StorageNamespace::parse_graph_source(source) {
                trees.insert(namespace.branch_prefix().to_string());
            }
        }
        // Scratch outside the storage: what crashed imports and rebuilds of
        // the holders, and crashed proving steps, left.
        let data_dir = match &self.indexing_mode {
            IndexingMode::Background(handle) => handle.data_dir().map(std::path::Path::to_path_buf),
            IndexingMode::Disabled => None,
        };
        let holders = plan.rebuild.clone();
        let prover = self
            .erasure_runtime()?
            .config
            .as_ref()
            .map(|c| std::sync::Arc::clone(&c.prover));
        let swept = tokio::task::spawn_blocking(move || {
            let mut swept = 0;
            let scratch = |e: std::io::Error| fluree_db_core::Error::io(format!("scratch: {e}"));
            for tree in &trees {
                let tree = std::path::Path::new(root.as_ref()).join(tree);
                swept += match staging_age {
                    None => fluree_db_core::FileStorage::sweep_foreign_staging(tree),
                    Some(age) => fluree_db_core::FileStorage::sweep_staging_older_than(tree, age),
                }
                .map_err(scratch)?;
            }
            for holder in &holders {
                swept += crate::import::clear_import_residue(holder).map_err(scratch)?;
                // The default spill directory, and the indexer's own.
                swept += fluree_db_indexer::clear_rebuild_residue(None, holder).map_err(scratch)?;
                if let Some(dir) = &data_dir {
                    swept += fluree_db_indexer::clear_rebuild_residue(Some(dir), holder)
                        .map_err(scratch)?;
                }
            }
            if let Some(prover) = &prover {
                swept += prover.clear_residue().map_err(scratch)?;
            }
            fluree_db_core::FileStorage::checkpoint_wal(root.as_ref())?;
            fluree_db_core::FileStorage::sync_parent_dirs(root.as_ref(), &keys)?;
            let trees: Vec<String> = trees.into_iter().collect();
            fluree_db_core::FileStorage::sync_dirs_under(root.as_ref(), &trees)?;
            Ok::<_, fluree_db_core::Error>(swept)
        })
        .await
        .map_err(|e| ApiError::internal(format!("purge flush: {e}")))??;
        Ok(swept)
    }

    /// Apply and retire the logs stopped members left under this storage, so
    /// none writes back what the purge deletes.
    async fn retire_stopped_logs(&self) -> Result<()> {
        let Some(root) = self.config.index_storage.path.clone() else {
            return Ok(());
        };
        tokio::task::spawn_blocking(move || {
            fluree_db_core::FileStorage::retire_stopped_logs(root.as_ref())
        })
        .await
        .map_err(|e| ApiError::internal(format!("write-ahead log retirement: {e}")))??;
        Ok(())
    }

    /// Segments other processes' write-ahead logs keep under this storage.
    async fn foreign_wal_segments(&self) -> Result<usize> {
        let Some(root) = self.config.index_storage.path.clone() else {
            return Ok(0);
        };
        Ok(tokio::task::spawn_blocking(move || {
            fluree_db_core::FileStorage::foreign_wal_segments(root.as_ref())
        })
        .await
        .map_err(|e| ApiError::internal(format!("write-ahead log check: {e}")))??)
    }

    /// Release every index of `branch` older than its current one, with what
    /// only they reference; dictionaries its siblings still use are left to
    /// the sweep.
    pub(crate) async fn collapse_index(&self, branch: &str) -> Result<()> {
        let id = LedgerId::parse(branch)?;
        let Some(record) = self.nameservice().lookup(&id).await? else {
            return Ok(());
        };
        let Some(root) = record.index_head_id.as_ref() else {
            return Ok(());
        };
        // This instance's indexer, and on a cluster's leader its leader
        // role's: either's collection or builds would race the release.
        let mut indexers = Vec::new();
        if let IndexingMode::Background(handle) = &self.indexing_mode {
            indexers.push((handle.clone(), None));
        }
        if let Some((handle, alive)) = self.leader_indexer() {
            indexers.push((handle, Some(alive)));
        }
        let mut guards = Vec::new();
        for (handle, _) in &indexers {
            guards.push(handle.hold_gc(&id.ledger_name()).await);
        }
        let store = self.content_store(&record.storage_namespace());
        let config = CleanGarbageConfig {
            max_old_indexes: Some(0),
            min_time_garbage_mins: Some(0),
            ..Default::default()
        };
        let plan = plan_garbage(store.as_ref(), root, &config).await?;
        if plan.is_empty() {
            return Ok(());
        }
        let mut windows = Vec::new();
        for ((handle, alive), guard) in indexers.iter().zip(&guards) {
            let window = match alive {
                None => handle.open_release_window(guard).await,
                Some(alive) => {
                    match crate::admin::while_alive(alive, handle.open_release_window(guard)).await
                    {
                        Some(window) => window,
                        // Its task ended: it builds nothing more.
                        None => continue,
                    }
                }
            };
            match window {
                Some(window) => windows.push(window),
                None => {
                    return Err(ApiError::internal(format!(
                        "{branch} is held for maintenance; its older indexes stay for now"
                    )))
                }
            }
        }
        let records = self.nameservice().all_records().await?;
        let mut siblings: Vec<LedgerId> = siblings_of(&records, &id)
            .into_iter()
            .map(|b| b.ledger_id)
            .collect();
        for (handle, _) in &indexers {
            siblings.extend(handle.built_branches(&id.ledger_name()));
        }
        let reader = self.nameservice_mode.as_arc_reader();
        release_garbage_plan(plan, self.backend(), reader.as_ref(), &id, Some(&siblings)).await?;
        Ok(())
    }
}
