//! A purge pass: rebuild the holders' indexes from the replacements,
//! release the older ones, collect the holders' ledgers' garbage, scan for
//! the erased values, delete what the plan and the scan name — the
//! originals last — and check that it is gone.

use std::collections::BTreeSet;
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
        let runtime = self.erasure_runtime()?;
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
        if progress(|p| p.scanned_ms).is_none() && progress(|p| p.rebuilt_ms).is_some_and(waiting) {
            report.waiting = true;
            return Ok(report);
        }
        let clock = Instant::now();
        let plan = self.plan_purge(ledger).await?;
        report.timings.plan += clock.elapsed();
        let stuck = self.unrebuildable_sources(&plan.rebuilt_sources).await?;
        let blocked = (!stuck.is_empty()).then_some(
            "full-text or vector indexes over a dropped ledger, or of a kind this build \
             cannot rebuild, keep their own copies: drop them first",
        );
        let blocked = blocked.or_else(|| {
            self.blob_pins().is_none().then_some(
                "this instance does not collect garbage: it does not hold its storage's lock",
            )
        });
        let blocked = match blocked {
            Some(why) => Some(why),
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
                    ms
                }
            };
            if waiting(rebuilt_ms) {
                report.waiting = true;
                return Ok(report);
            }
            let clock = Instant::now();
            for branch in &plan.rebuild {
                self.collapse_index(branch).await?;
            }
            report.deleted += self.release_source_leftovers(&plan).await?;
            let ledgers: BTreeSet<LedgerName> = plan
                .rebuild
                .iter()
                .chain(&plan.retracted)
                .filter_map(|b| LedgerId::parse(b).ok())
                .map(|id| id.ledger_name())
                .collect();
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
                if !self.resolve_garbage(ledger.as_str(), rebuilt_ms).await? {
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
        // Their folders' histories still read them: a later erasure of the
        // same commit must find them.
        self.erasure_runtime()?
            .index
            .record_replaced_at(&plan.originals)
            .await?;
        let originals: Vec<&String> = plan.originals.iter().collect();
        let deleted = self.delete_files(&originals).await?;
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
        report.deleted += self.make_deletes_durable(&plan).await?;
        report.timings.delete += clock.elapsed();
        for branch in &plan.rebuild {
            if let Ok(id) = LedgerId::parse(branch) {
                self.announce_refresh(&id).await;
            }
        }
        // What this process decoded from the released indexes. Other
        // processes' memory is beyond it: reported as unverified.
        self.leaflet_cache().clear();
        crate::ontology_imports::global_schema_bundle_cache().clear();
        crate::ontology_imports::global_schema_bundle_flakes_cache().clear();
        let ms = now_ms();
        for (branch, job) in &jobs {
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
        report.purged = jobs.len();
        Ok(report)
    }

    /// The published, unpurged jobs of `name`'s branches, with the branch
    /// whose registration holds each.
    pub(super) async fn purge_jobs(&self, name: &LedgerName) -> Result<Vec<(String, ErasureJob)>> {
        let ns = self.publisher()?;
        let mut jobs = Vec::new();
        for record in self.nameservice().list_branches(name.as_str()).await? {
            if record.retracted {
                continue;
            }
            let branch = record.ledger_id.to_string();
            if let Some(registration) = registry::read(ns, &branch).await?.1 {
                jobs.extend(
                    registration
                        .erasures
                        .into_iter()
                        .filter(ErasureJob::purge_pending)
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
                let Some(blob) = registry::read(ns, branch).await?.1.and_then(|r| r.schedule)
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
                registry::update(ns, branch, |r| {
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
    async fn make_deletes_durable(&self, plan: &PurgePlan) -> Result<usize> {
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
                swept += fluree_db_core::FileStorage::sweep_foreign_staging(
                    std::path::Path::new(root.as_ref()).join(tree),
                )
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
        let handle = match &self.indexing_mode {
            IndexingMode::Background(handle) => Some(handle),
            IndexingMode::Disabled => None,
        };
        let guard = match handle {
            Some(handle) => Some(handle.hold_gc(&id.ledger_name()).await),
            None => None,
        };
        let store = self.content_store(&record.storage_namespace());
        let config = CleanGarbageConfig {
            max_old_indexes: Some(0),
            min_time_garbage_mins: Some(0),
            ..Default::default()
        };
        let Some(plan) = plan_garbage(store.as_ref(), root, &config).await? else {
            return Ok(());
        };
        let _window = match (handle, &guard) {
            (Some(handle), Some(guard)) => match handle.open_release_window(guard).await {
                Some(window) => Some(window),
                None => {
                    return Err(ApiError::internal(format!(
                        "{branch} is held for maintenance; its older indexes stay for now"
                    )))
                }
            },
            _ => None,
        };
        let records = self.nameservice().all_records().await?;
        let mut siblings: Vec<LedgerId> = siblings_of(&records, &id)
            .into_iter()
            .map(|b| b.ledger_id)
            .collect();
        if let Some(handle) = handle {
            siblings.extend(handle.built_branches(&id.ledger_name()));
        }
        let reader = self.nameservice_mode.as_arc_reader();
        release_garbage_plan(plan, self.backend(), reader.as_ref(), &id, Some(&siblings)).await?;
        Ok(())
    }
}
