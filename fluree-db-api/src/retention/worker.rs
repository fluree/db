//! The retention worker, run by the maintenance owner.
//!
//! The indexer's catch-up tick ([`Housekeeping`]) starts it and wakes it; it
//! then also runs on its own, shorter interval, because TTLs come due on
//! quiet ledgers that have nothing to index. Each pass covers the branches
//! registered as active. A branch becomes active when the background indexer
//! is about to build commits that carry retention input
//! ([`BuildObserver`]): that check happens before the index can cover those
//! commits, so a crash cannot lose an activation.

// The worker loop and build observer run on native targets only; on wasm32
// the runtime state they drive is unused.
#![cfg_attr(target_arch = "wasm32", allow(dead_code, unused_imports))]

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

#[cfg(not(target_arch = "wasm32"))]
use fluree_db_indexer::{BuildObserver, Housekeeping};
use fluree_db_nameservice::NsRecord;
use tokio::sync::Notify;

use super::engine::{probe_activation, PassReport, ScheduleCache};
use super::registry;
use super::writer::{RetentionBatch, RetentionWriter, SubmitOutcome};
use super::RetentionLimits;
use crate::{Fluree, Result};

/// The retention state one `Fluree` instance keeps in memory.
#[derive(Default)]
pub(crate) struct RetentionRuntime {
    /// Branches known to be registered.
    active: parking_lot::Mutex<HashSet<String>>,
    /// Unregistered branches, with the incarnation whose chain was probed
    /// and the commit `t` it was probed through.
    probed: parking_lot::Mutex<HashMap<String, (Incarnation, i64)>>,
    /// Wakes the worker.
    pub(crate) nudge: Arc<Notify>,
    /// A worker runs in this process.
    running: AtomicBool,
    /// When the worker last finished a sweep (epoch ms).
    last_sweep_ms: AtomicI64,
    /// Schedules held between passes.
    pub(crate) cache: ScheduleCache,
    /// Removal journals held between reads.
    pub(crate) suppressions: super::suppression::SuppressionCache,
    /// Each branch's instance, head and journal, as this instance knows them.
    pub(crate) watch: Arc<super::watch::BranchWatch>,
    /// Branches whose history backfill (a rebuild) is running.
    backfills: parking_lot::Mutex<HashSet<String>>,
    /// Set on a cluster node.
    pub(crate) replicas: std::sync::OnceLock<Arc<dyn super::replicas::ReplicaSync>>,
    /// Registration chunks read.
    pub(crate) chunks: super::registry::ChunkCache,
    /// Registration chunks a sweep found no registration naming, since when.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) unnamed_chunks: parking_lot::Mutex<HashMap<String, std::time::Instant>>,
    /// When registration chunks were last swept (epoch ms).
    #[cfg(not(target_arch = "wasm32"))]
    chunks_swept_ms: AtomicI64,
    /// See [`super::registry::Objects`].
    pub(crate) registry_writes: tokio::sync::RwLock<()>,
    /// Where a test holds a sweep of registration chunks, before deleting.
    #[cfg(test)]
    #[allow(clippy::type_complexity)]
    pub(crate) sweep_hold:
        parking_lot::Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
    /// Set on a file-backed instance.
    #[cfg(feature = "native")]
    pub(crate) erasure: std::sync::OnceLock<Arc<super::erasure::ErasureRuntime>>,
}

/// Which ledger a record under a name is: one created again under a
/// dropped one's name has another fence and storage root.
type Incarnation = (Option<fluree_db_nameservice::Fence>, Option<u128>);

fn incarnation(record: &NsRecord) -> Incarnation {
    (
        record.fence,
        record
            .storage_root
            .as_ref()
            .and_then(fluree_db_core::StorageRoot::instance_bits),
    )
}

impl RetentionRuntime {
    pub(crate) fn mark_active(&self, ledger_id: &str) {
        self.active.lock().insert(ledger_id.to_string());
        self.probed.lock().remove(ledger_id);
        self.nudge.notify_one();
    }

    pub(crate) fn running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
}

/// Settings for the retention worker.
#[derive(Clone, Debug)]
pub struct RetentionOptions {
    /// How often the worker runs a pass over active branches, besides the
    /// catch-up ticks that wake it.
    pub interval: Duration,
    /// How often it re-reads every branch's registration, to pick up ones
    /// another process registered.
    pub rescan_interval: Duration,
    /// Bounds on one pass.
    pub limits: RetentionLimits,
    /// How long a purge leaves the indexes it rebuilt over before releasing
    /// them, for readers to finish: the index collector's minimum age.
    #[cfg(feature = "native")]
    pub purge_grace: Duration,
}

impl Default for RetentionOptions {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(15),
            rescan_interval: Duration::from_secs(600),
            limits: RetentionLimits::default(),
            #[cfg(feature = "native")]
            purge_grace: Duration::from_secs(
                60 * u64::from(fluree_db_indexer::IndexerConfig::default().gc_min_time_mins),
            ),
        }
    }
}

pub(crate) fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[cfg(not(target_arch = "wasm32"))]
struct Worker {
    fluree: Weak<Fluree>,
    writer: Arc<dyn RetentionWriter>,
    options: RetentionOptions,
}

#[cfg(not(target_arch = "wasm32"))]
/// The [`Housekeeping`] that starts and wakes the retention worker. Dropping
/// it (the owner losing catch-up, a Raft leader stepping down) stops the
/// worker after its current pass.
struct RetentionHousekeeping {
    worker: Arc<Worker>,
    started: AtomicBool,
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait::async_trait]
impl Housekeeping for RetentionHousekeeping {
    async fn tick(&self) {
        let Some(fluree) = self.worker.fluree.upgrade() else {
            return;
        };
        if !self.started.swap(true, Ordering::AcqRel) {
            let nudge = Arc::clone(&fluree.retention.nudge);
            tokio::spawn(drive(Arc::downgrade(&self.worker), nudge));
        }
        fluree.retention.nudge.notify_one();
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn drive(worker: Weak<Worker>, nudge: Arc<Notify>) {
    let mut last_rescan: Option<Instant> = None;
    loop {
        let interval = {
            let Some(worker) = worker.upgrade() else {
                break;
            };
            let Some(fluree) = worker.fluree.upgrade() else {
                break;
            };
            fluree.retention.running.store(true, Ordering::Relaxed);
            if last_rescan.is_none_or(|t| t.elapsed() >= worker.options.rescan_interval) {
                if let Err(e) = fluree.rescan_retention_registrations().await {
                    tracing::warn!(error = %e, "retention: cannot list registrations");
                }
                last_rescan = Some(Instant::now());
            }
            let busy = fluree.retention_sweep(&worker).await;
            if busy {
                Duration::from_millis(50)
            } else {
                worker.options.interval
            }
        };
        tokio::select! {
            () = tokio::time::sleep(interval) => {}
            () = nudge.notified() => {}
        }
    }
    if let Some(fluree) = worker.upgrade().and_then(|w| w.fluree.upgrade()) {
        fluree.retention.running.store(false, Ordering::Relaxed);
    }
}

/// Registers branches as the background indexer reaches commits that carry
/// retention input.
#[cfg(not(target_arch = "wasm32"))]
struct RetentionBuildObserver {
    fluree: Weak<Fluree>,
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait::async_trait]
impl BuildObserver for RetentionBuildObserver {
    async fn before_build(
        &self,
        record: &NsRecord,
    ) -> std::result::Result<fluree_db_indexer::BuildDirectives, String> {
        let Some(fluree) = self.fluree.upgrade() else {
            return Ok(Default::default());
        };
        fluree
            .observe_build_for_retention(record)
            .await
            .map_err(|e| format!("retention activation check: {e}"))?;
        let history_suppression = fluree
            .suppressed_history_for_index(record.ledger_id.as_ref())
            .await
            .map_err(|e| format!("retention journal: {e}"))?;
        Ok(fluree_db_indexer::BuildDirectives {
            history_suppression,
        })
    }
}

/// Commits through a borrowed `Fluree`, for explicit passes.
struct BorrowedWriter<'a>(&'a Fluree);

#[async_trait::async_trait]
impl RetentionWriter for BorrowedWriter<'_> {
    async fn submit(&self, ledger_id: &str, batch: RetentionBatch) -> Result<SubmitOutcome> {
        super::writer::outcome_of(self.0.commit_retention_batch(ledger_id, batch).await)
    }
}

impl Fluree {
    #[cfg(not(target_arch = "wasm32"))]
    /// Rebuild `ledger_id`'s index once its journal has history rules the
    /// index was not yet rebuilt for: incremental builds drop suppressed
    /// history only from the leaves they touch, so existing history goes in
    /// one rebuild. Runs in the background; a failure is retried on a later
    /// pass.
    pub(crate) fn backfill_history_if_needed(
        &self,
        ledger_id: &str,
        registration: &registry::Registration,
    ) {
        // Closed rules count too: one enabled and disabled between passes
        // still covers history the index holds.
        let journal = &registration.journal;
        if journal.history_rules.is_empty() || registration.history_backfilled >= journal.generation
        {
            return;
        }
        if !self
            .retention
            .backfills
            .lock()
            .insert(ledger_id.to_string())
        {
            return;
        }
        let fluree = self.clone();
        let ledger_id = ledger_id.to_string();
        let generation = journal.generation;
        tokio::spawn(async move {
            let outcome = fluree
                .reindex(&ledger_id, crate::ReindexOptions::default())
                .await;
            match outcome {
                Ok(_) => {
                    if let Ok(ns) = fluree.publisher() {
                        let _ = registry::update(
                            ns,
                            fluree.registry_objects(),
                            &ledger_id,
                            |current| {
                                let current = current?;
                                (current.history_backfilled < generation).then(|| {
                                    registry::Registration {
                                        history_backfilled: generation,
                                        ..current.clone()
                                    }
                                })
                            },
                        )
                        .await;
                    }
                    tracing::info!(
                        ledger_id,
                        generation,
                        "retention: history backfill rebuilt the index"
                    );
                }
                Err(e) => {
                    tracing::warn!(ledger_id, error = %e, "retention: history backfill failed; will retry");
                }
            }
            fluree.retention.backfills.lock().remove(&ledger_id);
        });
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Housekeeping that runs the retention worker, committing through
    /// `writer` (this instance directly when `None`). Install it on the
    /// worker that owns catch-up, composed with any other housekeeping (see
    /// [`fluree_db_indexer::Housekeepings`]), together with
    /// [`retention_build_observer`](Self::retention_build_observer).
    pub fn retention_housekeeping(
        self: &Arc<Self>,
        writer: Option<Arc<dyn RetentionWriter>>,
        options: RetentionOptions,
    ) -> Arc<dyn Housekeeping> {
        let writer = writer.unwrap_or_else(|| {
            Arc::new(super::writer::DirectRetentionWriter {
                fluree: Arc::downgrade(self),
            })
        });
        Arc::new(RetentionHousekeeping {
            worker: Arc::new(Worker {
                fluree: Arc::downgrade(self),
                writer,
                options,
            }),
            started: AtomicBool::new(false),
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// The build observer that registers branches for retention before the
    /// background indexer covers commits carrying retention input.
    pub fn retention_build_observer(self: &Arc<Self>) -> Arc<dyn BuildObserver> {
        Arc::new(RetentionBuildObserver {
            fluree: Arc::downgrade(self),
        })
    }

    /// Run one retention pass over `ledger_id` now, committing directly. A
    /// branch not yet registered is activated first when its commits carry
    /// retention input.
    pub async fn run_retention(&self, ledger_id: &str) -> Result<PassReport> {
        self.run_retention_at(ledger_id, now_ms()).await
    }

    /// [`run_retention`](Self::run_retention) as of `now_ms` (epoch ms): due
    /// times are compared against it rather than the clock.
    pub async fn run_retention_at(&self, ledger_id: &str, now_ms: i64) -> Result<PassReport> {
        let ns = self.publisher()?;
        if registry::read(ns, self.registry_objects(), ledger_id)
            .await?
            .1
            .is_none()
        {
            self.probe_and_activate(ledger_id, 0).await?;
        }
        let limits = RetentionLimits::default();
        self.retention_pass(
            &BorrowedWriter(self),
            &self.retention.cache,
            ledger_id,
            now_ms,
            &limits,
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// One pass over every active branch. Returns whether there is more
    /// work right away (a batch committed or was recomputed).
    async fn retention_sweep(&self, worker: &Worker) -> bool {
        let ledgers: Vec<String> = self.retention.active.lock().iter().cloned().collect();
        let mut busy = false;
        for ledger_id in ledgers {
            match self
                .retention_pass(
                    worker.writer.as_ref(),
                    &self.retention.cache,
                    &ledger_id,
                    now_ms(),
                    &worker.options.limits,
                )
                .await
            {
                Ok(report) if report.inactive => {
                    self.retention.active.lock().remove(&ledger_id);
                }
                Ok(report) => {
                    busy |= report.committed_t.is_some()
                        || report.head_moved
                        || report.discovered_commits >= worker.options.limits.max_commits_per_pass;
                }
                Err(e) if e.is_not_found() => {
                    self.retention.active.lock().remove(&ledger_id);
                }
                Err(e) => tracing::warn!(ledger_id, error = %e, "retention pass failed"),
            }
        }
        #[cfg(feature = "native")]
        {
            let ledgers: Vec<String> = self.retention.active.lock().iter().cloned().collect();
            self.erasure_sweep(&worker.fluree, &ledgers, worker.options.purge_grace)
                .await;
        }
        // Each chunk a write superseded is unnamed for at least the grace
        // before a sweep deletes it.
        let swept = self.retention.chunks_swept_ms.load(Ordering::Relaxed);
        let every = registry::CHUNK_GRACE.as_millis() as i64 / 2;
        if now_ms() - swept >= every {
            self.retention
                .chunks_swept_ms
                .store(now_ms(), Ordering::Relaxed);
            if let Err(e) = self.sweep_registration_chunks(registry::CHUNK_GRACE).await {
                tracing::warn!(error = %e, "retention: registration chunks not swept");
            }
        }
        self.retention
            .last_sweep_ms
            .store(now_ms(), Ordering::Relaxed);
        busy
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Re-read every branch's registration, and register a branch whose
    /// source branch is registered.
    async fn rescan_retention_registrations(&self) -> Result<()> {
        let ns = self.publisher()?;
        let records = self.nameservice().all_records().await?;
        let mut registered: HashMap<String, i64> = HashMap::new();
        for record in records.iter().filter(|r| !r.retracted) {
            let id = record.ledger_id.to_string();
            if let Some(r) = registry::read(ns, self.registry_objects(), &id).await?.1 {
                registered.insert(id, r.active_since_t);
            }
        }
        for record in records.iter().filter(|r| !r.retracted) {
            let id = record.ledger_id.to_string();
            if registered.contains_key(&id) {
                self.retention.mark_active(&id);
                continue;
            }
            let Some(source) = &record.source_branch else {
                continue;
            };
            let source_id = format!("{}:{source}", record.name);
            if let Some(since) = registered.get(&source_id) {
                registry::activate(ns, self.registry_objects(), &id, *since).await?;
                self.retention.mark_active(&id);
            }
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn observe_build_for_retention(&self, record: &NsRecord) -> Result<()> {
        let id = record.ledger_id.to_string();
        if self.retention.active.lock().contains(&id) {
            return Ok(());
        }
        let ns = self.publisher()?;
        if registry::read(ns, self.registry_objects(), &id)
            .await?
            .1
            .is_some()
        {
            self.retention.mark_active(&id);
            return Ok(());
        }
        let Some(head) = record.commit_head_id.clone() else {
            return Ok(());
        };
        let probed = match self.retention.probed.lock().get(&id) {
            Some((probed, t)) if *probed == incarnation(record) => *t,
            _ => 0,
        };
        let from = record.index_t.max(probed);
        if record.commit_t <= from {
            return Ok(());
        }
        if let Some(source) = &record.source_branch {
            let source_id = format!("{}:{source}", record.name);
            if let Some(r) = registry::read(ns, self.registry_objects(), &source_id)
                .await?
                .1
            {
                registry::activate(ns, self.registry_objects(), &id, r.active_since_t).await?;
                self.retention.mark_active(&id);
                return Ok(());
            }
        }
        let store = self
            .content_store_for_record_or_id(Some(record), &record.storage_namespace())
            .await?;
        match probe_activation(store.as_ref(), &head, from).await? {
            Some(since) => {
                registry::activate(ns, self.registry_objects(), &id, since).await?;
                self.retention.mark_active(&id);
            }
            None => {
                self.retention
                    .probed
                    .lock()
                    .insert(id, (incarnation(record), record.commit_t));
            }
        }
        Ok(())
    }
}
