//! The retention worker, run by the maintenance owner.
//!
//! The indexer's catch-up tick ([`Housekeeping`]) starts it and wakes it; it
//! then also runs on its own, shorter interval, because TTLs come due on
//! quiet ledgers that have nothing to index. Each pass covers the branches
//! registered as active. A branch becomes active when an index build reads
//! commits that carry retention input
//! ([`IndexResult::retention_input_from_t`]): it is registered before the
//! root is published, so a crash cannot lose an activation, and a ledger
//! that never uses retention pays nothing for the check.

// The worker loop and build observer run on native targets only; on wasm32
// the runtime state they drive is unused.
#![cfg_attr(target_arch = "wasm32", allow(dead_code, unused_imports))]

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

#[cfg(not(target_arch = "wasm32"))]
use fluree_db_indexer::{BuildObserver, Housekeeping, IndexResult};
use fluree_db_nameservice::NsRecord;
use tokio::sync::Notify;

use super::engine::{PassReport, ScheduleCache};
use super::registry;
use super::writer::{RetentionBatch, RetentionWriter, SubmitOutcome};
use super::RetentionLimits;
use crate::{Fluree, Result};

/// The retention state one `Fluree` instance keeps in memory.
#[derive(Default)]
pub(crate) struct RetentionRuntime {
    /// Branches known to be registered.
    active: parking_lot::Mutex<HashSet<String>>,
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

impl RetentionRuntime {
    pub(crate) fn mark_active(&self, ledger_id: &str) {
        self.active.lock().insert(ledger_id.to_string());
        self.nudge.notify_one();
    }

    fn active_ledgers(&self) -> Vec<String> {
        self.active.lock().iter().cloned().collect()
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

/// Gives the background indexer's builds the history retention removed,
/// and registers a branch for retention before an index covers the commits
/// carrying its retention input.
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
        let history_suppression = fluree
            .suppressed_history_for_index(record.ledger_id.as_ref())
            .await
            .map_err(|e| format!("retention journal: {e}"))?;
        Ok(fluree_db_indexer::BuildDirectives {
            history_suppression,
        })
    }

    async fn before_publish(
        &self,
        _record: &NsRecord,
        built: &IndexResult,
    ) -> std::result::Result<(), String> {
        if built.retention_input_from_t.is_none() {
            return Ok(());
        }
        let fluree = self
            .fluree
            .upgrade()
            .ok_or("retention registration: the instance is gone")?;
        fluree
            .register_built_retention(built)
            .await
            .map_err(|e| format!("retention registration: {e}"))
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
    /// retention input. Unlike a background pass, it saves what it discovers
    /// at once, so an index build after it applies `f:noHistory` through
    /// the commits it discovered.
    pub async fn run_retention(&self, ledger_id: &str) -> Result<PassReport> {
        self.run_retention_at(ledger_id, now_ms()).await
    }

    /// [`run_retention`](Self::run_retention) as of `now_ms` (epoch ms): due
    /// times are compared against it rather than the clock.
    pub async fn run_retention_at(&self, ledger_id: &str, now_ms: i64) -> Result<PassReport> {
        let ns = self.publisher()?;
        if registry::read_stored(ns, ledger_id).await?.1.is_none() {
            self.probe_and_activate(ledger_id, 0).await?;
        }
        let limits = RetentionLimits {
            checkpoint_ms: 0,
            ..Default::default()
        };
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
        let ledgers: Vec<String> = self.retention.active_ledgers();
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
            let ledgers: Vec<String> = self.retention.active_ledgers();
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
    /// Register `built`'s branch for retention from the earliest commit
    /// carrying retention input the build read. Call it before publishing
    /// the root: once an index covers those commits, no build reads them
    /// again. Nothing when the build read none.
    pub(crate) async fn register_built_retention(
        &self,
        built: &fluree_db_indexer::IndexResult,
    ) -> Result<()> {
        let Some(since) = built.retention_input_from_t else {
            return Ok(());
        };
        let id = built.ledger_id.to_string();
        registry::activate(self.publisher()?, self.registry_objects(), &id, since).await?;
        self.retention.mark_active(&id);
        Ok(())
    }

    /// Register `branch_id` for retention as `source_id` is, before the
    /// branch adopts the source's index: its builds never read the commits
    /// that index covers. Nothing when the source is not registered.
    pub(crate) async fn register_retention_as(
        &self,
        source_id: &str,
        branch_id: &str,
    ) -> Result<()> {
        let ns = self.publisher()?;
        let Some(source) = registry::read(ns, self.registry_objects(), source_id)
            .await?
            .1
        else {
            return Ok(());
        };
        registry::activate(
            ns,
            self.registry_objects(),
            branch_id,
            source.active_since_t,
        )
        .await?;
        self.retention.mark_active(branch_id);
        Ok(())
    }
}
