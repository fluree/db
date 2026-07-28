//! Background tracking worker for R2RML / Iceberg materialization.
//!
//! Watches a set of `(source graph source → target ledger)` jobs and keeps each
//! target ledger fresh by periodically calling
//! [`Fluree::materialize_r2rml_graph_source`](crate::Fluree::materialize_r2rml_graph_source).
//! That call resolves the watermark persisted in the target ledger and refreshes
//! incrementally (append/compaction-only windows) or fully (first run or a
//! non-incremental-safe window), committing nothing on a no-delta poll. The
//! worker therefore needs no state of its own beyond the set of jobs — the
//! watermark lives in the ledger, so a no-delta poll is a single metadata read.
//!
//! Each job carries its **own poll interval** (set per `track` call; defaults to
//! the worker's configured interval). The loop wakes on a fine base granularity
//! and polls only the jobs whose `next_due` has arrived, so jobs on different
//! timers are independent.
//!
//! # Restart
//!
//! The job set is **durable**. `track` writes a
//! [`PersistedMaterializeJob`](crate::PersistedMaterializeJob) into the shared
//! `fluree_materialize_state:main` ledger — the same one holding the watermarks —
//! and [`run`](MaterializeTrackingWorker::run) restores it before the first tick,
//! so a restart resumes tracking without a client re-issuing anything. Combined
//! with the watermarks, recovery re-reads nothing already materialized. `untrack`
//! clears the record, so it does not come back either.
//!
//! Unlike [`crate::bm25_worker`] (event-driven, single-threaded `Rc`/`RefCell`),
//! this worker is **polling** and `Send`: it owns an `Arc<Fluree>` and shares its
//! job set / stop flag / stats behind `Arc<Mutex<…>>`, so it spawns with a plain
//! `tokio::spawn`. Iceberg emits no commit events, so polling is the only option.
//!
//! Only available with the `iceberg` feature.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::time::{self, Duration, MissedTickBehavior};
use tracing::{debug, info, warn};

use crate::Fluree;

/// Configuration for the materialization tracking worker.
#[derive(Debug, Clone)]
pub struct MaterializeWorkerConfig {
    /// Default poll interval for a tracked job when `track` does not specify one.
    pub poll_interval: Duration,
}

impl Default for MaterializeWorkerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(30),
        }
    }
}

/// Statistics for the tracking worker.
#[derive(Debug, Clone, Default)]
pub struct MaterializeWorkerStats {
    /// Number of per-job poll attempts.
    pub polls: u64,
    /// Polls that committed data and/or an advanced watermark.
    pub syncs_committed: u64,
    /// Polls that found no delta (nothing committed).
    pub syncs_noop: u64,
    /// Polls that errored.
    pub syncs_failed: u64,
    /// Currently tracked jobs.
    pub tracked_jobs: usize,
}

/// A tracked materialization job: read `source`, materialize into `target`.
#[derive(Debug, Clone)]
struct TrackedJob {
    source: String,
    target: String,
    /// How often this job is polled.
    interval: Duration,
    /// When this job is next due for a poll.
    next_due: Instant,
}

/// Public view of a tracked job for status endpoints.
#[derive(Debug, Clone)]
pub struct JobInfo {
    pub source: String,
    pub target: String,
    /// This job's poll interval, in seconds.
    pub poll_interval_secs: u64,
}

type JobKey = (String, String);

/// Handle to a running [`MaterializeTrackingWorker`]: register / unregister
/// jobs, read stats, and request stop. Cheap to clone (all shared state is
/// behind `Arc`). Shared between the spawned worker task and request handlers.
#[derive(Clone)]
pub struct MaterializeWorkerHandle {
    tracked: Arc<Mutex<BTreeMap<JobKey, TrackedJob>>>,
    /// Poll interval applied when `track` is called without an explicit one.
    default_interval: Duration,
    stop: Arc<AtomicBool>,
    stats: Arc<Mutex<MaterializeWorkerStats>>,
}

impl MaterializeWorkerHandle {
    /// Start tracking `source → target` (idempotent). `interval` is this job's
    /// own poll cadence; `None` uses the worker's configured default. Returns the
    /// effective interval. The next poll fires one interval from now (the caller
    /// typically runs an immediate first sync itself).
    pub fn track(&self, source: &str, target: &str, interval: Option<Duration>) -> Duration {
        let interval = interval.unwrap_or(self.default_interval);
        let key = (source.to_string(), target.to_string());
        let mut tracked = self.tracked.lock().expect("materialize worker mutex");
        tracked.insert(
            key,
            TrackedJob {
                source: source.to_string(),
                target: target.to_string(),
                interval,
                next_due: Instant::now() + interval,
            },
        );
        let n = tracked.len();
        drop(tracked);
        self.stats.lock().expect("stats mutex").tracked_jobs = n;
        info!(
            source,
            target,
            tracked_jobs = n,
            poll_interval_secs = interval.as_secs(),
            "materialize tracking added"
        );
        interval
    }

    /// Stop tracking `source → target`. Returns whether a job was removed. The
    /// already-materialized data and watermark in the target ledger are left
    /// untouched.
    pub fn untrack(&self, source: &str, target: &str) -> bool {
        let key = (source.to_string(), target.to_string());
        let mut tracked = self.tracked.lock().expect("materialize worker mutex");
        let removed = tracked.remove(&key).is_some();
        let n = tracked.len();
        drop(tracked);
        self.stats.lock().expect("stats mutex").tracked_jobs = n;
        if removed {
            info!(
                source,
                target,
                tracked_jobs = n,
                "materialize tracking removed"
            );
        }
        removed
    }

    /// The currently tracked `(source, target)` pairs.
    pub fn tracked_jobs(&self) -> Vec<JobKey> {
        self.tracked
            .lock()
            .expect("materialize worker mutex")
            .keys()
            .cloned()
            .collect()
    }

    /// The currently tracked jobs with their per-job poll intervals.
    pub fn job_infos(&self) -> Vec<JobInfo> {
        self.tracked
            .lock()
            .expect("materialize worker mutex")
            .values()
            .map(|j| JobInfo {
                source: j.source.clone(),
                target: j.target.clone(),
                poll_interval_secs: j.interval.as_secs(),
            })
            .collect()
    }

    /// Snapshot of worker statistics.
    pub fn stats(&self) -> MaterializeWorkerStats {
        self.stats.lock().expect("stats mutex").clone()
    }

    /// Request the worker to stop after the current cycle.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        info!("materialize tracking worker stop requested");
    }

    /// Whether stop has been requested.
    pub fn is_stopped(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    /// Jobs whose `next_due` has arrived; reschedules each to `now + interval`
    /// before returning it (so a slow poll can't double-schedule the same job).
    fn take_due_jobs(&self) -> Vec<TrackedJob> {
        let now = Instant::now();
        let mut tracked = self.tracked.lock().expect("materialize worker mutex");
        let mut due = Vec::new();
        for job in tracked.values_mut() {
            if job.next_due <= now {
                job.next_due = now + job.interval;
                due.push(job.clone());
            }
        }
        due
    }
}

/// Polling worker that keeps tracked target ledgers fresh.
pub struct MaterializeTrackingWorker {
    fluree: Arc<Fluree>,
    config: MaterializeWorkerConfig,
    handle: MaterializeWorkerHandle,
}

impl MaterializeTrackingWorker {
    /// Create a worker with default config.
    pub fn new(fluree: Arc<Fluree>) -> Self {
        Self::with_config(fluree, MaterializeWorkerConfig::default())
    }

    /// Create a worker with a custom config.
    pub fn with_config(fluree: Arc<Fluree>, config: MaterializeWorkerConfig) -> Self {
        Self {
            fluree,
            handle: MaterializeWorkerHandle {
                tracked: Arc::new(Mutex::new(BTreeMap::new())),
                default_interval: config.poll_interval,
                stop: Arc::new(AtomicBool::new(false)),
                stats: Arc::new(Mutex::new(MaterializeWorkerStats::default())),
            },
            config,
        }
    }

    /// Get a clonable handle to register jobs / read stats / stop the worker.
    pub fn handle(&self) -> MaterializeWorkerHandle {
        self.handle.clone()
    }

    /// Restore the tracked-job set from the shared materialization-state ledger.
    /// Returns how many jobs were restored.
    ///
    /// Called once at the top of [`run`](Self::run) rather than from the server's
    /// construction path, which gets peer-exclusion for free: peers never spawn
    /// a worker, so they never restore (and `/iceberg/track` 400s there anyway).
    ///
    /// A restored job resumes incrementally — the per-(source, target-spec,
    /// table) watermarks live in the same ledger and are read by the materialize
    /// call itself, so recovery re-reads nothing it already materialized. The
    /// first poll of each job fires one interval from now, matching `track`.
    async fn restore_jobs(&self) -> usize {
        let jobs = match self.fluree.tracked_materialize_jobs().await {
            Ok(jobs) => jobs,
            Err(e) => {
                warn!(
                    error = %e,
                    "materialize tracking worker: could not restore jobs; \
                     tracking is empty until a client re-issues track"
                );
                return 0;
            }
        };
        for job in &jobs {
            self.handle.track(
                &job.source,
                &job.target,
                Some(Duration::from_secs(job.poll_interval_secs)),
            );
        }
        if !jobs.is_empty() {
            info!(
                restored = jobs.len(),
                "materialize tracking worker restored persisted jobs"
            );
        }
        jobs.len()
    }

    /// Run the poll loop until [`MaterializeWorkerHandle::stop`] is requested.
    /// Spawn this with `tokio::spawn(worker.run())`.
    pub async fn run(self) {
        self.restore_jobs().await;

        // Wake on a fine base granularity and poll whichever jobs are due; each
        // job's own interval decides how often it actually runs. Cap the tick at
        // 1s so a newly-added short-interval job (and a stop request) are noticed
        // promptly, but never faster than the default interval (small tests).
        let tick = self.config.poll_interval.min(Duration::from_secs(1));
        let mut ticker = time::interval(tick);
        // A slow materialize shouldn't cause a burst of catch-up ticks.
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        info!(
            default_poll_interval_secs = self.config.poll_interval.as_secs(),
            tick_granularity_ms = u64::try_from(tick.as_millis()).unwrap_or(u64::MAX),
            "materialize tracking worker started"
        );

        loop {
            ticker.tick().await;
            if self.handle.is_stopped() {
                info!("materialize tracking worker stopped");
                break;
            }

            for job in self.handle.take_due_jobs() {
                if self.handle.is_stopped() {
                    break;
                }
                self.poll_job(&job).await;
            }
        }
    }

    /// Poll one job: a single idempotent incremental materialize. Commits only
    /// when the source advanced; logs and counts the outcome.
    async fn poll_job(&self, job: &TrackedJob) {
        self.bump(|s| s.polls += 1);
        match self
            .fluree
            .materialize_r2rml_graph_source(&job.source, &job.target, false)
            .await
        {
            Ok(result) if result.committed => {
                self.bump(|s| s.syncs_committed += 1);
                info!(
                    source = %job.source,
                    target = %job.target,
                    from_snapshot_id = ?result.from_snapshot_id,
                    to_snapshot_id = ?result.to_snapshot_id,
                    incremental = result.incremental,
                    rows_read = result.rows_read,
                    subjects_upserted = result.subjects_upserted,
                    subjects_retracted = result.subjects_retracted,
                    "materialize tracking synced"
                );
            }
            Ok(_) => {
                self.bump(|s| s.syncs_noop += 1);
                debug!(source = %job.source, target = %job.target, "materialize tracking: no delta");
            }
            Err(e) => {
                self.bump(|s| s.syncs_failed += 1);
                warn!(source = %job.source, target = %job.target, error = %e, "materialize tracking sync failed");
            }
        }
    }

    fn bump(&self, f: impl FnOnce(&mut MaterializeWorkerStats)) {
        let mut stats = self.handle.stats.lock().expect("stats mutex");
        f(&mut stats);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl MaterializeWorkerHandle {
        /// Build a bare handle (no worker / `Fluree`) for scheduling tests.
        fn for_test(default_interval: Duration) -> Self {
            Self {
                tracked: Arc::new(Mutex::new(BTreeMap::new())),
                default_interval,
                stop: Arc::new(AtomicBool::new(false)),
                stats: Arc::new(Mutex::new(MaterializeWorkerStats::default())),
            }
        }
    }

    #[test]
    fn track_uses_explicit_interval_else_default() {
        let h = MaterializeWorkerHandle::for_test(Duration::from_secs(30));
        assert_eq!(
            h.track("s:main", "t:main", Some(Duration::from_secs(300))),
            Duration::from_secs(300),
            "explicit interval is used and returned"
        );
        assert_eq!(
            h.track("s2:main", "t:main", None),
            Duration::from_secs(30),
            "None falls back to the worker default"
        );
        let infos = h.job_infos();
        assert_eq!(
            infos
                .iter()
                .find(|j| j.source == "s:main")
                .unwrap()
                .poll_interval_secs,
            300
        );
        assert_eq!(
            infos
                .iter()
                .find(|j| j.source == "s2:main")
                .unwrap()
                .poll_interval_secs,
            30
        );
    }

    #[test]
    fn take_due_jobs_respects_next_due_and_reschedules() {
        let h = MaterializeWorkerHandle::for_test(Duration::from_secs(1));
        h.track("s:main", "t:main", Some(Duration::from_secs(3600)));
        assert!(
            h.take_due_jobs().is_empty(),
            "a just-tracked job is not due until now + interval"
        );
        // Rewind next_due into the past to make it due.
        {
            let mut tracked = h.tracked.lock().unwrap();
            tracked
                .get_mut(&("s:main".to_string(), "t:main".to_string()))
                .unwrap()
                .next_due = Instant::now() - Duration::from_secs(1);
        }
        assert_eq!(h.take_due_jobs().len(), 1, "overdue job is returned");
        assert!(
            h.take_due_jobs().is_empty(),
            "taking a due job reschedules it ~1h out"
        );
    }
}
