//! Lifecycle housekeeping, run on the background indexer's catch-up tick.
//!
//! Drops, restores and purges are resumable: a crash part way leaves state
//! that running the operation again finishes. Nothing runs it again unless
//! someone asks, so this does, once the state has sat unchanged from one tick
//! to the next; a live operation moves it on sooner. Each resume finishes only
//! the operation it saw, so one that completed meanwhile, and anything created
//! under its name since, are left alone.
//!
//! A create is rolled back instead, and only once its claim on the name has
//! not moved for [`ABANDONED_CREATE_AFTER`]: an import holds its create open
//! for as long as it runs, renewing the claim every minute. The rollback frees
//! the name and deletes whatever the create wrote. A branch create that has
//! not confirmed its listing for as long is rolled back the same way.
//!
//! With an interval set, it also runs the orphan sweep. Where the instance
//! collects, it collects commit and transaction garbage and reconciles the
//! ledgers due, in a task of its own, within a time budget per pass. And a
//! ledger whose commits and index have not moved since the last tick has its
//! pooled delta folded into its base, once per index root: otherwise the
//! delta would wait for the ledger's next write.

use crate::admin::{DropMode, DropStatus, PoolCompaction};
use crate::Fluree;
use fluree_db_core::{ContentId, InstanceId, LedgerId, LedgerName};
use fluree_db_indexer::Housekeeping;
use fluree_db_nameservice::{lifecycle, BindingState, DroppedState, Fence, NameServiceError};
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// An operation that has not finished, as one tick sees it.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum Unfinished {
    Create(String),
    Drop(String),
    Restore(InstanceId),
    Purge(InstanceId),
    BranchDrop {
        name: String,
        branch: String,
        fence: Fence,
    },
    BranchCreate {
        name: String,
        branch: String,
        fence: Fence,
    },
}

/// How long a create's claim on its name must stay unchanged before the
/// create is rolled back. Creates that write data before activating renew
/// the claim every minute.
pub const ABANDONED_CREATE_AFTER: Duration = Duration::from_secs(10 * 60);

/// Settings for [`Fluree::lifecycle_housekeeping_with`].
#[derive(Clone, Debug)]
pub struct HousekeepingOptions {
    /// Run the orphan sweep at most this often, the first time one interval
    /// after housekeeping starts; `None` never runs it.
    pub orphan_sweep_interval: Option<Duration>,
    /// Roll back a create whose claim on its name has stayed unchanged this
    /// long, and a branch create that has left its listing unconfirmed this
    /// long, timed by this process.
    pub abandoned_create_after: Duration,
    /// Collect commit and transaction garbage at most this often:
    /// [`Fluree::collect_garbage`].
    pub garbage_interval: Duration,
    /// How long a garbage record stands before its blob may be deleted.
    pub garbage_grace: Duration,
    /// Reconcile each ledger at most this often in the background:
    /// [`Fluree::reconcile_garbage`]. `None`, the default, never does: a
    /// reconciliation lists the ledger's files and reads its references
    /// whole, whatever `reconcile_budget` allows. Garbage already recorded
    /// is collected either way, and an erasure's purge reconciles what it
    /// needs.
    pub reconcile_interval: Option<Duration>,
    /// How long one garbage pass may spend collecting, and then
    /// reconciling, half of the latter pausing between steps.
    pub reconcile_budget: Duration,
    /// Fold the pooled delta of a ledger idle since the last tick into its
    /// base.
    pub compact_idle_pool_deltas: bool,
}

impl Default for HousekeepingOptions {
    fn default() -> Self {
        Self {
            orphan_sweep_interval: None,
            abandoned_create_after: ABANDONED_CREATE_AFTER,
            garbage_interval: Duration::from_secs(5 * 60),
            // The index collector's minimum age.
            garbage_grace: Duration::from_secs(
                60 * u64::from(fluree_db_indexer::IndexerConfig::default().gc_min_time_mins),
            ),
            reconcile_interval: None,
            reconcile_budget: Duration::from_secs(30),
            compact_idle_pool_deltas: true,
        }
    }
}

/// The version of the item that moves when an operation does, and when this
/// process first saw it at that version.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Seen {
    version: u64,
    since: Instant,
}

type Observed = HashMap<Unfinished, Seen>;

/// A ledger's position at the last tick, and the index root whose pooled
/// delta was last looked at.
#[derive(Clone, Debug)]
struct IndexSeen {
    commit_t: i64,
    head: ContentId,
    checked: Option<ContentId>,
}

struct LifecycleHousekeeping {
    fluree: Weak<Fluree>,
    options: HousekeepingOptions,
    previous: parking_lot::Mutex<Observed>,
    last_sweep: parking_lot::Mutex<Instant>,
    #[cfg_attr(not(feature = "native"), allow(dead_code))]
    last_garbage: parking_lot::Mutex<Instant>,
    #[cfg_attr(not(feature = "native"), allow(dead_code))]
    collecting: Arc<std::sync::atomic::AtomicBool>,
    indexes: parking_lot::Mutex<HashMap<LedgerId, IndexSeen>>,
}

#[async_trait::async_trait]
impl Housekeeping for LifecycleHousekeeping {
    async fn tick(&self) {
        let Some(fluree) = self.fluree.upgrade() else {
            return;
        };
        match observe(&fluree).await {
            Ok(versions) => {
                let now = Instant::now();
                let previous = std::mem::take(&mut *self.previous.lock());
                let seen: Observed = versions
                    .into_iter()
                    .map(|(op, version)| {
                        let since = previous
                            .get(&op)
                            .filter(|p| p.version == version)
                            .map_or(now, |p| p.since);
                        (op, Seen { version, since })
                    })
                    .collect();
                *self.previous.lock() = seen.clone();
                for (op, seen) in &seen {
                    // Unchanged since the last tick.
                    if seen.since == now {
                        continue;
                    }
                    if matches!(op, Unfinished::Create(_) | Unfinished::BranchCreate { .. })
                        && seen.since.elapsed() < self.options.abandoned_create_after
                    {
                        continue;
                    }
                    resume(&fluree, op, seen.version).await;
                }
            }
            Err(e) => warn!(error = %e, "lifecycle housekeeping: cannot list the nameservice"),
        }
        self.sweep_if_due(&fluree).await;
        #[cfg(feature = "native")]
        self.collect_if_due(&fluree).await;
        self.compact_idle_pool_deltas(&fluree).await;
    }
}

impl LifecycleHousekeeping {
    #[cfg(feature = "native")]
    async fn collect_if_due(&self, fluree: &Arc<Fluree>) {
        use std::sync::atomic::Ordering;
        if !fluree.collects_garbage() {
            return;
        }
        {
            let mut last = self.last_garbage.lock();
            if last.elapsed() < self.options.garbage_interval {
                return;
            }
            *last = Instant::now();
        }
        if self
            .collecting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let (fluree, options, collecting) = (
            Arc::clone(fluree),
            self.options.clone(),
            Arc::clone(&self.collecting),
        );
        tokio::spawn(async move {
            // Cleared even if the pass panics.
            struct Clear(Arc<std::sync::atomic::AtomicBool>);
            impl Drop for Clear {
                fn drop(&mut self) {
                    self.0.store(false, Ordering::Release);
                }
            }
            let _clear = Clear(collecting);
            collect_and_reconcile(&fluree, &options).await;
        });
    }

    async fn compact_idle_pool_deltas(&self, fluree: &Fluree) {
        if !self.options.compact_idle_pool_deltas {
            return;
        }
        let records = match fluree.nameservice().all_records().await {
            Ok(records) => records,
            Err(e) => {
                warn!(error = %e, "idle pool compaction: cannot list the nameservice");
                return;
            }
        };
        let previous = std::mem::take(&mut *self.indexes.lock());
        let mut seen = HashMap::new();
        for record in records.into_iter().filter(|r| !r.retracted) {
            let Some(head) = record.index_head_id.clone() else {
                continue;
            };
            let before = previous.get(&record.ledger_id);
            let mut checked = before.and_then(|b| b.checked.clone());
            let idle = record.index_t >= record.commit_t
                && before.is_some_and(|b| b.commit_t == record.commit_t && b.head == head);
            if idle && checked.as_ref() != Some(&head) {
                match fluree.compact_pool_deltas(&record.ledger_id).await {
                    // The new root is seen, and checked, on later ticks.
                    Ok(PoolCompaction::Compacted) => {}
                    Ok(PoolCompaction::NoDelta) => checked = Some(head.clone()),
                    // Tried again on the next idle tick.
                    Ok(PoolCompaction::Deferred) => {}
                    Err(e) => {
                        warn!(ledger_id = %record.ledger_id, error = %e, "idle pool compaction failed");
                    }
                }
            }
            seen.insert(
                record.ledger_id,
                IndexSeen {
                    commit_t: record.commit_t,
                    head,
                    checked,
                },
            );
        }
        *self.indexes.lock() = seen;
    }

    async fn sweep_if_due(&self, fluree: &Fluree) {
        let Some(interval) = self.options.orphan_sweep_interval else {
            return;
        };
        {
            let mut last = self.last_sweep.lock();
            if last.elapsed() < interval {
                return;
            }
            *last = Instant::now();
        }
        match fluree.sweep_orphan_instances(false).await {
            Ok(report) if report.orphans.is_empty() => debug!("orphan sweep found nothing"),
            Ok(report) => info!(
                folders = report.orphans.len(),
                files_deleted = report.artifacts_deleted,
                warnings = report.warnings.len(),
                "orphan sweep"
            ),
            Err(e) => warn!(error = %e, "orphan sweep failed"),
        }
    }
}

#[cfg(feature = "native")]
pub(crate) async fn collect_and_reconcile(fluree: &Fluree, options: &HousekeepingOptions) {
    match fluree
        .collect_garbage_within(options.garbage_grace, Some(options.reconcile_budget))
        .await
    {
        Ok(report) if report == crate::garbage::GarbageReport::default() => {}
        Ok(report) => info!(?report, "garbage collected"),
        Err(e) => warn!(error = %e, "garbage collection failed"),
    }
    let Some(reconcile_interval) = options.reconcile_interval else {
        return;
    };
    let pass = match fluree
        .reconcile_due(reconcile_interval, options.reconcile_budget)
        .await
    {
        Ok(pass) => pass,
        Err(e) => {
            warn!(error = %e, "garbage reconciliation failed");
            return;
        }
    };
    for (ledger, report) in &pass.ledgers {
        debug!(%ledger, ?report, "garbage reconciled");
    }
    // Throttling only defers: say when it falls behind.
    let behind = pass.oldest_ms.map(|ms| {
        Duration::from_millis(
            u64::try_from(chrono::Utc::now().timestamp_millis() - ms).unwrap_or(0),
        )
    });
    match behind {
        Some(age) if age > 2 * reconcile_interval => warn!(
            due = pass.due,
            behind_secs = age.as_secs(),
            "garbage reconciliation is behind"
        ),
        _ if !pass.ledgers.is_empty() => info!(
            ledgers = pass.ledgers.len(),
            due = pass.due,
            "garbage reconciled"
        ),
        _ => {}
    }
}

/// Every operation the nameservice shows unfinished, with the version of
/// the item that moves when it does.
async fn observe(fluree: &Fluree) -> crate::Result<HashMap<Unfinished, u64>> {
    let store = fluree.publisher()?;
    let mut observed = HashMap::new();
    for (name, binding) in store.list_bindings().await? {
        match binding.value.state {
            BindingState::Creating => {
                observed.insert(Unfinished::Create(name), binding.version);
            }
            BindingState::Dropping { .. } => {
                observed.insert(Unfinished::Drop(name), binding.version);
            }
            // A restore holds its registry entry until it finishes; that
            // entry is what is watched.
            BindingState::Restoring => {}
            BindingState::Active => {
                let ledger = LedgerName::parse(&name)?;
                // The listing's fence names the attempt, and nothing moves
                // while it is stuck: it is timed from when it was first seen.
                for listing in binding.value.branches.iter().filter(|b| b.creating) {
                    observed.insert(
                        Unfinished::BranchCreate {
                            name: name.clone(),
                            branch: listing.branch.clone(),
                            fence: listing.fence,
                        },
                        0,
                    );
                }
                for listing in binding.value.branches.iter().filter(|b| b.dropped) {
                    // A dropped branch with children keeps its data until they
                    // go. One without was stopped before its storage was
                    // deleted.
                    let childless = binding.value.children_of(&listing.branch).next().is_none()
                        && store
                            .raw_record(&ledger.with_branch(&listing.branch)?)
                            .await?
                            .is_some_and(|r| r.fence == Some(listing.fence));
                    if childless {
                        observed.insert(
                            Unfinished::BranchDrop {
                                name: name.clone(),
                                branch: listing.branch.clone(),
                                fence: listing.fence,
                            },
                            binding.version,
                        );
                    }
                }
            }
        }
    }
    for entry in store.list_dropped().await? {
        let instance = entry.value.instance.clone();
        match entry.value.state {
            DroppedState::Dropped => {}
            DroppedState::Restoring { .. } => {
                observed.insert(Unfinished::Restore(instance), entry.version);
            }
            DroppedState::Purging => {
                observed.insert(Unfinished::Purge(instance), entry.version);
            }
        }
    }
    Ok(observed)
}

async fn resume(fluree: &Fluree, op: &Unfinished, version: u64) {
    let outcome = match op {
        Unfinished::Create(name) => rollback_create(fluree, name, version).await,
        Unfinished::Drop(name) => resume_drop(fluree, name).await,
        Unfinished::Restore(instance) => resume_restore(fluree, instance).await,
        Unfinished::Purge(instance) => fluree
            .purge_dropped(instance.as_str())
            .await
            .map(|report| {
                info!(ledger = %report.ledger_id, %instance, data = ?report.data, "resumed an interrupted purge");
            }),
        Unfinished::BranchDrop {
            name,
            branch,
            fence,
        } => resume_branch_drop(fluree, name, branch, *fence).await,
        Unfinished::BranchCreate {
            name,
            branch,
            fence,
        } => rollback_branch_create(fluree, name, branch, *fence).await,
    };
    match outcome {
        Ok(()) => {}
        // It finished between the listing and now.
        Err(e) if e.is_not_found() => debug!(?op, error = %e, "nothing left to resume"),
        Err(e) => warn!(?op, error = %e, "cannot resume an interrupted operation"),
    }
}

/// Free the name a stopped create holds, then delete what it wrote.
async fn rollback_create(fluree: &Fluree, name: &str, version: u64) -> crate::Result<()> {
    let name = LedgerName::parse(name)?;
    let Some(report) = fluree.roll_back_create(&name, version).await? else {
        return Ok(());
    };
    for warning in &report.warnings {
        warn!(ledger = %name, %warning, "a rolled-back create's data left behind");
    }
    info!(ledger = %name, "the create's claim on the name had stopped being renewed");
    Ok(())
}

async fn resume_drop(fluree: &Fluree, name: &str) -> crate::Result<()> {
    let name = LedgerName::parse(name)?;
    let Some(binding) = fluree.publisher()?.get_binding(&name).await? else {
        return Ok(());
    };
    let report = fluree
        .drop_bound_ledger(&name, binding.value, DropMode::Soft, true)
        .await?;
    if report.status == DropStatus::Dropped {
        info!(ledger = %name, instance = ?report.instance, data = ?report.data, "resumed an interrupted drop");
    }
    Ok(())
}

async fn resume_restore(fluree: &Fluree, instance: &InstanceId) -> crate::Result<()> {
    #[cfg(feature = "native")]
    let _admission = match fluree.retention.erasure.get() {
        Some(erasure) => Some(erasure.admission.read().await),
        None => None,
    };
    let resumed =
        lifecycle::resume_restore_with(fluree.publisher()?, instance, |entry, records| {
            fluree.prepare_restore(entry, records)
        })
        .await;
    match resumed {
        Ok(Some(records)) => {
            fluree.resume_restored_retention(&records).await?;
            #[cfg(feature = "native")]
            if let Some(record) = records.first() {
                fluree.sweep_after_restore(record.ledger_id.name()).await;
            }
            info!(%instance, "resumed an interrupted restore");
            Ok(())
        }
        Ok(None) => Ok(()),
        // The name was taken meanwhile: the entry is back to `Dropped`.
        Err(NameServiceError::LedgerAlreadyExists(name)) => {
            warn!(%instance, ledger = %name, "an interrupted restore found its name taken; the ledger stays dropped");
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

async fn resume_branch_drop(
    fluree: &Fluree,
    name: &str,
    branch: &str,
    fence: Fence,
) -> crate::Result<()> {
    let name = LedgerName::parse(name)?;
    let id = name.with_branch(branch)?;
    let report = fluree.drop_bound_branch(&name, &id, Some(fence)).await?;
    if report.status == DropStatus::Dropped {
        info!(ledger_id = %id, artifacts_deleted = report.artifacts_deleted, "resumed an interrupted branch drop");
    }
    Ok(())
}

/// Unlist a branch whose create stopped before confirming it, then delete
/// its record. A create writes no data of its own before it confirms.
async fn rollback_branch_create(
    fluree: &Fluree,
    name: &str,
    branch: &str,
    fence: Fence,
) -> crate::Result<()> {
    let name = LedgerName::parse(name)?;
    if lifecycle::rollback_branch_create(fluree.publisher()?, &name, branch, fence).await? {
        info!(ledger = %name, %branch, "rolled back a branch create that did not finish");
    }
    Ok(())
}

impl Fluree {
    /// Housekeeping for [`IndexerHandle::set_housekeeping`](fluree_db_indexer::IndexerHandle::set_housekeeping):
    /// finishes the drops, restores and purges a crash interrupted, rolls
    /// back creates and branch creates that stopped, and, given an interval, runs
    /// [`sweep_orphan_instances`](Self::sweep_orphan_instances) at most that
    /// often, the first time one interval after it starts.
    ///
    /// Install it only where the nameservice knows every ledger, never on a
    /// peer. Holds this instance weakly, so it does not keep it alive.
    pub fn lifecycle_housekeeping(
        self: &Arc<Self>,
        orphan_sweep_interval: Option<Duration>,
    ) -> Arc<dyn Housekeeping> {
        self.lifecycle_housekeeping_with(HousekeepingOptions {
            orphan_sweep_interval,
            ..HousekeepingOptions::default()
        })
    }

    /// [`lifecycle_housekeeping`](Self::lifecycle_housekeeping) with every
    /// setting given.
    pub fn lifecycle_housekeeping_with(
        self: &Arc<Self>,
        options: HousekeepingOptions,
    ) -> Arc<dyn Housekeeping> {
        Arc::new(LifecycleHousekeeping {
            fluree: Arc::downgrade(self),
            options,
            previous: parking_lot::Mutex::default(),
            last_sweep: parking_lot::Mutex::new(Instant::now()),
            last_garbage: parking_lot::Mutex::new(Instant::now()),
            collecting: Arc::default(),
            indexes: parking_lot::Mutex::default(),
        })
    }

    /// Install [`lifecycle_housekeeping`](Self::lifecycle_housekeeping) on
    /// this instance's background indexer. Returns `false`, installing
    /// nothing, when it has none.
    pub fn start_lifecycle_housekeeping(
        self: &Arc<Self>,
        orphan_sweep_interval: Option<Duration>,
    ) -> bool {
        self.start_maintenance(
            HousekeepingOptions {
                orphan_sweep_interval,
                ..HousekeepingOptions::default()
            },
            None,
        )
    }

    /// Install this instance's maintenance on its background indexer, which
    /// must own catch-up: [`lifecycle_housekeeping`](Self::lifecycle_housekeeping)
    /// and, with `retention` settings, the retention worker committing
    /// directly ([`retention_housekeeping`](Self::retention_housekeeping) and
    /// its build observer). Returns `false`, installing nothing, when it has
    /// no background indexer.
    pub fn start_maintenance(
        self: &Arc<Self>,
        housekeeping: HousekeepingOptions,
        retention: Option<crate::retention::RetentionOptions>,
    ) -> bool {
        let crate::tx::IndexingMode::Background(handle) = &self.indexing_mode else {
            return false;
        };
        let lifecycle = self.lifecycle_housekeeping_with(housekeeping);
        match retention {
            Some(options) => {
                handle.set_housekeeping(Arc::new(fluree_db_indexer::Housekeepings(vec![
                    lifecycle,
                    self.retention_housekeeping(None, options),
                ])));
                handle.set_build_observer(self.retention_build_observer());
            }
            None => handle.set_housekeeping(lifecycle),
        }
        true
    }
}
