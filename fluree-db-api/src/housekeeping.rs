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
//! the name and deletes whatever the create wrote.
//!
//! With an interval set, it also runs the orphan sweep.

use crate::admin::{DropMode, DropReport, DropStatus};
use crate::Fluree;
use fluree_db_core::{InstanceId, LedgerName};
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
    /// long, timed by this process.
    pub abandoned_create_after: Duration,
}

impl Default for HousekeepingOptions {
    fn default() -> Self {
        Self {
            orphan_sweep_interval: None,
            abandoned_create_after: ABANDONED_CREATE_AFTER,
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

struct LifecycleHousekeeping {
    fluree: Weak<Fluree>,
    options: HousekeepingOptions,
    previous: parking_lot::Mutex<Observed>,
    last_sweep: parking_lot::Mutex<Instant>,
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
                    if matches!(op, Unfinished::Create(_))
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
    }
}

impl LifecycleHousekeeping {
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
                for listing in binding.value.branches.iter().filter(|b| b.dropped) {
                    // A dropped branch with children keeps its data until they
                    // go. One without was stopped before its storage was
                    // deleted.
                    let childless = store
                        .raw_record(&ledger.with_branch(&listing.branch)?)
                        .await?
                        .is_some_and(|r| r.fence == Some(listing.fence) && r.branches == 0);
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
    };
    match outcome {
        Ok(()) => {}
        // It finished between the listing and now.
        Err(e) if e.is_not_found() => debug!(?op, error = %e, "nothing left to resume"),
        Err(e) => warn!(?op, error = %e, "cannot resume an interrupted operation"),
    }
}

/// Free the name a stopped create holds, then delete what it wrote. Only an
/// instance root is deleted whole; a create always makes one.
async fn rollback_create(fluree: &Fluree, name: &str, version: u64) -> crate::Result<()> {
    let Some(binding) = lifecycle::rollback_create(fluree.publisher()?, name, version).await?
    else {
        return Ok(());
    };
    let mut report = DropReport::default();
    if let (Some(storage), Some(_)) = (fluree.admin_storage(), binding.root.instance()) {
        crate::admin::purge_instance_root(storage, &binding.root, &[], &mut report).await;
    }
    for warning in &report.warnings {
        warn!(ledger = %name, %warning, "a rolled-back create's data left behind");
    }
    info!(
        ledger = %name,
        instance = %binding.instance,
        files_deleted = report.artifacts_deleted,
        "rolled back a create whose claim on the name stopped being renewed"
    );
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
    match lifecycle::resume_restore(fluree.publisher()?, instance).await {
        Ok(Some(_)) => {
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

impl Fluree {
    /// Housekeeping for [`IndexerHandle::set_housekeeping`](fluree_db_indexer::IndexerHandle::set_housekeeping):
    /// finishes the drops, restores and purges a crash interrupted, rolls
    /// back creates that stopped, and, given an interval, runs
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
        })
    }

    /// Install [`lifecycle_housekeeping`](Self::lifecycle_housekeeping) on
    /// this instance's background indexer. Returns `false`, installing
    /// nothing, when it has none.
    pub fn start_lifecycle_housekeeping(
        self: &Arc<Self>,
        orphan_sweep_interval: Option<Duration>,
    ) -> bool {
        let crate::tx::IndexingMode::Background(handle) = &self.indexing_mode else {
            return false;
        };
        handle.set_housekeeping(self.lifecycle_housekeeping(orphan_sweep_interval));
        true
    }
}
