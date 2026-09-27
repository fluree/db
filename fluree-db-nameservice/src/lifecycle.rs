//! Ledger lifecycle protocols: create, create branch, drop, restore, purge.
//!
//! Written against backends that can compare-and-swap only one item at a
//! time. Every step is idempotent, and an operation interrupted at any step
//! resumes from the binding or registry entry it left behind. No step lists
//! records to decide anything: the binding names every live branch.
//!
//! Storage is not touched here. Callers write a new ledger's data between
//! [`begin_create`] and [`activate`], and delete a purged ledger's root
//! between [`begin_purge`] and [`finish_purge`].

use crate::binding::{
    new_instance_id, BindingState, BranchFence, BranchRecordStore, DroppedLedger, DroppedState,
    Fence, FenceOutcome, LedgerRegistry, NameBinding, RegistryCas, Versioned,
};
use crate::{NameServiceError, NsRecord, Result};
use fluree_db_core::{ContentId, InstanceId, LedgerId, LedgerName, StorageRoot};

/// A backend the protocols can drive.
pub trait LifecycleStore: LedgerRegistry + BranchRecordStore {}
impl<T: LedgerRegistry + BranchRecordStore + ?Sized> LifecycleStore for T {}

/// Compare-and-swap retries before an operation gives up on a contended
/// binding.
const MAX_ATTEMPTS: usize = 8;

fn now_ms() -> i64 {
    fluree_db_core::clock::SystemTime::now()
        .duration_since(fluree_db_core::clock::SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

fn contended(name: &str) -> NameServiceError {
    NameServiceError::storage(format!(
        "the name binding for '{name}' kept changing; retry"
    ))
}

fn name_taken(name: &str, binding: Option<&NameBinding>) -> NameServiceError {
    match binding.map(|b| b.state) {
        Some(BindingState::Dropping { .. }) => {
            NameServiceError::ledger_already_exists(format!("{name} (a drop is in progress)"))
        }
        _ => NameServiceError::ledger_already_exists(name),
    }
}

/// Insert `record` for a binding the caller holds. A record already at the
/// key with another fence is garbage, since the caller's binding names the
/// only fence that key can be live under, so it is deleted and the insert
/// retried. A record carrying no fence predates fencing and is never deleted.
async fn insert_fenced<S: LifecycleStore + ?Sized>(store: &S, record: &NsRecord) -> Result<()> {
    for _ in 0..MAX_ATTEMPTS {
        let Some(existing) = store.insert_record(record).await? else {
            return Ok(());
        };
        if existing.fence == record.fence {
            return Ok(());
        }
        let Some(garbage) = existing.fence else {
            return Err(NameServiceError::ledger_already_exists(format!(
                "{} (an unfenced record holds the key)",
                record.ledger_id
            )));
        };
        store.delete_record(&record.ledger_id, garbage).await?;
    }
    Err(contended(&record.name))
}

/// A ledger whose name a create has claimed but not yet activated.
#[derive(Clone, Debug)]
pub struct PendingLedger {
    pub instance: InstanceId,
    pub root: StorageRoot,
    /// The root branch's record, carrying its fence and storage root.
    pub record: NsRecord,
    binding: NameBinding,
    version: u64,
}

/// Claim `id`'s name for a new ledger whose root branch is `id`'s branch,
/// and insert that branch's record. The ledger is not visible until
/// [`activate`].
pub async fn begin_create<S: LifecycleStore + ?Sized>(
    store: &S,
    id: &LedgerId,
) -> Result<PendingLedger> {
    let name = id.ledger_name();
    let instance = new_instance_id();
    let fence = Fence::generate();
    let root = StorageRoot::for_instance(&name, &instance);
    let binding = NameBinding {
        instance: instance.clone(),
        root: root.clone(),
        root_branch: id.branch().to_string(),
        state: BindingState::Creating,
        branches: vec![BranchFence {
            branch: id.branch().to_string(),
            fence,
        }],
    };

    let version = match store.cas_binding(&name, None, Some(&binding)).await? {
        RegistryCas::Updated { version: Some(v) } => v,
        RegistryCas::Updated { version: None } => {
            return Err(NameServiceError::storage(
                "binding write returned no version",
            ))
        }
        RegistryCas::Conflict { actual } => {
            return Err(name_taken(&name, actual.as_ref().map(|v| &v.value)))
        }
    };

    let mut record = NsRecord::new(id.clone());
    record.fence = Some(fence);
    insert_fenced(store, &record).await?;
    record.storage_root = Some(root.clone());

    Ok(PendingLedger {
        instance,
        root,
        record,
        binding,
        version,
    })
}

/// Make a pending ledger visible.
pub async fn activate<S: LifecycleStore + ?Sized>(
    store: &S,
    pending: &PendingLedger,
) -> Result<()> {
    let name = pending.record.name.as_str();
    let active = NameBinding {
        state: BindingState::Active,
        ..pending.binding.clone()
    };
    match store
        .cas_binding(name, Some(pending.version), Some(&active))
        .await?
    {
        RegistryCas::Updated { .. } => Ok(()),
        RegistryCas::Conflict { .. } => Err(NameServiceError::storage(format!(
            "the create of '{name}' was rolled back before it finished"
        ))),
    }
}

/// Undo a pending ledger: delete its record, then its claim on the name.
/// Its storage, if any was written, is left for the orphan sweep.
pub async fn abandon<S: LifecycleStore + ?Sized>(store: &S, pending: &PendingLedger) -> Result<()> {
    for b in &pending.binding.branches {
        let id = LedgerId::from_parts(&pending.record.name, &b.branch)?;
        store.delete_record(&id, b.fence).await?;
    }
    match store
        .cas_binding(&pending.record.name, Some(pending.version), None)
        .await?
    {
        RegistryCas::Updated { .. } | RegistryCas::Conflict { actual: None } => Ok(()),
        RegistryCas::Conflict { .. } => Err(NameServiceError::storage(format!(
            "the name binding for '{}' changed while rolling back its create",
            pending.record.name
        ))),
    }
}

/// Create an empty ledger: [`begin_create`] then [`activate`].
pub async fn create_ledger<S: LifecycleStore + ?Sized>(
    store: &S,
    id: &LedgerId,
) -> Result<NsRecord> {
    let pending = begin_create(store, id).await?;
    activate(store, &pending).await?;
    Ok(pending.record)
}

/// The active binding for `name`, or `NotFound`.
async fn active_binding<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &str,
) -> Result<Versioned<NameBinding>> {
    match store.get_binding(name).await? {
        Some(v) if v.value.is_active() => Ok(v),
        _ => Err(NameServiceError::not_found(name)),
    }
}

/// Create `new_branch` from `source_branch`, starting at `at_commit` or at
/// the source's current head. Returns the new branch's record.
pub async fn create_branch<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &LedgerName,
    new_branch: &str,
    source_branch: &str,
    at_commit: Option<(ContentId, i64)>,
) -> Result<NsRecord> {
    let new_id = name.with_branch(new_branch)?;
    let source_id = name.with_branch(source_branch)?;

    for _ in 0..MAX_ATTEMPTS {
        let Versioned {
            value: binding,
            version,
        } = active_binding(store, name).await?;
        let source_fence = binding
            .fence_of(source_branch)
            .ok_or_else(|| NameServiceError::not_found(source_id.to_string()))?;
        let source = store
            .raw_record(&source_id)
            .await?
            .filter(|r| r.fence == Some(source_fence))
            .ok_or_else(|| NameServiceError::not_found(source_id.to_string()))?;

        // A listed branch whose record is missing is a create that did not
        // finish; its listing is replaced with a fresh fence, which turns any
        // record it later inserts into garbage.
        if let Some(listed) = binding.fence_of(new_branch) {
            let has_record = store
                .raw_record(&new_id)
                .await?
                .is_some_and(|r| r.fence == Some(listed));
            if has_record {
                return Err(NameServiceError::ledger_already_exists(new_id.to_string()));
            }
        }

        let fence = Fence::generate();
        let mut next = binding.clone();
        next.branches.retain(|b| b.branch != new_branch);
        next.branches.push(BranchFence {
            branch: new_branch.to_string(),
            fence,
        });
        match store.cas_binding(name, Some(version), Some(&next)).await? {
            RegistryCas::Updated { .. } => {}
            RegistryCas::Conflict { .. } => continue,
        }

        // The parent's count goes up before the child exists: a crash in
        // between leaves the parent undroppable rather than droppable under
        // a live child.
        if store.adjust_children(&source_id, source_fence, 1).await? != FenceOutcome::Applied {
            return Err(NameServiceError::not_found(source_id.to_string()));
        }

        let mut record = NsRecord::new(new_id.clone());
        record.fence = Some(fence);
        record.source_branch = Some(source_branch.to_string());
        (record.commit_head_id, record.commit_t) = match at_commit {
            Some((id, t)) => (Some(id), t),
            None => (source.commit_head_id.clone(), source.commit_t),
        };
        insert_fenced(store, &record).await?;
        record.storage_root = Some(binding.root.clone());
        return Ok(record);
    }
    Err(contended(name))
}

/// What a drop did.
#[derive(Clone, Debug)]
pub struct DroppedOutcome {
    pub instance: InstanceId,
    pub hard: bool,
    /// The registry entry the drop left: `Dropped`, or `Purging` for a hard
    /// drop that still needs [`finish_purge`] once its storage is deleted.
    pub entry: DroppedLedger,
}

/// Drop the ledger holding `name`, freeing the name.
///
/// A soft drop keeps the data in the registry; a hard drop leaves a
/// `Purging` entry whose storage the caller deletes before calling
/// [`finish_purge`]. A drop already in progress is resumed, keeping the
/// hardness it started with. Returns `None` when nothing holds the name.
pub async fn drop_ledger<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &LedgerName,
    hard: bool,
) -> Result<Option<DroppedOutcome>> {
    // 1. Claim the drop.
    let (binding, version) = {
        let mut claimed = None;
        for _ in 0..MAX_ATTEMPTS {
            let Some(Versioned { value, version }) = store.get_binding(name).await? else {
                return Ok(None);
            };
            match value.state {
                BindingState::Dropping { .. } => {
                    claimed = Some((value, version));
                    break;
                }
                BindingState::Restoring => {
                    return Err(NameServiceError::storage(format!(
                        "'{name}' is being restored; retry the drop once it finishes"
                    )))
                }
                BindingState::Active | BindingState::Creating => {
                    let dropping = NameBinding {
                        state: BindingState::Dropping { hard },
                        ..value
                    };
                    if let RegistryCas::Updated { version: Some(v) } = store
                        .cas_binding(name, Some(version), Some(&dropping))
                        .await?
                    {
                        claimed = Some((dropping, v));
                        break;
                    }
                }
            }
        }
        claimed.ok_or_else(|| contended(name))?
    };
    let BindingState::Dropping { hard } = binding.state else {
        unreachable!("claimed binding is dropping");
    };

    // 2. Freeze every listed branch, and copy its record.
    let mut records = Vec::with_capacity(binding.branches.len());
    for b in &binding.branches {
        let id = name.with_branch(&b.branch)?;
        if store.freeze_record(&id, b.fence).await? == FenceOutcome::Applied {
            if let Some(mut record) = store
                .raw_record(&id)
                .await?
                .filter(|r| r.fence == Some(b.fence))
            {
                record.storage_root = None;
                records.push(record);
            }
        }
    }

    // 3. Record the dropped ledger. An entry already there is this drop's
    // own earlier attempt, written before any record was deleted.
    let entry = DroppedLedger {
        instance: binding.instance.clone(),
        state: if hard {
            DroppedState::Purging
        } else {
            DroppedState::Dropped
        },
        dropped_at: now_ms(),
        name: name.to_string(),
        root: binding.root.clone(),
        root_branch: binding.root_branch.clone(),
        branches: records,
    };
    let entry = match store
        .cas_dropped(&binding.instance, None, Some(&entry))
        .await?
    {
        RegistryCas::Updated { .. } => entry,
        RegistryCas::Conflict {
            actual: Some(existing),
        } => existing.value,
        RegistryCas::Conflict { actual: None } => return Err(contended(name)),
    };

    // 4. Delete the branch records.
    for b in &binding.branches {
        store
            .delete_record(&name.with_branch(&b.branch)?, b.fence)
            .await?;
    }

    // 5. Free the name.
    match store.cas_binding(name, Some(version), None).await? {
        RegistryCas::Updated { .. } | RegistryCas::Conflict { actual: None } => {}
        RegistryCas::Conflict { .. } => return Err(contended(name)),
    }

    Ok(Some(DroppedOutcome {
        instance: binding.instance,
        hard,
        entry,
    }))
}

/// Restore a soft-dropped ledger under its own name, with a fresh fence on
/// every branch. Returns its branch records.
///
/// Fails with `LedgerAlreadyExists` when another ledger holds the name.
pub async fn restore_dropped<S: LifecycleStore + ?Sized>(
    store: &S,
    instance: &InstanceId,
) -> Result<Vec<NsRecord>> {
    // 1. Mark the entry restoring, recording the fresh fences.
    let Some(Versioned {
        value: entry,
        version,
    }) = store.get_dropped(instance).await?
    else {
        return Err(NameServiceError::not_found(instance.to_string()));
    };
    let (fences, version) = match &entry.state {
        DroppedState::Dropped => {
            let fences: Vec<BranchFence> = entry
                .branches
                .iter()
                .map(|r| BranchFence {
                    branch: r.branch.clone(),
                    fence: Fence::generate(),
                })
                .collect();
            let restoring = DroppedLedger {
                state: DroppedState::Restoring {
                    fences: fences.clone(),
                },
                ..entry.clone()
            };
            match store
                .cas_dropped(instance, Some(version), Some(&restoring))
                .await?
            {
                RegistryCas::Updated { version: Some(v) } => (fences, v),
                _ => return Err(contended(&entry.name)),
            }
        }
        DroppedState::Restoring { fences } => (fences.clone(), version),
        DroppedState::Purging => {
            return Err(NameServiceError::storage(format!(
                "dropped ledger {instance} is being purged"
            )))
        }
    };

    // 2. Claim the name.
    let binding = NameBinding {
        instance: instance.clone(),
        root: entry.root.clone(),
        root_branch: entry.root_branch.clone(),
        state: BindingState::Restoring,
        branches: fences.clone(),
    };
    let binding_version = match store.cas_binding(&entry.name, None, Some(&binding)).await? {
        RegistryCas::Updated { version: Some(v) } => v,
        RegistryCas::Conflict {
            actual: Some(existing),
        } if existing.value.instance == *instance => match existing.value.state {
            // This restore's own claim, from an interrupted attempt.
            BindingState::Restoring => existing.version,
            // An interrupted attempt already made it visible.
            BindingState::Active => {
                store.cas_dropped(instance, Some(version), None).await?;
                return restored_records(store, &entry.name, &existing.value).await;
            }
            BindingState::Creating | BindingState::Dropping { .. } => {
                return Err(contended(&entry.name))
            }
        },
        RegistryCas::Updated { version: None } | RegistryCas::Conflict { .. } => {
            let dropped = DroppedLedger {
                state: DroppedState::Dropped,
                ..entry.clone()
            };
            store
                .cas_dropped(instance, Some(version), Some(&dropped))
                .await?;
            return Err(NameServiceError::ledger_already_exists(entry.name.clone()));
        }
    };

    // 3. Reinstate the branch records under the fresh fences.
    let mut records = Vec::with_capacity(entry.branches.len());
    for saved in &entry.branches {
        let mut record = saved.clone();
        record.fence = binding.fence_of(&record.branch);
        record.frozen = false;
        insert_fenced(store, &record).await?;
        record.storage_root = Some(entry.root.clone());
        records.push(record);
    }

    // 4. Make it visible, then forget the entry.
    let active = NameBinding {
        state: BindingState::Active,
        ..binding
    };
    match store
        .cas_binding(&entry.name, Some(binding_version), Some(&active))
        .await?
    {
        RegistryCas::Updated { .. } => {}
        RegistryCas::Conflict { .. } => return Err(contended(&entry.name)),
    }
    store.cas_dropped(instance, Some(version), None).await?;
    Ok(records)
}

/// The live records a binding lists, with its root.
async fn restored_records<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &str,
    binding: &NameBinding,
) -> Result<Vec<NsRecord>> {
    let name = LedgerName::parse(name)?;
    let mut records = Vec::with_capacity(binding.branches.len());
    for b in &binding.branches {
        if let Some(mut record) = store
            .raw_record(&name.with_branch(&b.branch)?)
            .await?
            .filter(|r| r.fence == Some(b.fence))
        {
            record.storage_root = Some(binding.root.clone());
            records.push(record);
        }
    }
    Ok(records)
}

/// Mark a dropped ledger for purging and return its entry, whose root the
/// caller deletes before calling [`finish_purge`]. A purge already under way
/// is resumed.
pub async fn begin_purge<S: LifecycleStore + ?Sized>(
    store: &S,
    instance: &InstanceId,
) -> Result<DroppedLedger> {
    let Some(Versioned {
        value: entry,
        version,
    }) = store.get_dropped(instance).await?
    else {
        return Err(NameServiceError::not_found(instance.to_string()));
    };
    match entry.state {
        DroppedState::Purging => Ok(entry),
        DroppedState::Restoring { .. } => Err(NameServiceError::storage(format!(
            "dropped ledger {instance} is being restored"
        ))),
        DroppedState::Dropped => {
            let purging = DroppedLedger {
                state: DroppedState::Purging,
                ..entry
            };
            match store
                .cas_dropped(instance, Some(version), Some(&purging))
                .await?
            {
                RegistryCas::Updated { .. } => Ok(purging),
                RegistryCas::Conflict { .. } => Err(contended(&purging.name)),
            }
        }
    }
}

/// Forget a purged ledger once its storage is gone.
pub async fn finish_purge<S: LifecycleStore + ?Sized>(
    store: &S,
    instance: &InstanceId,
) -> Result<()> {
    let Some(Versioned { value, version }) = store.get_dropped(instance).await? else {
        return Ok(());
    };
    if value.state != DroppedState::Purging {
        return Err(NameServiceError::storage(format!(
            "dropped ledger {instance} is not being purged"
        )));
    }
    store.cas_dropped(instance, Some(version), None).await?;
    Ok(())
}
