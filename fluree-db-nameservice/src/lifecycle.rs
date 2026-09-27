//! Ledger lifecycle protocols: create, create or drop a branch, drop, restore,
//! purge.
//!
//! Written against backends that can compare-and-swap only one item at a
//! time. Every step is idempotent, and an operation interrupted at any step
//! resumes from the binding or registry entry it left behind. No step lists
//! records to decide anything: the binding names every live branch.
//!
//! Storage is not touched here. Callers write a new ledger's data between
//! [`begin_create`] and [`activate`], delete a purged ledger's root between
//! [`begin_purge`] and [`finish_purge`], and delete a dropped branch's data
//! between [`begin_drop_branch`] and [`finish_drop_branch`].

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
        branches: vec![BranchFence::new(id.branch(), fence)],
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
    let pending = PendingLedger {
        instance,
        root: root.clone(),
        record: NsRecord {
            storage_root: Some(root),
            ..record.clone()
        },
        binding,
        version,
    };
    if let Err(e) = insert_fenced(store, &record).await {
        // Release the claim, or the name stays held by a create that failed.
        if let Err(undo) = abandon(store, &pending).await {
            tracing::warn!(name = %name, error = %undo, "could not release a failed create's claim on the name");
        }
        return Err(e);
    }
    Ok(pending)
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
            .listing(source_branch)
            .filter(|b| !b.dropped)
            .ok_or_else(|| NameServiceError::not_found(source_id.to_string()))?
            .fence;
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
        next.branches.push(BranchFence::new(new_branch, fence));
        match store.cas_binding(name, Some(version), Some(&next)).await? {
            RegistryCas::Updated { .. } => {}
            RegistryCas::Conflict { .. } => continue,
        }

        // The parent's count goes up before the child exists: a crash in
        // between leaves the parent undroppable rather than droppable under
        // a live child. A parent frozen since it was read refuses.
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

/// Copy `record`, read from the nameservice that owns its ledger, into
/// `store`: bind its name to the record's instance and root, list its
/// branch, and insert it. A root at the name itself, from before name
/// bindings, takes the instance the migration derives from the name
/// ([`legacy_instance`]). A binding to another instance is replaced: the
/// origin has dropped that ledger and created this one under the name.
///
/// The record keeps its fence when it carries one; otherwise a local one is
/// issued, since only this copy has to agree with it. Heads are copied as the
/// record carries them, and an existing copy's heads are left for the caller
/// to fast-forward, presenting the returned fence.
pub async fn mirror_record<S: LifecycleStore + ?Sized>(
    store: &S,
    record: &NsRecord,
) -> Result<Fence> {
    let Some(root) = record.storage_root.clone() else {
        return Err(NameServiceError::invalid_id(format!(
            "{} has no storage root to mirror",
            record.ledger_id
        )));
    };
    let name = record.ledger_id.ledger_name();
    // A ledger from before name bindings keeps its root at the name; the
    // origin's migration gave it the instance derived from the name.
    let instance = root.instance().unwrap_or_else(|| legacy_instance(&name));
    let mut fence = None;
    for _ in 0..MAX_ATTEMPTS {
        let current = store.get_binding(&name).await?;
        let (expected, mut next) = match current {
            Some(Versioned { value, version }) if value.instance == instance => {
                if let Some(listed) = value.listing(&record.branch) {
                    if value.is_active() && listed.dropped == record.retracted {
                        fence = Some(listed.fence);
                        break;
                    }
                }
                (Some(version), value)
            }
            other => (
                other.map(|v| v.version),
                NameBinding {
                    instance: instance.clone(),
                    root: root.clone(),
                    root_branch: fluree_db_core::DEFAULT_BRANCH.to_string(),
                    state: BindingState::Active,
                    branches: Vec::new(),
                },
            ),
        };
        let branch_fence = next
            .fence_of(&record.branch)
            .or(record.fence)
            .unwrap_or_else(Fence::generate);
        next.state = BindingState::Active;
        if record.source_branch.is_none() {
            next.root_branch = record.branch.clone();
        }
        next.branches.retain(|b| b.branch != record.branch);
        next.branches.push(BranchFence {
            dropped: record.retracted,
            ..BranchFence::new(&record.branch, branch_fence)
        });
        if let RegistryCas::Updated { .. } = store.cas_binding(&name, expected, Some(&next)).await?
        {
            fence = Some(branch_fence);
            break;
        }
    }
    let fence = fence.ok_or_else(|| contended(&name))?;

    let mut copy = record.clone();
    copy.fence = Some(fence);
    copy.storage_root = None;
    copy.retracted = false;
    insert_fenced(store, &copy).await?;
    Ok(fence)
}

/// Remove a copy [`mirror_record`] made, once the nameservice that owns the
/// ledger has dropped the branch: unlist the branch, removing the binding
/// with its last branch, then delete the record. The copy owns no data, so
/// nothing else is deleted.
///
/// Given the `instance` the branch belonged to, a copy of another ledger
/// under the name is left alone: the drop is older than that ledger.
pub async fn unmirror_record<S: LifecycleStore + ?Sized>(
    store: &S,
    ledger_id: &LedgerId,
    instance: Option<&InstanceId>,
) -> Result<()> {
    let name = ledger_id.ledger_name();
    let branch = ledger_id.branch();
    for _ in 0..MAX_ATTEMPTS {
        let Some(Versioned { value, version }) = store.get_binding(&name).await? else {
            return Ok(());
        };
        if instance.is_some_and(|i| *i != value.instance) {
            return Ok(());
        }
        let Some(fence) = value.fence_of(branch) else {
            return Ok(());
        };
        let mut next = value;
        next.branches.retain(|b| b.branch != branch);
        let new = (!next.branches.is_empty()).then_some(&next);
        if let RegistryCas::Updated { .. } = store.cas_binding(&name, Some(version), new).await? {
            store.delete_record(ledger_id.as_ref(), fence).await?;
            return Ok(());
        }
    }
    Err(contended(&name))
}

/// What [`begin_drop_branch`] left to do.
#[derive(Clone, Debug)]
pub enum BranchDrop {
    /// The branch has child branches, whose history reaches into its data.
    /// It is frozen and reads as dropped, and keeps its record and data
    /// until [`finish_drop_branch`] of its last child hands it back.
    Deferred {
        /// Frozen by an earlier drop, not this one.
        already: bool,
    },
    /// The branch is frozen and has no children. The caller deletes its
    /// storage, then calls [`finish_drop_branch`] with this record.
    Purge(Box<NsRecord>),
}

/// Drop `branch` of the ledger holding `name`: mark it dropped in the
/// binding, so reads see it as absent and nothing can branch from it, then
/// freeze its record, so no writer can publish to it.
///
/// The branch stays listed until [`finish_drop_branch`], so its name cannot
/// be reused while its storage is being deleted. A drop already under way is
/// resumed.
pub async fn begin_drop_branch<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &LedgerName,
    branch: &str,
) -> Result<BranchDrop> {
    drop_branch_from(store, name, branch, None)
        .await?
        .ok_or_else(|| NameServiceError::not_found(format!("{name}:{branch}")))
}

/// [`begin_drop_branch`] for a drop of `branch` under `fence` that was
/// started and stopped short. Returns `None`, dropping nothing, unless the
/// branch is still listed as dropping under that fence: a branch created
/// under the name since is left alone.
pub async fn resume_drop_branch<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &LedgerName,
    branch: &str,
    fence: Fence,
) -> Result<Option<BranchDrop>> {
    drop_branch_from(store, name, branch, Some(fence)).await
}

async fn drop_branch_from<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &LedgerName,
    branch: &str,
    resume: Option<Fence>,
) -> Result<Option<BranchDrop>> {
    let id = name.with_branch(branch)?;
    let not_found = || NameServiceError::not_found(id.to_string());

    let mut marked = None;
    for _ in 0..MAX_ATTEMPTS {
        let Versioned {
            value: binding,
            version,
        } = active_binding(store, name).await?;
        if branch == binding.root_branch {
            return Err(NameServiceError::invalid_id(format!(
                "'{branch}' is the root branch of '{name}'; drop the ledger instead"
            )));
        }
        let listing = binding.listing(branch).ok_or_else(not_found)?.clone();
        if resume.is_some_and(|fence| !listing.dropped || listing.fence != fence) {
            return Ok(None);
        }
        if listing.dropped {
            marked = Some((binding.root, listing.fence, true));
            break;
        }
        let mut next = binding;
        for b in &mut next.branches {
            b.dropped |= b.branch == branch;
        }
        if let RegistryCas::Updated { .. } =
            store.cas_binding(name, Some(version), Some(&next)).await?
        {
            marked = Some((next.root, listing.fence, false));
            break;
        }
    }
    let (root, fence, already) = marked.ok_or_else(|| contended(name))?;

    if store.freeze_record(&id, fence).await? != FenceOutcome::Applied {
        return Err(not_found());
    }
    // Read after freezing: from here the count can only fall.
    let mut record = store
        .raw_record(&id)
        .await?
        .filter(|r| r.fence == Some(fence))
        .ok_or_else(not_found)?;
    if record.branches > 0 {
        return Ok(Some(BranchDrop::Deferred { already }));
    }
    record.storage_root = Some(root);
    Ok(Some(BranchDrop::Purge(Box::new(record))))
}

/// Forget a dropped branch once its storage is gone: take it out of the
/// binding, delete its record, and take it off its parent's child count.
///
/// Returns the parent's record when the parent is itself a dropped branch
/// that this left with no children, for the caller to purge in turn.
pub async fn finish_drop_branch<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &LedgerName,
    record: &NsRecord,
) -> Result<Option<NsRecord>> {
    let fence = record.fence.ok_or_else(|| {
        NameServiceError::storage(format!("{} carries no fence", record.ledger_id))
    })?;
    let mut parent_fence = None;
    let mut unlisted = false;
    for _ in 0..MAX_ATTEMPTS {
        let Versioned {
            value: binding,
            version,
        } = active_binding(store, name).await?;
        // An earlier attempt got further. If it stopped before the parent's
        // count, the count stays high, which only keeps the parent's data
        // longer.
        if binding.fence_of(&record.branch) != Some(fence) {
            return Ok(None);
        }
        parent_fence = record
            .source_branch
            .as_deref()
            .and_then(|p| binding.fence_of(p));
        let mut next = binding;
        next.branches.retain(|b| b.branch != record.branch);
        if let RegistryCas::Updated { .. } =
            store.cas_binding(name, Some(version), Some(&next)).await?
        {
            unlisted = true;
            break;
        }
    }
    if !unlisted {
        return Err(contended(name));
    }
    store.delete_record(&record.ledger_id, fence).await?;

    let (Some(parent), Some(parent_fence)) = (record.source_branch.as_deref(), parent_fence) else {
        return Ok(None);
    };
    let parent_id = name.with_branch(parent)?;
    if store.adjust_children(&parent_id, parent_fence, -1).await? != FenceOutcome::Applied {
        return Ok(None);
    }
    let Ok(binding) = active_binding(store, name).await else {
        return Ok(None);
    };
    if !binding
        .value
        .listing(parent)
        .is_some_and(|b| b.dropped && b.fence == parent_fence)
    {
        return Ok(None);
    }
    Ok(store
        .raw_record(&parent_id)
        .await?
        .filter(|r| r.fence == Some(parent_fence) && r.branches == 0)
        .map(|mut r| {
            r.storage_root = Some(binding.value.root);
            r
        }))
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
    drop_ledger_from(store, name, Some(hard)).await
}

/// Finish a drop of `name` that was started and stopped short, with the
/// hardness it started with. Returns `None`, dropping nothing, unless the
/// name's binding is dropping: a ledger that holds the name since is left
/// alone.
pub async fn resume_drop_ledger<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &LedgerName,
) -> Result<Option<DroppedOutcome>> {
    drop_ledger_from(store, name, None).await
}

/// [`drop_ledger`] with `start: Some(hard)`; [`resume_drop_ledger`] with
/// `None`, which claims no drop of its own.
async fn drop_ledger_from<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &LedgerName,
    start: Option<bool>,
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
                    let Some(hard) = start else {
                        return Ok(None);
                    };
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
                record.retracted = b.dropped;
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
    restore_from(store, instance, true)
        .await
        .map(Option::unwrap_or_default)
}

/// Finish a restore of `instance` that was started and stopped short.
/// Returns `None`, restoring nothing, unless its entry is restoring.
pub async fn resume_restore<S: LifecycleStore + ?Sized>(
    store: &S,
    instance: &InstanceId,
) -> Result<Option<Vec<NsRecord>>> {
    restore_from(store, instance, false).await
}

/// [`restore_dropped`] with `start`; [`resume_restore`] without, which
/// begins no restore of its own.
async fn restore_from<S: LifecycleStore + ?Sized>(
    store: &S,
    instance: &InstanceId,
    start: bool,
) -> Result<Option<Vec<NsRecord>>> {
    // 1. Mark the entry restoring, recording the fresh fences.
    let Some(Versioned {
        value: entry,
        version,
    }) = store.get_dropped(instance).await?
    else {
        return Err(NameServiceError::not_found(instance.to_string()));
    };
    let (fences, version) = match &entry.state {
        DroppedState::Dropped if !start => return Ok(None),
        DroppedState::Dropped => {
            let fences: Vec<BranchFence> = entry
                .branches
                .iter()
                .map(|r| BranchFence {
                    dropped: r.retracted,
                    ..BranchFence::new(&r.branch, Fence::generate())
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
                return restored_records(store, &entry.name, &existing.value)
                    .await
                    .map(Some);
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
        record.frozen = record.retracted;
        record.retracted = false;
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
    Ok(Some(records))
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

/// What [`migrate_legacy`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MigrationReport {
    /// Names bound to the ledgers they held.
    pub bound: Vec<String>,
    /// Names whose soft-dropped ledgers moved to the dropped-ledger registry.
    pub dropped: Vec<String>,
}

impl MigrationReport {
    pub fn is_empty(&self) -> bool {
        self.bound.is_empty() && self.dropped.is_empty()
    }
}

/// The instance id the migration gives a ledger created before name
/// bindings. Derived from the name, so an interrupted or concurrent
/// migration writes the same bindings and registry entries; its zero
/// timestamp keeps it apart from every generated id.
pub fn legacy_instance(name: &str) -> InstanceId {
    let digest = fluree_db_core::sha256_hex(format!("fluree:legacy-instance:{name}").as_bytes());
    let random = u128::from_str_radix(&digest[..20], 16).expect("hex digest");
    InstanceId::parse(&ulid::Ulid::from_parts(0, random).to_string())
        .expect("a ULID is a valid instance id")
}

/// The fence the migration gives a branch record created before fencing,
/// derived from its id for the same reason as [`legacy_instance`].
fn legacy_fence(ledger_id: &str) -> Fence {
    let digest = fluree_db_core::sha256_hex(format!("fluree:legacy-fence:{ledger_id}").as_bytes());
    Fence::from_u64(u64::from_str_radix(&digest[..16], 16).expect("hex digest"))
}

/// A legacy ledger's root branch: `main` when it has one, and otherwise the
/// first branch with no source.
fn legacy_root_branch(records: &[NsRecord]) -> String {
    let mut branches: Vec<&NsRecord> = records.iter().collect();
    branches.sort_by(|a, b| a.branch.cmp(&b.branch));
    branches
        .iter()
        .find(|r| r.branch == fluree_db_core::DEFAULT_BRANCH)
        .or_else(|| branches.iter().find(|r| r.source_branch.is_none()))
        .or_else(|| branches.first())
        .map_or_else(
            || fluree_db_core::DEFAULT_BRANCH.to_string(),
            |r| r.branch.clone(),
        )
}

/// Bind every ledger created before name bindings, so that no code path has
/// to infer a binding from branch records:
///
/// - a name with a live branch is bound to its ledger, rooted at the name,
///   and each of its records gets a fence; a retracted branch stays listed as
///   dropped;
/// - a name whose branches are all retracted was soft-dropped, and moves to
///   the dropped-ledger registry, where it can be restored or purged, and its
///   name is free.
///
/// Idempotent, and safe to run concurrently or to resume after a crash: the
/// instance ids and fences it issues are derived from the names, and every
/// step checks what an earlier attempt left.
pub async fn migrate_legacy<S: LifecycleStore + ?Sized>(store: &S) -> Result<MigrationReport> {
    // Records from before fencing, and records an interrupted migration
    // fenced but may not have listed yet.
    let mut by_name: std::collections::BTreeMap<String, Vec<NsRecord>> =
        std::collections::BTreeMap::new();
    for record in store.all_raw_records().await? {
        let legacy = legacy_fence(record.ledger_id.as_ref());
        if record.fence.is_none_or(|f| f == legacy) {
            by_name.entry(record.name.clone()).or_default().push(record);
        }
    }

    let mut report = MigrationReport::default();
    for (name, records) in by_name {
        let ledger_name = LedgerName::parse(&name)?;
        let bound = store.get_binding(&name).await?;
        let binding_is_legacy = bound
            .as_ref()
            .is_some_and(|b| b.value.instance == legacy_instance(&name));
        let migrated = binding_is_legacy
            && bound.as_ref().is_some_and(|b| {
                records
                    .iter()
                    .all(|r| r.fence.is_some() && b.value.fence_of(&r.branch) == r.fence)
            });
        if migrated {
            continue;
        }
        // A name bound since, by a ledger created over records that were all
        // retracted, keeps its ledger; those records were soft-dropped.
        if records.iter().any(|r| !r.retracted) && (bound.is_none() || binding_is_legacy) {
            bind_legacy(store, &ledger_name, &records).await?;
            report.bound.push(name);
        } else {
            register_legacy_drop(store, &ledger_name, records).await?;
            report.dropped.push(name);
        }
    }
    Ok(report)
}

async fn bind_legacy<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &LedgerName,
    records: &[NsRecord],
) -> Result<()> {
    let mut branches = Vec::with_capacity(records.len());
    for record in records {
        let id = record.ledger_id.to_string();
        let fence = legacy_fence(&id);
        if store.adopt_record(&id, fence).await? == FenceOutcome::Mismatch {
            return Err(NameServiceError::storage(format!(
                "cannot migrate {id}: its record was fenced by someone else"
            )));
        }
        if record.retracted {
            store.freeze_record(&id, fence).await?;
        }
        branches.push(BranchFence {
            dropped: record.retracted,
            ..BranchFence::new(&record.branch, fence)
        });
    }
    branches.sort_by(|a, b| a.branch.cmp(&b.branch));

    let binding = NameBinding {
        instance: legacy_instance(name),
        root: StorageRoot::legacy(name),
        root_branch: legacy_root_branch(records),
        state: BindingState::Active,
        branches,
    };
    match store.cas_binding(name, None, Some(&binding)).await? {
        RegistryCas::Updated { .. } => Ok(()),
        // An earlier attempt bound it, perhaps without a branch it had not
        // yet fenced then.
        RegistryCas::Conflict {
            actual: Some(existing),
        } if existing.value.instance == binding.instance => {
            let mut merged = existing.value.clone();
            for b in &binding.branches {
                if merged.listing(&b.branch).is_none() {
                    merged.branches.push(b.clone());
                }
            }
            if merged == existing.value {
                return Ok(());
            }
            match store
                .cas_binding(name, Some(existing.version), Some(&merged))
                .await?
            {
                RegistryCas::Updated { .. } => Ok(()),
                RegistryCas::Conflict { .. } => Err(contended(name)),
            }
        }
        RegistryCas::Conflict { .. } => Err(contended(name)),
    }
}

async fn register_legacy_drop<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &LedgerName,
    records: Vec<NsRecord>,
) -> Result<()> {
    let instance = legacy_instance(name);
    let entry = DroppedLedger {
        instance: instance.clone(),
        state: DroppedState::Dropped,
        dropped_at: now_ms(),
        name: name.to_string(),
        root: StorageRoot::legacy(name),
        root_branch: legacy_root_branch(&records),
        // Retracted by the drop of the whole ledger, so all come back.
        branches: records
            .iter()
            .map(|r| NsRecord {
                retracted: false,
                frozen: false,
                fence: None,
                storage_root: None,
                ..r.clone()
            })
            .collect(),
    };
    match store.cas_dropped(&instance, None, Some(&entry)).await? {
        RegistryCas::Updated { .. } | RegistryCas::Conflict { actual: Some(_) } => {}
        RegistryCas::Conflict { actual: None } => return Err(contended(name)),
    }
    for record in &records {
        let id = record.ledger_id.to_string();
        let fence = legacy_fence(&id);
        if store.adopt_record(&id, fence).await? == FenceOutcome::Applied {
            store.delete_record(&id, fence).await?;
        }
    }
    Ok(())
}
