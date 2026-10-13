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
    NameServiceError::conflict(format!(
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

/// [`LedgerRegistry::cas_binding`], reporting a write that landed as one. A
/// write can take effect while its caller is told it failed: the response
/// is lost and the write retried, and the retry finds the item already at
/// the version it would have written, holding its own value, and reports a
/// conflict. Versions never repeat for a key, so that value is this write,
/// or another writer's identical one, which leaves the same state.
async fn cas_binding<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &str,
    expected: Option<u64>,
    new: Option<&NameBinding>,
) -> Result<RegistryCas<NameBinding>> {
    let outcome = store.cas_binding(name, expected, new).await?;
    Ok(settled(expected, new, outcome))
}

/// [`cas_binding`] for the dropped-ledger registry.
async fn cas_dropped<S: LifecycleStore + ?Sized>(
    store: &S,
    instance: &InstanceId,
    expected: Option<u64>,
    new: Option<&DroppedLedger>,
) -> Result<RegistryCas<DroppedLedger>> {
    let outcome = store.cas_dropped(instance, expected, new).await?;
    Ok(settled(expected, new, outcome))
}

/// `outcome`, with a conflict holding exactly `new` at the version a write
/// expecting `expected` would have taken counted as that write.
fn settled<T: PartialEq>(
    expected: Option<u64>,
    new: Option<&T>,
    outcome: RegistryCas<T>,
) -> RegistryCas<T> {
    match (outcome, new) {
        (
            RegistryCas::Conflict {
                actual: Some(actual),
            },
            Some(new),
        ) if actual.value == *new && expected.is_none_or(|v| actual.version == v + 1) => {
            RegistryCas::Updated {
                version: Some(actual.version),
            }
        }
        (outcome, _) => outcome,
    }
}

/// Insert `record` for a binding the caller holds. A record already at the
/// key with another fence is garbage while the caller's binding still lists
/// the caller's fence, since that binding names the only fence the key can
/// be live under, so it is deleted and the insert retried. The binding is
/// read after the record: a record seen before the binding still listed the
/// caller cannot be live under a later one, whose fence is fresh. A caller
/// whose claim has gone deletes nothing: the record may belong to the
/// ledger that took the name. A record carrying no fence predates fencing
/// and is never deleted.
pub(crate) async fn insert_fenced<S: LifecycleStore + ?Sized>(
    store: &S,
    record: &NsRecord,
) -> Result<()> {
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
        let claimed = store
            .get_binding(&record.name)
            .await?
            .is_some_and(|b| b.value.fence_of(&record.branch) == record.fence);
        if !claimed {
            return Err(NameServiceError::conflict(format!(
                "the claim on '{}' that this write was part of is gone",
                record.name
            )));
        }
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
        extra: crate::binding::ExtraFields::new(),
        instance: instance.clone(),
        root: root.clone(),
        root_branch: id.branch().to_string(),
        state: BindingState::Creating,
        branches: vec![BranchFence::new(id.branch(), fence)],
    };

    let version = match cas_binding(store, &name, None, Some(&binding)).await? {
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
///
/// A write reported failed may have landed: before an error is reported,
/// the binding is read again, and the ledger counts as activated if it holds
/// this create's binding, active.
pub async fn activate<S: LifecycleStore + ?Sized>(
    store: &S,
    pending: &PendingLedger,
) -> Result<()> {
    let name = pending.record.name.as_str();
    let active = NameBinding {
        state: BindingState::Active,
        ..pending.binding.clone()
    };
    match cas_binding(store, name, Some(pending.version), Some(&active)).await {
        Ok(RegistryCas::Updated { .. }) => Ok(()),
        Ok(RegistryCas::Conflict { .. }) => Err(create_rolled_back(name)),
        // The write may have landed with its response lost: a caller that
        // rolls back on an error would delete a live ledger.
        Err(e) => match store.get_binding(name).await {
            Ok(Some(current)) if current.value == active => Ok(()),
            _ => Err(e),
        },
    }
}

/// The error a create reports when it finds its claim on `name` gone.
pub fn create_rolled_back(name: &str) -> NameServiceError {
    NameServiceError::conflict(format!(
        "the create of '{name}' was rolled back before it finished"
    ))
}

/// Undo a pending ledger: release its claim on the name, then delete its
/// records. Storage is not touched; the caller deletes what it wrote, and
/// only once this succeeds.
///
/// A claim that has moved on is not this create's to undo: the create may
/// have been activated after all, its write reported failed, and its
/// records and data are then a live ledger's. Nothing is deleted.
pub async fn abandon<S: LifecycleStore + ?Sized>(store: &S, pending: &PendingLedger) -> Result<()> {
    match cas_binding(store, &pending.record.name, Some(pending.version), None).await? {
        RegistryCas::Updated { .. } | RegistryCas::Conflict { actual: None } => {}
        RegistryCas::Conflict { .. } => {
            return Err(NameServiceError::conflict(format!(
                "the name binding for '{}' changed while rolling back its create",
                pending.record.name
            )))
        }
    }
    for b in &pending.binding.branches {
        let id = LedgerId::from_parts(&pending.record.name, &b.branch)?;
        store.delete_record(&id, b.fence).await?;
    }
    Ok(())
}

/// Renew a pending create's claim on its name, so the maintenance scan does
/// not take the create for abandoned: the binding is written back
/// unchanged, which moves its version. Returns `false` when the claim is
/// gone, the create having been rolled back.
pub async fn renew_claim<S: LifecycleStore + ?Sized>(
    store: &S,
    pending: &mut PendingLedger,
) -> Result<bool> {
    match cas_binding(
        store,
        &pending.record.name,
        Some(pending.version),
        Some(&pending.binding),
    )
    .await?
    {
        RegistryCas::Updated { version: Some(v) } => {
            pending.version = v;
            Ok(true)
        }
        RegistryCas::Updated { version: None } => Err(NameServiceError::storage(
            "binding write returned no version",
        )),
        RegistryCas::Conflict { .. } => Ok(false),
    }
}

/// Roll back a create that stopped: `name`'s binding, seen `Creating` at
/// `version`. The name is freed before the records are deleted, so a
/// creator that was only paused finds its claim gone and stops; a record
/// left by a crash in between is garbage no binding lists. Returns the
/// binding rolled back, whose root holds whatever the create wrote, or
/// `None` when the binding has moved on since it was seen.
pub async fn rollback_create<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &str,
    version: u64,
) -> Result<Option<NameBinding>> {
    let Some(current) = store.get_binding(name).await? else {
        return Ok(None);
    };
    if current.version != version || current.value.state != BindingState::Creating {
        return Ok(None);
    }
    if let RegistryCas::Conflict { .. } = cas_binding(store, name, Some(version), None).await? {
        return Ok(None);
    }
    for b in &current.value.branches {
        let id = LedgerId::from_parts(name, &b.branch)?;
        store.delete_record(&id, b.fence).await?;
    }
    Ok(Some(current.value))
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
///
/// The branch is listed as being created, naming its source, then its
/// record inserted, then the listing confirmed. Listing it in the same write
/// that checks the source is live means a source dropped since cannot gain
/// the child, and a source keeps its data while any listing names it. A
/// create that stops before confirming reads as absent; creating the branch
/// again takes it over, and the maintenance scan rolls it back
/// ([`rollback_branch_create`]).
pub async fn create_branch<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &LedgerName,
    new_branch: &str,
    source_branch: &str,
    at_commit: Option<(ContentId, i64)>,
) -> Result<NsRecord> {
    let new_id = name.with_branch(new_branch)?;
    let source_id = name.with_branch(source_branch)?;

    let mut listed = None;
    for _ in 0..MAX_ATTEMPTS {
        let Versioned {
            value: binding,
            version,
        } = active_binding(store, name).await?;
        let source_fence = binding
            .listing(source_branch)
            .filter(|_| binding.shows(source_branch))
            .ok_or_else(|| NameServiceError::not_found(source_id.to_string()))?
            .fence;
        let source = store
            .raw_record(&source_id)
            .await?
            .filter(|r| r.fence == Some(source_fence))
            .ok_or_else(|| NameServiceError::not_found(source_id.to_string()))?;

        // A confirmed listing whose record is missing is a create from before
        // creates were confirmed that did not finish; one being created is a
        // create under way or stopped. Either is replaced with a fresh fence,
        // which turns any record it later inserts into garbage.
        if let Some(existing) = binding.listing(new_branch).filter(|l| !l.creating) {
            let has_record = store
                .raw_record(&new_id)
                .await?
                .is_some_and(|r| r.fence == Some(existing.fence));
            if has_record {
                return Err(NameServiceError::ledger_already_exists(new_id.to_string()));
            }
        }

        let listing = BranchFence {
            source: Some(source_branch.to_string()),
            creating: true,
            ..BranchFence::new(new_branch, Fence::generate())
        };
        let mut next = binding.clone();
        next.branches.retain(|b| b.branch != new_branch);
        next.branches.push(listing.clone());
        if let RegistryCas::Updated { .. } =
            cas_binding(store, name, Some(version), Some(&next)).await?
        {
            listed = Some((listing.fence, source, binding.root));
            break;
        }
    }
    let (fence, source, root) = listed.ok_or_else(|| contended(name))?;

    let mut record = NsRecord::new(new_id.clone());
    record.fence = Some(fence);
    record.source_branch = Some(source_branch.to_string());
    (record.commit_head_id, record.commit_t) = match at_commit {
        Some((id, t)) => (Some(id), t),
        None => (source.commit_head_id.clone(), source.commit_t),
    };
    let confirmed = match insert_fenced(store, &record).await {
        Ok(()) => confirm_branch(store, name, new_branch, fence).await,
        Err(e) => Err(e),
    };
    if let Err(e) = confirmed {
        if let Err(undo) = rollback_branch_create(store, name, new_branch, fence).await {
            tracing::warn!(ledger_id = %new_id, error = %undo, "could not roll back a failed branch create");
        }
        return Err(e);
    }
    record.storage_root = Some(root);
    Ok(record)
}

/// Confirm the listing a create of `branch` under `fence` made, once its
/// record is in. Fails when the listing is gone or replaced: the create was
/// rolled back or taken over, and its record is garbage.
async fn confirm_branch<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &LedgerName,
    branch: &str,
    fence: Fence,
) -> Result<()> {
    for _ in 0..MAX_ATTEMPTS {
        let current = store.get_binding(name).await?;
        let Some(Versioned {
            value: binding,
            version,
        }) = current.filter(|v| v.value.is_active() && v.value.fence_of(branch) == Some(fence))
        else {
            return Err(NameServiceError::conflict(format!(
                "the create of '{name}:{branch}' was rolled back or taken over before it finished"
            )));
        };
        let mut next = binding;
        for b in &mut next.branches {
            if b.branch == branch {
                if !b.creating {
                    return Ok(());
                }
                b.creating = false;
            }
        }
        if let RegistryCas::Updated { .. } =
            cas_binding(store, name, Some(version), Some(&next)).await?
        {
            return Ok(());
        }
    }
    Err(contended(name))
}

/// Roll back a create of `branch` under `fence` that did not confirm its
/// listing: unlist the branch, then delete its record. Unlisting first means
/// a creator that was only paused fails to confirm and stops. A record left
/// under a fence no longer listed is deleted too: it is garbage. Returns
/// `false`, changing nothing, when the create was confirmed.
pub async fn rollback_branch_create<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &LedgerName,
    branch: &str,
    fence: Fence,
) -> Result<bool> {
    let id = name.with_branch(branch)?;
    for _ in 0..MAX_ATTEMPTS {
        let current = store.get_binding(name).await?;
        let creating = match current
            .as_ref()
            .and_then(|v| v.value.listing(branch))
            .filter(|l| l.fence == fence)
        {
            Some(l) if !l.creating => return Ok(false),
            listed => listed.is_some(),
        };
        // Only an active ledger's listings change here; a drop copies no
        // branch still being created, and deletes the record itself.
        if let Some(Versioned { value, version }) =
            current.filter(|v| creating && v.value.is_active())
        {
            let mut next = value;
            next.branches.retain(|b| b.branch != branch);
            if let RegistryCas::Conflict { .. } =
                cas_binding(store, name, Some(version), Some(&next)).await?
            {
                continue;
            }
        }
        store.delete_record(id.as_ref(), fence).await?;
        return Ok(true);
    }
    Err(contended(name))
}

/// Copy `record`, read from the nameservice that owns its ledger, into
/// `store`: bind its name to the record's instance and root, list its
/// branch, and insert it. A root at the name itself, from before name
/// bindings, takes the instance the migration derives from the name
/// ([`legacy_instance`]). A record with no root comes from a nameservice
/// from before name bindings, which keeps every ledger at its name; a later
/// one always sends the root. A binding to another instance is replaced: the
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
    let name = record.ledger_id.ledger_name();
    let root = record
        .storage_root
        .clone()
        .unwrap_or_else(|| StorageRoot::legacy(&name));
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
                    extra: crate::binding::ExtraFields::new(),
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
            ..BranchFence::for_record(record, branch_fence)
        });
        if let RegistryCas::Updated { .. } =
            cas_binding(store, &name, expected, Some(&next)).await?
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
/// A branch with children is kept, dropped and frozen, as the owner keeps
/// it: their history reaches into its data, which they find through its
/// record. The owner announces the drop again when it deletes the branch
/// after its last child, and then the copy goes.
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
        if value.children_of(branch).next().is_some() {
            if !value.listing(branch).is_some_and(|l| l.dropped) {
                let mut next = value;
                for b in &mut next.branches {
                    b.dropped |= b.branch == branch;
                }
                if !matches!(
                    cas_binding(store, &name, Some(version), Some(&next)).await?,
                    RegistryCas::Updated { .. }
                ) {
                    continue;
                }
            }
            store.freeze_record(ledger_id.as_ref(), fence).await?;
            return Ok(());
        }
        let mut next = value;
        next.branches.retain(|b| b.branch != branch);
        let new = (!next.branches.is_empty()).then_some(&next);
        if let RegistryCas::Updated { .. } = cas_binding(store, &name, Some(version), new).await? {
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
        let listing = binding
            .listing(branch)
            .filter(|l| !l.creating)
            .ok_or_else(not_found)?
            .clone();
        if resume.is_some_and(|fence| !listing.dropped || listing.fence != fence) {
            return Ok(None);
        }
        if listing.dropped {
            marked = Some((binding, listing.fence, true));
            break;
        }
        let mut next = binding;
        for b in &mut next.branches {
            b.dropped |= b.branch == branch;
        }
        if let RegistryCas::Updated { .. } =
            cas_binding(store, name, Some(version), Some(&next)).await?
        {
            marked = Some((next, listing.fence, false));
            break;
        }
    }
    // Once the branch is marked dropped no create lists a child of it, so
    // the children this binding lists can only go.
    let (binding, fence, already) = marked.ok_or_else(|| contended(name))?;

    if store.freeze_record(&id, fence).await? != FenceOutcome::Applied {
        return Err(not_found());
    }
    if binding.children_of(branch).next().is_some() {
        return Ok(Some(BranchDrop::Deferred { already }));
    }
    let mut record = store
        .raw_record(&id)
        .await?
        .filter(|r| r.fence == Some(fence))
        .ok_or_else(not_found)?;
    record.storage_root = Some(binding.root);
    Ok(Some(BranchDrop::Purge(Box::new(record))))
}

/// Renew a drop of `branch` under `fence` while it deletes the branch's
/// storage, so the maintenance scan does not take it for stopped: the
/// binding is written back unchanged, which moves its version. Returns
/// `false` when the drop is no longer this one's to finish: a drop finished
/// elsewhere unlisted the branch, and it may be listed again under another
/// fence.
pub async fn renew_branch_drop<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &LedgerName,
    branch: &str,
    fence: Fence,
) -> Result<bool> {
    for _ in 0..MAX_ATTEMPTS {
        let Some(Versioned { value, version }) = store.get_binding(name).await? else {
            return Ok(false);
        };
        if !dropping_under(&value, branch, fence) {
            return Ok(false);
        }
        if let RegistryCas::Updated { .. } =
            cas_binding(store, name, Some(version), Some(&value)).await?
        {
            return Ok(true);
        }
    }
    // Other writers kept moving the binding, which is all a renewal is for.
    Ok(true)
}

/// Whether a drop of `branch` under `fence` is still the one under way: the
/// binding lists the branch as dropped under that fence. A drop that finds
/// it is not was finished elsewhere, and must delete nothing more: the
/// branch's folder may already hold a branch created under its name since.
pub async fn branch_drop_under_way<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &LedgerName,
    branch: &str,
    fence: Fence,
) -> Result<bool> {
    Ok(store
        .get_binding(name)
        .await?
        .is_some_and(|b| dropping_under(&b.value, branch, fence)))
}

fn dropping_under(binding: &NameBinding, branch: &str, fence: Fence) -> bool {
    binding
        .listing(branch)
        .is_some_and(|l| l.dropped && l.fence == fence)
}

/// Forget a dropped branch once its storage is gone: take it out of the
/// binding, then delete its record.
///
/// Returns the parent's record when the parent is itself a dropped branch
/// that no listing names as its source any more, for the caller to purge in
/// turn.
pub async fn finish_drop_branch<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &LedgerName,
    record: &NsRecord,
) -> Result<Option<NsRecord>> {
    let fence = record.fence.ok_or_else(|| {
        NameServiceError::storage(format!("{} carries no fence", record.ledger_id))
    })?;
    let mut unlisted = None;
    for _ in 0..MAX_ATTEMPTS {
        let Versioned {
            value: binding,
            version,
        } = active_binding(store, name).await?;
        // An earlier attempt got as far as unlisting it.
        if binding.fence_of(&record.branch) != Some(fence) {
            unlisted = Some(binding);
            break;
        }
        let mut next = binding;
        next.branches.retain(|b| b.branch != record.branch);
        if let RegistryCas::Updated { .. } =
            cas_binding(store, name, Some(version), Some(&next)).await?
        {
            unlisted = Some(next);
            break;
        }
    }
    let binding = unlisted.ok_or_else(|| contended(name))?;
    store.delete_record(&record.ledger_id, fence).await?;

    // A dropped parent gains no children, so once none is listed it stays so.
    let Some(parent) = record.source_branch.as_deref() else {
        return Ok(None);
    };
    let Some(parent_fence) = binding
        .listing(parent)
        .filter(|b| b.dropped)
        .map(|b| b.fence)
    else {
        return Ok(None);
    };
    if binding.children_of(parent).next().is_some() {
        return Ok(None);
    }
    Ok(store
        .raw_record(&name.with_branch(parent)?)
        .await?
        .filter(|r| r.fence == Some(parent_fence))
        .map(|mut r| {
            r.storage_root = Some(binding.root);
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

/// Refuse `entry` when `store` cannot hold it, before anything is written.
fn check_entry_fits<S: LifecycleStore + ?Sized>(store: &S, entry: &DroppedLedger) -> Result<()> {
    let Some(max) = store.max_dropped_entry_bytes() else {
        return Ok(());
    };
    let bytes = serde_json::to_vec(entry)?.len();
    if bytes <= max {
        return Ok(());
    }
    Err(NameServiceError::TooLarge(format!(
        "'{}' has {} branches, and recording it as a dropped ledger takes {bytes} bytes, \
         more than the {max} this nameservice holds in one entry; drop some of its \
         branches first",
        entry.name,
        entry.branches.len()
    )))
}

/// The entry a drop of `binding`'s ledger records, holding `records`.
fn dropped_entry(
    name: &LedgerName,
    binding: &NameBinding,
    hard: bool,
    records: Vec<NsRecord>,
) -> DroppedLedger {
    DroppedLedger {
        extra: crate::binding::ExtraFields::new(),
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
    }
}

/// The records a drop of `binding`'s ledger copies into its entry: every
/// listed branch's but a branch still being created.
async fn records_to_copy<S: LifecycleStore + ?Sized>(
    store: &S,
    name: &LedgerName,
    binding: &NameBinding,
) -> Result<Vec<NsRecord>> {
    let mut records = Vec::with_capacity(binding.branches.len());
    for b in binding.branches.iter().filter(|b| !b.creating) {
        let id = name.with_branch(&b.branch)?;
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
    Ok(records)
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
                    return Err(NameServiceError::conflict(format!(
                        "'{name}' is being restored; retry the drop once it finishes"
                    )))
                }
                // A create under way owns the name until it activates or
                // is rolled back; it has nothing a drop could keep.
                BindingState::Creating => {
                    return Err(NameServiceError::conflict(format!(
                        "'{name}' is being created; roll the create back instead"
                    )))
                }
                BindingState::Active => {
                    let Some(hard) = start else {
                        return Ok(None);
                    };
                    if store.max_dropped_entry_bytes().is_some() {
                        let records = records_to_copy(store, name, &value).await?;
                        check_entry_fits(store, &dropped_entry(name, &value, hard, records))?;
                    }
                    let dropping = NameBinding {
                        state: BindingState::Dropping { hard },
                        ..value
                    };
                    if let RegistryCas::Updated { version: Some(v) } =
                        cas_binding(store, name, Some(version), Some(&dropping)).await?
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

    // 2. Freeze every listed branch, and copy its record. A branch still
    // being created is not copied: its creator was not told it succeeded,
    // and fails to confirm it. Step 4 deletes its record.
    for b in binding.branches.iter().filter(|b| !b.creating) {
        store
            .freeze_record(&name.with_branch(&b.branch)?, b.fence)
            .await?;
    }
    let records = records_to_copy(store, name, &binding).await?;

    // 3. Record the dropped ledger. A dropped or purging entry already there
    // is this drop's own earlier attempt, written before any record was
    // deleted. A restoring one is a restore that made the ledger visible
    // and stopped before forgetting its entry: its copies predate the
    // restore, and resuming it would bring the ledger back, so it is
    // replaced.
    let entry = dropped_entry(name, &binding, hard, records);
    let mut expected = None;
    let entry = loop {
        match cas_dropped(store, &binding.instance, expected, Some(&entry)).await? {
            RegistryCas::Updated { .. } => break entry,
            RegistryCas::Conflict {
                actual: Some(existing),
            } => match existing.value.state {
                DroppedState::Dropped | DroppedState::Purging => break existing.value,
                DroppedState::Restoring { .. } if expected.is_none() => {
                    expected = Some(existing.version);
                }
                DroppedState::Restoring { .. } => return Err(contended(name)),
            },
            RegistryCas::Conflict { actual: None } => return Err(contended(name)),
        }
    };

    // 4. Delete the branch records.
    for b in &binding.branches {
        store
            .delete_record(&name.with_branch(&b.branch)?, b.fence)
            .await?;
    }

    // 5. Free the name.
    match cas_binding(store, name, Some(version), None).await? {
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
                    ..BranchFence::for_record(r, Fence::generate())
                })
                .collect();
            let restoring = DroppedLedger {
                state: DroppedState::Restoring {
                    fences: fences.clone(),
                },
                ..entry.clone()
            };
            check_entry_fits(store, &restoring)?;
            match cas_dropped(store, instance, Some(version), Some(&restoring)).await? {
                RegistryCas::Updated { version: Some(v) } => (fences, v),
                _ => return Err(contended(&entry.name)),
            }
        }
        DroppedState::Restoring { fences } => (fences.clone(), version),
        DroppedState::Purging => {
            return Err(NameServiceError::conflict(format!(
                "dropped ledger {instance} is being purged"
            )))
        }
    };

    // 2. Claim the name.
    let binding = NameBinding {
        extra: crate::binding::ExtraFields::new(),
        instance: instance.clone(),
        root: entry.root.clone(),
        root_branch: entry.root_branch.clone(),
        state: BindingState::Restoring,
        branches: fences.clone(),
    };
    let binding_version = match cas_binding(store, &entry.name, None, Some(&binding)).await? {
        RegistryCas::Updated { version: Some(v) } => v,
        RegistryCas::Conflict {
            actual: Some(existing),
        } if existing.value.instance == *instance => match existing.value.state {
            // This restore's own claim, from an interrupted attempt.
            BindingState::Restoring => existing.version,
            // An interrupted attempt already made it visible.
            BindingState::Active => {
                cas_dropped(store, instance, Some(version), None).await?;
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
            cas_dropped(store, instance, Some(version), Some(&dropped)).await?;
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
        match insert_fenced(store, &record).await {
            Ok(()) => {}
            // A record from before name bindings holds the key, and is never
            // deleted to make room: no attempt can finish this restore, and
            // left half done it would hold the name with nothing able to
            // drop, purge or finish it. It is undone instead.
            Err(NameServiceError::LedgerAlreadyExists(held)) => {
                undo_restore(store, &entry, version, binding_version, &records).await?;
                return Err(NameServiceError::ledger_already_exists(format!(
                    "{held}; the restore was undone, and the dropped ledger can be restored \
                     once that record is dropped or bound"
                )));
            }
            Err(e) => return Err(e),
        }
        record.storage_root = Some(entry.root.clone());
        records.push(record);
    }

    // 4. Make it visible, then forget the entry.
    let active = NameBinding {
        state: BindingState::Active,
        ..binding
    };
    match cas_binding(store, &entry.name, Some(binding_version), Some(&active)).await? {
        RegistryCas::Updated { .. } => {}
        RegistryCas::Conflict { .. } => return Err(contended(&entry.name)),
    }
    cas_dropped(store, instance, Some(version), None).await?;
    Ok(Some(records))
}

/// Undo a restore that cannot finish: free the name, delete the records it
/// reinstated, and return its entry, at `entry_version`, to `Dropped`.
async fn undo_restore<S: LifecycleStore + ?Sized>(
    store: &S,
    entry: &DroppedLedger,
    entry_version: u64,
    binding_version: u64,
    reinstated: &[NsRecord],
) -> Result<()> {
    match cas_binding(store, &entry.name, Some(binding_version), None).await? {
        RegistryCas::Updated { .. } | RegistryCas::Conflict { actual: None } => {}
        RegistryCas::Conflict { .. } => return Err(contended(&entry.name)),
    }
    for record in reinstated {
        if let Some(fence) = record.fence {
            store.delete_record(&record.ledger_id, fence).await?;
        }
    }
    let dropped = DroppedLedger {
        state: DroppedState::Dropped,
        ..entry.clone()
    };
    match cas_dropped(store, &entry.instance, Some(entry_version), Some(&dropped)).await? {
        RegistryCas::Updated { .. } => Ok(()),
        RegistryCas::Conflict { .. } => Err(contended(&entry.name)),
    }
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
        DroppedState::Restoring { .. } => Err(NameServiceError::conflict(format!(
            "dropped ledger {instance} is being restored"
        ))),
        DroppedState::Dropped => {
            let purging = DroppedLedger {
                state: DroppedState::Purging,
                ..entry
            };
            match cas_dropped(store, instance, Some(version), Some(&purging)).await? {
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
        return Err(NameServiceError::conflict(format!(
            "dropped ledger {instance} is not being purged"
        )));
    }
    cas_dropped(store, instance, Some(version), None).await?;
    Ok(())
}

/// What [`migrate_legacy`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MigrationReport {
    /// Names bound to the ledgers they held.
    pub bound: Vec<String>,
    /// Names whose soft-dropped ledgers moved to the dropped-ledger registry.
    pub dropped: Vec<String>,
    /// Ledgers left unbound, as stored, because their ids are not
    /// [`migratable`]. Nothing can resolve them.
    pub quarantined: Vec<String>,
}

impl MigrationReport {
    pub fn is_empty(&self) -> bool {
        self.bound.is_empty() && self.dropped.is_empty() && self.quarantined.is_empty()
    }
}

/// Whether the migration can bind a ledger an older release stored as
/// `name:branch`. Releases before the ledger-id grammar accepted `@` and
/// `#`, which bindings, storage roots and every request refuse.
pub fn migratable(name: &str, branch: &str) -> bool {
    LedgerId::from_parts(name, branch).is_ok()
}

/// The ledgers in `store` from before name bindings that the migration
/// cannot bind ([`migratable`]), as stored.
pub async fn unmigratable_ids<S: LifecycleStore + ?Sized>(store: &S) -> Result<Vec<String>> {
    Ok(store
        .all_raw_records()
        .await?
        .into_iter()
        .filter(|r| r.fence.is_none() && !migratable(&r.name, &r.branch))
        .map(|r| r.ledger_id.to_string())
        .collect())
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
    let mut report = MigrationReport::default();
    for record in store.all_raw_records().await? {
        let legacy = legacy_fence(record.ledger_id.as_ref());
        if !record.fence.is_none_or(|f| f == legacy) {
            continue;
        }
        // Skipped, not fatal: one such ledger must not keep every other
        // from being bound. The file and object-store migrations refuse the
        // store before writing anything instead.
        if !migratable(&record.name, &record.branch) {
            report.quarantined.push(record.ledger_id.to_string());
            continue;
        }
        by_name.entry(record.name.clone()).or_default().push(record);
    }

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
        if records.iter().any(|r| !legacy_dropped(r)) && (bound.is_none() || binding_is_legacy) {
            bind_legacy(store, &ledger_name, &records).await?;
            report.bound.push(name);
        } else {
            register_legacy_drop(store, &ledger_name, records).await?;
            report.dropped.push(name);
        }
    }
    Ok(report)
}

/// Whether a record the migration finds was dropped: retracted by a release
/// from before name bindings, or frozen by a drop on this one, which no
/// earlier release does. A migration run again finds the second kind when a
/// branch drop stopped between unlisting the branch and deleting its record.
fn legacy_dropped(record: &NsRecord) -> bool {
    record.retracted || record.frozen
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
            dropped: legacy_dropped(record),
            ..BranchFence::for_record(record, fence)
        });
    }
    branches.sort_by(|a, b| a.branch.cmp(&b.branch));

    let binding = NameBinding {
        extra: crate::binding::ExtraFields::new(),
        instance: legacy_instance(name),
        root: StorageRoot::legacy(name),
        root_branch: legacy_root_branch(records),
        state: BindingState::Active,
        branches,
    };
    match cas_binding(store, name, None, Some(&binding)).await? {
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
            match cas_binding(store, name, Some(existing.version), Some(&merged)).await? {
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
        extra: crate::binding::ExtraFields::new(),
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
    match cas_dropped(store, &instance, None, Some(&entry)).await? {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::{DroppedLedger, FenceOutcome, LedgerRegistry};
    use crate::memory::MemoryNameService;
    use async_trait::async_trait;

    /// A memory store that can report an error for the binding writes that
    /// make a ledger active, applying them anyway, as a store whose response
    /// was lost would; and can hold dropped-ledger entries of at most
    /// `max_entry` bytes (0 for no limit).
    #[derive(Debug, Default)]
    struct TestStore {
        inner: MemoryNameService,
        lose_activation: bool,
        max_entry: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl LedgerRegistry for TestStore {
        async fn get_binding(&self, name: &str) -> Result<Option<Versioned<NameBinding>>> {
            self.inner.get_binding(name).await
        }

        async fn cas_binding(
            &self,
            name: &str,
            expected: Option<u64>,
            new: Option<&NameBinding>,
        ) -> Result<RegistryCas<NameBinding>> {
            let outcome = self.inner.cas_binding(name, expected, new).await?;
            if self.lose_activation && new.is_some_and(NameBinding::is_active) {
                return Err(NameServiceError::storage("the response was lost"));
            }
            Ok(outcome)
        }

        async fn list_bindings(&self) -> Result<Vec<(String, Versioned<NameBinding>)>> {
            self.inner.list_bindings().await
        }

        async fn get_dropped(
            &self,
            instance: &InstanceId,
        ) -> Result<Option<Versioned<DroppedLedger>>> {
            self.inner.get_dropped(instance).await
        }

        async fn cas_dropped(
            &self,
            instance: &InstanceId,
            expected: Option<u64>,
            new: Option<&DroppedLedger>,
        ) -> Result<RegistryCas<DroppedLedger>> {
            self.inner.cas_dropped(instance, expected, new).await
        }

        async fn list_dropped(&self) -> Result<Vec<Versioned<DroppedLedger>>> {
            self.inner.list_dropped().await
        }

        fn max_dropped_entry_bytes(&self) -> Option<usize> {
            match self.max_entry.load(std::sync::atomic::Ordering::SeqCst) {
                0 => None,
                max => Some(max),
            }
        }
    }

    #[async_trait]
    impl BranchRecordStore for TestStore {
        async fn raw_record(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
            self.inner.raw_record(ledger_id).await
        }

        async fn all_raw_records(&self) -> Result<Vec<NsRecord>> {
            self.inner.all_raw_records().await
        }

        async fn insert_record(&self, record: &NsRecord) -> Result<Option<NsRecord>> {
            self.inner.insert_record(record).await
        }

        async fn adopt_record(&self, ledger_id: &str, fence: Fence) -> Result<FenceOutcome> {
            self.inner.adopt_record(ledger_id, fence).await
        }

        async fn freeze_record(&self, ledger_id: &str, fence: Fence) -> Result<FenceOutcome> {
            self.inner.freeze_record(ledger_id, fence).await
        }

        async fn delete_record(&self, ledger_id: &str, fence: Fence) -> Result<FenceOutcome> {
            self.inner.delete_record(ledger_id, fence).await
        }
    }

    /// A write retried after it landed finds its own value at the version it
    /// took, and counts as written rather than as a conflict.
    #[tokio::test]
    async fn a_write_that_already_landed_counts_as_written() {
        let store = MemoryNameService::new();
        let pending = begin_create(&store, &LedgerId::parse("mydb:main").unwrap())
            .await
            .unwrap();
        let active = NameBinding {
            state: BindingState::Active,
            ..pending.binding.clone()
        };
        let expected = Some(pending.version);
        store
            .cas_binding("mydb", expected, Some(&active))
            .await
            .unwrap();
        assert!(matches!(
            cas_binding(&store, "mydb", expected, Some(&active)).await.unwrap(),
            RegistryCas::Updated { version: Some(v) } if Some(v) == expected.map(|v| v + 1)
        ));

        let other = NameBinding {
            root_branch: "other".into(),
            ..active
        };
        assert!(matches!(
            cas_binding(&store, "mydb", expected, Some(&other))
                .await
                .unwrap(),
            RegistryCas::Conflict { .. }
        ));
    }

    /// An activation that landed with its response lost is an activation:
    /// a caller that rolled back on its error would delete a live ledger.
    /// And a rollback of a create its claim has moved on from, as after
    /// such an activation, deletes nothing.
    #[tokio::test]
    async fn an_activation_reported_failed_is_kept_when_it_landed() {
        let store = TestStore {
            lose_activation: true,
            ..TestStore::default()
        };
        let pending = begin_create(&store, &LedgerId::parse("mydb:main").unwrap())
            .await
            .unwrap();
        activate(&store, &pending)
            .await
            .expect("the activation landed");

        assert!(abandon(&store, &pending).await.is_err());
        assert!(store
            .get_binding("mydb")
            .await
            .unwrap()
            .unwrap()
            .value
            .is_active());
        assert_eq!(
            store.raw_record("mydb:main").await.unwrap().unwrap().fence,
            pending.record.fence
        );
    }

    /// A ledger the migration cannot bind is skipped and reported, and every
    /// other is bound, including those after it in name order.
    #[tokio::test]
    async fn migration_skips_ledgers_it_cannot_bind() {
        let store = MemoryNameService::new();
        for (name, branch) in [
            ("alpha", "main"),
            ("mydb", "feature#1"),
            ("mydb", "main"),
            ("rep@2026", "main"),
            ("zeta", "main"),
        ] {
            let id = LedgerId::from_persisted_parts(name, branch).unwrap();
            assert!(store
                .insert_record(&NsRecord::new(id))
                .await
                .unwrap()
                .is_none());
        }
        let mut refused = unmigratable_ids(&store).await.unwrap();
        refused.sort();
        assert_eq!(refused, ["mydb:feature#1", "rep@2026:main"]);

        let mut report = migrate_legacy(&store).await.unwrap();
        report.quarantined.sort();
        assert_eq!(report.bound, ["alpha", "mydb", "zeta"]);
        assert_eq!(report.quarantined, refused);
        let mydb = store.get_binding("mydb").await.unwrap().unwrap().value;
        assert!(mydb.listing("main").is_some() && mydb.listing("feature#1").is_none());
    }

    /// A drop or restore whose dropped-ledger entry the store cannot hold is
    /// refused before it writes anything: the ledger stays live, or dropped.
    #[tokio::test]
    async fn an_entry_too_large_for_the_store_is_refused_before_any_write() {
        use std::sync::atomic::Ordering;
        let store = TestStore::default();
        let name = LedgerName::parse("mydb").unwrap();
        create_ledger(&store, &LedgerId::parse("mydb:main").unwrap())
            .await
            .unwrap();
        create_branch(&store, &name, "dev", "main", None)
            .await
            .unwrap();

        store.max_entry.store(64, Ordering::SeqCst);
        let err = drop_ledger(&store, &name, false).await.unwrap_err();
        assert!(matches!(err, NameServiceError::TooLarge(_)), "{err}");
        let binding = store.get_binding("mydb").await.unwrap().unwrap().value;
        assert!(binding.is_active());
        for branch in ["mydb:main", "mydb:dev"] {
            assert!(!store.raw_record(branch).await.unwrap().unwrap().frozen);
        }

        store.max_entry.store(0, Ordering::SeqCst);
        let instance = drop_ledger(&store, &name, false)
            .await
            .unwrap()
            .expect("dropped")
            .instance;
        let dropped = store.get_dropped(&instance).await.unwrap().unwrap().value;
        // Room for the dropped entry, not for the restoring one with fences.
        let fits = serde_json::to_vec(&dropped).unwrap().len();
        store.max_entry.store(fits + 8, Ordering::SeqCst);
        let err = restore_dropped(&store, &instance).await.unwrap_err();
        assert!(matches!(err, NameServiceError::TooLarge(_)), "{err}");
        let entry = store.get_dropped(&instance).await.unwrap().unwrap().value;
        assert_eq!(entry.state, DroppedState::Dropped);
        assert!(store.get_binding("mydb").await.unwrap().is_none());
    }
}
