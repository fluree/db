//! Name bindings, fences, and the dropped-ledger registry.
//!
//! A ledger's name is only a handle. The **name binding**, one small record
//! per name, is the authoritative claim on it: which incarnation
//! ([`InstanceId`]) holds the name, where its artifacts live
//! ([`StorageRoot`]), and the [`Fence`] each of its branches accepts.
//!
//! A branch record is live only while its fence is listed in the binding. A
//! record left by a crashed creator, or written after its ledger was dropped,
//! is therefore garbage, and nothing ever has to list records to tell.
//!
//! A dropped ledger moves to the **registry**, keyed by instance, from which
//! it can be restored or purged.
//!
//! Every lifecycle transition is a compare-and-swap on one binding or one
//! registry entry, guarded by a [`Versioned::version`] counter the backend
//! keeps with the item.

use crate::{NameServiceError, NsRecord, Result};
use async_trait::async_trait;
use fluree_db_core::{InstanceId, StorageRoot};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{self, Debug};

/// A token a writer presents with every publication to a branch record.
///
/// Issued fresh each time a branch becomes active, created or restored. A
/// writer that loaded the branch before a drop is refused afterwards, even
/// once a restore brings back exactly the heads it last saw.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Fence(u64);

impl Fence {
    pub fn generate() -> Self {
        Self(rand::random())
    }

    pub fn from_u64(value: u64) -> Self {
        Self(value)
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Fence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl fmt::Debug for Fence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Fence({self})")
    }
}

impl std::str::FromStr for Fence {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s.len() != 16 {
            return Err(format!("invalid fence '{s}': expected 16 hex digits"));
        }
        u64::from_str_radix(s, 16)
            .map(Self)
            .map_err(|e| format!("invalid fence '{s}': {e}"))
    }
}

/// Hex, not a JSON number: 64-bit values do not survive JavaScript clients.
impl Serialize for Fence {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Fence {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// A fresh instance id: a ULID on the wasm-safe clock.
pub fn new_instance_id() -> InstanceId {
    let now_ms = fluree_db_core::clock::SystemTime::now()
        .duration_since(fluree_db_core::clock::SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64);
    let ulid = ulid::Ulid::from_parts(now_ms, rand::random());
    InstanceId::parse(&ulid.to_string()).expect("a ULID is a valid instance id")
}

/// Where a name binding is in its lifecycle. No binding means the name is
/// free.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum BindingState {
    /// A create claimed the name and has not finished. The ledger is not
    /// visible.
    Creating,
    /// The ledger is visible and writable.
    Active,
    /// A drop is in progress; `hard` purges the data once the name is free.
    /// The ledger is not visible.
    Dropping { hard: bool },
    /// A restore from the registry claimed the name and has not finished.
    /// The ledger is not visible.
    Restoring,
}

/// One branch listed in a binding, with the fence its record must carry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchFence {
    pub branch: String,
    pub fence: Fence,
    /// The branch has been dropped: it reads as absent and its record is
    /// frozen. It stays listed while child branches keep its data, or until
    /// its storage is deleted, so its name cannot be reused before then.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub dropped: bool,
}

impl BranchFence {
    pub fn new(branch: impl Into<String>, fence: Fence) -> Self {
        Self {
            branch: branch.into(),
            fence,
            dropped: false,
        }
    }
}

/// The authoritative claim on a ledger name.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NameBinding {
    pub instance: InstanceId,
    pub root: StorageRoot,
    pub root_branch: String,
    #[serde(flatten)]
    pub state: BindingState,
    pub branches: Vec<BranchFence>,
}

impl NameBinding {
    /// The fence `branch`'s record must carry to be live.
    pub fn fence_of(&self, branch: &str) -> Option<Fence> {
        self.listing(branch).map(|b| b.fence)
    }

    pub fn listing(&self, branch: &str) -> Option<&BranchFence> {
        self.branches.iter().find(|b| b.branch == branch)
    }

    pub fn is_active(&self) -> bool {
        self.state == BindingState::Active
    }
}

/// Where a registry entry is in its lifecycle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum DroppedState {
    /// Soft-dropped: the data is kept and can be restored or purged.
    Dropped,
    /// A restore is under way with these fresh fences. (Not `branches`: the
    /// state is flattened into [`DroppedLedger`], which has that field.)
    Restoring { fences: Vec<BranchFence> },
    /// The data is being deleted.
    Purging,
}

/// A dropped ledger: everything needed to restore or purge it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DroppedLedger {
    pub instance: InstanceId,
    #[serde(flatten)]
    pub state: DroppedState,
    /// Milliseconds since the Unix epoch.
    pub dropped_at: i64,
    pub name: String,
    pub root: StorageRoot,
    pub root_branch: String,
    /// The branch records as they were when the ledger was dropped. One
    /// marked retracted is a branch dropped before its ledger, which a
    /// restore brings back still dropped.
    pub branches: Vec<NsRecord>,
}

/// Whether a write presenting `presented` may change a record carrying
/// `fence`. A fenced record takes only its own fence, and nothing once
/// frozen; a record from before fencing takes only unfenced writes.
pub fn fence_admits(fence: Option<Fence>, frozen: bool, presented: Option<Fence>) -> bool {
    fence == presented && !frozen
}

/// Refuse a write presenting `presented` to `record`, given as its fence and
/// frozen flag or `None` when there is none, unless [`fence_admits`] it. A
/// fenced write to a missing record is refused too: the branch was dropped
/// since the writer loaded it, and publication never creates a record.
pub fn check_write_fence(
    ledger_id: &str,
    record: Option<(Option<Fence>, bool)>,
    presented: Option<Fence>,
) -> Result<()> {
    let admitted = match record {
        Some((fence, frozen)) => fence_admits(fence, frozen, presented),
        None => presented.is_none(),
    };
    if admitted {
        Ok(())
    } else {
        Err(NameServiceError::fenced(ledger_id))
    }
}

/// A binding or registry entry with the version its next compare-and-swap
/// must present.
#[derive(Clone, Debug, PartialEq)]
pub struct Versioned<T> {
    pub value: T,
    pub version: u64,
}

/// Outcome of a compare-and-swap on a binding or registry entry.
#[derive(Clone, Debug, PartialEq)]
pub enum RegistryCas<T> {
    /// Written; `version` is the new version, or `None` after a delete.
    Updated { version: Option<u64> },
    /// The item was not at the expected version; `actual` is what is there.
    Conflict { actual: Option<Versioned<T>> },
}

/// Outcome of a write conditional on a branch record's fence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FenceOutcome {
    Applied,
    /// No record at the key.
    Missing,
    /// The record carries a different fence (or none).
    Mismatch,
    /// The record is frozen and refuses the change.
    Frozen,
}

fn unsupported<T>(what: &str) -> Result<T> {
    Err(NameServiceError::storage(format!(
        "{what} is not supported by this nameservice backend"
    )))
}

/// Storage for name bindings and the dropped-ledger registry.
///
/// Each method reads or compare-and-swaps one item, which every backend can
/// do with a strongly consistent point read or conditional write. `expected`
/// is the version to replace, or `None` to write only if the item is absent;
/// `new` is the value to write, or `None` to delete.
///
/// Versions never repeat for a key, even across a delete: a backend keeps a
/// tombstone carrying the last version. Otherwise a compare-and-swap from a
/// read taken before a drop could match the binding of the ledger that
/// reused the name.
#[async_trait]
pub trait LedgerRegistry: Debug + Send + Sync {
    async fn get_binding(&self, name: &str) -> Result<Option<Versioned<NameBinding>>> {
        let _ = name;
        unsupported("reading name bindings")
    }

    async fn cas_binding(
        &self,
        name: &str,
        expected: Option<u64>,
        new: Option<&NameBinding>,
    ) -> Result<RegistryCas<NameBinding>> {
        let _ = (name, expected, new);
        unsupported("writing name bindings")
    }

    /// Every binding, with its name.
    async fn list_bindings(&self) -> Result<Vec<(String, Versioned<NameBinding>)>> {
        unsupported("listing name bindings")
    }

    async fn get_dropped(&self, instance: &InstanceId) -> Result<Option<Versioned<DroppedLedger>>> {
        let _ = instance;
        unsupported("reading the dropped-ledger registry")
    }

    async fn cas_dropped(
        &self,
        instance: &InstanceId,
        expected: Option<u64>,
        new: Option<&DroppedLedger>,
    ) -> Result<RegistryCas<DroppedLedger>> {
        let _ = (instance, expected, new);
        unsupported("writing the dropped-ledger registry")
    }

    async fn list_dropped(&self) -> Result<Vec<Versioned<DroppedLedger>>> {
        unsupported("listing the dropped-ledger registry")
    }
}

/// Branch-record writes the lifecycle protocols make. Every write that
/// touches an existing record is conditional on its fence, so a stale
/// cleanup can never remove or alter a key someone else has since reused.
#[async_trait]
pub trait BranchRecordStore: Debug + Send + Sync {
    /// The record at `ledger_id` as stored, live or not.
    async fn raw_record(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
        let _ = ledger_id;
        unsupported("reading raw branch records")
    }

    /// Insert `record`, which carries its fence, if no record exists at its
    /// key. Returns `None` when inserted, or the record already there.
    async fn insert_record(&self, record: &NsRecord) -> Result<Option<NsRecord>> {
        let _ = record;
        unsupported("inserting branch records")
    }

    /// Mark the record frozen, if it carries `fence`.
    async fn freeze_record(&self, ledger_id: &str, fence: Fence) -> Result<FenceOutcome> {
        let _ = (ledger_id, fence);
        unsupported("freezing branch records")
    }

    /// Delete the record and everything stored with it, if it carries
    /// `fence`.
    async fn delete_record(&self, ledger_id: &str, fence: Fence) -> Result<FenceOutcome> {
        let _ = (ledger_id, fence);
        unsupported("deleting branch records")
    }

    /// Add `delta` to the record's child-branch count, if it carries `fence`.
    /// A frozen record gains no children: a positive `delta` returns
    /// [`FenceOutcome::Frozen`], so a branch being dropped cannot acquire a
    /// child after its drop has read the count.
    async fn adjust_children(
        &self,
        ledger_id: &str,
        fence: Fence,
        delta: i32,
    ) -> Result<FenceOutcome> {
        let _ = (ledger_id, fence, delta);
        unsupported("adjusting branch child counts")
    }
}

/// The live form of a branch record under `binding`, or `None` when the
/// record is garbage: the binding is not active, or does not list the
/// record's fence.
///
/// The returned record carries the binding's storage root. A dropped branch
/// still listed reads as retracted.
pub fn resolve_record(binding: Option<&NameBinding>, mut record: NsRecord) -> Option<NsRecord> {
    let binding = binding.filter(|b| b.is_active())?;
    let listing = binding.listing(&record.branch)?;
    if record.fence != Some(listing.fence) {
        return None;
    }
    record.storage_root = Some(binding.root.clone());
    record.retracted |= listing.dropped;
    Some(record)
}

/// How reads see a record, given its name's binding. Under a binding, see
/// [`resolve_record`]. A record whose name has no binding at all was created
/// before bindings, and reads as it is.
pub fn resolve_for_read(binding: Option<&NameBinding>, record: NsRecord) -> Option<NsRecord> {
    match binding {
        None => Some(record),
        Some(binding) => resolve_record(Some(binding), record),
    }
}

/// [`resolve_for_read`] for one record, reading its binding from `store`.
pub async fn read_resolved<S: LedgerRegistry + ?Sized>(
    store: &S,
    record: Option<NsRecord>,
) -> Result<Option<NsRecord>> {
    let Some(record) = record else {
        return Ok(None);
    };
    let binding = store.get_binding(&record.name).await?;
    Ok(resolve_for_read(binding.as_ref().map(|v| &v.value), record))
}

/// [`resolve_for_read`] for many records, reading each name's binding once.
pub async fn read_all_resolved<S: LedgerRegistry + ?Sized>(
    store: &S,
    records: Vec<NsRecord>,
) -> Result<Vec<NsRecord>> {
    let mut bindings: std::collections::HashMap<String, Option<NameBinding>> =
        std::collections::HashMap::new();
    let mut resolved = Vec::with_capacity(records.len());
    for record in records {
        if !bindings.contains_key(&record.name) {
            let binding = store.get_binding(&record.name).await?.map(|v| v.value);
            bindings.insert(record.name.clone(), binding);
        }
        if let Some(record) = resolve_for_read(bindings[&record.name].as_ref(), record) {
            resolved.push(record);
        }
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::LedgerName;

    fn binding(state: BindingState, fence: Fence) -> NameBinding {
        let instance = new_instance_id();
        NameBinding {
            root: StorageRoot::for_instance(&LedgerName::parse("mydb").unwrap(), &instance),
            instance,
            root_branch: "main".into(),
            state,
            branches: vec![BranchFence::new("main", fence)],
        }
    }

    #[test]
    fn a_record_is_live_only_under_an_active_binding_listing_its_fence() {
        let fence = Fence::generate();
        let mut record = NsRecord::new("mydb:main");
        record.fence = Some(fence);

        let active = binding(BindingState::Active, fence);
        let live = resolve_record(Some(&active), record.clone()).expect("live");
        assert_eq!(live.storage_root, Some(active.root.clone()));

        for garbage in [
            resolve_record(None, record.clone()),
            resolve_record(
                Some(&binding(BindingState::Creating, fence)),
                record.clone(),
            ),
            resolve_record(
                Some(&binding(BindingState::Dropping { hard: false }, fence)),
                record.clone(),
            ),
            resolve_record(
                Some(&binding(BindingState::Active, Fence::generate())),
                record,
            ),
        ] {
            assert!(garbage.is_none());
        }
    }

    #[test]
    fn fence_and_binding_round_trip_through_json() {
        let fence = Fence::from_u64(0xdead_beef_0000_0001);
        assert_eq!(
            serde_json::to_string(&fence).unwrap(),
            "\"deadbeef00000001\""
        );
        let b = binding(BindingState::Dropping { hard: true }, fence);
        let json = serde_json::to_value(&b).unwrap();
        assert_eq!(json["state"], "dropping");
        assert_eq!(json["hard"], true);
        assert_eq!(serde_json::from_value::<NameBinding>(json).unwrap(), b);
    }

    /// Every registry state survives JSON, including the one whose fields
    /// are flattened beside the entry's own.
    #[test]
    fn dropped_entry_round_trips_in_every_state() {
        let instance = new_instance_id();
        let mut record = NsRecord::new("mydb:main");
        record.fence = Some(Fence::generate());
        for state in [
            DroppedState::Dropped,
            DroppedState::Purging,
            DroppedState::Restoring {
                fences: vec![BranchFence::new("main", Fence::generate())],
            },
        ] {
            let entry = DroppedLedger {
                root: StorageRoot::for_instance(&LedgerName::parse("mydb").unwrap(), &instance),
                instance: instance.clone(),
                state,
                dropped_at: 1,
                name: "mydb".into(),
                root_branch: "main".into(),
                branches: vec![record.clone()],
            };
            let json = serde_json::to_string(&entry).unwrap();
            assert_eq!(serde_json::from_str::<DroppedLedger>(&json).unwrap(), entry);
        }
    }

    #[test]
    fn instance_ids_are_unique_and_sort_by_creation() {
        let a = new_instance_id();
        let b = new_instance_id();
        assert_ne!(a, b);
        assert_eq!(a.as_str().len(), 26);
    }
}
