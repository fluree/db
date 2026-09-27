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
        self.branches
            .iter()
            .find(|b| b.branch == branch)
            .map(|b| b.fence)
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
    /// The branch records as they were when the ledger was dropped.
    pub branches: Vec<NsRecord>,
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
/// The returned record carries the binding's storage root.
pub fn resolve_record(binding: Option<&NameBinding>, mut record: NsRecord) -> Option<NsRecord> {
    let binding = binding.filter(|b| b.is_active())?;
    let fence = binding.fence_of(&record.branch)?;
    if record.fence != Some(fence) {
        return None;
    }
    record.storage_root = Some(binding.root.clone());
    Some(record)
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
            branches: vec![BranchFence {
                branch: "main".into(),
                fence,
            }],
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
                fences: vec![BranchFence {
                    branch: "main".into(),
                    fence: Fence::generate(),
                }],
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
