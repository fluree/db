//! Name bindings, the dropped-ledger registry, and fenced branch-record
//! writes over any [`StorageCas`], shared by the file and storage-backed
//! nameservices.
//!
//! A binding or registry entry is a small JSON file holding a version and
//! the value. Deleting it leaves the file as a tombstone with the version,
//! so versions never repeat for a key.
//!
//! A branch record is two files: the main record and its index file. The
//! fence is in both, and a deleted record stays as a tombstone.

use crate::binding::{FenceOutcome, RegistryCas, Versioned};
use crate::ns_format::{
    ns_context, IndexRef, NsFileV2, NsIndexFileV2, STATUS_DELETED, STATUS_FROZEN,
};
use crate::{deserialize_json, serialize_json, Fence, NameServiceError, NsRecord, Result};
use fluree_db_core::{CasAction, CasOutcome, StorageCas, StorageRead};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Serialize, Deserialize)]
struct VersionedFile<T> {
    v: u64,
    #[serde(default = "Option::default", skip_serializing_if = "Option::is_none")]
    value: Option<T>,
}

impl<T> VersionedFile<T> {
    fn live(self) -> Option<Versioned<T>> {
        let version = self.v;
        self.value.map(|value| Versioned { value, version })
    }
}

async fn read_bytes<S: StorageRead + ?Sized>(storage: &S, key: &str) -> Result<Option<Vec<u8>>> {
    match storage.read_bytes(key).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(fluree_db_core::Error::NotFound(_)) => Ok(None),
        Err(e) => Err(NameServiceError::storage(format!(
            "Failed to read {key}: {e}"
        ))),
    }
}

/// The live item at `key`, if any.
pub(crate) async fn read_versioned<S, T>(storage: &S, key: &str) -> Result<Option<Versioned<T>>>
where
    S: StorageRead + ?Sized,
    T: DeserializeOwned,
{
    let Some(bytes) = read_bytes(storage, key).await? else {
        return Ok(None);
    };
    let file: VersionedFile<T> = serde_json::from_slice(&bytes)?;
    Ok(file.live())
}

/// Compare-and-swap the item at `key`; see
/// [`LedgerRegistry`](crate::LedgerRegistry) for the arguments.
pub(crate) async fn cas_versioned<S, T>(
    storage: &S,
    key: &str,
    expected: Option<u64>,
    new: Option<&T>,
) -> Result<RegistryCas<T>>
where
    S: StorageCas + ?Sized,
    T: Serialize + DeserializeOwned + Clone + Send + Sync,
{
    let written = AtomicU64::new(0);
    let outcome = storage
        .compare_and_swap(key, |bytes| {
            let current: Option<VersionedFile<T>> = bytes.map(deserialize_json).transpose()?;
            let live_version = current.as_ref().and_then(|f| f.value.as_ref().map(|_| f.v));
            if live_version != expected {
                return Ok(CasAction::Abort(current.and_then(VersionedFile::live)));
            }
            let v = current.as_ref().map_or(1, |f| f.v + 1);
            written.store(v, Ordering::SeqCst);
            let file = VersionedFile {
                v,
                value: new.cloned(),
            };
            Ok(CasAction::Write(serialize_json(&file)?))
        })
        .await?;
    Ok(match outcome {
        CasOutcome::Written => RegistryCas::Updated {
            version: new.map(|_| written.load(Ordering::SeqCst)),
        },
        CasOutcome::Aborted(actual) => RegistryCas::Conflict { actual },
    })
}

/// Where a branch record's two files live.
#[derive(Clone, Copy)]
pub(crate) struct RecordKeys<'a> {
    pub main: &'a str,
    pub index: &'a str,
}

async fn read_record<S: StorageRead + ?Sized>(
    storage: &S,
    keys: RecordKeys<'_>,
) -> Result<Option<NsRecord>> {
    let Some(bytes) = read_bytes(storage, keys.main).await? else {
        return Ok(None);
    };
    let main: NsFileV2 = serde_json::from_slice(&bytes)?;
    let index = match read_bytes(storage, keys.index).await? {
        Some(bytes) => Some(serde_json::from_slice::<NsIndexFileV2>(&bytes)?),
        None => None,
    };
    main.into_record(index)
}

/// The record as stored, live or not; `None` for no file or a tombstone.
pub(crate) async fn raw_record<S: StorageRead + ?Sized>(
    storage: &S,
    keys: RecordKeys<'_>,
) -> Result<Option<NsRecord>> {
    read_record(storage, keys).await
}

fn empty_index_file(fence: Option<Fence>, frozen: bool) -> NsIndexFileV2 {
    NsIndexFileV2 {
        context: ns_context(),
        index: IndexRef { cid: None, t: 0 },
        fence,
        frozen,
        extra: Default::default(),
    }
}

/// Insert `record` unless a live record holds the key; see
/// [`BranchRecordStore::insert_record`](crate::BranchRecordStore::insert_record).
///
/// The record is created first, and only its creator then resets the index
/// file to its fence, so an insert that loses the race never touches the
/// winner's index file. Until the reset, an index file an earlier
/// incarnation left does not merge into the new record, since reads take an
/// index file only under the record's own fence. An insert repeated under
/// the fence the record already carries redoes the reset, which finishes one
/// a crash interrupted.
pub(crate) async fn insert_record<S>(
    storage: &S,
    keys: RecordKeys<'_>,
    record: &NsRecord,
) -> Result<Option<NsRecord>>
where
    S: StorageCas + StorageRead + ?Sized,
{
    let existing = match read_record(storage, keys).await? {
        None => {
            let main_bytes = serialize_json(&NsFileV2::for_record(record))?;
            let outcome = storage
                .compare_and_swap(keys.main, |bytes| {
                    let current: Option<NsFileV2> = bytes.map(deserialize_json).transpose()?;
                    match current {
                        Some(file) if !file.is_deleted() => Ok(CasAction::Abort(())),
                        _ => Ok(CasAction::Write(main_bytes.clone())),
                    }
                })
                .await?;
            match outcome {
                CasOutcome::Written => {
                    reset_index_file(storage, keys, record.fence).await?;
                    return Ok(None);
                }
                CasOutcome::Aborted(()) => read_record(storage, keys).await?,
            }
        }
        existing => existing,
    };
    if let Some(existing) = &existing {
        if existing.fence.is_some() && existing.fence == record.fence {
            reset_index_file(storage, keys, record.fence).await?;
        }
    }
    Ok(existing)
}

/// Give the record at `keys` an index file under `fence` unless it has one:
/// an empty one, replacing any an earlier incarnation left.
///
/// A caller paused since it saw the record may find it replaced by a later
/// incarnation, whose index file it must not keep: the record is read again
/// after the reset, and if it no longer carries `fence`, the file replaced is
/// put back while the reset is still there.
async fn reset_index_file<S>(storage: &S, keys: RecordKeys<'_>, fence: Option<Fence>) -> Result<()>
where
    S: StorageCas + StorageRead + ?Sized,
{
    let index_bytes = serialize_json(&empty_index_file(fence, false))?;
    let replaced = parking_lot::Mutex::new(None);
    let outcome = storage
        .compare_and_swap(keys.index, |bytes| {
            let current: Option<NsIndexFileV2> = bytes.map(deserialize_json).transpose()?;
            match current {
                Some(file) if file.fence == fence => Ok(CasAction::Abort(())),
                _ => {
                    *replaced.lock() = bytes.map(<[u8]>::to_vec);
                    Ok(CasAction::Write(index_bytes.clone()))
                }
            }
        })
        .await?;
    if !matches!(outcome, CasOutcome::Written) {
        return Ok(());
    }
    let owner = read_record(storage, keys).await?.and_then(|r| r.fence);
    if owner == fence {
        return Ok(());
    }
    let Some(previous) = replaced.into_inner() else {
        return Ok(());
    };
    storage
        .compare_and_swap(keys.index, |bytes| match bytes {
            Some(current) if current == index_bytes.as_slice() => {
                Ok(CasAction::Write(previous.clone()))
            }
            _ => Ok(CasAction::<()>::Abort(())),
        })
        .await?;
    Ok(())
}

/// Apply `change` to the main record if it carries `fence`. `change` may
/// refuse with the outcome to report instead.
async fn update_fenced<S, F>(
    storage: &S,
    main_key: &str,
    fence: Fence,
    change: F,
) -> Result<FenceOutcome>
where
    S: StorageCas + ?Sized,
    F: Fn(&mut NsFileV2) -> std::result::Result<(), FenceOutcome> + Send + Sync,
{
    let outcome = storage
        .compare_and_swap(main_key, |bytes| {
            let Some(data) = bytes else {
                return Ok(CasAction::Abort(FenceOutcome::Missing));
            };
            let mut file: NsFileV2 = deserialize_json(data)?;
            if file.is_deleted() {
                return Ok(CasAction::Abort(FenceOutcome::Missing));
            }
            if file.fence != Some(fence) {
                return Ok(CasAction::Abort(FenceOutcome::Mismatch));
            }
            if let Err(refused) = change(&mut file) {
                return Ok(CasAction::Abort(refused));
            }
            Ok(CasAction::Write(serialize_json(&file)?))
        })
        .await?;
    Ok(match outcome {
        CasOutcome::Written => FenceOutcome::Applied,
        CasOutcome::Aborted(outcome) => outcome,
    })
}

/// Give a record from before fencing its first fence; see
/// [`BranchRecordStore::adopt_record`](crate::BranchRecordStore::adopt_record).
///
/// The index file takes the fence first: once the main record carries one,
/// an index file without it reads as stale, and a fenced index write to a
/// missing one is refused.
pub(crate) async fn adopt_record<S>(
    storage: &S,
    keys: RecordKeys<'_>,
    fence: Fence,
) -> Result<FenceOutcome>
where
    S: StorageCas + StorageRead + ?Sized,
{
    let Some(bytes) = read_bytes(storage, keys.main).await? else {
        return Ok(FenceOutcome::Missing);
    };
    let main: NsFileV2 = deserialize_json(&bytes)?;
    match main.fence {
        _ if main.is_deleted() => return Ok(FenceOutcome::Missing),
        Some(current) if current == fence => return Ok(FenceOutcome::Applied),
        Some(_) => return Ok(FenceOutcome::Mismatch),
        None => {}
    }

    storage
        .compare_and_swap(keys.index, |bytes| {
            let file = match bytes.map(deserialize_json::<NsIndexFileV2>).transpose()? {
                Some(file) if file.fence == Some(fence) => return Ok(CasAction::Abort(())),
                Some(mut file) if file.fence.is_none() => {
                    file.fence = Some(fence);
                    file
                }
                _ => empty_index_file(Some(fence), false),
            };
            Ok(CasAction::Write(serialize_json(&file)?))
        })
        .await?;

    let outcome = storage
        .compare_and_swap(keys.main, |bytes| {
            let Some(data) = bytes else {
                return Ok(CasAction::Abort(FenceOutcome::Missing));
            };
            let mut file: NsFileV2 = deserialize_json(data)?;
            match file.fence {
                _ if file.is_deleted() => Ok(CasAction::Abort(FenceOutcome::Missing)),
                Some(current) if current == fence => Ok(CasAction::Abort(FenceOutcome::Applied)),
                Some(_) => Ok(CasAction::Abort(FenceOutcome::Mismatch)),
                None => {
                    file.fence = Some(fence);
                    Ok(CasAction::Write(serialize_json(&file)?))
                }
            }
        })
        .await?;
    Ok(match outcome {
        CasOutcome::Written => FenceOutcome::Applied,
        CasOutcome::Aborted(outcome) => outcome,
    })
}

/// Freeze both files of the record, if it carries `fence`.
pub(crate) async fn freeze_record<S>(
    storage: &S,
    keys: RecordKeys<'_>,
    fence: Fence,
) -> Result<FenceOutcome>
where
    S: StorageCas + ?Sized,
{
    let outcome = update_fenced(storage, keys.main, fence, |file| {
        file.status = STATUS_FROZEN.to_string();
        Ok(())
    })
    .await?;
    if outcome == FenceOutcome::Applied {
        storage
            .compare_and_swap(keys.index, |bytes| {
                let mut file = match bytes.map(deserialize_json::<NsIndexFileV2>).transpose()? {
                    Some(file) if file.fence == Some(fence) => file,
                    _ => empty_index_file(Some(fence), false),
                };
                file.frozen = true;
                Ok(CasAction::<()>::Write(serialize_json(&file)?))
            })
            .await?;
    }
    Ok(outcome)
}

/// Turn the record into a tombstone, if it carries `fence`.
pub(crate) async fn delete_record<S>(
    storage: &S,
    main_key: &str,
    fence: Fence,
) -> Result<FenceOutcome>
where
    S: StorageCas + ?Sized,
{
    update_fenced(storage, main_key, fence, |file| {
        file.status = STATUS_DELETED.to_string();
        Ok(())
    })
    .await
}

/// Whether a write presenting `fence` may change the main file `current`,
/// its bytes as read: a live record [`admits`](NsFileV2::admits) it, and a
/// missing or deleted one takes nothing, since publication never creates a
/// record.
pub(crate) fn main_admits(current: Option<&NsFileV2>, fence: Option<Fence>) -> bool {
    current
        .filter(|f| !f.is_deleted())
        .is_some_and(|file| file.admits(fence))
}

/// [`main_admits`] for an index file.
pub(crate) fn index_admits(current: Option<&NsIndexFileV2>, fence: Option<Fence>) -> bool {
    current.is_some_and(|file| file.admits(fence))
}

/// A compare-and-swap refused because the write's fence was not admitted.
pub(crate) struct FenceRefused;

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::storage::MemoryStorage;
    use fluree_db_core::StorageWrite;

    const KEYS: RecordKeys<'static> = RecordKeys {
        main: "fluree:memory://ns@v3/mydb/main.json",
        index: "fluree:memory://ns@v3/mydb/main.index.json",
    };

    fn record() -> NsRecord {
        let mut record = NsRecord::new("mydb:main");
        record.fence = Some(Fence::generate());
        record
    }

    async fn index_fence(storage: &MemoryStorage) -> Option<Fence> {
        let bytes = storage.read_bytes(KEYS.index).await.unwrap();
        serde_json::from_slice::<NsIndexFileV2>(&bytes)
            .unwrap()
            .fence
    }

    /// An index file left under another fence, by a crash between the two
    /// writes or by a stale writer, is replaced when the insert is repeated
    /// under the record's own fence; an insert under another fence leaves
    /// the live record's index file alone.
    #[tokio::test]
    async fn only_the_records_own_insert_sets_its_index_file() {
        let storage = MemoryStorage::new();
        let record = record();
        assert!(insert_record(&storage, KEYS, &record)
            .await
            .unwrap()
            .is_none());
        assert_eq!(index_fence(&storage).await, record.fence);

        let stray = serialize_json(&empty_index_file(Some(Fence::generate()), false)).unwrap();
        storage.write_bytes(KEYS.index, &stray).await.unwrap();
        assert!(insert_record(&storage, KEYS, &record)
            .await
            .unwrap()
            .is_some());
        assert_eq!(index_fence(&storage).await, record.fence);

        let rival = self::record();
        let live = insert_record(&storage, KEYS, &rival).await.unwrap();
        assert_eq!(live.and_then(|r| r.fence), record.fence);
        assert_eq!(index_fence(&storage).await, record.fence);
    }
}
