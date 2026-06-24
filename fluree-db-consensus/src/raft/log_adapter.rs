//! openraft log-storage adapter.
//!
//! Wraps an `Arc<S: RaftStorage>` so the openraft engine can drive
//! our backend-agnostic log storage. Conversions between our owned
//! types ([`super::storage::LogId`], [`super::storage::Vote`],
//! [`super::storage::LogState`], [`super::storage::LogEntry`]) and
//! openraft's parametric versions live here too — this is the only
//! place openraft's type system leaks beyond the `raft` feature.
//!
//! The log entry payload — an [`openraft::Entry<TypeConfig>`] — is
//! serialized via postcard to bytes for storage in [`LogEntry`] and
//! deserialized back on read. Round-tripping costs one allocation
//! per entry; entries are small so this isn't on a hot path.

use crate::raft::storage::{
    LogEntry as OurLogEntry, LogId as OurLogId, LogState as OurLogState, RaftLogStore, RaftStorage,
    Vote as OurVote,
};
use crate::raft::{NodeId, TypeConfig};
use openraft::storage::{LogFlushed, RaftLogReader, RaftLogStorage};
use openraft::{
    AnyError, CommittedLeaderId, Entry, ErrorSubject, ErrorVerb, LeaderId, LogId, LogState,
    StorageError, StorageIOError, Vote,
};
use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

// === Conversions ===

pub(crate) fn to_openraft_log_id(id: OurLogId) -> LogId<NodeId> {
    LogId {
        leader_id: CommittedLeaderId::new(id.term, 0),
        index: id.index,
    }
}

pub(crate) fn from_openraft_log_id(id: LogId<NodeId>) -> OurLogId {
    OurLogId {
        term: id.leader_id.term,
        index: id.index,
    }
}

fn to_openraft_vote(v: &OurVote) -> Vote<NodeId> {
    Vote {
        leader_id: LeaderId::new(v.term, v.candidate),
        committed: v.committed,
    }
}

/// Convert an openraft Vote to ours.
///
/// Returns `None` when `voted_for` is unset — that maps to "no vote
/// has been saved yet" in our scheme, where the storage trait wraps
/// the whole `Vote` in `Option`.
fn from_openraft_vote(v: &Vote<NodeId>) -> Option<OurVote> {
    Some(OurVote {
        term: v.leader_id.term,
        candidate: v.leader_id.voted_for?,
        committed: v.committed,
    })
}

fn to_openraft_log_state(s: OurLogState) -> LogState<TypeConfig> {
    let last_purged = s.last_purged.map(to_openraft_log_id);
    let last_log = s.last_log.map(to_openraft_log_id).or(last_purged);
    LogState {
        last_purged_log_id: last_purged,
        last_log_id: last_log,
    }
}

// === Error mapping ===

fn io_err<S: ToString>(
    verb: ErrorVerb,
    subject: ErrorSubject<NodeId>,
    source: S,
) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::new(subject, verb, AnyError::error(source.to_string())),
    }
}

fn read_log_err<S: ToString>(source: S) -> StorageError<NodeId> {
    io_err(ErrorVerb::Read, ErrorSubject::Logs, source)
}

fn write_log_err<S: ToString>(source: S) -> StorageError<NodeId> {
    io_err(ErrorVerb::Write, ErrorSubject::Logs, source)
}

fn vote_err<S: ToString>(verb: ErrorVerb, source: S) -> StorageError<NodeId> {
    io_err(verb, ErrorSubject::Vote, source)
}

// === Entry (de)serialization ===

fn serialize_entry(entry: &Entry<TypeConfig>) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(entry)
}

fn deserialize_entry(bytes: &[u8]) -> Result<Entry<TypeConfig>, postcard::Error> {
    postcard::from_bytes(bytes)
}

// === Range normalization ===

/// Resolve a `RangeBounds<u64>` to a half-open `[start, end)` pair
/// using `u64::MAX` as the open upper bound.
fn resolve_range<RB: RangeBounds<u64>>(range: &RB) -> std::ops::Range<u64> {
    let start = match range.start_bound() {
        Bound::Included(&n) => n,
        Bound::Excluded(&n) => n.saturating_add(1),
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(&n) => n.saturating_add(1),
        Bound::Excluded(&n) => n,
        Bound::Unbounded => u64::MAX,
    };
    start..end
}

// === Adapter ===

/// openraft log-storage adapter wrapping an `Arc<S: RaftStorage>`.
///
/// Clone-friendly so it can satisfy both the `RaftLogStorage` trait
/// and serve as its own `LogReader` via [`RaftLogStorage::get_log_reader`].
pub struct LogAdapter<S>
where
    S: RaftStorage,
{
    storage: Arc<S>,
}

impl<S> LogAdapter<S>
where
    S: RaftStorage,
{
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    fn log(&self) -> &S::LogStore {
        self.storage.log()
    }
}

impl<S> Clone for LogAdapter<S>
where
    S: RaftStorage,
{
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
        }
    }
}

impl<S> RaftLogReader<TypeConfig> for LogAdapter<S>
where
    S: RaftStorage,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let range = resolve_range(&range);
        let raw = self.log().read_range(range).await.map_err(read_log_err)?;
        let mut entries = Vec::with_capacity(raw.len());
        for raw_entry in raw {
            entries.push(deserialize_entry(&raw_entry.payload).map_err(read_log_err)?);
        }
        Ok(entries)
    }
}

impl<S> RaftLogStorage<TypeConfig> for LogAdapter<S>
where
    S: RaftStorage,
{
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let state = self.log().log_state().await.map_err(read_log_err)?;
        Ok(to_openraft_log_state(state))
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let Some(ours) = from_openraft_vote(vote) else {
            // openraft never asks us to save an unset vote, but be
            // explicit: dropping it would lose state silently.
            return Err(vote_err(
                ErrorVerb::Write,
                "openraft attempted to save a vote with no voted_for",
            ));
        };
        self.log()
            .save_vote(&ours)
            .await
            .map_err(|e| vote_err(ErrorVerb::Write, e))
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let ours = self
            .log()
            .read_vote()
            .await
            .map_err(|e| vote_err(ErrorVerb::Read, e))?;
        Ok(ours.as_ref().map(to_openraft_vote))
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let id = committed.map(from_openraft_log_id);
        self.log().save_committed(id).await.map_err(write_log_err)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let id = self.log().read_committed().await.map_err(read_log_err)?;
        Ok(id.map(to_openraft_log_id))
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut ours = Vec::new();
        for entry in entries {
            let payload = serialize_entry(&entry).map_err(write_log_err)?;
            ours.push(OurLogEntry {
                log_id: from_openraft_log_id(entry.log_id),
                payload,
            });
        }
        match self.log().append(&ours).await {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(e) => {
                let err = std::io::Error::other(e.to_string());
                callback.log_io_completed(Err(err));
                Err(write_log_err(e))
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.log()
            .truncate_from(log_id.index)
            .await
            .map_err(write_log_err)
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let ours = from_openraft_log_id(log_id);
        self.log().purge_through(ours).await.map_err(write_log_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::storage::memory::MemoryRaftStorage;

    #[test]
    fn log_id_round_trip() {
        let ours = OurLogId::new(7, 42);
        let openraft = to_openraft_log_id(ours);
        assert_eq!(openraft.leader_id.term, 7);
        assert_eq!(openraft.index, 42);
        assert_eq!(from_openraft_log_id(openraft), ours);
    }

    #[test]
    fn vote_round_trip_with_candidate() {
        let ours = OurVote {
            term: 5,
            candidate: 42,
            committed: true,
        };
        let openraft = to_openraft_vote(&ours);
        assert_eq!(openraft.leader_id.term, 5);
        assert_eq!(openraft.leader_id.voted_for, Some(42));
        assert!(openraft.committed);
        assert_eq!(from_openraft_vote(&openraft), Some(ours));
    }

    #[test]
    fn from_openraft_vote_returns_none_when_voted_for_unset() {
        let openraft = Vote::<NodeId> {
            leader_id: LeaderId {
                term: 3,
                voted_for: None,
            },
            committed: false,
        };
        assert!(from_openraft_vote(&openraft).is_none());
    }

    #[test]
    fn log_state_empty_log_falls_back_to_purged_for_last_log() {
        let ours = OurLogState {
            last_purged: Some(OurLogId::new(1, 5)),
            last_log: None,
        };
        let openraft = to_openraft_log_state(ours);
        let purged = openraft.last_purged_log_id.unwrap();
        assert_eq!(purged.leader_id.term, 1);
        assert_eq!(purged.index, 5);
        // openraft expects last_log_id to fall back to last_purged when
        // there are no live entries.
        let last_log = openraft.last_log_id.unwrap();
        assert_eq!(last_log.index, 5);
    }

    #[test]
    fn resolve_range_handles_bounds() {
        let r = resolve_range(&(3u64..7u64));
        assert_eq!(r, 3..7);
        let r = resolve_range(&(3u64..=7u64));
        assert_eq!(r, 3..8);
        let r = resolve_range(&(..));
        assert_eq!(r, 0..u64::MAX);
    }

    #[tokio::test]
    async fn adapter_round_trips_an_empty_log_state() {
        let storage = Arc::new(MemoryRaftStorage::new());
        let mut adapter = LogAdapter::new(storage);
        let state = adapter.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());
        assert!(state.last_purged_log_id.is_none());
    }
}
