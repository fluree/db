//! Persistence layer for Raft consensus state.
//!
//! Defines the storage abstraction independently of openraft. State
//! machine *processing* (apply, leader pipeline) lives elsewhere and
//! talks to openraft directly; this module is purely about durably
//! holding bytes — log entries, the vote, the committed watermark,
//! and snapshots — so that backends are swappable.
//!
//! ## Shape
//!
//! - [`RaftStorage`] is the entry point a backend implements. It
//!   owns a [`RaftLogStore`] and a [`RaftSnapshotStore`].
//! - [`RaftLogStore`] persists log entries plus the vote and
//!   committed watermark.
//! - [`RaftSnapshotStore`] persists state-machine snapshots for log
//!   compaction.
//!
//! All trait methods take `&self`; implementations use interior
//! mutability so an `Arc<Backend>` can satisfy the bounds.
//!
//! ## Durability contract
//!
//! Every method's success return must imply on-disk durability of the
//! mutation it describes. openraft relies on this — for example, the
//! vote must be durable before a leader responds to a vote RPC, and
//! log entries must be durable before AppendEntries is acknowledged.
//! Buffered/async impls that violate this break Raft's safety.

use crate::raft::NodeId;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::ops::Range;
use thiserror::Error;

pub mod fs;
pub mod memory;

/// Identifier for a single log entry: a monotonically-increasing
/// `(term, index)` pair. Two entries are equivalent iff their full
/// ids match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LogId {
    pub term: u64,
    pub index: u64,
}

impl LogId {
    pub fn new(term: u64, index: u64) -> Self {
        Self { term, index }
    }
}

/// Persisted Raft vote.
///
/// `committed` reflects openraft 0.9's pre-vote / vote-commit tracking
/// — a vote becomes "committed" when a quorum has acknowledged it.
/// Implementations persist the full struct as one unit. "No vote
/// stored" is represented by [`RaftLogStore::read_vote`] returning
/// `None`, not by a sentinel value inside this struct.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Vote {
    pub term: u64,
    pub candidate: NodeId,
    pub committed: bool,
}

/// A persisted log entry. `payload` is opaque bytes — typically the
/// serialized form of an `openraft::Entry`'s payload. Implementations
/// don't interpret it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub log_id: LogId,
    pub payload: Vec<u8>,
}

/// Summary of the log's current extent.
///
/// `last_log == None && last_purged == None` describes a fresh,
/// never-written log. `last_log == None && last_purged == Some(p)`
/// describes a log whose entries have all been purged (post-snapshot)
/// — the next entry will be at `p.index + 1`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LogState {
    /// Highest log id ever purged via [`RaftLogStore::purge_through`].
    pub last_purged: Option<LogId>,
    /// Last entry currently present in the log.
    pub last_log: Option<LogId>,
}

/// Opaque snapshot identifier. Implementations generate these on
/// [`RaftSnapshotStore::write`]; callers keep them to fetch the same
/// snapshot back later.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotId(pub String);

impl SnapshotId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Metadata describing a snapshot, stored alongside the data bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub id: SnapshotId,
    /// Highest log id whose effects are reflected in this snapshot.
    pub last_applied: Option<LogId>,
    /// Serialized membership state at the snapshot point. Opaque to
    /// the store (the adapter writes openraft's `StoredMembership`).
    pub membership: Vec<u8>,
}

/// Storage backend errors. Specific backends map their failure modes
/// onto these variants so the openraft adapter sees a stable surface.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("storage corruption: {0}")]
    Corruption(String),
    #[error("{0}")]
    Other(String),
}

impl StorageError {
    pub fn io(msg: impl Into<String>) -> Self {
        Self::Io(msg.into())
    }

    pub fn serialization(msg: impl Into<String>) -> Self {
        Self::Serialization(msg.into())
    }

    pub fn corruption(msg: impl Into<String>) -> Self {
        Self::Corruption(msg.into())
    }

    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }
}

/// Persistence for the Raft log, vote, and committed watermark.
///
/// See the module-level docs for the durability contract.
#[async_trait]
pub trait RaftLogStore: Send + Sync + 'static {
    /// Append entries. Caller guarantees `entries` are in strictly
    /// increasing index order and contiguous with the existing tail
    /// of the log.
    async fn append(&self, entries: &[LogEntry]) -> Result<(), StorageError>;

    /// Read entries whose index falls in `range` (half-open). May
    /// return fewer entries than the range implies when the range
    /// extends past the end of the log; never returns more.
    async fn read_range(&self, range: Range<u64>) -> Result<Vec<LogEntry>, StorageError>;

    /// Remove all entries with index `>= from_index`. Idempotent —
    /// no-op when the log already ends below `from_index`.
    async fn truncate_from(&self, from_index: u64) -> Result<(), StorageError>;

    /// Remove all entries with index `<= log_id.index` and record
    /// `log_id` as the new `last_purged`. Idempotent — no-op when the
    /// log's current `last_purged` already covers this id.
    async fn purge_through(&self, log_id: LogId) -> Result<(), StorageError>;

    /// Summarize the log's current extent.
    async fn log_state(&self) -> Result<LogState, StorageError>;

    /// Persist the current vote.
    async fn save_vote(&self, vote: &Vote) -> Result<(), StorageError>;

    /// Read the persisted vote. `None` when no vote has ever been
    /// saved.
    async fn read_vote(&self) -> Result<Option<Vote>, StorageError>;

    /// Persist the highest log id known to be committed by quorum.
    async fn save_committed(&self, log_id: Option<LogId>) -> Result<(), StorageError>;

    /// Read the persisted committed watermark.
    async fn read_committed(&self) -> Result<Option<LogId>, StorageError>;
}

/// Persistence for Raft state-machine snapshots.
#[async_trait]
pub trait RaftSnapshotStore: Send + Sync + 'static {
    /// Persist a snapshot. `data` is opaque bytes — typically the
    /// serialized state machine state.
    async fn write(&self, meta: &SnapshotMeta, data: Vec<u8>) -> Result<(), StorageError>;

    /// Read a previously-written snapshot by id. `None` when the
    /// snapshot has been superseded and reclaimed.
    async fn read(&self, id: &SnapshotId) -> Result<Option<Vec<u8>>, StorageError>;

    /// Return the most recently written snapshot (metadata + data).
    async fn current(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, StorageError>;
}

/// Combined storage entry point. A backend implements one of these,
/// exposing log and snapshot stores. Associated types let the
/// adapter dispatch statically; impls that need dynamic backends can
/// hold `Arc<dyn ...>` internally and expose newtypes.
pub trait RaftStorage: Send + Sync + 'static {
    type LogStore: RaftLogStore;
    type SnapshotStore: RaftSnapshotStore;

    fn log(&self) -> &Self::LogStore;
    fn snapshots(&self) -> &Self::SnapshotStore;
}
