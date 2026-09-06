//! The generic state-machine seam.
//!
//! Two traits, split along the line that actually matters:
//!
//! - [`AppStateMachine`] — **deterministic state reduction**. Every node
//!   must reach byte-identical state from the same log, so `apply` is a
//!   pure function of `(state, command, log_index)`.
//! - [`StateMachineObserver`] — **effects**. Anything that touches the
//!   world outside the state: event buses, waiting proposers, cache
//!   invalidation, metrics.
//!
//! ## Why effects are captured, not called
//!
//! The obvious design — a hook handed `&State` after each apply — does
//! not work, because effects must run *outside* the state write lock so
//! a slow subscriber cannot stall apply. Once the guard drops there is
//! no `&State` left to hand anyone without cloning the whole thing or
//! re-acquiring the lock.
//!
//! So the observer runs in two phases. [`StateMachineObserver::on_command`]
//! and its siblings run **while the state is borrowed** and may only
//! push *owned* values into an effect buffer. Once the lock is released,
//! [`StateMachineObserver::publish`] receives the whole batch in log
//! order and does the real work.
//!
//! `publish` is synchronous and must not block: it runs on the apply
//! path, just without the lock. Ordering *between categories* of effect
//! is the observer's business — it has the entire batch, so it can make
//! as many passes over it as it needs (report cache watermarks first,
//! then broadcast events, then wake proposers, ...).
//!
//! ## Determinism rules for `apply`
//!
//! No clocks, no RNG, no IO, no `HashMap` iteration order in anything
//! observable. Timestamps ride *in* commands — the proposer stamps them
//! before proposing — so "expired" is judged against a command-carried
//! `now`, never a wall clock read inside `apply`. A single node reading
//! `SystemTime::now()` in `apply` diverges the cluster.
//!
//! ## Reads are advisory
//!
//! [`StateMachineAdapter::shared_state`] publishes a read handle to the
//! local, possibly-stale state. It is for serving reads, not for making
//! decisions: anything that must be correct happens inside `apply`,
//! where it is ordered by the log.

use crate::config::FlureeRaftConfig;
use crate::log_adapter::{from_openraft_log_id, to_openraft_log_id};
use crate::node::{ClusterNode, NodeId};
use crate::storage::{
    RaftSnapshotStore, RaftStorage, SnapshotId as OurSnapshotId, SnapshotMeta as OurSnapshotMeta,
};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    AnyError, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, Snapshot, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::BTreeSet;
use std::io::Cursor;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

// ============================================================================
// Snapshot codec
// ============================================================================

/// Why a snapshot could not be encoded or decoded.
#[derive(Debug, Error)]
pub enum SnapshotCodecError {
    #[error("snapshot encode failed: {0}")]
    Encode(String),
    #[error("snapshot decode failed: {0}")]
    Decode(String),
    #[error("snapshot is not a versioned fluree-raft-core snapshot (bad magic)")]
    BadMagic,
    #[error("snapshot truncated: {0} bytes is shorter than the {1}-byte header")]
    Truncated(usize, usize),
    #[error("snapshot format version {found} is not supported (this build handles {supported})")]
    UnsupportedVersion { found: u16, supported: String },
}

impl SnapshotCodecError {
    pub fn encode(e: impl std::fmt::Display) -> Self {
        Self::Encode(e.to_string())
    }
    pub fn decode(e: impl std::fmt::Display) -> Self {
        Self::Decode(e.to_string())
    }
}

/// Versioned postcard snapshot framing.
///
/// A bare `postcard(State)` is a trap: the first field anyone adds to a
/// state struct makes every existing snapshot undecodable, with no way
/// to tell an old snapshot from a corrupt one. This prefixes a magic
/// tag and an explicit `u16` version, so a node that meets a snapshot it
/// cannot read says so precisely, and an application that wants to
/// migrate can branch on [`codec::peek_version`].
///
/// Layout: `b"FRCS"` ++ `u16` little-endian version ++ postcard body.
pub mod codec {
    use super::SnapshotCodecError;
    use serde::de::DeserializeOwned;
    use serde::Serialize;

    /// Magic prefix: "Fluree Raft Core Snapshot".
    pub const MAGIC: &[u8; 4] = b"FRCS";
    /// Bytes before the postcard body: magic + version.
    pub const HEADER_LEN: usize = MAGIC.len() + 2;

    /// Frame `value` as a versioned snapshot blob.
    pub fn encode<T: Serialize>(version: u16, value: &T) -> Result<Vec<u8>, SnapshotCodecError> {
        let body = postcard::to_allocvec(value).map_err(SnapshotCodecError::encode)?;
        let mut out = Vec::with_capacity(HEADER_LEN + body.len());
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&version.to_le_bytes());
        out.extend_from_slice(&body);
        Ok(out)
    }

    /// Read the version tag without decoding the body. Use this to
    /// dispatch between historical state shapes during a migration.
    pub fn peek_version(bytes: &[u8]) -> Result<u16, SnapshotCodecError> {
        if bytes.len() < HEADER_LEN {
            return Err(SnapshotCodecError::Truncated(bytes.len(), HEADER_LEN));
        }
        if &bytes[..MAGIC.len()] != MAGIC {
            return Err(SnapshotCodecError::BadMagic);
        }
        Ok(u16::from_le_bytes([bytes[4], bytes[5]]))
    }

    /// Decode the body as `T`, requiring the blob to carry exactly
    /// `expect_version`.
    pub fn decode<T: DeserializeOwned>(
        bytes: &[u8],
        expect_version: u16,
    ) -> Result<T, SnapshotCodecError> {
        let found = peek_version(bytes)?;
        if found != expect_version {
            return Err(SnapshotCodecError::UnsupportedVersion {
                found,
                supported: expect_version.to_string(),
            });
        }
        postcard::from_bytes(&bytes[HEADER_LEN..]).map_err(SnapshotCodecError::decode)
    }
}

// ============================================================================
// The application seam
// ============================================================================

/// The cluster's membership as an application sees it.
///
/// Deliberately not openraft's `Membership` type: an application
/// mirroring the voter set into its own state should not have to depend
/// on openraft's shape for it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MembershipView {
    pub voters: BTreeSet<NodeId>,
    pub learners: BTreeSet<NodeId>,
}

/// Whether a state replacement came from booting or from the leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotLoad {
    /// Restored from local storage during startup, before the log tail
    /// replays. Nothing is in flight yet, so there is nothing to
    /// invalidate.
    BootRestore,
    /// Installed from the leader at runtime, because this node fell too
    /// far behind. **Any local operation waiting on the previous state
    /// is now unresolvable** — it belonged to a state this node no
    /// longer has and may never have had. This is where in-flight
    /// proposers get told to give up.
    LiveInstall,
}

/// Deterministic state reduction. See the module docs for the rules.
pub trait AppStateMachine: Send + Sync + 'static {
    /// The openraft type config this state machine is wired into. Its
    /// `D`/`R` must be this machine's command and response types.
    type Config: FlureeRaftConfig<D = Self::Command, R = Self::Response>;

    /// Log entry payload.
    type Command: Serialize + DeserializeOwned + Send + Sync + 'static;
    /// What `apply` hands back to the proposer.
    type Response: Serialize + DeserializeOwned + Send + Sync + 'static;
    /// Whole replicated state. `Clone` because snapshots are taken by
    /// cloning under a read lock rather than blocking apply.
    type State: Clone + Send + Sync + 'static;

    /// State for a cluster that has never applied anything.
    ///
    /// Preferred over a `Default` bound: "empty state machine" and
    /// "all fields defaulted" are not always the same thing, and a
    /// state type may legitimately want no `Default` at all.
    fn initial_state() -> Self::State;

    /// Reduce one command into the state. Pure — see the module docs.
    ///
    /// Takes the command by reference so the adapter can hand the same
    /// value to [`StateMachineObserver::on_command`] afterwards without
    /// cloning it. `apply` clones whatever it needs to retain, which is
    /// never more than cloning the whole command would have been.
    fn apply(state: &mut Self::State, command: &Self::Command, log_index: u64) -> Self::Response;

    /// Response for log entries that carry no command: openraft's blank
    /// entry (committed by each new leader) and membership changes.
    ///
    /// These are not proposed by the application, so nothing is waiting
    /// on the value — but openraft's `apply` contract is one response
    /// per entry, so there has to be one.
    fn noop_response() -> Self::Response;

    /// Reduce a membership change into the state.
    ///
    /// Membership entries do not travel through `Command`, so without
    /// this an application cannot maintain state derived from the voter
    /// set. Default is a no-op for machines that do not care.
    ///
    /// Still deterministic: every node applies the same membership
    /// entries in the same order.
    fn apply_membership(state: &mut Self::State, membership: &MembershipView, log_index: u64) {
        let _ = (state, membership, log_index);
    }

    /// Serialize the whole state for a snapshot.
    ///
    /// Applications own their format. [`codec::encode`] gives a
    /// versioned postcard framing that leaves room to migrate; use it
    /// unless you have a reason not to.
    fn encode_snapshot(state: &Self::State) -> Result<Vec<u8>, SnapshotCodecError>;

    /// Rebuild state from a snapshot produced by
    /// [`encode_snapshot`](Self::encode_snapshot) — possibly one
    /// written by an older build. Branch on [`codec::peek_version`] to
    /// migrate.
    fn decode_snapshot(bytes: &[u8]) -> Result<Self::State, SnapshotCodecError>;
}

/// Effects observed under the state lock and published after it drops.
///
/// The blanket [`NoObserver`] implementation covers state machines with
/// no side effects at all.
pub trait StateMachineObserver<A: AppStateMachine>: Send + Sync + 'static {
    /// One unit of deferred work. Must be owned — it outlives the
    /// borrow of the state it was derived from.
    type Effect: Send + 'static;

    /// Called for each applied command **while the state write lock is
    /// held**. Push whatever should happen afterwards into `out`.
    ///
    /// `response` is `&mut` so an observer can *take* fields out of it
    /// — moving a payload into an effect rather than cloning it — as
    /// long as what remains is still a valid response for the proposer.
    fn on_command(
        &self,
        state: &A::State,
        command: &A::Command,
        response: &mut A::Response,
        log_index: u64,
        out: &mut Vec<Self::Effect>,
    ) {
        let _ = (state, command, response, log_index, out);
    }

    /// Called for each membership entry, under the same lock, after
    /// [`AppStateMachine::apply_membership`].
    fn on_membership(
        &self,
        state: &A::State,
        membership: &MembershipView,
        log_index: u64,
        out: &mut Vec<Self::Effect>,
    ) {
        let _ = (state, membership, log_index, out);
    }

    /// Called with the *incoming* state just before it replaces the
    /// current one, on both boot restore and live install.
    ///
    /// Effects are published after the swap, so anything pushed here
    /// observes a node that has already adopted the snapshot. Check
    /// `load` before invalidating in-flight work: on
    /// [`SnapshotLoad::BootRestore`] there is none.
    fn on_snapshot_loaded(
        &self,
        state: &A::State,
        load: SnapshotLoad,
        out: &mut Vec<Self::Effect>,
    ) {
        let _ = (state, load, out);
    }

    /// Run the batch, in log order, with no lock held.
    ///
    /// Synchronous and non-blocking: this is still the apply path. Make
    /// several passes if categories of effect need a specific relative
    /// order — the whole batch is here.
    fn publish(&self, effects: Vec<Self::Effect>) {
        let _ = effects;
    }
}

/// Observer for a state machine with no side effects.
pub struct NoObserver<A>(PhantomData<A>);

impl<A> Default for NoObserver<A> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<A: AppStateMachine> StateMachineObserver<A> for NoObserver<A> {
    type Effect = std::convert::Infallible;
}

// ============================================================================
// Error mapping
// ============================================================================

fn io_err<S: ToString>(
    verb: ErrorVerb,
    subject: ErrorSubject<NodeId>,
    source: S,
) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::new(subject, verb, AnyError::error(source.to_string())),
    }
}

fn read_state_err<S: ToString>(source: S) -> StorageError<NodeId> {
    io_err(ErrorVerb::Read, ErrorSubject::StateMachine, source)
}

fn write_state_err<S: ToString>(source: S) -> StorageError<NodeId> {
    io_err(ErrorVerb::Write, ErrorSubject::StateMachine, source)
}

fn snapshot_err<S: ToString>(verb: ErrorVerb, source: S) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::new(
            ErrorSubject::Snapshot(None),
            verb,
            AnyError::error(source.to_string()),
        ),
    }
}

fn membership_view(m: &openraft::Membership<NodeId, ClusterNode>) -> MembershipView {
    MembershipView {
        voters: m.voter_ids().collect(),
        learners: m.learner_ids().collect(),
    }
}

// ============================================================================
// Adapter
// ============================================================================

/// Read-only handle to the local state. Cheap to clone.
///
/// Read-only by construction, not by convention. A writable handle
/// would let a consumer mutate replicated state outside the log — and
/// since a snapshot is built by cloning this state under the current
/// `last_applied`, that mutation would be persisted and shipped to
/// peers as though the log had produced it. The result is replica
/// divergence with no failed write to point at.
///
/// Everything that must change the state goes through a command.
pub struct ReadOnlyState<A: AppStateMachine> {
    inner: Arc<RwLock<A::State>>,
}

impl<A: AppStateMachine> Clone for ReadOnlyState<A> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<A: AppStateMachine> ReadOnlyState<A> {
    /// Borrow the state, waiting for any in-progress apply to finish.
    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, A::State> {
        self.inner.read().await
    }

    /// Borrow the state if no apply currently holds the write lock;
    /// returns `Err` rather than waiting.
    pub fn try_read(
        &self,
    ) -> Result<tokio::sync::RwLockReadGuard<'_, A::State>, tokio::sync::TryLockError> {
        self.inner.try_read()
    }

    /// A read-only view of state something else owns.
    ///
    /// Narrowing only: it removes the ability to write, never grants
    /// it. That is what separates it from handing a writable handle to
    /// a *live* adapter, which would let something race `apply` and
    /// diverge one replica with no failed write to point at.
    ///
    /// For a consumer that needs to stand a state view up itself —
    /// a test seeding a scenario, most often — rather than receiving
    /// one from [`StateMachineAdapter::shared_state`].
    pub fn view_of(inner: Arc<RwLock<A::State>>) -> Self {
        Self { inner }
    }
}

/// openraft's `RaftStateMachine` over an [`AppStateMachine`].
pub struct StateMachineAdapter<A, O, S>
where
    A: AppStateMachine,
    O: StateMachineObserver<A>,
    S: RaftStorage,
{
    /// Writable only from inside the adapter; readers get a
    /// [`ReadOnlyState`] from [`Self::shared_state`].
    state: Arc<RwLock<A::State>>,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, ClusterNode>,
    storage: Arc<S>,
    /// Monotonic counter for snapshot ids — combined with the
    /// last-applied index for uniqueness across rebuilds.
    snapshot_counter: AtomicU64,
    observer: Arc<O>,
    _app: PhantomData<A>,
}

impl<A, O, S> StateMachineAdapter<A, O, S>
where
    A: AppStateMachine,
    O: StateMachineObserver<A>,
    S: RaftStorage,
{
    /// Fresh adapter with no restored persistence.
    ///
    /// Tests and fresh-cluster bootstrap only — restart paths must use
    /// [`Self::open`], or committed state is silently lost (see there).
    ///
    /// The adapter owns the state it creates; readers obtain a
    /// [`ReadOnlyState`] from [`Self::shared_state`] afterwards. There
    /// is deliberately no constructor taking a caller-supplied state
    /// handle, because such a handle would be writable.
    pub fn new(storage: Arc<S>, observer: O) -> Self {
        Self {
            state: Arc::new(RwLock::new(A::initial_state())),
            last_applied: None,
            last_membership: StoredMembership::default(),
            storage,
            snapshot_counter: AtomicU64::new(0),
            observer: Arc::new(observer),
            _app: PhantomData,
        }
    }

    /// The production restart path: construct, then restore state,
    /// `last_applied`, and `last_membership` from the current snapshot.
    ///
    /// Without this the adapter boots at `last_applied = None` and
    /// openraft replays from index 0 — but the default snapshot policy
    /// purges the pre-snapshot log, so those entries are gone and
    /// committed state vanishes. With it, replay resumes at the
    /// snapshot's `last_applied + 1`.
    ///
    /// Note this restores the *snapshot's* last-applied, not the last
    /// entry applied before the crash: entries committed after the
    /// snapshot are re-applied, so their effects re-fire. Effects must
    /// tolerate replay — this is at-least-once, not exactly-once.
    pub async fn open(storage: Arc<S>, observer: O) -> Result<Self, StorageError<NodeId>> {
        let mut adapter = Self::new(storage, observer);
        adapter.restore_from_snapshot().await?;
        Ok(adapter)
    }

    /// Load the latest snapshot into memory. No-op on a fresh node.
    /// Idempotent.
    async fn restore_from_snapshot(&mut self) -> Result<(), StorageError<NodeId>> {
        let current = self
            .storage
            .snapshots()
            .current()
            .await
            .map_err(|e| snapshot_err(ErrorVerb::Read, e))?;
        let Some((our_meta, bytes)) = current else {
            return Ok(());
        };
        let restored = A::decode_snapshot(&bytes).map_err(read_state_err)?;
        let last_membership: StoredMembership<NodeId, ClusterNode> =
            postcard::from_bytes(&our_meta.membership).map_err(read_state_err)?;

        let mut effects = Vec::new();
        self.observer
            .on_snapshot_loaded(&restored, SnapshotLoad::BootRestore, &mut effects);

        *self.state.write().await = restored;
        self.last_applied = our_meta.last_applied.map(to_openraft_log_id);
        self.last_membership = last_membership;

        self.observer.publish(effects);
        Ok(())
    }

    /// Read handle to the local state — advisory, see the module docs.
    pub fn shared_state(&self) -> ReadOnlyState<A> {
        ReadOnlyState {
            inner: Arc::clone(&self.state),
        }
    }

    /// The observer, for callers that need to reach it after handing
    /// ownership to the adapter.
    pub fn observer(&self) -> Arc<O> {
        Arc::clone(&self.observer)
    }
}

impl<A, O, S> RaftStateMachine<A::Config> for StateMachineAdapter<A, O, S>
where
    A: AppStateMachine,
    O: StateMachineObserver<A>,
    S: RaftStorage,
{
    type SnapshotBuilder = SnapshotBuilder<A, S>;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, ClusterNode>), StorageError<NodeId>>
    {
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<A::Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<A::Config>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();
        let mut effects = Vec::new();
        {
            let mut state = self.state.write().await;
            for entry in entries {
                let log_id = entry.log_id;
                self.last_applied = Some(log_id);
                match entry.payload {
                    EntryPayload::Blank => {
                        // openraft's own no-op, committed by each new
                        // leader. Nothing to reduce; the entry still
                        // owes openraft one response.
                        responses.push(A::noop_response());
                    }
                    EntryPayload::Normal(command) => {
                        let mut response = A::apply(&mut state, &command, log_id.index);
                        self.observer.on_command(
                            &state,
                            &command,
                            &mut response,
                            log_id.index,
                            &mut effects,
                        );
                        responses.push(response);
                    }
                    EntryPayload::Membership(m) => {
                        let view = membership_view(&m);
                        A::apply_membership(&mut state, &view, log_id.index);
                        self.observer
                            .on_membership(&state, &view, log_id.index, &mut effects);
                        // openraft's own bookkeeping is not optional —
                        // it is what a restart reads back.
                        self.last_membership = StoredMembership::new(Some(log_id), m);
                        responses.push(A::noop_response());
                    }
                }
            }
        }
        // Lock released: subscribers can't stall apply from here.
        self.observer.publish(effects);
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let counter = self.snapshot_counter.fetch_add(1, Ordering::Relaxed) + 1;
        let state = self.state.read().await.clone();
        SnapshotBuilder {
            state,
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            storage: Arc::clone(&self.storage),
            counter,
            _app: PhantomData,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, ClusterNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = snapshot.into_inner();
        let new_state = A::decode_snapshot(&bytes).map_err(read_state_err)?;
        let membership_bytes =
            postcard::to_allocvec(&meta.last_membership).map_err(write_state_err)?;

        // Effects are derived from the incoming state before it lands,
        // and published after — so a subscriber that reads back sees a
        // node that has already adopted the snapshot.
        let mut effects = Vec::new();
        self.observer
            .on_snapshot_loaded(&new_state, SnapshotLoad::LiveInstall, &mut effects);

        // Durability before visibility: if the process dies between
        // these two steps, the node restarts into the snapshot it
        // already persisted. Swapping first would let it come back
        // claiming a `last_applied` it has no snapshot for.
        self.storage
            .snapshots()
            .write(
                &OurSnapshotMeta {
                    id: OurSnapshotId::new(&meta.snapshot_id),
                    last_applied: meta.last_log_id.map(from_openraft_log_id),
                    membership: membership_bytes,
                },
                bytes,
            )
            .await
            .map_err(|e| snapshot_err(ErrorVerb::Write, e))?;

        *self.state.write().await = new_state;
        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();

        self.observer.publish(effects);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<A::Config>>, StorageError<NodeId>> {
        let current = self
            .storage
            .snapshots()
            .current()
            .await
            .map_err(|e| snapshot_err(ErrorVerb::Read, e))?;
        let Some((our_meta, data)) = current else {
            return Ok(None);
        };
        let last_membership: StoredMembership<NodeId, ClusterNode> =
            postcard::from_bytes(&our_meta.membership).map_err(read_state_err)?;
        Ok(Some(Snapshot {
            meta: SnapshotMeta {
                last_log_id: our_meta.last_applied.map(to_openraft_log_id),
                last_membership,
                snapshot_id: our_meta.id.as_str().to_string(),
            },
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

/// Point-in-time snapshot source.
///
/// Holds a *cloned* state, so the snapshot reflects the moment
/// [`RaftStateMachine::get_snapshot_builder`] was called rather than
/// whenever the build finishes — and so building one never holds the
/// lock against apply.
pub struct SnapshotBuilder<A, S>
where
    A: AppStateMachine,
    S: RaftStorage,
{
    state: A::State,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, ClusterNode>,
    storage: Arc<S>,
    counter: u64,
    _app: PhantomData<A>,
}

impl<A, S> RaftSnapshotBuilder<A::Config> for SnapshotBuilder<A, S>
where
    A: AppStateMachine,
    S: RaftStorage,
{
    async fn build_snapshot(&mut self) -> Result<Snapshot<A::Config>, StorageError<NodeId>> {
        let bytes = A::encode_snapshot(&self.state).map_err(write_state_err)?;
        let last_index = self.last_applied.map(|id| id.index).unwrap_or(0);
        let snapshot_id = format!("snap-{}-{}", last_index, self.counter);

        let membership_bytes =
            postcard::to_allocvec(&self.last_membership).map_err(write_state_err)?;
        self.storage
            .snapshots()
            .write(
                &OurSnapshotMeta {
                    id: OurSnapshotId::new(&snapshot_id),
                    last_applied: self.last_applied.map(from_openraft_log_id),
                    membership: membership_bytes,
                },
                bytes.clone(),
            )
            .await
            .map_err(|e| snapshot_err(ErrorVerb::Write, e))?;

        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: self.last_applied,
                last_membership: self.last_membership.clone(),
                snapshot_id,
            },
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}
