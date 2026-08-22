//! The nameservice as an [`AppStateMachine`], plus the observer that
//! carries its effects.
//!
//! This is the whole nameservice-specific half of the openraft adapter.
//! The other half — last-applied bookkeeping, membership storage,
//! snapshot persistence, the persist-before-swap ordering — is generic
//! and lives in [`fluree_raft_core::state_machine`].
//!
//! ## The split
//!
//! [`NameServiceApp`] is the pure reduction: it routes a [`Command`]
//! through [`state_machine::apply`] and mirrors membership into the
//! replicated state. No clocks, no IO, no channels — every replica
//! reducing the same log must reach byte-identical state.
//!
//! [`NameServiceObserver`] is everything else: event-bus emissions,
//! waiter resolution, staged receipts, content-store releases, and the
//! ledger-cache watermark. Its hooks run under the state write lock and
//! only *record* what should happen; [`StateMachineObserver::publish`]
//! does it after the lock drops, so a subscriber that reads state back
//! cannot re-enter `apply`.
//!
//! ## Effect ordering is load-bearing
//!
//! `publish` runs in two phases, and the split is not cosmetic. Every
//! commit-head advance reaches the ledger cache **before any event
//! reaches the bus** — the watermark is a synchronous, lossless memory
//! write while the bus is bounded and lossy, so a cache lookup racing
//! this apply must already be comparing against the new watermark. Doing
//! it per-event instead would order each ledger against its own event
//! but let one ledger's *event* overtake another ledger's *watermark*,
//! and a subscriber that reacts to the first by reading the second would
//! see a stale head.
//!
//! Within phase two, order is the order effects were recorded: a waiter
//! bind lands before the terminal command that resolves it, because a
//! single apply batch can carry both.

use crate::raft::staged_receipt::{AppliedReceipt, StagedReceiptMap};
use crate::raft::state_machine::{self, Command, NameServiceState, RefKey, Response};
use crate::raft::waiter::{AbortReason, WaiterMap};
use crate::raft::TypeConfig;
use fluree_db_api::LedgerManager;
use fluree_db_core::ledger_id::format_ledger_id;
use fluree_db_core::ContentId;
use fluree_db_nameservice::{LedgerEventBus, NameServiceEvent};
use fluree_raft_core::node::NodeId;
use fluree_raft_core::state_machine::{
    AppStateMachine, MembershipView, ReadOnlyState, SnapshotCodecError, SnapshotLoad,
    StateMachineObserver,
};
use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};
use tokio::sync::mpsc::UnboundedSender;

/// Read-only handle to the replicated nameservice state.
///
/// Cheap to clone. Handed to every read-side consumer — the
/// [`RaftNameService`](super::nameservice::RaftNameService), the commit
/// workers, the queued transactor, the liveness monitor — so committed
/// state is visible without going through openraft's RPC surface.
///
/// Read-only by type: the only writer is `apply`.
pub type SharedState = ReadOnlyState<NameServiceApp>;

/// The nameservice's pure reduction.
pub struct NameServiceApp;

impl AppStateMachine for NameServiceApp {
    type Config = TypeConfig;
    type Command = Command;
    type Response = Response;
    type State = NameServiceState;

    fn initial_state() -> NameServiceState {
        NameServiceState::default()
    }

    fn apply(state: &mut NameServiceState, command: &Command, log_index: u64) -> Response {
        // `state_machine::apply` consumes the command — it moves fields
        // out of the payloads rather than cloning them per arm.
        state_machine::apply(state, command.clone(), log_index)
    }

    fn noop_response() -> Response {
        Response::NoOp
    }

    /// Mirror the new voter set into the replicated state so
    /// `apply_set_worker_eligibility` can validate against it without
    /// membership being threaded through the apply signature.
    ///
    /// Voters demoted before the change keep their demotion if they
    /// remain in the new voter set; newly-added voters start eligible;
    /// removed voters disappear from both sets in the same step.
    /// Without preserving surviving demotions, a membership change that
    /// does not drop the demoted voter — growing the cluster, say —
    /// re-promotes it, opening an `unreachable_after`-long window where
    /// work rendezvouses to an unreachable node.
    fn apply_membership(
        state: &mut NameServiceState,
        membership: &MembershipView,
        _log_index: u64,
    ) {
        let new_voters: BTreeSet<NodeId> = membership.voters.clone();
        let surviving_demotions: BTreeSet<NodeId> = state
            .configured_voters
            .difference(&state.worker_eligible_voters)
            .copied()
            .filter(|id| new_voters.contains(id))
            .collect();
        state.worker_eligible_voters = new_voters
            .difference(&surviving_demotions)
            .copied()
            .collect();
        state.configured_voters = new_voters;
    }

    /// Bare postcard, no envelope.
    ///
    /// The generic adapter stores whatever this returns verbatim, and
    /// `state_machine::codec`'s magic-plus-version envelope is offered
    /// rather than imposed — which is what lets this keep the exact
    /// snapshot bytes deployed clusters already hold. Changing it is a
    /// rolling-upgrade break.
    fn encode_snapshot(state: &NameServiceState) -> Result<Vec<u8>, SnapshotCodecError> {
        state
            .to_snapshot()
            .map_err(|e| SnapshotCodecError::Encode(e.to_string()))
    }

    fn decode_snapshot(bytes: &[u8]) -> Result<NameServiceState, SnapshotCodecError> {
        NameServiceState::from_snapshot(bytes)
            .map_err(|e| SnapshotCodecError::Decode(e.to_string()))
    }
}

/// One unit of post-apply work, recorded under the lock and performed
/// after it drops.
pub enum Effect {
    /// Broadcast on the event bus. Also the source of the ledger-cache
    /// watermark updates in phase one — see the module docs.
    Event(NameServiceEvent),
    /// A commit head restored in bulk by a snapshot install, which no
    /// per-entry apply will ever report.
    HeadAdvance { ledger_id: String, commit_t: i64 },
    /// Wake or bind a parked proposer.
    Waiter(WaiterResolution),
    /// Free an envelope blob the state machine has finished with.
    Release(String, ContentId),
    /// A snapshot replaced the state wholesale: every parked proposer
    /// and stashed receipt belonged to the prior-leader state, and the
    /// new state may or may not contain their queue ids. Neither can be
    /// trusted across the install.
    InvalidateInFlight,
}

/// The nameservice's effects.
#[derive(Default)]
pub struct NameServiceObserver {
    event_bus: Option<Arc<LedgerEventBus>>,
    waiter_map: Option<Arc<WaiterMap>>,
    staged_receipts: Option<Arc<StagedReceiptMap>>,
    release_tx: Option<UnboundedSender<(String, ContentId)>>,
    ledger_manager: Arc<OnceLock<Arc<LedgerManager>>>,
}

impl NameServiceObserver {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the [`LedgerEventBus`] to broadcast commit/index events on.
    pub fn with_event_bus(mut self, event_bus: Arc<LedgerEventBus>) -> Self {
        self.event_bus = Some(event_bus);
        self
    }

    /// Set the [`WaiterMap`] to resolve after each queue-related apply.
    /// Pair it with the handle the
    /// [`QueuedTransactor`](super::queued_transactor::QueuedTransactor)
    /// arms interest on.
    pub fn with_waiter_map(mut self, waiter_map: Arc<WaiterMap>) -> Self {
        self.waiter_map = Some(waiter_map);
        self
    }

    /// Set the [`StagedReceiptMap`] to read per-op staging detail from
    /// when answering a resolved waiter. Pair it with the handle the
    /// per-branch [`Worker`](super::commit_worker::Worker) stashes into.
    pub fn with_staged_receipts(mut self, staged_receipts: Arc<StagedReceiptMap>) -> Self {
        self.staged_receipts = Some(staged_receipts);
        self
    }

    /// Set the channel that carries release work to the per-node task
    /// owning the `Fluree` handle.
    pub fn with_release_sender(mut self, tx: UnboundedSender<(String, ContentId)>) -> Self {
        self.release_tx = Some(tx);
        self
    }

    /// Clone out the late-binding ledger-cache cell.
    ///
    /// A cell rather than a builder argument because the cache is
    /// constructed *after* the observer: bootstrap takes the cell,
    /// server assembly fills it once `Fluree` exists. An unfilled cell
    /// means applies proceed without reporting.
    pub fn ledger_manager_cell(&self) -> Arc<OnceLock<Arc<LedgerManager>>> {
        Arc::clone(&self.ledger_manager)
    }

    fn resolve_waiter(&self, resolution: WaiterResolution) {
        let Some(waiters) = self.waiter_map.as_ref() else {
            return;
        };
        match resolution {
            WaiterResolution::Bind {
                request_cid,
                queue_id,
            } => waiters.bind(&request_cid, queue_id),
            WaiterResolution::Applied {
                queue_id,
                commit_id,
                commit_t,
            } => {
                let receipt = self
                    .staged_receipts
                    .as_ref()
                    .and_then(|s| s.take(queue_id))
                    .unwrap_or(AppliedReceipt::Minimal {
                        commit_id,
                        commit_t,
                    });
                waiters.resolve_applied(queue_id, receipt);
            }
            WaiterResolution::Aborted { queue_id, reason } => {
                // The receipt is stale — this queue entry will never
                // produce an `ApplyHead`. Drop it so the map does not
                // accumulate one entry per poison for the life of the
                // process.
                if let Some(s) = self.staged_receipts.as_ref() {
                    s.take(queue_id);
                }
                waiters.resolve_aborted(queue_id, reason);
            }
            WaiterResolution::AbortBranch { ref_key, reason } => {
                // An admin clear blew away every queue entry on the
                // branch — drain their stashed receipts too, same
                // reasoning as `Aborted`.
                if let Some(s) = self.staged_receipts.as_ref() {
                    let _ = s.take_for_ref_key(&ref_key);
                }
                waiters.abort_all_for_branch(&ref_key, reason);
            }
        }
    }
}

impl StateMachineObserver<NameServiceApp> for NameServiceObserver {
    type Effect = Effect;

    fn on_command(
        &self,
        _state: &NameServiceState,
        command: &Command,
        response: &mut Response,
        _log_index: u64,
        out: &mut Vec<Effect>,
    ) {
        if let Some(event) = event_for(command, response) {
            out.push(Effect::Event(event));
        }
        if let Some(resolution) = waiter_resolution_for(command, response) {
            out.push(Effect::Waiter(resolution));
        }
        out.extend(
            drain_releases(response)
                .into_iter()
                .map(|(ledger_id, cid)| Effect::Release(ledger_id, cid)),
        );
    }

    fn on_snapshot_loaded(
        &self,
        state: &NameServiceState,
        load: SnapshotLoad,
        out: &mut Vec<Effect>,
    ) {
        // A snapshot advances heads in bulk without per-entry applies,
        // so the per-apply watermark report never fires for them.
        out.extend(state.refs.iter().map(|(key, entry)| Effect::HeadAdvance {
            ledger_id: key.ledger_id(),
            commit_t: entry.t,
        }));
        if matches!(load, SnapshotLoad::LiveInstall) {
            // Only a live install can strand in-flight work. At boot
            // there is none to strand.
            out.push(Effect::InvalidateInFlight);
        }
    }

    fn publish(&self, effects: Vec<Effect>) {
        // Phase one: every watermark, before any event reaches the bus.
        // See the module docs — this ordering is the point of the split.
        if let Some(manager) = self.ledger_manager.get() {
            for effect in &effects {
                match effect {
                    Effect::Event(NameServiceEvent::LedgerCommitPublished {
                        ledger_id,
                        commit_t,
                        ..
                    })
                    | Effect::HeadAdvance {
                        ledger_id,
                        commit_t,
                    } => manager.note_head_advance(ledger_id, *commit_t),
                    _ => {}
                }
            }
        }

        // Phase two: in the order the effects were recorded.
        for effect in effects {
            match effect {
                Effect::Event(event) => {
                    if let Some(bus) = self.event_bus.as_ref() {
                        bus.notify(event);
                    }
                }
                Effect::HeadAdvance { .. } => {}
                Effect::Waiter(resolution) => self.resolve_waiter(resolution),
                Effect::Release(ledger_id, cid) => {
                    if let Some(tx) = self.release_tx.as_ref() {
                        // Receiver dropped -> the release task is gone
                        // (shutdown window, or it died). The blob leaks;
                        // log its CID so an operator or an offline sweep
                        // can reclaim it.
                        if let Err(tokio::sync::mpsc::error::SendError((ledger_id, cid))) =
                            tx.send((ledger_id, cid))
                        {
                            tracing::warn!(
                                %ledger_id,
                                %cid,
                                "release channel closed; envelope blob will leak"
                            );
                        }
                    }
                }
                Effect::InvalidateInFlight => {
                    if let Some(waiters) = self.waiter_map.as_ref() {
                        waiters.drain_all_with(AbortReason::SnapshotInstalled);
                    }
                    if let Some(stash) = self.staged_receipts.as_ref() {
                        stash.clear_all();
                    }
                }
            }
        }
    }
}

/// What the waiter-map should do for a single `(Command, Response)`
/// pair. Computed under the apply lock, executed after it drops so
/// subscribers (the parked waiters' senders' receivers) can't
/// reenter apply.
pub enum WaiterResolution {
    /// This node applied an `EnqueueCommand`. If a local proposer
    /// armed interest in its `request_cid`, bind that interest to the
    /// `queue_id` the state machine just assigned.
    ///
    /// Ordered with the resolutions below rather than done inline,
    /// because a single apply batch can carry both the enqueue and its
    /// terminal command — the bind has to land first.
    ///
    /// A follower has nothing armed, so this is a no-op there. That is
    /// the whole reason a follower's waiter map stays empty.
    Bind {
        request_cid: ContentId,
        queue_id: u64,
    },
    /// `ApplyHead` advanced the head — wake the parked transactor
    /// with the new head identity.
    Applied {
        queue_id: u64,
        commit_id: ContentId,
        commit_t: i64,
    },
    /// `PoisonQueueEntry` recorded a terminal failure — wake the
    /// transactor with the abort reason.
    Aborted { queue_id: u64, reason: AbortReason },
    /// A head-mutating admin command cleared the per-branch queue —
    /// wake every parked transactor on that branch with the
    /// matching abort reason.
    AbortBranch {
        ref_key: RefKey,
        reason: AbortReason,
    },
}

/// Translate an apply-path `(Command, Response)` pair into the
/// matching waiter resolution, if any. Returns `None` for pairs that
/// don't terminate a queue entry (every non-queue command, plus
/// `QueueDesync` — the waiter for that queue_id has already been
/// resolved by whichever earlier event popped it).
fn waiter_resolution_for(cmd: &Command, response: &Response) -> Option<WaiterResolution> {
    match (cmd, response) {
        (
            Command::EnqueueCommand(args),
            Response::Enqueued { queue_id, .. } | Response::InFlight { queue_id, .. },
        ) => Some(WaiterResolution::Bind {
            request_cid: args.request_cid.clone(),
            queue_id: *queue_id,
        }),
        (
            Command::ApplyHead(args),
            Response::HeadApplied {
                commit_id,
                commit_t,
                ..
            },
        ) => Some(WaiterResolution::Applied {
            queue_id: args.queue_id,
            commit_id: commit_id.clone(),
            commit_t: *commit_t,
        }),
        (
            Command::PoisonQueueEntry(_),
            Response::Poisoned {
                queue_id, reason, ..
            },
        ) => Some(WaiterResolution::Aborted {
            queue_id: *queue_id,
            reason: AbortReason::Poisoned(reason.clone()),
        }),
        (
            Command::DropBranch {
                ledger_id, branch, ..
            },
            Response::BranchDropped { .. },
        ) => Some(WaiterResolution::AbortBranch {
            ref_key: RefKey::new(ledger_id, branch),
            reason: AbortReason::BranchDropped,
        }),
        (
            Command::PurgeBranch {
                ledger_id, branch, ..
            },
            Response::Purged { .. },
        ) => Some(WaiterResolution::AbortBranch {
            ref_key: RefKey::new(ledger_id, branch),
            reason: AbortReason::BranchPurged,
        }),
        (
            Command::ResetHead {
                ledger_id, branch, ..
            },
            Response::HeadReset { .. },
        ) => Some(WaiterResolution::AbortBranch {
            ref_key: RefKey::new(ledger_id, branch),
            reason: AbortReason::BranchHeadReset,
        }),
        (
            Command::RetractLedger {
                ledger_id, branch, ..
            },
            Response::Retracted { .. },
        ) => Some(WaiterResolution::AbortBranch {
            ref_key: RefKey::new(ledger_id, branch),
            reason: AbortReason::BranchRetracted,
        }),
        _ => None,
    }
}

/// Translate an apply-path `(Command, Response)` pair into the
/// matching [`NameServiceEvent`]. Returns `None` for pairs that
/// don't advance head state — desyncs, no-ops, idempotency hits.
fn event_for(cmd: &Command, response: &Response) -> Option<NameServiceEvent> {
    match (cmd, response) {
        (
            Command::ApplyHead(args),
            Response::HeadApplied {
                commit_id,
                commit_t,
                ..
            },
        ) => Some(NameServiceEvent::LedgerCommitPublished {
            ledger_id: format_ledger_id(&args.ledger_id, &args.branch),
            commit_id: commit_id.clone(),
            commit_t: *commit_t,
        }),
        (
            Command::AdvanceIndexHead(args),
            Response::IndexAdvanced {
                index_t,
                index_head,
            },
        ) => Some(NameServiceEvent::LedgerIndexPublished {
            ledger_id: format_ledger_id(&args.ledger_id, &args.branch),
            index_id: index_head.clone(),
            index_t: *index_t,
        }),
        (Command::RetractLedger { .. }, Response::Retracted { ledger_id, .. })
        | (Command::PurgeBranch { .. }, Response::Purged { ledger_id, .. })
        | (Command::DropBranch { .. }, Response::BranchDropped { ledger_id, .. }) => {
            Some(NameServiceEvent::LedgerRetracted {
                ledger_id: ledger_id.clone(),
            })
        }
        (Command::CreateBranch(_), Response::BranchCreated { ledger_id, head, t }) => {
            Some(NameServiceEvent::LedgerCommitPublished {
                ledger_id: ledger_id.clone(),
                commit_id: head.clone(),
                commit_t: *t,
            })
        }
        _ => None,
    }
}

/// Drain the `(ledger_id, request_cid)` pairs the state machine has
/// flagged for content-store release. Covers idempotency eviction
/// and the three head-mutating admin commands that clear pending
/// queue entries. Returning `Vec` (not a `&[..]`) lets the adapter
/// move the pairs into the release channel without an extra copy.
fn drain_releases(response: &mut Response) -> Vec<(String, ContentId)> {
    match response {
        Response::EvictionApplied {
            released_envelopes, ..
        }
        | Response::Purged {
            released_envelopes, ..
        }
        | Response::BranchDropped {
            released_envelopes, ..
        }
        | Response::HeadReset {
            released_envelopes, ..
        }
        | Response::Retracted {
            released_envelopes, ..
        } => std::mem::take(released_envelopes),
        // A keyless queue entry's envelope isn't held by any
        // idempotency record, so `apply_head` / `apply_poison`
        // release it as the entry retires.
        Response::HeadApplied {
            ledger_id,
            released_envelope,
            ..
        }
        | Response::Poisoned {
            ledger_id,
            released_envelope,
            ..
        } => released_envelope
            .take()
            .map(|cid| (ledger_id.clone(), cid))
            .into_iter()
            .collect(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::state_machine_adapter::StateMachineAdapter;
    use crate::raft::ClusterNode;
    use fluree_raft_core::storage::{RaftSnapshotStore, RaftStorage};
    use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
    use openraft::{Entry, EntryPayload, StoredMembership};

    /// Outcomes are delivered synchronously by `apply`; this only
    /// bounds a genuine failure.
    const WAIT: std::time::Duration = std::time::Duration::from_secs(5);
    use crate::raft::state_machine::NewLedger;
    use crate::raft::storage::memory::MemoryRaftStorage;
    use crate::raft::Command as RaftCommand;
    use fluree_db_api::{ContentId, ContentKind};
    use openraft::{CommittedLeaderId, LogId};

    fn cid(seed: u8) -> ContentId {
        ContentId::new(ContentKind::Commit, &[seed])
    }

    /// `drain_releases` must forward a keyless entry's envelope
    /// (paired with the response's ledger_id) so the per-node
    /// release task frees it, and forward nothing when the entry
    /// kept its envelope for later idempotency eviction.
    #[test]
    fn drain_releases_forwards_keyless_apply_envelopes() {
        let mut applied = Response::HeadApplied {
            ledger_id: "test/db:main".into(),
            commit_id: cid(1),
            commit_t: 1,
            released_envelope: Some(cid(9)),
        };
        assert_eq!(
            drain_releases(&mut applied),
            vec![("test/db:main".to_string(), cid(9))]
        );

        let mut keyed = Response::HeadApplied {
            ledger_id: "test/db:main".into(),
            commit_id: cid(1),
            commit_t: 1,
            released_envelope: None,
        };
        assert!(drain_releases(&mut keyed).is_empty());

        let mut poisoned = Response::Poisoned {
            ledger_id: "test/db:main".into(),
            queue_id: 7,
            reason: crate::raft::state_machine::PoisonReason::BodyMalformed { error: "x".into() },
            released_envelope: Some(cid(3)),
        };
        assert_eq!(
            drain_releases(&mut poisoned),
            vec![("test/db:main".to_string(), cid(3))]
        );
    }

    fn log_id(term: u64, index: u64) -> LogId<NodeId> {
        LogId {
            leader_id: CommittedLeaderId::new(term, 0),
            index,
        }
    }

    fn create_ledger_entry(index: u64, ledger_id: &str) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(1, index),
            payload: EntryPayload::Normal(RaftCommand::CreateLedger(NewLedger {
                ledger_id: ledger_id.into(),
                branch: "main".into(),
                created_at_millis: 1_000,
            })),
        }
    }

    #[tokio::test]
    async fn apply_routes_create_ledger_to_state_machine() {
        let storage = Arc::new(MemoryRaftStorage::new());
        let mut sm = StateMachineAdapter::new(storage, NameServiceObserver::new());
        let responses = sm.apply([create_ledger_entry(1, "test/db")]).await.unwrap();
        assert_eq!(responses.len(), 1);
        assert!(matches!(responses[0], Response::Created { .. }));
        assert!(sm
            .shared_state()
            .read()
            .await
            .ledgers
            .contains_key("test/db"));
        let (applied, _) = sm.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id(1, 1)));
    }

    #[tokio::test]
    async fn blank_entry_is_noop_but_advances_last_applied() {
        let storage = Arc::new(MemoryRaftStorage::new());
        let mut sm = StateMachineAdapter::new(storage, NameServiceObserver::new());
        let blank = Entry {
            log_id: log_id(1, 5),
            payload: EntryPayload::Blank,
        };
        let responses = sm.apply([blank]).await.unwrap();
        assert_eq!(responses, vec![Response::NoOp]);
        let (applied, _) = sm.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id(1, 5)));
    }

    fn cluster_node(id: NodeId) -> ClusterNode {
        ClusterNode::new(
            format!("http://node-{id}:9090/raft"),
            format!("http://node-{id}:8080"),
        )
    }

    /// The eligibility command the liveness monitor proposes when a
    /// voter stops answering.
    fn demote_entry(index: u64, voter: NodeId) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(1, index),
            payload: EntryPayload::Normal(RaftCommand::SetWorkerEligibility(
                crate::raft::state_machine::WorkerEligibility {
                    voter,
                    eligible: false,
                    applied_at_millis: 5_000,
                },
            )),
        }
    }

    fn membership_entry(index: u64, voters: &[NodeId]) -> Entry<TypeConfig> {
        let voter_set: BTreeSet<NodeId> = voters.iter().copied().collect();
        let nodes: std::collections::BTreeMap<NodeId, ClusterNode> =
            voters.iter().map(|&id| (id, cluster_node(id))).collect();
        Entry {
            log_id: log_id(1, index),
            payload: EntryPayload::Membership(openraft::Membership::new(vec![voter_set], nodes)),
        }
    }

    #[tokio::test]
    async fn membership_apply_mirrors_voter_set_into_state() {
        // First membership-apply sets `configured_voters` from
        // empty and seeds every voter as eligible — the shape the
        // worker supervisor's rendezvous expects at cluster boot.
        let storage = Arc::new(MemoryRaftStorage::new());
        let mut sm = StateMachineAdapter::new(storage, NameServiceObserver::new());

        sm.apply([membership_entry(1, &[1, 2, 3])]).await.unwrap();

        let shared = sm.shared_state();
        let state = shared.read().await;
        let expected: BTreeSet<NodeId> = [1, 2, 3].into_iter().collect();
        assert_eq!(state.configured_voters, expected);
        assert_eq!(state.worker_eligible_voters, expected);
    }

    #[tokio::test]
    async fn membership_change_removing_demoted_voter_drops_it_from_both_sets() {
        // A prior membership-apply seeded {1,2,3}; the leader's
        // monitor then demoted 2. The next membership-apply that
        // adds 4 and removes 2 should snap both sets to {1,3,4}
        // — newly-configured voters start eligible, demoted-then-
        // removed voters disappear from both sets.
        let storage = Arc::new(MemoryRaftStorage::new());
        let mut sm = StateMachineAdapter::new(storage, NameServiceObserver::new());

        sm.apply([membership_entry(1, &[1, 2, 3])]).await.unwrap();
        let shared = sm.shared_state();
        // Mid-life demotion of 2, through the command the liveness
        // monitor actually proposes — the state is read-only to
        // everything but `apply`.
        sm.apply([demote_entry(9, 2)]).await.unwrap();

        // Membership change: drop 2, add 4.
        sm.apply([membership_entry(2, &[1, 3, 4])]).await.unwrap();

        let state = shared.read().await;
        let expected: BTreeSet<NodeId> = [1, 3, 4].into_iter().collect();
        assert_eq!(state.configured_voters, expected);
        // 4 starts eligible alongside the survivors, 2 is gone.
        assert_eq!(state.worker_eligible_voters, expected);
    }

    #[tokio::test]
    async fn membership_change_preserves_demotion_for_voter_that_survives() {
        // A prior membership-apply seeded {1,2,3}; the leader's
        // monitor demoted 2 (still unreachable). The next
        // membership-apply *adds* 4 without dropping 2 — common
        // case when growing the cluster while a node is down.
        // 2's demotion must survive the membership change:
        // worker_eligible_voters should be {1,3,4}, not {1,2,3,4}.
        // Without this guard, work would rendezvous to the
        // unreachable 2 until the monitor re-demoted it
        // ~`unreachable_after` later.
        let storage = Arc::new(MemoryRaftStorage::new());
        let mut sm = StateMachineAdapter::new(storage, NameServiceObserver::new());

        sm.apply([membership_entry(1, &[1, 2, 3])]).await.unwrap();
        let shared = sm.shared_state();
        // Mid-life demotion of 2, through the command the liveness
        // monitor actually proposes — the state is read-only to
        // everything but `apply`.
        sm.apply([demote_entry(9, 2)]).await.unwrap();

        // Membership change: add 4, keep 2 in the voter set.
        sm.apply([membership_entry(2, &[1, 2, 3, 4])])
            .await
            .unwrap();

        let state = shared.read().await;
        let expected_configured: BTreeSet<NodeId> = [1, 2, 3, 4].into_iter().collect();
        let expected_eligible: BTreeSet<NodeId> = [1, 3, 4].into_iter().collect();
        assert_eq!(state.configured_voters, expected_configured);
        assert_eq!(
            state.worker_eligible_voters, expected_eligible,
            "demotion of 2 must survive the membership change; 4 starts eligible"
        );
    }

    #[tokio::test]
    async fn membership_change_starts_newly_added_voters_eligible() {
        // No prior demotions, just a cluster-growth scenario:
        // {1,2,3} → {1,2,3,4,5}. Both new voters start eligible.
        let storage = Arc::new(MemoryRaftStorage::new());
        let mut sm = StateMachineAdapter::new(storage, NameServiceObserver::new());

        sm.apply([membership_entry(1, &[1, 2, 3])]).await.unwrap();
        sm.apply([membership_entry(2, &[1, 2, 3, 4, 5])])
            .await
            .unwrap();

        let shared = sm.shared_state();
        let state = shared.read().await;
        let expected: BTreeSet<NodeId> = [1, 2, 3, 4, 5].into_iter().collect();
        assert_eq!(state.configured_voters, expected);
        assert_eq!(state.worker_eligible_voters, expected);
    }

    /// A `NameServiceState` holding one ledger with a populated branch
    /// head, built directly.
    ///
    /// Setup for tests that need a starting head but not the
    /// `LedgerCommitPublished` that reaching it through the queue path
    /// would emit into the bus they are draining.
    fn state_with_branch_head(
        ledger_id: &str,
        branch: &str,
        head: ContentId,
        t: i64,
    ) -> NameServiceState {
        let mut state = NameServiceState::default();
        NameServiceApp::apply(
            &mut state,
            &RaftCommand::CreateLedger(NewLedger {
                ledger_id: ledger_id.into(),
                branch: branch.into(),
                created_at_millis: 1_000,
            }),
            1,
        );
        if let Some(ledger) = state.ledgers.get_mut(ledger_id) {
            if !ledger.branches.iter().any(|b| b == branch) {
                ledger.branches.push(branch.to_string());
            }
        }
        state.refs.insert(
            RefKey::new(ledger_id, branch),
            crate::raft::state_machine::RefEntry {
                head,
                t,
                last_advanced_at_millis: 2_000,
                last_advanced_index: 1,
                index: None,
                source_branch: None,
                branches: 0,
            },
        );
        state
    }

    /// Storage already holding `state` as its current snapshot, so an
    /// adapter opened over it starts there.
    ///
    /// Setup goes through storage because nothing can write a live
    /// adapter's state — `apply` is the only writer, which is the whole
    /// point of the read-only handle.
    async fn storage_holding(state: &NameServiceState) -> Arc<MemoryRaftStorage> {
        let storage = Arc::new(MemoryRaftStorage::new());
        storage
            .snapshots()
            .write(
                &crate::raft::storage::SnapshotMeta {
                    id: crate::raft::storage::SnapshotId::new("seed-0-0"),
                    last_applied: Some(crate::raft::storage::LogId::new(1, 1)),
                    membership: postcard::to_allocvec(
                        &StoredMembership::<NodeId, ClusterNode>::default(),
                    )
                    .expect("membership encodes"),
                },
                state.to_snapshot().expect("state encodes"),
            )
            .await
            .expect("snapshot writes");
        storage
    }

    #[tokio::test]
    async fn snapshot_build_persists_and_get_current_round_trips() {
        let storage = Arc::new(MemoryRaftStorage::new());
        let mut sm = StateMachineAdapter::new(Arc::clone(&storage), NameServiceObserver::new());
        sm.apply([create_ledger_entry(1, "test/db")]).await.unwrap();

        let mut builder = sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();
        assert_eq!(snap.meta.last_log_id, Some(log_id(1, 1)));

        let current = sm.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(current.meta.snapshot_id, snap.meta.snapshot_id);
    }

    #[tokio::test]
    async fn apply_emits_retracted_event_on_fresh_retract() {
        let storage = Arc::new(MemoryRaftStorage::new());
        let bus = Arc::new(LedgerEventBus::new(16));
        let mut sm = StateMachineAdapter::new(
            storage,
            NameServiceObserver::new().with_event_bus(Arc::clone(&bus)),
        );
        let mut sub = bus.subscribe(fluree_db_nameservice::SubscriptionScope::All);

        sm.apply([create_ledger_entry(1, "test/db")]).await.unwrap();
        sm.apply([Entry {
            log_id: log_id(1, 2),
            payload: EntryPayload::Normal(RaftCommand::RetractLedger {
                ledger_id: "test/db".into(),
                branch: "main".into(),
                applied_at_millis: 0,
            }),
        }])
        .await
        .unwrap();

        match sub.receiver.try_recv().expect("retracted event") {
            NameServiceEvent::LedgerRetracted { ledger_id } => {
                assert_eq!(ledger_id, "test/db:main");
            }
            other => panic!("expected LedgerRetracted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn apply_emits_nothing_on_already_retracted() {
        let storage = Arc::new(MemoryRaftStorage::new());
        let bus = Arc::new(LedgerEventBus::new(16));
        let mut sm = StateMachineAdapter::new(
            storage,
            NameServiceObserver::new().with_event_bus(Arc::clone(&bus)),
        );
        let mut sub = bus.subscribe(fluree_db_nameservice::SubscriptionScope::All);

        sm.apply([create_ledger_entry(1, "test/db")]).await.unwrap();
        sm.apply([Entry {
            log_id: log_id(1, 2),
            payload: EntryPayload::Normal(RaftCommand::RetractLedger {
                ledger_id: "test/db".into(),
                branch: "main".into(),
                applied_at_millis: 0,
            }),
        }])
        .await
        .unwrap();
        let _ = sub.receiver.try_recv().expect("first retract emits");

        sm.apply([Entry {
            log_id: log_id(1, 3),
            payload: EntryPayload::Normal(RaftCommand::RetractLedger {
                ledger_id: "test/db".into(),
                branch: "main".into(),
                applied_at_millis: 0,
            }),
        }])
        .await
        .unwrap();
        assert!(
            sub.receiver.try_recv().is_err(),
            "idempotent retract should not emit"
        );
    }

    #[tokio::test]
    async fn apply_emits_retracted_event_on_purge_of_known_branch() {
        let storage = Arc::new(MemoryRaftStorage::new());
        let bus = Arc::new(LedgerEventBus::new(16));
        let mut sm = StateMachineAdapter::new(
            storage,
            NameServiceObserver::new().with_event_bus(Arc::clone(&bus)),
        );
        let mut sub = bus.subscribe(fluree_db_nameservice::SubscriptionScope::All);

        sm.apply([create_ledger_entry(1, "test/db")]).await.unwrap();
        sm.apply([Entry {
            log_id: log_id(1, 2),
            payload: EntryPayload::Normal(RaftCommand::PurgeBranch {
                ledger_id: "test/db".into(),
                branch: "main".into(),
                applied_at_millis: 0,
            }),
        }])
        .await
        .unwrap();

        match sub.receiver.try_recv().expect("purge event") {
            NameServiceEvent::LedgerRetracted { ledger_id } => {
                assert_eq!(ledger_id, "test/db:main");
            }
            other => panic!("expected LedgerRetracted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn apply_emits_nothing_on_purge_of_missing_branch() {
        let storage = Arc::new(MemoryRaftStorage::new());
        let bus = Arc::new(LedgerEventBus::new(16));
        let mut sm = StateMachineAdapter::new(
            storage,
            NameServiceObserver::new().with_event_bus(Arc::clone(&bus)),
        );
        let mut sub = bus.subscribe(fluree_db_nameservice::SubscriptionScope::All);

        sm.apply([Entry {
            log_id: log_id(1, 1),
            payload: EntryPayload::Normal(RaftCommand::PurgeBranch {
                ledger_id: "ghost".into(),
                branch: "main".into(),
                applied_at_millis: 0,
            }),
        }])
        .await
        .unwrap();
        assert!(
            sub.receiver.try_recv().is_err(),
            "purge of unknown branch should not emit"
        );
    }

    #[tokio::test]
    async fn apply_emits_commit_event_on_create_branch() {
        let storage = storage_holding(&state_with_branch_head("test/db", "main", cid(7), 10)).await;
        let bus = Arc::new(LedgerEventBus::new(16));
        let mut sm = StateMachineAdapter::open(
            storage,
            NameServiceObserver::new().with_event_bus(Arc::clone(&bus)),
        )
        .await
        .expect("adapter opens");
        let mut sub = bus.subscribe(fluree_db_nameservice::SubscriptionScope::All);

        sm.apply([Entry {
            log_id: log_id(1, 3),
            payload: EntryPayload::Normal(RaftCommand::CreateBranch(
                crate::raft::state_machine::NewBranch {
                    ledger_id: "test/db".into(),
                    branch: "feature".into(),
                    source_branch: "main".into(),
                    at_commit: None,
                    applied_at_millis: 3_000,
                },
            )),
        }])
        .await
        .unwrap();

        match sub.receiver.try_recv().expect("create-branch event") {
            NameServiceEvent::LedgerCommitPublished {
                ledger_id,
                commit_id,
                commit_t,
            } => {
                assert_eq!(ledger_id, "test/db:feature");
                assert_eq!(commit_id, cid(7));
                assert_eq!(commit_t, 10);
            }
            other => panic!("expected LedgerCommitPublished, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn apply_emits_retracted_event_on_drop_branch() {
        let storage = storage_holding(&state_with_branch_head("test/db", "main", cid(7), 10)).await;
        let bus = Arc::new(LedgerEventBus::new(16));
        let mut sm = StateMachineAdapter::open(
            storage,
            NameServiceObserver::new().with_event_bus(Arc::clone(&bus)),
        )
        .await
        .expect("adapter opens");
        let mut sub = bus.subscribe(fluree_db_nameservice::SubscriptionScope::All);
        sm.apply([Entry {
            log_id: log_id(1, 3),
            payload: EntryPayload::Normal(RaftCommand::CreateBranch(
                crate::raft::state_machine::NewBranch {
                    ledger_id: "test/db".into(),
                    branch: "feature".into(),
                    source_branch: "main".into(),
                    at_commit: None,
                    applied_at_millis: 3_000,
                },
            )),
        }])
        .await
        .unwrap();
        // Drain the create-branch event.
        let _ = sub.receiver.try_recv().expect("create-branch event");

        sm.apply([Entry {
            log_id: log_id(1, 4),
            payload: EntryPayload::Normal(RaftCommand::DropBranch {
                ledger_id: "test/db".into(),
                branch: "feature".into(),
                applied_at_millis: 0,
            }),
        }])
        .await
        .unwrap();

        match sub.receiver.try_recv().expect("drop-branch event") {
            NameServiceEvent::LedgerRetracted { ledger_id } => {
                assert_eq!(ledger_id, "test/db:feature");
            }
            other => panic!("expected LedgerRetracted, got {other:?}"),
        }
    }

    /// A real `LedgerManager` over memory backends, for asserting
    /// watermark reports through [`LedgerManager::head_watermark`].
    fn memory_ledger_manager() -> Arc<LedgerManager> {
        use fluree_db_api::{LedgerManagerConfig, NameServiceMode};
        use fluree_db_core::{MemoryStorage, StorageBackend};
        use fluree_db_nameservice::memory::MemoryNameService;

        Arc::new(LedgerManager::new(
            StorageBackend::Managed(Arc::new(MemoryStorage::new())),
            NameServiceMode::ReadWrite(Arc::new(MemoryNameService::new())),
            LedgerManagerConfig::default(),
        ))
    }

    #[tokio::test]
    async fn apply_reports_commit_head_advances_to_ledger_manager() {
        let storage = storage_holding(&state_with_branch_head("test/db", "main", cid(7), 10)).await;
        let observer = NameServiceObserver::new();
        let cell = observer.ledger_manager_cell();
        let mut sm = StateMachineAdapter::open(storage, observer)
            .await
            .expect("adapter opens");
        let manager = memory_ledger_manager();
        cell.set(Arc::clone(&manager)).ok().expect("cell empty");
        // CreateBranch emits `LedgerCommitPublished` for the new
        // branch at the source head — the cache's watermark must
        // reflect the same advance the event carries.
        sm.apply([Entry {
            log_id: log_id(1, 3),
            payload: EntryPayload::Normal(RaftCommand::CreateBranch(
                crate::raft::state_machine::NewBranch {
                    ledger_id: "test/db".into(),
                    branch: "feature".into(),
                    source_branch: "main".into(),
                    at_commit: None,
                    applied_at_millis: 3_000,
                },
            )),
        }])
        .await
        .unwrap();

        assert_eq!(manager.head_watermark("test/db:feature"), Some(10));
    }

    #[tokio::test]
    async fn install_snapshot_reports_restored_heads_to_ledger_manager() {
        let source_storage =
            storage_holding(&state_with_branch_head("test/db", "main", cid(7), 42)).await;
        let mut source = StateMachineAdapter::open(source_storage, NameServiceObserver::new())
            .await
            .expect("adapter opens");
        let mut builder = source.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();

        // A snapshot install moves heads in bulk with no per-entry
        // applies; the sweep must report every restored head or
        // caches keyed on the old state never revalidate.
        let target_storage = Arc::new(MemoryRaftStorage::new());
        let observer = NameServiceObserver::new();
        let cell = observer.ledger_manager_cell();
        let mut target = StateMachineAdapter::new(target_storage, observer);
        let manager = memory_ledger_manager();
        cell.set(Arc::clone(&manager)).ok().expect("cell empty");

        target
            .install_snapshot(&snap.meta, snap.snapshot)
            .await
            .unwrap();

        assert_eq!(manager.head_watermark("test/db:main"), Some(42));
    }

    #[tokio::test]
    async fn install_snapshot_replaces_state_and_persists() {
        let source_storage = Arc::new(MemoryRaftStorage::new());
        let mut source =
            StateMachineAdapter::new(Arc::clone(&source_storage), NameServiceObserver::new());
        source
            .apply([create_ledger_entry(1, "test/db")])
            .await
            .unwrap();
        let mut builder = source.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();

        let target_storage = Arc::new(MemoryRaftStorage::new());
        let mut target =
            StateMachineAdapter::new(Arc::clone(&target_storage), NameServiceObserver::new());
        target
            .install_snapshot(&snap.meta, snap.snapshot)
            .await
            .unwrap();

        assert!(target
            .shared_state()
            .read()
            .await
            .ledgers
            .contains_key("test/db"));
        let (applied, _) = target.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id(1, 1)));
    }

    #[tokio::test]
    async fn install_snapshot_drains_waiters_and_stash() {
        use crate::raft::staged_receipt::{StagedReceiptMap, TransactApplied};

        // Source adapter builds a snapshot from a fresh state.
        let source_storage = Arc::new(MemoryRaftStorage::new());
        let mut source =
            StateMachineAdapter::new(Arc::clone(&source_storage), NameServiceObserver::new());
        source
            .apply([create_ledger_entry(1, "test/db")])
            .await
            .unwrap();
        let mut builder = source.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();

        // Target adapter has both maps wired and prior-leader work
        // tracked in them: a parked waiter, a buffered Applied, and
        // a stashed receipt — all keyed on queue_ids the snapshot
        // does not (and cannot) recognize.
        let target_storage = Arc::new(MemoryRaftStorage::new());
        let waiter_map = Arc::new(WaiterMap::new());
        let staged = Arc::new(StagedReceiptMap::new());
        let mut target = StateMachineAdapter::new(
            target_storage,
            NameServiceObserver::new()
                .with_waiter_map(Arc::clone(&waiter_map))
                .with_staged_receipts(Arc::clone(&staged)),
        );

        let mut bound = waiter_map.arm_bound(cid(210), RefKey::new("test/db", "main"), 100);
        // A submission that was proposed but whose enqueue has not
        // applied here yet — still an interest, no queue_id.
        let mut unbound = waiter_map.arm(cid(211), RefKey::new("test/db", "main"));
        // A terminal apply for an id no local proposer is waiting on —
        // every such apply on a follower. It must leave nothing behind.
        waiter_map.resolve_applied(
            101,
            AppliedReceipt::Minimal {
                commit_id: cid(7),
                commit_t: 5,
            },
        );
        staged.stash(
            100,
            RefKey::new("test/db", "main"),
            AppliedReceipt::Transact(TransactApplied {
                commit_id: cid(42),
                commit_t: 10,
                flake_count: 0,
                tally: None,
            }),
        );
        assert_eq!(
            waiter_map.len(),
            2,
            "one bound waiter and one armed interest; the unmatched resolve tracks nothing",
        );
        assert_eq!(staged.len(), 1);

        target
            .install_snapshot(&snap.meta, snap.snapshot)
            .await
            .unwrap();

        // Both the bound waiter and the armed interest are abandoned:
        // the snapshot replaced the state wholesale, so neither the
        // queue entry nor the pending enqueue can be trusted to exist.
        assert!(matches!(
            bound.wait(WAIT).await.unwrap(),
            WaiterOutcome::Aborted(AbortReason::SnapshotInstalled)
        ));
        assert!(matches!(
            unbound.wait(WAIT).await.unwrap(),
            WaiterOutcome::Aborted(AbortReason::SnapshotInstalled)
        ));
        assert!(waiter_map.is_empty());
        // Stashed receipts cleared.
        assert!(staged.is_empty());
    }

    // ====================================================================
    // Waiter-map resolution
    // ====================================================================

    use crate::raft::state_machine::{
        BodyKind, EntryPoisoning, PoisonReason, QueueSubmission, ResetHeadSnapshot, StagedHead,
    };
    use crate::raft::waiter::{AbortReason, WaiterMap, WaiterOutcome};

    fn enqueue_entry(index: u64, ledger_id: &str, branch: &str) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(1, index),
            payload: EntryPayload::Normal(RaftCommand::EnqueueCommand(QueueSubmission {
                ledger_id: ledger_id.into(),
                branch: branch.into(),
                idempotency: None,
                request_cid: cid(0),
                body_cid: cid(0),
                body_kind: BodyKind::JsonLdInsert,
                applied_at_millis: 1_500,
            })),
        }
    }

    fn apply_head_entry(
        index: u64,
        ledger_id: &str,
        branch: &str,
        queue_id: u64,
        commit: ContentId,
        commit_t: i64,
    ) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(1, index),
            payload: EntryPayload::Normal(RaftCommand::ApplyHead(StagedHead {
                ledger_id: ledger_id.into(),
                branch: branch.into(),
                queue_id,
                commit_id: commit,
                commit_t,
                applied_at_millis: 2_000,
                tally: None,
                flake_count: 0,
            })),
        }
    }

    fn poison_entry(
        index: u64,
        ledger_id: &str,
        branch: &str,
        queue_id: u64,
        reason: PoisonReason,
    ) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(1, index),
            payload: EntryPayload::Normal(RaftCommand::PoisonQueueEntry(EntryPoisoning {
                ledger_id: ledger_id.into(),
                branch: branch.into(),
                queue_id,
                reason,
                applied_at_millis: 2_000,
            })),
        }
    }

    fn drop_branch_entry(index: u64, ledger_id: &str, branch: &str) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(1, index),
            payload: EntryPayload::Normal(RaftCommand::DropBranch {
                ledger_id: ledger_id.into(),
                branch: branch.into(),
                applied_at_millis: 0,
            }),
        }
    }

    async fn adapter_with_waiters() -> (StateMachineAdapter<MemoryRaftStorage>, Arc<WaiterMap>) {
        let storage = Arc::new(MemoryRaftStorage::new());
        let waiter_map = Arc::new(WaiterMap::new());
        let adapter = StateMachineAdapter::new(
            storage,
            NameServiceObserver::new().with_waiter_map(Arc::clone(&waiter_map)),
        );
        (adapter, waiter_map)
    }

    #[tokio::test]
    async fn apply_head_resolves_waiter_with_applied_outcome() {
        let (mut adapter, waiters) = adapter_with_waiters().await;
        adapter
            .apply([create_ledger_entry(1, "test/db")])
            .await
            .unwrap();
        adapter
            .apply([enqueue_entry(2, "test/db", "main")])
            .await
            .unwrap();

        let mut rx = waiters.arm_bound(cid(20), RefKey::new("test/db", "main"), 0);
        adapter
            .apply([apply_head_entry(3, "test/db", "main", 0, cid(42), 10)])
            .await
            .unwrap();

        // No StagedReceiptMap is configured on the adapter, so the
        // resolution falls back to Minimal — confirming the absent-
        // entry path delivers commit_id / commit_t without panicking.
        match rx.wait(WAIT).await.expect("receive") {
            WaiterOutcome::Applied(AppliedReceipt::Minimal {
                commit_id,
                commit_t,
            }) => {
                assert_eq!(commit_id, cid(42));
                assert_eq!(commit_t, 10);
            }
            other => panic!("expected Applied(Minimal), got {other:?}"),
        }
    }

    /// One apply batch can carry an enqueue *and* the `ApplyHead` that
    /// retires it. The bind has to land first, or the terminal command
    /// resolves a `queue_id` nothing is listening on and the proposer
    /// waits out its timeout on work that already succeeded.
    ///
    /// This is why waiter effects are ordered against each other rather
    /// than binds being done inline: collapsing `publish` into one pass
    /// that handled terminals before binds would strand exactly this
    /// case, and only under batching.
    #[tokio::test]
    async fn a_bind_and_its_terminal_command_in_one_batch_still_resolve() {
        let (mut adapter, waiters) = adapter_with_waiters().await;
        adapter
            .apply([create_ledger_entry(1, "test/db")])
            .await
            .unwrap();

        // Interest armed by `request_cid` before proposing — the
        // proposer has no `queue_id` yet, which is the whole point.
        let mut ticket = waiters.arm(cid(0), RefKey::new("test/db", "main"));

        adapter
            .apply([
                enqueue_entry(2, "test/db", "main"),
                apply_head_entry(3, "test/db", "main", 0, cid(42), 10),
            ])
            .await
            .unwrap();

        match ticket.wait(WAIT).await.expect("the waiter must resolve") {
            WaiterOutcome::Applied(AppliedReceipt::Minimal {
                commit_id,
                commit_t,
            }) => {
                assert_eq!(commit_id, cid(42));
                assert_eq!(commit_t, 10);
            }
            other => panic!("expected Applied(Minimal), got {other:?}"),
        }
        assert_eq!(
            waiters.len(),
            0,
            "a resolved waiter must leave nothing behind"
        );
    }

    /// Phase one of `publish` has to catch *every* effect that carries a
    /// head, or a cache keyed on the old value never revalidates. The
    /// failure mode is a new event variant carrying a head that nobody
    /// remembers to add here.
    #[tokio::test]
    async fn every_head_advancing_effect_reaches_the_watermark() {
        let observer = NameServiceObserver::new();
        let cell = observer.ledger_manager_cell();
        let manager = memory_ledger_manager();
        cell.set(Arc::clone(&manager)).ok().expect("cell empty");

        observer.publish(vec![
            Effect::Event(NameServiceEvent::LedgerCommitPublished {
                ledger_id: "a/db:main".into(),
                commit_id: cid(1),
                commit_t: 7,
            }),
            Effect::HeadAdvance {
                ledger_id: "b/db:main".into(),
                commit_t: 9,
            },
            // Carries no head; must not be mistaken for one.
            Effect::Event(NameServiceEvent::LedgerRetracted {
                ledger_id: "c/db:main".into(),
            }),
        ]);

        assert_eq!(manager.head_watermark("a/db:main"), Some(7));
        assert_eq!(manager.head_watermark("b/db:main"), Some(9));
        assert_eq!(manager.head_watermark("c/db:main"), None);
    }

    #[tokio::test]
    async fn apply_head_reads_stashed_receipt_when_present() {
        use crate::raft::staged_receipt::{StagedReceiptMap, TransactApplied};
        let storage = Arc::new(MemoryRaftStorage::new());
        let waiter_map = Arc::new(WaiterMap::new());
        let staged = Arc::new(StagedReceiptMap::new());
        let mut adapter = StateMachineAdapter::new(
            storage,
            NameServiceObserver::new()
                .with_waiter_map(Arc::clone(&waiter_map))
                .with_staged_receipts(Arc::clone(&staged)),
        );
        adapter
            .apply([create_ledger_entry(1, "test/db")])
            .await
            .unwrap();
        adapter
            .apply([enqueue_entry(2, "test/db", "main")])
            .await
            .unwrap();

        let mut rx = waiter_map.arm_bound(cid(20), RefKey::new("test/db", "main"), 0);
        staged.stash(
            0,
            RefKey::new("test/db", "main"),
            AppliedReceipt::Transact(TransactApplied {
                commit_id: cid(42),
                commit_t: 10,
                flake_count: 0,
                tally: None,
            }),
        );
        adapter
            .apply([apply_head_entry(3, "test/db", "main", 0, cid(42), 10)])
            .await
            .unwrap();

        match rx.wait(WAIT).await.expect("receive") {
            WaiterOutcome::Applied(AppliedReceipt::Transact(r)) => {
                assert_eq!(r.commit_id, cid(42));
                assert_eq!(r.commit_t, 10);
            }
            other => panic!("expected Applied(Transact), got {other:?}"),
        }
        assert_eq!(staged.len(), 0, "adapter must take from the map");
    }

    #[tokio::test]
    async fn poison_resolves_waiter_with_aborted_poisoned() {
        let (mut adapter, waiters) = adapter_with_waiters().await;
        adapter
            .apply([create_ledger_entry(1, "test/db")])
            .await
            .unwrap();
        adapter
            .apply([enqueue_entry(2, "test/db", "main")])
            .await
            .unwrap();

        let mut rx = waiters.arm_bound(cid(20), RefKey::new("test/db", "main"), 0);
        adapter
            .apply([poison_entry(
                3,
                "test/db",
                "main",
                0,
                PoisonReason::BodyMalformed {
                    error: "bad turtle".into(),
                },
            )])
            .await
            .unwrap();

        match rx.wait(WAIT).await.expect("receive") {
            WaiterOutcome::Aborted(AbortReason::Poisoned(PoisonReason::BodyMalformed {
                error,
            })) => assert_eq!(error, "bad turtle"),
            other => panic!("expected Poisoned, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn drop_branch_resolves_every_pending_waiter_on_that_branch() {
        let (mut adapter, waiters) = adapter_with_waiters().await;
        adapter
            .apply([create_ledger_entry(1, "test/db")])
            .await
            .unwrap();
        adapter
            .apply([enqueue_entry(2, "test/db", "main")])
            .await
            .unwrap();
        adapter
            .apply([enqueue_entry(3, "test/db", "main")])
            .await
            .unwrap();

        let mut rx_a = waiters.arm_bound(cid(20), RefKey::new("test/db", "main"), 0);
        let mut rx_b = waiters.arm_bound(cid(21), RefKey::new("test/db", "main"), 1);

        adapter
            .apply([drop_branch_entry(4, "test/db", "main")])
            .await
            .unwrap();

        assert!(matches!(
            rx_a.wait(WAIT).await.unwrap(),
            WaiterOutcome::Aborted(AbortReason::BranchDropped)
        ));
        assert!(matches!(
            rx_b.wait(WAIT).await.unwrap(),
            WaiterOutcome::Aborted(AbortReason::BranchDropped)
        ));
    }

    #[tokio::test]
    async fn reset_head_resolves_waiter_with_branch_head_reset() {
        let (mut adapter, waiters) = adapter_with_waiters().await;
        adapter
            .apply([create_ledger_entry(1, "test/db")])
            .await
            .unwrap();
        adapter
            .apply([enqueue_entry(2, "test/db", "main")])
            .await
            .unwrap();

        let mut rx = waiters.arm_bound(cid(20), RefKey::new("test/db", "main"), 0);
        adapter
            .apply([Entry {
                log_id: log_id(1, 3),
                payload: EntryPayload::Normal(RaftCommand::ResetHead {
                    ledger_id: "test/db".into(),
                    branch: "main".into(),
                    snapshot: ResetHeadSnapshot {
                        commit_head_id: None,
                        commit_t: 0,
                        index_head_id: None,
                        index_t: 0,
                    },
                    applied_at_millis: 0,
                }),
            }])
            .await
            .unwrap();

        assert!(matches!(
            rx.wait(WAIT).await.unwrap(),
            WaiterOutcome::Aborted(AbortReason::BranchHeadReset)
        ));
    }

    #[tokio::test]
    async fn apply_without_waiter_map_is_silent() {
        // No waiter_map configured — the adapter should still apply
        // and respond normally without trying to resolve anything.
        let storage = Arc::new(MemoryRaftStorage::new());
        let mut adapter = StateMachineAdapter::new(storage, NameServiceObserver::new());
        adapter
            .apply([create_ledger_entry(1, "test/db")])
            .await
            .unwrap();
        // No assertions beyond "didn't panic" — the absence of a
        // waiter handle should be benign.
    }
}
