//! Permanent erasure's replicated state: which view a branch's writes must
//! be staged on, and the activations that changed it.
//!
//! An erasure is prepared off the log: proved, with its replacements and
//! manifest written to shared storage. It takes effect through
//! [`ErasureCommand::Activate`], whose apply checks that every holder is
//! still in the state the preparation covered and advances each holder's
//! erasure generation. A write presents the generation of the view it was
//! staged on ([`Command::AtGeneration`]); one from an older view is refused,
//! so nothing staged before an activation lands after it.

use super::{BodyKind, Command, NameServiceState, RefKey, Response};
use crate::raft::NodeId;
use fluree_db_api::ContentId;
use fluree_db_core::ledger_id::split_ledger_id;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

/// The erasure part of [`NameServiceState`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErasureState {
    /// Whether [`Self::holder_set`] counts, from [`ErasureCommand::Track`]
    /// on. It counts nothing before: entries an earlier release wrote,
    /// replayed by one node and inside another's snapshot, must leave every
    /// node with the same count.
    pub tracking: bool,
    /// Advanced by every command that can add a branch holding data, so an
    /// activation prepared before one is refused.
    pub holder_set: u64,
    /// Each branch's erasure generation; absent is 0.
    pub generations: HashMap<RefKey, u64>,
    /// Branches whose head advances wait while an activation is prepared,
    /// until the leader's clock reaches the value (milliseconds).
    pub holds: HashMap<RefKey, u64>,
    /// Activations by number.
    pub activations: BTreeMap<u64, Activation>,
    pub next_activation: u64,
    /// What each member reported installed: every activation numbered
    /// below `through`, by the run it reported from.
    pub installed: BTreeMap<NodeId, Installed>,
}

/// A member's [`ErasureCommand::Ready`] report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Installed {
    pub through: u64,
    pub incarnation: Incarnation,
}

/// An applied [`ErasureCommand::Activate`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Activation {
    /// The prepared erasure's manifest.
    pub manifest: String,
    pub holders: Vec<RefKey>,
    pub log_index: u64,
    pub applied_at_millis: u64,
}

/// Which run of a member a report comes from: its process, and its local
/// storage. A member restarted, or brought back on an older disk, reports
/// again rather than inheriting what an earlier run reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Incarnation {
    pub process: u64,
    pub storage: u64,
}

/// One branch as an activation expects to find it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HolderState {
    pub key: RefKey,
    /// The branch's fence: which incarnation of the branch.
    pub fence: Option<u64>,
    /// Its head and `t`; `None` while unborn.
    pub head: Option<(ContentId, i64)>,
    /// Its erasure generation, which the prepared proofs build on.
    pub generation: u64,
}

/// Payload for [`ErasureCommand::Activate`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivateErasure {
    pub manifest: String,
    /// [`ErasureState::holder_set`] when the holders were found.
    pub holder_set: u64,
    pub holders: Vec<HolderState>,
    pub applied_at_millis: u64,
}

/// Replicated erasure commands. Variants are positional in the log:
/// append only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ErasureCommand {
    /// Hold head advances on `holders` until `until_millis` on the
    /// leader's clock, or until an activation or [`Self::Release`] lifts
    /// it, so their heads stay as an activation is prepared against them.
    /// Requests keep queueing.
    Hold {
        holders: Vec<RefKey>,
        until_millis: u64,
    },
    /// Lift the holds on `holders`.
    Release { holders: Vec<RefKey> },
    /// Activate a prepared erasure: see [`ActivateErasure`].
    Activate(ActivateErasure),
    /// `node`, in the run `incarnation` names, installed every activation
    /// numbered below `through`. Activations install in order.
    Ready {
        through: u64,
        node: NodeId,
        incarnation: Incarnation,
    },
    /// Start counting holder additions. Proposed before the first
    /// preparation finds its holders.
    Track,
}

/// Why [`Response::ErasureActivationRefused`] fired. The leader prepares
/// again against the current state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActivationRefusal {
    /// A branch that may hold the erased data was added.
    HolderSetChanged { current: u64 },
    /// A holder is not as the preparation found it.
    HolderChanged { current: HolderState },
    /// No holders, or one named twice.
    Malformed,
}

/// A branch's erasure generation.
pub fn generation(state: &NameServiceState, key: &RefKey) -> u64 {
    state.erasure.generations.get(key).copied().unwrap_or(0)
}

/// `key`'s generation as a node that has installed every activation
/// numbered below `installed_through` reads it: the replicated generation,
/// less each activation reaching it the node has not installed.
pub fn view_generation(state: &NameServiceState, key: &RefKey, installed_through: u64) -> u64 {
    let pending = state
        .erasure
        .activations
        .range(installed_through..)
        .filter(|(_, a)| a.holders.contains(key))
        .count() as u64;
    generation(state, key).saturating_sub(pending)
}

/// `key` as an activation would find it now.
pub fn holder_state(state: &NameServiceState, key: &RefKey) -> HolderState {
    HolderState {
        key: key.clone(),
        fence: state.fences.get(key).map(|f| f.fence),
        head: state.refs.get(key).map(|r| (r.head.clone(), r.t)),
        generation: generation(state, key),
    }
}

/// The branches whose content `command` publishes from a staged view,
/// looking through a fence.
fn staged_writes(state: &NameServiceState, command: &Command) -> Vec<RefKey> {
    match command {
        Command::ApplyHead(args) => vec![RefKey::new(&args.ledger_id, &args.branch)],
        Command::AdvanceIndexHead(args) | Command::RewriteIndexHead(args) => {
            vec![RefKey::new(&args.ledger_id, &args.branch)]
        }
        Command::CompareAndSetRef(args) => vec![RefKey::new(&args.ledger_id, &args.branch)],
        Command::ResetHead {
            ledger_id, branch, ..
        } => vec![RefKey::new(ledger_id, branch)],
        // A graph source's index is built from its source branches.
        Command::PublishGraphSourceIndex { name, branch, .. } => state
            .graph_sources
            .get(&fluree_db_core::ledger_id::format_ledger_id(name, branch))
            .map(|record| {
                record
                    .dependencies
                    .iter()
                    .filter_map(|id| split_ledger_id(id).ok())
                    .map(|(name, branch)| RefKey::new(name, branch))
                    .collect()
            })
            .unwrap_or_default(),
        Command::Fenced { command, .. } => staged_writes(state, command),
        _ => Vec::new(),
    }
}

/// Refuse `command` if it was staged on a view of another erasure
/// generation than a branch it writes holds, or advances a held head.
/// `presented` is what [`Command::AtGeneration`] carried; a write without
/// it presents generation 0 for every branch.
pub(super) fn admit(
    state: &mut NameServiceState,
    command: &Command,
    presented: Option<&[(RefKey, u64)]>,
) -> Option<Response> {
    let moved = |key: &RefKey, current: u64, presented: Option<u64>| {
        Some(Response::ErasureGenerationMoved {
            ledger_id: key.ledger_id(),
            current,
            presented,
        })
    };
    for (key, g) in presented.unwrap_or_default() {
        let current = generation(state, key);
        if current != *g {
            return moved(key, current, Some(*g));
        }
    }
    for key in staged_writes(state, command) {
        let current = generation(state, &key);
        let listed = presented.is_some_and(|p| p.iter().any(|(k, _)| *k == key));
        if current > 0 && !listed {
            return moved(&key, current, None);
        }
    }
    let advance = match command {
        Command::ApplyHead(args) => Some(args),
        Command::Fenced { command, .. } => match command.as_ref() {
            Command::ApplyHead(args) => Some(args),
            _ => None,
        },
        _ => None,
    };
    if let Some(args) = advance {
        let key = RefKey::new(&args.ledger_id, &args.branch);
        if let Some(&until_millis) = state.erasure.holds.get(&key) {
            if args.applied_at_millis < until_millis {
                return Some(Response::ErasureHeld {
                    ledger_id: key.ledger_id(),
                    until_millis,
                });
            }
            state.erasure.holds.remove(&key);
        }
    }
    None
}

/// Whether applying `command` can add a branch that holds data. A head
/// advance can too, by creating a branch's first head or bringing it
/// another branch's history; [`super::apply`] catches the first by the new
/// ref entry and the second through [`importing`].
pub(super) fn adds_holders(command: &Command) -> bool {
    match command {
        Command::CreateLedger(_)
        | Command::CreateBranch(_)
        | Command::ResetHead { .. }
        | Command::CompareAndSetRef(_)
        | Command::CasBinding { .. }
        | Command::CasDropped { .. }
        | Command::InsertBranchRecord(_)
        | Command::AdoptBranch { .. }
        | Command::ThawBranch { .. } => true,
        Command::AdvanceIndexHead(_)
        | Command::RewriteIndexHead(_)
        | Command::DropBranch { .. }
        | Command::RetractLedger { .. }
        | Command::PurgeBranch { .. }
        | Command::ReleaseContent { .. }
        | Command::PushStatus { .. }
        | Command::PushConfig(_)
        | Command::PublishGraphSource { .. }
        | Command::PublishGraphSourceIndex { .. }
        | Command::RetractGraphSource { .. }
        | Command::EnqueueCommand(_)
        | Command::ApplyHead(_)
        | Command::PoisonQueueEntry(_)
        | Command::EvictIdempotency { .. }
        | Command::SetWorkerEligibility(_)
        | Command::FreezeBranch { .. }
        | Command::DeleteBranch { .. }
        | Command::ResetGraphSourceIndex { .. }
        | Command::Erasure(_) => false,
        Command::Fenced { command, .. } | Command::AtGeneration { command, .. } => {
            adds_holders(command)
        }
    }
}

/// The branch `command` advances with history from elsewhere, as a merge,
/// rebase or push does, and its head before: an existing branch that did
/// not hold the erased data can after.
pub(super) fn importing(
    state: &NameServiceState,
    command: &Command,
) -> Option<(RefKey, Option<ContentId>)> {
    let args = match command {
        Command::ApplyHead(args) => args,
        Command::Fenced { command, .. } => match command.as_ref() {
            Command::ApplyHead(args) => args,
            _ => return None,
        },
        _ => return None,
    };
    let key = RefKey::new(&args.ledger_id, &args.branch);
    let front = state.queues.get(&key)?.front()?;
    let imports = matches!(
        front.body_kind,
        BodyKind::Merge | BodyKind::Rebase | BodyKind::Pushed
    );
    (front.queue_id == args.queue_id && imports).then(|| {
        let head = state.refs.get(&key).map(|r| r.head.clone());
        (key, head)
    })
}

pub(super) fn apply(
    state: &mut NameServiceState,
    command: ErasureCommand,
    log_index: u64,
) -> Response {
    match command {
        ErasureCommand::Hold {
            holders,
            until_millis,
        } => {
            for key in holders {
                state.erasure.holds.insert(key, until_millis);
            }
            Response::ErasureHoldApplied
        }
        ErasureCommand::Release { holders } => {
            for key in &holders {
                state.erasure.holds.remove(key);
            }
            Response::ErasureHoldApplied
        }
        ErasureCommand::Activate(args) => activate(state, args, log_index),
        ErasureCommand::Ready {
            through,
            node,
            incarnation,
        } => {
            if through > state.erasure.next_activation {
                return Response::ErasureActivationUnknown {
                    activation: through - 1,
                };
            }
            state.erasure.installed.insert(
                node,
                Installed {
                    through,
                    incarnation,
                },
            );
            Response::ErasureReadyRecorded
        }
        ErasureCommand::Track => {
            state.erasure.tracking = true;
            Response::ErasureTracking
        }
    }
}

fn activate(state: &mut NameServiceState, args: ActivateErasure, log_index: u64) -> Response {
    let refused = |refusal| Response::ErasureActivationRefused { refusal };
    let distinct: HashSet<&RefKey> = args.holders.iter().map(|h| &h.key).collect();
    if args.holders.is_empty() || distinct.len() != args.holders.len() {
        return refused(ActivationRefusal::Malformed);
    }
    if !state.erasure.tracking || args.holder_set != state.erasure.holder_set {
        return refused(ActivationRefusal::HolderSetChanged {
            current: state.erasure.holder_set,
        });
    }
    for expected in &args.holders {
        let current = holder_state(state, &expected.key);
        if current != *expected {
            return refused(ActivationRefusal::HolderChanged { current });
        }
    }
    let holders: Vec<RefKey> = args.holders.into_iter().map(|h| h.key).collect();
    for key in &holders {
        *state.erasure.generations.entry(key.clone()).or_insert(0) += 1;
        state.erasure.holds.remove(key);
    }
    let activation = state.erasure.next_activation;
    state.erasure.next_activation += 1;
    state.erasure.activations.insert(
        activation,
        Activation {
            manifest: args.manifest,
            holders,
            log_index,
            applied_at_millis: args.applied_at_millis,
        },
    );
    Response::ErasureActivated { activation }
}

#[cfg(test)]
mod tests {
    use super::super::{
        apply, BodyKind, NewBranch, NewIndexHead, NewLedger, QueueSubmission, StagedHead,
    };
    use super::*;
    use fluree_db_nameservice::GraphSourceType;

    fn cid(seed: u8) -> ContentId {
        ContentId::new(fluree_db_api::ContentKind::Commit, &[seed])
    }

    fn main_key() -> RefKey {
        RefKey::new("test/db", "main")
    }

    /// `test/db:main` with one request queued, holder additions counted.
    fn ledger_with_queued_request() -> NameServiceState {
        let mut state = NameServiceState::new();
        apply(&mut state, Command::Erasure(ErasureCommand::Track), 1);
        apply(
            &mut state,
            Command::CreateLedger(NewLedger {
                ledger_id: "test/db".into(),
                branch: "main".into(),
                created_at_millis: 1_000,
            }),
            1,
        );
        enqueue(&mut state, 7);
        state
    }

    fn enqueue(state: &mut NameServiceState, seed: u8) -> u64 {
        enqueue_kind(state, seed, BodyKind::JsonLdInsert)
    }

    fn enqueue_kind(state: &mut NameServiceState, seed: u8, body_kind: BodyKind) -> u64 {
        match apply(
            state,
            Command::EnqueueCommand(QueueSubmission {
                ledger_id: "test/db".into(),
                branch: "main".into(),
                idempotency: None,
                request_cid: cid(seed),
                body_cid: cid(seed),
                body_kind,
                applied_at_millis: 1_000,
            }),
            2,
        ) {
            Response::Enqueued { queue_id, .. } => queue_id,
            other => panic!("expected Enqueued, got {other:?}"),
        }
    }

    fn front(state: &NameServiceState) -> u64 {
        state.queues[&main_key()].front().unwrap().queue_id
    }

    fn head(state: &NameServiceState, seed: u8, t: i64, applied_at_millis: u64) -> Command {
        Command::ApplyHead(StagedHead {
            ledger_id: "test/db".into(),
            branch: "main".into(),
            queue_id: front(state),
            commit_id: cid(seed),
            commit_t: t,
            applied_at_millis,
            tally: None,
            flake_count: 0,
        })
    }

    /// Apply the queue front's head advance, staged on a view at
    /// `generation` (`None`: unwrapped).
    fn advance(
        state: &mut NameServiceState,
        generation: Option<u64>,
        seed: u8,
        t: i64,
        applied_at_millis: u64,
        log_index: u64,
    ) -> Response {
        let command = head(state, seed, t, applied_at_millis);
        let command = match generation {
            Some(g) => at(g, command),
            None => command,
        };
        apply(state, command, log_index)
    }

    fn at(generation: u64, command: Command) -> Command {
        Command::AtGeneration {
            generations: vec![(main_key(), generation)],
            command: Box::new(command),
        }
    }

    fn activate(state: &mut NameServiceState, holders: &[RefKey]) -> Response {
        let holders = holders.iter().map(|k| holder_state(state, k)).collect();
        let holder_set = state.erasure.holder_set;
        apply(
            state,
            Command::Erasure(ErasureCommand::Activate(ActivateErasure {
                manifest: "m".into(),
                holder_set,
                holders,
                applied_at_millis: 5_000,
            })),
            10,
        )
    }

    fn generation_moved(response: &Response) -> bool {
        matches!(response, Response::ErasureGenerationMoved { .. })
    }

    /// A head staged on a view from before an activation never lands: the
    /// entry stays at the front for a stage on the current view.
    #[test]
    fn a_head_staged_before_an_activation_is_refused_and_stays_queued() {
        let mut state = ledger_with_queued_request();
        let queued = front(&state);
        assert_eq!(
            activate(&mut state, &[main_key()]),
            Response::ErasureActivated { activation: 0 }
        );
        assert_eq!(generation(&state, &main_key()), 1);

        let unwrapped = advance(&mut state, None, 1, 1, 6_000, 11);
        assert!(generation_moved(&unwrapped), "{unwrapped:?}");
        let older = advance(&mut state, Some(0), 1, 1, 6_000, 12);
        assert!(generation_moved(&older), "{older:?}");
        assert_eq!(front(&state), queued);
        assert!(!state.refs.contains_key(&main_key()));

        let current = advance(&mut state, Some(1), 1, 1, 6_000, 13);
        assert!(
            matches!(current, Response::HeadApplied { commit_t: 1, .. }),
            "{current:?}"
        );
    }

    /// Before any activation a branch takes unwrapped writes, as every
    /// write before this release was.
    #[test]
    fn a_branch_no_erasure_reached_takes_unwrapped_writes() {
        let mut state = ledger_with_queued_request();
        let applied = advance(&mut state, None, 1, 1, 6_000, 11);
        assert!(
            matches!(applied, Response::HeadApplied { .. }),
            "{applied:?}"
        );
    }

    /// Every way a holder can differ from what the preparation covered
    /// refuses the activation, and leaves every generation as it was.
    #[test]
    fn an_activation_is_refused_when_its_holders_changed() {
        let mut state = ledger_with_queued_request();
        advance(&mut state, None, 1, 1, 6_000, 11);
        let prepared = holder_state(&state, &main_key());
        let prepared_set = state.erasure.holder_set;
        let refused = |state: &mut NameServiceState, holder_set, holders| {
            apply(
                state,
                Command::Erasure(ErasureCommand::Activate(ActivateErasure {
                    manifest: "m".into(),
                    holder_set,
                    holders,
                    applied_at_millis: 5_000,
                })),
                20,
            )
        };

        // The head moved: that commit may hold another copy.
        enqueue(&mut state, 8);
        advance(&mut state, None, 2, 2, 6_000, 12);
        let response = refused(&mut state, prepared_set, vec![prepared.clone()]);
        assert!(
            matches!(
                &response,
                Response::ErasureActivationRefused {
                    refusal: ActivationRefusal::HolderChanged { current }
                } if current.head == Some((cid(2), 2))
            ),
            "{response:?}"
        );

        // A branch was added after the holders were found.
        let prepared = holder_state(&state, &main_key());
        apply(
            &mut state,
            Command::CreateBranch(NewBranch {
                ledger_id: "test/db".into(),
                branch: "dev".into(),
                source_branch: "main".into(),
                at_commit: None,
                applied_at_millis: 6_000,
            }),
            13,
        );
        let response = refused(&mut state, prepared_set, vec![prepared.clone()]);
        assert!(
            matches!(
                response,
                Response::ErasureActivationRefused {
                    refusal: ActivationRefusal::HolderSetChanged { .. }
                }
            ),
            "{response:?}"
        );

        // Another erasure activated on the holder: the proofs built on the
        // generation before it.
        assert!(matches!(
            activate(&mut state, &[main_key()]),
            Response::ErasureActivated { .. }
        ));
        let current_set = state.erasure.holder_set;
        let response = refused(&mut state, current_set, vec![prepared.clone()]);
        assert!(
            matches!(
                &response,
                Response::ErasureActivationRefused {
                    refusal: ActivationRefusal::HolderChanged { current }
                } if current.generation == 1
            ),
            "{response:?}"
        );

        // Named twice, or no holders at all.
        let current = holder_state(&state, &main_key());
        for holders in [vec![current.clone(), current], vec![]] {
            assert_eq!(
                refused(&mut state, current_set, holders),
                Response::ErasureActivationRefused {
                    refusal: ActivationRefusal::Malformed
                }
            );
        }
        assert_eq!(generation(&state, &main_key()), 1);
        assert_eq!(state.erasure.activations.len(), 1);
    }

    /// A branch recreated under the same name is another incarnation, so a
    /// preparation of the old one cannot activate on it.
    #[test]
    fn an_activation_is_refused_for_another_incarnation_of_a_branch() {
        let mut state = ledger_with_queued_request();
        state.fences.insert(
            main_key(),
            super::super::FenceState {
                fence: 1,
                frozen: false,
                source_branch: None,
                children: 0,
            },
        );
        let prepared = holder_state(&state, &main_key());
        state.fences.get_mut(&main_key()).unwrap().fence = 2;
        let holder_set = state.erasure.holder_set;
        let response = apply(
            &mut state,
            Command::Erasure(ErasureCommand::Activate(ActivateErasure {
                manifest: "m".into(),
                holder_set,
                holders: vec![prepared],
                applied_at_millis: 5_000,
            })),
            20,
        );
        assert!(
            matches!(
                &response,
                Response::ErasureActivationRefused {
                    refusal: ActivationRefusal::HolderChanged { current }
                } if current.fence == Some(2)
            ),
            "{response:?}"
        );
    }

    /// Branches come and go through lifecycle commands; writes to existing
    /// branches leave the holder set alone, or a busy cluster would never
    /// activate. A branch's first head counts as adding it.
    #[test]
    fn the_holder_set_advances_with_branches_not_with_writes() {
        let mut state = ledger_with_queued_request();
        let registered = state.erasure.holder_set;
        assert!(registered > 0, "creating the ledger added a holder");
        advance(&mut state, None, 1, 1, 6_000, 11);
        let created = state.erasure.holder_set;
        assert!(created > registered, "the first head added one");
        enqueue(&mut state, 8);
        advance(&mut state, None, 2, 2, 6_000, 12);
        apply(
            &mut state,
            Command::AdvanceIndexHead(NewIndexHead {
                ledger_id: "test/db".into(),
                branch: "main".into(),
                new_index_head: cid(50),
                t: 2,
                applied_at_millis: 6_000,
            }),
            13,
        );
        assert_eq!(state.erasure.holder_set, created);

        apply(
            &mut state,
            Command::CreateBranch(NewBranch {
                ledger_id: "test/db".into(),
                branch: "dev".into(),
                source_branch: "main".into(),
                at_commit: None,
                applied_at_millis: 6_000,
            }),
            14,
        );
        assert!(state.erasure.holder_set > created);
    }

    /// A merge, rebase or push can bring an existing branch the erased data
    /// from a holder, so an activation whose holders were found before it
    /// lands is refused. A transaction brings nothing.
    #[test]
    fn a_head_brought_from_another_branch_changes_the_holder_set() {
        let mut state = ledger_with_queued_request();
        advance(&mut state, None, 1, 1, 6_000, 11);
        apply(
            &mut state,
            Command::CreateBranch(NewBranch {
                ledger_id: "test/db".into(),
                branch: "dev".into(),
                source_branch: "main".into(),
                at_commit: None,
                applied_at_millis: 6_000,
            }),
            12,
        );
        let dev = RefKey::new("test/db", "dev");
        let activate_after = |state: &mut NameServiceState, seed: u8, kind| {
            let holders = vec![holder_state(state, &dev)];
            let holder_set = state.erasure.holder_set;
            enqueue_kind(state, seed, kind);
            let moved = advance(state, None, seed, i64::from(seed), 6_000, 13);
            assert!(matches!(moved, Response::HeadApplied { .. }), "{moved:?}");
            apply(
                state,
                Command::Erasure(ErasureCommand::Activate(ActivateErasure {
                    manifest: "m".into(),
                    holder_set,
                    holders,
                    applied_at_millis: 6_000,
                })),
                14,
            )
        };
        for (seed, kind) in [
            (2, BodyKind::Merge),
            (3, BodyKind::Rebase),
            (4, BodyKind::Pushed),
        ] {
            let response = activate_after(&mut state, seed, kind);
            assert!(
                matches!(
                    response,
                    Response::ErasureActivationRefused {
                        refusal: ActivationRefusal::HolderSetChanged { .. }
                    }
                ),
                "{kind:?}: {response:?}"
            );
        }
        let response = activate_after(&mut state, 5, BodyKind::JsonLdInsert);
        assert_eq!(response, Response::ErasureActivated { activation: 0 });
    }

    /// A write built from another branch's view too, as a merge or rebase
    /// is from its source, is refused once an erasure reaches that branch,
    /// though none reached the one it writes.
    #[test]
    fn a_write_built_from_a_source_presents_its_generation() {
        let mut state = ledger_with_queued_request();
        advance(&mut state, None, 1, 1, 6_000, 11);
        apply(
            &mut state,
            Command::CreateBranch(NewBranch {
                ledger_id: "test/db".into(),
                branch: "dev".into(),
                source_branch: "main".into(),
                at_commit: None,
                applied_at_millis: 6_000,
            }),
            12,
        );
        let dev = RefKey::new("test/db", "dev");
        activate(&mut state, std::slice::from_ref(&dev));
        let index = |source_generation| Command::AtGeneration {
            generations: vec![(dev.clone(), source_generation)],
            command: Box::new(Command::AdvanceIndexHead(NewIndexHead {
                ledger_id: "test/db".into(),
                branch: "main".into(),
                new_index_head: cid(50),
                t: 1,
                applied_at_millis: 6_000,
            })),
        };
        let stale = apply(&mut state, index(0), 13);
        assert!(generation_moved(&stale), "{stale:?}");
        let current = apply(&mut state, index(1), 14);
        assert!(
            matches!(current, Response::IndexAdvanced { index_t: 1, .. }),
            "{current:?}"
        );
    }

    /// A hold keeps the head where the preparation saw it; it lapses on the
    /// leader's clock, and an activation lifts it.
    #[test]
    fn a_hold_keeps_the_head_until_it_lapses_or_an_activation_lifts_it() {
        let mut state = ledger_with_queued_request();
        let hold = |state: &mut NameServiceState| {
            apply(
                state,
                Command::Erasure(ErasureCommand::Hold {
                    holders: vec![main_key()],
                    until_millis: 8_000,
                }),
                10,
            )
        };
        assert_eq!(hold(&mut state), Response::ErasureHoldApplied);
        let queued = front(&state);
        let held = advance(&mut state, None, 1, 1, 7_000, 11);
        assert_eq!(
            held,
            Response::ErasureHeld {
                ledger_id: "test/db:main".into(),
                until_millis: 8_000
            }
        );
        assert_eq!(front(&state), queued);

        let lapsed = advance(&mut state, None, 1, 1, 8_000, 12);
        assert!(matches!(lapsed, Response::HeadApplied { .. }), "{lapsed:?}");
        assert!(state.erasure.holds.is_empty());

        enqueue(&mut state, 8);
        hold(&mut state);
        activate(&mut state, &[main_key()]);
        let lifted = advance(&mut state, Some(1), 2, 2, 7_000, 13);
        assert!(matches!(lifted, Response::HeadApplied { .. }), "{lifted:?}");

        enqueue(&mut state, 9);
        hold(&mut state);
        apply(
            &mut state,
            Command::Erasure(ErasureCommand::Release {
                holders: vec![main_key()],
            }),
            14,
        );
        let released = advance(&mut state, Some(1), 3, 3, 7_000, 15);
        assert!(
            matches!(released, Response::HeadApplied { .. }),
            "{released:?}"
        );
    }

    /// An index built from a view before an activation would publish the
    /// erased history again, at the same `t` a rewrite accepts.
    #[test]
    fn an_index_built_before_an_activation_does_not_publish() {
        let mut state = ledger_with_queued_request();
        advance(&mut state, None, 1, 1, 6_000, 11);
        activate(&mut state, &[main_key()]);
        let index = |rewrite: bool| {
            let args = NewIndexHead {
                ledger_id: "test/db".into(),
                branch: "main".into(),
                new_index_head: cid(50),
                t: 1,
                applied_at_millis: 6_000,
            };
            if rewrite {
                Command::RewriteIndexHead(args)
            } else {
                Command::AdvanceIndexHead(args)
            }
        };
        for rewrite in [false, true] {
            let stale = apply(&mut state, index(rewrite), 12);
            assert!(generation_moved(&stale), "{stale:?}");
            let stale = apply(&mut state, at(0, index(rewrite)), 12);
            assert!(generation_moved(&stale), "{stale:?}");
        }
        assert!(state.refs[&main_key()].index.is_none());
        let current = apply(&mut state, at(1, index(false)), 13);
        assert!(
            matches!(current, Response::IndexAdvanced { index_t: 1, .. }),
            "{current:?}"
        );
    }

    /// A graph source's index is built from its source branches, so it
    /// presents their generations.
    #[test]
    fn a_graph_source_index_presents_its_sources_generations() {
        let mut state = ledger_with_queued_request();
        apply(
            &mut state,
            Command::PublishGraphSource {
                name: "search".into(),
                branch: "main".into(),
                source_type: GraphSourceType::Bm25,
                config: "{}".into(),
                dependencies: vec!["test/db:main".into()],
            },
            11,
        );
        activate(&mut state, &[main_key()]);
        let publish = Command::PublishGraphSourceIndex {
            name: "search".into(),
            branch: "main".into(),
            index_id: cid(60),
            index_t: 1,
        };
        let stale = apply(&mut state, publish.clone(), 12);
        assert!(generation_moved(&stale), "{stale:?}");
        let current = apply(&mut state, at(1, publish), 13);
        assert!(
            matches!(current, Response::GraphSourceIndexAdvanced { index_t: 1 }),
            "{current:?}"
        );
    }

    /// A member's report is kept under the run it came from; a later run
    /// reports again and replaces it, even when it has installed less.
    #[test]
    fn ready_reports_are_kept_per_member_run() {
        let mut state = ledger_with_queued_request();
        let ready = |state: &mut NameServiceState, through, process| {
            apply(
                state,
                Command::Erasure(ErasureCommand::Ready {
                    through,
                    node: 2,
                    incarnation: Incarnation {
                        process,
                        storage: 1,
                    },
                }),
                20,
            )
        };
        assert_eq!(
            ready(&mut state, 1, 1),
            Response::ErasureActivationUnknown { activation: 0 }
        );
        activate(&mut state, &[main_key()]);
        assert_eq!(ready(&mut state, 1, 1), Response::ErasureReadyRecorded);
        assert_eq!(ready(&mut state, 0, 2), Response::ErasureReadyRecorded);
        assert_eq!(
            state.erasure.installed[&2],
            Installed {
                through: 0,
                incarnation: Incarnation {
                    process: 2,
                    storage: 1
                }
            }
        );
    }

    /// Entries written before counting began count nothing, so a node that
    /// replays them agrees with one restored from a snapshot taken without
    /// erasure state; an activation waits for counting.
    #[test]
    fn holder_additions_count_from_tracking_on() {
        let mut state = NameServiceState::new();
        apply(
            &mut state,
            Command::CreateLedger(NewLedger {
                ledger_id: "test/db".into(),
                branch: "main".into(),
                created_at_millis: 1_000,
            }),
            1,
        );
        assert_eq!(state.erasure, ErasureState::default());
        assert_eq!(
            activate(&mut state, &[main_key()]),
            Response::ErasureActivationRefused {
                refusal: ActivationRefusal::HolderSetChanged { current: 0 }
            }
        );
        apply(&mut state, Command::Erasure(ErasureCommand::Track), 2);
        assert!(matches!(
            activate(&mut state, &[main_key()]),
            Response::ErasureActivated { .. }
        ));
    }

    /// Log entries are positional: these variants can only be appended.
    #[test]
    fn erasure_commands_keep_their_log_positions() {
        let bytes = |command: &Command| postcard::to_allocvec(command).unwrap();
        assert_eq!(bytes(&Command::Erasure(ErasureCommand::Track)), vec![30, 4]);
        let wrapped = Command::AtGeneration {
            generations: Vec::new(),
            command: Box::new(Command::ReleaseContent { id: cid(1) }),
        };
        assert_eq!(bytes(&wrapped)[..3], [29, 0, 8]);
    }

    /// Snapshots deployed clusters hold, taken before erasure state was
    /// appended, still restore; one taken now keeps it.
    #[test]
    fn snapshots_from_before_erasure_state_restore() {
        let mut state = ledger_with_queued_request();
        advance(&mut state, None, 1, 1, 6_000, 11);
        let v2 = super::super::NameServiceStateV2 {
            refs: state.refs.clone(),
            ledgers: state.ledgers.clone(),
            retracted: state.retracted.clone(),
            idempotency: state.idempotency.clone(),
            queues: state.queues.clone(),
            next_queue_id: state.next_queue_id,
            recently_cleared: state.recently_cleared.clone(),
            evicted_idempotency_count: state.evicted_idempotency_count,
            queue_config: state.queue_config,
            status: state.status.clone(),
            config: state.config.clone(),
            graph_sources: state.graph_sources.clone(),
            worker_eligible_voters: state.worker_eligible_voters.clone(),
            configured_voters: state.configured_voters.clone(),
            bindings: state.bindings.clone(),
            dropped: state.dropped.clone(),
            fences: state.fences.clone(),
        };
        let restored =
            NameServiceState::from_snapshot(&postcard::to_allocvec(&v2).unwrap()).unwrap();
        assert_eq!(restored.refs, state.refs);
        assert_eq!(restored.erasure, ErasureState::default());

        activate(&mut state, &[main_key()]);
        let bytes = state.to_snapshot().unwrap();
        let restored = NameServiceState::from_snapshot(&bytes).unwrap();
        assert_eq!(restored, state);
        assert_eq!(generation(&restored, &main_key()), 1);

        // A damaged one is refused, not restored as the older layout
        // without its erasure state.
        assert!(NameServiceState::from_snapshot(&bytes[..bytes.len() - 1]).is_err());
    }
}
