//! Collection's replicated state: the intents condemning blobs for
//! deletion.
//!
//! The leader writes an intent (the addresses it found unreferenced) to
//! shared storage and condemns it ([`CollectCommand::Condemn`]); each node
//! loads it into the set its writers check before storing. A writer that
//! began after loading an intent and needs one of its addresses spares it
//! ([`CollectCommand::Spare`]). A write that began before is fenced
//! instead: it presents how many intents its writer had loaded
//! ([`Command::Seen`](super::Command::Seen)), and once an intent's deletion
//! has begun ([`CollectCommand::Delete`]) a write that began before loading
//! it is refused and staged again. So the references the leader finds once
//! deletion has begun are final.
//!
//! None of this is proposed before [`CollectCommand::Enable`], which the
//! leader proposes once every member checks its writes: a member a release
//! behind cannot decode these commands.

use super::{Command, NameServiceState, RefKind, Response};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// The collection part of [`NameServiceState`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectState {
    /// Set by [`CollectCommand::Enable`]: writes are fenced and intents
    /// condemned from then on.
    pub enabled: bool,
    /// Intents condemned and not yet completed, by number.
    pub intents: BTreeMap<u64, Intent>,
    pub next_intent: u64,
    /// One past the highest intent whose deletion has begun, completed or
    /// not: a write begun before loading it is refused from then on.
    pub deleted_through: u64,
}

/// An applied [`CollectCommand::Condemn`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Intent {
    /// The intent object's hash.
    pub intent: String,
    pub log_index: u64,
    /// Set by [`CollectCommand::Delete`]: nothing is spared after it.
    pub deleting: bool,
    /// Addresses a writer stored again before deletion began.
    pub spared: BTreeSet<String>,
}

/// Replicated collection commands. Variants are positional in the log:
/// append only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CollectCommand {
    /// Condemn the addresses the intent object names. Condemning an open
    /// intent again answers with its number, so a leader whose response
    /// was lost proposes again.
    Condemn { intent: String },
    /// Keep `address` out of intent `number`'s deletion: a writer is
    /// storing it again.
    Spare { number: u64, address: String },
    /// Begin intent `number`'s deletion.
    Delete { number: u64 },
    /// Intent `number`'s deletes are done.
    Complete { number: u64 },
    /// Every member checks its writes against intents: collect from now.
    Enable,
}

/// The intent whose deletion has begun that a write presenting `seen`
/// began before loading, if any. A completed one counts: the write may
/// name what it deleted.
pub(super) fn refused_by(state: &NameServiceState, seen: u64) -> Option<u64> {
    let through = state.collect.deleted_through;
    (seen < through).then(|| through - 1)
}

/// Whether `command`, carrying no [`Command::Seen`], is refused: once the
/// cluster collects, a command publishing what a writer stored must say
/// what its writer had loaded. One built before its leader applied
/// [`CollectCommand::Enable`] is made again.
pub(super) fn unfenced(state: &NameServiceState, command: &Command) -> bool {
    state.collect.enabled
        && match command.admitted() {
            Command::ApplyHead(_) | Command::EnqueueCommand(_) => true,
            Command::CompareAndSetRef(cas) => cas.kind == RefKind::CommitHead,
            _ => false,
        }
}

pub(super) fn apply(
    state: &mut NameServiceState,
    command: CollectCommand,
    log_index: u64,
) -> Response {
    let collect = &mut state.collect;
    match command {
        CollectCommand::Condemn { intent } => {
            if let Some((&number, _)) = collect.intents.iter().find(|(_, i)| i.intent == intent) {
                return Response::Condemned { number };
            }
            let number = collect.next_intent;
            collect.next_intent += 1;
            collect.intents.insert(
                number,
                Intent {
                    intent,
                    log_index,
                    deleting: false,
                    spared: BTreeSet::new(),
                },
            );
            Response::Condemned { number }
        }
        CollectCommand::Spare { number, address } => match collect.intents.get_mut(&number) {
            None => Response::IntentClosed { number },
            Some(intent) if intent.spared.contains(&address) => Response::Spared,
            Some(intent) if intent.deleting => Response::SpareRefused { number },
            Some(intent) => {
                intent.spared.insert(address);
                Response::Spared
            }
        },
        CollectCommand::Delete { number } => match collect.intents.get_mut(&number) {
            None => Response::IntentClosed { number },
            Some(intent) => {
                intent.deleting = true;
                collect.deleted_through = collect.deleted_through.max(number + 1);
                Response::Deleting {
                    spared: intent.spared.iter().cloned().collect(),
                }
            }
        },
        CollectCommand::Complete { number } => {
            collect.intents.remove(&number);
            Response::IntentClosed { number }
        }
        CollectCommand::Enable => {
            collect.enabled = true;
            Response::CollectionEnabled
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{apply as apply_command, Command};
    use super::*;
    use fluree_db_api::{ContentId, ContentKind};

    fn collect(state: &mut NameServiceState, command: CollectCommand) -> Response {
        apply_command(state, Command::Collect(command), 7)
    }

    fn condemn(state: &mut NameServiceState, intent: &str) -> Response {
        collect(
            state,
            CollectCommand::Condemn {
                intent: intent.into(),
            },
        )
    }

    fn spare(state: &mut NameServiceState, number: u64, address: &str) -> Response {
        collect(
            state,
            CollectCommand::Spare {
                number,
                address: address.into(),
            },
        )
    }

    /// Intents are numbered in order; condemning an open one again names
    /// it, and a completed one leaves the open set.
    #[test]
    fn intents_are_numbered_once_and_completed() {
        let mut state = NameServiceState::new();
        assert_eq!(condemn(&mut state, "a"), Response::Condemned { number: 0 });
        assert_eq!(condemn(&mut state, "b"), Response::Condemned { number: 1 });
        assert_eq!(condemn(&mut state, "a"), Response::Condemned { number: 0 });
        assert_eq!(state.collect.next_intent, 2);

        let complete = CollectCommand::Complete { number: 0 };
        assert_eq!(
            collect(&mut state, complete.clone()),
            Response::IntentClosed { number: 0 }
        );
        assert_eq!(state.collect.intents.keys().collect::<Vec<_>>(), [&1]);
        assert_eq!(
            collect(&mut state, complete),
            Response::IntentClosed { number: 0 }
        );
        // Completed, so a later condemnation of the same object is new work.
        assert_eq!(condemn(&mut state, "a"), Response::Condemned { number: 2 });
    }

    /// An address is spared until deletion begins, which reports what was
    /// spared; after that only what is already spared stays so, and a
    /// closed intent spares nothing because nothing is left to delete.
    #[test]
    fn addresses_are_spared_until_deletion_begins() {
        let mut state = NameServiceState::new();
        condemn(&mut state, "a");
        assert_eq!(spare(&mut state, 0, "x"), Response::Spared);
        assert_eq!(
            collect(&mut state, CollectCommand::Delete { number: 0 }),
            Response::Deleting {
                spared: vec!["x".into()]
            }
        );
        assert_eq!(spare(&mut state, 0, "x"), Response::Spared);
        assert_eq!(
            spare(&mut state, 0, "y"),
            Response::SpareRefused { number: 0 }
        );
        collect(&mut state, CollectCommand::Complete { number: 0 });
        assert_eq!(
            spare(&mut state, 0, "y"),
            Response::IntentClosed { number: 0 }
        );
        assert_eq!(
            collect(&mut state, CollectCommand::Delete { number: 0 }),
            Response::IntentClosed { number: 0 }
        );
    }

    /// Once an intent's deletion has begun, a write that began before
    /// loading it is refused, whatever it wraps, and still after the intent
    /// completes; one that began after, or before an intent that is not
    /// deleting, applies.
    #[test]
    fn a_write_begun_before_a_deleting_intent_is_refused() {
        let mut state = NameServiceState::new();
        let release = |seen| Command::Seen {
            seen,
            command: Box::new(Command::ReleaseContent {
                id: ContentId::new(ContentKind::Commit, b"c"),
            }),
        };
        condemn(&mut state, "a");
        condemn(&mut state, "b");
        assert_eq!(apply_command(&mut state, release(0), 8), Response::NoOp);
        collect(&mut state, CollectCommand::Delete { number: 1 });
        assert_eq!(apply_command(&mut state, release(2), 9), Response::NoOp);
        for seen in [0, 1] {
            assert_eq!(
                apply_command(&mut state, release(seen), 10),
                Response::CollectionMoved { deleting: Some(1) }
            );
        }
        collect(&mut state, CollectCommand::Complete { number: 1 });
        assert_eq!(
            apply_command(&mut state, release(1), 11),
            Response::CollectionMoved { deleting: Some(1) }
        );
        assert_eq!(apply_command(&mut state, release(2), 12), Response::NoOp);
    }

    /// Once the cluster collects, a commit-head write must say what its
    /// writer had loaded, whatever else wraps it; before, none need. An
    /// index-head write names nothing collection condemns.
    #[test]
    fn a_write_without_a_fence_is_refused_once_the_cluster_collects() {
        use fluree_db_nameservice::{RefKind, RefValue};
        let mut state = NameServiceState::new();
        let cas = |kind| Command::Fenced {
            fence: 1,
            command: Box::new(Command::CompareAndSetRef(super::super::RefCas {
                ledger_id: "l".into(),
                branch: "main".into(),
                kind,
                expected: None,
                new: RefValue {
                    id: Some(ContentId::new(ContentKind::Commit, b"c")),
                    t: 1,
                },
                applied_at_millis: 0,
            })),
        };
        let refused = Response::CollectionMoved { deleting: None };
        assert_ne!(
            apply_command(&mut state, cas(RefKind::CommitHead), 8),
            refused
        );
        assert_eq!(
            collect(&mut state, CollectCommand::Enable),
            Response::CollectionEnabled
        );
        assert_eq!(
            apply_command(&mut state, cas(RefKind::CommitHead), 9),
            refused
        );
        assert_ne!(
            apply_command(&mut state, cas(RefKind::IndexHead), 10),
            refused
        );
        let fenced = Command::Seen {
            seen: 0,
            command: Box::new(cas(RefKind::CommitHead)),
        };
        assert_ne!(apply_command(&mut state, fenced, 11), refused);
    }

    #[test]
    fn collect_commands_keep_their_log_positions() {
        let bytes = |command: &Command| postcard::to_allocvec(command).unwrap();
        assert_eq!(
            bytes(&Command::Collect(CollectCommand::Complete { number: 1 })),
            vec![31, 3, 1]
        );
        assert_eq!(
            bytes(&Command::Collect(CollectCommand::Enable)),
            vec![31, 4]
        );
        let seen = Command::Seen {
            seen: 2,
            command: Box::new(Command::ReleaseContent {
                id: ContentId::new(ContentKind::Commit, b"c"),
            }),
        };
        assert_eq!(bytes(&seen)[..3], [32, 2, 8]);
    }

    /// Snapshots taken before collection state restore with their erasure
    /// state; one taken now keeps its intents, and a damaged one is refused
    /// rather than restored without them.
    #[test]
    fn snapshots_from_before_collection_state_restore() {
        let mut state = NameServiceState::new();
        state.erasure.next_activation = 3;
        let v3 = super::super::NameServiceStateV3 {
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
            erasure: state.erasure.clone(),
        };
        let restored =
            NameServiceState::from_snapshot(&postcard::to_allocvec(&v3).unwrap()).unwrap();
        assert_eq!(restored, state);

        condemn(&mut state, "a");
        spare(&mut state, 0, "x");
        let bytes = state.to_snapshot().unwrap();
        assert_eq!(NameServiceState::from_snapshot(&bytes).unwrap(), state);
        assert!(NameServiceState::from_snapshot(&bytes[..bytes.len() - 1]).is_err());
    }
}
