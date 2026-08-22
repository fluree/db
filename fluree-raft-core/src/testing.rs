//! Conformance fixture for [`StateMachineAdapter`] implementations.
//!
//! The adapter's contract has parts that are easy to state and easy to
//! get subtly wrong — persist before swapping, restore before replaying,
//! keep membership bookkeeping openraft can read back, publish effects
//! only after the lock drops. This module runs an
//! [`AppStateMachine`] through all of them.
//!
//! It exists because the extraction leaves two adapters in the tree for
//! a while: the generic one here and the nameservice's bespoke one.
//! Running both through the same fixture is what keeps them from
//! quietly diverging. Application-specific behavior — waiter
//! resolution, receipt stashing, cache watermarks — stays in each
//! application's own tests; this covers only what every adapter owes
//! openraft.
//!
//! Available under the `testing` feature.

use crate::state_machine::{AppStateMachine, StateMachineAdapter, StateMachineObserver};
use crate::storage::RaftStorage;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, Membership, SnapshotMeta};
use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

/// What the fixture needs from a caller: how to build a fresh adapter
/// over a given storage handle, and a couple of sample commands.
pub trait ConformanceHarness: Send + Sync + 'static {
    type App: AppStateMachine;
    type Observer: StateMachineObserver<Self::App>;
    type Storage: RaftStorage;

    /// A fresh storage backend. Each call must yield an independent
    /// one — the fixture opens two adapters over separate stores to
    /// exercise snapshot transfer.
    fn storage(&self) -> Arc<Self::Storage>;

    /// A fresh observer.
    fn observer(&self) -> Self::Observer;

    /// A command whose effect on the state is observable through
    /// [`Self::probe`]. Called with an increasing index so the fixture
    /// can apply several distinguishable commands.
    fn command(&self, n: u64) -> <Self::App as AppStateMachine>::Command;

    /// Summarize the state as a comparable value. Two states that
    /// probe equal must be interchangeable for the application.
    fn probe(&self, state: &<Self::App as AppStateMachine>::State) -> u64;
}

fn log_id(term: u64, index: u64) -> LogId<u64> {
    LogId {
        leader_id: CommittedLeaderId::new(term, 0),
        index,
    }
}

fn normal<H: ConformanceHarness>(h: &H, term: u64, index: u64) -> Entry<AppConfig<H>> {
    Entry {
        log_id: log_id(term, index),
        payload: EntryPayload::Normal(h.command(index)),
    }
}

type AppConfig<H> = <<H as ConformanceHarness>::App as AppStateMachine>::Config;
type Adapter<H> = StateMachineAdapter<
    <H as ConformanceHarness>::App,
    <H as ConformanceHarness>::Observer,
    <H as ConformanceHarness>::Storage,
>;

async fn open<H: ConformanceHarness>(h: &H, storage: Arc<H::Storage>) -> Adapter<H> {
    StateMachineAdapter::open(storage, h.observer())
        .await
        .expect("adapter opens")
}

/// Run every conformance check. Panics with a description on failure.
pub async fn run_all<H: ConformanceHarness>(h: &H) {
    applied_state_tracks_the_last_entry(h).await;
    boot_restore_resumes_after_the_snapshot(h).await;
    snapshot_is_point_in_time(h).await;
    install_persists_before_it_swaps(h).await;
    install_failure_leaves_state_untouched(h).await;
    membership_survives_restart(h).await;
    blank_and_membership_entries_still_answer(h).await;
    snapshot_ids_are_path_safe(h).await;
}

/// `applied_state` must report the last entry seen, or openraft replays
/// from the wrong place after a restart.
pub async fn applied_state_tracks_the_last_entry<H: ConformanceHarness>(h: &H) {
    let mut sm = open(h, h.storage()).await;
    assert!(
        sm.applied_state().await.expect("applied_state").0.is_none(),
        "a fresh adapter has applied nothing",
    );

    sm.apply(vec![normal(h, 1, 1), normal(h, 1, 2)])
        .await
        .expect("apply");
    let (last, _) = sm.applied_state().await.expect("applied_state");
    assert_eq!(
        last.map(|id| id.index),
        Some(2),
        "applied_state must track the last applied entry",
    );
}

/// The restart path: state and `last_applied` come back from the
/// snapshot, so replay resumes at `last_applied + 1` rather than
/// re-running a log that has been purged.
pub async fn boot_restore_resumes_after_the_snapshot<H: ConformanceHarness>(h: &H) {
    let storage = h.storage();
    let expected = {
        let mut sm = open(h, Arc::clone(&storage)).await;
        sm.apply(vec![normal(h, 1, 1), normal(h, 1, 2), normal(h, 1, 3)])
            .await
            .expect("apply");
        let mut builder = sm.get_snapshot_builder().await;
        builder.build_snapshot().await.expect("build snapshot");
        h.probe(&*sm.shared_state().read().await)
    };

    // Simulate a restart over the same storage.
    let mut reopened = open(h, storage).await;
    assert_eq!(
        h.probe(&*reopened.shared_state().read().await),
        expected,
        "boot restore must reload the snapshot's state",
    );
    let (last, _) = reopened.applied_state().await.expect("applied_state");
    assert_eq!(
        last.map(|id| id.index),
        Some(3),
        "boot restore must reload last_applied so replay resumes after it",
    );
}

/// A snapshot reflects the moment the builder was obtained, not
/// whenever the build happens to finish — otherwise its `last_log_id`
/// would describe state it does not contain.
pub async fn snapshot_is_point_in_time<H: ConformanceHarness>(h: &H) {
    let mut sm = open(h, h.storage()).await;
    sm.apply(vec![normal(h, 1, 1)]).await.expect("apply");
    let at_snapshot = h.probe(&*sm.shared_state().read().await);

    let mut builder = sm.get_snapshot_builder().await;
    // More applies land between obtaining the builder and building.
    sm.apply(vec![normal(h, 1, 2), normal(h, 1, 3)])
        .await
        .expect("apply");
    let snap = builder.build_snapshot().await.expect("build snapshot");

    assert_eq!(
        snap.meta.last_log_id.map(|id| id.index),
        Some(1),
        "snapshot meta must describe the builder's instant",
    );
    let decoded =
        <H::App as AppStateMachine>::decode_snapshot(snap.snapshot.get_ref()).expect("decode");
    assert_eq!(
        h.probe(&decoded),
        at_snapshot,
        "snapshot contents must match the builder's instant, not the latest state",
    );
}

/// Install must persist the bytes before swapping the live state. A
/// node that swaps first and dies restarts claiming a `last_applied`
/// it has no snapshot for.
pub async fn install_persists_before_it_swaps<H: ConformanceHarness>(h: &H) {
    let source = h.storage();
    let (meta, bytes) = {
        let mut sm = open(h, Arc::clone(&source)).await;
        sm.apply(vec![normal(h, 1, 1), normal(h, 1, 2)])
            .await
            .expect("apply");
        let mut builder = sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.expect("build snapshot");
        (snap.meta.clone(), snap.snapshot.into_inner())
    };

    let target_storage = h.storage();
    let mut target = open(h, Arc::clone(&target_storage)).await;
    target
        .install_snapshot(&meta, Box::new(Cursor::new(bytes)))
        .await
        .expect("install");

    // Durable, not just live: a fresh adapter over the same storage
    // must come back with the installed state.
    let mut reopened = open(h, target_storage).await;
    let (last, _) = reopened.applied_state().await.expect("applied_state");
    assert_eq!(
        last.map(|id| id.index),
        meta.last_log_id.map(|id| id.index),
        "an installed snapshot must be durable before the state swap",
    );
    assert_eq!(
        h.probe(&*reopened.shared_state().read().await),
        h.probe(&*target.shared_state().read().await),
        "the persisted snapshot must match the state that was swapped in",
    );
}

/// A snapshot that cannot be decoded must leave the node exactly as it
/// was — a half-installed state machine is worse than a lagging one.
pub async fn install_failure_leaves_state_untouched<H: ConformanceHarness>(h: &H) {
    let mut sm = open(h, h.storage()).await;
    sm.apply(vec![normal(h, 1, 1)]).await.expect("apply");
    let before_probe = h.probe(&*sm.shared_state().read().await);
    let before_applied = sm.applied_state().await.expect("applied_state").0;

    let meta = SnapshotMeta {
        last_log_id: Some(log_id(9, 99)),
        last_membership: Default::default(),
        snapshot_id: "snap-99-1".to_string(),
    };
    let garbage = vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0x01, 0x02];
    let result = sm
        .install_snapshot(&meta, Box::new(Cursor::new(garbage)))
        .await;

    assert!(result.is_err(), "an undecodable snapshot must be refused");
    assert_eq!(
        h.probe(&*sm.shared_state().read().await),
        before_probe,
        "a refused install must not disturb the published state",
    );
    assert_eq!(
        sm.applied_state().await.expect("applied_state").0,
        before_applied,
        "a refused install must not advance last_applied",
    );
}

/// Membership rides in its own entry payload, not through `Command`.
/// The adapter has to record it, and a restart has to read it back —
/// otherwise a restarted node forgets who its peers are.
pub async fn membership_survives_restart<H: ConformanceHarness>(h: &H) {
    let storage = h.storage();
    let mut nodes = BTreeMap::new();
    for id in [1u64, 2, 3] {
        nodes.insert(
            id,
            crate::node::ClusterNode::new(
                format!("http://node-{id}:9090/raft"),
                format!("http://node-{id}:8080"),
            ),
        );
    }
    let membership = Membership::new(vec![[1u64, 2, 3].into_iter().collect()], nodes);

    {
        let mut sm = open(h, Arc::clone(&storage)).await;
        sm.apply(vec![
            normal(h, 1, 1),
            Entry {
                log_id: log_id(1, 2),
                payload: EntryPayload::Membership(membership.clone()),
            },
        ])
        .await
        .expect("apply");

        let (_, stored) = sm.applied_state().await.expect("applied_state");
        assert_eq!(
            stored.membership().voter_ids().collect::<Vec<_>>(),
            vec![1, 2, 3],
            "a membership entry must update stored membership",
        );

        let mut builder = sm.get_snapshot_builder().await;
        builder.build_snapshot().await.expect("build snapshot");
    }

    let mut reopened = open(h, storage).await;
    let (_, stored) = reopened.applied_state().await.expect("applied_state");
    assert_eq!(
        stored.membership().voter_ids().collect::<Vec<_>>(),
        vec![1, 2, 3],
        "membership must survive a restart via the snapshot",
    );
}

/// openraft's contract is one response per entry, including the blank
/// entry each new leader commits and membership changes. Returning
/// fewer desynchronizes openraft's response routing.
pub async fn blank_and_membership_entries_still_answer<H: ConformanceHarness>(h: &H) {
    let mut sm = open(h, h.storage()).await;
    let responses = sm
        .apply(vec![
            Entry {
                log_id: log_id(1, 1),
                payload: EntryPayload::Blank,
            },
            normal(h, 1, 2),
            Entry {
                log_id: log_id(1, 3),
                payload: EntryPayload::Membership(Membership::new(
                    vec![[1u64].into_iter().collect()],
                    BTreeMap::from([(1u64, crate::node::ClusterNode::default())]),
                )),
            },
        ])
        .await
        .expect("apply");
    assert_eq!(
        responses.len(),
        3,
        "every entry owes openraft exactly one response",
    );
}

/// Snapshot ids become filesystem path components in the fs backend,
/// so the adapter must never generate one that a path-safety check
/// would reject.
///
/// This covers the adapter's own ids only. Whether a *backend* rejects
/// a hostile id supplied by a peer is that backend's contract, tested
/// where the validation lives — a memory backend has no paths and
/// rightly does not check.
pub async fn snapshot_ids_are_path_safe<H: ConformanceHarness>(h: &H) {
    let mut sm = open(h, h.storage()).await;
    sm.apply(vec![normal(h, 1, 1)]).await.expect("apply");
    let mut builder = sm.get_snapshot_builder().await;
    let snap = builder.build_snapshot().await.expect("build snapshot");

    let id = &snap.meta.snapshot_id;
    assert!(!id.is_empty(), "snapshot id must not be empty");
    for bad in ["..", "/", "\\", "\0", ":"] {
        assert!(
            !id.contains(bad),
            "generated snapshot id {id:?} must not contain {bad:?}",
        );
    }
}
