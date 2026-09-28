//! End-to-end test: a single-node Raft<TypeConfig> built from our
//! adapters processes a Command::CreateLedger through openraft and
//! produces a Response::Created.
//!
//! Stub network — single-node mode never has peers, so the RPC
//! methods are wired to panic. If openraft ever calls one in this
//! configuration, that's a real bug to investigate, and a panic in
//! a test is louder than a silent unimplemented!().

#![cfg(feature = "raft")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use fluree_db_api::{ContentId, ContentKind};
use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{Config, Raft, ServerState};

use fluree_db_consensus::raft::log_adapter::LogAdapter;
use fluree_db_consensus::raft::nameservice::RaftNameService;
use fluree_db_consensus::raft::state_machine::{
    BodyKind, Command as SmCommand, NewLedger, QueueSubmission, RefKey, Response, StagedHead,
};
use fluree_db_consensus::raft::state_machine_adapter::{NameServiceObserver, StateMachineAdapter};
use fluree_db_consensus::raft::storage::memory::MemoryRaftStorage;
use fluree_db_consensus::raft::{ClusterNode, NodeId, TypeConfig};
use fluree_db_core::{LedgerId, LedgerName};
use fluree_db_nameservice::lifecycle::{self, BranchDrop};
use fluree_db_nameservice::testing::CurrentFence;
use fluree_db_nameservice::{
    ConfigCasResult, ConfigLookup, ConfigValue, LedgerEventBus, NameServiceError, NameServiceEvent,
    NameServiceLookup, NsRecordSnapshot, StatusCasResult, StatusLookup, StatusValue,
    SubscriptionScope,
};

struct StubFactory;
struct StubNetwork;

impl RaftNetworkFactory<TypeConfig> for StubFactory {
    type Network = StubNetwork;

    async fn new_client(&mut self, _target: NodeId, _node: &ClusterNode) -> Self::Network {
        StubNetwork
    }
}

impl RaftNetwork<TypeConfig> for StubNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, ClusterNode, RaftError<NodeId>>>
    {
        panic!("single-node Raft should never invoke append_entries");
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, ClusterNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        panic!("single-node Raft should never invoke install_snapshot");
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, ClusterNode, RaftError<NodeId>>> {
        panic!("single-node Raft should never invoke vote");
    }
}

fn drain(sub: &mut fluree_db_nameservice::Subscription) -> Vec<NameServiceEvent> {
    std::iter::from_fn(|| sub.receiver.try_recv().ok()).collect()
}

fn lid(s: &str) -> LedgerId {
    LedgerId::parse(s).unwrap()
}

fn cid(seed: u8) -> ContentId {
    ContentId::new(ContentKind::Commit, &[seed])
}

#[tokio::test]
async fn single_node_create_ledger_round_trip() {
    let storage = Arc::new(MemoryRaftStorage::new());
    let log = LogAdapter::new(Arc::clone(&storage));
    let sm = StateMachineAdapter::new(Arc::clone(&storage), NameServiceObserver::new());

    // Tight timing so the test doesn't dawdle.
    let config = Config {
        cluster_name: "single-node-test".into(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        ..Config::default()
    };
    let config = Arc::new(config.validate().unwrap());

    let raft = Raft::new(1, config, StubFactory, log, sm).await.unwrap();

    // Bootstrap as a single-member cluster.
    let mut members = BTreeMap::new();
    members.insert(1u64, ClusterNode::default());
    raft.initialize(members).await.unwrap();

    // Wait for self-election. With one node and a configured timeout,
    // this should happen well within a second.
    raft.wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "leader after self-election")
        .await
        .unwrap();

    let cmd = SmCommand::CreateLedger(NewLedger {
        ledger_id: "test/db".into(),
        branch: "main".into(),
        created_at_millis: 1_000,
    });
    let resp = raft.client_write(cmd).await.unwrap();
    match resp.data {
        Response::Created { ref ledger_id } => assert_eq!(ledger_id, "test/db:main"),
        other => panic!("expected Created, got {other:?}"),
    }

    raft.shutdown().await.unwrap();
}

#[tokio::test]
async fn single_node_raft_index_publisher_round_trip() {
    let storage = Arc::new(MemoryRaftStorage::new());
    let log = LogAdapter::new(Arc::clone(&storage));
    let sm = StateMachineAdapter::new(Arc::clone(&storage), NameServiceObserver::new());
    let shared_state = sm.shared_state();

    let config = Config {
        cluster_name: "single-node-index-publisher".into(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        ..Config::default()
    };
    let config = Arc::new(config.validate().unwrap());

    let raft = Arc::new(Raft::new(1, config, StubFactory, log, sm).await.unwrap());

    let mut members = BTreeMap::new();
    members.insert(1u64, ClusterNode::default());
    raft.initialize(members).await.unwrap();

    raft.wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "leader after self-election")
        .await
        .unwrap();

    // Create the ledger and a commit so the index publish has something
    // to attach to.
    let ns = RaftNameService::new(shared_state.clone(), Arc::clone(&raft));
    lifecycle::create_ledger(&ns, &LedgerId::parse("test/db:main").unwrap())
        .await
        .expect("create");

    // Drive the queue path end-to-end: enqueue a fake transaction
    // envelope, then apply its head. Equivalent setup to the
    // previously-used `AdvanceRef` but exercising the real
    // post-migration code path.
    let enqueue_resp = raft
        .client_write(SmCommand::EnqueueCommand(QueueSubmission {
            ledger_id: "test/db".into(),
            branch: "main".into(),
            idempotency: None,
            request_cid: cid(99),
            body_cid: cid(99),
            body_kind: BodyKind::JsonLdInsert,
            applied_at_millis: 1_500,
        }))
        .await
        .unwrap();
    let queue_id = match enqueue_resp.data {
        Response::Enqueued { queue_id, .. } => queue_id,
        other => panic!("expected Enqueued, got {other:?}"),
    };
    raft.client_write(SmCommand::ApplyHead(StagedHead {
        ledger_id: "test/db".into(),
        branch: "main".into(),
        queue_id,
        commit_id: cid(7),
        commit_t: 10,
        applied_at_millis: 2_000,
        tally: None,
        flake_count: 0,
    }))
    .await
    .unwrap();

    // Publish through the combined RaftNameService.
    let raft_arc = raft;
    ns.publish_index("test/db:main", 10, &cid(42))
        .await
        .expect("publish_index ok");

    // The state machine's RefEntry should now carry the index.
    {
        let state = shared_state.read().await;
        let entry = state
            .refs
            .get(&RefKey::new("test/db", "main"))
            .expect("ref entry");
        let index = entry.index.as_ref().expect("index populated");
        assert_eq!(index.head, cid(42));
        assert_eq!(index.t, 10);
    }

    // The same handle's `lookup` observes the new index head — the
    // combined type unifies reads and writes.
    let record = ns
        .lookup("test/db:main")
        .await
        .expect("lookup ok")
        .expect("record");
    assert_eq!(record.index_head_id, Some(cid(42)));
    assert_eq!(record.index_t, 10);

    // A second publish at the same t is treated as stale and
    // surfaces as Ok — the cluster's view is unchanged.
    ns.publish_index("test/db:main", 10, &cid(99))
        .await
        .expect("stale publish is ok");
    {
        let state = shared_state.read().await;
        let entry = state.refs.get(&RefKey::new("test/db", "main")).unwrap();
        let index = entry.index.as_ref().unwrap();
        // Original head preserved — second publish was stale.
        assert_eq!(index.head, cid(42));
        assert_eq!(index.t, 10);
    }

    raft_arc.shutdown().await.unwrap();
}

#[tokio::test]
async fn single_node_apply_emits_commit_event_on_bus() {
    // Wires the state-machine adapter to a LedgerEventBus and
    // proves that going through the full openraft pipeline (propose
    // → quorum → apply) results in a `LedgerCommitPublished` event on
    // the bus — exactly the path the indexer worker subscribes to.

    let storage = Arc::new(MemoryRaftStorage::new());
    let event_bus = Arc::new(LedgerEventBus::new(16));
    let log = LogAdapter::new(Arc::clone(&storage));
    let sm = StateMachineAdapter::new(
        Arc::clone(&storage),
        NameServiceObserver::new().with_event_bus(Arc::clone(&event_bus)),
    );

    let config = Config {
        cluster_name: "single-node-event-bus".into(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        ..Config::default()
    };
    let config = Arc::new(config.validate().unwrap());
    let raft = Raft::new(1, config, StubFactory, log, sm).await.unwrap();

    let mut members = BTreeMap::new();
    members.insert(1u64, ClusterNode::default());
    raft.initialize(members).await.unwrap();

    raft.wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "leader after self-election")
        .await
        .unwrap();

    // Subscribe BEFORE the AdvanceRef proposal so the event lands in
    // the receiver's buffer when apply emits it.
    let mut sub = event_bus.subscribe(SubscriptionScope::All);

    raft.client_write(SmCommand::CreateLedger(NewLedger {
        ledger_id: "test/db".into(),
        branch: "main".into(),
        created_at_millis: 1_000,
    }))
    .await
    .unwrap();
    // CreateLedger doesn't carry a published-commit semantic — the
    // bus stays quiet.
    assert!(
        sub.receiver.try_recv().is_err(),
        "CreateLedger should not emit a commit event"
    );

    // Drive the queue path end-to-end: enqueue a fake transaction
    // envelope, then apply its head. Equivalent setup to the
    // previously-used `AdvanceRef` but exercising the real
    // post-migration code path.
    let enqueue_resp = raft
        .client_write(SmCommand::EnqueueCommand(QueueSubmission {
            ledger_id: "test/db".into(),
            branch: "main".into(),
            idempotency: None,
            request_cid: cid(99),
            body_cid: cid(99),
            body_kind: BodyKind::JsonLdInsert,
            applied_at_millis: 1_500,
        }))
        .await
        .unwrap();
    let queue_id = match enqueue_resp.data {
        Response::Enqueued { queue_id, .. } => queue_id,
        other => panic!("expected Enqueued, got {other:?}"),
    };
    raft.client_write(SmCommand::ApplyHead(StagedHead {
        ledger_id: "test/db".into(),
        branch: "main".into(),
        queue_id,
        commit_id: cid(7),
        commit_t: 10,
        applied_at_millis: 2_000,
        tally: None,
        flake_count: 0,
    }))
    .await
    .unwrap();

    // The AdvanceRef Applied response should have triggered an
    // emission. Try-recv to keep the test deterministic — the event
    // is already on the broadcast buffer by the time client_write
    // returns (apply emits before returning the Response).
    match sub.receiver.try_recv().expect("commit event present") {
        NameServiceEvent::LedgerCommitPublished {
            ledger_id,
            commit_id,
            commit_t,
        } => {
            assert_eq!(ledger_id, "test/db:main");
            assert_eq!(commit_id, cid(7));
            assert_eq!(commit_t, 10);
        }
        other => panic!("expected LedgerCommitPublished, got {other:?}"),
    }

    raft.shutdown().await.unwrap();
}

#[tokio::test]
async fn single_node_branch_lifecycle_round_trip() {
    // create main → seed head → create branch feature → reset_head on
    // main → drop feature — driven through the lifecycle protocols on
    // RaftNameService.

    let storage = Arc::new(MemoryRaftStorage::new());
    let bus = Arc::new(LedgerEventBus::new(16));
    let log = LogAdapter::new(Arc::clone(&storage));
    let sm = StateMachineAdapter::new(
        Arc::clone(&storage),
        NameServiceObserver::new().with_event_bus(Arc::clone(&bus)),
    );
    let shared_state = sm.shared_state();

    let config = Config {
        cluster_name: "single-node-branch-lifecycle".into(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        ..Config::default()
    };
    let config = Arc::new(config.validate().unwrap());
    let raft = Arc::new(Raft::new(1, config, StubFactory, log, sm).await.unwrap());

    let mut members = BTreeMap::new();
    members.insert(1u64, ClusterNode::default());
    raft.initialize(members).await.unwrap();
    raft.wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "leader after self-election")
        .await
        .unwrap();

    let ns = RaftNameService::new(shared_state.clone(), Arc::clone(&raft));
    let mut sub = bus.subscribe(SubscriptionScope::All);

    // Set up: init main and seed it with a head so create_branch
    // has something to fork from. Drive the queue path end-to-end.
    lifecycle::create_ledger(&ns, &LedgerId::parse("test/db:main").unwrap())
        .await
        .expect("create main");
    let enqueue_resp = raft
        .client_write(SmCommand::EnqueueCommand(QueueSubmission {
            ledger_id: "test/db".into(),
            branch: "main".into(),
            idempotency: None,
            request_cid: cid(99),
            body_cid: cid(99),
            body_kind: BodyKind::JsonLdInsert,
            applied_at_millis: 500,
        }))
        .await
        .unwrap();
    let queue_id = match enqueue_resp.data {
        Response::Enqueued { queue_id, .. } => queue_id,
        other => panic!("expected Enqueued, got {other:?}"),
    };
    raft.client_write(SmCommand::ApplyHead(StagedHead {
        ledger_id: "test/db".into(),
        branch: "main".into(),
        queue_id,
        commit_id: cid(1),
        commit_t: 5,
        applied_at_millis: 1_000,
        tally: None,
        flake_count: 0,
    }))
    .await
    .unwrap();
    // Activating main announces it, naming its ledger.
    let instance = ns
        .lookup("test/db:main")
        .await
        .unwrap()
        .and_then(|r| r.instance())
        .expect("main resolves with its instance");
    let created = |ledger_id: &str| NameServiceEvent::LedgerCreated {
        ledger_id: lid(ledger_id),
        instance: instance.clone(),
    };
    let events = drain(&mut sub);
    assert!(events.contains(&created("test/db:main")), "{events:?}");

    // Fork feature from main.
    let name = LedgerName::parse("test/db").unwrap();
    lifecycle::create_branch(&ns, &name, "feature", "main", None)
        .await
        .expect("create_branch");
    let feature = ns
        .lookup("test/db:feature")
        .await
        .unwrap()
        .expect("feature record");
    assert_eq!(feature.commit_head_id, Some(cid(1)));
    assert_eq!(feature.source_branch, Some("main".to_string()));
    // main's child count went up.
    let main = ns.lookup("test/db:main").await.unwrap().expect("main");
    assert_eq!(main.branches, 1);

    // create_branch fires a LedgerCommitPublished against the new
    // branch so the indexer picks it up, and announces it.
    let events = drain(&mut sub);
    assert!(
        events.iter().any(|e| matches!(
            e,
            NameServiceEvent::LedgerCommitPublished { ledger_id, .. }
                if *ledger_id == lid("test/db:feature")
        )),
        "{events:?}"
    );
    assert!(events.contains(&created("test/db:feature")), "{events:?}");

    // The root branch cannot be dropped on its own.
    assert!(lifecycle::begin_drop_branch(&ns, &name, "main")
        .await
        .is_err());

    // reset_head rewrites main's head non-monotonically. Forwards
    // through the same client_write path.
    ns.reset_head(
        "test/db:main",
        NsRecordSnapshot {
            commit_head_id: Some(cid(0)),
            commit_t: 0,
            index_head_id: None,
            index_t: 0,
        },
    )
    .await
    .expect("reset_head");
    let main = ns.lookup("test/db:main").await.unwrap().unwrap();
    assert_eq!(main.commit_head_id, Some(cid(0)));
    assert_eq!(main.commit_t, 0);

    // Drop feature: a leaf, so its record goes, and main is left without
    // children.
    let BranchDrop::Purge(record) = lifecycle::begin_drop_branch(&ns, &name, "feature")
        .await
        .expect("drop")
    else {
        panic!("a leaf branch drop purges");
    };
    let parent = lifecycle::finish_drop_branch(&ns, &name, &record)
        .await
        .expect("finish drop");
    assert!(parent.is_none(), "main is not dropped, so nothing cascades");
    assert!(ns.lookup("test/db:feature").await.unwrap().is_none());
    assert_eq!(
        ns.lookup("test/db:main").await.unwrap().unwrap().branches,
        0
    );

    let events = drain(&mut sub);
    assert!(
        events.iter().any(|e| matches!(
            e,
            NameServiceEvent::LedgerRetracted { ledger_id, .. }
                if *ledger_id == lid("test/db:feature")
        )),
        "{events:?}"
    );

    // Dropping a missing branch surfaces NotFound.
    assert!(matches!(
        lifecycle::begin_drop_branch(&ns, &name, "ghost").await,
        Err(NameServiceError::NotFound(_))
    ));

    // A ledger drop names the ledger in its retraction; a restore announces
    // it again.
    drain(&mut sub);
    lifecycle::drop_ledger(&ns, &name, false)
        .await
        .expect("drop ledger");
    let events = drain(&mut sub);
    let retracted = NameServiceEvent::LedgerRetracted {
        ledger_id: lid("test/db:main"),
        instance: Some(instance.clone()),
    };
    assert!(events.contains(&retracted), "{events:?}");
    lifecycle::restore_dropped(&ns, &instance)
        .await
        .expect("restore");
    let events = drain(&mut sub);
    assert!(events.contains(&created("test/db:main")), "{events:?}");

    raft.shutdown().await.unwrap();
}

/// Status/config pushes and reads address the same replicated entry
/// regardless of ledger-id form — `"test/db"` and `"test/db:main"`
/// name the same branch. Pushes canonicalize before proposing and
/// reads canonicalize before lookup; without that, mixed-form
/// callers got two divergent CAS streams for one branch (a status
/// pushed as `"test/db:main"` was invisible to
/// `get_status("test/db")`, which returned `initial`).
///
/// Writing this test originally exposed that these commands could
/// not be written to the raft log at all: `StatusPayload` /
/// `ConfigPayload` are HTTP-shaped (`#[serde(flatten)]`), which
/// postcard rejects, and the failure surfaced as a raft fatal.
/// Values now travel and persist as the postcard-safe
/// `StoredStatus` / `StoredConfig` forms — this round trip pins
/// that end-to-end alongside the id normalization.
#[tokio::test]
async fn single_node_status_config_round_trip_normalizes_ledger_id() {
    let storage = Arc::new(MemoryRaftStorage::new());
    let log = LogAdapter::new(Arc::clone(&storage));
    let sm = StateMachineAdapter::new(Arc::clone(&storage), NameServiceObserver::new());
    let shared_state = sm.shared_state();

    let config = Config {
        cluster_name: "single-node-status-config".into(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        ..Config::default()
    };
    let config = Arc::new(config.validate().unwrap());
    let raft = Arc::new(Raft::new(1, config, StubFactory, log, sm).await.unwrap());

    let mut members = BTreeMap::new();
    members.insert(1u64, ClusterNode::default());
    raft.initialize(members).await.unwrap();
    raft.wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "leader after self-election")
        .await
        .unwrap();

    let ns = RaftNameService::new(shared_state.clone(), Arc::clone(&raft));
    lifecycle::create_ledger(&ns, &LedgerId::parse("test/db:main").unwrap())
        .await
        .expect("create");

    // Push under the bare name; read under the full form.
    let pushed_status = StatusValue::new(2, Default::default());
    let result = ns
        .push_status("test/db", Some(&StatusValue::initial()), &pushed_status)
        .await
        .expect("push_status ok");
    assert!(
        matches!(result, StatusCasResult::Updated),
        "expected Updated, got {result:?}"
    );
    assert_eq!(
        ns.get_status("test/db:main").await.expect("get_status ok"),
        Some(pushed_status.clone())
    );

    // And the reverse: push the full form, read the bare name.
    let next_status = StatusValue::new(3, Default::default());
    let result = ns
        .push_status("test/db:main", Some(&pushed_status), &next_status)
        .await
        .expect("push_status ok");
    assert!(
        matches!(result, StatusCasResult::Updated),
        "expected Updated, got {result:?}"
    );
    assert_eq!(
        ns.get_status("test/db").await.expect("get_status ok"),
        Some(next_status)
    );

    // Config: bare-name push, full-form read.
    let pushed_config = ConfigValue::new(1, None);
    let result = ns
        .push_config("test/db", Some(&ConfigValue::unborn()), &pushed_config)
        .await
        .expect("push_config ok");
    assert!(
        matches!(result, ConfigCasResult::Updated),
        "expected Updated, got {result:?}"
    );
    assert_eq!(
        ns.get_config("test/db:main").await.expect("get_config ok"),
        Some(pushed_config)
    );

    raft.shutdown().await.unwrap();
}
