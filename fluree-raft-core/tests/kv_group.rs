//! A KV fragment inside a real three-node group.
//!
//! Everything in `kv`'s own tests is a single-process reduction of a
//! command vector. That is enough to pin the algebra, and not enough to
//! pin the thing kv exists for: a fence is only a fence if it is
//! decided by the *replicated log*, so a holder that believes it still
//! owns a lease is refused by state a rival already committed — not by
//! a local check the holder could have skipped.
//!
//! What this drives, end to end over HTTP: a `KvFragment` composed into
//! an `AppStateMachine` alongside a tenant registry, lease acquire /
//! renew / lapse / takeover through `client_write`, followers reading
//! the same fence, and the sweep driver reclaiming a backlog through
//! Raft rather than through a direct `apply`.

#![cfg(all(feature = "raft", feature = "kv"))]

// Each test binary includes only the support modules it uses, so a
// binary is never compiled with code it does not reference.
#[path = "support/cluster.rs"]
mod cluster;
#[path = "support/leases.rs"]
mod leases;

use async_trait::async_trait;
use cluster::{eventually, form_cluster, leader, start_node, Node};
use fluree_raft_core::group::GroupId;
use fluree_raft_core::kv::sweep::{run_sweep, sweep_once, SweepConfig, SweepTarget};
use fluree_raft_core::kv::{Expect, KvCommand, KvResponse};
use fluree_raft_core::runtime::RaftGroup;
use leases::{acquire, evict, renew, LeaseCommand, Leases, Tenant};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const SECOND: u64 = 1_000;
const TTL: u64 = 30 * SECOND;

async fn propose(node: &Node<Leases>, command: LeaseCommand) -> KvResponse {
    node.group
        .raft
        .client_write(command)
        .await
        .expect("client_write")
        .data
        .kv()
}

/// The fence a write returned, or a panic naming what came back.
fn fence(response: KvResponse) -> u64 {
    match response {
        KvResponse::Written { version, .. } => version,
        other => panic!("expected a write, got {other:?}"),
    }
}

/// The largest command this app can propose: the biggest key and value
/// any tenant's policy admits, plus room for the command envelope.
/// Declaring it caps openraft's catch-up batch so a full one still fits
/// the append-entries body limit — without it, a follower far enough
/// behind receives a 413 and openraft retries the same oversized batch
/// forever.
fn max_command_bytes() -> u64 {
    let p = leases::policy(Tenant::Config);
    p.max_key_bytes + p.max_value_bytes + 4096
}

async fn cluster(name: &str) -> Vec<Node<Leases>> {
    let group_id = GroupId::new(name).expect("valid group id");
    let tune = |config: &mut fluree_raft_core::runtime::RaftGroupConfig| {
        config.max_command_bytes = Some(max_command_bytes());
    };
    let nodes = vec![
        start_node(1, &group_id, tune).await,
        start_node(2, &group_id, tune).await,
        start_node(3, &group_id, tune).await,
    ];
    form_cluster(&nodes).await;
    nodes
}

/// A group that declares its command size must end up with a catch-up
/// batch that fits the body limit it will be sent against. Otherwise a
/// lagging follower can never rejoin — the failure is silent, total,
/// and only reachable from a node that is already degraded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_catch_up_batch_fits_the_append_body_limit() {
    let nodes = cluster("headroom").await;
    let config = nodes[0].group.raft.config();
    let transport = fluree_raft_core::network::RaftTransportConfig::default();

    assert!(
        config.max_payload_entries < 300,
        "openraft's stock batch must have been capped, got {}",
        config.max_payload_entries,
    );
    assert!(
        config.max_payload_entries * max_command_bytes()
            <= transport.append_entries_max_body_bytes as u64,
        "a full batch ({} x {} bytes) must fit the {}-byte append limit",
        config.max_payload_entries,
        max_command_bytes(),
        transport.append_entries_max_body_bytes,
    );
}

/// Read a lease's live record from one node's local state.
async fn read_lease(node: &Node<Leases>, key: &str, now_ms: u64) -> Option<(Vec<u8>, u64)> {
    let state = node.group.state.read().await;
    state.fragment(Tenant::Lease).and_then(|fragment| {
        fragment
            .get_at(key, now_ms)
            .map(|record| (record.value.to_vec(), record.version))
    })
}

/// The whole point of the module: a lapsed holder is refused by
/// replicated state, and the rival that took over is visible on every
/// replica.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lapsed_holder_is_fenced_out_by_the_replicated_log() {
    let nodes = cluster("leases").await;
    let leader = leader(&nodes).await;

    // Holder A acquires, then renews once — proving a renewal moves
    // the fence rather than reusing it.
    let first = fence(propose(leader, acquire("shard-1", b"holder-a", TTL, 0)).await);
    let renewed = fence(propose(leader, renew("shard-1", b"holder-a", first, TTL, SECOND)).await);
    assert!(
        renewed > first,
        "every renewal must move the fence: {first} -> {renewed}",
    );

    // Every replica converges on the same holder and the same fence.
    for node in &nodes {
        let id = node.id;
        eventually(&format!("node {id} to see the renewed fence"), || async {
            read_lease(node, "shard-1", SECOND).await == Some((b"holder-a".to_vec(), renewed))
        })
        .await;
    }

    // A rival takes over after the lease lapses. Nothing swept — the
    // record is still physically present, and expiry alone is what
    // makes the acquire legal.
    let lapsed = TTL + SECOND;
    let rival = fence(propose(leader, acquire("shard-1", b"holder-b", TTL, lapsed)).await);
    assert!(rival > renewed);

    // Holder A, unaware, tries to renew with the fence it still holds.
    // This is the moment that matters: the refusal comes from the
    // replicated state machine, and it hands back the rival's record so
    // A can tell it was displaced rather than merely unlucky.
    let refused = propose(
        leader,
        renew("shard-1", b"holder-a", renewed, TTL, lapsed + SECOND),
    )
    .await;
    let KvResponse::Conflict {
        current: Some(current),
    } = refused
    else {
        panic!("a lapsed holder must be refused with the current record, got {refused:?}");
    };
    assert_eq!(current.value, b"holder-b");
    assert_eq!(current.version, rival);

    // And a *follower* answers the same, from its own replicated copy.
    for node in &nodes {
        let id = node.id;
        eventually(&format!("node {id} to see the takeover"), || async {
            read_lease(node, "shard-1", lapsed).await == Some((b"holder-b".to_vec(), rival))
        })
        .await;
    }
}

/// A fence must never repeat, including across a lapse — a per-key
/// counter would reset here and reissue a token a stale holder still
/// carries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fences_never_repeat_across_takeovers() {
    let nodes = cluster("fences").await;
    let leader = leader(&nodes).await;

    let mut seen = Vec::new();
    let mut now = 0;
    for round in 0..4 {
        let holder = format!("holder-{round}");
        seen.push(fence(
            propose(leader, acquire("shard-1", holder.as_bytes(), TTL, now)).await,
        ));
        now += TTL + SECOND;
    }

    assert!(
        seen.windows(2).all(|w| w[1] > w[0]),
        "fences must be strictly increasing across takeovers: {seen:?}",
    );
}

/// The tenant registry is real isolation: a `Config` entry may be
/// immortal, the same write against `Lease` is refused, and neither
/// tenant's keys are visible in the other's fragment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenants_carry_their_own_policy_and_their_own_keyspace() {
    let nodes = cluster("tenants").await;
    let leader = leader(&nodes).await;

    let immortal = |tenant| {
        LeaseCommand::Kv(
            tenant,
            KvCommand::Put {
                key: "same-key".into(),
                value: b"v".to_vec(),
                expect: Expect::Any,
                ttl_ms: None,
                now_ms: 0,
            },
        )
    };

    assert!(
        matches!(
            propose(leader, immortal(Tenant::Config)).await,
            KvResponse::Written { .. }
        ),
        "Config allows entries without a ttl",
    );
    assert!(
        matches!(
            propose(leader, immortal(Tenant::Lease)).await,
            KvResponse::Rejected(_)
        ),
        "Lease must refuse an immortal entry — a lease that never expires is a stuck one",
    );

    // Same key string, different tenants, no collision.
    propose(leader, acquire("same-key", b"holder", TTL, 0)).await;
    let state = leader.group.state.read().await;
    assert_eq!(
        state
            .fragment(Tenant::Config)
            .and_then(|f| f.get_at("same-key", 0))
            .map(|r| r.value.to_vec()),
        Some(b"v".to_vec()),
    );
    assert_eq!(
        state
            .fragment(Tenant::Lease)
            .and_then(|f| f.get_at("same-key", 0))
            .map(|r| r.value.to_vec()),
        Some(b"holder".to_vec()),
    );
}

/// `Tenant` is a map key inside a replicated snapshot, so its
/// discriminants are a wire format. Reordering the enum does not error;
/// it silently reassigns one tenant's entries to another.
#[test]
fn tenant_discriminants_are_pinned() {
    assert_eq!(
        postcard::to_allocvec(&Tenant::Lease).expect("encodes"),
        vec![0],
    );
    assert_eq!(
        postcard::to_allocvec(&Tenant::Config).expect("encodes"),
        vec![1],
    );
}

/// The sweep driver against a real group: reclamation goes through the
/// log, so every replica reclaims the same records at the same index.
struct GroupSweep {
    group: Arc<RaftGroup<Leases>>,
    tenant: Tenant,
}

#[async_trait]
impl SweepTarget for GroupSweep {
    type Error = String;

    async fn has_expired(&self, cutoff_ms: u64) -> bool {
        self.group
            .state
            .read()
            .await
            .fragment(self.tenant)
            .is_some_and(|fragment| fragment.has_expired_at(cutoff_ms))
    }

    async fn propose_evict(&self, cutoff_ms: u64, limit: u32) -> Result<KvResponse, Self::Error> {
        self.group
            .raft
            .client_write(evict(self.tenant, cutoff_ms, limit))
            .await
            .map(|written| written.data.kv())
            .map_err(|e| e.to_string())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_sweep_reclaims_a_backlog_through_the_log() {
    let nodes = cluster("sweeper").await;
    let leader_node = leader(&nodes).await;

    for i in 0..10 {
        propose(
            leader_node,
            acquire(&format!("shard-{i:02}"), b"holder", TTL, 0),
        )
        .await;
    }
    propose(leader_node, acquire("kept", b"holder", TTL, TTL)).await;

    let target = GroupSweep {
        group: Arc::clone(&leader_node.group),
        tenant: Tenant::Lease,
    };
    let outcome = sweep_once(
        &target,
        &SweepConfig {
            interval: Duration::from_millis(5),
            batch: 4,
            max_rounds: 8,
        },
        TTL,
        &CancellationToken::new(),
    )
    .await;

    assert_eq!(outcome.removed, 10);
    assert!(outcome.drained);
    assert!(
        outcome.rounds > 1,
        "a batch of 4 against 10 records must take several rounds",
    );

    // Reclamation is replicated: every node, not just the proposer,
    // ends with only the unexpired lease physically present.
    for node in &nodes {
        let id = node.id;
        eventually(&format!("node {id} to reclaim the backlog"), || async {
            node.group
                .state
                .read()
                .await
                .fragment(Tenant::Lease)
                .is_some_and(|fragment| fragment.physical_len() == 1)
        })
        .await;
    }
}

/// The ticker in its intended home: spawned from the leader watcher's
/// task factory, so only a leader proposes eviction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_sweep_ticker_runs_as_a_leader_task() {
    use fluree_raft_core::runtime::{spawn_leader_watcher, DEFAULT_LEADER_TASK_GRACE};

    let nodes = cluster("ticker").await;
    let leader_node = leader(&nodes).await;
    for i in 0..6 {
        propose(
            leader_node,
            acquire(&format!("shard-{i:02}"), b"holder", TTL, 0),
        )
        .await;
    }

    let group = Arc::clone(&leader_node.group);
    let watcher = spawn_leader_watcher(
        Arc::clone(&leader_node.group.raft),
        leader_node.id,
        DEFAULT_LEADER_TASK_GRACE,
        move |cancel: CancellationToken| {
            let target = GroupSweep {
                group: Arc::clone(&group),
                tenant: Tenant::Lease,
            };
            vec![tokio::spawn(async move {
                run_sweep(
                    target,
                    SweepConfig {
                        interval: Duration::from_millis(20),
                        batch: 2,
                        max_rounds: 8,
                    },
                    || TTL,
                    cancel,
                )
                .await;
            })]
        },
    );

    eventually("the ticker to reclaim everything", || async {
        leader_node
            .group
            .state
            .read()
            .await
            .fragment(Tenant::Lease)
            .is_some_and(|fragment| fragment.physical_len() == 0)
    })
    .await;

    watcher.shutdown().await;
}
