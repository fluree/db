//! Raft-replicated consensus. Compiled only under the `raft` feature
//! so non-replicated builds don't take the openraft dependency.
//!
//! Submission flow across the cluster:
//!
//! 1. [`queued_transactor::QueuedTransactor`] builds a
//!    [`crate::QueuedRequest`] envelope from a `Committer` call,
//!    writes it to shared content-addressed storage, and proposes
//!    [`state_machine::Command::EnqueueCommand`] through Raft.
//!    The propose itself is leader-only — a transactor running on a
//!    follower receives `ForwardToLeader` from openraft and the
//!    server-level forward middleware retargets the HTTP request at
//!    the current leader.
//! 2. The state machine appends a `QueueEntry` on the target branch's
//!    FIFO queue and assigns a `queue_id`. Every node sees the
//!    enqueue when it applies. The transactor registers a waiter on
//!    the per-process [`waiter::WaiterMap`].
//! 3. The node-lifetime [`commit_worker::WorkerSupervisor`] runs on
//!    every cluster member (leader and followers alike). Each tick
//!    it computes the desired set — branches whose rendezvous-hash
//!    owner over the current voter set resolves to this node — and
//!    reconciles its running [`commit_worker::Worker`]s against it.
//!    A worker drains its branch's queue, stages the work locally,
//!    writes the commit blob, stashes the typed receipt in
//!    [`staged_receipt::StagedReceiptMap`], and publishes the head
//!    advance through the [`fluree_db_nameservice::CommitPublisher`]
//!    impl on [`nameservice::RaftNameService`]. On the leader that
//!    proposes [`state_machine::Command::ApplyHead`] via
//!    `client_write`; on a follower it ferries the staged receipt to
//!    the leader's `apply_staged_commit` HTTP endpoint, which
//!    proposes the same command from the leader's side. The same
//!    forwarding shape covers [`state_machine::Command::PoisonQueueEntry`]
//!    when a worker hits a deterministic failure.
//! 4. The [`state_machine_adapter::StateMachineAdapter`] applies
//!    `ApplyHead`, takes the stashed receipt, and resolves the
//!    waiter. The transactor's `await` returns the typed receipt.
//!
//! See `docs/design/raft-command-queue.md` for the full design.
//!
//! # Threat model
//!
//! Every inter-node RPC in this crate — the openraft replication
//! protocol ([`network`]), the cross-node propose forwards
//! ([`nameservice`]'s `apply_staged_commit` / `apply_queue_poison`),
//! and the client-facing leader-forward middleware ([`forward`]) —
//! assumes a **peer-trusted** deployment posture:
//!
//! - All cluster nodes are reachable to one another over a private
//!   network (VPC / dedicated subnet / equivalent firewall
//!   boundary). External access reaches the cluster only through an
//!   explicit load balancer with a curated port allowlist.
//! - Peers are equally trusted. Compromise of any single node is
//!   assumed to compromise the entire cluster — there is no honest-
//!   party-among-malicious-peers guarantee, because a compromised
//!   follower can already win elections, refuse to replicate, vote
//!   against quorum, and propose arbitrary `client_write` commands
//!   through normal raft.
//! - The cluster-admin endpoints ([`admin`]) carry no auth of
//!   their own; mount points are expected to layer credential
//!   middleware over `/cluster/*`. The in-tree server applies
//!   `routes::admin_auth::require_admin_token` to that subtree,
//!   but the middleware is a pass-through when the operator
//!   hasn't set `admin_auth_mode = Required` in `ServerConfig` —
//!   the default is `None`, so an out-of-the-box deployment leans
//!   entirely on the network perimeter for admin protection.
//!   Embedders that go through `RaftIntegration::private_router`
//!   instead of the in-tree assembly get no layer at all and must
//!   wrap the router themselves. The consensus RPCs under
//!   `/raft/*` carry no authentication regardless of admin
//!   configuration.
//!
//! Consequences of this posture, and what it leaves the code
//! responsible for:
//!
//! - **No per-RPC caller-identity verification** on
//!   `apply_staged_commit` / `apply_queue_poison`. A peer that can
//!   reach these endpoints is, by assumption, already inside the
//!   trust boundary. Adding owner-of-`ref_key` checks would not
//!   buy anything against a malicious peer (who can simply skip the
//!   forward and use openraft directly).
//! - **Operator-error guards do still apply.** [`forward`]'s SSRF
//!   filter rejects loopback / link-local / unspecified peer URLs
//!   when this node isn't on loopback itself, catching the case
//!   where a hand-edited or fat-fingered `client_addr` /
//!   `raft_addr` would redirect every follower's forward at the
//!   wrong target.
//! - **Postcard decode and `DefaultBodyLimit::max` per-route caps**
//!   protect against malformed or oversized bodies regardless of
//!   source. They guard against bugs and crash conditions, not
//!   adversarial peers.
//!
//! If a future deployment shape (multi-cluster federation, public
//! peer joins, etc.) loosens the peer-trust assumption, every RPC
//! handler in this crate needs an authentication layer above it —
//! the load-bearing assumption is intentionally not duplicated
//! per-endpoint.

pub mod app;
pub mod commit_worker;
pub mod eviction_scheduler;
pub mod liveness_monitor;
pub mod nameservice;
pub mod network;
pub mod ownership;
pub mod queued_transactor;
pub mod staged_receipt;
pub mod state_machine;
pub mod state_machine_adapter;
pub mod waiter;

pub use state_machine::{Command, Response};

// Moved to `fluree-raft-core`. Re-exported at their historical
// paths so downstream `fluree_db_consensus::raft::storage::...`,
// `::NodeId`, and `::ClusterNode` imports keep resolving.
pub use fluree_raft_core::admin;
pub use fluree_raft_core::forward;
pub use fluree_raft_core::log_adapter;
pub use fluree_raft_core::runtime;
pub use fluree_raft_core::storage;
pub use fluree_raft_core::{ClusterNode, NodeId};

openraft::declare_raft_types!(
    /// Type config wiring [`Command`] / [`Response`] into openraft.
    pub TypeConfig:
        D = Command,
        R = Response,
        NodeId = NodeId,
        Node = ClusterNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

/// Wall-clock `now` as milliseconds since the Unix epoch.
///
/// Saturates to `0` if the system clock is set before the epoch —
/// preserves the `u64` return type without panicking. Used to stamp
/// `applied_at_millis` on the state-machine command payloads
/// (`HeadAdvance`, `EntryPoisoning`, `NewLedger`, ...) and as the
/// `applied_at_millis` on the eligibility / eviction proposes.
pub(crate) fn current_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
