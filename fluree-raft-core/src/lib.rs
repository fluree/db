//! Application-agnostic Raft substrate.
//!
//! This crate is the generic half of what began life inside
//! `fluree-db-consensus::raft`, where it was fused to the nameservice
//! state machine. Nothing here knows what is being replicated: the
//! storage traits deal in opaque `Vec<u8>` payloads, and the node and
//! group types carry only addressing and identity.
//!
//! The intended shape is one instantiation per replicated concern —
//! independent groups with separate logs, elections, and snapshots,
//! co-hosted in a single process — rather than one shared log.
//!
//! ## What is here
//!
//! - [`storage`] — the durable log/vote/snapshot abstraction plus a
//!   filesystem backend (atomic write, fsync, rename) and an in-memory
//!   backend for tests.
//! - [`node`] — [`NodeId`] and [`ClusterNode`], the address pair that
//!   travels through membership changes.
//! - [`group`] — [`GroupId`], the validated name of one group within a
//!   process.
//! - [`ownership`] — rendezvous hashing for assigning work to members
//!   without a consensus round.
//! - [`http`] — hop-by-hop header classification for request
//!   forwarding.
//!
//! Under the `raft` feature, which gates `openraft`:
//!
//! - [`config`] — [`FlureeRaftConfig`], the constrained openraft profile
//!   every group shares. Applications still write their own
//!   `declare_raft_types!`; a blanket impl covers what it produces.
//! - [`state_machine`] — the application seam: deterministic reduction
//!   in `AppStateMachine`, effects in `StateMachineObserver`, and the
//!   adapter that drives openraft from the pair.
//! - [`log_adapter`], [`network`], [`admin`], [`forward`] — the
//!   openraft-facing adapters: log storage, HTTP transport, membership
//!   administration, and follower→leader forwarding.
//! - [`runtime`] — group bootstrap and the leader-only task lifecycle.
//!
//! And under `testing`, [`testing`] — a conformance fixture any
//! `AppStateMachine` can be run through.
//!
//! ## What is deliberately not here
//!
//! No `fluree-db-*` dependency, and no domain types.
//!
//! Without the `raft` feature there is no `openraft` dependency either:
//! storage payloads are opaque bytes and [`ClusterNode`] satisfies
//! openraft's blanket `Node` impl through its derives alone. That is
//! what keeps monolithic Fluree builds — which reach this crate through
//! `fluree-db-consensus` for [`http::is_hop_by_hop`] — from compiling or
//! linking openraft.

pub mod group;
pub mod http;
pub mod node;
pub mod ownership;
pub mod storage;

#[cfg(feature = "raft")]
pub mod admin;
#[cfg(feature = "raft")]
pub mod config;
#[cfg(feature = "raft")]
pub mod forward;
#[cfg(feature = "raft")]
pub mod log_adapter;
#[cfg(feature = "raft")]
pub mod network;
#[cfg(feature = "raft")]
pub mod runtime;
#[cfg(feature = "raft")]
pub mod state_machine;
#[cfg(feature = "testing")]
pub mod testing;

pub use group::{GroupId, GroupIdError};
pub use node::{ClusterNode, NodeId};

#[cfg(feature = "raft")]
pub use config::FlureeRaftConfig;
