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
//! ## What is deliberately not here
//!
//! No `openraft` dependency. The current surface does not need one:
//! storage payloads are opaque bytes, and [`ClusterNode`] satisfies
//! openraft's blanket `Node` impl through its derives alone. The
//! openraft-facing adapters (log storage, HTTP transport, membership
//! admin, leader forwarding, the state-machine seam) land here next;
//! until then this crate compiles for consumers that only want its
//! storage or hashing pieces.
//!
//! No `fluree-db-*` dependency, and no domain types.

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

pub use group::{GroupId, GroupIdError};
pub use node::{ClusterNode, NodeId};

#[cfg(feature = "raft")]
pub use config::FlureeRaftConfig;
