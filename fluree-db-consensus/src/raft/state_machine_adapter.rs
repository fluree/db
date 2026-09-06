//! The nameservice's openraft state machine.
//!
//! Both halves now live elsewhere: the generic bookkeeping — last
//! applied, membership, snapshot persistence, the persist-before-swap
//! ordering — in [`fluree_raft_core::state_machine::StateMachineAdapter`],
//! and the nameservice-specific reduction and effects in
//! [`super::app`]. This module is the composition of the two, kept at
//! its historical path so existing imports resolve.
//!
//! Build one with [`NameServiceObserver`] carrying whichever effect
//! sinks this node has:
//!
//! ```ignore
//! let observer = NameServiceObserver::new()
//!     .with_event_bus(bus)
//!     .with_waiter_map(waiters);
//! let sm = StateMachineAdapter::open(storage, observer).await?;
//! ```

pub use super::app::{Effect, NameServiceApp, NameServiceObserver, SharedState};

/// The nameservice's openraft state machine over an
/// `Arc<S: RaftStorage>`.
pub type StateMachineAdapter<S> =
    fluree_raft_core::state_machine::StateMachineAdapter<NameServiceApp, NameServiceObserver, S>;
