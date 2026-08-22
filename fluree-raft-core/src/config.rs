//! The constrained openraft profile every Fluree Raft group shares.
//!
//! openraft's [`RaftTypeConfig`] leaves eight associated types open. The
//! adapters in this crate do not need that much freedom, and threading
//! the full set of bounds through every `impl` makes each one unreadable.
//! [`FlureeRaftConfig`] pins the four that are genuinely fixed across all
//! Fluree groups and leaves `D`, `R`, `AsyncRuntime`, and `Responder` to
//! the application.
//!
//! Each application still writes its own `declare_raft_types!`; this trait
//! is a bound, not a replacement. The blanket impl means a type config
//! that satisfies the pins gets it automatically:
//!
//! ```ignore
//! openraft::declare_raft_types!(
//!     pub TypeConfig:
//!         D = MyCommand,
//!         R = MyResponse,
//!         NodeId = fluree_raft_core::NodeId,
//!         Node = fluree_raft_core::ClusterNode,
//!         Entry = openraft::Entry<TypeConfig>,
//!         SnapshotData = std::io::Cursor<Vec<u8>>,
//!         AsyncRuntime = openraft::TokioRuntime,
//! );
//! // `TypeConfig: FlureeRaftConfig` now holds.
//! ```
//!
//! ## Why each pin
//!
//! - `NodeId = u64` — the id is a bare integer everywhere: log file
//!   names, metrics labels, rendezvous scoring.
//! - `Node = ClusterNode` — the address *pair* has to travel through
//!   membership, because the leader-forward middleware resolves a peer's
//!   client URL from replicated membership rather than from local config.
//!   This is the pin that makes `forward` possible at all.
//! - `Entry = openraft::Entry<C>` — the log adapter serializes entries
//!   with postcard and stores them as opaque bytes; it needs the concrete
//!   entry type to round-trip them.
//! - `SnapshotData = Cursor<Vec<u8>>` — snapshots are built in memory
//!   from a whole-state encode and handed to the storage backend as
//!   bytes.
//! - `Responder = OneshotResponder<C>` — openraft gates its *blocking*
//!   client-write API (`add_learner`, `change_membership`, and
//!   `client_write`) on this exact responder; without the pin, the
//!   membership admin surface simply does not exist on `Raft<C>`. This
//!   is what `declare_raft_types!` defaults to, so pinning it costs
//!   applications nothing and buys a comprehensible error when someone
//!   overrides it.
//!
//! `D` and `R` already carry `Serialize + DeserializeOwned` through
//! openraft's `AppData` / `AppDataResponse` supertraits under the `serde`
//! feature, so this trait does not restate them.

use crate::node::{ClusterNode, NodeId};
use openraft::impls::OneshotResponder;
use std::io::Cursor;

/// Marker for a `RaftTypeConfig` shaped the way this crate's adapters
/// expect. Blanket-implemented; do not implement it by hand.
pub trait FlureeRaftConfig:
    openraft::RaftTypeConfig<
    NodeId = NodeId,
    Node = ClusterNode,
    Entry = openraft::Entry<Self>,
    SnapshotData = Cursor<Vec<u8>>,
    Responder = OneshotResponder<Self>,
>
{
}

impl<C> FlureeRaftConfig for C where
    C: openraft::RaftTypeConfig<
        NodeId = NodeId,
        Node = ClusterNode,
        Entry = openraft::Entry<C>,
        SnapshotData = Cursor<Vec<u8>>,
        Responder = OneshotResponder<C>,
    >
{
}

#[cfg(test)]
mod tests {
    use super::*;

    openraft::declare_raft_types!(
        /// A type config with the pins satisfied.
        pub ConformingConfig:
            D = String,
            R = String,
            NodeId = NodeId,
            Node = ClusterNode,
            Entry = openraft::Entry<ConformingConfig>,
            SnapshotData = Cursor<Vec<u8>>,
            AsyncRuntime = openraft::TokioRuntime,
    );

    fn assert_conforms<C: FlureeRaftConfig>() {}

    /// The membership admin surface only exists when `Responder` is
    /// pinned; this is the call that fails to resolve without it.
    fn assert_blocking_write_available<C: FlureeRaftConfig>(raft: &openraft::Raft<C>) {
        let _ = || async { raft.change_membership([1u64], false).await };
    }

    /// The blanket impl covers a config declared the ordinary way, so an
    /// application never names `FlureeRaftConfig` in its declaration.
    #[test]
    fn declare_raft_types_output_satisfies_the_profile() {
        assert_conforms::<ConformingConfig>();
        // Referenced so the bound above is actually type-checked.
        let _ = assert_blocking_write_available::<ConformingConfig>;
    }
}
