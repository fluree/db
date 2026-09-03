//! Node identity for a Raft cluster member.

use serde::{Deserialize, Serialize};

/// Identifier for a node in a Raft cluster.
///
/// Plain `u64`; the address pair (raft RPC URL + client-facing URL) is
/// carried on the [`ClusterNode`] entries supplied at cluster-membership
/// time.
pub type NodeId = u64;

/// Address pair for a Raft cluster member.
///
/// Replaces openraft's `BasicNode` so both endpoints — the inter-node
/// Raft RPC URL **and** the client-facing URL the follower-forward
/// middleware needs — travel together through membership changes.
/// Storing both inside the Raft state machine means adding a peer at
/// runtime makes its client URL immediately resolvable on every other
/// node, no restart required.
///
/// The derives here are exactly openraft's blanket `Node` bound
/// (`Clone + Debug + Default + Eq + PartialEq + Serialize +
/// Deserialize + Send + Sync + 'static`), which is why this crate can
/// define the type without depending on openraft.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterNode {
    /// Base URL of the peer's inter-node Raft RPC endpoint, e.g.
    /// `"http://node-2:9090/raft"`.
    pub raft_addr: String,
    /// Base URL of the peer's client-facing endpoint, e.g.
    /// `"http://node-2:8080"`. Consumed by the follower-forward
    /// middleware to relay leader-only requests.
    pub client_addr: String,
}

impl ClusterNode {
    pub fn new(raft_addr: impl Into<String>, client_addr: impl Into<String>) -> Self {
        Self {
            raft_addr: raft_addr.into(),
            client_addr: client_addr.into(),
        }
    }
}

impl std::fmt::Display for ClusterNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ClusterNode {{ raft: {}, client: {} }}",
            self.raft_addr, self.client_addr
        )
    }
}
