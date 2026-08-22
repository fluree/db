//! Nameservice-specific HTTP transport configuration.
//!
//! The generic inter-node transport — the openraft RPC client, the
//! `append-entries` / `vote` / `install-snapshot` routes, and their
//! per-route body caps — lives in [`fluree_raft_core::network`] and is
//! re-exported here at its historical path.
//!
//! What stays is the configuration for the RPCs this crate adds on top
//! of openraft's: the leader-directed `apply_staged_commit` and
//! `apply_queue_poison` forwards the commit worker uses. Those routes
//! are nameservice concepts, so their timeout and body caps do not
//! belong in a crate that knows nothing about staged commits.

use std::time::Duration;

pub use fluree_raft_core::network::{
    build_client, router, HttpRaftNetwork, HttpRaftNetworkFactory, RaftTransportConfig,
};

/// Transport tuning for a nameservice Raft group: the generic
/// [`RaftTransportConfig`] plus this crate's own cross-node RPCs.
#[derive(Clone, Debug)]
pub struct NetworkConfig {
    /// Knobs shared by every Raft group — timeouts and the body caps
    /// for openraft's own three routes plus leader forwarding.
    pub transport: RaftTransportConfig,
    /// Per-request timeout for the worker-initiated cross-node
    /// `apply_staged_commit` / `apply_queue_poison` forwards.
    /// Larger than [`RaftTransportConfig::rpc_timeout`] because the
    /// leader has to commit + apply the proposed entry before
    /// responding (openraft's own RPCs return as soon as the entry is
    /// durably accepted), but bounded so a fully-stalled leader is
    /// detected before the worker loop times out a user's submission.
    pub cross_node_propose_timeout: Duration,
    /// Maximum buffered body size accepted on the cross-node
    /// `apply_staged_commit` route. Body shape: the staged receipt
    /// (mostly small) plus optional tracking tally (policy stats
    /// `HashMap`, reasoning details). The default covers any realistic
    /// single-commit receipt with headroom; without an explicit cap the
    /// route falls back to axum's 2 MiB default and an oversize receipt
    /// 413s into the follower worker's retry-forever path.
    pub apply_staged_commit_max_body_bytes: usize,
    /// Maximum buffered body size accepted on the cross-node
    /// `apply_queue_poison` route. Body shape: a `ref_key`, a
    /// `queue_id`, and a structured `PoisonReason` (a few strings at
    /// most). Tiny compared to the receipt path, but still pinned to a
    /// deliberate cap for parity with the sibling RPCs.
    pub apply_queue_poison_max_body_bytes: usize,
}

// Hand-written rather than derived: a derived `Default` would zero the
// three fields below, which are real tuning values, not absent ones.
// The transport half still defers to its own `Default`.
impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            transport: RaftTransportConfig::default(),
            cross_node_propose_timeout: Duration::from_secs(10),
            apply_staged_commit_max_body_bytes: 16 * 1024 * 1024,
            apply_queue_poison_max_body_bytes: 1024 * 1024,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The generic route caps are pinned in `fluree-raft-core`; this
    /// covers the two routes that stayed behind, including their
    /// relationship to the transport caps they sit inside.
    #[test]
    fn cross_node_route_caps_stay_inside_the_transport_envelope() {
        let cfg = NetworkConfig::default();
        assert_eq!(cfg.apply_staged_commit_max_body_bytes, 16 * 1024 * 1024);
        assert_eq!(cfg.apply_queue_poison_max_body_bytes, 1024 * 1024);
        assert_eq!(cfg.cross_node_propose_timeout, Duration::from_secs(10));

        // Within the append-entries envelope, so an operator who has
        // already tuned that knob upward doesn't get blindsided by a
        // narrower route silently 413-ing under the same workload.
        assert!(
            cfg.apply_staged_commit_max_body_bytes <= cfg.transport.append_entries_max_body_bytes
        );
        assert!(cfg.apply_queue_poison_max_body_bytes <= cfg.apply_staged_commit_max_body_bytes);

        // And both are meaningful against axum's 2 MiB fallback, so the
        // explicit cap is the binding constraint, not the framework
        // default.
        let axum_default = 2 * 1024 * 1024;
        assert!(cfg.apply_staged_commit_max_body_bytes > axum_default);
        assert!(cfg.apply_queue_poison_max_body_bytes >= axum_default / 2);
    }

    /// The propose timeout must exceed the plain RPC timeout: it covers
    /// commit *and* apply on the leader, not just durable acceptance.
    #[test]
    fn propose_timeout_exceeds_rpc_timeout() {
        let cfg = NetworkConfig::default();
        assert!(cfg.cross_node_propose_timeout > cfg.transport.rpc_timeout);
    }

    /// A derived `Default` would silently zero these; assert they are
    /// the documented values, not `0`.
    #[test]
    fn default_does_not_zero_the_nameservice_fields() {
        let cfg = NetworkConfig::default();
        assert!(!cfg.cross_node_propose_timeout.is_zero());
        assert!(cfg.apply_staged_commit_max_body_bytes > 0);
        assert!(cfg.apply_queue_poison_max_body_bytes > 0);
    }
}
