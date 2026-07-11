//! EXPLAIN seed (PR-1): a minimal, lock-free record of whether a
//! kill-switch-gated fast path was taken (`Proceed`) or skipped in favor of the
//! generic pipeline (`Fallback`).
//!
//! Each decision is emitted as a structured event on a dedicated tracing target
//! ([`FAST_PATH_TARGET`]) so triage and `EXPLAIN` consumers can see the
//! planned-vs-executed fast-path decision without paying for a lock on the hot
//! path (a disabled `tracing` event is a cheap atomic load). The four
//! kill-switch gate sites in `execute::operator_tree` stamp their plan-time
//! decision; the generic [`crate::fast_path_common::FastPathOperator`] stamps
//! its runtime decision when it opens.
//!
//! TODO(PR-3): generalize this seed into a per-operator `GateVerdict` computed
//! once and threaded through the execution context, rendered directly in
//! `ExplainPlan` (planned-vs-executed across ALL fast paths, not just the
//! kill-switch sites). This module and its call sites are the seam that grows
//! into that; the tracing target is the interim surface.

/// Tracing target every fast-path outcome is emitted on. Subscribe to it (e.g.
/// `RUST_LOG=fluree::fastpath=debug`) to observe fast-path routing.
pub const FAST_PATH_TARGET: &str = "fluree::fastpath";

/// Whether a fast path ran, or deferred to its generic fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FastPathOutcome {
    /// The fast path was planned / taken.
    Proceed,
    /// The fast path was skipped; the generic pipeline runs instead.
    Fallback(FastPathFallback),
}

/// Why a fast path deferred to the generic pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FastPathFallback {
    /// The differential kill switch (`FLUREE_DISABLE_QUERY_FAST_PATHS` /
    /// `set_fast_paths_disabled`) disabled fast paths at plan time.
    KillSwitch,
    /// The operator's runtime gate declined (overlay novelty, time-travel,
    /// non-root policy, multi-ledger, or a shape the fast path can't answer).
    GateDeclined,
}

impl FastPathOutcome {
    /// Short, stable label for the tracing field.
    pub fn label(self) -> &'static str {
        match self {
            FastPathOutcome::Proceed => "proceed",
            FastPathOutcome::Fallback(FastPathFallback::KillSwitch) => "fallback:kill_switch",
            FastPathOutcome::Fallback(FastPathFallback::GateDeclined) => "fallback:gate_declined",
        }
    }
}

/// Stamp a fast-path `outcome` for the named gate `site`.
///
/// Emits on [`FAST_PATH_TARGET`]; no allocation, no lock. Called at plan time
/// from the kill-switch gate sites and at `open()` time from `FastPathOperator`.
#[inline]
pub fn stamp_fast_path(site: &'static str, outcome: FastPathOutcome) {
    tracing::debug!(
        target: FAST_PATH_TARGET,
        site,
        outcome = outcome.label(),
        "fast-path outcome",
    );
}
