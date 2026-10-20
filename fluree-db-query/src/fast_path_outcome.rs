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

use crate::context::ExecutionContext;
use crate::index_scope::IndexScope;

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
    /// A lane over several graphs (`IndexScope::Union`) declined because the
    /// index alone does not answer for some member: it carries novelty, or it
    /// is read at a `t` below the index.
    Novelty,
    /// A lane over several graphs declined because its members are not all
    /// read through one store and one dictionary novelty, so their ids are not
    /// comparable.
    StoreMismatch,
    /// A lane over several graphs met an arena-backed `o_type` (NumBig,
    /// vector). Its encoded key is a handle per `(g_id, p_id)`, so equal keys in
    /// two members need not be equal values.
    ArenaType,
    /// A lane merging several graphs in POST met list rows (`o_i` set). Two
    /// rows of one fact can be separated there, so dropping adjacent
    /// duplicates would not remove every one.
    ListRowsInPost,
}

impl FastPathOutcome {
    /// Short, stable label for the tracing field.
    pub fn label(self) -> &'static str {
        match self {
            FastPathOutcome::Proceed => "proceed",
            FastPathOutcome::Fallback(FastPathFallback::KillSwitch) => "fallback:kill_switch",
            FastPathOutcome::Fallback(FastPathFallback::GateDeclined) => "fallback:gate_declined",
            FastPathOutcome::Fallback(FastPathFallback::Novelty) => "fallback:novelty",
            FastPathOutcome::Fallback(FastPathFallback::StoreMismatch) => "fallback:store_mismatch",
            FastPathOutcome::Fallback(FastPathFallback::ArenaType) => "fallback:arena_type",
            FastPathOutcome::Fallback(FastPathFallback::ListRowsInPost) => {
                "fallback:list_rows_in_post"
            }
        }
    }
}

/// Stamp a fast-path `outcome` for the named gate `site`.
///
/// Emits on [`FAST_PATH_TARGET`]; no allocation, no lock. Called at plan time
/// from the kill-switch gate sites, at `open()` time from `FastPathOperator`,
/// and mid-execution from gates that can only decide once they have seen data
/// (`MembershipJoinOperator::build_key_set` stamps on its first fully-bound
/// row). A new site may stamp at whichever point its gate actually decides.
#[inline]
pub fn stamp_fast_path(site: &'static str, outcome: FastPathOutcome) {
    tracing::debug!(
        target: FAST_PATH_TARGET,
        site,
        outcome = outcome.label(),
        "fast-path outcome",
    );
}

/// Stamp what `ctx` reads through the binary index
/// ([`ExecutionContext::index_scope`]) for the scope `site` opens: `scope` is
/// `one`, `union`, `empty`, or `none` where no index lane may run; `members`
/// counts the graphs it reads; `current` says whether the index alone answers
/// for every one of them. Computed only when the event is enabled, so a
/// per-row call site costs one interest check.
#[inline]
pub fn stamp_index_scope(site: &'static str, ctx: &ExecutionContext<'_>) {
    if !tracing::enabled!(target: FAST_PATH_TARGET, tracing::Level::DEBUG) {
        return;
    }
    let (scope, members, current) = match ctx.index_scope() {
        Some(IndexScope::One(member)) => ("one", 1, member.index_is_current()),
        Some(IndexScope::Union(set)) => ("union", set.members().len(), set.all_current()),
        Some(IndexScope::Empty) => ("empty", 0, true),
        None => ("none", 0, false),
    };
    tracing::debug!(
        target: FAST_PATH_TARGET,
        site,
        scope,
        members,
        current,
        "index scope",
    );
}
