//! Temporal mode for query planning.
//!
//! [`TemporalMode`] captures whether a query is asking about *current state*
//! (the snapshot at `to_t`, with retracts already applied) or *history*
//! (the full event stream of asserts and retracts in `[from_t, to_t]`).
//!
//! This is a **planning input**, not a runtime flag. The mode is detected
//! at the dataset/view layer (see `view::dataset::is_history_mode`) and
//! threaded into the planner via [`PlanningContext`]. The planner picks
//! mode-specific operators at construction time; operators do not branch
//! on temporal mode at runtime.
//!
//! Two source-of-truth sites for the underlying decision stay where they
//! are:
//! - `view/dataset.rs::is_history_mode()` — derives the mode from the
//!   dataset spec (two endpoints to the same ledger with explicit times).
//! - `core/query_bounds.rs::QueryBounds::history_mode` and
//!   `RangeOptions::history_mode` — published parameters for the
//!   `range_with_overlay` core API, below the planner.

/// Whether a query is evaluating current state or full history.
///
/// History queries return the merged stream of assert + retract events
/// across `[from_t, to_t]` with explicit `op` on each emitted binding.
/// Current-state queries collapse retracts and emit only the live state
/// at `to_t`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub enum TemporalMode {
    /// Snapshot query at `to_t`. Retracts are applied; emitted bindings
    /// have no `op`. This is the default for all non-history queries.
    #[default]
    Current,
    /// Full history range `[from_t, to_t]`. Asserts and retracts are
    /// preserved with explicit `op` on each emitted binding.
    History,
}

impl TemporalMode {
    /// Returns `true` for [`TemporalMode::History`].
    #[inline]
    pub const fn is_history(self) -> bool {
        matches!(self, TemporalMode::History)
    }

    /// Returns `true` for [`TemporalMode::Current`].
    #[inline]
    pub const fn is_current(self) -> bool {
        matches!(self, TemporalMode::Current)
    }
}

/// Planning-time context threaded through the operator-tree builder.
///
/// Carries decisions that must be made once at planning and captured at
/// operator construction — never read again at runtime. Currently this
/// is just [`TemporalMode`]; future planning inputs that want the same
/// "decide once, capture at construction" discipline should land here.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub struct PlanningContext {
    /// Temporal mode for this query.
    pub mode: TemporalMode,
    /// Whether the prepare caller has vouched that *semantic* stats-based
    /// rewrites (e.g. eliding a provably-redundant `rdf:type` filter) are sound
    /// for this execution. It is **only** set when the caller knows the query is
    /// current-state, against a single stats domain (one ledger — not a
    /// multi-ledger dataset), and under root policy (no visibility layer that
    /// could hide `rdf:type` differently than the predicate it is proven
    /// redundant against). Defaults to `false`, so any path that does not
    /// explicitly opt in is safe. Folded into `StatsView::class_coverage_trustworthy`
    /// and the stats-cache key so a trusted view is never reused for a
    /// non-vouched (policy / dataset) execution at the same overlay epoch.
    pub allow_semantic_elision: bool,
}

impl PlanningContext {
    /// Construct a planning context for a current-state query.
    #[inline]
    pub const fn current() -> Self {
        Self {
            mode: TemporalMode::Current,
            allow_semantic_elision: false,
        }
    }

    /// Construct a planning context for a history-range query.
    #[inline]
    pub const fn history() -> Self {
        Self {
            mode: TemporalMode::History,
            allow_semantic_elision: false,
        }
    }

    /// Vouch (or not) that semantic stats-based rewrites are sound for this
    /// execution. See [`Self::allow_semantic_elision`]. History plans never
    /// allow it regardless, so this is a no-op in history mode.
    #[inline]
    pub const fn with_semantic_elision(mut self, allow: bool) -> Self {
        self.allow_semantic_elision = allow && self.mode.is_current();
        self
    }

    /// Returns the temporal mode.
    #[inline]
    pub const fn mode(self) -> TemporalMode {
        self.mode
    }

    /// Returns `true` if this is a history-mode plan.
    #[inline]
    pub const fn is_history(self) -> bool {
        self.mode.is_history()
    }
}
