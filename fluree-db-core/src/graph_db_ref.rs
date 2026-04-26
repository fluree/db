//! Bundled database reference for range queries.
//!
//! `GraphDbRef<'a>` combines the four values that always travel together
//! through leaf-crate function signatures:
//!
//! - `snapshot` — the indexed ledger snapshot
//! - `g_id` — which named graph to query
//! - `overlay` — novelty / staged flakes
//! - `t` — upper bound for visible flakes (as-of time)
//!
//! # Time semantics
//!
//! `GraphDbRef.t` is **the db value's as-of time**: the upper bound for
//! visible flakes, including overlay.  It is the responsibility of the
//! bridge/constructor to set this correctly:
//!
//! - `GraphDb.as_graph_db_ref()` → `self.t`
//! - `LedgerState.as_graph_db_ref(g_id)` → `max(novelty.t, snapshot.t)`
//! - `StagedLedger.as_graph_db_ref(g_id)` → `base.t() + 1` when staged
//!
//! `from_t` is NOT part of the db value identity — history range queries
//! pass it via `RangeOptions` or as a separate parameter.

use crate::comparator::IndexType;
use crate::db::LedgerSnapshot;
use crate::error::Result;
use crate::flake::Flake;
use crate::ids::GraphId;
use crate::overlay::OverlayProvider;
use crate::query_bounds::{RangeMatch, RangeOptions, RangeTest};
use crate::range::{range_bounded_with_overlay, range_with_overlay_tracked};
use crate::runtime_small_dicts::RuntimeSmallDicts;

/// Bundled database reference for range queries.
///
/// Combines the snapshot, graph id, overlay, and as-of time that are
/// always passed together through 40+ function signatures.
///
/// `Copy` — all fields are references or primitives.
#[derive(Clone, Copy)]
pub struct GraphDbRef<'a> {
    pub snapshot: &'a LedgerSnapshot,
    pub g_id: GraphId,
    pub overlay: &'a dyn OverlayProvider,
    pub t: i64,
    pub runtime_small_dicts: Option<&'a RuntimeSmallDicts>,
    /// When true, queries built from this ref disable late materialization
    /// in `BinaryScanOperator`, always returning resolved `Binding::Sid`/`Lit`
    /// instead of `EncodedSid`/`EncodedLit`.
    ///
    /// Use for infrastructure queries (config resolution, policy loading) that
    /// call `binding.as_sid()` / `binding.as_lit()` directly.
    pub eager: bool,
    /// Optional fuel tracker. When set, `range()` / `range_with_opts()` charge
    /// 1 micro-fuel per flake returned. This catches read paths (e.g., SHACL
    /// validation) that don't go through the cursor's per-leaflet charge.
    pub tracker: Option<&'a crate::tracking::Tracker>,
}

impl std::fmt::Debug for GraphDbRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("GraphDbRef");
        s.field("g_id", &self.g_id).field("t", &self.t);
        if self.eager {
            s.field("eager", &true);
        }
        s.finish_non_exhaustive()
    }
}

impl<'a> GraphDbRef<'a> {
    /// Create a new `GraphDbRef`.
    pub fn new(
        snapshot: &'a LedgerSnapshot,
        g_id: GraphId,
        overlay: &'a dyn OverlayProvider,
        t: i64,
    ) -> Self {
        Self {
            snapshot,
            g_id,
            overlay,
            t,
            runtime_small_dicts: None,
            eager: false,
            tracker: None,
        }
    }

    /// Attach a fuel tracker. When set, `range`/`range_with_opts` charge
    /// 1 micro-fuel per flake returned.
    pub fn with_tracker(mut self, tracker: &'a crate::tracking::Tracker) -> Self {
        if tracker.is_enabled() {
            self.tracker = Some(tracker);
        }
        self
    }

    #[inline]
    fn charge_range_fuel(&self, n_flakes: usize) -> Result<()> {
        if let Some(t) = self.tracker {
            t.consume_fuel(n_flakes as u64)?;
        }
        Ok(())
    }

    pub fn with_runtime_small_dicts(mut self, runtime_small_dicts: &'a RuntimeSmallDicts) -> Self {
        self.runtime_small_dicts = Some(runtime_small_dicts);
        self
    }

    pub fn with_runtime_small_dicts_opt(
        self,
        runtime_small_dicts: Option<&'a RuntimeSmallDicts>,
    ) -> Self {
        match runtime_small_dicts {
            Some(runtime_small_dicts) => self.with_runtime_small_dicts(runtime_small_dicts),
            None => self,
        }
    }

    /// Return a copy with a different as-of time.
    pub fn with_t(mut self, t: i64) -> Self {
        self.t = t;
        self
    }

    /// Return a copy with eager materialization enabled.
    ///
    /// When set, queries built from this `GraphDbRef` will never use
    /// late-materialized `Binding::EncodedSid`/`EncodedLit` forms, even
    /// when novelty is empty (epoch=0). Use this for infrastructure queries
    /// (config resolution, policy loading) that call `binding.as_sid()` /
    /// `binding.as_lit()` directly.
    pub fn eager(mut self) -> Self {
        self.eager = true;
        self
    }

    /// Execute a range query, auto-filling `to_t` from `self.t`.
    ///
    /// This is the primary convenience method — eliminates the need for
    /// callers to manually set `RangeOptions::default().with_to_t(...)`.
    pub async fn range(
        &self,
        index: IndexType,
        test: RangeTest,
        match_val: RangeMatch,
    ) -> Result<Vec<Flake>> {
        let opts = RangeOptions::default().with_to_t(self.t);
        let flakes = range_with_overlay_tracked(
            self.snapshot,
            self.g_id,
            self.overlay,
            index,
            test,
            match_val,
            opts,
            self.tracker,
        )
        .await?;
        // Per-flake baseline (1 micro-fuel each) covers materialization cost and
        // novelty-only paths where no leaflet/dict touch was charged below.
        self.charge_range_fuel(flakes.len())?;
        Ok(flakes)
    }

    /// Execute a range query with explicit options, auto-filling `to_t`
    /// from `self.t` if the caller hasn't set it.
    pub async fn range_with_opts(
        &self,
        index: IndexType,
        test: RangeTest,
        match_val: RangeMatch,
        opts: RangeOptions,
    ) -> Result<Vec<Flake>> {
        let opts = if opts.to_t.is_none() {
            opts.with_to_t(self.t)
        } else {
            opts
        };
        let flakes = range_with_overlay_tracked(
            self.snapshot,
            self.g_id,
            self.overlay,
            index,
            test,
            match_val,
            opts,
            self.tracker,
        )
        .await?;
        self.charge_range_fuel(flakes.len())?;
        Ok(flakes)
    }

    /// Execute a bounded range query with explicit start/end flakes,
    /// auto-filling `to_t` from `self.t` if the caller hasn't set it.
    pub async fn range_bounded(
        &self,
        index: IndexType,
        start_bound: Flake,
        end_bound: Flake,
        opts: RangeOptions,
    ) -> Result<Vec<Flake>> {
        let opts = if opts.to_t.is_none() {
            opts.with_to_t(self.t)
        } else {
            opts
        };
        range_bounded_with_overlay(
            self.snapshot,
            self.g_id,
            self.overlay,
            index,
            start_bound,
            end_bound,
            opts,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overlay::NoOverlay;

    #[test]
    fn test_graph_db_ref_is_copy() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let overlay = NoOverlay;
        let db = GraphDbRef::new(&snapshot, 0, &overlay, 1);
        // Copy semantics — both bindings valid after copy
        let db2 = db;
        assert_eq!(db.t, db2.t);
        assert_eq!(db.g_id, db2.g_id);
    }

    #[test]
    fn test_eager_flag() {
        let snapshot = LedgerSnapshot::genesis("test:eager");
        let overlay = NoOverlay;
        let db = GraphDbRef::new(&snapshot, 0, &overlay, 1);
        assert!(!db.eager);

        let eager_db = db.eager();
        assert!(eager_db.eager);
        assert_eq!(eager_db.t, db.t);
        assert_eq!(eager_db.g_id, db.g_id);
    }

    #[test]
    fn test_with_t_preserves_flags() {
        let snapshot = LedgerSnapshot::genesis("test:with-t");
        let overlay = NoOverlay;
        let dicts = RuntimeSmallDicts::default();
        let db = GraphDbRef::new(&snapshot, 0, &overlay, 1)
            .with_runtime_small_dicts(&dicts)
            .eager();

        let shifted = db.with_t(42);
        assert_eq!(shifted.t, 42);
        assert!(shifted.eager);
        assert!(shifted.runtime_small_dicts.is_some());
    }

    #[tokio::test]
    async fn test_range_auto_fills_to_t() {
        let snapshot = LedgerSnapshot::genesis("test:main");
        let overlay = NoOverlay;
        let db = GraphDbRef::new(&snapshot, 0, &overlay, 0);
        // Genesis + NoOverlay → empty result, but should not error
        let result = db
            .range(IndexType::Spot, RangeTest::Eq, RangeMatch::new())
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}
