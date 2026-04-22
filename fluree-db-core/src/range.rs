//! Range query implementation
//!
//! This module provides the public `range` API for querying flakes from an index.
//! All queries delegate to the `RangeProvider` attached to the `LedgerSnapshot`.
//!
//! ## Example
//!
//! ```ignore
//! use fluree_db_core::{range, IndexType, RangeTest, RangeMatch, RangeOptions};
//!
//! let flakes = range(
//!     &snapshot,
//!     IndexType::Spot,
//!     RangeTest::Eq,
//!     RangeMatch::subject(subject_sid),
//!     RangeOptions::default(),
//! ).await?;
//! ```

// Re-export query parameter types from their canonical home.
pub use crate::query_bounds::{ObjectBounds, RangeMatch, RangeOptions, RangeTest};

use crate::comparator::IndexType;
use crate::db::LedgerSnapshot;
use crate::dt_compatible;
use crate::error::Result;
use crate::flake::Flake;
use crate::ids::GraphId;
use crate::overlay::{NoOverlay, OverlayProvider};
use crate::sid::Sid;
use crate::value::FlakeValue;

/// Batch size constant for batched subject joins.
///
/// When `NestedLoopJoinOperator` accumulates left rows for the batched seek path,
/// it flushes after this many Sid-bearing left rows.
pub const BATCHED_JOIN_SIZE: usize = 100_000;

/// Execute a range query on a database
///
/// Returns flakes matching the query criteria in index order.
///
/// # Arguments
///
/// * `snapshot` - The database snapshot to query
/// * `index` - Which index to use
/// * `test` - Comparison operator (=, <, <=, >, >=)
/// * `match_val` - Components to match
/// * `opts` - Query options (limits, offset)
pub async fn range(
    snapshot: &LedgerSnapshot,
    g_id: GraphId,
    index: IndexType,
    test: RangeTest,
    match_val: RangeMatch,
    opts: RangeOptions,
) -> Result<Vec<Flake>> {
    range_with_overlay(snapshot, g_id, &NoOverlay, index, test, match_val, opts).await
}

/// Execute a range query with an overlay provider (novelty).
///
/// Delegates to the `RangeProvider` attached to the `LedgerSnapshot`.  For genesis
/// databases (t=0, no provider), returns overlay-only flakes.
///
/// The overlay is graph-aware: per-graph novelty returns only flakes belonging
/// to the requested `g_id`, so no post-filtering is needed.
pub async fn range_with_overlay<O>(
    snapshot: &LedgerSnapshot,
    g_id: GraphId,
    overlay: &O,
    index: IndexType,
    test: RangeTest,
    match_val: RangeMatch,
    opts: RangeOptions,
) -> Result<Vec<Flake>>
where
    O: OverlayProvider + ?Sized,
{
    range_with_overlay_tracked(snapshot, g_id, overlay, index, test, match_val, opts, None).await
}

/// Tracker-aware variant of [`range_with_overlay`]. Threads `tracker` to the
/// underlying [`crate::range_provider::RangeProvider::range_tracked`] so dict
/// touches and leaflet decodes can be charged.
#[allow(clippy::too_many_arguments)]
pub async fn range_with_overlay_tracked<O>(
    snapshot: &LedgerSnapshot,
    g_id: GraphId,
    overlay: &O,
    index: IndexType,
    test: RangeTest,
    match_val: RangeMatch,
    opts: RangeOptions,
    tracker: Option<&crate::tracking::Tracker>,
) -> Result<Vec<Flake>>
where
    O: OverlayProvider + ?Sized,
{
    match snapshot.range_provider.as_ref() {
        Some(provider) => {
            let overlay_ref = SizedOverlayRef(overlay);
            let query = crate::range_provider::RangeQuery {
                g_id,
                index,
                test,
                match_val: &match_val,
                opts: &opts,
                overlay: &overlay_ref,
                tracker,
            };
            provider.range(&query).map_err(|e| {
                match e
                    .get_ref()
                    .and_then(|inner| inner.downcast_ref::<crate::tracking::FuelExceededError>())
                {
                    Some(fe) => crate::error::Error::FuelExceeded(fe.clone()),
                    None => crate::error::Error::Io(e.to_string()),
                }
            })
        }
        None if snapshot.t == 0 => {
            // Genesis Db: no base data, return overlay flakes only.
            // Per-graph novelty returns only the requested graph's flakes.
            let to_t = opts.to_t.unwrap_or(i64::MAX);
            let mut flakes = collect_overlay_only(overlay, g_id, index, to_t);
            // Apply RangeMatch filtering — collect_overlay_only returns all
            // overlay flakes for this graph; narrow them to the requested range.
            apply_range_filter(&mut flakes, test, &match_val);
            // Apply RangeOptions semantics for overlay-only path (object bounds, offset, limits).
            //
            // This matters for time resolution (`@iso:`), which uses `object_bounds`
            // and `flake_limit(1)` to efficiently resolve the first flake after a target.
            apply_overlay_only_options(&mut flakes, &opts);
            Ok(flakes)
        }
        None => Err(crate::error::Error::invalid_index(
            "binary-only db has no range_provider attached \
             — load and attach BinaryIndexStore before queries",
        )),
    }
}

/// Execute a bounded range query with explicit start and end flakes.
///
/// This variant allows specifying explicit start and end bound flakes,
/// which is useful for subject-range queries (e.g., SHA prefix scans)
/// that need to scan between two different subjects.
///
/// Delegates to `RangeProvider::range_bounded`.
pub async fn range_bounded_with_overlay<O>(
    snapshot: &LedgerSnapshot,
    g_id: GraphId,
    overlay: &O,
    index: IndexType,
    start_bound: Flake,
    end_bound: Flake,
    opts: RangeOptions,
) -> Result<Vec<Flake>>
where
    O: OverlayProvider + ?Sized,
{
    match snapshot.range_provider.as_ref() {
        Some(provider) => {
            let overlay_ref = SizedOverlayRef(overlay);
            provider
                .range_bounded(g_id, index, &start_bound, &end_bound, &opts, &overlay_ref)
                .map_err(|e| crate::error::Error::Io(e.to_string()))
        }
        None if snapshot.t == 0 => {
            // Genesis Db: no base data, return overlay flakes only.
            // Per-graph novelty returns only the requested graph's flakes.
            let to_t = opts.to_t.unwrap_or(i64::MAX);
            let cmp = index.comparator();
            let mut flakes = collect_overlay_only(overlay, g_id, index, to_t);
            // Apply start/end bounds — collect_overlay_only returns all
            // overlay flakes for this graph; narrow to the [start_bound, end_bound] range.
            flakes.retain(|f| {
                cmp(f, &start_bound) != std::cmp::Ordering::Less
                    && cmp(f, &end_bound) != std::cmp::Ordering::Greater
            });
            apply_overlay_only_options(&mut flakes, &opts);
            Ok(flakes)
        }
        None => Err(crate::error::Error::invalid_index(
            "binary-only db has no range_provider attached \
             — load and attach BinaryIndexStore before queries",
        )),
    }
}

// ============================================================================
// OverlayRef wrapper — coerce &O (?Sized) to &dyn OverlayProvider
// ============================================================================

struct SizedOverlayRef<'a, O: OverlayProvider + ?Sized>(&'a O);

impl<O: OverlayProvider + ?Sized> OverlayProvider for SizedOverlayRef<'_, O> {
    fn as_any(&self) -> &dyn std::any::Any {
        self.0.as_any()
    }

    fn epoch(&self) -> u64 {
        self.0.epoch()
    }
    fn for_each_overlay_flake(
        &self,
        g_id: GraphId,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
        to_t: i64,
        callback: &mut dyn FnMut(&Flake),
    ) {
        self.0
            .for_each_overlay_flake(g_id, index, first, rhs, leftmost, to_t, callback);
    }
}

// ============================================================================
// Range match filtering for genesis overlay path
// ============================================================================

/// Check whether a flake satisfies an equality `RangeMatch`.
#[inline]
pub fn flake_matches_range_eq(f: &Flake, match_val: &RangeMatch) -> bool {
    if let Some(ref s) = match_val.s {
        if f.s != *s {
            return false;
        }
    }
    if let Some(ref p) = match_val.p {
        if f.p != *p {
            return false;
        }
    }
    if let Some(ref o) = match_val.o {
        if f.o != *o {
            return false;
        }
    }
    if let Some(ref dt) = match_val.dt {
        if !dt_compatible(dt, &f.dt) {
            return false;
        }
    }
    if let Some(t) = match_val.t {
        if f.t != t {
            return false;
        }
    }
    true
}

/// Apply range match filtering to overlay flakes.
///
/// The genesis LedgerSnapshot path collects all overlay flakes; this narrows them
/// to the requested range.  For `RangeTest::Eq` every specified component
/// of `match_val` must match exactly.  Other test modes currently pass
/// through unfiltered (callers post-filter as needed).
fn apply_range_filter(flakes: &mut Vec<Flake>, test: RangeTest, match_val: &RangeMatch) {
    if test != RangeTest::Eq {
        // Non-equality tests are uncommon on genesis LedgerSnapshot; callers
        // post-filter so returning the full set is safe.
        return;
    }
    flakes.retain(|f| flake_matches_range_eq(f, match_val));
}

/// Apply RangeOptions to the overlay-only (genesis LedgerSnapshot) path.
///
/// The overlay-only path bypasses the index `RangeProvider`, so we must manually
/// apply options that providers typically enforce (object bounds, offset, limits).
fn apply_overlay_only_options(flakes: &mut Vec<Flake>, opts: &RangeOptions) {
    // Object bounds (post-filter) — used by datetime resolution (`ledger#time > target`).
    if let Some(bounds) = opts.object_bounds.as_ref() {
        flakes.retain(|f| bounds.matches(&f.o));
    }

    // Offset (flake-wise for overlay-only path).
    if let Some(offset) = opts.offset {
        if offset > 0 {
            let n = offset.min(flakes.len());
            flakes.drain(0..n);
        }
    }

    // Apply flake limit (preferred) or subject limit (fallback semantics for overlay-only).
    let cap = opts.flake_limit.or(opts.limit).unwrap_or(usize::MAX);
    if flakes.len() > cap {
        flakes.truncate(cap);
    }
}

// ============================================================================
// Overlay-only collection (genesis LedgerSnapshot fallback)
// ============================================================================

/// Collect overlay flakes for a genesis LedgerSnapshot (no base data).
///
/// Queries the overlay for all flakes matching the graph and index, applies time
/// filtering, sorts by index comparator, and removes stale flakes.
fn collect_overlay_only<O: OverlayProvider + ?Sized>(
    overlay: &O,
    g_id: GraphId,
    index: IndexType,
    to_t: i64,
) -> Vec<Flake> {
    let cmp = index.comparator();
    let mut flakes: Vec<Flake> = Vec::new();

    // Request all overlay flakes for this graph+index (leftmost=true, rhs=None → full range).
    overlay.for_each_overlay_flake(g_id, index, None, None, true, to_t, &mut |f| {
        if f.t <= to_t {
            flakes.push(f.clone());
        }
    });

    flakes.sort_by(cmp);

    // Remove stale: keep newest occurrence of each fact key, drop retractions.
    remove_stale_flakes(flakes)
}

/// Remove stale flakes from an owned vector.
///
/// Iterates in reverse (newest first for identical facts), keeps only the
/// first occurrence of each fact key, and drops retractions.
fn remove_stale_flakes(flakes: Vec<Flake>) -> Vec<Flake> {
    use std::collections::HashSet;

    #[derive(Clone, Copy, Hash, PartialEq, Eq)]
    struct FactKeyRef<'a> {
        s: &'a Sid,
        p: &'a Sid,
        o: &'a FlakeValue,
        dt: &'a Sid,
    }

    let mut seen: HashSet<FactKeyRef<'_>> = HashSet::new();
    let mut keep = vec![false; flakes.len()];

    for (idx, f) in flakes.iter().enumerate().rev() {
        let key = FactKeyRef {
            s: &f.s,
            p: &f.p,
            o: &f.o,
            dt: &f.dt,
        };
        if !seen.insert(key) {
            continue;
        }
        if f.op {
            keep[idx] = true;
        }
    }

    flakes
        .into_iter()
        .zip(keep)
        .filter_map(|(f, k)| k.then_some(f))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_match_builders() {
        let s = Sid::new(1, "test");
        let p = Sid::new(2, "prop");

        let m1 = RangeMatch::subject(s.clone());
        assert_eq!(m1.s, Some(s.clone()));
        assert!(m1.p.is_none());

        let m2 = RangeMatch::subject_predicate(s.clone(), p.clone());
        assert_eq!(m2.s, Some(s));
        assert_eq!(m2.p, Some(p));
    }

    #[test]
    fn test_object_bounds_matches() {
        let bounds = ObjectBounds::new().with_lower(FlakeValue::Long(10), true);
        assert!(!bounds.matches(&FlakeValue::Long(9)));
        assert!(bounds.matches(&FlakeValue::Long(10)));
        assert!(bounds.matches(&FlakeValue::Long(11)));

        let bounds = ObjectBounds::new().with_lower(FlakeValue::Long(10), false);
        assert!(!bounds.matches(&FlakeValue::Long(9)));
        assert!(!bounds.matches(&FlakeValue::Long(10)));
        assert!(bounds.matches(&FlakeValue::Long(11)));

        let bounds = ObjectBounds::new().with_upper(FlakeValue::Long(100), true);
        assert!(bounds.matches(&FlakeValue::Long(99)));
        assert!(bounds.matches(&FlakeValue::Long(100)));
        assert!(!bounds.matches(&FlakeValue::Long(101)));

        let bounds = ObjectBounds::new().with_upper(FlakeValue::Long(100), false);
        assert!(bounds.matches(&FlakeValue::Long(99)));
        assert!(!bounds.matches(&FlakeValue::Long(100)));
        assert!(!bounds.matches(&FlakeValue::Long(101)));
    }

    #[test]
    fn test_object_bounds_two_sided() {
        let bounds = ObjectBounds::new()
            .with_lower(FlakeValue::Long(10), false)
            .with_upper(FlakeValue::Long(100), false);
        assert!(!bounds.matches(&FlakeValue::Long(10)));
        assert!(bounds.matches(&FlakeValue::Long(11)));
        assert!(bounds.matches(&FlakeValue::Long(50)));
        assert!(bounds.matches(&FlakeValue::Long(99)));
        assert!(!bounds.matches(&FlakeValue::Long(100)));

        let bounds = ObjectBounds::new()
            .with_lower(FlakeValue::Long(10), true)
            .with_upper(FlakeValue::Long(100), true);
        assert!(!bounds.matches(&FlakeValue::Long(9)));
        assert!(bounds.matches(&FlakeValue::Long(10)));
        assert!(bounds.matches(&FlakeValue::Long(100)));
        assert!(!bounds.matches(&FlakeValue::Long(101)));
    }

    #[test]
    fn test_object_bounds_with_doubles() {
        let bounds = ObjectBounds::new()
            .with_lower(FlakeValue::Double(0.0), true)
            .with_upper(FlakeValue::Double(1.0), false);

        assert!(!bounds.matches(&FlakeValue::Double(-0.1)));
        assert!(bounds.matches(&FlakeValue::Double(0.0)));
        assert!(bounds.matches(&FlakeValue::Double(0.5)));
        assert!(bounds.matches(&FlakeValue::Double(0.99)));
        assert!(!bounds.matches(&FlakeValue::Double(1.0)));
    }

    #[test]
    fn test_object_bounds_type_mismatch() {
        let bounds = ObjectBounds::new().with_lower(FlakeValue::Long(10), true);
        assert!(!bounds.matches(&FlakeValue::String("hello".to_string())));

        let bounds = ObjectBounds::new()
            .with_lower(FlakeValue::String("b".to_string()), true)
            .with_upper(FlakeValue::String("d".to_string()), false);
        assert!(!bounds.matches(&FlakeValue::String("a".to_string())));
        assert!(bounds.matches(&FlakeValue::String("b".to_string())));
        assert!(bounds.matches(&FlakeValue::String("c".to_string())));
        assert!(!bounds.matches(&FlakeValue::String("d".to_string())));
    }

    #[test]
    fn test_object_bounds_numeric_class_comparison() {
        let bounds = ObjectBounds::new()
            .with_lower(FlakeValue::Long(10), true)
            .with_upper(FlakeValue::Long(100), false);

        assert!(bounds.matches(&FlakeValue::Double(15.5)));
        assert!(!bounds.matches(&FlakeValue::Double(9.9)));
        assert!(!bounds.matches(&FlakeValue::Double(100.0)));

        let bounds = ObjectBounds::new().with_lower(FlakeValue::Double(3.5), true);
        assert!(bounds.matches(&FlakeValue::Long(4)));
        assert!(!bounds.matches(&FlakeValue::Long(3)));
    }

    #[test]
    fn test_object_bounds_mixed_numeric_range() {
        let bounds = ObjectBounds::new().with_lower(FlakeValue::Long(3), false);

        assert!(bounds.matches(&FlakeValue::Double(3.5)));
        assert!(bounds.matches(&FlakeValue::Long(4)));
        assert!(!bounds.matches(&FlakeValue::Long(3)));
        assert!(!bounds.matches(&FlakeValue::Double(3.0)));
        assert!(!bounds.matches(&FlakeValue::Double(2.9)));
    }

    #[test]
    fn test_object_bounds_empty() {
        let bounds = ObjectBounds::new();
        assert!(bounds.is_empty());
        assert!(bounds.matches(&FlakeValue::Long(0)));
        assert!(bounds.matches(&FlakeValue::Long(i64::MAX)));
        assert!(bounds.matches(&FlakeValue::String("anything".to_string())));
    }
}
