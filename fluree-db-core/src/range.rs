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
use crate::flake::{Flake, FlakeMeta};
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
            //
            // Seek instead of scanning: when the equality match's bound
            // components form a prefix of `index` order, hand the overlay
            // min/max prefix bounds so it can binary-search its segments
            // rather than yielding every flake it holds. Without this,
            // point probes against an unindexed (all-novelty) ledger paid a
            // full clone+sort of the overlay PER CALL — per-focus-node
            // loops like SHACL validation went quadratic (`fluree validate
            // <file>`: 4k subjects 16.9s, 31.6k killed at 26min; linear
            // after this seek).
            let to_t = opts.to_t.unwrap_or(i64::MAX);
            let bounds = overlay_eq_bounds(index, test, &match_val);
            let (first, rhs) = match &bounds {
                Some((lo, hi)) => (Some(lo), Some(hi)),
                None => (None, None),
            };
            let mut flakes = collect_overlay_only(overlay, g_id, index, to_t, first, rhs);
            // Exact narrowing — the seek is a prefix bound (and a
            // non-prefix match takes the unbounded walk), so the requested
            // range still needs the full filter either way.
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
            // Upper-bound-only seek: the overlay's left bound is EXCLUSIVE
            // (`> first`), while this API's `start_bound` is inclusive — a
            // flake exactly equal to `start_bound` would be dropped by a
            // left seek, so only the right bound (inclusive on both sides)
            // prunes the walk. The retain below applies both bounds exactly.
            let mut flakes =
                collect_overlay_only(overlay, g_id, index, to_t, None, Some(&end_bound));
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

    fn is_effectively_empty(&self) -> bool {
        self.0.is_effectively_empty()
    }

    fn content_version(&self) -> Option<u64> {
        self.0.content_version()
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

    fn overlay_segments(&self, g_id: GraphId) -> Vec<crate::overlay::OverlaySegmentMeta> {
        self.0.overlay_segments(g_id)
    }

    fn for_each_overlay_segment_flake(
        &self,
        g_id: GraphId,
        seg_id: u64,
        seg_idx: usize,
        index: IndexType,
        callback: &mut dyn FnMut(&Flake),
    ) {
        self.0
            .for_each_overlay_segment_flake(g_id, seg_id, seg_idx, index, callback);
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
/// Derive overlay seek bounds for an equality range match.
///
/// Returns min/max prefix bound flakes in `index` order when the match's
/// bound components form a prefix of that order (SPOT: `s` / `s+p`;
/// PSOT/POST: `p`), letting the overlay binary-search its segments instead
/// of yielding everything it holds. The min/max sentinels carry
/// `t = i64::MIN / i64::MAX`, so every real flake compares strictly inside
/// them — the overlay's left-EXCLUSIVE `(first, rhs]` contract still yields
/// every matching flake. OPST leads with the object (no object-prefix
/// constructor exists) and non-equality tests post-filter at call sites, so
/// both take the unbounded walk.
fn overlay_eq_bounds(
    index: IndexType,
    test: RangeTest,
    match_val: &RangeMatch,
) -> Option<(Flake, Flake)> {
    if test != RangeTest::Eq {
        return None;
    }
    match index {
        IndexType::Spot => match (&match_val.s, &match_val.p) {
            (Some(s), Some(p)) => Some((
                Flake::min_for_subject_predicate(s.clone(), p.clone()),
                Flake::max_for_subject_predicate(s.clone(), p.clone()),
            )),
            (Some(s), None) => Some((
                Flake::min_for_subject(s.clone()),
                Flake::max_for_subject(s.clone()),
            )),
            _ => None,
        },
        IndexType::Psot | IndexType::Post => match_val.p.as_ref().map(|p| {
            (
                Flake::min_for_predicate(p.clone()),
                Flake::max_for_predicate(p.clone()),
            )
        }),
        IndexType::Opst => None,
    }
}

fn collect_overlay_only<O: OverlayProvider + ?Sized>(
    overlay: &O,
    g_id: GraphId,
    index: IndexType,
    to_t: i64,
    first: Option<&Flake>,
    rhs: Option<&Flake>,
) -> Vec<Flake> {
    let cmp = index.comparator();
    let mut flakes: Vec<Flake> = Vec::new();

    // `leftmost=true` (start from the beginning) exactly when no lower bound
    // was derived; a lower bound is a min-sentinel every real flake compares
    // strictly above, so the exclusive `> first` semantics lose nothing.
    overlay.for_each_overlay_flake(g_id, index, first, rhs, first.is_none(), to_t, &mut |f| {
        if f.t <= to_t {
            flakes.push(f.clone());
        }
    });

    // Providers must yield in index order already; this sort is a cheap
    // (now typically bounded-set) safety net for non-compliant overlays,
    // and `remove_stale_flakes` depends on that order.
    flakes.sort_by(cmp);

    // Remove stale: keep newest occurrence of each fact key, drop retractions.
    remove_stale_flakes(flakes)
}

/// Resolve a mixed bag of assert/retract flakes to the currently-asserted
/// set: sort in `index` order (which places the newest `t` last within a
/// fact key), keep the newest op per fact key, drop retractions. This is the
/// same lifecycle rule the overlay-only range path applies; callers that
/// merge base rows with a raw overlay walk themselves use it to get
/// identical results to [`range_with_overlay`].
pub fn resolve_current_flakes(mut flakes: Vec<Flake>, index: IndexType) -> Vec<Flake> {
    flakes.sort_by(index.comparator());
    remove_stale_flakes(flakes)
}

/// Remove stale flakes from an owned vector.
///
/// Iterates in reverse (newest first for identical facts), keeps only the
/// first occurrence of each fact key, and drops retractions.
///
/// The fact key includes the flake metadata `m` (language tag and list
/// index), not just `(s, p, o, dt)`. Two flakes that share a subject,
/// predicate, object value, and datatype but differ in their language tag
/// (e.g. `"animal"@en` vs `"animal"@fr`) or list position are **distinct
/// RDF facts** and must both survive — omitting `m` here silently collapses
/// language variants on insert (issue #1273).
fn remove_stale_flakes(flakes: Vec<Flake>) -> Vec<Flake> {
    use std::collections::HashSet;

    #[derive(Clone, Copy, Hash, PartialEq, Eq)]
    struct FactKeyRef<'a> {
        s: &'a Sid,
        p: &'a Sid,
        o: &'a FlakeValue,
        dt: &'a Sid,
        m: &'a Option<FlakeMeta>,
    }

    let mut seen: HashSet<FactKeyRef<'_>> = HashSet::new();
    let mut keep = vec![false; flakes.len()];

    for (idx, f) in flakes.iter().enumerate().rev() {
        let key = FactKeyRef {
            s: &f.s,
            p: &f.p,
            o: &f.o,
            dt: &f.dt,
            m: &f.m,
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

    fn fact(s: &str, o: &str, t: i64, op: bool, i: Option<i32>) -> Flake {
        let m = i.map(|i| FlakeMeta {
            lang: None,
            i: Some(i),
        });
        Flake {
            s: Sid::new(1, s),
            p: Sid::new(2, "p"),
            o: FlakeValue::String(o.to_string()),
            dt: Sid::new(3, "string"),
            t,
            op,
            m,
            g: None,
        }
    }

    #[test]
    fn resolve_current_flakes_newest_op_wins_and_retracts_drop() {
        // Out of order on purpose: the resolver must sort first.
        let flakes = vec![
            fact("a", "x", 3, false, None), // retract at t=3 …
            fact("a", "x", 1, true, None),  // … of an assert at t=1
            fact("a", "y", 2, true, None),  // untouched assert
            fact("b", "x", 2, false, None), // retract at t=2 …
            fact("b", "x", 4, true, None),  // … re-asserted later → live
        ];
        let out = resolve_current_flakes(flakes, IndexType::Spot);
        let keys: Vec<(String, String)> = out
            .iter()
            .map(|f| (f.s.name.to_string(), format!("{:?}", f.o)))
            .collect();
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|f| f.op));
        assert!(keys.contains(&("a".into(), format!("{:?}", FlakeValue::String("y".into())))));
        assert!(keys.contains(&("b".into(), format!("{:?}", FlakeValue::String("x".into())))));
    }

    #[test]
    fn resolve_current_flakes_list_positions_are_distinct_facts() {
        // Same (s, p, o, dt) at two list positions; retracting position 1
        // must leave position 0 live — `m` is part of the fact key.
        let flakes = vec![
            fact("a", "x", 1, true, Some(0)),
            fact("a", "x", 1, true, Some(1)),
            fact("a", "x", 2, false, Some(1)),
        ];
        let out = resolve_current_flakes(flakes, IndexType::Spot);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].m.as_ref().and_then(|m| m.i), Some(0));
    }

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

    /// Sorted-vec overlay that records the bounds each walk received and
    /// honors them the way novelty does (`(first, rhs]`, left-exclusive
    /// unless `leftmost`). Lets the tests below pin BOTH properties of the
    /// overlay-only seek: results stay correct, and prefix probes actually
    /// arrive bounded instead of walking everything.
    struct RecordingOverlay {
        flakes: Vec<Flake>, // pre-sorted in SPOT order by the test
        bounded_calls: std::sync::atomic::AtomicUsize,
        yielded: std::sync::atomic::AtomicUsize,
    }

    impl OverlayProvider for RecordingOverlay {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn epoch(&self) -> u64 {
            1
        }

        fn for_each_overlay_flake(
            &self,
            _g_id: GraphId,
            index: IndexType,
            first: Option<&Flake>,
            rhs: Option<&Flake>,
            leftmost: bool,
            to_t: i64,
            callback: &mut dyn FnMut(&Flake),
        ) {
            use std::sync::atomic::Ordering::Relaxed;
            if first.is_some() || rhs.is_some() {
                self.bounded_calls.fetch_add(1, Relaxed);
            }
            let cmp = index.comparator();
            for f in &self.flakes {
                if f.t > to_t {
                    continue;
                }
                if !leftmost {
                    if let Some(lo) = first {
                        if cmp(f, lo) != std::cmp::Ordering::Greater {
                            continue;
                        }
                    }
                }
                if let Some(hi) = rhs {
                    if cmp(f, hi) == std::cmp::Ordering::Greater {
                        continue;
                    }
                }
                self.yielded.fetch_add(1, Relaxed);
                callback(f);
            }
        }
    }

    fn subject_flake(s: &str, p: &str, o: &str) -> Flake {
        Flake {
            g: None,
            s: Sid::new(1, s),
            p: Sid::new(2, p),
            o: FlakeValue::String(o.to_string()),
            dt: Sid::new(3, "string"),
            t: 1,
            op: true,
            m: None,
        }
    }

    /// A subject+predicate Eq probe against a genesis (overlay-only) view
    /// must seek — the overlay sees prefix bounds and yields only the
    /// matching flakes — and still return exactly the right rows. This is
    /// the per-focus-node probe SHACL validation hammers; unbounded it made
    /// `fluree validate <file>` quadratic in focus nodes.
    #[tokio::test]
    async fn overlay_only_eq_probe_is_bounded_and_correct() {
        let mut flakes: Vec<Flake> = (0..100)
            .flat_map(|i| {
                let s = format!("s{i:03}");
                vec![
                    subject_flake(&s, "name", &format!("name-{i}")),
                    subject_flake(&s, "kind", "widget"),
                ]
            })
            .collect();
        flakes.sort_by(IndexType::Spot.comparator());
        let overlay = RecordingOverlay {
            flakes,
            bounded_calls: Default::default(),
            yielded: Default::default(),
        };

        let snapshot = crate::LedgerSnapshot::genesis("test/main");
        let out = range_with_overlay(
            &snapshot,
            0,
            &overlay,
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch::subject_predicate(Sid::new(1, "s042"), Sid::new(2, "name")),
            RangeOptions::default().with_to_t(10),
        )
        .await
        .expect("range");

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].o, FlakeValue::String("name-42".into()));
        use std::sync::atomic::Ordering::Relaxed;
        assert_eq!(
            overlay.bounded_calls.load(Relaxed),
            1,
            "prefix Eq probe must pass seek bounds to the overlay"
        );
        assert_eq!(
            overlay.yielded.load(Relaxed),
            1,
            "bounded walk must not yield the whole overlay"
        );
    }

    /// Bare-subject and predicate-prefix probes bound too; an OPST probe
    /// (object-led order, no prefix constructor) legitimately does not.
    #[test]
    fn overlay_eq_bounds_cover_index_prefixes() {
        let s = Sid::new(1, "s");
        let p = Sid::new(2, "p");

        let sp = overlay_eq_bounds(
            IndexType::Spot,
            RangeTest::Eq,
            &RangeMatch::subject_predicate(s.clone(), p.clone()),
        );
        assert!(sp.is_some());

        let s_only = overlay_eq_bounds(IndexType::Spot, RangeTest::Eq, &RangeMatch::subject(s));
        assert!(s_only.is_some());

        for index in [IndexType::Psot, IndexType::Post] {
            assert!(
                overlay_eq_bounds(index, RangeTest::Eq, &RangeMatch::predicate(p.clone()))
                    .is_some()
            );
        }

        // Non-prefix / non-Eq shapes stay unbounded.
        assert!(overlay_eq_bounds(
            IndexType::Opst,
            RangeTest::Eq,
            &RangeMatch::predicate(p.clone())
        )
        .is_none());
        assert!(
            overlay_eq_bounds(IndexType::Psot, RangeTest::Ge, &RangeMatch::predicate(p)).is_none()
        );
    }
}
