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

/// Resolve a mixed bag of assert/retract flakes to the currently-asserted
/// set: sort in `index` order, keep the newest op per fact key, drop
/// retractions. This is the same lifecycle rule the overlay-only range path
/// applies; callers that merge base rows with a raw overlay walk themselves
/// use it to get identical results to [`range_with_overlay`].
///
/// At equal `t` a retraction beats an assert, so an assert and a retract of
/// the same fact at one `t` resolve to absent. That matches
/// `fluree_db_binary_index::read::types::resolve_overlay_ops`, which the
/// cursor path uses, and the per-commit apply path. The transaction
/// accumulator dedups within a commit so the tie is not reachable through a
/// single transaction, but the segment-aware overlay assembly merges runs
/// across segments — the rule is here so every path lands on the same answer
/// rather than on whichever flake sorted last.
pub fn resolve_current_flakes(mut flakes: Vec<Flake>, index: IndexType) -> Vec<Flake> {
    flakes.sort_by(index.comparator());
    remove_stale_flakes(flakes)
}

/// Remove stale flakes from an owned vector: keep the winning op per fact
/// key, drop retractions, preserve the input's order among survivors.
///
/// The fact key includes the flake metadata `m` (language tag and list
/// index), not just `(s, p, o, dt)`. Two flakes that share a subject,
/// predicate, object value, and datatype but differ in their language tag
/// (e.g. `"animal"@en` vs `"animal"@fr`) or list position are **distinct
/// RDF facts** and must both survive — omitting `m` here silently collapses
/// language variants on insert (issue #1273).
///
/// Hashing the full identity is what makes that robust: it needs no
/// adjacency between a fact's own flakes, so it does not care that the
/// comparators order `t` before `m` and therefore interleave metadata
/// siblings (see #1703), nor that `FlakeMeta`'s `Ord` disagrees with its
/// `Eq` when both sides carry a list index (see #1711). `Hash`/`Eq` are
/// derived together and agree.
///
/// The winner is chosen explicitly rather than by taking the first hit of a
/// reverse scan, so the equal-`t` tie resolves to the retraction (see
/// [`resolve_current_flakes`]) instead of to whichever op the comparator
/// happened to sort last.
fn remove_stale_flakes(flakes: Vec<Flake>) -> Vec<Flake> {
    use std::collections::HashMap;

    #[derive(Clone, Copy, Hash, PartialEq, Eq)]
    struct FactKeyRef<'a> {
        s: &'a Sid,
        p: &'a Sid,
        o: &'a FlakeValue,
        dt: &'a Sid,
        m: &'a Option<FlakeMeta>,
    }

    let mut winner: HashMap<FactKeyRef<'_>, usize> = HashMap::with_capacity(flakes.len());

    for (idx, f) in flakes.iter().enumerate() {
        let key = FactKeyRef {
            s: &f.s,
            p: &f.p,
            o: &f.o,
            dt: &f.dt,
            m: &f.m,
        };
        match winner.get(&key) {
            None => {
                winner.insert(key, idx);
            }
            Some(&cur_idx) => {
                let cur = &flakes[cur_idx];
                // Newest op wins; at equal `t` the retraction does.
                if f.t > cur.t || (f.t == cur.t && !f.op && cur.op) {
                    winner.insert(key, idx);
                }
            }
        }
    }

    let mut keep = vec![false; flakes.len()];
    for idx in winner.into_values() {
        if flakes[idx].op {
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

    /// An assert and a retract of the same fact at the SAME `t` resolve to
    /// absent. The comparator orders `op` ascending, so a reverse scan that
    /// took its first hit picked the assert and the fact survived — which
    /// disagreed with `resolve_overlay_ops` on the cursor path, and so with
    /// this function's own documented promise of "identical results to
    /// `range_with_overlay`".
    #[test]
    fn resolve_current_flakes_retraction_wins_at_equal_t() {
        let out = resolve_current_flakes(
            vec![
                fact("a", "x", 2, true, None),
                fact("a", "x", 2, false, None),
            ],
            IndexType::Spot,
        );
        assert!(out.is_empty(), "same-t assert+retract resolves to absent");

        // Order of the input must not change the answer.
        let out = resolve_current_flakes(
            vec![
                fact("a", "x", 2, false, None),
                fact("a", "x", 2, true, None),
            ],
            IndexType::Spot,
        );
        assert!(out.is_empty());
    }

    /// Metadata siblings: the comparators order `t` before `m`, so a
    /// sibling's assert sits between a fact's own assert and its retraction.
    /// Hashing the full identity is immune to that — no adjacency is
    /// assumed (#1703).
    #[test]
    fn resolve_current_flakes_metadata_siblings_are_independent() {
        // `["a", "b", "a"]` with position 0 retracted. Deleting position 0 is
        // the failing direction for adjacency-based grouping; position 2
        // passes either way.
        let out = resolve_current_flakes(
            vec![
                fact("s", "a", 2, true, Some(0)),
                fact("s", "b", 2, true, Some(1)),
                fact("s", "a", 2, true, Some(2)),
                fact("s", "a", 3, false, Some(0)),
            ],
            IndexType::Spot,
        );
        let mut positions: Vec<i32> = out.iter().filter_map(|f| f.m.as_ref()?.i).collect();
        positions.sort_unstable();
        assert_eq!(positions, vec![1, 2], "only position 0 goes");
    }

    /// One list position under two language tags: `FlakeMeta::cmp` consults
    /// only `i` when both sides carry a list index, so those two compare
    /// `Equal` without being equal (#1711). The fact key hashes `Hash`/`Eq`,
    /// which agree, so they stay distinct facts here.
    #[test]
    fn resolve_current_flakes_one_position_two_language_tags() {
        let tagged = |lang: &str, t: i64, op: bool| Flake {
            s: Sid::new(1, "s"),
            p: Sid::new(2, "p"),
            o: FlakeValue::String("x".to_string()),
            dt: Sid::new(3, "string"),
            t,
            op,
            m: Some(FlakeMeta {
                lang: Some(lang.to_string()),
                i: Some(0),
            }),
            g: None,
        };
        let out = resolve_current_flakes(
            vec![
                tagged("en", 2, true),
                tagged("fr", 3, true),
                tagged("en", 4, false),
            ],
            IndexType::Spot,
        );
        assert_eq!(out.len(), 1, "the retracted @en must go");
        assert_eq!(
            out[0].m.as_ref().and_then(|m| m.lang.as_deref()),
            Some("fr")
        );
    }

    /// Mirror of the metadata-sibling case: retracting the LAST position
    /// passed even under adjacency-based grouping, so it is kept so a future
    /// regrouping cannot fix one direction by breaking the other.
    #[test]
    fn resolve_current_flakes_metadata_siblings_last_position() {
        let out = resolve_current_flakes(
            vec![
                fact("s", "a", 2, true, Some(0)),
                fact("s", "b", 2, true, Some(1)),
                fact("s", "a", 2, true, Some(2)),
                fact("s", "a", 3, false, Some(2)),
            ],
            IndexType::Spot,
        );
        let mut positions: Vec<i32> = out.iter().filter_map(|f| f.m.as_ref()?.i).collect();
        positions.sort_unstable();
        assert_eq!(positions, vec![0, 1]);
    }

    /// The winner is the newest op for a fact's OWN key, not the newest op in
    /// its neighbourhood: a position retracted and then re-asserted comes
    /// back even when a sibling carries a later `t`.
    #[test]
    fn resolve_current_flakes_reasserted_position_survives_later_sibling() {
        let out = resolve_current_flakes(
            vec![
                fact("s", "a", 2, true, Some(0)),
                fact("s", "a", 3, false, Some(0)),
                fact("s", "a", 4, true, Some(0)),
                fact("s", "a", 5, true, Some(1)),
            ],
            IndexType::Spot,
        );
        let mut positions: Vec<i32> = out.iter().filter_map(|f| f.m.as_ref()?.i).collect();
        positions.sort_unstable();
        assert_eq!(positions, vec![0, 1]);
    }

    /// Same lexical form under two language tags with no list index — the
    /// #1273 shape — must retract independently.
    #[test]
    fn resolve_current_flakes_language_siblings_without_list_index() {
        let tagged = |lang: &str, t: i64, op: bool| Flake {
            s: Sid::new(1, "s"),
            p: Sid::new(2, "p"),
            o: FlakeValue::String("hello".to_string()),
            dt: Sid::new(3, "string"),
            t,
            op,
            m: Some(FlakeMeta {
                lang: Some(lang.to_string()),
                i: None,
            }),
            g: None,
        };
        let out = resolve_current_flakes(
            vec![
                tagged("en", 2, true),
                tagged("fr", 2, true),
                tagged("en", 3, false),
            ],
            IndexType::Spot,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].m.as_ref().and_then(|m| m.lang.as_deref()),
            Some("fr")
        );
    }

    /// Distinct datatypes on one lexical value are distinct facts, so `dt`
    /// must stay in the key.
    #[test]
    fn resolve_current_flakes_datatype_siblings_are_independent() {
        let typed = |dt: &str, t: i64, op: bool| Flake {
            s: Sid::new(1, "s"),
            p: Sid::new(2, "p"),
            o: FlakeValue::String("1".to_string()),
            dt: Sid::new(3, dt),
            t,
            op,
            m: None,
            g: None,
        };
        let out = resolve_current_flakes(
            vec![
                typed("string", 2, true),
                typed("token", 2, true),
                typed("string", 3, false),
            ],
            IndexType::Spot,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].dt.name_str(), "token");
    }

    /// Survivors come back in index order — callers take this as a range
    /// result. Hashing the identity means the winner is chosen per key, so
    /// the output must still follow the sorted input's order.
    #[test]
    fn resolve_current_flakes_output_is_comparator_ordered() {
        let out = resolve_current_flakes(
            vec![
                // `m` ascending but `t` descending across the two survivors.
                fact("s", "a", 9, true, Some(0)),
                fact("s", "a", 3, true, Some(1)),
            ],
            IndexType::Spot,
        );
        let cmp = IndexType::Spot.comparator();
        assert!(
            out.windows(2)
                .all(|w| cmp(&w[0], &w[1]) != std::cmp::Ordering::Greater),
            "resolved range results must be index-ordered"
        );
    }

    /// A list holding ONE value at many positions puts every entry under the
    /// same `(s, p, o, dt)` with a distinct `m`. Hashing the identity keeps
    /// that linear; an adjacency- or scan-based grouper would go quadratic
    /// here, which is the shape this area of the code exists to have stopped
    /// doing.
    #[test]
    fn resolve_current_flakes_many_positions_of_one_value_stay_linear() {
        const K: i32 = 4_000;
        let mut flakes: Vec<Flake> = (0..K).map(|i| fact("s", "x", 2, true, Some(i))).collect();
        flakes.extend(
            (0..K)
                .filter(|i| i % 3 == 0)
                .map(|i| fact("s", "x", 3, false, Some(i))),
        );

        let started = std::time::Instant::now();
        let out = resolve_current_flakes(flakes, IndexType::Spot);
        let elapsed = started.elapsed();

        let expected = (0..K).filter(|i| i % 3 != 0).count();
        assert_eq!(out.len(), expected);
        let positions: Vec<i32> = out.iter().filter_map(|f| f.m.as_ref()?.i).collect();
        assert!(positions.iter().all(|i| i % 3 != 0));
        assert!(positions.windows(2).all(|w| w[0] < w[1]));
        // Generous by ~2 orders of magnitude against the real runtime.
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "resolution took {elapsed:?} for {K} positions — went superlinear"
        );
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
}
