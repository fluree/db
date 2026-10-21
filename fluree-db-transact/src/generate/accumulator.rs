//! Streaming-friendly flake accumulator with set-semantics cancellation.
//!
//! `FlakeAccumulator` replaces the older two-step pattern of "generate a
//! `Vec<Flake>` of every retraction + every assertion, then run
//! `apply_cancellation` over the concatenated Vec." Instead, callers push
//! flakes into the accumulator as they are produced (per-batch in the future
//! streaming-WHERE world, per-template-pass today), and `finalize()` produces
//! the SPOT-sorted survivor set.
//!
//! ## Why owned keys
//!
//! The accumulator stores the survivor flake by value (one per unique fact),
//! not by reference into a caller-owned buffer. Streaming consumers drop input
//! batches between pushes, so any borrow into those batches would dangle. The
//! cost is one `Flake::clone()` per *unique fact* in mixed mode (see
//! [`push_into_mixed`]); pure-DELETE mode pays zero clones because the
//! `FxHashSet` consumes the inserted flake and drops duplicates on collision.
//!
//! ## Modes
//!
//! - [`FlakeAccumulator::pure_delete`] for transactions with no INSERT
//!   templates and `txn_type != Upsert`. No assertion-side bookkeeping.
//! - [`FlakeAccumulator::mixed`] for everything else. Tracks per-fact counts
//!   so that 1:1 cancellation pairs collapse but surplus assertions or
//!   retractions survive (RDF set semantics: 4 retracts + 1 assert collapses
//!   to 1 surviving retract, not 0).
//!
//! ## Fact identity
//!
//! A retraction and an assertion cancel when they are one fact to storage
//! ([`FactIdentity`]): by term, except that a value whose index key can merge
//! terms is one fact per key and list position. A DELETE retracts the term
//! the index stores, and an INSERT of the same value as written may carry
//! more than that (a big integer's XSD subtype); told apart by term, both
//! would commit at one `t`, which storage resolves as the retraction.
//! Without an identity
//! ([`FlakeAccumulator::with_identity`]) facts are told apart by term.

use crate::current_facts::{FactIdentity, MergeKey, Retraction};
use fluree_db_core::{Flake, GraphId, IndexType, Sid};
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};

/// Accumulates flakes from one or more sources and produces a deterministic,
/// deduplicated survivor set. See [module docs](self) for design notes.
pub struct FlakeAccumulator<'l> {
    inner: AccInner,
    input_count: u64,
    capacity_hint: usize,
    /// Mixed mode only: count each asserted fact at most once, so a payload
    /// that states the same fact twice cannot out-vote one retraction of it.
    /// See [`FlakeAccumulator::mixed_set_assertions`].
    set_assertions: bool,
    /// Mixed mode only: the identity facts are told apart by (see the
    /// [module docs](self)); by term without one.
    identity: Option<FactIdentity<'l>>,
}

/// Dedup is keyed by **(graph, fact)**, not fact alone.
///
/// `Flake`'s `Eq`/`Hash` intentionally ignore the graph component `g`
/// (`fluree-db-core/src/flake.rs`), so a single fact asserted/retracted in two
/// different graphs within one transaction would otherwise collapse into one
/// bucket — silently dropping the second graph's flake, or cross-cancelling an
/// assertion in one graph against a retraction in another. Nesting the maps by
/// `Option<Sid>` (the graph; `None` = default graph) scopes cancellation to a
/// single graph. Single-graph transactions are unaffected (one outer entry).
enum AccInner {
    /// Pure-DELETE: retractions deduplicated by fact identity, per graph.
    PureRetract(FxHashMap<Option<Sid>, FxHashSet<Flake>>),
    /// Mixed assertions + retractions with 1:1 per-fact cancellation, per graph.
    Mixed(FxHashMap<Option<Sid>, MixedGraph>),
}

/// One graph's mixed-mode buckets.
#[derive(Default)]
struct MixedGraph {
    /// The graph's id in the ledger, which index keys are computed in;
    /// `None` without an identity or for a graph the ledger does not have
    /// (which holds nothing indexed).
    g_id: Option<GraphId>,
    /// Facts told apart by term.
    by_term: FxHashMap<Flake, FlakeBucket>,
    /// Facts told apart by key: `(s, p, (key, list position))`.
    by_key: FxHashMap<(Sid, Sid, MergeKey), FlakeBucket>,
}

#[derive(Default)]
struct FlakeBucket {
    assert_count: u32,
    retract_count: u32,
    /// Last-seen surviving assertion (None if never asserted in this txn).
    assertion: Option<Flake>,
    /// Last-seen surviving retraction (None if never retracted in this txn).
    retraction: Option<Flake>,
}

impl<'l> FlakeAccumulator<'l> {
    /// Pure-DELETE accumulator: drops the assertion bookkeeping side entirely.
    /// Use only when the transaction has no INSERT templates and is not Upsert.
    pub fn pure_delete(capacity_hint: usize) -> Self {
        Self {
            inner: AccInner::PureRetract(FxHashMap::default()),
            input_count: 0,
            capacity_hint,
            set_assertions: false,
            identity: None,
        }
    }

    /// Mixed assertion + retraction accumulator with set-semantics cancellation.
    pub fn mixed(capacity_hint: usize) -> Self {
        Self {
            inner: AccInner::Mixed(FxHashMap::default()),
            input_count: 0,
            capacity_hint,
            set_assertions: false,
            identity: None,
        }
    }

    /// Tell a mixed accumulator's facts apart by `identity` rather than by
    /// term (see the [module docs](self)). Needed only when the operation
    /// both retracts and asserts.
    pub fn with_identity(mut self, identity: FactIdentity<'l>) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Mixed accumulator whose assertion side has set semantics: pushing the
    /// same fact twice counts as one assertion.
    ///
    /// Graph sync needs this. Its payload is "the graph's desired contents",
    /// a set, and the sync wave retracts every current fact exactly once so
    /// that unchanged facts cancel. A payload that states one fact twice —
    /// the JSON-LD parallel-annotation shape does this for the base edge of
    /// every edge with several annotations — would otherwise leave a surplus
    /// assertion and commit a no-op delta on every re-sync.
    pub fn mixed_set_assertions(capacity_hint: usize) -> Self {
        Self {
            set_assertions: true,
            ..Self::mixed(capacity_hint)
        }
    }

    /// True if this accumulator can accept assertions.
    pub fn supports_assertions(&self) -> bool {
        matches!(self.inner, AccInner::Mixed(_))
    }

    /// Total flakes pushed (for tracing — not the survivor count).
    pub fn input_count(&self) -> u64 {
        self.input_count
    }

    /// Push retractions. Duplicates collapse to a single representative
    /// survivor (RDF set semantics).
    ///
    /// Only a [`Retraction`] is accepted — a retraction of a stored fact (see
    /// [`crate::current_facts`]). A retraction of a fact that is not stored
    /// would cancel a same-transaction assertion of that fact, and the
    /// assertion would never be written.
    pub fn push_retractions<I: IntoIterator<Item = Retraction>>(&mut self, retractions: I) {
        let hint = self.capacity_hint;
        let flakes = retractions.into_iter().map(Retraction::into_flake);
        match &mut self.inner {
            AccInner::PureRetract(graphs) => {
                for f in flakes {
                    debug_assert!(!f.op, "push_retractions received an assertion (op=true)");
                    self.input_count += 1;
                    // Dropping `f` on collision is the dedup — the per-graph
                    // `FxHashSet` keeps the first inserted flake of each fact.
                    graphs
                        .entry(f.g.clone())
                        .or_insert_with(|| FxHashSet::with_capacity_and_hasher(hint, FxBuildHasher))
                        .insert(f);
                }
            }
            AccInner::Mixed(graphs) => {
                for f in flakes {
                    debug_assert!(!f.op, "push_retractions received an assertion (op=true)");
                    self.input_count += 1;
                    let graph = mixed_graph(graphs, self.identity.as_ref(), hint, &f.g);
                    push_into_mixed(self.set_assertions, self.identity.as_ref(), graph, f);
                }
            }
        }
    }

    /// Push assertions from any source. Panics if the accumulator was
    /// constructed via [`FlakeAccumulator::pure_delete`] — callers are
    /// expected to short-circuit assertion generation in that case.
    pub fn push_assertions<I: IntoIterator<Item = Flake>>(&mut self, flakes: I) {
        match &mut self.inner {
            AccInner::PureRetract(_) => {
                // Guard: pure-delete callers must not generate assertions.
                // Touch the iterator only enough to detect a non-empty source
                // so an accidentally-fed empty Vec doesn't panic.
                assert!(
                    flakes.into_iter().next().is_none(),
                    "FlakeAccumulator::push_assertions called on a pure-DELETE \
                     accumulator — this indicates an upstream wiring bug"
                );
            }
            AccInner::Mixed(graphs) => {
                let hint = self.capacity_hint;
                for f in flakes {
                    debug_assert!(f.op, "push_assertions received a retraction (op=false)");
                    self.input_count += 1;
                    let graph = mixed_graph(graphs, self.identity.as_ref(), hint, &f.g);
                    push_into_mixed(self.set_assertions, self.identity.as_ref(), graph, f);
                }
            }
        }
    }

    /// Whether a retraction of `fact`, a stored fact, has been pushed.
    pub fn retracts(&self, fact: &Flake) -> bool {
        match &self.inner {
            AccInner::PureRetract(graphs) => graphs.get(&fact.g).is_some_and(|f| f.contains(fact)),
            AccInner::Mixed(graphs) => {
                let Some(graph) = graphs.get(&fact.g) else {
                    return false;
                };
                let key = self
                    .identity
                    .as_ref()
                    .and_then(|ids| ids.key(graph.g_id, fact));
                let bucket = match key {
                    Some(key) => graph.by_key.get(&(fact.s.clone(), fact.p.clone(), key)),
                    None => graph.by_term.get(fact),
                };
                bucket.is_some_and(|b| b.retract_count > 0)
            }
        }
    }

    /// Drain into the final SPOT-sorted survivor set.
    ///
    /// For pure-DELETE: every unique retracted fact contributes one survivor.
    /// For mixed: per fact, `cancel = min(assert_count, retract_count)` pairs
    /// cancel; surplus on either side contributes one survivor of that op.
    pub fn finalize(self) -> Vec<Flake> {
        self.drain(false).0
    }

    /// [`finalize`](Self::finalize), plus one `(retraction, assertion)` pair
    /// for each fact the transaction retracted and asserted again: a stored
    /// fact restated as it is, which stages nothing.
    pub fn finalize_with_restated(self) -> (Vec<Flake>, Vec<(Flake, Flake)>) {
        self.drain(true)
    }

    fn drain(self, keep_restated: bool) -> (Vec<Flake>, Vec<(Flake, Flake)>) {
        let mut restated = Vec::new();
        let mut out: Vec<Flake> = match self.inner {
            AccInner::PureRetract(graphs) => graphs
                .into_values()
                .flat_map(IntoIterator::into_iter)
                .collect(),
            AccInner::Mixed(graphs) => {
                let mut v = Vec::with_capacity(
                    graphs
                        .values()
                        .map(|g| g.by_term.len() + g.by_key.len())
                        .sum(),
                );
                for graph in graphs.into_values() {
                    let buckets = graph
                        .by_term
                        .into_values()
                        .chain(graph.by_key.into_values());
                    for b in buckets {
                        let cancel = b.assert_count.min(b.retract_count);
                        if keep_restated && cancel > 0 {
                            if let (Some(r), Some(a)) = (&b.retraction, &b.assertion) {
                                restated.push((r.clone(), a.clone()));
                            }
                        }
                        if b.assert_count > cancel {
                            if let Some(a) = b.assertion {
                                v.push(a);
                            }
                        }
                        if b.retract_count > cancel {
                            if let Some(r) = b.retraction {
                                v.push(r);
                            }
                        }
                    }
                }
                v
            }
        };
        out.sort_by(|a, b| IndexType::Spot.compare(a, b));
        (out, restated)
    }
}

/// `g`'s mixed-mode buckets, created on first use with the graph's id when
/// there is an identity to key facts by.
fn mixed_graph<'a>(
    graphs: &'a mut FxHashMap<Option<Sid>, MixedGraph>,
    identity: Option<&FactIdentity<'_>>,
    hint: usize,
    g: &Option<Sid>,
) -> &'a mut MixedGraph {
    graphs.entry(g.clone()).or_insert_with(|| MixedGraph {
        g_id: identity.and_then(|ids| ids.graph_id(g)),
        by_term: FxHashMap::with_capacity_and_hasher(hint, FxBuildHasher),
        by_key: FxHashMap::default(),
    })
}

/// Count `flake` into its fact's bucket and make it the survivor candidate
/// of its op.
fn count_into(set_assertions: bool, bucket: &mut FlakeBucket, flake: Flake) {
    if flake.op {
        if !(set_assertions && bucket.assert_count > 0) {
            bucket.assert_count = bucket.assert_count.saturating_add(1);
        }
        bucket.assertion = Some(flake);
    } else {
        bucket.retract_count = bucket.retract_count.saturating_add(1);
        bucket.retraction = Some(flake);
    }
}

/// Insert a flake into its graph's mixed-mode buckets: by index key when
/// the identity tells its fact apart by key, else by term.
///
/// First push for a given fact pays one `Flake::clone()` (one copy lives in
/// the map's key, one copy lives in the bucket as the survivor candidate).
/// Subsequent pushes for the same fact are clone-free: the bucket's survivor
/// slot is overwritten via `Some(flake)` and the previous survivor is dropped.
/// A fact keyed by index key clones its subject and predicate `Sid`s instead.
fn push_into_mixed(
    set_assertions: bool,
    identity: Option<&FactIdentity<'_>>,
    graph: &mut MixedGraph,
    flake: Flake,
) {
    if let Some(ids) = identity {
        if let Some(key) = ids.key(graph.g_id, &flake) {
            let bucket = graph
                .by_key
                .entry((flake.s.clone(), flake.p.clone(), key))
                .or_default();
            count_into(set_assertions, bucket, flake);
            return;
        }
    }
    let map = &mut graph.by_term;
    if let Some(bucket) = map.get_mut(&flake) {
        count_into(set_assertions, bucket, flake);
    } else {
        let bucket = if flake.op {
            FlakeBucket {
                assert_count: 1,
                retract_count: 0,
                assertion: Some(flake.clone()),
                retraction: None,
            }
        } else {
            FlakeBucket {
                assert_count: 0,
                retract_count: 1,
                assertion: None,
                retraction: Some(flake.clone()),
            }
        };
        map.insert(flake, bucket);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retractions(flakes: Vec<Flake>) -> Vec<Retraction> {
        flakes.into_iter().map(Retraction::for_test).collect()
    }
    use fluree_db_core::{FlakeValue, Sid};

    fn flake(s: u16, p: u16, o: i64, t: i64, op: bool) -> Flake {
        Flake::new(
            Sid::new(s, format!("s{s}")),
            Sid::new(p, format!("p{p}")),
            FlakeValue::Long(o),
            Sid::new(2, "long"),
            t,
            op,
            None,
        )
    }

    // ---- Pure-DELETE mode ---------------------------------------------------

    #[test]
    fn pure_delete_dedups_duplicates() {
        let mut acc = FlakeAccumulator::pure_delete(4);
        acc.push_retractions(retractions(vec![
            flake(1, 1, 100, 5, false),
            flake(1, 1, 100, 5, false),
            flake(1, 1, 100, 5, false),
        ]));
        let out = acc.finalize();
        assert_eq!(out.len(), 1);
        assert!(!out[0].op);
    }

    #[test]
    fn pure_delete_preserves_distinct_facts() {
        let mut acc = FlakeAccumulator::pure_delete(4);
        acc.push_retractions(retractions(vec![
            flake(1, 1, 100, 5, false),
            flake(2, 1, 100, 5, false),
            flake(3, 1, 100, 5, false),
        ]));
        let out = acc.finalize();
        assert_eq!(out.len(), 3);
        // SPOT order: by subject namespace_code first
        assert_eq!(out[0].s.namespace_code, 1);
        assert_eq!(out[1].s.namespace_code, 2);
        assert_eq!(out[2].s.namespace_code, 3);
    }

    #[test]
    fn pure_delete_input_count_tracks_total_pushes() {
        let mut acc = FlakeAccumulator::pure_delete(2);
        acc.push_retractions(retractions(vec![
            flake(1, 1, 100, 5, false),
            flake(1, 1, 100, 5, false),
        ]));
        assert_eq!(acc.input_count(), 2);
        assert_eq!(acc.finalize().len(), 1);
    }

    #[test]
    #[should_panic(expected = "pure-DELETE accumulator")]
    fn pure_delete_rejects_assertions() {
        let mut acc = FlakeAccumulator::pure_delete(1);
        acc.push_assertions(vec![flake(1, 1, 100, 5, true)]);
    }

    #[test]
    fn pure_delete_supports_assertions_returns_false() {
        let acc = FlakeAccumulator::pure_delete(0);
        assert!(!acc.supports_assertions());
    }

    // ---- Mixed mode: cancellation -------------------------------------------

    #[test]
    fn mixed_cancels_one_to_one_pair() {
        let mut acc = FlakeAccumulator::mixed(2);
        acc.push_retractions(retractions(vec![flake(1, 1, 100, 5, false)]));
        acc.push_assertions(vec![flake(1, 1, 100, 5, true)]);
        assert!(acc.finalize().is_empty());
    }

    #[test]
    fn mixed_keeps_unmatched() {
        let mut acc = FlakeAccumulator::mixed(2);
        acc.push_retractions(retractions(vec![flake(1, 1, 100, 5, false)]));
        acc.push_assertions(vec![flake(1, 1, 200, 5, true)]); // different object
        let out = acc.finalize();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn mixed_4_retracts_1_assert_yields_1_retract() {
        let mut acc = FlakeAccumulator::mixed(2);
        acc.push_retractions(retractions(vec![
            flake(1, 1, 100, 5, false),
            flake(1, 1, 100, 6, false),
            flake(1, 1, 100, 7, false),
            flake(1, 1, 100, 8, false),
        ]));
        acc.push_assertions(vec![flake(1, 1, 100, 9, true)]);
        let out = acc.finalize();
        assert_eq!(out.len(), 1);
        assert!(!out[0].op, "the survivor must be a retraction");
    }

    #[test]
    fn mixed_3_asserts_1_retract_yields_1_assert() {
        let mut acc = FlakeAccumulator::mixed(2);
        acc.push_assertions(vec![
            flake(1, 1, 100, 5, true),
            flake(1, 1, 100, 6, true),
            flake(1, 1, 100, 7, true),
        ]);
        acc.push_retractions(retractions(vec![flake(1, 1, 100, 8, false)]));
        let out = acc.finalize();
        assert_eq!(out.len(), 1);
        assert!(out[0].op, "the survivor must be an assertion");
    }

    #[test]
    fn mixed_set_assertions_lets_one_retraction_cancel_a_repeated_assert() {
        // The graph-sync shape: the payload states a fact twice, the sync
        // wave retracts the current copy once. Under counting semantics one
        // surplus assertion survives and a no-op re-sync commits; under set
        // semantics the fact is unchanged and nothing survives.
        let mut counting = FlakeAccumulator::mixed(2);
        counting.push_assertions(vec![flake(1, 1, 100, 5, true), flake(1, 1, 100, 5, true)]);
        counting.push_retractions(retractions(vec![flake(1, 1, 100, 5, false)]));
        assert_eq!(counting.finalize().len(), 1);

        let mut set = FlakeAccumulator::mixed_set_assertions(2);
        set.push_assertions(vec![flake(1, 1, 100, 5, true), flake(1, 1, 100, 5, true)]);
        set.push_retractions(retractions(vec![flake(1, 1, 100, 5, false)]));
        assert!(
            set.finalize().is_empty(),
            "unchanged fact must net to nothing"
        );

        // A genuinely new fact still survives, and a fact the payload drops
        // is still retracted.
        let mut set = FlakeAccumulator::mixed_set_assertions(2);
        set.push_assertions(vec![flake(1, 1, 101, 5, true), flake(1, 1, 101, 5, true)]);
        set.push_retractions(retractions(vec![flake(1, 1, 100, 5, false)]));
        let out = set.finalize();
        assert_eq!(out.len(), 2);
        assert_eq!(out.iter().filter(|f| f.op).count(), 1);
    }

    #[test]
    fn mixed_collapses_pure_duplicate_assertions() {
        // Same fact asserted 14 times → 1 survivor.
        let mut acc = FlakeAccumulator::mixed(1);
        let v: Vec<_> = (0..14).map(|i| flake(1, 1, 100, i, true)).collect();
        acc.push_assertions(v);
        let out = acc.finalize();
        assert_eq!(out.len(), 1);
        assert!(out[0].op);
    }

    #[test]
    fn mixed_finalize_sort_is_spot_order() {
        let mut acc = FlakeAccumulator::mixed(3);
        acc.push_assertions(vec![
            flake(3, 1, 100, 5, true),
            flake(1, 1, 100, 5, true),
            flake(2, 1, 100, 5, true),
        ]);
        let out = acc.finalize();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].s.namespace_code, 1);
        assert_eq!(out[1].s.namespace_code, 2);
        assert_eq!(out[2].s.namespace_code, 3);
    }

    // ---- Multi-feed: retractions can arrive from several pushes -------------

    #[test]
    fn mixed_multi_feed_retractions_merge_correctly() {
        let mut acc = FlakeAccumulator::mixed(2);
        acc.push_retractions(retractions(vec![flake(1, 1, 100, 5, false)]));
        acc.push_retractions(retractions(vec![flake(1, 1, 100, 6, false)])); // duplicate fact
        acc.push_assertions(vec![flake(1, 1, 100, 7, true)]);
        // 2 retracts vs 1 assert → cancel = 1 → 1 retract survives.
        let out = acc.finalize();
        assert_eq!(out.len(), 1);
        assert!(!out[0].op);
    }

    #[test]
    fn mixed_supports_assertions_returns_true() {
        let acc = FlakeAccumulator::mixed(0);
        assert!(acc.supports_assertions());
    }

    #[test]
    fn pure_delete_accepts_empty_assertion_push() {
        // Edge case: an empty assertion iterator must not trigger the panic
        // guard. (Allows defensively shaped call sites.)
        let mut acc = FlakeAccumulator::pure_delete(0);
        acc.push_assertions(Vec::<Flake>::new());
        assert!(acc.finalize().is_empty());
    }

    // ---- Graph scoping: dedup/cancellation is per (graph, fact) -------------

    fn flake_in_graph(g: u16, s: u16, p: u16, o: i64, t: i64, op: bool) -> Flake {
        Flake::new_in_graph(
            Sid::new(g, format!("g{g}")),
            Sid::new(s, format!("s{s}")),
            Sid::new(p, format!("p{p}")),
            FlakeValue::Long(o),
            Sid::new(2, "long"),
            t,
            op,
            None,
        )
    }

    #[test]
    fn pure_delete_distinguishes_graphs() {
        // Same fact retracted in the default graph AND a named graph must yield
        // two survivors (one per graph), not collapse to one.
        let mut acc = FlakeAccumulator::pure_delete(2);
        acc.push_retractions(retractions(vec![
            flake(1, 1, 100, 5, false),             // default graph (g = None)
            flake_in_graph(7, 1, 1, 100, 5, false), // named graph
        ]));
        let out = acc.finalize();
        assert_eq!(out.len(), 2, "default and named retractions are distinct");
        assert!(out.iter().any(|f| f.g.is_none()));
        assert!(out.iter().any(|f| f.g.is_some()));
    }

    #[test]
    fn mixed_assert_and_named_copy_both_survive() {
        // Same fact asserted in the default graph AND a named graph in one txn:
        // both must survive (no collapse).
        let mut acc = FlakeAccumulator::mixed(2);
        acc.push_assertions(vec![
            flake(1, 1, 100, 5, true),
            flake_in_graph(7, 1, 1, 100, 5, true),
        ]);
        let out = acc.finalize();
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|f| f.op));
    }

    #[test]
    fn mixed_does_not_cancel_across_graphs() {
        // An assertion in a named graph and a retraction of the same fact in the
        // default graph target different graphs and must NOT cancel.
        let mut acc = FlakeAccumulator::mixed(2);
        acc.push_assertions(vec![flake_in_graph(7, 1, 1, 100, 5, true)]);
        acc.push_retractions(retractions(vec![flake(1, 1, 100, 6, false)]));
        let out = acc.finalize();
        assert_eq!(out.len(), 2, "cross-graph assert/retract must not cancel");
        assert!(out.iter().any(|f| f.op && f.g.is_some()));
        assert!(out.iter().any(|f| !f.op && f.g.is_none()));
    }

    #[test]
    fn mixed_cancels_within_same_graph() {
        // Within the SAME named graph, an assert + retract of the same fact
        // cancel 1:1 (unchanged set semantics, now graph-scoped).
        let mut acc = FlakeAccumulator::mixed(2);
        acc.push_assertions(vec![flake_in_graph(7, 1, 1, 100, 5, true)]);
        acc.push_retractions(retractions(vec![flake_in_graph(7, 1, 1, 100, 6, false)]));
        assert!(acc.finalize().is_empty());
    }
}
