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

use fluree_db_core::{Flake, IndexType};
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};

/// Accumulates flakes from one or more sources and produces a deterministic,
/// deduplicated survivor set. See [module docs](self) for design notes.
pub struct FlakeAccumulator {
    inner: AccInner,
    input_count: u64,
}

enum AccInner {
    /// Pure-DELETE: only retractions, deduplicated by fact identity.
    PureRetract(FxHashSet<Flake>),
    /// Mixed assertions + retractions with 1:1 per-fact cancellation.
    Mixed(FxHashMap<Flake, FlakeBucket>),
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

impl FlakeAccumulator {
    /// Pure-DELETE accumulator: drops the assertion bookkeeping side entirely.
    /// Use only when the transaction has no INSERT templates and is not Upsert.
    pub fn pure_delete(capacity_hint: usize) -> Self {
        Self {
            inner: AccInner::PureRetract(FxHashSet::with_capacity_and_hasher(
                capacity_hint,
                FxBuildHasher,
            )),
            input_count: 0,
        }
    }

    /// Mixed assertion + retraction accumulator with set-semantics cancellation.
    pub fn mixed(capacity_hint: usize) -> Self {
        Self {
            inner: AccInner::Mixed(FxHashMap::with_capacity_and_hasher(
                capacity_hint,
                FxBuildHasher,
            )),
            input_count: 0,
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

    /// Push retractions from any source. Duplicates collapse to a single
    /// representative survivor (RDF set semantics).
    pub fn push_retractions<I: IntoIterator<Item = Flake>>(&mut self, flakes: I) {
        match &mut self.inner {
            AccInner::PureRetract(set) => {
                for f in flakes {
                    debug_assert!(!f.op, "push_retractions received an assertion (op=true)");
                    self.input_count += 1;
                    // Dropping `f` on collision is the dedup — the `FxHashSet`
                    // keeps the first inserted flake and drops the duplicate.
                    set.insert(f);
                }
            }
            AccInner::Mixed(map) => {
                for f in flakes {
                    debug_assert!(!f.op, "push_retractions received an assertion (op=true)");
                    self.input_count += 1;
                    push_into_mixed(map, f);
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
            AccInner::Mixed(map) => {
                for f in flakes {
                    debug_assert!(f.op, "push_assertions received a retraction (op=false)");
                    self.input_count += 1;
                    push_into_mixed(map, f);
                }
            }
        }
    }

    /// Drain into the final SPOT-sorted survivor set.
    ///
    /// For pure-DELETE: every unique retracted fact contributes one survivor.
    /// For mixed: per fact, `cancel = min(assert_count, retract_count)` pairs
    /// cancel; surplus on either side contributes one survivor of that op.
    pub fn finalize(self) -> Vec<Flake> {
        let mut out: Vec<Flake> = match self.inner {
            AccInner::PureRetract(set) => set.into_iter().collect(),
            AccInner::Mixed(map) => {
                let mut v = Vec::with_capacity(map.len());
                for (_key, b) in map {
                    let cancel = b.assert_count.min(b.retract_count);
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
                v
            }
        };
        out.sort_by(|a, b| IndexType::Spot.compare(a, b));
        out
    }
}

/// Insert a flake into the mixed-mode bucket map.
///
/// First push for a given fact pays one `Flake::clone()` (one copy lives in
/// the map's key, one copy lives in the bucket as the survivor candidate).
/// Subsequent pushes for the same fact are clone-free: the bucket's survivor
/// slot is overwritten via `Some(flake)` and the previous survivor is dropped.
fn push_into_mixed(map: &mut FxHashMap<Flake, FlakeBucket>, flake: Flake) {
    if let Some(bucket) = map.get_mut(&flake) {
        if flake.op {
            bucket.assert_count = bucket.assert_count.saturating_add(1);
            bucket.assertion = Some(flake);
        } else {
            bucket.retract_count = bucket.retract_count.saturating_add(1);
            bucket.retraction = Some(flake);
        }
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
        acc.push_retractions(vec![
            flake(1, 1, 100, 5, false),
            flake(1, 1, 100, 5, false),
            flake(1, 1, 100, 5, false),
        ]);
        let out = acc.finalize();
        assert_eq!(out.len(), 1);
        assert!(!out[0].op);
    }

    #[test]
    fn pure_delete_preserves_distinct_facts() {
        let mut acc = FlakeAccumulator::pure_delete(4);
        acc.push_retractions(vec![
            flake(1, 1, 100, 5, false),
            flake(2, 1, 100, 5, false),
            flake(3, 1, 100, 5, false),
        ]);
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
        acc.push_retractions(vec![flake(1, 1, 100, 5, false), flake(1, 1, 100, 5, false)]);
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
        acc.push_retractions(vec![flake(1, 1, 100, 5, false)]);
        acc.push_assertions(vec![flake(1, 1, 100, 5, true)]);
        assert!(acc.finalize().is_empty());
    }

    #[test]
    fn mixed_keeps_unmatched() {
        let mut acc = FlakeAccumulator::mixed(2);
        acc.push_retractions(vec![flake(1, 1, 100, 5, false)]);
        acc.push_assertions(vec![flake(1, 1, 200, 5, true)]); // different object
        let out = acc.finalize();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn mixed_4_retracts_1_assert_yields_1_retract() {
        let mut acc = FlakeAccumulator::mixed(2);
        acc.push_retractions(vec![
            flake(1, 1, 100, 5, false),
            flake(1, 1, 100, 6, false),
            flake(1, 1, 100, 7, false),
            flake(1, 1, 100, 8, false),
        ]);
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
        acc.push_retractions(vec![flake(1, 1, 100, 8, false)]);
        let out = acc.finalize();
        assert_eq!(out.len(), 1);
        assert!(out[0].op, "the survivor must be an assertion");
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
        acc.push_retractions(vec![flake(1, 1, 100, 5, false)]);
        acc.push_retractions(vec![flake(1, 1, 100, 6, false)]); // duplicate fact
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
}
