//! `NoveltyFactState` — the current-state fact index for the in-memory novelty
//! window.
//!
//! This is **not** database-wide uniqueness. It records, per graph, the *latest
//! known op for a fact identity within the current novelty window* — i.e. what
//! `Novelty::apply_commit` needs to apply RDF set semantics without scanning the
//! sorted index vectors (and, once novelty is segmented, without scanning every
//! segment). Persisted-index duplicates are still resolved later by the overlay
//! cursor / index merge, exactly as today.
//!
//! ## Why it exists
//!
//! Dedup previously bisected the per-graph SPOT vector (`fact_currently_-
//! asserted_in_graph`). Segmented novelty removes that single sorted vector, so
//! the dedup oracle is lifted into this structure behind a seam. It is backed by
//! a persistent map (`imbl::OrdMap`) so `clone` is O(1) structural sharing —
//! preserving the novelty clone win under concurrent readers.
//!
//! ## Complexity
//!
//! `clone` is O(1), but `is_asserted` / `record` are **O(log novelty_facts)**
//! (a balanced-tree probe plus `FactKey` cloning), *not* O(1). This is still far
//! cheaper than the old O(total-novelty) index re-merge it replaced, and the
//! `log` factor is dominated by the per-commit segment build. If a slope
//! measurement ever shows residual growth attributable to dedup, the candidate
//! is `imbl::HashMap` for amortized O(1) — but note that would switch key
//! equality from `Ord` (matching `same_identity` / `IndexType::compare`) to
//! `Hash`/`Eq`, and `FlakeValue`'s `Eq` treats cross-representation numerics
//! (`Long(3) == Double(3.0) == BigInt(3)`) as equal where `Ord` may not. That is
//! a semantic change the current equivalence harness (which only generates
//! `Long`) would not catch, so it needs dedicated cross-type dedup tests first.
//!
//! ## Identity & graph scoping
//!
//! The key is `(s, p, o, dt, m)`; its derived `Ord` matches `same_identity`
//! ordering (s, p, then object value, datatype, then meta). Graph is **not** part
//! of the key — `Flake` equality ignores `g`, so state is held in a separate map
//! per `g_id`.

use fluree_db_core::{Flake, FlakeMeta, FlakeValue, GraphId, Sid};
use imbl::OrdMap;

/// Fact identity within one graph: `(s, p, o, dt, m)`. Derived `Ord` is
/// component-wise (s, p, o, dt, m), which equals `same_identity`'s ordering.
type FactKey = (Sid, Sid, FlakeValue, Sid, Option<FlakeMeta>);

fn key_of(f: &Flake) -> FactKey {
    (
        f.s.clone(),
        f.p.clone(),
        f.o.clone(),
        f.dt.clone(),
        f.m.clone(),
    )
}

/// Per-graph "latest known op for an identity, within the current novelty
/// window." The stored value is `(t, op)`: the resolved transaction time and
/// op (`true` = currently asserted, `false` = currently retracted tombstone).
/// Cheap to clone (persistent map).
#[derive(Clone, Default)]
pub(crate) struct NoveltyFactState {
    /// Indexed by `g_id`; `None` for graphs with no novelty yet.
    graphs: Vec<Option<OrdMap<FactKey, (i64, bool)>>>,
}

impl NoveltyFactState {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Is `flake`'s identity currently asserted in graph `g`'s novelty window?
    /// Absent identity → `false`.
    pub(crate) fn is_asserted(&self, g: GraphId, flake: &Flake) -> bool {
        self.graphs
            .get(g as usize)
            .and_then(Option::as_ref)
            .and_then(|m| m.get(&key_of(flake)).map(|&(_, op)| op))
            .unwrap_or(false)
    }

    /// Record `flake` as an op for its identity in graph `g`. Lifecycle
    /// resolution is **order-independent**: the highest-`t` op wins, and on a
    /// same-`t` tie the retract wins. This mirrors the query overlay's
    /// resolution ([`fluree-db-binary-index`] `resolve_overlay_ops`), so
    /// `fact_state` cannot disagree with what a read would see no matter what
    /// order records arrive in (commit/batch order, the sorted bulk walk, or the
    /// `clear_up_to` rebuild) — rather than depending on callers feeding ops in
    /// ascending-`t`, retract-last order.
    pub(crate) fn record(&mut self, g: GraphId, flake: &Flake) {
        let idx = g as usize;
        if idx >= self.graphs.len() {
            self.graphs.resize_with(idx + 1, || None);
        }
        let m = self.graphs[idx].get_or_insert_with(OrdMap::new);
        let key = key_of(flake);
        let wins = match m.get(&key) {
            Some(&(cur_t, cur_op)) => {
                flake.t > cur_t || (flake.t == cur_t && cur_op && !flake.op)
            }
            None => true,
        };
        if wins {
            m.insert(key, (flake.t, flake.op));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(s: u16, t: i64, op: bool, lang: Option<&str>) -> Flake {
        Flake::new(
            Sid::new(1, format!("s{s}")),
            Sid::new(1, "p"),
            FlakeValue::Long(7),
            Sid::new(2, "long"),
            t,
            op,
            lang.map(FlakeMeta::with_lang),
        )
    }

    #[test]
    fn assert_then_retract_then_reassert() {
        // A real lifecycle is a sequence of distinct-`t` commits.
        let mut fs = NoveltyFactState::new();
        let f = mk(1, 0, true, None); // `t` irrelevant to identity lookup
        assert!(!fs.is_asserted(0, &f), "absent => not asserted");
        fs.record(0, &mk(1, 1, true, None));
        assert!(fs.is_asserted(0, &f));
        fs.record(0, &mk(1, 2, false, None)); // retract @ later t
        assert!(!fs.is_asserted(0, &f), "retract tombstones");
        fs.record(0, &mk(1, 3, true, None)); // reassert @ later t
        assert!(fs.is_asserted(0, &f), "later reassert wins");
    }

    #[test]
    fn same_t_retract_wins_order_independent() {
        // A same-`t` assert+retract of one identity resolves to ABSENT (retract
        // wins), matching overlay lifecycle resolution — regardless of the order
        // the two ops are recorded.
        let mut fs = NoveltyFactState::new();
        fs.record(0, &mk(1, 5, true, None));
        fs.record(0, &mk(1, 5, false, None));
        assert!(!fs.is_asserted(0, &mk(1, 5, true, None)), "assert then retract");

        let mut fs2 = NoveltyFactState::new();
        fs2.record(0, &mk(2, 5, false, None));
        fs2.record(0, &mk(2, 5, true, None));
        assert!(!fs2.is_asserted(0, &mk(2, 5, true, None)), "retract then assert");
    }

    #[test]
    fn highest_t_wins_regardless_of_record_order() {
        // An older op recorded after a newer one must not clobber it.
        let mut fs = NoveltyFactState::new();
        fs.record(0, &mk(1, 9, true, None)); // newer assert recorded first
        fs.record(0, &mk(1, 2, false, None)); // older retract recorded after
        assert!(
            fs.is_asserted(0, &mk(1, 9, true, None)),
            "stale lower-t op cannot override the resolved latest op"
        );
    }

    #[test]
    fn graphs_are_independent() {
        let mut fs = NoveltyFactState::new();
        let f = mk(1, 1, true, None);
        fs.record(1, &f);
        assert!(fs.is_asserted(1, &f));
        assert!(!fs.is_asserted(0, &f), "default graph unaffected");
        assert!(!fs.is_asserted(2, &f), "other graph unaffected");
    }

    #[test]
    fn meta_is_part_of_identity() {
        let mut fs = NoveltyFactState::new();
        fs.record(0, &mk(1, 1, true, Some("en")));
        assert!(fs.is_asserted(0, &mk(1, 1, true, Some("en"))));
        assert!(
            !fs.is_asserted(0, &mk(1, 1, true, Some("fr"))),
            "different lang = different identity"
        );
        assert!(
            !fs.is_asserted(0, &mk(1, 1, true, None)),
            "no-meta = different identity"
        );
    }
}
