//! Assertion/retraction cancellation
//!
//! When the same fact appears both as an assertion and a retraction
//! in a transaction, they cancel each other out. This module implements
//! that cancellation logic.
//!
//! Uses `Flake`'s existing `Eq`/`Hash` implementation which ignores
//! `t` and `op` but includes metadata `m`.

use crate::generate::accumulator::FlakeAccumulator;
use fluree_db_core::Flake;

/// Apply cancellation to a set of flakes
///
/// Removes matching assertion/retraction pairs where the flakes are
/// equal (same subject, predicate, object, datatype, metadata) but
/// differ only in operation.
///
/// Uses counter-based tracking so that duplicate flakes (same fact at
/// different `t` values) are handled correctly. For example, if a fact
/// was asserted 4 times (at t=1,2,3,4) and retracted once, 3 assertions
/// survive — not 0 or 1.
///
/// Returns flakes in deterministic sorted order (by SPOT index) for
/// reproducible hashing and tests.
pub fn apply_cancellation(flakes: Vec<Flake>) -> Vec<Flake> {
    if flakes.is_empty() {
        return flakes;
    }
    let mut acc = FlakeAccumulator::mixed(flakes.len());
    for f in flakes {
        if f.op {
            acc.push_assertions(std::iter::once(f));
        } else {
            acc.push_retractions(std::iter::once(f));
        }
    }
    acc.finalize()
}

/// Dedup a Vec of retractions (cheap path for pure-DELETE transactions).
///
/// Thin wrapper over [`FlakeAccumulator::pure_delete`] kept for backward
/// compatibility with callers that already hold an owned `Vec<Flake>`. New
/// code should construct a [`FlakeAccumulator`] directly so retractions can
/// stream from multiple sources without the intermediate `Vec`.
pub fn dedup_retractions(retractions: Vec<Flake>) -> Vec<Flake> {
    let mut acc = FlakeAccumulator::pure_delete(retractions.len());
    acc.push_retractions(retractions);
    acc.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeValue, Sid};

    fn make_flake(s: u16, p: u16, o: i64, t: i64, op: bool) -> Flake {
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

    #[test]
    fn test_cancellation_removes_pairs() {
        let assertion = make_flake(1, 2, 100, 1, true);
        let retraction = make_flake(1, 2, 100, 1, false);

        let result = apply_cancellation(vec![assertion, retraction]);

        assert!(result.is_empty());
    }

    #[test]
    fn test_cancellation_keeps_unmatched() {
        let assertion = make_flake(1, 2, 100, 1, true);
        let retraction = make_flake(1, 2, 200, 1, false); // Different object

        let result = apply_cancellation(vec![assertion, retraction]);

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_cancellation_order_independent() {
        let flakes1 = vec![
            make_flake(1, 2, 100, 1, true),
            make_flake(1, 2, 100, 1, false),
        ];
        let flakes2 = vec![
            make_flake(1, 2, 100, 1, false),
            make_flake(1, 2, 100, 1, true),
        ];

        let result1 = apply_cancellation(flakes1);
        let result2 = apply_cancellation(flakes2);

        assert!(result1.is_empty());
        assert!(result2.is_empty());
    }

    #[test]
    fn test_cancellation_deterministic_output() {
        let flakes = vec![
            make_flake(3, 1, 100, 1, true),
            make_flake(1, 1, 100, 1, true),
            make_flake(2, 1, 100, 1, true),
        ];

        let result = apply_cancellation(flakes);

        // Should be sorted by SPOT (subject first)
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].s.namespace_code, 1);
        assert_eq!(result[1].s.namespace_code, 2);
        assert_eq!(result[2].s.namespace_code, 3);
    }

    #[test]
    fn test_cancellation_multiple_pairs() {
        let flakes = vec![
            make_flake(1, 1, 100, 1, true),
            make_flake(1, 1, 100, 1, false), // Cancels first
            make_flake(2, 1, 200, 1, true),
            make_flake(2, 1, 200, 1, false), // Cancels third
            make_flake(3, 1, 300, 1, true),  // Remains
        ];

        let result = apply_cancellation(flakes);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].s.namespace_code, 3);
    }

    #[test]
    fn test_cancellation_different_t_still_cancels() {
        // Flake's Eq ignores t, so these should cancel
        let assertion = make_flake(1, 2, 100, 5, true);
        let retraction = make_flake(1, 2, 100, 10, false);

        let result = apply_cancellation(vec![assertion, retraction]);

        assert!(result.is_empty());
    }

    /// RDF set semantics: duplicate retractions of the same fact within one
    /// transaction collapse to a single retraction.
    ///
    /// Scenario: entity has 4 copies of "open" (asserted at t=1,2,3,4 due to
    /// prior bug). Upsert generates 4 retractions + 1 assertion for "open".
    /// After cancellation + set-dedup: 1 retraction survives.
    #[test]
    fn test_cancellation_collapses_duplicate_retractions() {
        let flakes = vec![
            // 4 retractions for same (s,p,o) at different t values
            make_flake(1, 2, 100, 1, false),
            make_flake(1, 2, 100, 2, false),
            make_flake(1, 2, 100, 3, false),
            make_flake(1, 2, 100, 4, false),
            // 1 assertion (upsert re-asserts "open")
            make_flake(1, 2, 100, 5, true),
        ];

        let result = apply_cancellation(flakes);

        // 1 assertion cancels 1 retraction, remaining 3 collapse to 1
        assert_eq!(result.len(), 1, "should have 1 surviving retraction");
        assert!(
            result.iter().all(|f| !f.op),
            "survivor should be a retraction"
        );
    }

    /// RDF set semantics: duplicate assertions collapse to one.
    #[test]
    fn test_cancellation_collapses_duplicate_assertions() {
        let flakes = vec![
            // 3 assertions for same (s,p,o) at different t values
            make_flake(1, 2, 100, 1, true),
            make_flake(1, 2, 100, 2, true),
            make_flake(1, 2, 100, 3, true),
            // 1 retraction
            make_flake(1, 2, 100, 5, false),
        ];

        let result = apply_cancellation(flakes);

        // 1 retraction cancels 1 assertion, remaining 2 collapse to 1
        assert_eq!(result.len(), 1, "should have 1 surviving assertion");
        assert!(
            result.iter().all(|f| f.op),
            "survivor should be an assertion"
        );
    }

    /// Mixed scenario: some facts have duplicates, some don't.
    /// Set semantics collapses duplicate survivors to one per fact.
    #[test]
    fn test_cancellation_mixed_duplicates_and_unique() {
        let flakes = vec![
            // Fact A: 3 retractions, 1 assertion → 2 retractions collapse to 1
            make_flake(1, 1, 100, 1, false),
            make_flake(1, 1, 100, 2, false),
            make_flake(1, 1, 100, 3, false),
            make_flake(1, 1, 100, 5, true),
            // Fact B: 1 retraction, 1 assertion → cancel out
            make_flake(1, 1, 200, 1, false),
            make_flake(1, 1, 200, 5, true),
            // Fact C: 1 assertion only → survives
            make_flake(1, 1, 300, 5, true),
        ];

        let result = apply_cancellation(flakes);

        // 1 retraction (fact A) + 1 assertion (fact C) = 2
        assert_eq!(result.len(), 2);
        let retraction_count = result.iter().filter(|f| !f.op).count();
        let assertion_count = result.iter().filter(|f| f.op).count();
        assert_eq!(retraction_count, 1, "fact A: 1 retraction survives");
        assert_eq!(assertion_count, 1, "fact C: 1 assertion survives");
    }

    /// Regression test for the nested-object duplication bug: when a JSON-LD
    /// transaction contains the same entity nested in multiple parents (e.g.,
    /// a Member nested in 14 different Channel objects), parsing produces N
    /// identical assertion flakes per property. Set semantics must collapse
    /// these to 1 flake per unique fact.
    #[test]
    fn test_set_semantics_dedup_pure_assertions() {
        // Simulates member with 4 properties, each duplicated 14 times
        let mut flakes = Vec::new();
        for prop in 1..=4u16 {
            for _ in 0..14 {
                flakes.push(make_flake(1, prop, 42, 1, true));
            }
        }
        assert_eq!(flakes.len(), 56);

        let result = apply_cancellation(flakes);

        // Should collapse to 4 unique assertions (one per property)
        assert_eq!(result.len(), 4, "56 duplicate assertions → 4 unique facts");
        assert!(result.iter().all(|f| f.op), "all should be assertions");
    }
}
