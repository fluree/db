//! Equality OWL2-RL rules (eq-*).
//!
//! This module implements equality rules from the OWL2-RL profile:
//! - `eq-sym` / `eq-trans` - owl:sameAs symmetry and transitivity via union-find

use fluree_db_core::value::FlakeValue;
use fluree_db_core::Sid;

use crate::same_as::SameAsTracker;
use crate::ReasoningDiagnostics;

use super::delta::DeltaSet;

/// Apply sameAs rules using union-find
///
/// For each new owl:sameAs assertion, union the equivalence classes.
/// This handles eq-sym and eq-trans implicitly through union-find.
pub fn apply_same_as_rule(
    delta: &DeltaSet,
    same_as_tracker: &mut SameAsTracker,
    owl_same_as_sid: &Sid,
    diagnostics: &mut ReasoningDiagnostics,
) -> bool {
    let mut changed = false;

    for flake in delta.get_by_p(owl_same_as_sid) {
        if let FlakeValue::Ref(y) = &flake.o {
            let x = &flake.s;
            if same_as_tracker.union(x, y) {
                changed = true;
                diagnostics.record_rule_fired("eq-union");
            }
        }
    }

    changed
}
