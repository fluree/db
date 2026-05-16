//! Cross-ledger resolution types.
//!
//! See `docs/design/cross-ledger-model-enforcement.md` for the
//! semantics of each type. The orchestration that uses these lives in
//! `resolver.rs`; pure helpers below (reserved-graph guard, cycle
//! check, memo lookup) are kept here and unit-tested in isolation.

use super::CrossLedgerError;
use crate::Fluree;
use fluree_db_core::graph_registry::{config_graph_iri, txn_meta_graph_iri};
use fluree_db_policy::PolicyArtifactWire;
use std::collections::HashMap;
use std::sync::Arc;

/// Which subsystem's artifact is being resolved.
///
/// Only `PolicyRules` is implemented in Phase 1a; the remaining
/// variants land in later phases per the design doc's phasing table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactKind {
    /// `f:policySource` → policy rule set.
    PolicyRules,
    // SchemaClosure  — Phase 1b
    // Shapes         — Phase 2
    // DatalogRules   — Phase 2
    // Constraints    — Phase 2
}

/// A successfully resolved, term-neutral governance artifact.
///
/// Cached at the API layer by `(model_ledger_id, graph_iri,
/// resolved_t)`; per-data-ledger interning is a separate step.
#[derive(Debug, Clone)]
pub struct ResolvedGraph {
    /// Canonical model ledger id this artifact came from.
    pub model_ledger_id: String,
    /// Graph IRI within the model ledger.
    pub graph_iri: String,
    /// Model ledger `t` at which the artifact was materialized.
    pub resolved_t: i64,
    /// The artifact itself, tagged by subsystem.
    pub artifact: GovernanceArtifact,
}

/// Tagged union of governance artifacts.
///
/// The kind is named so a `ResolveCtx` memo (which is keyed only on
/// `(ledger, graph, t)`) can carry mixed artifact types without
/// dynamic dispatch and so callers can pattern-match to extract the
/// shape they expect.
#[derive(Debug, Clone)]
pub enum GovernanceArtifact {
    /// Policy rule set in IRI-form. Translate to `PolicySet` via
    /// `fluree_db_policy::build_policy_set_from_wire`.
    PolicyRules(PolicyArtifactWire),
    // SchemaClosure(SchemaBundleWire) — Phase 1b
    // Shapes(ShapeSetWire)            — Phase 2
    // DatalogRules(DatalogRuleSetWire) — Phase 2
    // Constraints(ConstraintSetWire)  — Phase 2
}

/// Per-request resolution context.
///
/// Born with the full lifetime / consistency model so the resolver
/// API is correct from day one even before materialization lands:
///
/// - `resolved_ts` captures the lazy per-request head-t per
///   canonical model ledger id (governance-context capture). Lookup
///   on miss reads M's head once and stores it; later unpinned
///   references to the same M reuse the same value so policy and
///   shapes can never disagree about which version of M they're
///   enforcing.
///
/// - `active` is the resolution stack used for cycle detection. Push
///   before recursion, pop after. A tuple is a cycle only when it is
///   encountered while *already on the stack*.
///
/// - `memo` is the per-request completed map. Subsequent references
///   to the same `(ledger, graph, t)` tuple — from any subsystem —
///   short-circuit on a memo hit. Memo hits never enter `active`,
///   so cross-subsystem de-dup never trips cycle detection.
pub struct ResolveCtx<'a> {
    /// Canonical data-ledger id D.
    pub data_ledger_id: &'a str,
    /// The Fluree instance hosting D and (per the same-instance
    /// constraint) the referenced model ledger.
    pub fluree: &'a Fluree,
    /// Lazy governance-context capture: canonical model ledger id →
    /// `resolved_t` for unpinned references. Pinned `f:atT` does NOT
    /// populate this map; pinned values are per-resolve.
    pub resolved_ts: HashMap<String, i64>,
    /// Active resolution stack (cycle detection).
    pub active: Vec<(String, String, i64)>,
    /// Per-request completed memo.
    pub memo: HashMap<(String, String, i64), Arc<ResolvedGraph>>,
}

impl<'a> ResolveCtx<'a> {
    /// Build a fresh resolution context for a request against D.
    pub fn new(data_ledger_id: &'a str, fluree: &'a Fluree) -> Self {
        Self {
            data_ledger_id,
            fluree,
            resolved_ts: HashMap::new(),
            active: Vec::new(),
            memo: HashMap::new(),
        }
    }
}

/// Reject selectors that resolve to the model ledger's `#config` or
/// `#txn-meta` graphs.
///
/// Applied *before* any storage round-trip on the model ledger —
/// `#txn-meta` in particular can leak commit metadata, and `#config`
/// is the recursive seed that defines what model M is. Neither is
/// ever a legitimate target of a cross-ledger governance reference.
///
/// Pure on `(canonical_ledger_id, graph_iri)`; no I/O.
pub(crate) fn reject_if_reserved_graph(
    canonical_ledger_id: &str,
    graph_iri: &str,
) -> Result<(), CrossLedgerError> {
    if graph_iri == config_graph_iri(canonical_ledger_id)
        || graph_iri == txn_meta_graph_iri(canonical_ledger_id)
    {
        return Err(CrossLedgerError::ReservedGraphSelected {
            graph_iri: graph_iri.to_string(),
        });
    }
    Ok(())
}

/// Memo lookup for the per-request completed map.
///
/// Memo hits short-circuit before `active` is consulted, so two
/// subsystems referencing the same `(ledger, graph, t)` resolve once
/// and never trip cycle detection.
pub(crate) fn memo_hit(
    memo: &HashMap<(String, String, i64), Arc<ResolvedGraph>>,
    tuple: &(String, String, i64),
) -> Option<Arc<ResolvedGraph>> {
    memo.get(tuple).cloned()
}

/// Cycle check against the active resolution stack.
///
/// Returns the cycle as a chain (active stack + the offending tuple
/// appended) when one is detected.
pub(crate) fn check_cycle(
    active: &[(String, String, i64)],
    tuple: &(String, String, i64),
) -> Result<(), CrossLedgerError> {
    if active.iter().any(|t| t == tuple) {
        let mut chain = active.to_vec();
        chain.push(tuple.clone());
        return Err(CrossLedgerError::CycleDetected { chain });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_graph_guard_blocks_config_and_txn_meta() {
        let ledger = "model:main";
        assert!(matches!(
            reject_if_reserved_graph(ledger, "urn:fluree:model:main#config"),
            Err(CrossLedgerError::ReservedGraphSelected { .. })
        ));
        assert!(matches!(
            reject_if_reserved_graph(ledger, "urn:fluree:model:main#txn-meta"),
            Err(CrossLedgerError::ReservedGraphSelected { .. })
        ));
    }

    #[test]
    fn reserved_graph_guard_allows_application_graphs() {
        let ledger = "model:main";
        // A user-named graph that happens to live on the model
        // ledger is fine — we only block the system graphs.
        assert!(reject_if_reserved_graph(ledger, "http://example.org/policy").is_ok());
        // Even a config-shaped IRI for a *different* ledger is fine —
        // it can't actually resolve to model:main's #config.
        assert!(
            reject_if_reserved_graph(ledger, "urn:fluree:other:main#config").is_ok(),
            "config IRI for a different ledger should not match"
        );
    }

    #[test]
    fn cycle_check_passes_for_unique_tuples() {
        let active = vec![
            ("a:main".to_string(), "http://ex.org/p".to_string(), 10),
            ("b:main".to_string(), "http://ex.org/q".to_string(), 20),
        ];
        let new_tuple = ("c:main".to_string(), "http://ex.org/r".to_string(), 30);
        assert!(check_cycle(&active, &new_tuple).is_ok());
    }

    #[test]
    fn cycle_check_fails_on_reentry_and_renders_full_chain() {
        let cycle_tuple = ("a:main".to_string(), "http://ex.org/p".to_string(), 10);
        let active = vec![
            cycle_tuple.clone(),
            ("b:main".to_string(), "http://ex.org/q".to_string(), 20),
        ];
        let err = check_cycle(&active, &cycle_tuple).unwrap_err();
        match err {
            CrossLedgerError::CycleDetected { chain } => {
                // The chain should contain the original active stack
                // (in order) plus the offending tuple appended.
                assert_eq!(chain.len(), 3);
                assert_eq!(chain[0], cycle_tuple);
                assert_eq!(chain[2], cycle_tuple);
            }
            other => panic!("expected CycleDetected, got {other:?}"),
        }
    }

    #[test]
    fn cycle_check_treats_different_t_pins_of_same_graph_as_distinct() {
        // Two f:atT pins of the same (ledger, graph) are NOT a cycle.
        // The design doc is explicit on this.
        let active = vec![("a:main".to_string(), "http://ex.org/p".to_string(), 10)];
        let later_pin = ("a:main".to_string(), "http://ex.org/p".to_string(), 20);
        assert!(check_cycle(&active, &later_pin).is_ok());
    }

    #[test]
    fn memo_returns_cloned_arc_on_hit_and_none_on_miss() {
        let mut memo = HashMap::new();
        let tuple = ("a:main".to_string(), "http://ex.org/p".to_string(), 10);

        let payload = Arc::new(ResolvedGraph {
            model_ledger_id: tuple.0.clone(),
            graph_iri: tuple.1.clone(),
            resolved_t: tuple.2,
            artifact: GovernanceArtifact::PolicyRules(PolicyArtifactWire {
                origin: fluree_db_policy::WireOrigin {
                    model_ledger_id: tuple.0.clone(),
                    graph_iri: tuple.1.clone(),
                    resolved_t: tuple.2,
                },
                restrictions: vec![],
            }),
        });

        assert!(memo_hit(&memo, &tuple).is_none());
        memo.insert(tuple.clone(), payload.clone());

        let hit = memo_hit(&memo, &tuple).expect("hit after insert");
        assert!(Arc::ptr_eq(&hit, &payload), "memo must return shared Arc");
    }
}
