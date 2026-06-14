//! Pattern rewriting glue
//!
//! Applies RDFS/OWL expansion to query patterns based on reasoning modes.

use crate::ir::{Pattern, ReasoningModes};
use crate::rewrite::{rewrite_patterns, Diagnostics, EntailmentMode, PlanContext, PlanLimits};
use crate::rewrite_owl_ql::{rewrite_owl_ql_patterns, Ontology, OwlQlContext};
use fluree_db_core::SchemaHierarchy;

/// Internal helper for pattern rewriting based on entailment mode
///
/// Applies RDFS/OWL expansion to patterns if reasoning is enabled.
/// Returns the rewritten patterns (or original if no rewriting needed).
///
/// # Reasoning Modes
///
/// - No modes enabled: No rewriting, returns patterns unchanged
/// - `rdfs`: Expands `rdf:type` patterns to include subclasses and predicates to include subproperties
/// - `owl2ql`: Applies RDFS expansion plus OWL2-QL rewriting (owl:inverseOf, rdfs:domain/range type inference)
/// - Multiple modes can be combined (e.g., `rdfs` + `owl2ql`)
pub fn rewrite_query_patterns(
    patterns: &[Pattern],
    hierarchy: Option<SchemaHierarchy>,
    reasoning: &ReasoningModes,
    ontology: Option<&Ontology>,
) -> (Vec<Pattern>, Diagnostics) {
    // When OWL2-RL materialization is active, every entailment the pattern
    // rewriter would expand — subclass / subproperty UNIONs, owl:inverseOf,
    // rdfs:domain/range type inference — has already been forward-chained into
    // the derived-facts overlay. Re-applying the query rewrite on top would
    // match the same entailment twice (once via a materialized superclass/
    // superproperty fact, once via a base subclass/subproperty UNION branch),
    // yielding duplicate solution rows under bag semantics (COUNT, aggregation,
    // non-DISTINCT SELECT). Materialization is a superset of what the rewrite
    // produces, so rely on it alone and skip the rewrite entirely.
    if reasoning.owl2rl {
        return (patterns.to_vec(), Diagnostics::default());
    }

    // Check if any pattern rewriting is needed
    let needs_rdfs = reasoning.rdfs;
    let needs_owl2ql = reasoning.owl2ql;

    if !needs_rdfs && !needs_owl2ql {
        return (patterns.to_vec(), Diagnostics::default());
    }

    // Start with the original patterns
    let mut current_patterns = patterns.to_vec();
    let mut combined_diag = Diagnostics::default();

    // Apply RDFS expansion (subclass/subproperty) if enabled
    if needs_rdfs {
        let entailment_mode = if needs_owl2ql {
            EntailmentMode::OwlQl // Will also trigger OWL2-QL later
        } else {
            EntailmentMode::Rdfs
        };

        let plan_ctx = PlanContext {
            entailment_mode,
            hierarchy: hierarchy.clone(),
            limits: PlanLimits::default(),
        };

        let (rdfs_rewritten, rdfs_diag) = rewrite_patterns(&current_patterns, &plan_ctx);
        current_patterns = rdfs_rewritten;
        combined_diag = merge_diagnostics(combined_diag, rdfs_diag);
    }

    // Apply OWL2-QL rewriting if enabled
    if needs_owl2ql {
        if let Some(ont) = ontology {
            let plan_ctx = PlanContext {
                entailment_mode: EntailmentMode::OwlQl,
                hierarchy: hierarchy.clone(),
                limits: PlanLimits::default(),
            };
            let owl_ctx = OwlQlContext::new(plan_ctx, Some(ont.clone()));
            let (owl_rewritten, owl_diag) = rewrite_owl_ql_patterns(&current_patterns, &owl_ctx);
            current_patterns = owl_rewritten;
            combined_diag = merge_diagnostics(combined_diag, owl_diag);
        }
    }

    (current_patterns, combined_diag)
}

/// Merge two Diagnostics structs
pub fn merge_diagnostics(a: Diagnostics, b: Diagnostics) -> Diagnostics {
    Diagnostics {
        patterns_expanded: a.patterns_expanded + b.patterns_expanded,
        type_expansions: a.type_expansions + b.type_expansions,
        predicate_expansions: a.predicate_expansions + b.predicate_expansions,
        was_capped: a.was_capped || b.was_capped,
        schema_epoch: b.schema_epoch.or(a.schema_epoch),
        warnings: {
            let mut w = a.warnings;
            w.extend(b.warnings);
            w
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_rewriting_when_disabled() {
        let patterns = vec![];
        let modes = ReasoningModes::default();

        let (rewritten, diag) = rewrite_query_patterns(&patterns, None, &modes, None);
        assert_eq!(rewritten.len(), 0);
        assert_eq!(diag.patterns_expanded, 0);
    }
}
