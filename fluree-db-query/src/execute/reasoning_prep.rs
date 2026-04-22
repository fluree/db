//! Reasoning preparation helpers
//!
//! Handles schema hierarchy computation, effective reasoning mode determination,
//! and derived facts computation for OWL2-RL and datalog rules.

use crate::reasoning::{global_reasoning_cache, reason_owl2rl, ReasoningOverlay};
use crate::rewrite::ReasoningModes;
use fluree_db_core::{
    is_rdfs_subclass_of, is_rdfs_subproperty_of, overlay::OverlayProvider, GraphDbRef, GraphId,
    IndexSchema, LedgerSnapshot, SchemaHierarchy, SchemaPredicateInfo,
};
use fluree_db_reasoner::{
    DerivedFactsBuilder, DerivedFactsOverlay, FrozenSameAs, ReasoningOptions,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Build schema hierarchy from database and overlay
///
/// Merges overlay rdfs:subClassOf and rdfs:subPropertyOf assertions
/// with the existing database schema to create a unified hierarchy view.
pub fn schema_hierarchy_with_overlay(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
) -> Option<SchemaHierarchy> {
    use fluree_db_core::value::FlakeValue;

    // Build child -> parents from overlay rdfs:subClassOf assertions.
    let mut subclass_of: HashMap<fluree_db_core::Sid, Vec<fluree_db_core::Sid>> = HashMap::new();
    // Build child -> parents from overlay rdfs:subPropertyOf assertions.
    let mut subproperty_of: HashMap<fluree_db_core::Sid, Vec<fluree_db_core::Sid>> = HashMap::new();
    overlay.for_each_overlay_flake(
        0, // default graph — schema hierarchy is default-graph only
        fluree_db_core::IndexType::Psot,
        None,
        None,
        true,
        to_t,
        &mut |flake| {
            if !is_rdfs_subclass_of(&flake.p) {
                // fall through for subPropertyOf
            } else if let FlakeValue::Ref(parent) = &flake.o {
                subclass_of
                    .entry(flake.s.clone())
                    .or_default()
                    .push(parent.clone());
                return;
            }

            if !is_rdfs_subproperty_of(&flake.p) {
                return;
            }
            if let FlakeValue::Ref(parent) = &flake.o {
                subproperty_of
                    .entry(flake.s.clone())
                    .or_default()
                    .push(parent.clone());
            }
        },
    );

    // Merge overlay edges into the LedgerSnapshot's existing schema (if any).
    //
    // Important: in memory-backed tests, schema relationships often exist only in novelty,
    // while `db.schema` reflects the last indexed root. We need a merged view for entailment.
    let mut schema: IndexSchema = snapshot.schema.clone().unwrap_or_default();
    schema.t = to_t;

    // Index existing vals by id for merging.
    let mut by_id: HashMap<fluree_db_core::Sid, SchemaPredicateInfo> = schema
        .pred
        .vals
        .into_iter()
        .map(|spi| (spi.id.clone(), spi))
        .collect();

    for (id, mut parents) in subclass_of {
        parents.sort();
        parents.dedup();
        by_id
            .entry(id.clone())
            .and_modify(|spi| {
                spi.subclass_of.extend(parents.clone());
                spi.subclass_of.sort();
                spi.subclass_of.dedup();
            })
            .or_insert(SchemaPredicateInfo {
                id,
                subclass_of: parents,
                parent_props: Vec::new(),
                child_props: Vec::new(),
            });
    }

    // Merge overlay subPropertyOf edges.
    for (child, mut parents) in subproperty_of {
        parents.sort();
        parents.dedup();

        // Update child -> parent_props
        by_id
            .entry(child.clone())
            .and_modify(|spi| {
                spi.parent_props.extend(parents.clone());
                spi.parent_props.sort();
                spi.parent_props.dedup();
            })
            .or_insert(SchemaPredicateInfo {
                id: child.clone(),
                subclass_of: Vec::new(),
                parent_props: parents.clone(),
                child_props: Vec::new(),
            });

        // Update parent -> child_props (inverse edges)
        for parent in parents {
            by_id
                .entry(parent.clone())
                .and_modify(|spi| {
                    spi.child_props.push(child.clone());
                    spi.child_props.sort();
                    spi.child_props.dedup();
                })
                .or_insert(SchemaPredicateInfo {
                    id: parent,
                    subclass_of: Vec::new(),
                    parent_props: Vec::new(),
                    child_props: vec![child.clone()],
                });
        }
    }

    let mut vals: Vec<SchemaPredicateInfo> = by_id.into_values().collect();
    vals.sort_by(|a, b| a.id.cmp(&b.id));
    schema.pred.vals = vals;

    if schema.pred.vals.is_empty() {
        None
    } else {
        Some(SchemaHierarchy::from_db_root_schema(&schema))
    }
}

/// Compute effective reasoning modes given query options and available hierarchy
///
/// Applies auto-RDFS when:
/// - No explicit reasoning modes are set in query
/// - Reasoning is not explicitly disabled ("reasoning": "none")
/// - A schema hierarchy is available
pub fn effective_reasoning_modes(
    configured: &ReasoningModes,
    hierarchy_available: bool,
) -> ReasoningModes {
    configured
        .clone()
        .effective_with_hierarchy(hierarchy_available)
}

/// Compute derived facts from OWL2-RL reasoning and/or user-defined datalog rules
///
/// This function handles both reasoning modes:
/// - OWL2-RL: Materializes ontology-based inferences (symmetric, transitive, inverse properties, etc.)
/// - Datalog: Executes user-defined rules stored with `f:rule` predicate
///
/// When both are enabled, derived facts from both sources are combined into a single overlay.
pub async fn compute_derived_facts(
    snapshot: &LedgerSnapshot,
    g_id: GraphId,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    reasoning: &ReasoningModes,
) -> Option<Arc<DerivedFactsOverlay>> {
    use crate::datalog_rules::execute_datalog_rules_with_query_rules;

    let mut all_flakes: Vec<fluree_db_core::Flake> = Vec::new();
    let mut same_as = FrozenSameAs::empty();

    // OWL2-RL materialization
    if reasoning.owl2rl {
        tracing::debug!("computing OWL2-RL derived facts");
        let reasoning_opts = ReasoningOptions::default();
        let cache = global_reasoning_cache();
        let db = GraphDbRef::new(snapshot, g_id, overlay, to_t);
        match reason_owl2rl(db, &reasoning_opts, cache).await {
            Ok(result) => {
                tracing::debug!(
                    derived_facts = result.diagnostics.facts_derived,
                    "OWL2-RL reasoning completed"
                );
                // Collect flakes from the OWL2-RL overlay
                result.overlay.for_each_overlay_flake(
                    0, // derived facts are default-graph only
                    fluree_db_core::IndexType::Spot,
                    None,
                    None,
                    true,
                    i64::MAX,
                    &mut |flake| {
                        all_flakes.push(flake.clone());
                    },
                );
                // Preserve sameAs from OWL2-RL
                same_as = result.overlay.same_as().clone();
            }
            Err(e) => {
                tracing::warn!(error = %e, "OWL2-RL reasoning failed, continuing without OWL derived facts");
            }
        }
    }

    // User-defined datalog rules (from database and/or query-time)
    if reasoning.datalog {
        tracing::debug!(
            query_time_rules = reasoning.rules.len(),
            "executing user-defined datalog rules"
        );
        const MAX_DATALOG_ITERATIONS: usize = 100;

        // If OWL2-RL produced derived facts, build a combined overlay so datalog rules
        // can chain off OWL entailments. Otherwise use the base overlay.
        let datalog_result = if !all_flakes.is_empty() {
            // Build a temporary overlay combining base + OWL2-RL facts
            let mut builder = DerivedFactsBuilder::new();
            for flake in &all_flakes {
                builder.push(flake.clone());
            }
            let temp_overlay = Arc::new(builder.build(same_as.clone(), overlay.epoch()));
            let combined = ReasoningOverlay::new(overlay, temp_overlay);
            let combined_db = GraphDbRef::new(snapshot, g_id, &combined, to_t);
            execute_datalog_rules_with_query_rules(
                combined_db,
                MAX_DATALOG_ITERATIONS,
                &reasoning.rules,
            )
            .await
        } else {
            let base_db = GraphDbRef::new(snapshot, g_id, overlay, to_t);
            execute_datalog_rules_with_query_rules(
                base_db,
                MAX_DATALOG_ITERATIONS,
                &reasoning.rules,
            )
            .await
        };

        match datalog_result {
            Ok(datalog_result) => {
                tracing::debug!(
                    datalog_facts = datalog_result.derived_flakes.len(),
                    "datalog rules completed"
                );
                all_flakes.extend(datalog_result.derived_flakes);
            }
            Err(e) => {
                tracing::warn!(error = %e, "Datalog rule execution failed, continuing without datalog derived facts");
            }
        }
    }

    // If we computed any derived facts, wrap them in a DerivedFactsOverlay
    if all_flakes.is_empty() {
        return None;
    }

    let mut builder = DerivedFactsBuilder::new();
    for flake in all_flakes {
        builder.push(flake);
    }

    let derived_overlay = builder.build(same_as, overlay.epoch());
    Some(Arc::new(derived_overlay))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_effective_reasoning_modes_with_hierarchy() {
        let modes = ReasoningModes::default();
        let effective = effective_reasoning_modes(&modes, true);
        // With hierarchy available and no explicit modes, should enable RDFS
        assert!(effective.rdfs);
    }

    #[test]
    fn test_effective_reasoning_modes_without_hierarchy() {
        let modes = ReasoningModes::default();
        let effective = effective_reasoning_modes(&modes, false);
        // Without hierarchy, should not enable auto-RDFS
        assert!(!effective.rdfs);
    }
}
