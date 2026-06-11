//! Reasoning preparation helpers
//!
//! Handles schema hierarchy computation, effective reasoning mode determination,
//! and derived facts computation for OWL2-RL and datalog rules.

use crate::ir::ReasoningModes;
use crate::reasoning::{global_reasoning_cache, reason_owl2rl, ReasoningOverlay};
use crate::Result;
use fluree_db_core::{
    overlay::OverlayProvider, GraphDbRef, GraphId, IndexSchema, LedgerSnapshot, SchemaHierarchy,
    SchemaPredicateInfo,
};
use fluree_db_reasoner::{
    DerivedFactsBuilder, DerivedFactsOverlay, FrozenSameAs, ReasoningOptions,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Build schema hierarchy from database and overlay
///
/// Reads `rdfs:subClassOf` and `rdfs:subPropertyOf` assertions from the full
/// snapshot (indexed root *and* committed-but-unindexed novelty) plus the
/// overlay, and merges them with the existing database schema to create a
/// unified hierarchy view.
///
/// Reading via the range provider (rather than only `overlay` flakes) is
/// essential: right after `fluree create`/import, the ontology axioms live in
/// committed-but-not-yet-background-indexed data. Those flakes are invisible to
/// both an overlay scan and `snapshot.schema` (the last indexed root), so an
/// overlay-only scan returned an empty hierarchy and silently disabled RDFS
/// query rewriting until background indexing happened to run. OWL2-QL already
/// read its axioms through the range provider (`Ontology::from_db`); this brings
/// RDFS in line so subclass/subproperty expansion works on a freshly imported
/// ledger.
pub async fn schema_hierarchy_with_overlay(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
) -> Result<Option<SchemaHierarchy>> {
    use fluree_db_core::value::FlakeValue;
    use fluree_db_core::{IndexType, RangeMatch, RangeTest, Sid};
    use fluree_vocab::namespaces::RDFS;

    // Build child -> parents from rdfs:subClassOf assertions.
    let mut subclass_of: HashMap<fluree_db_core::Sid, Vec<fluree_db_core::Sid>> = HashMap::new();
    // Build child -> parents from rdfs:subPropertyOf assertions.
    let mut subproperty_of: HashMap<fluree_db_core::Sid, Vec<fluree_db_core::Sid>> = HashMap::new();

    // Scan the full default-graph state (indexed + unindexed commits + overlay).
    let db = GraphDbRef::new(snapshot, 0, overlay, to_t);

    for flake in db
        .range(
            IndexType::Psot,
            RangeTest::Eq,
            RangeMatch::predicate(Sid::new(RDFS, "subClassOf")),
        )
        .await?
    {
        if flake.op {
            if let FlakeValue::Ref(parent) = flake.o {
                subclass_of.entry(flake.s).or_default().push(parent);
            }
        }
    }

    for flake in db
        .range(
            IndexType::Psot,
            RangeTest::Eq,
            RangeMatch::predicate(Sid::new(RDFS, "subPropertyOf")),
        )
        .await?
    {
        if flake.op {
            if let FlakeValue::Ref(parent) = flake.o {
                subproperty_of.entry(flake.s).or_default().push(parent);
            }
        }
    }

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
        Ok(None)
    } else {
        Ok(Some(SchemaHierarchy::from_db_root_schema(&schema)))
    }
}

/// Build the OWL2-RL materialization budget for this query.
///
/// Layered, lowest to highest precedence:
/// 1. built-in default (1M facts / 30s),
/// 2. server env (`FLUREE_REASONING_MAX_FACTS` / `FLUREE_REASONING_MAX_SECONDS`)
///    — operator-wide override,
/// 3. `modes.max_facts` / `modes.max_seconds` — the merged ledger-config /
///    per-query budget (override control is enforced upstream at the view
///    layer, so by the time it reaches here the value is authoritative).
///
/// Datasets whose closure exceeds the budget get a CAPPED (incomplete)
/// materialization — see the warning in [`compute_derived_facts`].
fn reasoning_budget(modes: &ReasoningModes) -> fluree_db_reasoner::ReasoningBudget {
    let mut budget = fluree_db_reasoner::ReasoningBudget::default();
    if let Some(max_facts) = std::env::var("FLUREE_REASONING_MAX_FACTS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
    {
        budget.max_facts = max_facts;
    }
    if let Some(max_secs) = std::env::var("FLUREE_REASONING_MAX_SECONDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        budget.max_duration = std::time::Duration::from_secs(max_secs);
    }
    if let Some(max_facts) = modes.max_facts {
        budget.max_facts = max_facts as usize;
    }
    if let Some(max_secs) = modes.max_seconds {
        budget.max_duration = std::time::Duration::from_secs(max_secs);
    }
    budget
}

/// Result of [`compute_derived_facts`]: the overlay plus the OWL2-RL
/// materialization diagnostics (when OWL2-RL ran), so callers can surface a
/// capped (incomplete) closure in response metadata instead of only logging.
#[derive(Default)]
pub struct DerivedFactsOutcome {
    /// Combined derived-facts overlay (OWL2-RL and/or datalog), if any.
    pub overlay: Option<Arc<DerivedFactsOverlay>>,
    /// OWL2-RL materialization diagnostics; `None` when OWL2-RL didn't run
    /// (datalog-only reasoning) or failed.
    pub diagnostics: Option<fluree_db_reasoner::ReasoningDiagnostics>,
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
    rules_source_g_id: Option<GraphId>,
) -> DerivedFactsOutcome {
    use crate::datalog_rules::execute_datalog_rules_with_query_rules;

    let mut all_flakes: Vec<fluree_db_core::Flake> = Vec::new();
    let mut same_as = FrozenSameAs::empty();
    let mut diagnostics = None;

    // OWL2-RL materialization
    if reasoning.owl2rl {
        tracing::debug!("computing OWL2-RL derived facts");
        let reasoning_opts = ReasoningOptions {
            budget: reasoning_budget(reasoning),
            ..Default::default()
        };
        let cache = global_reasoning_cache();
        let db = GraphDbRef::new(snapshot, g_id, overlay, to_t);
        match reason_owl2rl(db, &reasoning_opts, cache).await {
            Ok(result) => {
                if result.diagnostics.capped {
                    // A capped materialization is an INCOMPLETE closure:
                    // reasoning queries will silently miss entailments. Make
                    // this loud — it is a correctness event, not a perf detail.
                    tracing::warn!(
                        derived_facts = result.diagnostics.facts_derived,
                        capped_reason = result.diagnostics.capped_reason.as_deref(),
                        iterations = result.diagnostics.iterations,
                        duration_ms = result.diagnostics.duration.as_millis() as u64,
                        "OWL2-RL materialization hit its budget before reaching \
                         fixpoint; query results may be missing entailments. \
                         Raise the budget via f:reasoningMaxFacts/f:reasoningMaxSeconds \
                         (ledger config), \"reasoningBudget\" (query), or \
                         FLUREE_REASONING_MAX_FACTS/FLUREE_REASONING_MAX_SECONDS (server)."
                    );
                } else {
                    tracing::debug!(
                        derived_facts = result.diagnostics.facts_derived,
                        iterations = result.diagnostics.iterations,
                        duration_ms = result.diagnostics.duration.as_millis() as u64,
                        "OWL2-RL reasoning completed"
                    );
                }
                diagnostics = Some(result.diagnostics.clone());

                // Without datalog there is nothing to combine: hand the
                // prebuilt (cached, pre-sorted) overlay straight to
                // execution instead of re-collecting and re-sorting its
                // flakes on every query.
                if !reasoning.datalog {
                    return DerivedFactsOutcome {
                        overlay: (!result.overlay.is_empty()).then(|| result.overlay.clone()),
                        diagnostics,
                    };
                }

                // Datalog chains off OWL entailments — collect flakes so
                // both rule sets land in one combined overlay below.
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
            rules_source_g_id = ?rules_source_g_id,
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
                rules_source_g_id,
            )
            .await
        } else {
            let base_db = GraphDbRef::new(snapshot, g_id, overlay, to_t);
            execute_datalog_rules_with_query_rules(
                base_db,
                MAX_DATALOG_ITERATIONS,
                &reasoning.rules,
                rules_source_g_id,
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
        return DerivedFactsOutcome {
            overlay: None,
            diagnostics,
        };
    }

    let mut builder = DerivedFactsBuilder::new();
    for flake in all_flakes {
        builder.push(flake);
    }

    let derived_overlay = builder.build(same_as, overlay.epoch());
    DerivedFactsOutcome {
        overlay: Some(Arc::new(derived_overlay)),
        diagnostics,
    }
}
