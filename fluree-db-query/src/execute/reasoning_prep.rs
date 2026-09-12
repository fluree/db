//! Reasoning preparation helpers
//!
//! Handles schema hierarchy computation, effective reasoning mode determination,
//! and derived facts computation for OWL2-RL and datalog rules.

use crate::ir::ReasoningModes;
use crate::reasoning::{global_reasoning_cache, reason_owl2rl, ReasoningOverlay};
use crate::Result;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::{
    overlay::OverlayProvider, GraphDbRef, GraphId, IndexSchema, LedgerSnapshot, SchemaHierarchy,
    SchemaPredicateInfo,
};
use fluree_db_reasoner::{
    DerivedFactsBuilder, DerivedFactsOverlay, FrozenSameAs, ReasoningOptions,
};
use fluree_db_reasoner::{ReasoningCacheKey, ReasoningResult};
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
    // The env vars are re-read on every call deliberately: they are a live
    // operator tuning knob (no restart needed), and two getenv calls per
    // reasoning query are negligible next to materialization itself.
    if let Some(max_facts) = budget_env_var::<usize>("FLUREE_REASONING_MAX_FACTS") {
        budget.max_facts = max_facts;
    }
    if let Some(max_secs) = budget_env_var::<u64>("FLUREE_REASONING_MAX_SECONDS") {
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

/// Read and parse a reasoning-budget env var, warning (instead of silently
/// ignoring) when a set value doesn't parse.
fn budget_env_var<T: std::str::FromStr>(name: &str) -> Option<T> {
    let raw = std::env::var(name).ok()?;
    match raw.parse::<T>() {
        Ok(v) => Some(v),
        Err(_) => {
            tracing::warn!(
                name,
                value = %raw,
                "ignoring unparseable reasoning budget env var"
            );
            None
        }
    }
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

/// Compute derived facts from OWL2-RL reasoning and/or user-defined datalog rules.
///
/// - OWL2-RL materializes ontology entailments (cached by `reason_owl2rl`).
/// - Datalog executes the stored `f:rule` rules plus any query-time rules
///   through the query executor (`crate::datalog_rules`).
///
/// When datalog runs, the combined overlay (OWL2-RL plus datalog) is cached
/// under a key that extends the OWL key with the datalog inputs — the rule
/// sources by content, the rules graph, the budget — so a repeated query is
/// served from the LRU exactly like an OWL-only query. Stored rules are
/// hashed by content rather than trusted to the ledger epoch because a
/// cross-ledger `f:rulesSource` can change without this ledger moving.
///
/// A rule that fails to parse or validate is an error: reasoning over an
/// incomplete rule set answers silently wrong, which is worse than a query
/// that fails naming the rule.
pub async fn compute_derived_facts(
    snapshot: &LedgerSnapshot,
    g_id: GraphId,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    reasoning: &ReasoningModes,
    rules_source_g_id: Option<GraphId>,
    binary_store: Option<&Arc<BinaryIndexStore>>,
) -> crate::error::Result<DerivedFactsOutcome> {
    use crate::datalog_rules::{load_rule_set, run_fixpoint, RuleExecutionEnv};
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    // One budget for both reasoning mechanisms — datalog reuses it rather than
    // standing up a second cap.
    let budget = reasoning_budget(reasoning);
    let cache = global_reasoning_cache();
    let base_db = GraphDbRef::new(snapshot, g_id, overlay, to_t);

    // Datalog: load the rule set first so its content can key the cache.
    let (rule_set, datalog_key) = if reasoning.datalog {
        let rule_set = load_rule_set(base_db, &reasoning.rules, rules_source_g_id).await?;
        let key = overlay.content_version().map(|overlay_version| {
            let mut h = DefaultHasher::new();
            "datalog-v2".hash(&mut h);
            budget.config_hash().hash(&mut h);
            reasoning.owl2rl.hash(&mut h);
            g_id.hash(&mut h);
            rules_source_g_id.hash(&mut h);
            rule_set.content_hash().hash(&mut h);
            ReasoningCacheKey {
                ledger_id: snapshot.ledger_id.as_str().into(),
                db_epoch: snapshot.t as u64,
                to_t,
                overlay_version,
                ontology_epoch: snapshot.schema_epoch().unwrap_or(0),
                rule_config_hash: h.finish(),
            }
        });
        if let Some(cached) = key.as_ref().and_then(|k| cache.get(k)) {
            tracing::debug!("derived facts served from the reasoning cache");
            return Ok(DerivedFactsOutcome {
                overlay: (!cached.overlay.is_empty()).then(|| cached.overlay.clone()),
                diagnostics: Some(cached.diagnostics.clone()),
            });
        }
        (Some(rule_set), key)
    } else {
        (None, None)
    };

    let mut all_flakes: Vec<fluree_db_core::Flake> = Vec::new();
    let mut same_as = FrozenSameAs::empty();
    let mut diagnostics = None;

    // OWL2-RL materialization
    if reasoning.owl2rl {
        tracing::debug!("computing OWL2-RL derived facts");
        let reasoning_opts = ReasoningOptions {
            budget: budget.clone(),
            ..Default::default()
        };
        match reason_owl2rl(base_db, &reasoning_opts, cache).await {
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
                    return Ok(DerivedFactsOutcome {
                        overlay: (!result.overlay.is_empty()).then(|| result.overlay.clone()),
                        diagnostics,
                    });
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
    if let Some(rule_set) = rule_set.as_ref() {
        tracing::debug!(
            query_time_rules = reasoning.rules.len(),
            rules_source_g_id = ?rules_source_g_id,
            rule_count = rule_set.len(),
            "executing user-defined datalog rules"
        );
        const MAX_DATALOG_ITERATIONS: usize = 100;
        let env = RuleExecutionEnv { binary_store };

        // If OWL2-RL produced derived facts, run the rules over base + OWL
        // entailments so datalog can chain off them.
        let datalog_result = if !all_flakes.is_empty() {
            let mut builder = DerivedFactsBuilder::new();
            for flake in &all_flakes {
                builder.push(flake.clone());
            }
            let temp_overlay = Arc::new(builder.build(same_as.clone(), overlay.epoch()));
            let combined = ReasoningOverlay::new(overlay, temp_overlay);
            let combined_db = GraphDbRef::new(snapshot, g_id, &combined, to_t);
            run_fixpoint(combined_db, rule_set, MAX_DATALOG_ITERATIONS, &budget, env).await?
        } else {
            run_fixpoint(base_db, rule_set, MAX_DATALOG_ITERATIONS, &budget, env).await?
        };

        let dl_diag = datalog_result.diagnostics;
        if dl_diag.capped {
            // A capped fixpoint is an INCOMPLETE derivation: reasoning
            // queries silently miss facts. Loud, like OWL2-RL — a
            // correctness event, not a perf detail.
            tracing::warn!(
                derived_facts = dl_diag.facts_derived,
                capped_reason = dl_diag.capped_reason.as_deref(),
                iterations = dl_diag.iterations,
                duration_ms = dl_diag.duration.as_millis() as u64,
                "datalog rule materialization hit its budget before reaching \
                 fixpoint; query results may be missing derived facts. \
                 Raise the budget via f:reasoningMaxFacts/f:reasoningMaxSeconds \
                 (ledger config), \"reasoningBudget\" (query), or \
                 FLUREE_REASONING_MAX_FACTS/FLUREE_REASONING_MAX_SECONDS (server)."
            );
        } else {
            tracing::debug!(
                datalog_facts = datalog_result.derived_flakes.len(),
                "datalog rules completed"
            );
        }
        // Fold datalog diagnostics into the outcome; if OWL2-RL also ran, OR
        // the capped flags and combine the counts so one tally reflects both.
        diagnostics = Some(match diagnostics.take() {
            None => dl_diag,
            Some(owl) => fluree_db_reasoner::ReasoningDiagnostics {
                iterations: owl.iterations + dl_diag.iterations,
                facts_derived: owl.facts_derived + dl_diag.facts_derived,
                capped: owl.capped || dl_diag.capped,
                capped_reason: owl.capped_reason.or(dl_diag.capped_reason),
                duration: owl.duration + dl_diag.duration,
                rules_fired: {
                    let mut fired = owl.rules_fired;
                    for (rule, n) in dl_diag.rules_fired {
                        *fired.entry(rule).or_insert(0) += n;
                    }
                    fired
                },
            },
        });
        all_flakes.extend(datalog_result.derived_flakes);
    }

    // Build the combined overlay (possibly empty) and, when datalog ran,
    // cache it so the next identical query is served from the LRU.
    let mut builder = DerivedFactsBuilder::with_capacity(all_flakes.len());
    builder.extend(all_flakes);
    let derived_overlay = Arc::new(builder.build(same_as, overlay.epoch()));
    if let Some(key) = datalog_key {
        cache.insert(
            key,
            Arc::new(ReasoningResult {
                overlay: derived_overlay.clone(),
                diagnostics: diagnostics.clone().unwrap_or_default(),
            }),
        );
    }
    Ok(DerivedFactsOutcome {
        overlay: (!derived_overlay.is_empty()).then(|| derived_overlay.clone()),
        diagnostics,
    })
}
