//! Fixpoint execution: rule bodies run through the planned query executor
//! over the base data plus the facts derived so far; heads are instantiated
//! from the result rows.

use super::{
    extract_datalog_rules, parse_query_time_rule, DatalogExecutionResult, DatalogRule,
    DatalogRuleSet, HeadTerm,
};
use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::execute::{execute_prepared, prepare_execution_with_config, PrepareConfig};
use crate::ir::reasoning::ReasoningConfig;
use crate::parse::IriEncoder;
use crate::reasoning::ReasoningOverlay;
use crate::{ContextConfig, ExecutableQuery};
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::dict_novelty::DictNovelty;
use fluree_db_core::edge::id_datatype_sid;
use fluree_db_core::flake::FlakeMeta;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{Flake, GraphDbRef, GraphId, LedgerSnapshot, Sid};
use fluree_db_reasoner::{
    approx_flake_bytes, DerivedFactsBuilder, DerivedFactsOverlay, FrozenSameAs, ReasoningBudget,
    ReasoningDiagnostics,
};
use std::collections::HashSet;
use std::sync::Arc;
use fluree_db_core::clock::Instant;

/// Execution resources rule bodies should use — the same binary index store
/// the enclosing query plans against, so rule bodies take the indexed lanes.
#[derive(Clone, Copy, Default)]
pub struct RuleExecutionEnv<'a> {
    pub binary_store: Option<&'a Arc<BinaryIndexStore>>,
}

/// Execute the stored rules of a graph with an unlimited budget.
pub async fn execute_datalog_rules(
    db: GraphDbRef<'_>,
    max_iterations: usize,
) -> Result<DatalogExecutionResult> {
    execute_datalog_rules_with_query_rules(
        db,
        max_iterations,
        &[],
        None,
        &ReasoningBudget::unlimited(),
        RuleExecutionEnv::default(),
    )
    .await
}

/// Load the stored rules (from `rules_source_g_id`, or the query graph) and
/// the query-time rules into one ordered rule set.
pub async fn load_rule_set(
    db: GraphDbRef<'_>,
    query_time_rules: &[serde_json::Value],
    rules_source_g_id: Option<GraphId>,
) -> Result<DatalogRuleSet> {
    let rules_db = match rules_source_g_id {
        Some(rg) if rg != db.g_id => GraphDbRef::new(db.snapshot, rg, db.overlay, db.t),
        _ => db,
    };
    let mut rule_set = extract_datalog_rules(rules_db).await?;
    for (idx, rule_json) in query_time_rules.iter().enumerate() {
        rule_set.add_rule(parse_query_time_rule(rule_json, db.snapshot, idx)?);
    }
    Ok(rule_set)
}

/// Execute stored plus query-time rules to a fixpoint under `budget`.
///
/// `rules_source_g_id` overrides the graph scanned for `f:rule` values; the
/// fixpoint itself always runs against `db`. A rule that fails to parse or
/// validate is an error (see the module docs).
pub async fn execute_datalog_rules_with_query_rules(
    db: GraphDbRef<'_>,
    max_iterations: usize,
    query_time_rules: &[serde_json::Value],
    rules_source_g_id: Option<GraphId>,
    budget: &ReasoningBudget,
    env: RuleExecutionEnv<'_>,
) -> Result<DatalogExecutionResult> {
    let rule_set = load_rule_set(db, query_time_rules, rules_source_g_id).await?;
    run_fixpoint(db, &rule_set, max_iterations, budget, env).await
}

/// Dedup key: RDF term identity plus datatype and metadata (language tag,
/// list index), so `"chat"@fr` and `"chat"@en` stay distinct facts.
type FactKey = (Sid, Sid, FlakeValue, Sid, Option<FlakeMeta>);

fn fact_key(flake: &Flake) -> FactKey {
    (
        flake.s.clone(),
        flake.p.clone(),
        flake.o.clone(),
        flake.dt.clone(),
        flake.m.clone(),
    )
}

fn over_budget(
    budget: &ReasoningBudget,
    start: &Instant,
    facts: usize,
    bytes: usize,
) -> Option<&'static str> {
    if start.elapsed() > budget.max_duration {
        Some("time")
    } else if facts > budget.max_facts {
        Some("facts")
    } else if bytes > budget.max_memory_bytes {
        Some("memory")
    } else {
        None
    }
}

/// Run the rules to a fixpoint.
///
/// Each round runs every rule body over the base data plus everything
/// derived in earlier rounds, instantiates the heads, and stops when a round
/// derives nothing new. The budget is checked between rounds, between rules,
/// and after every new fact, so a single round cannot overshoot it.
pub async fn run_fixpoint(
    db: GraphDbRef<'_>,
    rule_set: &DatalogRuleSet,
    max_iterations: usize,
    budget: &ReasoningBudget,
    env: RuleExecutionEnv<'_>,
) -> Result<DatalogExecutionResult> {
    if rule_set.is_empty() {
        return Ok(DatalogExecutionResult {
            derived_flakes: Vec::new(),
            diagnostics: ReasoningDiagnostics::completed(0, 0, std::time::Duration::ZERO),
        });
    }

    tracing::debug!(rule_count = rule_set.len(), "executing datalog rules");

    let start = Instant::now();
    // Derived facts carry the query's `t` so the overlay's `t <= to_t` filter
    // keeps them visible (same convention as OWL2-RL).
    let derived_t = db.t;
    let dict_novelty = ExecutionContext::extract_dict_novelty(db.snapshot);

    let mut derived: Vec<Flake> = Vec::new();
    let mut seen: HashSet<FactKey> = HashSet::new();
    let mut bytes: usize = 0;
    let mut derived_overlay: Arc<DerivedFactsOverlay> = Arc::new(DerivedFactsOverlay::empty());
    let mut iterations = 0usize;
    let mut capped: Option<&'static str> = None;
    let mut diagnostics = ReasoningDiagnostics::default();
    // Rules already flagged for "matched, but instantiated nothing" (#1560):
    // the fixpoint re-runs every rule each round, so warn once per rule.
    let mut warned_barren: HashSet<Sid> = HashSet::new();

    'rounds: loop {
        if let Some(reason) = over_budget(budget, &start, derived.len(), bytes) {
            capped = Some(reason);
            break;
        }
        if iterations >= max_iterations {
            // The previous round still derived something, so the closure may
            // be incomplete: surface it like any other budget cap instead of
            // returning as if the fixpoint had converged (#1559).
            tracing::warn!(
                iterations,
                derived = derived.len(),
                "datalog fixpoint stopped at the iteration cap before converging; \
                 results may be missing derived facts"
            );
            capped = Some("iterations");
            break;
        }
        iterations += 1;
        let mut new_this_round = 0usize;

        // Base data plus everything derived so far.
        let effective = ReasoningOverlay::new(db.overlay, derived_overlay.clone());
        let mut iter_db = GraphDbRef::new(db.snapshot, db.g_id, &effective, db.t);
        iter_db.runtime_small_dicts = db.runtime_small_dicts;
        // Derived facts live in the overlay as decoded `Sid`s; eager
        // materialization keeps base rows comparable with them on join keys
        // and hands head instantiation decoded bindings.
        iter_db.eager = true;

        for rule in rule_set.iter_in_order() {
            if let Some(reason) = over_budget(budget, &start, derived.len(), bytes) {
                capped = Some(reason);
                break 'rounds;
            }
            let batches = run_body(iter_db, rule, env, dict_novelty.clone()).await?;

            let mut matched_rows = 0usize;
            let mut instantiated = 0usize;
            for batch in &batches {
                for row in 0..batch.len() {
                    matched_rows += 1;
                    for head in &rule.heads {
                        let Some(flake) = instantiate(head, batch, row, db.snapshot, derived_t)
                        else {
                            continue;
                        };
                        instantiated += 1;
                        if !seen.insert(fact_key(&flake)) {
                            continue;
                        }
                        bytes += approx_flake_bytes(&flake);
                        diagnostics.record_rule_fired(&rule.name);
                        derived.push(flake);
                        new_this_round += 1;
                        if derived.len() > budget.max_facts {
                            capped = Some("facts");
                            break 'rounds;
                        }
                        if bytes > budget.max_memory_bytes {
                            capped = Some("memory");
                            break 'rounds;
                        }
                    }
                }
            }

            // "The where clause matched but the insert could not instantiate"
            // is almost always an authoring bug and is invisible otherwise:
            // the rule appears to run and the fixpoint completes normally
            // (#1560). Catches what parse-time range restriction cannot see —
            // a head variable bound to a literal in subject or predicate
            // position, or bound in no row of a UNION branch.
            if matched_rows > 0 && instantiated == 0 && warned_barren.insert(rule.id.clone()) {
                tracing::warn!(
                    rule = %rule.name,
                    matched_rows,
                    "datalog rule matched binding rows but derived no facts: every row \
                     failed to instantiate its insert pattern — a head variable is bound \
                     to a literal in subject/predicate position, or is unbound in every row"
                );
            }
        }

        tracing::debug!(
            iteration = iterations,
            new_facts = new_this_round,
            total_derived = derived.len(),
            "datalog fixpoint iteration"
        );

        if new_this_round == 0 {
            break;
        }

        // Rebuild the derived overlay for the next round.
        let mut builder = DerivedFactsBuilder::with_capacity(derived.len());
        builder.extend(derived.iter().cloned());
        derived_overlay = Arc::new(builder.build(FrozenSameAs::empty(), db.overlay.epoch()));
    }

    let facts = derived.len();
    let elapsed = start.elapsed();
    match capped {
        Some(reason) => diagnostics.mark_capped(reason, iterations, facts, elapsed),
        None => diagnostics.mark_completed(iterations, facts, elapsed),
    }

    Ok(DatalogExecutionResult {
        derived_flakes: derived,
        diagnostics,
    })
}

/// Run one rule body through the executor and collect its rows.
async fn run_body(
    db: GraphDbRef<'_>,
    rule: &DatalogRule,
    env: RuleExecutionEnv<'_>,
    dict_novelty: Option<Arc<DictNovelty>>,
) -> Result<Vec<Batch>> {
    // Reasoning is off for the body itself: derived facts arrive through the
    // overlay, and nested materialization would recurse.
    let executable = ExecutableQuery::new(rule.body.query.clone(), ReasoningConfig::default());
    // Boxed: the executor's prepare step is where reasoning prep lives, so
    // the future type would otherwise recurse (prepare → derived facts →
    // fixpoint → body → prepare). Rule bodies run with reasoning off, so the
    // recursion never happens at run time; the box only breaks the type cycle.
    let prepared = Box::pin(prepare_execution_with_config(
        db,
        &executable,
        &PrepareConfig::current(env.binary_store),
    ))
    .await?;
    let config = ContextConfig {
        binary_store: env.binary_store.cloned(),
        binary_g_id: db.g_id,
        dict_novelty,
        ..Default::default()
    };
    execute_prepared(db, &rule.body.vars, prepared, config).await
}

/// Instantiate one head against one result row. `None` when a position
/// cannot be filled (an unbound variable, or a literal where a node is
/// required); the caller counts those.
fn instantiate(
    head: &super::RuleHead,
    batch: &Batch,
    row: usize,
    snapshot: &LedgerSnapshot,
    t: i64,
) -> Option<Flake> {
    let s = resolve_node(&head.subject, batch, row, snapshot)?;
    let p = resolve_node(&head.predicate, batch, row, snapshot)?;
    let (o, dt, m) = resolve_object(&head.object, batch, row, snapshot)?;
    Some(Flake::new(s, p, o, dt, t, true, m))
}

fn resolve_node(
    term: &HeadTerm,
    batch: &Batch,
    row: usize,
    snapshot: &LedgerSnapshot,
) -> Option<Sid> {
    match term {
        HeadTerm::Sid(sid) => Some(sid.clone()),
        HeadTerm::Var(v) => binding_node(batch.get(row, *v)?, snapshot),
        HeadTerm::Literal { .. } => None,
    }
}

fn binding_node(binding: &Binding, snapshot: &LedgerSnapshot) -> Option<Sid> {
    match binding {
        Binding::Sid { sid, .. } => Some(sid.clone()),
        Binding::IriMatch { primary_sid, .. } => Some(primary_sid.clone()),
        Binding::Iri(iri) => IriEncoder::encode_iri(snapshot, iri),
        _ => None,
    }
}

fn resolve_object(
    term: &HeadTerm,
    batch: &Batch,
    row: usize,
    snapshot: &LedgerSnapshot,
) -> Option<(FlakeValue, Sid, Option<FlakeMeta>)> {
    match term {
        HeadTerm::Sid(sid) => Some((FlakeValue::Ref(sid.clone()), id_datatype_sid(), None)),
        HeadTerm::Literal {
            value,
            datatype,
            lang,
        } => Some((
            value.clone(),
            datatype.clone(),
            FlakeMeta::from_parts(lang.as_deref(), None),
        )),
        HeadTerm::Var(v) => match batch.get(row, *v)? {
            Binding::Lit { val, dtc, .. } => Some((
                val.clone(),
                dtc.datatype().clone(),
                FlakeMeta::from_parts(dtc.lang_tag(), None),
            )),
            other => binding_node(other, snapshot)
                .map(|sid| (FlakeValue::Ref(sid), id_datatype_sid(), None)),
        },
    }
}
