//! Fixpoint execution: rule bodies run through the planned query executor
//! over the base data plus the facts derived so far; heads are instantiated
//! from the result rows.

use super::closure::Closure;
use super::delta::{DeltaAtom, Reevaluation};
use super::{
    extract_datalog_rules, parse_query_time_rule, DatalogExecutionResult, DatalogRule,
    DatalogRuleSet, HeadTerm, RuleComponent, RuleDependencies,
};
use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::execute::{execute_prepared, prepare_execution_with_config, PrepareConfig};
use crate::ir::reasoning::ReasoningConfig;
use crate::ir::Query;
use crate::parse::IriEncoder;
use crate::reasoning::ReasoningOverlay;
use crate::{ContextConfig, ExecutableQuery};
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::clock::Instant;
use fluree_db_core::dict_novelty::DictNovelty;
use fluree_db_core::edge::id_datatype_sid;
use fluree_db_core::flake::FlakeMeta;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{
    Flake, GraphDbRef, GraphId, IndexType, LedgerSnapshot, RangeMatch, RangeTest, Sid,
};
use fluree_db_reasoner::{
    approx_flake_bytes, DerivedFactsBuilder, DerivedFactsOverlay, FrozenSameAs, ReasoningBudget,
    ReasoningDiagnostics,
};
use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

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
        rule_set.add_rule(parse_query_time_rule(rule_json, db.snapshot, idx)?)?;
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
    } else if facts >= budget.max_facts {
        Some("facts")
    } else if bytes > budget.max_memory_bytes {
        Some("memory")
    } else {
        None
    }
}

/// How the fixpoint orders rule evaluation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RuleSchedule {
    /// Rules run in dependency order. A rule that cannot observe its own
    /// conclusions runs once, after every rule it reads from has finished; a
    /// recursive group iterates on its own until it stops deriving, and after
    /// its first pass re-reads only what its previous pass derived where the
    /// rule body allows it (see the `delta` module). A recursive group of one
    /// transitive-closure rule runs once, as a property path (see the
    /// `closure` module).
    #[default]
    Dependencies,
    /// Every rule runs its whole body every round until a round derives
    /// nothing. Kept as the reference the dependency schedule is tested
    /// against.
    WholeProgram,
}

/// Run the rules to a fixpoint in dependency order.
pub async fn run_fixpoint(
    db: GraphDbRef<'_>,
    rule_set: &DatalogRuleSet,
    max_iterations: usize,
    budget: &ReasoningBudget,
    env: RuleExecutionEnv<'_>,
) -> Result<DatalogExecutionResult> {
    run_fixpoint_with_schedule(
        db,
        rule_set,
        max_iterations,
        budget,
        env,
        RuleSchedule::Dependencies,
    )
    .await
}

/// Run the rules to a fixpoint under `schedule`.
///
/// Work proceeds in stages; every group in a stage reads only facts derived
/// by earlier stages or by itself. Each pass over a stage evaluates its
/// still-active groups against one overlay of everything derived so far. A
/// non-recursive group is done after one pass; a recursive group stays
/// active until a pass derives nothing new for it. After its first pass, a
/// recursive group's rules are seeded with the facts the group derived in
/// its previous pass wherever their bodies allow it; a one-rule transitive
/// closure finishes in its first pass. `max_iterations` bounds
/// the passes of each stage. The fact and memory budgets are checked before
/// every new fact is kept, so a pass cannot overshoot them. The deadline is
/// checked before each rule, after its body runs, and periodically while its
/// heads are instantiated; a body already running is not interrupted. A rule
/// that finishes deriving has derived everything it can, so a run whose last
/// rule completes after the deadline still reports a completed fixpoint.
pub async fn run_fixpoint_with_schedule(
    db: GraphDbRef<'_>,
    rule_set: &DatalogRuleSet,
    max_iterations: usize,
    budget: &ReasoningBudget,
    env: RuleExecutionEnv<'_>,
    schedule: RuleSchedule,
) -> Result<DatalogExecutionResult> {
    if rule_set.is_empty() {
        return Ok(DatalogExecutionResult {
            derived_flakes: Vec::new(),
            diagnostics: ReasoningDiagnostics::completed(0, 0, std::time::Duration::ZERO),
            rule_body_runs: 0,
            body_rows: 0,
        });
    }

    let dependencies = rule_set.dependencies();
    let whole_program = RuleComponent {
        rules: (0..rule_set.len()).collect(),
        recursive: true,
    };
    let stages: Vec<Vec<&RuleComponent>> = match schedule {
        RuleSchedule::Dependencies => dependencies.stages(),
        RuleSchedule::WholeProgram => vec![vec![&whole_program]],
    };
    tracing::debug!(
        rule_count = rule_set.len(),
        ?schedule,
        groups = %dependencies.describe(rule_set),
        "executing datalog rules; groups producers first, * marks a recursive group"
    );
    let (reevaluations, closures) = match schedule {
        RuleSchedule::Dependencies => {
            warn_on_volatile_recursion(rule_set, &dependencies);
            (
                plan_reevaluations(rule_set, &dependencies, db.snapshot),
                plan_closures(rule_set, &dependencies),
            )
        }
        RuleSchedule::WholeProgram => (
            vec![Reevaluation::Full; rule_set.len()],
            vec![None; rule_set.len()],
        ),
    };

    // Derived facts carry the query's `t` so the overlay's `t <= to_t` filter
    // keeps them visible (same convention as OWL2-RL).
    let mut state = Derivation {
        snapshot: db.snapshot,
        derived_t: db.t,
        budget,
        start: Instant::now(),
        derived: Vec::new(),
        seen: HashSet::new(),
        bytes: 0,
        diagnostics: ReasoningDiagnostics::default(),
        warned_barren: HashSet::new(),
        rule_body_runs: 0,
        body_rows: 0,
    };
    let dict_novelty = ExecutionContext::extract_dict_novelty(db.snapshot);
    let mut derived_overlay: Arc<DerivedFactsOverlay> = Arc::new(DerivedFactsOverlay::empty());
    let mut passes = 0usize;
    let mut capped: Option<&'static str> = None;

    'stages: for (stage_index, stage) in stages.iter().enumerate() {
        let is_last_stage = stage_index + 1 == stages.len();
        let mut active: Vec<ActiveGroup> = stage
            .iter()
            .map(|&group| ActiveGroup { group, delta: None })
            .collect();
        let mut stage_passes = 0usize;

        while !active.is_empty() {
            if let Some(reason) = state.over_budget() {
                capped = Some(reason);
                break 'stages;
            }
            if stage_passes >= max_iterations {
                // The previous pass still derived something, so the closure
                // may be incomplete: surface it like any other budget cap
                // instead of returning as if the fixpoint had converged
                // (#1559).
                tracing::warn!(
                    stage_passes,
                    derived = state.derived.len(),
                    "datalog fixpoint stopped at the iteration cap before a recursive \
                     rule group converged; results may be missing derived facts"
                );
                capped = Some("iterations");
                break 'stages;
            }
            stage_passes += 1;
            passes += 1;

            // Base data plus everything derived so far.
            let effective = ReasoningOverlay::new(db.overlay, derived_overlay.clone());
            let mut pass_db = GraphDbRef::new(db.snapshot, db.g_id, &effective, db.t);
            pass_db.runtime_small_dicts = db.runtime_small_dicts;
            // Derived facts live in the overlay as decoded `Sid`s; eager
            // materialization keeps base rows comparable with them on join
            // keys and hands head instantiation decoded bindings.
            pass_db.eager = true;

            let mut new_this_pass = 0usize;
            let mut still_deriving: Vec<ActiveGroup> = Vec::new();
            for ActiveGroup { group, delta } in active {
                let first_new = state.derived.len();
                let mut closed = false;
                for &index in &group.rules {
                    let rule = &rule_set.rules[index];
                    if let (None, Some(closure)) = (&delta, &closures[index]) {
                        if let Some(reason) = state.over_budget() {
                            capped = Some(reason);
                            break 'stages;
                        }
                        match state
                            .apply_closure(pass_db, rule, closure, env, dict_novelty.clone())
                            .await?
                        {
                            Some(RuleOutcome::Finished) => {
                                closed = true;
                                continue;
                            }
                            Some(RuleOutcome::Capped(reason)) => {
                                capped = Some(reason);
                                break 'stages;
                            }
                            None => {}
                        }
                    }
                    // A group's first pass runs every whole body; nothing it
                    // reads has been seen before.
                    let seeds: Vec<Option<(&DeltaAtom, Range<usize>)>> =
                        match (&delta, &reevaluations[index]) {
                            (Some(delta), Reevaluation::Delta(atoms)) => atoms
                                .iter()
                                .map(|atom| Some((atom, delta.clone())))
                                .collect(),
                            _ => vec![None],
                        };
                    for seed in seeds {
                        if let Some(reason) = state.over_budget() {
                            capped = Some(reason);
                            break 'stages;
                        }
                        let body = match seed {
                            None => rule.body.query.clone(),
                            Some((atom, delta)) => {
                                match atom.seeded(&rule.body.query, &state.derived[delta]) {
                                    Some(body) => body,
                                    None => continue,
                                }
                            }
                        };
                        if let RuleOutcome::Capped(reason) = state
                            .apply(pass_db, rule, body, env, dict_novelty.clone())
                            .await?
                        {
                            capped = Some(reason);
                            break 'stages;
                        }
                    }
                }
                let derived = first_new..state.derived.len();
                new_this_pass += derived.len();
                if group.recursive && !closed && !derived.is_empty() {
                    still_deriving.push(ActiveGroup {
                        group,
                        delta: Some(derived),
                    });
                }
            }

            tracing::debug!(
                stage = stage_index,
                pass = stage_passes,
                new_facts = new_this_pass,
                total_derived = state.derived.len(),
                "datalog fixpoint pass"
            );

            let more_passes_follow = !still_deriving.is_empty() || !is_last_stage;
            if new_this_pass > 0 && more_passes_follow {
                let mut builder = DerivedFactsBuilder::with_capacity(state.derived.len());
                builder.extend(state.derived.iter().cloned());
                derived_overlay =
                    Arc::new(builder.build(FrozenSameAs::empty(), db.overlay.epoch()));
            }
            active = still_deriving;
        }
    }

    let facts = state.derived.len();
    let elapsed = state.start.elapsed();
    let mut diagnostics = state.diagnostics;
    match capped {
        Some(reason) => diagnostics.mark_capped(reason, passes, facts, elapsed),
        None => diagnostics.mark_completed(passes, facts, elapsed),
    }
    tracing::debug!(
        passes,
        rule_body_runs = state.rule_body_runs,
        derived = facts,
        capped = capped.unwrap_or("no"),
        "datalog fixpoint finished"
    );

    Ok(DatalogExecutionResult {
        derived_flakes: state.derived,
        diagnostics,
        rule_body_runs: state.rule_body_runs,
        body_rows: state.body_rows,
    })
}

/// A group still iterating in the current stage.
struct ActiveGroup<'g> {
    group: &'g RuleComponent,
    /// What the group derived in its previous pass, as a range of
    /// [`Derivation::derived`]; `None` before its first pass.
    delta: Option<Range<usize>>,
}

/// How each rule re-runs after its group's first pass. Only a recursive
/// group has later passes, and only its own facts change while it iterates:
/// a group in the same stage never writes what it reads.
fn plan_reevaluations(
    rule_set: &DatalogRuleSet,
    dependencies: &RuleDependencies,
    snapshot: &LedgerSnapshot,
) -> Vec<Reevaluation> {
    let mut plans = vec![Reevaluation::Full; rule_set.len()];
    for group in dependencies.components().iter().filter(|g| g.recursive) {
        let writes: Vec<_> = group
            .rules
            .iter()
            .flat_map(|&i| dependencies.footprint(i).writes.iter().cloned())
            .collect();
        for &index in &group.rules {
            plans[index] = Reevaluation::plan(&rule_set.rules[index].body.query, &writes, snapshot);
            if matches!(plans[index], Reevaluation::Full) {
                tracing::debug!(
                    rule = %rule_set.rules[index].name,
                    "recursive datalog rule reads its group's facts outside a top-level \
                     triple pattern; it re-runs its whole body every pass"
                );
            }
        }
    }
    plans
}

/// The closure form of each rule that is alone in a recursive group and
/// computes a transitive closure (see the `closure` module).
fn plan_closures(
    rule_set: &DatalogRuleSet,
    dependencies: &RuleDependencies,
) -> Vec<Option<Closure>> {
    let mut closures = vec![None; rule_set.len()];
    for group in dependencies.components().iter().filter(|g| g.recursive) {
        let &[index] = group.rules.as_slice() else {
            continue;
        };
        closures[index] = Closure::compile(&rule_set.rules[index]);
        if closures[index].is_some() {
            tracing::debug!(
                rule = %rule_set.rules[index].name,
                "datalog rule computes a transitive closure; evaluating it as one property path"
            );
        }
    }
    closures
}

/// Whether a fact with predicate `p` visible in `db` has a literal object.
async fn has_literal_object(db: GraphDbRef<'_>, p: &Sid) -> Result<bool> {
    let facts = db
        .range(
            IndexType::Psot,
            RangeTest::Eq,
            RangeMatch {
                p: Some(p.clone()),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| {
            QueryError::Internal(format!("failed to scan a rule's path predicate: {e}"))
        })?;
    Ok(facts
        .iter()
        .any(|f| f.op && !matches!(f.o, FlakeValue::Ref(_))))
}

/// A recursive group re-evaluates its rules every pass, so a rule whose body
/// gives different rows each time (`NOW()`, `STRUUID()`, …) can derive new
/// facts forever and run to the budget.
fn warn_on_volatile_recursion(rule_set: &DatalogRuleSet, dependencies: &RuleDependencies) {
    for group in dependencies.components().iter().filter(|g| g.recursive) {
        for &index in &group.rules {
            if !dependencies.footprint(index).repeatable {
                tracing::warn!(
                    rule = %rule_set.rules[index].name,
                    "datalog rule is part of a recursive group but calls a function whose \
                     result changes between calls, or uses a shortest path; it may keep \
                     deriving new facts until the reasoning budget stops it"
                );
            }
        }
    }
}

enum RuleOutcome {
    Finished,
    Capped(&'static str),
}

/// Everything a fixpoint run accumulates across passes.
struct Derivation<'a> {
    snapshot: &'a LedgerSnapshot,
    derived_t: i64,
    budget: &'a ReasoningBudget,
    start: Instant,
    derived: Vec<Flake>,
    seen: HashSet<FactKey>,
    bytes: usize,
    diagnostics: ReasoningDiagnostics,
    /// Rules already flagged for "matched, but instantiated nothing" (#1560):
    /// recursive groups re-run their rules every pass, so warn once per rule.
    warned_barren: HashSet<Sid>,
    rule_body_runs: usize,
    body_rows: usize,
}

impl Derivation<'_> {
    fn over_budget(&self) -> Option<&'static str> {
        over_budget(self.budget, &self.start, self.derived.len(), self.bytes)
    }

    fn past_deadline(&self) -> bool {
        self.start.elapsed() > self.budget.max_duration
    }

    /// Derive `rule`'s whole closure in one run of its path body. `None` when
    /// the path cannot stand in for the recursion here — the path's predicate
    /// has a literal object, or the traversal hit the path operator's
    /// visited-node limit — and the group should iterate instead.
    async fn apply_closure(
        &mut self,
        db: GraphDbRef<'_>,
        rule: &DatalogRule,
        closure: &Closure,
        env: RuleExecutionEnv<'_>,
        dict_novelty: Option<Arc<DictNovelty>>,
    ) -> Result<Option<RuleOutcome>> {
        if let Some(p) = &closure.requires_node_objects {
            if has_literal_object(db, p).await? {
                tracing::debug!(
                    rule = %rule.name,
                    "datalog closure rule iterates instead: its path predicate has literal objects"
                );
                return Ok(None);
            }
        }
        match self
            .apply(db, rule, closure.body.clone(), env, dict_novelty)
            .await
        {
            Ok(outcome) => Ok(Some(outcome)),
            Err(QueryError::ResourceLimit(reason)) => {
                tracing::debug!(
                    rule = %rule.name,
                    %reason,
                    "datalog closure rule iterates instead: its path hit a traversal limit"
                );
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    /// Evaluate `body` (the rule's own body or a seeded copy of it) over
    /// `db` and keep the facts the rule's heads derive that are new.
    async fn apply(
        &mut self,
        db: GraphDbRef<'_>,
        rule: &DatalogRule,
        body: Query,
        env: RuleExecutionEnv<'_>,
        dict_novelty: Option<Arc<DictNovelty>>,
    ) -> Result<RuleOutcome> {
        // Rows between deadline checks while instantiating heads.
        const DEADLINE_CHECK_ROWS: usize = 1024;

        self.rule_body_runs += 1;
        let batches = run_body(db, rule, body, env, dict_novelty).await?;
        self.body_rows += batches.iter().map(Batch::len).sum::<usize>();
        // The body cannot be interrupted, but a rule that ran past the
        // deadline must not go on to derive, and must not let a final pass
        // report a completed fixpoint.
        if self.past_deadline() {
            return Ok(RuleOutcome::Capped("time"));
        }

        let mut matched_rows = 0usize;
        let mut instantiated = 0usize;
        for batch in &batches {
            for row in 0..batch.len() {
                matched_rows += 1;
                if matched_rows.is_multiple_of(DEADLINE_CHECK_ROWS) && self.past_deadline() {
                    return Ok(RuleOutcome::Capped("time"));
                }
                for head in &rule.heads {
                    let Some(flake) = instantiate(head, batch, row, self.snapshot, self.derived_t)
                    else {
                        continue;
                    };
                    instantiated += 1;
                    if !self.seen.insert(fact_key(&flake)) {
                        continue;
                    }
                    // Check before pushing. Pushing first and testing
                    // `len() > max_facts` let one fact past the ceiling
                    // through, and nothing trimmed it afterwards, so a budget
                    // of N returned N+1 facts. Checking first also stops
                    // charging `bytes` for a fact never kept.
                    if self.derived.len() >= self.budget.max_facts {
                        return Ok(RuleOutcome::Capped("facts"));
                    }
                    self.bytes += approx_flake_bytes(&flake);
                    self.diagnostics.record_rule_fired(&rule.name);
                    self.derived.push(flake);
                    if self.bytes > self.budget.max_memory_bytes {
                        return Ok(RuleOutcome::Capped("memory"));
                    }
                }
            }
        }

        // "The where clause matched but the insert could not instantiate" is
        // almost always an authoring bug and is invisible otherwise: the rule
        // appears to run and the fixpoint completes normally (#1560). Catches
        // what parse-time range restriction cannot see — a head variable
        // bound to a literal in subject or predicate position, or bound in no
        // row of a UNION branch.
        if matched_rows > 0 && instantiated == 0 && self.warned_barren.insert(rule.id.clone()) {
            tracing::warn!(
                rule = %rule.name,
                matched_rows,
                "datalog rule matched binding rows but derived no facts: every row \
                 failed to instantiate its insert pattern — a head variable is bound \
                 to a literal in subject/predicate position, or is unbound in every row"
            );
        }
        Ok(RuleOutcome::Finished)
    }
}

/// Run one rule body through the executor and collect its rows.
async fn run_body(
    db: GraphDbRef<'_>,
    rule: &DatalogRule,
    body: Query,
    env: RuleExecutionEnv<'_>,
    dict_novelty: Option<Arc<DictNovelty>>,
) -> Result<Vec<Batch>> {
    // Reasoning is off for the body itself: derived facts arrive through the
    // overlay, and nested materialization would recurse.
    let executable = ExecutableQuery::new(body, ReasoningConfig::default());
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
