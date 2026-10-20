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
use crate::execute::{
    execute_prepared_streaming, prepare_execution_with_config, BatchSink, PrepareConfig,
    QueryRequest,
};
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
use fluree_db_core::graph_registry::FIRST_USER_GRAPH_ID;
use fluree_db_core::temporal::DateTime;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{
    range_with_overlay, Flake, GraphDbRef, GraphId, IndexType, LedgerSnapshot, RangeMatch,
    RangeOptions, RangeTest, Sid, TXN_META_GRAPH_ID,
};
use fluree_db_reasoner::{
    approx_flake_bytes, DerivedFactsBuilder, DerivedFactsOverlay, FrozenSameAs, ReasoningBudget,
    ReasoningDiagnostics,
};
use fluree_vocab::namespaces::FLUREE_DB;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;

/// Execution resources rule bodies should use — the same binary index store
/// the enclosing query plans against, so rule bodies take the indexed lanes —
/// and the evaluation time requested for the run.
#[derive(Clone, Copy, Default)]
pub struct RuleExecutionEnv<'a> {
    pub binary_store: Option<&'a Arc<BinaryIndexStore>>,
    /// What `NOW()` and `TODAY()` return in rule bodies. `None` uses the
    /// commit time of the snapshot the rules run against.
    pub evaluation_time: Option<&'a DateTime>,
    /// The query the rules run for: rule bodies charge its fuel, and its
    /// cancellation ends the run with an error rather than a capped result.
    pub request: QueryRequest<'a>,
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

/// Dedup key: the graph, RDF term identity, and datatype and metadata
/// (language tag, list index), so `"chat"@fr` and `"chat"@en` stay distinct
/// facts, as do one triple in two graphs.
type FactKey = (Option<Sid>, Sid, Sid, FlakeValue, Sid, Option<FlakeMeta>);

fn fact_key(flake: &Flake) -> FactKey {
    (
        flake.g.clone(),
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
/// the passes of each stage. A rule's heads are instantiated as its body's
/// rows arrive. The fact and memory budgets are checked before every new
/// fact is kept, so a pass cannot overshoot them, and stop the body; work an
/// operator does before returning its first row (a sort, a grouping, a
/// property path with both ends free) is not counted. The deadline is
/// checked before each rule, at the running body's checkpoints, and as its
/// rows arrive. A rule that finishes deriving has derived everything it can,
/// so a run whose last rule completes after the deadline still reports a
/// completed fixpoint. When a rule calls `NOW()` or `TODAY()`, every body
/// evaluates them at one time: `env.evaluation_time`, else the commit time of
/// the snapshot at `db.t`.
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
    if let Some(cycle) = dependencies.unstratified(rule_set, |sid| db.snapshot.decode_sid(sid)) {
        return Err(QueryError::InvalidQuery(cycle));
    }
    if schedule == RuleSchedule::WholeProgram {
        if let Some((rule, construct)) = rule_set.non_monotone_rules().first() {
            return Err(QueryError::InvalidQuery(format!(
                "datalog rule {rule} uses {construct}, which the whole-program schedule \
                 cannot evaluate: it runs every rule from the first round, before what the \
                 rule reads is complete"
            )));
        }
    }
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

    let head_graphs = HeadGraphs::resolve(rule_set, db)?;
    let evaluation_time = if rule_set.reads_clock() {
        Some(resolve_evaluation_time(db, env.evaluation_time).await?)
    } else {
        None
    };
    let start = Instant::now();

    // Derived facts carry the query's `t` so the overlay's `t <= to_t` filter
    // keeps them visible (same convention as OWL2-RL).
    let mut state = Derivation {
        snapshot: db.snapshot,
        derived_t: db.t,
        budget,
        start,
        deadline: start.checked_add(budget.max_duration),
        evaluation_time,
        head_graphs: &head_graphs,
        derived: Vec::new(),
        seen: HashSet::new(),
        bytes: 0,
        diagnostics: ReasoningDiagnostics::default(),
        warned_barren: HashSet::new(),
        rule_body_runs: 0,
        body_rows: 0,
    };
    let dict_novelty = ExecutionContext::extract_dict_novelty(db.snapshot);
    let plan = RunPlan {
        rule_set,
        reevaluations: &reevaluations,
        closures: &closures,
        env,
        dict_novelty,
    };
    let mut derived_overlay: Arc<DerivedFactsOverlay> = Arc::new(DerivedFactsOverlay::empty());
    let mut passes = 0usize;
    let mut capped: Option<&'static str> = None;
    // Where a cap fired: the groups it cut short, and the stage they were in.
    let mut cut_short: Vec<&RuleComponent> = Vec::new();
    let mut capped_stage = 0usize;

    'stages: for (stage_index, stage) in stages.iter().enumerate() {
        let is_last_stage = stage_index + 1 == stages.len();
        let mut active: Vec<ActiveGroup> = stage
            .iter()
            .map(|&group| ActiveGroup { group, delta: None })
            .collect();
        let mut stage_passes = 0usize;

        while !active.is_empty() {
            capped_stage = stage_index;
            if let Some(reason) = state.over_budget() {
                capped = Some(reason);
                cut_short = active.iter().map(|g| g.group).collect();
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
                cut_short = active.iter().map(|g| g.group).collect();
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
            let mut queue = active.into_iter();
            while let Some(ActiveGroup { group, delta }) = queue.next() {
                let first_new = state.derived.len();
                let closed = match state.run_group(pass_db, &plan, group, delta).await? {
                    GroupOutcome::Capped(reason) => {
                        capped = Some(reason);
                        cut_short = still_deriving
                            .iter()
                            .map(|g| g.group)
                            .chain(std::iter::once(group))
                            .chain(queue.map(|g| g.group))
                            .collect();
                        break 'stages;
                    }
                    GroupOutcome::Finished { closed } => closed,
                };
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
                derived_overlay = Arc::new(builder.build_routed(
                    FrozenSameAs::empty(),
                    db.overlay.epoch(),
                    |flake| head_graphs.route(flake, db.g_id),
                ));
            }
            active = still_deriving;
        }
    }

    let facts = state.derived.len();
    let elapsed = state.start.elapsed();
    let mut diagnostics = state.diagnostics;
    diagnostics.evaluation_time = state.evaluation_time.as_ref().map(DateTime::canonical);
    match capped {
        Some(reason) => {
            let pending: Vec<&RuleComponent> = stages
                .iter()
                .skip(capped_stage + 1)
                .flatten()
                .copied()
                .collect();
            let unfinished = cut_short
                .iter()
                .chain(&pending)
                .flat_map(|g| g.rules.iter().map(|&i| &rule_set.rules[i]));
            if let Some((rule, construct)) = unfinished.into_iter().find_map(|r| {
                r.footprint
                    .non_monotone_construct()
                    .map(|construct| (r, construct))
            }) {
                return Err(QueryError::ResourceLimit(format!(
                    "the reasoning budget ({reason}) ran out before datalog rule {} could \
                     finish. It uses {construct}, so its conclusions are only meaningful over \
                     complete inputs, and the query fails rather than answer from a partial \
                     run. Raise the budget via f:reasoningMaxFacts / f:reasoningMaxSeconds / \
                     f:reasoningMaxMemoryMb (ledger config) or \"reasoningBudget\" (query)",
                    rule.name
                )));
            }
            diagnostics.mark_capped(reason, passes, facts, elapsed);
            let names = |groups: &[&RuleComponent]| -> Vec<String> {
                groups
                    .iter()
                    .flat_map(|g| g.rules.iter().map(|&i| rule_set.rules[i].name.clone()))
                    .collect()
            };
            diagnostics.capped_rules = names(&cut_short);
            diagnostics.pending_rules = names(&pending);
        }
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

/// The evaluation time for a run whose rules read the clock: the requested
/// one, else the commit time of the snapshot at `db.t`. A snapshot with no
/// commit at that `t` (an empty ledger, a staged view) has no time of its
/// own, and the wall clock would make the result depend on when it ran.
async fn resolve_evaluation_time(
    db: GraphDbRef<'_>,
    requested: Option<&DateTime>,
) -> Result<DateTime> {
    if let Some(time) = requested {
        return Ok(time.clone());
    }
    let commit_times = range_with_overlay(
        db.snapshot,
        TXN_META_GRAPH_ID,
        db.overlay,
        IndexType::Psot,
        RangeTest::Eq,
        RangeMatch::predicate(Sid::new(FLUREE_DB, fluree_vocab::db::TIME)),
        RangeOptions::new().with_to_t(db.t),
    )
    .await
    .map_err(|e| QueryError::Internal(format!("failed to read commit times: {e}")))?;
    commit_times
        .iter()
        .find_map(|f| match f.o {
            FlakeValue::Long(millis) if f.op && f.t == db.t => millis
                .checked_mul(1000)
                .and_then(DateTime::from_epoch_micros),
            _ => None,
        })
        .ok_or_else(|| {
            QueryError::InvalidQuery(format!(
                "datalog rules call NOW() or TODAY(), but the snapshot at t={} has no commit \
                 time to evaluate them at (an empty ledger or uncommitted changes). Set the \
                 evaluation time with \"reasoningTime\" (or # PRAGMA reasoning-time:)",
                db.t
            ))
        })
}

/// The named graphs rule heads derive into: the SID a derived fact carries
/// for each (none for the graph the rules run against), and the graph its
/// facts are routed to.
#[derive(Default)]
struct HeadGraphs {
    sids: HashMap<Arc<str>, Option<Sid>>,
    ids: HashMap<Sid, GraphId>,
}

impl HeadGraphs {
    /// Resolve every head graph against the ledger. A head may derive only
    /// into a user named graph that exists: the overlay can serve facts only
    /// for a graph the ledger can address, and the system graphs hold
    /// metadata rather than data. A head naming the graph the rules run
    /// against derives into it as a head with no graph does, so a stored
    /// rule serves queries over that graph and over the default graph alike.
    fn resolve(rule_set: &DatalogRuleSet, db: GraphDbRef<'_>) -> Result<Self> {
        let mut graphs = Self::default();
        for rule in rule_set.iter_in_order() {
            for iri in rule.heads.iter().filter_map(|h| h.graph.as_ref()) {
                if graphs.sids.contains_key(iri) {
                    continue;
                }
                let refuse = |why: &str| {
                    QueryError::InvalidQuery(format!(
                        "datalog rule {} derives into graph <{iri}>, {why}",
                        rule.name
                    ))
                };
                let g_id = db
                    .snapshot
                    .graph_registry
                    .graph_id_for_iri(iri)
                    .ok_or_else(|| {
                        refuse(
                            "which this ledger does not have. A rule derives only into an \
                         existing named graph; transact data into the graph to create it",
                        )
                    })?;
                if g_id < FIRST_USER_GRAPH_ID {
                    return Err(refuse("a system graph, which rules cannot write"));
                }
                if g_id == db.g_id {
                    graphs.sids.insert(iri.clone(), None);
                    continue;
                }
                let sid = IriEncoder::encode_iri(db.snapshot, iri)
                    .ok_or_else(|| refuse("whose IRI cannot be encoded"))?;
                graphs.ids.insert(sid.clone(), g_id);
                graphs.sids.insert(iri.clone(), Some(sid));
            }
        }
        Ok(graphs)
    }

    /// The graph a derived fact belongs to: its head's named graph, else
    /// the graph the rules run against.
    fn route(&self, flake: &Flake, default: GraphId) -> GraphId {
        flake
            .g
            .as_ref()
            .and_then(|g| self.ids.get(g).copied())
            .unwrap_or(default)
    }
}

/// The graph a fact derived over `snapshot` belongs to: the named graph its
/// `g` names, else `default`, the graph the rules ran against.
pub fn derived_fact_graph(
    snapshot: &LedgerSnapshot,
    default: GraphId,
) -> impl Fn(&Flake) -> GraphId + '_ {
    move |flake| {
        flake
            .g
            .as_ref()
            .and_then(|g| snapshot.decode_sid(g))
            .and_then(|iri| snapshot.graph_registry.graph_id_for_iri(&iri))
            .unwrap_or(default)
    }
}

/// What every group of a run shares.
struct RunPlan<'r> {
    rule_set: &'r DatalogRuleSet,
    reevaluations: &'r [Reevaluation],
    closures: &'r [Option<Closure>],
    env: RuleExecutionEnv<'r>,
    dict_novelty: Option<Arc<DictNovelty>>,
}

enum GroupOutcome {
    /// Every rule ran. `closed` when the group's recursion was evaluated in
    /// full, as a closure path, and needs no further pass.
    Finished {
        closed: bool,
    },
    Capped(&'static str),
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
            // Skipping old matches is only sound when re-running the body
            // over them would give the same rows. A volatile function gives
            // new ones every pass, and a seed carries no transaction
            // metadata for `t`/`op` to read.
            if !dependencies.footprint(index).repeatable {
                continue;
            }
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
    /// `start` plus the time budget; `None` when the budget is unbounded.
    deadline: Option<Instant>,
    evaluation_time: Option<DateTime>,
    head_graphs: &'a HeadGraphs,
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
    /// Run one pass of `group`: each rule's whole body on the group's first
    /// pass (or its closure path), its seeded bodies after that.
    async fn run_group(
        &mut self,
        db: GraphDbRef<'_>,
        plan: &RunPlan<'_>,
        group: &RuleComponent,
        delta: Option<Range<usize>>,
    ) -> Result<GroupOutcome> {
        let mut closed = false;
        for &index in &group.rules {
            let rule = &plan.rule_set.rules[index];
            if let (None, Some(closure)) = (&delta, &plan.closures[index]) {
                if let Some(reason) = self.over_budget() {
                    return Ok(GroupOutcome::Capped(reason));
                }
                match self
                    .apply_closure(db, rule, closure, plan.env, plan.dict_novelty.clone())
                    .await?
                {
                    Some(RuleOutcome::Finished) => {
                        closed = true;
                        continue;
                    }
                    Some(RuleOutcome::Capped(reason)) => return Ok(GroupOutcome::Capped(reason)),
                    None => {}
                }
            }
            // A group's first pass runs every whole body; nothing it reads
            // has been seen before.
            let seeds: Vec<Option<&DeltaAtom>> = match (&delta, &plan.reevaluations[index]) {
                (Some(_), Reevaluation::Delta(atoms)) => atoms.iter().map(Some).collect(),
                _ => vec![None],
            };
            for seed in seeds {
                if let Some(reason) = self.over_budget() {
                    return Ok(GroupOutcome::Capped(reason));
                }
                let body = match (seed, &delta) {
                    (Some(atom), Some(delta)) => {
                        match atom.seeded(&rule.body.query, &self.derived[delta.clone()]) {
                            Some(body) => body,
                            None => continue,
                        }
                    }
                    _ => rule.body.query.clone(),
                };
                if let RuleOutcome::Capped(reason) = self
                    .apply(db, rule, body, plan.env, plan.dict_novelty.clone())
                    .await?
                {
                    return Ok(GroupOutcome::Capped(reason));
                }
            }
        }
        Ok(GroupOutcome::Finished { closed })
    }

    fn over_budget(&self) -> Option<&'static str> {
        over_budget(self.budget, &self.start, self.derived.len(), self.bytes)
    }

    fn past_deadline(&self) -> bool {
        self.start.elapsed() > self.budget.max_duration
    }

    /// Derive `rule`'s whole closure in one run of its path body. `None` when
    /// the path cannot stand in for the recursion here — the path's predicate
    /// has a literal object, the path computed more pairs than the budget has
    /// room for, or the traversal hit the path operator's visited-node limit —
    /// and the group should iterate instead.
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
        // A path with both ends free computes every pair before it returns a
        // row. Past what the budget still has room for, iterating does less
        // work: the budget stops it fact by fact.
        let fact_room = self.budget.max_facts.saturating_sub(self.derived.len());
        let memory_room =
            self.budget.max_memory_bytes.saturating_sub(self.bytes) / std::mem::size_of::<Flake>();
        let pair_limit = fact_room.min(memory_room).saturating_add(1);
        match self
            .apply_with_pair_limit(
                db,
                rule,
                closure.body.clone(),
                env,
                dict_novelty,
                Some(pair_limit),
            )
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
        self.apply_with_pair_limit(db, rule, body, env, dict_novelty, None)
            .await
    }

    /// [`Self::apply`], with the body's property paths limited to
    /// `path_pair_limit` pairs before their first row.
    async fn apply_with_pair_limit(
        &mut self,
        db: GraphDbRef<'_>,
        rule: &DatalogRule,
        body: Query,
        env: RuleExecutionEnv<'_>,
        dict_novelty: Option<Arc<DictNovelty>>,
        path_pair_limit: Option<usize>,
    ) -> Result<RuleOutcome> {
        if let Some(reason) = env.request.cancelled() {
            return Err(QueryError::Cancelled { reason });
        }
        self.rule_body_runs += 1;
        let config = ContextConfig {
            dict_novelty,
            path_pair_limit,
            cancellation: Some(
                env.request
                    .cancellation
                    .cloned()
                    .unwrap_or_default()
                    .with_deadline_inside(self.deadline),
            ),
            evaluation_time: self.evaluation_time.clone(),
            ..Default::default()
        };
        let mut heads = HeadSink {
            state: self,
            rule,
            request: env.request,
            matched_rows: 0,
            instantiated: 0,
            capped: None,
        };
        let run = run_body(db, rule, body, env, config, &mut heads).await;
        let HeadSink {
            matched_rows,
            instantiated,
            capped,
            ..
        } = heads;
        if let Some(reason) = capped {
            return Ok(RuleOutcome::Capped(reason));
        }
        match run {
            Ok(()) => {}
            Err(QueryError::Cancelled { reason }) if env.request.cancelled().is_some() => {
                return Err(QueryError::Cancelled { reason });
            }
            // The deadline passed while the body ran; it stopped at its next
            // checkpoint instead of running to completion.
            Err(QueryError::Cancelled { .. }) => return Ok(RuleOutcome::Capped("time")),
            Err(e) => return Err(e),
        }
        // A body can finish between checkpoints after the deadline; it must
        // not let a final pass report a completed fixpoint.
        if matched_rows == 0 && self.past_deadline() {
            return Ok(RuleOutcome::Capped("time"));
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

/// Instantiates a rule's heads from its body's rows as the executor produces
/// them, so the budget stops a body that would derive past it instead of
/// letting it buffer every row first.
struct HeadSink<'s, 'a> {
    state: &'s mut Derivation<'a>,
    rule: &'s DatalogRule,
    request: QueryRequest<'s>,
    matched_rows: usize,
    instantiated: usize,
    /// Why the budget stopped the body, if it did.
    capped: Option<&'static str>,
}

impl HeadSink<'_, '_> {
    /// The budget the batch reached, if it reached one.
    fn derive(&mut self, batch: &Batch) -> Result<Option<&'static str>> {
        // Rows between deadline checks while instantiating heads.
        const DEADLINE_CHECK_ROWS: usize = 1024;

        let state = &mut *self.state;
        state.body_rows += batch.len();
        if state.past_deadline() {
            return Ok(Some("time"));
        }
        for row in 0..batch.len() {
            self.matched_rows += 1;
            if self.matched_rows.is_multiple_of(DEADLINE_CHECK_ROWS) {
                if let Some(reason) = self.request.cancelled() {
                    return Err(QueryError::Cancelled { reason });
                }
                if state.past_deadline() {
                    return Ok(Some("time"));
                }
            }
            for head in &self.rule.heads {
                let Some(mut flake) =
                    instantiate(head, batch, row, state.snapshot, state.derived_t)
                else {
                    continue;
                };
                if let Some(graph) = &head.graph {
                    flake.g = state.head_graphs.sids.get(graph).cloned().flatten();
                }
                self.instantiated += 1;
                if !state.seen.insert(fact_key(&flake)) {
                    continue;
                }
                // Check before pushing. Pushing first and testing
                // `len() > max_facts` let one fact past the ceiling through,
                // and nothing trimmed it afterwards, so a budget of N
                // returned N+1 facts. Checking first also stops charging
                // `bytes` for a fact never kept.
                if state.derived.len() >= state.budget.max_facts {
                    return Ok(Some("facts"));
                }
                state.bytes += approx_flake_bytes(&flake);
                state.diagnostics.record_rule_fired(&self.rule.name);
                state.derived.push(flake);
                if state.bytes > state.budget.max_memory_bytes {
                    return Ok(Some("memory"));
                }
            }
        }
        Ok(None)
    }
}

impl BatchSink for HeadSink<'_, '_> {
    async fn push(&mut self, batch: Batch) -> Result<()> {
        match self.derive(&batch)? {
            None => Ok(()),
            Some(reason) => {
                self.capped = Some(reason);
                Err(QueryError::ResourceLimit(format!(
                    "datalog rule stopped at the reasoning {reason} budget"
                )))
            }
        }
    }
}

/// Run one rule body through the executor under `config` and collect its
/// rows.
pub(super) async fn collect_body<'a>(
    db: GraphDbRef<'a>,
    rule: &DatalogRule,
    body: Query,
    env: RuleExecutionEnv<'_>,
    config: ContextConfig<'a, '_>,
) -> Result<Vec<Batch>> {
    struct Collect(Vec<Batch>);
    impl BatchSink for Collect {
        async fn push(&mut self, batch: Batch) -> Result<()> {
            self.0.push(batch);
            Ok(())
        }
    }
    let mut rows = Collect(Vec::new());
    run_body(db, rule, body, env, config, &mut rows).await?;
    Ok(rows.0)
}

/// Run one rule body through the executor under `config`, handing its rows
/// to `sink`.
async fn run_body<'a>(
    db: GraphDbRef<'a>,
    rule: &DatalogRule,
    body: Query,
    env: RuleExecutionEnv<'_>,
    config: ContextConfig<'a, '_>,
    sink: &mut impl BatchSink,
) -> Result<()> {
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
        tracker: env.request.tracker,
        binary_store: env.binary_store.cloned(),
        binary_g_id: db.g_id,
        ..config
    };
    execute_prepared_streaming(db, &rule.body.vars, prepared, config, sink).await
}

/// Instantiate one head against one result row. `None` when a position
/// cannot be filled (an unbound variable, or a literal where a node is
/// required); the caller counts those.
pub(super) fn instantiate(
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
