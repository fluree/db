//! Why a reasoning run concluded a fact. For each rule that derives it: the
//! body row that matched, the facts that row read, the negated patterns it
//! found empty and where it looked, and the solutions an aggregate counted.
//!
//! An explanation is one step. A premise derived by reasoning is marked so;
//! explaining it in turn walks the derivation back to asserted facts.
//!
//! Only facts the run materialized are explained: a replayed row counts only
//! when its conclusion is one of them, so a capped run never explains a
//! conclusion it did not reach. A rule whose body gives different rows on
//! every run cannot be replayed and is named instead.

use super::execute::{collect_body, instantiate, RuleExecutionEnv};
use super::{DatalogRule, DatalogRuleSet, HeadTerm, RuleHead};
use crate::binding::{Batch, Binding};
use crate::error::{QueryError, Result};
use crate::execute::QueryRequest;
use crate::ir::pattern::produced_vars_of;
use crate::ir::{Expression, Function, GraphName, Grouping, Pattern, QueryOutput, Ref, Term};
use crate::var_registry::{VarId, VarRegistry};
use crate::ContextConfig;
use fluree_db_core::clock::Instant;
use fluree_db_core::comparator::IndexType;
use fluree_db_core::range::{RangeMatch, RangeTest};
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{Flake, GraphDbRef, GraphId, LedgerSnapshot, OverlayProvider, Sid};
use serde::Serialize;
use serde_json::{json, Value as JsonValue};
use std::collections::BTreeMap;
use std::sync::Arc;

/// The derived facts to explain.
#[derive(Debug, Clone)]
pub struct ExplainTarget {
    pub subject: Sid,
    pub predicate: Sid,
    /// `None` explains every derived fact with this subject and predicate.
    pub object: Option<FlakeValue>,
    /// The named graph the facts were derived into; `None` for the graph the
    /// rules ran against.
    pub graph: Option<Arc<str>>,
}

/// What a run concluded about a subject and predicate, and why.
#[derive(Debug, Clone, Serialize)]
pub struct Explanation {
    /// The snapshot the run read.
    pub t: i64,
    /// The rule set the run evaluated, as a content hash.
    pub rule_set: String,
    /// When the rules read the clock, the time `NOW()` returned.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evaluation_time: Option<String>,
    /// Whether the run reached its fixpoint and every replay finished
    /// within the reasoning budget. A capped run may be missing conclusions;
    /// a replay the deadline stopped may be missing derivations.
    pub complete: bool,
    pub facts: Vec<ExplainedFact>,
    /// Rules that may derive these facts but whose bodies cannot be replayed
    /// to the same rows: they call `RAND`, `UUID`, `STRUUID` or `BNODE`, or
    /// read transaction metadata. Their derivations are not reconstructed.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub not_replayed: Vec<String>,
}

/// One derived fact and every way the rules derived it.
#[derive(Debug, Clone, Serialize)]
pub struct ExplainedFact {
    pub subject: JsonValue,
    pub predicate: JsonValue,
    pub object: JsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub graph: Option<String>,
    /// The fact is also asserted; the derivations are additional support.
    pub asserted: bool,
    /// Empty when OWL 2 RL entailed the fact, or only a rule in
    /// [`Explanation::not_replayed`] derived it.
    pub derivations: Vec<Derivation>,
}

/// One rule body row that derives the fact.
#[derive(Debug, Clone, Serialize)]
pub struct Derivation {
    pub rule: String,
    /// The row's bindings, by variable.
    pub bindings: BTreeMap<String, JsonValue>,
    /// The body's triple patterns with the row substituted: the facts the
    /// row matched.
    pub premises: Vec<Premise>,
    /// Negated patterns the row passed, each of which found nothing.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub absences: Vec<Absence>,
    /// For each grouped subquery, the solutions in the row's group.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<GroupInputs>,
}

/// A fact a derivation read.
#[derive(Debug, Clone, Serialize)]
pub struct Premise {
    pub subject: JsonValue,
    pub predicate: JsonValue,
    pub object: JsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub graph: Option<String>,
    /// Derived by reasoning rather than asserted.
    pub derived: bool,
}

/// A negation that held: nothing matched these patterns.
#[derive(Debug, Clone, Serialize)]
pub struct Absence {
    /// Each pattern as `[subject, predicate, object]`, with the row's bindings
    /// substituted; a variable the row leaves open stays a variable.
    pub patterns: Vec<[JsonValue; 3]>,
    /// The graph searched; `None` for the graph the rules ran against.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub graph: Option<String>,
}

/// The solutions a grouped subquery aggregated into the row's group.
#[derive(Debug, Clone, Serialize)]
pub struct GroupInputs {
    /// The grouping variables' values; empty for the implicit group.
    pub key: BTreeMap<String, JsonValue>,
    /// The graph the subquery read; `None` for the graph the rules ran
    /// against.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub graph: Option<String>,
    pub inputs: Vec<BTreeMap<String, JsonValue>>,
}

/// The materialized facts `target` names, from the derived facts of the
/// graph it names (the rules' graph `default_g_id` when it names none).
pub(crate) fn materialized_facts(
    derived: &dyn OverlayProvider,
    snapshot: &LedgerSnapshot,
    default_g_id: GraphId,
    target: &ExplainTarget,
) -> Vec<Flake> {
    let g_id = match &target.graph {
        None => default_g_id,
        Some(iri) => match snapshot.graph_registry.graph_id_for_iri(iri) {
            Some(g_id) => g_id,
            None => return Vec::new(),
        },
    };
    let mut facts = Vec::new();
    derived.for_each_overlay_flake(
        g_id,
        IndexType::Spot,
        None,
        None,
        true,
        i64::MAX,
        &mut |f| {
            if f.op && matches_target(f, target) {
                facts.push(f.clone());
            }
        },
    );
    facts
}

/// Explain `materialized`, the facts `target` names that the run derived.
/// `view` reads asserted and derived facts together; `base` reads asserted
/// facts only. Replays run until `deadline`, as the run's rule bodies did: the
/// request's cancellation fails the explanation, the deadline leaves it
/// incomplete. Returns the facts, the rules that could not be replayed, and
/// whether every replay finished.
pub(crate) async fn explain_facts(
    view: GraphDbRef<'_>,
    base: GraphDbRef<'_>,
    rule_set: &DatalogRuleSet,
    env: RuleExecutionEnv<'_>,
    target: &ExplainTarget,
    materialized: &[Flake],
    deadline: Option<Instant>,
) -> Result<(Vec<ExplainedFact>, Vec<String>, bool)> {
    let graph = target.graph.as_deref();
    let mut facts: Vec<(&Flake, ExplainedFact)> = Vec::new();
    for flake in materialized {
        if facts.iter().any(|(f, _)| same_fact(f, flake)) {
            continue;
        }
        let fact = ExplainedFact {
            subject: iri(&flake.s, view.snapshot),
            predicate: iri(&flake.p, view.snapshot),
            object: value(&flake.o, Some(&flake.dt), lang(flake), view.snapshot),
            graph: graph.map(str::to_string),
            asserted: asserted(base, graph, &flake.s, &flake.p, &ObjectId::of(flake)).await?,
            derivations: Vec::new(),
        };
        facts.push((flake, fact));
    }

    // Nothing the run derived, so no replayed row can explain anything.
    if facts.is_empty() {
        return Ok((Vec::new(), Vec::new(), true));
    }

    let request = env.request;
    let replay = request
        .cancellation
        .cloned()
        .unwrap_or_default()
        .with_deadline_inside(deadline);
    let env = RuleExecutionEnv {
        request: QueryRequest {
            cancellation: Some(&replay),
            ..request
        },
        ..env
    };
    let past_deadline = || deadline.is_some_and(|d| Instant::now() >= d);
    let timed_out =
        |e: &QueryError| matches!(e, QueryError::Cancelled { .. }) && request.cancelled().is_none();

    let mut not_replayed = Vec::new();
    let mut complete = true;
    'rules: for rule in rule_set.iter_in_order() {
        let heads: Vec<&RuleHead> = rule
            .heads
            .iter()
            .filter(|h| may_derive(h, target))
            .collect();
        if heads.is_empty() {
            continue;
        }
        if !rule.footprint.repeatable {
            not_replayed.push(rule.name.clone());
            continue;
        }
        if past_deadline() {
            complete = false;
            break;
        }
        let batches = match body_rows(view, rule, rule.body.query.clone(), env).await {
            Ok(batches) => batches,
            Err(e) if timed_out(&e) => {
                complete = false;
                break;
            }
            Err(e) => return Err(e),
        };
        for batch in &batches {
            for row in 0..batch.len() {
                for head in &heads {
                    let Some(flake) = instantiate(head, batch, row, view.snapshot, view.t) else {
                        continue;
                    };
                    let Some((_, fact)) = facts.iter_mut().find(|(f, _)| same_fact(f, &flake))
                    else {
                        continue;
                    };
                    match derivation(view, base, rule, env, batch, row).await {
                        Ok(derivation) => fact.derivations.push(derivation),
                        Err(e) if timed_out(&e) => {
                            complete = false;
                            break 'rules;
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        }
    }
    Ok((
        facts.into_iter().map(|(_, fact)| fact).collect(),
        not_replayed,
        complete,
    ))
}

fn may_derive(head: &RuleHead, target: &ExplainTarget) -> bool {
    let fits = |term: &HeadTerm, sid: &Sid| match term {
        HeadTerm::Sid(s) => s == sid,
        HeadTerm::Var(_) => true,
        HeadTerm::Literal { .. } => false,
    };
    head.graph == target.graph
        && fits(&head.subject, &target.subject)
        && fits(&head.predicate, &target.predicate)
}

fn matches_target(flake: &Flake, target: &ExplainTarget) -> bool {
    flake.s == target.subject
        && flake.p == target.predicate
        && target.object.as_ref().is_none_or(|o| *o == flake.o)
}

/// Whether two facts in the same graph are the same RDF triple: subject,
/// predicate, value, datatype and language tag.
fn same_fact(a: &Flake, b: &Flake) -> bool {
    a.s == b.s && a.p == b.p && a.o == b.o && a.dt == b.dt && lang(a) == lang(b)
}

/// An object as a fact or pattern pins it: its value, its datatype when
/// known, and its language tag.
struct ObjectId {
    value: FlakeValue,
    datatype: Option<Sid>,
    lang: Option<String>,
}

impl ObjectId {
    fn of(flake: &Flake) -> Self {
        Self {
            value: flake.o.clone(),
            datatype: Some(flake.dt.clone()),
            lang: lang(flake).map(str::to_string),
        }
    }

    /// Whether `flake`'s object is this one. A language-tagged object is
    /// matched by its tag; any other by its datatype, when known.
    fn matches(&self, flake: &Flake) -> bool {
        flake.o == self.value
            && lang(flake) == self.lang.as_deref()
            && (self.lang.is_some() || self.datatype.as_ref().is_none_or(|dt| *dt == flake.dt))
    }
}

fn lang(flake: &Flake) -> Option<&str> {
    flake.m.as_ref().and_then(|m| m.lang.as_deref())
}

async fn body_rows(
    db: GraphDbRef<'_>,
    rule: &DatalogRule,
    body: crate::ir::Query,
    env: RuleExecutionEnv<'_>,
) -> Result<Vec<Batch>> {
    let config = ContextConfig {
        cancellation: Some(env.request.cancellation.cloned().unwrap_or_default()),
        evaluation_time: env.evaluation_time.cloned(),
        ..Default::default()
    };
    collect_body(db, rule, body, env, config).await
}

async fn derivation(
    view: GraphDbRef<'_>,
    base: GraphDbRef<'_>,
    rule: &DatalogRule,
    env: RuleExecutionEnv<'_>,
    batch: &Batch,
    row: usize,
) -> Result<Derivation> {
    let vars = &rule.body.vars;
    let snapshot = view.snapshot;
    let bindings = row_bindings(batch, row, vars, snapshot, None);

    let mut premises = Vec::new();
    let mut absences = Vec::new();
    let mut grouped = Vec::new();
    walk(
        &rule.body.query.patterns,
        None,
        &mut |pattern, graph| match pattern {
            Pattern::Triple(tp) => premises.push((tp.clone(), graph)),
            Pattern::NotExists(inner) => absences.push((inner.clone(), graph)),
            Pattern::Filter(expr) => {
                if let Some(inner) = negated_patterns(expr) {
                    absences.push((inner.to_vec(), graph));
                }
            }
            Pattern::Subquery(sq) => {
                if let Some(grouping) = &sq.grouping {
                    grouped.push((sq.patterns.clone(), group_keys(grouping), graph));
                }
            }
            _ => {}
        },
    );

    let mut resolved_premises = Vec::with_capacity(premises.len());
    for (tp, graph) in premises {
        let term = |r: &Ref| ref_term(r, batch, row, vars, snapshot);
        let object = object_term(&tp.o, tp.dtc.as_ref(), batch, row, vars, snapshot);
        let derived = match (
            node(&tp.s, batch, row),
            node(&tp.p, batch, row),
            object_id(&tp.o, tp.dtc.as_ref(), batch, row),
        ) {
            (Some(s), Some(p), Some(o)) => !asserted(base, graph.as_deref(), &s, &p, &o).await?,
            _ => false,
        };
        resolved_premises.push(Premise {
            subject: term(&tp.s),
            predicate: term(&tp.p),
            object,
            graph: graph.map(|g| g.to_string()),
            derived,
        });
    }

    let absences = absences
        .into_iter()
        .map(|(inner, graph)| Absence {
            patterns: inner
                .iter()
                .filter_map(|p| match p {
                    Pattern::Triple(tp) => Some([
                        ref_term(&tp.s, batch, row, vars, snapshot),
                        ref_term(&tp.p, batch, row, vars, snapshot),
                        object_term(&tp.o, tp.dtc.as_ref(), batch, row, vars, snapshot),
                    ]),
                    _ => None,
                })
                .collect(),
            graph: graph.map(|g| g.to_string()),
        })
        .collect();

    let mut groups = Vec::with_capacity(grouped.len());
    for (patterns, keys, graph) in grouped {
        groups.push(group_inputs(view, rule, env, &patterns, &keys, graph, batch, row).await?);
    }

    Ok(Derivation {
        rule: rule.name.clone(),
        bindings,
        premises: resolved_premises,
        absences,
        groups,
    })
}

/// Visit the body's top-level patterns, and those inside `GRAPH` blocks with
/// the graph they read.
fn walk<'p>(
    patterns: &'p [Pattern],
    graph: Option<Arc<str>>,
    visit: &mut impl FnMut(&'p Pattern, Option<Arc<str>>),
) {
    for pattern in patterns {
        match pattern {
            Pattern::Graph {
                name: GraphName::Iri(iri),
                patterns,
            } => walk(patterns, Some(iri.clone()), visit),
            other => visit(other, graph.clone()),
        }
    }
}

/// The patterns a negating filter requires to be empty.
fn negated_patterns(expr: &Expression) -> Option<&[Pattern]> {
    match expr {
        Expression::Exists {
            patterns,
            negated: true,
        } => Some(patterns),
        Expression::Call {
            func: Function::Not,
            args,
        } => match args.as_slice() {
            [Expression::Exists {
                patterns,
                negated: false,
            }] => Some(patterns),
            _ => None,
        },
        _ => None,
    }
}

fn group_keys(grouping: &Grouping) -> Vec<VarId> {
    match grouping {
        Grouping::Explicit { group_by, .. } => group_by.iter().copied().collect(),
        Grouping::Implicit { .. } => Vec::new(),
    }
}

/// The solutions of a grouped subquery's patterns that fall in the row's
/// group, read from `graph` when the subquery sits in a `GRAPH` block.
#[allow(clippy::too_many_arguments)]
async fn group_inputs(
    view: GraphDbRef<'_>,
    rule: &DatalogRule,
    env: RuleExecutionEnv<'_>,
    patterns: &[Pattern],
    keys: &[VarId],
    graph: Option<Arc<str>>,
    batch: &Batch,
    row: usize,
) -> Result<GroupInputs> {
    let vars = &rule.body.vars;
    let snapshot = view.snapshot;
    let key_values: Vec<(VarId, Option<&Binding>)> =
        keys.iter().map(|&v| (v, batch.get(row, v))).collect();
    let mut inner_vars: Vec<VarId> = produced_vars_of(patterns).into_iter().collect();
    inner_vars.sort_unstable();
    let mut query = rule.body.query.clone();
    query.patterns = match &graph {
        Some(iri) => vec![Pattern::Graph {
            name: GraphName::Iri(iri.clone()),
            patterns: patterns.to_vec(),
        }],
        None => patterns.to_vec(),
    };
    query.grouping = None;
    query.output = QueryOutput::select_all(inner_vars.clone());
    let inputs = body_rows(view, rule, query, env).await?;
    let mut rows = Vec::new();
    for input in &inputs {
        for r in 0..input.len() {
            let in_group = key_values
                .iter()
                .all(|(v, value)| input.get(r, *v) == *value);
            if in_group {
                rows.push(row_bindings(input, r, vars, snapshot, Some(&inner_vars)));
            }
        }
    }
    Ok(GroupInputs {
        key: key_values
            .iter()
            .filter_map(|(v, value)| Some((name(vars, *v), binding(value.as_ref()?, snapshot)?)))
            .collect(),
        graph: graph.map(|g| g.to_string()),
        inputs: rows,
    })
}

fn name(vars: &VarRegistry, v: VarId) -> String {
    vars.name(v).to_string()
}

/// A row's bindings by variable name, restricted to `only` when given.
fn row_bindings(
    batch: &Batch,
    row: usize,
    vars: &VarRegistry,
    snapshot: &LedgerSnapshot,
    only: Option<&[VarId]>,
) -> BTreeMap<String, JsonValue> {
    vars.iter()
        .filter(|(_, v)| only.is_none_or(|only| only.contains(v)))
        .filter_map(|(n, v)| {
            let value = binding(batch.get(row, v)?, snapshot)?;
            Some((n.to_string(), value))
        })
        .collect()
}

fn ref_term(
    r: &Ref,
    batch: &Batch,
    row: usize,
    vars: &VarRegistry,
    snapshot: &LedgerSnapshot,
) -> JsonValue {
    match r {
        Ref::Var(v) => bound_or_var(*v, batch, row, vars, snapshot),
        Ref::Sid(sid) => iri(sid, snapshot),
        Ref::Iri(i) => json!({"@id": i.as_ref()}),
    }
}

fn object_term(
    t: &Term,
    dtc: Option<&fluree_db_core::DatatypeConstraint>,
    batch: &Batch,
    row: usize,
    vars: &VarRegistry,
    snapshot: &LedgerSnapshot,
) -> JsonValue {
    match t {
        Term::Var(v) => bound_or_var(*v, batch, row, vars, snapshot),
        Term::Sid(sid) => iri(sid, snapshot),
        Term::Iri(i) => json!({"@id": i.as_ref()}),
        Term::Value(v) => value(
            v,
            dtc.map(fluree_db_core::DatatypeConstraint::datatype),
            dtc.and_then(|c| c.lang_tag()),
            snapshot,
        ),
    }
}

fn bound_or_var(
    v: VarId,
    batch: &Batch,
    row: usize,
    vars: &VarRegistry,
    snapshot: &LedgerSnapshot,
) -> JsonValue {
    batch
        .get(row, v)
        .and_then(|b| binding(b, snapshot))
        .unwrap_or_else(|| json!(name(vars, v)))
}

fn node(r: &Ref, batch: &Batch, row: usize) -> Option<Sid> {
    match r {
        Ref::Sid(sid) => Some(sid.clone()),
        Ref::Var(v) => match batch.get(row, *v)? {
            Binding::Sid { sid, .. } => Some(sid.clone()),
            Binding::IriMatch { primary_sid, .. } => Some(primary_sid.clone()),
            _ => None,
        },
        Ref::Iri(_) => None,
    }
}

fn object_id(
    t: &Term,
    dtc: Option<&fluree_db_core::DatatypeConstraint>,
    batch: &Batch,
    row: usize,
) -> Option<ObjectId> {
    let node = |sid: &Sid| ObjectId {
        value: FlakeValue::Ref(sid.clone()),
        datatype: None,
        lang: None,
    };
    let literal = |value: &FlakeValue, dtc: Option<&fluree_db_core::DatatypeConstraint>| ObjectId {
        value: value.clone(),
        datatype: dtc.map(|c| c.datatype().clone()),
        lang: dtc.and_then(|c| c.lang_tag()).map(str::to_string),
    };
    match t {
        Term::Sid(sid) => Some(node(sid)),
        Term::Value(v) => Some(literal(v, dtc)),
        Term::Var(v) => match batch.get(row, *v)? {
            Binding::Sid { sid, .. } => Some(node(sid)),
            Binding::IriMatch { primary_sid, .. } => Some(node(primary_sid)),
            Binding::Lit { val, dtc, .. } => Some(literal(val, Some(dtc))),
            _ => None,
        },
        Term::Iri(_) => None,
    }
}

/// Whether `(s, p, o)` is asserted in `graph` (the rules' graph when `None`).
async fn asserted(
    base: GraphDbRef<'_>,
    graph: Option<&str>,
    s: &Sid,
    p: &Sid,
    o: &ObjectId,
) -> Result<bool> {
    let db = match graph {
        None => base,
        Some(iri) => match base.snapshot.graph_registry.graph_id_for_iri(iri) {
            Some(g_id) => GraphDbRef::new(base.snapshot, g_id, base.overlay, base.t),
            None => return Ok(false),
        },
    };
    let facts = db
        .range(
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch {
                s: Some(s.clone()),
                p: Some(p.clone()),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| QueryError::Internal(format!("failed to look up a premise: {e}")))?;
    Ok(facts.iter().any(|f| f.op && o.matches(f)))
}

fn iri(sid: &Sid, snapshot: &LedgerSnapshot) -> JsonValue {
    json!({"@id": snapshot.decode_sid(sid).unwrap_or_else(|| sid.to_string())})
}

fn binding(b: &Binding, snapshot: &LedgerSnapshot) -> Option<JsonValue> {
    match b {
        Binding::Sid { sid, .. } => Some(iri(sid, snapshot)),
        Binding::IriMatch { iri: i, .. } | Binding::Iri(i) => Some(json!({"@id": i.as_ref()})),
        Binding::Lit { val, dtc, .. } => {
            Some(value(val, Some(dtc.datatype()), dtc.lang_tag(), snapshot))
        }
        Binding::Grouped(items) => Some(JsonValue::Array(
            items.iter().filter_map(|b| binding(b, snapshot)).collect(),
        )),
        _ => None,
    }
}

/// A value in JSON-LD form: native JSON for strings, numbers and booleans,
/// `{"@value", "@type"}` or `{"@value", "@language"}` otherwise.
fn value(
    v: &FlakeValue,
    datatype: Option<&Sid>,
    lang: Option<&str>,
    snapshot: &LedgerSnapshot,
) -> JsonValue {
    const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
    let datatype_iri = datatype.and_then(|dt| snapshot.decode_sid(dt));
    match v {
        FlakeValue::Ref(sid) => iri(sid, snapshot),
        FlakeValue::String(s) => match (lang, datatype_iri.as_deref()) {
            (Some(lang), _) => json!({"@value": s, "@language": lang}),
            (None, None | Some(XSD_STRING)) => json!(s),
            (None, Some(dt)) => json!({"@value": s, "@type": dt}),
        },
        FlakeValue::Long(n) => json!(n),
        FlakeValue::Double(d) => json!(d),
        FlakeValue::Boolean(b) => json!(b),
        other => match datatype_iri {
            Some(dt) => json!({"@value": other.to_string(), "@type": dt}),
            None => json!({"@value": other.to_string()}),
        },
    }
}
