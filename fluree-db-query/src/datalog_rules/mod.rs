//! User-defined datalog rules: parsing, validation and fixpoint execution.
//!
//! A rule is a **body** — an ordinary query `WHERE` clause, parsed and lowered
//! by the same JSON-LD / SPARQL parsers every query goes through — and one or
//! more **heads**, the triples to derive for each solution of the body. Rule
//! bodies are executed by the planned query executor, so everything the
//! executor supports is available to a rule: edge annotations (`@annotation`,
//! `{| |}`, `~ ?r`), claim-first patterns (`@reifies`, `rdf:reifies`), full
//! FILTER expressions, BIND, VALUES, UNION, property paths and subqueries.
//! `NOT EXISTS` and grouped aggregate subqueries are supported when the
//! program is stratified: no rule may negate or aggregate facts that depend
//! on its own conclusions. The constructs a rule body may not use (OPTIONAL,
//! MINUS, top-level aggregates, SERVICE) are rejected with an error naming
//! the construct.
//!
//! Rules are stored as `f:rule` values (JSON with `where`/`insert`, or an
//! `f:sparql`-typed `CONSTRUCT … WHERE …` string) or supplied per query in the
//! `rules` array. Both sources share the parsers and the executor.
//!
//! Design record: `docs/design/rules-engine.md`.

mod closure;
mod delta;
mod dependency;
mod execute;
mod explain;
mod parse;
mod validate;

pub use dependency::{
    Access, GraphScope, PredicateFootprint, RuleComponent, RuleDependencies, RuleFootprint,
};
pub use execute::{
    derived_fact_graph, execute_datalog_rules, execute_datalog_rules_with_query_rules,
    load_rule_set, run_fixpoint, run_fixpoint_with_schedule, RuleExecutionEnv, RuleSchedule,
};
pub(crate) use explain::{explain_facts, materialized_facts};
pub use explain::{
    Absence, Derivation, ExplainTarget, ExplainedFact, Explanation, GroupInputs, Premise,
};
pub use parse::parse_query_time_rule;

use crate::error::{QueryError, Result};
use crate::ir::Query;
use crate::var_registry::{VarId, VarRegistry};
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{Flake, GraphDbRef, IndexType, RangeMatch, RangeTest, Sid};
use fluree_db_reasoner::ReasoningDiagnostics;
use fluree_vocab::namespaces::FLUREE_DB;
use serde_json::Value as JsonValue;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Local name for the `f:rule` predicate.
const RULE_LOCAL_NAME: &str = "rule";

/// One term of a rule head (an `insert` pattern position or a CONSTRUCT
/// template position).
#[derive(Debug, Clone)]
pub enum HeadTerm {
    /// Bound by the rule body; every head variable is checked to be
    /// body-bound at parse time (range restriction).
    Var(VarId),
    /// Constant IRI — a subject, a predicate, or an object reference.
    Sid(Sid),
    /// Constant literal object with its datatype and optional language tag.
    Literal {
        value: FlakeValue,
        datatype: Sid,
        lang: Option<Arc<str>>,
    },
}

/// A triple the rule derives for every solution of its body.
#[derive(Debug, Clone)]
pub struct RuleHead {
    pub subject: HeadTerm,
    pub predicate: HeadTerm,
    pub object: HeadTerm,
    /// The named graph the triple is derived into, as an expanded IRI.
    /// `None`: the graph the rules run against.
    pub graph: Option<Arc<str>>,
}

/// The rule body: a lowered query whose `output` projects every variable the
/// body binds, plus the registry the head terms' [`VarId`]s belong to.
#[derive(Debug, Clone)]
pub struct RuleBody {
    pub query: Query,
    pub vars: VarRegistry,
}

/// A parsed, validated rule ready for the fixpoint.
#[derive(Debug, Clone)]
pub struct DatalogRule {
    /// Stored rules: the `f:rule` subject. Query-time rules: a synthetic id.
    pub id: Sid,
    /// Human-readable name used in diagnostics.
    pub name: String,
    pub body: RuleBody,
    pub heads: Vec<RuleHead>,
    /// What the body may read and the heads may write, for dependency
    /// analysis.
    pub footprint: RuleFootprint,
    /// The rule's source text (JSON or SPARQL). Hashed into the reasoning
    /// cache key so a changed rule never serves a stale materialization.
    pub source: Arc<str>,
}

/// The rule set for one materialization, in the order rules were added. The
/// fixpoint schedules them by [`DatalogRuleSet::dependencies`], not by this
/// order.
#[derive(Debug, Default)]
pub struct DatalogRuleSet {
    rules: Vec<DatalogRule>,
}

impl DatalogRuleSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a rule, refusing a second rule with the same id.
    ///
    /// This used to replace the earlier rule. A stored rule's id is its
    /// `f:rule` subject, and `f:rule` is multi-cardinality like any other
    /// predicate, so a subject carrying two rule values kept only whichever
    /// the index scan returned last — silently, and in an order the author
    /// does not control. That is the same silent-drop shape the rest of this
    /// work is closing: a reasoning query answered over a rule set that
    /// quietly lost a rule is wrong, and being wrong is worse than failing
    /// with the subject's name in the message.
    pub fn add_rule(&mut self, rule: DatalogRule) -> Result<()> {
        if let Some(existing) = self.rules.iter().find(|r| r.id == rule.id) {
            // No parenthetical naming "the two rules": they collide precisely
            // because they share a subject, and a rule's `name` is that
            // subject decoded, so both names are the same string. The earlier
            // wording promised to tell them apart and always read
            // `(ex:r1 and ex:r1)`.
            return Err(QueryError::InvalidQuery(format!(
                "two datalog rules share the id <{}>; a rule id must name exactly one \
                 rule. Give each rule its own subject, or retract one of the `f:rule` \
                 values on that subject",
                existing.name
            )));
        }
        self.rules.push(rule);
        Ok(())
    }

    pub fn iter_in_order(&self) -> impl Iterator<Item = &DatalogRule> {
        self.rules.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// The rules that negate or aggregate facts, each with what makes it so
    /// (see [`RuleFootprint::non_monotone_construct`]).
    pub fn non_monotone_rules(&self) -> Vec<(&str, &'static str)> {
        self.rules
            .iter()
            .filter_map(|r| {
                r.footprint
                    .non_monotone_construct()
                    .map(|construct| (r.name.as_str(), construct))
            })
            .collect()
    }

    /// Whether some rule calls `NOW()` or `TODAY()`, so the run needs an
    /// evaluation time.
    pub fn reads_clock(&self) -> bool {
        self.rules.iter().any(|r| r.footprint.reads_clock)
    }

    /// Read/write footprints and recursive groups, indexed by execution order.
    pub fn dependencies(&self) -> RuleDependencies {
        RuleDependencies::analyze(&self.rules)
    }

    /// Order-independent hash of every rule's id and source text, for the
    /// reasoning cache key. Stored rules are hashed by content rather than
    /// trusted to the ledger epoch because a cross-ledger `f:rulesSource` can
    /// change without the querying ledger moving.
    pub fn content_hash(&self) -> u64 {
        let mut parts: Vec<(String, &str)> = self
            .rules
            .iter()
            .map(|r| (r.id.to_string(), r.source.as_ref()))
            .collect();
        parts.sort();
        let mut h = std::collections::hash_map::DefaultHasher::new();
        for (id, source) in parts {
            id.hash(&mut h);
            source.hash(&mut h);
        }
        h.finish()
    }
}

/// Result of executing datalog rules.
#[derive(Debug)]
pub struct DatalogExecutionResult {
    /// All derived flakes (deduplicated).
    pub derived_flakes: Vec<Flake>,
    /// Fixpoint diagnostics — iterations, fact count, budget capping.
    pub diagnostics: ReasoningDiagnostics,
    /// Rule bodies evaluated across all passes, counting each seeded run of
    /// a recursive rule separately.
    pub rule_body_runs: usize,
    /// Rows those bodies returned. Re-deriving known facts shows up here.
    pub body_rows: usize,
}

/// Human-readable IRI for a rule id, for diagnostics.
pub(crate) fn rule_iri(snapshot: &fluree_db_core::LedgerSnapshot, id: &Sid) -> String {
    snapshot.decode_sid(id).unwrap_or_else(|| id.to_string())
}

/// Extract the stored datalog rules (`f:rule` assertions) from a graph.
///
/// A rule that fails to parse or validate is an error, not a warning: a
/// reasoning query answered over an incomplete rule set is silently wrong,
/// which is worse than a query that fails naming the broken rule.
pub async fn extract_datalog_rules(db: GraphDbRef<'_>) -> Result<DatalogRuleSet> {
    let mut rule_set = DatalogRuleSet::new();

    let rule_predicate_sid = Sid::new(FLUREE_DB, RULE_LOCAL_NAME);
    let rule_flakes: Vec<Flake> = db
        .range(
            IndexType::Psot,
            RangeTest::Eq,
            RangeMatch {
                p: Some(rule_predicate_sid),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| QueryError::Internal(format!("Failed to query for rules: {e}")))?
        .into_iter()
        .filter(|f| f.op)
        .collect();

    // The literal's datatype selects the rule language: `@json` → JSON-LD
    // `{"where": …, "insert": …}`; `f:sparql`-typed string → SPARQL
    // `CONSTRUCT … WHERE …`.
    let sparql_dt = Sid::new(FLUREE_DB, fluree_vocab::db::SPARQL);
    for flake in &rule_flakes {
        let rule_id = flake.s.clone();
        let label = rule_iri(db.snapshot, &rule_id);
        let parsed = match &flake.o {
            FlakeValue::Json(json_str) => {
                let rule_json: JsonValue = serde_json::from_str(json_str).map_err(|e| {
                    QueryError::InvalidQuery(format!(
                        "stored datalog rule <{label}> is not valid JSON: {e}"
                    ))
                })?;
                parse::parse_jsonld_rule(
                    &rule_id,
                    &rule_json,
                    db.snapshot,
                    json_str.as_str(),
                    parse::RuleOrigin::Stored,
                )
            }
            FlakeValue::String(source) if flake.dt == sparql_dt => {
                parse::parse_sparql_rule(&rule_id, source, db.snapshot, parse::RuleOrigin::Stored)
            }
            _ => Err(QueryError::InvalidQuery(format!(
                "stored datalog rule <{label}> has datatype {}; expected an @json rule \
                 body ({{\"@type\": \"@json\", \"@value\": {{\"where\": …, \"insert\": …}}}}) or an \
                 f:sparql-typed CONSTRUCT query. Retract the f:rule value or re-store it in \
                 one of those shapes",
                rule_iri(db.snapshot, &flake.dt)
            ))),
        };
        match parsed {
            Ok(rule) => rule_set.add_rule(rule)?,
            Err(e) => {
                return Err(QueryError::InvalidQuery(format!(
                    "stored datalog rule <{label}> is invalid and was not applied: {e}"
                )));
            }
        }
    }

    Ok(rule_set)
}
