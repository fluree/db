//! User-defined datalog rules: parsing, validation and fixpoint execution.
//!
//! A rule is a **body** — an ordinary query `WHERE` clause, parsed and lowered
//! by the same JSON-LD / SPARQL parsers every query goes through — and one or
//! more **heads**, the triples to derive for each solution of the body. Rule
//! bodies are executed by the planned query executor, so everything the
//! executor supports is available to a rule: edge annotations (`@annotation`,
//! `{| |}`, `~ ?r`), claim-first patterns (`@reifies`, `rdf:reifies`), full
//! FILTER expressions, BIND, VALUES, UNION, property paths and subqueries.
//! The only constructs a rule body may not use are the non-monotonic ones
//! (OPTIONAL, MINUS, NOT EXISTS, aggregates, SERVICE), which a fixpoint cannot
//! evaluate soundly; those are rejected with an error naming the construct.
//!
//! Rules are stored as `f:rule` values (JSON with `where`/`insert`, or an
//! `f:sparql`-typed `CONSTRUCT … WHERE …` string) or supplied per query in the
//! `rules` array. Both sources share the parsers and the executor.
//!
//! Design record: `docs/design/rules-engine.md`.

mod execute;
mod parse;
mod validate;

pub use execute::{
    execute_datalog_rules, execute_datalog_rules_with_query_rules, load_rule_set, run_fixpoint,
    RuleExecutionEnv,
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
    /// Predicates the body reads (constant predicates of its triple patterns).
    pub depends_on: Vec<Sid>,
    /// Predicates the heads write.
    pub generates: Vec<Sid>,
    /// The rule's source text (JSON or SPARQL). Hashed into the reasoning
    /// cache key so a changed rule never serves a stale materialization.
    pub source: Arc<str>,
}

/// The rule set for one materialization, in execution order.
///
/// Rules with fewer predicate dependencies run first; the fixpoint re-runs
/// every rule each round until nothing new derives, so the order affects only
/// how quickly it converges, never the result.
#[derive(Debug, Default)]
pub struct DatalogRuleSet {
    rules: Vec<DatalogRule>,
}

impl DatalogRuleSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a rule, replacing any earlier rule with the same id.
    pub fn add_rule(&mut self, rule: DatalogRule) {
        self.rules.retain(|r| r.id != rule.id);
        self.rules.push(rule);
        self.rules.sort_by_key(|r| r.depends_on.len());
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
                parse::parse_jsonld_rule(&rule_id, &rule_json, db.snapshot, json_str.as_str())
            }
            FlakeValue::String(source) if flake.dt == sparql_dt => {
                parse::parse_sparql_rule(&rule_id, source, db.snapshot)
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
            Ok(rule) => rule_set.add_rule(rule),
            Err(e) => {
                return Err(QueryError::InvalidQuery(format!(
                    "stored datalog rule <{label}> is invalid and was not applied: {e}"
                )));
            }
        }
    }

    Ok(rule_set)
}
