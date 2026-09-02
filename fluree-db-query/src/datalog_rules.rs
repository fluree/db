//! Datalog rule extraction and parsing
//!
//! This module provides functionality for extracting user-defined datalog rules
//! from a Fluree database and parsing them into executable form.
//!
//! Rules are stored with the `f:rule` predicate (`https://ns.flur.ee/db#rule`)
//! and have a JSON format:
//!
//! ```json
//! {
//!   "@context": {"ex": "http://example.org/"},
//!   "where": {"@id": "?person", "ex:parent": {"ex:parent": "?grandparent"}},
//!   "insert": {"@id": "?person", "ex:grandparent": "?grandparent"}
//! }
//! ```

use crate::reasoning::ReasoningOverlay;
use fluree_db_core::comparator::IndexType;
use fluree_db_core::flake::Flake;
use fluree_db_core::overlay::OverlayProvider;
use fluree_db_core::range::{RangeMatch, RangeTest};
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{GraphDbRef, LedgerSnapshot, Sid};
use fluree_db_reasoner::{
    BindingValue, Bindings, CompareOp, DatalogRule, DatalogRuleSet, DerivedFactsBuilder,
    FrozenSameAs, ReasoningBudget, ReasoningDiagnostics, RuleFilter, RuleTerm, RuleTriplePattern,
    RuleValue,
};
use fluree_vocab::namespaces::FLUREE_DB;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use crate::error::{QueryError, Result};

/// Execution error for datalog rules
#[derive(Debug)]
pub struct RuleError(pub String);

impl From<RuleError> for QueryError {
    fn from(e: RuleError) -> Self {
        QueryError::InvalidQuery(e.0)
    }
}

/// Local name for the f:rule predicate
const RULE_LOCAL_NAME: &str = "rule";

/// Prefix the parser puts on variables it invents for anonymous and nested
/// nodes (`?__implicit_0`, `?__nested_1`, `?__anon_2`). A rule author never
/// writes one, so diagnostics must not quote them back
/// (see [`unsafe_insert_variables_message`]).
const IMPLICIT_VAR_PREFIX: &str = "?__";

/// Extract datalog rules from a database
///
/// Queries for all `f:rule` triples and parses the rule definitions.
/// Returns a `DatalogRuleSet` ready for execution in the reasoning loop.
pub async fn extract_datalog_rules(db: GraphDbRef<'_>) -> Result<DatalogRuleSet> {
    let mut rule_set = DatalogRuleSet::new();

    // Create the SID for f:rule predicate
    let rule_predicate_sid = Sid::new(FLUREE_DB, RULE_LOCAL_NAME);

    // Query PSOT index for all f:rule assertions
    let rule_flakes: Vec<Flake> = db
        .range(
            IndexType::Psot,
            RangeTest::Eq,
            RangeMatch {
                p: Some(rule_predicate_sid.clone()),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| QueryError::Internal(format!("Failed to query for rules: {e}")))?
        .into_iter()
        .filter(|f| f.op) // Only active assertions
        .collect();

    // Parse each rule. The literal's datatype selects the rule language:
    // `@json` (FlakeValue::Json) → JSON-LD `{"where":..., "insert":...}`;
    // `f:sparql`-typed string → SPARQL `CONSTRUCT ... WHERE ...`.
    let sparql_dt = Sid::new(FLUREE_DB, fluree_vocab::db::SPARQL);
    for flake in &rule_flakes {
        let rule_id = flake.s.clone();

        match &flake.o {
            FlakeValue::Json(json_str) => match serde_json::from_str::<JsonValue>(json_str) {
                Ok(rule_json) => match parse_rule_definition(&rule_id, &rule_json, db.snapshot) {
                    Ok(rule) => {
                        rule_set.add_rule(rule);
                    }
                    Err(e) => {
                        tracing::warn!(?rule_id, %e, "Failed to parse datalog rule definition");
                    }
                },
                Err(e) => {
                    tracing::warn!(?rule_id, %e, "Failed to parse datalog rule JSON");
                }
            },
            FlakeValue::String(source) if flake.dt == sparql_dt => {
                match parse_sparql_rule(&rule_id, source, db.snapshot) {
                    Ok(rule) => {
                        rule_set.add_rule(rule);
                    }
                    Err(e) => {
                        tracing::warn!(?rule_id, %e, "Failed to parse SPARQL datalog rule");
                    }
                }
            }
            _ => {
                tracing::warn!(
                    ?rule_id,
                    dt = ?flake.dt,
                    "f:rule literal has unrecognized datatype; expected @json or f:sparql — rule skipped"
                );
            }
        }
    }

    Ok(rule_set)
}

/// Parse a SPARQL `CONSTRUCT ... WHERE ...` rule into a `DatalogRule`.
///
/// SPARQL lowering is provided by a higher layer via
/// [`crate::lang_support::register_sparql_support`]; if it is absent the
/// rule fails to parse (and is skipped with a warning by callers) rather
/// than being silently misread.
fn parse_sparql_rule(
    rule_id: &Sid,
    source: &str,
    snapshot: &LedgerSnapshot,
) -> Result<DatalogRule> {
    let support = crate::lang_support::sparql_support().ok_or_else(|| {
        QueryError::Internal(
            "SPARQL rule support is not registered in this process; \
             cannot parse f:sparql datalog rule"
                .to_string(),
        )
    })?;

    let parts = (support.lower_rule)(source, snapshot).map_err(QueryError::InvalidQuery)?;

    let mut rule = DatalogRule::new(rule_id.clone(), parts.where_patterns, parts.insert_patterns);
    if !parts.filters.is_empty() {
        rule = rule.with_filters(parts.filters);
    }
    rule = rule.with_name(rule_id.name.to_string());
    Ok(rule)
}

/// Recognize a SPARQL typed-value object:
/// `{"@type": "f:sparql", "@value": "CONSTRUCT ..."}` (compact or full
/// `https://ns.flur.ee/db#sparql` type IRI). Returns the SPARQL source.
fn as_sparql_typed_value(json: &JsonValue) -> Option<&str> {
    let obj = json.as_object()?;
    let type_str = obj.get("@type")?.as_str()?;
    if type_str != "f:sparql" && type_str != fluree_vocab::fluree::SPARQL {
        return None;
    }
    obj.get("@value")?.as_str()
}

/// Parse a query-time rule from JSON-LD
///
/// Query-time rules can have three formats:
/// 1. Direct rule format: `{"where": ..., "insert": ...}`
/// 2. Stored rule format: `{"@id": "...", "f:rule": {"@value": {"where": ..., "insert": ...}}}`
/// 3. SPARQL typed value: `{"@type": "f:sparql", "@value": "CONSTRUCT ..."}`
///    (directly, or as the `f:rule` value of format 2)
fn parse_query_time_rule(
    json: &JsonValue,
    snapshot: &LedgerSnapshot,
    index: usize,
) -> Result<DatalogRule> {
    // Check if this is a stored rule format with f:rule wrapper
    if let Some(f_rule) = json
        .get("f:rule")
        .or_else(|| json.get(fluree_vocab::fluree::RULE))
    {
        // Get rule ID from @id, or generate one
        let rule_id = if let Some(id_str) = json.get("@id").and_then(|v| v.as_str()) {
            Sid::new(0, id_str)
        } else {
            Sid::new(0, format!("_:query_rule_{index}"))
        };

        // SPARQL typed value inside the wrapper
        if let Some(source) = as_sparql_typed_value(f_rule) {
            return parse_sparql_rule(&rule_id, source, snapshot);
        }

        // Extract the actual rule from the @value wrapper
        let rule_value = if let Some(value) = f_rule.get("@value") {
            value
        } else {
            f_rule
        };

        return parse_rule_definition(&rule_id, rule_value, snapshot);
    }

    // Generate a synthetic rule ID
    let rule_id = Sid::new(0, format!("_:query_rule_{index}"));

    // Direct SPARQL typed value
    if let Some(source) = as_sparql_typed_value(json) {
        return parse_sparql_rule(&rule_id, source, snapshot);
    }

    // Direct rule format
    parse_rule_definition(&rule_id, json, snapshot)
}

/// Parse a rule definition JSON into a DatalogRule
fn parse_rule_definition(
    rule_id: &Sid,
    json: &JsonValue,
    snapshot: &LedgerSnapshot,
) -> Result<DatalogRule> {
    // Extract context for IRI resolution
    let context = json.get("@context").cloned().unwrap_or(JsonValue::Null);

    // Parse where clause (patterns and filters)
    let where_json = json
        .get("where")
        .ok_or_else(|| QueryError::InvalidQuery("Rule missing 'where' clause".to_string()))?;
    let (where_patterns, filters) = parse_where_clause(where_json, &context, snapshot)?;

    // Parse insert clause
    let insert_json = json
        .get("insert")
        .ok_or_else(|| QueryError::InvalidQuery("Rule missing 'insert' clause".to_string()))?;
    let insert_patterns = parse_insert_patterns(insert_json, &context, snapshot)?;

    // Range restriction (#1560), enforced PER insert pattern to preserve the
    // runtime semantic: `execute_rule_with_bindings` instantiates each insert
    // pattern independently, so one head that can never instantiate must not
    // silence the others. A head referencing a variable nothing can bind is
    // skipped with a diagnostic naming the variable, and the remaining heads
    // keep deriving; only a rule with NO instantiable head — which can never
    // derive anything at all — is rejected outright. Left alone either case is
    // invisible: every binding row is dropped inside `instantiate_pattern`, so
    // the rule "runs" and derives nothing (or less than it was written to).
    let bound = authored_bound_vars(&where_patterns);
    let (insert_patterns, skipped): (Vec<RuleTriplePattern>, Vec<RuleTriplePattern>) =
        insert_patterns
            .into_iter()
            .partition(|p| insert_pattern_is_instantiable(p, &bound));
    if !skipped.is_empty() {
        if insert_patterns.is_empty() {
            let message = unsafe_insert_variables_message(&where_patterns, &skipped)
                .expect("skipped insert patterns reference unbound variables");
            return Err(QueryError::InvalidQuery(message));
        }
        let description = unbound_insert_description(&where_patterns, &skipped)
            .expect("skipped insert patterns reference unbound variables");
        tracing::warn!(
            rule_id = %rule_id,
            skipped = skipped.len(),
            kept = insert_patterns.len(),
            "datalog rule insert pattern skipped — {}. Every variable used in \
             `insert` must also appear in `where`; the rule's remaining insert \
             patterns still derive",
            description
        );
    }

    let mut rule = DatalogRule::new(rule_id.clone(), where_patterns, insert_patterns);

    // Add filters if any were parsed
    if !filters.is_empty() {
        rule = rule.with_filters(filters);
    }

    // Set rule name from the SID's local name
    rule = rule.with_name(rule_id.name.to_string());

    Ok(rule)
}

/// Parse the where clause into triple patterns and filters
///
/// The where clause can be:
/// - A single node pattern object: `{"@id": "?x", "ex:age": "?age"}`
/// - An array of patterns and filters: `[{"@id": "?x", "ex:age": "?age"}, ["filter", "(>= ?age 62)"]]`
fn parse_where_clause(
    json: &JsonValue,
    context: &JsonValue,
    snapshot: &LedgerSnapshot,
) -> Result<(Vec<RuleTriplePattern>, Vec<RuleFilter>)> {
    let mut patterns = Vec::new();
    let mut filters = Vec::new();

    match json {
        JsonValue::Object(map) => {
            // Single node pattern
            parse_node_pattern(map, context, snapshot, &mut patterns)?;
        }
        JsonValue::Array(arr) => {
            // Array of patterns and/or filters
            for item in arr {
                match item {
                    JsonValue::Object(map) => {
                        parse_node_pattern(map, context, snapshot, &mut patterns)?;
                    }
                    JsonValue::Array(filter_arr) if is_filter_expression(filter_arr) => {
                        // Filter expression like ["filter", "(>= ?age 62)"]
                        filters.push(parse_filter_expression(filter_arr, context, snapshot)?);
                    }
                    // A near-miss on the filter keyword — `["FILTER", ...]` —
                    // is rejected outright. It is unambiguously a filter the
                    // author meant to apply, and dropping it silently is the
                    // #1556 failure mode by another route: the filter
                    // disappears, `rule.filters` is empty, and the rule derives
                    // everything it was written to restrict.
                    _ if looks_like_misspelled_filter(item) => {
                        return Err(QueryError::InvalidQuery(format!(
                            "Unrecognized element in rule where clause: {item}. \
                             The filter keyword is lowercase `filter` — this element \
                             would otherwise be dropped, leaving the rule to derive \
                             the facts the filter was written to exclude."
                        )));
                    }
                    // Anything else stays tolerated, as it has been. Whether an
                    // unknown where-clause element should be an outright error
                    // is a language decision beyond this fix, but it should not
                    // vanish without a trace.
                    _ => {
                        tracing::warn!(
                            element = %item,
                            "unrecognized element in datalog rule where clause — ignored; \
                             expected a node pattern object or [\"filter\", \"(op ?var value)\"]"
                        );
                    }
                }
            }
        }
        _ => {
            return Err(QueryError::InvalidQuery(
                "Invalid where clause format".to_string(),
            ));
        }
    }

    Ok((patterns, filters))
}

/// Variables an `insert` pattern references that no `where` pattern can bind.
///
/// This is datalog's range-restriction (safety) condition: every variable in
/// the head must appear in the body. A rule that breaks it can never derive
/// anything — `instantiate_pattern` returns `None` for each row — which is the
/// silent dead end reported in #1560.
///
/// Parser-generated names on the where side are deliberately NOT treated as
/// binding. They are numbered off each clause's own `patterns.len()`, so a
/// where-side `?__implicit_0` and an unrelated insert-side `?__implicit_0`
/// would otherwise collide and suppress the diagnostic for exactly the
/// anonymous-node case it exists to report. An author cannot write one of
/// these names, so an insert pattern can never legitimately be referring to
/// the where clause's copy.
fn unbound_insert_variables(
    where_patterns: &[RuleTriplePattern],
    insert_patterns: &[RuleTriplePattern],
) -> Vec<Arc<str>> {
    let bound = authored_bound_vars(where_patterns);

    let mut unbound: Vec<Arc<str>> = Vec::new();
    for pattern in insert_patterns {
        for term in [&pattern.subject, &pattern.predicate, &pattern.object] {
            if let RuleTerm::Var(v) = term {
                if !bound.contains(v) && !unbound.contains(v) {
                    unbound.push(v.clone());
                }
            }
        }
    }
    unbound
}

/// The author-written variables a where clause binds. Parser-generated names
/// are excluded — see [`unbound_insert_variables`] for why they must never be
/// treated as binding.
fn authored_bound_vars(where_patterns: &[RuleTriplePattern]) -> HashSet<Arc<str>> {
    let mut bound: HashSet<Arc<str>> = HashSet::new();
    for pattern in where_patterns {
        bind_pattern_vars(pattern, &mut bound);
    }
    bound.retain(|v| !v.starts_with(IMPLICIT_VAR_PREFIX));
    bound
}

/// Whether every variable `pattern` references is in `bound` — i.e. whether
/// `instantiate_pattern` can ever produce a flake from it.
fn insert_pattern_is_instantiable(pattern: &RuleTriplePattern, bound: &HashSet<Arc<str>>) -> bool {
    [&pattern.subject, &pattern.predicate, &pattern.object]
        .into_iter()
        .all(|term| match term {
            RuleTerm::Var(v) => bound.contains(v),
            _ => true,
        })
}

/// Human-readable description of why `insert_patterns` violate range
/// restriction, or `None` when they are all safe.
///
/// Names the offending variables, because "this rule derives nothing" without
/// them sends an author reading engine source (#1560). Auto-generated
/// variables get their own wording: the author never wrote `?__implicit_0`, so
/// naming it would be useless — what they need to know is that a node in the
/// insert clause has no `@id`.
fn unbound_insert_description(
    where_patterns: &[RuleTriplePattern],
    insert_patterns: &[RuleTriplePattern],
) -> Option<String> {
    let unbound = unbound_insert_variables(where_patterns, insert_patterns);
    if unbound.is_empty() {
        return None;
    }

    let (generated, authored): (Vec<&Arc<str>>, Vec<&Arc<str>>) = unbound
        .iter()
        .partition(|v| v.starts_with(IMPLICIT_VAR_PREFIX));

    let mut parts: Vec<String> = Vec::new();
    if !authored.is_empty() {
        let names: Vec<&str> = authored.iter().map(|v| &***v).collect();
        parts.push(format!(
            "insert pattern references {} that the where clause never binds \
             (check for a typo against the where clause's variable names)",
            names.join(", ")
        ));
    }
    if !generated.is_empty() {
        parts.push(
            "insert pattern contains a node with no `@id` (or a nested anonymous \
             node), which has no subject to derive facts about"
                .to_string(),
        );
    }

    Some(parts.join("; "))
}

/// Actionable message for a rule that violates range restriction in EVERY
/// insert pattern — such a rule can never derive anything — or `None` when at
/// least the given patterns are safe.
fn unsafe_insert_variables_message(
    where_patterns: &[RuleTriplePattern],
    insert_patterns: &[RuleTriplePattern],
) -> Option<String> {
    unbound_insert_description(where_patterns, insert_patterns).map(|description| {
        format!(
            "Rule cannot derive anything: {description}. Every variable used in \
             `insert` must also appear in `where`."
        )
    })
}

/// Check if an array is a filter expression (starts with "filter")
fn is_filter_expression(arr: &[JsonValue]) -> bool {
    matches!(arr.first(), Some(JsonValue::String(s)) if s == "filter")
}

/// Whether a where-clause element is an array whose head is some case variant
/// of `filter` — i.e. the author meant a filter and the keyword did not match.
/// Used only to sharpen the rejection message.
fn looks_like_misspelled_filter(item: &JsonValue) -> bool {
    matches!(
        item.as_array().and_then(|a| a.first()).and_then(|v| v.as_str()),
        Some(s) if s.eq_ignore_ascii_case("filter")
    )
}

/// Parse a filter expression like ["filter", "(>= ?age 62)"]
///
/// `context` and `snapshot` are threaded in so an IRI operand can be expanded
/// and resolved the same way a node-pattern term is (#1556) — without them a
/// colon-bearing operand was silently demoted to a string and could never
/// equal the IRI it named.
///
/// Every rejection path is an error rather than a quiet `None`. A filter that
/// fails to parse and disappears leaves `rule.filters` empty, and
/// [`execute_rule_matching`] then skips filtering entirely — so a malformed
/// exclusion filter derives exactly the facts it was written to withhold,
/// which is the #1556 failure mode reached by a different route.
fn parse_filter_expression(
    arr: &[JsonValue],
    context: &JsonValue,
    snapshot: &LedgerSnapshot,
) -> Result<RuleFilter> {
    if arr.len() != 2 {
        return Err(QueryError::InvalidQuery(format!(
            "A rule filter must have exactly two elements, \
             [\"filter\", \"(op ?var value)\"], but got {} — refusing to drop the \
             filter, since a rule that silently loses its filter derives the facts \
             the filter was written to exclude.",
            arr.len()
        )));
    }

    let expr = match &arr[1] {
        JsonValue::String(s) => s.as_str(),
        other => {
            return Err(QueryError::InvalidQuery(format!(
                "A rule filter expression must be a string, but got {other} — \
                 refusing to drop the filter, since a rule that silently loses its \
                 filter derives the facts the filter was written to exclude."
            )));
        }
    };

    // Parse S-expression style filter: "(op arg1 arg2)"
    let expr = expr.trim();
    if !expr.starts_with('(') || !expr.ends_with(')') {
        return Err(QueryError::InvalidQuery(format!(
            "Invalid filter expression: {expr}"
        )));
    }

    let inner = &expr[1..expr.len() - 1];
    let parts = split_filter_tokens(inner)?;

    if parts.len() < 2 {
        return Err(QueryError::InvalidQuery(format!(
            "Filter expression needs at least operator and one argument: {expr}"
        )));
    }

    let op_str = parts[0];
    let op = match op_str {
        "=" => CompareOp::Equal,
        "!=" | "not=" => CompareOp::NotEqual,
        "<" => CompareOp::LessThan,
        "<=" => CompareOp::LessThanOrEqual,
        ">" => CompareOp::GreaterThan,
        ">=" => CompareOp::GreaterThanOrEqual,
        _ => {
            return Err(QueryError::InvalidQuery(format!(
                "Unknown filter operator: {op_str}"
            )));
        }
    };

    // Parse left and right terms
    let left = parse_filter_term(parts[1], context, snapshot)?;
    let right = if parts.len() > 2 {
        parse_filter_term(parts[2], context, snapshot)?
    } else {
        return Err(QueryError::InvalidQuery(format!(
            "Comparison filter needs two arguments: {expr}"
        )));
    };

    // An IRI has no ordering, so `<`/`<=`/`>`/`>=` against an IRI operand can
    // never be answered. Reject it here rather than letting it evaluate to
    // "no" per row — a silently-unsatisfiable ordering filter is the same
    // class of invisible failure as the fail-open equality of #1556.
    //
    // This parse-time guard is JSON-LD-only: SPARQL rules never reach this
    // function, and `sparql_lang.rs` lowers a constant IRI to
    // `RuleTerm::Value(RuleValue::Ref(_))` rather than `RuleTerm::Sid(_)`.
    // Both paths still fail closed — `compare_values` returns `Error` for IRI
    // ordering at run time — so the difference is only in the diagnostic: a
    // JSON-LD author gets a parse error naming the operator, a SPARQL author
    // gets a per-rule warn from `execute_rule_matching`.
    if !matches!(op, CompareOp::Equal | CompareOp::NotEqual)
        && (matches!(left, RuleTerm::Sid(_)) || matches!(right, RuleTerm::Sid(_)))
    {
        return Err(QueryError::InvalidQuery(format!(
            "Filter operator `{op_str}` cannot order IRIs: {expr}. \
             Only `=` and `!=` are defined for IRI operands."
        )));
    }

    Ok(RuleFilter::Compare { op, left, right })
}

/// Split a filter's inner S-expression into tokens, keeping a quoted operand
/// whole.
///
/// Plain `split_whitespace` tore `"John Smith"` into `"John` and `Smith"`, so
/// the quoting escape hatch that [`parse_filter_term`]'s own error message
/// recommends did not survive a value containing a space — the operand became
/// the literal string `"John`, leading quote included. Quotes are kept in the
/// token so `quoted_literal` can still recognise and strip them.
///
/// An unterminated quote is an error rather than a silently truncated operand.
fn split_filter_tokens(inner: &str) -> Result<Vec<&str>> {
    let bytes = inner.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;

    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }

        let start = i;
        if bytes[i] == b'"' || bytes[i] == b'\'' {
            let quote = bytes[i];
            i += 1;
            while i < bytes.len() && bytes[i] != quote {
                i += 1;
            }
            if i >= bytes.len() {
                return Err(QueryError::InvalidQuery(format!(
                    "Unterminated quote in filter expression: {inner}. \
                     A quoted operand must close its quote."
                )));
            }
            i += 1; // consume the closing quote
        } else {
            while i < bytes.len() && !bytes[i].is_ascii_whitespace() {
                i += 1;
            }
        }
        tokens.push(&inner[start..i]);
    }

    Ok(tokens)
}

/// The prefix of a token shaped like a compact IRI (`prefix:local`).
///
/// Deliberately narrow: the prefix must be a plausible NCName (leading letter
/// or `_`), which keeps colon-bearing *literals* — `12:30:00`,
/// `2026-08-25T09:30:00Z` — out of IRI classification, since no NCName starts
/// with a digit. Returns `None` for anything that is not CURIE-shaped.
/// (A non-CURIE unquoted token is then rejected by [`parse_filter_term`], with
/// quoting as the escape hatch — so a colon-bearing literal is never misread
/// as an IRI, and never silently read as a string either.)
fn curie_prefix(s: &str) -> Option<&str> {
    let colon = s.find(':')?;
    let prefix = &s[..colon];
    let mut chars = prefix.chars();
    let first = chars.next()?;
    if !(first.is_ascii_alphabetic() || first == '_') {
        return None;
    }
    if chars.any(|c| !(c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')) {
        return None;
    }
    Some(prefix)
}

/// Whether a filter token is wrapped in matching quotes — the explicit
/// "compare this as a string" escape hatch for a literal that would otherwise
/// read as a compact IRI.
fn quoted_literal(s: &str) -> Option<&str> {
    for q in ['"', '\''] {
        if s.len() >= 2 && s.starts_with(q) && s.ends_with(q) {
            return Some(&s[1..s.len() - 1]);
        }
    }
    None
}

/// Parse a single term in a filter expression (variable, literal, or IRI)
///
/// Classification order is load-bearing (#1556):
///
/// 1. A quoted token is always a string literal — the escape hatch for a
///    literal that happens to look like a compact IRI.
/// 2. `?x` is a variable.
/// 3. Numbers and booleans are literals.
/// 4. A [`curie_prefix`]-shaped token is an IRI (an absolute `http(s)` IRI is
///    itself CURIE-shaped — `http`/`https` parse as the prefix — so one
///    classification covers both forms, and [`expand_iri`] passes an absolute
///    IRI through unchanged): it is expanded through the rule's `@context` and
///    resolved to a Sid, exactly as [`parse_term`] resolves a node-pattern
///    term.
/// 5. Anything else — a bare unquoted token — is **rejected**.
///
/// Steps 4 and 5 both **fail closed**. When an IRI-shaped operand cannot be
/// resolved to a namespace registered on this ledger, the rule is rejected
/// instead of falling back to a string comparison. That fallback is precisely
/// the #1556 defect: `(!= ?prop ex:ssn)` compared the string `"ex:ssn"`
/// against the bare local name `"ssn"`, was therefore always true, and copied
/// the property the author wrote the rule to withhold.
///
/// A bare token is rejected for the same reason: before #1556 was fixed, the
/// bare form — `(!= ?prop ssn)` — was the only operand shape that ever matched
/// a bound IRI, so it is exactly what a workaround rule looks like. Silently
/// reading it as a string now would fail invisibly in BOTH directions: `=`
/// derives nothing (a string never equals an IRI) and `!=` keeps every row and
/// derives the very fact the filter was written to exclude — and no run-time
/// gate can see the `!=` case, because every row surviving looks like success.
/// Rejection covers both directions symmetrically, at zero per-row cost. A
/// string comparison is still one keystroke away (quote the operand), and a
/// rule that refuses to run is recoverable; a rule that silently derives an
/// excluded fact is not.
fn parse_filter_term(s: &str, context: &JsonValue, snapshot: &LedgerSnapshot) -> Result<RuleTerm> {
    if let Some(inner) = quoted_literal(s) {
        return Ok(RuleTerm::Value(RuleValue::String(inner.to_string())));
    }
    if s.starts_with('?') {
        return Ok(RuleTerm::var(s));
    }
    if let Ok(n) = s.parse::<i64>() {
        return Ok(RuleTerm::Value(RuleValue::Long(n)));
    }
    if let Ok(f) = s.parse::<f64>() {
        return Ok(RuleTerm::Value(RuleValue::Double(f)));
    }
    if s == "true" {
        return Ok(RuleTerm::Value(RuleValue::Boolean(true)));
    }
    if s == "false" {
        return Ok(RuleTerm::Value(RuleValue::Boolean(false)));
    }

    if curie_prefix(s).is_some() {
        let expanded = expand_iri(s, context)?;
        // `encode_iri_strict` (unlike `encode_iri`) refuses to mint a Sid in
        // the EMPTY namespace for an unrecognized IRI, which is what makes the
        // failure loud instead of producing a Sid that can never match. Same
        // rule the SPARQL rule lowerer applies to an unresolved IRI.
        return match snapshot.encode_iri_strict(&expanded) {
            Some(sid) => Ok(RuleTerm::Sid(sid)),
            None => Err(QueryError::InvalidQuery(format!(
                "Filter operand `{s}` names an IRI that is not registered on this \
                 ledger{}. Define the prefix in the rule's @context, use an absolute \
                 IRI, or quote the operand (\"{s}\") to compare it as a string. \
                 Refusing to compare it as a string: a filter whose IRI operand can \
                 never match makes `!=` always true, so an exclusion rule derives the \
                 fact it was written to exclude.",
                if expanded == s {
                    String::new()
                } else {
                    format!(" (expanded to `{expanded}`)")
                }
            ))),
        };
    }

    Err(QueryError::InvalidQuery(format!(
        "Filter operand `{s}` is a bare unquoted token. Quote it (\"{s}\") to \
         compare it as a string, or write a prefixed or absolute IRI (ex:{s}) \
         to compare it as an IRI. Refusing to guess: read as a string the bare \
         form fails invisibly against an IRI-bound variable — `=` derives \
         nothing, and `!=` keeps every row, so an exclusion rule derives the \
         fact it was written to exclude."
    )))
}

/// Parse a node-map pattern into triple patterns
fn parse_node_pattern(
    map: &serde_json::Map<String, JsonValue>,
    context: &JsonValue,
    snapshot: &LedgerSnapshot,
    patterns: &mut Vec<RuleTriplePattern>,
) -> Result<()> {
    // Get subject (@id or generate implicit variable)
    let subject = if let Some(id_val) = map.get("@id") {
        parse_term(id_val, context, snapshot)?
    } else {
        // Generate unique implicit variable for anonymous node
        // Use patterns.len() to ensure uniqueness across multiple node patterns
        let var_name = format!("{IMPLICIT_VAR_PREFIX}implicit_{}", patterns.len());
        RuleTerm::var(&var_name)
    };

    // Process each predicate-object pair
    for (key, value) in map {
        // Skip JSON-LD keywords
        if key == "@id" || key == "@context" || key == "@type" {
            if key == "@type" {
                // Handle @type as rdf:type
                let type_pred = resolve_iri(fluree_vocab::rdf::TYPE, snapshot)?;
                let type_obj = parse_term(value, context, snapshot)?;
                patterns.push(RuleTriplePattern {
                    subject: subject.clone(),
                    predicate: RuleTerm::Sid(type_pred),
                    object: type_obj,
                });
            }
            continue;
        }

        let predicate = parse_predicate_term(key, context, snapshot)?;

        // Parse object(s)
        match value {
            JsonValue::Array(arr) => {
                for item in arr {
                    let obj = parse_object_value(item, context, snapshot, patterns)?;
                    patterns.push(RuleTriplePattern {
                        subject: subject.clone(),
                        predicate: predicate.clone(),
                        object: obj,
                    });
                }
            }
            JsonValue::Object(nested) => {
                // Nested node pattern - create intermediate variable and recurse
                let nested_subject = if let Some(nested_id) = nested.get("@id") {
                    parse_term(nested_id, context, snapshot)?
                } else {
                    // Generate intermediate variable
                    let var_name = format!("{IMPLICIT_VAR_PREFIX}nested_{}", patterns.len());
                    RuleTerm::var(&var_name)
                };

                // Add pattern linking parent to nested subject
                patterns.push(RuleTriplePattern {
                    subject: subject.clone(),
                    predicate: predicate.clone(),
                    object: nested_subject.clone(),
                });

                // If the nested object has properties beyond @id, recursively parse
                if nested.len() > 1 || !nested.contains_key("@id") {
                    // Create a new map with the nested subject as @id
                    let mut nested_with_id = nested.clone();
                    if let RuleTerm::Var(var) = &nested_subject {
                        nested_with_id
                            .insert("@id".to_string(), JsonValue::String(var.to_string()));
                    }
                    parse_node_pattern(&nested_with_id, context, snapshot, patterns)?;
                }
            }
            _ => {
                let obj = parse_term(value, context, snapshot)?;
                patterns.push(RuleTriplePattern {
                    subject: subject.clone(),
                    predicate: predicate.clone(),
                    object: obj,
                });
            }
        }
    }

    Ok(())
}

/// Parse a node-map key into a predicate term: a `?`-prefixed key is a
/// variable; anything else expands and resolves as an IRI.
fn parse_predicate_term(
    key: &str,
    context: &JsonValue,
    snapshot: &LedgerSnapshot,
) -> Result<RuleTerm> {
    if key.starts_with('?') {
        Ok(RuleTerm::var(key))
    } else {
        let iri = expand_iri(key, context)?;
        Ok(RuleTerm::Sid(resolve_iri(&iri, snapshot)?))
    }
}

/// Parse an object value, handling nested structures
fn parse_object_value(
    value: &JsonValue,
    context: &JsonValue,
    snapshot: &LedgerSnapshot,
    patterns: &mut Vec<RuleTriplePattern>,
) -> Result<RuleTerm> {
    match value {
        JsonValue::Object(nested) if nested.contains_key("@id") => {
            // Reference to another node
            parse_term(nested.get("@id").unwrap(), context, snapshot)
        }
        JsonValue::Object(nested) => {
            // Nested anonymous node - generate variable and recurse
            let var_name = format!("{IMPLICIT_VAR_PREFIX}anon_{}", patterns.len());
            let nested_subject = RuleTerm::var(&var_name);

            // Create a map with @id for recursive parsing
            let mut nested_with_id = nested.clone();
            nested_with_id.insert("@id".to_string(), JsonValue::String(var_name.clone()));
            parse_node_pattern(&nested_with_id, context, snapshot, patterns)?;

            Ok(nested_subject)
        }
        _ => parse_term(value, context, snapshot),
    }
}

/// Parse a JSON value into a RuleTerm
fn parse_term(
    value: &JsonValue,
    context: &JsonValue,
    snapshot: &LedgerSnapshot,
) -> Result<RuleTerm> {
    match value {
        JsonValue::String(s) => {
            if s.starts_with('?') {
                // Variable
                Ok(RuleTerm::var(s))
            } else if s.contains(':') || s.starts_with("http://") || s.starts_with("https://") {
                // IRI or compact IRI (CURIE) - contains a colon or is a full URL
                let expanded = expand_iri(s, context)?;
                let sid = resolve_iri(&expanded, snapshot)?;
                Ok(RuleTerm::Sid(sid))
            } else {
                // Plain string literal (no colon, not a variable, not a URL)
                Ok(RuleTerm::Value(RuleValue::String(s.clone())))
            }
        }
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(RuleTerm::Value(RuleValue::Long(i)))
            } else if let Some(f) = n.as_f64() {
                Ok(RuleTerm::Value(RuleValue::Double(f)))
            } else {
                Err(QueryError::InvalidQuery(format!("Invalid number: {n}")))
            }
        }
        JsonValue::Bool(b) => Ok(RuleTerm::Value(RuleValue::Boolean(*b))),
        JsonValue::Object(obj) => {
            // Could be {"@id": "..."} reference or {"@value": ...} literal
            if let Some(id) = obj.get("@id") {
                parse_term(id, context, snapshot)
            } else if let Some(val) = obj.get("@value") {
                // Typed literal like {"@value": "senior", "@type": "xsd:string"}
                parse_term(val, context, snapshot)
            } else {
                Err(QueryError::InvalidQuery(
                    "Object without @id or @value in term position".to_string(),
                ))
            }
        }
        _ => Err(QueryError::InvalidQuery(format!(
            "Invalid term value: {value:?}"
        ))),
    }
}

/// Parse the insert clause into triple patterns (templates)
fn parse_insert_patterns(
    json: &JsonValue,
    context: &JsonValue,
    snapshot: &LedgerSnapshot,
) -> Result<Vec<RuleTriplePattern>> {
    // Insert patterns use the same format as where patterns (but we ignore any filters)
    let (patterns, _filters) = parse_where_clause(json, context, snapshot)?;
    Ok(patterns)
}

/// Expand a compact IRI using the context
fn expand_iri(compact: &str, context: &JsonValue) -> Result<String> {
    // Check if it's already a full IRI
    if compact.starts_with("http://") || compact.starts_with("https://") {
        return Ok(compact.to_string());
    }

    // Try to expand using context
    if let Some(colon_pos) = compact.find(':') {
        let prefix = &compact[..colon_pos];
        let local = &compact[colon_pos + 1..];

        if let JsonValue::Object(ctx) = context {
            if let Some(JsonValue::String(ns)) = ctx.get(prefix) {
                return Ok(format!("{ns}{local}"));
            }
        }
    }

    // Return as-is if no expansion possible
    Ok(compact.to_string())
}

/// Resolve an IRI to a SID
fn resolve_iri(iri: &str, snapshot: &LedgerSnapshot) -> Result<Sid> {
    // Use the database's IRI encoding
    snapshot
        .encode_iri(iri)
        .ok_or_else(|| QueryError::InvalidQuery(format!("Failed to encode IRI '{iri}'")))
}

// ============================================================================
// Pattern Matching Execution
// ============================================================================

/// Whether `term` grounds an index lookup: a constant, or a variable already
/// bound by an earlier pattern. Grounding the subject or predicate keeps a
/// pattern off the full-scan path in [`choose_index_and_match`].
fn term_is_grounded(term: &RuleTerm, bound: &HashSet<Arc<str>>) -> bool {
    match term {
        RuleTerm::Sid(_) | RuleTerm::Value(_) => true,
        RuleTerm::Var(v) => bound.contains(v),
    }
}

/// Selectivity score for matching `pattern` given the currently-bound
/// variables. A grounded subject or predicate (worth 2 each) avoids the
/// `(None, None)` full ledger scan; a grounded object (worth 1) constrains
/// the result further. Higher is cheaper to match.
fn pattern_selectivity(pattern: &RuleTriplePattern, bound: &HashSet<Arc<str>>) -> u32 {
    2 * term_is_grounded(&pattern.subject, bound) as u32
        + 2 * term_is_grounded(&pattern.predicate, bound) as u32
        + term_is_grounded(&pattern.object, bound) as u32
}

/// Record `pattern`'s variables as bound (they will be after it matches).
fn bind_pattern_vars(pattern: &RuleTriplePattern, bound: &mut HashSet<Arc<str>>) {
    for term in [&pattern.subject, &pattern.predicate, &pattern.object] {
        if let RuleTerm::Var(v) = term {
            bound.insert(v.clone());
        }
    }
}

/// Order `patterns` most-constrained-first via a greedy join order: at each
/// step take the pattern with the highest [`pattern_selectivity`] given the
/// variables already bound, then mark its variables bound. A where-clause is
/// a conjunction, so the join result is order-independent — this only lowers
/// matching cost, keeping an all-unbound pattern (a full ledger scan) from
/// leading when another pattern can ground its variables first. Ties keep
/// source order.
fn selective_pattern_order(patterns: &[RuleTriplePattern]) -> Vec<usize> {
    let mut bound: HashSet<Arc<str>> = HashSet::new();
    let mut remaining: Vec<usize> = (0..patterns.len()).collect();
    let mut order = Vec::with_capacity(patterns.len());

    while !remaining.is_empty() {
        let best = remaining
            .iter()
            .enumerate()
            .max_by_key(|(rank, &idx)| {
                // Higher selectivity wins; on ties the lower source index
                // (earlier `rank`) wins, so `max_by_key` compares Reverse.
                (
                    pattern_selectivity(&patterns[idx], &bound),
                    std::cmp::Reverse(*rank),
                )
            })
            .map(|(rank, _)| rank)
            .expect("remaining is non-empty");
        let idx = remaining.remove(best);
        bind_pattern_vars(&patterns[idx], &mut bound);
        order.push(idx);
    }

    order
}

/// Execute pattern matching for a datalog rule against the database
///
/// Finds all bindings that satisfy the rule's where patterns and filters, and returns them.
/// The bindings can then be used with `execute_rule_with_bindings` to generate flakes.
pub async fn execute_rule_matching(
    rule: &DatalogRule,
    db: GraphDbRef<'_>,
) -> Result<Vec<Bindings>> {
    if rule.where_patterns.is_empty() {
        return Ok(Vec::new());
    }

    // Match patterns most-constrained-first so a pattern whose subject and
    // predicate are both unbound variables — a full ledger scan (the
    // `(None, None)` arm of `choose_index_and_match`), now reachable via
    // property-position variables — does not lead when another pattern can
    // ground its variables first.
    let order = selective_pattern_order(&rule.where_patterns);
    let mut patterns = order.iter().map(|&i| &rule.where_patterns[i]);

    // Start with the first pattern to get initial bindings
    let first = patterns.next().expect("where_patterns is non-empty");
    let mut binding_rows = match_pattern(first, db, &[]).await?;

    // Join with subsequent patterns
    for pattern in patterns {
        if binding_rows.is_empty() {
            break;
        }

        let mut new_bindings = Vec::new();
        for existing_bindings in &binding_rows {
            let extended =
                match_pattern(pattern, db, std::slice::from_ref(existing_bindings)).await?;
            new_bindings.extend(extended);
        }
        binding_rows = new_bindings;
    }

    // Apply filters to eliminate non-matching bindings.
    //
    // A row survives only on an outright `True`. `Error` — an unresolvable
    // operand or a pair of literals with no defined comparison — drops the row,
    // never keeps it, so filter *evaluation* cannot fail open (#1556).
    if !rule.filters.is_empty() {
        let rows_before = binding_rows.len();
        let mut diagnostics = FilterDiagnostics::default();
        binding_rows.retain(|bindings| {
            let mut row_errored = false;
            let keep = rule.filters.iter().all(|filter| {
                match evaluate_filter(filter, bindings, &mut diagnostics) {
                    FilterOutcome::True => true,
                    FilterOutcome::False => false,
                    FilterOutcome::Error => {
                        row_errored = true;
                        false
                    }
                }
            });
            diagnostics.errored_rows += row_errored as usize;
            keep
        });

        if diagnostics.errored_rows > 0 {
            // Excluding the row is the safe answer, but silently excluding it
            // is how a mis-typed filter looks exactly like a rule that
            // legitimately matched nothing.
            tracing::warn!(
                rule = rule.name.as_deref().unwrap_or("<unnamed>"),
                rule_id = %rule.id,
                errored_rows = diagnostics.errored_rows,
                "datalog rule filter could not compare its operands on some binding \
                 rows; those rows were excluded — check the filter's operand types"
            );
        } else if rows_before > 0 && binding_rows.is_empty() && diagnostics.iri_vs_literal > 0 {
            // Every row was eliminated, and at least one comparison put an IRI
            // against a literal. The bare-local-name workaround form —
            // `(= ?p knows)` — is rejected at parse time now, so what reaches
            // here is a QUOTED operand spelling out an IRI — `(= ?p "ex:knows")`
            // — or a variable that binds a string where its partner binds an
            // IRI. Either way the rule correctly derives nothing, and without
            // this it would do so in complete silence: RDFterm-equal makes
            // IRI-vs-literal a clean `False`, not an `Error`, so the branch
            // above cannot see it. (The `!=` twin of this mistake keeps every
            // row instead of dropping them all, which no run-time gate can
            // distinguish from success — that direction is exactly why the
            // bare form is rejected at parse.)
            tracing::warn!(
                rule = rule.name.as_deref().unwrap_or("<unnamed>"),
                rule_id = %rule.id,
                rows_before,
                "datalog rule filter compared an IRI against a literal and excluded \
                 every binding row; a quoted operand is a string literal and never \
                 equals an IRI — if the operand was meant to name an IRI, write it \
                 unquoted as a prefixed or absolute IRI (`ex:knows`, not \
                 `\"ex:knows\"`)"
            );
        }
    }

    Ok(binding_rows)
}

/// Per-rule tallies gathered while filtering, used only for diagnostics.
#[derive(Default)]
struct FilterDiagnostics {
    /// Rows dropped because some filter could not be evaluated at all.
    errored_rows: usize,
    /// Comparisons that put an IRI against a literal. Well-defined (and false)
    /// per RDFterm-equal, so it never drops a row on its own — but when it
    /// coincides with "every row eliminated" it is the fingerprint of a filter
    /// whose operand spells an IRI as a string (a quoted `"ex:knows"`, or a
    /// string-bound variable compared against an IRI-bound one).
    iri_vs_literal: usize,
}

/// Outcome of evaluating a rule filter against one binding row.
///
/// Three-valued on purpose, following SPARQL's treatment of a type error.
/// Collapsing `Error` into `False` would be wrong under `Not`: `!(error)`
/// would become `true` and re-admit a row the filter could not actually judge.
/// Only [`FilterOutcome::True`] keeps a row, so an incomparable filter can
/// exclude but never include.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterOutcome {
    True,
    False,
    Error,
}

/// Evaluate a filter expression against a set of bindings
///
/// `diagnostics` accumulates the tallies that make a silently-empty filter
/// explainable afterwards; evaluation itself does not consult them.
fn evaluate_filter(
    filter: &RuleFilter,
    bindings: &Bindings,
    diagnostics: &mut FilterDiagnostics,
) -> FilterOutcome {
    match filter {
        RuleFilter::Compare { op, left, right } => {
            let left_val = resolve_filter_term(left, bindings);
            let right_val = resolve_filter_term(right, bindings);

            match (left_val, right_val) {
                (Some(l), Some(r)) => {
                    if matches!(l, FlakeValue::Ref(_)) != matches!(r, FlakeValue::Ref(_)) {
                        diagnostics.iri_vs_literal += 1;
                    }
                    compare_values(&l, &r, *op)
                }
                // An unbound operand cannot be judged either way.
                _ => FilterOutcome::Error,
            }
        }
        // `false && error` is `false`; `true && error` is `error`.
        RuleFilter::And(filters) => {
            let mut saw_error = false;
            for f in filters {
                match evaluate_filter(f, bindings, diagnostics) {
                    FilterOutcome::False => return FilterOutcome::False,
                    FilterOutcome::Error => saw_error = true,
                    FilterOutcome::True => {}
                }
            }
            if saw_error {
                FilterOutcome::Error
            } else {
                FilterOutcome::True
            }
        }
        // `true || error` is `true`; `false || error` is `error`.
        RuleFilter::Or(filters) => {
            let mut saw_error = false;
            for f in filters {
                match evaluate_filter(f, bindings, diagnostics) {
                    FilterOutcome::True => return FilterOutcome::True,
                    FilterOutcome::Error => saw_error = true,
                    FilterOutcome::False => {}
                }
            }
            if saw_error {
                FilterOutcome::Error
            } else {
                FilterOutcome::False
            }
        }
        RuleFilter::Not(inner) => match evaluate_filter(inner, bindings, diagnostics) {
            FilterOutcome::True => FilterOutcome::False,
            FilterOutcome::False => FilterOutcome::True,
            // Never `True`: negating something uncomparable must not admit the row.
            FilterOutcome::Error => FilterOutcome::Error,
        },
    }
}

/// Resolve a filter term to a comparable value
///
/// An IRI stays an IRI. Rendering a bound Sid as its bare local name — what
/// this did before #1556 — threw away the namespace, so `ex:knows` and
/// `foaf:knows` both resolved to `"knows"` and neither could ever equal the
/// operand `"ex:knows"` that the author actually wrote.
fn resolve_filter_term(term: &RuleTerm, bindings: &Bindings) -> Option<FlakeValue> {
    match term {
        RuleTerm::Var(name) => bindings.get(name.as_ref()).map(|bv| match bv {
            BindingValue::Long(n) => FlakeValue::Long(*n),
            BindingValue::Double(d) => FlakeValue::Double(*d),
            BindingValue::Decimal(d) => FlakeValue::Decimal(d.clone()),
            BindingValue::BigInt(n) => FlakeValue::BigInt(n.clone()),
            BindingValue::String(s) => FlakeValue::String(s.clone()),
            BindingValue::Boolean(b) => FlakeValue::Boolean(*b),
            BindingValue::Sid(sid) => FlakeValue::Ref(sid.clone()),
        }),
        RuleTerm::Value(val) => Some(match val {
            RuleValue::Long(n) => FlakeValue::Long(*n),
            RuleValue::Double(d) => FlakeValue::Double(*d),
            RuleValue::String(s) => FlakeValue::String(s.clone()),
            RuleValue::Boolean(b) => FlakeValue::Boolean(*b),
            RuleValue::Ref(sid) => FlakeValue::Ref(sid.clone()),
        }),
        RuleTerm::Sid(sid) => Some(FlakeValue::Ref(sid.clone())),
    }
}

/// Compare two filter values using the given operator.
///
/// Numeric, IRI (`Ref`), string and boolean pairs are comparable; every other
/// pairing is [`FilterOutcome::Error`], which excludes the row. Note what is
/// deliberately absent: a `Ref`-versus-`String` pairing does NOT stringify the
/// IRI to make the comparison "work". That coercion is what made `!=` against
/// an IRI always true (#1556).
fn compare_values(left: &FlakeValue, right: &FlakeValue, op: CompareOp) -> FilterOutcome {
    let outcome = |b: bool| {
        if b {
            FilterOutcome::True
        } else {
            FilterOutcome::False
        }
    };

    // Try numeric comparison first: exact across all numeric representations
    // (Long/Double/BigInt/Decimal), with BigDecimal promotion where an f64
    // cast would lose precision.
    let numeric_result = if left.is_numeric() && right.is_numeric() {
        left.numeric_cmp(right)
    } else {
        None
    };

    if let Some(cmp) = numeric_result {
        return outcome(match op {
            CompareOp::Equal => cmp == std::cmp::Ordering::Equal,
            CompareOp::NotEqual => cmp != std::cmp::Ordering::Equal,
            CompareOp::LessThan => cmp == std::cmp::Ordering::Less,
            CompareOp::LessThanOrEqual => cmp != std::cmp::Ordering::Greater,
            CompareOp::GreaterThan => cmp == std::cmp::Ordering::Greater,
            CompareOp::GreaterThanOrEqual => cmp != std::cmp::Ordering::Less,
        });
    }

    // Fall back to string comparison
    let (left_str, right_str) = match (left, right) {
        (FlakeValue::String(l), FlakeValue::String(r)) => (l.as_str(), r.as_str()),
        // IRI identity: Sid equality, namespace included. Ordering is
        // undefined for IRIs — the parser rejects an ordering operator against
        // a constant IRI, and a variable that turns out to be IRI-bound at run
        // time lands here.
        (FlakeValue::Ref(l), FlakeValue::Ref(r)) => {
            return match op {
                CompareOp::Equal => outcome(l == r),
                CompareOp::NotEqual => outcome(l != r),
                _ => FilterOutcome::Error,
            };
        }
        // RDFterm-equal (SPARQL 1.1 §17.4.1.7) is a type error only "if the
        // arguments are both literal but are not the same RDF term". An IRI
        // against a literal is not both-literal, so the comparison is
        // well-defined and simply false — they can never be the same RDF term.
        // Routing this pair to `Error` instead would drop the row, which is
        // safe but is silent under-derivation: a copy-properties rule filtering
        // on a value, `(!= ?val "Bob")`, would discard every IRI-valued
        // property. Must stay below the `(Ref, Ref)` arm so Sid identity wins
        // when both sides really are IRIs.
        (FlakeValue::Ref(_), _) | (_, FlakeValue::Ref(_)) => {
            return match op {
                CompareOp::Equal => outcome(false),
                CompareOp::NotEqual => outcome(true),
                // Ordering across an IRI and a literal remains undefined.
                _ => FilterOutcome::Error,
            };
        }
        (FlakeValue::Boolean(l), FlakeValue::Boolean(r)) => {
            // Boolean comparison
            return match op {
                CompareOp::Equal => outcome(l == r),
                CompareOp::NotEqual => outcome(l != r),
                _ => FilterOutcome::Error, // Other comparisons don't make sense for booleans
            };
        }
        // Both literals, not the same RDF term: a genuine type error per
        // §17.4.1.7 (e.g. a string against a number).
        _ => return FilterOutcome::Error,
    };

    let cmp = left_str.cmp(right_str);
    outcome(match op {
        CompareOp::Equal => cmp == std::cmp::Ordering::Equal,
        CompareOp::NotEqual => cmp != std::cmp::Ordering::Equal,
        CompareOp::LessThan => cmp == std::cmp::Ordering::Less,
        CompareOp::LessThanOrEqual => cmp != std::cmp::Ordering::Greater,
        CompareOp::GreaterThan => cmp == std::cmp::Ordering::Greater,
        CompareOp::GreaterThanOrEqual => cmp != std::cmp::Ordering::Less,
    })
}

/// Match a single triple pattern against the database
///
/// If `existing_bindings` is provided, uses those bindings to constrain the pattern.
/// Returns all binding rows that satisfy the pattern.
async fn match_pattern(
    pattern: &RuleTriplePattern,
    db: GraphDbRef<'_>,
    existing_bindings: &[Bindings],
) -> Result<Vec<Bindings>> {
    let mut results = Vec::new();

    // If we have existing bindings, extend each one
    if !existing_bindings.is_empty() {
        for bindings in existing_bindings {
            let extended = match_pattern_with_bindings(pattern, db, bindings).await?;
            results.extend(extended);
        }
    } else {
        // No existing bindings - match freely
        let empty_bindings = Bindings::new();
        let extended = match_pattern_with_bindings(pattern, db, &empty_bindings).await?;
        results.extend(extended);
    }

    Ok(results)
}

/// Match a single pattern with existing bindings, returning extended binding rows
async fn match_pattern_with_bindings(
    pattern: &RuleTriplePattern,
    db: GraphDbRef<'_>,
    bindings: &Bindings,
) -> Result<Vec<Bindings>> {
    // Resolve pattern terms using existing bindings. A subject/predicate
    // variable bound to a non-Sid value can never occupy that position in a
    // flake, so the pattern matches nothing for this binding row.
    let Some((subject_sid, subject_var)) =
        resolve_term_with_bindings(&pattern.subject, bindings).split()
    else {
        return Ok(Vec::new());
    };
    let Some((predicate_sid, predicate_var)) =
        resolve_term_with_bindings(&pattern.predicate, bindings).split()
    else {
        return Ok(Vec::new());
    };
    let (object_match, object_var) = resolve_object_term_with_bindings(&pattern.object, bindings)?;

    // Choose index based on what's bound
    let (index_type, range_match) = choose_index_and_match(
        subject_sid.as_ref(),
        predicate_sid.as_ref(),
        object_match.as_ref(),
    );

    // Query the index
    let flakes: Vec<Flake> = db
        .range(index_type, RangeTest::Eq, range_match)
        .await
        .map_err(|e| QueryError::Internal(format!("Pattern matching failed: {e}")))?
        .into_iter()
        .filter(|f| {
            if !f.op {
                return false;
            } // Only active assertions
              // Post-filter: range provider may return a superset; ensure
              // subject and predicate actually match the requested pattern.
            if let Some(ref s) = subject_sid {
                if &f.s != s {
                    return false;
                }
            }
            if let Some(ref p) = predicate_sid {
                if &f.p != p {
                    return false;
                }
            }
            true
        })
        .collect();

    // Build binding rows from results
    let mut results = Vec::new();
    for flake in flakes {
        // Filter by object if we have a specific value
        if let Some(ref obj_match) = object_match {
            if !flake_object_matches(&flake.o, obj_match) {
                continue;
            }
        }

        // Create extended bindings
        let mut new_bindings = bindings.clone();

        // Bind subject variable if present
        if let Some(ref var) = subject_var {
            let bound_value = BindingValue::Sid(flake.s.clone());
            if !try_bind_or_check(&mut new_bindings, var.clone(), bound_value)? {
                continue;
            }
        }

        // Bind predicate variable if present
        if let Some(ref var) = predicate_var {
            let bound_value = BindingValue::Sid(flake.p.clone());
            if !try_bind_or_check(&mut new_bindings, var.clone(), bound_value)? {
                continue;
            }
        }

        // Bind object variable if present
        if let Some(ref var) = object_var {
            let bound_value = flake_value_to_binding(&flake.o);
            if !try_bind_or_check(&mut new_bindings, var.clone(), bound_value)? {
                continue;
            }
        }

        results.push(new_bindings);
    }

    Ok(results)
}

/// Resolution of a subject- or predicate-position term against existing bindings
enum TermResolution {
    /// Constant, or a variable already bound to a Sid
    Bound(Sid),
    /// Unbound variable, to be bound from each matched flake
    Unbound(Arc<str>),
    /// A literal constant, or a variable bound to a non-Sid value: no flake
    /// can carry a literal in subject/predicate position, so the pattern
    /// matches nothing (an empty join, not an error)
    Incompatible,
}

impl TermResolution {
    /// Split into `(bound_sid, unbound_var)`, or `None` when incompatible
    fn split(self) -> Option<(Option<Sid>, Option<Arc<str>>)> {
        match self {
            TermResolution::Bound(sid) => Some((Some(sid), None)),
            TermResolution::Unbound(var) => Some((None, Some(var))),
            TermResolution::Incompatible => None,
        }
    }
}

/// Resolve a subject- or predicate-position term using existing bindings.
///
/// Total: a literal constant (`RuleTerm::Value`) — a plausible typo for an
/// unprefixed IRI like `{"@id": "Alice"}` — can no more occupy subject or
/// predicate position than a variable bound to a literal, so it resolves to
/// `Incompatible` (that pattern matches nothing) rather than erroring, which
/// would abort the whole fixpoint and drop every rule's derivations.
fn resolve_term_with_bindings(term: &RuleTerm, bindings: &Bindings) -> TermResolution {
    match term {
        RuleTerm::Sid(sid) => TermResolution::Bound(sid.clone()),
        RuleTerm::Var(var) => match bindings.get(var.as_ref()) {
            Some(BindingValue::Sid(sid)) => TermResolution::Bound(sid.clone()),
            Some(_) => TermResolution::Incompatible,
            None => TermResolution::Unbound(var.clone()),
        },
        RuleTerm::Value(_) => TermResolution::Incompatible,
    }
}

/// Resolve an object term using existing bindings
fn resolve_object_term_with_bindings(
    term: &RuleTerm,
    bindings: &Bindings,
) -> Result<(Option<ObjectMatch>, Option<Arc<str>>)> {
    match term {
        RuleTerm::Sid(sid) => Ok((Some(ObjectMatch::Ref(sid.clone())), None)),
        RuleTerm::Var(var) => {
            if let Some(binding) = bindings.get(var.as_ref()) {
                let obj_match = binding_to_object_match(binding);
                Ok((Some(obj_match), None))
            } else {
                Ok((None, Some(var.clone())))
            }
        }
        RuleTerm::Value(val) => {
            let obj_match = rule_value_to_object_match(val);
            Ok((Some(obj_match), None))
        }
    }
}

/// Object value for matching
#[derive(Clone)]
enum ObjectMatch {
    Ref(Sid),
    String(String),
    Long(i64),
    Double(f64),
    Decimal(Box<bigdecimal::BigDecimal>),
    BigInt(Box<num_bigint::BigInt>),
    Boolean(bool),
}

impl ObjectMatch {
    /// Numeric value for exact cross-representation comparison; None for
    /// non-numeric variants.
    fn as_numeric_flake(&self) -> Option<FlakeValue> {
        match self {
            ObjectMatch::Long(n) => Some(FlakeValue::Long(*n)),
            ObjectMatch::Double(d) => Some(FlakeValue::Double(*d)),
            ObjectMatch::Decimal(d) => Some(FlakeValue::Decimal(d.clone())),
            ObjectMatch::BigInt(n) => Some(FlakeValue::BigInt(n.clone())),
            _ => None,
        }
    }
}

fn binding_to_object_match(binding: &BindingValue) -> ObjectMatch {
    match binding {
        BindingValue::Sid(sid) => ObjectMatch::Ref(sid.clone()),
        BindingValue::String(s) => ObjectMatch::String(s.clone()),
        BindingValue::Long(n) => ObjectMatch::Long(*n),
        BindingValue::Double(d) => ObjectMatch::Double(*d),
        BindingValue::Decimal(d) => ObjectMatch::Decimal(d.clone()),
        BindingValue::BigInt(n) => ObjectMatch::BigInt(n.clone()),
        BindingValue::Boolean(b) => ObjectMatch::Boolean(*b),
    }
}

fn rule_value_to_object_match(val: &RuleValue) -> ObjectMatch {
    match val {
        RuleValue::String(s) => ObjectMatch::String(s.clone()),
        RuleValue::Long(n) => ObjectMatch::Long(*n),
        RuleValue::Double(d) => ObjectMatch::Double(*d),
        RuleValue::Boolean(b) => ObjectMatch::Boolean(*b),
        RuleValue::Ref(sid) => ObjectMatch::Ref(sid.clone()),
    }
}

fn flake_object_matches(flake_obj: &FlakeValue, expected: &ObjectMatch) -> bool {
    // Numeric pairs compare by exact value across representations
    // (Long/Double/BigInt/Decimal) — epsilon comparison conflates distinct
    // values and a per-variant match would silently never match decimals.
    if flake_obj.is_numeric() {
        if let Some(expected_num) = expected.as_numeric_flake() {
            return flake_obj.numeric_cmp(&expected_num) == Some(std::cmp::Ordering::Equal);
        }
    }
    match (flake_obj, expected) {
        (FlakeValue::Ref(a), ObjectMatch::Ref(b)) => a == b,
        (FlakeValue::String(a), ObjectMatch::String(b)) => a == b,
        (FlakeValue::Boolean(a), ObjectMatch::Boolean(b)) => a == b,
        _ => false,
    }
}

fn flake_value_to_binding(val: &FlakeValue) -> BindingValue {
    match val {
        FlakeValue::Ref(sid) => BindingValue::Sid(sid.clone()),
        FlakeValue::String(s) => BindingValue::String(s.clone()),
        FlakeValue::Long(n) => BindingValue::Long(*n),
        FlakeValue::Double(d) => BindingValue::Double(*d),
        FlakeValue::Boolean(b) => BindingValue::Boolean(*b),
        FlakeValue::Json(j) => BindingValue::String(j.clone()),
        FlakeValue::BigInt(b) => BindingValue::BigInt(b.clone()),
        FlakeValue::Decimal(b) => BindingValue::Decimal(b.clone()),
        FlakeValue::DateTime(dt) => BindingValue::String(dt.to_string()),
        FlakeValue::Date(d) => BindingValue::String(d.to_string()),
        FlakeValue::Time(t) => BindingValue::String(t.to_string()),
        FlakeValue::Vector(v) => BindingValue::String(format!("{v:?}")),
        FlakeValue::Null => BindingValue::String("null".to_string()),
        FlakeValue::GYear(v) => BindingValue::String(v.to_string()),
        FlakeValue::GYearMonth(v) => BindingValue::String(v.to_string()),
        FlakeValue::GMonth(v) => BindingValue::String(v.to_string()),
        FlakeValue::GDay(v) => BindingValue::String(v.to_string()),
        FlakeValue::GMonthDay(v) => BindingValue::String(v.to_string()),
        FlakeValue::YearMonthDuration(v) => BindingValue::String(v.to_string()),
        FlakeValue::DayTimeDuration(v) => BindingValue::String(v.to_string()),
        FlakeValue::Duration(v) => BindingValue::String(v.to_string()),
        FlakeValue::GeoPoint(v) => BindingValue::String(v.to_string()),
    }
}

/// Choose the best index based on which components are bound
fn choose_index_and_match(
    subject: Option<&Sid>,
    predicate: Option<&Sid>,
    _object: Option<&ObjectMatch>,
) -> (IndexType, RangeMatch) {
    match (subject, predicate) {
        (Some(s), Some(p)) => {
            // Both bound: use SPOT for most selective access
            (
                IndexType::Spot,
                RangeMatch {
                    s: Some(s.clone()),
                    p: Some(p.clone()),
                    ..Default::default()
                },
            )
        }
        (Some(s), None) => {
            // Subject bound: use SPOT
            (
                IndexType::Spot,
                RangeMatch {
                    s: Some(s.clone()),
                    ..Default::default()
                },
            )
        }
        (None, Some(p)) => {
            // Predicate bound: use PSOT
            (
                IndexType::Psot,
                RangeMatch {
                    p: Some(p.clone()),
                    ..Default::default()
                },
            )
        }
        (None, None) => {
            // Nothing bound: scan all (expensive!)
            // In practice, at least one pattern should have something bound
            (IndexType::Spot, RangeMatch::default())
        }
    }
}

/// Try to bind a variable, or check if it matches an existing binding
fn try_bind_or_check(bindings: &mut Bindings, var: Arc<str>, value: BindingValue) -> Result<bool> {
    if let Some(existing) = bindings.get(var.as_ref()) {
        // Variable already bound - check if values match
        Ok(bindings_equal(existing, &value))
    } else {
        // Bind the variable
        bindings.insert(var, value);
        Ok(true)
    }
}

fn bindings_equal(a: &BindingValue, b: &BindingValue) -> bool {
    match (a, b) {
        (BindingValue::Sid(a), BindingValue::Sid(b)) => a == b,
        (BindingValue::String(a), BindingValue::String(b)) => a == b,
        (BindingValue::Long(a), BindingValue::Long(b)) => a == b,
        (BindingValue::Boolean(a), BindingValue::Boolean(b)) => a == b,
        // Numeric pairs (incl. cross-representation Decimal/BigInt) compare
        // by exact value — epsilon comparison conflates distinct values.
        (
            BindingValue::Long(_)
            | BindingValue::Double(_)
            | BindingValue::Decimal(_)
            | BindingValue::BigInt(_),
            BindingValue::Long(_)
            | BindingValue::Double(_)
            | BindingValue::Decimal(_)
            | BindingValue::BigInt(_),
        ) => a.to_flake_value().numeric_cmp(&b.to_flake_value()) == Some(std::cmp::Ordering::Equal),
        _ => false,
    }
}

// ============================================================================
// Datalog Fixpoint Execution
// ============================================================================

/// Result of datalog rule execution
pub struct DatalogExecutionResult {
    /// Derived flakes from rule execution
    pub derived_flakes: Vec<Flake>,
    /// Fixpoint diagnostics (iterations, facts, capped flag, duration) — the
    /// same type OWL2-RL returns, so a capped run surfaces on the query
    /// response the same way (see `reasoning_prep`).
    pub diagnostics: ReasoningDiagnostics,
}

/// Execute datalog rules to fixpoint, generating derived facts
///
/// This function:
/// 1. Extracts rules from the database
/// 2. Optionally merges in query-time rules
/// 3. For each rule, finds bindings that match the `where` patterns
/// 4. Instantiates `insert` patterns to generate new flakes
/// 5. Repeats until no new facts are generated (fixpoint)
///
/// Each iteration uses a combined overlay that includes facts derived in previous
/// iterations, enabling recursive rules to work correctly.
///
/// # Arguments
///
/// * `snapshot` - The database to query
/// * `overlay` - Overlay provider for novelty/derived facts
/// * `to_t` - Time point for queries
/// * `max_iterations` - Maximum number of fixpoint iterations
/// * `query_time_rules` - Optional rules provided at query time (JSON-LD format)
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
    )
    .await
}

/// Execute datalog rules with optional query-time rules
///
/// This is the full implementation that supports both database-stored rules
/// and query-time rules passed as JSON-LD.
///
/// `rules_source_g_id` overrides the graph that `extract_datalog_rules`
/// scans for `f:rule` flakes. When `None`, extraction reads from
/// `db.g_id` (legacy behaviour). When `Some(g)`, a separate
/// `GraphDbRef` is built at graph `g` and used only for rule
/// extraction — the fixpoint loop still executes against `db`.
///
/// `budget` bounds the fixpoint by derived-fact count and wall-clock time
/// (checked between iterations, mirroring OWL2-RL's `fixpoint.rs`). This is
/// load-bearing because a rule with an unbound-subject-and-predicate `where`
/// pattern scans the whole ledger every iteration; `max_iterations` alone is
/// not a defensible ceiling for that. Exhausting the budget stops early and
/// sets `diagnostics.capped`.
pub async fn execute_datalog_rules_with_query_rules(
    db: GraphDbRef<'_>,
    max_iterations: usize,
    query_time_rules: &[serde_json::Value],
    rules_source_g_id: Option<fluree_db_core::GraphId>,
    budget: &ReasoningBudget,
) -> Result<DatalogExecutionResult> {
    // Extract rules from the configured source graph if set,
    // otherwise from the query graph. The fixpoint loop below
    // continues to execute against `db` regardless.
    let rules_db = match rules_source_g_id {
        Some(rg) if rg != db.g_id => GraphDbRef::new(db.snapshot, rg, db.overlay, db.t),
        _ => db,
    };
    let mut rule_set = extract_datalog_rules(rules_db).await?;

    // Parse and add query-time rules
    for (idx, rule_json) in query_time_rules.iter().enumerate() {
        match parse_query_time_rule(rule_json, db.snapshot, idx) {
            Ok(rule) => {
                rule_set.add_rule(rule);
            }
            Err(e) => {
                tracing::warn!("Failed to parse query-time rule {}: {}", idx, e);
            }
        }
    }

    if rule_set.is_empty() {
        return Ok(DatalogExecutionResult {
            derived_flakes: Vec::new(),
            diagnostics: ReasoningDiagnostics::completed(0, 0, std::time::Duration::ZERO),
        });
    }

    tracing::debug!(rule_count = rule_set.len(), "executing datalog rules");

    // Dedup key includes (s, p, o, dt, m).
    //
    // IMPORTANT: `m` (metadata) carries JSON-LD language tags and list indices,
    // so flakes are not truly equal unless `m` is also equal.
    let mut all_derived: HashMap<
        (
            Sid,
            Sid,
            String,
            Sid,
            Option<fluree_db_core::flake::FlakeMeta>,
        ),
        Flake,
    > = HashMap::new();
    let mut iterations = 0;

    // Use the same t as the query for derived facts (matching OWL2-RL approach)
    // This is important because the overlay filters with flake.t <= to_t
    let derived_t = db.t;

    // Track derived overlay for recursive rule support
    let mut derived_overlay: Option<Arc<fluree_db_reasoner::DerivedFactsOverlay>> = None;

    let start = Instant::now();
    let mut capped: Option<&str> = None;
    // Rules already flagged for "matched, but instantiated nothing" (#1560).
    // The fixpoint re-runs every rule each round, so without this the same
    // rule would warn once per iteration.
    let mut warned_barren: HashSet<Sid> = HashSet::new();

    loop {
        // Budget check BEFORE another round — a round can scan the whole ledger
        // per rule, so stop before paying for one we can't afford. Mirrors the
        // OWL2-RL fixpoint (`fluree-db-reasoner/src/fixpoint.rs`).
        if start.elapsed() > budget.max_duration {
            capped = Some("time");
            break;
        }
        if all_derived.len() > budget.max_facts {
            capped = Some("facts");
            break;
        }

        iterations += 1;
        let mut new_facts_this_round = 0;

        // Build combined overlay: base + derived facts from previous iterations
        // This enables recursive rules to match against their own derived facts
        let effective_overlay: Box<dyn OverlayProvider + '_> = match &derived_overlay {
            Some(derived) => Box::new(ReasoningOverlay::new(db.overlay, derived.clone())),
            None => {
                // First iteration: use base overlay directly
                // We wrap in a trivial struct that implements OverlayProvider
                Box::new(OverlayRef(db.overlay))
            }
        };

        // Execute each rule in order
        for rule in rule_set.iter_in_order() {
            // Find all bindings matching the where patterns
            // Use effective_overlay which includes derived facts from previous iterations
            let iter_db = GraphDbRef::new(db.snapshot, db.g_id, effective_overlay.as_ref(), db.t);
            let binding_rows = execute_rule_matching(rule, iter_db).await?;

            if binding_rows.is_empty() {
                // The where clause matched nothing. That is ordinary and says
                // nothing about the rule's soundness — stay quiet (#1560).
                continue;
            }
            let matched_rows = binding_rows.len();

            // Generate flakes from bindings
            let flakes =
                fluree_db_reasoner::execute_rule_with_bindings(rule, binding_rows, derived_t);

            // "The where clause matched but the insert could not instantiate"
            // is almost always an authoring bug, and it is invisible otherwise:
            // the rule appears to run and the fixpoint completes normally
            // (#1560). Catches what the parse-time safety check cannot see —
            // SPARQL-lowered rules, and a subject/predicate variable that binds
            // to a literal only at run time.
            if flakes.is_empty() && warned_barren.insert(rule.id.clone()) {
                tracing::warn!(
                    rule = rule.name.as_deref().unwrap_or("<unnamed>"),
                    rule_id = %rule.id,
                    matched_rows,
                    "datalog rule matched binding rows but derived no facts: every row \
                     failed to instantiate its insert pattern — an insert variable is \
                     unbound, or is bound to a literal in subject/predicate position"
                );
            }

            // Add new flakes (deduplicating by s, p, o, dt, m)
            for flake in flakes {
                let key = (
                    flake.s.clone(),
                    flake.p.clone(),
                    format!("{:?}", flake.o),
                    flake.dt.clone(),
                    flake.m.clone(),
                );
                if let std::collections::hash_map::Entry::Vacant(e) = all_derived.entry(key) {
                    e.insert(flake);
                    new_facts_this_round += 1;
                }
            }
        }

        tracing::debug!(
            iteration = iterations,
            new_facts = new_facts_this_round,
            total_derived = all_derived.len(),
            "datalog fixpoint iteration"
        );

        // Check for fixpoint
        if new_facts_this_round == 0 || iterations >= max_iterations {
            break;
        }

        // Build derived overlay for next iteration with current derived facts
        // This allows subsequent iterations to match against facts derived so far
        let mut builder = DerivedFactsBuilder::with_capacity(all_derived.len());
        builder.extend(all_derived.values().cloned());
        derived_overlay = Some(Arc::new(
            builder.build(FrozenSameAs::empty(), db.overlay.epoch()),
        ));
    }

    let facts = all_derived.len();
    let elapsed = start.elapsed();
    let mut diagnostics = ReasoningDiagnostics::default();
    match capped {
        Some(reason) => diagnostics.mark_capped(reason, iterations, facts, elapsed),
        None => diagnostics.mark_completed(iterations, facts, elapsed),
    }

    Ok(DatalogExecutionResult {
        derived_flakes: all_derived.into_values().collect(),
        diagnostics,
    })
}

/// Wrapper to use a `&dyn OverlayProvider` as an owned `OverlayProvider`
struct OverlayRef<'a>(&'a dyn OverlayProvider);

impl OverlayProvider for OverlayRef<'_> {
    fn as_any(&self) -> &dyn std::any::Any {
        self.0.as_any()
    }

    fn epoch(&self) -> u64 {
        self.0.epoch()
    }

    fn overlay_flake_count(&self, g_id: fluree_db_core::GraphId) -> Option<usize> {
        self.0.overlay_flake_count(g_id)
    }

    fn for_each_overlay_flake(
        &self,
        g_id: fluree_db_core::GraphId,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
        to_t: i64,
        callback: &mut dyn FnMut(&Flake),
    ) {
        self.0
            .for_each_overlay_flake(g_id, index, first, rhs, leftmost, to_t, callback);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_iri_full() {
        let context = serde_json::json!({});
        let result = expand_iri("http://example.org/Person", &context).unwrap();
        assert_eq!(result, "http://example.org/Person");
    }

    #[test]
    fn test_expand_iri_compact() {
        let context = serde_json::json!({"ex": "http://example.org/"});
        let result = expand_iri("ex:Person", &context).unwrap();
        assert_eq!(result, "http://example.org/Person");
    }

    #[test]
    fn resolve_term_non_sid_binding_is_incompatible() {
        // A subject/predicate variable bound to a literal can never match a
        // flake; resolution must report an empty join, not an error, so one
        // bad binding row cannot abort the whole rule execution.
        let mut bindings = Bindings::new();
        bindings.insert(Arc::from("?p"), BindingValue::String("blue".to_string()));

        let resolved = resolve_term_with_bindings(&RuleTerm::var("?p"), &bindings);
        assert!(matches!(resolved, TermResolution::Incompatible));
        assert!(resolved.split().is_none());
    }

    fn triple(subject: RuleTerm, predicate: RuleTerm, object: RuleTerm) -> RuleTriplePattern {
        RuleTriplePattern {
            subject,
            predicate,
            object,
        }
    }

    #[test]
    fn selective_order_hoists_grounded_pattern_ahead_of_all_unbound_leading() {
        // where: [ {?s ?p ?o}, {?s ex:parent ?o} ]
        // The all-unbound pattern is written first (a full ledger scan), but
        // the constant-predicate pattern must be matched first so it grounds
        // ?s/?o before the unbound pattern runs.
        let ex_parent = Sid::new(0, "http://example.org/parent");
        let patterns = vec![
            triple(
                RuleTerm::var("?s"),
                RuleTerm::var("?p"),
                RuleTerm::var("?o"),
            ),
            triple(
                RuleTerm::var("?s"),
                RuleTerm::sid(ex_parent),
                RuleTerm::var("?o"),
            ),
        ];
        assert_eq!(selective_pattern_order(&patterns), vec![1, 0]);
    }

    #[test]
    fn selective_order_keeps_source_order_on_ties() {
        // Two equally-grounded patterns (both constant predicate) keep their
        // written order — reordering is deterministic.
        let p1 = Sid::new(0, "http://example.org/a");
        let p2 = Sid::new(0, "http://example.org/b");
        let patterns = vec![
            triple(RuleTerm::var("?s"), RuleTerm::sid(p1), RuleTerm::var("?o")),
            triple(RuleTerm::var("?x"), RuleTerm::sid(p2), RuleTerm::var("?y")),
        ];
        assert_eq!(selective_pattern_order(&patterns), vec![0, 1]);
    }

    #[test]
    fn resolve_term_literal_constant_is_incompatible() {
        // A literal constant in subject/predicate position (e.g. a typo like
        // `{"@id": "Alice"}` parsed as a string value) can never match a
        // flake; resolution must report an empty join, not an error, so the
        // authoring mistake cannot abort the whole fixpoint.
        let bindings = Bindings::new();
        let literal = RuleTerm::Value(RuleValue::String("Alice".to_string()));

        let resolved = resolve_term_with_bindings(&literal, &bindings);
        assert!(matches!(resolved, TermResolution::Incompatible));
        assert!(resolved.split().is_none());
    }

    fn dec(s: &str) -> FlakeValue {
        FlakeValue::Decimal(Box::new(s.parse().expect("decimal")))
    }

    #[test]
    fn test_decimal_object_matching_and_binding_roundtrip() {
        // Decimal flake objects must match decimal-bound constraints (they
        // previously fell to the catch-all and never matched).
        let bound = flake_value_to_binding(&dec("19.99"));
        let m = binding_to_object_match(&bound);
        assert!(flake_object_matches(&dec("19.99"), &m));
        assert!(flake_object_matches(&dec("19.990"), &m), "scale variants");
        assert!(!flake_object_matches(&dec("19.98"), &m));

        // Round-trip preserves the exact value (no f64, no zeroing).
        assert_eq!(bound.to_flake_value(), dec("19.99"));

        // BigInt round-trip: previously parse::<i64>().unwrap_or(0) → 0.
        let big = FlakeValue::BigInt(Box::new(
            "123456789012345678901234567890".parse().expect("bigint"),
        ));
        let bound = flake_value_to_binding(&big);
        assert_eq!(bound.to_flake_value(), big);
        assert!(flake_object_matches(&big, &binding_to_object_match(&bound)));
    }

    #[test]
    fn test_bindings_equal_exact_numeric() {
        // Exact equality: the old epsilon comparison conflated distinct
        // neighboring doubles.
        let a = BindingValue::Double(1.0);
        let next = BindingValue::Double(f64::from_bits(1.0f64.to_bits() + 1));
        assert!(bindings_equal(&a, &a.clone()));
        assert!(!bindings_equal(&a, &next), "adjacent doubles are distinct");
        // Cross-representation numeric equality.
        assert!(bindings_equal(
            &BindingValue::Long(3),
            &flake_value_to_binding(&dec("3.00"))
        ));
        // 19.99 has no exact f64 twin: decimal != nearest double.
        assert!(!bindings_equal(
            &flake_value_to_binding(&dec("19.99")),
            &BindingValue::Double(19.99)
        ));
    }

    // ------------------------------------------------------------------
    // IRI comparison in filters (#1556)
    // ------------------------------------------------------------------

    #[test]
    fn curie_prefix_rejects_colon_bearing_literals() {
        // Real CURIEs.
        assert_eq!(curie_prefix("ex:ssn"), Some("ex"));
        assert_eq!(curie_prefix("foaf:knows"), Some("foaf"));
        assert_eq!(curie_prefix("_x:y"), Some("_x"));
        assert_eq!(curie_prefix("dc-terms:title"), Some("dc-terms"));

        // Literals that merely contain a colon must NOT be read as IRIs — no
        // NCName may start with a digit, which is what keeps times and
        // timestamps out of IRI classification.
        assert_eq!(curie_prefix("12:30:00"), None);
        assert_eq!(curie_prefix("2026-08-25T09:30:00Z"), None);
        assert_eq!(curie_prefix("no-colon-here"), None);
        assert_eq!(curie_prefix("has space:x"), None);
    }

    #[test]
    fn quoted_operand_is_the_string_escape_hatch() {
        assert_eq!(quoted_literal("\"ex:ssn\""), Some("ex:ssn"));
        assert_eq!(quoted_literal("'ex:ssn'"), Some("ex:ssn"));
        assert_eq!(quoted_literal("ex:ssn"), None);
        assert_eq!(quoted_literal("\"\""), Some(""));
    }

    fn sid(ns: u16, name: &str) -> Sid {
        Sid::new(ns, name)
    }

    #[test]
    fn iri_equality_compares_sids_not_local_names() {
        // The #1556 defect in miniature: `ex:knows` and `foaf:knows` share a
        // local name. Comparing local names conflated them; comparing Sids
        // does not.
        let ex_knows = FlakeValue::Ref(sid(100, "knows"));
        let foaf_knows = FlakeValue::Ref(sid(200, "knows"));

        assert_eq!(
            compare_values(&ex_knows, &ex_knows.clone(), CompareOp::Equal),
            FilterOutcome::True
        );
        assert_eq!(
            compare_values(&ex_knows, &foaf_knows, CompareOp::Equal),
            FilterOutcome::False,
            "same local name in different namespaces must not compare equal"
        );
        assert_eq!(
            compare_values(&ex_knows, &foaf_knows, CompareOp::NotEqual),
            FilterOutcome::True
        );
    }

    #[test]
    fn iri_ordering_is_an_error_not_a_silent_false() {
        let a = FlakeValue::Ref(sid(100, "a"));
        let b = FlakeValue::Ref(sid(100, "b"));
        for op in [
            CompareOp::LessThan,
            CompareOp::LessThanOrEqual,
            CompareOp::GreaterThan,
            CompareOp::GreaterThanOrEqual,
        ] {
            assert_eq!(compare_values(&a, &b, op), FilterOutcome::Error);
        }
    }

    #[test]
    fn iri_versus_literal_follows_rdfterm_equal() {
        // SPARQL 1.1 §17.4.1.7: RDFterm-equal is a type error only when BOTH
        // operands are literals. An IRI and a literal are simply never the same
        // RDF term, so `=` is false and `!=` is true — routing this pair to
        // Error would drop rows the spec says to keep.
        let iri = FlakeValue::Ref(sid(100, "ssn"));
        let text = FlakeValue::String("ex:ssn".to_string());

        assert_eq!(
            compare_values(&iri, &text, CompareOp::Equal),
            FilterOutcome::False
        );
        assert_eq!(
            compare_values(&iri, &text, CompareOp::NotEqual),
            FilterOutcome::True,
            "an IRI is genuinely not a literal; `!=` must keep the row"
        );
        // Direction must not matter.
        assert_eq!(
            compare_values(&text, &iri, CompareOp::NotEqual),
            FilterOutcome::True
        );
        // An IRI against a number is the same story.
        assert_eq!(
            compare_values(&iri, &FlakeValue::Long(5), CompareOp::NotEqual),
            FilterOutcome::True
        );
        // Ordering across an IRI and a literal stays undefined.
        for op in [CompareOp::LessThan, CompareOp::GreaterThan] {
            assert_eq!(compare_values(&iri, &text, op), FilterOutcome::Error);
        }
    }

    #[test]
    fn two_literals_of_different_types_are_a_type_error() {
        // The other half of §17.4.1.7: both operands literal and not the same
        // RDF term IS the type-error case.
        let text = FlakeValue::String("5".to_string());
        let number = FlakeValue::Long(5);
        assert_eq!(
            compare_values(&text, &number, CompareOp::Equal),
            FilterOutcome::Error
        );
        assert_eq!(
            compare_values(&text, &number, CompareOp::NotEqual),
            FilterOutcome::Error
        );
    }

    #[test]
    fn incomparable_operands_never_fail_open() {
        // #1556's invariant, stated over the pairing that is genuinely
        // undecidable: a type error must never answer "true" and admit the row.
        let text = FlakeValue::String("5".to_string());
        let number = FlakeValue::Long(5);

        for op in [
            CompareOp::Equal,
            CompareOp::NotEqual,
            CompareOp::LessThan,
            CompareOp::GreaterThan,
        ] {
            assert_ne!(
                compare_values(&text, &number, op),
                FilterOutcome::True,
                "a type error must never evaluate true for {op:?}"
            );
        }
    }

    #[test]
    fn negating_an_uncomparable_filter_does_not_admit_the_row() {
        // `Not` is reachable from SPARQL rules (`FILTER(!(...))`). If Error
        // collapsed to False, `!Error` would become True and re-admit a row
        // the engine could not judge — a fail-open path straight back in.
        let bindings = Bindings::new();
        let uncomparable = RuleFilter::Compare {
            op: CompareOp::Equal,
            left: RuleTerm::var("?never_bound"),
            right: RuleTerm::Value(RuleValue::Long(1)),
        };

        assert_eq!(
            evaluate_filter(&uncomparable, &bindings, &mut FilterDiagnostics::default()),
            FilterOutcome::Error
        );
        assert_eq!(
            evaluate_filter(
                &RuleFilter::Not(Box::new(uncomparable)),
                &bindings,
                &mut FilterDiagnostics::default()
            ),
            FilterOutcome::Error,
            "negating an error must stay an error, never become True"
        );
    }

    #[test]
    fn and_or_follow_sparql_error_propagation() {
        let bindings = Bindings::new();
        let error = || RuleFilter::Compare {
            op: CompareOp::Equal,
            left: RuleTerm::var("?never_bound"),
            right: RuleTerm::Value(RuleValue::Long(1)),
        };
        let truth = || RuleFilter::Compare {
            op: CompareOp::Equal,
            left: RuleTerm::Value(RuleValue::Long(1)),
            right: RuleTerm::Value(RuleValue::Long(1)),
        };
        let falsehood = || RuleFilter::Compare {
            op: CompareOp::Equal,
            left: RuleTerm::Value(RuleValue::Long(1)),
            right: RuleTerm::Value(RuleValue::Long(2)),
        };

        // false && error => false; true && error => error
        assert_eq!(
            evaluate_filter(
                &RuleFilter::And(vec![falsehood(), error()]),
                &bindings,
                &mut FilterDiagnostics::default()
            ),
            FilterOutcome::False
        );
        assert_eq!(
            evaluate_filter(
                &RuleFilter::And(vec![truth(), error()]),
                &bindings,
                &mut FilterDiagnostics::default()
            ),
            FilterOutcome::Error
        );
        // true || error => true; false || error => error
        assert_eq!(
            evaluate_filter(
                &RuleFilter::Or(vec![truth(), error()]),
                &bindings,
                &mut FilterDiagnostics::default()
            ),
            FilterOutcome::True
        );
        assert_eq!(
            evaluate_filter(
                &RuleFilter::Or(vec![falsehood(), error()]),
                &bindings,
                &mut FilterDiagnostics::default()
            ),
            FilterOutcome::Error
        );
    }

    #[test]
    fn sid_bound_variable_resolves_as_an_iri() {
        let mut bindings = Bindings::new();
        bindings.insert(Arc::from("?p"), BindingValue::Sid(sid(100, "knows")));

        let resolved = resolve_filter_term(&RuleTerm::var("?p"), &bindings);
        assert_eq!(
            resolved,
            Some(FlakeValue::Ref(sid(100, "knows"))),
            "a Sid binding must stay a Ref, not collapse to its local name"
        );
    }

    #[test]
    fn quoted_operand_survives_whitespace() {
        // The escape hatch the parse error recommends has to work for a value
        // with a space in it; `split_whitespace` used to tear it in half and
        // leave the operand as the literal `"John`.
        let tokens = split_filter_tokens(r#"= ?name "John Smith""#).expect("tokenize");
        assert_eq!(tokens, vec!["=", "?name", "\"John Smith\""]);
        assert_eq!(quoted_literal(tokens[2]), Some("John Smith"));
    }

    #[test]
    fn unterminated_quote_is_rejected() {
        let err = split_filter_tokens(r#"= ?name "John"#).expect_err("must reject");
        assert!(
            format!("{err:?}").contains("Unterminated quote"),
            "got: {err:?}"
        );
    }

    #[test]
    fn plain_tokens_still_split_on_whitespace() {
        assert_eq!(
            split_filter_tokens(">=  ?age   62").expect("tokenize"),
            vec![">=", "?age", "62"]
        );
        assert!(split_filter_tokens("   ").expect("tokenize").is_empty());
    }

    #[test]
    fn misspelled_filter_keyword_is_recognized_for_the_message() {
        assert!(looks_like_misspelled_filter(&serde_json::json!([
            "FILTER",
            "(!= ?p ex:ssn)"
        ])));
        assert!(looks_like_misspelled_filter(&serde_json::json!([
            "Filter",
            "(!= ?p ex:ssn)"
        ])));
        assert!(!looks_like_misspelled_filter(&serde_json::json!([
            "bind", "(?x 1)"
        ])));
        assert!(!looks_like_misspelled_filter(&serde_json::json!({
            "@id": "?s"
        })));
    }

    // ------------------------------------------------------------------
    // Unbound insert variables (#1560)
    // ------------------------------------------------------------------

    #[test]
    fn unbound_insert_variable_is_named() {
        // The issue's own repro: where binds ?relation, insert says ?rel.
        let where_patterns = vec![triple(
            RuleTerm::var("?s"),
            RuleTerm::sid(sid(100, "relType")),
            RuleTerm::var("?relation"),
        )];
        let insert_patterns = vec![triple(
            RuleTerm::var("?s"),
            RuleTerm::var("?rel"),
            RuleTerm::var("?s"),
        )];

        let unbound = unbound_insert_variables(&where_patterns, &insert_patterns);
        assert_eq!(unbound.len(), 1);
        assert_eq!(unbound[0].as_ref(), "?rel");

        let message = unsafe_insert_variables_message(&where_patterns, &insert_patterns)
            .expect("unsafe rule must produce a message");
        assert!(
            message.contains("?rel"),
            "the diagnostic must name the offending variable, got: {message}"
        );
    }

    #[test]
    fn where_side_generated_name_does_not_mask_an_insert_side_one() {
        // Both clauses number their generated variables from their own
        // `patterns.len()`, so the names collide at index 0. A where-side
        // `?__implicit_0` must not be treated as binding the insert-side one,
        // or the anonymous-node diagnostic is suppressed exactly where it is
        // needed.
        let where_patterns = vec![triple(
            RuleTerm::var("?__implicit_0"),
            RuleTerm::sid(sid(100, "parent")),
            RuleTerm::var("?p"),
        )];
        let insert_patterns = vec![triple(
            RuleTerm::var("?__implicit_0"),
            RuleTerm::sid(sid(100, "ancestor")),
            RuleTerm::var("?p"),
        )];

        let message = unsafe_insert_variables_message(&where_patterns, &insert_patterns)
            .expect("collision must not suppress the diagnostic");
        assert!(message.contains("@id"), "got: {message}");
    }

    #[test]
    fn instantiability_is_judged_per_insert_pattern() {
        // A two-head rule with one typo'd head: the good head must be
        // recognized as instantiable on its own, so parsing can keep it while
        // skipping the bad one — matching `execute_rule_with_bindings`, which
        // instantiates each insert pattern independently.
        let where_patterns = vec![triple(
            RuleTerm::var("?s"),
            RuleTerm::sid(sid(100, "relType")),
            RuleTerm::var("?relation"),
        )];
        let good = triple(
            RuleTerm::var("?s"),
            RuleTerm::sid(sid(100, "hasRelation")),
            RuleTerm::var("?relation"),
        );
        let bad = triple(
            RuleTerm::var("?s"),
            RuleTerm::var("?rel"),
            RuleTerm::var("?s"),
        );

        let bound = authored_bound_vars(&where_patterns);
        assert!(insert_pattern_is_instantiable(&good, &bound));
        assert!(!insert_pattern_is_instantiable(&bad, &bound));

        // The description for the skipped head names only its own variable.
        let description = unbound_insert_description(&where_patterns, std::slice::from_ref(&bad))
            .expect("the bad head must produce a description");
        assert!(description.contains("?rel"), "got: {description}");
        assert!(!description.contains("?relation"), "got: {description}");
    }

    #[test]
    fn safe_rule_produces_no_message() {
        let where_patterns = vec![triple(
            RuleTerm::var("?s"),
            RuleTerm::sid(sid(100, "parent")),
            RuleTerm::var("?p"),
        )];
        let insert_patterns = vec![triple(
            RuleTerm::var("?s"),
            RuleTerm::sid(sid(100, "ancestor")),
            RuleTerm::var("?p"),
        )];

        assert!(unbound_insert_variables(&where_patterns, &insert_patterns).is_empty());
        assert!(unsafe_insert_variables_message(&where_patterns, &insert_patterns).is_none());
    }

    #[test]
    fn generated_variables_get_their_own_wording() {
        // An author never wrote `?__implicit_0`, so quoting it back is useless
        // — the message has to say what they actually did wrong.
        let where_patterns = vec![triple(
            RuleTerm::var("?s"),
            RuleTerm::sid(sid(100, "parent")),
            RuleTerm::var("?p"),
        )];
        let insert_patterns = vec![triple(
            RuleTerm::var("?__implicit_0"),
            RuleTerm::sid(sid(100, "ancestor")),
            RuleTerm::var("?p"),
        )];

        let message = unsafe_insert_variables_message(&where_patterns, &insert_patterns)
            .expect("anonymous insert node must produce a message");
        assert!(
            message.contains("@id"),
            "the diagnostic must point at the missing @id, got: {message}"
        );
        assert!(
            !message.contains("?__implicit_0"),
            "the diagnostic must not quote an engine-generated name, got: {message}"
        );
    }
}
