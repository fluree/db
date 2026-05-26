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
    FrozenSameAs, RuleFilter, RuleTerm, RuleTriplePattern, RuleValue,
};
use fluree_vocab::namespaces::FLUREE_DB;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;

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

    // Parse each rule
    for flake in &rule_flakes {
        let rule_id = flake.s.clone();

        // The rule value should be a JSON string
        if let FlakeValue::Json(json_str) = &flake.o {
            match serde_json::from_str::<JsonValue>(json_str) {
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
            }
        }
    }

    Ok(rule_set)
}

/// Parse a query-time rule from JSON-LD
///
/// Query-time rules can have two formats:
/// 1. Direct rule format: `{"where": ..., "insert": ...}`
/// 2. Stored rule format: `{"@id": "...", "f:rule": {"@value": {"where": ..., "insert": ...}}}`
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
        // Extract the actual rule from the @value wrapper
        let rule_value = if let Some(value) = f_rule.get("@value") {
            value
        } else {
            f_rule
        };

        // Get rule ID from @id, or generate one
        let rule_id = if let Some(id_str) = json.get("@id").and_then(|v| v.as_str()) {
            Sid::new(0, id_str)
        } else {
            Sid::new(0, format!("_:query_rule_{index}"))
        };

        return parse_rule_definition(&rule_id, rule_value, snapshot);
    }

    // Direct rule format
    // Generate a synthetic rule ID
    let rule_id = Sid::new(0, format!("_:query_rule_{index}"));
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
                        if let Some(filter) = parse_filter_expression(filter_arr)? {
                            filters.push(filter);
                        }
                    }
                    _ => {
                        // Skip unknown array elements
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

/// Check if an array is a filter expression (starts with "filter")
fn is_filter_expression(arr: &[JsonValue]) -> bool {
    matches!(arr.first(), Some(JsonValue::String(s)) if s == "filter")
}

/// Parse a filter expression like ["filter", "(>= ?age 62)"]
fn parse_filter_expression(arr: &[JsonValue]) -> Result<Option<RuleFilter>> {
    if arr.len() != 2 {
        return Ok(None);
    }

    let expr = match &arr[1] {
        JsonValue::String(s) => s.as_str(),
        _ => return Ok(None),
    };

    // Parse S-expression style filter: "(op arg1 arg2)"
    let expr = expr.trim();
    if !expr.starts_with('(') || !expr.ends_with(')') {
        return Err(QueryError::InvalidQuery(format!(
            "Invalid filter expression: {expr}"
        )));
    }

    let inner = &expr[1..expr.len() - 1];
    let parts: Vec<&str> = inner.split_whitespace().collect();

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
    let left = parse_filter_term(parts[1])?;
    let right = if parts.len() > 2 {
        parse_filter_term(parts[2])?
    } else {
        return Err(QueryError::InvalidQuery(format!(
            "Comparison filter needs two arguments: {expr}"
        )));
    };

    Ok(Some(RuleFilter::Compare { op, left, right }))
}

/// Parse a single term in a filter expression (variable or literal)
fn parse_filter_term(s: &str) -> Result<RuleTerm> {
    if s.starts_with('?') {
        // Variable
        Ok(RuleTerm::var(s))
    } else if let Ok(n) = s.parse::<i64>() {
        // Integer literal
        Ok(RuleTerm::Value(RuleValue::Long(n)))
    } else if let Ok(f) = s.parse::<f64>() {
        // Float literal
        Ok(RuleTerm::Value(RuleValue::Double(f)))
    } else if s == "true" {
        Ok(RuleTerm::Value(RuleValue::Boolean(true)))
    } else if s == "false" {
        Ok(RuleTerm::Value(RuleValue::Boolean(false)))
    } else {
        // String literal (strip quotes if present)
        let s = s.trim_matches('"').trim_matches('\'');
        Ok(RuleTerm::Value(RuleValue::String(s.to_string())))
    }
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
        let var_name = format!("?__implicit_{}", patterns.len());
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

        // Resolve predicate IRI
        let predicate_iri = expand_iri(key, context)?;
        let predicate_sid = resolve_iri(&predicate_iri, snapshot)?;

        // Parse object(s)
        match value {
            JsonValue::Array(arr) => {
                for item in arr {
                    let obj = parse_object_value(
                        item,
                        context,
                        snapshot,
                        patterns,
                        &subject,
                        &predicate_sid,
                    )?;
                    patterns.push(RuleTriplePattern {
                        subject: subject.clone(),
                        predicate: RuleTerm::Sid(predicate_sid.clone()),
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
                    let var_name = format!("?__nested_{}", patterns.len());
                    RuleTerm::var(&var_name)
                };

                // Add pattern linking parent to nested subject
                patterns.push(RuleTriplePattern {
                    subject: subject.clone(),
                    predicate: RuleTerm::Sid(predicate_sid.clone()),
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
                    predicate: RuleTerm::Sid(predicate_sid.clone()),
                    object: obj,
                });
            }
        }
    }

    Ok(())
}

/// Parse an object value, handling nested structures
fn parse_object_value(
    value: &JsonValue,
    context: &JsonValue,
    snapshot: &LedgerSnapshot,
    patterns: &mut Vec<RuleTriplePattern>,
    _parent_subject: &RuleTerm,
    _predicate_sid: &Sid,
) -> Result<RuleTerm> {
    match value {
        JsonValue::Object(nested) if nested.contains_key("@id") => {
            // Reference to another node
            parse_term(nested.get("@id").unwrap(), context, snapshot)
        }
        JsonValue::Object(nested) => {
            // Nested anonymous node - generate variable and recurse
            let var_name = format!("?__anon_{}", patterns.len());
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

    // Check if it's a variable
    if compact.starts_with('?') {
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

    // Start with the first pattern to get initial bindings
    let mut binding_rows = match_pattern(&rule.where_patterns[0], db, &[]).await?;

    // Join with subsequent patterns
    for pattern in rule.where_patterns.iter().skip(1) {
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

    // Apply filters to eliminate non-matching bindings
    if !rule.filters.is_empty() {
        binding_rows.retain(|bindings| {
            rule.filters
                .iter()
                .all(|filter| evaluate_filter(filter, bindings))
        });
    }

    Ok(binding_rows)
}

/// Evaluate a filter expression against a set of bindings
fn evaluate_filter(filter: &RuleFilter, bindings: &Bindings) -> bool {
    match filter {
        RuleFilter::Compare { op, left, right } => {
            let left_val = resolve_filter_term(left, bindings);
            let right_val = resolve_filter_term(right, bindings);

            match (left_val, right_val) {
                (Some(l), Some(r)) => compare_values(&l, &r, *op),
                _ => false, // If either side can't be resolved, filter fails
            }
        }
        RuleFilter::And(filters) => filters.iter().all(|f| evaluate_filter(f, bindings)),
        RuleFilter::Or(filters) => filters.iter().any(|f| evaluate_filter(f, bindings)),
        RuleFilter::Not(inner) => !evaluate_filter(inner, bindings),
    }
}

/// Resolve a filter term to a comparable value
fn resolve_filter_term(term: &RuleTerm, bindings: &Bindings) -> Option<FlakeValue> {
    match term {
        RuleTerm::Var(name) => bindings.get(name.as_ref()).map(|bv| match bv {
            BindingValue::Long(n) => FlakeValue::Long(*n),
            BindingValue::Double(d) => FlakeValue::Double(*d),
            BindingValue::String(s) => FlakeValue::String(s.clone()),
            BindingValue::Boolean(b) => FlakeValue::Boolean(*b),
            BindingValue::Sid(sid) => FlakeValue::String(sid.name.to_string()),
        }),
        RuleTerm::Value(val) => Some(match val {
            RuleValue::Long(n) => FlakeValue::Long(*n),
            RuleValue::Double(d) => FlakeValue::Double(*d),
            RuleValue::String(s) => FlakeValue::String(s.clone()),
            RuleValue::Boolean(b) => FlakeValue::Boolean(*b),
            RuleValue::Ref(sid) => FlakeValue::String(sid.name.to_string()),
        }),
        RuleTerm::Sid(sid) => Some(FlakeValue::String(sid.name.to_string())),
    }
}

/// Compare two filter values using the given operator. Only the
/// Long/Double/String/Boolean variants of FlakeValue are inspected; other
/// variants fall through to "incompatible".
fn compare_values(left: &FlakeValue, right: &FlakeValue, op: CompareOp) -> bool {
    // Try numeric comparison first
    let numeric_result = match (left, right) {
        (FlakeValue::Long(l), FlakeValue::Long(r)) => Some(l.cmp(r)),
        (FlakeValue::Long(l), FlakeValue::Double(r)) => (*l as f64).partial_cmp(r),
        (FlakeValue::Double(l), FlakeValue::Long(r)) => l.partial_cmp(&(*r as f64)),
        (FlakeValue::Double(l), FlakeValue::Double(r)) => l.partial_cmp(r),
        _ => None,
    };

    if let Some(cmp) = numeric_result {
        return match op {
            CompareOp::Equal => cmp == std::cmp::Ordering::Equal,
            CompareOp::NotEqual => cmp != std::cmp::Ordering::Equal,
            CompareOp::LessThan => cmp == std::cmp::Ordering::Less,
            CompareOp::LessThanOrEqual => cmp != std::cmp::Ordering::Greater,
            CompareOp::GreaterThan => cmp == std::cmp::Ordering::Greater,
            CompareOp::GreaterThanOrEqual => cmp != std::cmp::Ordering::Less,
        };
    }

    // Fall back to string comparison
    let (left_str, right_str) = match (left, right) {
        (FlakeValue::String(l), FlakeValue::String(r)) => (l.as_str(), r.as_str()),
        (FlakeValue::Boolean(l), FlakeValue::Boolean(r)) => {
            // Boolean comparison
            return match op {
                CompareOp::Equal => l == r,
                CompareOp::NotEqual => l != r,
                _ => false, // Other comparisons don't make sense for booleans
            };
        }
        _ => return false, // Incompatible types
    };

    let cmp = left_str.cmp(right_str);
    match op {
        CompareOp::Equal => cmp == std::cmp::Ordering::Equal,
        CompareOp::NotEqual => cmp != std::cmp::Ordering::Equal,
        CompareOp::LessThan => cmp == std::cmp::Ordering::Less,
        CompareOp::LessThanOrEqual => cmp != std::cmp::Ordering::Greater,
        CompareOp::GreaterThan => cmp == std::cmp::Ordering::Greater,
        CompareOp::GreaterThanOrEqual => cmp != std::cmp::Ordering::Less,
    }
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
    // Resolve pattern terms using existing bindings
    let (subject_sid, subject_var) = resolve_term_with_bindings(&pattern.subject, bindings)?;
    let (predicate_sid, predicate_var) = resolve_term_with_bindings(&pattern.predicate, bindings)?;
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

/// Resolve a term using existing bindings, returning (resolved_sid, unbound_var_name)
fn resolve_term_with_bindings(
    term: &RuleTerm,
    bindings: &Bindings,
) -> Result<(Option<Sid>, Option<Arc<str>>)> {
    match term {
        RuleTerm::Sid(sid) => Ok((Some(sid.clone()), None)),
        RuleTerm::Var(var) => {
            // Check if variable is already bound
            if let Some(binding) = bindings.get(var.as_ref()) {
                match binding {
                    BindingValue::Sid(sid) => Ok((Some(sid.clone()), None)),
                    _ => Err(QueryError::InvalidQuery(format!(
                        "Variable {var} bound to non-SID value in subject/predicate position"
                    ))),
                }
            } else {
                Ok((None, Some(var.clone())))
            }
        }
        RuleTerm::Value(_) => Err(QueryError::InvalidQuery(
            "Literal value not allowed in subject/predicate position".to_string(),
        )),
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
    Boolean(bool),
}

fn binding_to_object_match(binding: &BindingValue) -> ObjectMatch {
    match binding {
        BindingValue::Sid(sid) => ObjectMatch::Ref(sid.clone()),
        BindingValue::String(s) => ObjectMatch::String(s.clone()),
        BindingValue::Long(n) => ObjectMatch::Long(*n),
        BindingValue::Double(d) => ObjectMatch::Double(*d),
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
    match (flake_obj, expected) {
        (FlakeValue::Ref(a), ObjectMatch::Ref(b)) => a == b,
        (FlakeValue::String(a), ObjectMatch::String(b)) => a == b,
        (FlakeValue::Long(a), ObjectMatch::Long(b)) => a == b,
        (FlakeValue::Double(a), ObjectMatch::Double(b)) => (a - b).abs() < f64::EPSILON,
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
        FlakeValue::BigInt(b) => BindingValue::Long(b.to_string().parse().unwrap_or(0)),
        FlakeValue::Decimal(b) => BindingValue::Double(b.to_string().parse().unwrap_or(0.0)),
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
        (BindingValue::Double(a), BindingValue::Double(b)) => (a - b).abs() < f64::EPSILON,
        (BindingValue::Boolean(a), BindingValue::Boolean(b)) => a == b,
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
    /// Number of fixpoint iterations
    pub iterations: usize,
    /// Number of rules executed
    pub rules_executed: usize,
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
    execute_datalog_rules_with_query_rules(db, max_iterations, &[], None).await
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
pub async fn execute_datalog_rules_with_query_rules(
    db: GraphDbRef<'_>,
    max_iterations: usize,
    query_time_rules: &[serde_json::Value],
    rules_source_g_id: Option<fluree_db_core::GraphId>,
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
            iterations: 0,
            rules_executed: 0,
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
    let mut rules_executed = 0;

    // Use the same t as the query for derived facts (matching OWL2-RL approach)
    // This is important because the overlay filters with flake.t <= to_t
    let derived_t = db.t;

    // Track derived overlay for recursive rule support
    let mut derived_overlay: Option<Arc<fluree_db_reasoner::DerivedFactsOverlay>> = None;

    loop {
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
            rules_executed += 1;

            // Find all bindings matching the where patterns
            // Use effective_overlay which includes derived facts from previous iterations
            let iter_db = GraphDbRef::new(db.snapshot, db.g_id, effective_overlay.as_ref(), db.t);
            let binding_rows = execute_rule_matching(rule, iter_db).await?;

            if binding_rows.is_empty() {
                continue;
            }

            // Generate flakes from bindings
            let flakes =
                fluree_db_reasoner::execute_rule_with_bindings(rule, binding_rows, derived_t);

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

    Ok(DatalogExecutionResult {
        derived_flakes: all_derived.into_values().collect(),
        iterations,
        rules_executed,
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
    fn test_expand_iri_variable() {
        let context = serde_json::json!({});
        let result = expand_iri("?person", &context).unwrap();
        assert_eq!(result, "?person");
    }
}
