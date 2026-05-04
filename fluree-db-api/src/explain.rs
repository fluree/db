//! Explain API: query optimization plan
//!
//! This module builds a user-facing explanation of query planning decisions.
//! It is intended for integration testing coverage of the explain surface.

use crate::error::{ApiError, Result};
use crate::format::iri::IriCompactor;
use crate::query::helpers::{parse_jsonld_query, parse_sparql_to_ir};
use fluree_db_core::{is_rdf_type, StatsView};
use fluree_db_query::{
    explain_execution_hints, parse_query, ExplainPlan, OptimizationStatus, Pattern, Query, Ref,
    Term, TriplePattern, VarRegistry,
};
use serde_json::{json, Map, Value as JsonValue};

fn status_to_str(s: OptimizationStatus) -> &'static str {
    match s {
        OptimizationStatus::Reordered => "reordered",
        OptimizationStatus::Unchanged => "unchanged",
    }
}

fn ref_to_user_string(r: &Ref, vars: &VarRegistry, compactor: &IriCompactor) -> String {
    match r {
        Ref::Var(v) => vars.name(*v).to_string(),
        Ref::Sid(sid) => compactor.compact_vocab_iri(
            &compactor
                .decode_sid(sid)
                .unwrap_or_else(|_| sid.name.to_string()),
        ),
        Ref::Iri(iri) => compactor.compact_vocab_iri(iri),
    }
}

fn term_to_user_string(term: &Term, vars: &VarRegistry, compactor: &IriCompactor) -> String {
    match term {
        Term::Var(v) => vars.name(*v).to_string(),
        Term::Sid(sid) => compactor.compact_vocab_iri(
            &compactor
                .decode_sid(sid)
                .unwrap_or_else(|_| sid.name.to_string()),
        ),
        Term::Iri(iri) => compactor.compact_vocab_iri(iri),
        Term::Value(v) => match v {
            fluree_db_core::FlakeValue::String(s) => s.clone(),
            _ => format!("{v:?}"),
        },
    }
}

fn triple_pattern_to_user_object(
    tp: &TriplePattern,
    vars: &VarRegistry,
    compactor: &IriCompactor,
) -> JsonValue {
    let property = if let Ref::Sid(pred) = &tp.p {
        if is_rdf_type(pred) {
            "@type".to_string()
        } else {
            ref_to_user_string(&tp.p, vars, compactor)
        }
    } else {
        ref_to_user_string(&tp.p, vars, compactor)
    };

    json!({
        "subject": ref_to_user_string(&tp.s, vars, compactor),
        "property": property,
        "object": term_to_user_string(&tp.o, vars, compactor),
    })
}

fn plan_patterns_to_json(
    explain: &ExplainPlan,
    triples_in_order: &[TriplePattern],
    vars: &VarRegistry,
    compactor: &IriCompactor,
) -> (JsonValue, JsonValue) {
    let mut by_pattern_str: std::collections::HashMap<String, &fluree_db_query::PatternDisplay> =
        std::collections::HashMap::new();

    // Use the ExplainPlan's stable formatting of patterns to correlate inputs/scores.
    for pd in &explain.original_patterns {
        by_pattern_str.insert(pd.pattern.clone(), pd);
    }
    for pd in &explain.optimized_patterns {
        by_pattern_str.insert(pd.pattern.clone(), pd);
    }

    let to_entry = |tp: &TriplePattern| -> JsonValue {
        let key = fluree_db_query::explain::format_pattern(tp);
        let pd = by_pattern_str.get(&key);
        let selectivity = pd.map(|p| p.selectivity_score).unwrap_or(0);
        let pattern_type = pd
            .map(|p| p.pattern_type)
            .unwrap_or(fluree_db_query::planner::PatternType::PropertyScan);

        let typ = match pattern_type {
            fluree_db_query::planner::PatternType::ClassPattern => "class",
            _ => "triple",
        };

        // Inputs: keep close to the existing fields, but backed by our explain inputs.
        let mut inputs = Map::new();
        inputs.insert("type".to_string(), JsonValue::String(typ.to_string()));
        // Always include these flags for parity/stability; they will be overwritten
        // when NDV inputs are available.
        inputs.insert("used-values-ndv?".to_string(), json!(false));
        inputs.insert("clamped-to-one?".to_string(), json!(false));
        if let Some(inp) = pd.map(|p| &p.inputs) {
            if let Some(sid) = &inp.property_sid {
                inputs.insert("property-sid".to_string(), json!(sid));
            }
            if let Some(c) = inp.count {
                inputs.insert("count".to_string(), json!(c));
            }
            if let Some(n) = inp.ndv_values {
                inputs.insert("ndv-values".to_string(), json!(n));
                // used-values-ndv? is true if we have NDV and the object is bound constant
                let used = tp.o_bound() && n > 0;
                inputs.insert("used-values-ndv?".to_string(), json!(used));
                if let Some(c) = inp.count {
                    let sel = if n == 0 { 1 } else { c.div_ceil(n).max(1) };
                    let clamped = sel == 1 && c > 0 && n > c;
                    inputs.insert("clamped-to-one?".to_string(), json!(clamped));
                } else {
                    inputs.insert("clamped-to-one?".to_string(), json!(false));
                }
            } else if tp.o_bound() {
                // These flags are present for bound-object patterns even if NDV.
                // stats aren't available (they'll just be false).
                inputs.insert("used-values-ndv?".to_string(), json!(false));
                inputs.insert("clamped-to-one?".to_string(), json!(false));
            }
            if let Some(n) = inp.ndv_subjects {
                inputs.insert("ndv-subjects".to_string(), json!(n));
            }
            if let Some(cc) = inp.class_count {
                inputs.insert("class-count".to_string(), json!(cc));
            }
            inputs.insert("fallback".to_string(), json!(inp.fallback));
        }

        json!({
            "type": typ,
            "pattern": triple_pattern_to_user_object(tp, vars, compactor),
            "selectivity": selectivity,
            "inputs": JsonValue::Object(inputs),
        })
    };

    // Original order is the query's triple pattern order.
    let original = JsonValue::Array(triples_in_order.iter().map(to_entry).collect());

    // Optimized order is ExplainPlan's optimized order.
    let optimized = JsonValue::Array(
        explain
            .optimized_patterns
            .iter()
            .filter_map(|pd| {
                // parse the pd.pattern back into a TriplePattern isn't worth it; instead,
                // find the matching TriplePattern by its formatted string.
                triples_in_order
                    .iter()
                    .find(|tp| fluree_db_query::explain::format_pattern(tp) == pd.pattern)
            })
            .map(to_entry)
            .collect(),
    );

    (original, optimized)
}

/// Shared explain logic operating on an already-parsed query.
///
/// Both JSON-LD and SPARQL entry points parse into `(VarRegistry, Query)`
/// and then delegate here.  The `query_echo` value is placed in the `"query"`
/// field of the response (JSON-LD echoes the original JSON object; SPARQL echoes
/// the raw SPARQL string).  `where_clause` is optionally included in the
/// no-stats early-return path (only meaningful for JSON-LD).
fn explain_from_parsed(
    snapshot: &fluree_db_core::LedgerSnapshot,
    vars: &VarRegistry,
    parsed: &Query,
    query_echo: JsonValue,
    where_clause: Option<JsonValue>,
) -> Result<JsonValue> {
    let compactor = IriCompactor::new(snapshot.namespaces(), &parsed.context);

    // Extract triple patterns in query order.
    // Normalize any IRI terms into SID when possible so that
    // stats lookups (which are SID-keyed) work for explain/optimization parity.
    let normalize_ref = |r: &Ref| -> Ref {
        match r {
            Ref::Iri(iri) => snapshot
                .encode_iri(iri)
                .map(Ref::Sid)
                .unwrap_or_else(|| r.clone()),
            _ => r.clone(),
        }
    };
    let normalize_term = |t: &Term| -> Term {
        match t {
            Term::Iri(iri) => snapshot
                .encode_iri(iri)
                .map(Term::Sid)
                .unwrap_or_else(|| t.clone()),
            _ => t.clone(),
        }
    };

    fn collect_triples_in_order(
        out: &mut Vec<TriplePattern>,
        patterns: &[Pattern],
        normalize_ref: &impl Fn(&Ref) -> Ref,
        normalize_term: &impl Fn(&Term) -> Term,
    ) {
        for p in patterns {
            match p {
                Pattern::Triple(tp) => out.push(TriplePattern {
                    s: normalize_ref(&tp.s),
                    p: normalize_ref(&tp.p),
                    o: normalize_term(&tp.o),
                    dtc: tp.dtc.clone(),
                }),
                Pattern::Optional(inner)
                | Pattern::Minus(inner)
                | Pattern::Exists(inner)
                | Pattern::NotExists(inner) => {
                    collect_triples_in_order(out, inner, normalize_ref, normalize_term);
                }
                Pattern::Union(branches) => {
                    for branch in branches {
                        collect_triples_in_order(out, branch, normalize_ref, normalize_term);
                    }
                }
                Pattern::Graph { patterns, .. } => {
                    collect_triples_in_order(out, patterns, normalize_ref, normalize_term);
                }
                Pattern::Service(sp) => {
                    collect_triples_in_order(out, &sp.patterns, normalize_ref, normalize_term);
                }
                Pattern::Subquery(sq) => {
                    collect_triples_in_order(out, &sq.patterns, normalize_ref, normalize_term);
                }
                // Non-triple patterns (FILTER, BIND, VALUES, SEARCH, etc.) don't contribute
                // to triple-level selectivity scoring.
                _ => {}
            }
        }
    }

    let mut triples_in_order = Vec::new();
    collect_triples_in_order(
        &mut triples_in_order,
        &parsed.patterns,
        &normalize_ref,
        &normalize_term,
    );

    let stats_view = snapshot
        .stats
        .as_ref()
        .map(|s| StatsView::from_db_stats_with_namespaces(s, snapshot.namespaces()));
    let stats_available = stats_view
        .as_ref()
        .map(fluree_db_core::StatsView::has_property_stats)
        .unwrap_or(false);
    let execution_hints = explain_execution_hints(&parsed.patterns, stats_view.as_ref());

    if !stats_available {
        let mut plan = serde_json::Map::new();
        plan.insert("optimization".into(), json!("none"));
        plan.insert("reason".into(), json!("No statistics available"));
        plan.insert("execution-hints".into(), json!(execution_hints));
        if let Some(wc) = where_clause {
            plan.insert("where-clause".into(), wc);
        }
        return Ok(json!({
            "query": query_echo,
            "plan": JsonValue::Object(plan)
        }));
    }

    let explain = fluree_db_query::explain_patterns(&triples_in_order, stats_view.as_ref());
    let (original, optimized) =
        plan_patterns_to_json(&explain, &triples_in_order, vars, &compactor);

    // Minimal statistics summary (stable + useful).
    let stats = snapshot.stats.as_ref().unwrap();
    let statistics = json!({
        "total-flakes": stats.flakes,
    });

    Ok(json!({
        "query": query_echo,
        "plan": {
            "optimization": status_to_str(explain.optimization),
            "statistics-available": explain.statistics_available,
            "statistics": statistics,
            "execution-hints": execution_hints,
            "original": original,
            "optimized": optimized
        }
    }))
}

/// Explain a JSON-LD query against a LedgerSnapshot.
///
/// Returns a JSON object like:
/// `{ "query": <parsed/echo>, "plan": { ... } }`
pub async fn explain_jsonld(
    snapshot: &fluree_db_core::LedgerSnapshot,
    query_json: &JsonValue,
) -> Result<JsonValue> {
    let mut vars = VarRegistry::new();
    let parsed = parse_query(query_json, snapshot, &mut vars, None)
        .map_err(|e| ApiError::query(format!("Explain parse error: {e}")))?;

    let query_obj = query_json
        .as_object()
        .ok_or_else(|| ApiError::query("Query must be an object"))?;
    let where_clause = query_obj.get("where").cloned();

    explain_from_parsed(snapshot, &vars, &parsed, query_json.clone(), where_clause)
}

/// Explain a JSON-LD query against a LedgerSnapshot, using a default JSON-LD context.
///
/// This mirrors query execution behavior: if the query provides no `@context`,
/// `default_context` is used.
pub async fn explain_jsonld_with_default_context(
    snapshot: &fluree_db_core::LedgerSnapshot,
    query_json: &JsonValue,
    default_context: Option<&JsonValue>,
) -> Result<JsonValue> {
    let (vars, parsed) = parse_jsonld_query(query_json, snapshot, default_context, None)?;

    let query_obj = query_json
        .as_object()
        .ok_or_else(|| ApiError::query("Query must be an object"))?;
    let where_clause = query_obj.get("where").cloned();

    explain_from_parsed(snapshot, &vars, &parsed, query_json.clone(), where_clause)
}

/// Explain a SPARQL query against a LedgerSnapshot.
///
/// Returns a JSON object like:
/// `{ "query": "<sparql string>", "plan": { ... } }`
pub async fn explain_sparql(
    snapshot: &fluree_db_core::LedgerSnapshot,
    sparql: &str,
) -> Result<JsonValue> {
    let (vars, parsed) = parse_sparql_to_ir(sparql, snapshot, None)?;

    explain_from_parsed(snapshot, &vars, &parsed, json!(sparql), None)
}

/// Explain a SPARQL query against a LedgerSnapshot, using a default JSON-LD context.
///
/// The `default_context` is used for prefix expansion during parsing and for
/// user-facing IRI compaction in the plan output.
pub async fn explain_sparql_with_default_context(
    snapshot: &fluree_db_core::LedgerSnapshot,
    sparql: &str,
    default_context: Option<&JsonValue>,
) -> Result<JsonValue> {
    let (vars, parsed) = parse_sparql_to_ir(sparql, snapshot, default_context)?;

    explain_from_parsed(snapshot, &vars, &parsed, json!(sparql), None)
}
