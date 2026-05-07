//! SPARQL 1.1 Query Results JSON format
//!
//! W3C standard format with type metadata:
//! ```json
//! {
//!   "head": {"vars": ["s", "name"]},
//!   "results": {"bindings": [{
//!     "s": {"type": "uri", "value": "http://example.org/alice"},
//!     "name": {"type": "literal", "value": "Alice"}
//!   }]}
//! }
//! ```
//!
//! Key features:
//! - Variable names without `?` prefix
//! - Each binding: `{"type": "literal|uri|bnode", "value": "...", "datatype"?: "...", "xml:lang"?: "..."}`
//! - Omit datatype for inferable types (xsd:string, xsd:integer, xsd:double, xsd:boolean)
//! - Disaggregation: `Binding::Grouped` explodes into multiple rows (cartesian product)

use super::config::FormatterConfig;
use super::datatype::is_inferable_datatype;
use super::iri::IriCompactor;
use super::{FormatError, Result};
use crate::QueryResult;
use fluree_db_core::FlakeValue;
use fluree_db_query::binding::Binding;
use fluree_db_query::VarRegistry;
use serde_json::{json, Map, Value as JsonValue};

/// Format query results in SPARQL 1.1 JSON format
pub fn format(
    result: &QueryResult,
    compactor: &IriCompactor,
    _config: &FormatterConfig,
) -> Result<JsonValue> {
    // Build head.vars from select list (without ? prefix).
    // For wildcard, use the operator schema (all variables).
    // Fall back to VarRegistry when batches are empty (W3C: exists-02 etc.).
    let head_vars: Vec<fluree_db_query::VarId> = if result.output.is_wildcard() {
        result
            .batches
            .first()
            .map(|b| {
                b.schema()
                    .iter()
                    .copied()
                    // Skip internal variables (?__pp0, ?__s0, etc.) from wildcard output.
                    .filter(|&vid| !result.vars.name(vid).starts_with("?__"))
                    .collect()
            })
            .unwrap_or_else(|| {
                // Empty result set: derive vars from the registry (all user-visible variables).
                result
                    .vars
                    .iter()
                    .filter(|(name, _)| !name.starts_with("?__"))
                    .map(|(_, id)| id)
                    .collect()
            })
    } else {
        result.output.projected_vars_or_empty()
    };

    // Order head vars lexicographically by variable name (without '?').
    // This also stabilizes output across planner reorderings.
    let mut head_pairs: Vec<(String, fluree_db_query::VarId)> = head_vars
        .iter()
        .map(|&var_id| (strip_question_mark(result.vars.name(var_id)), var_id))
        .collect();
    head_pairs.sort_by(|(a, _), (b, _)| a.cmp(b));

    let vars: Vec<String> = head_pairs.iter().map(|(name, _)| name.clone()).collect();
    let head_vars: Vec<fluree_db_query::VarId> = head_pairs.into_iter().map(|(_, id)| id).collect();

    let select_one = result.output.is_select_one();
    let mut bindings = Vec::new();

    for batch in &result.batches {
        for row_idx in 0..batch.len() {
            // Collect bindings for this row
            let row_bindings: Vec<_> = head_vars
                .iter()
                .map(|&var_id| {
                    let binding = batch.get(row_idx, var_id).unwrap_or(&Binding::Unbound);
                    (var_id, binding)
                })
                .collect();

            // Disaggregate grouped bindings (cartesian product)
            let disaggregated = disaggregate_row(result, &row_bindings, &result.vars, compactor)?;
            if select_one {
                // SelectOne: only return a single formatted row (after disaggregation)
                if let Some(first) = disaggregated.into_iter().next() {
                    bindings.push(first);
                }
                break;
            }
            bindings.extend(disaggregated);
        }
        if select_one && !bindings.is_empty() {
            break;
        }
    }

    // Build the SPARQL JSON structure
    Ok(json!({
        "head": {"vars": vars},
        "results": {"bindings": bindings}
    }))
}

/// Strip the leading '?' from a variable name
fn strip_question_mark(var_name: &str) -> String {
    var_name.strip_prefix('?').unwrap_or(var_name).to_string()
}

/// Format a single binding to SPARQL JSON format
///
/// Returns None for Unbound/Poisoned (omit from output per SPARQL spec)
fn format_binding(
    result: &QueryResult,
    binding: &Binding,
    compactor: &IriCompactor,
) -> Result<Option<JsonValue>> {
    // Late materialization for encoded bindings.
    if binding.is_encoded() {
        let materialized = super::materialize::materialize_binding(result, binding)?;
        return format_binding(result, &materialized, compactor);
    }

    match binding {
        // Unbound/Poisoned: omit from SPARQL JSON (not an error, just absent)
        Binding::Unbound | Binding::Poisoned => Ok(None),

        // Reference (IRI or blank node)
        Binding::Sid { sid, .. } => {
            // SPARQL JSON output uses compact IRIs where possible (not full IRIs).
            let iri = compactor.compact_sid(sid)?;
            // Check if it's a blank node (starts with _:)
            if iri.starts_with("_:") {
                Ok(Some(json!({
                    "type": "bnode",
                    "value": iri.strip_prefix("_:").unwrap_or(&iri)
                })))
            } else {
                Ok(Some(json!({
                    "type": "uri",
                    "value": iri
                })))
            }
        }

        // IriMatch: use canonical IRI, then compact (multi-ledger mode)
        Binding::IriMatch { iri, .. } => {
            let compacted = compactor.compact_iri(iri)?;
            if compacted.starts_with("_:") {
                Ok(Some(json!({
                    "type": "bnode",
                    "value": compacted.strip_prefix("_:").unwrap_or(&compacted)
                })))
            } else {
                Ok(Some(json!({
                    "type": "uri",
                    "value": compacted
                })))
            }
        }

        // Raw IRI string (from graph source, not in namespace table)
        Binding::Iri(iri) => {
            // Check if it's a blank node (starts with _:)
            if iri.starts_with("_:") {
                Ok(Some(json!({
                    "type": "bnode",
                    "value": iri.strip_prefix("_:").unwrap_or(iri.as_ref())
                })))
            } else {
                Ok(Some(json!({
                    "type": "uri",
                    "value": iri.as_ref()
                })))
            }
        }

        // Literal value
        Binding::Lit { val, dtc, .. } => {
            let dt = dtc.datatype();
            // Decode datatype sid to full IRI
            let dt_iri = compactor.decode_sid(dt)?;

            match val {
                FlakeValue::String(s) => {
                    if let Some(lang_tag) = dtc.lang_tag() {
                        // Language-tagged string
                        Ok(Some(json!({
                            "type": "literal",
                            "value": s,
                            "xml:lang": lang_tag
                        })))
                    } else if is_inferable_datatype(&dt_iri) {
                        // Inferable type - omit datatype
                        Ok(Some(json!({
                            "type": "literal",
                            "value": s
                        })))
                    } else {
                        // Non-inferable type - include datatype
                        Ok(Some(json!({
                            "type": "literal",
                            "value": s,
                            "datatype": dt_iri
                        })))
                    }
                }
                FlakeValue::Long(n) => Ok(Some(json!({
                    "type": "literal",
                    "value": n.to_string(),
                    "datatype": dt_iri
                }))),
                FlakeValue::Double(d) => {
                    // Handle special float values
                    let value_str = if d.is_nan() {
                        "NaN".to_string()
                    } else if d.is_infinite() {
                        if d.is_sign_positive() {
                            "INF".to_string()
                        } else {
                            "-INF".to_string()
                        }
                    } else {
                        d.to_string()
                    };
                    // Include datatype for numeric literals.
                    Ok(Some(json!({
                        "type": "literal",
                        "value": value_str,
                        "datatype": dt_iri
                    })))
                }
                FlakeValue::Boolean(b) => {
                    // Include datatype for non-string literals.
                    Ok(Some(json!({
                        "type": "literal",
                        "value": b.to_string(),
                        "datatype": dt_iri
                    })))
                }
                FlakeValue::Vector(v) => {
                    // SPARQL Query Results JSON does not have an array literal type.
                    // Encode as a stringified JSON array and preserve the datatype IRI.
                    let value = serde_json::to_string(v).unwrap_or_else(|_| "[]".to_string());
                    Ok(Some(json!({
                        "type": "literal",
                        "value": value,
                        "datatype": dt_iri
                    })))
                }
                FlakeValue::Json(json_str) => {
                    // SPARQL JSON uses the full RDF datatype IRI (not the JSON-LD "@json" keyword)
                    Ok(Some(json!({
                        "type": "literal",
                        "value": json_str,
                        "datatype": dt_iri
                    })))
                }
                FlakeValue::Null => Ok(None),
                FlakeValue::Ref(_) => {
                    // This should never happen due to Binding invariant
                    Err(FormatError::InvalidBinding(
                        "Binding::Lit invariant violated: contains Ref".to_string(),
                    ))
                }
                // Extended numeric types
                FlakeValue::BigInt(n) => Ok(Some(json!({
                    "type": "literal",
                    "value": n.to_string(),
                    "datatype": dt_iri
                }))),
                FlakeValue::Decimal(d) => Ok(Some(json!({
                    "type": "literal",
                    "value": d.to_string(),
                    "datatype": dt_iri
                }))),
                // Temporal types
                FlakeValue::DateTime(dt) => Ok(Some(json!({
                    "type": "literal",
                    "value": dt.to_string(),
                    "datatype": dt_iri
                }))),
                FlakeValue::Date(d) => Ok(Some(json!({
                    "type": "literal",
                    "value": d.to_string(),
                    "datatype": dt_iri
                }))),
                FlakeValue::Time(t) => Ok(Some(json!({
                    "type": "literal",
                    "value": t.to_string(),
                    "datatype": dt_iri
                }))),
                // Additional temporal types
                FlakeValue::GYear(v) => Ok(Some(json!({
                    "type": "literal",
                    "value": v.to_string(),
                    "datatype": dt_iri
                }))),
                FlakeValue::GYearMonth(v) => Ok(Some(json!({
                    "type": "literal",
                    "value": v.to_string(),
                    "datatype": dt_iri
                }))),
                FlakeValue::GMonth(v) => Ok(Some(json!({
                    "type": "literal",
                    "value": v.to_string(),
                    "datatype": dt_iri
                }))),
                FlakeValue::GDay(v) => Ok(Some(json!({
                    "type": "literal",
                    "value": v.to_string(),
                    "datatype": dt_iri
                }))),
                FlakeValue::GMonthDay(v) => Ok(Some(json!({
                    "type": "literal",
                    "value": v.to_string(),
                    "datatype": dt_iri
                }))),
                FlakeValue::YearMonthDuration(v) => Ok(Some(json!({
                    "type": "literal",
                    "value": v.to_string(),
                    "datatype": dt_iri
                }))),
                FlakeValue::DayTimeDuration(v) => Ok(Some(json!({
                    "type": "literal",
                    "value": v.to_string(),
                    "datatype": dt_iri
                }))),
                FlakeValue::Duration(v) => Ok(Some(json!({
                    "type": "literal",
                    "value": v.to_string(),
                    "datatype": dt_iri
                }))),
                FlakeValue::GeoPoint(v) => Ok(Some(json!({
                    "type": "literal",
                    "value": v.to_string(),
                    "datatype": dt_iri
                }))),
            }
        }

        // Grouped values should be disaggregated before reaching here
        Binding::Grouped(_) => Err(FormatError::InvalidBinding(
            "Binding::Grouped should be disaggregated before formatting".to_string(),
        )),

        Binding::EncodedLit { .. } | Binding::EncodedSid { .. } | Binding::EncodedPid { .. } => {
            unreachable!(
                "Encoded bindings should have been materialized before SPARQL JSON formatting"
            )
        }
    }
}

// NOTE: encoded binding materialization is centralized in `format::materialize`.

/// Disaggregate grouped bindings into multiple rows (cartesian product)
///
/// Input row with Grouped columns: {a: [1,2], b: [x,y]}
/// Output: [{a:1, b:x}, {a:1, b:y}, {a:2, b:x}, {a:2, b:y}]
fn disaggregate_row(
    result: &QueryResult,
    bindings: &[(fluree_db_query::VarId, &Binding)],
    vars: &VarRegistry,
    compactor: &IriCompactor,
) -> Result<Vec<JsonValue>> {
    // Separate grouped from scalar columns
    let mut grouped_cols: Vec<(fluree_db_query::VarId, &[Binding])> = Vec::new();
    let mut scalar_cols: Vec<(fluree_db_query::VarId, &Binding)> = Vec::new();

    for &(var_id, binding) in bindings {
        match binding {
            Binding::Grouped(values) => grouped_cols.push((var_id, values)),
            _ => scalar_cols.push((var_id, binding)),
        }
    }

    if grouped_cols.is_empty() {
        // No grouped columns - single row output
        let row = format_sparql_row(result, &scalar_cols, vars, compactor)?;
        return Ok(vec![JsonValue::Object(row)]);
    }

    // Start with a single empty row
    let mut results: Vec<Map<String, JsonValue>> = vec![Map::new()];

    // Add scalar columns to all result rows
    for (var_id, binding) in &scalar_cols {
        if let Some(formatted) = format_binding(result, binding, compactor)? {
            let var_name = strip_question_mark(vars.name(*var_id));
            for row in &mut results {
                row.insert(var_name.clone(), formatted.clone());
            }
        }
    }

    // Expand grouped columns via cartesian product
    for (var_id, values) in grouped_cols {
        let var_name = strip_question_mark(vars.name(var_id));
        let mut new_results = Vec::new();

        for row in results {
            for val in values {
                let mut new_row = row.clone();
                if let Some(formatted) = format_binding(result, val, compactor)? {
                    new_row.insert(var_name.clone(), formatted);
                }
                new_results.push(new_row);
            }
        }
        results = new_results;
    }

    Ok(results.into_iter().map(JsonValue::Object).collect())
}

/// Format a single row of scalar bindings to a SPARQL JSON binding object
fn format_sparql_row(
    result: &QueryResult,
    bindings: &[(fluree_db_query::VarId, &Binding)],
    vars: &VarRegistry,
    compactor: &IriCompactor,
) -> Result<Map<String, JsonValue>> {
    let mut obj = Map::new();

    for &(var_id, binding) in bindings {
        if let Some(formatted) = format_binding(result, binding, compactor)? {
            let var_name = strip_question_mark(vars.name(var_id));
            obj.insert(var_name, formatted);
        }
    }

    Ok(obj)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::Sid;
    use std::collections::HashMap;

    fn make_test_compactor() -> IriCompactor {
        let mut namespaces = HashMap::new();
        namespaces.insert(2, "http://www.w3.org/2001/XMLSchema#".to_string());
        namespaces.insert(3, "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string());
        namespaces.insert(100, "http://example.org/".to_string());
        IriCompactor::from_namespaces(&namespaces)
    }

    /// Create a minimal QueryResult for tests that don't need binary_store.
    fn make_test_result() -> QueryResult {
        QueryResult {
            vars: VarRegistry::new(),
            t: Some(0),
            novelty: None,
            context: crate::ParsedContext::default(),
            orig_context: None,
            output: crate::QueryOutput::select_all(vec![]),
            batches: vec![],
            binary_graph: None,
        }
    }

    #[test]
    fn test_strip_question_mark() {
        assert_eq!(strip_question_mark("?name"), "name");
        assert_eq!(strip_question_mark("name"), "name");
        assert_eq!(strip_question_mark("?"), "");
    }

    #[test]
    fn test_format_binding_uri() {
        let compactor = make_test_compactor();
        let result = make_test_result();
        let binding = Binding::sid(Sid::new(100, "alice"));
        let formatted = format_binding(&result, &binding, &compactor)
            .unwrap()
            .unwrap();
        assert_eq!(
            formatted,
            json!({"type": "uri", "value": "http://example.org/alice"})
        );
    }

    #[test]
    fn test_format_binding_literal_string() {
        let compactor = make_test_compactor();
        let result = make_test_result();
        let binding = Binding::lit(
            FlakeValue::String("Alice".to_string()),
            Sid::new(2, "string"),
        );
        let formatted = format_binding(&result, &binding, &compactor)
            .unwrap()
            .unwrap();
        // xsd:string is inferable, so no datatype
        assert_eq!(formatted, json!({"type": "literal", "value": "Alice"}));
    }

    #[test]
    fn test_format_binding_literal_long() {
        let compactor = make_test_compactor();
        let result = make_test_result();
        let binding = Binding::lit(FlakeValue::Long(42), Sid::new(2, "long"));
        let formatted = format_binding(&result, &binding, &compactor)
            .unwrap()
            .unwrap();
        // SPARQL JSON includes datatype for typed literals
        assert_eq!(
            formatted,
            json!({"type": "literal", "value": "42", "datatype": "http://www.w3.org/2001/XMLSchema#long"})
        );
    }

    #[test]
    fn test_format_binding_literal_boolean() {
        let compactor = make_test_compactor();
        let result = make_test_result();
        let binding = Binding::lit(FlakeValue::Boolean(true), Sid::new(2, "boolean"));
        let formatted = format_binding(&result, &binding, &compactor)
            .unwrap()
            .unwrap();
        assert_eq!(
            formatted,
            json!({"type": "literal", "value": "true", "datatype": "http://www.w3.org/2001/XMLSchema#boolean"})
        );
    }

    #[test]
    fn test_format_binding_language_tagged() {
        let compactor = make_test_compactor();
        let result = make_test_result();
        let binding = Binding::lit_lang(FlakeValue::String("Hello".to_string()), "en");
        let formatted = format_binding(&result, &binding, &compactor)
            .unwrap()
            .unwrap();
        assert_eq!(
            formatted,
            json!({"type": "literal", "value": "Hello", "xml:lang": "en"})
        );
    }

    #[test]
    fn test_format_binding_non_inferable_datatype() {
        let compactor = make_test_compactor();
        let result = make_test_result();
        let binding = Binding::lit(
            FlakeValue::String("2024-01-15".to_string()),
            Sid::new(2, "date"),
        );
        let formatted = format_binding(&result, &binding, &compactor)
            .unwrap()
            .unwrap();
        // xsd:date is NOT inferable, so include datatype
        assert_eq!(
            formatted,
            json!({
                "type": "literal",
                "value": "2024-01-15",
                "datatype": "http://www.w3.org/2001/XMLSchema#date"
            })
        );
    }

    #[test]
    fn test_format_binding_unbound() {
        let compactor = make_test_compactor();
        let result = make_test_result();
        let binding = Binding::Unbound;
        let formatted = format_binding(&result, &binding, &compactor).unwrap();
        assert!(formatted.is_none());
    }

    #[test]
    fn test_format_binding_double_special_values() {
        let compactor = make_test_compactor();
        let result = make_test_result();

        // NaN
        let binding = Binding::lit(FlakeValue::Double(f64::NAN), Sid::new(2, "double"));
        let formatted = format_binding(&result, &binding, &compactor)
            .unwrap()
            .unwrap();
        assert_eq!(
            formatted,
            json!({"type": "literal", "value": "NaN", "datatype": "http://www.w3.org/2001/XMLSchema#double"})
        );

        // Positive infinity
        let binding = Binding::lit(FlakeValue::Double(f64::INFINITY), Sid::new(2, "double"));
        let formatted = format_binding(&result, &binding, &compactor)
            .unwrap()
            .unwrap();
        assert_eq!(
            formatted,
            json!({"type": "literal", "value": "INF", "datatype": "http://www.w3.org/2001/XMLSchema#double"})
        );

        // Negative infinity
        let binding = Binding::lit(FlakeValue::Double(f64::NEG_INFINITY), Sid::new(2, "double"));
        let formatted = format_binding(&result, &binding, &compactor)
            .unwrap()
            .unwrap();
        assert_eq!(
            formatted,
            json!({"type": "literal", "value": "-INF", "datatype": "http://www.w3.org/2001/XMLSchema#double"})
        );
    }
}
