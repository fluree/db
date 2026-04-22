//! TypedJson format
//!
//! Always includes explicit datatype (even for inferable types):
//! ```json
//! [
//!   {"?s": {"@id": "ex:alice"},
//!    "?name": {"@value": "Alice", "@type": "xsd:string"},
//!    "?age": {"@value": 30, "@type": "xsd:long"}}
//! ]
//! ```
//!
//! Key features:
//! - References use `{"@id": "..."}`
//! - Literals always have `{"@value": ..., "@type": "..."}`
//! - Language-tagged strings use `{"@value": ..., "@language": "..."}`
//! - IRIs are compacted using @context

use super::config::FormatterConfig;
use super::iri::IriCompactor;
use super::{FormatError, Result};
use crate::QueryResult;
use fluree_db_core::FlakeValue;
use fluree_db_query::binding::Binding;
use serde_json::{json, Map, Value as JsonValue};

/// Format query results in TypedJson format
pub fn format(
    result: &QueryResult,
    compactor: &IriCompactor,
    _config: &FormatterConfig,
) -> Result<JsonValue> {
    let select_one = result.output.is_select_one();
    let mut rows = Vec::new();

    for batch in &result.batches {
        for row_idx in 0..batch.len() {
            let row = if result.output.is_wildcard() {
                // Wildcard: use batch schema, return all bound vars as object
                format_row_wildcard(batch, row_idx, &result.vars, compactor, result)?
            } else {
                format_row(
                    batch,
                    row_idx,
                    result.output.select_vars_or_empty(),
                    &result.vars,
                    compactor,
                    result,
                )?
            };
            rows.push(row);

            if select_one {
                break;
            }
        }
        if select_one && !rows.is_empty() {
            break;
        }
    }

    if select_one {
        Ok(rows.into_iter().next().unwrap_or(JsonValue::Null))
    } else {
        Ok(JsonValue::Array(rows))
    }
}

/// Format a single binding to TypedJson
pub(crate) fn format_binding(
    result: &QueryResult,
    binding: &Binding,
    compactor: &IriCompactor,
) -> Result<JsonValue> {
    // Late materialization for encoded bindings.
    if binding.is_encoded() {
        let materialized = super::materialize::materialize_binding(result, binding)?;
        return format_binding(result, &materialized, compactor);
    }

    match binding {
        Binding::Unbound | Binding::Poisoned => Ok(JsonValue::Null),

        // Reference - use @id notation
        Binding::Sid(sid) => {
            let iri = compactor.compact_sid(sid)?;
            Ok(json!({"@id": iri}))
        }

        // IriMatch: use canonical IRI, then compact (multi-ledger mode)
        Binding::IriMatch { iri, .. } => {
            let compacted = compactor.compact_iri(iri)?;
            Ok(json!({"@id": compacted}))
        }

        // Raw IRI string (from graph source, not in namespace table)
        Binding::Iri(iri) => Ok(json!({"@id": iri.as_ref()})),

        // Literal value - always include @type (except language-tagged)
        Binding::Lit { val, dtc, .. } => {
            let dt_iri = compactor.compact_sid(dtc.datatype())?;

            match val {
                FlakeValue::String(s) => {
                    if let Some(lang_tag) = dtc.lang_tag() {
                        // Language-tagged string - use @language instead of @type
                        Ok(json!({
                            "@value": s,
                            "@language": lang_tag
                        }))
                    } else {
                        Ok(json!({
                            "@value": s,
                            "@type": dt_iri
                        }))
                    }
                }
                FlakeValue::Long(n) => Ok(json!({
                    "@value": n,
                    "@type": dt_iri
                })),
                FlakeValue::Double(d) => {
                    // Handle special float values
                    if d.is_nan() || d.is_infinite() {
                        let value_str = if d.is_nan() {
                            "NaN"
                        } else if d.is_sign_positive() {
                            "INF"
                        } else {
                            "-INF"
                        };
                        Ok(json!({
                            "@value": value_str,
                            "@type": dt_iri
                        }))
                    } else {
                        Ok(json!({
                            "@value": d,
                            "@type": dt_iri
                        }))
                    }
                }
                FlakeValue::Boolean(b) => Ok(json!({
                    "@value": b,
                    "@type": dt_iri
                })),
                FlakeValue::Vector(v) => Ok(json!({
                    "@value": v,
                    "@type": dt_iri
                })),
                FlakeValue::Json(json_str) => {
                    // @json datatype: deserialize for output
                    let json_val: JsonValue = serde_json::from_str(json_str).map_err(|e| {
                        FormatError::InvalidBinding(format!("Invalid JSON in @json value: {e}"))
                    })?;
                    Ok(json!({
                        "@value": json_val,
                        "@type": "@json"
                    }))
                }
                FlakeValue::Null => Ok(JsonValue::Null),
                FlakeValue::Ref(_) => {
                    // This should never happen due to Binding invariant
                    Err(FormatError::InvalidBinding(
                        "Binding::Lit invariant violated: contains Ref".to_string(),
                    ))
                }
                // Extended numeric types - serialize as string with datatype
                FlakeValue::BigInt(n) => Ok(json!({
                    "@value": n.to_string(),
                    "@type": dt_iri
                })),
                FlakeValue::Decimal(d) => Ok(json!({
                    "@value": d.to_string(),
                    "@type": dt_iri
                })),
                // Temporal types - serialize as original string with datatype
                FlakeValue::DateTime(dt) => Ok(json!({
                    "@value": dt.to_string(),
                    "@type": dt_iri
                })),
                FlakeValue::Date(d) => Ok(json!({
                    "@value": d.to_string(),
                    "@type": dt_iri
                })),
                FlakeValue::Time(t) => Ok(json!({
                    "@value": t.to_string(),
                    "@type": dt_iri
                })),
                // Additional temporal types - serialize as original string with datatype
                FlakeValue::GYear(v) => Ok(json!({
                    "@value": v.to_string(),
                    "@type": dt_iri
                })),
                FlakeValue::GYearMonth(v) => Ok(json!({
                    "@value": v.to_string(),
                    "@type": dt_iri
                })),
                FlakeValue::GMonth(v) => Ok(json!({
                    "@value": v.to_string(),
                    "@type": dt_iri
                })),
                FlakeValue::GDay(v) => Ok(json!({
                    "@value": v.to_string(),
                    "@type": dt_iri
                })),
                FlakeValue::GMonthDay(v) => Ok(json!({
                    "@value": v.to_string(),
                    "@type": dt_iri
                })),
                FlakeValue::YearMonthDuration(v) => Ok(json!({
                    "@value": v.to_string(),
                    "@type": dt_iri
                })),
                FlakeValue::DayTimeDuration(v) => Ok(json!({
                    "@value": v.to_string(),
                    "@type": dt_iri
                })),
                FlakeValue::Duration(v) => Ok(json!({
                    "@value": v.to_string(),
                    "@type": dt_iri
                })),
                FlakeValue::GeoPoint(v) => Ok(json!({
                    "@value": v.to_string(),
                    "@type": dt_iri
                })),
            }
        }

        Binding::EncodedLit { .. } | Binding::EncodedSid { .. } | Binding::EncodedPid { .. } => {
            unreachable!(
                "Encoded bindings should have been materialized before TypedJson formatting"
            )
        }

        // Grouped values - format as array of typed values
        Binding::Grouped(values) => {
            let arr: Result<Vec<_>> = values
                .iter()
                .map(|v| format_binding(result, v, compactor))
                .collect();
            Ok(JsonValue::Array(arr?))
        }
    }
}

// NOTE: encoded binding materialization is centralized in `format::materialize`.

/// Format a single binding to TypedJson, materializing encoded bindings first.
pub(crate) fn format_binding_with_result(
    result: &QueryResult,
    binding: &Binding,
    compactor: &IriCompactor,
) -> Result<JsonValue> {
    if binding.is_encoded() {
        let materialized = super::materialize::materialize_binding(result, binding)?;
        return format_binding_with_result(result, &materialized, compactor);
    }
    format_binding(result, binding, compactor)
}

/// Format a single row as object {var: typed_value}
fn format_row(
    batch: &fluree_db_query::Batch,
    row_idx: usize,
    select: &[fluree_db_query::VarId],
    vars: &fluree_db_query::VarRegistry,
    compactor: &IriCompactor,
    result: &QueryResult,
) -> Result<JsonValue> {
    let mut obj = Map::new();

    for &var_id in select {
        let var_name = vars.name(var_id);
        let value = match batch.get(row_idx, var_id) {
            Some(binding) => format_binding(result, binding, compactor)?,
            None => JsonValue::Null,
        };
        obj.insert(var_name.to_string(), value);
    }

    Ok(JsonValue::Object(obj))
}

/// Format row for wildcard select (all bound variables as object)
///
/// Uses batch.schema() to get all variables, omits unbound/poisoned.
fn format_row_wildcard(
    batch: &fluree_db_query::Batch,
    row_idx: usize,
    vars: &fluree_db_query::VarRegistry,
    compactor: &IriCompactor,
    result: &QueryResult,
) -> Result<JsonValue> {
    let mut obj = Map::new();

    for &var_id in batch.schema() {
        if let Some(binding) = batch.get(row_idx, var_id) {
            if matches!(binding, Binding::Unbound | Binding::Poisoned) {
                continue;
            }
            let var_name = vars.name(var_id);

            // Skip internal variables (e.g. ?__pp0, ?__s0, ?__n0) from wildcard output.
            // The ?__ prefix is reserved for internal use.
            if var_name.starts_with("?__") {
                continue;
            }

            let value = format_binding(result, binding, compactor)?;
            obj.insert(var_name.to_string(), value);
        }
    }

    Ok(JsonValue::Object(obj))
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
    /// Used for testing format_binding with non-encoded bindings.
    fn make_test_result() -> QueryResult {
        QueryResult {
            vars: crate::VarRegistry::new(),
            t: Some(0),
            novelty: None,
            context: crate::ParsedContext::default(),
            orig_context: None,
            output: crate::QueryOutput::Select(vec![]),
            batches: vec![],
            binary_graph: None,
            graph_select: None,
        }
    }

    #[test]
    fn test_format_binding_sid() {
        let compactor = make_test_compactor();
        let result = make_test_result();
        let binding = Binding::Sid(Sid::new(100, "alice"));
        let formatted = format_binding(&result, &binding, &compactor).unwrap();
        assert_eq!(formatted, json!({"@id": "http://example.org/alice"}));
    }

    #[test]
    fn test_format_binding_string() {
        let compactor = make_test_compactor();
        let result = make_test_result();
        let binding = Binding::lit(
            FlakeValue::String("Alice".to_string()),
            Sid::new(2, "string"),
        );
        let formatted = format_binding(&result, &binding, &compactor).unwrap();
        assert_eq!(
            formatted,
            json!({"@value": "Alice", "@type": "http://www.w3.org/2001/XMLSchema#string"})
        );
    }

    #[test]
    fn test_format_binding_long() {
        let compactor = make_test_compactor();
        let result = make_test_result();
        let binding = Binding::lit(FlakeValue::Long(42), Sid::new(2, "long"));
        let formatted = format_binding(&result, &binding, &compactor).unwrap();
        assert_eq!(
            formatted,
            json!({"@value": 42, "@type": "http://www.w3.org/2001/XMLSchema#long"})
        );
    }

    #[test]
    fn test_format_binding_double() {
        let compactor = make_test_compactor();
        let result = make_test_result();
        let binding = Binding::lit(FlakeValue::Double(3.5), Sid::new(2, "double"));
        let formatted = format_binding(&result, &binding, &compactor).unwrap();
        assert_eq!(
            formatted,
            json!({"@value": 3.5, "@type": "http://www.w3.org/2001/XMLSchema#double"})
        );
    }

    #[test]
    fn test_format_binding_boolean() {
        let compactor = make_test_compactor();
        let result = make_test_result();
        let binding = Binding::lit(FlakeValue::Boolean(true), Sid::new(2, "boolean"));
        let formatted = format_binding(&result, &binding, &compactor).unwrap();
        assert_eq!(
            formatted,
            json!({"@value": true, "@type": "http://www.w3.org/2001/XMLSchema#boolean"})
        );
    }

    #[test]
    fn test_format_binding_language_tagged() {
        let compactor = make_test_compactor();
        let result = make_test_result();
        let binding = Binding::lit_lang(FlakeValue::String("Hello".to_string()), "en");
        let formatted = format_binding(&result, &binding, &compactor).unwrap();
        // Language-tagged strings use @language, not @type
        assert_eq!(formatted, json!({"@value": "Hello", "@language": "en"}));
    }

    #[test]
    fn test_format_binding_unbound() {
        let compactor = make_test_compactor();
        let result = make_test_result();
        let binding = Binding::Unbound;
        let formatted = format_binding(&result, &binding, &compactor).unwrap();
        assert_eq!(formatted, JsonValue::Null);
    }

    #[test]
    fn test_format_binding_grouped() {
        let compactor = make_test_compactor();
        let result = make_test_result();
        let binding = Binding::Grouped(vec![
            Binding::lit(FlakeValue::Long(1), Sid::new(2, "long")),
            Binding::lit(FlakeValue::Long(2), Sid::new(2, "long")),
        ]);
        let formatted = format_binding(&result, &binding, &compactor).unwrap();
        assert_eq!(
            formatted,
            json!([
                {"@value": 1, "@type": "http://www.w3.org/2001/XMLSchema#long"},
                {"@value": 2, "@type": "http://www.w3.org/2001/XMLSchema#long"}
            ])
        );
    }

    #[test]
    fn test_format_binding_double_special() {
        let compactor = make_test_compactor();
        let result = make_test_result();

        // NaN
        let binding = Binding::lit(FlakeValue::Double(f64::NAN), Sid::new(2, "double"));
        let formatted = format_binding(&result, &binding, &compactor).unwrap();
        assert_eq!(
            formatted,
            json!({"@value": "NaN", "@type": "http://www.w3.org/2001/XMLSchema#double"})
        );

        // Infinity
        let binding = Binding::lit(FlakeValue::Double(f64::INFINITY), Sid::new(2, "double"));
        let formatted = format_binding(&result, &binding, &compactor).unwrap();
        assert_eq!(
            formatted,
            json!({"@value": "INF", "@type": "http://www.w3.org/2001/XMLSchema#double"})
        );
    }
}
