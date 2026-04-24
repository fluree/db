//! JSON-LD Query format
//!
//! Simple JSON format with compact IRIs. Rows are arrays aligned to SELECT order:
//! `[["ex:alice", "Alice", 30], ...]`
//!
//! Row shaping is driven by `ProjectionShape`:
//! - `Tuple` (default; SPARQL and JSON-LD array-form select): every row stays
//!   as an array, preserving tabular semantics.
//! - `Scalar` (JSON-LD bare-string `select: "?x"` only): 1-var rows flatten
//!   to bare values — opt-in scalar output.

use super::config::FormatterConfig;
use super::datatype::is_inferable_datatype;
use super::iri::IriCompactor;
use super::{FormatError, Result};
use crate::QueryResult;
use fluree_db_core::FlakeValue;
use fluree_db_query::binding::Binding;
use fluree_vocab::rdf;
use serde_json::{json, Map, Value as JsonValue};

/// Format query results in JSON-LD Query format
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
                format_row_wildcard(result, batch, row_idx, &result.vars, compactor)?
            } else {
                format_row_array(
                    result,
                    batch,
                    row_idx,
                    result.output.select_vars_or_empty(),
                    compactor,
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

    // Scalar shaping: flatten 1-var rows to bare values. Fires only for
    // JSON-LD `select: "?x"` (bare-string form), which is the user's opt-in
    // to scalar output. SPARQL and JSON-LD array-form select use `Tuple`
    // and skip this step — their rows stay tabular.
    if result.output.should_flatten_scalar() {
        rows = rows
            .into_iter()
            .map(|row| match row {
                JsonValue::Array(mut a) if a.len() == 1 => a.remove(0),
                other => other,
            })
            .collect();
    }

    if select_one {
        Ok(rows.into_iter().next().unwrap_or(JsonValue::Null))
    } else {
        Ok(JsonValue::Array(rows))
    }
}

/// Format a single binding to JSON-LD Query JSON
///
/// Note: Binding::Lit NEVER contains FlakeValue::Ref (Rust invariant)
pub(crate) fn format_binding(binding: &Binding, compactor: &IriCompactor) -> Result<JsonValue> {
    match binding {
        Binding::Unbound | Binding::Poisoned => Ok(JsonValue::Null),

        // Reference (IRI or blank node) - compact using @context
        Binding::Sid(sid) => Ok(JsonValue::String(compactor.compact_sid(sid)?)),

        // IriMatch: use canonical IRI, then compact (multi-ledger mode)
        Binding::IriMatch { iri, .. } => Ok(JsonValue::String(compactor.compact_iri(iri)?)),

        // Raw IRI string (from graph source, not in namespace table)
        // Output as-is without compaction (no namespace mapping available)
        Binding::Iri(iri) => Ok(JsonValue::String(iri.to_string())),

        // Literal value - never contains Ref (enforced by Binding::from_object)
        Binding::Lit { val, dtc, .. } => {
            let dt = dtc.datatype();
            // Full datatype IRI string (e.g., "http://www.w3.org/2001/XMLSchema#string" or "@json")
            let dt_full = compactor.decode_sid(dt)?;
            // Compact datatype for presentation in JSON-LD Query format.
            let dt_compact = compactor.compact_sid(dt)?;

            // Special handling for @json datatype: deserialize the JSON string.
            // Accept both FlakeValue::Json and FlakeValue::String because serde's
            // untagged enum deserialization (used when loading commit JSON) cannot
            // distinguish the two variants — String always wins in enum order.
            if dt_full == rdf::JSON || dt_compact == "@json" {
                return match val {
                    FlakeValue::Json(json_str) | FlakeValue::String(json_str) => {
                        serde_json::from_str(json_str).map_err(|e| {
                            FormatError::InvalidBinding(format!("Invalid JSON in @json value: {e}"))
                        })
                    }
                    _ => Err(FormatError::InvalidBinding(
                        "@json datatype must have FlakeValue::Json".to_string(),
                    )),
                };
            }

            // Language-tagged strings always use @language form (no @type)
            if let Some(lang_tag) = dtc.lang_tag() {
                return match val {
                    FlakeValue::String(s) => Ok(json!({
                        "@value": s,
                        "@language": lang_tag
                    })),
                    FlakeValue::Null => Ok(JsonValue::Null),
                    FlakeValue::Ref(_) => Err(FormatError::InvalidBinding(
                        "Binding::Lit invariant violated: contains Ref".to_string(),
                    )),
                    _ => Err(FormatError::InvalidBinding(
                        "Language-tagged literals must be strings".to_string(),
                    )),
                };
            }

            // Inferable datatypes can omit @type annotation.
            if is_inferable_datatype(&dt_full) {
                return match val {
                    FlakeValue::String(s) => Ok(JsonValue::String(s.clone())),
                    FlakeValue::Long(n) => Ok(json!(n)),
                    FlakeValue::Double(d) => {
                        if d.is_nan() {
                            Ok(JsonValue::String("NaN".to_string()))
                        } else if d.is_infinite() {
                            if d.is_sign_positive() {
                                Ok(JsonValue::String("INF".to_string()))
                            } else {
                                Ok(JsonValue::String("-INF".to_string()))
                            }
                        } else {
                            Ok(json!(d))
                        }
                    }
                    FlakeValue::Boolean(b) => Ok(json!(b)),
                    FlakeValue::Vector(v) => {
                        Ok(JsonValue::Array(v.iter().map(|f| json!(f)).collect()))
                    }
                    FlakeValue::Json(_) => Err(FormatError::InvalidBinding(
                        "@json should have been handled above".to_string(),
                    )),
                    FlakeValue::Null => Ok(JsonValue::Null),
                    FlakeValue::Ref(_) => Err(FormatError::InvalidBinding(
                        "Binding::Lit invariant violated: contains Ref".to_string(),
                    )),
                    // Extended numeric types - serialize as string
                    FlakeValue::BigInt(n) => Ok(JsonValue::String(n.to_string())),
                    FlakeValue::Decimal(d) => Ok(JsonValue::String(d.to_string())),
                    // Temporal types - serialize as original string
                    FlakeValue::DateTime(dt) => Ok(JsonValue::String(dt.to_string())),
                    FlakeValue::Date(d) => Ok(JsonValue::String(d.to_string())),
                    FlakeValue::Time(t) => Ok(JsonValue::String(t.to_string())),
                    // Additional temporal types - serialize as original string
                    FlakeValue::GYear(v) => Ok(JsonValue::String(v.to_string())),
                    FlakeValue::GYearMonth(v) => Ok(JsonValue::String(v.to_string())),
                    FlakeValue::GMonth(v) => Ok(JsonValue::String(v.to_string())),
                    FlakeValue::GDay(v) => Ok(JsonValue::String(v.to_string())),
                    FlakeValue::GMonthDay(v) => Ok(JsonValue::String(v.to_string())),
                    FlakeValue::YearMonthDuration(v) => Ok(JsonValue::String(v.to_string())),
                    FlakeValue::DayTimeDuration(v) => Ok(JsonValue::String(v.to_string())),
                    FlakeValue::Duration(v) => Ok(JsonValue::String(v.to_string())),
                    FlakeValue::GeoPoint(v) => Ok(JsonValue::String(v.to_string())),
                };
            }

            // Non-inferable datatypes must include @type.
            let value_json = match val {
                FlakeValue::String(s) => JsonValue::String(s.clone()),
                FlakeValue::Long(n) => json!(n),
                FlakeValue::Double(d) => {
                    if d.is_nan() {
                        JsonValue::String("NaN".to_string())
                    } else if d.is_infinite() {
                        if d.is_sign_positive() {
                            JsonValue::String("INF".to_string())
                        } else {
                            JsonValue::String("-INF".to_string())
                        }
                    } else {
                        json!(d)
                    }
                }
                FlakeValue::Boolean(b) => json!(b),
                FlakeValue::Vector(v) => JsonValue::Array(v.iter().map(|f| json!(f)).collect()),
                FlakeValue::Json(json_str) => {
                    // For non-@json context, return as string (shouldn't normally happen)
                    JsonValue::String(json_str.clone())
                }
                FlakeValue::Null => JsonValue::Null,
                FlakeValue::Ref(_) => {
                    return Err(FormatError::InvalidBinding(
                        "Binding::Lit invariant violated: contains Ref".to_string(),
                    ));
                }
                // Extended numeric types - serialize as string with @type
                FlakeValue::BigInt(n) => JsonValue::String(n.to_string()),
                FlakeValue::Decimal(d) => JsonValue::String(d.to_string()),
                // Temporal types - serialize as original string with @type
                FlakeValue::DateTime(dt) => JsonValue::String(dt.to_string()),
                FlakeValue::Date(d) => JsonValue::String(d.to_string()),
                FlakeValue::Time(t) => JsonValue::String(t.to_string()),
                // Additional temporal types - serialize as original string with @type
                FlakeValue::GYear(v) => JsonValue::String(v.to_string()),
                FlakeValue::GYearMonth(v) => JsonValue::String(v.to_string()),
                FlakeValue::GMonth(v) => JsonValue::String(v.to_string()),
                FlakeValue::GDay(v) => JsonValue::String(v.to_string()),
                FlakeValue::GMonthDay(v) => JsonValue::String(v.to_string()),
                FlakeValue::YearMonthDuration(v) => JsonValue::String(v.to_string()),
                FlakeValue::DayTimeDuration(v) => JsonValue::String(v.to_string()),
                FlakeValue::Duration(v) => JsonValue::String(v.to_string()),
                FlakeValue::GeoPoint(v) => JsonValue::String(v.to_string()),
            };

            Ok(json!({
                "@value": value_json,
                "@type": dt_compact
            }))
        }

        // Encoded literal (late materialization) - decode via binary store at formatting time.
        Binding::EncodedLit { .. } => Err(FormatError::InvalidBinding(
            "Internal error: format_binding called without QueryResult for EncodedLit".to_string(),
        )),

        // Encoded IRI types (late materialization) - require QueryResult for decoding.
        Binding::EncodedSid { .. } | Binding::EncodedPid { .. } => {
            Err(FormatError::InvalidBinding(
                "Internal error: format_binding called without QueryResult for encoded IRI binding"
                    .to_string(),
            ))
        }

        // Grouped values (from GROUP BY without aggregation)
        Binding::Grouped(values) => {
            let arr: Result<Vec<_>> = values
                .iter()
                .map(|v| format_binding(v, compactor))
                .collect();
            Ok(JsonValue::Array(arr?))
        }
    }
}

pub(crate) fn format_binding_with_result(
    result: &QueryResult,
    binding: &Binding,
    compactor: &IriCompactor,
) -> Result<JsonValue> {
    if binding.is_encoded() {
        let materialized = super::materialize::materialize_binding(result, binding)?;
        return format_binding_with_result(result, &materialized, compactor);
    }

    format_binding(binding, compactor)
}

// NOTE: encoded binding materialization is centralized in `format::materialize`.

/// Format a single row as a `JsonValue::Array` of length `select.len()`.
///
/// Always returns array-shaped output regardless of arity. Scalar flattening is
/// applied once at the top-level `format()` when `ProjectionShape::Scalar` is set.
fn format_row_array(
    result: &QueryResult,
    batch: &fluree_db_query::Batch,
    row_idx: usize,
    select: &[fluree_db_query::VarId],
    compactor: &IriCompactor,
) -> Result<JsonValue> {
    let values: Result<Vec<_>> = select
        .iter()
        .map(|&var_id| match batch.get(row_idx, var_id) {
            Some(binding) => format_binding_with_result(result, binding, compactor),
            None => Ok(JsonValue::Null),
        })
        .collect();
    Ok(JsonValue::Array(values?))
}

/// Format row for wildcard select (all bound variables as object)
///
/// Uses batch.schema() to get all variables, omits unbound/poisoned.
fn format_row_wildcard(
    result: &QueryResult,
    batch: &fluree_db_query::Batch,
    row_idx: usize,
    vars: &fluree_db_query::VarRegistry,
    compactor: &IriCompactor,
) -> Result<JsonValue> {
    let mut obj = Map::new();

    // Iterate over all variables in the batch schema
    for &var_id in batch.schema() {
        if let Some(binding) = batch.get(row_idx, var_id) {
            // Skip unbound/poisoned for wildcard (omit from output)
            //
            // NOTE: Binding::is_bound() treats Poisoned as "bound" (definite state),
            // so we must explicitly omit Poisoned here.
            if matches!(binding, Binding::Unbound | Binding::Poisoned) {
                continue;
            }

            let var_name = vars.name(var_id);

            // Skip internal variables (e.g. ?__pp0, ?__s0, ?__n0) from wildcard output.
            // The ?__ prefix is reserved for internal use.
            if var_name.starts_with("?__") {
                continue;
            }

            let value = format_binding_with_result(result, binding, compactor)?;
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

    #[test]
    fn test_format_binding_string() {
        let compactor = make_test_compactor();
        let binding = Binding::lit(
            FlakeValue::String("Alice".to_string()),
            Sid::new(2, "string"),
        );
        let result = format_binding(&binding, &compactor).unwrap();
        assert_eq!(result, json!("Alice"));
    }

    #[test]
    fn test_format_binding_long() {
        let compactor = make_test_compactor();
        let binding = Binding::lit(FlakeValue::Long(42), Sid::new(2, "long"));
        let result = format_binding(&binding, &compactor).unwrap();
        assert_eq!(result, json!(42));
    }

    #[test]
    fn test_format_binding_double() {
        let compactor = make_test_compactor();
        let binding = Binding::lit(FlakeValue::Double(3.13), Sid::new(2, "double"));
        let result = format_binding(&binding, &compactor).unwrap();
        assert_eq!(result, json!(3.13));
    }

    #[test]
    fn test_format_binding_boolean() {
        let compactor = make_test_compactor();
        let binding = Binding::lit(FlakeValue::Boolean(true), Sid::new(2, "boolean"));
        let result = format_binding(&binding, &compactor).unwrap();
        assert_eq!(result, json!(true));
    }

    #[test]
    fn test_format_binding_sid() {
        let compactor = make_test_compactor();
        let binding = Binding::Sid(Sid::new(100, "alice"));
        let result = format_binding(&binding, &compactor).unwrap();
        // Without @context, returns full IRI
        assert_eq!(result, json!("http://example.org/alice"));
    }

    #[test]
    fn test_format_binding_unbound() {
        let compactor = make_test_compactor();
        let binding = Binding::Unbound;
        let result = format_binding(&binding, &compactor).unwrap();
        assert_eq!(result, JsonValue::Null);
    }

    #[test]
    fn test_format_binding_language_tagged() {
        let compactor = make_test_compactor();
        let binding = Binding::lit_lang(FlakeValue::String("Hello".to_string()), "en");
        let result = format_binding(&binding, &compactor).unwrap();
        assert_eq!(result, json!({"@value": "Hello", "@language": "en"}));
    }

    #[test]
    fn test_format_binding_grouped() {
        let compactor = make_test_compactor();
        let binding = Binding::Grouped(vec![
            Binding::lit(FlakeValue::Long(1), Sid::new(2, "long")),
            Binding::lit(FlakeValue::Long(2), Sid::new(2, "long")),
        ]);
        let result = format_binding(&binding, &compactor).unwrap();
        assert_eq!(result, json!([1, 2]));
    }

    #[test]
    fn test_format_binding_json_variant() {
        // FlakeValue::Json with rdf:JSON datatype — the normal in-memory path.
        let compactor = make_test_compactor();
        let binding = Binding::lit(
            FlakeValue::Json(r#"{"name":"Alice","age":30}"#.to_string()),
            Sid::new(3, "JSON"), // rdf:JSON
        );
        let result = format_binding(&binding, &compactor).unwrap();
        assert_eq!(result, json!({"name": "Alice", "age": 30}));
    }

    #[test]
    fn test_format_binding_json_as_string_variant() {
        // FlakeValue::String with rdf:JSON datatype — happens after commit
        // deserialization when serde's untagged enum picks String over Json.
        // Without the defense-in-depth fix, this fails with:
        // "Invalid JSON in @json value" or "@json datatype must have FlakeValue::Json"
        let compactor = make_test_compactor();
        let binding = Binding::lit(
            FlakeValue::String(r#"{"name":"Alice","age":30}"#.to_string()),
            Sid::new(3, "JSON"), // rdf:JSON
        );
        let result = format_binding(&binding, &compactor).unwrap();
        assert_eq!(
            result,
            json!({"name": "Alice", "age": 30}),
            "rdf:JSON with FlakeValue::String should deserialize as JSON object"
        );
    }
}
