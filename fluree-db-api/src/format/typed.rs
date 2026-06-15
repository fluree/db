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
use super::json_write::{push_bool, push_f64, push_i64, push_json_string, push_value};
use super::{materialize, FormatError, Result};
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

    let select_vars = result.output.projected_vars_or_empty();
    for batch in &result.batches {
        for row_idx in 0..batch.len() {
            let row = if result.output.is_wildcard() {
                // Wildcard: use batch schema, return all bound vars as object
                format_row_wildcard(batch, row_idx, &result.vars, compactor, result)?
            } else {
                format_row(
                    batch,
                    row_idx,
                    &select_vars,
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

/// Stream TypedJson directly into a `String`, byte-identical to
/// `serde_json::to_string(&format(...))` for the non-`select_one` SELECT case.
///
/// `select_one`, ASK, CONSTRUCT, and `pretty` are routed through the DOM path by
/// the caller and never reach here.
pub fn format_string(
    result: &QueryResult,
    compactor: &IriCompactor,
    _config: &FormatterConfig,
) -> Result<String> {
    debug_assert!(
        !result.output.is_select_one(),
        "format_string is only for the non-select_one path; select_one routes through the DOM"
    );

    let is_wildcard = result.output.is_wildcard();
    let select_vars = result.output.projected_vars_or_empty();

    let cols_hint = if is_wildcard {
        result.batches.first().map_or(0, |b| b.schema().len())
    } else {
        select_vars.len()
    };
    let est = (result.row_count() + 1)
        .saturating_mul(cols_hint.max(1))
        .saturating_mul(48)
        .saturating_add(16);
    let mut out = String::with_capacity(est);

    out.push('[');
    let mut first_row = true;
    for batch in &result.batches {
        // Precompute the column index for each projected var once per batch.
        let cols: Vec<Option<usize>> = if is_wildcard {
            Vec::new()
        } else {
            let schema = batch.schema();
            select_vars
                .iter()
                .map(|&v| schema.iter().position(|&sv| sv == v))
                .collect()
        };

        for row_idx in 0..batch.len() {
            if !first_row {
                out.push(',');
            }
            first_row = false;

            out.push('{');
            if is_wildcard {
                let mut first = true;
                for (col_idx, &var_id) in batch.schema().iter().enumerate() {
                    let binding = batch.get_by_col(row_idx, col_idx);
                    if matches!(binding, Binding::Unbound | Binding::Poisoned) {
                        continue;
                    }
                    let name = result.vars.name(var_id);
                    if name.starts_with("?__") {
                        continue;
                    }
                    if !first {
                        out.push(',');
                    }
                    first = false;
                    push_json_string(&mut out, name);
                    out.push(':');
                    write_value(&mut out, result, binding, compactor)?;
                }
            } else {
                for (i, &var_id) in select_vars.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    push_json_string(&mut out, result.vars.name(var_id));
                    out.push(':');
                    match cols[i] {
                        Some(col) => write_value(
                            &mut out,
                            result,
                            batch.get_by_col(row_idx, col),
                            compactor,
                        )?,
                        None => out.push_str("null"),
                    }
                }
            }
            out.push('}');
        }
    }
    out.push(']');
    Ok(out)
}

/// Write a single TypedJson value (the per-cell `{...}` / `null` / `[...]`),
/// matching [`format_binding`] but streamed into `out`.
fn write_value(
    out: &mut String,
    result: &QueryResult,
    binding: &Binding,
    compactor: &IriCompactor,
) -> Result<()> {
    if binding.is_encoded() {
        let materialized = materialize::materialize_binding(result, binding)?;
        return write_value(out, result, &materialized, compactor);
    }

    match binding {
        Binding::Unbound | Binding::Poisoned => out.push_str("null"),
        Binding::Sid { sid, .. } => {
            out.push_str(r#"{"@id":"#);
            push_json_string(out, &compactor.compact_id_sid(sid)?);
            out.push('}');
        }
        Binding::IriMatch { iri, .. } => {
            out.push_str(r#"{"@id":"#);
            push_json_string(out, &compactor.compact_id_iri(iri));
            out.push('}');
        }
        Binding::Iri(iri) => {
            out.push_str(r#"{"@id":"#);
            push_json_string(out, iri.as_ref());
            out.push('}');
        }
        Binding::Lit { val, dtc, .. } => write_lit(out, val, dtc, compactor)?,
        Binding::Grouped(values) => {
            out.push('[');
            for (i, v) in values.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(out, result, v, compactor)?;
            }
            out.push(']');
        }
        // A path renders as an array of `{"@id": ...}` node references.
        Binding::Path(nodes) => {
            out.push('[');
            for (i, sid) in nodes.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(r#"{"@id":"#);
                push_json_string(out, &compactor.compact_id_sid(sid)?);
                out.push('}');
            }
            out.push(']');
        }
        Binding::EncodedLit { .. } | Binding::EncodedSid { .. } | Binding::EncodedPid { .. } => {
            unreachable!("encoded bindings are materialized before write_value")
        }
    }
    Ok(())
}

/// Write a TypedJson literal value. Mirrors the DOM `format_binding` Lit arm,
/// including computing the compacted datatype up front (so a decode error
/// surfaces even on the `@json` / null paths, exactly as the DOM does).
fn write_lit(
    out: &mut String,
    val: &FlakeValue,
    dtc: &fluree_db_core::DatatypeConstraint,
    compactor: &IriCompactor,
) -> Result<()> {
    let dt_iri = compactor.compact_sid(dtc.datatype())?;

    match val {
        FlakeValue::String(s) => {
            out.push_str(r#"{"@value":"#);
            push_json_string(out, s);
            if let Some(lang) = dtc.lang_tag() {
                out.push_str(r#","@language":"#);
                push_json_string(out, lang);
            } else {
                out.push_str(r#","@type":"#);
                push_json_string(out, &dt_iri);
            }
            out.push('}');
        }
        FlakeValue::Long(n) => {
            out.push_str(r#"{"@value":"#);
            push_i64(out, *n);
            out.push_str(r#","@type":"#);
            push_json_string(out, &dt_iri);
            out.push('}');
        }
        FlakeValue::Double(d) => {
            out.push_str(r#"{"@value":"#);
            if d.is_nan() {
                push_json_string(out, "NaN");
            } else if d.is_infinite() {
                push_json_string(out, if d.is_sign_positive() { "INF" } else { "-INF" });
            } else {
                push_f64(out, *d);
            }
            out.push_str(r#","@type":"#);
            push_json_string(out, &dt_iri);
            out.push('}');
        }
        FlakeValue::Boolean(b) => {
            out.push_str(r#"{"@value":"#);
            push_bool(out, *b);
            out.push_str(r#","@type":"#);
            push_json_string(out, &dt_iri);
            out.push('}');
        }
        FlakeValue::Vector(v) => {
            // Rare leaf: serialize the float array via serde for exact parity.
            out.push_str(r#"{"@value":"#);
            push_value(out, &json!(v))?;
            out.push_str(r#","@type":"#);
            push_json_string(out, &dt_iri);
            out.push('}');
        }
        FlakeValue::Json(json_str) => {
            let json_val: JsonValue = serde_json::from_str(json_str).map_err(|e| {
                FormatError::InvalidBinding(format!("Invalid JSON in @json value: {e}"))
            })?;
            out.push_str(r#"{"@value":"#);
            push_value(out, &json_val)?;
            out.push_str(r#","@type":"@json"}"#);
        }
        FlakeValue::Null => out.push_str("null"),
        FlakeValue::Ref(_) => {
            return Err(FormatError::InvalidBinding(
                "Binding::Lit invariant violated: contains Ref".to_string(),
            ));
        }
        // Extended numeric + temporal types: stringified value with @type.
        other => {
            out.push_str(r#"{"@value":"#);
            push_json_string(out, &stringified(other));
            out.push_str(r#","@type":"#);
            push_json_string(out, &dt_iri);
            out.push('}');
        }
    }
    Ok(())
}

/// The `.to_string()` lexical form the DOM uses for BigInt/Decimal/temporal/Geo.
fn stringified(val: &FlakeValue) -> String {
    match val {
        FlakeValue::BigInt(n) => n.to_string(),
        FlakeValue::Decimal(d) => d.to_string(),
        other => other.to_string(),
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

        // Reference - use @id notation. An `@id` is a node identifier, so it
        // compacts via `@base` + explicit prefixes only, never `@vocab`
        // (issue #1280).
        Binding::Sid { sid, .. } => {
            let iri = compactor.compact_id_sid(sid)?;
            Ok(json!({"@id": iri}))
        }

        // IriMatch: use canonical IRI, then compact (multi-ledger mode)
        Binding::IriMatch { iri, .. } => {
            let compacted = compactor.compact_id_iri(iri);
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

        // A path - array of `{"@id": ...}` node references.
        Binding::Path(nodes) => {
            let arr: Result<Vec<_>> = nodes
                .iter()
                .map(|sid| compactor.compact_id_sid(sid).map(|iri| json!({"@id": iri})))
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

            // Skip internal / non-distinguished variables (planner synthetics,
            // annotation-reifier synthetics, SPARQL blank-node vars).
            if super::is_internal_var_name(var_name) {
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
    use fluree_graph_json_ld::ParsedContext;
    use std::collections::HashMap;

    fn make_test_compactor() -> IriCompactor {
        let mut namespaces = HashMap::new();
        namespaces.insert(2, "http://www.w3.org/2001/XMLSchema#".to_string());
        namespaces.insert(3, "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string());
        namespaces.insert(100, "http://example.org/".to_string());
        IriCompactor::from_namespaces(std::sync::Arc::new(namespaces))
    }

    /// A compactor whose context sets `@vocab` to the same namespace as the
    /// `lists` subject, so the `@vocab`-vs-`@id` distinction is observable.
    fn make_vocab_compactor() -> IriCompactor {
        let mut namespaces = HashMap::new();
        namespaces.insert(100, "http://example.org/lists/".to_string());
        let context = ParsedContext::parse(
            None,
            &serde_json::json!({"@vocab": "http://example.org/lists/"}),
        )
        .unwrap();
        IriCompactor::new(std::sync::Arc::new(namespaces), &context)
    }

    /// Regression for #1280: the `@id` of a reference must not be compacted
    /// against `@vocab`, even when its IRI falls under the vocab namespace.
    #[test]
    fn test_format_binding_sid_id_not_vocab_compacted() {
        let compactor = make_vocab_compactor();
        let result = make_test_result();
        let binding = Binding::sid(Sid::new(100, "summer")); // http://example.org/lists/summer
        let formatted = format_binding(&result, &binding, &compactor).unwrap();
        assert_eq!(formatted, json!({"@id": "http://example.org/lists/summer"}));
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
            output: crate::QueryOutput::select_all(vec![]),
            batches: vec![],
            binary_graph: None,
        }
    }

    #[test]
    fn test_format_binding_sid() {
        let compactor = make_test_compactor();
        let result = make_test_result();
        let binding = Binding::sid(Sid::new(100, "alice"));
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

    // ------------------------------------------------------------------
    // Streaming `format_string` parity with the DOM `format` + serde_json.
    // ------------------------------------------------------------------

    use fluree_db_query::var_registry::VarRegistry;
    use std::sync::Arc;

    fn make_result(var_names: &[&str], rows: Vec<Vec<Binding>>) -> QueryResult {
        let mut vars = VarRegistry::new();
        let var_ids: Vec<fluree_db_query::VarId> =
            var_names.iter().map(|&n| vars.get_or_insert(n)).collect();
        let mut columns: Vec<Vec<Binding>> = vec![Vec::new(); var_ids.len()];
        for row in rows {
            for (col, b) in row.into_iter().enumerate() {
                columns[col].push(b);
            }
        }
        let batch = fluree_db_query::binding::Batch::new(
            Arc::from(var_ids.clone().into_boxed_slice()),
            columns,
        )
        .unwrap();
        QueryResult {
            vars,
            t: Some(0),
            novelty: None,
            context: crate::ParsedContext::default(),
            orig_context: None,
            output: crate::QueryOutput::select_all(var_ids),
            batches: vec![batch],
            binary_graph: None,
        }
    }

    fn assert_parity(result: &QueryResult, compactor: &IriCompactor) {
        let dom = format(result, compactor, &FormatterConfig::typed_json()).unwrap();
        let want = serde_json::to_string(&dom).unwrap();
        let got = format_string(result, compactor, &FormatterConfig::typed_json()).unwrap();
        assert_eq!(got, want, "streaming TypedJson diverged from DOM");
    }

    #[test]
    fn parity_scalar_terms() {
        let c = make_test_compactor();
        let r = make_result(
            &["?id", "?s", "?n", "?d", "?b", "?lang", "?date"],
            vec![vec![
                Binding::sid(Sid::new(100, "alice")),
                Binding::lit(
                    FlakeValue::String("A \"quoted\"\tval".to_string()),
                    Sid::new(2, "string"),
                ),
                Binding::lit(FlakeValue::Long(-42), Sid::new(2, "long")),
                Binding::lit(FlakeValue::Double(3.13), Sid::new(2, "double")),
                Binding::lit(FlakeValue::Boolean(false), Sid::new(2, "boolean")),
                Binding::lit_lang(FlakeValue::String("Hola".to_string()), "es"),
                Binding::lit(
                    FlakeValue::String("2024-01-15".to_string()),
                    Sid::new(2, "date"),
                ),
            ]],
        );
        assert_parity(&r, &c);
    }

    #[test]
    fn parity_double_special_and_large() {
        let c = make_test_compactor();
        for d in [
            3.13_f64,
            1e30,
            1e-7,
            -0.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            let r = make_result(
                &["?d"],
                vec![vec![Binding::lit(
                    FlakeValue::Double(d),
                    Sid::new(2, "double"),
                )]],
            );
            assert_parity(&r, &c);
        }
    }

    #[test]
    fn parity_unbound_emits_null_and_multi_row() {
        let c = make_test_compactor();
        let r = make_result(
            &["?a", "?b"],
            vec![
                vec![Binding::sid(Sid::new(100, "a")), Binding::Unbound],
                vec![
                    Binding::Unbound,
                    Binding::lit(FlakeValue::Long(2), Sid::new(2, "long")),
                ],
            ],
        );
        assert_parity(&r, &c);
    }

    #[test]
    fn parity_wildcard_and_grouped() {
        let c = make_test_compactor();
        let mut r = make_result(
            &["?s", "?g"],
            vec![vec![
                Binding::sid(Sid::new(100, "a")),
                Binding::Grouped(vec![
                    Binding::lit(FlakeValue::Long(1), Sid::new(2, "long")),
                    Binding::lit(FlakeValue::Long(2), Sid::new(2, "long")),
                ]),
            ]],
        );
        r.output = crate::QueryOutput::select_all(vec![]); // wildcard
        assert_parity(&r, &c);
    }

    #[test]
    fn parity_json_and_vector() {
        let c = make_test_compactor();
        let r = make_result(
            &["?j", "?v"],
            vec![vec![
                Binding::lit(
                    FlakeValue::Json(r#"{"k":[1,2],"s":"x"}"#.to_string()),
                    Sid::new(3, "JSON"),
                ),
                Binding::lit(
                    FlakeValue::Vector(vec![1.0, 2.5, -3.0]),
                    Sid::new(2, "double"),
                ),
            ]],
        );
        assert_parity(&r, &c);
    }

    #[test]
    fn parity_extended_value_types() {
        use fluree_db_core::coerce::coerce_string_value;
        use fluree_vocab::xsd;
        let c = make_test_compactor();
        let r = make_result(
            &["?big", "?dec", "?dt"],
            vec![vec![
                Binding::lit(
                    coerce_string_value("99999999999999999999999999", xsd::INTEGER).unwrap(),
                    Sid::new(2, "integer"),
                ),
                Binding::lit(
                    coerce_string_value("3.14159265358979", xsd::DECIMAL).unwrap(),
                    Sid::new(2, "decimal"),
                ),
                Binding::lit(
                    coerce_string_value("2024-01-15T10:30:00Z", xsd::DATE_TIME).unwrap(),
                    Sid::new(2, "dateTime"),
                ),
            ]],
        );
        assert_parity(&r, &c);
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
