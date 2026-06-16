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
use super::json_write::{push_bool, push_f64, push_i64, push_json_string, push_value};
use super::{materialize, FormatError, Result};
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

    let select_vars = result.output.projected_vars_or_empty();
    for batch in &result.batches {
        for row_idx in 0..batch.len() {
            let row = if result.output.is_wildcard() {
                // Wildcard: use batch schema, return all bound vars as object
                format_row_wildcard(result, batch, row_idx, &result.vars, compactor)?
            } else {
                format_row_array(result, batch, row_idx, &select_vars, compactor)?
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

/// Stream JSON-LD Query results directly into a `String`, byte-identical to
/// `serde_json::to_string(&format(...))` for the non-`select_one` case.
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
    // Scalar flattening fires only for JSON-LD bare-string `select: "?x"` (one
    // projected var). In that mode every row is a 1-element array and flattens to
    // the bare value — see the DOM `format`'s `should_flatten_scalar` step.
    let flatten = result.output.should_flatten_scalar() && select_vars.len() == 1;

    let cols_hint = if is_wildcard {
        result.batches.first().map_or(0, |b| b.schema().len())
    } else {
        select_vars.len()
    };
    let est = (result.row_count() + 1)
        .saturating_mul(cols_hint.max(1))
        .saturating_mul(40)
        .saturating_add(16);
    let mut out = String::with_capacity(est);

    out.push('[');
    let mut first_row = true;
    for batch in &result.batches {
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

            if is_wildcard {
                out.push('{');
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
                    write_value_with_result(&mut out, result, binding, compactor)?;
                }
                out.push('}');
            } else if flatten {
                match cols[0] {
                    Some(col) => write_value_with_result(
                        &mut out,
                        result,
                        batch.get_by_col(row_idx, col),
                        compactor,
                    )?,
                    None => out.push_str("null"),
                }
            } else {
                out.push('[');
                for (i, &col) in cols.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    match col {
                        Some(col) => write_value_with_result(
                            &mut out,
                            result,
                            batch.get_by_col(row_idx, col),
                            compactor,
                        )?,
                        None => out.push_str("null"),
                    }
                }
                out.push(']');
            }
        }
    }
    out.push(']');
    Ok(out)
}

/// Streaming counterpart of [`format_binding_with_result`]: materialize an
/// encoded top binding, then stream it.
///
/// Exposed to `agent_json`, whose row cells use JSON-LD value shaping.
pub(super) fn write_value_with_result(
    out: &mut String,
    result: &QueryResult,
    binding: &Binding,
    compactor: &IriCompactor,
) -> Result<()> {
    if binding.is_encoded() {
        let materialized = materialize::materialize_binding(result, binding)?;
        return write_value_with_result(out, result, &materialized, compactor);
    }
    write_value(out, binding, compactor)
}

/// Streaming counterpart of [`format_binding`] (no `QueryResult`): encoded
/// bindings error here, exactly as the DOM path does. Grouped elements recurse
/// through this same no-materialize path.
fn write_value(out: &mut String, binding: &Binding, compactor: &IriCompactor) -> Result<()> {
    match binding {
        Binding::Unbound | Binding::Poisoned => out.push_str("null"),
        // A reference is a bare compacted IRI string (not an `{"@id":...}` object).
        Binding::Sid { sid, .. } => push_json_string(out, &compactor.compact_id_sid(sid)?),
        Binding::IriMatch { iri, .. } => push_json_string(out, &compactor.compact_id_iri(iri)),
        Binding::Iri(iri) => push_json_string(out, iri.as_ref()),
        Binding::Lit { val, dtc, .. } => write_lit(out, val, dtc, compactor)?,
        Binding::EncodedLit { .. } => {
            return Err(FormatError::InvalidBinding(
                "Internal error: format_binding called without QueryResult for EncodedLit"
                    .to_string(),
            ));
        }
        Binding::EncodedSid { .. } | Binding::EncodedPid { .. } => {
            return Err(FormatError::InvalidBinding(
                "Internal error: format_binding called without QueryResult for encoded IRI binding"
                    .to_string(),
            ));
        }
        Binding::Grouped(values) => {
            out.push('[');
            for (i, v) in values.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value(out, v, compactor)?;
            }
            out.push(']');
        }
    }
    Ok(())
}

/// Write a JSON-LD literal value, mirroring the DOM `format_binding` Lit arm:
/// `@json` returns the parsed value bare, inferable datatypes emit a bare native
/// value, and everything else wraps as `{"@value":...,"@type":<compact>}`.
fn write_lit(
    out: &mut String,
    val: &FlakeValue,
    dtc: &fluree_db_core::DatatypeConstraint,
    compactor: &IriCompactor,
) -> Result<()> {
    let dt = dtc.datatype();
    let dt_full = compactor.decode_sid(dt)?;
    let dt_compact = compactor.compact_sid(dt)?;

    // @json: emit the parsed value directly (accept Json or String, matching DOM).
    if dt_full == rdf::JSON || dt_compact == "@json" {
        return match val {
            FlakeValue::Json(json_str) | FlakeValue::String(json_str) => {
                let parsed: JsonValue = serde_json::from_str(json_str).map_err(|e| {
                    FormatError::InvalidBinding(format!("Invalid JSON in @json value: {e}"))
                })?;
                push_value(out, &parsed)
            }
            _ => Err(FormatError::InvalidBinding(
                "@json datatype must have FlakeValue::Json".to_string(),
            )),
        };
    }

    // Language-tagged strings: `{"@value":s,"@language":lang}`.
    if let Some(lang_tag) = dtc.lang_tag() {
        return match val {
            FlakeValue::String(s) => {
                out.push_str(r#"{"@value":"#);
                push_json_string(out, s);
                out.push_str(r#","@language":"#);
                push_json_string(out, lang_tag);
                out.push('}');
                Ok(())
            }
            FlakeValue::Null => {
                out.push_str("null");
                Ok(())
            }
            FlakeValue::Ref(_) => Err(FormatError::InvalidBinding(
                "Binding::Lit invariant violated: contains Ref".to_string(),
            )),
            _ => Err(FormatError::InvalidBinding(
                "Language-tagged literals must be strings".to_string(),
            )),
        };
    }

    // Inferable datatypes emit a bare value (no @type wrapper).
    if is_inferable_datatype(&dt_full) {
        return write_scalar(out, val, false);
    }

    // Non-inferable: wrap with the compacted @type.
    out.push_str(r#"{"@value":"#);
    write_scalar(out, val, true)?;
    out.push_str(r#","@type":"#);
    push_json_string(out, &dt_compact);
    out.push('}');
    Ok(())
}

/// Write the bare scalar JSON for a literal value.
///
/// `json_as_string` distinguishes the two DOM contexts for `FlakeValue::Json`:
/// in the inferable branch a stray `Json` is an internal error (it should have
/// been caught by the `@json` check); in the non-inferable branch it is rendered
/// as a plain string value.
fn write_scalar(out: &mut String, val: &FlakeValue, json_as_string: bool) -> Result<()> {
    match val {
        FlakeValue::String(s) => push_json_string(out, s),
        FlakeValue::Long(n) => push_i64(out, *n),
        FlakeValue::Double(d) => {
            if d.is_nan() {
                push_json_string(out, "NaN");
            } else if d.is_infinite() {
                push_json_string(out, if d.is_sign_positive() { "INF" } else { "-INF" });
            } else {
                push_f64(out, *d);
            }
        }
        FlakeValue::Boolean(b) => push_bool(out, *b),
        FlakeValue::Vector(v) => push_value(out, &json!(v))?,
        FlakeValue::Json(json_str) => {
            if json_as_string {
                push_json_string(out, json_str);
            } else {
                return Err(FormatError::InvalidBinding(
                    "@json should have been handled above".to_string(),
                ));
            }
        }
        FlakeValue::Null => out.push_str("null"),
        FlakeValue::Ref(_) => {
            return Err(FormatError::InvalidBinding(
                "Binding::Lit invariant violated: contains Ref".to_string(),
            ));
        }
        FlakeValue::BigInt(n) => push_json_string(out, &n.to_string()),
        FlakeValue::Decimal(d) => push_json_string(out, &d.to_string()),
        // Temporal types + GeoPoint: original lexical string (FlakeValue Display
        // delegates to the inner value's Display for these variants).
        other => push_json_string(out, &other.to_string()),
    }
    Ok(())
}

/// Format a single binding to JSON-LD Query JSON
///
/// Note: Binding::Lit NEVER contains FlakeValue::Ref (Rust invariant)
pub(crate) fn format_binding(binding: &Binding, compactor: &IriCompactor) -> Result<JsonValue> {
    match binding {
        Binding::Unbound | Binding::Poisoned => Ok(JsonValue::Null),

        // Reference (IRI or blank node) - compact using @context.
        // A reference binding names a node, so it's an `@id`-position value:
        // compact via `@base` + explicit prefixes, never `@vocab` (issue #1280).
        Binding::Sid { sid, .. } => Ok(JsonValue::String(compactor.compact_id_sid(sid)?)),

        // IriMatch: use canonical IRI, then compact (multi-ledger mode)
        Binding::IriMatch { iri, .. } => Ok(JsonValue::String(compactor.compact_id_iri(iri))),

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
    use fluree_db_core::{NsCode, Sid};
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

    /// Regression for #1280: a reference binding is a node identifier (`@id`
    /// position). Even when its IRI falls under `@vocab`, the flat JSON-LD
    /// formatter must emit the full IRI, not the bare `@vocab` term.
    #[test]
    fn test_format_binding_sid_id_not_vocab_compacted() {
        let compactor = make_vocab_compactor();
        let binding = Binding::sid(Sid::new(NsCode(100), "summer")); // http://example.org/lists/summer
        let result = format_binding(&binding, &compactor).unwrap();
        assert_eq!(result, json!("http://example.org/lists/summer"));
        assert_ne!(
            result,
            json!("summer"),
            "an @id under @vocab must not collapse to a bare term"
        );
    }

    #[test]
    fn test_format_binding_string() {
        let compactor = make_test_compactor();
        let binding = Binding::lit(
            FlakeValue::String("Alice".to_string()),
            Sid::new(NsCode(2), "string"),
        );
        let result = format_binding(&binding, &compactor).unwrap();
        assert_eq!(result, json!("Alice"));
    }

    #[test]
    fn test_format_binding_long() {
        let compactor = make_test_compactor();
        let binding = Binding::lit(FlakeValue::Long(42), Sid::new(NsCode(2), "long"));
        let result = format_binding(&binding, &compactor).unwrap();
        assert_eq!(result, json!(42));
    }

    #[test]
    fn test_format_binding_double() {
        let compactor = make_test_compactor();
        let binding = Binding::lit(FlakeValue::Double(3.13), Sid::new(NsCode(2), "double"));
        let result = format_binding(&binding, &compactor).unwrap();
        assert_eq!(result, json!(3.13));
    }

    #[test]
    fn test_format_binding_boolean() {
        let compactor = make_test_compactor();
        let binding = Binding::lit(FlakeValue::Boolean(true), Sid::new(NsCode(2), "boolean"));
        let result = format_binding(&binding, &compactor).unwrap();
        assert_eq!(result, json!(true));
    }

    #[test]
    fn test_format_binding_sid() {
        let compactor = make_test_compactor();
        let binding = Binding::sid(Sid::new(NsCode(100), "alice"));
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
            Binding::lit(FlakeValue::Long(1), Sid::new(NsCode(2), "long")),
            Binding::lit(FlakeValue::Long(2), Sid::new(NsCode(2), "long")),
        ]);
        let result = format_binding(&binding, &compactor).unwrap();
        assert_eq!(result, json!([1, 2]));
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
        let dom = format(result, compactor, &FormatterConfig::jsonld()).unwrap();
        let want = serde_json::to_string(&dom).unwrap();
        let got = format_string(result, compactor, &FormatterConfig::jsonld()).unwrap();
        assert_eq!(got, want, "streaming JSON-LD diverged from DOM");
    }

    #[test]
    fn parity_array_rows_and_scalars() {
        let c = make_test_compactor();
        let r = make_result(
            &["?ref", "?s", "?n", "?b", "?lang", "?date"],
            vec![
                vec![
                    Binding::sid(Sid::new(NsCode(100), "alice")),
                    Binding::lit(
                        FlakeValue::String("tab\there/slash".to_string()),
                        Sid::new(NsCode(2), "string"),
                    ),
                    Binding::lit(FlakeValue::Long(42), Sid::new(NsCode(2), "long")),
                    Binding::lit(FlakeValue::Boolean(true), Sid::new(NsCode(2), "boolean")),
                    Binding::lit_lang(FlakeValue::String("Salut".to_string()), "fr"),
                    Binding::lit(
                        FlakeValue::String("2024-01-15".to_string()),
                        Sid::new(NsCode(2), "date"),
                    ),
                ],
                vec![
                    Binding::Unbound,
                    Binding::lit(
                        FlakeValue::String("bob".to_string()),
                        Sid::new(NsCode(2), "string"),
                    ),
                    Binding::lit(FlakeValue::Long(-1), Sid::new(NsCode(2), "long")),
                    Binding::lit(FlakeValue::Boolean(false), Sid::new(NsCode(2), "boolean")),
                    Binding::Unbound,
                    Binding::Unbound,
                ],
            ],
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
                    Sid::new(NsCode(2), "double"),
                )]],
            );
            assert_parity(&r, &c);
        }
    }

    #[test]
    fn parity_json_and_vector() {
        let c = make_test_compactor();
        let r = make_result(
            &["?j", "?v"],
            vec![vec![
                Binding::lit(
                    FlakeValue::Json(r#"{"k":[1,2.5],"s":"x"}"#.to_string()),
                    Sid::new(NsCode(3), "JSON"),
                ),
                Binding::lit(
                    FlakeValue::Vector(vec![1.0, 2.5, -3.0]),
                    Sid::new(NsCode(2), "double"),
                ),
            ]],
        );
        assert_parity(&r, &c);
    }

    #[test]
    fn parity_wildcard_and_grouped() {
        let c = make_test_compactor();
        let mut r = make_result(
            &["?s", "?g"],
            vec![vec![
                Binding::sid(Sid::new(NsCode(100), "a")),
                Binding::Grouped(vec![
                    Binding::sid(Sid::new(NsCode(100), "x")),
                    Binding::lit(FlakeValue::Long(9), Sid::new(NsCode(2), "long")),
                ]),
            ]],
        );
        r.output = crate::QueryOutput::wildcard();
        assert_parity(&r, &c);
    }

    #[test]
    fn parity_scalar_flatten() {
        use fluree_db_query::ir::projection::{Column, Projection};
        let c = make_test_compactor();
        let mut r = make_result(
            &["?x"],
            vec![
                vec![Binding::sid(Sid::new(NsCode(100), "a"))],
                vec![Binding::lit(
                    FlakeValue::Long(5),
                    Sid::new(NsCode(2), "long"),
                )],
                vec![Binding::Unbound],
            ],
        );
        let x = r.vars.get_or_insert("?x");
        r.output = fluree_db_query::ir::QueryOutput::Select {
            projection: Projection::Scalar(Column::Var(x)),
            restriction: None,
        };
        assert!(r.output.should_flatten_scalar());
        assert_parity(&r, &c);
    }

    #[test]
    fn parity_empty() {
        let c = make_test_compactor();
        assert_parity(&make_result(&["?s", "?p"], vec![]), &c);
    }

    #[test]
    fn parity_extended_value_types() {
        use fluree_db_core::coerce::coerce_string_value;
        use fluree_vocab::xsd;
        let c = make_test_compactor();
        // xsd:integer + xsd:decimal are inferable (bare value); xsd:dateTime is
        // not (wrapped with @type). All funnel through the Display/.to_string()
        // arms of write_scalar.
        let r = make_result(
            &["?big", "?dec", "?dt"],
            vec![vec![
                Binding::lit(
                    coerce_string_value("99999999999999999999999999", xsd::INTEGER).unwrap(),
                    Sid::new(NsCode(2), "integer"),
                ),
                Binding::lit(
                    coerce_string_value("3.14159265358979", xsd::DECIMAL).unwrap(),
                    Sid::new(NsCode(2), "decimal"),
                ),
                Binding::lit(
                    coerce_string_value("2024-01-15T10:30:00Z", xsd::DATE_TIME).unwrap(),
                    Sid::new(NsCode(2), "dateTime"),
                ),
            ]],
        );
        assert_parity(&r, &c);
    }

    #[test]
    fn test_format_binding_json_variant() {
        // FlakeValue::Json with rdf:JSON datatype — the normal in-memory path.
        let compactor = make_test_compactor();
        let binding = Binding::lit(
            FlakeValue::Json(r#"{"name":"Alice","age":30}"#.to_string()),
            Sid::new(NsCode(3), "JSON"), // rdf:JSON
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
            Sid::new(NsCode(3), "JSON"), // rdf:JSON
        );
        let result = format_binding(&binding, &compactor).unwrap();
        assert_eq!(
            result,
            json!({"name": "Alice", "age": 30}),
            "rdf:JSON with FlakeValue::String should deserialize as JSON object"
        );
    }
}
