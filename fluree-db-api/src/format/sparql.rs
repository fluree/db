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
use super::json_write::{push_json_string, push_value};
use super::{materialize, FormatError, Result};
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
    let (vars, head_vars) = compute_head(result);

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

/// Compute the `head.vars` names (without `?`) and the parallel `VarId` list,
/// ordered lexicographically by name. Shared by the DOM and streaming paths so
/// the head order can never drift between them.
fn compute_head(result: &QueryResult) -> (Vec<String>, Vec<fluree_db_query::VarId>) {
    // For wildcard, use the operator schema (all variables); fall back to the
    // VarRegistry when batches are empty (W3C: exists-02 etc.).
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

    // Order head vars lexicographically by name (stable across planner reorderings).
    let mut head_pairs: Vec<(String, fluree_db_query::VarId)> = head_vars
        .iter()
        .map(|&var_id| (strip_question_mark(result.vars.name(var_id)), var_id))
        .collect();
    head_pairs.sort_by(|(a, _), (b, _)| a.cmp(b));

    let vars: Vec<String> = head_pairs.iter().map(|(name, _)| name.clone()).collect();
    let head_vars: Vec<fluree_db_query::VarId> = head_pairs.into_iter().map(|(_, id)| id).collect();
    (vars, head_vars)
}

/// Stream SPARQL 1.1 JSON results directly into a `String`, byte-identical to
/// `serde_json::to_string(&format(...))` for the non-`select_one` SELECT case.
///
/// The common (non-grouped) row streams cell-by-cell with no per-cell
/// `serde_json::Value` allocation. Grouped rows (GROUP BY without aggregation)
/// are rare and reuse the proven [`disaggregate_row`] cartesian expansion,
/// serialized leaf-wise via [`push_value`]. `select_one`, ASK, CONSTRUCT, and
/// `pretty` are handled by the caller on the DOM path and never reach here.
pub fn format_string(
    result: &QueryResult,
    compactor: &IriCompactor,
    _config: &FormatterConfig,
) -> Result<String> {
    debug_assert!(
        !result.output.is_select_one(),
        "format_string is only for the non-select_one path; select_one routes through the DOM"
    );

    let (vars, head_vars) = compute_head(result);

    // Pre-size: rows × cols × an estimated per-cell width plus envelope slack.
    let est = (result.row_count() + 1)
        .saturating_mul(head_vars.len().max(1))
        .saturating_mul(64)
        .saturating_add(64);
    let mut out = String::with_capacity(est);

    out.push_str("{\"head\":{\"vars\":[");
    for (i, name) in vars.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        push_json_string(&mut out, name);
    }
    out.push_str("]},\"results\":{\"bindings\":[");

    let mut first_binding = true;
    for batch in &result.batches {
        let schema = batch.schema();
        // Map each head var to its column index in this batch once per batch.
        let cols: Vec<Option<usize>> = head_vars
            .iter()
            .map(|&v| schema.iter().position(|&sv| sv == v))
            .collect();

        for row_idx in 0..batch.len() {
            let has_grouped = cols.iter().any(|&c| {
                matches!(
                    c.map(|c| batch.get_by_col(row_idx, c)),
                    Some(Binding::Grouped(_))
                )
            });

            if has_grouped {
                // Rare: cartesian-expand via the DOM disaggregator, then splice
                // each fully-built binding object in verbatim.
                let row_bindings: Vec<(fluree_db_query::VarId, &Binding)> = head_vars
                    .iter()
                    .map(|&var_id| {
                        (
                            var_id,
                            batch.get(row_idx, var_id).unwrap_or(&Binding::Unbound),
                        )
                    })
                    .collect();
                let objs = disaggregate_row(result, &row_bindings, &result.vars, compactor)?;
                for obj in &objs {
                    if !first_binding {
                        out.push(',');
                    }
                    first_binding = false;
                    push_value(&mut out, obj)?;
                }
            } else {
                if !first_binding {
                    out.push(',');
                }
                first_binding = false;
                out.push('{');
                let mut first_cell = true;
                for (k, &col) in cols.iter().enumerate() {
                    if let Some(col) = col {
                        let binding = batch.get_by_col(row_idx, col);
                        write_cell(
                            &mut out,
                            result,
                            binding,
                            &vars[k],
                            compactor,
                            &mut first_cell,
                        )?;
                    }
                }
                out.push('}');
            }
        }
    }

    out.push_str("]}}");
    Ok(out)
}

/// Write one `"name":{term}` cell, or nothing for Unbound/Poisoned/null literals
/// (omitted per the SPARQL Results spec, matching [`format_binding`]).
fn write_cell(
    out: &mut String,
    result: &QueryResult,
    binding: &Binding,
    name: &str,
    compactor: &IriCompactor,
    first_cell: &mut bool,
) -> Result<()> {
    // Late materialization: resolve encoded bindings to a concrete binding (the
    // same step the DOM path takes), then stream the result.
    if binding.is_encoded() {
        let materialized = materialize::materialize_binding(result, binding)?;
        return write_cell(out, result, &materialized, name, compactor, first_cell);
    }

    // Omitted cells produce no key at all.
    match binding {
        Binding::Unbound | Binding::Poisoned => return Ok(()),
        Binding::Lit {
            val: FlakeValue::Null,
            ..
        } => return Ok(()),
        _ => {}
    }

    if !*first_cell {
        out.push(',');
    }
    *first_cell = false;
    push_json_string(out, name);
    out.push(':');
    write_term(out, binding, compactor)
}

/// Write the `{"type":...,"value":...}` term object for a (non-omitted) binding.
fn write_term(out: &mut String, binding: &Binding, compactor: &IriCompactor) -> Result<()> {
    match binding {
        Binding::Sid { sid, .. } => write_node(out, &compactor.compact_id_sid(sid)?),
        Binding::IriMatch { iri, .. } => write_node(out, &compactor.compact_id_iri(iri)),
        Binding::Iri(iri) => write_node(out, iri.as_ref()),
        Binding::Lit { val, dtc, .. } => {
            let dt_iri = compactor.decode_sid(dtc.datatype())?;
            write_literal(out, val, dtc.lang_tag(), &dt_iri)?;
        }
        Binding::Grouped(_) => {
            return Err(FormatError::InvalidBinding(
                "Binding::Grouped should be disaggregated before formatting".to_string(),
            ));
        }
        Binding::EncodedLit { .. } | Binding::EncodedSid { .. } | Binding::EncodedPid { .. } => {
            unreachable!("encoded bindings are materialized before write_term")
        }
        Binding::Unbound | Binding::Poisoned => unreachable!("omitted before write_term"),
    }
    Ok(())
}

/// Write a node reference as `{"type":"uri"|"bnode","value":...}`.
fn write_node(out: &mut String, iri: &str) {
    if let Some(label) = iri.strip_prefix("_:") {
        out.push_str(r#"{"type":"bnode","value":"#);
        push_json_string(out, label);
    } else {
        out.push_str(r#"{"type":"uri","value":"#);
        push_json_string(out, iri);
    }
    out.push('}');
}

/// Write a literal term. SPARQL JSON encodes every literal value as a JSON
/// string and carries the datatype for non-inferable / non-string types.
fn write_literal(
    out: &mut String,
    val: &FlakeValue,
    lang: Option<&str>,
    dt_iri: &str,
) -> Result<()> {
    match val {
        FlakeValue::String(s) => {
            out.push_str(r#"{"type":"literal","value":"#);
            push_json_string(out, s);
            if let Some(lang) = lang {
                out.push_str(r#","xml:lang":"#);
                push_json_string(out, lang);
            } else if !is_inferable_datatype(dt_iri) {
                out.push_str(r#","datatype":"#);
                push_json_string(out, dt_iri);
            }
            out.push('}');
        }
        FlakeValue::Ref(_) => {
            return Err(FormatError::InvalidBinding(
                "Binding::Lit invariant violated: contains Ref".to_string(),
            ));
        }
        FlakeValue::Null => unreachable!("null literals are omitted before write_literal"),
        // Every other value type is rendered as its string lexical form with the
        // datatype attached (matching the DOM `format_binding`).
        other => {
            let value = scalar_lexical(other);
            out.push_str(r#"{"type":"literal","value":"#);
            push_json_string(out, &value);
            out.push_str(r#","datatype":"#);
            push_json_string(out, dt_iri);
            out.push('}');
        }
    }
    Ok(())
}

/// The lexical string form of a non-string literal value, matching the DOM
/// `format_binding` value strings exactly.
fn scalar_lexical(val: &FlakeValue) -> String {
    match val {
        FlakeValue::Long(n) => n.to_string(),
        FlakeValue::Double(d) => {
            if d.is_nan() {
                "NaN".to_string()
            } else if d.is_infinite() {
                if d.is_sign_positive() {
                    "INF".to_string()
                } else {
                    "-INF".to_string()
                }
            } else {
                d.to_string()
            }
        }
        FlakeValue::Boolean(b) => b.to_string(),
        FlakeValue::Vector(v) => serde_json::to_string(v).unwrap_or_else(|_| "[]".to_string()),
        FlakeValue::Json(json_str) => json_str.clone(),
        FlakeValue::BigInt(n) => n.to_string(),
        FlakeValue::Decimal(d) => d.to_string(),
        other => other.to_string(),
    }
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
            // A `uri` value names a node, so it's an `@id`-position identifier:
            // compact via `@base` + explicit prefixes, never `@vocab` (issue #1280).
            let iri = compactor.compact_id_sid(sid)?;
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
            let compacted = compactor.compact_id_iri(iri);
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
        IriCompactor::from_namespaces(std::sync::Arc::new(namespaces))
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

    /// The streaming serializer must be byte-identical to compact-serializing the
    /// DOM tree produced by `format`.
    fn assert_parity(result: &QueryResult, compactor: &IriCompactor) {
        let dom = format(result, compactor, &FormatterConfig::sparql_json()).unwrap();
        let want = serde_json::to_string(&dom).unwrap();
        let got = format_string(result, compactor, &FormatterConfig::sparql_json()).unwrap();
        assert_eq!(got, want, "streaming SPARQL JSON diverged from DOM");
    }

    #[test]
    fn parity_scalar_terms() {
        let c = make_test_compactor();
        let r = make_result(
            &["?uri", "?str", "?long", "?bool", "?lang", "?date"],
            vec![vec![
                Binding::sid(Sid::new(100, "alice")),
                Binding::lit(
                    FlakeValue::String("Alice & <Bob>".to_string()),
                    Sid::new(2, "string"),
                ),
                Binding::lit(FlakeValue::Long(42), Sid::new(2, "long")),
                Binding::lit(FlakeValue::Boolean(true), Sid::new(2, "boolean")),
                Binding::lit_lang(FlakeValue::String("Bonjour".to_string()), "fr"),
                Binding::lit(
                    FlakeValue::String("2024-01-15".to_string()),
                    Sid::new(2, "date"),
                ),
            ]],
        );
        assert_parity(&r, &c);
    }

    #[test]
    fn parity_double_special_and_normal() {
        let c = make_test_compactor();
        for d in [
            3.13_f64,
            1e30,
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
    fn parity_blank_node_and_unbound() {
        let c = make_test_compactor();
        let r = make_result(
            &["?a", "?b", "?c"],
            vec![vec![
                Binding::sid(Sid::new(0, "_:b1")),
                Binding::Unbound,
                Binding::sid(Sid::new(100, "x")),
            ]],
        );
        assert_parity(&r, &c);
    }

    #[test]
    fn parity_multi_row() {
        let c = make_test_compactor();
        let r = make_result(
            &["?s", "?n"],
            vec![
                vec![
                    Binding::sid(Sid::new(100, "a")),
                    Binding::lit(FlakeValue::Long(1), Sid::new(2, "long")),
                ],
                vec![
                    Binding::sid(Sid::new(100, "b")),
                    Binding::lit(FlakeValue::String("two".to_string()), Sid::new(2, "string")),
                ],
                vec![Binding::Unbound, Binding::Unbound],
            ],
        );
        assert_parity(&r, &c);
    }

    #[test]
    fn parity_empty_results() {
        let c = make_test_compactor();
        let r = make_result(&["?s", "?p"], vec![]);
        assert_parity(&r, &c);
    }

    #[test]
    fn parity_wildcard() {
        let c = make_test_compactor();
        let mut r = make_result(
            &["?s", "?p"],
            vec![vec![
                Binding::sid(Sid::new(100, "a")),
                Binding::lit(FlakeValue::Long(7), Sid::new(2, "long")),
            ]],
        );
        r.output = crate::QueryOutput::select_all(vec![]); // wildcard
        assert_parity(&r, &c);
    }

    #[test]
    fn parity_grouped_disaggregation() {
        let c = make_test_compactor();
        let r = make_result(
            &["?a", "?b"],
            vec![vec![
                Binding::Grouped(vec![
                    Binding::lit(FlakeValue::Long(10), Sid::new(2, "long")),
                    Binding::lit(FlakeValue::Long(20), Sid::new(2, "long")),
                ]),
                Binding::Grouped(vec![
                    Binding::lit(FlakeValue::Long(1), Sid::new(2, "long")),
                    Binding::lit(FlakeValue::Long(2), Sid::new(2, "long")),
                ]),
            ]],
        );
        assert_parity(&r, &c);
    }

    #[test]
    fn parity_json_and_vector_values() {
        let c = make_test_compactor();
        let r = make_result(
            &["?j", "?v"],
            vec![vec![
                Binding::lit(
                    FlakeValue::Json(r#"{"k":1}"#.to_string()),
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
