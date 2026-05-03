//! SPARQL 1.1 Query Results XML format (`application/sparql-results+xml`)
//!
//! Implements SELECT/ASK results in the W3C XML format:
//! - Root element: `<sparql xmlns="http://www.w3.org/2005/sparql-results#">`
//! - `<head>` contains `<variable name="..."/>`
//! - SELECT results: `<results><result>...`
//! - ASK results: `<boolean>true|false</boolean>`
//!
//! Note: CONSTRUCT/DESCRIBE return RDF graphs and are not representable in this format.

use crate::QueryResult;

use super::config::FormatterConfig;
use super::datatype::is_inferable_datatype;
use super::iri::IriCompactor;
use super::{materialize, FormatError, Result};

use fluree_db_core::FlakeValue;
use fluree_db_query::binding::Binding;
use fluree_db_query::VarRegistry;
use rustc_hash::FxHashMap;

const SPARQL_XML_NS: &str = "http://www.w3.org/2005/sparql-results#";

/// Format query results as SPARQL Results XML.
pub fn format(
    result: &QueryResult,
    compactor: &IriCompactor,
    _config: &FormatterConfig,
) -> Result<String> {
    // CONSTRUCT/DESCRIBE produce graphs (handled by JSON-LD/Turtle etc.), not result sets.
    if result.output.construct_template().is_some() {
        return Err(FormatError::InvalidBinding(
            "SPARQL Results XML is only valid for SELECT/ASK queries (not CONSTRUCT/DESCRIBE)"
                .to_string(),
        ));
    }

    if result.output.is_boolean() {
        let has_solution = result.batches.iter().any(|b| !b.is_empty());
        return Ok(format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><sparql xmlns="{ns}"><head></head><boolean>{val}</boolean></sparql>"#,
            ns = SPARQL_XML_NS,
            val = if has_solution { "true" } else { "false" }
        ));
    }

    // Build head.vars from select list (without ? prefix).
    // For wildcard, use the operator schema (all variables).
    // Fall back to VarRegistry when batches are empty.
    let head_vars: Vec<fluree_db_query::VarId> = if result.output.is_wildcard() {
        result
            .batches
            .first()
            .map(|b| {
                b.schema()
                    .iter()
                    .copied()
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
        result.output.select_vars_or_empty().to_vec()
    };

    // Order head vars lexicographically by variable name (without '?') for stability.
    let mut head_pairs: Vec<(String, fluree_db_query::VarId)> = head_vars
        .iter()
        .map(|&var_id| (strip_question_mark(result.vars.name(var_id)), var_id))
        .collect();
    head_pairs.sort_by(|(a, _), (b, _)| a.cmp(b));

    let head_names: Vec<String> = head_pairs.iter().map(|(name, _)| name.clone()).collect();
    let head_vars: Vec<fluree_db_query::VarId> = head_pairs.into_iter().map(|(_, id)| id).collect();

    let select_one = result.output.is_select_one();

    // Pre-allocate output (best-effort).
    let mut out = String::new();
    out.push_str(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    out.push_str(r#"<sparql xmlns=""#);
    out.push_str(SPARQL_XML_NS);
    out.push_str(r#"">"#);

    // head
    out.push_str("<head>");
    for name in &head_names {
        out.push_str(r#"<variable name=""#);
        escape_attr_into(name, &mut out);
        out.push_str(r#""/>"#);
    }
    out.push_str("</head>");

    // results
    out.push_str("<results>");

    for batch in &result.batches {
        for row_idx in 0..batch.len() {
            let row_bindings: Vec<_> = head_vars
                .iter()
                .map(|&var_id| {
                    let binding = batch.get(row_idx, var_id).unwrap_or(&Binding::Unbound);
                    (var_id, binding)
                })
                .collect();

            let disaggregated =
                disaggregate_row(&row_bindings).map_err(FormatError::InvalidBinding)?;

            if select_one {
                if let Some(first) = disaggregated.into_iter().next() {
                    append_result_row(
                        result,
                        &first,
                        &head_vars,
                        &result.vars,
                        compactor,
                        &mut out,
                    )?;
                }
                break;
            }
            for row in disaggregated {
                append_result_row(result, &row, &head_vars, &result.vars, compactor, &mut out)?;
            }
        }
        if select_one {
            break;
        }
    }

    out.push_str("</results>");
    out.push_str("</sparql>");
    Ok(out)
}

fn append_result_row(
    result: &QueryResult,
    row: &[(fluree_db_query::VarId, Binding)],
    head_vars: &[fluree_db_query::VarId],
    vars: &VarRegistry,
    compactor: &IriCompactor,
    out: &mut String,
) -> Result<()> {
    let mut map: FxHashMap<fluree_db_query::VarId, &Binding> = FxHashMap::default();
    for (vid, b) in row {
        map.insert(*vid, b);
    }

    out.push_str("<result>");
    for &var_id in head_vars {
        let binding = map.get(&var_id).copied().unwrap_or(&Binding::Unbound);
        if let Some(term_xml) = format_binding_xml(result, binding, compactor)? {
            out.push_str(r#"<binding name=""#);
            escape_attr_into(&strip_question_mark(vars.name(var_id)), out);
            out.push_str(r#"">"#);
            out.push_str(&term_xml);
            out.push_str("</binding>");
        }
    }
    out.push_str("</result>");
    Ok(())
}

/// Disaggregate grouped bindings into multiple rows (cartesian product).
///
/// Returns a vector of rows where each row contains owned bindings.
fn disaggregate_row(
    bindings: &[(fluree_db_query::VarId, &Binding)],
) -> std::result::Result<Vec<Vec<(fluree_db_query::VarId, Binding)>>, String> {
    let mut grouped_cols: Vec<(fluree_db_query::VarId, Vec<Binding>)> = Vec::new();
    let mut scalars: Vec<(fluree_db_query::VarId, Binding)> = Vec::new();

    for &(var_id, binding) in bindings {
        match binding {
            Binding::Grouped(values) => grouped_cols.push((var_id, values.to_vec())),
            _ => scalars.push((var_id, binding.clone())),
        }
    }

    let mut results: Vec<Vec<(fluree_db_query::VarId, Binding)>> = vec![scalars];
    for (var_id, values) in grouped_cols {
        let mut new_results = Vec::new();
        for row in results {
            for val in &values {
                let mut new_row = row.clone();
                new_row.push((var_id, val.clone()));
                new_results.push(new_row);
            }
        }
        results = new_results;
    }
    Ok(results)
}

fn format_binding_xml(
    result: &QueryResult,
    binding: &Binding,
    compactor: &IriCompactor,
) -> Result<Option<String>> {
    // Late materialization for encoded bindings.
    if binding.is_encoded() {
        let materialized = materialize::materialize_binding(result, binding)?;
        return format_binding_xml(result, &materialized, compactor);
    }

    match binding {
        Binding::Unbound | Binding::Poisoned => Ok(None),

        Binding::Sid { sid, .. } => {
            let iri = compactor.decode_sid(sid)?;
            Ok(Some(if iri.starts_with("_:") {
                let mut s = String::from("<bnode>");
                escape_text_into(iri.strip_prefix("_:").unwrap_or(&iri), &mut s);
                s.push_str("</bnode>");
                s
            } else {
                let mut s = String::from("<uri>");
                escape_text_into(&iri, &mut s);
                s.push_str("</uri>");
                s
            }))
        }

        Binding::IriMatch { iri, .. } => Ok(Some({
            let iri = iri.as_ref();
            if iri.starts_with("_:") {
                let mut s = String::from("<bnode>");
                escape_text_into(iri.strip_prefix("_:").unwrap_or(iri), &mut s);
                s.push_str("</bnode>");
                s
            } else {
                let mut s = String::from("<uri>");
                escape_text_into(iri, &mut s);
                s.push_str("</uri>");
                s
            }
        })),

        Binding::Iri(iri) => Ok(Some({
            let iri = iri.as_ref();
            if iri.starts_with("_:") {
                let mut s = String::from("<bnode>");
                escape_text_into(iri.strip_prefix("_:").unwrap_or(iri), &mut s);
                s.push_str("</bnode>");
                s
            } else {
                let mut s = String::from("<uri>");
                escape_text_into(iri, &mut s);
                s.push_str("</uri>");
                s
            }
        })),

        Binding::Lit { val, dtc, .. } => {
            let dt = dtc.datatype();
            let dt_iri = compactor.decode_sid(dt)?;

            let mut s = String::from("<literal");

            if let Some(lang) = dtc.lang_tag() {
                s.push_str(r#" xml:lang=""#);
                escape_attr_into(lang, &mut s);
                s.push('"');
            } else if !is_inferable_datatype(&dt_iri) {
                s.push_str(r#" datatype=""#);
                escape_attr_into(&dt_iri, &mut s);
                s.push('"');
            }

            s.push('>');

            match val {
                FlakeValue::String(v) => escape_text_into(v, &mut s),
                FlakeValue::Long(n) => s.push_str(&n.to_string()),
                FlakeValue::Double(d) => {
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
                    s.push_str(&value_str);
                }
                FlakeValue::Boolean(b) => s.push_str(&b.to_string()),
                FlakeValue::Vector(v) => {
                    let value = serde_json::to_string(v).unwrap_or_else(|_| "[]".to_string());
                    escape_text_into(&value, &mut s);
                }
                FlakeValue::Json(json_str) => escape_text_into(json_str, &mut s),
                FlakeValue::Ref(_) => {
                    return Err(FormatError::InvalidBinding(
                        "Lit cannot contain Ref - expected Binding::Sid".to_string(),
                    ));
                }
                // Temporal / other scalar values: use Display + keep datatype.
                other => escape_text_into(&other.to_string(), &mut s),
            }

            s.push_str("</literal>");
            Ok(Some(s))
        }

        Binding::Grouped(_) => Err(FormatError::InvalidBinding(
            "Binding::Grouped should be disaggregated before SPARQL XML formatting".to_string(),
        )),

        Binding::EncodedLit { .. } | Binding::EncodedSid { .. } | Binding::EncodedPid { .. } => {
            unreachable!(
                "Encoded bindings should have been materialized before SPARQL XML formatting"
            )
        }
    }
}

fn strip_question_mark(var_name: &str) -> String {
    var_name.strip_prefix('?').unwrap_or(var_name).to_string()
}

use super::xml_escape::{escape_attr_into, escape_text_into};

#[cfg(test)]
mod tests {
    use super::*;

    use fluree_db_core::Sid;
    use fluree_db_query::binding::Batch;
    use fluree_db_query::var_registry::VarRegistry;

    fn make_test_compactor() -> IriCompactor {
        use std::collections::HashMap;
        let mut namespaces = HashMap::new();
        namespaces.insert(100, "http://example.org/".to_string());
        namespaces.insert(2, "http://www.w3.org/2001/XMLSchema#".to_string());
        IriCompactor::from_namespaces(&namespaces)
    }

    fn make_test_result() -> QueryResult {
        QueryResult {
            vars: VarRegistry::new(),
            t: Some(0),
            novelty: None,
            context: fluree_graph_json_ld::ParsedContext::default(),
            orig_context: None,
            output: fluree_db_query::ir::QueryOutput::select(vec![]),
            batches: vec![],
            binary_graph: None,
        }
    }

    #[test]
    fn format_ask_true() {
        let compactor = make_test_compactor();
        let mut result = make_test_result();
        result.output = fluree_db_query::ir::QueryOutput::Boolean;
        result.batches = vec![Batch::single_empty()];

        let xml = format(&result, &compactor, &FormatterConfig::sparql_xml()).unwrap();
        assert!(xml.contains("<boolean>true</boolean>"), "{xml}");
        assert!(xml.contains(SPARQL_XML_NS), "{xml}");
    }

    #[test]
    fn format_select_single_uri_binding() {
        let compactor = make_test_compactor();
        let mut result = make_test_result();

        let s_var = result.vars.get_or_insert("?s");
        result.output = fluree_db_query::ir::QueryOutput::select(vec![s_var]);

        let schema = std::sync::Arc::from(vec![s_var].into_boxed_slice());
        let sid = Sid::new(100, "alice");
        let batch = Batch::single_row(schema, vec![Binding::sid(sid)]).unwrap();
        result.batches = vec![batch];

        let xml = format(&result, &compactor, &FormatterConfig::sparql_xml()).unwrap();
        assert!(xml.contains(r#"<variable name="s"/>"#), "{xml}");
        assert!(xml.contains(r#"<binding name="s">"#), "{xml}");
        assert!(xml.contains("<uri>http://example.org/alice</uri>"), "{xml}");
    }
}
