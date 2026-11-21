//! Bench-only: the SPARQL Results XML writer as of 4f527e30d, selected by
//! `FLUREE_BENCH_XML_WRITER=previous` so both writers run in one binary.
//! Not for merge.
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
use super::datatype::may_omit_datatype;
use super::iri::IriCompactor;
use super::xml_escape::{escape_attr_into, escape_text_into};
use super::{materialize, FormatError, Result};

use fluree_db_binary_index::BinaryGraphView;
use fluree_db_core::{DatatypeConstraint, FlakeValue, Sid};
use fluree_db_query::binding::Binding;
use fluree_db_query::VarId;

const SPARQL_XML_NS: &str = "http://www.w3.org/2005/sparql-results#";

/// Format query results as SPARQL Results XML.
///
/// Writes directly into one pre-sized output buffer: no per-cell `String`, no
/// per-row hash map, and no binding clones on the common (non-grouped) path.
/// Encoded subject/predicate refs are resolved inline (no materialize
/// re-encode round-trip); only grouped rows fall back to cartesian expansion.
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

    if result.output.is_ask() {
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
    let head_vars: Vec<VarId> = if result.output.is_wildcard() {
        result
            .batches
            .first()
            .map(|b| {
                b.schema()
                    .iter()
                    .copied()
                    .filter(|&vid| !super::is_internal_var_name(result.vars.name(vid)))
                    .collect()
            })
            .unwrap_or_else(|| {
                result
                    .vars
                    .iter()
                    .filter(|(name, _)| !super::is_internal_var_name(name))
                    .map(|(_, id)| id)
                    .collect()
            })
    } else {
        result.output.projected_vars_or_empty()
    };

    // Order head vars lexicographically by variable name (without '?') for stability.
    let mut head_pairs: Vec<(String, VarId)> = head_vars
        .iter()
        .map(|&var_id| (strip_question_mark(result.vars.name(var_id)), var_id))
        .collect();
    head_pairs.sort_by(|(a, _), (b, _)| a.cmp(b));

    let head_names: Vec<String> = head_pairs.iter().map(|(name, _)| name.clone()).collect();
    let head_vars: Vec<VarId> = head_pairs.into_iter().map(|(_, id)| id).collect();

    let select_one = result.output.is_select_one();
    let gv = result.binary_graph.as_ref();

    // Pre-size: rows × cols × an estimated per-cell width, plus envelope slack.
    let est = (result.row_count() + 1)
        .saturating_mul(head_vars.len().max(1))
        .saturating_mul(96)
        .saturating_add(128);
    let mut out = String::with_capacity(est);
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

    'outer: for batch in &result.batches {
        // Map each head var to its column index in this batch once per batch
        // (None = absent → treated as Unbound and omitted).
        let schema = batch.schema();
        let cols: Vec<Option<usize>> = head_vars
            .iter()
            .map(|&v| schema.iter().position(|&sv| sv == v))
            .collect();

        for row_idx in 0..batch.len() {
            // Stream the row straight into the buffer.
            out.push_str("<result>");
            for (k, &col) in cols.iter().enumerate() {
                if let Some(col) = col {
                    let binding = batch.get_by_col(row_idx, col);
                    write_binding_cell(&mut out, result, binding, &head_names[k], compactor, gv)?;
                }
            }
            out.push_str("</result>");
            if select_one {
                break 'outer;
            }
        }
    }

    out.push_str("</results>");
    out.push_str("</sparql>");
    Ok(out)
}

/// Write a `<binding name="…">term</binding>` element, or nothing for
/// Unbound/Poisoned (omitted per the SPARQL Results spec).
fn write_binding_cell(
    out: &mut String,
    result: &QueryResult,
    binding: &Binding,
    name: &str,
    compactor: &IriCompactor,
    gv: Option<&BinaryGraphView>,
) -> Result<()> {
    if matches!(binding, Binding::Unbound | Binding::Poisoned) {
        return Ok(());
    }
    out.push_str(r#"<binding name=""#);
    escape_attr_into(name, out);
    out.push_str(r#"">"#);
    write_term(out, result, binding, compactor, gv)?;
    out.push_str("</binding>");
    Ok(())
}

/// Write the `<uri>`/`<bnode>`/`<literal>` term for a (non-unbound) binding.
fn write_term(
    out: &mut String,
    result: &QueryResult,
    binding: &Binding,
    compactor: &IriCompactor,
    gv: Option<&BinaryGraphView>,
) -> Result<()> {
    match binding {
        Binding::Sid { sid, .. } => write_sid_ref(out, compactor, sid)?,
        Binding::IriMatch { iri, .. } => write_iri_ref(out, iri.as_ref()),
        Binding::Iri(iri) => write_iri_ref(out, iri.as_ref()),
        Binding::Lit { val, dtc, .. } => write_literal(out, compactor, val, dtc)?,

        // Encoded subject/predicate refs resolve directly to their full IRI —
        // no materialize re-encode round-trip (XML never compacts node IRIs).
        Binding::EncodedSid { .. } | Binding::EncodedPid { .. } => {
            let gv = gv.ok_or_else(|| {
                FormatError::InvalidBinding(
                    "Encountered encoded binding during SPARQL XML formatting but QueryResult \
                     has no binary_graph"
                        .to_string(),
                )
            })?;
            write_encoded_ref(out, binding, gv)?;
        }
        // Encoded literals: decode via the shared materializer (its value decode
        // is not a wasted round-trip), then write the concrete value.
        Binding::EncodedLit { .. } => {
            let materialized = materialize::materialize_binding(result, binding)?;
            write_term(out, result, &materialized, compactor, gv)?;
        }

        Binding::Grouped(_) => return Err(super::sparql::grouped_cell_error()),
        Binding::Path { .. } | Binding::Rel(_) | Binding::List(_) | Binding::Map(_) => {
            return Err(FormatError::InvalidBinding(
                "SPARQL results have no path/list type (Cypher-only)".to_string(),
            ));
        }
        // Skipped by the caller; unreachable here.
        Binding::Unbound | Binding::Poisoned => {}
    }
    Ok(())
}

/// Write a `Sid` reference, streaming `prefix` + `name` without `decode_sid`'s
/// intermediate `format!` allocation for the common registered-namespace case.
fn write_sid_ref(out: &mut String, compactor: &IriCompactor, sid: &Sid) -> Result<()> {
    match compactor.namespace_prefix(sid)? {
        // The BLANK_NODE namespace is registered with the `"_:"` prefix, so a
        // blank node arrives here as `Some("_:")` (not via the EMPTY `None`
        // branch). Frame it as `<bnode>` with the bare label, matching
        // `decode_sid` + `starts_with("_:")` in the pre-rewrite formatter.
        Some("_:") => {
            out.push_str("<bnode>");
            escape_text_into(sid.name.as_ref(), out);
            out.push_str("</bnode>");
        }
        Some(prefix) => {
            // Any other registered prefix is an absolute IRI, so the joined form
            // is never a blank node — emit `<uri>` directly.
            out.push_str("<uri>");
            escape_text_into(prefix, out);
            escape_text_into(sid.name.as_ref(), out);
            out.push_str("</uri>");
        }
        // EMPTY / OVERFLOW: the name is the verbatim IRI (possibly a `_:` label).
        None => write_iri_ref(out, sid.name.as_ref()),
    }
    Ok(())
}

/// Write a full IRI string as `<uri>` (or `<bnode>` when it is a `_:` label).
fn write_iri_ref(out: &mut String, iri: &str) {
    if let Some(label) = iri.strip_prefix("_:") {
        out.push_str("<bnode>");
        escape_text_into(label, out);
        out.push_str("</bnode>");
    } else {
        out.push_str("<uri>");
        escape_text_into(iri, out);
        out.push_str("</uri>");
    }
}

/// Resolve an `EncodedSid`/`EncodedPid` to its full IRI and write it.
fn write_encoded_ref(out: &mut String, binding: &Binding, gv: &BinaryGraphView) -> Result<()> {
    match binding {
        Binding::EncodedSid { s_id, .. } => {
            let iri = gv.resolve_subject_iri(*s_id).map_err(|e| {
                FormatError::InvalidBinding(format!(
                    "Failed to resolve subject IRI for s_id {s_id}: {e}"
                ))
            })?;
            write_iri_ref(out, &iri);
        }
        Binding::EncodedPid { p_id } => {
            let iri = gv.store().resolve_predicate_iri(*p_id).ok_or_else(|| {
                FormatError::InvalidBinding(format!(
                    "Failed to resolve predicate IRI for p_id {p_id}"
                ))
            })?;
            write_iri_ref(out, iri);
        }
        _ => unreachable!("write_encoded_ref only handles EncodedSid/EncodedPid"),
    }
    Ok(())
}

/// Write a `<literal>` element with lang/datatype attributes and value text.
fn write_literal(
    out: &mut String,
    compactor: &IriCompactor,
    val: &FlakeValue,
    dtc: &DatatypeConstraint,
) -> Result<()> {
    out.push_str("<literal");
    if let Some(lang) = dtc.lang_tag() {
        // A language tag short-circuits the datatype, so don't decode it at all
        // (avoids an allocation — and a spurious decode error — per lang literal).
        out.push_str(r#" xml:lang=""#);
        escape_attr_into(lang, out);
        out.push('"');
    } else {
        let dt_iri = compactor.decode_sid(dtc.datatype())?;
        // `<literal>` content is text, so nothing about the datatype survives
        // into the serialized form — under the W3C profile only `xsd:string`
        // may go untagged (issue #45 (b), same rule as the SRJ writer).
        if !may_omit_datatype(&dt_iri, compactor.emits_absolute_iris()) {
            out.push_str(r#" datatype=""#);
            escape_attr_into(&dt_iri, out);
            out.push('"');
        }
    }
    out.push('>');

    match val {
        FlakeValue::String(v) => escape_text_into(v, out),
        FlakeValue::Long(n) => {
            let mut buf = itoa::Buffer::new();
            out.push_str(buf.format(*n));
        }
        FlakeValue::Double(d) => write_double(out, *d),
        FlakeValue::Boolean(b) => out.push_str(if *b { "true" } else { "false" }),
        FlakeValue::Vector(v) => {
            let value = serde_json::to_string(v).unwrap_or_else(|_| "[]".to_string());
            escape_text_into(&value, out);
        }
        FlakeValue::Json(json_str) => escape_text_into(json_str, out),
        FlakeValue::Ref(_) => {
            return Err(FormatError::InvalidBinding(
                "Lit cannot contain Ref - expected Binding::Sid".to_string(),
            ));
        }
        // Temporal / other scalar values: use Display + keep datatype.
        other => escape_text_into(&other.to_string(), out),
    }

    out.push_str("</literal>");
    Ok(())
}

/// Write an `xsd:double` lexical value in W3C canonical form (`1.0E6`),
/// preserving the special-value spellings (`NaN`, `INF`, `-INF`).
fn write_double(out: &mut String, d: f64) {
    fluree_graph_ir::push_canonical_xsd_double(out, d);
}

fn strip_question_mark(var_name: &str) -> String {
    var_name.strip_prefix('?').unwrap_or(var_name).to_string()
}
