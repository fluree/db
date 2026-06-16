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
            let has_grouped = cols.iter().any(|&c| {
                matches!(
                    c.map(|c| batch.get_by_col(row_idx, c)),
                    Some(Binding::Grouped(_))
                )
            });

            if has_grouped {
                // Rare path: cartesian-expand grouped columns into multiple rows.
                let cells: Vec<Option<&Binding>> = cols
                    .iter()
                    .map(|&c| c.map(|c| batch.get_by_col(row_idx, c)))
                    .collect();
                let wrote = write_grouped_rows(
                    &mut out,
                    result,
                    &cells,
                    &head_names,
                    compactor,
                    gv,
                    select_one,
                )?;
                if select_one && wrote {
                    break 'outer;
                }
            } else {
                // Fast path: stream the row straight into the buffer.
                out.push_str("<result>");
                for (k, &col) in cols.iter().enumerate() {
                    if let Some(col) = col {
                        let binding = batch.get_by_col(row_idx, col);
                        write_binding_cell(
                            &mut out,
                            result,
                            binding,
                            &head_names[k],
                            compactor,
                            gv,
                        )?;
                    }
                }
                out.push_str("</result>");
                if select_one {
                    break 'outer;
                }
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

        Binding::Grouped(_) => {
            return Err(FormatError::InvalidBinding(
                "Binding::Grouped should be disaggregated before SPARQL XML formatting".to_string(),
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
    let store = gv.store();
    match binding {
        Binding::EncodedSid { s_id, .. } => {
            let iri = store.resolve_subject_iri(*s_id).map_err(|e| {
                FormatError::InvalidBinding(format!(
                    "Failed to resolve subject IRI for s_id {s_id}: {e}"
                ))
            })?;
            write_iri_ref(out, &iri);
        }
        Binding::EncodedPid { p_id } => {
            let iri = store.resolve_predicate_iri(*p_id).ok_or_else(|| {
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
        if !is_inferable_datatype(&dt_iri) {
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

/// Write an `xsd:double` lexical value, preserving the special-value spellings.
fn write_double(out: &mut String, d: f64) {
    if d.is_nan() {
        out.push_str("NaN");
    } else if d.is_infinite() {
        out.push_str(if d.is_sign_positive() { "INF" } else { "-INF" });
    } else {
        out.push_str(&d.to_string());
    }
}

/// Cartesian-expand a row containing `Grouped` columns into multiple `<result>`
/// elements (one per combination), preserving the original disaggregation order
/// where the first grouped column varies slowest. Returns whether any row was
/// written (an empty grouped column drops the source row, matching prior
/// behavior). `cells` is aligned with `head_names`; `None` entries are omitted.
fn write_grouped_rows(
    out: &mut String,
    result: &QueryResult,
    cells: &[Option<&Binding>],
    head_names: &[String],
    compactor: &IriCompactor,
    gv: Option<&BinaryGraphView>,
    select_one: bool,
) -> Result<bool> {
    // Collect grouped columns; an empty group means zero output rows.
    let mut grouped: Vec<&[Binding]> = Vec::new();
    for cell in cells {
        if let Some(Binding::Grouped(values)) = cell {
            if values.is_empty() {
                return Ok(false);
            }
            grouped.push(values.as_slice());
        }
    }

    let total: usize = grouped.iter().map(|v| v.len()).product();
    let combos = if select_one { total.min(1) } else { total };

    for combo in 0..combos {
        // Mixed-radix decode: last grouped column varies fastest so the first
        // varies slowest, matching the original nested-loop expansion order.
        let mut picks = vec![0usize; grouped.len()];
        let mut rem = combo;
        for gi in (0..grouped.len()).rev() {
            let r = grouped[gi].len();
            picks[gi] = rem % r;
            rem /= r;
        }

        out.push_str("<result>");
        let mut gi = 0usize;
        for (k, cell) in cells.iter().enumerate() {
            match cell {
                Some(Binding::Grouped(_)) => {
                    let chosen = &grouped[gi][picks[gi]];
                    gi += 1;
                    write_binding_cell(out, result, chosen, &head_names[k], compactor, gv)?;
                }
                Some(binding) => {
                    write_binding_cell(out, result, binding, &head_names[k], compactor, gv)?;
                }
                None => {} // absent column → Unbound → omitted
            }
        }
        out.push_str("</result>");
    }

    Ok(combos > 0)
}

fn strip_question_mark(var_name: &str) -> String {
    var_name.strip_prefix('?').unwrap_or(var_name).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    use fluree_db_core::{NsCode, Sid};
    use fluree_db_query::binding::Batch;
    use fluree_db_query::var_registry::VarRegistry;

    fn make_test_compactor() -> IriCompactor {
        use std::collections::HashMap;
        let mut namespaces = HashMap::new();
        namespaces.insert(100, "http://example.org/".to_string());
        namespaces.insert(2, "http://www.w3.org/2001/XMLSchema#".to_string());
        // BLANK_NODE (code 10) is registered with the "_:" prefix in production
        // (default_namespace_codes); mirror that so blank-node Sids resolve.
        namespaces.insert(
            fluree_vocab::namespaces::BLANK_NODE.as_u16(),
            "_:".to_string(),
        );
        IriCompactor::from_namespaces(std::sync::Arc::new(namespaces))
    }

    fn make_test_result() -> QueryResult {
        QueryResult {
            vars: VarRegistry::new(),
            t: Some(0),
            novelty: None,
            context: fluree_graph_json_ld::ParsedContext::default(),
            orig_context: None,
            output: fluree_db_query::ir::QueryOutput::select_all(vec![]),
            batches: vec![],
            binary_graph: None,
        }
    }

    #[test]
    fn format_ask_true() {
        let compactor = make_test_compactor();
        let mut result = make_test_result();
        result.output = fluree_db_query::ir::QueryOutput::Ask;
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
        result.output = fluree_db_query::ir::QueryOutput::select_all(vec![s_var]);

        let schema = std::sync::Arc::from(vec![s_var].into_boxed_slice());
        let sid = Sid::new(NsCode(100), "alice");
        let batch = Batch::single_row(schema, vec![Binding::sid(sid)]).unwrap();
        result.batches = vec![batch];

        let xml = format(&result, &compactor, &FormatterConfig::sparql_xml()).unwrap();
        assert!(xml.contains(r#"<variable name="s"/>"#), "{xml}");
        assert!(xml.contains(r#"<binding name="s">"#), "{xml}");
        assert!(xml.contains("<uri>http://example.org/alice</uri>"), "{xml}");
    }

    // ------------------------------------------------------------------
    // Direct-write rewrite coverage (previously untested invariants).
    // ------------------------------------------------------------------

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
        let batch = Batch::new(Arc::from(var_ids.clone().into_boxed_slice()), columns).unwrap();
        QueryResult {
            vars,
            t: Some(0),
            novelty: None,
            context: fluree_graph_json_ld::ParsedContext::default(),
            orig_context: None,
            output: fluree_db_query::ir::QueryOutput::select_all(var_ids),
            batches: vec![batch],
            binary_graph: None,
        }
    }

    fn fmt(result: &QueryResult, compactor: &IriCompactor) -> String {
        format(result, compactor, &FormatterConfig::sparql_xml()).unwrap()
    }

    #[test]
    fn inferable_datatype_omitted_string() {
        let c = make_test_compactor();
        let r = make_result(
            &["?v"],
            vec![vec![Binding::lit(
                FlakeValue::String("Alice".to_string()),
                Sid::new(NsCode(2), "string"),
            )]],
        );
        let xml = fmt(&r, &c);
        // xsd:string is inferable → no datatype attribute, just the value.
        assert!(xml.contains("<literal>Alice</literal>"), "{xml}");
    }

    #[test]
    fn inferable_datatype_omitted_long() {
        let c = make_test_compactor();
        // xsd:long is inferable, so XML omits the datatype (value-type agnostic,
        // unlike SPARQL-JSON which always carries datatype on numerics).
        let r = make_result(
            &["?v"],
            vec![vec![Binding::lit(
                FlakeValue::Long(42),
                Sid::new(NsCode(2), "long"),
            )]],
        );
        let xml = fmt(&r, &c);
        assert!(xml.contains("<literal>42</literal>"), "{xml}");
    }

    #[test]
    fn non_inferable_datatype_present_date() {
        let c = make_test_compactor();
        // xsd:date is NOT inferable → the datatype attribute must be emitted.
        let r = make_result(
            &["?v"],
            vec![vec![Binding::lit(
                FlakeValue::String("2024-01-15".to_string()),
                Sid::new(NsCode(2), "date"),
            )]],
        );
        let xml = fmt(&r, &c);
        assert!(
            xml.contains(
                r#"<literal datatype="http://www.w3.org/2001/XMLSchema#date">2024-01-15</literal>"#
            ),
            "{xml}"
        );
    }

    #[test]
    fn language_tag_wins_over_datatype() {
        let c = make_test_compactor();
        let r = make_result(
            &["?v"],
            vec![vec![Binding::lit_lang(
                FlakeValue::String("Bonjour".to_string()),
                "fr",
            )]],
        );
        let xml = fmt(&r, &c);
        assert!(
            xml.contains(r#"<literal xml:lang="fr">Bonjour</literal>"#),
            "{xml}"
        );
        assert!(!xml.contains("datatype="), "{xml}");
    }

    #[test]
    fn blank_node_stripped_and_framed() {
        let c = make_test_compactor();
        // EMPTY namespace (code 0) carries the verbatim IRI, here a `_:` label.
        let r = make_result(
            &["?v"],
            vec![vec![Binding::sid(Sid::new(NsCode(0), "_:b1"))]],
        );
        let xml = fmt(&r, &c);
        assert!(xml.contains("<bnode>b1</bnode>"), "{xml}");
        assert!(!xml.contains("<uri>"), "{xml}");
    }

    #[test]
    fn blank_node_registered_namespace_code() {
        // The real encoding: a blank node lands in the registered BLANK_NODE
        // namespace (prefix "_:"), so `namespace_prefix` returns Some("_:").
        // This must still frame as <bnode>, not <uri>.
        let c = make_test_compactor();
        let r = make_result(
            &["?v"],
            vec![vec![Binding::sid(Sid::new(
                fluree_vocab::namespaces::BLANK_NODE,
                "b1",
            ))]],
        );
        let xml = fmt(&r, &c);
        assert!(xml.contains("<bnode>b1</bnode>"), "{xml}");
        assert!(!xml.contains("<uri>"), "{xml}");
    }

    #[test]
    fn double_special_values() {
        let c = make_test_compactor();
        for (d, expected) in [
            (f64::NAN, "NaN"),
            (f64::INFINITY, "INF"),
            (f64::NEG_INFINITY, "-INF"),
        ] {
            let r = make_result(
                &["?v"],
                vec![vec![Binding::lit(
                    FlakeValue::Double(d),
                    Sid::new(NsCode(2), "double"),
                )]],
            );
            let xml = fmt(&r, &c);
            assert!(xml.contains(&format!(">{expected}</literal>")), "{xml}");
        }
    }

    #[test]
    fn unbound_binding_is_omitted() {
        let c = make_test_compactor();
        let r = make_result(
            &["?a", "?b"],
            vec![vec![
                Binding::sid(Sid::new(NsCode(100), "x")),
                Binding::Unbound,
            ]],
        );
        let xml = fmt(&r, &c);
        assert!(xml.contains(r#"<binding name="a">"#), "{xml}");
        // Unbound ?b must not produce a <binding> element.
        assert!(!xml.contains(r#"<binding name="b">"#), "{xml}");
    }

    #[test]
    fn head_vars_sorted_lexicographically() {
        let c = make_test_compactor();
        let r = make_result(
            &["?zebra", "?apple"],
            vec![vec![
                Binding::sid(Sid::new(NsCode(100), "z")),
                Binding::sid(Sid::new(NsCode(100), "a")),
            ]],
        );
        let xml = fmt(&r, &c);
        let apple = xml.find(r#"<variable name="apple"/>"#).unwrap();
        let zebra = xml.find(r#"<variable name="zebra"/>"#).unwrap();
        assert!(apple < zebra, "head vars must be sorted: {xml}");
    }

    #[test]
    fn xml_special_chars_escaped_in_value() {
        let c = make_test_compactor();
        let r = make_result(
            &["?v"],
            vec![vec![Binding::lit(
                FlakeValue::String("a & b < c > d".to_string()),
                Sid::new(NsCode(2), "string"),
            )]],
        );
        let xml = fmt(&r, &c);
        assert!(xml.contains("a &amp; b &lt; c &gt; d"), "{xml}");
    }

    #[test]
    fn grouped_disaggregates_in_cartesian_order() {
        let c = make_test_compactor();
        // Two grouped columns; first (sorted) var must vary slowest.
        let r = make_result(
            &["?a", "?b"],
            vec![vec![
                Binding::Grouped(vec![
                    Binding::lit(FlakeValue::Long(10), Sid::new(NsCode(2), "long")),
                    Binding::lit(FlakeValue::Long(20), Sid::new(NsCode(2), "long")),
                ]),
                Binding::Grouped(vec![
                    Binding::lit(FlakeValue::Long(1), Sid::new(NsCode(2), "long")),
                    Binding::lit(FlakeValue::Long(2), Sid::new(NsCode(2), "long")),
                ]),
            ]],
        );
        let xml = fmt(&r, &c);
        // Expect 4 <result> rows in order (a=10,b=1),(a=10,b=2),(a=20,b=1),(a=20,b=2).
        assert_eq!(xml.matches("<result>").count(), 4, "{xml}");
        let results_section = &xml[xml.find("<results>").unwrap()..];
        let blocks: Vec<&str> = results_section
            .split("<result>")
            .skip(1)
            .map(|s| s.split("</result>").next().unwrap())
            .collect();
        assert_eq!(blocks.len(), 4);
        let pairs: Vec<(i64, i64)> = blocks
            .iter()
            .map(|blk| (seg_val(blk, "a"), seg_val(blk, "b")))
            .collect();
        assert_eq!(pairs, vec![(10, 1), (10, 2), (20, 1), (20, 2)], "{xml}");
    }

    /// Extract the integer literal value of `<binding name="{name}">…</binding>`.
    fn seg_val(blk: &str, name: &str) -> i64 {
        let start = blk.find(&format!(r#"<binding name="{name}">"#)).unwrap();
        let seg = &blk[start..];
        // Skip to the end of the <literal ...> opening tag, then read until '<'.
        let lit = &seg[seg.find("<literal").unwrap()..];
        let val_start = &lit[lit.find('>').unwrap() + 1..];
        let val_end = val_start.find('<').unwrap();
        val_start[..val_end].parse().unwrap()
    }

    #[test]
    fn select_one_emits_single_result() {
        let c = make_test_compactor();
        let mut r = make_result(
            &["?s"],
            vec![
                vec![Binding::sid(Sid::new(NsCode(100), "a"))],
                vec![Binding::sid(Sid::new(NsCode(100), "b"))],
                vec![Binding::sid(Sid::new(NsCode(100), "c"))],
            ],
        );
        let s_var = r.vars.get_or_insert("?s");
        r.output = fluree_db_query::ir::QueryOutput::select_one(vec![s_var]);
        let xml = fmt(&r, &c);
        assert_eq!(xml.matches("<result>").count(), 1, "{xml}");
        assert!(xml.contains("<uri>http://example.org/a</uri>"), "{xml}");
    }

    #[test]
    fn iri_binding_full_iri_no_compaction() {
        let c = make_test_compactor();
        let r = make_result(
            &["?g"],
            vec![vec![Binding::Iri(Arc::from("http://example.org/graph1"))]],
        );
        let xml = fmt(&r, &c);
        // XML uses the full IRI (no @vocab/@base compaction).
        assert!(
            xml.contains("<uri>http://example.org/graph1</uri>"),
            "{xml}"
        );
    }
}
