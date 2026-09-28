//! RDF/XML graph serializer (`application/rdf+xml`)
//!
//! Serializes an instantiated CONSTRUCT / DESCRIBE graph; see
//! [`super::graph_text`] for the entry point.

use super::{FormatError, Result};

use fluree_graph_ir::{push_canonical_xsd_double, Graph, LiteralValue, Term};

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

const RDF_NS: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

pub(super) fn format_graph(graph: &Graph) -> Result<String> {
    // Collect namespaces from predicate IRIs and datatype IRIs.
    let mut namespaces: BTreeSet<String> = BTreeSet::new();
    namespaces.insert(RDF_NS.to_string());

    for t in graph.iter() {
        let p = t.predicate().as_iri().ok_or_else(|| {
            FormatError::InvalidBinding("RDF/XML requires IRI predicates".to_string())
        })?;
        let (ns, _local) = split_iri_for_qname(p)?;
        namespaces.insert(ns.to_string());

        if let Some((_, dt, _lang)) = t.object().as_literal() {
            if let Some(ns) = split_namespace(dt.as_iri()) {
                namespaces.insert(ns.to_string());
            }
        }
    }

    // Deterministic prefix assignment (rdf + ns0..).
    let mut ns_to_prefix: BTreeMap<String, String> = BTreeMap::new();
    ns_to_prefix.insert(RDF_NS.to_string(), "rdf".to_string());
    let mut i = 0usize;
    for ns in namespaces {
        if ns == RDF_NS {
            continue;
        }
        ns_to_prefix.insert(ns, format!("ns{i}"));
        i += 1;
    }

    let mut out = String::new();
    out.push_str(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    out.push_str(r"<rdf:RDF");
    for (ns, prefix) in &ns_to_prefix {
        out.push_str(r" xmlns:");
        out.push_str(prefix);
        out.push_str(r#"=""#);
        escape_attr_into(ns, &mut out);
        out.push('"');
    }
    out.push('>');

    // Group triples by subject (graph is sorted SPO).
    let mut current_subject: Option<&Term> = None;
    for triple in graph.iter() {
        let s = triple.subject();
        if current_subject.map(|cs| cs != s).unwrap_or(true) {
            if current_subject.is_some() {
                out.push_str("</rdf:Description>");
            }
            out.push_str("<rdf:Description");
            write_subject_attr(s, &mut out)?;
            out.push('>');
            current_subject = Some(s);
        }

        write_predicate_object(triple.predicate(), triple.object(), &ns_to_prefix, &mut out)?;
    }
    if current_subject.is_some() {
        out.push_str("</rdf:Description>");
    }

    out.push_str("</rdf:RDF>");
    Ok(out)
}

/// `rdf:nodeID` takes an XML name. A label that is not one (ids minted by old
/// imports contain `/` and `:`, and a Turtle label may start with a digit) is
/// hex-encoded behind an `x`, as the Turtle and N-Triples writers do.
fn push_node_id(label: &str, out: &mut String) {
    let is_name = label
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && label
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    if is_name {
        out.push_str(label);
    } else {
        out.push('x');
        for b in label.bytes() {
            let _ = write!(out, "{b:02x}");
        }
    }
}

fn write_subject_attr(subject: &Term, out: &mut String) -> Result<()> {
    match subject {
        // A stored blank node comes back as a `_:`-prefixed IRI.
        Term::Iri(iri) if iri.starts_with("_:") => {
            out.push_str(r#" rdf:nodeID=""#);
            push_node_id(&iri[2..], out);
            out.push('"');
            Ok(())
        }
        Term::Iri(iri) => {
            out.push_str(r#" rdf:about=""#);
            escape_attr_into(iri.as_ref(), out);
            out.push('"');
            Ok(())
        }
        Term::BlankNode(id) => {
            out.push_str(r#" rdf:nodeID=""#);
            push_node_id(id.as_str(), out);
            out.push('"');
            Ok(())
        }
        Term::Literal { .. } => Err(FormatError::InvalidBinding(
            "RDF/XML subjects cannot be literals".to_string(),
        )),
    }
}

fn write_predicate_object(
    predicate: &Term,
    object: &Term,
    ns_to_prefix: &BTreeMap<String, String>,
    out: &mut String,
) -> Result<()> {
    let p_iri = predicate.as_iri().ok_or_else(|| {
        FormatError::InvalidBinding("RDF/XML requires IRI predicates".to_string())
    })?;
    let (ns, local) = split_iri_for_qname(p_iri)?;
    let prefix = ns_to_prefix.get(ns).ok_or_else(|| {
        FormatError::InvalidBinding(format!("Missing RDF/XML namespace mapping for: {ns}"))
    })?;

    out.push('<');
    out.push_str(prefix);
    out.push(':');
    out.push_str(local);

    match object {
        Term::Iri(iri) if iri.starts_with("_:") => {
            out.push_str(r#" rdf:nodeID=""#);
            push_node_id(&iri[2..], out);
            out.push_str(r#""/>"#);
            Ok(())
        }
        Term::Iri(iri) => {
            out.push_str(r#" rdf:resource=""#);
            escape_attr_into(iri.as_ref(), out);
            out.push_str(r#""/>"#);
            Ok(())
        }
        Term::BlankNode(id) => {
            out.push_str(r#" rdf:nodeID=""#);
            push_node_id(id.as_str(), out);
            out.push_str(r#""/>"#);
            Ok(())
        }
        Term::Literal {
            value,
            datatype,
            language,
        } => {
            if let Some(lang) = language {
                out.push_str(r#" xml:lang=""#);
                escape_attr_into(lang, out);
                out.push('"');
            } else if !datatype.is_xsd_string() {
                out.push_str(r#" rdf:datatype=""#);
                escape_attr_into(datatype.as_iri(), out);
                out.push('"');
            }

            out.push('>');
            match value {
                LiteralValue::String(s) | LiteralValue::Json(s) => escape_text_into(s, out),
                // Numbers and booleans have nothing to escape.
                LiteralValue::Boolean(b) => out.push_str(if *b { "true" } else { "false" }),
                LiteralValue::Integer(i) => {
                    let _ = write!(out, "{i}");
                }
                LiteralValue::Double(d) => push_canonical_xsd_double(out, *d),
            }
            out.push_str("</");
            out.push_str(prefix);
            out.push(':');
            out.push_str(local);
            out.push('>');
            Ok(())
        }
    }
}

fn split_iri_for_qname(iri: &str) -> Result<(&str, &str)> {
    // Heuristic split: last '#' or '/', keeping delimiter in namespace.
    let idx = iri.rfind('#').or_else(|| iri.rfind('/')).ok_or_else(|| {
        FormatError::InvalidBinding(format!(
            "RDF/XML requires QName-splittable predicate IRIs (no '#' or '/'): {iri}"
        ))
    })?;
    let (ns, local) = iri.split_at(idx + 1);
    if local.is_empty() || !is_ncname(local) {
        return Err(FormatError::InvalidBinding(format!(
            "RDF/XML requires predicate local names to be XML NCName; got '{local}' from '{iri}'"
        )));
    }
    Ok((ns, local))
}

fn split_namespace(iri: &str) -> Option<&str> {
    let idx = iri.rfind('#').or_else(|| iri.rfind('/'))?;
    Some(&iri[..=idx])
}

fn is_ncname(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    // Minimal NCName check sufficient for typical RDF IRIs (BSBM, W3C tests).
    // - No ':' allowed
    // - First: letter or '_' (ASCII)
    // - Rest: letter/digit/'_'/'-' '.' (ASCII)
    if first == ':' || !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    for ch in chars {
        if ch == ':' {
            return false;
        }
        if !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == '.') {
            return false;
        }
    }
    true
}

use super::xml_escape::{escape_attr_into, escape_text_into};

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_graph_ir::{Datatype, Graph, LiteralValue, Term, Triple};
    use fluree_vocab::xsd;

    #[test]
    fn rdfxml_basic_graph() {
        let mut g = Graph::new();
        g.add(Triple::new(
            Term::iri("http://example.org/alice"),
            Term::iri("http://example.org/name"),
            Term::Literal {
                value: LiteralValue::string("Alice"),
                datatype: Datatype::from_iri(xsd::STRING),
                language: None,
            },
        ));
        g.sort();

        let xml = format_graph(&g).unwrap();
        assert!(xml.contains("<rdf:RDF"), "{xml}");
        assert!(
            xml.contains(r#"rdf:about="http://example.org/alice""#),
            "{xml}"
        );
        assert!(xml.contains(">Alice<"), "{xml}");
    }

    /// Labels that are not XML names (legacy import ids with `/` and `:`, or a
    /// leading digit) are hex-encoded, so `rdf:nodeID` stays valid.
    #[test]
    fn rdfxml_node_ids_are_xml_names() {
        let mut g = Graph::new();
        g.add(Triple::new(
            Term::iri("_:old/ledger:1"),
            Term::iri("http://example.org/knows"),
            Term::blank("0"),
        ));
        g.sort();
        let xml = format_graph(&g).unwrap();
        assert!(
            xml.contains(r#"rdf:nodeID="x6f6c642f6c65646765723a31""#),
            "{xml}"
        );
        assert!(xml.contains(r#"rdf:nodeID="x30""#), "{xml}");
    }

    #[test]
    fn rdfxml_stored_blank_nodes_use_node_ids() {
        let mut g = Graph::new();
        g.add(Triple::new(
            Term::iri("_:fdb-1"),
            Term::iri("http://example.org/knows"),
            Term::iri("_:fdb-2"),
        ));
        g.add(Triple::new(
            Term::iri("_:fdb-1"),
            Term::iri("http://example.org/age"),
            Term::integer(42),
        ));
        g.sort();

        let xml = format_graph(&g).unwrap();
        assert!(
            xml.contains(r#"<rdf:Description rdf:nodeID="fdb-1">"#),
            "{xml}"
        );
        assert!(xml.contains(r#"rdf:nodeID="fdb-2"/>"#), "{xml}");
        assert!(!xml.contains("rdf:about"), "{xml}");
        assert!(xml.contains(">42<"), "{xml}");
    }
}
