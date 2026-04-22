//! RDF/XML graph serializer (`application/rdf+xml`)
//!
//! This formatter is intended for SPARQL CONSTRUCT/DESCRIBE (graph results).
//! It serializes the instantiated construct graph as RDF/XML.

use super::config::FormatterConfig;
use super::construct::instantiate_construct_graph;
use super::iri::IriCompactor;
use super::{FormatError, Result};
use crate::QueryResult;

use fluree_graph_ir::{Graph, Term};

use std::collections::{BTreeMap, BTreeSet};

const RDF_NS: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

pub fn format(
    result: &QueryResult,
    compactor: &IriCompactor,
    _config: &FormatterConfig,
) -> Result<String> {
    if result.output.construct_template().is_none() {
        return Err(FormatError::InvalidBinding(
            "RDF/XML is only valid for graph results (SPARQL CONSTRUCT/DESCRIBE)".to_string(),
        ));
    }

    let mut graph = instantiate_construct_graph(result, compactor)?;
    graph.sort();
    format_graph(&graph)
}

fn format_graph(graph: &Graph) -> Result<String> {
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

fn write_subject_attr(subject: &Term, out: &mut String) -> Result<()> {
    match subject {
        Term::Iri(iri) => {
            out.push_str(r#" rdf:about=""#);
            escape_attr_into(iri.as_ref(), out);
            out.push('"');
            Ok(())
        }
        Term::BlankNode(id) => {
            out.push_str(r#" rdf:nodeID=""#);
            escape_attr_into(id.as_str(), out);
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
        Term::Iri(iri) => {
            out.push_str(r#" rdf:resource=""#);
            escape_attr_into(iri.as_ref(), out);
            out.push_str(r#""/>"#);
            Ok(())
        }
        Term::BlankNode(id) => {
            out.push_str(r#" rdf:nodeID=""#);
            escape_attr_into(id.as_str(), out);
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
            escape_text_into(&value.lexical(), out);
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
}
