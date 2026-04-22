//! Minimal RDF/XML to N-Triples converter for W3C test data files.
//!
//! This parser handles the subset of RDF/XML used by W3C SPARQL test data
//! (e.g., `subquery/sq01.rdf`):
//! - `<rdf:Description rdf:about="...">` subjects
//! - Property elements with `rdf:resource="..."` (IRI objects)
//! - Property elements with `rdf:datatype="..."` + text (typed literals)
//! - Plain text content (string literals)
//!
//! Not a general-purpose RDF/XML parser — intentionally scoped for test data.

use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use quick_xml::events::Event;
use quick_xml::Reader;

/// Parse an RDF/XML document and return its triples as an N-Triples string.
///
/// N-Triples is a valid Turtle subset, so the output can be fed directly to
/// `insert_turtle()`. All IRIs in the output are absolute.
///
/// `base_url` is used to resolve relative IRIs (e.g., `rdf:resource=""`).
pub fn rdfxml_to_ntriples(content: &str, base_url: &str) -> Result<String> {
    let mut reader = Reader::from_str(content);
    let mut output = String::new();

    let mut namespaces: HashMap<String, String> = HashMap::new();
    let mut current_subject: Option<String> = None;
    let mut current_predicate: Option<String> = None;
    let mut current_resource: Option<String> = None;
    let mut current_datatype: Option<String> = None;
    let mut text_buf = String::new();
    let mut in_property = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let (prefix, local) = split_qname(e.name().as_ref());
                collect_namespaces(e, &mut namespaces);

                if local == "RDF" {
                    // Root element — namespaces already collected
                } else if local == "Description" && prefix_is_rdf(&prefix, &namespaces) {
                    current_subject = extract_attr(e, "about", &namespaces, base_url);
                } else if current_subject.is_some() {
                    // Opening property element — content/end follows
                    current_predicate = Some(resolve_qname(&prefix, &local, &namespaces)?);
                    current_resource = extract_attr(e, "resource", &namespaces, base_url);
                    current_datatype = extract_attr(e, "datatype", &namespaces, base_url);
                    text_buf.clear();
                    in_property = true;
                }
            }
            Ok(Event::Empty(ref e)) => {
                let (prefix, local) = split_qname(e.name().as_ref());
                collect_namespaces(e, &mut namespaces);

                if current_subject.is_some()
                    && !(local == "Description" && prefix_is_rdf(&prefix, &namespaces))
                {
                    // Self-closing property (e.g., <ex:p rdf:resource="..."/>)
                    let pred = resolve_qname(&prefix, &local, &namespaces)?;
                    if let Some(ref subj) = current_subject {
                        if let Some(obj_iri) = extract_attr(e, "resource", &namespaces, base_url) {
                            write_triple_iri(&mut output, subj, &pred, &obj_iri);
                        }
                    }
                }
            }
            Ok(Event::Text(ref e)) if in_property => {
                if let Ok(unescaped) = e.unescape() {
                    text_buf.push_str(&unescaped);
                }
            }
            Ok(Event::End(ref e)) => {
                let (prefix, local) = split_qname(e.name().as_ref());

                if local == "Description" && prefix_is_rdf(&prefix, &namespaces) {
                    current_subject = None;
                } else if in_property {
                    if let (Some(ref subj), Some(ref pred)) = (&current_subject, &current_predicate)
                    {
                        if let Some(ref iri) = current_resource {
                            write_triple_iri(&mut output, subj, pred, iri);
                        } else {
                            let val = text_buf.trim().to_string();
                            if !val.is_empty() {
                                write_triple_literal(
                                    &mut output,
                                    subj,
                                    pred,
                                    &val,
                                    current_datatype.as_deref(),
                                );
                            }
                        }
                    }
                    current_predicate = None;
                    current_resource = None;
                    current_datatype = None;
                    text_buf.clear();
                    in_property = false;
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => bail!("RDF/XML parse error: {e}"),
            _ => {}
        }
    }

    Ok(output)
}

/// Split a qualified XML name (e.g., b"rdf:Description") into (prefix, local).
fn split_qname(name: &[u8]) -> (String, String) {
    let name_str = String::from_utf8_lossy(name);
    if let Some(pos) = name_str.find(':') {
        (name_str[..pos].to_string(), name_str[pos + 1..].to_string())
    } else {
        (String::new(), name_str.to_string())
    }
}

/// Check if a prefix maps to the RDF namespace.
fn prefix_is_rdf(prefix: &str, namespaces: &HashMap<String, String>) -> bool {
    namespaces
        .get(prefix)
        .is_some_and(|ns| ns == "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
}

/// Collect xmlns: namespace declarations from an element's attributes.
fn collect_namespaces(e: &quick_xml::events::BytesStart, namespaces: &mut HashMap<String, String>) {
    for attr in e.attributes().flatten() {
        let key = String::from_utf8_lossy(attr.key.0).to_string();
        if let Some(prefix) = key.strip_prefix("xmlns:") {
            namespaces.insert(
                prefix.to_string(),
                String::from_utf8_lossy(&attr.value).to_string(),
            );
        }
    }
}

/// Extract an RDF attribute value (e.g., `rdf:about`, `rdf:resource`).
///
/// Matches any prefix that maps to the RDF namespace. Resolves empty relative
/// IRIs against `base_url`.
fn extract_attr(
    e: &quick_xml::events::BytesStart,
    attr_local: &str,
    namespaces: &HashMap<String, String>,
    base_url: &str,
) -> Option<String> {
    for attr in e.attributes().flatten() {
        let key = String::from_utf8_lossy(attr.key.0).to_string();
        let (aprefix, alocal) = if let Some(pos) = key.find(':') {
            (&key[..pos], &key[pos + 1..])
        } else {
            ("", key.as_str())
        };

        if alocal != attr_local {
            continue;
        }

        // Accept if prefix maps to RDF namespace, or if unprefixed
        let is_rdf_attr = aprefix.is_empty()
            || namespaces
                .get(aprefix)
                .is_some_and(|ns| ns == "http://www.w3.org/1999/02/22-rdf-syntax-ns#");

        if is_rdf_attr {
            let value = String::from_utf8_lossy(&attr.value).to_string();
            if value.is_empty() {
                return Some(base_url.to_string());
            }
            return Some(value);
        }
    }
    None
}

/// Resolve a prefixed XML name to a full IRI.
fn resolve_qname(
    prefix: &str,
    local: &str,
    namespaces: &HashMap<String, String>,
) -> Result<String> {
    if prefix.is_empty() {
        bail!("Unprefixed property element: {local}");
    }
    let ns = namespaces
        .get(prefix)
        .with_context(|| format!("Unknown namespace prefix: {prefix}"))?;
    Ok(format!("{ns}{local}"))
}

fn write_triple_iri(output: &mut String, subject: &str, predicate: &str, object: &str) {
    output.push_str(&format!("<{subject}> <{predicate}> <{object}> .\n"));
}

fn write_triple_literal(
    output: &mut String,
    subject: &str,
    predicate: &str,
    value: &str,
    datatype: Option<&str>,
) {
    let escaped = escape_ntriples(value);
    match datatype {
        Some(dt) => output.push_str(&format!(
            "<{subject}> <{predicate}> \"{escaped}\"^^<{dt}> .\n"
        )),
        None => output.push_str(&format!("<{subject}> <{predicate}> \"{escaped}\" .\n")),
    }
}

fn escape_ntriples(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iri_object() {
        let xml = r#"<rdf:RDF
            xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
            xmlns:ex="http://example.org/schema#">
          <rdf:Description rdf:about="http://example.org/a">
            <ex:p rdf:resource="http://example.org/b"/>
          </rdf:Description>
        </rdf:RDF>"#;

        let nt = rdfxml_to_ntriples(xml, "http://base/").unwrap();
        assert_eq!(
            nt.trim(),
            "<http://example.org/a> <http://example.org/schema#p> <http://example.org/b> ."
        );
    }

    #[test]
    fn test_typed_literal() {
        let xml = r#"<rdf:RDF
            xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
            xmlns:ex="http://example.org/schema#">
          <rdf:Description rdf:about="http://example.org/a">
            <ex:p rdf:datatype="http://www.w3.org/2001/XMLSchema#integer">42</ex:p>
          </rdf:Description>
        </rdf:RDF>"#;

        let nt = rdfxml_to_ntriples(xml, "http://base/").unwrap();
        assert!(nt.contains(
            r#"<http://example.org/a> <http://example.org/schema#p> "42"^^<http://www.w3.org/2001/XMLSchema#integer> ."#
        ));
    }

    #[test]
    fn test_plain_literal() {
        let xml = r#"<rdf:RDF
            xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
            xmlns:ex="http://example.org/schema#">
          <rdf:Description rdf:about="http://example.org/a">
            <ex:name>Alice</ex:name>
          </rdf:Description>
        </rdf:RDF>"#;

        let nt = rdfxml_to_ntriples(xml, "http://base/").unwrap();
        assert!(nt.contains(r#"<http://example.org/a> <http://example.org/schema#name> "Alice" ."#));
    }

    #[test]
    fn test_empty_resource_resolves_to_base() {
        let xml = r#"<rdf:RDF
            xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
            xmlns:ex="http://example.org/schema#">
          <rdf:Description rdf:about="http://example.org/c">
            <ex:p rdf:resource=""/>
          </rdf:Description>
        </rdf:RDF>"#;

        let nt = rdfxml_to_ntriples(xml, "http://base/doc.rdf").unwrap();
        assert!(nt.contains(
            "<http://example.org/c> <http://example.org/schema#p> <http://base/doc.rdf> ."
        ));
    }

    #[test]
    fn test_multiple_subjects_and_properties() {
        let xml = r#"<rdf:RDF
            xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
            xmlns:ex="http://example.org/schema#">
          <rdf:Description rdf:about="http://example.org/a">
            <ex:p rdf:resource="http://example.org/b"/>
            <ex:q rdf:resource="http://example.org/c"/>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org/d">
            <ex:p rdf:resource="http://example.org/e"/>
          </rdf:Description>
        </rdf:RDF>"#;

        let nt = rdfxml_to_ntriples(xml, "http://base/").unwrap();
        let lines: Vec<&str> = nt.trim().lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(nt.contains(
            "<http://example.org/a> <http://example.org/schema#p> <http://example.org/b> ."
        ));
        assert!(nt.contains(
            "<http://example.org/a> <http://example.org/schema#q> <http://example.org/c> ."
        ));
        assert!(nt.contains(
            "<http://example.org/d> <http://example.org/schema#p> <http://example.org/e> ."
        ));
    }

    /// Verify the parser handles the actual W3C subquery test data format.
    #[test]
    fn test_sq01_format() {
        let xml = r#"<rdf:RDF
            xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
            xmlns:in="http://www.example.org/instance#"
            xmlns:ex="http://www.example.org/schema#"
            xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#" >
          <rdf:Description rdf:about="http://www.example.org/instance#a">
            <ex:p rdf:resource="http://www.example.org/instance#b"/>
          </rdf:Description>
          <rdf:Description rdf:about="http://www.example.org/instance#c">
            <ex:p rdf:resource=""/>
          </rdf:Description>
        </rdf:RDF>"#;

        let base = "http://www.example.org/data/sq01.rdf";
        let nt = rdfxml_to_ntriples(xml, base).unwrap();
        let lines: Vec<&str> = nt.trim().lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(nt.contains(
            "<http://www.example.org/instance#a> <http://www.example.org/schema#p> <http://www.example.org/instance#b> ."
        ));
        assert!(nt.contains(&format!(
            "<http://www.example.org/instance#c> <http://www.example.org/schema#p> <{base}> ."
        )));
    }

    #[test]
    fn test_sq08_typed_literals() {
        let xml = r#"<rdf:RDF
            xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
            xmlns:ex="http://www.example.org/schema#">
          <rdf:Description rdf:about="http://www.example.org/instance#a">
            <ex:p rdf:datatype='http://www.w3.org/2001/XMLSchema#integer'>1</ex:p>
            <ex:p rdf:datatype='http://www.w3.org/2001/XMLSchema#integer'>2</ex:p>
          </rdf:Description>
          <rdf:Description rdf:about="http://www.example.org/instance#b">
            <ex:p rdf:datatype='http://www.w3.org/2001/XMLSchema#integer'>3</ex:p>
          </rdf:Description>
        </rdf:RDF>"#;

        let nt = rdfxml_to_ntriples(xml, "http://base/").unwrap();
        let lines: Vec<&str> = nt.trim().lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(nt.contains(
            r#"<http://www.example.org/instance#a> <http://www.example.org/schema#p> "1"^^<http://www.w3.org/2001/XMLSchema#integer> ."#
        ));
    }
}
