//! A reader's answer turned into nodes for the document's commit.
//!
//! The answer is a JSON-LD dataset: the document graph it was sent, echoed
//! under the document's IRI, and the reading in graphs of its own. The echo
//! is dropped (the commit already carries the structure graph), and the
//! reading is made to fit the rest of the document's commit:
//!
//! - offsets are moved from the part's text back to the document's, and the
//!   IRIs a reader mints under the document are kept apart per part, so two
//!   calls' `…/entity/1` stay two entities;
//! - every node is stamped `doc:sourceDocument`, which is what re-ingest
//!   retracts by;
//! - a reified statement (`rdf:Statement`) is a `doc:Relation`, and is marked
//!   `doc:asserted` when the reading also asserts its triple with an IRI
//!   object, so re-ingest can take that edge away with it;
//! - a reader whose terms are not this pipeline's has its [`Vocabulary`]
//!   applied, the one place its terms are translated.

use crate::vocab::{DOC_NS, NIF_NS, RDF_NS, XSD_NS};
use crate::{DocError, Result};
use serde_json::{json, Map, Value};
use std::collections::BTreeSet;

/// How a reader's terms translate into this pipeline's. IRIs matching a term
/// are replaced whole; otherwise the first matching namespace is swapped.
/// Applied to every IRI: predicates, types, node ids and datatypes.
#[derive(Debug)]
pub struct Vocabulary {
    pub terms: &'static [(&'static str, &'static str)],
    pub namespaces: &'static [(&'static str, &'static str)],
}

impl Vocabulary {
    pub fn translate<'a>(&self, iri: &'a str) -> std::borrow::Cow<'a, str> {
        if let Some((_, to)) = self.terms.iter().find(|(from, _)| *from == iri) {
            return std::borrow::Cow::Owned((*to).to_string());
        }
        for (from, to) in self.namespaces {
            if let Some(rest) = iri.strip_prefix(from) {
                return std::borrow::Cow::Owned(format!("{to}{rest}"));
            }
        }
        std::borrow::Cow::Borrowed(iri)
    }
}

/// Readers whose output needs translating, by the model ids they serve.
/// A model with no entry is stored in its own terms.
#[derive(Debug)]
pub struct Profile {
    pub models: &'static [&'static str],
    pub vocabulary: Vocabulary,
}

const PROFILES: &[Profile] = &[];

pub fn profile(model: &str) -> Option<&'static Profile> {
    PROFILES
        .iter()
        .find(|p| p.models.iter().any(|m| m.eq_ignore_ascii_case(model)))
}

/// Where a part's answer sits in the whole document.
#[derive(Debug, Clone, Copy)]
pub struct Frame<'a> {
    pub doc_iri: &'a str,
    /// The part's first character in the document's text.
    pub offset: usize,
    /// The part's index, and how many parts the document was read in.
    pub part: usize,
    pub parts: usize,
}

/// One part's reading, ready for the commit.
#[derive(Debug, Default)]
pub struct Reading {
    /// Expanded JSON-LD nodes.
    pub nodes: Vec<Value>,
    pub mentions: usize,
    pub entities: usize,
    pub relations: usize,
    /// Records outside the reading its mentions link to (`nif:entity`):
    /// the catalog's, as against the entities it minted.
    pub linked: BTreeSet<String>,
}

const BEGIN: &str = "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#beginIndex";
const END: &str = "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#endIndex";

pub fn read_answer(
    body: &str,
    frame: &Frame<'_>,
    vocabulary: Option<&Vocabulary>,
) -> Result<Reading> {
    let value: Value = serde_json::from_str(body)
        .map_err(|e| DocError::Model(format!("reading is not JSON: {e}")))?;
    let expanded = if has_context(&value) {
        fluree_graph_json_ld::expand(&value)
            .map_err(|e| DocError::Model(format!("reading is not JSON-LD: {e}")))?
    } else {
        value
    };
    let items = match expanded {
        Value::Array(items) => items,
        Value::Object(o) => match o.get("@graph") {
            Some(Value::Array(g)) if !o.contains_key("@id") => g.clone(),
            _ => vec![Value::Object(o)],
        },
        _ => {
            return Err(DocError::Model(
                "reading: expected a JSON-LD dataset".into(),
            ))
        }
    };
    // Graphs other than the echoed document, and default-graph nodes other
    // than the document node.
    let mut nodes = Vec::new();
    for item in items {
        let id = item.get("@id").and_then(Value::as_str);
        match item.get("@graph") {
            Some(Value::Array(graph)) => {
                if id != Some(frame.doc_iri) {
                    nodes.extend(graph.iter().cloned());
                }
            }
            _ if id == Some(frame.doc_iri) => {}
            _ => nodes.push(item),
        }
    }

    let rdf_type = format!("{RDF_NS}type");
    let statement = format!("{RDF_NS}Statement");
    let mut out = Vec::with_capacity(nodes.len());
    for node in nodes {
        let Value::Object(node) = node else { continue };
        out.push(rewrite_node(node, frame, vocabulary, &rdf_type));
    }

    // Triples the reading asserts with an IRI object, for `doc:asserted`.
    let mut asserted = BTreeSet::new();
    for node in &out {
        let Some(s) = node.get("@id").and_then(Value::as_str) else {
            continue;
        };
        for (p, values) in node.as_object().into_iter().flatten() {
            if p.starts_with('@') {
                continue;
            }
            for v in values.as_array().into_iter().flatten() {
                if let Some(o) = v.get("@id").and_then(Value::as_str) {
                    asserted.insert((s.to_string(), p.clone(), o.to_string()));
                }
            }
        }
    }
    let relation = format!("{DOC_NS}Relation");
    let mut reading = Reading::default();
    for node in &mut out {
        let types = types(node);
        if types.contains(&statement) || types.contains(&relation) {
            if !types.contains(&relation) {
                push_type(node, &relation);
            }
            let first_iri = |p: &str| {
                node.get(p)
                    .and_then(Value::as_array)
                    .and_then(|a| a.first())
                    .and_then(|v| v.get("@id"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
            };
            let triple = (
                first_iri(&format!("{RDF_NS}subject")),
                first_iri(&format!("{RDF_NS}predicate")),
                first_iri(&format!("{RDF_NS}object")),
            );
            if let (Some(s), Some(p), Some(o)) = triple {
                if asserted.contains(&(s, p, o)) {
                    node[format!("{DOC_NS}asserted")] = json!([{ "@value": true }]);
                }
            }
            reading.relations += 1;
        }
        if types.contains(&format!("{DOC_NS}Mention")) {
            reading.mentions += 1;
        }
        if types.contains(&format!("{DOC_NS}Entity")) {
            reading.entities += 1;
        }
    }
    let entity_p = format!("{NIF_NS}entity");
    let minted: BTreeSet<&str> = out
        .iter()
        .filter(|n| types(n).contains(&format!("{DOC_NS}Entity")))
        .filter_map(|n| n.get("@id").and_then(Value::as_str))
        .collect();
    reading.linked = out
        .iter()
        .filter_map(|n| n.get(&entity_p).and_then(Value::as_array))
        .flatten()
        .filter_map(|v| v.get("@id").and_then(Value::as_str))
        .filter(|iri| !minted.contains(iri))
        .map(str::to_string)
        .collect();
    reading.nodes = out;
    Ok(reading)
}

fn rewrite_node(
    node: Map<String, Value>,
    frame: &Frame<'_>,
    vocabulary: Option<&Vocabulary>,
    rdf_type: &str,
) -> Value {
    let iri = |s: &str| -> String {
        let local = scope_iri(s, frame);
        match vocabulary {
            Some(v) => v.translate(&local).into_owned(),
            None => local,
        }
    };
    let mut out = Map::new();
    let mut types: Vec<Value> = Vec::new();
    for (key, value) in node {
        match key.as_str() {
            "@id" => {
                let id = value.as_str().map(&iri).map(Value::String).unwrap_or(value);
                out.insert(key, id);
            }
            "@type" => {
                for t in value
                    .as_array()
                    .cloned()
                    .unwrap_or_else(|| vec![value.clone()])
                {
                    if let Some(t) = t.as_str() {
                        types.push(Value::String(iri(t)));
                    }
                }
            }
            _ => {
                let predicate = iri(&key);
                let shifted = predicate == BEGIN || predicate == END;
                let values: Vec<Value> = value
                    .as_array()
                    .cloned()
                    .unwrap_or_else(|| vec![value.clone()])
                    .into_iter()
                    .map(|v| rewrite_value(v, &iri, shifted.then_some(frame.offset)))
                    .collect();
                // `rdf:type` written as a predicate is folded into `@type`.
                if predicate == rdf_type {
                    types.extend(values.iter().filter_map(|v| v.get("@id").cloned()));
                    continue;
                }
                match out.get_mut(&predicate) {
                    Some(Value::Array(existing)) => existing.extend(values),
                    _ => {
                        out.insert(predicate, Value::Array(values));
                    }
                }
            }
        }
    }
    if !types.is_empty() {
        out.insert("@type".into(), Value::Array(types));
    }
    let source = format!("{DOC_NS}sourceDocument");
    out.entry(source)
        .or_insert_with(|| json!([{ "@id": frame.doc_iri }]));
    Value::Object(out)
}

fn rewrite_value(value: Value, iri: &dyn Fn(&str) -> String, shift: Option<usize>) -> Value {
    let Value::Object(mut v) = value else {
        return value;
    };
    if let Some(Value::String(id)) = v.get("@id") {
        let id = iri(id);
        v.insert("@id".into(), Value::String(id));
        return Value::Object(v);
    }
    if let Some(Value::String(dt)) = v.get("@type") {
        let dt = iri(dt);
        v.insert("@type".into(), Value::String(dt));
    }
    if let Some(offset) = shift.filter(|&o| o > 0) {
        let n = match v.get("@value") {
            Some(Value::Number(n)) => n.as_u64(),
            Some(Value::String(s)) => s.parse().ok(),
            _ => None,
        };
        if let Some(n) = n {
            v.insert("@value".into(), json!(n + offset as u64));
            v.insert("@type".into(), json!(format!("{XSD_NS}integer")));
        }
    }
    Value::Object(v)
}

/// An IRI the reader minted under the document (`<doc>/…`, `<doc>#…`) is
/// moved under the part it came from when the document took several calls;
/// every other IRI (catalog records, the ontology, the structure graph's own
/// `<doc>-element-N`) is left alone.
fn scope_iri(iri: &str, frame: &Frame<'_>) -> String {
    if frame.parts <= 1 {
        return iri.to_string();
    }
    match iri.strip_prefix(frame.doc_iri) {
        Some(rest) if rest.starts_with('/') || rest.starts_with('#') => {
            format!("{}/part-{}{rest}", frame.doc_iri, frame.part + 1)
        }
        _ => iri.to_string(),
    }
}

fn has_context(v: &Value) -> bool {
    match v {
        Value::Object(o) => o.contains_key("@context"),
        Value::Array(a) => a.iter().any(has_context),
        _ => false,
    }
}

fn types(node: &Value) -> Vec<String> {
    node.get("@type")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn push_type(node: &mut Value, t: &str) {
    match node.get_mut("@type") {
        Some(Value::Array(a)) => a.push(Value::String(t.to_string())),
        _ => node["@type"] = json!([t]),
    }
}

/// Offsets in a reading, for tests and for `doc read`: every `nif:beginIndex`
/// in the nodes.
pub fn begin_offsets(nodes: &[Value]) -> Vec<u64> {
    let key = format!("{NIF_NS}beginIndex");
    nodes
        .iter()
        .filter_map(|n| n.get(&key))
        .filter_map(Value::as_array)
        .flatten()
        .filter_map(|v| v.get("@value"))
        .filter_map(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const EX: &str = "http://example.org/r#";

    /// A reader's answer in a vocabulary of its own: the echoed document
    /// graph, a reading graph with one occurrence linked to a catalog
    /// record, a local entity and two reified claims, one with an IRI object.
    fn answer() -> String {
        json!([
            { "@id": "urn:d", "@graph": [
                { "@id": "urn:d", "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#isString": [{ "@value": "text" }] }
            ]},
            { "@id": "urn:d/reading", "@graph": [
                { "@id": "urn:d/reading#o4-9",
                  format!("{RDF_NS}type"): [{ "@id": format!("{EX}Occurrence") }],
                  BEGIN: [{ "@type": format!("{XSD_NS}integer"), "@value": "4" }],
                  END: [{ "@type": format!("{XSD_NS}integer"), "@value": "9" }],
                  format!("{EX}refersTo"): [{ "@id": "urn:catalog:acme" }] },
                { "@id": "urn:d/entity/1",
                  format!("{RDF_NS}type"): [{ "@id": format!("{EX}LocalEntity") }],
                  "urn:onto:supplier": [{ "@id": "urn:catalog:acme" }],
                  "urn:onto:total": [{ "@value": "10" }] },
                { "@id": "urn:d/reading#r-0",
                  format!("{RDF_NS}type"): [{ "@id": format!("{RDF_NS}Statement") }],
                  format!("{RDF_NS}subject"): [{ "@id": "urn:d/entity/1" }],
                  format!("{RDF_NS}predicate"): [{ "@id": "urn:onto:supplier" }],
                  format!("{RDF_NS}object"): [{ "@id": "urn:catalog:acme" }] },
                { "@id": "urn:d/reading#r-1",
                  format!("{RDF_NS}type"): [{ "@id": format!("{RDF_NS}Statement") }],
                  format!("{RDF_NS}subject"): [{ "@id": "urn:d/entity/1" }],
                  format!("{RDF_NS}predicate"): [{ "@id": "urn:onto:total" }],
                  format!("{RDF_NS}object"): [{ "@value": "10" }] }
            ]}
        ])
        .to_string()
    }

    fn node<'a>(r: &'a Reading, id: &str) -> &'a Value {
        r.nodes
            .iter()
            .find(|n| n["@id"] == id)
            .unwrap_or_else(|| panic!("no node {id} in {:#?}", r.nodes))
    }

    #[test]
    fn the_echo_is_dropped_and_every_node_is_stamped() {
        let frame = Frame {
            doc_iri: "urn:d",
            offset: 0,
            part: 0,
            parts: 1,
        };
        let r = read_answer(&answer(), &frame, None).unwrap();
        assert_eq!(r.nodes.len(), 4, "{:#?}", r.nodes);
        for n in &r.nodes {
            assert_eq!(n[format!("{DOC_NS}sourceDocument")][0]["@id"], "urn:d");
        }
        // `rdf:type` as a predicate became `@type`.
        assert_eq!(
            node(&r, "urn:d/reading#o4-9")["@type"][0],
            format!("{EX}Occurrence")
        );
    }

    #[test]
    fn a_statement_is_a_relation_asserted_only_with_an_iri_object() {
        let frame = Frame {
            doc_iri: "urn:d",
            offset: 0,
            part: 0,
            parts: 1,
        };
        let r = read_answer(&answer(), &frame, None).unwrap();
        assert_eq!(r.relations, 2);
        let asserted = format!("{DOC_NS}asserted");
        let r0 = node(&r, "urn:d/reading#r-0");
        assert!(types(r0).contains(&format!("{DOC_NS}Relation")));
        assert_eq!(r0[&asserted][0]["@value"], true);
        assert!(node(&r, "urn:d/reading#r-1").get(&asserted).is_none());
    }

    #[test]
    fn a_later_part_moves_offsets_and_keeps_its_minted_iris_apart() {
        let frame = Frame {
            doc_iri: "urn:d",
            offset: 100,
            part: 1,
            parts: 2,
        };
        let r = read_answer(&answer(), &frame, None).unwrap();
        let occ = node(&r, "urn:d/part-2/reading#o4-9");
        assert_eq!(occ[BEGIN][0]["@value"], 104);
        assert_eq!(occ[END][0]["@value"], 109);
        // The catalog link is untouched; the local entity moved with its part.
        assert_eq!(occ[format!("{EX}refersTo")][0]["@id"], "urn:catalog:acme");
        let r0 = node(&r, "urn:d/part-2/reading#r-0");
        assert_eq!(
            r0[format!("{RDF_NS}subject")][0]["@id"],
            "urn:d/part-2/entity/1"
        );
        assert_eq!(r0[format!("{DOC_NS}asserted")][0]["@value"], true);
    }

    #[test]
    fn a_vocabulary_translates_terms_and_namespaces_everywhere() {
        static V: Vocabulary = Vocabulary {
            terms: &[
                (
                    "http://example.org/r#Occurrence",
                    "https://ns.flur.ee/doc#Mention",
                ),
                (
                    "http://example.org/r#refersTo",
                    "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#entity",
                ),
            ],
            namespaces: &[("http://example.org/r#", "https://ns.flur.ee/doc#")],
        };
        let frame = Frame {
            doc_iri: "urn:d",
            offset: 0,
            part: 0,
            parts: 1,
        };
        let r = read_answer(&answer(), &frame, Some(&V)).unwrap();
        let occ = node(&r, "urn:d/reading#o4-9");
        assert_eq!(occ["@type"][0], "https://ns.flur.ee/doc#Mention");
        assert_eq!(occ[format!("{NIF_NS}entity")][0]["@id"], "urn:catalog:acme");
        assert_eq!(
            node(&r, "urn:d/entity/1")["@type"][0],
            "https://ns.flur.ee/doc#LocalEntity"
        );
        assert_eq!(r.mentions, 1);
    }

    #[test]
    fn a_compacted_answer_is_expanded_first() {
        let compact = json!({
            "@context": { "ex": EX },
            "@graph": [{ "@id": "urn:d/reading", "@graph": [{ "@id": "urn:d/reading#x", "@type": "ex:Occurrence" }] }]
        })
        .to_string();
        let frame = Frame {
            doc_iri: "urn:d",
            offset: 0,
            part: 0,
            parts: 1,
        };
        let r = read_answer(&compact, &frame, None).unwrap();
        assert_eq!(
            node(&r, "urn:d/reading#x")["@type"][0],
            format!("{EX}Occurrence")
        );
    }
}
