//! Graph to transaction JSON adapter.
//!
//! Converts a `fluree_graph_ir::Graph` to JSON format suitable for
//! `fluree-db-transact::parse_transaction()`.

use crate::error::{Result, TurtleError};
use fluree_graph_ir::{Graph, LiteralValue, Term, Triple};
use fluree_vocab::rdf::TYPE as RDF_TYPE_IRI;
use serde_json::{json, Map, Value as JsonValue};
use std::collections::{BTreeMap, HashMap};

/// Convert a Graph to transaction JSON format.
///
/// The output format is an array of JSON-LD node objects:
/// ```json
/// [
///   {
///     "@id": "http://example.org/alice",
///     "http://xmlns.com/foaf/0.1/name": [{"@value": "Alice"}],
///     "http://example.org/knows": [{"@id": "http://example.org/bob"}]
///   }
/// ]
/// ```
///
/// This matches the expanded JSON-LD format expected by the transaction parser.
///
/// # Reified triples
///
/// RDF 1.2 reifier attachments ([`Graph::reifications`], produced by Turtle
/// `~ reifier`, `{| … |}` and `<< s p o >>`) are emitted as the JSON-LD
/// `@annotation` surface: the reified object value carries
/// `"@annotation": {"@id": "<reifier>"}`, and the reifier's own triples (the
/// annotation body) stay a node of their own. The transaction parser lowers
/// both into one `f:reifies*` bundle, exactly as it does for a hand-written
/// `@annotation` block, so Turtle-star reaches every JSON-LD write path
/// (insert, upsert, graph sync, memory import) with the same semantics as a
/// direct Turtle insert:
///
/// ```json
/// [
///   { "@id": "http://example.org/alice",
///     "http://example.org/knows": [
///       { "@id": "http://example.org/bob",
///         "@annotation": { "@id": "http://example.org/claim1" } }
///     ] },
///   { "@id": "http://example.org/claim1",
///     "http://example.org/confidence": [ { "@value": 0.9, "@type": "…#double" } ] }
/// ]
/// ```
///
/// Several reifiers on one edge produce one annotated object value each
/// (the parallel-annotation shape). A reification is attached to the first
/// occurrence of its base triple only, so a document that states an edge
/// twice does not mint its bundle twice.
///
/// # Errors
///
/// An annotated `rdf:type` edge (`:s a :C {| … |}`) is refused with
/// [`TurtleError::Unsupported`]: JSON-LD `@type` values are bare IRIs with
/// nowhere to hang an `@annotation`, and the transaction parser rejects the
/// expanded `rdf:type` predicate key. The direct Turtle insert path and
/// SPARQL UPDATE accept the shape.
pub fn graph_to_transaction_json(graph: &Graph) -> Result<JsonValue> {
    if let Some(r) = graph
        .reifications()
        .iter()
        .find(|r| r.triple.p.as_iri() == Some(RDF_TYPE_IRI))
    {
        return Err(TurtleError::Unsupported(format!(
            "an annotation on an rdf:type edge (<{}> a <{}> ~ {}) cannot be expressed on \
             this ingest path (upsert, graph sync, memory import): JSON-LD @type values \
             carry no @annotation. Ingest the file with insert, or add the annotation \
             with SPARQL UPDATE.",
            term_to_subject_key(&r.triple.s),
            term_to_iri(&r.triple.o),
            term_to_subject_key(&r.reifier),
        )));
    }

    // Group triples by subject
    let mut subjects: BTreeMap<String, Vec<&Triple>> = BTreeMap::new();

    for triple in graph.iter() {
        let subject_key = term_to_subject_key(&triple.s);
        subjects.entry(subject_key).or_default().push(triple);
    }

    // Reifiers keyed by their base triple; drained as base triples are
    // emitted so each attachment lands exactly once.
    let mut reifiers: HashMap<&Triple, Vec<&Term>> = HashMap::new();
    for reification in graph.reifications() {
        reifiers
            .entry(&reification.triple)
            .or_default()
            .push(&reification.reifier);
    }

    // Build JSON-LD nodes
    let mut nodes = Vec::new();

    for (subject_key, triples) in subjects {
        let mut node = Map::new();

        // Add @id
        node.insert("@id".to_string(), JsonValue::String(subject_key));

        // Group by predicate
        let mut predicates: BTreeMap<String, Vec<JsonValue>> = BTreeMap::new();
        let mut types: Vec<JsonValue> = Vec::new();

        for triple in triples {
            let pred_key = term_to_iri(&triple.p);
            if pred_key == RDF_TYPE_IRI {
                // Never annotated here: the guard above refused those.
                if let Some(type_value) = term_to_type_value(&triple.o) {
                    types.push(type_value);
                }
                continue;
            }
            let attached = reifiers.remove(&Triple::new(
                triple.s.clone(),
                triple.p.clone(),
                triple.o.clone(),
            ));
            let values = predicates.entry(pred_key).or_default();
            match attached {
                Some(attached) => {
                    for reifier in attached {
                        let mut obj_value = term_to_object_value(&triple.o);
                        if let JsonValue::Object(map) = &mut obj_value {
                            map.insert(
                                "@annotation".to_string(),
                                json!({ "@id": term_to_subject_key(reifier) }),
                            );
                        }
                        values.push(obj_value);
                    }
                }
                None => values.push(term_to_object_value(&triple.o)),
            }
        }

        // Add predicates to node
        for (pred, values) in predicates {
            node.insert(pred, JsonValue::Array(values));
        }

        if !types.is_empty() {
            node.insert("@type".to_string(), JsonValue::Array(types));
        }

        nodes.push(JsonValue::Object(node));
    }

    // Every attachment names a base triple the producer also emitted
    // (`GraphSink::emit_reified_triple` contract), so nothing is left over.
    debug_assert!(
        reifiers.is_empty(),
        "reifications without a base triple in the graph: {reifiers:?}"
    );

    Ok(JsonValue::Array(nodes))
}

/// Convert a subject term to a string key.
fn term_to_subject_key(term: &Term) -> String {
    match term {
        Term::Iri(iri) => iri.to_string(),
        Term::BlankNode(id) => format!("_:{}", id.as_str()),
        Term::Literal { .. } => {
            // Literals shouldn't be subjects in RDF, but handle gracefully
            "_:literal".to_string()
        }
    }
}

/// Convert an IRI term to string.
fn term_to_iri(term: &Term) -> String {
    match term {
        Term::Iri(iri) => iri.to_string(),
        _ => "_:invalid".to_string(),
    }
}

/// Convert an object term to a JSON-LD value object.
fn term_to_object_value(term: &Term) -> JsonValue {
    match term {
        Term::Iri(iri) => {
            json!({ "@id": iri.as_ref() })
        }
        Term::BlankNode(id) => {
            json!({ "@id": format!("_:{}", id.as_str()) })
        }
        Term::Literal {
            value,
            datatype,
            language,
        } => {
            let mut obj = Map::new();

            // Add @value
            match value {
                LiteralValue::String(s) => {
                    obj.insert("@value".to_string(), JsonValue::String(s.to_string()));
                }
                LiteralValue::Integer(n) => {
                    obj.insert("@value".to_string(), JsonValue::Number((*n).into()));
                }
                LiteralValue::Double(n) => {
                    if let Some(num) = serde_json::Number::from_f64(*n) {
                        obj.insert("@value".to_string(), JsonValue::Number(num));
                    } else {
                        obj.insert("@value".to_string(), JsonValue::String(n.to_string()));
                    }
                }
                LiteralValue::Boolean(b) => {
                    obj.insert("@value".to_string(), JsonValue::Bool(*b));
                }
                LiteralValue::Json(j) => {
                    // Parse the JSON string back to a value
                    if let Ok(parsed) = serde_json::from_str::<JsonValue>(j.as_ref()) {
                        obj.insert("@value".to_string(), parsed);
                    } else {
                        obj.insert("@value".to_string(), JsonValue::String(j.to_string()));
                    }
                }
            }

            // Add @language if present
            if let Some(lang) = language {
                obj.insert("@language".to_string(), JsonValue::String(lang.to_string()));
            }

            // Add @type for non-string datatypes (skip xsd:string as it's the default)
            if !datatype.is_xsd_string() && !datatype.is_lang_string() {
                obj.insert(
                    "@type".to_string(),
                    JsonValue::String(datatype.as_iri().to_string()),
                );
            }

            JsonValue::Object(obj)
        }
    }
}

fn term_to_type_value(term: &Term) -> Option<JsonValue> {
    match term {
        Term::Iri(iri) => Some(JsonValue::String(iri.to_string())),
        Term::BlankNode(id) => Some(JsonValue::String(format!("_:{}", id.as_str()))),
        Term::Literal { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_graph_ir::{Datatype, GraphCollectorSink, GraphSink};

    #[test]
    fn test_simple_graph_to_json() {
        let mut sink = GraphCollectorSink::new();

        let alice = sink.term_iri("http://example.org/alice");
        let name = sink.term_iri("http://xmlns.com/foaf/0.1/name");
        let alice_name = sink.term_literal("Alice", Datatype::xsd_string(), None);

        sink.emit_triple(alice, name, alice_name).unwrap();

        let graph = sink.into_graph();
        let json = graph_to_transaction_json(&graph).unwrap();

        assert!(json.is_array());
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 1);

        let node = &arr[0];
        assert_eq!(node["@id"], "http://example.org/alice");
        assert!(node["http://xmlns.com/foaf/0.1/name"].is_array());
    }

    #[test]
    fn test_blank_node_to_json() {
        let mut sink = GraphCollectorSink::new();

        let bnode = sink.term_blank(Some("b0"));
        let name = sink.term_iri("http://xmlns.com/foaf/0.1/name");
        let value = sink.term_literal("Bob", Datatype::xsd_string(), None);

        sink.emit_triple(bnode, name, value).unwrap();

        let graph = sink.into_graph();
        let json = graph_to_transaction_json(&graph).unwrap();

        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 1);

        let node = &arr[0];
        assert_eq!(node["@id"], "_:b0");
    }

    #[test]
    fn test_reference_to_json() {
        let mut sink = GraphCollectorSink::new();

        let alice = sink.term_iri("http://example.org/alice");
        let knows = sink.term_iri("http://xmlns.com/foaf/0.1/knows");
        let bob = sink.term_iri("http://example.org/bob");

        sink.emit_triple(alice, knows, bob).unwrap();

        let graph = sink.into_graph();
        let json = graph_to_transaction_json(&graph).unwrap();

        let arr = json.as_array().unwrap();
        let node = &arr[0];
        let knows_arr = node["http://xmlns.com/foaf/0.1/knows"].as_array().unwrap();
        assert_eq!(knows_arr[0]["@id"], "http://example.org/bob");
    }

    #[test]
    fn test_typed_literal_to_json() {
        let mut sink = GraphCollectorSink::new();

        let alice = sink.term_iri("http://example.org/alice");
        let birthdate = sink.term_iri("http://example.org/birthdate");
        let date = sink.term_literal("2000-01-01", Datatype::xsd_date(), None);

        sink.emit_triple(alice, birthdate, date).unwrap();

        let graph = sink.into_graph();
        let json = graph_to_transaction_json(&graph).unwrap();

        let arr = json.as_array().unwrap();
        let node = &arr[0];
        let date_arr = node["http://example.org/birthdate"].as_array().unwrap();
        assert_eq!(date_arr[0]["@value"], "2000-01-01");
        assert_eq!(
            date_arr[0]["@type"],
            "http://www.w3.org/2001/XMLSchema#date"
        );
    }

    #[test]
    fn test_language_tagged_to_json() {
        let mut sink = GraphCollectorSink::new();

        let alice = sink.term_iri("http://example.org/alice");
        let name = sink.term_iri("http://xmlns.com/foaf/0.1/name");
        let alice_name = sink.term_literal("Alice", Datatype::rdf_lang_string(), Some("en"));

        sink.emit_triple(alice, name, alice_name).unwrap();

        let graph = sink.into_graph();
        let json = graph_to_transaction_json(&graph).unwrap();

        let arr = json.as_array().unwrap();
        let node = &arr[0];
        let name_arr = node["http://xmlns.com/foaf/0.1/name"].as_array().unwrap();
        assert_eq!(name_arr[0]["@value"], "Alice");
        assert_eq!(name_arr[0]["@language"], "en");
    }

    // ------------------------------------------------------------------
    // Reified triples → `@annotation`
    // ------------------------------------------------------------------

    fn parse(turtle: &str) -> JsonValue {
        crate::parse_to_json(&format!("@prefix ex: <http://example.org/> .\n{turtle}"))
            .expect("Turtle-star parses on the collector path")
    }

    fn node<'a>(json: &'a JsonValue, id: &str) -> &'a JsonValue {
        json.as_array()
            .unwrap()
            .iter()
            .find(|n| n["@id"] == id)
            .unwrap_or_else(|| panic!("no node {id} in {json:#}"))
    }

    #[test]
    fn named_reifier_becomes_an_annotation_block_and_keeps_its_body_node() {
        let json = parse(
            "ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence 0.9 ; ex:source \"hr\" |} .",
        );
        let alice = node(&json, "http://example.org/alice");
        let knows = alice["http://example.org/knows"].as_array().unwrap();
        assert_eq!(knows.len(), 1);
        assert_eq!(knows[0]["@id"], "http://example.org/bob");
        assert_eq!(
            knows[0]["@annotation"],
            json!({ "@id": "http://example.org/claim1" })
        );
        // The annotation body is the reifier's own node.
        let claim = node(&json, "http://example.org/claim1");
        assert_eq!(claim["http://example.org/source"][0]["@value"], "hr");
        // Turtle `0.9` is an xsd:decimal, carried as a typed string.
        let confidence = &claim["http://example.org/confidence"][0];
        assert_eq!(confidence["@value"], "0.9");
        assert_eq!(
            confidence["@type"],
            "http://www.w3.org/2001/XMLSchema#decimal"
        );
    }

    #[test]
    fn anonymous_annotation_block_mints_a_blank_reifier() {
        let json = parse("ex:alice ex:knows ex:bob {| ex:source \"hr\" |} .");
        let alice = node(&json, "http://example.org/alice");
        let ann = &alice["http://example.org/knows"][0]["@annotation"];
        let reifier = ann["@id"].as_str().expect("blank reifier id");
        assert!(reifier.starts_with("_:"), "{reifier}");
        assert_eq!(
            node(&json, reifier)["http://example.org/source"][0]["@value"],
            "hr"
        );
    }

    #[test]
    fn reified_triple_in_subject_position_annotates_the_base_edge() {
        let json = parse("<< ex:alice ex:knows ex:bob >> ex:certainty 0.5 .");
        let alice = node(&json, "http://example.org/alice");
        let ann = &alice["http://example.org/knows"][0]["@annotation"];
        let reifier = ann["@id"].as_str().expect("blank reifier id");
        assert_eq!(
            node(&json, reifier)["http://example.org/certainty"][0]["@value"],
            "0.5"
        );
    }

    #[test]
    fn annotated_literal_objects_keep_type_and_language() {
        let json = parse(
            "ex:alice ex:age 42 ~ ex:c1 .\n\
             ex:alice ex:name \"Alice\"@en ~ ex:c2 .\n\
             ex:alice ex:nick \"Al\" ~ ex:c3 .",
        );
        let alice = node(&json, "http://example.org/alice");
        let age = &alice["http://example.org/age"][0];
        assert_eq!(age["@value"], 42);
        assert_eq!(age["@type"], "http://www.w3.org/2001/XMLSchema#integer");
        assert_eq!(age["@annotation"]["@id"], "http://example.org/c1");
        let name = &alice["http://example.org/name"][0];
        assert_eq!(name["@language"], "en");
        assert_eq!(name["@annotation"]["@id"], "http://example.org/c2");
        let nick = &alice["http://example.org/nick"][0];
        assert_eq!(
            nick,
            &json!({ "@value": "Al", "@annotation": { "@id": "http://example.org/c3" } })
        );
    }

    #[test]
    fn two_reifiers_on_one_edge_become_parallel_annotated_values() {
        let json = parse("ex:alice ex:knows ex:bob ~ ex:c1 ~ ex:c2 .");
        let alice = node(&json, "http://example.org/alice");
        let knows = alice["http://example.org/knows"].as_array().unwrap();
        assert_eq!(knows.len(), 2, "{knows:#?}");
        let ids: Vec<_> = knows
            .iter()
            .map(|v| v["@annotation"]["@id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, ["http://example.org/c1", "http://example.org/c2"]);
        assert!(knows.iter().all(|v| v["@id"] == "http://example.org/bob"));
    }

    #[test]
    fn restated_base_triple_attaches_its_reifier_once() {
        let json = parse("ex:alice ex:knows ex:bob .\nex:alice ex:knows ex:bob ~ ex:c1 .");
        let alice = node(&json, "http://example.org/alice");
        let knows = alice["http://example.org/knows"].as_array().unwrap();
        let annotated: Vec<_> = knows
            .iter()
            .filter(|v| v.get("@annotation").is_some())
            .collect();
        assert_eq!(annotated.len(), 1, "{knows:#?}");
    }

    #[test]
    fn annotated_type_edge_is_refused_with_a_clear_error() {
        let err = crate::parse_to_json(
            "@prefix ex: <http://example.org/> .\n\
             ex:alice a ex:Person {| ex:source \"hr\" |} ; a ex:Employee .",
        )
        .expect_err("annotated rdf:type edges have no JSON-LD @annotation home");
        assert!(matches!(err, TurtleError::Unsupported(_)), "{err:?}");
        let msg = err.to_string();
        assert!(msg.contains("rdf:type") && msg.contains("insert"), "{msg}");
        // Un-annotated type edges are unaffected.
        let json = parse("ex:alice a ex:Person ; ex:knows ex:bob {| ex:source \"hr\" |} .");
        assert_eq!(
            node(&json, "http://example.org/alice")["@type"],
            json!(["http://example.org/Person"])
        );
    }
}
