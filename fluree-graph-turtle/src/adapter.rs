//! Graph to transaction JSON adapter.
//!
//! Converts a `fluree_graph_ir::Graph` to JSON format suitable for
//! `fluree-db-transact::parse_transaction()`.

use fluree_graph_ir::{Graph, LiteralValue, Term, Triple};
use fluree_vocab::rdf::TYPE as RDF_TYPE_IRI;
use serde_json::{json, Map, Value as JsonValue};
use std::collections::BTreeMap;

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
pub fn graph_to_transaction_json(graph: &Graph) -> JsonValue {
    // Group triples by subject
    let mut subjects: BTreeMap<String, Vec<&Triple>> = BTreeMap::new();

    for triple in graph.iter() {
        let subject_key = term_to_subject_key(&triple.s);
        subjects.entry(subject_key).or_default().push(triple);
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
                if let Some(type_value) = term_to_type_value(&triple.o) {
                    types.push(type_value);
                }
            } else {
                let obj_value = term_to_object_value(&triple.o);
                predicates.entry(pred_key).or_default().push(obj_value);
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

    JsonValue::Array(nodes)
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

        sink.emit_triple(alice, name, alice_name);

        let graph = sink.finish();
        let json = graph_to_transaction_json(&graph);

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

        sink.emit_triple(bnode, name, value);

        let graph = sink.finish();
        let json = graph_to_transaction_json(&graph);

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

        sink.emit_triple(alice, knows, bob);

        let graph = sink.finish();
        let json = graph_to_transaction_json(&graph);

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

        sink.emit_triple(alice, birthdate, date);

        let graph = sink.finish();
        let json = graph_to_transaction_json(&graph);

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

        sink.emit_triple(alice, name, alice_name);

        let graph = sink.finish();
        let json = graph_to_transaction_json(&graph);

        let arr = json.as_array().unwrap();
        let node = &arr[0];
        let name_arr = node["http://xmlns.com/foaf/0.1/name"].as_array().unwrap();
        assert_eq!(name_arr[0]["@value"], "Alice");
        assert_eq!(name_arr[0]["@language"], "en");
    }
}
