//! GraphSink adapter for JSON-LD
//!
//! This module provides functions to convert expanded JSON-LD documents into
//! GraphSink events, allowing integration with `fluree-graph-ir`.
//!
//! # Features
//!
//! - Converts expanded JSON-LD (output of `expand()`) to RDF triples
//! - Preserves blank node identity (same `_:label` produces same blank node)
//! - Handles `@type` → `rdf:type` conversion
//! - Handles language-tagged strings and typed literals
//! - Handles `@json` typed values with canonical normalization
//!
//! # Limitations
//!
//! - **`@graph`**: Named graphs are flattened into the default graph (no quad support)
//! - **`@reverse`**: Reverse properties are not yet supported
//!
//! # Example
//!
//! ```
//! use fluree_graph_json_ld::{expand, adapter::to_graph_events};
//! use fluree_graph_ir::{Graph, GraphCollectorSink};
//! use serde_json::json;
//!
//! let doc = json!({
//!     "@context": {"ex": "http://example.org/"},
//!     "@id": "ex:alice",
//!     "ex:name": "Alice"
//! });
//!
//! let expanded = expand(&doc).unwrap();
//! let mut sink = GraphCollectorSink::new();
//! to_graph_events(&expanded, &mut sink).unwrap();
//!
//! let graph = sink.graph();
//! assert_eq!(graph.len(), 1);
//! ```

use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, TermId};
use fluree_vocab::rdf;
use serde_json::Value;
use std::sync::Arc;

/// Strip `_:` prefix from blank node label if present
///
/// BlankId expects labels WITHOUT the `_:` prefix.
fn strip_blank_prefix(id: &str) -> &str {
    id.strip_prefix("_:").unwrap_or(id)
}

/// Error type for adapter operations
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    /// Invalid expanded JSON-LD structure
    #[error("Invalid expanded JSON-LD: {0}")]
    InvalidStructure(String),
}

/// Result type for adapter operations
pub type Result<T> = std::result::Result<T, AdapterError>;

/// Represents the result of processing a JSON-LD value
///
/// Used to distinguish between single values, lists, and absent values.
enum ProcessedValue {
    /// A single term (IRI, literal, or blank node)
    Single(TermId),
    /// An ordered list of terms with their indices
    List(Vec<(i32, TermId)>),
    /// No value (e.g., null or unsupported structure)
    None,
}

/// Convert expanded JSON-LD to GraphSink events
///
/// The input must be expanded JSON-LD (output of `expand()`). This function
/// walks the expanded structure and emits triple events to the sink.
///
/// # Arguments
///
/// * `expanded` - Expanded JSON-LD document (array or object)
/// * `sink` - GraphSink to receive triple events
///
/// # Example
///
/// ```
/// use fluree_graph_json_ld::{expand, adapter::to_graph_events};
/// use fluree_graph_ir::GraphCollectorSink;
/// use serde_json::json;
///
/// let doc = json!({
///     "@id": "http://example.org/alice",
///     "http://xmlns.com/foaf/0.1/name": [{"@value": "Alice"}]
/// });
///
/// // Document is already in expanded form
/// let mut sink = GraphCollectorSink::new();
/// to_graph_events(&doc, &mut sink).unwrap();
///
/// let graph = sink.graph();
/// assert_eq!(graph.len(), 1);
/// ```
pub fn to_graph_events<S: GraphSink>(expanded: &Value, sink: &mut S) -> Result<()> {
    match expanded {
        Value::Array(arr) => {
            for item in arr {
                process_node(item, sink, None)?;
            }
        }
        Value::Object(_) => {
            process_node(expanded, sink, None)?;
        }
        _ => {
            return Err(AdapterError::InvalidStructure(
                "Expected expanded array or object".to_string(),
            ))
        }
    }
    Ok(())
}

/// Process a single node in the expanded JSON-LD
///
/// If `forced_subject` is provided, it will be used as the subject instead of
/// deriving one from `@id`. This is used for embedded nodes without `@id` to
/// ensure the object edge and the embedded node's triples use the same blank node.
fn process_node<S: GraphSink>(
    node: &Value,
    sink: &mut S,
    forced_subject: Option<TermId>,
) -> Result<TermId> {
    let obj = node.as_object().ok_or_else(|| {
        AdapterError::InvalidStructure("Expected node to be an object".to_string())
    })?;

    // Get subject from forced_subject, @id, or generate blank node
    let subject_id = if let Some(id) = forced_subject {
        id
    } else if let Some(id_val) = obj.get("@id") {
        let id_str = id_val
            .as_str()
            .ok_or_else(|| AdapterError::InvalidStructure("@id must be a string".to_string()))?;
        if id_str.starts_with("_:") {
            sink.term_blank(Some(strip_blank_prefix(id_str)))
        } else {
            sink.term_iri(id_str)
        }
    } else {
        // Anonymous blank node
        sink.term_blank(None)
    };

    // Process each predicate-object pair
    for (key, value) in obj {
        // Skip JSON-LD keywords except @type
        if key.starts_with('@') && key != "@type" {
            continue;
        }

        // Handle @type specially (maps to rdf:type)
        if key == "@type" {
            let rdf_type_id = sink.term_iri(rdf::TYPE);

            let types = match value {
                Value::Array(arr) => arr.iter().collect::<Vec<_>>(),
                _ => vec![value],
            };

            for type_val in types {
                if let Some(type_iri) = type_val.as_str() {
                    let object_id = sink.term_iri(type_iri);
                    sink.emit_triple(subject_id, rdf_type_id, object_id);
                }
            }
            continue;
        }

        // Regular predicate (expanded IRI)
        let predicate_id = sink.term_iri(key);

        // Process values (always an array in expanded form)
        let values = match value {
            Value::Array(arr) => arr.iter().collect::<Vec<_>>(),
            _ => vec![value],
        };

        for val in values {
            match process_value(val, sink)? {
                ProcessedValue::Single(object_id) => {
                    sink.emit_triple(subject_id, predicate_id, object_id);
                }
                ProcessedValue::List(items) => {
                    for (index, object_id) in items {
                        sink.emit_list_item(subject_id, predicate_id, object_id, index);
                    }
                }
                ProcessedValue::None => {}
            }
        }
    }

    Ok(subject_id)
}

/// Process a value and return the processed result
fn process_value<S: GraphSink>(value: &Value, sink: &mut S) -> Result<ProcessedValue> {
    match value {
        Value::Object(obj) => {
            // Check for @id (reference to another node)
            if let Some(id_val) = obj.get("@id") {
                let id_str = id_val.as_str().ok_or_else(|| {
                    AdapterError::InvalidStructure("@id must be a string".to_string())
                })?;
                let term_id = if id_str.starts_with("_:") {
                    sink.term_blank(Some(strip_blank_prefix(id_str)))
                } else {
                    sink.term_iri(id_str)
                };
                return Ok(ProcessedValue::Single(term_id));
            }

            // Check for @value (literal)
            if let Some(val) = obj.get("@value") {
                return process_literal(val, obj, sink);
            }

            // Check for @list
            if let Some(list_val) = obj.get("@list") {
                return process_list(list_val, sink);
            }

            // Nested/embedded node without @id - allocate blank node first,
            // then process the node using that same blank node as subject.
            // This ensures the object edge and embedded node triples share identity.
            let subject_id = sink.term_blank(None);
            process_node(value, sink, Some(subject_id))?;
            Ok(ProcessedValue::Single(subject_id))
        }
        // Direct scalar values (shouldn't happen in properly expanded JSON-LD)
        Value::String(s) => Ok(ProcessedValue::Single(sink.term_literal(
            s,
            Datatype::xsd_string(),
            None,
        ))),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(ProcessedValue::Single(sink.term_literal_value(
                    LiteralValue::Integer(i),
                    Datatype::xsd_integer(),
                )))
            } else if let Some(f) = n.as_f64() {
                Ok(ProcessedValue::Single(sink.term_literal_value(
                    LiteralValue::Double(f),
                    Datatype::xsd_double(),
                )))
            } else {
                Ok(ProcessedValue::None)
            }
        }
        Value::Bool(b) => Ok(ProcessedValue::Single(
            sink.term_literal_value(LiteralValue::Boolean(*b), Datatype::xsd_boolean()),
        )),
        _ => Ok(ProcessedValue::None),
    }
}

/// Process a @list value and return the list items with indices
fn process_list<S: GraphSink>(list_val: &Value, sink: &mut S) -> Result<ProcessedValue> {
    let items = match list_val {
        Value::Array(arr) => arr,
        _ => {
            return Err(AdapterError::InvalidStructure(
                "@list value must be an array".to_string(),
            ))
        }
    };

    let mut indexed_items = Vec::with_capacity(items.len());

    for (index, item) in items.iter().enumerate() {
        // Process each list item as a single value (lists cannot be nested directly)
        match process_list_item(item, sink)? {
            Some(term_id) => {
                indexed_items.push((index as i32, term_id));
            }
            None => {
                // Skip null/unsupported items in list
            }
        }
    }

    Ok(ProcessedValue::List(indexed_items))
}

/// Process a single list item (cannot return a list, only single values)
fn process_list_item<S: GraphSink>(value: &Value, sink: &mut S) -> Result<Option<TermId>> {
    match value {
        Value::Object(obj) => {
            // Check for @id (reference to another node)
            if let Some(id_val) = obj.get("@id") {
                let id_str = id_val.as_str().ok_or_else(|| {
                    AdapterError::InvalidStructure("@id must be a string".to_string())
                })?;
                return Ok(Some(if id_str.starts_with("_:") {
                    sink.term_blank(Some(strip_blank_prefix(id_str)))
                } else {
                    sink.term_iri(id_str)
                }));
            }

            // Check for @value (literal)
            if let Some(val) = obj.get("@value") {
                return match process_literal(val, obj, sink)? {
                    ProcessedValue::Single(id) => Ok(Some(id)),
                    _ => Ok(None),
                };
            }

            // Nested @list inside @list is an error in JSON-LD
            if obj.contains_key("@list") {
                return Err(AdapterError::InvalidStructure(
                    "Nested @list is not allowed".to_string(),
                ));
            }

            // Embedded node in list - allocate blank node and process
            let subject_id = sink.term_blank(None);
            process_node(value, sink, Some(subject_id))?;
            Ok(Some(subject_id))
        }
        // Direct scalar values
        Value::String(s) => Ok(Some(sink.term_literal(s, Datatype::xsd_string(), None))),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Some(sink.term_literal_value(
                    LiteralValue::Integer(i),
                    Datatype::xsd_integer(),
                )))
            } else if let Some(f) = n.as_f64() {
                Ok(Some(sink.term_literal_value(
                    LiteralValue::Double(f),
                    Datatype::xsd_double(),
                )))
            } else {
                Ok(None)
            }
        }
        Value::Bool(b) => Ok(Some(
            sink.term_literal_value(LiteralValue::Boolean(*b), Datatype::xsd_boolean()),
        )),
        _ => Ok(None),
    }
}

/// Process a literal value with @value, @type, @language
fn process_literal<S: GraphSink>(
    val: &Value,
    obj: &serde_json::Map<String, Value>,
    sink: &mut S,
) -> Result<ProcessedValue> {
    // Check for @type (datatype)
    let datatype = if let Some(type_val) = obj.get("@type") {
        if let Some(type_iri) = type_val.as_str() {
            Datatype::from_iri(type_iri)
        } else {
            Datatype::xsd_string()
        }
    } else {
        Datatype::xsd_string()
    };

    // Check for @language
    let language = obj
        .get("@language")
        .and_then(|v| v.as_str())
        .map(std::string::ToString::to_string);

    // Handle the actual value
    match val {
        Value::String(s) => {
            if language.is_some() {
                // Language-tagged string
                Ok(ProcessedValue::Single(sink.term_literal(
                    s,
                    Datatype::rdf_lang_string(),
                    language.as_deref(),
                )))
            } else {
                Ok(ProcessedValue::Single(sink.term_literal(s, datatype, None)))
            }
        }
        Value::Number(n) => {
            // When the declared @type decodes as F64 (float, double, decimal),
            // always produce LiteralValue::Double even if the JSON number is an
            // integer. Otherwise the integer bits get stored as NUM_INT but
            // decoded as F64, producing garbage subnormal values after indexing.
            // (fluree/db-r#142)
            let is_float_type = {
                let iri = datatype.as_iri();
                iri == fluree_vocab::xsd::DOUBLE
                    || iri == fluree_vocab::xsd::FLOAT
                    || iri == fluree_vocab::xsd::DECIMAL
            };
            if is_float_type {
                if let Some(f) = n.as_f64() {
                    Ok(ProcessedValue::Single(
                        sink.term_literal_value(LiteralValue::Double(f), datatype),
                    ))
                } else {
                    Ok(ProcessedValue::None)
                }
            } else if let Some(i) = n.as_i64() {
                Ok(ProcessedValue::Single(
                    sink.term_literal_value(LiteralValue::Integer(i), datatype),
                ))
            } else if let Some(f) = n.as_f64() {
                Ok(ProcessedValue::Single(
                    sink.term_literal_value(LiteralValue::Double(f), datatype),
                ))
            } else {
                Ok(ProcessedValue::None)
            }
        }
        Value::Bool(b) => Ok(ProcessedValue::Single(
            sink.term_literal_value(LiteralValue::Boolean(*b), datatype),
        )),
        // Handle @json typed values
        Value::Object(_) | Value::Array(_) => {
            if datatype.is_json() {
                let json_str = crate::normalize::normalize(val);
                Ok(ProcessedValue::Single(sink.term_literal_value(
                    LiteralValue::Json(Arc::from(json_str.as_str())),
                    Datatype::rdf_json(),
                )))
            } else if matches!(val, Value::Array(_))
                && datatype.as_iri() == fluree_vocab::fluree::EMBEDDING_VECTOR
            {
                // Vector arrays come through here once JSON-LD expansion produces
                // {"@value": [...], "@type": f:embeddingVector}. Stringify and
                // route through the typed-string-literal path so that
                // value_convert::convert_string_literal (and its core::coerce
                // delegate) does the f32-quantized parse — keeping JSON-LD,
                // Turtle, and SPARQL on a single shared parser.
                let lexical = serde_json::to_string(val).map_err(|e| {
                    AdapterError::InvalidStructure(format!(
                        "failed to serialize vector @value: {e}"
                    ))
                })?;
                Ok(ProcessedValue::Single(sink.term_literal(
                    &lexical,
                    Datatype::from_iri(fluree_vocab::fluree::EMBEDDING_VECTOR),
                    None,
                )))
            } else {
                // Non-JSON object/array in @value - shouldn't happen
                Ok(ProcessedValue::None)
            }
        }
        Value::Null => Ok(ProcessedValue::None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_graph_ir::{GraphCollectorSink, Term};
    use fluree_vocab::rdf;
    use serde_json::json;

    #[test]
    fn test_simple_triple() {
        let expanded = json!([{
            "@id": "http://example.org/alice",
            "http://xmlns.com/foaf/0.1/name": [{"@value": "Alice"}]
        }]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let graph = sink.graph();
        assert_eq!(graph.len(), 1);

        let triple = graph.iter().next().unwrap();
        assert!(matches!(&triple.s, Term::Iri(iri) if iri.as_ref() == "http://example.org/alice"));
        assert!(
            matches!(&triple.p, Term::Iri(iri) if iri.as_ref() == "http://xmlns.com/foaf/0.1/name")
        );
        assert!(
            matches!(&triple.o, Term::Literal { value: LiteralValue::String(s), .. } if s.as_ref() == "Alice")
        );
    }

    #[test]
    fn test_typed_literal() {
        let expanded = json!([{
            "@id": "http://example.org/alice",
            "http://xmlns.com/foaf/0.1/age": [{
                "@value": 30,
                "@type": "http://www.w3.org/2001/XMLSchema#integer"
            }]
        }]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let graph = sink.graph();
        assert_eq!(graph.len(), 1);

        let triple = graph.iter().next().unwrap();
        assert!(matches!(
            &triple.o,
            Term::Literal {
                value: LiteralValue::Integer(30),
                ..
            }
        ));
    }

    #[test]
    fn test_language_tagged_string() {
        let expanded = json!([{
            "@id": "http://example.org/alice",
            "http://xmlns.com/foaf/0.1/name": [{
                "@value": "Alice",
                "@language": "en"
            }]
        }]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let graph = sink.graph();
        assert_eq!(graph.len(), 1);

        let triple = graph.iter().next().unwrap();
        match &triple.o {
            Term::Literal {
                value,
                language,
                datatype,
            } => {
                assert!(matches!(value, LiteralValue::String(s) if s.as_ref() == "Alice"));
                assert!(language.as_ref().map(std::convert::AsRef::as_ref) == Some("en"));
                assert!(datatype.is_lang_string());
            }
            _ => panic!("Expected literal"),
        }
    }

    #[test]
    fn test_rdf_type() {
        let expanded = json!([{
            "@id": "http://example.org/alice",
            "@type": ["http://xmlns.com/foaf/0.1/Person"]
        }]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let graph = sink.graph();
        assert_eq!(graph.len(), 1);

        let triple = graph.iter().next().unwrap();
        assert!(matches!(&triple.p, Term::Iri(iri) if iri.as_ref() == rdf::TYPE));
        assert!(
            matches!(&triple.o, Term::Iri(iri) if iri.as_ref() == "http://xmlns.com/foaf/0.1/Person")
        );
    }

    #[test]
    fn test_reference() {
        let expanded = json!([{
            "@id": "http://example.org/alice",
            "http://xmlns.com/foaf/0.1/knows": [{"@id": "http://example.org/bob"}]
        }]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let graph = sink.graph();
        assert_eq!(graph.len(), 1);

        let triple = graph.iter().next().unwrap();
        assert!(matches!(&triple.o, Term::Iri(iri) if iri.as_ref() == "http://example.org/bob"));
    }

    #[test]
    fn test_blank_node() {
        let expanded = json!([{
            "@id": "_:b0",
            "http://xmlns.com/foaf/0.1/name": [{"@value": "Anonymous"}]
        }]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let graph = sink.graph();
        assert_eq!(graph.len(), 1);

        let triple = graph.iter().next().unwrap();
        assert!(matches!(&triple.s, Term::BlankNode(_)));
    }

    #[test]
    fn test_multiple_values() {
        let expanded = json!([{
            "@id": "http://example.org/alice",
            "http://xmlns.com/foaf/0.1/name": [
                {"@value": "Alice"},
                {"@value": "Alicia", "@language": "es"}
            ]
        }]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let graph = sink.graph();
        assert_eq!(graph.len(), 2);
    }

    #[test]
    fn test_multiple_types() {
        let expanded = json!([{
            "@id": "http://example.org/alice",
            "@type": [
                "http://xmlns.com/foaf/0.1/Person",
                "http://schema.org/Person"
            ]
        }]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let graph = sink.graph();
        assert_eq!(graph.len(), 2);
    }

    #[test]
    fn test_with_expand() {
        use crate::expand;

        let doc = json!({
            "@context": {"ex": "http://example.org/", "name": "ex:name"},
            "@id": "ex:alice",
            "name": "Alice"
        });

        let expanded = expand(&doc).unwrap();
        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let graph = sink.graph();
        assert_eq!(graph.len(), 1);

        let triple = graph.iter().next().unwrap();
        assert!(matches!(&triple.s, Term::Iri(iri) if iri.as_ref() == "http://example.org/alice"));
        assert!(matches!(&triple.p, Term::Iri(iri) if iri.as_ref() == "http://example.org/name"));
    }

    #[test]
    fn test_blank_node_identity() {
        // Same blank node (_:x) referenced from two different subjects
        let expanded = json!([
            {
                "@id": "http://example.org/alice",
                "http://xmlns.com/foaf/0.1/knows": [{"@id": "_:x"}]
            },
            {
                "@id": "http://example.org/bob",
                "http://xmlns.com/foaf/0.1/knows": [{"@id": "_:x"}]
            },
            {
                "@id": "_:x",
                "http://xmlns.com/foaf/0.1/name": [{"@value": "Charlie"}]
            }
        ]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let graph = sink.graph();
        // 3 triples: alice knows _:x, bob knows _:x, _:x name "Charlie"
        assert_eq!(graph.len(), 3);

        // Verify both "knows" triples point to the same blank node
        let knows_triples: Vec<_> = graph
            .iter()
            .filter(|t| matches!(&t.p, Term::Iri(iri) if iri.as_ref().contains("knows")))
            .collect();
        assert_eq!(knows_triples.len(), 2);

        // Both should have the same blank node object
        match (&knows_triples[0].o, &knows_triples[1].o) {
            (Term::BlankNode(id1), Term::BlankNode(id2)) => {
                assert_eq!(
                    id1.as_str(),
                    id2.as_str(),
                    "Blank nodes should have same identity"
                );
            }
            _ => panic!("Expected blank node objects"),
        }
    }

    #[test]
    fn test_blank_node_label_no_prefix() {
        // Verify that blank node labels are stored WITHOUT the `_:` prefix
        // BlankId expects labels like "b0", not "_:b0"
        let expanded = json!([{
            "@id": "_:myblank",
            "http://xmlns.com/foaf/0.1/name": [{"@value": "Test"}]
        }]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let graph = sink.graph();
        let triple = graph.iter().next().unwrap();

        match &triple.s {
            Term::BlankNode(id) => {
                // Should be "myblank", not "_:myblank"
                assert_eq!(id.as_str(), "myblank");
                // The N-Triples representation should add the prefix
                assert_eq!(id.to_ntriples(), "_:myblank");
            }
            _ => panic!("Expected blank node subject"),
        }
    }

    #[test]
    fn test_embedded_node_without_id() {
        // Embedded node without @id should use the SAME blank node for:
        // 1. The object edge from the parent
        // 2. The subject of the embedded node's triples
        let expanded = json!([{
            "@id": "http://example.org/alice",
            "http://xmlns.com/foaf/0.1/knows": [{
                // No @id - this is an embedded anonymous node
                "http://xmlns.com/foaf/0.1/name": [{"@value": "Bob"}]
            }]
        }]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let graph = sink.graph();
        // 2 triples: alice knows _:bX, _:bX name "Bob"
        assert_eq!(graph.len(), 2);

        // Find the "knows" triple (alice -> bnode)
        let knows_triple = graph
            .iter()
            .find(|t| matches!(&t.p, Term::Iri(iri) if iri.as_ref().contains("knows")))
            .expect("Should have knows triple");

        // Find the "name" triple (bnode -> "Bob")
        let name_triple = graph
            .iter()
            .find(|t| matches!(&t.p, Term::Iri(iri) if iri.as_ref().contains("name")))
            .expect("Should have name triple");

        // The object of "knows" should be the same blank node as the subject of "name"
        match (&knows_triple.o, &name_triple.s) {
            (Term::BlankNode(obj_id), Term::BlankNode(subj_id)) => {
                assert_eq!(
                    obj_id.as_str(),
                    subj_id.as_str(),
                    "Object of parent edge should match subject of embedded node"
                );
            }
            _ => panic!("Expected blank nodes"),
        }
    }

    // =========================================================================
    // @list parsing tests
    // =========================================================================

    #[test]
    fn test_list_with_scalar_values() {
        let expanded = json!([{
            "@id": "http://example.org/alice",
            "http://example.org/favorites": [{
                "@list": [
                    {"@value": "Alice"},
                    {"@value": "Bob"},
                    {"@value": "Charlie"}
                ]
            }]
        }]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let mut graph = sink.finish();
        assert_eq!(graph.len(), 3, "Should have 3 list item triples");

        // All triples should be list elements
        for triple in graph.iter() {
            assert!(
                triple.is_list_element(),
                "All triples should be list elements"
            );
        }

        // Sort and verify order
        graph.sort();
        let items: Vec<_> = graph.iter().collect();

        assert_eq!(items[0].list_index(), Some(0));
        assert_eq!(items[1].list_index(), Some(1));
        assert_eq!(items[2].list_index(), Some(2));

        // Verify values
        assert!(
            matches!(&items[0].o, Term::Literal { value: LiteralValue::String(s), .. } if s.as_ref() == "Alice")
        );
        assert!(
            matches!(&items[1].o, Term::Literal { value: LiteralValue::String(s), .. } if s.as_ref() == "Bob")
        );
        assert!(
            matches!(&items[2].o, Term::Literal { value: LiteralValue::String(s), .. } if s.as_ref() == "Charlie")
        );
    }

    #[test]
    fn test_list_with_iri_references() {
        let expanded = json!([{
            "@id": "http://example.org/collection",
            "http://example.org/members": [{
                "@list": [
                    {"@id": "http://example.org/alice"},
                    {"@id": "http://example.org/bob"}
                ]
            }]
        }]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let mut graph = sink.finish();
        assert_eq!(graph.len(), 2);

        graph.sort();
        let items: Vec<_> = graph.iter().collect();

        assert_eq!(items[0].list_index(), Some(0));
        assert!(
            matches!(&items[0].o, Term::Iri(iri) if iri.as_ref() == "http://example.org/alice")
        );

        assert_eq!(items[1].list_index(), Some(1));
        assert!(matches!(&items[1].o, Term::Iri(iri) if iri.as_ref() == "http://example.org/bob"));
    }

    #[test]
    fn test_list_with_embedded_nodes() {
        let expanded = json!([{
            "@id": "http://example.org/alice",
            "http://example.org/friends": [{
                "@list": [
                    {
                        // Embedded node without @id
                        "http://xmlns.com/foaf/0.1/name": [{"@value": "Bob"}]
                    },
                    {
                        "http://xmlns.com/foaf/0.1/name": [{"@value": "Charlie"}]
                    }
                ]
            }]
        }]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let graph = sink.finish();
        // 2 list item triples + 2 name triples for embedded nodes = 4
        assert_eq!(
            graph.len(),
            4,
            "Should have 4 triples (2 list items + 2 embedded names)"
        );

        // Find list element triples
        let list_triples: Vec<_> = graph.iter().filter(|t| t.is_list_element()).collect();
        assert_eq!(list_triples.len(), 2, "Should have 2 list element triples");

        // List items should point to blank nodes
        for triple in &list_triples {
            assert!(
                matches!(&triple.o, Term::BlankNode(_)),
                "List items should be blank nodes"
            );
        }
    }

    #[test]
    fn test_list_with_mixed_values() {
        let expanded = json!([{
            "@id": "http://example.org/test",
            "http://example.org/mixed": [{
                "@list": [
                    {"@value": "string"},
                    {"@value": 42},
                    {"@value": true},
                    {"@id": "http://example.org/ref"}
                ]
            }]
        }]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let mut graph = sink.finish();
        assert_eq!(graph.len(), 4);

        graph.sort();
        let items: Vec<_> = graph.iter().collect();

        // Verify types and indices
        assert_eq!(items[0].list_index(), Some(0));
        assert!(
            matches!(&items[0].o, Term::Literal { value: LiteralValue::String(s), .. } if s.as_ref() == "string")
        );

        assert_eq!(items[1].list_index(), Some(1));
        assert!(matches!(
            &items[1].o,
            Term::Literal {
                value: LiteralValue::Integer(42),
                ..
            }
        ));

        assert_eq!(items[2].list_index(), Some(2));
        assert!(matches!(
            &items[2].o,
            Term::Literal {
                value: LiteralValue::Boolean(true),
                ..
            }
        ));

        assert_eq!(items[3].list_index(), Some(3));
        assert!(matches!(&items[3].o, Term::Iri(iri) if iri.as_ref() == "http://example.org/ref"));
    }

    #[test]
    fn test_empty_list() {
        let expanded = json!([{
            "@id": "http://example.org/test",
            "http://example.org/empty": [{
                "@list": []
            }]
        }]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let graph = sink.finish();
        // Empty list produces no triples
        assert_eq!(graph.len(), 0, "Empty list should produce no triples");
    }

    #[test]
    fn test_list_with_duplicate_values() {
        let expanded = json!([{
            "@id": "http://example.org/test",
            "http://example.org/dupes": [{
                "@list": [
                    {"@value": "repeat"},
                    {"@value": "middle"},
                    {"@value": "repeat"}
                ]
            }]
        }]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let mut graph = sink.finish();
        assert_eq!(
            graph.len(),
            3,
            "Should preserve all items including duplicates"
        );

        graph.sort();
        let items: Vec<_> = graph.iter().collect();

        // Verify duplicates are preserved with correct indices
        assert_eq!(items[0].list_index(), Some(0));
        assert!(
            matches!(&items[0].o, Term::Literal { value: LiteralValue::String(s), .. } if s.as_ref() == "repeat")
        );

        assert_eq!(items[1].list_index(), Some(1));
        assert!(
            matches!(&items[1].o, Term::Literal { value: LiteralValue::String(s), .. } if s.as_ref() == "middle")
        );

        assert_eq!(items[2].list_index(), Some(2));
        assert!(
            matches!(&items[2].o, Term::Literal { value: LiteralValue::String(s), .. } if s.as_ref() == "repeat")
        );
    }

    #[test]
    fn test_list_with_expand() {
        use crate::expand;

        let doc = json!({
            "@context": {
                "ex": "http://example.org/",
                "items": {"@id": "ex:items", "@container": "@list"}
            },
            "@id": "ex:test",
            "items": ["a", "b", "c"]
        });

        let expanded = expand(&doc).unwrap();
        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let mut graph = sink.finish();
        assert_eq!(graph.len(), 3, "Should have 3 list items");

        graph.sort();
        for (i, triple) in graph.iter().enumerate() {
            assert_eq!(triple.list_index(), Some(i as i32));
        }
    }

    /// Regression test for fluree/db-r#142: JSON integer @value with xsd:float
    /// or xsd:double @type must produce LiteralValue::Double, not Integer.
    #[test]
    fn test_float_typed_integer_produces_double() {
        let expanded = json!([{
            "@id": "http://example.org/campaign1",
            "http://example.org/ns#budget": [{
                "@value": 1_350_000,
                "@type": "http://www.w3.org/2001/XMLSchema#float"
            }],
            "http://example.org/ns#revenue": [{
                "@value": 5_000_000,
                "@type": "http://www.w3.org/2001/XMLSchema#double"
            }]
        }]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let graph = sink.graph();
        assert_eq!(graph.len(), 2);

        for triple in graph.iter() {
            match &triple.o {
                Term::Literal { value, .. } => {
                    assert!(
                        matches!(value, LiteralValue::Double(_)),
                        "Expected Double for float-typed integer, got {value:?}"
                    );
                }
                other => panic!("Expected literal, got {other:?}"),
            }
        }
    }
}
