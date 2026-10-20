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
//! # Graph scoping
//!
//! A node's statements, and those of every node nested in it, are written to
//! the node's graph: its own graph selector (`"@graph": "<graph>"`, Fluree's
//! extension) if it has one, else the enclosing node's. A JSON-LD named graph
//! (`{"@id": G, "@graph": [nodes]}`) keeps its own properties in the
//! enclosing graph and writes its content nodes to `G`. Embedded nodes, list
//! members, `@included` nodes and `@reverse` statements share their node's
//! graph. This is how the transaction parser reads a document.
//!
//! [`to_graph_events_scoped`] writes the statements of a named graph as quads
//! ([`fluree_graph_ir::GraphSink::emit_quad`]) and asks the caller to resolve
//! each graph name. [`to_graph_events`] has no resolver and refuses graph
//! scoping with [`AdapterError::Unsupported`], as does a sink without quad
//! support: folding the statements into the default graph would lose data.
//! (A top-level envelope is unwrapped by expansion and never reaches the
//! adapter.)
//!
//! `@reverse` and `@included` are read as the transaction parser reads them:
//! a reverse property's values are the subjects of statements pointing at the
//! node, and included nodes are described alongside it with no linking
//! statement. Any other keyword on a node object is refused, as the
//! transaction parser refuses it.
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

use crate::graph_shape::{node_graph, NodeGraph};
use crate::is_keyword_form;
use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, TermId};
use fluree_vocab::{rdf, reifies_iris};
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

    /// The sink refused an emitted event, or its downstream writer failed.
    /// Conversion stops at the first such error.
    #[error("Sink error: {0}")]
    Sink(#[from] fluree_graph_ir::SinkError),

    /// Well-formed JSON-LD this adapter cannot represent faithfully.
    #[error("{0}")]
    Unsupported(String),
}

/// Result type for adapter operations
pub type Result<T> = std::result::Result<T, AdapterError>;

/// A graph name a document writes, as [`to_graph_events_scoped`] meets it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphNameRef<'a> {
    /// A node's graph selector (`"@graph": "ex:g"`), as written: compact,
    /// relative or absolute, or a keyword such as `default`.
    Selector(&'a str),
    /// The (expanded) `@id` of a JSON-LD named graph, already checked by
    /// [`node_graph`]: not a blank node and not a variable.
    NamedGraph(&'a str),
}

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

/// The graph a node's statements are written to.
#[derive(Debug, Clone, Copy)]
enum Scope {
    Default,
    /// A named graph, by its IRI term.
    Named(TermId),
}

/// Convert expanded JSON-LD to GraphSink events
///
/// The input must be expanded JSON-LD (output of `expand()`). This function
/// walks the expanded structure and emits triple events to the sink. Graph
/// scoping is refused; [`to_graph_events_scoped`] writes it.
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
    to_graph_events_scoped(expanded, sink, |_| {
        Err(AdapterError::Unsupported(
            "JSON-LD `@graph` graph scoping is not supported here".to_string(),
        ))
    })
}

/// Convert expanded JSON-LD to GraphSink events, writing each statement to
/// its graph (see the [module docs](self)).
///
/// `resolve` names each graph the document writes: `Ok(None)` is the default
/// graph, `Ok(Some(iri))` the named graph `iri` (absolute), and an error
/// refuses the document. A named graph needs a sink that supports quads.
pub fn to_graph_events_scoped<S, F>(expanded: &Value, sink: &mut S, resolve: F) -> Result<()>
where
    S: GraphSink,
    F: FnMut(GraphNameRef<'_>) -> Result<Option<String>>,
{
    let mut walk = Walk { sink, resolve };
    match expanded {
        Value::Array(arr) => {
            for item in arr {
                walk.node(item, Scope::Default, None)?;
            }
        }
        Value::Object(_) => {
            walk.node(expanded, Scope::Default, None)?;
        }
        _ => {
            return Err(AdapterError::InvalidStructure(
                "Expected expanded array or object".to_string(),
            ))
        }
    }
    Ok(())
}

/// One walk over an expanded document: the sink, and the caller's graph-name
/// resolver.
struct Walk<'s, S, F> {
    sink: &'s mut S,
    resolve: F,
}

impl<S, F> Walk<'_, S, F>
where
    S: GraphSink,
    F: FnMut(GraphNameRef<'_>) -> Result<Option<String>>,
{
    /// The scope a graph name opens.
    fn open(&mut self, name: GraphNameRef<'_>) -> Result<Scope> {
        let Some(iri) = (self.resolve)(name)? else {
            return Ok(Scope::Default);
        };
        if !self.sink.supports_quads() {
            return Err(AdapterError::Unsupported(format!(
                "graph <{iri}>: this sink cannot write named graphs"
            )));
        }
        Ok(Scope::Named(self.sink.term_iri(&iri)))
    }

    /// Emit one statement in `scope`.
    fn emit(
        &mut self,
        scope: Scope,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        index: Option<i32>,
    ) -> Result<()> {
        let emitted = match (scope, index) {
            (Scope::Default, None) => self.sink.emit_triple(subject, predicate, object),
            (Scope::Default, Some(index)) => {
                self.sink.emit_list_item(subject, predicate, object, index)
            }
            (Scope::Named(graph), None) => self.sink.emit_quad(subject, predicate, object, graph),
            (Scope::Named(graph), Some(index)) => self
                .sink
                .emit_quad_list_item(subject, predicate, object, graph, index),
        };
        Ok(emitted?)
    }

    /// Process a single node in the expanded JSON-LD, in `scope` unless it
    /// opens its own.
    ///
    /// If `forced_subject` is provided, it will be used as the subject instead of
    /// deriving one from `@id`. This is used for embedded nodes without `@id` to
    /// ensure the object edge and the embedded node's triples use the same blank node.
    fn node(
        &mut self,
        node: &Value,
        scope: Scope,
        forced_subject: Option<TermId>,
    ) -> Result<TermId> {
        let obj = node.as_object().ok_or_else(|| {
            AdapterError::InvalidStructure("Expected node to be an object".to_string())
        })?;

        // A node's own graph selector opens its scope. A JSON-LD named
        // graph's own properties stay in the enclosing scope, and its
        // content nodes are written to the graph its `@id` names (after the
        // properties, below).
        let mut named_graph_content: Option<(&str, &[Value])> = None;
        let scope = match node_graph(obj).map_err(AdapterError::InvalidStructure)? {
            NodeGraph::Enclosing => scope,
            NodeGraph::Selector(raw) => self.open(GraphNameRef::Selector(raw))?,
            NodeGraph::NamedGraph { name, content } => {
                named_graph_content = Some((name, content));
                scope
            }
        };

        // Get subject from forced_subject, @id, or generate blank node
        let subject_id = if let Some(id) = forced_subject {
            id
        } else if let Some(id_val) = obj.get("@id") {
            let id_str = id_val.as_str().ok_or_else(|| {
                AdapterError::InvalidStructure("@id must be a string".to_string())
            })?;
            if id_str.starts_with("_:") {
                self.sink.term_blank(Some(strip_blank_prefix(id_str)))
            } else {
                self.sink.term_iri(id_str)
            }
        } else {
            // Anonymous blank node
            self.sink.term_blank(None)
        };

        // Process each predicate-object pair
        for (key, value) in obj {
            match key.as_str() {
                // Handled above, or (`@index`) carries no statement.
                "@id" | "@graph" | "@index" => continue,
                // Nodes described alongside this one, with no linking statement.
                "@included" => {
                    for node in node_items(value, "@included")? {
                        self.node(node, scope, None)?;
                    }
                    continue;
                }
                // Statements whose object is this node.
                "@reverse" => {
                    let Value::Object(properties) = value else {
                        return Err(AdapterError::InvalidStructure(
                            "@reverse must be a map of properties".to_string(),
                        ));
                    };
                    for (property, values) in properties {
                        let predicate_id = self.sink.term_iri(property);
                        for node in node_items(values, "a reverse property's value")? {
                            if let ProcessedValue::Single(node_id) = self.value(node, scope)? {
                                self.emit(scope, node_id, predicate_id, subject_id, None)?;
                            }
                        }
                    }
                    continue;
                }
                "@type" => {}
                // Skipping a keyword would drop what it says.
                k if is_keyword_form(k) => {
                    return Err(AdapterError::Unsupported(format!(
                        "JSON-LD keyword {k:?} is not supported on a node object"
                    )));
                }
                // A key that merely starts with `@` (`@odata.etag`) expands to
                // no IRI, so JSON-LD expansion drops it.
                k if k.starts_with('@') => continue,
                _ => {}
            }

            // Handle @type specially (maps to rdf:type)
            if key == "@type" {
                let rdf_type_id = self.sink.term_iri(rdf::TYPE);

                let types = match value {
                    Value::Array(arr) => arr.iter().collect::<Vec<_>>(),
                    _ => vec![value],
                };

                for type_val in types {
                    if let Some(type_iri) = type_val.as_str() {
                        let object_id = self.sink.term_iri(type_iri);
                        self.emit(scope, subject_id, rdf_type_id, object_id, None)?;
                    }
                }
                continue;
            }

            // Regular predicate (expanded IRI)
            let predicate_id = self.sink.term_iri(key);

            // A reifier bundle in a named graph names that graph, which is
            // the graph of the edge it reifies: `f:reifiesGraph` beside its
            // `f:reifiesSubject`, as the transaction parser writes it.
            let anchor = match scope {
                Scope::Named(graph) if key == reifies_iris::SUBJECT => Some(graph),
                _ => None,
            };

            // Process values (always an array in expanded form)
            let values = match value {
                Value::Array(arr) => arr.iter().collect::<Vec<_>>(),
                _ => vec![value],
            };

            for val in values {
                match self.value(val, scope)? {
                    ProcessedValue::Single(object_id) => {
                        self.emit(scope, subject_id, predicate_id, object_id, None)?;
                        if let Some(graph) = anchor {
                            let reifies_graph = self.sink.term_iri(reifies_iris::GRAPH);
                            self.sink
                                .emit_quad(subject_id, reifies_graph, graph, graph)?;
                        }
                    }
                    ProcessedValue::List(items) => {
                        for (index, object_id) in items {
                            self.emit(scope, subject_id, predicate_id, object_id, Some(index))?;
                        }
                    }
                    ProcessedValue::None => {}
                }
            }
        }

        if let Some((name, nodes)) = named_graph_content {
            let graph = self.open(GraphNameRef::NamedGraph(name))?;
            for node in nodes {
                self.node(node, graph, None)?;
            }
        }

        Ok(subject_id)
    }

    /// Process a value, in `scope`, and return the processed result
    fn value(&mut self, value: &Value, scope: Scope) -> Result<ProcessedValue> {
        match value {
            Value::Object(obj) => {
                // Check for @id (reference to another node)
                if let Some(id_val) = obj.get("@id") {
                    // An embedded node object carrying more than a bare `@id`
                    // asserts its own triples (JSON-LD embedded-node semantics);
                    // recurse so they aren't silently dropped. A `{"@id": ...}`
                    // singleton is a pure reference.
                    if obj.len() > 1 {
                        let subject_id = self.node(value, scope, None)?;
                        return Ok(ProcessedValue::Single(subject_id));
                    }
                    let id_str = id_val.as_str().ok_or_else(|| {
                        AdapterError::InvalidStructure("@id must be a string".to_string())
                    })?;
                    let term_id = if id_str.starts_with("_:") {
                        self.sink.term_blank(Some(strip_blank_prefix(id_str)))
                    } else {
                        self.sink.term_iri(id_str)
                    };
                    return Ok(ProcessedValue::Single(term_id));
                }

                // Check for @value (literal)
                if let Some(val) = obj.get("@value") {
                    return process_literal(val, obj, self.sink);
                }

                // Check for @list
                if let Some(list_val) = obj.get("@list") {
                    return self.list(list_val, scope);
                }

                // Nested/embedded node without @id - allocate blank node first,
                // then process the node using that same blank node as subject.
                // This ensures the object edge and embedded node triples share identity.
                let subject_id = self.sink.term_blank(None);
                self.node(value, scope, Some(subject_id))?;
                Ok(ProcessedValue::Single(subject_id))
            }
            // Direct scalar values (shouldn't happen in properly expanded JSON-LD)
            Value::String(s) => Ok(ProcessedValue::Single(self.sink.term_literal(
                s,
                Datatype::xsd_string(),
                None,
            ))),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(ProcessedValue::Single(self.sink.term_literal_value(
                        LiteralValue::Integer(i),
                        Datatype::xsd_integer(),
                    )))
                } else if let Some(f) = n.as_f64() {
                    Ok(ProcessedValue::Single(self.sink.term_literal_value(
                        LiteralValue::Double(f),
                        Datatype::xsd_double(),
                    )))
                } else {
                    Ok(ProcessedValue::None)
                }
            }
            Value::Bool(b) => Ok(ProcessedValue::Single(
                self.sink
                    .term_literal_value(LiteralValue::Boolean(*b), Datatype::xsd_boolean()),
            )),
            _ => Ok(ProcessedValue::None),
        }
    }

    /// Process a @list value, in `scope`, and return the list items with indices
    fn list(&mut self, list_val: &Value, scope: Scope) -> Result<ProcessedValue> {
        let items = match list_val {
            Value::Array(arr) => arr,
            _ => {
                return Err(AdapterError::InvalidStructure(
                    "@list value must be an array".to_string(),
                ))
            }
        };

        // An empty @list denotes the IRI rdf:nil (JSON-LD 1.1 § List to RDF
        // Conversion): the statement stores as one ordinary triple, the twin of
        // Turtle's `()` fix (issue #1694). It used to produce zero events,
        // silently losing the statement.
        if items.is_empty() {
            return Ok(ProcessedValue::Single(self.sink.term_iri(rdf::NIL)));
        }

        let mut indexed_items = Vec::with_capacity(items.len());

        for (index, item) in items.iter().enumerate() {
            // Process each list item as a single value (lists cannot be nested directly)
            match self.list_item(item, scope)? {
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

    /// Process a single list item, in `scope` (cannot return a list, only
    /// single values)
    fn list_item(&mut self, value: &Value, scope: Scope) -> Result<Option<TermId>> {
        match value {
            Value::Object(obj) => {
                // Check for @id (reference to another node)
                if let Some(id_val) = obj.get("@id") {
                    // A member carrying more than a bare `@id` asserts its own
                    // triples, as an embedded node does outside a list.
                    if obj.len() > 1 {
                        return Ok(Some(self.node(value, scope, None)?));
                    }
                    let id_str = id_val.as_str().ok_or_else(|| {
                        AdapterError::InvalidStructure("@id must be a string".to_string())
                    })?;
                    return Ok(Some(if id_str.starts_with("_:") {
                        self.sink.term_blank(Some(strip_blank_prefix(id_str)))
                    } else {
                        self.sink.term_iri(id_str)
                    }));
                }

                // Check for @value (literal)
                if let Some(val) = obj.get("@value") {
                    return match process_literal(val, obj, self.sink)? {
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
                let subject_id = self.sink.term_blank(None);
                self.node(value, scope, Some(subject_id))?;
                Ok(Some(subject_id))
            }
            // Direct scalar values
            Value::String(s) => Ok(Some(self.sink.term_literal(
                s,
                Datatype::xsd_string(),
                None,
            ))),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Some(self.sink.term_literal_value(
                        LiteralValue::Integer(i),
                        Datatype::xsd_integer(),
                    )))
                } else if let Some(f) = n.as_f64() {
                    Ok(Some(self.sink.term_literal_value(
                        LiteralValue::Double(f),
                        Datatype::xsd_double(),
                    )))
                } else {
                    Ok(None)
                }
            }
            Value::Bool(b) => Ok(Some(
                self.sink
                    .term_literal_value(LiteralValue::Boolean(*b), Datatype::xsd_boolean()),
            )),
            _ => Ok(None),
        }
    }
}

/// The node objects of a keyword value (one node or an array of them).
fn node_items<'v>(value: &'v Value, what: &str) -> Result<Vec<&'v Value>> {
    let items: Vec<&Value> = match value {
        Value::Array(items) => items.iter().collect(),
        other => vec![other],
    };
    if items.iter().any(|item| {
        !item
            .as_object()
            .is_some_and(|m| !m.contains_key("@value") && !m.contains_key("@list"))
    }) {
        return Err(AdapterError::InvalidStructure(format!(
            "{what} must hold node objects"
        )));
    }
    Ok(items)
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
            // xsd:decimal must stay exact: route the number's lexical through
            // the typed-string-literal path, whose shared parser produces a
            // BigDecimal (same as a string @value). Going through f64 here
            // both corrupts the value and loses the declared datatype, since
            // term_literal_value re-infers xsd:double from the Double.
            if datatype.as_iri() == fluree_vocab::xsd::DECIMAL {
                return Ok(ProcessedValue::Single(sink.term_literal(
                    &n.to_string(),
                    datatype,
                    None,
                )));
            }
            // When the declared @type decodes as F64 (float, double), always
            // produce LiteralValue::Double even if the JSON number is an
            // integer. Otherwise the integer bits get stored as NUM_INT but
            // decoded as F64, producing garbage subnormal values after indexing.
            // (fluree/db-r#142)
            let is_float_type = {
                let iri = datatype.as_iri();
                iri == fluree_vocab::xsd::DOUBLE || iri == fluree_vocab::xsd::FLOAT
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
    use fluree_graph_ir::{GraphCollectorSink, SinkResult, Term};
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

        let mut graph = sink.into_graph();
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

        let mut graph = sink.into_graph();
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

        let graph = sink.into_graph();
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

        let mut graph = sink.into_graph();
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

        let graph = sink.into_graph();
        // An empty @list denotes rdf:nil: one ordinary triple, no list
        // metadata (issue #1694 twin — it used to produce zero events).
        assert_eq!(graph.len(), 1, "empty @list must store the rdf:nil triple");
        let triple = graph.iter().next().unwrap();
        assert!(!triple.is_list_element());
        assert!(
            matches!(&triple.o, Term::Iri(iri) if iri.as_ref() == rdf::NIL),
            "expected rdf:nil object, got {:?}",
            triple.o
        );
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

        let mut graph = sink.into_graph();
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

        let mut graph = sink.into_graph();
        assert_eq!(graph.len(), 3, "Should have 3 list items");

        graph.sort();
        for (i, triple) in graph.iter().enumerate() {
            assert_eq!(triple.list_index(), Some(i as i32));
        }
    }

    /// JSON number @value with xsd:decimal @type must keep the declared
    /// datatype and the exact lexical (string path), never f64 / xsd:double.
    #[test]
    fn test_decimal_typed_number_keeps_lexical_and_datatype() {
        let expanded = json!([{
            "@id": "http://example.org/item",
            "http://example.org/ns#price": [{
                "@value": 19.99,
                "@type": "http://www.w3.org/2001/XMLSchema#decimal"
            }],
            "http://example.org/ns#count": [{
                "@value": 42,
                "@type": "http://www.w3.org/2001/XMLSchema#decimal"
            }]
        }]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let graph = sink.graph();
        assert_eq!(graph.len(), 2);

        let mut lexicals: Vec<String> = Vec::new();
        for triple in graph.iter() {
            match &triple.o {
                Term::Literal {
                    value, datatype, ..
                } => {
                    assert_eq!(
                        datatype.as_iri(),
                        "http://www.w3.org/2001/XMLSchema#decimal",
                        "declared xsd:decimal must be preserved"
                    );
                    match value {
                        LiteralValue::String(s) => lexicals.push(s.to_string()),
                        other => panic!("Expected lexical string for decimal, got {other:?}"),
                    }
                }
                other => panic!("Expected literal, got {other:?}"),
            }
        }
        lexicals.sort();
        assert_eq!(lexicals, vec!["19.99", "42"]);
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

    #[test]
    fn embedded_node_with_id_asserts_its_own_triples() {
        // {"@id": ..., <props>} nested as a value is an embedded node, not a
        // bare reference — its own triples must be emitted, not dropped.
        let expanded = json!([{
            "@id": "http://example.org/shape",
            "http://example.org/ns#property": [{
                "@id": "http://example.org/ps1",
                "http://example.org/ns#minCount": [{"@value": 1}]
            }]
        }]);

        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();

        let graph = sink.graph();
        // shape →property→ ps1, plus ps1 →minCount→ 1
        assert_eq!(graph.len(), 2);

        let mut sink = GraphCollectorSink::new();
        // A bare {"@id": ...} singleton stays a pure reference.
        let expanded = json!([{
            "@id": "http://example.org/shape",
            "http://example.org/ns#property": [{"@id": "http://example.org/ps1"}]
        }]);
        to_graph_events(&expanded, &mut sink).unwrap();
        assert_eq!(sink.graph().len(), 1);
    }

    /// Graph scoping is refused, not folded into the default graph: a node's
    /// graph selector used to be dropped (its statements landed in the
    /// default graph) and a named graph's content was dropped entirely.
    #[test]
    fn graph_scoping_is_refused_not_folded() {
        for doc in [
            json!({"@context": {"ex": "http://example.org/"}, "@graph": [
                {"@id": "ex:a", "@graph": "ex:g", "ex:p": 1}
            ]}),
            json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:g", "@graph": [
                {"@id": "ex:a", "ex:p": 1}
            ]}),
        ] {
            let expanded = crate::expand(&doc).unwrap();
            let mut sink = GraphCollectorSink::new();
            let err = to_graph_events(&expanded, &mut sink).unwrap_err();
            assert!(matches!(err, AdapterError::Unsupported(_)), "{doc}: {err}");
        }
        // An envelope is unwrapped by expansion and imports as before.
        let envelope = json!({"@context": {"ex": "http://example.org/"}, "@graph": [
            {"@id": "ex:a", "ex:p": 1}, {"@id": "ex:b", "ex:p": 2}
        ]});
        let mut sink = GraphCollectorSink::new();
        to_graph_events(&crate::expand(&envelope).unwrap(), &mut sink).unwrap();
        assert_eq!(sink.graph().len(), 2);
    }

    /// Bulk import reads `@reverse` and `@included` as the transaction
    /// parser does; they used to be dropped.
    #[test]
    fn reverse_and_included_are_read_like_the_transaction_parser() {
        let doc = json!({
            "@context": {
                "ex": "http://example.org/",
                "parent": {"@reverse": "ex:child", "@type": "@id"}
            },
            "@id": "ex:kid",
            "parent": "ex:mom",
            "@included": [{"@id": "ex:other", "ex:p": 1}]
        });
        let expanded = crate::expand(&doc).unwrap();
        let mut sink = GraphCollectorSink::new();
        to_graph_events(&expanded, &mut sink).unwrap();
        let graph = sink.graph();
        assert_eq!(graph.len(), 2, "{graph:?}");
        let iri = |t: &Term| match t {
            Term::Iri(iri) => iri.to_string(),
            other => format!("{other:?}"),
        };
        let child = graph
            .iter()
            .find(|t| iri(&t.p) == "http://example.org/child")
            .expect("the reverse statement");
        assert_eq!(iri(&child.s), "http://example.org/mom");
        assert_eq!(iri(&child.o), "http://example.org/kid");
        assert!(graph
            .iter()
            .any(|t| iri(&t.s) == "http://example.org/other"));
    }

    /// Records each statement as `graph s p o` (`s p o [i]` for a list
    /// member), with `http://example.org/` written `ex:` and every blank node
    /// `_:`.
    #[derive(Default)]
    struct Statements {
        terms: Vec<String>,
        out: Vec<String>,
    }

    impl Statements {
        fn term(&mut self, term: String) -> TermId {
            self.terms.push(term.replace("http://example.org/", "ex:"));
            TermId::new(self.terms.len() as u32 - 1)
        }

        fn record(&mut self, graph: &str, s: TermId, p: TermId, o: TermId, index: Option<i32>) {
            let [s, p, o] = [s, p, o].map(|t| self.terms[t.index() as usize].clone());
            let index = index.map(|i| format!(" [{i}]")).unwrap_or_default();
            self.out.push(format!("{graph} {s} {p} {o}{index}"));
        }

        fn sorted(mut self) -> Vec<String> {
            self.out.sort();
            self.out
        }
    }

    impl GraphSink for Statements {
        fn on_base(&mut self, _base_iri: &str) {}
        fn on_prefix(&mut self, _prefix: &str, _namespace_iri: &str) {}
        fn term_iri(&mut self, iri: &str) -> TermId {
            self.term(iri.to_string())
        }
        fn term_blank(&mut self, _label: Option<&str>) -> TermId {
            self.term("_:".to_string())
        }
        fn term_literal(&mut self, value: &str, _: Datatype, _: Option<&str>) -> TermId {
            self.term(format!("{value:?}"))
        }
        fn term_literal_value(&mut self, value: LiteralValue, _: Datatype) -> TermId {
            self.term(format!("{value:?}"))
        }
        fn emit_triple(&mut self, s: TermId, p: TermId, o: TermId) -> SinkResult {
            self.record("default", s, p, o, None);
            Ok(())
        }
        fn emit_list_item(&mut self, s: TermId, p: TermId, o: TermId, index: i32) -> SinkResult {
            self.record("default", s, p, o, Some(index));
            Ok(())
        }
        fn supports_quads(&self) -> bool {
            true
        }
        fn emit_quad(&mut self, s: TermId, p: TermId, o: TermId, g: TermId) -> SinkResult {
            let graph = self.terms[g.index() as usize].clone();
            self.record(&graph, s, p, o, None);
            Ok(())
        }
        fn emit_quad_list_item(
            &mut self,
            s: TermId,
            p: TermId,
            o: TermId,
            g: TermId,
            index: i32,
        ) -> SinkResult {
            let graph = self.terms[g.index() as usize].clone();
            self.record(&graph, s, p, o, Some(index));
            Ok(())
        }
    }

    /// Resolves `ex:` selectors and named-graph IRIs; `default` is the
    /// default graph.
    fn resolve(name: GraphNameRef<'_>) -> Result<Option<String>> {
        Ok(match name {
            GraphNameRef::Selector("default") => None,
            GraphNameRef::Selector(raw) => Some(raw.replace("ex:", "http://example.org/")),
            GraphNameRef::NamedGraph(iri) => Some(iri.to_string()),
        })
    }

    fn scoped(doc: Value) -> Result<Vec<String>> {
        let mut sink = Statements::default();
        to_graph_events_scoped(&crate::expand(&doc).unwrap(), &mut sink, resolve)?;
        Ok(sink.sorted())
    }

    /// Every statement lands in its node's graph, as the transaction parser
    /// writes it: a selector scopes its node and everything nested in it, a
    /// named graph keeps its own properties outside and its content inside,
    /// and a node with its own selector inside a named graph goes to its own.
    #[test]
    fn each_statement_is_written_to_its_nodes_graph() {
        let doc = json!({"@context": {"ex": "http://example.org/"}, "@graph": [
            {"@id": "ex:a", "@graph": "ex:g",
             "ex:knows": {"@id": "ex:b", "ex:q": "w"},
             "ex:anon": {"ex:q": "x"},
             "ex:items": {"@list": [{"@id": "ex:c", "ex:q": "y"}]},
             "@included": [{"@id": "ex:d", "ex:q": "z"}],
             "@reverse": {"ex:likes": {"@id": "ex:e"}}},
            {"@id": "ex:G", "ex:label": "G", "@graph": [
                {"@id": "ex:f", "ex:q": "f"},
                {"@id": "ex:i", "@graph": "ex:h", "ex:q": "i"}
            ]},
            {"@id": "ex:j", "@graph": "default", "ex:q": "j"}
        ]});
        assert_eq!(
            scoped(doc).unwrap(),
            [
                "default ex:G ex:label \"G\"",
                "default ex:j ex:q \"j\"",
                "ex:G ex:f ex:q \"f\"",
                "ex:g _: ex:q \"x\"",
                "ex:g ex:a ex:anon _:",
                "ex:g ex:a ex:items ex:c [0]",
                "ex:g ex:a ex:knows ex:b",
                "ex:g ex:b ex:q \"w\"",
                "ex:g ex:c ex:q \"y\"",
                "ex:g ex:d ex:q \"z\"",
                "ex:g ex:e ex:likes ex:a",
                "ex:h ex:i ex:q \"i\"",
            ]
        );
    }

    /// A list member that carries more than its `@id` keeps its own
    /// statements, in the default graph as in a named one. It used to be
    /// read as a bare reference, and its statements were dropped.
    #[test]
    fn a_list_member_keeps_its_own_statements() {
        let doc = json!({"@context": {"ex": "http://example.org/"},
            "@id": "ex:a", "ex:items": {"@list": [{"@id": "ex:b", "ex:q": "w"}, {"@id": "ex:c"}]}});
        assert_eq!(
            scoped(doc).unwrap(),
            [
                "default ex:a ex:items ex:b [0]",
                "default ex:a ex:items ex:c [1]",
                "default ex:b ex:q \"w\"",
            ]
        );
    }

    /// A reifier bundle written in a named graph names that graph
    /// (`f:reifiesGraph`), as the transaction parser anchors it; in the
    /// default graph it names none.
    #[test]
    fn a_reifier_in_a_named_graph_names_it() {
        let doc = |graph: &str| {
            json!({"@context": {"ex": "http://example.org/"},
                "@id": "ex:r", "@graph": graph,
                "https://ns.flur.ee/db#reifiesSubject": {"@id": "ex:a"}})
        };
        let graph = reifies_iris::GRAPH.to_string();
        let subject = reifies_iris::SUBJECT.to_string();
        assert_eq!(
            scoped(doc("ex:g")).unwrap(),
            [
                format!("ex:g ex:r {graph} ex:g"),
                format!("ex:g ex:r {subject} ex:a"),
            ]
        );
        assert_eq!(
            scoped(doc("default")).unwrap(),
            [format!("default ex:r {subject} ex:a")]
        );
    }

    /// A graph name the resolver refuses refuses the document, and so does
    /// a named graph without an IRI name.
    #[test]
    fn a_graph_that_cannot_be_named_is_refused() {
        let ctx = json!({"ex": "http://example.org/"});
        let mut sink = Statements::default();
        let doc =
            crate::expand(&json!({"@context": ctx, "@id": "ex:a", "@graph": "ex:g", "ex:q": 1}))
                .unwrap();
        let err = to_graph_events_scoped(&doc, &mut sink, |_| {
            Err(AdapterError::Unsupported("no such graph".to_string()))
        })
        .unwrap_err();
        assert_eq!(err.to_string(), "no such graph");

        for doc in [
            json!({"@context": ctx, "@graph": [{"ex:q": 1, "@graph": [{"@id": "ex:a", "ex:q": 2}]}]}),
            json!({"@context": ctx, "@id": "_:g", "@graph": [{"@id": "ex:a", "ex:q": 2}]}),
        ] {
            let err = scoped(doc.clone()).unwrap_err();
            assert!(
                matches!(err, AdapterError::InvalidStructure(_)),
                "{doc}: {err}"
            );
        }
    }

    /// A sink without quads refuses a named graph; the default graph needs
    /// none.
    #[test]
    fn a_named_graph_needs_a_quad_sink() {
        let ctx = json!({"ex": "http://example.org/"});
        let doc = |graph: &str| {
            crate::expand(&json!({"@context": ctx, "@id": "ex:a", "@graph": graph, "ex:q": 1}))
                .unwrap()
        };
        let mut sink = GraphCollectorSink::new();
        let err = to_graph_events_scoped(&doc("ex:g"), &mut sink, resolve).unwrap_err();
        assert!(matches!(err, AdapterError::Unsupported(_)), "{err}");

        let mut sink = GraphCollectorSink::new();
        to_graph_events_scoped(&doc("default"), &mut sink, resolve).unwrap();
        assert_eq!(sink.graph().len(), 1);
    }

    /// A keyword on a node is refused, as the transaction parser refuses
    /// it, rather than skipped with whatever it says. A key that merely
    /// starts with `@` is no keyword and expands to no IRI.
    #[test]
    fn a_keyword_on_a_node_is_refused_not_skipped() {
        let ctx = json!({"ex": "http://example.org/"});
        let err =
            scoped(json!({"@context": ctx, "@id": "ex:a", "ex:q": 1, "@foo": "x"})).unwrap_err();
        assert!(err.to_string().contains("\"@foo\""), "{err}");

        assert_eq!(
            scoped(json!({"@context": ctx, "@id": "ex:a", "ex:q": 1, "@odata.etag": "x"})).unwrap(),
            ["default ex:a ex:q Integer(1)"]
        );
    }
}
