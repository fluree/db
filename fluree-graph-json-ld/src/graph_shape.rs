//! The one reading of `@graph`.
//!
//! A node's `@graph` value has two meanings in Fluree's JSON-LD:
//!
//! - a **graph selector**, Fluree's extension: `"@graph": "<graph IRI>"`
//!   writes the node, and every node nested in it, to that named graph;
//! - **named-graph content**, JSON-LD 1.1 §4.9: `{"@id": G, "@graph": [...]}`
//!   holds node objects that belong to the graph `G`.
//!
//! Every consumer that asks which one a value is (the expander, the
//! transaction parser, the edge-annotation lowering, the txn-meta extractor,
//! the bulk-import adapter) asks [`classify_graph_value`], and every consumer
//! that asks what a whole document is asks [`doc_shape`], so they cannot
//! disagree about the same bytes.

use crate::context::ParsedContext;
use crate::error::{JsonLdError, Result};
use serde_json::{Map, Value};

/// What a node's `@graph` value means.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GraphValue<'a> {
    /// A graph selector (Fluree extension): a string, an object whose only
    /// key is `@id`, or a one-element array holding either. The string is the
    /// graph name as written (compact, relative or absolute).
    Selector(&'a str),
    /// Named-graph content (JSON-LD 1.1): node objects. Any other array
    /// (empty, several elements, or one property-bearing node), or a single
    /// property-bearing node object.
    Content(&'a [Value]),
    /// Neither; the reason is suitable for an error message.
    Invalid(&'static str),
}

const INVALID_GRAPH_VALUE: &str =
    "`@graph` must be a graph IRI (string) or node object(s); got a number, boolean, null, \
     value object, list, or nested array";

/// The selector string of a bare `{"@id": "..."}` object, if `map` is one.
fn bare_id(map: &Map<String, Value>) -> Option<&str> {
    if map.len() == 1 {
        map.get("@id").and_then(Value::as_str)
    } else {
        None
    }
}

/// A map that describes a value rather than a node: it can never be a
/// member of a graph.
fn is_value_like(map: &Map<String, Value>) -> bool {
    ["@value", "@list", "@set", "@variable"]
        .iter()
        .any(|k| map.contains_key(*k))
}

/// Classify a `@graph` value. Works on raw and on expanded JSON-LD: the two
/// spell selectors and node objects the same way.
pub fn classify_graph_value(v: &Value) -> GraphValue<'_> {
    match v {
        Value::String(s) => GraphValue::Selector(s),
        Value::Object(map) => {
            if let Some(id) = bare_id(map) {
                GraphValue::Selector(id)
            } else if is_value_like(map) || (map.len() == 1 && map.contains_key("@id")) {
                GraphValue::Invalid(INVALID_GRAPH_VALUE)
            } else {
                GraphValue::Content(std::slice::from_ref(v))
            }
        }
        Value::Array(items) => {
            if let [only] = items.as_slice() {
                match only {
                    Value::String(s) => return GraphValue::Selector(s),
                    Value::Object(map) => {
                        if let Some(id) = bare_id(map) {
                            return GraphValue::Selector(id);
                        }
                    }
                    _ => {}
                }
            }
            let all_nodes = items
                .iter()
                .all(|item| matches!(item, Value::Object(map) if !is_value_like(map)));
            if all_nodes {
                GraphValue::Content(items)
            } else {
                GraphValue::Invalid(INVALID_GRAPH_VALUE)
            }
        }
        Value::Number(_) | Value::Bool(_) | Value::Null => GraphValue::Invalid(INVALID_GRAPH_VALUE),
    }
}

/// What a top-level JSON-LD object is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocShape {
    /// `{"@graph": [nodes], ...}` with no `@id`: a default-graph wrapper.
    /// Only an envelope's other top-level keys are transaction metadata.
    Envelope,
    /// `{"@id": G, "@graph": [nodes], ...}`: a JSON-LD 1.1 named graph. Its
    /// other keys are properties of the graph node `G` (data).
    NamedGraph,
    /// Any other object, including a node with a graph selector.
    Node,
}

/// Classify a top-level object. `@graph` and `@id` are matched literally:
/// the callers run on documents whose keywords are spelled canonically at
/// the top level (the transaction surfaces reject a `@context` term that
/// shadows a reserved key).
pub fn doc_shape(obj: &Map<String, Value>) -> Result<DocShape> {
    let Some(graph) = obj.get("@graph") else {
        return Ok(DocShape::Node);
    };
    let has_id = obj.contains_key("@id");
    if is_envelope_graph(graph, has_id)? {
        return Ok(DocShape::Envelope);
    }
    match classify_graph_value(graph) {
        GraphValue::Content(_) => Ok(DocShape::NamedGraph),
        GraphValue::Selector(_) => Ok(DocShape::Node),
        GraphValue::Invalid(reason) => Err(JsonLdError::InvalidGraphValue { reason }),
    }
}

/// Whether a top-level object with this `@graph` value is an envelope (a
/// default-graph wrapper): it has no `@id`, and the value is node content. At
/// the top level any array of node objects is content, a one-element array
/// of a bare `{"@id"}` included: every reader has always taken
/// `{"@graph": [...]}` to be an envelope. (Below the top level that
/// one-element form is a graph selector.)
pub fn is_envelope_graph(graph: &Value, has_id: bool) -> Result<bool> {
    if has_id {
        return Ok(false);
    }
    if let Value::Array(items) = graph {
        if items
            .iter()
            .all(|item| matches!(item, Value::Object(map) if !is_value_like(map)))
        {
            return Ok(true);
        }
    }
    match classify_graph_value(graph) {
        GraphValue::Content(_) => Ok(true),
        GraphValue::Selector(_) => Ok(false),
        GraphValue::Invalid(reason) => Err(JsonLdError::InvalidGraphValue { reason }),
    }
}

/// True when `key` means `@graph` under `ctx`: the keyword itself, a context
/// alias of it, or the legacy bare `graph` key **only when the context does
/// not define `graph` as a term** (a defined term is an ordinary property).
pub fn is_graph_key(key: &str, ctx: &ParsedContext) -> bool {
    if key == "@graph" {
        return true;
    }
    match ctx.get(key) {
        Some(entry) => entry.id.as_deref() == Some("@graph"),
        None => key == "graph",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn graph_value_classification_table() {
        let selector = |v: Value, expect: &str| {
            assert_eq!(
                classify_graph_value(&v),
                GraphValue::Selector(expect),
                "{v} should be a selector"
            );
        };
        selector(json!("ex:g"), "ex:g");
        selector(json!({"@id": "ex:g"}), "ex:g");
        selector(json!(["ex:g"]), "ex:g");
        selector(json!([{"@id": "ex:g"}]), "ex:g");

        let content = |v: Value, n: usize| match classify_graph_value(&v) {
            GraphValue::Content(nodes) => assert_eq!(nodes.len(), n, "{v}"),
            other => panic!("{v} should be content, got {other:?}"),
        };
        content(json!([]), 0);
        content(json!([{"@id": "ex:a", "ex:p": 1}]), 1);
        content(json!([{"@id": "ex:a"}, {"@id": "ex:b"}]), 2);
        content(json!({"@id": "ex:a", "ex:p": 1}), 1);
        content(json!({"ex:p": 1}), 1);

        for v in [
            json!(1),
            json!(true),
            json!(null),
            json!({"@value": "x"}),
            json!({"@list": []}),
            json!([["ex:g"]]),
            json!(["ex:g", "ex:h"]),
            json!([1]),
            json!({"@id": 7}),
        ] {
            assert!(
                matches!(classify_graph_value(&v), GraphValue::Invalid(_)),
                "{v} should be invalid"
            );
        }
    }

    #[test]
    fn doc_shape_distinguishes_envelope_named_graph_and_node() {
        let shape = |v: Value| doc_shape(v.as_object().unwrap()).unwrap();
        assert_eq!(
            shape(json!({"@graph": [{"@id": "ex:a"}, {"@id": "ex:b"}]})),
            DocShape::Envelope
        );
        assert_eq!(
            shape(json!({"@graph": [], "ex:batch": 1})),
            DocShape::Envelope
        );
        assert_eq!(
            shape(json!({"@id": "ex:G", "@graph": [{"@id": "ex:a", "ex:p": 1}]})),
            DocShape::NamedGraph
        );
        // At the top level a one-element array of a bare node is still an
        // envelope; with an `@id` the same value is a selector.
        assert_eq!(
            shape(json!({"@graph": [{"@id": "ex:a"}], "ex:m": 1})),
            DocShape::Envelope
        );
        assert_eq!(
            shape(json!({"@id": "ex:s", "@graph": [{"@id": "ex:g"}]})),
            DocShape::Node
        );
        // A single property-bearing object without an `@id` is an envelope
        // (solo's MCP single-node form carries txn-meta beside it).
        assert_eq!(
            shape(json!({"@graph": {"@id": "ex:a", "ex:p": 1}, "f:message": "m"})),
            DocShape::Envelope
        );
        // A single object with a string selector is a node, never an envelope.
        assert_eq!(
            shape(json!({"@id": "ex:s", "@graph": "ex:g", "ex:p": 1})),
            DocShape::Node
        );
        assert_eq!(shape(json!({"@id": "ex:s", "ex:p": 1})), DocShape::Node);
        assert!(doc_shape(json!({"@graph": 3}).as_object().unwrap()).is_err());
    }

    #[test]
    fn graph_key_honors_aliases_and_a_graph_term() {
        let plain = ParsedContext::new();
        assert!(is_graph_key("@graph", &plain));
        assert!(is_graph_key("graph", &plain));
        assert!(!is_graph_key("ex:graph", &plain));

        let aliased = crate::parse_context(&json!({"g": "@graph"})).unwrap();
        assert!(is_graph_key("g", &aliased));

        // A context that defines `graph` as a property takes it back.
        let term = crate::parse_context(&json!({"graph": "http://example.org/graphProp"})).unwrap();
        assert!(!is_graph_key("graph", &term));
        assert!(is_graph_key("@graph", &term));
    }
}
