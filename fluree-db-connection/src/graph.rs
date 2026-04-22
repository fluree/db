//! JSON-LD graph indexing and reference resolution
//!
//! This module provides `ConfigGraph`, which indexes JSON-LD nodes by their
//! expanded `@id` values and provides methods to resolve `@id` references.

use crate::error::Result;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// A flattened graph of JSON-LD nodes indexed by expanded @id
///
/// This handles the JSON-LD patterns used in the legacy connection config:
/// - Nodes are objects with `@id` fields (after expansion)
/// - References are objects with only `@id` (no other properties)
/// - Property values are arrays (post-expansion)
/// - Nested subject nodes are flattened to top level
pub struct ConfigGraph {
    nodes: HashMap<String, JsonValue>,
}

impl ConfigGraph {
    /// Build graph from expanded JSON-LD
    ///
    /// Accepts the output of `json_ld::expand()`, which may be:
    /// - A single expanded node (object with @id)
    /// - An array of expanded nodes
    ///
    /// Nested subject nodes are recursively flattened and indexed.
    pub fn from_expanded(expanded: &JsonValue) -> Result<Self> {
        let mut nodes = HashMap::new();
        Self::collect_nodes(expanded, &mut nodes)?;
        Ok(Self { nodes })
    }

    /// Recursively collect and flatten nodes
    fn collect_nodes(value: &JsonValue, nodes: &mut HashMap<String, JsonValue>) -> Result<()> {
        match value {
            JsonValue::Array(arr) => {
                for item in arr {
                    Self::collect_nodes(item, nodes)?;
                }
            }
            JsonValue::Object(obj) => {
                // Check if this looks like a node (has @id)
                if let Some(id) = obj.get("@id").and_then(|v| v.as_str()) {
                    // Store the node
                    nodes.insert(id.to_string(), value.clone());

                    // Recursively check property values for nested nodes
                    for (key, val) in obj {
                        if key != "@id" && key != "@type" {
                            Self::collect_nested_nodes(val, nodes)?;
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Collect nested nodes from property values (which may be arrays)
    fn collect_nested_nodes(
        value: &JsonValue,
        nodes: &mut HashMap<String, JsonValue>,
    ) -> Result<()> {
        match value {
            JsonValue::Array(arr) => {
                for item in arr {
                    Self::collect_nested_nodes(item, nodes)?;
                }
            }
            JsonValue::Object(obj) => {
                // Skip value objects - they have @value, not subject nodes
                if obj.contains_key("@value") {
                    return Ok(());
                }
                // If it has @id and other properties (beyond just @id), it's a nested node
                if obj.contains_key("@id") && obj.len() > 1 {
                    Self::collect_nodes(value, nodes)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Get node by expanded @id
    pub fn get(&self, id: &str) -> Option<&JsonValue> {
        self.nodes.get(id)
    }

    /// Check if value is a reference (only @id, nothing else)
    pub fn is_ref(value: &JsonValue) -> bool {
        if let Some(obj) = value.as_object() {
            obj.len() == 1 && obj.contains_key("@id")
        } else {
            false
        }
    }

    /// Resolve a single reference (expects single value or single-element array)
    ///
    /// Use for fields that should have at most one value (e.g., indexStorage).
    /// If given an array, resolves the first element.
    pub fn resolve_first<'a>(&'a self, value: &'a JsonValue) -> Option<&'a JsonValue> {
        match value {
            JsonValue::Array(arr) => arr.first().and_then(|v| self.resolve_single(v)),
            _ => self.resolve_single(value),
        }
    }

    /// Resolve all references in an array
    ///
    /// Use for fields that may have multiple values.
    pub fn resolve_all<'a>(&'a self, value: &'a JsonValue) -> Vec<&'a JsonValue> {
        match value {
            JsonValue::Array(arr) => arr.iter().filter_map(|v| self.resolve_single(v)).collect(),
            _ => self.resolve_single(value).into_iter().collect(),
        }
    }

    /// Resolve a single value (not array)
    fn resolve_single<'a>(&'a self, value: &'a JsonValue) -> Option<&'a JsonValue> {
        if Self::is_ref(value) {
            value
                .get("@id")
                .and_then(|id| id.as_str())
                .and_then(|id| self.get(id))
        } else {
            Some(value)
        }
    }

    /// Find nodes by @type IRI
    pub fn find_by_type(&self, type_iri: &str) -> Vec<&JsonValue> {
        self.nodes
            .values()
            .filter(|node| {
                node.get("@type")
                    .and_then(|t| match t {
                        JsonValue::Array(arr) => {
                            Some(arr.iter().any(|v| v.as_str() == Some(type_iri)))
                        }
                        JsonValue::String(s) => Some(s.as_str() == type_iri),
                        _ => None,
                    })
                    .unwrap_or(false)
            })
            .collect()
    }
}

/// Test helper methods for ConfigGraph
#[cfg(test)]
impl ConfigGraph {
    /// Get number of indexed nodes
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Check if graph is empty
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_from_expanded_array() {
        // Simulates expanded JSON-LD output (array of nodes)
        let expanded = json!([
            {
                "@id": "https://ns.flur.ee/config/connection/fileStorage",
                "@type": ["https://ns.flur.ee/system#Storage"],
                "https://ns.flur.ee/system#filePath": [{"@value": "/data/fluree"}]
            },
            {
                "@id": "https://ns.flur.ee/config/connection/connection",
                "@type": ["https://ns.flur.ee/system#Connection"],
                "https://ns.flur.ee/system#indexStorage": [
                    {"@id": "https://ns.flur.ee/config/connection/fileStorage"}
                ]
            }
        ]);

        let graph = ConfigGraph::from_expanded(&expanded).unwrap();
        assert_eq!(graph.len(), 2);

        let storage = graph.get("https://ns.flur.ee/config/connection/fileStorage");
        assert!(storage.is_some());

        let conn = graph.get("https://ns.flur.ee/config/connection/connection");
        assert!(conn.is_some());
    }

    #[test]
    fn test_resolve_first_ref() {
        let expanded = json!([
            {
                "@id": "https://example.org/storage",
                "https://example.org/path": [{"@value": "/data"}]
            },
            {
                "@id": "https://example.org/conn",
                "https://example.org/storage": [{"@id": "https://example.org/storage"}]
            }
        ]);

        let graph = ConfigGraph::from_expanded(&expanded).unwrap();
        let conn = graph.get("https://example.org/conn").unwrap();
        let storage_ref = conn.get("https://example.org/storage").unwrap();

        let resolved = graph.resolve_first(storage_ref);
        assert!(resolved.is_some());
        assert!(resolved.unwrap().get("https://example.org/path").is_some());
    }

    #[test]
    fn test_find_by_type() {
        let expanded = json!([
            {
                "@id": "https://example.org/a",
                "@type": ["https://example.org/Storage"]
            },
            {
                "@id": "https://example.org/b",
                "@type": ["https://example.org/Connection"]
            },
            {
                "@id": "https://example.org/c",
                "@type": ["https://example.org/Storage"]
            }
        ]);

        let graph = ConfigGraph::from_expanded(&expanded).unwrap();
        let storages = graph.find_by_type("https://example.org/Storage");
        assert_eq!(storages.len(), 2);
    }

    #[test]
    fn test_value_objects_not_treated_as_nodes() {
        // Value objects like {"@value": "foo"} should not be collected as nodes
        let expanded = json!([
            {
                "@id": "https://example.org/node",
                "https://example.org/name": [{"@value": "Test Name"}],
                "https://example.org/count": [{"@value": 42}]
            }
        ]);

        let graph = ConfigGraph::from_expanded(&expanded).unwrap();
        // Only the one actual node should be indexed
        assert_eq!(graph.len(), 1);
    }

    #[test]
    fn test_is_ref() {
        assert!(ConfigGraph::is_ref(
            &json!({"@id": "https://example.org/x"})
        ));
        assert!(!ConfigGraph::is_ref(
            &json!({"@id": "x", "@type": ["Storage"]})
        ));
        assert!(!ConfigGraph::is_ref(&json!({"@value": "foo"})));
        assert!(!ConfigGraph::is_ref(&json!("string")));
    }
}
