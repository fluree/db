//! Shared helper functions for graph source operations.
//!
//! These utilities handle common tasks like IRI expansion and prefix mapping.

use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Extract prefix mappings from a JSON-LD @context.
///
/// Returns a map from prefix (e.g., "ex") to namespace IRI (e.g., "http://example.org/").
/// Excludes JSON-LD keywords like `@vocab`, `@base`, etc.
pub fn extract_prefix_map(context: &JsonValue) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let JsonValue::Object(obj) = context {
        for (key, value) in obj {
            // Skip JSON-LD keywords (start with @)
            if key.starts_with('@') {
                continue;
            }
            if let JsonValue::String(ns) = value {
                // Simple string value = prefix mapping
                map.insert(key.clone(), ns.clone());
            }
        }
    }
    map
}

/// Expand prefixed IRIs in @id fields of JSON-LD results to full IRIs.
///
/// This is necessary because query results are compacted using the @context,
/// but the BM25 index needs full IRIs for proper cross-ledger joins.
///
/// Example: "ex:doc1" -> "http://example.org/doc1" (given @context: {"ex": "http://example.org/"})
pub fn expand_ids_in_results(
    results: Vec<JsonValue>,
    prefix_map: &HashMap<String, String>,
) -> Vec<JsonValue> {
    results
        .into_iter()
        .map(|result| expand_ids_in_value(result, prefix_map))
        .collect()
}

/// Recursively expand prefixed IRIs in @id fields within a JSON value.
pub fn expand_ids_in_value(value: JsonValue, prefix_map: &HashMap<String, String>) -> JsonValue {
    match value {
        JsonValue::Object(mut obj) => {
            // Expand @id if present and prefixed
            if let Some(JsonValue::String(id)) = obj.get("@id") {
                if let Some(expanded) = expand_prefixed_iri(id, prefix_map) {
                    obj.insert("@id".to_string(), JsonValue::String(expanded));
                }
            }

            // Recursively expand nested objects/arrays
            let expanded: serde_json::Map<String, JsonValue> = obj
                .into_iter()
                .map(|(k, v)| (k, expand_ids_in_value(v, prefix_map)))
                .collect();
            JsonValue::Object(expanded)
        }
        JsonValue::Array(arr) => {
            let expanded: Vec<JsonValue> = arr
                .into_iter()
                .map(|v| expand_ids_in_value(v, prefix_map))
                .collect();
            JsonValue::Array(expanded)
        }
        other => other,
    }
}

/// Expand property names in JSON-LD results to full IRIs using the prefix map.
///
/// This ensures that property names like "ex:embedding" are expanded to
/// "http://example.org/embedding" for consistent property matching.
#[cfg(feature = "vector")]
pub fn expand_properties_in_results(
    results: Vec<JsonValue>,
    prefix_map: &HashMap<String, String>,
) -> Vec<JsonValue> {
    results
        .into_iter()
        .map(|result| expand_properties_in_value(result, prefix_map))
        .collect()
}

/// Recursively expand prefixed property names in a JSON value.
#[cfg(feature = "vector")]
fn expand_properties_in_value(value: JsonValue, prefix_map: &HashMap<String, String>) -> JsonValue {
    match value {
        JsonValue::Object(obj) => {
            let expanded: serde_json::Map<String, JsonValue> = obj
                .into_iter()
                .map(|(k, v)| {
                    let expanded_key = if k.starts_with('@') {
                        // Keep JSON-LD keywords as-is
                        k
                    } else if let Some(full_iri) = expand_prefixed_iri(&k, prefix_map) {
                        full_iri
                    } else {
                        k
                    };
                    (expanded_key, expand_properties_in_value(v, prefix_map))
                })
                .collect();
            JsonValue::Object(expanded)
        }
        JsonValue::Array(arr) => {
            let expanded: Vec<JsonValue> = arr
                .into_iter()
                .map(|v| expand_properties_in_value(v, prefix_map))
                .collect();
            JsonValue::Array(expanded)
        }
        other => other,
    }
}

/// Expand a prefixed IRI (e.g., "ex:doc1") to a full IRI using the prefix map.
///
/// Returns None if the IRI is not prefixed or the prefix is not in the map.
pub fn expand_prefixed_iri(iri: &str, prefix_map: &HashMap<String, String>) -> Option<String> {
    // Already a full IRI (contains "://")
    if iri.contains("://") {
        return None;
    }

    // Look for prefix:local pattern
    if let Some(colon_pos) = iri.find(':') {
        let prefix = &iri[..colon_pos];
        let local = &iri[colon_pos + 1..];

        if let Some(namespace) = prefix_map.get(prefix) {
            return Some(format!("{namespace}{local}"));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_extract_prefix_map() {
        let context = json!({
            "ex": "http://example.org/",
            "schema": "http://schema.org/",
            "@vocab": "http://default.org/"
        });

        let map = extract_prefix_map(&context);
        assert_eq!(map.get("ex"), Some(&"http://example.org/".to_string()));
        assert_eq!(map.get("schema"), Some(&"http://schema.org/".to_string()));
        // @vocab is not included because it's not a simple prefix mapping
        assert!(!map.contains_key("@vocab"));
    }

    #[test]
    fn test_expand_prefixed_iri() {
        let mut prefix_map = HashMap::new();
        prefix_map.insert("ex".to_string(), "http://example.org/".to_string());

        assert_eq!(
            expand_prefixed_iri("ex:doc1", &prefix_map),
            Some("http://example.org/doc1".to_string())
        );
        assert_eq!(
            expand_prefixed_iri("http://example.org/doc1", &prefix_map),
            None
        );
        assert_eq!(expand_prefixed_iri("unknown:doc1", &prefix_map), None);
    }

    #[test]
    fn test_expand_ids_in_value() {
        let mut prefix_map = HashMap::new();
        prefix_map.insert("ex".to_string(), "http://example.org/".to_string());

        let value = json!({
            "@id": "ex:doc1",
            "title": "Test",
            "nested": {
                "@id": "ex:doc2"
            }
        });

        let expanded = expand_ids_in_value(value, &prefix_map);
        assert_eq!(expanded["@id"], "http://example.org/doc1");
        assert_eq!(expanded["nested"]["@id"], "http://example.org/doc2");
    }

    #[test]
    #[cfg(feature = "vector")]
    fn test_expand_properties_in_value() {
        let mut prefix_map = HashMap::new();
        prefix_map.insert("ex".to_string(), "http://example.org/".to_string());

        let value = json!({
            "@id": "ex:doc1",
            "ex:embedding": [0.1, 0.2, 0.3],
            "ex:title": "Test"
        });

        let expanded = expand_properties_in_value(value, &prefix_map);
        // @id is a keyword, should be unchanged
        assert_eq!(expanded["@id"], "ex:doc1");
        // Properties should be expanded
        assert!(expanded.get("http://example.org/embedding").is_some());
        assert!(expanded.get("http://example.org/title").is_some());
        // Original prefixed form should be gone
        assert!(expanded.get("ex:embedding").is_none());
    }
}
