//! Minimal JSON-LD processing library
//!
//! This library provides core JSON-LD functionality including:
//! - Context parsing
//! - IRI expansion and compaction
//! - Document expansion
//! - JSON normalization (RFC 8785)
//!
//! # Example
//!
//! ```
//! use fluree_graph_json_ld::{parse_context, expand, expand_iri};
//! use serde_json::json;
//!
//! // Parse a context
//! let ctx = parse_context(&json!({
//!     "schema": "http://schema.org/",
//!     "name": "schema:name"
//! })).unwrap();
//!
//! // Expand an IRI
//! let expanded = expand_iri("schema:Person", &ctx);
//! assert_eq!(expanded, "http://schema.org/Person");
//!
//! // Expand a document
//! let doc = json!({
//!     "@context": {"name": "http://schema.org/name"},
//!     "@id": "http://example.org/person/1",
//!     "name": "John Doe"
//! });
//! let expanded_doc = expand(&doc).unwrap();
//! ```

pub mod compact;
pub mod context;
pub mod error;
pub mod expand;
pub mod iri;
pub mod normalize;

// GraphSink adapter for emitting triples to fluree-graph-ir
pub mod adapter;

// JSON-LD file splitter for bulk import
pub mod splitter;

pub use compact::ContextCompactor;
pub use context::{Container, ContextEntry, ParsedContext, TypeValue};
pub use error::{JsonLdError, Result};
pub use iri::UnresolvedIriDisposition;
pub use normalize::{Algorithm, Format, NormalizeOptions};

use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Parse a JSON-LD context.
///
/// # Arguments
/// * `context` - The JSON-LD context to parse (can be string, object, array, or null)
///
/// # Returns
/// A parsed context that can be used for expansion and compaction operations.
///
/// # Example
/// ```
/// use fluree_graph_json_ld::parse_context;
/// use serde_json::json;
///
/// let ctx = parse_context(&json!({"schema": "http://schema.org/"})).unwrap();
/// ```
pub fn parse_context(context: &JsonValue) -> Result<ParsedContext> {
    ParsedContext::parse(None, context)
}

/// Parse a context with a base context.
///
/// # Arguments
/// * `base` - The base context to merge into
/// * `context` - The JSON-LD context to parse
///
/// # Returns
/// A parsed context that includes both the base and new context definitions.
pub fn parse_context_with_base(base: &ParsedContext, context: &JsonValue) -> Result<ParsedContext> {
    ParsedContext::parse(Some(base), context)
}

/// Expand a compact IRI to its full form.
///
/// Uses @vocab for expansion (appropriate for property names and types).
///
/// # Arguments
/// * `compact_iri` - The compact IRI to expand (e.g., "schema:name")
/// * `context` - The parsed context
///
/// # Returns
/// The expanded IRI string.
///
/// # Example
/// ```
/// use fluree_graph_json_ld::{parse_context, expand_iri};
/// use serde_json::json;
///
/// let ctx = parse_context(&json!({"schema": "http://schema.org/"})).unwrap();
/// assert_eq!(expand_iri("schema:name", &ctx), "http://schema.org/name");
/// ```
pub fn expand_iri(compact_iri: &str, context: &ParsedContext) -> String {
    expand::iri(compact_iri, context, true)
}

/// Expand a compact IRI with explicit vocab/base control.
///
/// # Arguments
/// * `compact_iri` - The compact IRI to expand
/// * `context` - The parsed context
/// * `vocab` - If true, use @vocab for expansion; if false, use @base
pub fn expand_iri_with_vocab(compact_iri: &str, context: &ParsedContext, vocab: bool) -> String {
    expand::iri(compact_iri, context, vocab)
}

/// Get expansion details for an IRI.
///
/// Returns both the expanded IRI and the context entry with metadata.
///
/// # Arguments
/// * `compact_iri` - The compact IRI to expand
/// * `context` - The parsed context
///
/// # Returns
/// A tuple of (expanded_iri, optional_context_entry)
pub fn details(compact_iri: &str, context: &ParsedContext) -> (String, Option<ContextEntry>) {
    expand::details(compact_iri, context, true)
}

/// Get expansion details with explicit vocab/base control.
pub fn details_with_vocab(
    compact_iri: &str,
    context: &ParsedContext,
    vocab: bool,
) -> (String, Option<ContextEntry>) {
    expand::details(compact_iri, context, vocab)
}

/// Like [`expand_iri`] but rejects unresolved compact-looking IRIs.
///
/// Returns an error if the IRI looks like a compact IRI (e.g. `ex:Person`)
/// but the prefix is not defined in `@context` and is not a recognised
/// absolute IRI scheme.
pub fn expand_iri_checked(compact_iri: &str, context: &ParsedContext) -> Result<String> {
    expand::iri_checked(compact_iri, context, true)
}

/// Like [`details`] but rejects unresolved compact-looking IRIs.
pub fn details_checked(
    compact_iri: &str,
    context: &ParsedContext,
) -> Result<(String, Option<ContextEntry>)> {
    expand::details_checked(compact_iri, context, true)
}

/// Like [`details_with_vocab`] but rejects unresolved compact-looking IRIs.
pub fn details_with_vocab_checked(
    compact_iri: &str,
    context: &ParsedContext,
    vocab: bool,
) -> Result<(String, Option<ContextEntry>)> {
    expand::details_checked(compact_iri, context, vocab)
}

/// Expand a JSON-LD document.
///
/// Expands all compact IRIs to their full forms and processes @value, @list, @set, etc.
///
/// # Arguments
/// * `node_map` - The JSON-LD document to expand
///
/// # Returns
/// The expanded document.
///
/// # Example
/// ```
/// use fluree_graph_json_ld::expand;
/// use serde_json::json;
///
/// let doc = json!({
///     "@context": {"name": "http://schema.org/name"},
///     "@id": "http://example.org/1",
///     "name": "Test"
/// });
/// let expanded = expand(&doc).unwrap();
/// ```
pub fn expand(node_map: &JsonValue) -> Result<JsonValue> {
    expand::node(node_map, &ParsedContext::new())
}

/// Expand a JSON-LD document with a pre-parsed context.
pub fn expand_with_context(node_map: &JsonValue, context: &ParsedContext) -> Result<JsonValue> {
    expand::node(node_map, context)
}

/// Expand a JSON-LD document with strict compact-IRI checking.
///
/// Rejects unresolved compact-looking IRIs at every IRI position.
/// Use this at Fluree's JSON-LD parsing boundaries (queries and transactions).
pub fn expand_checked(node_map: &JsonValue) -> Result<JsonValue> {
    expand::node_checked(node_map, &ParsedContext::new())
}

/// Expand a JSON-LD document with a pre-parsed context and strict compact-IRI checking.
pub fn expand_with_context_checked(
    node_map: &JsonValue,
    context: &ParsedContext,
) -> Result<JsonValue> {
    expand::node_checked(node_map, context)
}

// ── Policy-driven dispatch: strict=true → checked, strict=false → permissive ──
//
// These always return Result so callers use `?` uniformly regardless of mode.

/// Expand IRI details, strict or permissive based on `strict` flag.
pub fn details_with_policy(
    compact_iri: &str,
    context: &ParsedContext,
    strict: bool,
) -> Result<(String, Option<ContextEntry>)> {
    if strict {
        expand::details_checked(compact_iri, context, true)
    } else {
        Ok(expand::details(compact_iri, context, true))
    }
}

/// Expand IRI details with explicit vocab/base control, strict or permissive.
pub fn details_with_vocab_policy(
    compact_iri: &str,
    context: &ParsedContext,
    vocab: bool,
    strict: bool,
) -> Result<(String, Option<ContextEntry>)> {
    if strict {
        expand::details_checked(compact_iri, context, vocab)
    } else {
        Ok(expand::details(compact_iri, context, vocab))
    }
}

/// Expand a compact IRI, strict or permissive based on `strict` flag.
pub fn expand_iri_with_policy(
    compact_iri: &str,
    context: &ParsedContext,
    strict: bool,
) -> Result<String> {
    if strict {
        expand::iri_checked(compact_iri, context, true)
    } else {
        Ok(expand::iri(compact_iri, context, true))
    }
}

/// Expand a JSON-LD document with a pre-parsed context, strict or permissive.
pub fn expand_with_context_policy(
    node_map: &JsonValue,
    context: &ParsedContext,
    strict: bool,
) -> Result<JsonValue> {
    expand::node_impl(node_map, context, strict)
}

/// Compact an IRI using a context.
///
/// # Arguments
/// * `iri` - The full IRI to compact
/// * `context` - The parsed context
///
/// # Returns
/// The compacted IRI (e.g., "schema:name") or the original IRI if no match found.
///
/// # Example
/// ```
/// use fluree_graph_json_ld::{parse_context, compact_iri};
/// use serde_json::json;
///
/// let ctx = parse_context(&json!({"schema": "http://schema.org/"})).unwrap();
/// assert_eq!(compact_iri("http://schema.org/name", &ctx), "schema:name");
/// ```
pub fn compact_iri(iri: &str, context: &ParsedContext) -> String {
    compact::compact(iri, context)
}

/// Create a compaction function from a context.
///
/// Returns a function that can be called repeatedly to compact IRIs efficiently.
pub fn compact_fn(context: &ParsedContext) -> impl Fn(&str) -> String + '_ {
    compact::compact_fn(context, None)
}

/// Create a compaction function with usage tracking.
///
/// The `used` map will be populated with the context terms that were actually used
/// during compaction. This is useful for identifying which subset of a large context
/// (like schema.org) is actually needed.
///
/// # Arguments
/// * `context` - The parsed context
/// * `used` - A thread-safe map to track used context terms
pub fn compact_fn_with_tracking(
    context: &ParsedContext,
    used: Arc<Mutex<HashMap<String, String>>>,
) -> impl Fn(&str) -> String + '_ {
    compact::compact_fn(context, Some(used))
}

/// Normalize JSON data to canonical form (RFC 8785).
///
/// This produces a deterministic string representation suitable for hashing
/// or comparison.
///
/// # Arguments
/// * `data` - The JSON data to normalize
///
/// # Returns
/// The canonical JSON string.
///
/// # Example
/// ```
/// use fluree_graph_json_ld::normalize_data;
/// use serde_json::json;
///
/// let data = json!({"b": 2, "a": 1});
/// let normalized = normalize_data(&data);
/// assert_eq!(normalized, r#"{"a":1,"b":2}"#);
/// ```
pub fn normalize_data(data: &JsonValue) -> String {
    normalize::normalize(data)
}

/// Normalize JSON data with options.
pub fn normalize_data_with_options(data: &JsonValue, opts: &NormalizeOptions) -> String {
    normalize::normalize_with_options(data, opts)
}

/// Check if a value appears to be JSON-LD.
///
/// Returns true if the document has @graph at the top level,
/// or if it's an array with @context or @id in the first element.
pub fn is_json_ld(value: &JsonValue) -> bool {
    match value {
        JsonValue::Object(map) => map.contains_key("@graph"),
        JsonValue::Array(arr) => arr.first().is_some_and(|first| {
            if let JsonValue::Object(map) = first {
                map.contains_key("@context") || map.contains_key("@id")
            } else {
                false
            }
        }),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_and_expand() {
        let ctx = parse_context(&json!({
            "schema": "http://schema.org/",
            "name": "schema:name"
        }))
        .unwrap();

        assert_eq!(
            expand_iri("schema:Person", &ctx),
            "http://schema.org/Person"
        );
        assert_eq!(expand_iri("name", &ctx), "http://schema.org/name");
    }

    #[test]
    fn test_expand_document() {
        let doc = json!({
            "@context": {
                "schema": "http://schema.org/",
                "name": "schema:name"
            },
            "@id": "http://example.org/1",
            "@type": "schema:Person",
            "name": "John Doe"
        });

        let expanded = expand(&doc).unwrap();
        let obj = expanded.as_object().unwrap();

        assert_eq!(obj["@id"], "http://example.org/1");
        assert_eq!(obj["@type"], json!(["http://schema.org/Person"]));
        assert!(obj.contains_key("http://schema.org/name"));
    }

    #[test]
    fn test_compact_iri() {
        let ctx = parse_context(&json!({
            "schema": "http://schema.org/"
        }))
        .unwrap();

        assert_eq!(compact_iri("http://schema.org/name", &ctx), "schema:name");
    }

    #[test]
    fn test_normalize() {
        let data = json!({"z": 1, "a": 2});
        assert_eq!(normalize_data(&data), r#"{"a":2,"z":1}"#);
    }

    #[test]
    fn test_is_json_ld() {
        assert!(is_json_ld(&json!({"@graph": []})));
        assert!(is_json_ld(&json!([{"@context": {}}])));
        assert!(is_json_ld(&json!([{"@id": "test"}])));
        assert!(!is_json_ld(&json!({"foo": "bar"})));
        assert!(!is_json_ld(&json!([{"foo": "bar"}])));
    }
}
