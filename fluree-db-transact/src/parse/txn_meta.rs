//! Transaction metadata extraction from JSON-LD
//!
//! User-provided transaction metadata reaches the commit via two channels.
//! Both produce the same `Vec<TxnMetaEntry>` and are merged by
//! [`extract_txn_meta`].
//!
//! # 1. Envelope form (`@graph` present)
//!
//! Top-level non-reserved keys are metadata; `@graph` contents are data.
//!
//! ```json
//! {
//!   "@context": {"ex": "http://example.org/"},
//!   "@graph": [{ "@id": "ex:alice", "ex:name": "Alice" }],
//!   "ex:machine": "server-01",
//!   "ex:batchId": 42
//! }
//! ```
//!
//! # 2. Sidecar form (top-level `"txn-meta"` object)
//!
//! Works for any transaction shape — including `update`, which has no
//! envelope. The block may carry its own `@context` or inherit the outer
//! one.
//!
//! ```json
//! {
//!   "@context": {"f": "https://ns.flur.ee/db#", "ex": "http://example.org/"},
//!   "where":  [{ "@id": "?s", "ex:name": "Alice" }],
//!   "delete": [{ "@id": "?s", "ex:name": "Alice" }],
//!   "insert": [{ "@id": "?s", "ex:name": "Alicia" }],
//!   "txn-meta": {
//!     "f:message": "rename Alice → Alicia",
//!     "ex:batchId": 42
//!   }
//! }
//! ```
//!
//! Single-object insert transactions (no `@graph`, no sidecar) have **no
//! metadata** — all properties are data.

use crate::error::{Result, TransactError};
use crate::namespace::NamespaceRegistry;
use crate::parse::jsonld::expand_datatype_iri;
use fluree_db_novelty::{TxnMetaEntry, TxnMetaValue, MAX_TXN_META_BYTES, MAX_TXN_META_ENTRIES};
use fluree_graph_json_ld::{details_with_policy, ParsedContext};
use fluree_vocab::namespaces::{FLUREE_COMMIT, FLUREE_DB, FLUREE_URN};
use serde_json::Value;

/// Namespace codes that are reserved for Fluree-generated provenance and must
/// never appear as a user-supplied txn-meta predicate. System provenance
/// (commit address, t, time, identity, etc.) is emitted on the same commit
/// subject as user metadata — allowing these namespaces would let a user
/// clobber or shadow real provenance properties.
///
/// FLUREE_DB is handled separately via `FLUREE_DB_USER_ALLOWED` below:
/// `f:message` and `f:author` are explicitly permitted as user claims.
///
/// Why check by namespace code (not IRI string): the code is already resolved
/// at the call site, comparison is O(1), and it's immune to IRI encoding
/// tricks (percent-encoding, alternate prefixes, etc.).
const RESERVED_PREDICATE_NAMESPACES: &[u16] = &[FLUREE_COMMIT, FLUREE_URN];

/// Local names in the `FLUREE_DB` namespace that users *may* set as txn-meta.
///
/// `f:message` — commit message (free-form user claim).
/// `f:author` — commit author (user claim; distinct from `f:identity` which is
/// the authenticated subject and is system-controlled).
const FLUREE_DB_USER_ALLOWED: &[&str] = &[fluree_vocab::db::MESSAGE, fluree_vocab::db::AUTHOR];

/// The top-level body key for the txn-meta sidecar.
///
/// Aligns with the named graph (`txn-meta`) where these entries are
/// ultimately stored on commit. Works for any transaction shape — useful
/// especially for `update` transactions, which have no envelope-form
/// channel.
const TXN_META_SIDECAR_KEY: &str = "txn-meta";

/// Reserved keys that are never transaction metadata.
///
/// `opts` is a Fluree-specific reserved key for parse-time options
/// (e.g., `opts.strictCompactIri`). It must never be confused with metadata.
/// `txn-meta` is the dedicated sidecar key — its contents are extracted
/// separately, so it must not also be picked up as an envelope-form entry.
const RESERVED_KEYS: &[&str] = &[
    "@context",
    "@graph",
    "@id",
    "@type",
    "@base",
    "@vocab",
    "opts",
    TXN_META_SIDECAR_KEY,
];

/// Extract transaction metadata from a JSON-LD document.
///
/// Two channels are supported and merged:
///
/// 1. **Sidecar form**: a top-level `"txn-meta"` object whose properties are
///    metadata. Works for any transaction shape (insert/upsert/update). The
///    block may carry its own `@context`; otherwise it inherits the outer
///    context.
/// 2. **Envelope form**: when `@graph` is present, top-level non-reserved
///    keys are also treated as metadata.
///
/// Both forms run through the same predicate validation (Fluree-namespace
/// allowlist — only `f:message` and `f:author` permitted in `f:`).
///
/// Returns an empty `Vec` if neither channel produces entries.
///
/// # Errors
///
/// Returns an error if:
/// - The `txn-meta` sidecar is present but not an object
/// - A metadata value cannot be converted (e.g., nested object without @value/@id)
/// - A double value is non-finite (NaN, +Inf, -Inf)
/// - Entry count exceeds `MAX_TXN_META_ENTRIES`
/// - Estimated encoded size exceeds `MAX_TXN_META_BYTES`
pub fn extract_txn_meta(
    json: &Value,
    context: &ParsedContext,
    ns_registry: &mut NamespaceRegistry,
    strict: bool,
) -> Result<Vec<TxnMetaEntry>> {
    let Some(obj) = json.as_object() else {
        return Ok(Vec::new());
    };

    let mut entries = Vec::new();

    // Sidecar form: top-level "txn-meta" object.
    if let Some(meta_block) = obj.get(TXN_META_SIDECAR_KEY) {
        let block_obj = meta_block.as_object().ok_or_else(|| {
            TransactError::Parse(format!(
                "'{TXN_META_SIDECAR_KEY}' must be an object containing metadata properties"
            ))
        })?;

        // The block may override @context; otherwise inherit the outer context.
        let block_context_owned = if let Some(ctx_val) = block_obj.get("@context") {
            Some(fluree_graph_json_ld::parse_context(ctx_val).map_err(|e| {
                TransactError::Parse(format!(
                    "invalid @context inside '{TXN_META_SIDECAR_KEY}': {e}"
                ))
            })?)
        } else {
            None
        };
        let block_context = block_context_owned.as_ref().unwrap_or(context);

        for (key, value) in block_obj {
            if key.starts_with('@') {
                continue;
            }
            extract_one_entry(key, value, block_context, ns_registry, strict, &mut entries)?;
        }
    }

    // Envelope form: top-level non-reserved keys when @graph is present.
    if obj.contains_key("@graph") {
        for (key, value) in obj {
            if key.starts_with('@') || RESERVED_KEYS.contains(&key.as_str()) {
                continue;
            }
            extract_one_entry(key, value, context, ns_registry, strict, &mut entries)?;
        }
    }

    validate_limits(&entries)?;
    Ok(entries)
}

/// Extract one (predicate, value(s)) pair into `entries`.
///
/// Applies IRI expansion, the Fluree-namespace allowlist, and value parsing.
fn extract_one_entry(
    key: &str,
    value: &Value,
    context: &ParsedContext,
    ns_registry: &mut NamespaceRegistry,
    strict: bool,
    entries: &mut Vec<TxnMetaEntry>,
) -> Result<()> {
    let (expanded_iri, _) = details_with_policy(key, context, strict)?;

    let sid = ns_registry.sid_for_iri(&expanded_iri);
    let predicate_ns = sid.namespace_code;
    let predicate_name = sid.name.to_string();

    if RESERVED_PREDICATE_NAMESPACES.contains(&predicate_ns) {
        return Err(TransactError::Parse(format!(
            "txn-meta predicate '{key}' (expanded: '{expanded_iri}') uses Fluree-reserved namespace and would collide with system provenance; use a different namespace"
        )));
    }

    if predicate_ns == FLUREE_DB && !FLUREE_DB_USER_ALLOWED.contains(&predicate_name.as_str()) {
        return Err(TransactError::Parse(format!(
            "txn-meta predicate '{key}' (expanded: '{expanded_iri}') uses Fluree-reserved namespace and would collide with system provenance; only f:message and f:author are user-settable"
        )));
    }

    let meta_values = json_to_txn_meta_values(value, context, ns_registry, strict)?;
    for mv in meta_values {
        entries.push(TxnMetaEntry::new(predicate_ns, predicate_name.clone(), mv));
    }
    Ok(())
}

/// Convert a JSON value to txn-meta values.
///
/// Arrays produce multiple values; scalars produce one.
fn json_to_txn_meta_values(
    value: &Value,
    context: &ParsedContext,
    ns_registry: &mut NamespaceRegistry,
    strict: bool,
) -> Result<Vec<TxnMetaValue>> {
    match value {
        Value::Array(arr) => {
            let mut results = Vec::with_capacity(arr.len());
            for item in arr {
                results.push(json_to_single_txn_meta_value(
                    item,
                    context,
                    ns_registry,
                    strict,
                )?);
            }
            Ok(results)
        }
        _ => Ok(vec![json_to_single_txn_meta_value(
            value,
            context,
            ns_registry,
            strict,
        )?]),
    }
}

/// Convert a single JSON value to a TxnMetaValue.
fn json_to_single_txn_meta_value(
    value: &Value,
    context: &ParsedContext,
    ns_registry: &mut NamespaceRegistry,
    strict: bool,
) -> Result<TxnMetaValue> {
    match value {
        Value::Null => Err(TransactError::Parse(
            "txn-meta value cannot be null".to_string(),
        )),

        Value::Bool(b) => Ok(TxnMetaValue::Boolean(*b)),

        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(TxnMetaValue::Long(i))
            } else if let Some(f) = n.as_f64() {
                if !f.is_finite() {
                    return Err(TransactError::Parse(
                        "txn-meta does not support non-finite double values (NaN, Inf)".to_string(),
                    ));
                }
                Ok(TxnMetaValue::Double(f))
            } else {
                Err(TransactError::Parse(format!(
                    "Unsupported number type in txn-meta: {n}"
                )))
            }
        }

        Value::String(s) => {
            // Plain strings are always literals in JSON-LD semantics.
            // Use {"@id": "..."} to create IRI references.
            // This avoids surprising behavior with strings like "acct:123" or "foo:bar".
            Ok(TxnMetaValue::String(s.clone()))
        }

        Value::Object(obj) => {
            // @id object → IRI reference
            if let Some(id_val) = obj.get("@id") {
                let id_str = id_val.as_str().ok_or_else(|| {
                    TransactError::Parse("@id in txn-meta must be a string".to_string())
                })?;
                let (expanded, _) = details_with_policy(id_str, context, strict)?;
                let sid = ns_registry.sid_for_iri(&expanded);
                return Ok(TxnMetaValue::Ref {
                    ns: sid.namespace_code,
                    name: sid.name.to_string(),
                });
            }

            // @value object → literal with optional @type or @language
            if let Some(val) = obj.get("@value") {
                return parse_value_object(val, obj, context, ns_registry, strict);
            }

            // Other object shapes not supported in txn-meta
            Err(TransactError::Parse(
                "txn-meta objects must contain @id or @value; nested objects not supported"
                    .to_string(),
            ))
        }

        Value::Array(_) => {
            // Should not reach here - arrays handled by caller
            Err(TransactError::Parse(
                "Unexpected array in single value position".to_string(),
            ))
        }
    }
}

/// Parse a `{"@value": ..., "@type"?: ..., "@language"?: ...}` object.
fn parse_value_object(
    val: &Value,
    obj: &serde_json::Map<String, Value>,
    context: &ParsedContext,
    ns_registry: &mut NamespaceRegistry,
    strict: bool,
) -> Result<TxnMetaValue> {
    // @type present → typed literal
    if let Some(type_val) = obj.get("@type") {
        let type_iri = type_val.as_str().ok_or_else(|| {
            TransactError::Parse("@type in txn-meta must be a string".to_string())
        })?;

        // Expand the datatype IRI
        let expanded_type = expand_datatype_iri(type_iri, context, strict)?;
        let dt_sid = ns_registry.sid_for_iri(&expanded_type);

        // Get the string value
        let value_str = val.as_str().ok_or_else(|| {
            TransactError::Parse("txn-meta typed literal @value must be a string".to_string())
        })?;

        return Ok(TxnMetaValue::TypedLiteral {
            value: value_str.to_string(),
            dt_ns: dt_sid.namespace_code,
            dt_name: dt_sid.name.to_string(),
        });
    }

    // @language present → language-tagged string
    if let Some(lang_val) = obj.get("@language") {
        let lang = lang_val.as_str().ok_or_else(|| {
            TransactError::Parse("@language in txn-meta must be a string".to_string())
        })?;

        let value_str = val.as_str().ok_or_else(|| {
            TransactError::Parse("txn-meta language-tagged @value must be a string".to_string())
        })?;

        return Ok(TxnMetaValue::LangString {
            value: value_str.to_string(),
            lang: lang.to_string(),
        });
    }

    // Plain @value (no @type or @language)
    match val {
        Value::String(s) => Ok(TxnMetaValue::String(s.clone())),
        Value::Bool(b) => Ok(TxnMetaValue::Boolean(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(TxnMetaValue::Long(i))
            } else if let Some(f) = n.as_f64() {
                if !f.is_finite() {
                    return Err(TransactError::Parse(
                        "txn-meta does not support non-finite double values (NaN, Inf)".to_string(),
                    ));
                }
                Ok(TxnMetaValue::Double(f))
            } else {
                Err(TransactError::Parse(format!(
                    "Unsupported number in txn-meta @value: {n}"
                )))
            }
        }
        _ => Err(TransactError::Parse(format!(
            "Unsupported @value type in txn-meta: {val:?}"
        ))),
    }
}

/// Validate that txn-meta entries are within limits.
fn validate_limits(entries: &[TxnMetaEntry]) -> Result<()> {
    // Entry count limit
    if entries.len() > MAX_TXN_META_ENTRIES {
        return Err(TransactError::Parse(format!(
            "txn-meta entry count {} exceeds maximum {}",
            entries.len(),
            MAX_TXN_META_ENTRIES
        )));
    }

    // Estimate encoded size (conservative estimate)
    let mut estimated_bytes: usize = 0;
    for entry in entries {
        // predicate: 2 (ns) + 4 (len) + name bytes
        estimated_bytes += 6 + entry.predicate_name.len();

        // value: tag byte + payload
        estimated_bytes += 1 + estimate_value_size(&entry.value);
    }

    if estimated_bytes > MAX_TXN_META_BYTES {
        return Err(TransactError::Parse(format!(
            "txn-meta estimated size {estimated_bytes} bytes exceeds maximum {MAX_TXN_META_BYTES} bytes"
        )));
    }

    Ok(())
}

/// Estimate the encoded size of a TxnMetaValue.
fn estimate_value_size(value: &TxnMetaValue) -> usize {
    match value {
        TxnMetaValue::String(s) => 4 + s.len(),
        TxnMetaValue::Long(_) => 8,
        TxnMetaValue::Double(_) => 8,
        TxnMetaValue::Boolean(_) => 1,
        TxnMetaValue::Ref { name, .. } => 6 + name.len(),
        TxnMetaValue::LangString { value, lang } => 8 + value.len() + lang.len(),
        TxnMetaValue::TypedLiteral { value, dt_name, .. } => 10 + value.len() + dt_name.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_registry() -> NamespaceRegistry {
        NamespaceRegistry::new()
    }

    fn empty_context() -> ParsedContext {
        ParsedContext::new()
    }

    #[test]
    fn test_no_graph_returns_empty() {
        let mut ns = test_registry();
        let ctx = empty_context();

        // Single object (no @graph) → no metadata
        let json = json!({
            "@id": "http://example.org/alice",
            "http://example.org/name": "Alice"
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_envelope_form_extracts_metadata() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [{ "@id": "http://example.org/alice" }],
            "http://example.org/machine": "server-01",
            "http://example.org/batchId": 42
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true).unwrap();
        assert_eq!(result.len(), 2);

        // Find machine entry
        let machine = result
            .iter()
            .find(|e| e.predicate_name == "machine")
            .unwrap();
        assert!(matches!(&machine.value, TxnMetaValue::String(s) if s == "server-01"));

        // Find batchId entry
        let batch = result
            .iter()
            .find(|e| e.predicate_name == "batchId")
            .unwrap();
        assert!(matches!(&batch.value, TxnMetaValue::Long(42)));
    }

    #[test]
    fn test_reserved_keys_skipped() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@context": {},
            "@graph": [],
            "@id": "ignored",
            "@type": "ignored",
            "@base": "ignored",
            "@vocab": "ignored",
            "http://example.org/keep": "this one"
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].predicate_name, "keep");
    }

    #[test]
    fn test_boolean_value() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/active": true
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true).unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0].value, TxnMetaValue::Boolean(true)));
    }

    #[test]
    fn test_double_value() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/ratio": 1.23
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true).unwrap();
        assert_eq!(result.len(), 1);
        if let TxnMetaValue::Double(f) = &result[0].value {
            assert!((f - 1.23).abs() < 0.001);
        } else {
            panic!("Expected double");
        }
    }

    #[test]
    fn test_array_values() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/tags": ["a", "b", "c"]
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true).unwrap();
        assert_eq!(result.len(), 3);
        assert!(result.iter().all(|e| e.predicate_name == "tags"));
    }

    #[test]
    fn test_id_object_becomes_ref() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/author": { "@id": "http://example.org/alice" }
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true).unwrap();
        assert_eq!(result.len(), 1);
        if let TxnMetaValue::Ref { name, .. } = &result[0].value {
            assert_eq!(name, "alice");
        } else {
            panic!("Expected ref");
        }
    }

    #[test]
    fn test_iri_string_stays_string() {
        // Plain strings are always literals, even if they look like IRIs.
        // Use {"@id": "..."} to create IRI references.
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/related": "http://example.org/bob"
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true).unwrap();
        assert_eq!(result.len(), 1);
        // Should be a string, not a ref
        assert!(
            matches!(&result[0].value, TxnMetaValue::String(s) if s == "http://example.org/bob")
        );
    }

    #[test]
    fn test_plain_string_stays_string() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/note": "hello world"
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true).unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0].value, TxnMetaValue::String(s) if s == "hello world"));
    }

    #[test]
    fn test_typed_literal() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/date": {
                "@value": "2025-01-15",
                "@type": "http://www.w3.org/2001/XMLSchema#date"
            }
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true).unwrap();
        assert_eq!(result.len(), 1);
        if let TxnMetaValue::TypedLiteral { value, dt_name, .. } = &result[0].value {
            assert_eq!(value, "2025-01-15");
            assert_eq!(dt_name, "date");
        } else {
            panic!("Expected typed literal");
        }
    }

    #[test]
    fn test_language_tagged_string() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/title": {
                "@value": "Bonjour",
                "@language": "fr"
            }
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true).unwrap();
        assert_eq!(result.len(), 1);
        if let TxnMetaValue::LangString { value, lang } = &result[0].value {
            assert_eq!(value, "Bonjour");
            assert_eq!(lang, "fr");
        } else {
            panic!("Expected lang string");
        }
    }

    #[test]
    fn test_reject_nested_object() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/nested": {
                "foo": "bar"
            }
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("nested objects not supported"));
    }

    #[test]
    fn test_reject_null_value() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "http://example.org/bad": null
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("null"));
    }

    #[test]
    fn test_entry_count_limit() {
        let mut ns = test_registry();
        let ctx = empty_context();

        // Create JSON with too many entries
        let mut obj = serde_json::Map::new();
        obj.insert("@graph".to_string(), json!([]));
        for i in 0..300 {
            obj.insert(format!("http://example.org/key{i}"), json!(i));
        }
        let json = Value::Object(obj);

        let result = extract_txn_meta(&json, &ctx, &mut ns, true);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds maximum"));
    }

    #[test]
    fn test_reject_fluree_db_namespace_predicate() {
        // A user cannot override system provenance by aliasing the FLUREE_DB
        // namespace and using `db:t`, `db:address`, etc. as top-level keys.
        let mut ns = test_registry();
        let ctx_json = json!({ "db": "https://ns.flur.ee/db#" });
        let ctx = fluree_graph_json_ld::parse_context(&ctx_json).unwrap();

        let json = json!({
            "@graph": [],
            "db:t": 999_999
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Fluree-reserved namespace"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_reject_fluree_db_namespace_predicate_full_iri() {
        // Same check but with the full IRI written directly (no prefix alias).
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "https://ns.flur.ee/db#address": "spoofed-address"
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Fluree-reserved namespace"));
    }

    #[test]
    fn test_reject_fluree_db_identity_user_supplied() {
        // `f:identity` is system-controlled (derived from opts.identity /
        // signed DID). Users must not supply it directly.
        let mut ns = test_registry();
        let ctx_json = json!({ "f": "https://ns.flur.ee/db#" });
        let ctx = fluree_graph_json_ld::parse_context(&ctx_json).unwrap();

        let json = json!({
            "@graph": [],
            "f:identity": "did:example:spoofed"
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("only f:message and f:author are user-settable"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_allow_fluree_db_message_and_author() {
        // `f:message` and `f:author` are permitted user claims.
        let mut ns = test_registry();
        let ctx_json = json!({ "f": "https://ns.flur.ee/db#" });
        let ctx = fluree_graph_json_ld::parse_context(&ctx_json).unwrap();

        let json = json!({
            "@graph": [],
            "f:message": "initial load",
            "f:author": "alice"
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.iter().any(|e| e.predicate_name == "message"
            && matches!(&e.value, TxnMetaValue::String(s) if s == "initial load")));
        assert!(result.iter().any(|e| e.predicate_name == "author"
            && matches!(&e.value, TxnMetaValue::String(s) if s == "alice")));
    }

    #[test]
    fn test_reject_fluree_commit_namespace_predicate() {
        // Defense-in-depth: FLUREE_COMMIT namespace is also blocked as a predicate.
        let mut ns = test_registry();
        let ctx_json = json!({ "c": "fluree:commit:sha256:" });
        let ctx = fluree_graph_json_ld::parse_context(&ctx_json).unwrap();

        let json = json!({
            "@graph": [],
            "c:injected": "nope"
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Fluree-reserved namespace"));
    }

    #[test]
    fn test_reject_fluree_urn_namespace_predicate() {
        // Defense-in-depth: urn:fluree: is used for internal graph IRIs
        // (`urn:fluree:{ledger}#txn-meta`, `#config`). Canonical split only
        // lands on FLUREE_URN for bare `urn:fluree:name` forms; per-ledger
        // IRIs with `#` split into their own namespace. The block still
        // covers the former — the latter requires a separate graph-IRI check.
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "@graph": [],
            "urn:fluree:injected": "nope"
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Fluree-reserved namespace"));
    }

    #[test]
    fn test_context_expansion() {
        let mut ns = test_registry();

        // Use a context with prefix
        let ctx_json = json!({
            "ex": "http://example.org/"
        });
        let ctx = fluree_graph_json_ld::parse_context(&ctx_json).unwrap();

        let json = json!({
            "@graph": [],
            "ex:machine": "server-01"
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].predicate_name, "machine");
        // The ns code should be for http://example.org/
        assert!(ns.has_prefix("http://example.org/"));
    }

    // ---------- Sidecar form (`txn-meta` top-level key) ----------

    #[test]
    fn test_sidecar_works_without_graph() {
        // Update-shaped transaction — no @graph, no envelope. Sidecar still
        // delivers metadata.
        let mut ns = test_registry();
        let ctx_json = json!({ "f": "https://ns.flur.ee/db#" });
        let ctx = fluree_graph_json_ld::parse_context(&ctx_json).unwrap();

        let json = json!({
            "where": [{"@id": "?s", "@type": "ex:Thing"}],
            "delete": [],
            "insert": [],
            "txn-meta": {
                "f:message": "rename Alice → Alicia"
            }
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].predicate_name, "message");
        assert!(matches!(
            &result[0].value,
            TxnMetaValue::String(s) if s == "rename Alice → Alicia"
        ));
    }

    #[test]
    fn test_sidecar_inherits_outer_context() {
        let mut ns = test_registry();
        let ctx_json = json!({
            "f": "https://ns.flur.ee/db#",
            "ex": "http://example.org/"
        });
        let ctx = fluree_graph_json_ld::parse_context(&ctx_json).unwrap();

        let json = json!({
            "txn-meta": {
                "f:message": "hello",
                "ex:batchId": 7
            }
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.iter().any(|e| e.predicate_name == "message"));
        assert!(result
            .iter()
            .any(|e| e.predicate_name == "batchId" && matches!(&e.value, TxnMetaValue::Long(7))));
    }

    #[test]
    fn test_sidecar_overrides_with_own_context() {
        // Outer context has no `m:` prefix; the sidecar declares its own.
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "txn-meta": {
                "@context": {"m": "http://example.org/meta/"},
                "m:tag": "release"
            }
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].predicate_name, "tag");
    }

    #[test]
    fn test_sidecar_merges_with_envelope_form() {
        let mut ns = test_registry();
        let ctx_json = json!({
            "f": "https://ns.flur.ee/db#",
            "ex": "http://example.org/"
        });
        let ctx = fluree_graph_json_ld::parse_context(&ctx_json).unwrap();

        let json = json!({
            "@graph": [{"@id": "ex:alice"}],
            "ex:envelopeKey": "from-envelope",
            "txn-meta": {
                "f:message": "from-sidecar"
            }
        });

        let result = extract_txn_meta(&json, &ctx, &mut ns, true).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.iter().any(|e| e.predicate_name == "envelopeKey"));
        assert!(result.iter().any(|e| e.predicate_name == "message"));
    }

    #[test]
    fn test_sidecar_must_be_object() {
        let mut ns = test_registry();
        let ctx = empty_context();

        let json = json!({
            "txn-meta": "not an object"
        });

        let err = extract_txn_meta(&json, &ctx, &mut ns, true).unwrap_err();
        assert!(err.to_string().contains("must be an object"));
    }

    #[test]
    fn test_sidecar_enforces_fluree_namespace_allowlist() {
        // f:identity is rejected from the sidecar same as from envelope form.
        let mut ns = test_registry();
        let ctx_json = json!({ "f": "https://ns.flur.ee/db#" });
        let ctx = fluree_graph_json_ld::parse_context(&ctx_json).unwrap();

        let json = json!({
            "txn-meta": {
                "f:identity": "did:example:spoofed"
            }
        });

        let err = extract_txn_meta(&json, &ctx, &mut ns, true).unwrap_err();
        assert!(
            err.to_string()
                .contains("only f:message and f:author are user-settable"),
            "unexpected error: {err}"
        );
    }
}
