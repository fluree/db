//! JSON-LD parse context: bundles the parsed `@context`, path aliases,
//! and parse-policy flags into one object threaded through JSON-LD parsers.
//!
//! This replaces separate `(context, path_aliases, strict)` parameter lists
//! with a single `&JsonLdParseCtx`.

use super::PathAliasMap;
use fluree_graph_json_ld::{
    details_with_policy, details_with_vocab_policy, expand_iri_with_policy, ContextEntry,
    ParsedContext,
};
use serde_json::Value as JsonValue;

/// Parse-time policy flags for JSON-LD parsing.
///
/// Not query semantics — does not flow with `UnresolvedOptions`.
#[derive(Debug, Clone, Copy)]
pub struct JsonLdParsePolicy {
    /// When `true`, reject unresolved compact-looking IRIs at parse time.
    pub strict_compact_iri: bool,
}

impl Default for JsonLdParsePolicy {
    fn default() -> Self {
        Self {
            strict_compact_iri: true,
        }
    }
}

/// Bundled JSON-LD parse context: `@context` + path aliases + policy.
///
/// Threaded through all JSON-LD query/transaction parse functions.
/// Expansion methods apply the strict/permissive IRI policy automatically.
pub struct JsonLdParseCtx {
    pub context: ParsedContext,
    pub path_aliases: PathAliasMap,
    pub policy: JsonLdParsePolicy,
}

impl JsonLdParseCtx {
    pub fn new(
        context: ParsedContext,
        path_aliases: PathAliasMap,
        policy: JsonLdParsePolicy,
    ) -> Self {
        Self {
            context,
            path_aliases,
            policy,
        }
    }

    /// Expand a subject `@id` value (uses `@base`, not `@vocab`).
    pub fn expand_id(
        &self,
        s: &str,
    ) -> fluree_graph_json_ld::Result<(String, Option<ContextEntry>)> {
        details_with_vocab_policy(s, &self.context, false, self.policy.strict_compact_iri)
    }

    /// Expand a predicate or `@type` value (uses `@vocab`).
    pub fn expand_vocab(
        &self,
        s: &str,
    ) -> fluree_graph_json_ld::Result<(String, Option<ContextEntry>)> {
        details_with_policy(s, &self.context, self.policy.strict_compact_iri)
    }

    /// Expand a compact IRI to its full form (uses `@vocab`, returns only the IRI).
    pub fn expand_iri(&self, s: &str) -> fluree_graph_json_ld::Result<String> {
        expand_iri_with_policy(s, &self.context, self.policy.strict_compact_iri)
    }

    /// Unchecked vocab expansion — for lookup comparisons, not IRI production.
    pub fn expand_vocab_unchecked(&self, s: &str) -> String {
        fluree_graph_json_ld::expand_iri(s, &self.context)
    }
}

/// Read `opts.strictCompactIri` from a JSON-LD query or transaction object.
///
/// Returns `None` if the key is absent (caller applies default).
/// Only reads from the `opts` sub-object — no top-level fallback.
pub fn parse_strict_compact_iri_opt(obj: &serde_json::Map<String, JsonValue>) -> Option<bool> {
    obj.get("opts")
        .and_then(|v| v.as_object())
        .and_then(|opts| opts.get("strictCompactIri"))
        .and_then(serde_json::Value::as_bool)
}

/// Resolve a [`JsonLdParsePolicy`] from an optional programmatic override and
/// the raw JSON-LD object.
///
/// Precedence:
/// 1. `override_strict` (builder/API override) if `Some`
/// 2. `opts.strictCompactIri` in `obj`
/// 3. default `true`
pub fn resolve_parse_policy(
    override_strict: Option<bool>,
    obj: &serde_json::Map<String, JsonValue>,
) -> JsonLdParsePolicy {
    let strict = override_strict
        .or_else(|| parse_strict_compact_iri_opt(obj))
        .unwrap_or(true);
    JsonLdParsePolicy {
        strict_compact_iri: strict,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_default_is_strict() {
        let policy = JsonLdParsePolicy::default();
        assert!(policy.strict_compact_iri);
    }

    #[test]
    fn test_parse_from_opts() {
        let json = json!({"opts": {"strictCompactIri": false}});
        let obj = json.as_object().unwrap();
        assert_eq!(parse_strict_compact_iri_opt(obj), Some(false));
    }

    #[test]
    fn test_parse_absent_returns_none() {
        let json = json!({"select": ["?s"]});
        let obj = json.as_object().unwrap();
        assert_eq!(parse_strict_compact_iri_opt(obj), None);
    }

    #[test]
    fn test_no_top_level_fallback() {
        let json = json!({"strictCompactIri": false});
        let obj = json.as_object().unwrap();
        assert_eq!(parse_strict_compact_iri_opt(obj), None);
    }

    #[test]
    fn test_resolve_precedence_override_wins() {
        let json = json!({"opts": {"strictCompactIri": true}});
        let obj = json.as_object().unwrap();
        let policy = resolve_parse_policy(Some(false), obj);
        assert!(!policy.strict_compact_iri);
    }

    #[test]
    fn test_resolve_precedence_json_over_default() {
        let json = json!({"opts": {"strictCompactIri": false}});
        let obj = json.as_object().unwrap();
        let policy = resolve_parse_policy(None, obj);
        assert!(!policy.strict_compact_iri);
    }

    #[test]
    fn test_resolve_precedence_default() {
        let json = json!({"select": ["?s"]});
        let obj = json.as_object().unwrap();
        let policy = resolve_parse_policy(None, obj);
        assert!(policy.strict_compact_iri);
    }

    #[test]
    fn test_ctx_expand_vocab_strict() {
        let ctx_json = json!({"ex": "http://example.org/"});
        let context = fluree_graph_json_ld::parse_context(&ctx_json).unwrap();
        let ctx = JsonLdParseCtx::new(context, PathAliasMap::new(), JsonLdParsePolicy::default());

        // Defined prefix resolves
        let (expanded, _) = ctx.expand_vocab("ex:Person").unwrap();
        assert_eq!(expanded, "http://example.org/Person");

        // Undefined prefix is rejected in strict mode
        let err = ctx.expand_vocab("foo:Bar");
        assert!(err.is_err());
    }

    #[test]
    fn test_ctx_expand_vocab_permissive() {
        let ctx_json = json!({"ex": "http://example.org/"});
        let context = fluree_graph_json_ld::parse_context(&ctx_json).unwrap();
        let policy = JsonLdParsePolicy {
            strict_compact_iri: false,
        };
        let ctx = JsonLdParseCtx::new(context, PathAliasMap::new(), policy);

        // Undefined prefix passes through in permissive mode
        let (expanded, _) = ctx.expand_vocab("foo:Bar").unwrap();
        assert_eq!(expanded, "foo:Bar");
    }
}
