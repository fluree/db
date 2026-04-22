//! IRI compaction utilities
//!
//! This module provides `IriCompactor` for converting Sids to IRIs and
//! compacting them using @context prefix mappings via the json-ld library.
//!
//! When a namespace exists in `Db.namespaces()` but has no prefix alias in
//! the query's `@context`, a short prefix is auto-derived from the namespace
//! URI so that every IRI in the database gets a compact display form.

use fluree_db_core::Sid;
use fluree_graph_json_ld::{ContextCompactor, ParsedContext};
use fluree_vocab::namespaces;
use std::collections::{HashMap, HashSet};

use super::{FormatError, Result};

/// Context for compacting IRIs using the json-ld library and Db namespace codes.
///
/// The compactor performs two operations:
/// 1. **Decode**: Convert a Sid (namespace_code + name) to a full IRI using Db.namespaces()
/// 2. **Compact**: Use a precomputed [`ContextCompactor`] to replace IRI prefixes with @context aliases
///
/// When a namespace has no explicit prefix in the query context, a short prefix
/// is auto-derived from the namespace URI (e.g., `https://dblp.org/rec/` → `rec:`).
/// This ensures every IRI in the database gets a readable compact form.
///
/// The reverse lookup table is built once at construction time and reused
/// for every IRI compacted through this instance.
#[derive(Debug, Clone)]
pub struct IriCompactor {
    /// Namespace code -> IRI prefix (from Db.namespaces())
    ///
    /// Example: `2 -> "http://www.w3.org/2001/XMLSchema#"`
    namespace_codes: HashMap<u16, String>,

    /// Parsed @context from the query (for advanced access / @reverse lookups)
    context: ParsedContext,

    /// Precomputed reverse lookup for fast IRI → compact-form compaction
    compactor: ContextCompactor,

    /// @reverse term definitions: maps full IRI → compact term name.
    /// Built once at construction for reverse property compaction.
    reverse_terms: HashMap<String, String>,

    /// Auto-derived prefixes for namespaces not covered by the @context.
    /// Sorted longest-first for greedy matching (same strategy as ContextCompactor).
    fallback_prefixes: Vec<(String, String)>,
}

impl IriCompactor {
    /// Build a compactor from Db namespace codes and a ParsedContext.
    ///
    /// Precomputes the reverse lookup tables so that every subsequent
    /// `compact_vocab_iri` / `compact_id_iri` call is a pure lookup.
    ///
    /// For namespaces in `namespace_codes` that have no matching prefix in
    /// the context, a short prefix is auto-derived from the namespace URI.
    pub fn new(namespace_codes: &HashMap<u16, String>, context: &ParsedContext) -> Self {
        let compactor = ContextCompactor::new(context);
        let reverse_terms = build_reverse_terms(context);
        let fallback_prefixes = build_fallback_prefixes(namespace_codes, context);
        Self {
            namespace_codes: namespace_codes.clone(),
            context: context.clone(),
            compactor,
            reverse_terms,
            fallback_prefixes,
        }
    }

    /// Build a compactor with just namespace codes (no @context compaction).
    ///
    /// No fallback prefixes are generated — IRIs come through uncompacted.
    /// Use `new()` with a `ParsedContext` to enable compaction.
    pub fn from_namespaces(namespace_codes: &HashMap<u16, String>) -> Self {
        let default_ctx = ParsedContext::default();
        let compactor = ContextCompactor::new(&default_ctx);
        Self {
            namespace_codes: namespace_codes.clone(),
            context: default_ctx,
            compactor,
            reverse_terms: HashMap::new(),
            fallback_prefixes: Vec::new(),
        }
    }

    /// Decode a Sid to a full IRI.
    ///
    /// Returns an error if the namespace code is not registered (this indicates
    /// a serious invariant violation: we should never have Sids we cannot decode).
    pub fn decode_sid(&self, sid: &Sid) -> Result<String> {
        if sid.namespace_code == namespaces::EMPTY || sid.namespace_code == namespaces::OVERFLOW {
            return Ok(sid.name.to_string());
        }
        let prefix = self
            .namespace_codes
            .get(&sid.namespace_code)
            .ok_or(FormatError::UnknownNamespace(sid.namespace_code))?;
        Ok(format!("{}{}", prefix, sid.name))
    }

    /// Compact a **forward** predicate / @type IRI using the @context (vocab rules).
    ///
    /// Handles:
    /// - Exact term matches (e.g., `"Person"` ← `"http://schema.org/Person"`)
    /// - Prefix matches (e.g., `"schema:xyz"` ← `"http://schema.org/xyz"`)
    /// - @vocab handling (bare terms for vocab-prefixed IRIs)
    ///
    /// Does NOT consult `@reverse` term definitions — those are direction-specific
    /// and would corrupt output when a forward predicate's IRI matches a reverse
    /// alias's target. Use [`compact_reverse_iri`](Self::compact_reverse_iri) when
    /// formatting an edge in the reverse direction.
    ///
    /// Returns the compacted form or the full IRI if no match.
    pub fn compact_vocab_iri(&self, iri: &str) -> String {
        self.compactor.compact_vocab(iri)
    }

    /// Compact an IRI for an `@id` position.
    ///
    /// Per JSON-LD rules, `@vocab` must NOT compact node identifiers; only explicit
    /// prefixes/terms and `@base` are allowed.
    pub fn compact_id_iri(&self, iri: &str) -> String {
        self.compactor.compact_id(iri)
    }

    /// Compact an IRI for **display purposes** (CLI table/CSV output).
    ///
    /// Like `compact_vocab_iri`, but also tries auto-derived fallback prefixes
    /// for namespaces that have no explicit context prefix. These synthetic
    /// prefixes are suitable for human-readable output but should NOT be used
    /// in structured formats (JSON-LD, SPARQL JSON) where consumers need to
    /// know the prefix mappings.
    pub fn compact_for_display(&self, iri: &str) -> String {
        let result = self.compactor.compact_vocab(iri);
        if result == iri {
            if let Some(compacted) = self.try_fallback(iri) {
                return compacted;
            }
        }
        result
    }

    /// Decode a Sid and compact as a forward predicate / @type value.
    ///
    /// This is the most common operation for formatting.
    pub fn compact_sid(&self, sid: &Sid) -> Result<String> {
        let iri = self.decode_sid(sid)?;
        Ok(self.compact_vocab_iri(&iri))
    }

    /// Compact an IRI string (already decoded) as a forward predicate.
    ///
    /// Used for IriMatch bindings where the canonical IRI is already available.
    pub fn compact_iri(&self, iri: &str) -> Result<String> {
        Ok(self.compact_vocab_iri(iri))
    }

    /// Compact an IRI for a **reverse-direction** edge key.
    ///
    /// Prefers `@reverse` aliases from the context. Falls back to forward
    /// vocab compaction if no reverse alias exists (producing the bare
    /// predicate IRI — the caller should still treat it as reverse).
    pub fn compact_reverse_iri(&self, iri: &str) -> String {
        if let Some(term) = self.reverse_terms.get(iri) {
            return term.clone();
        }
        self.compactor.compact_vocab(iri)
    }

    /// Decode a Sid and compact as a reverse-direction edge key.
    pub fn compact_reverse_sid(&self, sid: &Sid) -> Result<String> {
        let iri = self.decode_sid(sid)?;
        Ok(self.compact_reverse_iri(&iri))
    }

    /// Decode a Sid and compact for display (with fallback prefixes).
    pub fn compact_sid_for_display(&self, sid: &Sid) -> Result<String> {
        let iri = self.decode_sid(sid)?;
        Ok(self.compact_for_display(&iri))
    }

    /// Compact an IRI string for display (with fallback prefixes).
    pub fn compact_iri_for_display(&self, iri: &str) -> Result<String> {
        Ok(self.compact_for_display(iri))
    }

    /// Try to encode a full IRI back into a `Sid` using namespace codes.
    ///
    /// Returns `None` if the IRI doesn't match any known namespace prefix.
    /// This is the inverse of `decode_sid` and is used for schema index lookups
    /// when the source data is a full IRI (e.g., from `BinaryIndexStore`).
    pub fn try_encode_iri(&self, iri: &str) -> Option<Sid> {
        // Try each namespace prefix (longest match wins)
        let mut best: Option<(u16, &str, usize)> = None;
        for (&code, prefix) in &self.namespace_codes {
            if iri.starts_with(prefix.as_str()) && prefix.len() > best.map_or(0, |b| b.2) {
                let local = &iri[prefix.len()..];
                best = Some((code, local, prefix.len()));
            }
        }
        best.map(|(code, local, _)| Sid::new(code, local))
    }

    /// Check if a namespace code is registered
    pub fn has_namespace(&self, code: u16) -> bool {
        self.namespace_codes.contains_key(&code)
    }

    /// Get the ParsedContext (for advanced use)
    pub fn context(&self) -> &ParsedContext {
        &self.context
    }

    /// Get the precomputed compactor (for constructing closures)
    pub fn ctx_compactor(&self) -> &ContextCompactor {
        &self.compactor
    }

    /// Build the effective prefix → IRI map used by this compactor.
    ///
    /// Combines prefixes from the `@context` (term definitions that look like
    /// namespace prefixes) with auto-derived fallback prefixes. This is the
    /// authoritative map that matches what `compact_for_display` actually uses.
    pub fn effective_prefixes(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();

        // 1. Context term definitions that are simple prefix mappings
        for (term, entry) in &self.context.terms {
            if let Some(ref iri) = entry.id {
                // Only include terms that look like prefix mappings
                // (IRI ends with / or # — otherwise it's a specific term definition)
                if iri.ends_with('/') || iri.ends_with('#') {
                    map.insert(term.clone(), iri.clone());
                }
            }
        }

        // 2. Fallback prefixes (auto-derived for uncovered namespaces)
        for (ns_iri, prefix_name) in &self.fallback_prefixes {
            map.entry(prefix_name.clone())
                .or_insert_with(|| ns_iri.clone());
        }

        map
    }

    /// Try to compact an IRI using the auto-derived fallback prefixes.
    fn try_fallback(&self, iri: &str) -> Option<String> {
        for (ns_iri, prefix_name) in &self.fallback_prefixes {
            if iri.starts_with(ns_iri.as_str()) {
                let suffix = &iri[ns_iri.len()..];
                return Some(format!("{prefix_name}:{suffix}"));
            }
        }
        None
    }
}

/// Build a map of @reverse IRI → term name for fast reverse-property lookup.
///
/// If multiple terms define @reverse for the same IRI, the lexicographically
/// smallest term name wins (deterministic).
fn build_reverse_terms(context: &ParsedContext) -> HashMap<String, String> {
    let mut map: HashMap<String, String> = HashMap::new();
    for (key, entry) in &context.terms {
        if let Some(ref rev_iri) = entry.reverse {
            map.entry(rev_iri.clone())
                .and_modify(|existing| {
                    if key < existing {
                        *existing = key.clone();
                    }
                })
                .or_insert_with(|| key.clone());
        }
    }
    map
}

/// Build auto-derived prefix names for namespaces in `namespace_codes` that
/// have no matching prefix in the query context.
///
/// Only generates fallbacks when the context is non-empty (has terms, @vocab,
/// or @base), indicating the user/query established some prefix context.
/// For empty contexts (no @context at all), returns an empty vec so that
/// IRIs come through uncompacted.
///
/// For each unmapped namespace URI, derives a short prefix from the URI's
/// last meaningful path segment (e.g., `https://dblp.org/rec/` → `rec`).
/// Conflicts with existing context prefixes are resolved by appending a
/// numeric suffix (e.g., `rec2`).
///
/// Returns entries sorted longest-first for greedy matching.
fn build_fallback_prefixes(
    namespace_codes: &HashMap<u16, String>,
    context: &ParsedContext,
) -> Vec<(String, String)> {
    // Only auto-derive when the context is non-empty — if the user/query
    // didn't establish any prefix context, don't impose synthetic prefixes.
    if context.terms.is_empty() && context.vocab.is_none() && context.base.is_none() {
        return Vec::new();
    }
    // Collect all namespace URIs that already have a context prefix.
    let covered_iris: HashSet<&str> = context
        .terms
        .values()
        .filter_map(|e| e.id.as_deref())
        .collect();

    // Also treat @vocab and @base as covered.
    let mut covered = covered_iris;
    if let Some(ref vocab) = context.vocab {
        covered.insert(vocab.as_str());
    }
    if let Some(ref base) = context.base {
        covered.insert(base.as_str());
    }

    // Collect existing prefix names to avoid conflicts.
    let mut used_names: HashSet<String> = context.terms.keys().cloned().collect();

    let mut fallbacks: Vec<(String, String)> = Vec::new();

    // Sort by namespace code for deterministic prefix generation.
    let mut ns_entries: Vec<_> = namespace_codes.iter().collect();
    ns_entries.sort_by_key(|(code, _)| *code);

    for (_code, ns_iri) in ns_entries {
        // Skip empty namespace (internal blank node namespace).
        if ns_iri.is_empty() {
            continue;
        }
        // Skip namespaces that end with neither `/` nor `#` — not standard prefixes.
        if !ns_iri.ends_with('/') && !ns_iri.ends_with('#') {
            continue;
        }
        // Skip if the context already covers this namespace.
        if covered.contains(ns_iri.as_str()) {
            continue;
        }

        let base_name = derive_prefix_name(ns_iri);
        if base_name.is_empty() {
            continue;
        }

        // Resolve conflicts with existing prefix names.
        let name = if !used_names.contains(&base_name) {
            base_name.clone()
        } else {
            let mut counter = 2u32;
            loop {
                let candidate = format!("{base_name}{counter}");
                if !used_names.contains(&candidate) {
                    break candidate;
                }
                counter += 1;
            }
        };

        used_names.insert(name.clone());
        fallbacks.push((ns_iri.clone(), name));
    }

    // Sort longest-first for greedy matching (most specific prefix wins).
    fallbacks.sort_by_key(|b| std::cmp::Reverse(b.0.len()));
    fallbacks
}

/// Derive a short prefix name from a namespace URI.
///
/// Extracts the last meaningful path segment:
/// - `https://dblp.org/rec/` → `rec`
/// - `http://www.w3.org/2001/XMLSchema#` → `xmlschema`
/// - `http://schema.org/` → `schema`
/// - `https://example.org/ns/vocab/` → `vocab`
///
/// For domain-only URIs (e.g., `http://schema.org/`), uses the domain's
/// first label (before the first dot, excluding `www`).
///
/// Filters to ASCII alphanumeric characters and lowercases the result.
fn derive_prefix_name(ns_iri: &str) -> String {
    // Strip trailing `/` or `#`
    let trimmed = ns_iri.trim_end_matches(['/', '#']);

    // Find the last path segment
    let segment = trimmed.rsplit(['/', '#', ':']).next().unwrap_or("");

    // If the segment looks like a domain (contains dots), extract the meaningful part.
    // e.g., "schema.org" → "schema", "www.w3.org" → "w3"
    let effective = if segment.contains('.') {
        segment
            .split('.')
            .find(|&part| !part.is_empty() && part != "www" && part != "com" && part != "org")
            .unwrap_or(segment)
    } else {
        segment
    };

    // Filter to alphanumeric, lowercase
    let name: String = effective
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect::<String>()
        .to_lowercase();

    // Avoid very short or numeric-only names
    if name.len() < 2 || name.chars().all(|c| c.is_ascii_digit()) {
        return String::new();
    }

    name
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_vocab::{rdf, xsd};
    use serde_json::json;

    fn make_test_namespaces() -> HashMap<u16, String> {
        let mut map = HashMap::new();
        map.insert(0, String::new());
        map.insert(2, xsd::NS.to_string());
        map.insert(3, rdf::NS.to_string());
        map.insert(17, "http://schema.org/".to_string());
        map.insert(100, "http://example.org/".to_string());
        map
    }

    fn make_test_context() -> ParsedContext {
        ParsedContext::parse(
            None,
            &json!({
                "xsd": xsd::NS,
                "rdf": rdf::NS,
                "schema": "http://schema.org/",
                "ex": "http://example.org/"
            }),
        )
        .unwrap()
    }

    #[test]
    fn test_decode_sid() {
        let compactor = IriCompactor::from_namespaces(&make_test_namespaces());

        let sid = Sid::new(2, "string");
        assert_eq!(
            compactor.decode_sid(&sid).unwrap(),
            "http://www.w3.org/2001/XMLSchema#string".to_string()
        );

        let sid = Sid::new(100, "Person");
        assert_eq!(
            compactor.decode_sid(&sid).unwrap(),
            "http://example.org/Person".to_string()
        );

        // Unknown namespace
        let sid = Sid::new(999, "unknown");
        assert!(matches!(
            compactor.decode_sid(&sid),
            Err(FormatError::UnknownNamespace(999))
        ));
    }

    #[test]
    fn test_compact_iri_with_context() {
        let compactor = IriCompactor::new(&make_test_namespaces(), &make_test_context());

        // Prefix matches via @context
        assert_eq!(compactor.compact_vocab_iri(xsd::STRING), "xsd:string");

        assert_eq!(compactor.compact_vocab_iri(rdf::TYPE), "rdf:type");

        assert_eq!(
            compactor.compact_vocab_iri("http://schema.org/Person"),
            "schema:Person"
        );

        assert_eq!(
            compactor.compact_vocab_iri("http://example.org/myThing"),
            "ex:myThing"
        );
    }

    #[test]
    fn test_compact_iri_no_match() {
        let compactor = IriCompactor::new(&make_test_namespaces(), &make_test_context());

        // No matching prefix - returns full IRI
        assert_eq!(
            compactor.compact_vocab_iri("http://unknown.org/something"),
            "http://unknown.org/something"
        );
    }

    #[test]
    fn test_compact_sid() {
        let compactor = IriCompactor::new(&make_test_namespaces(), &make_test_context());

        // Known namespace with @context prefix
        let sid = Sid::new(2, "string");
        assert_eq!(compactor.compact_sid(&sid).unwrap(), "xsd:string");

        let sid = Sid::new(17, "Person");
        assert_eq!(compactor.compact_sid(&sid).unwrap(), "schema:Person");
    }

    #[test]
    fn test_compact_without_context() {
        let compactor = IriCompactor::from_namespaces(&make_test_namespaces());

        // No @context and no fallback — IRIs come through uncompacted
        let sid = Sid::new(2, "string");
        assert_eq!(
            compactor.compact_sid(&sid).unwrap(),
            "http://www.w3.org/2001/XMLSchema#string"
        );
    }

    #[test]
    fn test_fallback_prefixes_for_unmapped_namespaces() {
        // Context only has "ex" for example.org, but DB also has schema.org
        let mut namespaces = HashMap::new();
        namespaces.insert(100, "http://example.org/".to_string());
        namespaces.insert(101, "http://schema.org/".to_string());
        namespaces.insert(102, "https://dblp.org/rec/".to_string());

        let context = ParsedContext::parse(
            None,
            &json!({
                "ex": "http://example.org/"
            }),
        )
        .unwrap();

        let compactor = IriCompactor::new(&namespaces, &context);

        // Context prefix works via standard method
        assert_eq!(
            compactor.compact_vocab_iri("http://example.org/Person"),
            "ex:Person"
        );

        // Standard method does NOT use fallback
        assert_eq!(
            compactor.compact_vocab_iri("http://schema.org/name"),
            "http://schema.org/name"
        );

        // Display method DOES use fallback
        assert_eq!(
            compactor.compact_for_display("http://schema.org/name"),
            "schema:name"
        );
        assert_eq!(
            compactor.compact_for_display("https://dblp.org/rec/conf/sigir/123"),
            "rec:conf/sigir/123"
        );
    }

    #[test]
    fn test_fallback_prefix_conflict_resolution() {
        let mut namespaces = HashMap::new();
        namespaces.insert(100, "http://a.org/foo/".to_string());
        namespaces.insert(101, "http://b.org/foo/".to_string());
        namespaces.insert(102, "http://example.org/".to_string());

        // Need a non-empty context to trigger fallback generation
        let context = ParsedContext::parse(None, &json!({"ex": "http://example.org/"})).unwrap();
        let compactor = IriCompactor::new(&namespaces, &context);

        // Both derive "foo", but one should get "foo" and the other "foo2"
        let a = compactor.compact_for_display("http://a.org/foo/bar");
        let b = compactor.compact_for_display("http://b.org/foo/bar");

        assert!(a.ends_with(":bar"), "expected prefix:bar, got {a}");
        assert!(b.ends_with(":bar"), "expected prefix:bar, got {b}");
        assert_ne!(a, b, "should have different prefixes");
    }

    #[test]
    fn test_derive_prefix_name() {
        assert_eq!(derive_prefix_name("https://dblp.org/rec/"), "rec");
        assert_eq!(
            derive_prefix_name("http://www.w3.org/2001/XMLSchema#"),
            "xmlschema"
        );
        assert_eq!(derive_prefix_name("http://schema.org/"), "schema");
        assert_eq!(derive_prefix_name("http://example.org/ns/vocab/"), "vocab");
        // Too short
        assert_eq!(derive_prefix_name("http://example.org/a/"), "");
        // Numeric only
        assert_eq!(derive_prefix_name("http://example.org/2001/"), "");
    }
}
