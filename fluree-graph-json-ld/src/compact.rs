use crate::context::ParsedContext;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A precomputed lookup table for efficient IRI compaction.
///
/// Namespace IRI entries are sorted longest-first so the most specific
/// prefix always wins (e.g. `http://schema.org/Person/` before
/// `http://schema.org/`). This produces the shortest possible compact
/// form for every IRI.
///
/// Build once from a `ParsedContext`, then call `compact_vocab()` or
/// `compact_id()` for each IRI — no per-call HashMap rebuilds.
#[derive(Debug, Clone)]
pub struct ContextCompactor {
    /// Full IRI → compact term (exact matches like `"schema:Person"` → `"REPLACE"`)
    exact_vocab: HashMap<String, String>,
    /// Namespace IRIs (ending in `/` or `#`) paired with their compact prefix,
    /// sorted longest-first for greedy matching.
    prefixes_vocab: Vec<(String, String)>,

    /// Same as above but without @vocab (for @id compaction)
    exact_id: HashMap<String, String>,
    prefixes_id: Vec<(String, String)>,
}

impl ContextCompactor {
    /// Build a compactor from a parsed JSON-LD context.
    ///
    /// Precomputes two reverse lookup tables (vocab and id variants)
    /// so that repeated `compact_vocab` / `compact_id` calls are
    /// pure lookups with no allocation beyond the result string.
    pub fn new(context: &ParsedContext) -> Self {
        let exact_vocab = reverse_context(context, true, true);
        let prefixes_vocab = sorted_prefix_iris(&exact_vocab);

        let exact_id = reverse_context(context, false, true);
        let prefixes_id = sorted_prefix_iris(&exact_id);

        Self {
            exact_vocab,
            prefixes_vocab,
            exact_id,
            prefixes_id,
        }
    }

    /// Compact an IRI using @vocab rules (for property names, @type values).
    ///
    /// Tries exact match first, then longest-prefix match. @vocab and @base
    /// both participate as implicit prefixes.
    pub fn compact_vocab(&self, iri: &str) -> String {
        compact_with(&self.exact_vocab, &self.prefixes_vocab, iri, true)
    }

    /// Compact an IRI for an `@id` position.
    ///
    /// Per JSON-LD rules, `@vocab` must NOT shorten node identifiers.
    /// Only explicit prefix terms and `@base` may be used.
    pub fn compact_id(&self, iri: &str) -> String {
        compact_with(&self.exact_id, &self.prefixes_id, iri, false)
    }

    /// Create a compaction closure suitable for APIs that expect `Fn(&str) -> String`.
    ///
    /// Optionally tracks which context terms were actually used (for
    /// producing a minimal output context).
    pub fn vocab_fn(
        &self,
        used: Option<Arc<Mutex<HashMap<String, String>>>>,
    ) -> impl Fn(&str) -> String + '_ {
        move |iri: &str| {
            let result = compact_with_tracking(
                &self.exact_vocab,
                &self.prefixes_vocab,
                iri,
                true,
                used.as_ref(),
            );
            result
        }
    }
}

/// Core compaction: exact match → longest prefix → return as-is.
fn compact_with(
    exact: &HashMap<String, String>,
    prefixes: &[(String, String)],
    iri: &str,
    allow_vocab_strip: bool,
) -> String {
    // 1. Exact match
    if let Some(prefix) = exact.get(iri) {
        return prefix.clone();
    }

    // 2. Longest-prefix match
    for (prefix_iri, prefix_name) in prefixes {
        if iri.starts_with(prefix_iri.as_str()) {
            let suffix = &iri[prefix_iri.len()..];
            if prefix_name == ":vocab" || prefix_name == ":base" {
                if allow_vocab_strip || prefix_name == ":base" {
                    return suffix.to_string();
                }
                continue;
            }
            return format!("{prefix_name}:{suffix}");
        }
    }

    // 3. No match
    iri.to_string()
}

/// Like `compact_with` but records which terms were used.
fn compact_with_tracking(
    exact: &HashMap<String, String>,
    prefixes: &[(String, String)],
    iri: &str,
    allow_vocab_strip: bool,
    used: Option<&Arc<Mutex<HashMap<String, String>>>>,
) -> String {
    if let Some(prefix) = exact.get(iri) {
        if let Some(used) = used {
            if let Ok(mut guard) = used.lock() {
                guard.insert(prefix.clone(), iri.to_string());
            }
        }
        return prefix.clone();
    }

    for (prefix_iri, prefix_name) in prefixes {
        if iri.starts_with(prefix_iri.as_str()) {
            let suffix = &iri[prefix_iri.len()..];
            if prefix_name == ":vocab" || prefix_name == ":base" {
                if allow_vocab_strip || prefix_name == ":base" {
                    if let Some(used) = used {
                        if let Ok(mut guard) = used.lock() {
                            guard.insert(prefix_name.clone(), prefix_iri.clone());
                        }
                    }
                    return suffix.to_string();
                }
                continue;
            }
            if let Some(used) = used {
                if let Ok(mut guard) = used.lock() {
                    guard.insert(prefix_name.clone(), prefix_iri.clone());
                }
            }
            return format!("{prefix_name}:{suffix}");
        }
    }

    iri.to_string()
}

/// Build the reverse context: full IRI → compact prefix name.
fn reverse_context(
    context: &ParsedContext,
    include_vocab: bool,
    include_base: bool,
) -> HashMap<String, String> {
    let mut flipped = HashMap::new();

    for (prefix, entry) in &context.terms {
        if let Some(ref id) = entry.id {
            if !entry.derived {
                flipped.insert(id.clone(), prefix.clone());
            }
        }
    }

    if include_vocab {
        if let Some(ref vocab) = context.vocab {
            flipped.insert(vocab.clone(), ":vocab".to_string());
        }
    }
    if include_base {
        if let Some(ref base) = context.base {
            flipped.insert(base.clone(), ":base".to_string());
        }
    }

    flipped
}

/// Extract namespace IRIs (ending in `/` or `#`) and sort longest-first.
fn sorted_prefix_iris(flipped: &HashMap<String, String>) -> Vec<(String, String)> {
    let mut pairs: Vec<(String, String)> = flipped
        .iter()
        .filter(|(iri, _)| iri.ends_with('/') || iri.ends_with('#'))
        .map(|(iri, prefix)| (iri.clone(), prefix.clone()))
        .collect();
    pairs.sort_by_key(|b| std::cmp::Reverse(b.0.len()));
    pairs
}

// ---------------------------------------------------------------------------
// Convenience functions (delegate to ContextCompactor)
// ---------------------------------------------------------------------------

/// Create a compaction function from a parsed context.
///
/// For repeated compaction, prefer building a [`ContextCompactor`] directly.
pub fn compact_fn(
    context: &ParsedContext,
    used: Option<Arc<Mutex<HashMap<String, String>>>>,
) -> impl Fn(&str) -> String + '_ {
    let compactor = ContextCompactor::new(context);
    move |iri: &str| {
        compact_with_tracking(
            &compactor.exact_vocab,
            &compactor.prefixes_vocab,
            iri,
            true,
            used.as_ref(),
        )
    }
}

/// Compact an IRI using a context (convenience; rebuilds lookup per call).
///
/// For repeated compaction, prefer [`ContextCompactor::compact_vocab`].
pub fn compact(iri: &str, context: &ParsedContext) -> String {
    let c = ContextCompactor::new(context);
    c.compact_vocab(iri)
}

/// Compact an IRI for `@id` position (convenience; rebuilds lookup per call).
///
/// For repeated compaction, prefer [`ContextCompactor::compact_id`].
pub fn compact_id(iri: &str, context: &ParsedContext) -> String {
    let c = ContextCompactor::new(context);
    c.compact_id(iri)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_compact_exact_match() {
        let ctx = ParsedContext::parse(
            None,
            &json!({
                "schema": "http://schema.org/",
                "REPLACE": "http://schema.org/Person",
                "x": "schema:x"
            }),
        )
        .unwrap();

        assert_eq!(compact("http://schema.org/x", &ctx), "x");
        assert_eq!(compact("http://schema.org/Person", &ctx), "REPLACE");
    }

    #[test]
    fn test_compact_prefix_match() {
        let ctx = ParsedContext::parse(
            None,
            &json!({
                "schema": "http://schema.org/",
                "REPLACE": "http://schema.org/Person"
            }),
        )
        .unwrap();

        assert_eq!(compact("http://schema.org/xyz", &ctx), "schema:xyz");
    }

    #[test]
    fn test_compact_vocab() {
        let ctx = ParsedContext::parse(None, &json!("https://schema.org")).unwrap();

        assert_eq!(compact("https://schema.org/name", &ctx), "name");
    }

    #[test]
    fn test_compact_no_match() {
        let ctx = ParsedContext::parse(None, &json!({"schema": "http://schema.org/"})).unwrap();

        assert_eq!(compact("schemas", &ctx), "schemas");
        assert_eq!(
            compact("http://example.org/ns#blah", &ctx),
            "http://example.org/ns#blah"
        );
    }

    #[test]
    fn test_compact_nil_clears() {
        let ctx = ParsedContext::parse(
            None,
            &json!([
                {"schema": "http://schema.org/", "REPLACE": "http://schema.org/Person"},
                null
            ]),
        )
        .unwrap();

        // After nil, context should be cleared
        assert_eq!(compact("http://schema.org/x", &ctx), "http://schema.org/x");
    }

    #[test]
    fn test_compact_fn_with_tracking() {
        let ctx = ParsedContext::parse(
            None,
            &json!({
                "schema": "http://schema.org/",
                "REPLACE": "http://schema.org/Person"
            }),
        )
        .unwrap();

        let used = Arc::new(Mutex::new(HashMap::new()));
        let f = compact_fn(&ctx, Some(used.clone()));

        assert_eq!(f("http://schema.org/name"), "schema:name");
        {
            let guard = used.lock().unwrap();
            assert_eq!(guard.get("schema"), Some(&"http://schema.org/".to_string()));
        }

        assert_eq!(f("http://schema.org/Person"), "REPLACE");
        {
            let guard = used.lock().unwrap();
            assert_eq!(
                guard.get("REPLACE"),
                Some(&"http://schema.org/Person".to_string())
            );
        }
    }
}
