use crate::context::ParsedContext;
use rustc_hash::FxHashMap;
use std::borrow::Cow;
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
    /// Full IRI → the term defined for exactly that IRI. Every compaction
    /// hashes the IRI here, so it uses Fx rather than SipHash; its keys are
    /// the caller's own context terms.
    exact: FxHashMap<String, String>,
    /// Namespaces for a property or type, sorted longest-first for greedy
    /// matching.
    prefixes_vocab: Vec<(String, Namespace)>,
    /// The same without `@vocab`, for an `@id`.
    prefixes_id: Vec<(String, Namespace)>,
}

/// What shortens IRIs that start with a namespace.
#[derive(Debug, Clone)]
enum Namespace {
    /// A prefix term: `term:suffix`.
    Term(String),
    /// `@vocab`: the suffix alone, as a term.
    Vocab,
    /// `@base`: the suffix as a reference resolving back against the base.
    Base,
}

impl ContextCompactor {
    /// Build a compactor from a parsed JSON-LD context.
    ///
    /// Precomputes the lookup tables so that repeated `compact_vocab` /
    /// `compact_id` calls are pure lookups with no allocation beyond the
    /// result string.
    pub fn new(context: &ParsedContext) -> Self {
        let exact = context
            .terms
            .iter()
            .filter(|(_, entry)| !entry.derived)
            .filter_map(|(term, entry)| Some((entry.id.clone()?, term.clone())))
            .collect();
        Self {
            exact,
            prefixes_vocab: namespaces(context, true),
            prefixes_id: namespaces(context, false),
        }
    }

    /// Compact an IRI using @vocab rules (for property names, @type values).
    ///
    /// Tries exact match first, then longest-prefix match. @vocab and @base
    /// both participate as implicit prefixes.
    pub fn compact_vocab(&self, iri: &str) -> String {
        compact_with(&self.exact, &self.prefixes_vocab, iri, None).into_owned()
    }

    /// Compact an IRI for an `@id` position.
    ///
    /// Per JSON-LD rules, `@vocab` must NOT shorten node identifiers.
    /// Only explicit prefix terms and `@base` may be used.
    pub fn compact_id(&self, iri: &str) -> String {
        self.compact_id_cow(iri).into_owned()
    }

    /// [`compact_id`](Self::compact_id), borrowing `iri` when nothing
    /// shortens it.
    pub fn compact_id_cow<'a>(&self, iri: &'a str) -> Cow<'a, str> {
        compact_with(&self.exact, &self.prefixes_id, iri, None)
    }

    /// Create a compaction closure suitable for APIs that expect `Fn(&str) -> String`.
    ///
    /// Optionally tracks which context entries were actually used (for
    /// producing a minimal output context): a term under its name, and the
    /// vocabulary and base under `@vocab` and `@base`.
    pub fn vocab_fn(
        &self,
        used: Option<Arc<Mutex<HashMap<String, String>>>>,
    ) -> impl Fn(&str) -> String + '_ {
        move |iri: &str| {
            compact_with(&self.exact, &self.prefixes_vocab, iri, used.as_ref()).into_owned()
        }
    }
}

/// Core compaction: exact match → longest prefix → `iri` itself, borrowed,
/// recording in `used` the context entry that shortened the IRI.
fn compact_with<'a>(
    exact: &FxHashMap<String, String>,
    prefixes: &[(String, Namespace)],
    iri: &'a str,
    used: Option<&Arc<Mutex<HashMap<String, String>>>>,
) -> Cow<'a, str> {
    let note = |key: &str, value: &str| {
        if let Some(mut guard) = used.and_then(|used| used.lock().ok()) {
            guard.insert(key.to_string(), value.to_string());
        }
    };

    if let Some(term) = exact.get(iri) {
        note(term, iri);
        return Cow::Owned(term.clone());
    }

    for (namespace, shortens) in prefixes {
        let Some(suffix) = iri.strip_prefix(namespace.as_str()) else {
            continue;
        };
        let (compacted, key) = match shortens {
            Namespace::Term(term) => (Some(prefixed(term, suffix)), term.as_str()),
            Namespace::Vocab => (vocab_term(suffix), "@vocab"),
            Namespace::Base => (base_reference(namespace, suffix), "@base"),
        };
        if let Some(compacted) = compacted {
            note(key, namespace);
            return Cow::Owned(compacted);
        }
    }

    Cow::Borrowed(iri)
}

/// The reference that resolves against `base` (RFC 3986) back to `base`
/// followed by `suffix`: `#suffix` under a base ending in `#`, the suffix
/// itself under one ending in `/`, and `None` for a suffix resolution would
/// read differently (a leading `/`, a scheme-like first segment, a dot
/// segment).
/// `term:suffix`, built by concatenation rather than through `format!`.
fn prefixed(term: &str, suffix: &str) -> String {
    let mut compacted = String::with_capacity(term.len() + 1 + suffix.len());
    compacted.push_str(term);
    compacted.push(':');
    compacted.push_str(suffix);
    compacted
}

fn base_reference(base: &str, suffix: &str) -> Option<String> {
    if base.ends_with('#') {
        return Some(format!("#{suffix}"));
    }
    let path = suffix.split(['?', '#']).next().unwrap_or("");
    let first_segment = path.split('/').next().unwrap_or("");
    let reads_back = !path.starts_with('/')
        && !first_segment.contains(':')
        && !path.split('/').any(|s| s == "." || s == "..");
    reads_back.then(|| suffix.to_string())
}

/// The term that expands against the vocabulary back to the vocabulary
/// followed by `suffix`, and `None` for a suffix that is empty or holds a
/// `:`, which reads as a compact or absolute IRI.
fn vocab_term(suffix: &str) -> Option<String> {
    (!suffix.is_empty() && !suffix.contains(':')).then(|| suffix.to_string())
}

/// The namespaces a context shortens IRIs by, longest first. A prefix term
/// or the base ends in `/` or `#`; `@vocab` joins as written, so it is one
/// whatever it ends in. For one IRI the vocabulary wins over the base (a
/// property shortened by the base's rule, `#name`, reads back as a
/// reference) and both win over a term.
fn namespaces(context: &ParsedContext, include_vocab: bool) -> Vec<(String, Namespace)> {
    let ends_a_namespace = |iri: &str| iri.ends_with('/') || iri.ends_with('#');
    let mut by_iri: HashMap<String, Namespace> = context
        .terms
        .iter()
        .filter(|(_, entry)| !entry.derived)
        .filter_map(|(term, entry)| Some((entry.id.clone()?, Namespace::Term(term.clone()))))
        .filter(|(iri, _)| ends_a_namespace(iri))
        .collect();
    if let Some(base) = context.base.as_ref().filter(|b| ends_a_namespace(b)) {
        by_iri.insert(base.clone(), Namespace::Base);
    }
    if let Some(vocab) = context.vocab.as_ref().filter(|_| include_vocab) {
        by_iri.insert(vocab.clone(), Namespace::Vocab);
    }
    let mut namespaces: Vec<_> = by_iri.into_iter().collect();
    namespaces.sort_by_key(|(iri, _)| std::cmp::Reverse(iri.len()));
    namespaces
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
        compact_with(
            &compactor.exact,
            &compactor.prefixes_vocab,
            iri,
            used.as_ref(),
        )
        .into_owned()
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

    /// An `@id` compaction borrows the IRI when no term or prefix shortens it.
    #[test]
    fn an_unshortened_id_is_borrowed() {
        let ctx = ParsedContext::parse(
            None,
            &json!({"schema": "http://schema.org/", "me": "http://example.org/me"}),
        )
        .unwrap();
        let compactor = ContextCompactor::new(&ctx);
        let unmatched = "http://elsewhere.example/y";
        assert!(matches!(compactor.compact_id_cow(unmatched), Cow::Borrowed(i) if i == unmatched));
        assert!(matches!(
            compactor.compact_id_cow("http://schema.org/x"),
            Cow::Owned(c) if c == "schema:x"
        ));
        assert!(matches!(
            compactor.compact_id_cow("http://example.org/me"),
            Cow::Owned(c) if c == "me"
        ));
    }

    #[test]
    fn test_compact_vocab() {
        let ctx = ParsedContext::parse(None, &json!("https://schema.org")).unwrap();

        assert_eq!(compact("https://schema.org/name", &ctx), "name");
    }

    #[test]
    fn a_vocab_ending_in_neither_slash_nor_hash_compacts() {
        let ctx = ParsedContext::parse(None, &json!({"@vocab": "urn:ex:", "p": "urn:p:"})).unwrap();
        assert_eq!(compact("urn:ex:name", &ctx), "name");
        // A prefix still needs to end in `/` or `#`.
        assert_eq!(compact("urn:p:x", &ctx), "urn:p:x");
    }

    #[test]
    fn a_vocab_name_holding_a_colon_stays_whole() {
        let ctx = ParsedContext::parse(None, &json!({"@vocab": "urn:ex:"})).unwrap();
        assert_eq!(compact("urn:ex:a:b", &ctx), "urn:ex:a:b");
        let f = compact_fn(&ctx, None);
        assert_eq!(f("urn:ex:a:b"), "urn:ex:a:b");
        assert_eq!(f("urn:ex:a"), "a");
    }

    #[test]
    fn an_equal_base_leaves_properties_to_the_vocab() {
        for ns in ["http://example.org/ns#", "urn:ex:"] {
            let ctx = ParsedContext::parse(None, &json!({"@base": ns, "@vocab": ns})).unwrap();
            let name = format!("{ns}name");
            assert_eq!(compact(&name, &ctx), "name", "under {ns}");
            assert_eq!(crate::expand::iri("name", &ctx, true), name);
        }
        // An id still compacts by the base.
        let ns = "http://example.org/ns#";
        let ctx = ParsedContext::parse(None, &json!({"@base": ns, "@vocab": ns})).unwrap();
        assert_eq!(compact_id("http://example.org/ns#alice", &ctx), "#alice");
    }

    /// The base and the vocabulary are namespaces, not terms: an IRI equal to
    /// one compacts as any IRI under it does, or stays whole.
    #[test]
    fn an_iri_equal_to_the_base_or_vocab_reads_back() {
        let ctx = ParsedContext::parse(None, &json!({"@vocab": "urn:ex:"})).unwrap();
        assert_eq!(compact("urn:ex:", &ctx), "urn:ex:");
        for (base, compacted) in [
            ("http://example.org/dir/", ""),
            ("http://example.org/ns#", "#"),
            ("http://example.org/doc", "http://example.org/doc"),
        ] {
            let ctx = ParsedContext::parse(None, &json!({"@base": base})).unwrap();
            assert_eq!(compact_id(base, &ctx), compacted, "under {base}");
            assert_eq!(crate::expand::iri(compacted, &ctx, false), base);
        }
    }

    #[test]
    fn tracking_names_the_vocab_and_base_by_their_keywords() {
        let ctx = ParsedContext::parse(
            None,
            &json!({"@vocab": "urn:ex:", "@base": "http://example.org/"}),
        )
        .unwrap();
        let used = Arc::new(Mutex::new(HashMap::new()));
        let f = compact_fn(&ctx, Some(used.clone()));
        assert_eq!(f("urn:ex:name"), "name");
        assert_eq!(f("http://example.org/alice"), "alice");
        let used = used.lock().unwrap();
        assert_eq!(used.get("@vocab").map(String::as_str), Some("urn:ex:"));
        assert_eq!(
            used.get("@base").map(String::as_str),
            Some("http://example.org/")
        );
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

    /// An `@id` compacted against `@base` expands back to the same IRI; one
    /// that would not stays whole.
    #[test]
    fn base_compaction_expands_back() {
        let cases = [
            ("http://b.org/ns#", "http://b.org/ns#alice", "#alice"),
            ("http://b.org/dir/", "http://b.org/dir/alice", "alice"),
            ("http://b.org/dir/", "http://b.org/dir/a/b?q#f", "a/b?q#f"),
            (
                "http://b.org/dir/",
                "http://b.org/dir//x",
                "http://b.org/dir//x",
            ),
            (
                "http://b.org/dir/",
                "http://b.org/dir/a:b",
                "http://b.org/dir/a:b",
            ),
            (
                "http://b.org/dir/",
                "http://b.org/dir/../x",
                "http://b.org/dir/../x",
            ),
        ];
        for (base, iri, compacted) in cases {
            let ctx = ParsedContext::parse(None, &json!({"@base": base})).unwrap();
            assert_eq!(compact_id(iri, &ctx), compacted, "{iri} under {base}");
            assert_eq!(crate::expand::iri(compacted, &ctx, false), iri);
        }
    }
}
