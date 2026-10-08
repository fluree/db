//! IRI reference resolution (RFC 3986 §5).
//!
//! Shared by every surface that must resolve a (potentially relative) IRI
//! reference against a base IRI: the Turtle/TriG parser (`@base` / `BASE`
//! directives) and the SPARQL lowering (`BASE` prologue declaration —
//! constant `GRAPH` IRIs, `FROM`/`FROM NAMED` clause IRIs, relative
//! `PREFIX` namespaces, `IRI()`/`URI()` constant folding, and any other
//! position where lowering materializes a constant IRI).
//!
//! Resolution is a pure string transform — no I/O, no allocation beyond the
//! output string — so it is safe to run at parse/lower/prepare time.

/// Does the reference carry a valid scheme (RFC 3986 §3.1), making it an
/// absolute IRI reference that resolution must return verbatim?
///
/// `scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )`
///
/// Note that prefixed-name look-alikes (`ex:local`) satisfy this grammar;
/// callers that support prefixed names must expand them *before* asking
/// whether the result is absolute.
#[inline]
#[must_use]
pub fn is_absolute_iri(reference: &str) -> bool {
    if let Some(colon_pos) = reference.find(':') {
        let potential_scheme = &reference[..colon_pos];
        !potential_scheme.is_empty()
            && potential_scheme
                .chars()
                .next()
                .unwrap()
                .is_ascii_alphabetic()
            && potential_scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.')
    } else {
        false
    }
}

/// Resolve a potentially relative IRI reference against a base (RFC 3986 §5).
///
/// Absolute references (those with a valid scheme) are returned verbatim,
/// fragment included — resolution does not apply. Relative references are
/// resolved against `base` per the §5.2.2 transform algorithm, including
/// dot-segment removal and the fragment rules (the resolved fragment is
/// always the *reference's* fragment; the base's fragment is never
/// inherited).
#[must_use]
pub fn resolve_iri(base: &str, reference: &str) -> String {
    if is_absolute_iri(reference) {
        return reference.to_string();
    }

    // RFC 3986 §5.2.1: split the reference into its fragment and everything
    // before it. The fragment is the portion after the FIRST `#`. Per
    // §5.2.2 the resolved fragment is ALWAYS the reference's fragment and is
    // never inherited from the base, so scheme/authority/path/query are
    // resolved against the fragment-less portion and the reference fragment
    // is re-attached during recomposition (§5.3).
    let (ref_no_fragment, ref_fragment) = match reference.find('#') {
        Some(pos) => (&reference[..pos], Some(&reference[pos + 1..])),
        None => (reference, None),
    };

    let (base_scheme, base_authority, base_path, base_query) = parse_iri_components(base);

    let (scheme, authority, path, query) = if ref_no_fragment.is_empty() {
        // Same-document reference (`<>` or `<#frag>`): the reference has an
        // empty path and no query, so the target inherits the base path and
        // query (RFC 3986 §5.2.2). The base's own fragment is dropped because
        // `parse_iri_components` never returns it.
        (
            base_scheme.to_string(),
            base_authority.map(std::string::ToString::to_string),
            base_path.to_string(),
            base_query.map(std::string::ToString::to_string),
        )
    } else if let Some(rest) = ref_no_fragment.strip_prefix("//") {
        let (ref_authority, ref_path, ref_query) = parse_hier_part(rest);
        (
            base_scheme.to_string(),
            Some(ref_authority),
            remove_dot_segments(&ref_path),
            ref_query,
        )
    } else if ref_no_fragment.starts_with('/') {
        let (ref_path, ref_query) = split_path_query(ref_no_fragment);
        (
            base_scheme.to_string(),
            base_authority.map(std::string::ToString::to_string),
            remove_dot_segments(ref_path),
            ref_query.map(std::string::ToString::to_string),
        )
    } else if let Some(query_rest) = ref_no_fragment.strip_prefix('?') {
        (
            base_scheme.to_string(),
            base_authority.map(std::string::ToString::to_string),
            base_path.to_string(),
            Some(query_rest.to_string()),
        )
    } else {
        let (ref_path, ref_query) = split_path_query(ref_no_fragment);
        let merged = if base_authority.is_some() && base_path.is_empty() {
            format!("/{ref_path}")
        } else {
            let base_dir = match base_path.rfind('/') {
                Some(pos) => &base_path[..=pos],
                None => "",
            };
            format!("{base_dir}{ref_path}")
        };
        (
            base_scheme.to_string(),
            base_authority.map(std::string::ToString::to_string),
            remove_dot_segments(&merged),
            ref_query.map(std::string::ToString::to_string),
        )
    };

    let mut result = scheme;
    result.push(':');
    if let Some(auth) = authority {
        result.push_str("//");
        result.push_str(&auth);
    }
    result.push_str(&path);
    if let Some(q) = query {
        result.push('?');
        result.push_str(&q);
    }
    if let Some(fragment) = ref_fragment {
        result.push('#');
        result.push_str(fragment);
    }

    result
}

/// Split an IRI into `(scheme, authority, path, query)`. The fragment is
/// deliberately dropped (RFC 3986 §5.2.2 never inherits the base fragment).
fn parse_iri_components(iri: &str) -> (&str, Option<&str>, &str, Option<&str>) {
    let (scheme, rest) = match iri.find(':') {
        Some(pos) => (&iri[..pos], &iri[pos + 1..]),
        None => return ("", None, iri, None),
    };

    let (authority, path_query) = if let Some(after_slashes) = rest.strip_prefix("//") {
        let auth_end = after_slashes
            .find(['/', '?', '#'])
            .unwrap_or(after_slashes.len());
        (Some(&after_slashes[..auth_end]), &after_slashes[auth_end..])
    } else {
        (None, rest)
    };

    let (path, query) = split_path_query(path_query);

    (scheme, authority, path, query)
}

fn parse_hier_part(s: &str) -> (String, String, Option<String>) {
    let auth_end = s.find(['/', '?', '#']).unwrap_or(s.len());
    let authority = s[..auth_end].to_string();
    let rest = &s[auth_end..];

    let (path, query) = split_path_query(rest);
    (
        authority,
        path.to_string(),
        query.map(std::string::ToString::to_string),
    )
}

fn split_path_query(s: &str) -> (&str, Option<&str>) {
    let s = match s.find('#') {
        Some(pos) => &s[..pos],
        None => s,
    };

    match s.find('?') {
        Some(pos) => (&s[..pos], Some(&s[pos + 1..])),
        None => (s, None),
    }
}

fn remove_dot_segments(path: &str) -> String {
    let mut output: Vec<&str> = Vec::new();

    for segment in path.split('/') {
        match segment {
            "." => {}
            ".." => {
                output.pop();
            }
            s => {
                output.push(s);
            }
        }
    }

    let mut result = output.join("/");
    // RFC 3986 §5.2.4: a FINAL `.` / `..` segment resolves to the directory
    // itself, leaving a trailing slash — `/def/.` → `/def/`, `/def/..` →
    // `/` — which the segment loop above drops (PR-1454 review; W3C
    // IRI-resolution-08 fixtures). Bare `.` / `..` (no leading slash, empty
    // output) stay empty per the algorithm's rule 2D.
    if matches!(path.rsplit('/').next(), Some("." | ".."))
        && (!result.is_empty() || path.starts_with('/'))
        && !result.ends_with('/')
    {
        result.push('/');
    }
    if path.starts_with('/') && !result.starts_with('/') {
        format!("/{result}")
    } else {
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absolute_references_returned_verbatim() {
        assert!(is_absolute_iri("http://example.org/x"));
        assert!(is_absolute_iri("urn:uuid:1234"));
        assert!(is_absolute_iri("did:key:z6Mk"));
        assert!(is_absolute_iri("mailto:a@b.c"));
        assert!(!is_absolute_iri("data-g1.ttl"));
        assert!(!is_absolute_iri("#frag"));
        assert!(!is_absolute_iri(""));
        assert!(!is_absolute_iri("/abs/path"));
        // Leading digit → not a valid scheme.
        assert!(!is_absolute_iri("1x:y"));

        assert_eq!(
            resolve_iri("http://example.org/base/", "urn:uuid:1234"),
            "urn:uuid:1234"
        );
        assert_eq!(
            resolve_iri("http://example.org/base/", "http://other.org/#f"),
            "http://other.org/#f"
        );
    }

    #[test]
    fn sibling_file_reference_replaces_last_segment() {
        // The graph-exist / dataset-clause case: <data-g1.ttl> named from a
        // query document resolves to a sibling of the document.
        assert_eq!(
            resolve_iri(
                "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/graph/graph-empty-exist.rq",
                "data-g1.ttl"
            ),
            "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/graph/data-g1.ttl"
        );
    }

    #[test]
    fn final_dot_segments_keep_the_trailing_slash() {
        // RFC 3986 §5.2.4 / W3C rdf-tests IRI-resolution-08 (s295–s300): a
        // final `.` / `..` segment resolves to the DIRECTORY — the output
        // ends in `/`. The old segment loop dropped it (PR-1454 review).
        let base = "http://abc/def/ghi";
        for (reference, expected) in [
            (".", "http://abc/def/"),
            (".?a=b", "http://abc/def/?a=b"),
            (".#a=b", "http://abc/def/#a=b"),
            ("..", "http://abc/"),
            ("..?a=b", "http://abc/?a=b"),
            ("..#a=b", "http://abc/#a=b"),
        ] {
            assert_eq!(
                resolve_iri(base, reference),
                expected,
                "resolve_iri({base:?}, {reference:?})"
            );
        }
        // Mid-path dot-segments keep collapsing without a trailing slash.
        assert_eq!(resolve_iri(base, "../x"), "http://abc/x");
        assert_eq!(resolve_iri(base, "./x"), "http://abc/def/x");
        // A root-consuming `..` still yields the root.
        assert_eq!(resolve_iri("http://abc/def", ".."), "http://abc/");
    }

    #[test]
    fn empty_and_colon_path_segments_survive_resolution() {
        // W3C rdf-tests IRI-resolution-08 (s301–s306): empty path segments
        // (`//de//ghi`) and colon-in-path segments (`d:f`) are ordinary
        // segments — merging and dot-segment removal must not collapse or
        // misparse them.
        let double = "http://ab//de//ghi";
        assert_eq!(resolve_iri(double, "xyz"), "http://ab//de//xyz");
        assert_eq!(resolve_iri(double, "./xyz"), "http://ab//de//xyz");
        assert_eq!(resolve_iri(double, "../xyz"), "http://ab//de/xyz");

        let colon = "http://abc/d:f/ghi";
        assert_eq!(resolve_iri(colon, "xyz"), "http://abc/d:f/xyz");
        assert_eq!(resolve_iri(colon, "./xyz"), "http://abc/d:f/xyz");
        assert_eq!(resolve_iri(colon, "../xyz"), "http://abc/xyz");
    }

    #[test]
    fn empty_reference_resolves_to_base_without_fragment() {
        assert_eq!(
            resolve_iri("http://example.org/x/", ""),
            "http://example.org/x/"
        );
        assert_eq!(
            resolve_iri("http://example.org/path#frag", ""),
            "http://example.org/path"
        );
    }

    #[test]
    fn fragment_reference_keeps_base_path() {
        assert_eq!(
            resolve_iri("http://example.org/x/", "#p"),
            "http://example.org/x/#p"
        );
        assert_eq!(
            resolve_iri("http://example.org/path#old", "#new"),
            "http://example.org/path#new"
        );
    }

    #[test]
    fn relative_path_against_directory_base() {
        assert_eq!(
            resolve_iri("http://example.org/x/", "x"),
            "http://example.org/x/x"
        );
        assert_eq!(
            resolve_iri("http://example.org/a/b/c", "d/e"),
            "http://example.org/a/b/d/e"
        );
    }

    #[test]
    fn dot_segments_removed() {
        assert_eq!(
            resolve_iri("http://example.org/a/b/c", "../d"),
            "http://example.org/a/d"
        );
        assert_eq!(
            resolve_iri("http://example.org/a/b/c", "./d"),
            "http://example.org/a/b/d"
        );
    }

    #[test]
    fn absolute_path_and_network_path_references() {
        assert_eq!(
            resolve_iri("http://example.org/a/b", "/c/d"),
            "http://example.org/c/d"
        );
        assert_eq!(
            resolve_iri("http://example.org/a/b", "//other.org/c"),
            "http://other.org/c"
        );
    }

    #[test]
    fn query_only_reference_keeps_base_path() {
        assert_eq!(
            resolve_iri("http://example.org/a/b", "?q=1"),
            "http://example.org/a/b?q=1"
        );
    }

    #[test]
    fn reference_query_and_fragment_survive() {
        assert_eq!(
            resolve_iri("http://example.org/a/", "x?q=1#f"),
            "http://example.org/a/x?q=1#f"
        );
    }
}
