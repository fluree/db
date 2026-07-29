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

/// Why a string is not usable as an RDF IRI term.
///
/// Indices are byte offsets into the *checked* string — which, for a Turtle
/// IRI, is the value after `\uXXXX` expansion and base resolution, not the
/// source text. They locate the character within the IRI for a human-readable
/// message; they do NOT map back to a source offset, because escape expansion
/// and resolution both change the length. A caller reporting a position should
/// use the IRI token's own start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IriViolation {
    /// A character the IRI grammar excludes appears in the IRI.
    ForbiddenChar {
        /// Byte offset of the character within the checked IRI.
        index: usize,
        /// The offending character.
        ch: char,
    },
    /// The IRI has no scheme, so it is a relative reference rather than an
    /// IRI. RDF terms are absolute IRIs; a relative one denotes nothing on
    /// its own.
    NotAbsolute,
}

impl std::fmt::Display for IriViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IriViolation::ForbiddenChar { index, ch } => write!(
                f,
                "IRI contains {} at position {index}, which is not allowed in an IRI",
                describe_char(*ch)
            ),
            IriViolation::NotAbsolute => {
                f.write_str("IRI is relative — an RDF term must be an absolute IRI with a scheme")
            }
        }
    }
}

/// Spell a character so the message is readable when it is invisible.
fn describe_char(ch: char) -> String {
    match ch {
        ' ' => "a space".to_string(),
        '\t' => "a tab".to_string(),
        '\n' => "a newline".to_string(),
        '\r' => "a carriage return".to_string(),
        c if (c as u32) < 0x20 => format!("the control character U+{:04X}", c as u32),
        c => format!("'{c}' (U+{:04X})", c as u32),
    }
}

/// Is this character excluded from an IRI?
///
/// The set is the one the Turtle / N-Triples `IRIREF` production excludes:
/// `[^#x00-#x20<>"{}|^`\]`. Deliberately exactly that set and no more —
/// over-rejection is the standing hazard for a conformance parser, and the
/// grammar is the thing the W3C suites actually test. Notably U+007F is NOT
/// excluded here, because the production does not exclude it.
#[inline]
#[must_use]
pub const fn is_forbidden_iri_char(ch: char) -> bool {
    matches!(
        ch,
        '\u{0}'..='\u{20}' | '<' | '>' | '"' | '{' | '}' | '|' | '^' | '`' | '\\'
    )
}

/// Check a **resolved** IRI for use as an RDF term.
///
/// Turtle's `IRIREF` production already excludes the forbidden characters
/// from the *source* text, so a raw `<http://ex/a b>` never lexes. What it
/// does not constrain is what a `\uXXXX` escape expands to, nor what base
/// resolution produces — and RDF requires the *result* to be an IRI. That is
/// exactly the `turtle-eval-bad-01/02/03` class: the document lexes, and the
/// term it denotes is not an IRI.
///
/// So this runs on the final string, after expansion and resolution. It is a
/// scan with no allocation on the passing path.
///
/// # Example
///
/// ```
/// use fluree_vocab::iri::{iri_violation, IriViolation};
///
/// assert_eq!(iri_violation("http://example.org/ok"), None);
///
/// // What `<http://example.org/\u0020>` expands to.
/// assert!(matches!(
///     iri_violation("http://example.org/ "),
///     Some(IriViolation::ForbiddenChar { ch: ' ', .. })
/// ));
///
/// assert_eq!(iri_violation("not-absolute"), Some(IriViolation::NotAbsolute));
/// ```
#[must_use]
pub fn iri_violation(iri: &str) -> Option<IriViolation> {
    if let Some((index, ch)) = iri.char_indices().find(|(_, c)| is_forbidden_iri_char(*c)) {
        return Some(IriViolation::ForbiddenChar { index, ch });
    }
    if !is_absolute_iri(iri) {
        return Some(IriViolation::NotAbsolute);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // =====================================================================
    // Term validation (H-8)
    // =====================================================================

    #[test]
    fn the_three_w3c_eval_bad_cases() {
        // turtle-eval-bad-01/02/03 are `<http://.../\u0020>`, `\u003C`,
        // `\u003E`. The document LEXES — the escape is legal source — and
        // what it expands to is not an IRI. These are the expanded values.
        for (iri, ch) in [
            ("http://www.w3.org/2013/TurtleTests/ ", ' '),
            ("http://www.w3.org/2013/TurtleTests/<", '<'),
            ("http://www.w3.org/2013/TurtleTests/>", '>'),
        ] {
            assert_eq!(
                iri_violation(iri),
                Some(IriViolation::ForbiddenChar { index: 35, ch }),
                "{iri:?}"
            );
        }
    }

    #[test]
    fn accepts_ordinary_iris_including_non_ascii() {
        for iri in [
            "http://example.org/",
            "http://example.org/a/b?q=1#f",
            "urn:uuid:1234",
            "mailto:a@b.c",
            "did:key:z6Mk",
            // RFC 3987 is an IRI spec, not a URI spec: non-ASCII is the point.
            "http://example.org/\u{e9}",
            "http://example.org/\u{4e2d}\u{6587}",
            // U+007F is NOT excluded by the IRIREF production, so we do not
            // exclude it either — matching the grammar exactly is what keeps
            // this from over-rejecting.
            "http://example.org/\u{7f}",
        ] {
            assert_eq!(iri_violation(iri), None, "{iri:?}");
        }
    }

    #[test]
    fn rejects_every_character_the_iriref_production_excludes() {
        for ch in [
            '<', '>', '"', '{', '}', '|', '^', '`', '\\', ' ', '\t', '\n', '\r', '\u{0}', '\u{1f}',
            '\u{20}',
        ] {
            assert!(is_forbidden_iri_char(ch), "{ch:?} must be forbidden");
            let iri = format!("http://example.org/{ch}");
            assert_eq!(
                iri_violation(&iri),
                Some(IriViolation::ForbiddenChar { index: 19, ch }),
                "{iri:?}"
            );
        }
    }

    #[test]
    fn a_relative_reference_is_not_a_term() {
        // After resolution an RDF term must be absolute; a leftover relative
        // reference denotes nothing.
        for iri in ["not-absolute", "/abs/path", "#frag", "", "1x:y"] {
            assert_eq!(
                iri_violation(iri),
                Some(IriViolation::NotAbsolute),
                "{iri:?}"
            );
        }
    }

    #[test]
    fn forbidden_characters_are_reported_before_absoluteness() {
        // A relative reference that ALSO holds a space should name the
        // space: it is the more specific, more actionable complaint.
        assert!(matches!(
            iri_violation("rel ative"),
            Some(IriViolation::ForbiddenChar { ch: ' ', .. })
        ));
    }

    #[test]
    fn violation_messages_spell_invisible_characters() {
        let msg = iri_violation("http://example.org/ ").unwrap().to_string();
        assert!(msg.contains("a space"), "{msg}");
        let msg = iri_violation("http://example.org/\t").unwrap().to_string();
        assert!(msg.contains("a tab"), "{msg}");
        let msg = iri_violation("http://example.org/\u{1}")
            .unwrap()
            .to_string();
        assert!(msg.contains("U+0001"), "{msg}");
        let msg = iri_violation("http://example.org/<").unwrap().to_string();
        assert!(msg.contains("'<'"), "{msg}");
        let msg = iri_violation("relative").unwrap().to_string();
        assert!(msg.contains("absolute"), "{msg}");
    }

    #[test]
    fn indices_are_byte_offsets_into_the_checked_string() {
        // Multi-byte characters before the offender: the index must be a byte
        // offset so a caller can slice with it without panicking.
        let iri = "http://example.org/\u{e9}<";
        let Some(IriViolation::ForbiddenChar { index, ch }) = iri_violation(iri) else {
            panic!("expected a forbidden char");
        };
        assert_eq!(ch, '<');
        assert_eq!(index, 21, "19 ASCII bytes + 2 for the e-acute");
        assert_eq!(&iri[index..], "<");
    }

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
