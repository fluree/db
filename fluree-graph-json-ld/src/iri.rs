/// Parse a compact IRI like "schema:name" into (prefix, suffix).
/// Returns None if not a valid compact IRI.
///
/// A compact IRI has the form prefix:suffix where:
/// - prefix does not contain : or /
/// - suffix does not start with //
///
/// Special case: ":suffix" is valid with prefix = ":"
pub fn parse_prefix(s: &str) -> Option<(String, String)> {
    // Special case: prefix is ":"
    if s.starts_with(':') && s.len() > 1 {
        let suffix = &s[1..];
        return Some((":".to_string(), suffix.to_string()));
    }

    // Find the first colon
    if let Some(colon_pos) = s.find(':') {
        let prefix = &s[..colon_pos];
        let suffix = &s[colon_pos + 1..];

        // Prefix must not contain / (would indicate absolute IRI like http://)
        if prefix.contains('/') {
            return None;
        }

        // Suffix must not start with // (would indicate absolute IRI)
        if suffix.starts_with("//") {
            return None;
        }

        // Prefix must not be empty (except for special ":" case handled above)
        if prefix.is_empty() {
            return None;
        }

        return Some((prefix.to_string(), suffix.to_string()));
    }

    None
}

/// Returns true if string contains a colon (looks like an IRI or compact IRI)
pub fn any_iri(s: &str) -> bool {
    s.contains(':')
}

/// Disposition of an unresolved IRI string that wasn't matched by `@context`.
///
/// Used by the strict compact-IRI guard to decide whether an unresolved value
/// should be accepted as a legitimate absolute IRI or rejected as a likely
/// user error (missing `@context` prefix).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnresolvedIriDisposition {
    /// Hierarchical absolute IRI — suffix after first `:` starts with `//`
    /// (e.g. `http://example.org`, `https://schema.org/`)
    AllowAbsolute,
    /// Prefix matches a known non-hierarchical scheme
    /// (e.g. `urn:isbn:...`, `did:example:...`, `mailto:user@...`)
    AllowKnownScheme,
    /// Looks like a compact IRI with an undefined prefix
    /// (e.g. `ex:Person` when `ex` is not in `@context`)
    RejectLikelyCompact { prefix: String },
    /// No colon found, or is a blank node — no opinion
    NoOpinion,
}

/// Known non-hierarchical URI schemes accepted without `@context` resolution.
/// Compared case-insensitively (URI schemes are ASCII-case-insensitive per RFC 3986).
const KNOWN_SCHEMES: &[&str] = &[
    "urn", "did", "mailto", "tel", "data", "ipfs", "ipns", "geo", "blob", "magnet", "fluree",
];

/// Check whether an unresolved IRI (one that was NOT matched by `@context`)
/// looks like it was intended as a compact IRI with a missing prefix definition.
///
/// Call this only on values that failed context resolution.
/// Variables (`?x`) should be filtered out before calling.
///
/// Fast path: a single `split_once(':')` plus a few prefix comparisons.
pub fn check_unresolved_iri(s: &str) -> UnresolvedIriDisposition {
    match s.split_once(':') {
        None => UnresolvedIriDisposition::NoOpinion,
        Some((prefix, suffix)) => {
            // Blank nodes (_:b0) bypass the heuristic
            if prefix == "_" {
                return UnresolvedIriDisposition::NoOpinion;
            }
            if suffix.starts_with("//") {
                UnresolvedIriDisposition::AllowAbsolute
            } else if KNOWN_SCHEMES.iter().any(|s| s.eq_ignore_ascii_case(prefix)) {
                UnresolvedIriDisposition::AllowKnownScheme
            } else {
                UnresolvedIriDisposition::RejectLikelyCompact {
                    prefix: prefix.to_string(),
                }
            }
        }
    }
}

/// Returns true if the IRI is absolute (has an RFC 3986 scheme).
///
/// An absolute IRI starts with a scheme: `ALPHA *( ALPHA / DIGIT / "+" / "-" / "." ) ":"`.
/// This handles all schemes (http, https, urn, did, mailto, ftp, ipfs, etc.)
/// without maintaining a hardcoded list.
pub fn is_absolute(iri: &str) -> bool {
    if let Some(colon_pos) = iri.find(':') {
        let scheme = &iri[..colon_pos];
        !scheme.is_empty()
            && scheme.as_bytes()[0].is_ascii_alphabetic()
            && scheme
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'+' || b == b'-' || b == b'.')
    } else {
        false
    }
}

/// Ensure IRI ends with '/' or '#'
pub fn add_trailing_slash(iri: &str) -> String {
    if iri.ends_with('/') || iri.ends_with('#') {
        iri.to_string()
    } else {
        format!("{iri}/")
    }
}

/// Join base IRI with relative IRI
pub fn join(base: &str, relative: &str) -> String {
    if relative.starts_with('#') {
        // Fragment: append to base
        format!("{}{}", base.trim_end_matches('/'), relative)
    } else if is_absolute(relative) {
        // Already absolute
        relative.to_string()
    } else {
        // Relative: ensure base ends with / and append
        let base = if base.ends_with('/') || base.ends_with('#') {
            base.to_string()
        } else {
            format!("{base}/")
        };
        format!("{base}{relative}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_prefix() {
        assert_eq!(
            parse_prefix("schema:name"),
            Some(("schema".to_string(), "name".to_string()))
        );
        assert_eq!(
            parse_prefix("ex:Person"),
            Some(("ex".to_string(), "Person".to_string()))
        );
        assert_eq!(
            parse_prefix(":localName"),
            Some((":".to_string(), "localName".to_string()))
        );

        // Not compact IRIs
        assert_eq!(parse_prefix("http://example.org"), None);
        assert_eq!(parse_prefix("https://schema.org/"), None);
        assert_eq!(parse_prefix("noColon"), None);
    }

    #[test]
    fn test_any_iri() {
        assert!(any_iri("schema:name"));
        assert!(any_iri("http://example.org"));
        assert!(!any_iri("localName"));
    }

    #[test]
    fn test_is_absolute() {
        assert!(is_absolute("http://example.org"));
        assert!(is_absolute("https://schema.org/"));
        assert!(is_absolute("urn:isbn:0451450523"));
        assert!(is_absolute("did:example:123"));
        assert!(is_absolute("file:///path/to/file"));
        assert!(is_absolute("mailto:user@example.com"));
        assert!(is_absolute("ftp://ftp.example.org"));
        assert!(is_absolute("ipfs:QmHash123"));
        // Compact IRIs look like schemes but are valid — is_absolute returns true
        // for anything with a valid scheme-like prefix. The distinction between
        // compact IRIs and absolute IRIs is handled by parse_prefix() which
        // rejects patterns like "http://..." (suffix starts with "//").
        assert!(is_absolute("schema:name"));
        assert!(!is_absolute("localName"));
        assert!(!is_absolute(""));
    }

    #[test]
    fn test_add_trailing_slash() {
        assert_eq!(
            add_trailing_slash("http://example.org"),
            "http://example.org/"
        );
        assert_eq!(
            add_trailing_slash("http://example.org/"),
            "http://example.org/"
        );
        assert_eq!(
            add_trailing_slash("http://example.org#"),
            "http://example.org#"
        );
    }

    #[test]
    fn test_check_unresolved_iri_absolute() {
        assert_eq!(
            check_unresolved_iri("http://example.org/Person"),
            UnresolvedIriDisposition::AllowAbsolute
        );
        assert_eq!(
            check_unresolved_iri("https://schema.org/name"),
            UnresolvedIriDisposition::AllowAbsolute
        );
        assert_eq!(
            check_unresolved_iri("ftp://ftp.example.org/file"),
            UnresolvedIriDisposition::AllowAbsolute
        );
    }

    #[test]
    fn test_check_unresolved_iri_known_schemes() {
        assert_eq!(
            check_unresolved_iri("urn:isbn:0451450523"),
            UnresolvedIriDisposition::AllowKnownScheme
        );
        assert_eq!(
            check_unresolved_iri("did:example:123"),
            UnresolvedIriDisposition::AllowKnownScheme
        );
        assert_eq!(
            check_unresolved_iri("mailto:user@example.com"),
            UnresolvedIriDisposition::AllowKnownScheme
        );
        assert_eq!(
            check_unresolved_iri("tel:+1-201-555-0123"),
            UnresolvedIriDisposition::AllowKnownScheme
        );
        assert_eq!(
            check_unresolved_iri("data:text/plain;base64,SGVsbG8="),
            UnresolvedIriDisposition::AllowKnownScheme
        );
        assert_eq!(
            check_unresolved_iri("ipfs:QmHash123"),
            UnresolvedIriDisposition::AllowKnownScheme
        );
        assert_eq!(
            check_unresolved_iri("geo:37.786971,-122.399677"),
            UnresolvedIriDisposition::AllowKnownScheme
        );
    }

    #[test]
    fn test_check_unresolved_iri_reject_compact() {
        assert_eq!(
            check_unresolved_iri("ex:Person"),
            UnresolvedIriDisposition::RejectLikelyCompact {
                prefix: "ex".to_string()
            }
        );
        assert_eq!(
            check_unresolved_iri("schema:name"),
            UnresolvedIriDisposition::RejectLikelyCompact {
                prefix: "schema".to_string()
            }
        );
        assert_eq!(
            check_unresolved_iri("foo:bar"),
            UnresolvedIriDisposition::RejectLikelyCompact {
                prefix: "foo".to_string()
            }
        );
        assert_eq!(
            check_unresolved_iri("f:commitMsg"),
            UnresolvedIriDisposition::RejectLikelyCompact {
                prefix: "f".to_string()
            }
        );
    }

    #[test]
    fn test_check_unresolved_iri_fluree_addresses() {
        assert_eq!(
            check_unresolved_iri("fluree:file:///path/to/db"),
            UnresolvedIriDisposition::AllowKnownScheme
        );
        assert_eq!(
            check_unresolved_iri("fluree:s3://bucket/key"),
            UnresolvedIriDisposition::AllowKnownScheme
        );
        assert_eq!(
            check_unresolved_iri("fluree:commit-storage:s3://db/commit/abc.fcv2"),
            UnresolvedIriDisposition::AllowKnownScheme
        );
    }

    #[test]
    fn test_check_unresolved_iri_case_insensitive() {
        assert_eq!(
            check_unresolved_iri("URN:isbn:0451450523"),
            UnresolvedIriDisposition::AllowKnownScheme
        );
        assert_eq!(
            check_unresolved_iri("DID:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"),
            UnresolvedIriDisposition::AllowKnownScheme
        );
        assert_eq!(
            check_unresolved_iri("Mailto:user@example.com"),
            UnresolvedIriDisposition::AllowKnownScheme
        );
    }

    #[test]
    fn test_check_unresolved_iri_no_opinion() {
        // No colon
        assert_eq!(
            check_unresolved_iri("localName"),
            UnresolvedIriDisposition::NoOpinion
        );
        // Blank nodes
        assert_eq!(
            check_unresolved_iri("_:b0"),
            UnresolvedIriDisposition::NoOpinion
        );
        assert_eq!(
            check_unresolved_iri("_:genid1"),
            UnresolvedIriDisposition::NoOpinion
        );
    }

    #[test]
    fn test_join() {
        assert_eq!(
            join("http://example.org/", "name"),
            "http://example.org/name"
        );
        assert_eq!(
            join("http://example.org", "name"),
            "http://example.org/name"
        );
        assert_eq!(
            join("http://example.org/", "#fragment"),
            "http://example.org#fragment"
        );
    }
}
