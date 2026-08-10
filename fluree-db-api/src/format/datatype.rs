//! Well-known datatype constants and utilities
//!
//! This module provides datatype IRI constants and helper functions
//! for determining formatting behavior based on datatype.

// Re-export vocabulary constants from the vocab crate for convenience
pub use fluree_vocab::fluree;
pub use fluree_vocab::rdf;
pub use fluree_vocab::xsd;

/// JSON-LD internal types
pub mod jsonld {
    /// @json - JSON literal
    pub const JSON: &str = "@json";
}

/// Check if a datatype is "inferable" from the JSON value.
///
/// SPARQL 1.1 JSON Results format allows omitting the datatype for types
/// that can be inferred from the JSON representation:
/// - xsd:string - plain string in JSON
/// - xsd:integer/xsd:long - whole number in JSON
/// - xsd:double/xsd:decimal - floating point in JSON
/// - xsd:boolean - true/false in JSON
/// - fluree:vector - JSON array of floats
///
/// These types are automatically inferred by JSON parsers.
pub fn is_inferable_datatype(dt_iri: &str) -> bool {
    matches!(
        dt_iri,
        xsd::STRING
            | xsd::LONG
            | xsd::INTEGER
            | xsd::DOUBLE
            | xsd::BOOLEAN
            | xsd::DECIMAL
            | fluree::EMBEDDING_VECTOR
            // Some code paths may provide already-compacted datatype strings
            // (e.g., "xsd:string") instead of full IRIs. Treat these the same
            // as their full-IRI counterparts for JSON-LD output.
            | "xsd:string"
            | "xsd:long"
            | "xsd:integer"
            | "xsd:double"
            | "xsd:boolean"
            | "xsd:decimal"
            | "f:embeddingVector"
    )
}

/// Whether a **string-backed** literal may be serialized without its `datatype`.
///
/// `w3c_strict` selects the rule for the W3C result serializations (SPARQL
/// Results JSON, CSV, TSV — see [`crate::FormatterConfig::absolute_iris`]).
/// There, only `xsd:string` may be dropped: those formats encode every value as
/// text, so nothing about the datatype is recoverable from the serialized form,
/// and SPARQL Results JSON §3.2.2 defines a literal with neither `datatype` nor
/// `xml:lang` as a *simple literal* — i.e. an `xsd:string`. Dropping the tag off
/// anything else changes which RDF term the document denotes: `STRDT("2",
/// xsd:integer)` would come back as `"2"^^xsd:string`.
///
/// Otherwise the looser [`is_inferable_datatype`] rule applies, which is sound
/// for the JSON-LD-flavored outputs: those render the value as a native JSON
/// number / boolean, so the datatype really is recoverable from the JSON.
pub fn omit_datatype_for_string_literal(dt_iri: &str, w3c_strict: bool) -> bool {
    if w3c_strict {
        matches!(dt_iri, xsd::STRING | "xsd:string")
    } else {
        is_inferable_datatype(dt_iri)
    }
}

// Note: is_reference_datatype is NOT needed - Binding::Sid already indicates references.
// The Rust invariant (Binding::Lit never contains FlakeValue::Ref) eliminates the need
// for datatype checks to identify references.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_inferable_datatype() {
        // Inferable types
        assert!(is_inferable_datatype(xsd::STRING));
        assert!(is_inferable_datatype(xsd::LONG));
        assert!(is_inferable_datatype(xsd::INTEGER));
        assert!(is_inferable_datatype(xsd::DOUBLE));
        assert!(is_inferable_datatype(xsd::BOOLEAN));
        assert!(is_inferable_datatype(xsd::DECIMAL));

        assert!(is_inferable_datatype(fluree::EMBEDDING_VECTOR));

        // Non-inferable types
        assert!(!is_inferable_datatype(xsd::DATE_TIME));
        assert!(!is_inferable_datatype(xsd::DATE));
        assert!(!is_inferable_datatype(rdf::LANG_STRING));
        assert!(!is_inferable_datatype(jsonld::JSON));
        assert!(!is_inferable_datatype("http://example.org/customType"));
    }

    /// Issue #45 (b): under the W3C profile only `xsd:string` may lose its
    /// datatype tag; the other "inferable" types must keep theirs because a
    /// SPARQL-Results-JSON `value` is always a JSON string.
    #[test]
    fn test_omit_datatype_for_string_literal() {
        assert!(omit_datatype_for_string_literal(xsd::STRING, true));
        assert!(omit_datatype_for_string_literal("xsd:string", true));
        for dt in [
            xsd::INTEGER,
            xsd::LONG,
            xsd::DOUBLE,
            xsd::DECIMAL,
            xsd::BOOLEAN,
            fluree::EMBEDDING_VECTOR,
        ] {
            assert!(
                !omit_datatype_for_string_literal(dt, true),
                "{dt} must keep its datatype in W3C output"
            );
            assert!(
                omit_datatype_for_string_literal(dt, false),
                "{dt} stays inferable for JSON-LD-flavored output"
            );
        }
        // Non-inferable types are emitted under either profile.
        assert!(!omit_datatype_for_string_literal(xsd::DATE, true));
        assert!(!omit_datatype_for_string_literal(xsd::DATE, false));
    }
}
