//! IRI encoding trait for WASM-compatible abstraction
//!
//! This trait allows the parser to remain runtime-agnostic by abstracting
//! the IRI-to-SID encoding step. Native implementations can use the database's
//! namespace codes, while WASM/offline implementations can use stubs or
//! context-only encoders.

use fluree_db_core::{canonical_split, LedgerSnapshot, NsSplitMode, Sid};
use fluree_vocab::{rdf, xsd};

/// Trait for encoding IRIs to SIDs
///
/// Keeps the parser runtime-agnostic - the actual encoding implementation
/// can be provided by the database (native) or a stub (WASM/offline).
pub trait IriEncoder {
    /// Encode an IRI to a SID
    ///
    /// Returns `None` if the IRI's namespace is not registered.
    fn encode_iri(&self, iri: &str) -> Option<Sid>;

    /// Encode an IRI only when its canonical prefix is registered.
    ///
    /// Unlike `encode_iri`, this does not fall back to a full-IRI SID.
    fn encode_iri_strict(&self, iri: &str) -> Option<Sid> {
        self.encode_iri(iri)
    }
}

// Native: LedgerSnapshot implements IriEncoder
impl IriEncoder for LedgerSnapshot {
    fn encode_iri(&self, iri: &str) -> Option<Sid> {
        // Delegates to the existing LedgerSnapshot::encode_iri method
        LedgerSnapshot::encode_iri(self, iri)
    }

    fn encode_iri_strict(&self, iri: &str) -> Option<Sid> {
        LedgerSnapshot::encode_iri_strict(self, iri)
    }
}

/// Stub encoder that always fails
///
/// Useful for testing parsing without a database, or for WASM contexts
/// where namespace encoding isn't available.
pub struct NoEncoder;

impl IriEncoder for NoEncoder {
    fn encode_iri(&self, _iri: &str) -> Option<Sid> {
        None
    }

    fn encode_iri_strict(&self, _iri: &str) -> Option<Sid> {
        None
    }
}

/// In-memory encoder with a fixed namespace mapping
///
/// Useful for testing or when the full database isn't available.
/// Uses [`canonical_split`] for deterministic IRI→SID encoding.
#[derive(Debug, Default)]
pub struct MemoryEncoder {
    namespaces: std::collections::HashMap<String, u16>,
    split_mode: NsSplitMode,
}

impl MemoryEncoder {
    /// Create a new empty encoder
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a namespace mapping
    pub fn add_namespace(&mut self, prefix: impl Into<String>, code: u16) -> &mut Self {
        self.namespaces.insert(prefix.into(), code);
        self
    }

    /// Create an encoder with common namespaces pre-registered
    pub fn with_common_namespaces() -> Self {
        let mut encoder = Self::new();
        encoder
            .add_namespace("", 0)
            .add_namespace("@", 1)
            .add_namespace(xsd::NS, 2)
            .add_namespace(rdf::NS, 3);
        encoder
    }
}

impl IriEncoder for MemoryEncoder {
    /// Always returns `Some(...)` — falls back to `Sid(EMPTY, iri)` for
    /// unregistered prefixes rather than returning `None`. This deviates
    /// from the `IriEncoder` trait doc ("Returns None if not registered")
    /// but matches `LedgerSnapshot::encode_iri` behavior and is convenient
    /// for testing where unknown IRIs should produce empty results, not errors.
    fn encode_iri(&self, iri: &str) -> Option<Sid> {
        let (prefix, suffix) = canonical_split(iri, self.split_mode);
        if let Some(&code) = self.namespaces.get(prefix) {
            Some(Sid::new(code, suffix))
        } else {
            Some(Sid::new(fluree_vocab::namespaces::EMPTY, iri))
        }
    }

    fn encode_iri_strict(&self, iri: &str) -> Option<Sid> {
        let (prefix, suffix) = canonical_split(iri, self.split_mode);
        self.namespaces
            .get(prefix)
            .copied()
            .map(|code| Sid::new(code, suffix))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_encoder() {
        let encoder = NoEncoder;
        assert!(encoder.encode_iri("http://example.org/test").is_none());
    }

    #[test]
    fn test_memory_encoder() {
        let mut encoder = MemoryEncoder::new();
        encoder.add_namespace("http://example.org/", 100);

        let sid = encoder.encode_iri("http://example.org/Person").unwrap();
        assert_eq!(sid.namespace_code, 100);
        assert_eq!(sid.name.as_ref(), "Person");

        // Unknown namespace falls back to EMPTY namespace (code 0) with full IRI as name
        let fallback = encoder.encode_iri("http://other.org/Thing").unwrap();
        assert_eq!(fallback.namespace_code, fluree_vocab::namespaces::EMPTY);
        assert_eq!(fallback.name.as_ref(), "http://other.org/Thing");
    }

    #[test]
    fn test_memory_encoder_common_namespaces() {
        let encoder = MemoryEncoder::with_common_namespaces();

        let xsd_string = encoder.encode_iri(xsd::STRING).unwrap();
        assert_eq!(xsd_string.namespace_code, 2);
        assert_eq!(xsd_string.name.as_ref(), "string");

        let rdf_type = encoder.encode_iri(rdf::TYPE).unwrap();
        assert_eq!(rdf_type.namespace_code, 3);
        assert_eq!(rdf_type.name.as_ref(), "type");
    }

    #[test]
    fn test_memory_encoder_canonical_split() {
        let mut encoder = MemoryEncoder::new();
        encoder.add_namespace("http://example.org/ns/", 101);
        encoder.add_namespace("http://example.org/", 100);

        // canonical_split(MostGranular) splits at last '/' → prefix = "http://example.org/ns/"
        // Exact lookup finds code 101
        let sid = encoder.encode_iri("http://example.org/ns/Thing").unwrap();
        assert_eq!(sid.namespace_code, 101);
        assert_eq!(sid.name.as_ref(), "Thing");

        // canonical_split for "http://example.org/Other" → prefix = "http://example.org/"
        let sid2 = encoder.encode_iri("http://example.org/Other").unwrap();
        assert_eq!(sid2.namespace_code, 100);
        assert_eq!(sid2.name.as_ref(), "Other");
    }
}
