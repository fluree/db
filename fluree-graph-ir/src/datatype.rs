//! RDF datatype representation
//!
//! Datatypes are always explicit in this IR - there is no "untyped" literal.
//! Plain strings default to `xsd:string`, and language-tagged strings use
//! `rdf:langString`.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Common XSD and RDF datatype IRIs (re-exported from vocab crate)
pub mod iri {
    pub use fluree_vocab::rdf::{
        JSON as RDF_JSON, LANG_STRING as RDF_LANG_STRING, TYPE as RDF_TYPE,
    };
    pub use fluree_vocab::xsd::{
        ANY_URI as XSD_ANY_URI, BOOLEAN as XSD_BOOLEAN, DATE as XSD_DATE,
        DATE_TIME as XSD_DATE_TIME, DECIMAL as XSD_DECIMAL, DOUBLE as XSD_DOUBLE,
        INTEGER as XSD_INTEGER, LONG as XSD_LONG, STRING as XSD_STRING,
    };
}

/// RDF literal datatype
///
/// Datatypes are always explicit. Use `Datatype::xsd_string()` for plain
/// strings, `Datatype::rdf_lang_string()` for language-tagged strings.
///
/// # Special Handling
///
/// - `JsonLdJson` represents the JSON-LD `@json` datatype, which maps to
///   `rdf:JSON` in RDF but has special formatting behavior.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Datatype {
    /// Standard XSD/RDF datatype IRI (always expanded)
    Iri(Arc<str>),
    /// JSON-LD @json special type (maps to rdf:JSON)
    ///
    /// This is kept separate because formatters need to handle it specially:
    /// JSON-LD outputs `{"@value": ..., "@type": "@json"}` rather than the
    /// full IRI.
    JsonLdJson,
}

impl Datatype {
    /// Create a datatype from an expanded IRI
    pub fn from_iri(iri: impl AsRef<str>) -> Self {
        let iri = iri.as_ref();
        // Recognize @json / rdf:JSON specially
        if iri == iri::RDF_JSON || iri == "@json" {
            Datatype::JsonLdJson
        } else if iri == "@vector" {
            // Normalize @vector shorthand to full IRI
            Datatype::Iri(Arc::from(fluree_vocab::fluree::EMBEDDING_VECTOR))
        } else {
            Datatype::Iri(Arc::from(iri))
        }
    }

    /// xsd:string - default for plain string literals
    pub fn xsd_string() -> Self {
        Datatype::Iri(Arc::from(iri::XSD_STRING))
    }

    /// xsd:boolean
    pub fn xsd_boolean() -> Self {
        Datatype::Iri(Arc::from(iri::XSD_BOOLEAN))
    }

    /// xsd:integer
    pub fn xsd_integer() -> Self {
        Datatype::Iri(Arc::from(iri::XSD_INTEGER))
    }

    /// xsd:long
    pub fn xsd_long() -> Self {
        Datatype::Iri(Arc::from(iri::XSD_LONG))
    }

    /// xsd:double
    pub fn xsd_double() -> Self {
        Datatype::Iri(Arc::from(iri::XSD_DOUBLE))
    }

    /// xsd:decimal
    pub fn xsd_decimal() -> Self {
        Datatype::Iri(Arc::from(iri::XSD_DECIMAL))
    }

    /// xsd:date
    pub fn xsd_date() -> Self {
        Datatype::Iri(Arc::from(iri::XSD_DATE))
    }

    /// xsd:dateTime
    pub fn xsd_date_time() -> Self {
        Datatype::Iri(Arc::from(iri::XSD_DATE_TIME))
    }

    /// xsd:anyURI
    pub fn xsd_any_uri() -> Self {
        Datatype::Iri(Arc::from(iri::XSD_ANY_URI))
    }

    /// rdf:langString - for language-tagged literals
    pub fn rdf_lang_string() -> Self {
        Datatype::Iri(Arc::from(iri::RDF_LANG_STRING))
    }

    /// rdf:JSON / @json
    pub fn rdf_json() -> Self {
        Datatype::JsonLdJson
    }

    /// Get the IRI representation of this datatype
    pub fn as_iri(&self) -> &str {
        match self {
            Datatype::Iri(iri) => iri,
            Datatype::JsonLdJson => iri::RDF_JSON,
        }
    }

    /// Check if this is the xsd:string datatype
    pub fn is_xsd_string(&self) -> bool {
        matches!(self, Datatype::Iri(iri) if iri.as_ref() == iri::XSD_STRING)
    }

    /// Check if this is the rdf:langString datatype
    pub fn is_lang_string(&self) -> bool {
        matches!(self, Datatype::Iri(iri) if iri.as_ref() == iri::RDF_LANG_STRING)
    }

    /// Check if this is the @json / rdf:JSON datatype
    pub fn is_json(&self) -> bool {
        matches!(self, Datatype::JsonLdJson)
    }

    /// Check if this is a numeric type (integer, long, double, decimal)
    pub fn is_numeric(&self) -> bool {
        match self {
            Datatype::Iri(iri) => matches!(
                iri.as_ref(),
                iri::XSD_INTEGER | iri::XSD_LONG | iri::XSD_DOUBLE | iri::XSD_DECIMAL
            ),
            Datatype::JsonLdJson => false,
        }
    }
}

impl PartialEq for Datatype {
    fn eq(&self, other: &Self) -> bool {
        self.as_iri() == other.as_iri()
    }
}

impl Eq for Datatype {}

impl Hash for Datatype {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_iri().hash(state);
    }
}

impl PartialOrd for Datatype {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Datatype {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_iri().cmp(other.as_iri())
    }
}

impl std::fmt::Display for Datatype {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_iri())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datatype_constructors() {
        assert_eq!(Datatype::xsd_string().as_iri(), iri::XSD_STRING);
        assert_eq!(Datatype::xsd_boolean().as_iri(), iri::XSD_BOOLEAN);
        assert_eq!(Datatype::xsd_integer().as_iri(), iri::XSD_INTEGER);
        assert_eq!(Datatype::xsd_double().as_iri(), iri::XSD_DOUBLE);
        assert_eq!(Datatype::rdf_lang_string().as_iri(), iri::RDF_LANG_STRING);
        assert_eq!(Datatype::rdf_json().as_iri(), iri::RDF_JSON);
    }

    #[test]
    fn test_from_iri_recognizes_json() {
        assert!(Datatype::from_iri(iri::RDF_JSON).is_json());
        assert!(Datatype::from_iri("@json").is_json());
    }

    #[test]
    fn test_datatype_equality() {
        // JsonLdJson equals itself
        assert_eq!(Datatype::JsonLdJson, Datatype::JsonLdJson);

        // JsonLdJson equals Iri(rdf:JSON)
        assert_eq!(Datatype::JsonLdJson, Datatype::from_iri(iri::RDF_JSON));

        // Different datatypes are not equal
        assert_ne!(Datatype::xsd_string(), Datatype::xsd_integer());
    }

    #[test]
    fn test_is_checks() {
        assert!(Datatype::xsd_string().is_xsd_string());
        assert!(!Datatype::xsd_integer().is_xsd_string());

        assert!(Datatype::rdf_lang_string().is_lang_string());
        assert!(!Datatype::xsd_string().is_lang_string());

        assert!(Datatype::xsd_integer().is_numeric());
        assert!(Datatype::xsd_double().is_numeric());
        assert!(!Datatype::xsd_string().is_numeric());
    }
}
