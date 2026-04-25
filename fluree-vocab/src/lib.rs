//! RDF Vocabulary Constants and Namespace Codes for Fluree
//!
//! This crate provides a centralized location for RDF vocabulary IRIs,
//! namespace codes, and other common constants used throughout the Fluree ecosystem.
//!
//! # Organization
//!
//! Constants are organized by vocabulary:
//! - `rdf` - RDF vocabulary (http://www.w3.org/1999/02/22-rdf-syntax-ns#)
//! - `rdfs` - RDFS vocabulary (http://www.w3.org/2000/01/rdf-schema#)
//! - `xsd` - XSD vocabulary (http://www.w3.org/2001/XMLSchema#)
//! - `owl` - OWL vocabulary (http://www.w3.org/2002/07/owl#)
//! - `namespaces` - Namespace codes used for IRI encoding
//! - `errors` - Error type compact IRIs for API responses

use std::sync::Arc;

pub mod errors;

/// Constraint on the datatype of an unresolved literal, using IRI strings.
///
/// Either an explicit datatype IRI or a language tag. Setting a language tag
/// implies that the datatype is `rdf:langString` (per RDF 1.1); this sum
/// type makes the illegal state (both an explicit non-`rdf:langString`
/// datatype and a language tag) unrepresentable.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum UnresolvedDatatypeConstraint {
    /// Datatype IRI (not yet resolved to Sid)
    Explicit(Arc<str>),
    /// Language tag (implies the datatype is `rdf:langString`)
    LangTag(Arc<str>),
}

impl UnresolvedDatatypeConstraint {
    /// The effective datatype IRI.
    ///
    /// Returns the explicit IRI for [`Explicit`](Self::Explicit), or the
    /// canonical `rdf:langString` IRI for [`LangTag`](Self::LangTag).
    pub fn datatype_iri(&self) -> &str {
        match self {
            UnresolvedDatatypeConstraint::Explicit(iri) => iri,
            UnresolvedDatatypeConstraint::LangTag(_) => rdf::LANG_STRING,
        }
    }

    /// The language tag, if this is a [`LangTag`](Self::LangTag) constraint.
    pub fn lang_tag(&self) -> Option<&str> {
        match self {
            UnresolvedDatatypeConstraint::LangTag(tag) => Some(tag),
            UnresolvedDatatypeConstraint::Explicit(_) => None,
        }
    }
}

/// RDF vocabulary constants
pub mod rdf {
    /// rdf namespace IRI (prefix)
    pub const NS: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

    /// rdf:type IRI
    pub const TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

    /// rdf:langString IRI
    pub const LANG_STRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";

    /// rdf:JSON IRI
    pub const JSON: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON";

    /// rdf:first IRI (RDF list head)
    pub const FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";

    /// rdf:rest IRI (RDF list tail)
    pub const REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";

    /// rdf:nil IRI (RDF list terminator)
    pub const NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";

    /// rdf:Property IRI (the class of RDF properties)
    pub const PROPERTY: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property";
}

/// RDFS vocabulary constants
pub mod rdfs {
    /// rdfs namespace IRI (prefix)
    pub const NS: &str = "http://www.w3.org/2000/01/rdf-schema#";

    /// rdfs:subClassOf IRI
    pub const SUB_CLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";

    /// rdfs:subPropertyOf IRI
    pub const SUB_PROPERTY_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";

    /// rdfs:domain IRI
    pub const DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";

    /// rdfs:range IRI
    pub const RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
}

/// XSD vocabulary constants
pub mod xsd {
    /// xsd namespace IRI (prefix)
    pub const NS: &str = "http://www.w3.org/2001/XMLSchema#";

    /// xsd:string IRI
    pub const STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

    /// xsd:integer IRI
    pub const INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

    /// xsd:long IRI
    pub const LONG: &str = "http://www.w3.org/2001/XMLSchema#long";

    /// xsd:int IRI
    pub const INT: &str = "http://www.w3.org/2001/XMLSchema#int";

    /// xsd:short IRI
    pub const SHORT: &str = "http://www.w3.org/2001/XMLSchema#short";

    /// xsd:byte IRI
    pub const BYTE: &str = "http://www.w3.org/2001/XMLSchema#byte";

    /// xsd:unsignedLong IRI
    pub const UNSIGNED_LONG: &str = "http://www.w3.org/2001/XMLSchema#unsignedLong";

    /// xsd:unsignedInt IRI
    pub const UNSIGNED_INT: &str = "http://www.w3.org/2001/XMLSchema#unsignedInt";

    /// xsd:unsignedShort IRI
    pub const UNSIGNED_SHORT: &str = "http://www.w3.org/2001/XMLSchema#unsignedShort";

    /// xsd:unsignedByte IRI
    pub const UNSIGNED_BYTE: &str = "http://www.w3.org/2001/XMLSchema#unsignedByte";

    /// xsd:nonNegativeInteger IRI
    pub const NON_NEGATIVE_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#nonNegativeInteger";

    /// xsd:positiveInteger IRI
    pub const POSITIVE_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#positiveInteger";

    /// xsd:nonPositiveInteger IRI
    pub const NON_POSITIVE_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#nonPositiveInteger";

    /// xsd:negativeInteger IRI
    pub const NEGATIVE_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#negativeInteger";

    /// xsd:decimal IRI
    pub const DECIMAL: &str = "http://www.w3.org/2001/XMLSchema#decimal";

    /// xsd:float IRI
    pub const FLOAT: &str = "http://www.w3.org/2001/XMLSchema#float";

    /// xsd:double IRI
    pub const DOUBLE: &str = "http://www.w3.org/2001/XMLSchema#double";

    /// xsd:boolean IRI
    pub const BOOLEAN: &str = "http://www.w3.org/2001/XMLSchema#boolean";

    /// xsd:dateTime IRI
    pub const DATE_TIME: &str = "http://www.w3.org/2001/XMLSchema#dateTime";

    /// xsd:date IRI
    pub const DATE: &str = "http://www.w3.org/2001/XMLSchema#date";

    /// xsd:time IRI
    pub const TIME: &str = "http://www.w3.org/2001/XMLSchema#time";

    /// xsd:gYear IRI
    pub const G_YEAR: &str = "http://www.w3.org/2001/XMLSchema#gYear";

    /// xsd:gYearMonth IRI
    pub const G_YEAR_MONTH: &str = "http://www.w3.org/2001/XMLSchema#gYearMonth";

    /// xsd:gMonth IRI
    pub const G_MONTH: &str = "http://www.w3.org/2001/XMLSchema#gMonth";

    /// xsd:gDay IRI
    pub const G_DAY: &str = "http://www.w3.org/2001/XMLSchema#gDay";

    /// xsd:gMonthDay IRI
    pub const G_MONTH_DAY: &str = "http://www.w3.org/2001/XMLSchema#gMonthDay";

    /// xsd:duration IRI
    pub const DURATION: &str = "http://www.w3.org/2001/XMLSchema#duration";

    /// xsd:dayTimeDuration IRI
    pub const DAY_TIME_DURATION: &str = "http://www.w3.org/2001/XMLSchema#dayTimeDuration";

    /// xsd:yearMonthDuration IRI
    pub const YEAR_MONTH_DURATION: &str = "http://www.w3.org/2001/XMLSchema#yearMonthDuration";

    /// xsd:anyURI IRI
    pub const ANY_URI: &str = "http://www.w3.org/2001/XMLSchema#anyURI";

    /// xsd:normalizedString IRI
    pub const NORMALIZED_STRING: &str = "http://www.w3.org/2001/XMLSchema#normalizedString";

    /// xsd:token IRI
    pub const TOKEN: &str = "http://www.w3.org/2001/XMLSchema#token";

    /// xsd:language IRI
    pub const LANGUAGE: &str = "http://www.w3.org/2001/XMLSchema#language";

    /// xsd:base64Binary IRI
    pub const BASE64_BINARY: &str = "http://www.w3.org/2001/XMLSchema#base64Binary";

    /// xsd:hexBinary IRI
    pub const HEX_BINARY: &str = "http://www.w3.org/2001/XMLSchema#hexBinary";

    // ========================================================================
    // Datatype Normalization Helpers
    // ========================================================================
    //
    // These functions normalize XSD datatypes to canonical forms for storage.
    // This ensures consistency between transact and query paths.

    /// Normalize integer-family datatypes to xsd:integer
    ///
    /// XSD defines a type hierarchy where int, short, byte, long are subtypes
    /// of integer. For storage consistency, we normalize all of these to
    /// xsd:integer since they all map to the same Rust type (i64 or BigInt).
    ///
    /// # Arguments
    /// * `datatype_iri` - The full IRI of the datatype
    ///
    /// # Returns
    /// * `xsd:integer` IRI if input is an integer-family type
    /// * The original IRI unchanged otherwise
    #[inline]
    pub fn normalize_integer_family(datatype_iri: &str) -> &str {
        match datatype_iri {
            LONG | INT | SHORT | BYTE | UNSIGNED_LONG | UNSIGNED_INT | UNSIGNED_SHORT
            | UNSIGNED_BYTE | NON_NEGATIVE_INTEGER | POSITIVE_INTEGER | NON_POSITIVE_INTEGER
            | NEGATIVE_INTEGER => INTEGER,
            _ => datatype_iri,
        }
    }

    /// Normalize float to double
    ///
    /// XSD float and double both map to f64 in Rust. Normalize to double
    /// for storage consistency.
    #[inline]
    pub fn normalize_float_family(datatype_iri: &str) -> &str {
        match datatype_iri {
            FLOAT => DOUBLE,
            _ => datatype_iri,
        }
    }

    /// Normalize all numeric datatypes to their canonical storage form
    ///
    /// Combines integer-family and float-family normalization:
    /// - xsd:int, xsd:short, xsd:byte, xsd:long → xsd:integer
    /// - xsd:float → xsd:double
    /// - All other types pass through unchanged
    #[inline]
    pub fn normalize_numeric_datatype(datatype_iri: &str) -> &str {
        match datatype_iri {
            LONG | INT | SHORT | BYTE | UNSIGNED_LONG | UNSIGNED_INT | UNSIGNED_SHORT
            | UNSIGNED_BYTE | NON_NEGATIVE_INTEGER | POSITIVE_INTEGER | NON_POSITIVE_INTEGER
            | NEGATIVE_INTEGER => INTEGER,
            FLOAT => DOUBLE,
            _ => datatype_iri,
        }
    }

    /// Check if a datatype IRI is a numeric type
    #[inline]
    pub fn is_numeric_datatype(datatype_iri: &str) -> bool {
        matches!(
            datatype_iri,
            INTEGER
                | LONG
                | INT
                | SHORT
                | BYTE
                | UNSIGNED_LONG
                | UNSIGNED_INT
                | UNSIGNED_SHORT
                | UNSIGNED_BYTE
                | NON_NEGATIVE_INTEGER
                | POSITIVE_INTEGER
                | NON_POSITIVE_INTEGER
                | NEGATIVE_INTEGER
                | DECIMAL
                | FLOAT
                | DOUBLE
        )
    }

    /// Check if a datatype IRI is an integer-family type
    #[inline]
    pub fn is_integer_family(datatype_iri: &str) -> bool {
        matches!(
            datatype_iri,
            INTEGER
                | LONG
                | INT
                | SHORT
                | BYTE
                | UNSIGNED_LONG
                | UNSIGNED_INT
                | UNSIGNED_SHORT
                | UNSIGNED_BYTE
                | NON_NEGATIVE_INTEGER
                | POSITIVE_INTEGER
                | NON_POSITIVE_INTEGER
                | NEGATIVE_INTEGER
        )
    }

    /// Check if a datatype IRI is a string-like type
    ///
    /// String-like types are those that can hold string values and should not
    /// accept implicit coercion from numbers or booleans.
    #[inline]
    pub fn is_string_like(datatype_iri: &str) -> bool {
        matches!(
            datatype_iri,
            STRING | NORMALIZED_STRING | TOKEN | LANGUAGE | ANY_URI
        )
    }

    /// Check if a datatype IRI is a temporal type
    #[inline]
    pub fn is_temporal(datatype_iri: &str) -> bool {
        matches!(datatype_iri, DATE_TIME | DATE | TIME)
    }

    // ========================================================================
    // Integer Range Validation
    // ========================================================================

    /// Get the valid range bounds for an integer subtype as (min, max) inclusive.
    ///
    /// Returns `None` for unbounded types (xsd:integer) or non-integer types.
    /// Uses i128 to accommodate the full range of xsd:unsignedLong.
    ///
    /// # Practical Limitation
    ///
    /// Per XSD spec, sign-constrained types (`positiveInteger`, `nonNegativeInteger`,
    /// `negativeInteger`, `nonPositiveInteger`) are semantically unbounded—they only
    /// constrain the sign, not the magnitude. However, for practical purposes, we
    /// bound them to the i128 range. Values outside this range will be rejected.
    ///
    /// Use `xsd:integer` (returns `None`) for truly unbounded integers.
    #[inline]
    pub fn integer_bounds(datatype_iri: &str) -> Option<(i128, i128)> {
        match datatype_iri {
            BYTE => Some((i8::MIN as i128, i8::MAX as i128)),
            SHORT => Some((i16::MIN as i128, i16::MAX as i128)),
            INT => Some((i32::MIN as i128, i32::MAX as i128)),
            LONG => Some((i64::MIN as i128, i64::MAX as i128)),
            UNSIGNED_BYTE => Some((0, u8::MAX as i128)),
            UNSIGNED_SHORT => Some((0, u16::MAX as i128)),
            UNSIGNED_INT => Some((0, u32::MAX as i128)),
            UNSIGNED_LONG => Some((0, u64::MAX as i128)),
            // Sign-constrained types: semantically unbounded per XSD, but we bound
            // to i128 range for practical purposes (see doc comment above)
            POSITIVE_INTEGER => Some((1, i128::MAX)),
            NON_NEGATIVE_INTEGER => Some((0, i128::MAX)),
            NEGATIVE_INTEGER => Some((i128::MIN, -1)),
            NON_POSITIVE_INTEGER => Some((i128::MIN, 0)),
            // xsd:integer is truly unbounded
            INTEGER => None,
            _ => None,
        }
    }

    /// Validate that an i64 value is within the valid range for the given integer datatype.
    ///
    /// Returns `Ok(())` if valid, or `Err` with a descriptive message if out of range.
    pub fn validate_integer_range_i64(datatype_iri: &str, value: i64) -> Result<(), String> {
        if let Some((min, max)) = integer_bounds(datatype_iri) {
            let v = value as i128;
            if v < min || v > max {
                return Err(format!(
                    "Value {} is out of range for {}: expected {} to {}",
                    value,
                    datatype_local_name(datatype_iri).unwrap_or(datatype_iri),
                    min,
                    max
                ));
            }
        }
        Ok(())
    }

    /// Validate that an i128 value is within the valid range for the given integer datatype.
    ///
    /// This is useful for BigInt values that may exceed i64 range.
    pub fn validate_integer_range_i128(datatype_iri: &str, value: i128) -> Result<(), String> {
        if let Some((min, max)) = integer_bounds(datatype_iri) {
            if value < min || value > max {
                return Err(format!(
                    "Value {} is out of range for {}: expected {} to {}",
                    value,
                    datatype_local_name(datatype_iri).unwrap_or(datatype_iri),
                    min,
                    max
                ));
            }
        }
        Ok(())
    }

    /// Get the local name portion of a datatype IRI (e.g., "integer" from xsd:integer)
    #[inline]
    pub fn datatype_local_name(datatype_iri: &str) -> Option<&str> {
        datatype_iri.rsplit('#').next()
    }
}

/// OGC GeoSPARQL vocabulary constants
///
/// GeoSPARQL is the OGC standard for representing and querying geospatial
/// data in RDF. See <http://www.opengis.net/ont/geosparql>.
pub mod geo {
    /// GeoSPARQL namespace IRI
    pub const NS: &str = "http://www.opengis.net/ont/geosparql#";

    /// geo:wktLiteral datatype IRI (Well-Known Text encoding)
    pub const WKT_LITERAL: &str = "http://www.opengis.net/ont/geosparql#wktLiteral";

    /// geo:gmlLiteral datatype IRI (GML encoding)
    pub const GML_LITERAL: &str = "http://www.opengis.net/ont/geosparql#gmlLiteral";

    /// geo:asWKT property IRI (geometry as WKT)
    pub const AS_WKT: &str = "http://www.opengis.net/ont/geosparql#asWKT";

    /// geo:hasGeometry property IRI
    pub const HAS_GEOMETRY: &str = "http://www.opengis.net/ont/geosparql#hasGeometry";
}

/// GeoSPARQL local names (for SID construction)
pub mod geo_names {
    /// wktLiteral local name
    pub const WKT_LITERAL: &str = "wktLiteral";

    /// gmlLiteral local name
    pub const GML_LITERAL: &str = "gmlLiteral";

    /// asWKT local name
    pub const AS_WKT: &str = "asWKT";

    /// hasGeometry local name
    pub const HAS_GEOMETRY: &str = "hasGeometry";
}

/// XSD datatype local names (for SID construction)
///
/// XSD datatypes are encoded with namespace code 2 (XSD).
/// These constants provide the local name portion for constructing SIDs.
///
/// # Example
/// ```
/// use fluree_vocab::xsd_names;
///
/// assert_eq!(xsd_names::STRING, "string");
/// assert_eq!(xsd_names::LONG, "long");
/// ```
pub mod xsd_names {
    /// xsd:string local name
    pub const STRING: &str = "string";

    /// xsd:integer local name
    pub const INTEGER: &str = "integer";

    /// xsd:long local name
    pub const LONG: &str = "long";

    /// xsd:int local name
    pub const INT: &str = "int";

    /// xsd:short local name
    pub const SHORT: &str = "short";

    /// xsd:byte local name
    pub const BYTE: &str = "byte";

    /// xsd:unsignedLong local name
    pub const UNSIGNED_LONG: &str = "unsignedLong";

    /// xsd:unsignedInt local name
    pub const UNSIGNED_INT: &str = "unsignedInt";

    /// xsd:unsignedShort local name
    pub const UNSIGNED_SHORT: &str = "unsignedShort";

    /// xsd:unsignedByte local name
    pub const UNSIGNED_BYTE: &str = "unsignedByte";

    /// xsd:nonNegativeInteger local name
    pub const NON_NEGATIVE_INTEGER: &str = "nonNegativeInteger";

    /// xsd:positiveInteger local name
    pub const POSITIVE_INTEGER: &str = "positiveInteger";

    /// xsd:nonPositiveInteger local name
    pub const NON_POSITIVE_INTEGER: &str = "nonPositiveInteger";

    /// xsd:negativeInteger local name
    pub const NEGATIVE_INTEGER: &str = "negativeInteger";

    /// xsd:decimal local name
    pub const DECIMAL: &str = "decimal";

    /// xsd:float local name
    pub const FLOAT: &str = "float";

    /// xsd:double local name
    pub const DOUBLE: &str = "double";

    /// xsd:boolean local name
    pub const BOOLEAN: &str = "boolean";

    /// xsd:dateTime local name
    pub const DATE_TIME: &str = "dateTime";

    /// xsd:date local name
    pub const DATE: &str = "date";

    /// xsd:time local name
    pub const TIME: &str = "time";

    /// xsd:duration local name
    pub const DURATION: &str = "duration";

    /// xsd:dayTimeDuration local name
    pub const DAY_TIME_DURATION: &str = "dayTimeDuration";

    /// xsd:yearMonthDuration local name
    pub const YEAR_MONTH_DURATION: &str = "yearMonthDuration";

    /// xsd:anyURI local name
    pub const ANY_URI: &str = "anyURI";

    /// xsd:normalizedString local name
    pub const NORMALIZED_STRING: &str = "normalizedString";

    /// xsd:token local name
    pub const TOKEN: &str = "token";

    /// xsd:language local name
    pub const LANGUAGE: &str = "language";

    /// xsd:base64Binary local name
    pub const BASE64_BINARY: &str = "base64Binary";

    /// xsd:hexBinary local name
    pub const HEX_BINARY: &str = "hexBinary";

    /// xsd:gYear local name
    pub const G_YEAR: &str = "gYear";

    /// xsd:gMonth local name
    pub const G_MONTH: &str = "gMonth";

    /// xsd:gDay local name
    pub const G_DAY: &str = "gDay";

    /// xsd:gYearMonth local name
    pub const G_YEAR_MONTH: &str = "gYearMonth";

    /// xsd:gMonthDay local name
    pub const G_MONTH_DAY: &str = "gMonthDay";

    // ========================================================================
    // Classification Helpers (for SID-based datatype checking)
    // ========================================================================

    /// Check if a local name is an integer-family type
    ///
    /// This is useful when you have a SID and want to check the datatype
    /// without reconstructing the full IRI.
    #[inline]
    pub fn is_integer_family_name(name: &str) -> bool {
        matches!(
            name,
            INTEGER
                | LONG
                | INT
                | SHORT
                | BYTE
                | UNSIGNED_LONG
                | UNSIGNED_INT
                | UNSIGNED_SHORT
                | UNSIGNED_BYTE
                | NON_NEGATIVE_INTEGER
                | POSITIVE_INTEGER
                | NON_POSITIVE_INTEGER
                | NEGATIVE_INTEGER
        )
    }

    /// Check if a local name is a string-like type
    #[inline]
    pub fn is_string_like_name(name: &str) -> bool {
        matches!(
            name,
            STRING | NORMALIZED_STRING | TOKEN | LANGUAGE | ANY_URI
        )
    }

    /// Check if a local name is a temporal type
    #[inline]
    pub fn is_temporal_name(name: &str) -> bool {
        matches!(name, DATE_TIME | DATE | TIME)
    }
}

/// RDF vocabulary local names (for SID construction)
///
/// RDF terms are encoded with namespace code 3 (RDF).
pub mod rdf_names {
    /// rdf:type local name
    pub const TYPE: &str = "type";

    /// rdf:langString local name
    pub const LANG_STRING: &str = "langString";

    /// rdf:JSON local name
    pub const JSON: &str = "JSON";

    /// rdf:first local name
    pub const FIRST: &str = "first";

    /// rdf:rest local name
    pub const REST: &str = "rest";

    /// rdf:nil local name
    pub const NIL: &str = "nil";
}

/// OWL vocabulary constants
pub mod owl {
    /// owl namespace IRI (prefix)
    pub const NS: &str = "http://www.w3.org/2002/07/owl#";

    /// owl:inverseOf IRI
    pub const INVERSE_OF: &str = "http://www.w3.org/2002/07/owl#inverseOf";

    /// owl:equivalentClass IRI
    pub const EQUIVALENT_CLASS: &str = "http://www.w3.org/2002/07/owl#equivalentClass";

    /// owl:equivalentProperty IRI
    pub const EQUIVALENT_PROPERTY: &str = "http://www.w3.org/2002/07/owl#equivalentProperty";

    /// owl:sameAs IRI
    pub const SAME_AS: &str = "http://www.w3.org/2002/07/owl#sameAs";

    /// owl:SymmetricProperty IRI
    pub const SYMMETRIC_PROPERTY: &str = "http://www.w3.org/2002/07/owl#SymmetricProperty";

    /// owl:TransitiveProperty IRI
    pub const TRANSITIVE_PROPERTY: &str = "http://www.w3.org/2002/07/owl#TransitiveProperty";

    /// owl:FunctionalProperty IRI
    pub const FUNCTIONAL_PROPERTY: &str = "http://www.w3.org/2002/07/owl#FunctionalProperty";

    /// owl:InverseFunctionalProperty IRI
    pub const INVERSE_FUNCTIONAL_PROPERTY: &str =
        "http://www.w3.org/2002/07/owl#InverseFunctionalProperty";

    /// owl:propertyChainAxiom IRI
    pub const PROPERTY_CHAIN_AXIOM: &str = "http://www.w3.org/2002/07/owl#propertyChainAxiom";

    /// owl:hasKey IRI
    pub const HAS_KEY: &str = "http://www.w3.org/2002/07/owl#hasKey";

    /// owl:Restriction IRI
    pub const RESTRICTION: &str = "http://www.w3.org/2002/07/owl#Restriction";

    /// owl:onProperty IRI
    pub const ON_PROPERTY: &str = "http://www.w3.org/2002/07/owl#onProperty";

    /// owl:hasValue IRI
    pub const HAS_VALUE: &str = "http://www.w3.org/2002/07/owl#hasValue";

    /// owl:someValuesFrom IRI
    pub const SOME_VALUES_FROM: &str = "http://www.w3.org/2002/07/owl#someValuesFrom";

    /// owl:allValuesFrom IRI
    pub const ALL_VALUES_FROM: &str = "http://www.w3.org/2002/07/owl#allValuesFrom";

    /// owl:maxCardinality IRI
    pub const MAX_CARDINALITY: &str = "http://www.w3.org/2002/07/owl#maxCardinality";

    /// owl:maxQualifiedCardinality IRI
    pub const MAX_QUALIFIED_CARDINALITY: &str =
        "http://www.w3.org/2002/07/owl#maxQualifiedCardinality";

    /// owl:onClass IRI
    pub const ON_CLASS: &str = "http://www.w3.org/2002/07/owl#onClass";

    /// owl:intersectionOf IRI
    pub const INTERSECTION_OF: &str = "http://www.w3.org/2002/07/owl#intersectionOf";

    /// owl:unionOf IRI
    pub const UNION_OF: &str = "http://www.w3.org/2002/07/owl#unionOf";

    /// owl:oneOf IRI
    pub const ONE_OF: &str = "http://www.w3.org/2002/07/owl#oneOf";

    /// owl:Ontology IRI
    pub const ONTOLOGY: &str = "http://www.w3.org/2002/07/owl#Ontology";

    /// owl:imports IRI
    pub const IMPORTS: &str = "http://www.w3.org/2002/07/owl#imports";

    /// owl:Class IRI
    pub const CLASS: &str = "http://www.w3.org/2002/07/owl#Class";

    /// owl:ObjectProperty IRI
    pub const OBJECT_PROPERTY: &str = "http://www.w3.org/2002/07/owl#ObjectProperty";

    /// owl:DatatypeProperty IRI
    pub const DATATYPE_PROPERTY: &str = "http://www.w3.org/2002/07/owl#DatatypeProperty";
}

/// OWL local names (for SID construction)
pub mod owl_names {
    /// owl:inverseOf local name
    pub const INVERSE_OF: &str = "inverseOf";

    /// owl:equivalentClass local name
    pub const EQUIVALENT_CLASS: &str = "equivalentClass";

    /// owl:equivalentProperty local name
    pub const EQUIVALENT_PROPERTY: &str = "equivalentProperty";

    /// owl:sameAs local name
    pub const SAME_AS: &str = "sameAs";

    /// owl:SymmetricProperty local name
    pub const SYMMETRIC_PROPERTY: &str = "SymmetricProperty";

    /// owl:TransitiveProperty local name
    pub const TRANSITIVE_PROPERTY: &str = "TransitiveProperty";

    /// owl:FunctionalProperty local name
    pub const FUNCTIONAL_PROPERTY: &str = "FunctionalProperty";

    /// owl:InverseFunctionalProperty local name
    pub const INVERSE_FUNCTIONAL_PROPERTY: &str = "InverseFunctionalProperty";

    /// owl:propertyChainAxiom local name
    pub const PROPERTY_CHAIN_AXIOM: &str = "propertyChainAxiom";

    /// owl:hasKey local name
    pub const HAS_KEY: &str = "hasKey";

    /// owl:Restriction local name
    pub const RESTRICTION: &str = "Restriction";

    /// owl:onProperty local name
    pub const ON_PROPERTY: &str = "onProperty";

    /// owl:hasValue local name
    pub const HAS_VALUE: &str = "hasValue";

    /// owl:someValuesFrom local name
    pub const SOME_VALUES_FROM: &str = "someValuesFrom";

    /// owl:allValuesFrom local name
    pub const ALL_VALUES_FROM: &str = "allValuesFrom";

    /// owl:maxCardinality local name
    pub const MAX_CARDINALITY: &str = "maxCardinality";

    /// owl:maxQualifiedCardinality local name
    pub const MAX_QUALIFIED_CARDINALITY: &str = "maxQualifiedCardinality";

    /// owl:onClass local name
    pub const ON_CLASS: &str = "onClass";

    /// owl:intersectionOf local name
    pub const INTERSECTION_OF: &str = "intersectionOf";

    /// owl:unionOf local name
    pub const UNION_OF: &str = "unionOf";

    /// owl:oneOf local name
    pub const ONE_OF: &str = "oneOf";

    /// owl:Ontology local name
    pub const ONTOLOGY: &str = "Ontology";

    /// owl:imports local name
    pub const IMPORTS: &str = "imports";

    /// owl:Class local name
    pub const CLASS: &str = "Class";

    /// owl:ObjectProperty local name
    pub const OBJECT_PROPERTY: &str = "ObjectProperty";

    /// owl:DatatypeProperty local name
    pub const DATATYPE_PROPERTY: &str = "DatatypeProperty";
}

/// JSON-LD keyword local names (for SID construction)
///
/// JSON-LD keywords like `@id`, `@type`, `@value` are encoded with namespace code 1 (JSON_LD).
/// These constants provide the local name portion for constructing SIDs.
pub mod jsonld_names {
    /// JSON-LD @id keyword local name
    ///
    /// Used as the datatype for Ref values (IRI references).
    /// SID construction: `Sid::new(namespaces::JSON_LD, ID)` → `$id`
    pub const ID: &str = "id";

    /// JSON-LD @type keyword local name
    pub const TYPE: &str = "type";

    /// JSON-LD @value keyword local name
    pub const VALUE: &str = "value";

    /// JSON-LD @language keyword local name
    pub const LANGUAGE: &str = "language";

    /// JSON-LD @graph keyword local name
    pub const GRAPH: &str = "graph";

    /// JSON-LD @context keyword local name
    pub const CONTEXT: &str = "context";
}

/// SHACL vocabulary constants
pub mod shacl {
    /// SHACL namespace IRI
    pub const NS: &str = "http://www.w3.org/ns/shacl#";

    // ========================================================================
    // Shape Classes
    // ========================================================================

    /// sh:NodeShape IRI
    pub const NODE_SHAPE: &str = "http://www.w3.org/ns/shacl#NodeShape";

    /// sh:PropertyShape IRI
    pub const PROPERTY_SHAPE: &str = "http://www.w3.org/ns/shacl#PropertyShape";

    // ========================================================================
    // Targeting
    // ========================================================================

    /// sh:targetClass IRI
    pub const TARGET_CLASS: &str = "http://www.w3.org/ns/shacl#targetClass";

    /// sh:targetNode IRI
    pub const TARGET_NODE: &str = "http://www.w3.org/ns/shacl#targetNode";

    /// sh:targetSubjectsOf IRI
    pub const TARGET_SUBJECTS_OF: &str = "http://www.w3.org/ns/shacl#targetSubjectsOf";

    /// sh:targetObjectsOf IRI
    pub const TARGET_OBJECTS_OF: &str = "http://www.w3.org/ns/shacl#targetObjectsOf";

    // ========================================================================
    // Property Shape
    // ========================================================================

    /// sh:property IRI
    pub const PROPERTY: &str = "http://www.w3.org/ns/shacl#property";

    /// sh:path IRI
    pub const PATH: &str = "http://www.w3.org/ns/shacl#path";

    // ========================================================================
    // Cardinality Constraints
    // ========================================================================

    /// sh:minCount IRI
    pub const MIN_COUNT: &str = "http://www.w3.org/ns/shacl#minCount";

    /// sh:maxCount IRI
    pub const MAX_COUNT: &str = "http://www.w3.org/ns/shacl#maxCount";

    // ========================================================================
    // Value Type Constraints
    // ========================================================================

    /// sh:datatype IRI
    pub const DATATYPE: &str = "http://www.w3.org/ns/shacl#datatype";

    /// sh:nodeKind IRI
    pub const NODE_KIND: &str = "http://www.w3.org/ns/shacl#nodeKind";

    /// sh:class IRI
    pub const CLASS: &str = "http://www.w3.org/ns/shacl#class";

    // ========================================================================
    // Value Range Constraints
    // ========================================================================

    /// sh:minInclusive IRI
    pub const MIN_INCLUSIVE: &str = "http://www.w3.org/ns/shacl#minInclusive";

    /// sh:maxInclusive IRI
    pub const MAX_INCLUSIVE: &str = "http://www.w3.org/ns/shacl#maxInclusive";

    /// sh:minExclusive IRI
    pub const MIN_EXCLUSIVE: &str = "http://www.w3.org/ns/shacl#minExclusive";

    /// sh:maxExclusive IRI
    pub const MAX_EXCLUSIVE: &str = "http://www.w3.org/ns/shacl#maxExclusive";

    // ========================================================================
    // String Constraints
    // ========================================================================

    /// sh:pattern IRI
    pub const PATTERN: &str = "http://www.w3.org/ns/shacl#pattern";

    /// sh:flags IRI
    pub const FLAGS: &str = "http://www.w3.org/ns/shacl#flags";

    /// sh:minLength IRI
    pub const MIN_LENGTH: &str = "http://www.w3.org/ns/shacl#minLength";

    /// sh:maxLength IRI
    pub const MAX_LENGTH: &str = "http://www.w3.org/ns/shacl#maxLength";

    // ========================================================================
    // Value Constraints
    // ========================================================================

    /// sh:hasValue IRI
    pub const HAS_VALUE: &str = "http://www.w3.org/ns/shacl#hasValue";

    /// sh:in IRI
    pub const IN: &str = "http://www.w3.org/ns/shacl#in";

    // ========================================================================
    // Pair Constraints
    // ========================================================================

    /// sh:equals IRI
    pub const EQUALS: &str = "http://www.w3.org/ns/shacl#equals";

    /// sh:disjoint IRI
    pub const DISJOINT: &str = "http://www.w3.org/ns/shacl#disjoint";

    /// sh:lessThan IRI
    pub const LESS_THAN: &str = "http://www.w3.org/ns/shacl#lessThan";

    /// sh:lessThanOrEquals IRI
    pub const LESS_THAN_OR_EQUALS: &str = "http://www.w3.org/ns/shacl#lessThanOrEquals";

    // ========================================================================
    // Closed Shape Constraints
    // ========================================================================

    /// sh:closed IRI
    pub const CLOSED: &str = "http://www.w3.org/ns/shacl#closed";

    /// sh:ignoredProperties IRI
    pub const IGNORED_PROPERTIES: &str = "http://www.w3.org/ns/shacl#ignoredProperties";

    // ========================================================================
    // Logical Constraints
    // ========================================================================

    /// sh:not IRI
    pub const NOT: &str = "http://www.w3.org/ns/shacl#not";

    /// sh:and IRI
    pub const AND: &str = "http://www.w3.org/ns/shacl#and";

    /// sh:or IRI
    pub const OR: &str = "http://www.w3.org/ns/shacl#or";

    /// sh:xone IRI
    pub const XONE: &str = "http://www.w3.org/ns/shacl#xone";

    // ========================================================================
    // Qualified Value Shape
    // ========================================================================

    /// sh:qualifiedValueShape IRI
    pub const QUALIFIED_VALUE_SHAPE: &str = "http://www.w3.org/ns/shacl#qualifiedValueShape";

    /// sh:qualifiedMinCount IRI
    pub const QUALIFIED_MIN_COUNT: &str = "http://www.w3.org/ns/shacl#qualifiedMinCount";

    /// sh:qualifiedMaxCount IRI
    pub const QUALIFIED_MAX_COUNT: &str = "http://www.w3.org/ns/shacl#qualifiedMaxCount";

    /// sh:qualifiedValueShapesDisjoint IRI
    pub const QUALIFIED_VALUE_SHAPES_DISJOINT: &str =
        "http://www.w3.org/ns/shacl#qualifiedValueShapesDisjoint";

    // ========================================================================
    // Language Constraints
    // ========================================================================

    /// sh:uniqueLang IRI
    pub const UNIQUE_LANG: &str = "http://www.w3.org/ns/shacl#uniqueLang";

    /// sh:languageIn IRI
    pub const LANGUAGE_IN: &str = "http://www.w3.org/ns/shacl#languageIn";

    // ========================================================================
    // Node Kind Values
    // ========================================================================

    /// sh:BlankNode IRI
    pub const BLANK_NODE: &str = "http://www.w3.org/ns/shacl#BlankNode";

    /// sh:IRI IRI
    pub const IRI: &str = "http://www.w3.org/ns/shacl#IRI";

    /// sh:Literal IRI
    pub const LITERAL: &str = "http://www.w3.org/ns/shacl#Literal";

    /// sh:BlankNodeOrIRI IRI
    pub const BLANK_NODE_OR_IRI: &str = "http://www.w3.org/ns/shacl#BlankNodeOrIRI";

    /// sh:BlankNodeOrLiteral IRI
    pub const BLANK_NODE_OR_LITERAL: &str = "http://www.w3.org/ns/shacl#BlankNodeOrLiteral";

    /// sh:IRIOrLiteral IRI
    pub const IRI_OR_LITERAL: &str = "http://www.w3.org/ns/shacl#IRIOrLiteral";

    // ========================================================================
    // Severity Levels
    // ========================================================================

    /// sh:severity IRI
    pub const SEVERITY: &str = "http://www.w3.org/ns/shacl#severity";

    /// sh:Violation IRI
    pub const VIOLATION: &str = "http://www.w3.org/ns/shacl#Violation";

    /// sh:Warning IRI
    pub const WARNING: &str = "http://www.w3.org/ns/shacl#Warning";

    /// sh:Info IRI
    pub const INFO: &str = "http://www.w3.org/ns/shacl#Info";

    // ========================================================================
    // Result Reporting
    // ========================================================================

    /// sh:message IRI
    pub const MESSAGE: &str = "http://www.w3.org/ns/shacl#message";

    /// sh:name IRI
    pub const NAME: &str = "http://www.w3.org/ns/shacl#name";

    /// sh:description IRI
    pub const DESCRIPTION: &str = "http://www.w3.org/ns/shacl#description";

    // ========================================================================
    // Validation Report
    // ========================================================================

    /// sh:ValidationReport IRI
    pub const VALIDATION_REPORT: &str = "http://www.w3.org/ns/shacl#ValidationReport";

    /// sh:ValidationResult IRI
    pub const VALIDATION_RESULT: &str = "http://www.w3.org/ns/shacl#ValidationResult";

    /// sh:conforms IRI
    pub const CONFORMS: &str = "http://www.w3.org/ns/shacl#conforms";

    /// sh:result IRI
    pub const RESULT: &str = "http://www.w3.org/ns/shacl#result";

    /// sh:focusNode IRI
    pub const FOCUS_NODE: &str = "http://www.w3.org/ns/shacl#focusNode";

    /// sh:resultPath IRI
    pub const RESULT_PATH: &str = "http://www.w3.org/ns/shacl#resultPath";

    /// sh:value IRI
    pub const VALUE: &str = "http://www.w3.org/ns/shacl#value";

    /// sh:sourceShape IRI
    pub const SOURCE_SHAPE: &str = "http://www.w3.org/ns/shacl#sourceShape";

    /// sh:sourceConstraintComponent IRI
    pub const SOURCE_CONSTRAINT_COMPONENT: &str =
        "http://www.w3.org/ns/shacl#sourceConstraintComponent";

    /// sh:resultSeverity IRI
    pub const RESULT_SEVERITY: &str = "http://www.w3.org/ns/shacl#resultSeverity";

    /// sh:resultMessage IRI
    pub const RESULT_MESSAGE: &str = "http://www.w3.org/ns/shacl#resultMessage";
}

/// SHACL vocabulary local names (for SID construction)
///
/// SHACL terms are encoded with namespace code 5 (SHACL).
/// These constants provide the local name portion for constructing SIDs.
///
/// # Example
/// ```
/// use fluree_vocab::shacl_names;
///
/// assert_eq!(shacl_names::TARGET_CLASS, "targetClass");
/// assert_eq!(shacl_names::MIN_COUNT, "minCount");
/// ```
pub mod shacl_names {
    // ========================================================================
    // Shape Classes
    // ========================================================================

    /// sh:NodeShape local name
    pub const NODE_SHAPE: &str = "NodeShape";

    /// sh:PropertyShape local name
    pub const PROPERTY_SHAPE: &str = "PropertyShape";

    // ========================================================================
    // Targeting
    // ========================================================================

    /// sh:targetClass local name
    pub const TARGET_CLASS: &str = "targetClass";

    /// sh:targetNode local name
    pub const TARGET_NODE: &str = "targetNode";

    /// sh:targetSubjectsOf local name
    pub const TARGET_SUBJECTS_OF: &str = "targetSubjectsOf";

    /// sh:targetObjectsOf local name
    pub const TARGET_OBJECTS_OF: &str = "targetObjectsOf";

    // ========================================================================
    // Property Shape
    // ========================================================================

    /// sh:property local name
    pub const PROPERTY: &str = "property";

    /// sh:path local name
    pub const PATH: &str = "path";

    // ========================================================================
    // Cardinality Constraints
    // ========================================================================

    /// sh:minCount local name
    pub const MIN_COUNT: &str = "minCount";

    /// sh:maxCount local name
    pub const MAX_COUNT: &str = "maxCount";

    // ========================================================================
    // Value Type Constraints
    // ========================================================================

    /// sh:datatype local name
    pub const DATATYPE: &str = "datatype";

    /// sh:nodeKind local name
    pub const NODE_KIND: &str = "nodeKind";

    /// sh:class local name
    pub const CLASS: &str = "class";

    // ========================================================================
    // Value Range Constraints
    // ========================================================================

    /// sh:minInclusive local name
    pub const MIN_INCLUSIVE: &str = "minInclusive";

    /// sh:maxInclusive local name
    pub const MAX_INCLUSIVE: &str = "maxInclusive";

    /// sh:minExclusive local name
    pub const MIN_EXCLUSIVE: &str = "minExclusive";

    /// sh:maxExclusive local name
    pub const MAX_EXCLUSIVE: &str = "maxExclusive";

    // ========================================================================
    // String Constraints
    // ========================================================================

    /// sh:pattern local name
    pub const PATTERN: &str = "pattern";

    /// sh:flags local name
    pub const FLAGS: &str = "flags";

    /// sh:minLength local name
    pub const MIN_LENGTH: &str = "minLength";

    /// sh:maxLength local name
    pub const MAX_LENGTH: &str = "maxLength";

    // ========================================================================
    // Value Constraints
    // ========================================================================

    /// sh:hasValue local name
    pub const HAS_VALUE: &str = "hasValue";

    /// sh:in local name
    pub const IN: &str = "in";

    // ========================================================================
    // Pair Constraints
    // ========================================================================

    /// sh:equals local name
    pub const EQUALS: &str = "equals";

    /// sh:disjoint local name
    pub const DISJOINT: &str = "disjoint";

    /// sh:lessThan local name
    pub const LESS_THAN: &str = "lessThan";

    /// sh:lessThanOrEquals local name
    pub const LESS_THAN_OR_EQUALS: &str = "lessThanOrEquals";

    // ========================================================================
    // Closed Shape Constraints
    // ========================================================================

    /// sh:closed local name
    pub const CLOSED: &str = "closed";

    /// sh:ignoredProperties local name
    pub const IGNORED_PROPERTIES: &str = "ignoredProperties";

    // ========================================================================
    // Logical Constraints
    // ========================================================================

    /// sh:not local name
    pub const NOT: &str = "not";

    /// sh:and local name
    pub const AND: &str = "and";

    /// sh:or local name
    pub const OR: &str = "or";

    /// sh:xone local name
    pub const XONE: &str = "xone";

    // ========================================================================
    // Qualified Value Shape
    // ========================================================================

    /// sh:qualifiedValueShape local name
    pub const QUALIFIED_VALUE_SHAPE: &str = "qualifiedValueShape";

    /// sh:qualifiedMinCount local name
    pub const QUALIFIED_MIN_COUNT: &str = "qualifiedMinCount";

    /// sh:qualifiedMaxCount local name
    pub const QUALIFIED_MAX_COUNT: &str = "qualifiedMaxCount";

    /// sh:qualifiedValueShapesDisjoint local name
    pub const QUALIFIED_VALUE_SHAPES_DISJOINT: &str = "qualifiedValueShapesDisjoint";

    // ========================================================================
    // Language Constraints
    // ========================================================================

    /// sh:uniqueLang local name
    pub const UNIQUE_LANG: &str = "uniqueLang";

    /// sh:languageIn local name
    pub const LANGUAGE_IN: &str = "languageIn";

    // ========================================================================
    // Node Kind Values
    // ========================================================================

    /// sh:BlankNode local name
    pub const BLANK_NODE: &str = "BlankNode";

    /// sh:IRI local name
    pub const IRI: &str = "IRI";

    /// sh:Literal local name
    pub const LITERAL: &str = "Literal";

    /// sh:BlankNodeOrIRI local name
    pub const BLANK_NODE_OR_IRI: &str = "BlankNodeOrIRI";

    /// sh:BlankNodeOrLiteral local name
    pub const BLANK_NODE_OR_LITERAL: &str = "BlankNodeOrLiteral";

    /// sh:IRIOrLiteral local name
    pub const IRI_OR_LITERAL: &str = "IRIOrLiteral";

    // ========================================================================
    // Severity Levels
    // ========================================================================

    /// sh:severity local name
    pub const SEVERITY: &str = "severity";

    /// sh:Violation local name
    pub const VIOLATION: &str = "Violation";

    /// sh:Warning local name
    pub const WARNING: &str = "Warning";

    /// sh:Info local name
    pub const INFO: &str = "Info";

    // ========================================================================
    // Result Reporting
    // ========================================================================

    /// sh:message local name
    pub const MESSAGE: &str = "message";

    /// sh:name local name
    pub const NAME: &str = "name";

    /// sh:description local name
    pub const DESCRIPTION: &str = "description";

    // ========================================================================
    // Validation Report
    // ========================================================================

    /// sh:ValidationReport local name
    pub const VALIDATION_REPORT: &str = "ValidationReport";

    /// sh:ValidationResult local name
    pub const VALIDATION_RESULT: &str = "ValidationResult";

    /// sh:conforms local name
    pub const CONFORMS: &str = "conforms";

    /// sh:result local name
    pub const RESULT: &str = "result";

    /// sh:focusNode local name
    pub const FOCUS_NODE: &str = "focusNode";

    /// sh:resultPath local name
    pub const RESULT_PATH: &str = "resultPath";

    /// sh:value local name
    pub const VALUE: &str = "value";

    /// sh:sourceShape local name
    pub const SOURCE_SHAPE: &str = "sourceShape";

    /// sh:sourceConstraintComponent local name
    pub const SOURCE_CONSTRAINT_COMPONENT: &str = "sourceConstraintComponent";

    /// sh:resultSeverity local name
    pub const RESULT_SEVERITY: &str = "resultSeverity";

    /// sh:resultMessage local name
    pub const RESULT_MESSAGE: &str = "resultMessage";
}

/// Fluree-specific vocabulary constants
pub mod fluree {
    /// Fluree DB system namespace IRI (canonical base for all Fluree system vocabulary)
    pub const DB: &str = "https://ns.flur.ee/db#";

    /// Fluree URN prefix for ledger-scoped identifiers.
    ///
    /// Used as a namespace prefix so `encode_iri` can decompose ledger-scoped IRIs.
    /// e.g., `urn:fluree:mydb:main#txn-meta` → `Sid::new(FLUREE_URN, "mydb:main#txn-meta")`
    pub const URN: &str = "urn:fluree:";

    /// db:rule IRI - datalog rule definition predicate
    pub const RULE: &str = "https://ns.flur.ee/db#rule";

    /// Fluree commit subject identifier scheme (not a predicate vocabulary)
    pub const COMMIT: &str = "fluree:commit:sha256:";

    /// db:embeddingVector datatype IRI (f32-precision embedding vectors)
    /// The `@vector` shorthand in JSON-LD resolves to this IRI.
    pub const EMBEDDING_VECTOR: &str = "https://ns.flur.ee/db#embeddingVector";

    /// db:fullText datatype IRI (inline full-text search literals)
    /// The `@fulltext` shorthand in JSON-LD resolves to this IRI.
    pub const FULL_TEXT: &str = "https://ns.flur.ee/db#fullText";

    /// Full IRI for db:t predicate (used in RDF-Star annotation matching)
    pub const DB_T: &str = "https://ns.flur.ee/db#t";

    /// Full IRI for db:op predicate (used in RDF-Star annotation matching)
    pub const DB_OP: &str = "https://ns.flur.ee/db#op";

    /// "This commit" placeholder IRI (HTTP form, expands from `fluree:commit:this` with db# context)
    pub const COMMIT_THIS_HTTP: &str = "https://ns.flur.ee/db#commit:this";

    /// "This commit" placeholder IRI (scheme form, used without prefix definition)
    pub const COMMIT_THIS_SCHEME: &str = "fluree:commit:this";
}

/// Namespace codes for IRI encoding
///
/// Codes 0 through `USER_START - 1` are reserved for built-in namespaces.
/// User-defined namespaces are allocated contiguously starting at `USER_START`.
/// Code `OVERFLOW` (0xFFFF) is reserved for IRIs whose namespace could not
/// be assigned a code (full IRI stored as the SID name).
pub mod namespaces {
    /// Code 0: empty / relative IRI prefix (@base resolution)
    pub const EMPTY: u16 = 0;

    /// Code 1: JSON-LD keywords / internal "@" namespace
    pub const JSON_LD: u16 = 1;

    /// Code 2: XSD datatypes
    pub const XSD: u16 = 2;

    /// Code 3: RDF
    pub const RDF: u16 = 3;

    /// Code 4: RDFS
    pub const RDFS: u16 = 4;

    /// Code 5: SHACL
    pub const SHACL: u16 = 5;

    /// Code 6: OWL
    pub const OWL: u16 = 6;

    /// Code 7: Fluree DB system namespace (https://ns.flur.ee/db#)
    pub const FLUREE_DB: u16 = 7;

    /// Code 8: DID key prefix
    pub const DID_KEY: u16 = 8;

    /// Code 9: Fluree commit content address prefix
    pub const FLUREE_COMMIT: u16 = 9;

    /// Code 10: blank nodes (_:)
    pub const BLANK_NODE: u16 = 10;

    /// Code 11: OGC GeoSPARQL namespace (geo:)
    pub const OGC_GEO: u16 = 11;

    /// Code 12: Fluree URN prefix (urn:fluree:) for ledger-scoped identifiers.
    ///
    /// Used for txn-meta graph IRIs: `urn:fluree:{ledger_id}#txn-meta`.
    /// `encode_iri` decomposes as `Sid::new(FLUREE_URN, "{ledger_id}#txn-meta")`.
    pub const FLUREE_URN: u16 = 12;

    /// First code available for user-defined namespaces.
    /// Built-in codes occupy 0..=12.
    pub const USER_START: u16 = 13;

    /// Overflow namespace code (0xFFFE).
    /// Assigned when all user codes are exhausted. The SID name stores the
    /// full IRI (no prefix stripping).
    ///
    /// Note: 0xFFFF is reserved for `Sid::max()` sentinel, so overflow
    /// uses 0xFFFE.
    pub const OVERFLOW: u16 = 0xFFFE;
}

/// Common predicate local names (for schema extraction, validation, etc.)
pub mod predicates {
    /// rdf:type local name
    pub const RDF_TYPE: &str = "type";

    /// rdf:first local name (RDF list head element)
    pub const RDF_FIRST: &str = "first";

    /// rdf:rest local name (RDF list tail)
    pub const RDF_REST: &str = "rest";

    /// rdf:nil local name (RDF list terminator)
    pub const RDF_NIL: &str = "nil";

    /// rdfs:subClassOf local name
    pub const RDFS_SUBCLASSOF: &str = "subClassOf";

    /// rdfs:subPropertyOf local name
    pub const RDFS_SUBPROPERTYOF: &str = "subPropertyOf";

    /// rdfs:domain local name
    pub const RDFS_DOMAIN: &str = "domain";

    /// rdfs:range local name
    pub const RDFS_RANGE: &str = "range";

    /// owl:inverseOf local name
    pub const OWL_INVERSEOF: &str = "inverseOf";

    /// owl:equivalentClass local name
    pub const OWL_EQUIVALENTCLASS: &str = "equivalentClass";

    /// owl:equivalentProperty local name
    pub const OWL_EQUIVALENTPROPERTY: &str = "equivalentProperty";

    /// owl:sameAs local name
    pub const OWL_SAMEAS: &str = "sameAs";

    /// owl:SymmetricProperty local name (class, not predicate)
    pub const OWL_SYMMETRICPROPERTY: &str = "SymmetricProperty";

    /// owl:TransitiveProperty local name (class, not predicate)
    pub const OWL_TRANSITIVEPROPERTY: &str = "TransitiveProperty";

    /// owl:FunctionalProperty local name (class, not predicate)
    pub const OWL_FUNCTIONALPROPERTY: &str = "FunctionalProperty";

    /// owl:InverseFunctionalProperty local name (class, not predicate)
    pub const OWL_INVERSEFUNCTIONALPROPERTY: &str = "InverseFunctionalProperty";

    /// owl:imports local name
    pub const OWL_IMPORTS: &str = "imports";

    /// owl:Ontology local name (class)
    pub const OWL_ONTOLOGY: &str = "Ontology";

    /// owl:Class local name (class)
    pub const OWL_CLASS: &str = "Class";

    /// owl:ObjectProperty local name (class)
    pub const OWL_OBJECTPROPERTY: &str = "ObjectProperty";

    /// owl:DatatypeProperty local name (class)
    pub const OWL_DATATYPEPROPERTY: &str = "DatatypeProperty";

    /// rdf:Property local name (class)
    pub const RDF_PROPERTY: &str = "Property";
}

/// Fluree DB namespace predicate local names (for SID construction)
///
/// These are local name constants under `https://ns.flur.ee/db#`.
/// Used with `Sid::new(FLUREE_DB, db::FIELD)` for commit metadata flakes.
pub mod db {
    /// db:address - storage address (commit or DB snapshot)
    pub const ADDRESS: &str = "address";

    /// db:alias - ledger alias (used in commit metadata)
    pub const ALIAS: &str = "alias";

    /// db:v - version number
    pub const V: &str = "v";

    /// db:previous - reference to previous commit
    pub const PREVIOUS: &str = "previous";

    /// db:time - commit timestamp (epoch milliseconds)
    pub const TIME: &str = "time";

    /// db:message - commit message (optional)
    pub const MESSAGE: &str = "message";

    /// db:author - commit author (optional, user claim)
    pub const AUTHOR: &str = "author";

    /// db:identity - authenticated/impersonated identity acting on the transaction.
    /// System-controlled: derived from signed credential DID or `opts.identity`.
    pub const IDENTITY: &str = "identity";

    /// db:txn - transaction address (optional)
    pub const TXN: &str = "txn";

    /// db:t - transaction number (watermark)
    pub const T: &str = "t";

    /// db:size - data size in bytes (cumulative on DB data, per-commit on txn-meta)
    pub const SIZE: &str = "size";

    /// db:flakes - cumulative flake count (on DB data subject in default graph)
    pub const FLAKES: &str = "flakes";

    /// db:asserts - number of assertions in this commit (txn-meta graph)
    pub const ASSERTS: &str = "asserts";

    /// db:retracts - number of retractions in this commit (txn-meta graph)
    pub const RETRACTS: &str = "retracts";

    /// db:rule - datalog rule definition
    pub const RULE: &str = "rule";

    /// db:op - operation type in RDF-Star annotations (assert/retract)
    pub const OP: &str = "op";

    /// db:ledgerCommit - nameservice field: pointer to latest ledger commit address
    pub const LEDGER_COMMIT: &str = "ledgerCommit";

    /// db:ledgerIndex - nameservice field: pointer to latest ledger index root
    pub const LEDGER_INDEX: &str = "ledgerIndex";
}

/// Fluree DB search query key local names (under `https://ns.flur.ee/db#`)
///
/// These are the local names for BM25/vector search pattern keys in queries.
/// Users must declare `"f": "https://ns.flur.ee/db#"` in their `@context`
/// and use keys like `"f:searchText"`, or use the full IRI directly.
pub mod search {
    /// db:searchText - BM25 search query text
    pub const SEARCH_TEXT: &str = "searchText";

    /// db:searchLimit - maximum number of search results
    pub const SEARCH_LIMIT: &str = "searchLimit";

    /// db:searchResult - result binding specification (variable or nested object)
    pub const SEARCH_RESULT: &str = "searchResult";

    /// db:resultId - document ID binding in result pattern
    pub const RESULT_ID: &str = "resultId";

    /// db:resultScore - score binding in result pattern
    pub const RESULT_SCORE: &str = "resultScore";

    /// db:resultLedger - ledger alias binding for multi-ledger disambiguation
    pub const RESULT_LEDGER: &str = "resultLedger";

    /// db:syncBeforeQuery - synchronization mode (true = sync before query)
    pub const SYNC_BEFORE_QUERY: &str = "syncBeforeQuery";

    /// db:timeoutMs - query timeout in milliseconds
    pub const TIMEOUT_MS: &str = "timeoutMs";

    /// db:queryVector - query vector for similarity search
    pub const QUERY_VECTOR: &str = "queryVector";

    /// db:distanceMetric - distance metric for vector search (cosine, dot, euclidean)
    pub const DISTANCE_METRIC: &str = "distanceMetric";

    /// db:graphSource - graph source alias for search patterns
    pub const GRAPH_SOURCE: &str = "graphSource";

    /// BM25 k1 parameter (term frequency saturation) - config key
    pub const BM25_K1: &str = "k1";

    /// BM25 b parameter (document length normalization) - config key
    pub const BM25_B: &str = "b";

    /// Property to index for BM25 - config key
    pub const PROPERTY: &str = "property";
}

/// Full IRI constants for search query keys
///
/// Used by the query parser for IRI-based pattern matching after JSON-LD expansion.
pub mod search_iris {
    /// `https://ns.flur.ee/db#searchText`
    pub const SEARCH_TEXT: &str = "https://ns.flur.ee/db#searchText";

    /// `https://ns.flur.ee/db#searchLimit`
    pub const SEARCH_LIMIT: &str = "https://ns.flur.ee/db#searchLimit";

    /// `https://ns.flur.ee/db#searchResult`
    pub const SEARCH_RESULT: &str = "https://ns.flur.ee/db#searchResult";

    /// `https://ns.flur.ee/db#resultId`
    pub const RESULT_ID: &str = "https://ns.flur.ee/db#resultId";

    /// `https://ns.flur.ee/db#resultScore`
    pub const RESULT_SCORE: &str = "https://ns.flur.ee/db#resultScore";

    /// `https://ns.flur.ee/db#resultLedger`
    pub const RESULT_LEDGER: &str = "https://ns.flur.ee/db#resultLedger";

    /// `https://ns.flur.ee/db#syncBeforeQuery`
    pub const SYNC_BEFORE_QUERY: &str = "https://ns.flur.ee/db#syncBeforeQuery";

    /// `https://ns.flur.ee/db#timeoutMs`
    pub const TIMEOUT_MS: &str = "https://ns.flur.ee/db#timeoutMs";

    /// `https://ns.flur.ee/db#queryVector`
    pub const QUERY_VECTOR: &str = "https://ns.flur.ee/db#queryVector";

    /// `https://ns.flur.ee/db#distanceMetric`
    pub const DISTANCE_METRIC: &str = "https://ns.flur.ee/db#distanceMetric";

    /// `https://ns.flur.ee/db#graphSource`
    pub const GRAPH_SOURCE: &str = "https://ns.flur.ee/db#graphSource";
}

/// Nameservice record type IRIs (full IRIs for `@type` values)
pub mod ns_types {
    /// `https://ns.flur.ee/db#LedgerSource` - ledger-backed knowledge graph
    pub const LEDGER_SOURCE: &str = "https://ns.flur.ee/db#LedgerSource";

    /// `https://ns.flur.ee/db#IndexSource` - index-backed graph source (BM25/HNSW/GEO)
    pub const INDEX_SOURCE: &str = "https://ns.flur.ee/db#IndexSource";

    /// `https://ns.flur.ee/db#MappedSource` - mapped database (Iceberg, etc.)
    pub const MAPPED_SOURCE: &str = "https://ns.flur.ee/db#MappedSource";

    /// `https://ns.flur.ee/db#Bm25Index` - BM25 full-text search index
    pub const BM25_INDEX: &str = "https://ns.flur.ee/db#Bm25Index";

    /// `https://ns.flur.ee/db#HnswIndex` - HNSW/vector similarity search index
    pub const HNSW_INDEX: &str = "https://ns.flur.ee/db#HnswIndex";

    /// `https://ns.flur.ee/db#GeoIndex` - geospatial index
    pub const GEO_INDEX: &str = "https://ns.flur.ee/db#GeoIndex";

    /// `https://ns.flur.ee/db#IcebergMapping` - Iceberg-mapped database
    pub const ICEBERG_MAPPING: &str = "https://ns.flur.ee/db#IcebergMapping";

    /// `https://ns.flur.ee/db#R2rmlMapping` - R2RML relational mapping
    pub const R2RML_MAPPING: &str = "https://ns.flur.ee/db#R2rmlMapping";
}

/// Graph source nameservice field local names (under `https://ns.flur.ee/db#`)
pub mod graph_source {
    /// db:graphSourceConfig - graph source configuration JSON
    pub const CONFIG: &str = "graphSourceConfig";

    /// db:graphSourceDependencies - dependent ledger aliases
    pub const DEPENDENCIES: &str = "graphSourceDependencies";

    /// db:graphSourceIndex - graph source index address
    pub const INDEX: &str = "graphSourceIndex";

    /// db:graphSourceIndexT - graph source index watermark (commit t value)
    pub const INDEX_T: &str = "graphSourceIndexT";

    /// db:graphSourceIndexAddress - graph source index address (string)
    pub const INDEX_ADDRESS: &str = "graphSourceIndexAddress";
}

/// Full IRI constants for Fluree policy vocabulary
///
/// Used by the policy builder for programmatic policy construction.
/// All IRIs are under `https://ns.flur.ee/db#`.
pub mod policy_iris {
    /// `https://ns.flur.ee/db#policyClass` - policy class marker
    pub const POLICY_CLASS: &str = "https://ns.flur.ee/db#policyClass";

    /// `https://ns.flur.ee/db#allow` - allow/deny flag
    pub const ALLOW: &str = "https://ns.flur.ee/db#allow";

    /// `https://ns.flur.ee/db#action` - action predicate (view/modify)
    pub const ACTION: &str = "https://ns.flur.ee/db#action";

    /// `https://ns.flur.ee/db#view` - view action IRI
    pub const VIEW: &str = "https://ns.flur.ee/db#view";

    /// `https://ns.flur.ee/db#modify` - modify action IRI
    pub const MODIFY: &str = "https://ns.flur.ee/db#modify";

    /// `https://ns.flur.ee/db#onProperty` - property-level targeting
    pub const ON_PROPERTY: &str = "https://ns.flur.ee/db#onProperty";

    /// `https://ns.flur.ee/db#onSubject` - subject-level targeting
    pub const ON_SUBJECT: &str = "https://ns.flur.ee/db#onSubject";

    /// `https://ns.flur.ee/db#onClass` - class-level targeting
    pub const ON_CLASS: &str = "https://ns.flur.ee/db#onClass";

    /// `https://ns.flur.ee/db#query` - policy query predicate
    pub const QUERY: &str = "https://ns.flur.ee/db#query";

    /// `https://ns.flur.ee/db#required` - required flag
    pub const REQUIRED: &str = "https://ns.flur.ee/db#required";

    /// `https://ns.flur.ee/db#exMessage` - exception/error message
    pub const EX_MESSAGE: &str = "https://ns.flur.ee/db#exMessage";
}

/// Full IRI constants for Fluree ledger config graph vocabulary
///
/// Used by the config resolver for reading/writing the config graph.
/// All IRIs are under `https://ns.flur.ee/db#`.
pub mod config_iris {
    // ---- Type classes ----

    /// `f:LedgerConfig` — ledger-wide configuration resource
    pub const LEDGER_CONFIG: &str = "https://ns.flur.ee/db#LedgerConfig";

    /// `f:GraphConfig` — per-graph configuration override
    pub const GRAPH_CONFIG: &str = "https://ns.flur.ee/db#GraphConfig";

    /// `f:GraphRef` — reference to a graph source (local or remote)
    pub const GRAPH_REF: &str = "https://ns.flur.ee/db#GraphRef";

    /// `f:TrustPolicy` — trust verification model for a GraphRef
    pub const TRUST_POLICY: &str = "https://ns.flur.ee/db#TrustPolicy";

    // ---- Setting group predicates ----

    /// `f:policyDefaults` — policy defaults object on LedgerConfig/GraphConfig
    pub const POLICY_DEFAULTS: &str = "https://ns.flur.ee/db#policyDefaults";

    /// `f:shaclDefaults` — SHACL validation defaults object
    pub const SHACL_DEFAULTS: &str = "https://ns.flur.ee/db#shaclDefaults";

    /// `f:reasoningDefaults` — reasoning defaults object
    pub const REASONING_DEFAULTS: &str = "https://ns.flur.ee/db#reasoningDefaults";

    /// `f:datalogDefaults` — datalog rules defaults object
    pub const DATALOG_DEFAULTS: &str = "https://ns.flur.ee/db#datalogDefaults";

    /// `f:graphOverrides` — list of per-graph config overrides
    pub const GRAPH_OVERRIDES: &str = "https://ns.flur.ee/db#graphOverrides";

    // ---- Policy fields ----

    /// `f:defaultAllow` — boolean, allow-all vs deny-all default
    pub const DEFAULT_ALLOW: &str = "https://ns.flur.ee/db#defaultAllow";

    /// `f:policySource` — GraphRef pointing to policy rules graph
    pub const POLICY_SOURCE: &str = "https://ns.flur.ee/db#policySource";

    // Note: f:policyClass is already defined in policy_iris::POLICY_CLASS

    // ---- SHACL fields ----

    /// `f:shaclEnabled` — boolean, enable/disable SHACL validation
    pub const SHACL_ENABLED: &str = "https://ns.flur.ee/db#shaclEnabled";

    /// `f:shapesSource` — GraphRef pointing to SHACL shapes graph
    pub const SHAPES_SOURCE: &str = "https://ns.flur.ee/db#shapesSource";

    /// `f:validationMode` — IRI: f:ValidationReject or f:ValidationWarn
    pub const VALIDATION_MODE: &str = "https://ns.flur.ee/db#validationMode";

    /// `f:ValidationReject` — reject transactions that fail SHACL validation
    pub const VALIDATION_REJECT: &str = "https://ns.flur.ee/db#ValidationReject";

    /// `f:ValidationWarn` — warn but allow transactions that fail SHACL validation
    pub const VALIDATION_WARN: &str = "https://ns.flur.ee/db#ValidationWarn";

    // ---- Reasoning fields ----

    /// `f:reasoningModes` — string or array of reasoning mode identifiers
    pub const REASONING_MODES: &str = "https://ns.flur.ee/db#reasoningModes";

    /// `f:schemaSource` — GraphRef pointing to schema hierarchy graph
    pub const SCHEMA_SOURCE: &str = "https://ns.flur.ee/db#schemaSource";

    /// `f:followOwlImports` — boolean, follow owl:imports closure from schemaSource
    pub const FOLLOW_OWL_IMPORTS: &str = "https://ns.flur.ee/db#followOwlImports";

    /// `f:ontologyImportMap` — list of OntologyImportBinding
    pub const ONTOLOGY_IMPORT_MAP: &str = "https://ns.flur.ee/db#ontologyImportMap";

    /// `f:ontologyIri` — the external ontology IRI being mapped
    pub const ONTOLOGY_IRI: &str = "https://ns.flur.ee/db#ontologyIri";

    /// `f:graphRef` — nested GraphRef inside an OntologyImportBinding
    pub const GRAPH_REF_PROP: &str = "https://ns.flur.ee/db#graphRef";

    // ---- Datalog fields ----

    /// `f:datalogEnabled` — boolean, enable/disable datalog rules
    pub const DATALOG_ENABLED: &str = "https://ns.flur.ee/db#datalogEnabled";

    /// `f:rulesSource` — GraphRef pointing to graph containing f:rule resources
    pub const RULES_SOURCE: &str = "https://ns.flur.ee/db#rulesSource";

    /// `f:allowQueryTimeRules` — boolean, allow query-time rule injection
    pub const ALLOW_QUERY_TIME_RULES: &str = "https://ns.flur.ee/db#allowQueryTimeRules";

    // ---- Override control ----

    /// `f:overrideControl` — override control policy (string or object)
    pub const OVERRIDE_CONTROL: &str = "https://ns.flur.ee/db#overrideControl";

    /// `f:controlMode` — mode within an identityRestricted override control object
    pub const CONTROL_MODE: &str = "https://ns.flur.ee/db#controlMode";

    /// `f:allowedIdentities` — list of DIDs permitted to override
    pub const ALLOWED_IDENTITIES: &str = "https://ns.flur.ee/db#allowedIdentities";

    // ---- GraphConfig fields ----

    /// `f:targetGraph` — IRI of the graph a GraphConfig applies to
    pub const TARGET_GRAPH: &str = "https://ns.flur.ee/db#targetGraph";

    /// `f:defaultGraph` — sentinel IRI representing the default graph (g_id=0)
    pub const DEFAULT_GRAPH: &str = "https://ns.flur.ee/db#defaultGraph";

    /// `f:txnMetaGraph` — sentinel IRI representing the txn-meta graph (g_id=1)
    pub const TXN_META_GRAPH: &str = "https://ns.flur.ee/db#txnMetaGraph";

    // ---- GraphRef / GraphSource fields ----

    /// `f:graphSource` — nested object describing the graph source
    pub const GRAPH_SOURCE: &str = "https://ns.flur.ee/db#graphSource";

    /// `f:ledger` — ledger identifier within a graphSource
    pub const LEDGER_PRED: &str = "https://ns.flur.ee/db#ledger";

    /// `f:graphSelector` — graph selector IRI within a graphSource
    pub const GRAPH_SELECTOR: &str = "https://ns.flur.ee/db#graphSelector";

    // ---- TrustPolicy fields ----

    /// `f:trustPolicy` — trust policy object on a GraphRef
    pub const TRUST_POLICY_PRED: &str = "https://ns.flur.ee/db#trustPolicy";

    /// `f:trustMode` — trust verification mode IRI
    pub const TRUST_MODE: &str = "https://ns.flur.ee/db#trustMode";

    /// `f:Trusted` — accept nameservice head without additional validation
    pub const TRUSTED: &str = "https://ns.flur.ee/db#Trusted";

    /// `f:SignedIndex` — verify signed index root
    pub const SIGNED_INDEX: &str = "https://ns.flur.ee/db#SignedIndex";

    /// `f:CommitVerified` — full commit chain verification
    pub const COMMIT_VERIFIED: &str = "https://ns.flur.ee/db#CommitVerified";

    // ---- Override control mode values ----

    /// `f:OverrideNone` — no overrides permitted, regardless of identity.
    pub const OVERRIDE_NONE: &str = "https://ns.flur.ee/db#OverrideNone";

    /// `f:OverrideAll` — any request can override.
    pub const OVERRIDE_ALL: &str = "https://ns.flur.ee/db#OverrideAll";

    /// `f:IdentityRestricted` — only requests with verified matching identity can override.
    pub const IDENTITY_RESTRICTED: &str = "https://ns.flur.ee/db#IdentityRestricted";

    // ---- Temporal predicates ----

    /// `f:atT` — pin a GraphRef to a specific commit number.
    pub const AT_T: &str = "https://ns.flur.ee/db#atT";

    // ---- Rollback guard ----

    /// `f:rollbackGuard` — freshness constraints for a GraphRef.
    pub const ROLLBACK_GUARD: &str = "https://ns.flur.ee/db#rollbackGuard";

    /// `f:minT` — reject any resolved head where `head_t < minT`.
    pub const MIN_T: &str = "https://ns.flur.ee/db#minT";

    // ---- Transact defaults fields ----

    /// `f:transactDefaults` — transact-time constraint defaults on LedgerConfig/GraphConfig.
    pub const TRANSACT_DEFAULTS: &str = "https://ns.flur.ee/db#transactDefaults";

    /// `f:uniqueEnabled` — boolean, enable unique constraint enforcement.
    pub const UNIQUE_ENABLED: &str = "https://ns.flur.ee/db#uniqueEnabled";

    /// `f:constraintsSource` — GraphRef(s) pointing to graphs containing constraint annotations.
    pub const CONSTRAINTS_SOURCE: &str = "https://ns.flur.ee/db#constraintsSource";

    /// `f:enforceUnique` — annotation on property IRIs requiring unique values per graph.
    pub const ENFORCE_UNIQUE: &str = "https://ns.flur.ee/db#enforceUnique";

    // ---- Full-text indexing fields ----

    /// `f:fullTextDefaults` — full-text indexing defaults object on LedgerConfig/GraphConfig.
    pub const FULL_TEXT_DEFAULTS: &str = "https://ns.flur.ee/db#fullTextDefaults";

    /// `f:FullTextDefaults` — class/type for the full-text defaults resource.
    pub const FULL_TEXT_DEFAULTS_CLASS: &str = "https://ns.flur.ee/db#FullTextDefaults";

    /// `f:FullTextProperty` — class/type for a configured full-text property entry.
    pub const FULL_TEXT_PROPERTY_CLASS: &str = "https://ns.flur.ee/db#FullTextProperty";

    /// `f:defaultLanguage` — BCP-47 language tag (e.g. `"en"`, `"fr"`) used as
    /// the default analyzer language for configured properties.
    pub const DEFAULT_LANGUAGE: &str = "https://ns.flur.ee/db#defaultLanguage";

    /// `f:property` — one configured property entry on a `FullTextDefaults`
    /// resource. Value is a `FullTextProperty` node (cardinality 0..n).
    pub const FULL_TEXT_PROPERTY: &str = "https://ns.flur.ee/db#property";

    /// `f:target` — property IRI that a `FullTextProperty` entry applies to.
    pub const FULL_TEXT_TARGET: &str = "https://ns.flur.ee/db#target";
}

// ============================================================================
// Built-in datatype recognition
// ============================================================================

/// Recognized built-in datatypes.
///
/// This enum is the single source of truth for "which well-known datatype
/// does an IRI (or namespace + local name) refer to." Downstream crates
/// convert `KnownDatatype` to their own representation (`OType`,
/// `ValueTypeTag`, etc.) via per-crate match functions — keeping the
/// vocabulary recognition in one place while allowing each crate to own
/// its target-type mapping.
///
/// Variants cover every built-in datatype that appears in any of the
/// historical parallel tables: `o_type_registry::resolve_xsd_local_to_otype`,
/// `commit::codec::legacy_v3::known_xsd_local` and friends,
/// `value_id::ValueTypeTag::from_xsd_name` and friends, and
/// `parse::jsonld::expand_builtin_xsd_datatype`. A new well-known datatype
/// should be added as a new variant here first; the exhaustiveness of
/// downstream match statements then forces every consumer to decide how to
/// represent it.
pub mod datatype {
    use super::{fluree, jsonld_names, rdf, rdf_names, xsd, xsd_names};

    /// A well-known built-in datatype that Fluree recognizes.
    ///
    /// Use [`from_xsd_local`](Self::from_xsd_local),
    /// [`from_rdf_local`](Self::from_rdf_local),
    /// [`from_jsonld_local`](Self::from_jsonld_local),
    /// [`from_fluree_db_local`](Self::from_fluree_db_local), or the
    /// convenience [`from_ns_and_local`](Self::from_ns_and_local) to
    /// recognize a datatype from its namespace + local name. Use
    /// [`from_canonical_form`](Self::from_canonical_form) to recognize one
    /// from its canonical external form (a fully-qualified IRI for most
    /// variants, or the JSON-LD keyword `"@id"` for [`JsonLdId`](Self::JsonLdId)).
    ///
    /// # Round-trip guarantee
    ///
    /// For every variant,
    /// `KnownDatatype::from_canonical_form(dt.canonical_form()) == Some(dt)`.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub enum KnownDatatype {
        // XSD string / binary / URI / language subtypes
        XsdString,
        XsdAnyUri,
        XsdNormalizedString,
        XsdToken,
        XsdLanguage,
        XsdBase64Binary,
        XsdHexBinary,

        // XSD boolean
        XsdBoolean,

        // XSD numeric — int family
        XsdInteger,
        XsdLong,
        XsdInt,
        XsdShort,
        XsdByte,
        XsdUnsignedLong,
        XsdUnsignedInt,
        XsdUnsignedShort,
        XsdUnsignedByte,
        XsdNonNegativeInteger,
        XsdPositiveInteger,
        XsdNonPositiveInteger,
        XsdNegativeInteger,

        // XSD numeric — decimal family
        XsdDecimal,
        XsdFloat,
        XsdDouble,

        // XSD temporal
        XsdDateTime,
        XsdDate,
        XsdTime,
        XsdGYear,
        XsdGYearMonth,
        XsdGMonth,
        XsdGDay,
        XsdGMonthDay,

        // XSD duration
        XsdDuration,
        XsdDayTimeDuration,
        XsdYearMonthDuration,

        // RDF
        RdfJson,
        RdfLangString,

        // JSON-LD keyword namespace (used as datatype for IRI references)
        JsonLdId,

        // Fluree built-ins
        FlureeEmbeddingVector,
        FlureeFullText,
    }

    impl KnownDatatype {
        /// Recognize an XSD built-in by its local name (e.g. `"string"`,
        /// `"gYearMonth"`).
        pub fn from_xsd_local(local: &str) -> Option<Self> {
            use KnownDatatype::*;
            let dt = match local {
                s if s == xsd_names::STRING => XsdString,
                s if s == xsd_names::ANY_URI => XsdAnyUri,
                s if s == xsd_names::NORMALIZED_STRING => XsdNormalizedString,
                s if s == xsd_names::TOKEN => XsdToken,
                s if s == xsd_names::LANGUAGE => XsdLanguage,
                s if s == xsd_names::BASE64_BINARY => XsdBase64Binary,
                s if s == xsd_names::HEX_BINARY => XsdHexBinary,
                s if s == xsd_names::BOOLEAN => XsdBoolean,
                s if s == xsd_names::INTEGER => XsdInteger,
                s if s == xsd_names::LONG => XsdLong,
                s if s == xsd_names::INT => XsdInt,
                s if s == xsd_names::SHORT => XsdShort,
                s if s == xsd_names::BYTE => XsdByte,
                s if s == xsd_names::UNSIGNED_LONG => XsdUnsignedLong,
                s if s == xsd_names::UNSIGNED_INT => XsdUnsignedInt,
                s if s == xsd_names::UNSIGNED_SHORT => XsdUnsignedShort,
                s if s == xsd_names::UNSIGNED_BYTE => XsdUnsignedByte,
                s if s == xsd_names::NON_NEGATIVE_INTEGER => XsdNonNegativeInteger,
                s if s == xsd_names::POSITIVE_INTEGER => XsdPositiveInteger,
                s if s == xsd_names::NON_POSITIVE_INTEGER => XsdNonPositiveInteger,
                s if s == xsd_names::NEGATIVE_INTEGER => XsdNegativeInteger,
                s if s == xsd_names::DECIMAL => XsdDecimal,
                s if s == xsd_names::FLOAT => XsdFloat,
                s if s == xsd_names::DOUBLE => XsdDouble,
                s if s == xsd_names::DATE_TIME => XsdDateTime,
                s if s == xsd_names::DATE => XsdDate,
                s if s == xsd_names::TIME => XsdTime,
                s if s == xsd_names::G_YEAR => XsdGYear,
                s if s == xsd_names::G_YEAR_MONTH => XsdGYearMonth,
                s if s == xsd_names::G_MONTH => XsdGMonth,
                s if s == xsd_names::G_DAY => XsdGDay,
                s if s == xsd_names::G_MONTH_DAY => XsdGMonthDay,
                s if s == xsd_names::DURATION => XsdDuration,
                s if s == xsd_names::DAY_TIME_DURATION => XsdDayTimeDuration,
                s if s == xsd_names::YEAR_MONTH_DURATION => XsdYearMonthDuration,
                _ => return None,
            };
            Some(dt)
        }

        /// Recognize an RDF built-in by its local name (`"JSON"`, `"langString"`).
        pub fn from_rdf_local(local: &str) -> Option<Self> {
            match local {
                s if s == rdf_names::JSON => Some(Self::RdfJson),
                s if s == rdf_names::LANG_STRING => Some(Self::RdfLangString),
                _ => None,
            }
        }

        /// Recognize a JSON-LD keyword-namespace built-in by its local name
        /// (`"id"`).
        pub fn from_jsonld_local(local: &str) -> Option<Self> {
            match local {
                s if s == jsonld_names::ID => Some(Self::JsonLdId),
                _ => None,
            }
        }

        /// Recognize a Fluree built-in by its local name (e.g.
        /// `"embeddingVector"`, `"fullText"`).
        pub fn from_fluree_db_local(local: &str) -> Option<Self> {
            match local {
                "embeddingVector" => Some(Self::FlureeEmbeddingVector),
                "fullText" => Some(Self::FlureeFullText),
                _ => None,
            }
        }

        /// Recognize a built-in by its namespace code + local name.
        ///
        /// `ns_code` is a Fluree namespace code (see `namespaces::XSD`, etc.);
        /// `local` is the trailing local-name portion of the IRI.
        pub fn from_ns_and_local(ns_code: u16, local: &str) -> Option<Self> {
            use super::namespaces;
            match ns_code {
                namespaces::XSD => Self::from_xsd_local(local),
                namespaces::RDF => Self::from_rdf_local(local),
                namespaces::JSON_LD => Self::from_jsonld_local(local),
                namespaces::FLUREE_DB => Self::from_fluree_db_local(local),
                _ => None,
            }
        }

        /// Recognize a built-in by its canonical external form.
        ///
        /// For most variants this is a fully-qualified IRI (e.g.
        /// `http://www.w3.org/2001/XMLSchema#string`). For
        /// [`JsonLdId`](Self::JsonLdId) it is the JSON-LD keyword
        /// `"@id"`, which is how that datatype appears on the wire and in
        /// JSON-LD documents — there is no absolute IRI form for JSON-LD
        /// keywords.
        ///
        /// Satisfies the round-trip property
        /// `KnownDatatype::from_canonical_form(dt.canonical_form()) == Some(dt)`
        /// for every variant.
        pub fn from_canonical_form(iri: &str) -> Option<Self> {
            if let Some(local) = iri.strip_prefix(xsd::NS) {
                return Self::from_xsd_local(local);
            }
            if let Some(local) = iri.strip_prefix(rdf::NS) {
                return Self::from_rdf_local(local);
            }
            match iri {
                "@id" => Some(Self::JsonLdId),
                s if s == fluree::EMBEDDING_VECTOR => Some(Self::FlureeEmbeddingVector),
                s if s == fluree::FULL_TEXT => Some(Self::FlureeFullText),
                _ => None,
            }
        }

        /// Return the canonical external form for this datatype.
        ///
        /// For most variants this is a fully-qualified IRI. For
        /// [`JsonLdId`](Self::JsonLdId) it is the JSON-LD keyword
        /// `"@id"` — that's how JSON-LD keywords are written on the wire
        /// and there is no absolute IRI for them.
        ///
        /// Round-trips through [`from_canonical_form`](Self::from_canonical_form)
        /// for every variant.
        pub const fn canonical_form(&self) -> &'static str {
            use KnownDatatype::*;
            match self {
                XsdString => xsd::STRING,
                XsdAnyUri => xsd::ANY_URI,
                XsdNormalizedString => xsd::NORMALIZED_STRING,
                XsdToken => xsd::TOKEN,
                XsdLanguage => xsd::LANGUAGE,
                XsdBase64Binary => xsd::BASE64_BINARY,
                XsdHexBinary => xsd::HEX_BINARY,
                XsdBoolean => xsd::BOOLEAN,
                XsdInteger => xsd::INTEGER,
                XsdLong => xsd::LONG,
                XsdInt => xsd::INT,
                XsdShort => xsd::SHORT,
                XsdByte => xsd::BYTE,
                XsdUnsignedLong => xsd::UNSIGNED_LONG,
                XsdUnsignedInt => xsd::UNSIGNED_INT,
                XsdUnsignedShort => xsd::UNSIGNED_SHORT,
                XsdUnsignedByte => xsd::UNSIGNED_BYTE,
                XsdNonNegativeInteger => xsd::NON_NEGATIVE_INTEGER,
                XsdPositiveInteger => xsd::POSITIVE_INTEGER,
                XsdNonPositiveInteger => xsd::NON_POSITIVE_INTEGER,
                XsdNegativeInteger => xsd::NEGATIVE_INTEGER,
                XsdDecimal => xsd::DECIMAL,
                XsdFloat => xsd::FLOAT,
                XsdDouble => xsd::DOUBLE,
                XsdDateTime => xsd::DATE_TIME,
                XsdDate => xsd::DATE,
                XsdTime => xsd::TIME,
                XsdGYear => xsd::G_YEAR,
                XsdGYearMonth => xsd::G_YEAR_MONTH,
                XsdGMonth => xsd::G_MONTH,
                XsdGDay => xsd::G_DAY,
                XsdGMonthDay => xsd::G_MONTH_DAY,
                XsdDuration => xsd::DURATION,
                XsdDayTimeDuration => xsd::DAY_TIME_DURATION,
                XsdYearMonthDuration => xsd::YEAR_MONTH_DURATION,
                RdfJson => rdf::JSON,
                RdfLangString => rdf::LANG_STRING,
                JsonLdId => "@id",
                FlureeEmbeddingVector => fluree::EMBEDDING_VECTOR,
                FlureeFullText => fluree::FULL_TEXT,
            }
        }

        /// Return the local-name component of this datatype (e.g. `"string"`,
        /// `"JSON"`, `"embeddingVector"`, `"id"`).
        pub const fn local_name(&self) -> &'static str {
            use KnownDatatype::*;
            match self {
                XsdString => xsd_names::STRING,
                XsdAnyUri => xsd_names::ANY_URI,
                XsdNormalizedString => xsd_names::NORMALIZED_STRING,
                XsdToken => xsd_names::TOKEN,
                XsdLanguage => xsd_names::LANGUAGE,
                XsdBase64Binary => xsd_names::BASE64_BINARY,
                XsdHexBinary => xsd_names::HEX_BINARY,
                XsdBoolean => xsd_names::BOOLEAN,
                XsdInteger => xsd_names::INTEGER,
                XsdLong => xsd_names::LONG,
                XsdInt => xsd_names::INT,
                XsdShort => xsd_names::SHORT,
                XsdByte => xsd_names::BYTE,
                XsdUnsignedLong => xsd_names::UNSIGNED_LONG,
                XsdUnsignedInt => xsd_names::UNSIGNED_INT,
                XsdUnsignedShort => xsd_names::UNSIGNED_SHORT,
                XsdUnsignedByte => xsd_names::UNSIGNED_BYTE,
                XsdNonNegativeInteger => xsd_names::NON_NEGATIVE_INTEGER,
                XsdPositiveInteger => xsd_names::POSITIVE_INTEGER,
                XsdNonPositiveInteger => xsd_names::NON_POSITIVE_INTEGER,
                XsdNegativeInteger => xsd_names::NEGATIVE_INTEGER,
                XsdDecimal => xsd_names::DECIMAL,
                XsdFloat => xsd_names::FLOAT,
                XsdDouble => xsd_names::DOUBLE,
                XsdDateTime => xsd_names::DATE_TIME,
                XsdDate => xsd_names::DATE,
                XsdTime => xsd_names::TIME,
                XsdGYear => xsd_names::G_YEAR,
                XsdGYearMonth => xsd_names::G_YEAR_MONTH,
                XsdGMonth => xsd_names::G_MONTH,
                XsdGDay => xsd_names::G_DAY,
                XsdGMonthDay => xsd_names::G_MONTH_DAY,
                XsdDuration => xsd_names::DURATION,
                XsdDayTimeDuration => xsd_names::DAY_TIME_DURATION,
                XsdYearMonthDuration => xsd_names::YEAR_MONTH_DURATION,
                RdfJson => rdf_names::JSON,
                RdfLangString => rdf_names::LANG_STRING,
                JsonLdId => jsonld_names::ID,
                FlureeEmbeddingVector => "embeddingVector",
                FlureeFullText => "fullText",
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn xsd_local_roundtrip() {
            // Every xsd_names constant must round-trip through
            // from_xsd_local → local_name → from_xsd_local.
            for local in [
                xsd_names::STRING,
                xsd_names::INTEGER,
                xsd_names::LONG,
                xsd_names::INT,
                xsd_names::SHORT,
                xsd_names::BYTE,
                xsd_names::UNSIGNED_LONG,
                xsd_names::UNSIGNED_INT,
                xsd_names::UNSIGNED_SHORT,
                xsd_names::UNSIGNED_BYTE,
                xsd_names::NON_NEGATIVE_INTEGER,
                xsd_names::POSITIVE_INTEGER,
                xsd_names::NON_POSITIVE_INTEGER,
                xsd_names::NEGATIVE_INTEGER,
                xsd_names::DECIMAL,
                xsd_names::FLOAT,
                xsd_names::DOUBLE,
                xsd_names::BOOLEAN,
                xsd_names::DATE_TIME,
                xsd_names::DATE,
                xsd_names::TIME,
                xsd_names::DURATION,
                xsd_names::DAY_TIME_DURATION,
                xsd_names::YEAR_MONTH_DURATION,
                xsd_names::ANY_URI,
                xsd_names::NORMALIZED_STRING,
                xsd_names::TOKEN,
                xsd_names::LANGUAGE,
                xsd_names::BASE64_BINARY,
                xsd_names::HEX_BINARY,
                xsd_names::G_YEAR,
                xsd_names::G_YEAR_MONTH,
                xsd_names::G_MONTH,
                xsd_names::G_DAY,
                xsd_names::G_MONTH_DAY,
            ] {
                let dt = KnownDatatype::from_xsd_local(local)
                    .unwrap_or_else(|| panic!("KnownDatatype::from_xsd_local missed {local:?}"));
                assert_eq!(dt.local_name(), local);
                // Canonical form should also recognize it.
                let canon = dt.canonical_form();
                assert_eq!(KnownDatatype::from_canonical_form(canon), Some(dt));
            }
        }

        #[test]
        fn rdf_local_roundtrip() {
            for local in [rdf_names::JSON, rdf_names::LANG_STRING] {
                let dt = KnownDatatype::from_rdf_local(local).unwrap();
                assert_eq!(dt.local_name(), local);
                assert_eq!(
                    KnownDatatype::from_canonical_form(dt.canonical_form()),
                    Some(dt)
                );
            }
        }

        #[test]
        fn jsonld_id_roundtrip() {
            let dt = KnownDatatype::from_jsonld_local(jsonld_names::ID).unwrap();
            assert_eq!(dt, KnownDatatype::JsonLdId);
            assert_eq!(dt.local_name(), jsonld_names::ID);

            // Canonical form round-trip: "@id" → JsonLdId → "@id".
            // The JSON-LD keyword `@id` is the canonical external form for
            // this datatype — there is no absolute IRI form for JSON-LD
            // keywords, so `canonical_form` returns the keyword itself.
            assert_eq!(dt.canonical_form(), "@id");
            assert_eq!(
                KnownDatatype::from_canonical_form("@id"),
                Some(KnownDatatype::JsonLdId)
            );
        }

        #[test]
        fn fluree_db_locals_roundtrip() {
            let v = KnownDatatype::from_fluree_db_local("embeddingVector").unwrap();
            assert_eq!(v, KnownDatatype::FlureeEmbeddingVector);
            assert_eq!(
                KnownDatatype::from_canonical_form(fluree::EMBEDDING_VECTOR),
                Some(v)
            );

            let f = KnownDatatype::from_fluree_db_local("fullText").unwrap();
            assert_eq!(f, KnownDatatype::FlureeFullText);
            assert_eq!(
                KnownDatatype::from_canonical_form(fluree::FULL_TEXT),
                Some(f)
            );
        }

        #[test]
        fn canonical_form_roundtrip_covers_every_variant() {
            // Exhaustive round-trip: every variant must satisfy
            // `from_canonical_form(dt.canonical_form()) == Some(dt)`.
            // This is the invariant the module documents and any new
            // variant must uphold. Add the variant here whenever a new
            // one is added to the enum.
            use KnownDatatype::*;
            let all = [
                XsdString,
                XsdAnyUri,
                XsdNormalizedString,
                XsdToken,
                XsdLanguage,
                XsdBase64Binary,
                XsdHexBinary,
                XsdBoolean,
                XsdInteger,
                XsdLong,
                XsdInt,
                XsdShort,
                XsdByte,
                XsdUnsignedLong,
                XsdUnsignedInt,
                XsdUnsignedShort,
                XsdUnsignedByte,
                XsdNonNegativeInteger,
                XsdPositiveInteger,
                XsdNonPositiveInteger,
                XsdNegativeInteger,
                XsdDecimal,
                XsdFloat,
                XsdDouble,
                XsdDateTime,
                XsdDate,
                XsdTime,
                XsdGYear,
                XsdGYearMonth,
                XsdGMonth,
                XsdGDay,
                XsdGMonthDay,
                XsdDuration,
                XsdDayTimeDuration,
                XsdYearMonthDuration,
                RdfJson,
                RdfLangString,
                JsonLdId,
                FlureeEmbeddingVector,
                FlureeFullText,
            ];
            for dt in all {
                let canon = dt.canonical_form();
                assert_eq!(
                    KnownDatatype::from_canonical_form(canon),
                    Some(dt),
                    "round-trip failed for {dt:?} (canonical form {canon:?})",
                );
            }
        }

        #[test]
        fn unknown_locals_return_none() {
            assert!(KnownDatatype::from_xsd_local("notAType").is_none());
            assert!(KnownDatatype::from_rdf_local("notAType").is_none());
            assert!(KnownDatatype::from_canonical_form("http://example.org/custom").is_none());
        }
    }
}
