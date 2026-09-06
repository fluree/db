//! Datatype utilities.
//!
//! Centralizes datatype matching semantics shared across query execution paths.

use crate::Sid;
use fluree_vocab::namespaces::XSD;
use fluree_vocab::xsd_names;

/// Datatype match semantics for value objects.
///
/// Query parsing normalizes some numeric datatype IRIs (e.g., xsd:int → xsd:integer),
/// so matching must treat numeric "families" as compatible at execution time:
/// - xsd:integer matches integer-family stored datatypes (xsd:int, xsd:long, ...)
/// - xsd:double matches xsd:float
#[inline]
pub fn dt_compatible(expected: &Sid, actual: &Sid) -> bool {
    if expected == actual {
        return true;
    }
    if expected.namespace_code != XSD || actual.namespace_code != XSD {
        return false;
    }
    match expected.name.as_ref() {
        xsd_names::INTEGER => matches!(
            actual.name.as_ref(),
            xsd_names::INTEGER
                | xsd_names::INT
                | xsd_names::SHORT
                | xsd_names::BYTE
                | xsd_names::LONG
        ),
        xsd_names::DOUBLE => matches!(actual.name.as_ref(), xsd_names::DOUBLE | xsd_names::FLOAT),
        _ => false,
    }
}

/// Whether literals of this datatype are interned in the shared **string
/// dictionary** — `xsd:string`, its XSD subtypes (`xsd:anyURI`, `xsd:token`,
/// `xsd:normalizedString`, `xsd:language`, `xsd:base64Binary`,
/// `xsd:hexBinary`), `rdf:langString`, `@fulltext`, and every customer-defined
/// datatype.
///
/// These share one `o_key` per lexical form, so the datatype is the rest of the
/// term's identity and has to travel with the value. Everything else — numerics,
/// temporals, booleans, `@json`, `@vector`, geo points, node references — is
/// stored in its own lane and identified without it.
///
/// A datatype IRI Fluree does not recognize is customer-defined, and customer
/// datatypes always route to the string dictionary
/// ([`crate::o_type_registry::OTypeRegistry::resolve`] falls through to
/// [`crate::o_type::OType::customer_datatype`]), so an unrecognized `Sid`
/// answers `true`. Membership for the recognized ones is read off the `OType`
/// each datatype resolves to, so this cannot drift from the storage layout.
#[inline]
pub fn is_string_dict_datatype(dt: &Sid) -> bool {
    use fluree_vocab::datatype::KnownDatatype;
    use fluree_vocab::{fluree, namespaces};

    let known = match dt.namespace_code {
        namespaces::XSD => KnownDatatype::from_xsd_local(dt.name_str()),
        namespaces::RDF => KnownDatatype::from_rdf_local(dt.name_str()),
        // The full IRI lives in `name` for these two.
        namespaces::EMPTY | namespaces::OVERFLOW => {
            KnownDatatype::from_canonical_form(dt.name_str())
        }
        // Fluree's own datatypes: `@fulltext` is string-backed, the embedding
        // vector is not.
        namespaces::FLUREE_DB => {
            return fluree::FULL_TEXT
                .strip_prefix(fluree::DB)
                .is_some_and(|local| local == dt.name_str());
        }
        // `@id` is a node reference; `geo:wktLiteral` is an embedded point.
        namespaces::JSON_LD | namespaces::OGC_GEO => return false,
        // Any other namespace is a user vocabulary, hence a customer datatype.
        _ => return true,
    };
    match known {
        // `rdf:langString` resolves to the positional marker rather than an
        // `OType` (the real one needs a `lang_id`), so name it directly.
        Some(KnownDatatype::RdfLangString) => true,
        Some(k) => crate::o_type_registry::known_datatype_to_otype(k).is_string_dict(),
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_vocab::{fluree, namespaces, rdf_names, xsd_names};

    fn xsd(name: &str) -> Sid {
        Sid::new(namespaces::XSD, name)
    }

    #[test]
    fn string_dict_datatypes_are_the_string_family_plus_customer_types() {
        for name in [
            xsd_names::STRING,
            xsd_names::ANY_URI,
            xsd_names::NORMALIZED_STRING,
            xsd_names::TOKEN,
            xsd_names::LANGUAGE,
            xsd_names::BASE64_BINARY,
            xsd_names::HEX_BINARY,
        ] {
            assert!(is_string_dict_datatype(&xsd(name)), "xsd:{name}");
        }
        assert!(is_string_dict_datatype(&Sid::new(
            namespaces::RDF,
            rdf_names::LANG_STRING
        )));
        assert!(is_string_dict_datatype(&Sid::new(
            namespaces::FLUREE_DB,
            fluree::FULL_TEXT.strip_prefix(fluree::DB).unwrap()
        )));
        // Customer-defined: a user namespace, and an unrecognized full IRI.
        assert!(is_string_dict_datatype(&Sid::new(42, "custom")));
        assert!(is_string_dict_datatype(&Sid::new(
            namespaces::EMPTY,
            "http://example.org/ns/custom"
        )));
    }

    #[test]
    fn other_lanes_are_not_string_dict() {
        for name in [
            xsd_names::INTEGER,
            xsd_names::LONG,
            xsd_names::INT,
            xsd_names::DOUBLE,
            xsd_names::FLOAT,
            xsd_names::DECIMAL,
            xsd_names::BOOLEAN,
            xsd_names::DATE,
            xsd_names::DATE_TIME,
            xsd_names::TIME,
            xsd_names::DURATION,
        ] {
            assert!(!is_string_dict_datatype(&xsd(name)), "xsd:{name}");
        }
        assert!(!is_string_dict_datatype(&Sid::new(
            namespaces::RDF,
            rdf_names::JSON
        )));
        assert!(!is_string_dict_datatype(&Sid::new(
            namespaces::FLUREE_DB,
            "embeddingVector"
        )));
        assert!(!is_string_dict_datatype(&Sid::new(
            namespaces::JSON_LD,
            fluree_vocab::jsonld_names::ID
        )));
        assert!(!is_string_dict_datatype(&Sid::new(
            namespaces::OGC_GEO,
            fluree_vocab::geo_names::WKT_LITERAL
        )));
    }
}
