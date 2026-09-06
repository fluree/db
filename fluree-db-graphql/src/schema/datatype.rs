//! RDF datatype → GraphQL scalar.
//!
//! One table serves both tiers: tier 1 arrives with a [`ValueTypeTag`] from index
//! statistics, tier 2 with a `sh:datatype` IRI. The IRI path resolves to a tag
//! first so the two can never disagree.
//!
//! Range fidelity is preferred over familiar names. GraphQL's `Int` is 32-bit
//! signed, so only the XSD types that fit go there; `xsd:integer`, `xsd:long` and
//! the unbounded unsigned types get a custom `Long` scalar, and `xsd:decimal` a
//! custom `Decimal` — Fluree already renders decimals as JSON strings to keep
//! their precision, and mapping them to `Float` would quietly round.

use fluree_db_core::value_id::ValueTypeTag;

use crate::schema::model::Scalar;

/// The GraphQL scalar for a statistics value-type tag.
///
/// `JSON_LD_ID` is not a scalar — it marks a reference — and callers must route it
/// to an object or union type before calling this.
pub fn scalar_for_tag(tag: ValueTypeTag) -> Scalar {
    use ValueTypeTag as T;
    match tag {
        T::BOOLEAN => Scalar::Boolean,
        // Fits in a 32-bit signed Int.
        T::INT | T::SHORT | T::BYTE | T::UNSIGNED_SHORT | T::UNSIGNED_BYTE => Scalar::Int,
        // Unbounded or wider than i32.
        T::INTEGER
        | T::LONG
        | T::UNSIGNED_INT
        | T::UNSIGNED_LONG
        | T::NON_NEGATIVE_INTEGER
        | T::POSITIVE_INTEGER
        | T::NON_POSITIVE_INTEGER
        | T::NEGATIVE_INTEGER => Scalar::Long,
        T::DOUBLE | T::FLOAT => Scalar::Float,
        T::DECIMAL => Scalar::Decimal,
        T::DATE_TIME => Scalar::DateTime,
        T::DATE => Scalar::Date,
        T::TIME => Scalar::Time,
        T::ANY_URI => Scalar::Id,
        T::RDF_JSON | T::VECTOR => Scalar::Json,
        // Everything else — langString, tokens, gYear/gMonth/…, durations,
        // binaries, full-text, UNKNOWN — renders as its lexical form.
        _ => Scalar::String,
    }
}

/// The value-type tag for a datatype IRI, `UNKNOWN` outside XSD and RDF.
pub fn tag_for_datatype_iri(iri: &str) -> ValueTypeTag {
    if let Some(local) = iri.strip_prefix(fluree_vocab::xsd::NS) {
        ValueTypeTag::from_ns_name(fluree_vocab::namespaces::XSD, local)
    } else if let Some(local) = iri.strip_prefix(fluree_vocab::rdf::NS) {
        ValueTypeTag::from_ns_name(fluree_vocab::namespaces::RDF, local)
    } else {
        ValueTypeTag::UNKNOWN
    }
}

/// The GraphQL scalar for a `sh:datatype` IRI.
pub fn scalar_for_datatype_iri(iri: &str) -> Scalar {
    scalar_for_tag(tag_for_datatype_iri(iri))
}

/// Reduce the datatypes observed for one property to a single scalar.
///
/// A property carrying several datatypes has no one GraphQL type, so it falls back
/// to `String` — every value has a lexical form. Integer widths are the exception:
/// they are one number line, so a property seen as both `xsd:int` and `xsd:long`
/// widens to `Long` rather than degrading to text.
pub fn reduce_scalars(scalars: &[Scalar]) -> Scalar {
    match scalars {
        [] => Scalar::String,
        [only] => *only,
        _ => {
            let mut distinct: Vec<Scalar> = scalars.to_vec();
            distinct.sort_unstable();
            distinct.dedup();
            match distinct.as_slice() {
                [only] => *only,
                [Scalar::Int, Scalar::Long] => Scalar::Long,
                _ => Scalar::String,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xsd_iris_and_tags_agree() {
        assert_eq!(
            scalar_for_datatype_iri(fluree_vocab::xsd::STRING),
            Scalar::String
        );
        assert_eq!(
            scalar_for_datatype_iri("http://www.w3.org/2001/XMLSchema#int"),
            Scalar::Int
        );
        assert_eq!(
            scalar_for_datatype_iri("http://www.w3.org/2001/XMLSchema#long"),
            Scalar::Long
        );
        assert_eq!(
            scalar_for_datatype_iri("http://www.w3.org/2001/XMLSchema#integer"),
            Scalar::Long
        );
        assert_eq!(
            scalar_for_datatype_iri("http://www.w3.org/2001/XMLSchema#decimal"),
            Scalar::Decimal
        );
        assert_eq!(
            scalar_for_datatype_iri("http://www.w3.org/2001/XMLSchema#dateTime"),
            Scalar::DateTime
        );
        assert_eq!(
            scalar_for_datatype_iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"),
            Scalar::String
        );
        assert_eq!(
            scalar_for_datatype_iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON"),
            Scalar::Json
        );
        // Unrecognised datatypes keep their lexical form rather than guessing.
        assert_eq!(
            scalar_for_datatype_iri("http://example.org/Money"),
            Scalar::String
        );
    }

    #[test]
    fn mixed_datatypes_reduce_predictably() {
        assert_eq!(reduce_scalars(&[]), Scalar::String);
        assert_eq!(reduce_scalars(&[Scalar::Int]), Scalar::Int);
        assert_eq!(reduce_scalars(&[Scalar::Int, Scalar::Int]), Scalar::Int);
        // One number line: widen instead of degrading to text.
        assert_eq!(reduce_scalars(&[Scalar::Long, Scalar::Int]), Scalar::Long);
        // Genuinely different kinds have no common GraphQL type.
        assert_eq!(
            reduce_scalars(&[Scalar::Int, Scalar::Boolean]),
            Scalar::String
        );
        assert_eq!(
            reduce_scalars(&[Scalar::DateTime, Scalar::Date]),
            Scalar::String
        );
    }
}
