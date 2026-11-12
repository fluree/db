//! Shared value conversion helpers for Turtle literal → FlakeValue.
//!
//! These pure functions are used by both [`FlakeSink`](crate::flake_sink::FlakeSink)
//! and [`ImportSink`](crate::import_sink::ImportSink) to convert parsed
//! literal values into `FlakeValue` + datatype `Sid`.

use crate::error::TransactError;
use crate::generate::{
    DT_BOOLEAN, DT_DATE, DT_DATE_TIME, DT_DAY_TIME_DURATION, DT_DECIMAL, DT_DOUBLE, DT_DURATION,
    DT_G_DAY, DT_G_MONTH, DT_G_MONTH_DAY, DT_G_YEAR, DT_G_YEAR_MONTH, DT_INTEGER, DT_JSON,
    DT_LANG_STRING, DT_STRING, DT_TIME, DT_WKT_LITERAL, DT_YEAR_MONTH_DURATION,
};
use crate::namespace::NsAllocator;
use fluree_db_core::{FlakeValue, Sid};
use fluree_graph_ir::LiteralValue;
use fluree_vocab::{geo, rdf, xsd};

/// Fast-path: return cached Sid for the most common datatype IRIs.
/// Avoids trie lookup + Sid::new() allocation for high-frequency types.
fn cached_dt_sid(dt_iri: &str) -> Option<Sid> {
    match dt_iri {
        xsd::STRING => Some(DT_STRING.clone()),
        xsd::INTEGER => Some(DT_INTEGER.clone()),
        xsd::DOUBLE => Some(DT_DOUBLE.clone()),
        xsd::BOOLEAN => Some(DT_BOOLEAN.clone()),
        xsd::DATE_TIME => Some(DT_DATE_TIME.clone()),
        xsd::DATE => Some(DT_DATE.clone()),
        xsd::TIME => Some(DT_TIME.clone()),
        xsd::DECIMAL => Some(DT_DECIMAL.clone()),
        xsd::G_YEAR => Some(DT_G_YEAR.clone()),
        xsd::G_YEAR_MONTH => Some(DT_G_YEAR_MONTH.clone()),
        xsd::G_MONTH => Some(DT_G_MONTH.clone()),
        xsd::G_DAY => Some(DT_G_DAY.clone()),
        xsd::G_MONTH_DAY => Some(DT_G_MONTH_DAY.clone()),
        xsd::DURATION => Some(DT_DURATION.clone()),
        xsd::DAY_TIME_DURATION => Some(DT_DAY_TIME_DURATION.clone()),
        xsd::YEAR_MONTH_DURATION => Some(DT_YEAR_MONTH_DURATION.clone()),
        rdf::JSON => Some(DT_JSON.clone()),
        rdf::LANG_STRING => Some(DT_LANG_STRING.clone()),
        geo::WKT_LITERAL => Some(DT_WKT_LITERAL.clone()),
        _ => None,
    }
}

/// What a write does with a literal whose lexical form is not in its
/// datatype's lexical space (`"landed"^^xsd:integer`). Each caller of
/// [`convert_string_literal`] names it, so the choice is made where the
/// write path is known.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LiteralCheck {
    /// Refuse it with the insert's error ([`typed_lexical`]). Every
    /// transaction: Turtle, TriG and N-Triples insert and upsert, graph sync,
    /// graph insert.
    Strict,
    /// Store it as text under its declared datatype ([`lenient_value`]).
    /// Bulk import only, the one lenient write path: it loads RDF text from
    /// elsewhere, which often carries ill-typed literals, in one pass.
    #[cfg_attr(
        not(feature = "import"),
        expect(
            dead_code,
            reason = "bulk import, its only user, needs the `import` feature"
        )
    )]
    Lenient,
}

/// Parse `value` as a lexical form of the datatype `dt_iri`, as every write
/// path that refuses an ill-typed literal does.
///
/// Delegates to [`fluree_db_core::coerce::coerce_string_value`], the parser
/// the JSON-LD insert and SPARQL `INSERT DATA` use, and fails with the error
/// they give. A string-like or unrecognized datatype takes any string.
pub(crate) fn typed_lexical(value: &str, dt_iri: &str) -> Result<FlakeValue, TransactError> {
    fluree_db_core::coerce::coerce_string_value(value, dt_iri)
        .map_err(|e| TransactError::Parse(e.message))
}

/// Convert a string literal with an explicit datatype IRI to (FlakeValue, dt Sid).
///
/// For well-known XSD types, parses the string into the native FlakeValue variant.
/// Unknown types are stored as FlakeValue::String with the declared dt Sid.
/// An ill-typed literal is refused or kept as `check` says.
pub(crate) fn convert_string_literal(
    value: &str,
    dt_iri: &str,
    ns: &mut NsAllocator<'_>,
    check: LiteralCheck,
) -> Result<(FlakeValue, Sid), TransactError> {
    let fv = match check {
        LiteralCheck::Lenient => lenient_value(value, dt_iri),
        // The language tag rides on the flake's metadata; the value is text.
        LiteralCheck::Strict if dt_iri == rdf::LANG_STRING => FlakeValue::String(value.to_string()),
        LiteralCheck::Strict => typed_lexical(value, dt_iri)?,
    };
    let dt_sid = cached_dt_sid(dt_iri).unwrap_or_else(|| ns.sid_for_iri(dt_iri));

    // Always preserve the declared datatype from the Turtle source.
    // Normalization (e.g., xsd:long → xsd:integer) only applies to native
    // literals (term_literal_value) where the parser infers type from syntax.
    Ok((fv, dt_sid))
}

/// The lenient parse of a typed lexical form, bulk import's: the value the
/// strict parse ([`typed_lexical`]) gives, or, for a lexical form it refuses,
/// the text itself under the declared datatype.
///
/// One parser, so the two cannot drift: a literal a transaction stores is
/// stored as the same value by an import, and only what a transaction
/// refuses is kept as text. An `rdf:JSON` lexical form that is not JSON is
/// kept as written. One that is not a vector is kept as text, which the
/// value/datatype pairing guard on the write path then refuses.
fn lenient_value(value: &str, dt_iri: &str) -> FlakeValue {
    fluree_db_core::coerce::coerce_string_value(value, dt_iri).unwrap_or_else(|_| {
        if dt_iri == rdf::JSON {
            FlakeValue::Json(value.to_string())
        } else {
            FlakeValue::String(value.to_string())
        }
    })
}

/// Decode a typed lexical form a ledger holds into the `FlakeValue` it is
/// stored as, *without* touching the namespace allocator. Used by surfaces
/// that already hold a resolved datatype Sid (e.g., the cross-ledger SHACL
/// translator) and need same-ledger-parity parsing for SHACL `sh:hasValue`
/// / range comparisons.
///
/// It is the write paths' parser ([`fluree_db_core::coerce::coerce_string_value`]),
/// so a literal decodes to the value a transaction or an import stores for
/// it: a native value for a recognized datatype, and the text for a
/// string-like or application-defined one. An ill-typed form of a recognized
/// datatype (`"abc"^^xsd:integer`, `"300"^^xsd:byte`) is an `Err`, which
/// callers surface as a hard error rather than silently re-typing it as a
/// string. An `rdf:JSON` form that is not JSON decodes as written, as bulk
/// import keeps it.
///
/// `rdf:langString` is not special-cased: the language tag rides on
/// `FlakeMeta`, so the caller builds the meta and the value is the text.
pub fn parse_xsd_lexical(value: &str, dt_iri: &str) -> Result<FlakeValue, String> {
    match fluree_db_core::coerce::coerce_string_value(value, dt_iri) {
        Ok(fv) => Ok(fv),
        Err(_) if dt_iri == rdf::JSON => Ok(FlakeValue::Json(value.to_string())),
        Err(e) => Err(e.message),
    }
}

/// Convert a native `LiteralValue` (from the parser) to a `FlakeValue`.
pub(crate) fn convert_native_literal(value: &LiteralValue) -> FlakeValue {
    match value {
        LiteralValue::Integer(i) => FlakeValue::Long(*i),
        LiteralValue::Double(d) => FlakeValue::Double(*d),
        LiteralValue::Boolean(b) => FlakeValue::Boolean(*b),
        LiteralValue::String(s) => FlakeValue::String(s.to_string()),
        LiteralValue::Json(s) => FlakeValue::Json(
            fluree_graph_ir::canonicalize_json(s).unwrap_or_else(|_| s.to_string()),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_vocab::fluree;

    /// The cross-ledger decoder reads a literal as the value the write paths
    /// store for it, and refuses one a transaction refuses, except an
    /// `rdf:JSON` form that is not JSON, which import keeps as written.
    #[test]
    fn a_held_literal_decodes_as_the_write_paths_store_it() {
        let xsd = |local: &str| format!("{}{local}", xsd::NS);
        let decoded =
            |lexical: &str, dt: &str| parse_xsd_lexical(lexical, dt).map(|v| format!("{v:?}"));
        let kind = |lexical: &str, dt: &str| {
            decoded(lexical, dt).map(|v| v.split('(').next().unwrap_or_default().to_string())
        };
        assert_eq!(
            kind("POINT(1 2)", geo::WKT_LITERAL).as_deref(),
            Ok("GeoPoint")
        );
        assert_eq!(
            kind("[0.5, 1.5]", fluree::EMBEDDING_VECTOR).as_deref(),
            Ok("Vector")
        );
        assert_eq!(kind("12", &xsd("byte")).as_deref(), Ok("Long"));
        assert_eq!(kind("1", &xsd("boolean")).as_deref(), Ok("Boolean"));
        assert_eq!(
            decoded("[1, 2]", rdf::JSON),
            Ok(format!("{:?}", FlakeValue::Json("[1,2]".into())))
        );
        assert_eq!(
            decoded("not json", rdf::JSON),
            Ok(format!("{:?}", FlakeValue::Json("not json".into())))
        );
        assert_eq!(
            decoded("anything", "http://example.org/types#custom"),
            Ok(format!("{:?}", FlakeValue::String("anything".into())))
        );
        for (lexical, dt) in [("300", xsd("byte")), ("abc", xsd("integer"))] {
            assert!(parse_xsd_lexical(lexical, &dt).is_err(), "{lexical} {dt}");
        }
    }
}
