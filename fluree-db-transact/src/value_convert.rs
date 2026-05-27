//! Shared value conversion helpers for Turtle literal → FlakeValue.
//!
//! These pure functions are used by both [`FlakeSink`](crate::flake_sink::FlakeSink)
//! and [`ImportSink`](crate::import_sink::ImportSink) to convert parsed
//! literal values into `FlakeValue` + datatype `Sid`.

use crate::generate::{
    DT_BOOLEAN, DT_DATE, DT_DATE_TIME, DT_DAY_TIME_DURATION, DT_DECIMAL, DT_DOUBLE, DT_DURATION,
    DT_G_DAY, DT_G_MONTH, DT_G_MONTH_DAY, DT_G_YEAR, DT_G_YEAR_MONTH, DT_INTEGER, DT_JSON,
    DT_LANG_STRING, DT_STRING, DT_TIME, DT_WKT_LITERAL, DT_YEAR_MONTH_DURATION,
};
use crate::namespace::NsAllocator;
use fluree_db_core::geo::try_extract_point;
use fluree_db_core::temporal::{
    Date, DateTime, DayTimeDuration, Duration, GDay, GMonth, GMonthDay, GYear, GYearMonth, Time,
    YearMonthDuration,
};
use fluree_db_core::{FlakeValue, GeoPointBits, Sid};
use fluree_graph_ir::LiteralValue;
use fluree_vocab::{fluree, geo, rdf, xsd};

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

/// Convert a string literal with an explicit datatype IRI to (FlakeValue, dt Sid).
///
/// For well-known XSD types, parses the string into the native FlakeValue variant.
/// Unknown types are stored as FlakeValue::String with the declared dt Sid.
pub(crate) fn convert_string_literal(
    value: &str,
    dt_iri: &str,
    ns: &mut NsAllocator<'_>,
) -> (FlakeValue, Sid) {
    let dt_sid = cached_dt_sid(dt_iri).unwrap_or_else(|| ns.sid_for_iri(dt_iri));

    let fv = match dt_iri {
        xsd::STRING | xsd::NORMALIZED_STRING | xsd::TOKEN | xsd::LANGUAGE | xsd::ANY_URI => {
            FlakeValue::String(value.to_string())
        }
        xsd::INTEGER
        | xsd::LONG
        | xsd::INT
        | xsd::SHORT
        | xsd::BYTE
        | xsd::UNSIGNED_LONG
        | xsd::UNSIGNED_INT
        | xsd::UNSIGNED_SHORT
        | xsd::UNSIGNED_BYTE
        | xsd::NON_NEGATIVE_INTEGER
        | xsd::POSITIVE_INTEGER
        | xsd::NON_POSITIVE_INTEGER
        | xsd::NEGATIVE_INTEGER => parse_integer(value),
        xsd::DOUBLE | xsd::FLOAT => value
            .parse::<f64>()
            .map(FlakeValue::Double)
            .unwrap_or_else(|_| FlakeValue::String(value.to_string())),
        xsd::DECIMAL => value
            .parse::<bigdecimal::BigDecimal>()
            .map(|d| FlakeValue::Decimal(Box::new(d)))
            .unwrap_or_else(|_| FlakeValue::String(value.to_string())),
        xsd::BOOLEAN => match value {
            "true" | "1" => FlakeValue::Boolean(true),
            "false" | "0" => FlakeValue::Boolean(false),
            _ => FlakeValue::String(value.to_string()),
        },
        xsd::DATE_TIME => DateTime::parse(value)
            .map(|dt| FlakeValue::DateTime(Box::new(dt)))
            .unwrap_or_else(|_| FlakeValue::String(value.to_string())),
        xsd::DATE => Date::parse(value)
            .map(|d| FlakeValue::Date(Box::new(d)))
            .unwrap_or_else(|_| FlakeValue::String(value.to_string())),
        xsd::TIME => Time::parse(value)
            .map(|t| FlakeValue::Time(Box::new(t)))
            .unwrap_or_else(|_| FlakeValue::String(value.to_string())),
        xsd::G_YEAR => GYear::parse(value)
            .map(|v| FlakeValue::GYear(Box::new(v)))
            .unwrap_or_else(|_| FlakeValue::String(value.to_string())),
        xsd::G_YEAR_MONTH => GYearMonth::parse(value)
            .map(|v| FlakeValue::GYearMonth(Box::new(v)))
            .unwrap_or_else(|_| FlakeValue::String(value.to_string())),
        xsd::G_MONTH => GMonth::parse(value)
            .map(|v| FlakeValue::GMonth(Box::new(v)))
            .unwrap_or_else(|_| FlakeValue::String(value.to_string())),
        xsd::G_DAY => GDay::parse(value)
            .map(|v| FlakeValue::GDay(Box::new(v)))
            .unwrap_or_else(|_| FlakeValue::String(value.to_string())),
        xsd::G_MONTH_DAY => GMonthDay::parse(value)
            .map(|v| FlakeValue::GMonthDay(Box::new(v)))
            .unwrap_or_else(|_| FlakeValue::String(value.to_string())),
        xsd::DURATION => Duration::parse(value)
            .map(|v| FlakeValue::Duration(Box::new(v)))
            .unwrap_or_else(|_| FlakeValue::String(value.to_string())),
        xsd::DAY_TIME_DURATION => DayTimeDuration::parse(value)
            .map(|v| FlakeValue::DayTimeDuration(Box::new(v)))
            .unwrap_or_else(|_| FlakeValue::String(value.to_string())),
        xsd::YEAR_MONTH_DURATION => YearMonthDuration::parse(value)
            .map(|v| FlakeValue::YearMonthDuration(Box::new(v)))
            .unwrap_or_else(|_| FlakeValue::String(value.to_string())),
        rdf::JSON => FlakeValue::Json(value.to_string()),
        fluree::EMBEDDING_VECTOR => {
            // Delegate to core's shared lexical parser so JSON-LD bulk import,
            // Turtle, and SPARQL `"[..]"^^f:embeddingVector` all share f32
            // quantization. On parse failure, fall through to a string literal
            // — the late `validate_value_dt_pair` guard at the write path
            // (FlakeGenerator / ImportSink / FlakeSink) rejects the corrupt
            // (String, embeddingVector) pair as a hard transaction error.
            fluree_db_core::coerce::coerce_string_value(value, fluree::EMBEDDING_VECTOR)
                .unwrap_or_else(|_| FlakeValue::String(value.to_string()))
        }
        rdf::LANG_STRING => {
            // Language goes in FlakeMeta, value is a plain string
            FlakeValue::String(value.to_string())
        }
        geo::WKT_LITERAL => {
            // Detect POINT and store as GeoPoint, others as string
            if let Some((lat, lng)) = try_extract_point(value) {
                GeoPointBits::new(lat, lng)
                    .map(FlakeValue::GeoPoint)
                    .unwrap_or_else(|| FlakeValue::String(value.to_string()))
            } else {
                // Non-point WKT: store as string for sidecar spatial index
                FlakeValue::String(value.to_string())
            }
        }
        _ => {
            // Unknown datatype — store as string, preserve the dt Sid
            FlakeValue::String(value.to_string())
        }
    };

    // Always preserve the declared datatype from the Turtle source.
    // Normalization (e.g., xsd:long → xsd:integer) only applies to native
    // literals (term_literal_value) where the parser infers type from syntax.
    (fv, dt_sid)
}

/// Parse a typed XSD-style lexical form into the matching
/// `FlakeValue` variant, *without* touching the namespace
/// allocator. Used by surfaces that already hold a resolved
/// datatype Sid (e.g., the cross-ledger SHACL translator) and
/// need same-ledger-parity parsing for SHACL `sh:hasValue` /
/// range comparisons.
///
/// Return values:
/// - `Ok(Some(fv))` — recognized XSD (or `rdf:JSON`) datatype
///   parsed successfully.
/// - `Err(msg)` — recognized datatype whose lexical form is
///   invalid (e.g., `"abc"^^xsd:integer`). Callers should
///   surface this as a hard error rather than silently
///   coerce to `FlakeValue::String`.
/// - `Ok(None)` — datatype IRI is not in the XSD / RDF /
///   Fluree-recognized set. Callers store the value as
///   `FlakeValue::String` and preserve the declared datatype
///   Sid; the application is responsible for interpreting
///   non-recognized datatypes.
///
/// `rdf:langString` is intentionally NOT handled here — the
/// language tag rides on `FlakeMeta`, not on the FlakeValue,
/// so the caller has to construct the meta separately and the
/// value is just a plain string.
pub fn parse_xsd_lexical(value: &str, dt_iri: &str) -> Result<Option<FlakeValue>, String> {
    Ok(Some(match dt_iri {
        xsd::STRING | xsd::NORMALIZED_STRING | xsd::TOKEN | xsd::LANGUAGE | xsd::ANY_URI => {
            FlakeValue::String(value.to_string())
        }
        xsd::INTEGER
        | xsd::LONG
        | xsd::INT
        | xsd::SHORT
        | xsd::BYTE
        | xsd::UNSIGNED_LONG
        | xsd::UNSIGNED_INT
        | xsd::UNSIGNED_SHORT
        | xsd::UNSIGNED_BYTE
        | xsd::NON_NEGATIVE_INTEGER
        | xsd::POSITIVE_INTEGER
        | xsd::NON_POSITIVE_INTEGER
        | xsd::NEGATIVE_INTEGER => match parse_integer(value) {
            FlakeValue::String(_) => {
                return Err(format!("invalid {dt_iri} lexical `{value}`"));
            }
            other => other,
        },
        xsd::DOUBLE | xsd::FLOAT => value
            .parse::<f64>()
            .map(FlakeValue::Double)
            .map_err(|e| format!("invalid {dt_iri} lexical `{value}`: {e}"))?,
        xsd::DECIMAL => value
            .parse::<bigdecimal::BigDecimal>()
            .map(|d| FlakeValue::Decimal(Box::new(d)))
            .map_err(|e| format!("invalid xsd:decimal lexical `{value}`: {e}"))?,
        xsd::BOOLEAN => match value {
            "true" | "1" => FlakeValue::Boolean(true),
            "false" | "0" => FlakeValue::Boolean(false),
            _ => return Err(format!("invalid xsd:boolean lexical `{value}`")),
        },
        xsd::DATE_TIME => DateTime::parse(value)
            .map(|dt| FlakeValue::DateTime(Box::new(dt)))
            .map_err(|e| format!("invalid xsd:dateTime lexical `{value}`: {e}"))?,
        xsd::DATE => Date::parse(value)
            .map(|d| FlakeValue::Date(Box::new(d)))
            .map_err(|e| format!("invalid xsd:date lexical `{value}`: {e}"))?,
        xsd::TIME => Time::parse(value)
            .map(|t| FlakeValue::Time(Box::new(t)))
            .map_err(|e| format!("invalid xsd:time lexical `{value}`: {e}"))?,
        xsd::G_YEAR => GYear::parse(value)
            .map(|v| FlakeValue::GYear(Box::new(v)))
            .map_err(|e| format!("invalid xsd:gYear lexical `{value}`: {e}"))?,
        xsd::G_YEAR_MONTH => GYearMonth::parse(value)
            .map(|v| FlakeValue::GYearMonth(Box::new(v)))
            .map_err(|e| format!("invalid xsd:gYearMonth lexical `{value}`: {e}"))?,
        xsd::G_MONTH => GMonth::parse(value)
            .map(|v| FlakeValue::GMonth(Box::new(v)))
            .map_err(|e| format!("invalid xsd:gMonth lexical `{value}`: {e}"))?,
        xsd::G_DAY => GDay::parse(value)
            .map(|v| FlakeValue::GDay(Box::new(v)))
            .map_err(|e| format!("invalid xsd:gDay lexical `{value}`: {e}"))?,
        xsd::G_MONTH_DAY => GMonthDay::parse(value)
            .map(|v| FlakeValue::GMonthDay(Box::new(v)))
            .map_err(|e| format!("invalid xsd:gMonthDay lexical `{value}`: {e}"))?,
        xsd::DURATION => Duration::parse(value)
            .map(|v| FlakeValue::Duration(Box::new(v)))
            .map_err(|e| format!("invalid xsd:duration lexical `{value}`: {e}"))?,
        xsd::DAY_TIME_DURATION => DayTimeDuration::parse(value)
            .map(|v| FlakeValue::DayTimeDuration(Box::new(v)))
            .map_err(|e| format!("invalid xsd:dayTimeDuration lexical `{value}`: {e}"))?,
        xsd::YEAR_MONTH_DURATION => YearMonthDuration::parse(value)
            .map(|v| FlakeValue::YearMonthDuration(Box::new(v)))
            .map_err(|e| format!("invalid xsd:yearMonthDuration lexical `{value}`: {e}"))?,
        rdf::JSON => FlakeValue::Json(value.to_string()),
        // Non-XSD / non-Fluree-recognized datatype. Caller treats
        // the value as a plain string under the (already-registered)
        // application datatype Sid.
        _ => return Ok(None),
    }))
}

/// Parse an integer string into FlakeValue::Long or FlakeValue::BigInt.
pub(crate) fn parse_integer(value: &str) -> FlakeValue {
    if let Ok(n) = value.parse::<i64>() {
        FlakeValue::Long(n)
    } else if let Ok(n) = value.parse::<num_bigint::BigInt>() {
        FlakeValue::BigInt(Box::new(n))
    } else {
        FlakeValue::String(value.to_string())
    }
}

/// Convert a native `LiteralValue` (from the parser) to a `FlakeValue`.
pub(crate) fn convert_native_literal(value: &LiteralValue) -> FlakeValue {
    match value {
        LiteralValue::Integer(i) => FlakeValue::Long(*i),
        LiteralValue::Double(d) => FlakeValue::Double(*d),
        LiteralValue::Boolean(b) => FlakeValue::Boolean(*b),
        LiteralValue::String(s) => FlakeValue::String(s.to_string()),
        LiteralValue::Json(s) => FlakeValue::Json(s.to_string()),
    }
}
