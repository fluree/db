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
