//! Centralized type coercion for Fluree
//!
//! This module provides the authoritative coercion logic for converting values
//! to typed FlakeValues based on XSD datatypes. It is used by both the query
//! layer (for literal coercion in WHERE patterns) and the transaction layer
//! (for typed value parsing in INSERT/DELETE).
//!
//! ## Design Principles
//!
//! 1. **Single source of truth**: All coercion rules are defined here to prevent drift
//! 2. **Fully expanded IRIs**: This module expects fully expanded datatype IRIs
//!    (e.g., `http://www.w3.org/2001/XMLSchema#integer`). Prefix expansion is the
//!    responsibility of the JSON-LD parsing layer.
//! 3. **Strict validation**: For transactions, range bounds are enforced to reject
//!    invalid data. For queries, helpful error messages are provided.
//!
//! ## Supported Datatypes
//!
//! ### Integer Family (14 types)
//! - xsd:integer (unbounded, stored as BigInt)
//! - xsd:long, xsd:int, xsd:short, xsd:byte (signed, bounded to their bit width)
//! - xsd:unsignedLong, xsd:unsignedInt, xsd:unsignedShort, xsd:unsignedByte
//! - xsd:positiveInteger, xsd:nonNegativeInteger, xsd:negativeInteger, xsd:nonPositiveInteger
//!
//! ### Practical Limitation: Sign-Constrained Integer Types
//!
//! Per XSD spec, types like `xsd:positiveInteger` are semantically unbounded (just sign-constrained).
//! However, for practical purposes, we bound them to i128 range:
//! - `positiveInteger`: 1 to i128::MAX
//! - `nonNegativeInteger`: 0 to i128::MAX
//! - `negativeInteger`: i128::MIN to -1
//! - `nonPositiveInteger`: i128::MIN to 0
//!
//! Values outside this range will be rejected, even though technically valid per XSD.
//! Use `xsd:integer` for truly unbounded integers (no sign constraint).
//!
//! ### Other Numeric
//! - xsd:decimal → BigDecimal
//! - xsd:double, xsd:float → f64
//!
//! ### String-like (5 types)
//! - xsd:string, xsd:normalizedString, xsd:token, xsd:language, xsd:anyURI
//!
//! ### Temporal
//! - xsd:dateTime, xsd:date, xsd:time
//!
//! ### Other
//! - xsd:boolean
//! - rdf:JSON

use crate::geo::try_extract_point;
use crate::temporal::{
    DayTimeDuration, Duration, GDay, GMonth, GMonthDay, GYear, GYearMonth, YearMonthDuration,
};
use crate::{Date, DateTime, FlakeValue, GeoPointBits, Time};
use bigdecimal::BigDecimal;
use fluree_vocab::{errors, geo, rdf, xsd};
use num_bigint::BigInt;
use std::str::FromStr;

/// Error returned when coercion fails
#[derive(Debug, Clone)]
pub struct CoercionError {
    /// Human-readable error message
    pub message: String,
    /// Error type IRI for API responses
    pub error_type: &'static str,
}

impl CoercionError {
    /// Create a new coercion error
    pub fn new(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
            error_type: errors::TYPE_COERCION,
        }
    }

    /// Create an incompatible type error
    pub fn incompatible(value_desc: &str, target_type: &str, hint: Option<&str>) -> Self {
        let msg = match hint {
            Some(h) => format!("Cannot coerce {value_desc} to {target_type}. {h}"),
            None => format!("Cannot coerce {value_desc} to {target_type}"),
        };
        Self::new(msg)
    }

    /// Create a parse error
    pub fn parse_failed(value: &str, target_type: &str, detail: Option<&str>) -> Self {
        let msg = match detail {
            Some(d) => format!("Cannot parse '{value}' as {target_type}: {d}"),
            None => format!("Cannot parse '{value}' as {target_type}"),
        };
        Self::new(msg)
    }

    /// Create a range error
    pub fn out_of_range(
        value: impl std::fmt::Display,
        target_type: &str,
        min: i128,
        max: i128,
    ) -> Self {
        Self::new(format!(
            "Value {value} is out of range for {target_type}: expected {min} to {max}"
        ))
    }
}

impl std::fmt::Display for CoercionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for CoercionError {}

/// Result type for coercion operations
pub type CoercionResult<T> = Result<T, CoercionError>;

// =============================================================================
// Core Coercion Functions
// =============================================================================

/// Coerce a FlakeValue to match the target datatype.
///
/// This is the primary entry point for query-time coercion where the value
/// has already been parsed from JSON into a FlakeValue.
///
/// # Arguments
/// * `value` - The value to coerce
/// * `datatype_iri` - Fully expanded datatype IRI (e.g., `http://www.w3.org/2001/XMLSchema#integer`)
///
/// # Returns
/// * `Ok(FlakeValue)` - The coerced value
/// * `Err(CoercionError)` - If coercion is impossible or value is out of range
pub fn coerce_value(value: FlakeValue, datatype_iri: &str) -> CoercionResult<FlakeValue> {
    match (&value, datatype_iri) {
        // ====================================================================
        // Incompatible combinations - throw errors
        // ====================================================================

        // Number → String-like is invalid
        (FlakeValue::Long(n), dt) if xsd::is_string_like(dt) => Err(CoercionError::incompatible(
            &format!("number {n}"),
            dt,
            Some(&format!("Use {{\"@value\": \"{n}\"}} instead.")),
        )),
        (FlakeValue::Double(n), dt) if xsd::is_string_like(dt) => Err(CoercionError::incompatible(
            &format!("number {n}"),
            dt,
            Some(&format!("Use {{\"@value\": \"{n}\"}} instead.")),
        )),
        (FlakeValue::BigInt(n), dt) if xsd::is_string_like(dt) => Err(CoercionError::incompatible(
            &format!("number {n}"),
            dt,
            Some(&format!("Use {{\"@value\": \"{n}\"}} instead.")),
        )),

        // Boolean → String-like is invalid
        (FlakeValue::Boolean(b), dt) if xsd::is_string_like(dt) => {
            Err(CoercionError::incompatible(
                &format!("boolean {b}"),
                dt,
                Some(&format!("Use {{\"@value\": \"{b}\"}} instead.")),
            ))
        }

        // Boolean → numeric types is invalid
        (FlakeValue::Boolean(b), dt)
            if xsd::is_integer_family(dt)
                || dt == xsd::DECIMAL
                || dt == xsd::DOUBLE
                || dt == xsd::FLOAT =>
        {
            Err(CoercionError::incompatible(
                &format!("boolean {b}"),
                dt,
                None,
            ))
        }

        // Number → temporal types is invalid
        (FlakeValue::Long(_) | FlakeValue::Double(_) | FlakeValue::BigInt(_), dt)
            if xsd::is_temporal(dt) =>
        {
            Err(CoercionError::incompatible(
                "number",
                dt,
                Some("Use a string value instead."),
            ))
        }

        // Boolean → temporal types is invalid
        (FlakeValue::Boolean(_), dt) if xsd::is_temporal(dt) => {
            Err(CoercionError::incompatible("boolean", dt, None))
        }

        // Number → Boolean is invalid
        (FlakeValue::Long(n), dt) if dt == xsd::BOOLEAN => {
            let hint = format!(
                "Use {{\"@value\": \"{}\"}} or {{\"@value\": {}}} instead.",
                if *n != 0 { "true" } else { "false" },
                *n != 0
            );
            Err(CoercionError::incompatible(
                &format!("number {n}"),
                "xsd:boolean",
                Some(&hint),
            ))
        }
        (FlakeValue::Double(n), dt) if dt == xsd::BOOLEAN => Err(CoercionError::incompatible(
            &format!("number {n}"),
            "xsd:boolean",
            None,
        )),

        // ====================================================================
        // Numeric coercions
        // ====================================================================

        // Non-integral Double → Integer types is invalid
        (FlakeValue::Double(d), dt) if xsd::is_integer_family(dt) && d.fract() != 0.0 => {
            Err(CoercionError::incompatible(
                &format!("non-integer {d}"),
                dt,
                Some("Value must be a whole number."),
            ))
        }

        // Integral Double → Integer types: coerce to Long and validate
        (FlakeValue::Double(d), dt) if xsd::is_integer_family(dt) => {
            if d.is_finite() && *d >= i64::MIN as f64 && *d <= i64::MAX as f64 {
                let as_i64 = *d as i64;
                validate_integer_range(as_i64, dt)?;
                Ok(FlakeValue::Long(as_i64))
            } else {
                Err(CoercionError::new(format!(
                    "Number {d} is out of range for integer type {dt}"
                )))
            }
        }

        // Long → Integer-family: validate range constraints
        (FlakeValue::Long(n), dt) if xsd::is_integer_family(dt) => {
            validate_integer_range(*n, dt)?;
            Ok(FlakeValue::Long(*n))
        }

        // BigInt → Integer-family: validate range constraints
        (FlakeValue::BigInt(n), dt) if xsd::is_integer_family(dt) => {
            validate_bigint_range(n, dt)?;
            Ok(FlakeValue::BigInt(n.clone()))
        }

        // Long → xsd:decimal: convert to Double (policy: JSON numbers are lossy)
        (FlakeValue::Long(n), dt) if dt == xsd::DECIMAL => Ok(FlakeValue::Double(*n as f64)),

        // Long → xsd:double/xsd:float: widen to Double
        (FlakeValue::Long(n), dt) if dt == xsd::DOUBLE || dt == xsd::FLOAT => {
            Ok(FlakeValue::Double(*n as f64))
        }

        // BigInt → xsd:double/xsd:float: widen to Double (may lose precision for very large values)
        (FlakeValue::BigInt(n), dt) if dt == xsd::DOUBLE || dt == xsd::FLOAT => {
            use num_traits::ToPrimitive;
            Ok(FlakeValue::Double(n.to_f64().unwrap_or(f64::INFINITY)))
        }

        // ====================================================================
        // String → typed value coercions
        // ====================================================================

        // String → Integer types
        (FlakeValue::String(s), dt) if xsd::is_integer_family(dt) => parse_string_to_integer(s, dt),

        // String → Decimal
        (FlakeValue::String(s), dt) if dt == xsd::DECIMAL => BigDecimal::from_str(s)
            .map(|bd| FlakeValue::Decimal(Box::new(bd)))
            .map_err(|_| CoercionError::parse_failed(s, "xsd:decimal", None)),

        // String → Double/Float
        (FlakeValue::String(s), dt) if dt == xsd::DOUBLE || dt == xsd::FLOAT => {
            parse_string_to_double(s)
        }

        // String → DateTime
        (FlakeValue::String(s), dt) if dt == xsd::DATE_TIME => DateTime::parse(s)
            .map(|dt| FlakeValue::DateTime(Box::new(dt)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:dateTime", Some(&e))),

        // String → Date
        (FlakeValue::String(s), dt) if dt == xsd::DATE => Date::parse(s)
            .map(|d| FlakeValue::Date(Box::new(d)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:date", Some(&e))),

        // String → Time
        (FlakeValue::String(s), dt) if dt == xsd::TIME => Time::parse(s)
            .map(|t| FlakeValue::Time(Box::new(t)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:time", Some(&e))),

        // String → GYear
        (FlakeValue::String(s), dt) if dt == xsd::G_YEAR => GYear::parse(s)
            .map(|g| FlakeValue::GYear(Box::new(g)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:gYear", Some(&e))),

        // String → GYearMonth
        (FlakeValue::String(s), dt) if dt == xsd::G_YEAR_MONTH => GYearMonth::parse(s)
            .map(|g| FlakeValue::GYearMonth(Box::new(g)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:gYearMonth", Some(&e))),

        // String → GMonth
        (FlakeValue::String(s), dt) if dt == xsd::G_MONTH => GMonth::parse(s)
            .map(|g| FlakeValue::GMonth(Box::new(g)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:gMonth", Some(&e))),

        // String → GDay
        (FlakeValue::String(s), dt) if dt == xsd::G_DAY => GDay::parse(s)
            .map(|g| FlakeValue::GDay(Box::new(g)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:gDay", Some(&e))),

        // String → GMonthDay
        (FlakeValue::String(s), dt) if dt == xsd::G_MONTH_DAY => GMonthDay::parse(s)
            .map(|g| FlakeValue::GMonthDay(Box::new(g)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:gMonthDay", Some(&e))),

        // String → Duration
        (FlakeValue::String(s), dt) if dt == xsd::DURATION => Duration::parse(s)
            .map(|d| FlakeValue::Duration(Box::new(d)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:duration", Some(&e))),

        // String → DayTimeDuration
        (FlakeValue::String(s), dt) if dt == xsd::DAY_TIME_DURATION => DayTimeDuration::parse(s)
            .map(|d| FlakeValue::DayTimeDuration(Box::new(d)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:dayTimeDuration", Some(&e))),

        // String → YearMonthDuration
        (FlakeValue::String(s), dt) if dt == xsd::YEAR_MONTH_DURATION => {
            YearMonthDuration::parse(s)
                .map(|d| FlakeValue::YearMonthDuration(Box::new(d)))
                .map_err(|e| CoercionError::parse_failed(s, "xsd:yearMonthDuration", Some(&e)))
        }

        // String → Boolean
        (FlakeValue::String(s), dt) if dt == xsd::BOOLEAN => match s.as_str() {
            "true" | "1" => Ok(FlakeValue::Boolean(true)),
            "false" | "0" => Ok(FlakeValue::Boolean(false)),
            _ => Err(CoercionError::parse_failed(
                s,
                "xsd:boolean",
                Some("expected 'true', 'false', '1', or '0'"),
            )),
        },

        // String → rdf:JSON: validate as JSON
        (FlakeValue::String(s), dt) if dt == rdf::JSON => {
            serde_json::from_str::<serde_json::Value>(s)
                .map(|_| FlakeValue::Json(s.clone()))
                .map_err(|e| CoercionError::parse_failed(s, "rdf:JSON", Some(&e.to_string())))
        }

        // Already JSON → rdf:JSON
        (FlakeValue::Json(j), dt) if dt == rdf::JSON => Ok(FlakeValue::Json(j.clone())),

        // ====================================================================
        // Pass-through: already correct type or unknown datatype
        // ====================================================================
        _ => Ok(value),
    }
}

/// Coerce a JSON value to a FlakeValue with the specified datatype.
///
/// This is the primary entry point for transaction-time coercion where the
/// value comes directly from JSON input.
///
/// # Arguments
/// * `value` - The JSON value to coerce
/// * `datatype_iri` - Fully expanded datatype IRI
///
/// # Returns
/// * `Ok(FlakeValue)` - The coerced value
/// * `Err(CoercionError)` - If coercion is impossible or value is out of range
pub fn coerce_json_value(
    value: &serde_json::Value,
    datatype_iri: &str,
) -> CoercionResult<FlakeValue> {
    match value {
        serde_json::Value::String(s) => coerce_string_value(s, datatype_iri),

        serde_json::Value::Number(n) => coerce_number_value(n, datatype_iri),

        serde_json::Value::Bool(b) => coerce_bool_value(*b, datatype_iri),

        serde_json::Value::Array(arr) => {
            // Only vectors are supported
            if datatype_iri == fluree_vocab::fluree::EMBEDDING_VECTOR {
                coerce_array_to_vector(arr)
            } else {
                Err(CoercionError::incompatible(
                    "array",
                    datatype_iri,
                    Some("Arrays are only supported for vector datatypes."),
                ))
            }
        }

        serde_json::Value::Null => Ok(FlakeValue::Null),

        serde_json::Value::Object(_) => Err(CoercionError::incompatible(
            "object",
            datatype_iri,
            Some("Objects cannot be coerced to scalar values."),
        )),
    }
}

// =============================================================================
// Internal Coercion Helpers
// =============================================================================

/// Coerce a string value to the target datatype
fn coerce_string_value(s: &str, datatype_iri: &str) -> CoercionResult<FlakeValue> {
    match datatype_iri {
        // String-like types: pass through
        dt if xsd::is_string_like(dt) => Ok(FlakeValue::String(s.to_string())),

        // Integer family
        dt if xsd::is_integer_family(dt) => parse_string_to_integer(s, dt),

        // Decimal
        xsd::DECIMAL => BigDecimal::from_str(s)
            .map(|bd| FlakeValue::Decimal(Box::new(bd)))
            .map_err(|_| CoercionError::parse_failed(s, "xsd:decimal", None)),

        // Double/Float
        xsd::DOUBLE | xsd::FLOAT => parse_string_to_double(s),

        // Boolean
        xsd::BOOLEAN => match s {
            "true" | "1" => Ok(FlakeValue::Boolean(true)),
            "false" | "0" => Ok(FlakeValue::Boolean(false)),
            _ => Err(CoercionError::parse_failed(
                s,
                "xsd:boolean",
                Some("expected 'true', 'false', '1', or '0'"),
            )),
        },

        // DateTime
        xsd::DATE_TIME => DateTime::parse(s)
            .map(|dt| FlakeValue::DateTime(Box::new(dt)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:dateTime", Some(&e))),

        // Date
        xsd::DATE => Date::parse(s)
            .map(|d| FlakeValue::Date(Box::new(d)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:date", Some(&e))),

        // Time
        xsd::TIME => Time::parse(s)
            .map(|t| FlakeValue::Time(Box::new(t)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:time", Some(&e))),

        // GYear
        xsd::G_YEAR => GYear::parse(s)
            .map(|g| FlakeValue::GYear(Box::new(g)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:gYear", Some(&e))),

        // GYearMonth
        xsd::G_YEAR_MONTH => GYearMonth::parse(s)
            .map(|g| FlakeValue::GYearMonth(Box::new(g)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:gYearMonth", Some(&e))),

        // GMonth
        xsd::G_MONTH => GMonth::parse(s)
            .map(|g| FlakeValue::GMonth(Box::new(g)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:gMonth", Some(&e))),

        // GDay
        xsd::G_DAY => GDay::parse(s)
            .map(|g| FlakeValue::GDay(Box::new(g)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:gDay", Some(&e))),

        // GMonthDay
        xsd::G_MONTH_DAY => GMonthDay::parse(s)
            .map(|g| FlakeValue::GMonthDay(Box::new(g)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:gMonthDay", Some(&e))),

        // Duration
        xsd::DURATION => Duration::parse(s)
            .map(|d| FlakeValue::Duration(Box::new(d)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:duration", Some(&e))),

        // DayTimeDuration
        xsd::DAY_TIME_DURATION => DayTimeDuration::parse(s)
            .map(|d| FlakeValue::DayTimeDuration(Box::new(d)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:dayTimeDuration", Some(&e))),

        // YearMonthDuration
        xsd::YEAR_MONTH_DURATION => YearMonthDuration::parse(s)
            .map(|d| FlakeValue::YearMonthDuration(Box::new(d)))
            .map_err(|e| CoercionError::parse_failed(s, "xsd:yearMonthDuration", Some(&e))),

        // rdf:JSON (validate as JSON)
        dt if dt == rdf::JSON => serde_json::from_str::<serde_json::Value>(s)
            .map(|_| FlakeValue::Json(s.to_string()))
            .map_err(|e| CoercionError::parse_failed(s, "rdf:JSON", Some(&e.to_string()))),

        // geo:wktLiteral - detect POINT and store as GeoPoint, others as string
        geo::WKT_LITERAL => {
            if let Some((lat, lng)) = try_extract_point(s) {
                match GeoPointBits::new(lat, lng) {
                    Some(bits) => Ok(FlakeValue::GeoPoint(bits)),
                    None => Ok(FlakeValue::String(s.to_string())), // fallback on encode fail
                }
            } else {
                // Non-point WKT: store as string for sidecar spatial index
                Ok(FlakeValue::String(s.to_string()))
            }
        }

        // Unknown datatype: store as string with explicit datatype (caller handles datatype SID)
        _ => Ok(FlakeValue::String(s.to_string())),
    }
}

/// Coerce a JSON number to the target datatype
fn coerce_number_value(n: &serde_json::Number, datatype_iri: &str) -> CoercionResult<FlakeValue> {
    // Number → String-like is invalid
    if xsd::is_string_like(datatype_iri) {
        return Err(CoercionError::incompatible(
            &format!("number {n}"),
            datatype_iri,
            Some(&format!("Use {{\"@value\": \"{n}\"}} instead.")),
        ));
    }

    // Number → Boolean is invalid
    if datatype_iri == xsd::BOOLEAN {
        return Err(CoercionError::incompatible(
            &format!("number {n}"),
            "xsd:boolean",
            None,
        ));
    }

    // Number → Temporal is invalid
    if xsd::is_temporal(datatype_iri) {
        return Err(CoercionError::incompatible(
            &format!("number {n}"),
            datatype_iri,
            Some("Use a string value instead."),
        ));
    }

    // Number → Decimal
    if datatype_iri == xsd::DECIMAL {
        let f = n
            .as_f64()
            .ok_or_else(|| CoercionError::new(format!("Cannot convert number {n} to f64")))?;
        return Ok(FlakeValue::Double(f));
    }

    // Number → Integer family
    if xsd::is_integer_family(datatype_iri) {
        if let Some(i) = n.as_i64() {
            validate_integer_range(i, datatype_iri)?;
            return Ok(FlakeValue::Long(i));
        } else if let Some(f) = n.as_f64() {
            // Check if it's an integer value
            if f.fract() != 0.0 {
                return Err(CoercionError::incompatible(
                    &format!("non-integer {f}"),
                    datatype_iri,
                    Some("Value must be a whole number."),
                ));
            }
            // Integral double - convert to Long
            if f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                let as_i64 = f as i64;
                validate_integer_range(as_i64, datatype_iri)?;
                return Ok(FlakeValue::Long(as_i64));
            }
            return Err(CoercionError::new(format!(
                "Number {f} is out of range for {datatype_iri}"
            )));
        }
    }

    // Number → Double/Float
    if datatype_iri == xsd::DOUBLE || datatype_iri == xsd::FLOAT {
        let f = n
            .as_f64()
            .ok_or_else(|| CoercionError::new(format!("Cannot convert number {n} to f64")))?;
        return Ok(FlakeValue::Double(f));
    }

    // Default: preserve as Long or Double
    if let Some(i) = n.as_i64() {
        Ok(FlakeValue::Long(i))
    } else if let Some(f) = n.as_f64() {
        Ok(FlakeValue::Double(f))
    } else {
        Err(CoercionError::new(format!("Unsupported number: {n}")))
    }
}

/// Coerce a boolean to the target datatype
fn coerce_bool_value(b: bool, datatype_iri: &str) -> CoercionResult<FlakeValue> {
    // Boolean → String-like is invalid
    if xsd::is_string_like(datatype_iri) {
        return Err(CoercionError::incompatible(
            &format!("boolean {b}"),
            datatype_iri,
            Some(&format!("Use {{\"@value\": \"{b}\"}} instead.")),
        ));
    }

    // Boolean → Numeric is invalid
    if xsd::is_integer_family(datatype_iri)
        || datatype_iri == xsd::DECIMAL
        || datatype_iri == xsd::DOUBLE
        || datatype_iri == xsd::FLOAT
    {
        return Err(CoercionError::incompatible(
            &format!("boolean {b}"),
            datatype_iri,
            None,
        ));
    }

    // Boolean → Temporal is invalid
    if xsd::is_temporal(datatype_iri) {
        return Err(CoercionError::incompatible(
            &format!("boolean {b}"),
            datatype_iri,
            None,
        ));
    }

    // Boolean → Boolean
    if datatype_iri == xsd::BOOLEAN {
        return Ok(FlakeValue::Boolean(b));
    }

    // Unknown datatype: store as boolean
    Ok(FlakeValue::Boolean(b))
}

/// Coerce an array to a vector.
///
/// **Precision contract**: `f:vector` / `@vector` values are quantized to f32
/// at ingest. Each JSON number is parsed as f64, then downcast to f32 and
/// promoted back to f64. This ensures the stored value is exactly
/// representable in f32, giving a lossless round-trip through the packed
/// f32 vector arena used for SIMD scoring at query time.
///
/// Non-finite values (NaN, ±Infinity) and values outside f32 range are
/// rejected. Users needing higher-precision numeric arrays can use a custom
/// datatype (stored as a string literal).
fn coerce_array_to_vector(arr: &[serde_json::Value]) -> CoercionResult<FlakeValue> {
    let mut vector = Vec::with_capacity(arr.len());
    for (i, item) in arr.iter().enumerate() {
        match item {
            serde_json::Value::Number(n) => {
                let f = n.as_f64().ok_or_else(|| {
                    CoercionError::new(format!("Vector element must be a number, got: {n}"))
                })?;
                // Quantize to f32: reject non-finite and out-of-range values.
                let f32_val = f as f32;
                if !f32_val.is_finite() {
                    return Err(CoercionError::new(format!(
                        "Vector element [{i}] is not representable as f32: {f}"
                    )));
                }
                // Store the f32 bit pattern as f64 for exact round-trip.
                vector.push(f32_val as f64);
            }
            _ => {
                return Err(CoercionError::new(format!(
                    "Vector elements must be numbers, got: {item:?}"
                )));
            }
        }
    }
    Ok(FlakeValue::Vector(vector))
}

/// Parse a string to an integer value, validating range constraints
fn parse_string_to_integer(s: &str, datatype_iri: &str) -> CoercionResult<FlakeValue> {
    // Try i64 first (fast path)
    if let Ok(i) = s.parse::<i64>() {
        validate_integer_range(i, datatype_iri)?;
        return Ok(FlakeValue::Long(i));
    }

    // Fall back to BigInt for large numbers
    if let Ok(bi) = BigInt::from_str(s) {
        validate_bigint_range(&bi, datatype_iri)?;
        return Ok(FlakeValue::BigInt(Box::new(bi)));
    }

    Err(CoercionError::parse_failed(
        s,
        xsd::datatype_local_name(datatype_iri).unwrap_or(datatype_iri),
        None,
    ))
}

/// Parse a string to a double value
fn parse_string_to_double(s: &str) -> CoercionResult<FlakeValue> {
    let parsed = match s {
        "INF" | "+INF" => Ok(f64::INFINITY),
        "-INF" => Ok(f64::NEG_INFINITY),
        "NaN" => Ok(f64::NAN),
        _ => s.parse::<f64>(),
    };
    parsed
        .map(FlakeValue::Double)
        .map_err(|_| CoercionError::parse_failed(s, "xsd:double", None))
}

/// Validate that an i64 value is within range for the target integer datatype
fn validate_integer_range(value: i64, datatype_iri: &str) -> CoercionResult<()> {
    if let Some((min, max)) = xsd::integer_bounds(datatype_iri) {
        let v = value as i128;
        if v < min || v > max {
            return Err(CoercionError::out_of_range(
                value,
                xsd::datatype_local_name(datatype_iri).unwrap_or(datatype_iri),
                min,
                max,
            ));
        }
    }
    Ok(())
}

/// Validate that a BigInt value is within range for the target integer datatype
fn validate_bigint_range(value: &BigInt, datatype_iri: &str) -> CoercionResult<()> {
    use num_traits::ToPrimitive;

    if let Some((min, max)) = xsd::integer_bounds(datatype_iri) {
        // For bounded types, we need to check if the BigInt fits
        if let Some(v) = value.to_i128() {
            if v < min || v > max {
                return Err(CoercionError::out_of_range(
                    value,
                    xsd::datatype_local_name(datatype_iri).unwrap_or(datatype_iri),
                    min,
                    max,
                ));
            }
        } else {
            // BigInt is too large for i128, definitely out of range for bounded types
            // (except xsd:integer which is unbounded)
            if datatype_iri != xsd::INTEGER {
                return Err(CoercionError::new(format!(
                    "Value {} is out of range for {}",
                    value,
                    xsd::datatype_local_name(datatype_iri).unwrap_or(datatype_iri)
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coerce_long_to_integer() {
        let result = coerce_value(FlakeValue::Long(42), xsd::INTEGER);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), FlakeValue::Long(42)));
    }

    #[test]
    fn test_coerce_long_to_byte_in_range() {
        let result = coerce_value(FlakeValue::Long(127), xsd::BYTE);
        assert!(result.is_ok());
    }

    #[test]
    fn test_coerce_long_to_byte_out_of_range() {
        let result = coerce_value(FlakeValue::Long(128), xsd::BYTE);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("out of range"));
    }

    #[test]
    fn test_coerce_negative_to_unsigned() {
        let result = coerce_value(FlakeValue::Long(-1), xsd::UNSIGNED_BYTE);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_zero_to_positive_integer() {
        let result = coerce_value(FlakeValue::Long(0), xsd::POSITIVE_INTEGER);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_one_to_positive_integer() {
        let result = coerce_value(FlakeValue::Long(1), xsd::POSITIVE_INTEGER);
        assert!(result.is_ok());
    }

    #[test]
    fn test_coerce_string_to_integer() {
        let result = coerce_value(FlakeValue::String("12345".to_string()), xsd::INTEGER);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), FlakeValue::Long(12345)));
    }

    #[test]
    fn test_coerce_number_to_string_fails() {
        let result = coerce_value(FlakeValue::Long(42), xsd::STRING);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("Cannot coerce"));
    }

    #[test]
    fn test_coerce_boolean_to_string_fails() {
        let result = coerce_value(FlakeValue::Boolean(true), xsd::STRING);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_string_to_boolean() {
        assert!(matches!(
            coerce_value(FlakeValue::String("true".to_string()), xsd::BOOLEAN),
            Ok(FlakeValue::Boolean(true))
        ));
        assert!(matches!(
            coerce_value(FlakeValue::String("false".to_string()), xsd::BOOLEAN),
            Ok(FlakeValue::Boolean(false))
        ));
        assert!(matches!(
            coerce_value(FlakeValue::String("1".to_string()), xsd::BOOLEAN),
            Ok(FlakeValue::Boolean(true))
        ));
        assert!(matches!(
            coerce_value(FlakeValue::String("0".to_string()), xsd::BOOLEAN),
            Ok(FlakeValue::Boolean(false))
        ));
    }

    #[test]
    fn test_coerce_json_number_to_integer() {
        let json = serde_json::json!(42);
        let result = coerce_json_value(&json, xsd::INTEGER);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), FlakeValue::Long(42)));
    }

    #[test]
    fn test_coerce_json_number_to_byte_out_of_range() {
        let json = serde_json::json!(256);
        let result = coerce_json_value(&json, xsd::UNSIGNED_BYTE);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_json_string_to_datetime() {
        let json = serde_json::json!("2024-01-15T10:30:00Z");
        let result = coerce_json_value(&json, xsd::DATE_TIME);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), FlakeValue::DateTime(_)));
    }

    #[test]
    fn test_coerce_long_to_double() {
        let result = coerce_value(FlakeValue::Long(42), xsd::DOUBLE);
        assert!(result.is_ok());
        assert!(
            matches!(result.unwrap(), FlakeValue::Double(d) if (d - 42.0).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn test_coerce_long_to_float() {
        let result = coerce_value(FlakeValue::Long(42), xsd::FLOAT);
        assert!(result.is_ok());
        assert!(
            matches!(result.unwrap(), FlakeValue::Double(d) if (d - 42.0).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn test_coerce_bigint_to_double() {
        let big = BigInt::from(1_000_000_000_000i64);
        let result = coerce_value(FlakeValue::BigInt(Box::new(big)), xsd::DOUBLE);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), FlakeValue::Double(_)));
    }
}
