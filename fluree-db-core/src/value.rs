//! FlakeValue - polymorphic object value type
//!
//! The object field in a Flake can hold various types:
//! - References to other subjects (`Ref`)
//! - Strings, integers, floats, booleans
//! - Date/time types, JSON, binary data
//! - BigInt/Decimal for arbitrary precision numbers
//!
//! ## Ordering
//!
//! FlakeValue implements strict total ordering with **numeric class comparison**:
//!
//! 1. **Numeric class**: All numeric types (Long, Double, BigInt, Decimal) are compared
//!    mathematically by value, not by type ("number is a number" semantics).
//!    semantics. For example, `Long(3) < Double(3.5) < Long(4)`.
//!
//! 2. **Temporal class**: DateTime, Date, Time are compared by instant (not lexically).
//!    Cross-type temporal comparisons return `None` (incompatible).
//!
//! 3. **Other types**: Compared by type discriminant first, then by value within type.
//!
//! ## Sentinels
//!
//! `FlakeValue::min()` and `FlakeValue::max()` provide bounds for wildcard queries.

use crate::sid::Sid;
use crate::temporal::{
    Date, DateTime, DayTimeDuration, Duration, GDay, GMonth, GMonthDay, GYear, GYearMonth, Time,
    YearMonthDuration,
};
use crate::value_id::ObjKey;
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

// ============================================================================
// GeoPointBits — Packed lat/lng for geographic point storage
// ============================================================================

/// Packed GeoPoint: 30-bit lat + 30-bit lng in a single u64.
///
/// This is the canonical in-memory representation for geographic POINT values.
/// The encoding matches the index format exactly, avoiding representation drift.
///
/// Uses the same encoding as [`ObjKey::encode_geo_point`]:
/// - Latitude scaled from [-90, 90] to [0, 2^30-1] in upper 30 bits
/// - Longitude scaled from [-180, 180] to [0, 2^30-1] in lower 30 bits
/// - Precision is approximately 0.3mm at the equator
///
/// **Important**: The sort order is latitude-primary (south-to-north), which
/// enables efficient latitude-band scans for proximity queries.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct GeoPointBits(pub u64);

impl GeoPointBits {
    /// Create a new GeoPointBits from lat/lng coordinates.
    ///
    /// Returns `None` if coordinates are out of range or non-finite.
    /// - Latitude must be in [-90, 90]
    /// - Longitude must be in [-180, 180]
    pub fn new(lat: f64, lng: f64) -> Option<Self> {
        ObjKey::encode_geo_point(lat, lng)
            .ok()
            .map(|k| Self(k.as_u64()))
    }

    /// Create from a raw packed u64 (e.g., from index storage).
    #[inline]
    pub const fn from_u64(raw: u64) -> Self {
        Self(raw)
    }

    /// Get the latitude coordinate.
    #[inline]
    pub fn lat(&self) -> f64 {
        ObjKey::from_u64(self.0).decode_geo_point().0
    }

    /// Get the longitude coordinate.
    #[inline]
    pub fn lng(&self) -> f64 {
        ObjKey::from_u64(self.0).decode_geo_point().1
    }

    /// Get the raw packed u64 representation.
    #[inline]
    pub const fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Debug for GeoPointBits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (lat, lng) = ObjKey::from_u64(self.0).decode_geo_point();
        write!(f, "GeoPointBits({lat:.6}, {lng:.6})")
    }
}

impl fmt::Display for GeoPointBits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // WKT format: POINT(lng lat) - note: longitude first!
        let (lat, lng) = ObjKey::from_u64(self.0).decode_geo_point();
        write!(f, "POINT({lng} {lat})")
    }
}

/// Polymorphic value type for flake objects
///
/// Covers XSD datatypes supported by Fluree.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FlakeValue {
    /// Reference to another subject (when datatype is $id)
    Ref(Sid),
    /// Boolean value (xsd:boolean)
    Boolean(bool),
    /// 64-bit signed integer (xsd:long, xsd:integer when fits)
    Long(i64),
    /// 64-bit floating point (xsd:double, xsd:float)
    Double(f64),
    /// Arbitrary precision integer (xsd:integer when > i64 range)
    /// Boxed to keep enum size small
    BigInt(Box<BigInt>),
    /// Arbitrary precision decimal (xsd:decimal for exact representation)
    /// Boxed to keep enum size small
    Decimal(Box<BigDecimal>),
    /// XSD dateTime with timezone preservation
    DateTime(Box<DateTime>),
    /// XSD date with optional timezone
    Date(Box<Date>),
    /// XSD time with optional timezone
    Time(Box<Time>),
    /// XSD gYear (year with optional timezone)
    GYear(Box<GYear>),
    /// XSD gYearMonth (year-month with optional timezone)
    GYearMonth(Box<GYearMonth>),
    /// XSD gMonth (month with optional timezone)
    GMonth(Box<GMonth>),
    /// XSD gDay (day with optional timezone)
    GDay(Box<GDay>),
    /// XSD gMonthDay (month-day with optional timezone)
    GMonthDay(Box<GMonthDay>),
    /// XSD yearMonthDuration (totally orderable by months)
    YearMonthDuration(Box<YearMonthDuration>),
    /// XSD dayTimeDuration (totally orderable by microseconds)
    DayTimeDuration(Box<DayTimeDuration>),
    /// XSD duration (not totally orderable — months vs days indeterminate)
    Duration(Box<Duration>),
    /// String value (xsd:string and other string-like types)
    String(String),
    /// Dense vector/embedding (fluree:vector)
    Vector(Vec<f64>),
    /// JSON value (@json datatype) - stored as serialized JSON string
    /// Deserialized on output for queries
    Json(String),
    /// Geographic point (geo:wktLiteral POINT) — packed 60-bit lat/lng
    GeoPoint(GeoPointBits),
    /// Null/None value
    Null,
}

impl FlakeValue {
    /// Minimum possible value (for range query lower bounds)
    ///
    /// Uses Null which has the lowest type discriminant (0).
    pub fn min() -> Self {
        FlakeValue::Null
    }

    /// Maximum possible value (for range query upper bounds)
    ///
    /// Uses a **vector sentinel** (highest discriminant) that sorts
    /// after all other values.
    pub fn max() -> Self {
        // IMPORTANT: we treat the empty vector as a special-case sentinel
        // that compares greater-than any non-empty vector (see Ord impl).
        FlakeValue::Vector(Vec::new())
    }

    /// Get the type discriminant for ordering
    ///
    /// Lower discriminant = sorts earlier.
    ///
    /// **Numeric class** (3-6): All numeric types are grouped together and compared
    /// by mathematical value, not by discriminant. The discriminant is only used
    /// as a tie-breaker when values are equal.
    ///
    /// **Temporal class** (7-9): Grouped together but cross-type comparisons are
    /// incompatible (return None from temporal_cmp).
    fn type_discriminant(&self) -> u8 {
        match self {
            FlakeValue::Null => 0,
            FlakeValue::Ref(_) => 1,
            FlakeValue::Boolean(_) => 2,
            // Numeric class: grouped together (3-6)
            FlakeValue::Long(_) => 3,
            FlakeValue::BigInt(_) => 4,
            FlakeValue::Double(_) => 5,
            FlakeValue::Decimal(_) => 6,
            // Temporal class: grouped together (7-9)
            FlakeValue::Date(_) => 7,
            FlakeValue::Time(_) => 8,
            FlakeValue::DateTime(_) => 9,
            // Calendar fragment class (10-14)
            FlakeValue::GYear(_) => 10,
            FlakeValue::GYearMonth(_) => 11,
            FlakeValue::GMonth(_) => 12,
            FlakeValue::GDay(_) => 13,
            FlakeValue::GMonthDay(_) => 14,
            // Duration class (15-17)
            FlakeValue::YearMonthDuration(_) => 15,
            FlakeValue::DayTimeDuration(_) => 16,
            FlakeValue::Duration(_) => 17,
            // Other types
            FlakeValue::String(_) => 18,
            FlakeValue::Json(_) => 19,
            FlakeValue::GeoPoint(_) => 20,
            // Vector MUST be highest discriminant: empty Vector is used as max() sentinel
            FlakeValue::Vector(_) => 21,
        }
    }

    /// Check if this is a reference (object pointing to another subject)
    pub fn is_ref(&self) -> bool {
        matches!(self, FlakeValue::Ref(_))
    }

    /// Check if this is any numeric type (Long, Double, BigInt, Decimal)
    ///
    /// All numeric types form a **comparison class** where values are compared
    /// mathematically, with datatype as a tie-breaker for equal values.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            FlakeValue::Long(_)
                | FlakeValue::Double(_)
                | FlakeValue::BigInt(_)
                | FlakeValue::Decimal(_)
        )
    }

    /// Check if this is any temporal type (DateTime, Date, Time, g-types)
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            FlakeValue::DateTime(_)
                | FlakeValue::Date(_)
                | FlakeValue::Time(_)
                | FlakeValue::GYear(_)
                | FlakeValue::GYearMonth(_)
                | FlakeValue::GMonth(_)
                | FlakeValue::GDay(_)
                | FlakeValue::GMonthDay(_)
        )
    }

    /// Check if this is any duration type
    pub fn is_duration(&self) -> bool {
        matches!(
            self,
            FlakeValue::YearMonthDuration(_)
                | FlakeValue::DayTimeDuration(_)
                | FlakeValue::Duration(_)
        )
    }

    /// Check if this is a string type
    pub fn is_string(&self) -> bool {
        matches!(self, FlakeValue::String(_))
    }

    /// Check if this is a JSON type
    pub fn is_json(&self) -> bool {
        matches!(self, FlakeValue::Json(_))
    }

    /// Check if this is a vector type
    pub fn is_vector(&self) -> bool {
        matches!(self, FlakeValue::Vector(_))
    }

    /// Try to get as i64
    pub fn as_long(&self) -> Option<i64> {
        match self {
            FlakeValue::Long(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to get as f64 (converts Long to f64)
    pub fn as_double(&self) -> Option<f64> {
        match self {
            FlakeValue::Double(v) => Some(*v),
            FlakeValue::Long(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// Try to get as string reference
    pub fn as_str(&self) -> Option<&str> {
        match self {
            FlakeValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as Sid reference
    pub fn as_ref(&self) -> Option<&Sid> {
        match self {
            FlakeValue::Ref(sid) => Some(sid),
            _ => None,
        }
    }

    /// Try to get as vector reference
    pub fn as_vector(&self) -> Option<&[f64]> {
        match self {
            FlakeValue::Vector(v) => Some(v),
            _ => None,
        }
    }

    /// Try to get as BigInt reference
    pub fn as_bigint(&self) -> Option<&BigInt> {
        match self {
            FlakeValue::BigInt(v) => Some(v),
            _ => None,
        }
    }

    /// Try to get as BigDecimal reference
    pub fn as_decimal(&self) -> Option<&BigDecimal> {
        match self {
            FlakeValue::Decimal(v) => Some(v),
            _ => None,
        }
    }

    /// Try to get as DateTime reference
    pub fn as_datetime(&self) -> Option<&DateTime> {
        match self {
            FlakeValue::DateTime(v) => Some(v),
            _ => None,
        }
    }

    /// Try to get as Date reference
    pub fn as_date(&self) -> Option<&Date> {
        match self {
            FlakeValue::Date(v) => Some(v),
            _ => None,
        }
    }

    /// Try to get as Time reference
    pub fn as_time(&self) -> Option<&Time> {
        match self {
            FlakeValue::Time(v) => Some(v),
            _ => None,
        }
    }

    /// Compare two numeric values mathematically.
    ///
    /// Returns `None` if either value is not numeric.
    ///
    /// This implements "number is a number" semantics:
    /// - `Long(3) < Double(3.5) < Long(4)`
    /// - Equal values compare as Equal regardless of type
    pub fn numeric_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            // === Fast paths: same type ===
            (FlakeValue::Long(a), FlakeValue::Long(b)) => Some(a.cmp(b)),
            (FlakeValue::Double(a), FlakeValue::Double(b)) => {
                // Handle NaN: use bit comparison as fallback for total ordering
                a.partial_cmp(b)
                    .or_else(|| Some(a.to_bits().cmp(&b.to_bits())))
            }
            (FlakeValue::BigInt(a), FlakeValue::BigInt(b)) => Some((**a).cmp(&**b)),
            (FlakeValue::Decimal(a), FlakeValue::Decimal(b)) => {
                a.partial_cmp(b).or(Some(Ordering::Equal))
            }

            // === Long vs Double ===
            (FlakeValue::Long(a), FlakeValue::Double(b)) => {
                if Self::i64_fits_f64(*a) {
                    (*a as f64).partial_cmp(b)
                } else {
                    // Large i64: promote both to BigDecimal for exact comparison
                    let a_dec = BigDecimal::from(*a);
                    BigDecimal::try_from(*b)
                        .ok()
                        .and_then(|b_dec| a_dec.partial_cmp(&b_dec))
                }
            }
            (FlakeValue::Double(_), FlakeValue::Long(_)) => {
                other.numeric_cmp(self).map(Ordering::reverse)
            }

            // === Long vs BigInt ===
            (FlakeValue::Long(a), FlakeValue::BigInt(b)) => Some(BigInt::from(*a).cmp(&**b)),
            (FlakeValue::BigInt(a), FlakeValue::Long(b)) => Some((**a).cmp(&BigInt::from(*b))),

            // === Long vs Decimal ===
            (FlakeValue::Long(a), FlakeValue::Decimal(b)) => BigDecimal::from(*a).partial_cmp(&**b),
            (FlakeValue::Decimal(a), FlakeValue::Long(b)) => {
                (**a).partial_cmp(&BigDecimal::from(*b))
            }

            // === Double vs BigInt ===
            (FlakeValue::Double(a), FlakeValue::BigInt(b)) => {
                let a_dec = BigDecimal::try_from(*a).ok()?;
                let b_dec = BigDecimal::from((**b).clone());
                a_dec.partial_cmp(&b_dec)
            }
            (FlakeValue::BigInt(_), FlakeValue::Double(_)) => {
                other.numeric_cmp(self).map(Ordering::reverse)
            }

            // === Double vs Decimal ===
            (FlakeValue::Double(a), FlakeValue::Decimal(b)) => BigDecimal::try_from(*a)
                .ok()
                .and_then(|a_dec| a_dec.partial_cmp(&**b)),
            (FlakeValue::Decimal(_), FlakeValue::Double(_)) => {
                other.numeric_cmp(self).map(Ordering::reverse)
            }

            // === BigInt vs Decimal ===
            (FlakeValue::BigInt(a), FlakeValue::Decimal(b)) => {
                BigDecimal::from((**a).clone()).partial_cmp(&**b)
            }
            (FlakeValue::Decimal(a), FlakeValue::BigInt(b)) => {
                (**a).partial_cmp(&BigDecimal::from((**b).clone()))
            }

            // Not both numeric
            _ => None,
        }
    }

    /// Compare two temporal values by instant (same temporal type only).
    ///
    /// Returns `None` for cross-type comparisons (e.g., Date vs DateTime).
    /// This is consistent with XSD semantics where these are distinct types.
    pub fn temporal_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (FlakeValue::DateTime(a), FlakeValue::DateTime(b)) => Some(a.cmp(b)),
            (FlakeValue::Date(a), FlakeValue::Date(b)) => Some(a.cmp(b)),
            (FlakeValue::Time(a), FlakeValue::Time(b)) => Some(a.cmp(b)),
            (FlakeValue::GYear(a), FlakeValue::GYear(b)) => Some(a.cmp(b)),
            (FlakeValue::GYearMonth(a), FlakeValue::GYearMonth(b)) => Some(a.cmp(b)),
            (FlakeValue::GMonth(a), FlakeValue::GMonth(b)) => Some(a.cmp(b)),
            (FlakeValue::GDay(a), FlakeValue::GDay(b)) => Some(a.cmp(b)),
            (FlakeValue::GMonthDay(a), FlakeValue::GMonthDay(b)) => Some(a.cmp(b)),
            (FlakeValue::YearMonthDuration(a), FlakeValue::YearMonthDuration(b)) => Some(a.cmp(b)),
            (FlakeValue::DayTimeDuration(a), FlakeValue::DayTimeDuration(b)) => Some(a.cmp(b)),
            (FlakeValue::Duration(a), FlakeValue::Duration(b)) => (**a).partial_cmp(&**b),
            _ => None, // Cross-type comparisons are incompatible
        }
    }

    /// Check if i64 is exactly representable as f64 (within 2^53)
    fn i64_fits_f64(v: i64) -> bool {
        const MAX_SAFE: i64 = 1 << 53;
        v.abs() <= MAX_SAFE
    }

    /// Compare values of the same type
    fn same_type_cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (FlakeValue::Null, FlakeValue::Null) => Ordering::Equal,
            (FlakeValue::Ref(a), FlakeValue::Ref(b)) => a.cmp(b),
            (FlakeValue::Boolean(a), FlakeValue::Boolean(b)) => a.cmp(b),
            (FlakeValue::Long(a), FlakeValue::Long(b)) => a.cmp(b),
            (FlakeValue::Double(a), FlakeValue::Double(b)) => a
                .partial_cmp(b)
                .unwrap_or_else(|| a.to_bits().cmp(&b.to_bits())),
            (FlakeValue::BigInt(a), FlakeValue::BigInt(b)) => a.cmp(b),
            (FlakeValue::Decimal(a), FlakeValue::Decimal(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (FlakeValue::DateTime(a), FlakeValue::DateTime(b)) => a.cmp(b),
            (FlakeValue::Date(a), FlakeValue::Date(b)) => a.cmp(b),
            (FlakeValue::Time(a), FlakeValue::Time(b)) => a.cmp(b),
            (FlakeValue::GYear(a), FlakeValue::GYear(b)) => a.cmp(b),
            (FlakeValue::GYearMonth(a), FlakeValue::GYearMonth(b)) => a.cmp(b),
            (FlakeValue::GMonth(a), FlakeValue::GMonth(b)) => a.cmp(b),
            (FlakeValue::GDay(a), FlakeValue::GDay(b)) => a.cmp(b),
            (FlakeValue::GMonthDay(a), FlakeValue::GMonthDay(b)) => a.cmp(b),
            (FlakeValue::YearMonthDuration(a), FlakeValue::YearMonthDuration(b)) => a.cmp(b),
            (FlakeValue::DayTimeDuration(a), FlakeValue::DayTimeDuration(b)) => a.cmp(b),
            // Duration: use storage order (months, micros) tuple — NOT semantic comparison
            (FlakeValue::Duration(a), FlakeValue::Duration(b)) => a.cmp(b),
            (FlakeValue::String(a), FlakeValue::String(b)) => a.cmp(b),
            (FlakeValue::Json(a), FlakeValue::Json(b)) => a.cmp(b),
            (FlakeValue::Vector(a), FlakeValue::Vector(b)) => {
                // Special-case: empty vector is MAX sentinel
                if a.is_empty() && b.is_empty() {
                    return Ordering::Equal;
                }
                if a.is_empty() {
                    return Ordering::Greater;
                }
                if b.is_empty() {
                    return Ordering::Less;
                }
                // Lexicographic with NaN handling
                for (x, y) in a.iter().zip(b.iter()) {
                    let cmp = x
                        .partial_cmp(y)
                        .unwrap_or_else(|| x.to_bits().cmp(&y.to_bits()));
                    if cmp != Ordering::Equal {
                        return cmp;
                    }
                }
                a.len().cmp(&b.len())
            }
            // GeoPoint: compare by packed u64 (latitude-primary ordering)
            (FlakeValue::GeoPoint(a), FlakeValue::GeoPoint(b)) => a.cmp(b),
            // Should not happen since discriminants are equal
            _ => Ordering::Equal,
        }
    }

    /// Canonical hash for HLL statistics.
    ///
    /// Includes type tags for collision safety (e.g., string "true" vs boolean true).
    /// Uses xxHash64 for fast, high-quality hashing.
    ///
    /// # Determinism
    ///
    /// - All NaN values → fixed canonical bit pattern
    /// - -0.0 → +0.0 (both compare equal, use +0.0 bits)
    pub fn canonical_hash(&self) -> u64 {
        use xxhash_rust::xxh64::xxh64;

        // Canonical NaN bit pattern for deterministic hashing (quiet NaN)
        const CANONICAL_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

        // Type tag prefixes prevent collisions (e.g., string "true" vs boolean true)
        match self {
            FlakeValue::Null => xxh64(b"\x00null", 0),
            FlakeValue::Boolean(b) => {
                if *b {
                    xxh64(b"\x01true", 0)
                } else {
                    xxh64(b"\x01false", 0)
                }
            }
            FlakeValue::Long(n) => {
                let mut buf = [0u8; 9];
                buf[0] = 0x02; // type tag
                buf[1..].copy_from_slice(&n.to_le_bytes());
                xxh64(&buf, 0)
            }
            FlakeValue::Double(f) => {
                // CRITICAL: Canonicalize NaN and -0.0/+0.0 for determinism
                let canonical_bits = if f.is_nan() {
                    CANONICAL_NAN_BITS // Fixed bit pattern for ALL NaN values
                } else if *f == 0.0 {
                    0u64 // Normalize -0.0 to +0.0 (both compare equal)
                } else {
                    f.to_bits()
                };
                let mut buf = [0u8; 9];
                buf[0] = 0x03; // type tag
                buf[1..].copy_from_slice(&canonical_bits.to_le_bytes());
                xxh64(&buf, 0)
            }
            FlakeValue::BigInt(v) => {
                // Use string representation for consistent hashing
                use xxhash_rust::xxh64::Xxh64;
                let mut hasher = Xxh64::new(0);
                hasher.update(&[0x04]); // type tag for BigInt
                let s = v.to_string();
                hasher.update(&(s.len() as u64).to_le_bytes());
                hasher.update(s.as_bytes());
                hasher.digest()
            }
            FlakeValue::Decimal(v) => {
                // Use normalized string representation
                use xxhash_rust::xxh64::Xxh64;
                let mut hasher = Xxh64::new(0);
                hasher.update(&[0x05]); // type tag for Decimal
                let s = v.to_string();
                hasher.update(&(s.len() as u64).to_le_bytes());
                hasher.update(s.as_bytes());
                hasher.digest()
            }
            FlakeValue::DateTime(v) => {
                // Hash by instant for consistent cardinality estimation
                use xxhash_rust::xxh64::Xxh64;
                let mut hasher = Xxh64::new(0);
                hasher.update(&[0x06]); // type tag for DateTime
                hasher.update(&v.epoch_micros().to_le_bytes());
                hasher.digest()
            }
            FlakeValue::Date(v) => {
                use xxhash_rust::xxh64::Xxh64;
                let mut hasher = Xxh64::new(0);
                hasher.update(&[0x07]); // type tag for Date
                hasher.update(&v.days_since_epoch().to_le_bytes());
                hasher.digest()
            }
            FlakeValue::Time(v) => {
                use xxhash_rust::xxh64::Xxh64;
                let mut hasher = Xxh64::new(0);
                hasher.update(&[0x08]); // type tag for Time
                hasher.update(&v.micros_since_midnight().to_le_bytes());
                hasher.digest()
            }
            FlakeValue::GYear(v) => {
                use xxhash_rust::xxh64::Xxh64;
                let mut hasher = Xxh64::new(0);
                hasher.update(&[0x0D]);
                hasher.update(&(v.year() as i64).to_le_bytes());
                hasher.digest()
            }
            FlakeValue::GYearMonth(v) => {
                use xxhash_rust::xxh64::Xxh64;
                let mut hasher = Xxh64::new(0);
                hasher.update(&[0x0E]);
                hasher.update(&(v.year() as i64).to_le_bytes());
                hasher.update(&v.month().to_le_bytes());
                hasher.digest()
            }
            FlakeValue::GMonth(v) => {
                use xxhash_rust::xxh64::Xxh64;
                let mut hasher = Xxh64::new(0);
                hasher.update(&[0x0F]);
                hasher.update(&v.month().to_le_bytes());
                hasher.digest()
            }
            FlakeValue::GDay(v) => {
                use xxhash_rust::xxh64::Xxh64;
                let mut hasher = Xxh64::new(0);
                hasher.update(&[0x10]);
                hasher.update(&v.day().to_le_bytes());
                hasher.digest()
            }
            FlakeValue::GMonthDay(v) => {
                use xxhash_rust::xxh64::Xxh64;
                let mut hasher = Xxh64::new(0);
                hasher.update(&[0x11]);
                hasher.update(&v.month().to_le_bytes());
                hasher.update(&v.day().to_le_bytes());
                hasher.digest()
            }
            FlakeValue::YearMonthDuration(v) => {
                use xxhash_rust::xxh64::Xxh64;
                let mut hasher = Xxh64::new(0);
                hasher.update(&[0x12]);
                hasher.update(&v.months().to_le_bytes());
                hasher.digest()
            }
            FlakeValue::DayTimeDuration(v) => {
                use xxhash_rust::xxh64::Xxh64;
                let mut hasher = Xxh64::new(0);
                hasher.update(&[0x13]);
                hasher.update(&v.micros().to_le_bytes());
                hasher.digest()
            }
            FlakeValue::Duration(v) => {
                use xxhash_rust::xxh64::Xxh64;
                let mut hasher = Xxh64::new(0);
                hasher.update(&[0x14]);
                hasher.update(&v.months().to_le_bytes());
                hasher.update(&v.micros().to_le_bytes());
                hasher.digest()
            }
            FlakeValue::String(s) => {
                // tag + length + bytes for unambiguous encoding
                use xxhash_rust::xxh64::Xxh64;
                let mut hasher = Xxh64::new(0);
                hasher.update(&[0x09]); // type tag
                hasher.update(&(s.len() as u64).to_le_bytes());
                hasher.update(s.as_bytes());
                hasher.digest()
            }
            FlakeValue::Ref(sid) => {
                // tag + sid hash
                let sid_hash = sid.canonical_hash();
                let mut buf = [0u8; 9];
                buf[0] = 0x0A; // type tag
                buf[1..].copy_from_slice(&sid_hash.to_le_bytes());
                xxh64(&buf, 0)
            }
            FlakeValue::Json(s) => {
                // tag + length + bytes (similar to String, but distinct type tag)
                use xxhash_rust::xxh64::Xxh64;
                let mut hasher = Xxh64::new(0);
                hasher.update(&[0x0B]); // type tag for JSON
                hasher.update(&(s.len() as u64).to_le_bytes());
                hasher.update(s.as_bytes());
                hasher.digest()
            }
            FlakeValue::Vector(v) => {
                // tag + length + canonicalized float bytes
                use xxhash_rust::xxh64::Xxh64;
                let mut hasher = Xxh64::new(0);
                hasher.update(&[0x0C]); // type tag
                hasher.update(&(v.len() as u64).to_le_bytes());
                for f in v {
                    let canonical_bits = if f.is_nan() {
                        CANONICAL_NAN_BITS
                    } else if *f == 0.0 {
                        0u64
                    } else {
                        f.to_bits()
                    };
                    hasher.update(&canonical_bits.to_le_bytes());
                }
                hasher.digest()
            }
            FlakeValue::GeoPoint(bits) => {
                // tag + packed u64 (already canonical)
                let mut buf = [0u8; 9];
                buf[0] = 0x15; // type tag for GeoPoint
                buf[1..].copy_from_slice(&bits.as_u64().to_le_bytes());
                xxh64(&buf, 0)
            }
        }
    }
}

// === Strict Total Ordering with Numeric Class Comparison ===

impl PartialEq for FlakeValue {
    fn eq(&self, other: &Self) -> bool {
        // For numeric types, use numeric_cmp for value equality
        if self.is_numeric() && other.is_numeric() {
            return self.numeric_cmp(other) == Some(Ordering::Equal);
        }

        // For temporal types of the same kind, use temporal_cmp
        if self.is_temporal() && other.is_temporal() {
            if let Some(ord) = self.temporal_cmp(other) {
                return ord == Ordering::Equal;
            }
            // Different temporal types are not equal
            return false;
        }

        // Duration types: same-type comparison
        if self.is_duration() && other.is_duration() {
            if let Some(ord) = self.temporal_cmp(other) {
                return ord == Ordering::Equal;
            }
            return false;
        }

        // Same discriminant: type-specific equality
        if std::mem::discriminant(self) == std::mem::discriminant(other) {
            match (self, other) {
                (FlakeValue::Null, FlakeValue::Null) => true,
                (FlakeValue::Ref(a), FlakeValue::Ref(b)) => a == b,
                (FlakeValue::Boolean(a), FlakeValue::Boolean(b)) => a == b,
                (FlakeValue::String(a), FlakeValue::String(b)) => a == b,
                (FlakeValue::Json(a), FlakeValue::Json(b)) => a == b,
                (FlakeValue::Vector(a), FlakeValue::Vector(b)) => {
                    a.len() == b.len()
                        && a.iter()
                            .zip(b.iter())
                            .all(|(x, y)| x.to_bits() == y.to_bits())
                }
                (FlakeValue::GeoPoint(a), FlakeValue::GeoPoint(b)) => a == b,
                // Numeric and temporal types already handled above
                _ => false,
            }
        } else {
            false
        }
    }
}

impl Eq for FlakeValue {}

impl Ord for FlakeValue {
    fn cmp(&self, other: &Self) -> Ordering {
        // 1. Same discriminant → type-specific comparison
        if std::mem::discriminant(self) == std::mem::discriminant(other) {
            return self.same_type_cmp(other);
        }

        // 2. Both numeric → compare by mathematical value
        // Equal numeric values return Equal so cmp_object can use dt as tie-breaker
        // "number is a number": equal values are equal regardless of numeric type
        if self.is_numeric() && other.is_numeric() {
            if let Some(ord) = self.numeric_cmp(other) {
                return ord;
            }
            // numeric_cmp returned None - shouldn't happen for two numeric values
            // Fall through to discriminant comparison as defensive measure
        }

        // 3. Both temporal → compare by instant (if same temporal type)
        if self.is_temporal() && other.is_temporal() {
            if let Some(ord) = self.temporal_cmp(other) {
                return ord;
            }
            // Different temporal types: order by discriminant
            return self.type_discriminant().cmp(&other.type_discriminant());
        }

        // 3b. Both duration → compare (if same duration type)
        if self.is_duration() && other.is_duration() {
            if let Some(ord) = self.temporal_cmp(other) {
                return ord;
            }
            // Different duration types: order by discriminant
            return self.type_discriminant().cmp(&other.type_discriminant());
        }

        // 4. Different type classes → order by discriminant
        self.type_discriminant().cmp(&other.type_discriminant())
    }
}

impl PartialOrd for FlakeValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for FlakeValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // IMPORTANT: Hash must be consistent with PartialEq.
        // Since numeric types can be equal across types (Long(3) == Double(3.0) == BigInt(3)),
        // we hash ALL numerics to a canonical binary form (no string allocation).
        //
        // Strategy:
        // - Integer values (Long, BigInt, integer-valued Double/Decimal): hash as BigInt bytes
        // - Non-integer Double: hash f64 bits with a different tag (can't equal integers)
        // - Non-integer Decimal: hash (sign, scale, digits) with a different tag
        match self {
            // Long: convert to BigInt for canonical integer hash
            FlakeValue::Long(l) => {
                0u8.hash(state); // integer tag
                BigInt::from(*l).to_signed_bytes_le().hash(state);
            }
            FlakeValue::Double(d) => {
                if d.is_nan() {
                    2u8.hash(state); // NaN tag
                } else if d.is_infinite() {
                    3u8.hash(state); // infinity tag
                    (*d > 0.0).hash(state); // sign
                } else if d.fract() == 0.0 && *d >= i64::MIN as f64 && *d <= i64::MAX as f64 {
                    // Integer-valued double in i64 range: hash as integer
                    0u8.hash(state); // integer tag
                    BigInt::from(*d as i64).to_signed_bytes_le().hash(state);
                } else {
                    // Non-integer double: hash bits (can't equal any integer)
                    1u8.hash(state); // non-integer numeric tag
                    d.to_bits().hash(state);
                }
            }
            FlakeValue::BigInt(v) => {
                0u8.hash(state); // integer tag
                v.to_signed_bytes_le().hash(state);
            }
            FlakeValue::Decimal(v) => {
                let normalized = v.normalized();
                if normalized.is_integer() {
                    // Integer decimal: hash as BigInt
                    0u8.hash(state); // integer tag
                                     // Extract integer part via with_scale(0)
                    let int_part = normalized.with_scale(0);
                    // Get digits and sign
                    let (digits, _scale) = int_part.as_bigint_and_exponent();
                    digits.to_signed_bytes_le().hash(state);
                } else {
                    // Non-integer decimal: hash (sign, scale, digits)
                    1u8.hash(state); // non-integer numeric tag
                    let (digits, scale) = normalized.as_bigint_and_exponent();
                    scale.hash(state);
                    digits.to_signed_bytes_le().hash(state);
                }
            }
            // Non-numeric types: hash with type discriminant
            FlakeValue::Null => {
                self.type_discriminant().hash(state);
            }
            FlakeValue::Ref(sid) => {
                self.type_discriminant().hash(state);
                sid.hash(state);
            }
            FlakeValue::Boolean(b) => {
                self.type_discriminant().hash(state);
                b.hash(state);
            }
            FlakeValue::DateTime(v) => {
                self.type_discriminant().hash(state);
                v.hash(state);
            }
            FlakeValue::Date(v) => {
                self.type_discriminant().hash(state);
                v.hash(state);
            }
            FlakeValue::Time(v) => {
                self.type_discriminant().hash(state);
                v.hash(state);
            }
            FlakeValue::GYear(v) => {
                self.type_discriminant().hash(state);
                v.hash(state);
            }
            FlakeValue::GYearMonth(v) => {
                self.type_discriminant().hash(state);
                v.hash(state);
            }
            FlakeValue::GMonth(v) => {
                self.type_discriminant().hash(state);
                v.hash(state);
            }
            FlakeValue::GDay(v) => {
                self.type_discriminant().hash(state);
                v.hash(state);
            }
            FlakeValue::GMonthDay(v) => {
                self.type_discriminant().hash(state);
                v.hash(state);
            }
            FlakeValue::YearMonthDuration(v) => {
                self.type_discriminant().hash(state);
                v.hash(state);
            }
            FlakeValue::DayTimeDuration(v) => {
                self.type_discriminant().hash(state);
                v.hash(state);
            }
            FlakeValue::Duration(v) => {
                self.type_discriminant().hash(state);
                v.hash(state);
            }
            FlakeValue::String(s) => {
                self.type_discriminant().hash(state);
                s.hash(state);
            }
            FlakeValue::Json(s) => {
                self.type_discriminant().hash(state);
                s.hash(state);
            }
            FlakeValue::Vector(v) => {
                self.type_discriminant().hash(state);
                v.len().hash(state);
                for f in v {
                    f.to_bits().hash(state);
                }
            }
            FlakeValue::GeoPoint(bits) => {
                self.type_discriminant().hash(state);
                bits.hash(state);
            }
        }
    }
}

impl fmt::Display for FlakeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FlakeValue::Null => write!(f, "null"),
            FlakeValue::Ref(sid) => write!(f, "ref:{sid}"),
            FlakeValue::Boolean(b) => write!(f, "{b}"),
            FlakeValue::Long(l) => write!(f, "{l}"),
            FlakeValue::Double(d) => write!(f, "{d}"),
            FlakeValue::BigInt(v) => write!(f, "{v}"),
            FlakeValue::Decimal(v) => write!(f, "{v}"),
            FlakeValue::DateTime(v) => write!(f, "{v}"),
            FlakeValue::Date(v) => write!(f, "{v}"),
            FlakeValue::Time(v) => write!(f, "{v}"),
            FlakeValue::GYear(v) => write!(f, "{v}"),
            FlakeValue::GYearMonth(v) => write!(f, "{v}"),
            FlakeValue::GMonth(v) => write!(f, "{v}"),
            FlakeValue::GDay(v) => write!(f, "{v}"),
            FlakeValue::GMonthDay(v) => write!(f, "{v}"),
            FlakeValue::YearMonthDuration(v) => write!(f, "{v}"),
            FlakeValue::DayTimeDuration(v) => write!(f, "{v}"),
            FlakeValue::Duration(v) => write!(f, "{v}"),
            FlakeValue::String(s) => write!(f, "\"{s}\""),
            FlakeValue::Json(s) => write!(f, "@json:{s}"),
            FlakeValue::Vector(v) => {
                write!(f, "[")?;
                for (i, val) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{val}")?;
                }
                write!(f, "]")
            }
            FlakeValue::GeoPoint(bits) => write!(f, "{bits}"),
        }
    }
}

// === Convenient From implementations ===

impl From<Sid> for FlakeValue {
    fn from(sid: Sid) -> Self {
        FlakeValue::Ref(sid)
    }
}

impl From<bool> for FlakeValue {
    fn from(b: bool) -> Self {
        FlakeValue::Boolean(b)
    }
}

impl From<i64> for FlakeValue {
    fn from(l: i64) -> Self {
        FlakeValue::Long(l)
    }
}

impl From<i32> for FlakeValue {
    fn from(i: i32) -> Self {
        FlakeValue::Long(i as i64)
    }
}

impl From<f64> for FlakeValue {
    fn from(d: f64) -> Self {
        FlakeValue::Double(d)
    }
}

impl From<String> for FlakeValue {
    fn from(s: String) -> Self {
        FlakeValue::String(s)
    }
}

impl From<&str> for FlakeValue {
    fn from(s: &str) -> Self {
        FlakeValue::String(s.to_string())
    }
}

impl From<Vec<f64>> for FlakeValue {
    fn from(v: Vec<f64>) -> Self {
        FlakeValue::Vector(v)
    }
}

impl From<BigInt> for FlakeValue {
    fn from(v: BigInt) -> Self {
        // Normalize: if it fits in i64, use Long
        if let Some(i) = v.to_i64() {
            FlakeValue::Long(i)
        } else {
            FlakeValue::BigInt(Box::new(v))
        }
    }
}

impl From<BigDecimal> for FlakeValue {
    fn from(v: BigDecimal) -> Self {
        FlakeValue::Decimal(Box::new(v))
    }
}

impl From<DateTime> for FlakeValue {
    fn from(v: DateTime) -> Self {
        FlakeValue::DateTime(Box::new(v))
    }
}

impl From<Date> for FlakeValue {
    fn from(v: Date) -> Self {
        FlakeValue::Date(Box::new(v))
    }
}

impl From<Time> for FlakeValue {
    fn from(v: Time) -> Self {
        FlakeValue::Time(Box::new(v))
    }
}

impl From<GeoPointBits> for FlakeValue {
    fn from(v: GeoPointBits) -> Self {
        FlakeValue::GeoPoint(v)
    }
}

// === Numeric Parsing Functions ===

/// Parse an integer value, using i64 when possible, BigInt when necessary.
///
/// This is the entry point for parsing xsd:integer, xsd:long, xsd:int, etc.
pub fn parse_integer(value: &serde_json::Value) -> Result<FlakeValue, String> {
    match value {
        serde_json::Value::Number(n) => {
            // Fast path: i64
            if let Some(i) = n.as_i64() {
                return Ok(FlakeValue::Long(i));
            }
            // u64 that exceeds i64::MAX
            if let Some(u) = n.as_u64() {
                return Ok(FlakeValue::BigInt(Box::new(BigInt::from(u))));
            }
            Err("Number cannot be represented as integer".to_string())
        }
        serde_json::Value::String(s) => parse_integer_string(s),
        _ => Err("Expected number or string for integer".to_string()),
    }
}

/// Parse an integer from a string, normalizing to i64 when possible.
pub fn parse_integer_string(s: &str) -> Result<FlakeValue, String> {
    // Fast path: try i64
    if let Ok(i) = s.parse::<i64>() {
        return Ok(FlakeValue::Long(i));
    }
    // Slow path: BigInt
    s.parse::<BigInt>()
        .map(|bi| {
            // Normalize: if it fits in i64, use Long
            if let Some(i) = bi.to_i64() {
                FlakeValue::Long(i)
            } else {
                FlakeValue::BigInt(Box::new(bi))
            }
        })
        .map_err(|e| format!("Invalid integer '{s}': {e}"))
}

/// Parse a decimal value, using f64 when safe, BigDecimal for exact representation.
///
/// This is the entry point for parsing xsd:decimal.
pub fn parse_decimal(value: &serde_json::Value) -> Result<FlakeValue, String> {
    match value {
        serde_json::Value::Number(n) => {
            // JSON integer → Long
            if let Some(i) = n.as_i64() {
                return Ok(FlakeValue::Long(i));
            }
            // JSON float → Double (JSON numbers are inherently limited precision)
            if let Some(f) = n.as_f64() {
                return Ok(FlakeValue::Double(f));
            }
            Err("Invalid number for decimal".to_string())
        }
        // Explicit decimal precision requested via string lexical form
        serde_json::Value::String(s) => parse_decimal_string(s),
        _ => Err("Expected number or string for decimal".to_string()),
    }
}

/// Parse a decimal from a string, using BigDecimal for exact representation.
pub fn parse_decimal_string(s: &str) -> Result<FlakeValue, String> {
    // Check significant digits - f64 has ~15-16 digits of precision
    let sig_digits = count_significant_digits(s);

    if sig_digits <= 15 {
        if let Ok(f) = s.parse::<f64>() {
            if f.is_finite() {
                // Check if it's actually a whole number
                if f.fract() == 0.0 {
                    if let Ok(i) = s.parse::<i64>() {
                        return Ok(FlakeValue::Long(i));
                    }
                }
                return Ok(FlakeValue::Double(f));
            }
        }
    }

    // Too many digits or special case — use BigDecimal
    s.parse::<BigDecimal>()
        .map(|bd| FlakeValue::Decimal(Box::new(bd)))
        .map_err(|e| format!("Invalid decimal '{s}': {e}"))
}

/// Parse a double/float value.
pub fn parse_double(value: &serde_json::Value) -> Result<FlakeValue, String> {
    match value {
        serde_json::Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                Ok(FlakeValue::Double(f))
            } else {
                Err("Cannot convert to double".to_string())
            }
        }
        serde_json::Value::String(s) => s
            .parse::<f64>()
            .map(FlakeValue::Double)
            .map_err(|e| format!("Invalid double '{s}': {e}")),
        _ => Err("Expected number or string for double".to_string()),
    }
}

/// Count significant digits in a numeric string (for precision checking).
fn count_significant_digits(s: &str) -> usize {
    s.chars()
        .filter(char::is_ascii_digit)
        .skip_while(|&c| c == '0') // Skip leading zeros
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_ordering() {
        // Type ordering: Null < Ref < Boolean < Numeric < Temporal < String < Json < Vector
        let null = FlakeValue::Null;
        let ref_val = FlakeValue::Ref(Sid::new(1, "test"));
        let bool_val = FlakeValue::Boolean(true);
        let long_val = FlakeValue::Long(42);
        let double_val = FlakeValue::Double(3.13);
        let string_val = FlakeValue::String("hello".to_string());
        let vector_val = FlakeValue::Vector(vec![1.0, 2.0]);

        assert!(null < ref_val);
        assert!(ref_val < bool_val);
        assert!(bool_val < long_val);
        // Numeric types are compared by value, not discriminant
        // long_val (42) > double_val (3.14) mathematically
        assert!(double_val < long_val);
        assert!(long_val < string_val);
        assert!(string_val < vector_val);
    }

    #[test]
    fn test_numeric_class_comparison() {
        // All numeric types should be compared mathematically
        let long_3 = FlakeValue::Long(3);
        let double_3_5 = FlakeValue::Double(3.5);
        let long_4 = FlakeValue::Long(4);

        // Numeric ordering semantics: 3 < 3.5 < 4 regardless of type
        assert!(long_3 < double_3_5);
        assert!(double_3_5 < long_4);
    }

    #[test]
    fn test_numeric_equality_across_types() {
        // Long(3) and Double(3.0) should compare equal in value
        let long_3 = FlakeValue::Long(3);
        let double_3 = FlakeValue::Double(3.0);

        // They should compare as equal by numeric_cmp
        assert_eq!(long_3.numeric_cmp(&double_3), Some(Ordering::Equal));

        // Ord should return Equal for mathematically equal values
        // (cmp_object uses dt as tie-breaker, not FlakeValue::cmp)
        assert_eq!(long_3.cmp(&double_3), Ordering::Equal);
        assert_eq!(double_3.cmp(&long_3), Ordering::Equal);

        // Verify PartialEq also considers them equal
        assert_eq!(long_3, double_3);
    }

    #[test]
    fn test_numeric_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn compute_hash<T: Hash>(t: &T) -> u64 {
            let mut s = DefaultHasher::new();
            t.hash(&mut s);
            s.finish()
        }

        // All representations of 3 must hash identically (required for HashMap/HashSet)
        let long_3 = FlakeValue::Long(3);
        let double_3 = FlakeValue::Double(3.0);
        let bigint_3 = FlakeValue::BigInt(Box::new(BigInt::from(3)));
        let decimal_3 = FlakeValue::Decimal(Box::new(BigDecimal::from(3)));
        let decimal_3_00 = FlakeValue::Decimal(Box::new("3.00".parse::<BigDecimal>().unwrap()));

        // All these are equal
        assert_eq!(long_3, double_3);
        assert_eq!(long_3, bigint_3);
        assert_eq!(long_3, decimal_3);
        assert_eq!(long_3, decimal_3_00);

        // Therefore they must have the same hash
        let h_long = compute_hash(&long_3);
        let h_double = compute_hash(&double_3);
        let h_bigint = compute_hash(&bigint_3);
        let h_decimal = compute_hash(&decimal_3);
        let h_decimal_00 = compute_hash(&decimal_3_00);

        assert_eq!(
            h_long, h_double,
            "Long(3) and Double(3.0) must hash equally"
        );
        assert_eq!(h_long, h_bigint, "Long(3) and BigInt(3) must hash equally");
        assert_eq!(
            h_long, h_decimal,
            "Long(3) and Decimal(3) must hash equally"
        );
        assert_eq!(
            h_decimal, h_decimal_00,
            "Decimal(3) and Decimal(3.00) must hash equally"
        );
    }

    #[test]
    fn test_same_type_ordering() {
        // Longs
        assert!(FlakeValue::Long(1) < FlakeValue::Long(2));
        assert!(FlakeValue::Long(-1) < FlakeValue::Long(1));

        // Strings
        assert!(FlakeValue::String("a".to_string()) < FlakeValue::String("b".to_string()));
        assert!(FlakeValue::String("aa".to_string()) < FlakeValue::String("ab".to_string()));

        // Booleans
        assert!(FlakeValue::Boolean(false) < FlakeValue::Boolean(true));

        // Refs
        let ref1 = FlakeValue::Ref(Sid::new(1, "a"));
        let ref2 = FlakeValue::Ref(Sid::new(1, "b"));
        assert!(ref1 < ref2);
    }

    #[test]
    fn test_min_max() {
        let min = FlakeValue::min();
        let max = FlakeValue::max();
        let regular = FlakeValue::Long(42);

        assert!(min < regular);
        assert!(regular < max);

        // max sentinel should be greater than any "real" vector values
        let v1 = FlakeValue::Vector(vec![0.0]);
        let v2 = FlakeValue::Vector(vec![f64::MAX]);
        let v3 = FlakeValue::Vector(vec![f64::MAX, 0.0]);
        assert!(v1 < max);
        assert!(v2 < max);
        assert!(v3 < max);
    }

    #[test]
    fn test_equality() {
        assert_eq!(FlakeValue::Long(42), FlakeValue::Long(42));
        assert_ne!(FlakeValue::Long(42), FlakeValue::Long(43));

        // Cross-type numeric comparison: values equal but types different
        // PartialEq uses numeric_cmp which returns Equal for Long(3) == Double(3.0)
        assert_eq!(FlakeValue::Long(3), FlakeValue::Double(3.0));

        assert_eq!(
            FlakeValue::String("test".to_string()),
            FlakeValue::String("test".to_string())
        );
    }

    #[test]
    fn test_double_nan_ordering() {
        // NaN should have deterministic ordering
        let nan1 = FlakeValue::Double(f64::NAN);
        let nan2 = FlakeValue::Double(f64::NAN);

        // NaN == NaN for our purposes (using to_bits)
        assert_eq!(nan1, nan2);
        assert_eq!(nan1.cmp(&nan2), Ordering::Equal);
    }

    #[test]
    fn test_bigint_comparison() {
        let big = FlakeValue::BigInt(Box::new(
            BigInt::parse_bytes(b"99999999999999999999", 10).unwrap(),
        ));
        let small = FlakeValue::Long(100);
        let double = FlakeValue::Double(1e18);

        assert!(small < big);
        assert!(double < big);
    }

    #[test]
    fn test_parse_integer() {
        // i64 range
        let val = parse_integer(&serde_json::json!(42)).unwrap();
        assert_eq!(val, FlakeValue::Long(42));

        // String that fits i64
        let val = parse_integer(&serde_json::json!("12345")).unwrap();
        assert_eq!(val, FlakeValue::Long(12345));

        // String that exceeds i64
        let val = parse_integer(&serde_json::json!("99999999999999999999")).unwrap();
        assert!(matches!(val, FlakeValue::BigInt(_)));
    }

    #[test]
    fn test_parse_decimal() {
        // JSON number → Double
        let val = parse_decimal(&serde_json::json!(3.13)).unwrap();
        assert!(matches!(val, FlakeValue::Double(_)));

        // String with many digits → BigDecimal
        let val = parse_decimal(&serde_json::json!("3.141592653589793238462643383279")).unwrap();
        assert!(matches!(val, FlakeValue::Decimal(_)));
    }

    #[test]
    fn test_datetime_flakevalue() {
        let dt = DateTime::parse("2024-01-15T10:30:00Z").unwrap();
        let val = FlakeValue::from(dt);
        assert!(val.is_temporal());
        assert!(val.as_datetime().is_some());
    }

    // === HLL Canonical Hash Tests ===

    mod hll_hash_tests {
        use super::*;

        #[test]
        fn test_canonical_hash_deterministic() {
            // Same value should always produce same hash
            let v1 = FlakeValue::Long(42);
            let v2 = FlakeValue::Long(42);
            assert_eq!(v1.canonical_hash(), v2.canonical_hash());

            let s1 = FlakeValue::String("hello".to_string());
            let s2 = FlakeValue::String("hello".to_string());
            assert_eq!(s1.canonical_hash(), s2.canonical_hash());
        }

        #[test]
        fn test_canonical_hash_type_collision_prevention() {
            // String "true" should NOT hash to same value as Boolean true
            let bool_true = FlakeValue::Boolean(true);
            let str_true = FlakeValue::String("true".to_string());
            assert_ne!(bool_true.canonical_hash(), str_true.canonical_hash());

            // String "42" should NOT hash to same value as Long 42
            let long_42 = FlakeValue::Long(42);
            let str_42 = FlakeValue::String("42".to_string());
            assert_ne!(long_42.canonical_hash(), str_42.canonical_hash());
        }

        #[test]
        fn test_canonical_hash_nan_determinism() {
            // All NaN values should produce the same hash
            let nan1 = FlakeValue::Double(f64::NAN);
            let nan2 = FlakeValue::Double(-f64::NAN);
            let nan3 = FlakeValue::Double(f64::from_bits(0x7ff8_0000_0000_0001)); // different NaN payload

            assert_eq!(nan1.canonical_hash(), nan2.canonical_hash());
            assert_eq!(nan2.canonical_hash(), nan3.canonical_hash());
        }

        #[test]
        fn test_canonical_hash_zero_determinism() {
            // -0.0 and +0.0 should produce the same hash
            let pos_zero = FlakeValue::Double(0.0);
            let neg_zero = FlakeValue::Double(-0.0);
            assert_eq!(pos_zero.canonical_hash(), neg_zero.canonical_hash());
        }

        #[test]
        fn test_canonical_hash_different_values() {
            // Different values should (almost certainly) produce different hashes
            let values = vec![
                FlakeValue::Null,
                FlakeValue::Boolean(true),
                FlakeValue::Boolean(false),
                FlakeValue::Long(0),
                FlakeValue::Long(1),
                FlakeValue::Long(-1),
                FlakeValue::Double(0.0),
                FlakeValue::Double(1.0),
                FlakeValue::String(String::new()),
                FlakeValue::String("a".to_string()),
                FlakeValue::String("b".to_string()),
                FlakeValue::Ref(Sid::new(1, "test")),
                FlakeValue::Ref(Sid::new(2, "test")),
            ];

            let hashes: Vec<u64> = values
                .iter()
                .map(super::super::FlakeValue::canonical_hash)
                .collect();

            // Check for uniqueness (no collisions in this small set)
            let unique_count = {
                let mut set = std::collections::HashSet::new();
                for h in &hashes {
                    set.insert(*h);
                }
                set.len()
            };
            assert_eq!(
                unique_count,
                hashes.len(),
                "Hash collision detected in test values"
            );
        }
    }
}
