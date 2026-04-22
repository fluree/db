//! Object key encoding for index records.
//!
//! Index records encode object values as a pair `(ObjKind, ObjKey)`:
//!
//! - [`ObjKind`] is a `u8` discriminant that selects the storage/comparison
//!   family (integer, float, ref, string, temporal, …). Ordering across kinds
//!   is lexicographic on the raw byte.
//!
//! - [`ObjKey`] is a `u64` payload whose interpretation depends on the kind.
//!   Within a kind the natural `u64` ordering is correct (e.g., `NumInt` uses
//!   an XOR sign-flip so that negative integers sort before positive ones).
//!
//! Together, `(ObjKind, ObjKey)` forms an order-preserving composite key:
//! compare kind first, then key. Cross-kind numeric equivalence (e.g.,
//! `NumInt(3)` vs `NumF64(3.0)`) is a query-layer concern resolved via
//! multi-scan merge, not an index property.
//!
//! [`ValueTypeTag`] is a compact `u8` identifier for XSD/RDF datatypes, used as
//! a tie-breaker in index sort keys so that values with the same `(ObjKind,
//! ObjKey)` but different types (e.g., `xsd:integer 3` vs `xsd:long 3`)
//! remain distinguishable.

use crate::Sid;
use fluree_vocab::{jsonld_names, namespaces, rdf_names, xsd_names};
use std::fmt;
use std::sync::OnceLock;

// ============================================================================
// ObjKind
// ============================================================================

/// Type discriminant for object values in index records.
///
/// Each variant selects a storage family and determines how [`ObjKey`] bits
/// are interpreted. The raw `u8` value defines cross-kind sort order.
///
/// `Min` (0x00) and `Max` (0xFF) are sentinel-only constants used for
/// constructing range bounds — they are never stored in index data.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct ObjKind(u8);

impl ObjKind {
    // ---- Sentinel constants (never stored) ----

    /// Minimum sentinel — sorts before every stored kind.
    pub const MIN: Self = Self(0x00);

    /// Maximum sentinel — sorts after every stored kind.
    pub const MAX: Self = Self(0xFF);

    // ---- Stored kind constants (consistent with VALUE_ID_PROPOSAL tag set) ----

    /// Null value.
    pub const NULL: Self = Self(0x01);

    /// Boolean (o_key: 0 = false, 1 = true).
    pub const BOOL: Self = Self(0x02);

    /// Signed 64-bit integer, order-preserving XOR-sign encoding.
    pub const NUM_INT: Self = Self(0x03);

    /// IEEE 754 f64, order-preserving total-order bit transform.
    /// NaN and ±Inf are rejected at ingest; `-0.0` is canonicalized to `+0.0`.
    pub const NUM_F64: Self = Self(0x04);

    /// IRI reference — u32 subject dictionary ID (zero-extended in ObjKey).
    pub const REF_ID: Self = Self(0x05);

    /// Lexicographic string — u32 string dictionary ID (zero-extended in ObjKey).
    pub const LEX_ID: Self = Self(0x06);

    /// xsd:date — days since Unix epoch, order-preserving signed encoding.
    pub const DATE: Self = Self(0x07);

    /// xsd:time — microseconds since midnight (unsigned).
    pub const TIME: Self = Self(0x08);

    /// xsd:dateTime — epoch microseconds, order-preserving signed encoding.
    pub const DATE_TIME: Self = Self(0x09);

    /// Vector arena handle (u32 zero-extended).
    pub const VECTOR_ID: Self = Self(0x0A);

    /// JSON arena handle (u32 zero-extended).
    pub const JSON_ID: Self = Self(0x0B);

    /// Overflow BigInt/BigDecimal — equality-only arena handle (u32 zero-extended).
    /// Not ordered by numeric value in the index; range queries post-filter.
    pub const NUM_BIG: Self = Self(0x0C);

    /// xsd:gYear — signed year, order-preserving encoding via i64.
    pub const G_YEAR: Self = Self(0x0D);

    /// xsd:gYearMonth — (year, month) packed, order-preserving signed encoding.
    pub const G_YEAR_MONTH: Self = Self(0x0E);

    /// xsd:gMonth — month number 1..=12 (unsigned).
    pub const G_MONTH: Self = Self(0x0F);

    /// xsd:gDay — day number 1..=31 (unsigned).
    pub const G_DAY: Self = Self(0x10);

    /// xsd:gMonthDay — (month, day) packed unsigned.
    pub const G_MONTH_DAY: Self = Self(0x11);

    /// xsd:yearMonthDuration — total months (signed), order-preserving encoding.
    pub const YEAR_MONTH_DUR: Self = Self(0x12);

    /// xsd:dayTimeDuration — total microseconds (signed), order-preserving encoding.
    pub const DAY_TIME_DUR: Self = Self(0x13);

    /// GeoPoint — latitude/longitude encoded as 60-bit packed value.
    /// Upper 30 bits: latitude scaled from [-90, 90] to [0, 2^30-1]
    /// Lower 30 bits: longitude scaled from [-180, 180] to [0, 2^30-1]
    /// Precision: approximately 0.3mm at the equator.
    pub const GEO_POINT: Self = Self(0x14);

    /// Get the raw `u8` discriminant.
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self.0
    }

    /// Construct from a raw `u8`.
    #[inline]
    pub const fn from_u8(raw: u8) -> Self {
        Self(raw)
    }

    /// Returns `true` if this is a sentinel-only kind (`Min` or `Max`).
    #[inline]
    pub const fn is_sentinel(self) -> bool {
        self.0 == 0x00 || self.0 == 0xFF
    }
}

impl fmt::Debug for ObjKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            0x00 => write!(f, "ObjKind::Min"),
            0x01 => write!(f, "ObjKind::Null"),
            0x02 => write!(f, "ObjKind::Bool"),
            0x03 => write!(f, "ObjKind::NumInt"),
            0x04 => write!(f, "ObjKind::NumF64"),
            0x05 => write!(f, "ObjKind::RefId"),
            0x06 => write!(f, "ObjKind::LexId"),
            0x07 => write!(f, "ObjKind::Date"),
            0x08 => write!(f, "ObjKind::Time"),
            0x09 => write!(f, "ObjKind::DateTime"),
            0x0A => write!(f, "ObjKind::VectorId"),
            0x0B => write!(f, "ObjKind::JsonId"),
            0x0C => write!(f, "ObjKind::NumBig"),
            0x0D => write!(f, "ObjKind::GYear"),
            0x0E => write!(f, "ObjKind::GYearMonth"),
            0x0F => write!(f, "ObjKind::GMonth"),
            0x10 => write!(f, "ObjKind::GDay"),
            0x11 => write!(f, "ObjKind::GMonthDay"),
            0x12 => write!(f, "ObjKind::YearMonthDur"),
            0x13 => write!(f, "ObjKind::DayTimeDur"),
            0x14 => write!(f, "ObjKind::GeoPoint"),
            0xFF => write!(f, "ObjKind::Max"),
            n => write!(f, "ObjKind({n:#04x})"),
        }
    }
}

impl fmt::Display for ObjKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

// ============================================================================
// ObjKey
// ============================================================================

/// 64-bit payload for object values in index records.
///
/// The encoding depends on the accompanying [`ObjKind`]; within a kind the
/// natural `u64` ordering is correct.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct ObjKey(u64);

/// Sign bit mask for the i64 ↔ u64 order-preserving transform.
const SIGN_FLIP: u64 = 1u64 << 63;

/// Sign bit mask for f64 bits.
const F64_SIGN_BIT: u64 = 1u64 << 63;

/// Error returned when a value cannot be stored in the index.
#[derive(Debug, Clone, PartialEq)]
pub enum ObjKeyError {
    /// f64 is NaN — not representable in ordered index.
    NaN,
    /// f64 is +Inf or -Inf — not representable in ordered index.
    Infinite,
    /// Latitude is outside [-90, 90] or non-finite.
    GeoLatitudeOutOfRange,
    /// Longitude is outside [-180, 180] or non-finite.
    GeoLongitudeOutOfRange,
}

impl fmt::Display for ObjKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NaN => write!(f, "NaN is not allowed in index values"),
            Self::Infinite => write!(f, "infinite values are not allowed in index values"),
            Self::GeoLatitudeOutOfRange => {
                write!(f, "latitude must be finite and in range [-90, 90]")
            }
            Self::GeoLongitudeOutOfRange => {
                write!(f, "longitude must be finite and in range [-180, 180]")
            }
        }
    }
}

impl std::error::Error for ObjKeyError {}

impl ObjKey {
    /// The zero key — used as the null/false/min-within-kind payload.
    pub const ZERO: Self = Self(0);

    /// The maximum key — used for unbounded upper range bounds within a kind.
    pub const MAX: Self = Self(u64::MAX);

    // ---- Signed integer encoding (NumInt, Date, DateTime) ----

    /// Encode a signed `i64` as an order-preserving `u64`.
    ///
    /// Uses the classic XOR-sign-bit transform: `(v as u64) ^ (1 << 63)`.
    /// This maps `i64::MIN → 0`, `0 → 2^63`, `i64::MAX → u64::MAX`.
    #[inline]
    pub const fn encode_i64(value: i64) -> Self {
        Self((value as u64) ^ SIGN_FLIP)
    }

    /// Decode an order-preserving `u64` back to `i64`.
    #[inline]
    pub const fn decode_i64(self) -> i64 {
        (self.0 ^ SIGN_FLIP) as i64
    }

    // ---- Float encoding (NumF64) ----

    /// Encode a finite `f64` as an order-preserving `u64`.
    ///
    /// Rejects NaN and ±Inf. Canonicalizes `-0.0` to `+0.0`.
    ///
    /// The transform:
    /// - For positive (sign bit clear): flip only the sign bit → maps `+0.0`
    ///   to `2^63` and larger positives to larger `u64` values.
    /// - For negative (sign bit set): flip ALL bits → maps `-0.0` to `2^63 - 1`
    ///   (but we canonicalize to +0.0 first) and more-negative values to smaller
    ///   `u64` values.
    #[inline]
    pub fn encode_f64(value: f64) -> Result<Self, ObjKeyError> {
        if value.is_nan() {
            return Err(ObjKeyError::NaN);
        }
        if value.is_infinite() {
            return Err(ObjKeyError::Infinite);
        }
        // Canonicalize -0.0 → +0.0
        let value = if value == 0.0 { 0.0 } else { value };
        let bits = value.to_bits();
        let key = if bits & F64_SIGN_BIT != 0 {
            !bits // negative: flip all bits
        } else {
            bits ^ F64_SIGN_BIT // positive: flip sign bit only
        };
        Ok(Self(key))
    }

    /// Decode an order-preserving `u64` back to `f64`.
    ///
    /// Inverse of [`encode_f64`](Self::encode_f64).
    #[inline]
    pub fn decode_f64(self) -> f64 {
        let bits = if self.0 & F64_SIGN_BIT != 0 {
            self.0 ^ F64_SIGN_BIT // was positive: undo sign flip
        } else {
            !self.0 // was negative: undo full flip
        };
        f64::from_bits(bits)
    }

    // ---- Boolean encoding ----

    /// Encode a boolean (false = 0, true = 1).
    #[inline]
    pub const fn encode_bool(value: bool) -> Self {
        Self(value as u64)
    }

    /// Decode a boolean.
    #[inline]
    pub const fn decode_bool(self) -> bool {
        self.0 != 0
    }

    // ---- Dictionary ID encodings (RefId, LexId, JsonId, VectorId, NumBig) ----

    /// Encode a `u32` dictionary ID (zero-extended to `u64`).
    ///
    /// Used for RefId, LexId, JsonId, VectorId, and NumBig arena handles.
    #[inline]
    pub const fn encode_u32_id(id: u32) -> Self {
        Self(id as u64)
    }

    /// Decode a `u32` dictionary ID.
    #[inline]
    pub const fn decode_u32_id(self) -> u32 {
        self.0 as u32
    }

    /// Encode a `u64` subject reference (sid64) directly.
    ///
    /// Used for REF_ID when subject IDs are namespace-structured 64-bit values.
    #[inline]
    pub const fn encode_sid64(id: u64) -> Self {
        Self(id)
    }

    /// Decode a `u64` subject reference (sid64).
    #[inline]
    pub const fn decode_sid64(self) -> u64 {
        self.0
    }

    // ---- Date/Time/DateTime convenience wrappers ----

    /// Encode days since Unix epoch (signed i32) using the signed transform.
    #[inline]
    pub const fn encode_date(days_since_epoch: i32) -> Self {
        Self::encode_i64(days_since_epoch as i64)
    }

    /// Decode days since Unix epoch.
    #[inline]
    pub const fn decode_date(self) -> i32 {
        self.decode_i64() as i32
    }

    /// Encode microseconds since midnight (unsigned, always ≥ 0).
    #[inline]
    pub const fn encode_time(micros_since_midnight: i64) -> Self {
        debug_assert!(micros_since_midnight >= 0);
        Self(micros_since_midnight as u64)
    }

    /// Decode microseconds since midnight.
    #[inline]
    pub const fn decode_time(self) -> i64 {
        self.0 as i64
    }

    /// Encode epoch microseconds (signed) using the signed transform.
    #[inline]
    pub const fn encode_datetime(epoch_micros: i64) -> Self {
        Self::encode_i64(epoch_micros)
    }

    /// Decode epoch microseconds.
    #[inline]
    pub const fn decode_datetime(self) -> i64 {
        self.decode_i64()
    }

    // ---- gYear / gYearMonth / gMonth / gDay / gMonthDay encodings ----

    /// Encode an xsd:gYear (signed year) using the signed i64 transform.
    #[inline]
    pub fn encode_g_year(year: i32) -> Self {
        Self::encode_i64(year as i64)
    }

    /// Decode an xsd:gYear back to a signed year.
    #[inline]
    pub fn decode_g_year(self) -> i32 {
        self.decode_i64() as i32
    }

    /// Encode an xsd:gYearMonth as `year * 12 + (month - 1)`, signed transform.
    #[inline]
    pub fn encode_g_year_month(year: i32, month: u32) -> Self {
        Self::encode_i64(year as i64 * 12 + (month as i64 - 1))
    }

    /// Decode an xsd:gYearMonth back to `(year, month)`.
    #[inline]
    pub fn decode_g_year_month(self) -> (i32, u32) {
        let v = self.decode_i64();
        let year = v.div_euclid(12) as i32;
        let month = (v.rem_euclid(12) + 1) as u32;
        (year, month)
    }

    /// Encode an xsd:gMonth (1..=12) as a plain unsigned value.
    #[inline]
    pub fn encode_g_month(month: u32) -> Self {
        Self(month as u64)
    }

    /// Decode an xsd:gMonth.
    #[inline]
    pub fn decode_g_month(self) -> u32 {
        self.0 as u32
    }

    /// Encode an xsd:gDay (1..=31) as a plain unsigned value.
    #[inline]
    pub fn encode_g_day(day: u32) -> Self {
        Self(day as u64)
    }

    /// Decode an xsd:gDay.
    #[inline]
    pub fn decode_g_day(self) -> u32 {
        self.0 as u32
    }

    /// Encode an xsd:gMonthDay as `(month - 1) * 31 + (day - 1)`, unsigned.
    #[inline]
    pub fn encode_g_month_day(month: u32, day: u32) -> Self {
        Self(((month - 1) * 31 + (day - 1)) as u64)
    }

    /// Decode an xsd:gMonthDay back to `(month, day)`.
    #[inline]
    pub fn decode_g_month_day(self) -> (u32, u32) {
        let v = self.0 as u32;
        let month = v / 31 + 1;
        let day = v % 31 + 1;
        (month, day)
    }

    // ---- Duration encodings (yearMonthDuration, dayTimeDuration) ----

    /// Encode an xsd:yearMonthDuration as total months (signed).
    #[inline]
    pub fn encode_year_month_dur(months: i32) -> Self {
        Self::encode_i64(months as i64)
    }

    /// Decode an xsd:yearMonthDuration back to total months.
    #[inline]
    pub fn decode_year_month_dur(self) -> i32 {
        self.decode_i64() as i32
    }

    /// Encode an xsd:dayTimeDuration as total microseconds (signed).
    #[inline]
    pub fn encode_day_time_dur(micros: i64) -> Self {
        Self::encode_i64(micros)
    }

    /// Decode an xsd:dayTimeDuration back to total microseconds.
    #[inline]
    pub fn decode_day_time_dur(self) -> i64 {
        self.decode_i64()
    }

    // ---- GeoPoint encoding (30-bit lat, 30-bit lng) ----

    /// Maximum encoded value for 30-bit coordinate component.
    const GEO_MAX_ENCODED: u64 = (1 << 30) - 1; // 1,073,741,823

    /// Encode latitude/longitude as a 60-bit packed value.
    ///
    /// Latitude is scaled from [-90, 90] to [0, 2^30-1].
    /// Longitude is scaled from [-180, 180] to [0, 2^30-1].
    /// Precision is approximately 0.3mm at the equator.
    ///
    /// Rejects non-finite coordinates. Canonicalizes -0.0 to +0.0.
    ///
    /// The encoding produces a **latitude-primary sort order**: values sort
    /// first by latitude, then by longitude within the same latitude band.
    #[inline]
    pub fn encode_geo_point(lat: f64, lng: f64) -> Result<Self, ObjKeyError> {
        // Validate and canonicalize latitude
        if !lat.is_finite() || !(-90.0..=90.0).contains(&lat) {
            return Err(ObjKeyError::GeoLatitudeOutOfRange);
        }
        // Validate and canonicalize longitude
        if !lng.is_finite() || !(-180.0..=180.0).contains(&lng) {
            return Err(ObjKeyError::GeoLongitudeOutOfRange);
        }

        // Canonicalize -0.0 to +0.0
        let lat = if lat == 0.0 { 0.0 } else { lat };
        let lng = if lng == 0.0 { 0.0 } else { lng };

        // Normalize to [0, 1] range
        let lat_norm = (lat + 90.0) / 180.0; // [-90, 90] → [0, 1]
        let lng_norm = (lng + 180.0) / 360.0; // [-180, 180] → [0, 1]

        // Scale to integer range
        let lat_encoded = (lat_norm * Self::GEO_MAX_ENCODED as f64).round() as u64;
        let lng_encoded = (lng_norm * Self::GEO_MAX_ENCODED as f64).round() as u64;

        // Pack: lat in upper 30 bits, lng in lower 30 bits
        Ok(Self((lat_encoded << 30) | lng_encoded))
    }

    /// Decode a 60-bit packed value back to (latitude, longitude).
    ///
    /// Inverse of [`encode_geo_point`](Self::encode_geo_point).
    #[inline]
    pub fn decode_geo_point(self) -> (f64, f64) {
        let lat_encoded = (self.0 >> 30) & Self::GEO_MAX_ENCODED;
        let lng_encoded = self.0 & Self::GEO_MAX_ENCODED;

        let lat = (lat_encoded as f64 / Self::GEO_MAX_ENCODED as f64) * 180.0 - 90.0;
        let lng = (lng_encoded as f64 / Self::GEO_MAX_ENCODED as f64) * 360.0 - 180.0;

        (lat, lng)
    }

    // ---- Raw access ----

    /// Get the raw `u64` value.
    #[inline]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Construct from a raw `u64`.
    #[inline]
    pub const fn from_u64(raw: u64) -> Self {
        Self(raw)
    }
}

impl fmt::Debug for ObjKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjKey({:#018x})", self.0)
    }
}

impl fmt::Display for ObjKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

// ============================================================================
// ValueTypeTag
// ============================================================================

/// Compact datatype identifier for index sort-key tie-breaking.
///
/// Maps (namespace_code, local_name) pairs to fixed u8 values.
/// The ordering of ValueTypeTag values is arbitrary but stable — it serves
/// only to distinguish types that share the same `(ObjKind, ObjKey)` payload.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[repr(transparent)]
pub struct ValueTypeTag(u8);

impl ValueTypeTag {
    // Fixed mapping constants
    pub const STRING: Self = Self(0);
    pub const BOOLEAN: Self = Self(1);
    pub const INTEGER: Self = Self(2);
    pub const LONG: Self = Self(3);
    pub const INT: Self = Self(4);
    pub const SHORT: Self = Self(5);
    pub const BYTE: Self = Self(6);
    pub const DOUBLE: Self = Self(7);
    pub const FLOAT: Self = Self(8);
    pub const DECIMAL: Self = Self(9);
    pub const DATE_TIME: Self = Self(10);
    pub const DATE: Self = Self(11);
    pub const TIME: Self = Self(12);
    pub const ANY_URI: Self = Self(13);
    pub const LANG_STRING: Self = Self(14);
    pub const JSON_LD_ID: Self = Self(16);
    pub const UNSIGNED_LONG: Self = Self(17);
    pub const UNSIGNED_INT: Self = Self(18);
    pub const UNSIGNED_SHORT: Self = Self(19);
    pub const UNSIGNED_BYTE: Self = Self(20);
    pub const NON_NEGATIVE_INTEGER: Self = Self(21);
    pub const POSITIVE_INTEGER: Self = Self(22);
    pub const NON_POSITIVE_INTEGER: Self = Self(23);
    pub const NEGATIVE_INTEGER: Self = Self(24);
    pub const NORMALIZED_STRING: Self = Self(25);
    pub const TOKEN: Self = Self(26);
    pub const LANGUAGE: Self = Self(27);
    pub const DURATION: Self = Self(28);
    pub const DAY_TIME_DURATION: Self = Self(29);
    pub const YEAR_MONTH_DURATION: Self = Self(30);
    pub const BASE64_BINARY: Self = Self(31);
    pub const HEX_BINARY: Self = Self(32);
    pub const RDF_JSON: Self = Self(33);
    pub const G_YEAR: Self = Self(15);
    pub const G_MONTH: Self = Self(34);
    pub const G_DAY: Self = Self(35);
    pub const G_YEAR_MONTH: Self = Self(36);
    pub const G_MONTH_DAY: Self = Self(37);
    pub const VECTOR: Self = Self(38);
    pub const FULL_TEXT: Self = Self(39);
    pub const UNKNOWN: Self = Self(255);

    /// Resolve a (namespace_code, local_name) pair to a ValueTypeTag.
    ///
    /// This is the primary entry point for the resolver. Matches against
    /// well-known `fluree_vocab` namespace codes and local names.
    #[inline]
    pub fn from_ns_name(ns_code: u16, name: &str) -> Self {
        match ns_code {
            namespaces::XSD => Self::from_xsd_name(name),
            namespaces::RDF => Self::from_rdf_name(name),
            namespaces::JSON_LD => Self::from_jsonld_name(name),
            namespaces::FLUREE_DB => Self::from_fluree_db_name(name),
            _ => Self::UNKNOWN,
        }
    }

    /// Resolve an XSD local name to ValueTypeTag.
    fn from_xsd_name(name: &str) -> Self {
        fluree_vocab::datatype::KnownDatatype::from_xsd_local(name)
            .map(Self::from_known_datatype)
            .unwrap_or(Self::UNKNOWN)
    }

    /// Resolve an RDF local name to ValueTypeTag.
    fn from_rdf_name(name: &str) -> Self {
        fluree_vocab::datatype::KnownDatatype::from_rdf_local(name)
            .map(Self::from_known_datatype)
            .unwrap_or(Self::UNKNOWN)
    }

    /// Resolve a JSON-LD local name to ValueTypeTag.
    fn from_jsonld_name(name: &str) -> Self {
        fluree_vocab::datatype::KnownDatatype::from_jsonld_local(name)
            .map(Self::from_known_datatype)
            .unwrap_or(Self::UNKNOWN)
    }

    /// Resolve a Fluree DB local name to ValueTypeTag.
    fn from_fluree_db_name(name: &str) -> Self {
        fluree_vocab::datatype::KnownDatatype::from_fluree_db_local(name)
            .map(Self::from_known_datatype)
            .unwrap_or(Self::UNKNOWN)
    }

    /// Map a shared `KnownDatatype` to its `ValueTypeTag`.
    ///
    /// This is the single conversion table; the `from_*_name` helpers above
    /// all delegate here after `fluree_vocab::datatype::KnownDatatype`
    /// handles the vocabulary recognition.
    fn from_known_datatype(dt: fluree_vocab::datatype::KnownDatatype) -> Self {
        use fluree_vocab::datatype::KnownDatatype::*;
        match dt {
            XsdString => Self::STRING,
            XsdBoolean => Self::BOOLEAN,
            XsdInteger => Self::INTEGER,
            XsdLong => Self::LONG,
            XsdInt => Self::INT,
            XsdShort => Self::SHORT,
            XsdByte => Self::BYTE,
            XsdDouble => Self::DOUBLE,
            XsdFloat => Self::FLOAT,
            XsdDecimal => Self::DECIMAL,
            XsdDateTime => Self::DATE_TIME,
            XsdDate => Self::DATE,
            XsdTime => Self::TIME,
            XsdAnyUri => Self::ANY_URI,
            XsdUnsignedLong => Self::UNSIGNED_LONG,
            XsdUnsignedInt => Self::UNSIGNED_INT,
            XsdUnsignedShort => Self::UNSIGNED_SHORT,
            XsdUnsignedByte => Self::UNSIGNED_BYTE,
            XsdNonNegativeInteger => Self::NON_NEGATIVE_INTEGER,
            XsdPositiveInteger => Self::POSITIVE_INTEGER,
            XsdNonPositiveInteger => Self::NON_POSITIVE_INTEGER,
            XsdNegativeInteger => Self::NEGATIVE_INTEGER,
            XsdNormalizedString => Self::NORMALIZED_STRING,
            XsdToken => Self::TOKEN,
            XsdLanguage => Self::LANGUAGE,
            XsdDuration => Self::DURATION,
            XsdDayTimeDuration => Self::DAY_TIME_DURATION,
            XsdYearMonthDuration => Self::YEAR_MONTH_DURATION,
            XsdBase64Binary => Self::BASE64_BINARY,
            XsdHexBinary => Self::HEX_BINARY,
            XsdGYear => Self::G_YEAR,
            XsdGMonth => Self::G_MONTH,
            XsdGDay => Self::G_DAY,
            XsdGYearMonth => Self::G_YEAR_MONTH,
            XsdGMonthDay => Self::G_MONTH_DAY,
            RdfJson => Self::RDF_JSON,
            RdfLangString => Self::LANG_STRING,
            JsonLdId => Self::JSON_LD_ID,
            FlureeEmbeddingVector => Self::VECTOR,
            FlureeFullText => Self::FULL_TEXT,
        }
    }

    /// Get the raw u8 value.
    #[inline]
    pub fn as_u8(self) -> u8 {
        self.0
    }

    /// Construct from raw u8.
    #[inline]
    pub fn from_u8(raw: u8) -> Self {
        Self(raw)
    }

    /// Check if this is an integer-like type (all XSD integer subtypes).
    #[inline]
    pub fn is_integer_type(self) -> bool {
        matches!(
            self.0,
            2 | 3 | 4 | 5 | 6 | 17 | 18 | 19 | 20 | 21 | 22 | 23 | 24
        )
    }

    /// Check if this is a floating-point type (double or float).
    #[inline]
    pub fn is_float_type(self) -> bool {
        self == Self::DOUBLE || self == Self::FLOAT
    }

    /// Convert to the corresponding `Sid` for result formatting.
    ///
    /// Uses a process-wide `OnceLock` cache — zero allocation after first access.
    /// Returns `None` for `UNKNOWN` (255).
    pub fn to_sid(self) -> Option<&'static Sid> {
        static TABLE: OnceLock<Vec<Option<Sid>>> = OnceLock::new();

        let table = TABLE.get_or_init(|| {
            let mut t: Vec<Option<Sid>> = vec![None; 38];
            t[0] = Some(Sid::new(namespaces::XSD, xsd_names::STRING));
            t[1] = Some(Sid::new(namespaces::XSD, xsd_names::BOOLEAN));
            t[2] = Some(Sid::new(namespaces::XSD, xsd_names::INTEGER));
            t[3] = Some(Sid::new(namespaces::XSD, xsd_names::LONG));
            t[4] = Some(Sid::new(namespaces::XSD, xsd_names::INT));
            t[5] = Some(Sid::new(namespaces::XSD, xsd_names::SHORT));
            t[6] = Some(Sid::new(namespaces::XSD, xsd_names::BYTE));
            t[7] = Some(Sid::new(namespaces::XSD, xsd_names::DOUBLE));
            t[8] = Some(Sid::new(namespaces::XSD, xsd_names::FLOAT));
            t[9] = Some(Sid::new(namespaces::XSD, xsd_names::DECIMAL));
            t[10] = Some(Sid::new(namespaces::XSD, xsd_names::DATE_TIME));
            t[11] = Some(Sid::new(namespaces::XSD, xsd_names::DATE));
            t[12] = Some(Sid::new(namespaces::XSD, xsd_names::TIME));
            t[13] = Some(Sid::new(namespaces::XSD, xsd_names::ANY_URI));
            t[14] = Some(Sid::new(namespaces::RDF, rdf_names::LANG_STRING));
            t[15] = Some(Sid::new(namespaces::XSD, xsd_names::G_YEAR));
            t[16] = Some(Sid::new(namespaces::JSON_LD, jsonld_names::ID));
            t[17] = Some(Sid::new(namespaces::XSD, xsd_names::UNSIGNED_LONG));
            t[18] = Some(Sid::new(namespaces::XSD, xsd_names::UNSIGNED_INT));
            t[19] = Some(Sid::new(namespaces::XSD, xsd_names::UNSIGNED_SHORT));
            t[20] = Some(Sid::new(namespaces::XSD, xsd_names::UNSIGNED_BYTE));
            t[21] = Some(Sid::new(namespaces::XSD, xsd_names::NON_NEGATIVE_INTEGER));
            t[22] = Some(Sid::new(namespaces::XSD, xsd_names::POSITIVE_INTEGER));
            t[23] = Some(Sid::new(namespaces::XSD, xsd_names::NON_POSITIVE_INTEGER));
            t[24] = Some(Sid::new(namespaces::XSD, xsd_names::NEGATIVE_INTEGER));
            t[25] = Some(Sid::new(namespaces::XSD, xsd_names::NORMALIZED_STRING));
            t[26] = Some(Sid::new(namespaces::XSD, xsd_names::TOKEN));
            t[27] = Some(Sid::new(namespaces::XSD, xsd_names::LANGUAGE));
            t[28] = Some(Sid::new(namespaces::XSD, xsd_names::DURATION));
            t[29] = Some(Sid::new(namespaces::XSD, xsd_names::DAY_TIME_DURATION));
            t[30] = Some(Sid::new(namespaces::XSD, xsd_names::YEAR_MONTH_DURATION));
            t[31] = Some(Sid::new(namespaces::XSD, xsd_names::BASE64_BINARY));
            t[32] = Some(Sid::new(namespaces::XSD, xsd_names::HEX_BINARY));
            t[33] = Some(Sid::new(namespaces::RDF, rdf_names::JSON));
            t[34] = Some(Sid::new(namespaces::XSD, xsd_names::G_MONTH));
            t[35] = Some(Sid::new(namespaces::XSD, xsd_names::G_DAY));
            t[36] = Some(Sid::new(namespaces::XSD, xsd_names::G_YEAR_MONTH));
            t[37] = Some(Sid::new(namespaces::XSD, xsd_names::G_MONTH_DAY));
            t
        });

        let idx = self.0 as usize;
        if idx < table.len() {
            table[idx].as_ref()
        } else {
            None
        }
    }

    /// Convert to the corresponding reserved `DatatypeDictId`, if this tag
    /// has a well-known dictionary position.
    ///
    /// Returns `None` for tags that have no reserved dictionary ID (e.g.,
    /// `ANY_URI`, `UNKNOWN`, numeric sub-types like `INT`, `SHORT`, etc.).
    pub fn to_reserved_dict_id(&self) -> Option<crate::DatatypeDictId> {
        use crate::DatatypeDictId;
        match *self {
            Self::JSON_LD_ID => Some(DatatypeDictId::ID),
            Self::STRING => Some(DatatypeDictId::STRING),
            Self::BOOLEAN => Some(DatatypeDictId::BOOLEAN),
            Self::INTEGER => Some(DatatypeDictId::INTEGER),
            Self::LONG => Some(DatatypeDictId::LONG),
            Self::DECIMAL => Some(DatatypeDictId::DECIMAL),
            Self::DOUBLE => Some(DatatypeDictId::DOUBLE),
            Self::FLOAT => Some(DatatypeDictId::FLOAT),
            Self::DATE_TIME => Some(DatatypeDictId::DATE_TIME),
            Self::DATE => Some(DatatypeDictId::DATE),
            Self::TIME => Some(DatatypeDictId::TIME),
            Self::LANG_STRING => Some(DatatypeDictId::LANG_STRING),
            Self::RDF_JSON => Some(DatatypeDictId::JSON),
            _ => None,
        }
    }
}

impl fmt::Display for ValueTypeTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self.0 {
            0 => "xsd:string",
            1 => "xsd:boolean",
            2 => "xsd:integer",
            3 => "xsd:long",
            4 => "xsd:int",
            5 => "xsd:short",
            6 => "xsd:byte",
            7 => "xsd:double",
            8 => "xsd:float",
            9 => "xsd:decimal",
            10 => "xsd:dateTime",
            11 => "xsd:date",
            12 => "xsd:time",
            13 => "xsd:anyURI",
            14 => "rdf:langString",
            15 => "xsd:gYear",
            16 => "jsonld:id",
            17 => "xsd:unsignedLong",
            18 => "xsd:unsignedInt",
            19 => "xsd:unsignedShort",
            20 => "xsd:unsignedByte",
            21 => "xsd:nonNegativeInteger",
            22 => "xsd:positiveInteger",
            23 => "xsd:nonPositiveInteger",
            24 => "xsd:negativeInteger",
            25 => "xsd:normalizedString",
            26 => "xsd:token",
            27 => "xsd:language",
            28 => "xsd:duration",
            29 => "xsd:dayTimeDuration",
            30 => "xsd:yearMonthDuration",
            31 => "xsd:base64Binary",
            32 => "xsd:hexBinary",
            33 => "rdf:JSON",
            34 => "xsd:gMonth",
            35 => "xsd:gDay",
            36 => "xsd:gYearMonth",
            37 => "xsd:gMonthDay",
            38 => "f:embeddingVector",
            39 => "f:fullText",
            255 => "UNKNOWN",
            n => return write!(f, "ValueTypeTag({n})"),
        };
        write!(f, "{name}")
    }
}

// ============================================================================
// ObjPair
// ============================================================================

/// A `(kind, key)` pair representing a typed value in index-space.
///
/// This formalizes the common `(ObjKind, ObjKey)` tuple used throughout
/// comparators, range bounds, and pattern matching. It is **not** a column
/// type — `ObjKind` and `ObjKey` remain separate columns in leaflet storage.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct ObjPair {
    pub kind: ObjKind,
    pub key: ObjKey,
}

impl ObjPair {
    #[inline]
    pub fn new(kind: ObjKind, key: ObjKey) -> Self {
        Self { kind, key }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- ObjKind tests ----

    #[test]
    fn test_kind_schedule_values() {
        assert_eq!(ObjKind::MIN.as_u8(), 0x00);
        assert_eq!(ObjKind::NULL.as_u8(), 0x01);
        assert_eq!(ObjKind::BOOL.as_u8(), 0x02);
        assert_eq!(ObjKind::NUM_INT.as_u8(), 0x03);
        assert_eq!(ObjKind::NUM_F64.as_u8(), 0x04);
        assert_eq!(ObjKind::REF_ID.as_u8(), 0x05);
        assert_eq!(ObjKind::LEX_ID.as_u8(), 0x06);
        assert_eq!(ObjKind::DATE.as_u8(), 0x07);
        assert_eq!(ObjKind::TIME.as_u8(), 0x08);
        assert_eq!(ObjKind::DATE_TIME.as_u8(), 0x09);
        assert_eq!(ObjKind::VECTOR_ID.as_u8(), 0x0A);
        assert_eq!(ObjKind::JSON_ID.as_u8(), 0x0B);
        assert_eq!(ObjKind::NUM_BIG.as_u8(), 0x0C);
        assert_eq!(ObjKind::G_YEAR.as_u8(), 0x0D);
        assert_eq!(ObjKind::G_YEAR_MONTH.as_u8(), 0x0E);
        assert_eq!(ObjKind::G_MONTH.as_u8(), 0x0F);
        assert_eq!(ObjKind::G_DAY.as_u8(), 0x10);
        assert_eq!(ObjKind::G_MONTH_DAY.as_u8(), 0x11);
        assert_eq!(ObjKind::YEAR_MONTH_DUR.as_u8(), 0x12);
        assert_eq!(ObjKind::DAY_TIME_DUR.as_u8(), 0x13);
        assert_eq!(ObjKind::GEO_POINT.as_u8(), 0x14);
        assert_eq!(ObjKind::MAX.as_u8(), 0xFF);
    }

    #[test]
    fn test_kind_ordering() {
        assert!(ObjKind::MIN < ObjKind::NULL);
        assert!(ObjKind::NULL < ObjKind::BOOL);
        assert!(ObjKind::BOOL < ObjKind::NUM_INT);
        assert!(ObjKind::NUM_INT < ObjKind::NUM_F64);
        assert!(ObjKind::NUM_F64 < ObjKind::REF_ID);
        assert!(ObjKind::REF_ID < ObjKind::LEX_ID);
        assert!(ObjKind::LEX_ID < ObjKind::DATE);
        assert!(ObjKind::DATE < ObjKind::TIME);
        assert!(ObjKind::TIME < ObjKind::DATE_TIME);
        assert!(ObjKind::DATE_TIME < ObjKind::VECTOR_ID);
        assert!(ObjKind::VECTOR_ID < ObjKind::JSON_ID);
        assert!(ObjKind::JSON_ID < ObjKind::NUM_BIG);
        assert!(ObjKind::NUM_BIG < ObjKind::G_YEAR);
        assert!(ObjKind::G_YEAR < ObjKind::G_YEAR_MONTH);
        assert!(ObjKind::G_YEAR_MONTH < ObjKind::G_MONTH);
        assert!(ObjKind::G_MONTH < ObjKind::G_DAY);
        assert!(ObjKind::G_DAY < ObjKind::G_MONTH_DAY);
        assert!(ObjKind::G_MONTH_DAY < ObjKind::YEAR_MONTH_DUR);
        assert!(ObjKind::YEAR_MONTH_DUR < ObjKind::DAY_TIME_DUR);
        assert!(ObjKind::DAY_TIME_DUR < ObjKind::GEO_POINT);
        assert!(ObjKind::GEO_POINT < ObjKind::MAX);
    }

    #[test]
    fn test_kind_sentinels() {
        assert!(ObjKind::MIN.is_sentinel());
        assert!(ObjKind::MAX.is_sentinel());
        assert!(!ObjKind::NULL.is_sentinel());
        assert!(!ObjKind::NUM_INT.is_sentinel());
    }

    // ---- NumInt encoding/decoding ----

    #[test]
    fn test_num_int_round_trip() {
        for &v in &[
            0i64,
            1,
            -1,
            42,
            -42,
            1000,
            -1000,
            i32::MAX as i64,
            i32::MIN as i64,
        ] {
            let key = ObjKey::encode_i64(v);
            assert_eq!(key.decode_i64(), v, "round-trip failed for {v}");
        }
    }

    #[test]
    fn test_num_int_extremes() {
        let key_min = ObjKey::encode_i64(i64::MIN);
        let key_max = ObjKey::encode_i64(i64::MAX);
        assert_eq!(key_min.decode_i64(), i64::MIN);
        assert_eq!(key_max.decode_i64(), i64::MAX);
        assert_eq!(key_min.as_u64(), 0); // i64::MIN maps to u64 0
        assert_eq!(key_max.as_u64(), u64::MAX); // i64::MAX maps to u64::MAX
    }

    #[test]
    fn test_num_int_ordering() {
        let neg2 = ObjKey::encode_i64(-2);
        let neg1 = ObjKey::encode_i64(-1);
        let zero = ObjKey::encode_i64(0);
        let pos1 = ObjKey::encode_i64(1);
        let pos2 = ObjKey::encode_i64(2);

        assert!(neg2 < neg1);
        assert!(neg1 < zero);
        assert!(zero < pos1);
        assert!(pos1 < pos2);

        // Large range
        let big_neg = ObjKey::encode_i64(-1_000_000);
        let big_pos = ObjKey::encode_i64(1_000_000);
        assert!(big_neg < zero);
        assert!(zero < big_pos);
    }

    #[test]
    fn test_num_int_sign_boundary() {
        // Critical: -1 must sort before 0
        let neg1 = ObjKey::encode_i64(-1);
        let zero = ObjKey::encode_i64(0);
        assert!(neg1 < zero);

        // i64::MIN must sort before i64::MAX
        let min = ObjKey::encode_i64(i64::MIN);
        let max = ObjKey::encode_i64(i64::MAX);
        assert!(min < max);
    }

    // ---- NumF64 encoding/decoding ----

    #[test]
    fn test_num_f64_round_trip() {
        for &v in &[0.0f64, 1.0, -1.0, 0.5, -0.5, 3.13, -2.77, 1e10, -1e10] {
            let key = ObjKey::encode_f64(v).unwrap();
            let decoded = key.decode_f64();
            assert_eq!(decoded.to_bits(), v.to_bits(), "round-trip failed for {v}");
        }
    }

    #[test]
    fn test_num_f64_extremes() {
        let key_min = ObjKey::encode_f64(-f64::MAX).unwrap();
        let key_max = ObjKey::encode_f64(f64::MAX).unwrap();
        assert_eq!(key_min.decode_f64(), -f64::MAX);
        assert_eq!(key_max.decode_f64(), f64::MAX);
        assert!(key_min < key_max);

        let key_min_pos = ObjKey::encode_f64(f64::MIN_POSITIVE).unwrap();
        let key_zero = ObjKey::encode_f64(0.0).unwrap();
        assert!(key_zero < key_min_pos);
    }

    #[test]
    fn test_num_f64_ordering() {
        let neg_big = ObjKey::encode_f64(-1000.0).unwrap();
        let neg_small = ObjKey::encode_f64(-0.001).unwrap();
        let zero = ObjKey::encode_f64(0.0).unwrap();
        let pos_small = ObjKey::encode_f64(0.001).unwrap();
        let pos_big = ObjKey::encode_f64(1000.0).unwrap();

        assert!(neg_big < neg_small);
        assert!(neg_small < zero);
        assert!(zero < pos_small);
        assert!(pos_small < pos_big);
    }

    #[test]
    fn test_num_f64_sign_boundary() {
        let neg = ObjKey::encode_f64(-f64::MIN_POSITIVE).unwrap();
        let zero = ObjKey::encode_f64(0.0).unwrap();
        let pos = ObjKey::encode_f64(f64::MIN_POSITIVE).unwrap();
        assert!(neg < zero);
        assert!(zero < pos);
    }

    #[test]
    fn test_num_f64_neg_zero_canonicalization() {
        let pos_zero = ObjKey::encode_f64(0.0).unwrap();
        let neg_zero = ObjKey::encode_f64(-0.0).unwrap();
        assert_eq!(pos_zero, neg_zero);
    }

    #[test]
    fn test_num_f64_reject_nan() {
        assert_eq!(ObjKey::encode_f64(f64::NAN), Err(ObjKeyError::NaN));
    }

    #[test]
    fn test_num_f64_reject_infinity() {
        assert_eq!(
            ObjKey::encode_f64(f64::INFINITY),
            Err(ObjKeyError::Infinite)
        );
        assert_eq!(
            ObjKey::encode_f64(f64::NEG_INFINITY),
            Err(ObjKeyError::Infinite)
        );
    }

    #[test]
    fn test_num_f64_probability_ordering() {
        // Simulate the probability workload that caused rank exhaustion
        let mut keys: Vec<ObjKey> = (1..=999)
            .map(|i| ObjKey::encode_f64(i as f64 / 1000.0).unwrap())
            .collect();
        let sorted = {
            let mut s = keys.clone();
            s.sort();
            s
        };
        assert_eq!(
            keys, sorted,
            "probability floats should already be in order"
        );

        // Also verify in randomized order
        keys.reverse();
        keys.sort();
        assert_eq!(keys, sorted);
    }

    // ---- Boolean encoding ----

    #[test]
    fn test_bool_encoding() {
        let f = ObjKey::encode_bool(false);
        let t = ObjKey::encode_bool(true);
        assert!(f < t);
        assert!(!f.decode_bool());
        assert!(t.decode_bool());
    }

    // ---- Dictionary ID encoding ----

    #[test]
    fn test_u32_id_round_trip() {
        for &id in &[0u32, 1, 42, u32::MAX] {
            let key = ObjKey::encode_u32_id(id);
            assert_eq!(key.decode_u32_id(), id);
        }
    }

    #[test]
    fn test_u32_id_ordering() {
        let k0 = ObjKey::encode_u32_id(0);
        let k1 = ObjKey::encode_u32_id(1);
        let kmax = ObjKey::encode_u32_id(u32::MAX);
        assert!(k0 < k1);
        assert!(k1 < kmax);
    }

    // ---- Date/Time/DateTime encoding ----

    #[test]
    fn test_date_round_trip() {
        for &days in &[0i32, 19737, -365, i32::MIN, i32::MAX] {
            let key = ObjKey::encode_date(days);
            assert_eq!(key.decode_date(), days, "date round-trip failed for {days}");
        }
    }

    #[test]
    fn test_date_ordering() {
        let before = ObjKey::encode_date(-365);
        let epoch = ObjKey::encode_date(0);
        let after = ObjKey::encode_date(19737);
        assert!(before < epoch);
        assert!(epoch < after);
    }

    #[test]
    fn test_time_round_trip() {
        let micros: i64 = 37_800_000_000; // 10:30:00
        let key = ObjKey::encode_time(micros);
        assert_eq!(key.decode_time(), micros);
    }

    #[test]
    fn test_time_ordering() {
        let midnight = ObjKey::encode_time(0);
        let morning = ObjKey::encode_time(37_800_000_000);
        assert!(midnight < morning);
    }

    #[test]
    fn test_datetime_round_trip() {
        for &micros in &[
            0i64,
            1_705_312_200_000_000,
            -86_400_000_000,
            i64::MIN,
            i64::MAX,
        ] {
            let key = ObjKey::encode_datetime(micros);
            assert_eq!(
                key.decode_datetime(),
                micros,
                "datetime round-trip failed for {micros}"
            );
        }
    }

    #[test]
    fn test_datetime_ordering() {
        let before_epoch = ObjKey::encode_datetime(-86_400_000_000);
        let epoch = ObjKey::encode_datetime(0);
        let after_epoch = ObjKey::encode_datetime(1_705_312_200_000_000);
        assert!(before_epoch < epoch);
        assert!(epoch < after_epoch);
    }

    // ---- GeoPoint encoding/decoding ----

    #[test]
    fn test_geo_point_round_trip() {
        let cases = [
            (48.8566, 2.3522),    // Paris
            (-33.8688, 151.2093), // Sydney
            (0.0, 0.0),           // Null Island
            (-90.0, 0.0),         // South Pole
            (90.0, 180.0),        // North Pole at dateline
            (51.5074, -0.1278),   // London (negative lng)
            (35.6762, 139.6503),  // Tokyo
            (-22.9068, -43.1729), // Rio de Janeiro
            (90.0, -180.0),       // North Pole at antimeridian
            (-90.0, 180.0),       // South Pole at dateline
        ];
        for (lat, lng) in cases {
            let key = ObjKey::encode_geo_point(lat, lng).unwrap();
            let (dec_lat, dec_lng) = key.decode_geo_point();
            // 30-bit precision gives ~0.0001 degree accuracy (~11m at equator)
            assert!(
                (lat - dec_lat).abs() < 0.0001,
                "lat round-trip failed for ({lat}, {lng}): got {dec_lat}"
            );
            assert!(
                (lng - dec_lng).abs() < 0.0001,
                "lng round-trip failed for ({lat}, {lng}): got {dec_lng}"
            );
        }
    }

    #[test]
    fn test_geo_point_ordering() {
        // Latitude-primary sort: south to north, then west to east
        let south = ObjKey::encode_geo_point(-45.0, 0.0).unwrap();
        let equator = ObjKey::encode_geo_point(0.0, 0.0).unwrap();
        let north = ObjKey::encode_geo_point(45.0, 0.0).unwrap();

        assert!(south < equator, "south should sort before equator");
        assert!(equator < north, "equator should sort before north");

        // Same latitude: west to east
        let west = ObjKey::encode_geo_point(0.0, -90.0).unwrap();
        let prime = ObjKey::encode_geo_point(0.0, 0.0).unwrap();
        let east = ObjKey::encode_geo_point(0.0, 90.0).unwrap();

        assert!(west < prime, "west should sort before prime meridian");
        assert!(prime < east, "prime meridian should sort before east");
    }

    #[test]
    fn test_geo_point_extremes() {
        // All corners of the coordinate space
        let sw = ObjKey::encode_geo_point(-90.0, -180.0).unwrap();
        let se = ObjKey::encode_geo_point(-90.0, 180.0).unwrap();
        let nw = ObjKey::encode_geo_point(90.0, -180.0).unwrap();
        let ne = ObjKey::encode_geo_point(90.0, 180.0).unwrap();

        // Latitude-primary: south before north
        assert!(sw < nw);
        assert!(se < ne);
        // At same latitude: west before east
        assert!(sw < se);
        assert!(nw < ne);
    }

    #[test]
    fn test_geo_point_neg_zero_canonicalization() {
        let pos_zero = ObjKey::encode_geo_point(0.0, 0.0).unwrap();
        let neg_zero = ObjKey::encode_geo_point(-0.0, -0.0).unwrap();
        assert_eq!(pos_zero, neg_zero, "-0.0 should be canonicalized to +0.0");
    }

    #[test]
    fn test_geo_point_reject_invalid() {
        // Out of range latitude
        assert_eq!(
            ObjKey::encode_geo_point(-90.1, 0.0),
            Err(ObjKeyError::GeoLatitudeOutOfRange)
        );
        assert_eq!(
            ObjKey::encode_geo_point(90.1, 0.0),
            Err(ObjKeyError::GeoLatitudeOutOfRange)
        );

        // Out of range longitude
        assert_eq!(
            ObjKey::encode_geo_point(0.0, -180.1),
            Err(ObjKeyError::GeoLongitudeOutOfRange)
        );
        assert_eq!(
            ObjKey::encode_geo_point(0.0, 180.1),
            Err(ObjKeyError::GeoLongitudeOutOfRange)
        );

        // Non-finite values
        assert_eq!(
            ObjKey::encode_geo_point(f64::NAN, 0.0),
            Err(ObjKeyError::GeoLatitudeOutOfRange)
        );
        assert_eq!(
            ObjKey::encode_geo_point(0.0, f64::NAN),
            Err(ObjKeyError::GeoLongitudeOutOfRange)
        );
        assert_eq!(
            ObjKey::encode_geo_point(f64::INFINITY, 0.0),
            Err(ObjKeyError::GeoLatitudeOutOfRange)
        );
        assert_eq!(
            ObjKey::encode_geo_point(0.0, f64::NEG_INFINITY),
            Err(ObjKeyError::GeoLongitudeOutOfRange)
        );
    }

    #[test]
    fn test_geo_point_precision() {
        // Verify 30-bit precision gives ~0.3mm accuracy
        // At equator, 1 degree = 111,320m, so 0.0001 degree = ~11m
        // Our encoding should preserve at least this precision

        let lat = 45.123_456_789;
        let lng = -122.987_654_321;
        let key = ObjKey::encode_geo_point(lat, lng).unwrap();
        let (dec_lat, dec_lng) = key.decode_geo_point();

        // Should preserve at least 4 decimal places
        let lat_error = (lat - dec_lat).abs();
        let lng_error = (lng - dec_lng).abs();
        assert!(lat_error < 0.00002, "lat precision error: {lat_error}");
        assert!(lng_error < 0.00004, "lng precision error: {lng_error}");
    }

    // ---- Composite (kind, key) ordering ----

    #[test]
    fn test_cross_kind_ordering() {
        // (kind, key) pairs should sort lexicographically: kind first, then key
        let null = (ObjKind::NULL, ObjKey::ZERO);
        let bool_f = (ObjKind::BOOL, ObjKey::encode_bool(false));
        let int_0 = (ObjKind::NUM_INT, ObjKey::encode_i64(0));
        let f64_0 = (ObjKind::NUM_F64, ObjKey::encode_f64(0.0).unwrap());
        let ref_0 = (ObjKind::REF_ID, ObjKey::encode_u32_id(0));
        let lex_0 = (ObjKind::LEX_ID, ObjKey::encode_u32_id(0));
        let date_0 = (ObjKind::DATE, ObjKey::encode_date(0));
        let time_0 = (ObjKind::TIME, ObjKey::encode_time(0));
        let dt_0 = (ObjKind::DATE_TIME, ObjKey::encode_datetime(0));
        let json_0 = (ObjKind::JSON_ID, ObjKey::encode_u32_id(0));

        assert!(null < bool_f);
        assert!(bool_f < int_0);
        assert!(int_0 < f64_0);
        assert!(f64_0 < ref_0);
        assert!(ref_0 < lex_0);
        assert!(lex_0 < date_0);
        assert!(date_0 < time_0);
        assert!(time_0 < dt_0);
        assert!(dt_0 < json_0);
    }

    #[test]
    fn test_sentinel_bounds() {
        let min = (ObjKind::MIN, ObjKey::ZERO);
        let max = (ObjKind::MAX, ObjKey::MAX);
        let null = (ObjKind::NULL, ObjKey::ZERO);
        let big = (ObjKind::NUM_BIG, ObjKey::encode_u32_id(u32::MAX));

        assert!(min < null);
        assert!(big < max);
    }

    // ---- ValueTypeTag tests (unchanged) ----

    #[test]
    fn test_datatype_from_xsd() {
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "string"),
            ValueTypeTag::STRING
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "boolean"),
            ValueTypeTag::BOOLEAN
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "integer"),
            ValueTypeTag::INTEGER
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "long"),
            ValueTypeTag::LONG
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "int"),
            ValueTypeTag::INT
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "short"),
            ValueTypeTag::SHORT
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "byte"),
            ValueTypeTag::BYTE
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "double"),
            ValueTypeTag::DOUBLE
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "float"),
            ValueTypeTag::FLOAT
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "decimal"),
            ValueTypeTag::DECIMAL
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "dateTime"),
            ValueTypeTag::DATE_TIME
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "date"),
            ValueTypeTag::DATE
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "time"),
            ValueTypeTag::TIME
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "anyURI"),
            ValueTypeTag::ANY_URI
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "unsignedLong"),
            ValueTypeTag::UNSIGNED_LONG
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "unsignedInt"),
            ValueTypeTag::UNSIGNED_INT
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "unsignedShort"),
            ValueTypeTag::UNSIGNED_SHORT
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "unsignedByte"),
            ValueTypeTag::UNSIGNED_BYTE
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "nonNegativeInteger"),
            ValueTypeTag::NON_NEGATIVE_INTEGER
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "positiveInteger"),
            ValueTypeTag::POSITIVE_INTEGER
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "nonPositiveInteger"),
            ValueTypeTag::NON_POSITIVE_INTEGER
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "negativeInteger"),
            ValueTypeTag::NEGATIVE_INTEGER
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "normalizedString"),
            ValueTypeTag::NORMALIZED_STRING
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "token"),
            ValueTypeTag::TOKEN
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "language"),
            ValueTypeTag::LANGUAGE
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "duration"),
            ValueTypeTag::DURATION
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "dayTimeDuration"),
            ValueTypeTag::DAY_TIME_DURATION
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "yearMonthDuration"),
            ValueTypeTag::YEAR_MONTH_DURATION
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "base64Binary"),
            ValueTypeTag::BASE64_BINARY
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "hexBinary"),
            ValueTypeTag::HEX_BINARY
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "gYear"),
            ValueTypeTag::G_YEAR
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "gMonth"),
            ValueTypeTag::G_MONTH
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "gDay"),
            ValueTypeTag::G_DAY
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "gYearMonth"),
            ValueTypeTag::G_YEAR_MONTH
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "gMonthDay"),
            ValueTypeTag::G_MONTH_DAY
        );
    }

    #[test]
    fn test_datatype_from_rdf() {
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::RDF, "langString"),
            ValueTypeTag::LANG_STRING
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::RDF, "JSON"),
            ValueTypeTag::RDF_JSON
        );
    }

    #[test]
    fn test_datatype_from_jsonld() {
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::JSON_LD, "id"),
            ValueTypeTag::JSON_LD_ID
        );
    }

    #[test]
    fn test_datatype_unknown() {
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::XSD, "foobar"),
            ValueTypeTag::UNKNOWN
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::RDF, "foobar"),
            ValueTypeTag::UNKNOWN
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(namespaces::JSON_LD, "foobar"),
            ValueTypeTag::UNKNOWN
        );
        assert_eq!(
            ValueTypeTag::from_ns_name(99, "anything"),
            ValueTypeTag::UNKNOWN
        );
    }

    #[test]
    fn test_datatype_as_u8_from_u8_round_trip() {
        for dt in [
            ValueTypeTag::STRING,
            ValueTypeTag::BOOLEAN,
            ValueTypeTag::INTEGER,
            ValueTypeTag::LONG,
            ValueTypeTag::DOUBLE,
            ValueTypeTag::DATE_TIME,
            ValueTypeTag::LANG_STRING,
            ValueTypeTag::JSON_LD_ID,
            ValueTypeTag::UNKNOWN,
        ] {
            let raw = dt.as_u8();
            assert_eq!(ValueTypeTag::from_u8(raw), dt);
        }
    }

    #[test]
    fn test_datatype_integer_type_classification() {
        for dt in [
            ValueTypeTag::INTEGER,
            ValueTypeTag::LONG,
            ValueTypeTag::INT,
            ValueTypeTag::SHORT,
            ValueTypeTag::BYTE,
            ValueTypeTag::UNSIGNED_LONG,
            ValueTypeTag::UNSIGNED_INT,
            ValueTypeTag::UNSIGNED_SHORT,
            ValueTypeTag::UNSIGNED_BYTE,
            ValueTypeTag::NON_NEGATIVE_INTEGER,
            ValueTypeTag::POSITIVE_INTEGER,
            ValueTypeTag::NON_POSITIVE_INTEGER,
            ValueTypeTag::NEGATIVE_INTEGER,
        ] {
            assert!(dt.is_integer_type(), "{dt} should be integer type");
            assert!(!dt.is_float_type(), "{dt} should not be float type");
        }

        for dt in [ValueTypeTag::DOUBLE, ValueTypeTag::FLOAT] {
            assert!(dt.is_float_type(), "{dt} should be float type");
            assert!(!dt.is_integer_type(), "{dt} should not be integer type");
        }

        for dt in [
            ValueTypeTag::STRING,
            ValueTypeTag::BOOLEAN,
            ValueTypeTag::DATE_TIME,
            ValueTypeTag::DATE,
            ValueTypeTag::TIME,
            ValueTypeTag::LANG_STRING,
        ] {
            assert!(!dt.is_integer_type(), "{dt} should not be integer type");
            assert!(!dt.is_float_type(), "{dt} should not be float type");
        }
    }
}
