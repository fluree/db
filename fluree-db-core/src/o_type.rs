//! `OType` — unified object type tag for the V3 index format.
//!
//! Replaces the `(ObjKind, DatatypeDictId, LangId)` triple with a single `u16`
//! that captures type identity, decode routing, and (for `rdf:langString`) language.
//!
//! ## Layout (u16)
//!
//! ```text
//! ┌──────────┬────────────────────────────────────┐
//! │ tag (2b) │          payload (14b)              │
//! └──────────┴────────────────────────────────────┘
//!  bits 15:14              bits 13:0
//! ```
//!
//! | Tag | Category | Payload semantics |
//! |-----|----------|-------------------|
//! | `00` | Embedded | `o_key` **is** the value (integer/float/temporal/point/bnode) |
//! | `01` | Customer-defined | Payload is a customer datatype id; `o_key` → string dict |
//! | `10` | Fluree-reserved dict/arena | Payload selects family; `o_key` → dict/arena |
//! | `11` | `rdf:langString` | Payload = `lang_id`; `o_key` → string dict |

use std::fmt;

/// Unified object type discriminator (u16).
///
/// Encodes datatype identity, decode routing, and (for langStrings) language
/// in a single 16-bit value. Designed for the V3 columnar index format.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct OType(u16);

// ── Tag constants ──────────────────────────────────────────────────────

/// Two-bit category tag extracted from the high bits.
pub const TAG_EMBEDDED: u8 = 0b00;
pub const TAG_CUSTOMER: u8 = 0b01;
pub const TAG_FLUREE: u8 = 0b10;
pub const TAG_LANG_STRING: u8 = 0b11;

// ── Tag `00` — Embedded values ─────────────────────────────────────────

impl OType {
    // -- Sentinels / special --

    /// Reserved sentinel (sorts before all stored types).
    pub const RESERVED: Self = Self(0x0000);
    /// Null value.
    pub const NULL: Self = Self(0x0001);
    /// `xsd:boolean` — `o_key`: 0 = false, 1 = true.
    pub const XSD_BOOLEAN: Self = Self(0x0002);

    // -- Signed integers (order-preserving XOR-sign i64 in o_key) --

    /// `xsd:integer`
    pub const XSD_INTEGER: Self = Self(0x0003);
    /// `xsd:long`
    pub const XSD_LONG: Self = Self(0x0004);
    /// `xsd:int`
    pub const XSD_INT: Self = Self(0x0005);
    /// `xsd:short`
    pub const XSD_SHORT: Self = Self(0x0006);
    /// `xsd:byte`
    pub const XSD_BYTE: Self = Self(0x0007);

    // -- Unsigned integers (u64 in o_key) --

    /// `xsd:unsignedLong`
    pub const XSD_UNSIGNED_LONG: Self = Self(0x0008);
    /// `xsd:unsignedInt`
    pub const XSD_UNSIGNED_INT: Self = Self(0x0009);
    /// `xsd:unsignedShort`
    pub const XSD_UNSIGNED_SHORT: Self = Self(0x000A);
    /// `xsd:unsignedByte`
    pub const XSD_UNSIGNED_BYTE: Self = Self(0x000B);

    // -- Constrained integers --

    /// `xsd:nonNegativeInteger`
    pub const XSD_NON_NEGATIVE_INTEGER: Self = Self(0x000C);
    /// `xsd:positiveInteger`
    pub const XSD_POSITIVE_INTEGER: Self = Self(0x000D);
    /// `xsd:nonPositiveInteger`
    pub const XSD_NON_POSITIVE_INTEGER: Self = Self(0x000E);
    /// `xsd:negativeInteger`
    pub const XSD_NEGATIVE_INTEGER: Self = Self(0x000F);

    // -- Floating point (order-preserving total-order f64 bits in o_key) --

    /// `xsd:double`
    pub const XSD_DOUBLE: Self = Self(0x0010);
    /// `xsd:float`
    pub const XSD_FLOAT: Self = Self(0x0011);
    /// `xsd:decimal` (inline f64 — overflow goes to NumBig arena via `NUM_BIG_OVERFLOW`).
    pub const XSD_DECIMAL: Self = Self(0x0012);

    // -- Temporal (various order-preserving encodings in o_key) --

    /// `xsd:date` — days since epoch, order-preserving signed.
    pub const XSD_DATE: Self = Self(0x0013);
    /// `xsd:time` — microseconds since midnight, unsigned.
    pub const XSD_TIME: Self = Self(0x0014);
    /// `xsd:dateTime` — epoch microseconds, order-preserving signed.
    pub const XSD_DATE_TIME: Self = Self(0x0015);
    /// `xsd:gYear`
    pub const XSD_G_YEAR: Self = Self(0x0016);
    /// `xsd:gYearMonth`
    pub const XSD_G_YEAR_MONTH: Self = Self(0x0017);
    /// `xsd:gMonth`
    pub const XSD_G_MONTH: Self = Self(0x0018);
    /// `xsd:gDay`
    pub const XSD_G_DAY: Self = Self(0x0019);
    /// `xsd:gMonthDay`
    pub const XSD_G_MONTH_DAY: Self = Self(0x001A);
    /// `xsd:yearMonthDuration` — total months, order-preserving signed.
    pub const XSD_YEAR_MONTH_DURATION: Self = Self(0x001B);
    /// `xsd:dayTimeDuration` — total microseconds, order-preserving signed.
    pub const XSD_DAY_TIME_DURATION: Self = Self(0x001C);
    /// `xsd:duration` — compound (months + microseconds), TBD.
    pub const XSD_DURATION: Self = Self(0x001D);

    // -- Spatial + blank node --

    /// `geo:Point` (embedded) — 60-bit packed: 30-bit lat + 30-bit lon.
    pub const GEO_POINT: Self = Self(0x001E);
    /// Blank node (`_:b{id}`) — `o_key` is the atomic bnode integer.
    pub const BLANK_NODE: Self = Self(0x001F);

    // Tag `00` payload range 0x0020–0x3FFF reserved for future embedded types.

    // ── Tag `10` — Fluree-reserved dictionary/arena-backed ─────────────

    /// `xsd:string` — `o_key` is a string dictionary ID.
    pub const XSD_STRING: Self = Self(0x8000);
    /// `xsd:anyURI` — `o_key` is a string dictionary ID.
    pub const XSD_ANY_URI: Self = Self(0x8001);
    /// `xsd:normalizedString`
    pub const XSD_NORMALIZED_STRING: Self = Self(0x8002);
    /// `xsd:token`
    pub const XSD_TOKEN: Self = Self(0x8003);
    /// `xsd:language`
    pub const XSD_LANGUAGE: Self = Self(0x8004);
    /// `xsd:base64Binary`
    pub const XSD_BASE64_BINARY: Self = Self(0x8005);
    /// `xsd:hexBinary`
    pub const XSD_HEX_BINARY: Self = Self(0x8006);
    /// IRI reference (`@id`) — `o_key` is a subject dictionary s_id.
    pub const IRI_REF: Self = Self(0x8007);
    /// `rdf:JSON` / `@json` — `o_key` is a JSON arena handle.
    pub const RDF_JSON: Self = Self(0x8008);
    /// `@vector` (embedding) — `o_key` is a vector arena handle (per-predicate shard).
    pub const VECTOR: Self = Self(0x8009);
    /// `@fulltext` — `o_key` is a string dictionary ID (+ BM25 BoW index).
    pub const FULLTEXT: Self = Self(0x800A);
    /// NumBig overflow — `o_key` is a NumBig arena handle (per-predicate).
    pub const NUM_BIG_OVERFLOW: Self = Self(0x800B);
    /// Spatial (complex geometry) — `o_key` is a spatial arena handle.
    pub const SPATIAL_COMPLEX: Self = Self(0x800C);

    // Tag `10` payload range 0x800D–0xBFFF reserved for future Fluree domains.

    // ── Tag `11` — rdf:langString ──────────────────────────────────────

    /// Base value for `rdf:langString`. Actual o_type = `LANG_STRING_BASE + lang_id`.
    pub const LANG_STRING_BASE: u16 = 0xC000;
}

// ── Core methods ───────────────────────────────────────────────────────

impl OType {
    /// Create from a raw u16 value.
    #[inline]
    pub const fn from_u16(raw: u16) -> Self {
        Self(raw)
    }

    /// Return the raw u16 value.
    #[inline]
    pub const fn as_u16(self) -> u16 {
        self.0
    }

    /// Two-bit category tag (bits 15:14).
    #[inline]
    pub const fn tag(self) -> u8 {
        (self.0 >> 14) as u8
    }

    /// 14-bit payload (bits 13:0).
    #[inline]
    pub const fn payload(self) -> u16 {
        self.0 & 0x3FFF
    }

    // ── Constructors ───────────────────────────────────────────────────

    /// Construct an `rdf:langString` OType from a language ID.
    #[inline]
    pub const fn lang_string(lang_id: u16) -> Self {
        debug_assert!(lang_id < 0x4000, "lang_id exceeds 14-bit payload");
        Self(Self::LANG_STRING_BASE | lang_id)
    }

    /// Construct a customer-defined datatype OType from a payload.
    #[inline]
    pub const fn customer_datatype(payload: u16) -> Self {
        debug_assert!(payload < 0x4000, "customer payload exceeds 14-bit range");
        Self(0x4000 | payload)
    }

    // ── Category predicates ────────────────────────────────────────────

    /// True if this is an embedded type (tag `00`): value lives in `o_key`.
    #[inline]
    pub const fn is_embedded(self) -> bool {
        self.tag() == TAG_EMBEDDED
    }

    /// True if this is a Fluree-reserved dictionary/arena-backed type (tag `10`).
    #[inline]
    pub const fn is_fluree_dict(self) -> bool {
        self.tag() == TAG_FLUREE
    }

    /// True if this is a customer-defined datatype (tag `01`).
    #[inline]
    pub const fn is_customer_datatype(self) -> bool {
        self.tag() == TAG_CUSTOMER
    }

    /// True if this is `rdf:langString` (tag `11`).
    #[inline]
    pub const fn is_lang_string(self) -> bool {
        self.tag() == TAG_LANG_STRING
    }

    /// True if `o_key` indexes into a dictionary or arena (tags `01`, `10`, or `11`).
    #[inline]
    pub const fn is_dict_backed(self) -> bool {
        // Any tag other than 00 is dict-backed.
        self.tag() != TAG_EMBEDDED
    }

    // ── Specific type predicates ───────────────────────────────────────

    /// True if this represents an IRI reference (`@id`).
    #[inline]
    pub const fn is_iri_ref(self) -> bool {
        self.0 == Self::IRI_REF.0
    }

    /// True if this represents a blank node.
    #[inline]
    pub const fn is_blank_node(self) -> bool {
        self.0 == Self::BLANK_NODE.0
    }

    /// True if this represents any kind of RDF node reference (IRI or blank node).
    #[inline]
    pub const fn is_node_ref(self) -> bool {
        self.is_iri_ref() || self.is_blank_node()
    }

    /// True if this is an integer subtype (xsd:integer and all derived types).
    #[inline]
    pub const fn is_integer(self) -> bool {
        self.0 >= Self::XSD_INTEGER.0 && self.0 <= Self::XSD_NEGATIVE_INTEGER.0
    }

    /// True if this is a floating-point subtype (double/float/decimal).
    #[inline]
    pub const fn is_float(self) -> bool {
        self.0 >= Self::XSD_DOUBLE.0 && self.0 <= Self::XSD_DECIMAL.0
    }

    /// True if this is any numeric type (integer or float, inline only).
    #[inline]
    pub const fn is_numeric(self) -> bool {
        self.is_integer() || self.is_float()
    }

    /// True if this is a temporal type (date/time/dateTime/gYear/etc./durations).
    #[inline]
    pub const fn is_temporal(self) -> bool {
        self.0 >= Self::XSD_DATE.0 && self.0 <= Self::XSD_DURATION.0
    }

    /// True if this is a string-like dictionary-backed type (xsd:string,
    /// xsd:anyURI, xsd:normalizedString, xsd:token, xsd:language,
    /// xsd:base64Binary, xsd:hexBinary, fulltext).
    #[inline]
    pub const fn is_string_dict(self) -> bool {
        self.0 >= Self::XSD_STRING.0 && self.0 <= Self::XSD_HEX_BINARY.0
            || self.0 == Self::FULLTEXT.0
    }

    // ── langString helpers ─────────────────────────────────────────────

    /// Extract `lang_id` from a langString OType. Returns `None` if not langString.
    #[inline]
    pub const fn lang_id(self) -> Option<u16> {
        if self.is_lang_string() {
            Some(self.payload())
        } else {
            None
        }
    }

    // ── Decode routing ─────────────────────────────────────────────────

    /// Determine the decode kind for this OType. Used by readers to select
    /// the correct deserialization path for `o_key`.
    #[inline]
    pub const fn decode_kind(self) -> DecodeKind {
        match self.tag() {
            TAG_EMBEDDED => self.decode_kind_embedded(),
            TAG_CUSTOMER => DecodeKind::StringDict,
            TAG_FLUREE => self.decode_kind_fluree(),
            TAG_LANG_STRING => DecodeKind::StringDict,
            _ => unreachable!(), // only 4 possible tags
        }
    }

    #[inline]
    const fn decode_kind_embedded(self) -> DecodeKind {
        match self.0 {
            0x0000 => DecodeKind::Sentinel,
            0x0001 => DecodeKind::Null,
            0x0002 => DecodeKind::Bool,
            0x0003..=0x000F => DecodeKind::I64,
            0x0010..=0x0012 => DecodeKind::F64,
            0x0013 => DecodeKind::Date,
            0x0014 => DecodeKind::Time,
            0x0015 => DecodeKind::DateTime,
            0x0016 => DecodeKind::GYear,
            0x0017 => DecodeKind::GYearMonth,
            0x0018 => DecodeKind::GMonth,
            0x0019 => DecodeKind::GDay,
            0x001A => DecodeKind::GMonthDay,
            0x001B => DecodeKind::YearMonthDuration,
            0x001C => DecodeKind::DayTimeDuration,
            0x001D => DecodeKind::Duration,
            0x001E => DecodeKind::GeoPoint,
            0x001F => DecodeKind::BlankNode,
            _ => DecodeKind::Sentinel, // future embedded types
        }
    }

    #[inline]
    const fn decode_kind_fluree(self) -> DecodeKind {
        match self.0 {
            0x8000..=0x8006 => DecodeKind::StringDict,
            0x8007 => DecodeKind::IriRef,
            0x8008 => DecodeKind::JsonArena,
            0x8009 => DecodeKind::VectorArena,
            0x800A => DecodeKind::StringDict, // fulltext (string dict + BM25)
            0x800B => DecodeKind::NumBigArena,
            0x800C => DecodeKind::SpatialArena,
            _ => DecodeKind::Sentinel, // future Fluree domains
        }
    }
}

/// Decode routing kind — tells the reader which deserialization path to use
/// for an `o_key` value.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
#[repr(u8)]
pub enum DecodeKind {
    /// Sentinel / reserved (not a real value).
    Sentinel,
    /// Null literal.
    Null,
    /// Boolean (o_key: 0 = false, 1 = true).
    Bool,
    /// Signed 64-bit integer, order-preserving XOR-sign encoding.
    I64,
    /// IEEE 754 f64, order-preserving total-order bit encoding.
    F64,
    /// `xsd:date` — days since epoch.
    Date,
    /// `xsd:time` — microseconds since midnight.
    Time,
    /// `xsd:dateTime` — epoch microseconds.
    DateTime,
    /// `xsd:gYear`
    GYear,
    /// `xsd:gYearMonth`
    GYearMonth,
    /// `xsd:gMonth`
    GMonth,
    /// `xsd:gDay`
    GDay,
    /// `xsd:gMonthDay`
    GMonthDay,
    /// `xsd:yearMonthDuration`
    YearMonthDuration,
    /// `xsd:dayTimeDuration`
    DayTimeDuration,
    /// `xsd:duration` (compound)
    Duration,
    /// `geo:Point` (embedded 60-bit lat/lng)
    GeoPoint,
    /// Blank node — o_key is the bnode integer.
    BlankNode,
    /// IRI reference — o_key is a subject dictionary s_id.
    IriRef,
    /// String dictionary lookup (string, anyURI, normalizedString, token,
    /// language, base64Binary, hexBinary, fulltext, langString, customer types).
    StringDict,
    /// JSON arena handle.
    JsonArena,
    /// Vector arena handle (per-predicate).
    VectorArena,
    /// NumBig arena handle (per-predicate).
    NumBigArena,
    /// Spatial arena handle (per-predicate).
    SpatialArena,
}

impl DecodeKind {
    /// Attempt to convert a `u8` discriminant back to a `DecodeKind`.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Sentinel),
            1 => Some(Self::Null),
            2 => Some(Self::Bool),
            3 => Some(Self::I64),
            4 => Some(Self::F64),
            5 => Some(Self::Date),
            6 => Some(Self::Time),
            7 => Some(Self::DateTime),
            8 => Some(Self::GYear),
            9 => Some(Self::GYearMonth),
            10 => Some(Self::GMonth),
            11 => Some(Self::GDay),
            12 => Some(Self::GMonthDay),
            13 => Some(Self::YearMonthDuration),
            14 => Some(Self::DayTimeDuration),
            15 => Some(Self::Duration),
            16 => Some(Self::GeoPoint),
            17 => Some(Self::BlankNode),
            18 => Some(Self::IriRef),
            19 => Some(Self::StringDict),
            20 => Some(Self::JsonArena),
            21 => Some(Self::VectorArena),
            22 => Some(Self::NumBigArena),
            23 => Some(Self::SpatialArena),
            _ => None,
        }
    }
}

// ── Display + Debug ────────────────────────────────────────────────────

impl fmt::Debug for OType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            0x0000 => write!(f, "OType::RESERVED"),
            0x0001 => write!(f, "OType::NULL"),
            0x0002 => write!(f, "OType::XSD_BOOLEAN"),
            0x0003 => write!(f, "OType::XSD_INTEGER"),
            0x0004 => write!(f, "OType::XSD_LONG"),
            0x0005 => write!(f, "OType::XSD_INT"),
            0x0006 => write!(f, "OType::XSD_SHORT"),
            0x0007 => write!(f, "OType::XSD_BYTE"),
            0x0008 => write!(f, "OType::XSD_UNSIGNED_LONG"),
            0x0009 => write!(f, "OType::XSD_UNSIGNED_INT"),
            0x000A => write!(f, "OType::XSD_UNSIGNED_SHORT"),
            0x000B => write!(f, "OType::XSD_UNSIGNED_BYTE"),
            0x000C => write!(f, "OType::XSD_NON_NEGATIVE_INTEGER"),
            0x000D => write!(f, "OType::XSD_POSITIVE_INTEGER"),
            0x000E => write!(f, "OType::XSD_NON_POSITIVE_INTEGER"),
            0x000F => write!(f, "OType::XSD_NEGATIVE_INTEGER"),
            0x0010 => write!(f, "OType::XSD_DOUBLE"),
            0x0011 => write!(f, "OType::XSD_FLOAT"),
            0x0012 => write!(f, "OType::XSD_DECIMAL"),
            0x0013 => write!(f, "OType::XSD_DATE"),
            0x0014 => write!(f, "OType::XSD_TIME"),
            0x0015 => write!(f, "OType::XSD_DATE_TIME"),
            0x0016 => write!(f, "OType::XSD_G_YEAR"),
            0x0017 => write!(f, "OType::XSD_G_YEAR_MONTH"),
            0x0018 => write!(f, "OType::XSD_G_MONTH"),
            0x0019 => write!(f, "OType::XSD_G_DAY"),
            0x001A => write!(f, "OType::XSD_G_MONTH_DAY"),
            0x001B => write!(f, "OType::XSD_YEAR_MONTH_DURATION"),
            0x001C => write!(f, "OType::XSD_DAY_TIME_DURATION"),
            0x001D => write!(f, "OType::XSD_DURATION"),
            0x001E => write!(f, "OType::GEO_POINT"),
            0x001F => write!(f, "OType::BLANK_NODE"),
            0x8000 => write!(f, "OType::XSD_STRING"),
            0x8001 => write!(f, "OType::XSD_ANY_URI"),
            0x8002 => write!(f, "OType::XSD_NORMALIZED_STRING"),
            0x8003 => write!(f, "OType::XSD_TOKEN"),
            0x8004 => write!(f, "OType::XSD_LANGUAGE"),
            0x8005 => write!(f, "OType::XSD_BASE64_BINARY"),
            0x8006 => write!(f, "OType::XSD_HEX_BINARY"),
            0x8007 => write!(f, "OType::IRI_REF"),
            0x8008 => write!(f, "OType::RDF_JSON"),
            0x8009 => write!(f, "OType::VECTOR"),
            0x800A => write!(f, "OType::FULLTEXT"),
            0x800B => write!(f, "OType::NUM_BIG_OVERFLOW"),
            0x800C => write!(f, "OType::SPATIAL_COMPLEX"),
            v if self.is_lang_string() => write!(f, "OType::LANG_STRING({})", v & 0x3FFF),
            v if self.is_customer_datatype() => {
                write!(f, "OType::CUSTOMER({})", v & 0x3FFF)
            }
            v => write!(f, "OType(0x{v:04X})"),
        }
    }
}

impl fmt::Display for OType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OType(0x{:04X})", self.0)
    }
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_extraction() {
        assert_eq!(OType::NULL.tag(), TAG_EMBEDDED);
        assert_eq!(OType::XSD_INTEGER.tag(), TAG_EMBEDDED);
        assert_eq!(OType::XSD_STRING.tag(), TAG_FLUREE);
        assert_eq!(OType::IRI_REF.tag(), TAG_FLUREE);
        assert_eq!(OType::lang_string(42).tag(), TAG_LANG_STRING);
        assert_eq!(OType::customer_datatype(7).tag(), TAG_CUSTOMER);
    }

    #[test]
    fn payload_extraction() {
        assert_eq!(OType::NULL.payload(), 1);
        assert_eq!(OType::XSD_STRING.payload(), 0);
        assert_eq!(OType::IRI_REF.payload(), 7);
        assert_eq!(OType::lang_string(42).payload(), 42);
        assert_eq!(OType::customer_datatype(100).payload(), 100);
    }

    #[test]
    fn category_predicates() {
        assert!(OType::XSD_INTEGER.is_embedded());
        assert!(!OType::XSD_INTEGER.is_dict_backed());

        assert!(OType::XSD_STRING.is_fluree_dict());
        assert!(OType::XSD_STRING.is_dict_backed());

        assert!(OType::lang_string(0).is_lang_string());
        assert!(OType::lang_string(0).is_dict_backed());

        assert!(OType::customer_datatype(5).is_customer_datatype());
        assert!(OType::customer_datatype(5).is_dict_backed());
    }

    #[test]
    fn type_predicates() {
        assert!(OType::IRI_REF.is_iri_ref());
        assert!(OType::IRI_REF.is_node_ref());
        assert!(!OType::IRI_REF.is_blank_node());

        assert!(OType::BLANK_NODE.is_blank_node());
        assert!(OType::BLANK_NODE.is_node_ref());
        assert!(!OType::BLANK_NODE.is_iri_ref());

        assert!(OType::XSD_INTEGER.is_integer());
        assert!(OType::XSD_LONG.is_integer());
        assert!(OType::XSD_UNSIGNED_BYTE.is_integer());
        assert!(OType::XSD_NEGATIVE_INTEGER.is_integer());
        assert!(!OType::XSD_DOUBLE.is_integer());

        assert!(OType::XSD_DOUBLE.is_float());
        assert!(OType::XSD_FLOAT.is_float());
        assert!(OType::XSD_DECIMAL.is_float());
        assert!(!OType::XSD_INTEGER.is_float());

        assert!(OType::XSD_INTEGER.is_numeric());
        assert!(OType::XSD_DOUBLE.is_numeric());
        assert!(!OType::XSD_DATE.is_numeric());

        assert!(OType::XSD_DATE.is_temporal());
        assert!(OType::XSD_DATE_TIME.is_temporal());
        assert!(OType::XSD_DURATION.is_temporal());
        assert!(!OType::XSD_INTEGER.is_temporal());
    }

    #[test]
    fn lang_string_roundtrip() {
        let ot = OType::lang_string(1234);
        assert!(ot.is_lang_string());
        assert_eq!(ot.lang_id(), Some(1234));
        assert_eq!(ot.as_u16(), 0xC000 + 1234);
    }

    #[test]
    fn customer_datatype_roundtrip() {
        let ot = OType::customer_datatype(999);
        assert!(ot.is_customer_datatype());
        assert_eq!(ot.payload(), 999);
        assert_eq!(ot.as_u16(), 0x4000 + 999);
    }

    #[test]
    fn sort_order_matches_proposal() {
        // Embedded types sort before dict-backed (tag 00 < 01 < 10 < 11).
        assert!(OType::XSD_INTEGER < OType::customer_datatype(0));
        assert!(OType::customer_datatype(0) < OType::XSD_STRING);
        assert!(OType::XSD_STRING < OType::lang_string(0));

        // Within embedded, the numeric values determine order.
        assert!(OType::NULL < OType::XSD_BOOLEAN);
        assert!(OType::XSD_BOOLEAN < OType::XSD_INTEGER);
        assert!(OType::XSD_INTEGER < OType::XSD_DOUBLE);
    }

    #[test]
    fn decode_routing() {
        assert_eq!(OType::NULL.decode_kind(), DecodeKind::Null);
        assert_eq!(OType::XSD_BOOLEAN.decode_kind(), DecodeKind::Bool);
        assert_eq!(OType::XSD_INTEGER.decode_kind(), DecodeKind::I64);
        assert_eq!(OType::XSD_LONG.decode_kind(), DecodeKind::I64);
        assert_eq!(OType::XSD_UNSIGNED_INT.decode_kind(), DecodeKind::I64);
        assert_eq!(OType::XSD_DOUBLE.decode_kind(), DecodeKind::F64);
        assert_eq!(OType::XSD_DECIMAL.decode_kind(), DecodeKind::F64);
        assert_eq!(OType::XSD_DATE.decode_kind(), DecodeKind::Date);
        assert_eq!(OType::XSD_DATE_TIME.decode_kind(), DecodeKind::DateTime);
        assert_eq!(OType::GEO_POINT.decode_kind(), DecodeKind::GeoPoint);
        assert_eq!(OType::BLANK_NODE.decode_kind(), DecodeKind::BlankNode);
        assert_eq!(OType::XSD_STRING.decode_kind(), DecodeKind::StringDict);
        assert_eq!(OType::IRI_REF.decode_kind(), DecodeKind::IriRef);
        assert_eq!(OType::RDF_JSON.decode_kind(), DecodeKind::JsonArena);
        assert_eq!(OType::VECTOR.decode_kind(), DecodeKind::VectorArena);
        assert_eq!(OType::FULLTEXT.decode_kind(), DecodeKind::StringDict);
        assert_eq!(
            OType::NUM_BIG_OVERFLOW.decode_kind(),
            DecodeKind::NumBigArena
        );
        assert_eq!(OType::lang_string(5).decode_kind(), DecodeKind::StringDict);
        assert_eq!(
            OType::customer_datatype(10).decode_kind(),
            DecodeKind::StringDict
        );
    }

    #[test]
    fn from_u16_roundtrip() {
        for val in [0x0000, 0x0003, 0x001F, 0x4000, 0x8007, 0xC042, 0xFFFF] {
            let ot = OType::from_u16(val);
            assert_eq!(ot.as_u16(), val);
        }
    }

    #[test]
    fn unsigned_integer_decode_kind() {
        // Unsigned integers also use the I64 decode path (stored as u64 in o_key,
        // but the decode path handles the unsigned interpretation).
        assert_eq!(OType::XSD_UNSIGNED_LONG.decode_kind(), DecodeKind::I64);
        assert_eq!(OType::XSD_UNSIGNED_INT.decode_kind(), DecodeKind::I64);
        assert_eq!(OType::XSD_UNSIGNED_SHORT.decode_kind(), DecodeKind::I64);
        assert_eq!(OType::XSD_UNSIGNED_BYTE.decode_kind(), DecodeKind::I64);
    }
}
