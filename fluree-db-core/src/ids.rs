//! Typed ID newtypes for index-space columns.
//!
//! Each type is `#[repr(transparent)]` + `Copy`, so wrapping a raw primitive costs
//! nothing at runtime — the compiler enforces type boundaries at zero cost.

use std::fmt;

// ---------------------------------------------------------------------------
// PredicateId
// ---------------------------------------------------------------------------

/// Predicate dictionary ID (u32).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[repr(transparent)]
pub struct PredicateId(pub u32);

impl PredicateId {
    #[inline]
    pub fn as_u32(self) -> u32 {
        self.0
    }
    #[inline]
    pub fn from_u32(v: u32) -> Self {
        Self(v)
    }
}

impl fmt::Display for PredicateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PredicateId({})", self.0)
    }
}

// ---------------------------------------------------------------------------
// RuntimePredicateId
// ---------------------------------------------------------------------------

/// Ledger-scoped runtime predicate ID (u32).
///
/// Persisted predicate IDs retain their original values; novelty-only
/// predicates are appended above the persisted count for the lifetime of a
/// ledger state.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[repr(transparent)]
pub struct RuntimePredicateId(pub u32);

impl RuntimePredicateId {
    #[inline]
    pub fn as_u32(self) -> u32 {
        self.0
    }

    #[inline]
    pub fn from_u32(v: u32) -> Self {
        Self(v)
    }
}

impl fmt::Display for RuntimePredicateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RuntimePredicateId({})", self.0)
    }
}

// ---------------------------------------------------------------------------
// GraphId
// ---------------------------------------------------------------------------

/// Graph dictionary ID (u16).
///
/// 0 = default graph, 1 = txn-meta. Named-graph dict indices start at 2.
///
/// A newtype (not an alias) so graph ids cannot be cross-assigned with the
/// system's other pervasive `u16` spaces — namespace codes above all; the
/// 2026-06 audit flagged that confusion class as compile-time preventable.
/// `#[repr(transparent)]` + `Copy`: zero runtime cost. Wire formats that
/// serialize a graph id store the raw `u16` via `as_u16`/`from_u16` at the
/// codec boundary; in-memory structs carry `GraphId`.
#[derive(
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Debug,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct GraphId(pub u16);

impl GraphId {
    /// The default graph (id 0).
    pub const DEFAULT: GraphId = GraphId(0);
    /// The transaction-metadata graph (id 1).
    pub const TXN_META: GraphId = GraphId(1);

    #[inline]
    pub fn as_u16(self) -> u16 {
        self.0
    }
    #[inline]
    pub fn from_u16(v: u16) -> Self {
        Self(v)
    }
    /// For indexing per-graph tables.
    #[inline]
    pub fn as_usize(self) -> usize {
        self.0 as usize
    }
}

impl fmt::Display for GraphId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GraphId({})", self.0)
    }
}

// ---------------------------------------------------------------------------
// TxnGraphId
// ---------------------------------------------------------------------------

/// Transaction-local graph ID (u16).
///
/// Scoped to a single transaction envelope: `0` = default graph, `1` = txn-meta,
/// `2+` = named graphs registered in this transaction's `graph_delta`. It is
/// **not ledger-stable** — the same graph IRI can map to a different [`GraphId`]
/// in the ledger's graph registry. The correct translation is
/// `txn-local id → graph IRI (Txn.graph_delta) → ledger GraphId (GraphRegistry)`;
/// do it before any per-graph index/range query.
///
/// A distinct newtype (not [`GraphId`], not a bare `u16`) so the transaction-local
/// space cannot be silently cross-assigned with ledger graph ids — the confusion
/// class surfaced during the GraphId migration. `#[repr(transparent)]` + `Copy`:
/// zero runtime cost. Wire forms (the commit envelope's `graph_delta`) store the
/// raw `u16` via `as_u16`/`from_u16` at the codec boundary.
#[derive(
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Debug,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct TxnGraphId(pub u16);

impl TxnGraphId {
    /// The default graph (id 0).
    pub const DEFAULT: TxnGraphId = TxnGraphId(0);
    /// The transaction-metadata graph (id 1).
    pub const TXN_META: TxnGraphId = TxnGraphId(1);

    #[inline]
    pub const fn as_u16(self) -> u16 {
        self.0
    }
    #[inline]
    pub const fn from_u16(v: u16) -> Self {
        Self(v)
    }
    /// For indexing per-graph tables.
    #[inline]
    pub fn as_usize(self) -> usize {
        self.0 as usize
    }

    /// Adopt this transaction-local id **directly** as a ledger [`GraphId`],
    /// without IRI translation.
    ///
    /// Sound only on the novelty-routing path, where staging adopts the
    /// transaction-local numbering as the ledger numbering for the commit being
    /// staged (see `build_reverse_graph_lookup`). Anywhere the two numberings
    /// can differ (e.g. upsert against pre-existing named graphs), translate via
    /// the graph IRI + `GraphRegistry` instead — never call this.
    #[inline]
    pub fn adopt_as_ledger(self) -> GraphId {
        GraphId(self.0)
    }
}

impl fmt::Display for TxnGraphId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TxnGraphId({})", self.0)
    }
}

// ---------------------------------------------------------------------------
// TxnT
// ---------------------------------------------------------------------------

/// Transaction number (i64). A monotonic commit counter used for ordering,
/// not an entity identifier.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[repr(transparent)]
pub struct TxnT(pub i64);

impl TxnT {
    #[inline]
    pub fn as_i64(self) -> i64 {
        self.0
    }
    #[inline]
    pub fn from_i64(v: i64) -> Self {
        Self(v)
    }
    #[inline]
    pub fn min() -> Self {
        Self(i64::MIN)
    }
    #[inline]
    pub fn max() -> Self {
        Self(i64::MAX)
    }
}

impl fmt::Display for TxnT {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TxnT({})", self.0)
    }
}

// ---------------------------------------------------------------------------
// StringId
// ---------------------------------------------------------------------------

/// String dictionary ID (u32). Used when `ObjKind::LEX_ID` — the ObjKey
/// payload is a string dictionary handle.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[repr(transparent)]
pub struct StringId(pub u32);

impl StringId {
    #[inline]
    pub fn as_u32(self) -> u32 {
        self.0
    }
    #[inline]
    pub fn from_u32(v: u32) -> Self {
        Self(v)
    }
}

impl fmt::Display for StringId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StringId({})", self.0)
    }
}

// ---------------------------------------------------------------------------
// LangId
// ---------------------------------------------------------------------------

/// Language tag dictionary ID (u16). 0 = none.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[repr(transparent)]
pub struct LangId(pub u16);

impl LangId {
    #[inline]
    pub fn as_u16(self) -> u16 {
        self.0
    }
    #[inline]
    pub fn from_u16(v: u16) -> Self {
        Self(v)
    }
    /// Sentinel for "no language tag" (0).
    #[inline]
    pub fn none() -> Self {
        Self(0)
    }
    /// Returns `true` if this is the "no language" sentinel.
    #[inline]
    pub fn is_none(self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for LangId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LangId({})", self.0)
    }
}

// ---------------------------------------------------------------------------
// ListIndex
// ---------------------------------------------------------------------------

/// List position (i32). `i32::MIN` = none (not a list member).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[repr(transparent)]
pub struct ListIndex(pub i32);

impl ListIndex {
    #[inline]
    pub fn as_i32(self) -> i32 {
        self.0
    }
    #[inline]
    pub fn from_i32(v: i32) -> Self {
        Self(v)
    }
    /// Sentinel for "not a list member" (`i32::MIN`).
    #[inline]
    pub fn none() -> Self {
        Self(i32::MIN)
    }
    /// Returns `true` if this is the "no list" sentinel.
    #[inline]
    pub fn is_none(self) -> bool {
        self.0 == i32::MIN
    }
}

impl fmt::Display for ListIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ListIndex({})", self.0)
    }
}

// ---------------------------------------------------------------------------
// DatatypeDictId
// ---------------------------------------------------------------------------

/// Per-ledger datatype dictionary position (u16).
///
/// Used as a tie-breaker in index sort keys so that values with the same
/// `(ObjKind, ObjKey)` but different XSD types remain distinguishable.
///
/// **Not** the same as `ValueTypeTag` (semantic type classifier). The numbering
/// is different and incompatible:
///
/// | Datatype  | `ValueTypeTag` (u8) | `DatatypeDictId` (u16) |
/// |-----------|---------------------|------------------------|
/// | @id       | 16 (JSON_LD_ID)     | 0  (ID)                |
/// | string    | 0  (STRING)         | 1  (STRING)            |
/// | boolean   | 1  (BOOLEAN)        | 2  (BOOLEAN)           |
/// | integer   | 2  (INTEGER)        | 3  (INTEGER)           |
///
/// The first 15 IDs (0–14) are reserved for well-known datatypes. Custom
/// datatypes are assigned dynamically starting at `RESERVED_COUNT`.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[repr(transparent)]
pub struct DatatypeDictId(pub u16);

impl DatatypeDictId {
    /// `@id` — IRI reference sentinel.
    pub const ID: Self = Self(0);
    /// `xsd:string`
    pub const STRING: Self = Self(1);
    /// `xsd:boolean`
    pub const BOOLEAN: Self = Self(2);
    /// `xsd:integer`
    pub const INTEGER: Self = Self(3);
    /// `xsd:long`
    pub const LONG: Self = Self(4);
    /// `xsd:decimal`
    pub const DECIMAL: Self = Self(5);
    /// `xsd:double`
    pub const DOUBLE: Self = Self(6);
    /// `xsd:float`
    pub const FLOAT: Self = Self(7);
    /// `xsd:dateTime`
    pub const DATE_TIME: Self = Self(8);
    /// `xsd:date`
    pub const DATE: Self = Self(9);
    /// `xsd:time`
    pub const TIME: Self = Self(10);
    /// `rdf:langString`
    pub const LANG_STRING: Self = Self(11);
    /// `@json`
    pub const JSON: Self = Self(12);
    /// `@vector`
    pub const VECTOR: Self = Self(13);
    /// `@fulltext`
    pub const FULL_TEXT: Self = Self(14);
    /// Number of reserved well-known datatype dictionary IDs.
    pub const RESERVED_COUNT: u16 = 15;

    #[inline]
    pub fn as_u16(self) -> u16 {
        self.0
    }
    #[inline]
    pub fn from_u16(v: u16) -> Self {
        Self(v)
    }

    /// Convert a reserved dictionary ID to its corresponding `ValueTypeTag`.
    ///
    /// Returns `None` for custom (non-reserved) datatypes that have no
    /// corresponding semantic tag.
    pub fn to_value_type_tag(self) -> Option<crate::ValueTypeTag> {
        use crate::ValueTypeTag;
        match self {
            Self::ID => Some(ValueTypeTag::JSON_LD_ID),
            Self::STRING => Some(ValueTypeTag::STRING),
            Self::BOOLEAN => Some(ValueTypeTag::BOOLEAN),
            Self::INTEGER => Some(ValueTypeTag::INTEGER),
            Self::LONG => Some(ValueTypeTag::LONG),
            Self::DECIMAL => Some(ValueTypeTag::DECIMAL),
            Self::DOUBLE => Some(ValueTypeTag::DOUBLE),
            Self::FLOAT => Some(ValueTypeTag::FLOAT),
            Self::DATE_TIME => Some(ValueTypeTag::DATE_TIME),
            Self::DATE => Some(ValueTypeTag::DATE),
            Self::TIME => Some(ValueTypeTag::TIME),
            Self::LANG_STRING => Some(ValueTypeTag::LANG_STRING),
            Self::JSON => Some(ValueTypeTag::RDF_JSON),
            Self::VECTOR => None,    // no ValueTypeTag for @vector
            Self::FULL_TEXT => None, // no ValueTypeTag for @fulltext
            _ => None,
        }
    }
}

impl fmt::Display for DatatypeDictId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DatatypeDictId({})", self.0)
    }
}

// ---------------------------------------------------------------------------
// RuntimeDatatypeId
// ---------------------------------------------------------------------------

/// Ledger-scoped runtime datatype ID (u16).
///
/// Persisted datatype dictionary IDs retain their original values; novelty-only
/// datatypes are appended above the persisted count for the lifetime of a
/// ledger state.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[repr(transparent)]
pub struct RuntimeDatatypeId(pub u16);

impl RuntimeDatatypeId {
    #[inline]
    pub fn as_u16(self) -> u16 {
        self.0
    }

    #[inline]
    pub fn from_u16(v: u16) -> Self {
        Self(v)
    }
}

impl fmt::Display for RuntimeDatatypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RuntimeDatatypeId({})", self.0)
    }
}
