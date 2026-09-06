//! Tier 2 input: what a `sh:NodeShape` says about a class.
//!
//! The shapes reach this crate as plain IRIs and scalars, resolved from
//! `CompiledShape` by the caller — the same seam tier 1 uses, and for the same
//! reason: SID resolution needs a ledger snapshot, so it belongs on the other
//! side, and the builder stays testable without one.

use crate::schema::model::Direction;

/// One `sh:NodeShape` with a class target.
#[derive(Debug, Clone)]
pub struct ShapeDescription {
    /// The `sh:targetClass` (or implicit class target) this shape describes.
    pub class_iri: String,
    /// `sh:name` on the node shape, overriding the derived type name.
    pub name: Option<String>,
    /// `sh:description` — the type's documentation.
    pub description: Option<String>,
    /// `sh:closed true`: properties this shape does not declare are dropped
    /// from the type even when the data has them.
    pub closed: bool,
    pub properties: Vec<ShapedProperty>,
}

/// One `sh:property` on a node shape.
#[derive(Debug, Clone, Default)]
pub struct ShapedProperty {
    /// The predicate IRI from `sh:path`.
    pub iri: String,
    /// `sh:inversePath` makes this a reverse field.
    pub direction: Direction,
    /// `sh:name` — the field's name, overriding the one derived from the IRI.
    pub name: Option<String>,
    /// `sh:description` — the field's documentation.
    pub description: Option<String>,
    /// `sh:order` — where the field appears among its siblings.
    pub order: Option<f64>,
    /// `sh:datatype` IRI.
    pub datatype: Option<String>,
    /// `sh:class`, or the target class of a `sh:node` reference.
    pub class: Option<String>,
    /// True when `sh:nodeKind` says the values are IRIs or blank nodes, which
    /// makes the field a reference even with no `sh:class` to name.
    pub node_kind_is_iri: bool,
    pub min_count: Option<usize>,
    pub max_count: Option<usize>,
    /// `sh:in`, when every member is usable as an enum value.
    pub allowed_values: Vec<AllowedValue>,
}

/// One `sh:in` member.
#[derive(Debug, Clone, PartialEq)]
pub enum AllowedValue {
    /// An IRI; the enum value's name comes from its local part.
    Iri(String),
    /// A string literal, used as the enum value's name directly.
    String(String),
}

impl AllowedValue {
    /// The underlying value, as it must be written back into a query.
    pub fn as_str(&self) -> &str {
        match self {
            AllowedValue::Iri(s) | AllowedValue::String(s) => s,
        }
    }
}

impl ShapeDescription {
    /// The property shape for `iri` in this shape's declared direction.
    pub fn property(&self, iri: &str, direction: Direction) -> Option<&ShapedProperty> {
        self.properties
            .iter()
            .find(|p| p.iri == iri && p.direction == direction)
    }
}

impl ShapedProperty {
    /// `sh:maxCount 1` is the only thing that makes a field single-valued.
    pub fn is_single(&self) -> bool {
        self.max_count == Some(1)
    }

    /// `sh:minCount ≥ 1` makes the field non-null.
    pub fn is_required(&self) -> bool {
        self.min_count.is_some_and(|n| n >= 1)
    }
}
