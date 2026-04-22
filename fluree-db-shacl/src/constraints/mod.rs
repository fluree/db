//! SHACL constraint types and validators
//!
//! This module defines the constraint types supported by the SHACL engine
//! and provides validation logic for each constraint type.

pub mod cardinality;
pub mod datatype;
pub mod pair;
pub mod pattern;
pub mod value;

use crate::compile::NodeKind;
use fluree_db_core::{FlakeValue, Sid};
use std::collections::HashSet;
use std::sync::Arc;

/// A SHACL constraint that can be validated against values
#[derive(Debug, Clone, PartialEq)]
pub enum Constraint {
    // Cardinality constraints
    /// sh:minCount - minimum number of values
    MinCount(usize),
    /// sh:maxCount - maximum number of values
    MaxCount(usize),

    // Value type constraints
    /// sh:datatype - values must have this datatype
    Datatype(Sid),
    /// sh:nodeKind - values must be of this node kind
    NodeKind(NodeKind),
    /// sh:class - values must be instances of this class
    Class(Sid),

    // Value range constraints
    /// sh:minInclusive - value >= this
    MinInclusive(FlakeValue),
    /// sh:maxInclusive - value <= this
    MaxInclusive(FlakeValue),
    /// sh:minExclusive - value > this
    MinExclusive(FlakeValue),
    /// sh:maxExclusive - value < this
    MaxExclusive(FlakeValue),

    // String constraints
    /// sh:pattern with optional flags
    Pattern(String, Option<String>),
    /// sh:minLength - minimum string length
    MinLength(usize),
    /// sh:maxLength - maximum string length
    MaxLength(usize),

    // Value constraints
    /// sh:hasValue - must have this specific value
    HasValue(FlakeValue),
    /// sh:in - value must be in this list
    In(Vec<FlakeValue>),

    // Pair constraints (comparing two properties)
    /// sh:equals - values must equal values of another property
    Equals(Sid),
    /// sh:disjoint - values must not overlap with values of another property
    Disjoint(Sid),
    /// sh:lessThan - values must be less than values of another property
    LessThan(Sid),
    /// sh:lessThanOrEquals - values must be <= values of another property
    LessThanOrEquals(Sid),

    // Language constraints
    /// sh:uniqueLang - each language tag may only appear once
    UniqueLang(bool),
    /// sh:languageIn - language must be one of these
    LanguageIn(Vec<String>),

    // Qualified value shape constraints
    /// sh:qualifiedValueShape with min/max counts
    QualifiedValueShape {
        /// The nested shape to validate against
        shape: Arc<QualifiedShape>,
        /// sh:qualifiedMinCount
        min_count: Option<usize>,
        /// sh:qualifiedMaxCount
        max_count: Option<usize>,
    },
}

/// A qualified shape for sh:qualifiedValueShape
#[derive(Debug, Clone, PartialEq)]
pub struct QualifiedShape {
    /// The shape ID
    pub id: Sid,
    /// Constraints to apply
    pub constraints: Vec<Constraint>,
}

/// Node-level constraints (applied to the focus node, not property values)
#[derive(Debug, Clone, PartialEq)]
pub enum NodeConstraint {
    /// sh:closed - the node may only have the declared properties
    Closed {
        /// Whether the shape is closed
        is_closed: bool,
        /// Properties to ignore when checking closed shape (sh:ignoredProperties)
        ignored_properties: HashSet<Sid>,
    },

    // Logical constraints
    /// sh:not - the nested shape must NOT match
    Not(Arc<NestedShape>),
    /// sh:and - all nested shapes must match
    And(Vec<Arc<NestedShape>>),
    /// sh:or - at least one nested shape must match
    Or(Vec<Arc<NestedShape>>),
    /// sh:xone - exactly one nested shape must match
    Xone(Vec<Arc<NestedShape>>),
}

/// A nested shape for logical constraints
#[derive(Debug, Clone, PartialEq)]
pub struct NestedShape {
    /// The shape ID
    pub id: Sid,
    /// Property constraints (path → constraints on values at that path)
    pub property_constraints: Vec<(Sid, Vec<Constraint>)>,
    /// Node-level constraints
    pub node_constraints: Vec<NodeConstraint>,
    /// Value-level constraints (e.g. sh:datatype on an anonymous shape without sh:path).
    /// These constrain the focus node's own value/datatype rather than a nested property.
    pub value_constraints: Vec<Constraint>,
}

impl Constraint {
    /// Get a human-readable description of this constraint
    pub fn description(&self) -> String {
        match self {
            Constraint::MinCount(n) => format!("sh:minCount {n}"),
            Constraint::MaxCount(n) => format!("sh:maxCount {n}"),
            Constraint::Datatype(dt) => format!("sh:datatype {}", dt.name),
            Constraint::NodeKind(kind) => format!("sh:nodeKind {kind:?}"),
            Constraint::Class(class) => format!("sh:class {}", class.name),
            Constraint::MinInclusive(v) => format!("sh:minInclusive {v:?}"),
            Constraint::MaxInclusive(v) => format!("sh:maxInclusive {v:?}"),
            Constraint::MinExclusive(v) => format!("sh:minExclusive {v:?}"),
            Constraint::MaxExclusive(v) => format!("sh:maxExclusive {v:?}"),
            Constraint::Pattern(p, _) => format!("sh:pattern \"{p}\""),
            Constraint::MinLength(n) => format!("sh:minLength {n}"),
            Constraint::MaxLength(n) => format!("sh:maxLength {n}"),
            Constraint::HasValue(v) => format!("sh:hasValue {v:?}"),
            Constraint::In(vs) => format!("sh:in ({} values)", vs.len()),
            Constraint::Equals(prop) => format!("sh:equals {}", prop.name),
            Constraint::Disjoint(prop) => format!("sh:disjoint {}", prop.name),
            Constraint::LessThan(prop) => format!("sh:lessThan {}", prop.name),
            Constraint::LessThanOrEquals(prop) => format!("sh:lessThanOrEquals {}", prop.name),
            Constraint::UniqueLang(v) => format!("sh:uniqueLang {v}"),
            Constraint::LanguageIn(langs) => format!("sh:languageIn {langs:?}"),
            Constraint::QualifiedValueShape {
                min_count,
                max_count,
                ..
            } => {
                format!("sh:qualifiedValueShape (min: {min_count:?}, max: {max_count:?})")
            }
        }
    }
}

impl NodeConstraint {
    /// Get a human-readable description of this constraint
    pub fn description(&self) -> String {
        match self {
            NodeConstraint::Closed {
                is_closed,
                ignored_properties,
            } => {
                format!(
                    "sh:closed {} (ignored: {} properties)",
                    is_closed,
                    ignored_properties.len()
                )
            }
            NodeConstraint::Not(_) => "sh:not".to_string(),
            NodeConstraint::And(shapes) => format!("sh:and ({} shapes)", shapes.len()),
            NodeConstraint::Or(shapes) => format!("sh:or ({} shapes)", shapes.len()),
            NodeConstraint::Xone(shapes) => format!("sh:xone ({} shapes)", shapes.len()),
        }
    }
}

/// Result of validating a single constraint
#[derive(Debug, Clone)]
pub struct ConstraintViolation {
    /// The constraint that was violated
    pub constraint: Constraint,
    /// The value that violated the constraint (if applicable)
    pub value: Option<FlakeValue>,
    /// Human-readable message about the violation
    pub message: String,
}
