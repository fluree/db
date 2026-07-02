//! SHACL constraint types and validators
//!
//! This module defines the constraint types supported by the SHACL engine
//! and provides validation logic for each constraint type.

pub mod cardinality;
pub mod datatype;
pub mod lang;
pub mod pair;
pub mod pattern;
pub mod value;

use crate::compile::NodeKind;
use crate::path::PropertyPath;
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
    /// sh:qualifiedValueShape with min/max counts: the number of values
    /// conforming to the nested shape must fall within the counts.
    QualifiedValueShape {
        /// The nested shape to validate against
        shape: Arc<NestedShape>,
        /// sh:qualifiedMinCount
        min_count: Option<usize>,
        /// sh:qualifiedMaxCount
        max_count: Option<usize>,
        /// sh:qualifiedValueShapesDisjoint — when true, a value only counts
        /// if it does NOT conform to any sibling qualified shape.
        disjoint: bool,
        /// Qualified shapes of the other property shapes of the same node
        /// shape (filled during finalize; consulted only when `disjoint`).
        sibling_shapes: Vec<Arc<NestedShape>>,
    },
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

    /// sh:node - the node must conform to the referenced node shape. On a node
    /// shape this applies to the focus node; on a property shape it applies to
    /// each value node individually.
    Node(Arc<NestedShape>),

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
    pub property_constraints: Vec<(PropertyPath, Vec<Constraint>)>,
    /// Node-level constraints
    pub node_constraints: Vec<NodeConstraint>,
    /// Value-level constraints (e.g. sh:datatype on an anonymous shape without sh:path).
    /// These constrain the focus node's own value/datatype rather than a nested property.
    pub value_constraints: Vec<Constraint>,
    /// sh:message declared on an anonymous member shape. Named references get
    /// their message from the referenced CompiledShape at validation time.
    pub message: Option<String>,
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

    /// The W3C constraint-component IRI reported as
    /// `sh:sourceConstraintComponent` for violations of this constraint.
    ///
    /// `QualifiedValueShape` maps to the min-count component when
    /// `sh:qualifiedMinCount` is declared (max-count otherwise); emit sites
    /// that know which bound actually failed pick the component directly.
    pub fn component(&self) -> &'static str {
        use fluree_vocab::shacl as sh;
        match self {
            Constraint::MinCount(_) => sh::MIN_COUNT_CONSTRAINT_COMPONENT,
            Constraint::MaxCount(_) => sh::MAX_COUNT_CONSTRAINT_COMPONENT,
            Constraint::Datatype(_) => sh::DATATYPE_CONSTRAINT_COMPONENT,
            Constraint::NodeKind(_) => sh::NODE_KIND_CONSTRAINT_COMPONENT,
            Constraint::Class(_) => sh::CLASS_CONSTRAINT_COMPONENT,
            Constraint::MinInclusive(_) => sh::MIN_INCLUSIVE_CONSTRAINT_COMPONENT,
            Constraint::MaxInclusive(_) => sh::MAX_INCLUSIVE_CONSTRAINT_COMPONENT,
            Constraint::MinExclusive(_) => sh::MIN_EXCLUSIVE_CONSTRAINT_COMPONENT,
            Constraint::MaxExclusive(_) => sh::MAX_EXCLUSIVE_CONSTRAINT_COMPONENT,
            Constraint::Pattern(..) => sh::PATTERN_CONSTRAINT_COMPONENT,
            Constraint::MinLength(_) => sh::MIN_LENGTH_CONSTRAINT_COMPONENT,
            Constraint::MaxLength(_) => sh::MAX_LENGTH_CONSTRAINT_COMPONENT,
            Constraint::HasValue(_) => sh::HAS_VALUE_CONSTRAINT_COMPONENT,
            Constraint::In(_) => sh::IN_CONSTRAINT_COMPONENT,
            Constraint::Equals(_) => sh::EQUALS_CONSTRAINT_COMPONENT,
            Constraint::Disjoint(_) => sh::DISJOINT_CONSTRAINT_COMPONENT,
            Constraint::LessThan(_) => sh::LESS_THAN_CONSTRAINT_COMPONENT,
            Constraint::LessThanOrEquals(_) => sh::LESS_THAN_OR_EQUALS_CONSTRAINT_COMPONENT,
            Constraint::UniqueLang(_) => sh::UNIQUE_LANG_CONSTRAINT_COMPONENT,
            Constraint::LanguageIn(_) => sh::LANGUAGE_IN_CONSTRAINT_COMPONENT,
            Constraint::QualifiedValueShape { min_count, .. } => {
                if min_count.is_some() {
                    sh::QUALIFIED_MIN_COUNT_CONSTRAINT_COMPONENT
                } else {
                    sh::QUALIFIED_MAX_COUNT_CONSTRAINT_COMPONENT
                }
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
            NodeConstraint::Node(shape) => format!("sh:node {}", shape.id.name),
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
    /// Index of `value` within the value set being checked, when the
    /// violation concerns exactly one value. Lets result construction
    /// recover the value's datatype / language tag from the parallel
    /// `datatypes` / `langs` arrays (term fidelity for `sh:value`).
    pub value_index: Option<usize>,
    /// Human-readable message about the violation
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn component_iris_match_w3c_names() {
        assert_eq!(
            Constraint::MinCount(1).component(),
            "http://www.w3.org/ns/shacl#MinCountConstraintComponent"
        );
        assert_eq!(
            Constraint::Pattern("a".into(), None).component(),
            "http://www.w3.org/ns/shacl#PatternConstraintComponent"
        );
        assert_eq!(
            Constraint::LessThanOrEquals(Sid::new(0, "p")).component(),
            "http://www.w3.org/ns/shacl#LessThanOrEqualsConstraintComponent"
        );
    }

    #[test]
    fn qualified_component_tracks_declared_bound() {
        let qualified = |min_count, max_count| Constraint::QualifiedValueShape {
            shape: Arc::new(NestedShape {
                id: Sid::new(0, "q"),
                property_constraints: Vec::new(),
                node_constraints: Vec::new(),
                value_constraints: Vec::new(),
                message: None,
            }),
            min_count,
            max_count,
            disjoint: false,
            sibling_shapes: Vec::new(),
        };
        assert_eq!(
            qualified(Some(1), None).component(),
            "http://www.w3.org/ns/shacl#QualifiedMinCountConstraintComponent"
        );
        assert_eq!(
            qualified(None, Some(2)).component(),
            "http://www.w3.org/ns/shacl#QualifiedMaxCountConstraintComponent"
        );
    }
}
