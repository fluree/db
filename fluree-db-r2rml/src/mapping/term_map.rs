//! R2RML term map structures
//!
//! Term maps define how RDF terms are generated from table data.

use serde::{Deserialize, Serialize};

use super::RefObjectMap;

/// R2RML term type
///
/// Specifies whether a term map generates IRIs, blank nodes, or literals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum TermType {
    /// Generate an IRI (default for subject and predicate maps)
    #[default]
    Iri,
    /// Generate a blank node
    BlankNode,
    /// Generate a literal (default for object maps with column or constant)
    Literal,
}

impl TermType {
    /// Parse term type from R2RML IRI
    pub fn from_iri(iri: &str) -> Option<Self> {
        match iri {
            "http://www.w3.org/ns/r2rml#IRI" => Some(TermType::Iri),
            "http://www.w3.org/ns/r2rml#BlankNode" => Some(TermType::BlankNode),
            "http://www.w3.org/ns/r2rml#Literal" => Some(TermType::Literal),
            _ => None,
        }
    }

    /// Check if this term type produces IRIs
    pub fn is_iri(&self) -> bool {
        matches!(self, TermType::Iri)
    }

    /// Check if this term type produces blank nodes
    pub fn is_blank_node(&self) -> bool {
        matches!(self, TermType::BlankNode)
    }

    /// Check if this term type produces literals
    pub fn is_literal(&self) -> bool {
        matches!(self, TermType::Literal)
    }
}

/// Predicate-object map pair
///
/// Represents a `rr:predicateObjectMap` containing a predicate map
/// and an object map that together generate predicate-object pairs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredicateObjectMap {
    /// The predicate map (usually a constant IRI)
    pub predicate_map: PredicateMap,
    /// The object map (column, constant, template, or reference)
    pub object_map: ObjectMap,
}

/// Predicate map
///
/// Defines how predicates are generated. In most R2RML mappings,
/// predicates are constant IRIs specified via `rr:predicate`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PredicateMap {
    /// `rr:predicate` - a constant predicate IRI
    Constant(String),
    /// `rr:predicateMap` with `rr:template` - templated predicate (rare)
    Template {
        /// Template string with `{column}` placeholders
        template: String,
        /// Column names referenced in the template
        columns: Vec<String>,
    },
    /// `rr:predicateMap` with `rr:column` - column-based predicate (very rare)
    Column(String),
}

impl PredicateMap {
    /// Create a constant predicate map
    pub fn constant(iri: impl Into<String>) -> Self {
        PredicateMap::Constant(iri.into())
    }

    /// Create a template predicate map
    pub fn template(template: impl Into<String>, columns: Vec<String>) -> Self {
        PredicateMap::Template {
            template: template.into(),
            columns,
        }
    }

    /// Get the constant IRI if this is a constant predicate
    pub fn as_constant(&self) -> Option<&str> {
        match self {
            PredicateMap::Constant(iri) => Some(iri),
            _ => None,
        }
    }

    /// Check if this predicate map generates a constant value
    pub fn is_constant(&self) -> bool {
        matches!(self, PredicateMap::Constant(_))
    }

    /// Get all columns referenced by this predicate map
    pub fn referenced_columns(&self) -> Vec<&str> {
        match self {
            PredicateMap::Constant(_) => vec![],
            PredicateMap::Template { columns, .. } => {
                columns.iter().map(std::string::String::as_str).collect()
            }
            PredicateMap::Column(col) => vec![col.as_str()],
        }
    }
}

/// Object map
///
/// Defines how objects are generated. Can be:
/// - A column reference (value from a table column)
/// - A constant value (fixed IRI or literal)
/// - A template (interpolated IRI or literal)
/// - A reference to another TriplesMap (for joins)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObjectMap {
    /// `rr:column` - generate object from column value
    Column {
        /// Column name
        column: String,
        /// Optional datatype IRI for typed literals
        datatype: Option<String>,
        /// Optional language tag for language-tagged strings
        language: Option<String>,
        /// Term type (default: Literal for column maps)
        term_type: TermType,
    },

    /// `rr:constant` - generate a constant object
    Constant {
        /// Constant IRI or literal value
        value: ConstantValue,
    },

    /// `rr:template` - generate object from template
    Template {
        /// Template string with `{column}` placeholders
        template: String,
        /// Column names referenced in the template
        columns: Vec<String>,
        /// Term type (default: Literal if template produces literal)
        term_type: TermType,
        /// Optional datatype for typed literals
        datatype: Option<String>,
        /// Optional language tag for language-tagged strings
        language: Option<String>,
    },

    /// `rr:parentTriplesMap` - reference to another TriplesMap (join)
    RefObjectMap(RefObjectMap),
}

/// Constant value in an object map
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConstantValue {
    /// Constant IRI
    Iri(String),
    /// Constant literal with optional datatype and language
    Literal {
        value: String,
        datatype: Option<String>,
        language: Option<String>,
    },
}

impl ObjectMap {
    /// Create a column object map
    pub fn column(column: impl Into<String>) -> Self {
        ObjectMap::Column {
            column: column.into(),
            datatype: None,
            language: None,
            term_type: TermType::Literal,
        }
    }

    /// Create a column object map with datatype
    pub fn column_typed(column: impl Into<String>, datatype: impl Into<String>) -> Self {
        ObjectMap::Column {
            column: column.into(),
            datatype: Some(datatype.into()),
            language: None,
            term_type: TermType::Literal,
        }
    }

    /// Create a column object map that generates IRIs
    pub fn column_iri(column: impl Into<String>) -> Self {
        ObjectMap::Column {
            column: column.into(),
            datatype: None,
            language: None,
            term_type: TermType::Iri,
        }
    }

    /// Create a constant IRI object map
    pub fn constant_iri(iri: impl Into<String>) -> Self {
        ObjectMap::Constant {
            value: ConstantValue::Iri(iri.into()),
        }
    }

    /// Create a constant literal object map
    pub fn constant_literal(value: impl Into<String>) -> Self {
        ObjectMap::Constant {
            value: ConstantValue::Literal {
                value: value.into(),
                datatype: None,
                language: None,
            },
        }
    }

    /// Create a template object map
    pub fn template(template: impl Into<String>, columns: Vec<String>) -> Self {
        ObjectMap::Template {
            template: template.into(),
            columns,
            term_type: TermType::Iri,
            datatype: None,
            language: None,
        }
    }

    /// Create a reference object map
    pub fn reference(ref_object_map: RefObjectMap) -> Self {
        ObjectMap::RefObjectMap(ref_object_map)
    }

    /// Check if this is a reference object map
    pub fn is_ref(&self) -> bool {
        matches!(self, ObjectMap::RefObjectMap(_))
    }

    /// Get the RefObjectMap if this is a reference
    pub fn as_ref(&self) -> Option<&RefObjectMap> {
        match self {
            ObjectMap::RefObjectMap(ref_map) => Some(ref_map),
            _ => None,
        }
    }

    /// Get all columns referenced by this object map
    pub fn referenced_columns(&self) -> Vec<&str> {
        match self {
            ObjectMap::Column { column, .. } => vec![column.as_str()],
            ObjectMap::Constant { .. } => vec![],
            ObjectMap::Template { columns, .. } => {
                columns.iter().map(std::string::String::as_str).collect()
            }
            ObjectMap::RefObjectMap(ref_map) => ref_map
                .join_conditions
                .iter()
                .map(|jc| jc.child_column.as_str())
                .collect(),
        }
    }

    /// Get the term type for this object map
    pub fn term_type(&self) -> TermType {
        match self {
            ObjectMap::Column { term_type, .. } => *term_type,
            ObjectMap::Constant { value } => match value {
                ConstantValue::Iri(_) => TermType::Iri,
                ConstantValue::Literal { .. } => TermType::Literal,
            },
            ObjectMap::Template { term_type, .. } => *term_type,
            ObjectMap::RefObjectMap(_) => TermType::Iri, // Refs always produce IRIs
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_term_type_from_iri() {
        assert_eq!(
            TermType::from_iri("http://www.w3.org/ns/r2rml#IRI"),
            Some(TermType::Iri)
        );
        assert_eq!(
            TermType::from_iri("http://www.w3.org/ns/r2rml#BlankNode"),
            Some(TermType::BlankNode)
        );
        assert_eq!(
            TermType::from_iri("http://www.w3.org/ns/r2rml#Literal"),
            Some(TermType::Literal)
        );
        assert_eq!(TermType::from_iri("invalid"), None);
    }

    #[test]
    fn test_predicate_map_constant() {
        let pm = PredicateMap::constant("http://example.org/name");
        assert!(pm.is_constant());
        assert_eq!(pm.as_constant(), Some("http://example.org/name"));
        assert!(pm.referenced_columns().is_empty());
    }

    #[test]
    fn test_object_map_column() {
        let om = ObjectMap::column("name");
        assert!(!om.is_ref());
        assert_eq!(om.referenced_columns(), vec!["name"]);
        assert_eq!(om.term_type(), TermType::Literal);
    }

    #[test]
    fn test_object_map_template() {
        let om = ObjectMap::template("http://example.org/{id}", vec!["id".to_string()]);
        assert!(!om.is_ref());
        assert_eq!(om.referenced_columns(), vec!["id"]);
        assert_eq!(om.term_type(), TermType::Iri);
    }
}
