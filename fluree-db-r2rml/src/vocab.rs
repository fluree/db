//! R2RML vocabulary constants
//!
//! This module defines all R2RML vocabulary IRIs as specified in the
//! W3C R2RML Recommendation: https://www.w3.org/TR/r2rml/
//!
//! # Usage
//!
//! ```
//! use fluree_db_r2rml::R2RML;
//!
//! // Check if an IRI is the TriplesMap type
//! let iri = "http://www.w3.org/ns/r2rml#TriplesMap";
//! assert_eq!(iri, R2RML::TRIPLES_MAP);
//! ```

/// R2RML vocabulary namespace and constants
pub struct R2RML;

impl R2RML {
    // ==========================================================================
    // Namespace
    // ==========================================================================

    /// R2RML namespace IRI
    pub const NS: &'static str = "http://www.w3.org/ns/r2rml#";

    // ==========================================================================
    // Classes
    // ==========================================================================

    /// rr:TriplesMap - A mapping that generates RDF triples from a logical table
    pub const TRIPLES_MAP: &'static str = "http://www.w3.org/ns/r2rml#TriplesMap";

    /// rr:LogicalTable - A logical table (base table, view, or SQL query)
    pub const LOGICAL_TABLE_CLASS: &'static str = "http://www.w3.org/ns/r2rml#LogicalTable";

    /// rr:BaseTableOrView - A base table or view
    pub const BASE_TABLE_OR_VIEW: &'static str = "http://www.w3.org/ns/r2rml#BaseTableOrView";

    /// rr:R2RMLView - A logical table defined by an SQL query
    pub const R2RML_VIEW: &'static str = "http://www.w3.org/ns/r2rml#R2RMLView";

    /// rr:SubjectMap - A term map for generating subjects
    pub const SUBJECT_MAP_CLASS: &'static str = "http://www.w3.org/ns/r2rml#SubjectMap";

    /// rr:PredicateMap - A term map for generating predicates
    pub const PREDICATE_MAP_CLASS: &'static str = "http://www.w3.org/ns/r2rml#PredicateMap";

    /// rr:ObjectMap - A term map for generating objects
    pub const OBJECT_MAP_CLASS: &'static str = "http://www.w3.org/ns/r2rml#ObjectMap";

    /// rr:PredicateObjectMap - A pair of predicate and object maps
    pub const PREDICATE_OBJECT_MAP_CLASS: &'static str =
        "http://www.w3.org/ns/r2rml#PredicateObjectMap";

    /// rr:RefObjectMap - A reference to another TriplesMap
    pub const REF_OBJECT_MAP_CLASS: &'static str = "http://www.w3.org/ns/r2rml#RefObjectMap";

    /// rr:Join - A join condition
    pub const JOIN_CLASS: &'static str = "http://www.w3.org/ns/r2rml#Join";

    // ==========================================================================
    // Properties - Logical Table
    // ==========================================================================

    /// rr:logicalTable - Links a TriplesMap to its logical table
    pub const LOGICAL_TABLE: &'static str = "http://www.w3.org/ns/r2rml#logicalTable";

    /// rr:tableName - Specifies the name of a base table or view
    pub const TABLE_NAME: &'static str = "http://www.w3.org/ns/r2rml#tableName";

    /// rr:sqlQuery - Specifies an SQL query (NOT SUPPORTED for Iceberg graph sources)
    pub const SQL_QUERY: &'static str = "http://www.w3.org/ns/r2rml#sqlQuery";

    /// rr:sqlVersion - Specifies the SQL version
    pub const SQL_VERSION: &'static str = "http://www.w3.org/ns/r2rml#sqlVersion";

    // ==========================================================================
    // Properties - Subject Map
    // ==========================================================================

    /// rr:subjectMap - Links a TriplesMap to its subject map
    pub const SUBJECT_MAP: &'static str = "http://www.w3.org/ns/r2rml#subjectMap";

    /// rr:subject - Shortcut for constant-valued subject map
    pub const SUBJECT: &'static str = "http://www.w3.org/ns/r2rml#subject";

    /// rr:class - Specifies the RDF class for generated subjects
    pub const CLASS: &'static str = "http://www.w3.org/ns/r2rml#class";

    /// rr:graphMap - Links to a graph map
    pub const GRAPH_MAP: &'static str = "http://www.w3.org/ns/r2rml#graphMap";

    /// rr:graph - Shortcut for constant-valued graph map
    pub const GRAPH: &'static str = "http://www.w3.org/ns/r2rml#graph";

    // ==========================================================================
    // Properties - Predicate-Object Map
    // ==========================================================================

    /// rr:predicateObjectMap - Links a TriplesMap to a predicate-object map
    pub const PREDICATE_OBJECT_MAP: &'static str = "http://www.w3.org/ns/r2rml#predicateObjectMap";

    /// rr:predicateMap - Links a predicate-object map to its predicate map
    pub const PREDICATE_MAP: &'static str = "http://www.w3.org/ns/r2rml#predicateMap";

    /// rr:predicate - Shortcut for constant-valued predicate map
    pub const PREDICATE: &'static str = "http://www.w3.org/ns/r2rml#predicate";

    /// rr:objectMap - Links a predicate-object map to its object map
    pub const OBJECT_MAP: &'static str = "http://www.w3.org/ns/r2rml#objectMap";

    /// rr:object - Shortcut for constant-valued object map
    pub const OBJECT: &'static str = "http://www.w3.org/ns/r2rml#object";

    // ==========================================================================
    // Properties - Term Maps (common)
    // ==========================================================================

    /// rr:template - Specifies a string template for generating terms
    pub const TEMPLATE: &'static str = "http://www.w3.org/ns/r2rml#template";

    /// rr:column - Specifies a column name for generating terms
    pub const COLUMN: &'static str = "http://www.w3.org/ns/r2rml#column";

    /// rr:constant - Specifies a constant value for generating terms
    pub const CONSTANT: &'static str = "http://www.w3.org/ns/r2rml#constant";

    /// rr:termType - Specifies the type of generated RDF term
    pub const TERM_TYPE: &'static str = "http://www.w3.org/ns/r2rml#termType";

    /// rr:datatype - Specifies the datatype for generated literals
    pub const DATATYPE: &'static str = "http://www.w3.org/ns/r2rml#datatype";

    /// rr:language - Specifies the language tag for generated literals
    pub const LANGUAGE: &'static str = "http://www.w3.org/ns/r2rml#language";

    /// rr:inverseExpression - Specifies an inverse expression (NOT SUPPORTED)
    pub const INVERSE_EXPRESSION: &'static str = "http://www.w3.org/ns/r2rml#inverseExpression";

    // ==========================================================================
    // Properties - RefObjectMap
    // ==========================================================================

    /// rr:parentTriplesMap - Links a RefObjectMap to its parent TriplesMap
    pub const PARENT_TRIPLES_MAP: &'static str = "http://www.w3.org/ns/r2rml#parentTriplesMap";

    /// rr:joinCondition - Specifies a join condition for RefObjectMap
    pub const JOIN_CONDITION: &'static str = "http://www.w3.org/ns/r2rml#joinCondition";

    /// rr:child - Specifies the child column in a join condition
    pub const CHILD: &'static str = "http://www.w3.org/ns/r2rml#child";

    /// rr:parent - Specifies the parent column in a join condition
    pub const PARENT: &'static str = "http://www.w3.org/ns/r2rml#parent";

    // ==========================================================================
    // Term Type Values
    // ==========================================================================

    /// rr:IRI - Term type for IRIs
    pub const IRI: &'static str = "http://www.w3.org/ns/r2rml#IRI";

    /// rr:BlankNode - Term type for blank nodes
    pub const BLANK_NODE: &'static str = "http://www.w3.org/ns/r2rml#BlankNode";

    /// rr:Literal - Term type for literals
    pub const LITERAL: &'static str = "http://www.w3.org/ns/r2rml#Literal";

    // ==========================================================================
    // Other namespaces used in R2RML processing
    // (Re-exported from fluree-vocab for convenience)
    // ==========================================================================

    /// RDF namespace - rdf:type
    pub const RDF_TYPE: &'static str = fluree_vocab::rdf::TYPE;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace() {
        assert!(R2RML::TRIPLES_MAP.starts_with(R2RML::NS));
        assert!(R2RML::LOGICAL_TABLE.starts_with(R2RML::NS));
        assert!(R2RML::SUBJECT_MAP.starts_with(R2RML::NS));
    }

    #[test]
    fn test_term_types() {
        assert_eq!(R2RML::IRI, "http://www.w3.org/ns/r2rml#IRI");
        assert_eq!(R2RML::BLANK_NODE, "http://www.w3.org/ns/r2rml#BlankNode");
        assert_eq!(R2RML::LITERAL, "http://www.w3.org/ns/r2rml#Literal");
    }
}
