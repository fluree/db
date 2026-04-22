//! Index schema types.
//!
//! These types describe the class/property hierarchy information persisted
//! alongside the index. Used for query optimization (e.g. subclass inference,
//! property path expansion).

use crate::sid::Sid;

/// Schema predicate/class metadata entry.
///
/// Each entry describes a predicate/class with its relationships:
/// - id: The predicate/class SID
/// - subclass_of: Parent classes (for rdfs:subClassOf)
/// - parent_props: Parent properties (for rdfs:subPropertyOf)
/// - child_props: Child properties (inverse of subPropertyOf)
#[derive(Debug, Clone)]
pub struct SchemaPredicateInfo {
    /// Predicate/class SID.
    pub id: Sid,
    /// Parent classes (from rdfs:subClassOf assertions).
    pub subclass_of: Vec<Sid>,
    /// Parent properties (from rdfs:subPropertyOf assertions).
    pub parent_props: Vec<Sid>,
    /// Child properties (inverse of subPropertyOf).
    pub child_props: Vec<Sid>,
}

/// Schema predicates structure.
///
/// Uses a columnar format with fixed keys and values arrays:
/// - keys: `["id", "subclassOf", "parentProps", "childProps"]`
/// - vals: one entry per predicate, sorted by SID for determinism
#[derive(Debug, Clone, Default)]
pub struct SchemaPredicates {
    /// Fixed keys: `["id", "subclassOf", "parentProps", "childProps"]`.
    pub keys: Vec<String>,
    /// Values: one entry per predicate, sorted by SID for determinism.
    pub vals: Vec<SchemaPredicateInfo>,
}

/// Index schema metadata.
///
/// Tracks class/property hierarchy information for query optimization.
#[derive(Debug, Clone)]
pub struct IndexSchema {
    /// Transaction ID when schema was last updated.
    pub t: i64,
    /// Predicate/class metadata.
    pub pred: SchemaPredicates,
}

impl Default for IndexSchema {
    fn default() -> Self {
        Self {
            t: 0,
            pred: SchemaPredicates {
                keys: vec![
                    "id".to_string(),
                    "subclassOf".to_string(),
                    "parentProps".to_string(),
                    "childProps".to_string(),
                ],
                vals: Vec::new(),
            },
        }
    }
}
