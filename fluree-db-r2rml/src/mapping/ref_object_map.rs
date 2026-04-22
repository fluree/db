//! R2RML RefObjectMap structures
//!
//! RefObjectMaps define references between TriplesMap definitions,
//! enabling joins across tables.

use serde::{Deserialize, Serialize};

/// Reference to another TriplesMap with join conditions
///
/// A RefObjectMap allows one TriplesMap to reference subjects generated
/// by another TriplesMap, creating relationships between entities from
/// different tables.
///
/// # Example R2RML
///
/// ```turtle
/// <#RouteMapping> a rr:TriplesMap ;
///     rr:predicateObjectMap [
///         rr:predicate ex:airline ;
///         rr:objectMap [
///             rr:parentTriplesMap <#AirlineMapping> ;
///             rr:joinCondition [
///                 rr:child "airline_id" ;
///                 rr:parent "id"
///             ]
///         ]
///     ] .
/// ```
///
/// This says: "For each route, generate a triple with predicate ex:airline
/// whose object is the subject IRI of the AirlineMapping where the route's
/// airline_id matches the airline's id."
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefObjectMap {
    /// IRI of the parent TriplesMap
    pub parent_triples_map: String,
    /// Join conditions (may be empty for cross-product, rare)
    pub join_conditions: Vec<JoinCondition>,
}

impl RefObjectMap {
    /// Create a new RefObjectMap with a single join condition
    pub fn new(
        parent_triples_map: impl Into<String>,
        child_column: impl Into<String>,
        parent_column: impl Into<String>,
    ) -> Self {
        Self {
            parent_triples_map: parent_triples_map.into(),
            join_conditions: vec![JoinCondition {
                child_column: child_column.into(),
                parent_column: parent_column.into(),
            }],
        }
    }

    /// Create a RefObjectMap with multiple join conditions (composite key)
    pub fn with_conditions(
        parent_triples_map: impl Into<String>,
        conditions: Vec<JoinCondition>,
    ) -> Self {
        Self {
            parent_triples_map: parent_triples_map.into(),
            join_conditions: conditions,
        }
    }

    /// Add a join condition
    pub fn add_condition(&mut self, child: impl Into<String>, parent: impl Into<String>) {
        self.join_conditions.push(JoinCondition {
            child_column: child.into(),
            parent_column: parent.into(),
        });
    }

    /// Get all child columns used in join conditions
    pub fn child_columns(&self) -> Vec<&str> {
        self.join_conditions
            .iter()
            .map(|jc| jc.child_column.as_str())
            .collect()
    }

    /// Get all parent columns used in join conditions
    pub fn parent_columns(&self) -> Vec<&str> {
        self.join_conditions
            .iter()
            .map(|jc| jc.parent_column.as_str())
            .collect()
    }

    /// Check if this RefObjectMap has any join conditions
    pub fn has_conditions(&self) -> bool {
        !self.join_conditions.is_empty()
    }
}

/// A single join condition
///
/// Specifies that the child column in the current table must equal
/// the parent column in the parent TriplesMap's table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinCondition {
    /// Column in the current (child) TriplesMap's logical table
    pub child_column: String,
    /// Column in the parent TriplesMap's logical table
    pub parent_column: String,
}

impl JoinCondition {
    /// Create a new join condition
    pub fn new(child: impl Into<String>, parent: impl Into<String>) -> Self {
        Self {
            child_column: child.into(),
            parent_column: parent.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ref_object_map_new() {
        let rom = RefObjectMap::new("<#AirlineMapping>", "airline_id", "id");
        assert_eq!(rom.parent_triples_map, "<#AirlineMapping>");
        assert_eq!(rom.join_conditions.len(), 1);
        assert_eq!(rom.join_conditions[0].child_column, "airline_id");
        assert_eq!(rom.join_conditions[0].parent_column, "id");
    }

    #[test]
    fn test_ref_object_map_composite_key() {
        let rom = RefObjectMap::with_conditions(
            "<#FlightMapping>",
            vec![
                JoinCondition::new("origin_airport", "code"),
                JoinCondition::new("destination_airport", "code"),
            ],
        );
        assert_eq!(rom.join_conditions.len(), 2);
        assert_eq!(
            rom.child_columns(),
            vec!["origin_airport", "destination_airport"]
        );
        assert_eq!(rom.parent_columns(), vec!["code", "code"]);
    }

    #[test]
    fn test_add_condition() {
        let mut rom = RefObjectMap::new("<#Parent>", "col1", "id");
        rom.add_condition("col2", "other_id");
        assert_eq!(rom.join_conditions.len(), 2);
    }
}
