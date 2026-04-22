//! Compiled R2RML mapping with indexes
//!
//! Provides efficient lookup structures for finding TriplesMap definitions
//! that can produce triples matching a given pattern.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::TriplesMap;

/// Complete compiled R2RML mapping
///
/// Contains all TriplesMap definitions from a mapping document along with
/// indexes for efficient lookup during query execution.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompiledR2rmlMapping {
    /// All TriplesMap definitions, keyed by IRI
    pub triples_maps: HashMap<String, TriplesMap>,

    /// Index: table name → TriplesMap IRIs that use it
    table_to_maps: HashMap<String, Vec<String>>,

    /// Index: class IRI → TriplesMap IRIs that produce subjects of that class
    class_to_maps: HashMap<String, Vec<String>>,

    /// Index: predicate IRI → (TriplesMap IRI, PredicateObjectMap index)
    ///
    /// Allows finding which TriplesMap(s) can produce triples with a given predicate.
    predicate_to_maps: HashMap<String, Vec<(String, usize)>>,
}

impl CompiledR2rmlMapping {
    /// Create a new CompiledR2rmlMapping from a list of TriplesMap definitions
    pub fn new(triples_maps: Vec<TriplesMap>) -> Self {
        let mut mapping = Self::default();

        for tm in triples_maps {
            mapping.add_triples_map(tm);
        }

        mapping
    }

    /// Add a TriplesMap and update indexes
    pub fn add_triples_map(&mut self, tm: TriplesMap) {
        let tm_iri = tm.iri.clone();

        // Index by table name
        if let Some(table_name) = tm.table_name() {
            self.table_to_maps
                .entry(table_name.to_string())
                .or_default()
                .push(tm_iri.clone());
        }

        // Index by class
        for class in &tm.subject_map.classes {
            self.class_to_maps
                .entry(class.clone())
                .or_default()
                .push(tm_iri.clone());
        }

        // Index by predicate
        for (idx, pom) in tm.predicate_object_maps.iter().enumerate() {
            if let Some(pred_iri) = pom.predicate_map.as_constant() {
                self.predicate_to_maps
                    .entry(pred_iri.to_string())
                    .or_default()
                    .push((tm_iri.clone(), idx));
            }
        }

        // Store the TriplesMap
        self.triples_maps.insert(tm_iri, tm);
    }

    /// Get a TriplesMap by IRI
    pub fn get(&self, iri: &str) -> Option<&TriplesMap> {
        self.triples_maps.get(iri)
    }

    /// Get all TriplesMap IRIs
    pub fn triples_map_iris(&self) -> impl Iterator<Item = &str> {
        self.triples_maps.keys().map(std::string::String::as_str)
    }

    /// Get the number of TriplesMap definitions
    pub fn len(&self) -> usize {
        self.triples_maps.len()
    }

    /// Check if the mapping is empty
    pub fn is_empty(&self) -> bool {
        self.triples_maps.is_empty()
    }

    /// Find TriplesMap(s) that use a given table
    pub fn find_maps_for_table(&self, table_name: &str) -> Vec<&TriplesMap> {
        self.table_to_maps
            .get(table_name)
            .map(|iris| {
                iris.iter()
                    .filter_map(|iri| self.triples_maps.get(iri))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Find TriplesMap(s) that produce subjects of a given class
    pub fn find_maps_for_class(&self, class_iri: &str) -> Vec<&TriplesMap> {
        self.class_to_maps
            .get(class_iri)
            .map(|iris| {
                iris.iter()
                    .filter_map(|iri| self.triples_maps.get(iri))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Find TriplesMap(s) that can produce triples with a given predicate
    pub fn find_maps_for_predicate(&self, predicate_iri: &str) -> Vec<&TriplesMap> {
        self.predicate_to_maps
            .get(predicate_iri)
            .map(|entries| {
                // Deduplicate TriplesMap IRIs (a TriplesMap might have multiple POMs with same predicate)
                let mut seen = std::collections::HashSet::new();
                entries
                    .iter()
                    .filter_map(|(iri, _)| {
                        if seen.insert(iri.as_str()) {
                            self.triples_maps.get(iri)
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Find TriplesMap(s) that can produce triples matching a pattern
    ///
    /// If both class and predicate are specified, returns TriplesMap(s) that
    /// satisfy both constraints. If only one is specified, uses that constraint.
    pub fn find_maps_for_pattern(
        &self,
        subject_class: Option<&str>,
        predicate: Option<&str>,
    ) -> Vec<&TriplesMap> {
        match (subject_class, predicate) {
            (Some(class), Some(pred)) => {
                // Intersect class and predicate results
                let class_maps: std::collections::HashSet<_> = self
                    .find_maps_for_class(class)
                    .into_iter()
                    .map(|tm| &tm.iri)
                    .collect();

                self.find_maps_for_predicate(pred)
                    .into_iter()
                    .filter(|tm| class_maps.contains(&tm.iri))
                    .collect()
            }
            (Some(class), None) => self.find_maps_for_class(class),
            (None, Some(pred)) => self.find_maps_for_predicate(pred),
            (None, None) => self.triples_maps.values().collect(),
        }
    }

    /// Get all unique table names referenced by the mapping
    pub fn table_names(&self) -> Vec<&str> {
        self.table_to_maps
            .keys()
            .map(std::string::String::as_str)
            .collect()
    }

    /// Get all unique class IRIs produced by the mapping
    pub fn class_iris(&self) -> Vec<&str> {
        self.class_to_maps
            .keys()
            .map(std::string::String::as_str)
            .collect()
    }

    /// Get all unique predicate IRIs produced by the mapping
    pub fn predicate_iris(&self) -> Vec<&str> {
        self.predicate_to_maps
            .keys()
            .map(std::string::String::as_str)
            .collect()
    }

    /// Get TriplesMap(s) that have RefObjectMaps pointing to a given parent
    pub fn find_maps_referencing(&self, parent_tm_iri: &str) -> Vec<&TriplesMap> {
        self.triples_maps
            .values()
            .filter(|tm| {
                tm.predicate_object_maps.iter().any(|pom| {
                    pom.object_map
                        .as_ref()
                        .map(|rom| rom.parent_triples_map == parent_tm_iri)
                        .unwrap_or(false)
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::{ObjectMap, PredicateMap, PredicateObjectMap, SubjectMap};

    fn make_airline_mapping() -> TriplesMap {
        let mut tm = TriplesMap::new("<#AirlineMapping>", "openflights.airlines");
        tm.subject_map = SubjectMap::template("http://example.org/airline/{id}")
            .with_class("http://example.org/Airline");
        tm.predicate_object_maps = vec![
            PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://example.org/name"),
                object_map: ObjectMap::column("name"),
            },
            PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://example.org/country"),
                object_map: ObjectMap::column("country"),
            },
        ];
        tm
    }

    fn make_route_mapping() -> TriplesMap {
        use crate::mapping::RefObjectMap;

        let mut tm = TriplesMap::new("<#RouteMapping>", "openflights.routes");
        tm.subject_map = SubjectMap::template("http://example.org/route/{id}")
            .with_class("http://example.org/Route");
        tm.predicate_object_maps = vec![PredicateObjectMap {
            predicate_map: PredicateMap::constant("http://example.org/airline"),
            object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                "<#AirlineMapping>",
                "airline_id",
                "id",
            )),
        }];
        tm
    }

    #[test]
    fn test_compiled_mapping_new() {
        let mapping = CompiledR2rmlMapping::new(vec![make_airline_mapping()]);

        assert_eq!(mapping.len(), 1);
        assert!(!mapping.is_empty());
        assert!(mapping.get("<#AirlineMapping>").is_some());
    }

    #[test]
    fn test_find_maps_for_table() {
        let mapping = CompiledR2rmlMapping::new(vec![make_airline_mapping(), make_route_mapping()]);

        let airline_maps = mapping.find_maps_for_table("openflights.airlines");
        assert_eq!(airline_maps.len(), 1);
        assert_eq!(airline_maps[0].iri, "<#AirlineMapping>");

        let route_maps = mapping.find_maps_for_table("openflights.routes");
        assert_eq!(route_maps.len(), 1);
        assert_eq!(route_maps[0].iri, "<#RouteMapping>");

        let no_maps = mapping.find_maps_for_table("nonexistent.table");
        assert!(no_maps.is_empty());
    }

    #[test]
    fn test_find_maps_for_class() {
        let mapping = CompiledR2rmlMapping::new(vec![make_airline_mapping(), make_route_mapping()]);

        let airline_maps = mapping.find_maps_for_class("http://example.org/Airline");
        assert_eq!(airline_maps.len(), 1);
        assert_eq!(airline_maps[0].iri, "<#AirlineMapping>");

        let no_maps = mapping.find_maps_for_class("http://example.org/Unknown");
        assert!(no_maps.is_empty());
    }

    #[test]
    fn test_find_maps_for_predicate() {
        let mapping = CompiledR2rmlMapping::new(vec![make_airline_mapping(), make_route_mapping()]);

        let name_maps = mapping.find_maps_for_predicate("http://example.org/name");
        assert_eq!(name_maps.len(), 1);
        assert_eq!(name_maps[0].iri, "<#AirlineMapping>");

        let airline_maps = mapping.find_maps_for_predicate("http://example.org/airline");
        assert_eq!(airline_maps.len(), 1);
        assert_eq!(airline_maps[0].iri, "<#RouteMapping>");
    }

    #[test]
    fn test_find_maps_for_pattern() {
        let mapping = CompiledR2rmlMapping::new(vec![make_airline_mapping(), make_route_mapping()]);

        // Class + predicate (intersection)
        let maps = mapping.find_maps_for_pattern(
            Some("http://example.org/Airline"),
            Some("http://example.org/name"),
        );
        assert_eq!(maps.len(), 1);
        assert_eq!(maps[0].iri, "<#AirlineMapping>");

        // Class only
        let maps = mapping.find_maps_for_pattern(Some("http://example.org/Route"), None);
        assert_eq!(maps.len(), 1);

        // Predicate only
        let maps = mapping.find_maps_for_pattern(None, Some("http://example.org/country"));
        assert_eq!(maps.len(), 1);

        // No constraints
        let maps = mapping.find_maps_for_pattern(None, None);
        assert_eq!(maps.len(), 2);
    }

    #[test]
    fn test_find_maps_referencing() {
        let mapping = CompiledR2rmlMapping::new(vec![make_airline_mapping(), make_route_mapping()]);

        let refs = mapping.find_maps_referencing("<#AirlineMapping>");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].iri, "<#RouteMapping>");

        let no_refs = mapping.find_maps_referencing("<#RouteMapping>");
        assert!(no_refs.is_empty());
    }

    #[test]
    fn test_metadata_accessors() {
        let mapping = CompiledR2rmlMapping::new(vec![make_airline_mapping(), make_route_mapping()]);

        let tables = mapping.table_names();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"openflights.airlines"));
        assert!(tables.contains(&"openflights.routes"));

        let classes = mapping.class_iris();
        assert_eq!(classes.len(), 2);

        let predicates = mapping.predicate_iris();
        assert_eq!(predicates.len(), 3); // name, country, airline
    }
}
