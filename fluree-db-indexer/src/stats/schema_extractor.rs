//! Schema hierarchy extraction from flakes.
//!
//! Watches for `rdfs:subClassOf` and `rdfs:subPropertyOf` assertions to build
//! the schema hierarchy. Handles both assertions and retractions.

use std::collections::{HashMap, HashSet};

use fluree_db_core::{
    is_rdfs_subclass_of, is_rdfs_subproperty_of, Flake, FlakeValue, IndexSchema,
    SchemaPredicateInfo, SchemaPredicates, Sid,
};

/// Schema entry for tracking class/property relationships during extraction
///
/// Tracks the relationships for a single class or property:
/// - `subclass_of`: For classes, the parent classes (rdfs:subClassOf targets)
/// - `parent_props`: For properties, the parent properties (rdfs:subPropertyOf targets)
/// - `child_props`: For properties, the child properties (inverse of subPropertyOf)
#[derive(Debug, Clone, Default)]
pub struct SchemaEntry {
    pub subclass_of: HashSet<Sid>,
    pub parent_props: HashSet<Sid>,
    pub child_props: HashSet<Sid>,
}

/// Schema extractor for tracking class/property hierarchy from flakes
///
/// Watches for rdfs:subClassOf and rdfs:subPropertyOf assertions to build
/// the schema hierarchy. Handles both assertions and retractions.
///
/// # Usage
///
/// ```ignore
/// let mut extractor = SchemaExtractor::new();
/// // Or with prior schema:
/// let mut extractor = SchemaExtractor::from_prior(Some(&db.schema));
///
/// for flake in novelty.iter() {
///     extractor.on_flake(flake);
/// }
///
/// let schema = extractor.finalize(target_t);
/// ```
#[derive(Debug, Default)]
pub struct SchemaExtractor {
    /// Schema entries keyed by class/property SID
    entries: HashMap<Sid, SchemaEntry>,
    /// Most recent t for schema modifications
    schema_t: i64,
}

impl SchemaExtractor {
    /// Create a new empty schema extractor
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a schema extractor initialized with prior schema
    ///
    /// Used during refresh to incrementally update the schema.
    pub fn from_prior(prior_schema: Option<&IndexSchema>) -> Self {
        if let Some(schema) = prior_schema {
            let mut entries = HashMap::new();
            for info in &schema.pred.vals {
                let entry = SchemaEntry {
                    subclass_of: info.subclass_of.iter().cloned().collect(),
                    parent_props: info.parent_props.iter().cloned().collect(),
                    child_props: info.child_props.iter().cloned().collect(),
                };
                entries.insert(info.id.clone(), entry);
            }
            Self {
                entries,
                schema_t: schema.t,
            }
        } else {
            Self::default()
        }
    }

    /// Process a flake, extracting schema relationships
    ///
    /// Watches for:
    /// - `rdfs:subClassOf`: Adds/removes parent class relationship
    /// - `rdfs:subPropertyOf`: Adds/removes parent property and child property relationships
    pub fn on_flake(&mut self, flake: &Flake) {
        // rdfs:subClassOf - track class hierarchy
        if is_rdfs_subclass_of(&flake.p) {
            if let FlakeValue::Ref(parent_sid) = &flake.o {
                let class_entry = self.entries.entry(flake.s.clone()).or_default();

                if flake.op {
                    // Assertion: add parent class
                    class_entry.subclass_of.insert(parent_sid.clone());
                } else {
                    // Retraction: remove parent class
                    class_entry.subclass_of.remove(parent_sid);
                }

                // Update schema t
                if flake.t > self.schema_t {
                    self.schema_t = flake.t;
                }
            }
        }

        // rdfs:subPropertyOf - track property hierarchy (both directions)
        if is_rdfs_subproperty_of(&flake.p) {
            if let FlakeValue::Ref(parent_sid) = &flake.o {
                if flake.op {
                    // Assertion: add parent property to subject, add child to parent
                    let prop_entry = self.entries.entry(flake.s.clone()).or_default();
                    prop_entry.parent_props.insert(parent_sid.clone());

                    let parent_entry = self.entries.entry(parent_sid.clone()).or_default();
                    parent_entry.child_props.insert(flake.s.clone());
                } else {
                    // Retraction: remove relationships
                    if let Some(prop_entry) = self.entries.get_mut(&flake.s) {
                        prop_entry.parent_props.remove(parent_sid);
                    }
                    if let Some(parent_entry) = self.entries.get_mut(parent_sid) {
                        parent_entry.child_props.remove(&flake.s);
                    }
                }

                // Update schema t
                if flake.t > self.schema_t {
                    self.schema_t = flake.t;
                }
            }
        }
    }

    /// Finalize extraction and produce IndexSchema
    ///
    /// Returns None if no schema relationships were found.
    /// The schema's t is set to the maximum t seen during extraction,
    /// or falls back to `fallback_t` if no schema flakes were processed.
    pub fn finalize(self, fallback_t: i64) -> Option<IndexSchema> {
        // Filter out empty entries (all relationships removed)
        let vals: Vec<SchemaPredicateInfo> = self
            .entries
            .into_iter()
            .filter(|(_, entry)| {
                // Keep entries that have at least one relationship
                !entry.subclass_of.is_empty()
                    || !entry.parent_props.is_empty()
                    || !entry.child_props.is_empty()
            })
            .map(|(sid, entry)| SchemaPredicateInfo {
                id: sid,
                subclass_of: entry.subclass_of.into_iter().collect(),
                parent_props: entry.parent_props.into_iter().collect(),
                child_props: entry.child_props.into_iter().collect(),
            })
            .collect();

        if vals.is_empty() {
            None
        } else {
            Some(IndexSchema {
                t: if self.schema_t > 0 {
                    self.schema_t
                } else {
                    fallback_t
                },
                pred: SchemaPredicates {
                    keys: vec![
                        "id".to_string(),
                        "subclassOf".to_string(),
                        "parentProps".to_string(),
                        "childProps".to_string(),
                    ],
                    vals,
                },
            })
        }
    }

    /// Check if any schema relationships have been extracted
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_schema_flake(
        subject: &str,
        predicate_ns: u16,
        predicate_name: &str,
        object: &str,
        t: i64,
        op: bool,
    ) -> Flake {
        Flake::new(
            Sid::new(100, subject),
            Sid::new(predicate_ns, predicate_name),
            FlakeValue::Ref(Sid::new(100, object)),
            Sid::new(0, ""), // dt not relevant for schema
            t,
            op,
            None,
        )
    }

    #[test]
    fn test_schema_extractor_subclass_of() {
        let mut extractor = SchemaExtractor::new();

        // Person rdfs:subClassOf Thing
        extractor.on_flake(&make_schema_flake(
            "Person",
            fluree_vocab::namespaces::RDFS,
            "subClassOf",
            "Thing",
            1,
            true,
        ));

        // Student rdfs:subClassOf Person
        extractor.on_flake(&make_schema_flake(
            "Student",
            fluree_vocab::namespaces::RDFS,
            "subClassOf",
            "Person",
            2,
            true,
        ));

        let schema = extractor.finalize(2).expect("should have schema");
        assert_eq!(schema.t, 2);
        assert_eq!(schema.pred.vals.len(), 2);

        // Find Person entry
        let person = schema
            .pred
            .vals
            .iter()
            .find(|v| v.id.name.as_ref() == "Person")
            .expect("Person should exist");
        assert_eq!(person.subclass_of.len(), 1);
        assert_eq!(person.subclass_of[0].name.as_ref(), "Thing");

        // Find Student entry
        let student = schema
            .pred
            .vals
            .iter()
            .find(|v| v.id.name.as_ref() == "Student")
            .expect("Student should exist");
        assert_eq!(student.subclass_of.len(), 1);
        assert_eq!(student.subclass_of[0].name.as_ref(), "Person");
    }

    #[test]
    fn test_schema_extractor_subproperty_of() {
        let mut extractor = SchemaExtractor::new();

        // givenName rdfs:subPropertyOf name
        extractor.on_flake(&make_schema_flake(
            "givenName",
            fluree_vocab::namespaces::RDFS,
            "subPropertyOf",
            "name",
            1,
            true,
        ));

        let schema = extractor.finalize(1).expect("should have schema");
        assert_eq!(schema.pred.vals.len(), 2); // givenName and name

        // givenName should have name as parent
        let given_name = schema
            .pred
            .vals
            .iter()
            .find(|v| v.id.name.as_ref() == "givenName")
            .expect("givenName should exist");
        assert_eq!(given_name.parent_props.len(), 1);
        assert_eq!(given_name.parent_props[0].name.as_ref(), "name");

        // name should have givenName as child
        let name = schema
            .pred
            .vals
            .iter()
            .find(|v| v.id.name.as_ref() == "name")
            .expect("name should exist");
        assert_eq!(name.child_props.len(), 1);
        assert_eq!(name.child_props[0].name.as_ref(), "givenName");
    }

    #[test]
    fn test_schema_extractor_retraction() {
        let mut extractor = SchemaExtractor::new();

        // Assert Person rdfs:subClassOf Thing
        extractor.on_flake(&make_schema_flake(
            "Person",
            fluree_vocab::namespaces::RDFS,
            "subClassOf",
            "Thing",
            1,
            true,
        ));

        // Retract Person rdfs:subClassOf Thing
        extractor.on_flake(&make_schema_flake(
            "Person",
            fluree_vocab::namespaces::RDFS,
            "subClassOf",
            "Thing",
            2,
            false,
        ));

        // Schema should be empty now
        let schema = extractor.finalize(2);
        assert!(schema.is_none(), "schema should be empty after retraction");
    }

    #[test]
    fn test_schema_extractor_from_prior() {
        // Create prior schema with Person -> Thing
        let prior = IndexSchema {
            t: 1,
            pred: SchemaPredicates {
                keys: vec![
                    "id".to_string(),
                    "subclassOf".to_string(),
                    "parentProps".to_string(),
                    "childProps".to_string(),
                ],
                vals: vec![SchemaPredicateInfo {
                    id: Sid::new(100, "Person"),
                    subclass_of: vec![Sid::new(100, "Thing")],
                    parent_props: vec![],
                    child_props: vec![],
                }],
            },
        };

        let mut extractor = SchemaExtractor::from_prior(Some(&prior));

        // Add Student -> Person
        extractor.on_flake(&make_schema_flake(
            "Student",
            fluree_vocab::namespaces::RDFS,
            "subClassOf",
            "Person",
            2,
            true,
        ));

        let schema = extractor.finalize(2).expect("should have schema");
        assert_eq!(schema.t, 2);
        assert_eq!(schema.pred.vals.len(), 2); // Person and Student

        // Person should still have Thing as parent
        let person = schema
            .pred
            .vals
            .iter()
            .find(|v| v.id.name.as_ref() == "Person")
            .expect("Person should exist");
        assert_eq!(person.subclass_of.len(), 1);
        assert_eq!(person.subclass_of[0].name.as_ref(), "Thing");
    }

    #[test]
    fn test_schema_extractor_empty() {
        let extractor = SchemaExtractor::new();
        let schema = extractor.finalize(1);
        assert!(schema.is_none());
    }
}
