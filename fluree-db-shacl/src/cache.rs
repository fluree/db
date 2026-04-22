//! SHACL shape caching
//!
//! This module provides caching for compiled SHACL shapes to avoid
//! recompilation on every validation.
//!
//! When a `SchemaHierarchy` is provided, shapes targeting a class are also
//! indexed under all subclasses of that class. This enables proper RDFS
//! reasoning: a shape targeting `Animal` will also apply to instances of
//! `Dog` (if `Dog rdfs:subClassOf Animal`).

use crate::compile::CompiledShape;
use fluree_db_core::{SchemaHierarchy, Sid};
use std::collections::HashMap;
use std::sync::Arc;

/// Cache key for SHACL shapes
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShaclCacheKey {
    /// Database/ledger identifier
    pub ledger_id: String,
    /// Schema epoch (transaction time when schema was last modified)
    pub schema_epoch: u64,
}

impl ShaclCacheKey {
    /// Create a new cache key
    pub fn new(ledger_id: impl Into<String>, schema_epoch: u64) -> Self {
        Self {
            ledger_id: ledger_id.into(),
            schema_epoch,
        }
    }
}

/// Cached SHACL shapes for a database
#[derive(Debug, Clone)]
pub struct ShaclCache {
    /// Cache key for validation
    pub key: ShaclCacheKey,
    /// All compiled shapes
    pub shapes: Arc<[CompiledShape]>,
    /// Index: target class -> shape indices
    pub by_target_class: HashMap<Sid, Vec<usize>>,
    /// Index: target node -> shape indices
    pub by_target_node: HashMap<Sid, Vec<usize>>,
    /// Index: predicate used in `sh:targetSubjectsOf` -> shape indices.
    ///
    /// Used on the staged write path to discover shapes applicable to a
    /// focus node based on the *outbound* predicates it carries.
    pub by_target_subjects_of: HashMap<Sid, Vec<usize>>,
    /// Index: predicate used in `sh:targetObjectsOf` -> shape indices.
    ///
    /// Used on the staged write path to discover shapes applicable to a
    /// focus node based on the *inbound* predicates pointing at it.
    pub by_target_objects_of: HashMap<Sid, Vec<usize>>,
}

impl ShaclCache {
    /// Build a cache from compiled shapes
    ///
    /// If `hierarchy` is provided, shapes targeting a class will also be indexed
    /// under all subclasses of that class (RDFS reasoning).
    pub fn new(
        key: ShaclCacheKey,
        shapes: Vec<CompiledShape>,
        hierarchy: Option<&SchemaHierarchy>,
    ) -> Self {
        let mut by_target_class: HashMap<Sid, Vec<usize>> = HashMap::new();
        let mut by_target_node: HashMap<Sid, Vec<usize>> = HashMap::new();
        let mut by_target_subjects_of: HashMap<Sid, Vec<usize>> = HashMap::new();
        let mut by_target_objects_of: HashMap<Sid, Vec<usize>> = HashMap::new();

        for (idx, shape) in shapes.iter().enumerate() {
            for target in &shape.targets {
                match target {
                    crate::compile::TargetType::Class(class)
                    | crate::compile::TargetType::ImplicitClass(class) => {
                        // Index under the target class itself
                        by_target_class.entry(class.clone()).or_default().push(idx);

                        // Also index under all subclasses (if hierarchy available)
                        // This enables RDFS reasoning: shape targeting Animal
                        // also applies to instances of Dog (subclass of Animal)
                        if let Some(h) = hierarchy {
                            for subclass in h.subclasses_of(class) {
                                by_target_class
                                    .entry(subclass.clone())
                                    .or_default()
                                    .push(idx);
                            }
                        }
                    }
                    crate::compile::TargetType::Node(nodes) => {
                        for node in nodes {
                            by_target_node.entry(node.clone()).or_default().push(idx);
                        }
                    }
                    crate::compile::TargetType::SubjectsOf(pred) => {
                        by_target_subjects_of
                            .entry(pred.clone())
                            .or_default()
                            .push(idx);
                    }
                    crate::compile::TargetType::ObjectsOf(pred) => {
                        by_target_objects_of
                            .entry(pred.clone())
                            .or_default()
                            .push(idx);
                    }
                }
            }
        }

        Self {
            key,
            shapes: shapes.into(),
            by_target_class,
            by_target_node,
            by_target_subjects_of,
            by_target_objects_of,
        }
    }

    /// Get shapes that target a specific class
    pub fn shapes_for_class(&self, class: &Sid) -> Vec<&CompiledShape> {
        self.by_target_class
            .get(class)
            .map(|indices| indices.iter().map(|&i| &self.shapes[i]).collect())
            .unwrap_or_default()
    }

    /// Get shapes that target a specific node
    pub fn shapes_for_node(&self, node: &Sid) -> Vec<&CompiledShape> {
        self.by_target_node
            .get(node)
            .map(|indices| indices.iter().map(|&i| &self.shapes[i]).collect())
            .unwrap_or_default()
    }

    /// Get shapes targeting subjects of `predicate` (`sh:targetSubjectsOf`).
    ///
    /// Returns shapes whose `TargetType::SubjectsOf(p)` matches `predicate`.
    /// The caller is responsible for determining that the focus node actually
    /// carries `predicate` as an outbound property — the cache only indexes
    /// which shapes *could* apply.
    pub fn shapes_for_subjects_of(&self, predicate: &Sid) -> Vec<&CompiledShape> {
        self.by_target_subjects_of
            .get(predicate)
            .map(|indices| indices.iter().map(|&i| &self.shapes[i]).collect())
            .unwrap_or_default()
    }

    /// Get shapes targeting objects of `predicate` (`sh:targetObjectsOf`).
    ///
    /// Returns shapes whose `TargetType::ObjectsOf(p)` matches `predicate`.
    /// The caller is responsible for determining that the focus node actually
    /// appears as the object of `predicate` — the cache only indexes which
    /// shapes *could* apply.
    pub fn shapes_for_objects_of(&self, predicate: &Sid) -> Vec<&CompiledShape> {
        self.by_target_objects_of
            .get(predicate)
            .map(|indices| indices.iter().map(|&i| &self.shapes[i]).collect())
            .unwrap_or_default()
    }

    /// Get all shapes
    pub fn all_shapes(&self) -> &[CompiledShape] {
        &self.shapes
    }

    /// Check if cache is valid for the given key
    pub fn is_valid_for(&self, key: &ShaclCacheKey) -> bool {
        &self.key == key
    }

    /// Number of shapes in cache
    pub fn len(&self) -> usize {
        self.shapes.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.shapes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile::{Severity, TargetType};
    use fluree_db_core::SidInterner;
    use fluree_db_core::{IndexSchema, SchemaPredicateInfo, SchemaPredicates};

    fn make_test_shape(id_name: &str, target_class: Sid) -> CompiledShape {
        let interner = SidInterner::new();
        CompiledShape {
            id: interner.intern(100, id_name),
            targets: vec![TargetType::Class(target_class)],
            property_shapes: vec![],
            node_constraints: vec![],
            structural_constraints: vec![],
            severity: Severity::Violation,
            name: None,
            message: None,
            deactivated: false,
        }
    }

    /// A hierarchy entry tuple: (namespace, name, subclass_of entries)
    type HierarchyEntry<'a> = (u16, &'a str, Vec<(u16, &'a str)>);

    fn make_hierarchy(entries: Vec<HierarchyEntry<'_>>) -> (SchemaHierarchy, SidInterner) {
        let interner = SidInterner::new();
        let vals: Vec<SchemaPredicateInfo> = entries
            .into_iter()
            .map(|(ns, name, subclass_of)| {
                let id = interner.intern(ns, name);
                let subclass_of: Vec<Sid> = subclass_of
                    .into_iter()
                    .map(|(ns, name)| interner.intern(ns, name))
                    .collect();
                SchemaPredicateInfo {
                    id,
                    subclass_of,
                    parent_props: vec![],
                    child_props: vec![],
                }
            })
            .collect();

        let schema = IndexSchema {
            t: 1,
            pred: SchemaPredicates {
                keys: vec![
                    "id".to_string(),
                    "subclassOf".to_string(),
                    "parentProps".to_string(),
                    "childProps".to_string(),
                ],
                vals,
            },
        };

        (SchemaHierarchy::from_db_root_schema(&schema), interner)
    }

    #[test]
    fn test_cache_without_hierarchy() {
        let interner = SidInterner::new();
        let animal = interner.intern(100, "Animal");
        let dog = interner.intern(100, "Dog");

        let shape = make_test_shape("AnimalShape", animal.clone());
        let key = ShaclCacheKey::new("test", 1);
        let cache = ShaclCache::new(key, vec![shape], None);

        // Without hierarchy, shape only indexed under Animal
        assert_eq!(cache.shapes_for_class(&animal).len(), 1);
        assert_eq!(cache.shapes_for_class(&dog).len(), 0);
    }

    #[test]
    fn test_cache_with_hierarchy_single_level() {
        // Dog rdfs:subClassOf Animal
        let (hierarchy, interner) = make_hierarchy(vec![(100, "Dog", vec![(100, "Animal")])]);

        let animal = interner.intern(100, "Animal");
        let dog = interner.intern(100, "Dog");
        let cat = interner.intern(100, "Cat"); // Not in hierarchy

        let shape = make_test_shape("AnimalShape", animal.clone());
        let key = ShaclCacheKey::new("test", 1);
        let cache = ShaclCache::new(key, vec![shape], Some(&hierarchy));

        // Shape targeting Animal should be indexed under Animal AND Dog
        assert_eq!(cache.shapes_for_class(&animal).len(), 1);
        assert_eq!(cache.shapes_for_class(&dog).len(), 1);
        assert_eq!(cache.shapes_for_class(&cat).len(), 0);
    }

    #[test]
    fn test_cache_with_hierarchy_multi_level() {
        // Poodle rdfs:subClassOf Dog
        // Dog rdfs:subClassOf Animal
        let (hierarchy, interner) = make_hierarchy(vec![
            (100, "Poodle", vec![(100, "Dog")]),
            (100, "Dog", vec![(100, "Animal")]),
        ]);

        let animal = interner.intern(100, "Animal");
        let dog = interner.intern(100, "Dog");
        let poodle = interner.intern(100, "Poodle");

        let shape = make_test_shape("AnimalShape", animal.clone());
        let key = ShaclCacheKey::new("test", 1);
        let cache = ShaclCache::new(key, vec![shape], Some(&hierarchy));

        // Shape targeting Animal should be indexed under Animal, Dog, AND Poodle
        assert_eq!(cache.shapes_for_class(&animal).len(), 1);
        assert_eq!(cache.shapes_for_class(&dog).len(), 1);
        assert_eq!(cache.shapes_for_class(&poodle).len(), 1);
    }

    #[test]
    fn test_cache_multiple_shapes_same_hierarchy() {
        // Dog rdfs:subClassOf Animal
        let (hierarchy, interner) = make_hierarchy(vec![(100, "Dog", vec![(100, "Animal")])]);

        let animal = interner.intern(100, "Animal");
        let dog = interner.intern(100, "Dog");

        // Two shapes: one targeting Animal, one targeting Dog
        let animal_shape = make_test_shape("AnimalShape", animal.clone());
        let dog_shape = make_test_shape("DogShape", dog.clone());

        let key = ShaclCacheKey::new("test", 1);
        let cache = ShaclCache::new(key, vec![animal_shape, dog_shape], Some(&hierarchy));

        // Looking up Animal: only AnimalShape
        assert_eq!(cache.shapes_for_class(&animal).len(), 1);
        assert_eq!(&*cache.shapes_for_class(&animal)[0].id.name, "AnimalShape");

        // Looking up Dog: AnimalShape (inherited) + DogShape (direct)
        assert_eq!(cache.shapes_for_class(&dog).len(), 2);
    }

    #[test]
    fn test_cache_diamond_hierarchy() {
        // Diamond: D inherits from both B and C, which both inherit from A
        //     A
        //    / \
        //   B   C
        //    \ /
        //     D
        let (hierarchy, interner) = make_hierarchy(vec![
            (100, "D", vec![(100, "B"), (100, "C")]),
            (100, "B", vec![(100, "A")]),
            (100, "C", vec![(100, "A")]),
        ]);

        let a = interner.intern(100, "A");
        let b = interner.intern(100, "B");
        let c = interner.intern(100, "C");
        let d = interner.intern(100, "D");

        let shape = make_test_shape("AShape", a.clone());
        let key = ShaclCacheKey::new("test", 1);
        let cache = ShaclCache::new(key, vec![shape], Some(&hierarchy));

        // Shape targeting A should be indexed under A, B, C, and D
        assert_eq!(cache.shapes_for_class(&a).len(), 1);
        assert_eq!(cache.shapes_for_class(&b).len(), 1);
        assert_eq!(cache.shapes_for_class(&c).len(), 1);
        assert_eq!(cache.shapes_for_class(&d).len(), 1);
    }
}
