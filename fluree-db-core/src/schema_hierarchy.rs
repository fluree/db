//! Schema hierarchy for RDFS/OWL reasoning
//!
//! Provides precomputed transitive closures of class and property hierarchies
//! for efficient query expansion during reasoning.
//!
//! # Closure Direction
//!
//! The closures compute **descendants** (not ancestors):
//! - `subclasses_of(Animal)` returns `[Dog, Cat, ...]` (things that are subClassOf Animal)
//! - `subproperties_of(hasColor)` returns `[hasFurColor, ...]` (properties that are subPropertyOf hasColor)
//!
//! This is the direction needed for query expansion: when querying `?s rdf:type Animal`,
//! we expand to include instances of Dog, Cat, etc.

use crate::index_schema::IndexSchema;
use crate::sid::Sid;
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

/// Static empty slice for missing entries
static EMPTY_SIDS: &[Sid] = &[];

/// Arc-backed schema hierarchy for cheap cloning.
///
/// Computed once per schema epoch and cached in [`Db`](crate::db::Db).
#[derive(Clone, Debug)]
pub struct SchemaHierarchy {
    inner: Arc<SchemaHierarchyInner>,
}

#[derive(Debug)]
struct SchemaHierarchyInner {
    /// Direct subclass relationships: parent -> immediate children
    direct_subclass_of: HashMap<Sid, SmallVec<[Sid; 2]>>,
    /// Direct subproperty relationships: parent -> immediate children
    direct_subproperty_of: HashMap<Sid, SmallVec<[Sid; 2]>>,
    /// Transitive closure: class C -> all descendants of C (NOT including C itself)
    subclasses_closure: HashMap<Sid, Arc<[Sid]>>,
    /// Transitive closure: property P -> all descendants of P (NOT including P itself)
    subproperties_closure: HashMap<Sid, Arc<[Sid]>>,
    /// Epoch derived from IndexSchema.t
    epoch: u64,
}

impl SchemaHierarchy {
    /// Build a SchemaHierarchy from a IndexSchema.
    ///
    /// Computes transitive closures for all class and property hierarchies.
    /// Handles cycles gracefully (no infinite loops).
    pub fn from_db_root_schema(schema: &IndexSchema) -> Self {
        // Build direct parent -> children maps by inverting the stored relationships.
        // In IndexSchema:
        //   - subclass_of contains *parent* classes (Dog.subclass_of = [Animal])
        //   - parent_props contains *parent* properties (hasFurColor.parent_props = [hasColor])
        //
        // We invert these to get parent -> children maps for closure computation.
        let mut direct_subclass_of: HashMap<Sid, SmallVec<[Sid; 2]>> = HashMap::new();
        let mut direct_subproperty_of: HashMap<Sid, SmallVec<[Sid; 2]>> = HashMap::new();

        for pred_info in &schema.pred.vals {
            let child = &pred_info.id;

            // Invert subclass_of: for each parent, add this child
            for parent in &pred_info.subclass_of {
                direct_subclass_of
                    .entry(parent.clone())
                    .or_default()
                    .push(child.clone());
            }

            // Invert parent_props: for each parent property, add this child property
            for parent in &pred_info.parent_props {
                direct_subproperty_of
                    .entry(parent.clone())
                    .or_default()
                    .push(child.clone());
            }
        }

        // Compute transitive closures
        let subclasses_closure = compute_transitive_closure(&direct_subclass_of);
        let subproperties_closure = compute_transitive_closure(&direct_subproperty_of);

        Self {
            inner: Arc::new(SchemaHierarchyInner {
                direct_subclass_of,
                direct_subproperty_of,
                subclasses_closure,
                subproperties_closure,
                epoch: schema.t as u64,
            }),
        }
    }

    /// Returns all descendants of class `c` (subclasses, transitively).
    ///
    /// Does NOT include `c` itself. Returns empty slice if `c` has no subclasses.
    ///
    /// # Example
    /// If `Dog rdfs:subClassOf Animal` and `Poodle rdfs:subClassOf Dog`,
    /// then `subclasses_of(Animal)` returns `[Dog, Poodle]`.
    pub fn subclasses_of(&self, c: &Sid) -> &[Sid] {
        self.inner
            .subclasses_closure
            .get(c)
            .map(std::convert::AsRef::as_ref)
            .unwrap_or(EMPTY_SIDS)
    }

    /// Returns all descendants of property `p` (subproperties, transitively).
    ///
    /// Does NOT include `p` itself. Returns empty slice if `p` has no subproperties.
    ///
    /// # Example
    /// If `hasFurColor rdfs:subPropertyOf hasColor`,
    /// then `subproperties_of(hasColor)` returns `[hasFurColor]`.
    pub fn subproperties_of(&self, p: &Sid) -> &[Sid] {
        self.inner
            .subproperties_closure
            .get(p)
            .map(std::convert::AsRef::as_ref)
            .unwrap_or(EMPTY_SIDS)
    }

    /// Returns direct children of class `c` (immediate subclasses only).
    pub fn direct_subclasses_of(&self, c: &Sid) -> &[Sid] {
        self.inner
            .direct_subclass_of
            .get(c)
            .map(smallvec::SmallVec::as_slice)
            .unwrap_or(EMPTY_SIDS)
    }

    /// Returns direct children of property `p` (immediate subproperties only).
    pub fn direct_subproperties_of(&self, p: &Sid) -> &[Sid] {
        self.inner
            .direct_subproperty_of
            .get(p)
            .map(smallvec::SmallVec::as_slice)
            .unwrap_or(EMPTY_SIDS)
    }

    /// Schema epoch (transaction ID when schema was last updated).
    ///
    /// Useful for cache invalidation and diagnostics.
    pub fn epoch(&self) -> u64 {
        self.inner.epoch
    }

    /// Check if the hierarchy is empty (no class or property relationships).
    pub fn is_empty(&self) -> bool {
        self.inner.direct_subclass_of.is_empty() && self.inner.direct_subproperty_of.is_empty()
    }
}

/// Compute transitive closure using BFS from each node.
///
/// For each node with children, computes all reachable descendants.
/// Handles cycles by tracking visited nodes.
fn compute_transitive_closure(
    direct: &HashMap<Sid, SmallVec<[Sid; 2]>>,
) -> HashMap<Sid, Arc<[Sid]>> {
    let mut closure: HashMap<Sid, Arc<[Sid]>> = HashMap::new();

    // For each node that has children, compute all descendants
    for start in direct.keys() {
        let descendants = compute_descendants(start, direct);
        if !descendants.is_empty() {
            closure.insert(start.clone(), descendants.into());
        }
    }

    closure
}

/// Compute all descendants of a node using BFS.
///
/// Returns all nodes reachable from `start` (not including `start` itself).
/// Handles cycles by excluding the start node from results even if
/// reachable through a cycle.
fn compute_descendants(start: &Sid, direct: &HashMap<Sid, SmallVec<[Sid; 2]>>) -> Vec<Sid> {
    let mut visited: HashSet<Sid> = HashSet::new();
    let mut queue: VecDeque<Sid> = VecDeque::new();
    let mut result: Vec<Sid> = Vec::new();

    // Mark start as visited to prevent it from appearing in results
    // even if reachable through a cycle.
    visited.insert(start.clone());

    // Initialize with direct children
    if let Some(children) = direct.get(start) {
        for child in children {
            if visited.insert(child.clone()) {
                queue.push_back(child.clone());
                result.push(child.clone());
            }
        }
    }

    // BFS to find all descendants
    while let Some(current) = queue.pop_front() {
        if let Some(children) = direct.get(&current) {
            for child in children {
                if visited.insert(child.clone()) {
                    queue.push_back(child.clone());
                    result.push(child.clone());
                }
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_schema::{SchemaPredicateInfo, SchemaPredicates};
    use crate::sid::SidInterner;

    /// A schema entry tuple: (namespace, name, subclass_of entries, parent_props entries)
    type SchemaEntry<'a> = (u16, &'a str, Vec<(u16, &'a str)>, Vec<(u16, &'a str)>);

    fn make_schema(entries: Vec<SchemaEntry<'_>>) -> IndexSchema {
        let interner = SidInterner::new();
        let vals: Vec<SchemaPredicateInfo> = entries
            .into_iter()
            .map(|(ns, name, subclass_of, parent_props)| {
                let id = interner.intern(ns, name);
                let subclass_of: Vec<Sid> = subclass_of
                    .into_iter()
                    .map(|(ns, name)| interner.intern(ns, name))
                    .collect();
                let parent_props: Vec<Sid> = parent_props
                    .into_iter()
                    .map(|(ns, name)| interner.intern(ns, name))
                    .collect();
                SchemaPredicateInfo {
                    id,
                    subclass_of,
                    parent_props,
                    child_props: vec![], // Not used for closure computation
                }
            })
            .collect();

        IndexSchema {
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
        }
    }

    #[test]
    fn test_empty_schema() {
        let schema = IndexSchema::default();
        let hierarchy = SchemaHierarchy::from_db_root_schema(&schema);

        assert!(hierarchy.is_empty());
        assert_eq!(hierarchy.epoch(), 0);

        let interner = SidInterner::new();
        let animal = interner.intern(100, "Animal");
        assert!(hierarchy.subclasses_of(&animal).is_empty());
    }

    #[test]
    fn test_single_level_hierarchy() {
        // Dog rdfs:subClassOf Animal
        // Cat rdfs:subClassOf Animal
        let schema = make_schema(vec![
            (100, "Dog", vec![(100, "Animal")], vec![]),
            (100, "Cat", vec![(100, "Animal")], vec![]),
        ]);

        let hierarchy = SchemaHierarchy::from_db_root_schema(&schema);
        let interner = SidInterner::new();

        let animal = interner.intern(100, "Animal");
        let dog = interner.intern(100, "Dog");
        let cat = interner.intern(100, "Cat");

        let subclasses = hierarchy.subclasses_of(&animal);
        assert_eq!(subclasses.len(), 2);
        assert!(subclasses.contains(&dog));
        assert!(subclasses.contains(&cat));

        // Dog and Cat have no subclasses
        assert!(hierarchy.subclasses_of(&dog).is_empty());
        assert!(hierarchy.subclasses_of(&cat).is_empty());
    }

    #[test]
    fn test_multi_level_hierarchy() {
        // Poodle rdfs:subClassOf Dog
        // Dog rdfs:subClassOf Animal
        let schema = make_schema(vec![
            (100, "Poodle", vec![(100, "Dog")], vec![]),
            (100, "Dog", vec![(100, "Animal")], vec![]),
        ]);

        let hierarchy = SchemaHierarchy::from_db_root_schema(&schema);
        let interner = SidInterner::new();

        let animal = interner.intern(100, "Animal");
        let dog = interner.intern(100, "Dog");
        let poodle = interner.intern(100, "Poodle");

        // Animal has Dog and Poodle as descendants
        let animal_subclasses = hierarchy.subclasses_of(&animal);
        assert_eq!(animal_subclasses.len(), 2);
        assert!(animal_subclasses.contains(&dog));
        assert!(animal_subclasses.contains(&poodle));

        // Dog has only Poodle as descendant
        let dog_subclasses = hierarchy.subclasses_of(&dog);
        assert_eq!(dog_subclasses.len(), 1);
        assert!(dog_subclasses.contains(&poodle));

        // Poodle has no descendants
        assert!(hierarchy.subclasses_of(&poodle).is_empty());
    }

    #[test]
    fn test_diamond_hierarchy() {
        // Diamond: D inherits from both B and C, which both inherit from A
        //     A
        //    / \
        //   B   C
        //    \ /
        //     D
        let schema = make_schema(vec![
            (100, "D", vec![(100, "B"), (100, "C")], vec![]),
            (100, "B", vec![(100, "A")], vec![]),
            (100, "C", vec![(100, "A")], vec![]),
        ]);

        let hierarchy = SchemaHierarchy::from_db_root_schema(&schema);
        let interner = SidInterner::new();

        let a = interner.intern(100, "A");
        let b = interner.intern(100, "B");
        let c = interner.intern(100, "C");
        let d = interner.intern(100, "D");

        // A has B, C, D as descendants
        let a_subclasses = hierarchy.subclasses_of(&a);
        assert_eq!(a_subclasses.len(), 3);
        assert!(a_subclasses.contains(&b));
        assert!(a_subclasses.contains(&c));
        assert!(a_subclasses.contains(&d));

        // B has D as descendant
        let b_subclasses = hierarchy.subclasses_of(&b);
        assert_eq!(b_subclasses.len(), 1);
        assert!(b_subclasses.contains(&d));

        // C has D as descendant
        let c_subclasses = hierarchy.subclasses_of(&c);
        assert_eq!(c_subclasses.len(), 1);
        assert!(c_subclasses.contains(&d));
    }

    #[test]
    fn test_cycle_handling() {
        // Cycle: A -> B -> C -> A
        // This shouldn't happen in valid RDFS, but we handle it gracefully.
        // When inverted (parent -> children):
        //   C has child A, A has child B, B has child C
        // So from C, we can reach A, then B, then back to C (skipped as visited).
        let schema = make_schema(vec![
            (100, "A", vec![(100, "C")], vec![]), // A subClassOf C
            (100, "B", vec![(100, "A")], vec![]), // B subClassOf A
            (100, "C", vec![(100, "B")], vec![]), // C subClassOf B
        ]);

        let hierarchy = SchemaHierarchy::from_db_root_schema(&schema);
        let interner = SidInterner::new();

        let a = interner.intern(100, "A");
        let b = interner.intern(100, "B");
        let c = interner.intern(100, "C");

        // In a cycle, all nodes are reachable from each other (including
        // eventually reaching back to start through the cycle).
        // The key is that we don't infinite loop.
        // From C: children are [A], from A children are [B], from B children are [C]
        // But C started the traversal so it won't be re-added.
        let c_subclasses = hierarchy.subclasses_of(&c);
        assert_eq!(
            c_subclasses.len(),
            2,
            "C should have A and B as descendants"
        );
        assert!(c_subclasses.contains(&a));
        assert!(c_subclasses.contains(&b));

        // From A: children are [B], from B children are [C], from C children are [A]
        // A started, so won't be re-added.
        let a_subclasses = hierarchy.subclasses_of(&a);
        assert_eq!(
            a_subclasses.len(),
            2,
            "A should have B and C as descendants"
        );
        assert!(a_subclasses.contains(&b));
        assert!(a_subclasses.contains(&c));
    }

    #[test]
    fn test_property_hierarchy() {
        // hasFurColor rdfs:subPropertyOf hasColor
        // hasSkinColor rdfs:subPropertyOf hasColor
        let schema = make_schema(vec![
            (100, "hasFurColor", vec![], vec![(100, "hasColor")]),
            (100, "hasSkinColor", vec![], vec![(100, "hasColor")]),
        ]);

        let hierarchy = SchemaHierarchy::from_db_root_schema(&schema);
        let interner = SidInterner::new();

        let has_color = interner.intern(100, "hasColor");
        let has_fur_color = interner.intern(100, "hasFurColor");
        let has_skin_color = interner.intern(100, "hasSkinColor");

        let subprops = hierarchy.subproperties_of(&has_color);
        assert_eq!(subprops.len(), 2);
        assert!(subprops.contains(&has_fur_color));
        assert!(subprops.contains(&has_skin_color));
    }

    #[test]
    fn test_direct_vs_transitive() {
        // A <- B <- C (C subClassOf B subClassOf A)
        let schema = make_schema(vec![
            (100, "C", vec![(100, "B")], vec![]),
            (100, "B", vec![(100, "A")], vec![]),
        ]);

        let hierarchy = SchemaHierarchy::from_db_root_schema(&schema);
        let interner = SidInterner::new();

        let a = interner.intern(100, "A");
        let b = interner.intern(100, "B");
        let c = interner.intern(100, "C");

        // Direct: A -> [B], B -> [C]
        let a_direct = hierarchy.direct_subclasses_of(&a);
        assert_eq!(a_direct.len(), 1);
        assert!(a_direct.contains(&b));

        // Transitive: A -> [B, C]
        let a_transitive = hierarchy.subclasses_of(&a);
        assert_eq!(a_transitive.len(), 2);
        assert!(a_transitive.contains(&b));
        assert!(a_transitive.contains(&c));
    }
}
