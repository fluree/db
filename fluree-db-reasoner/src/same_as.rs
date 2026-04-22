//! Union-find based owl:sameAs equivalence tracking
//!
//! This module provides efficient handling of owl:sameAs equivalence classes
//! using a union-find data structure with path compression and union-by-rank.
//!
//! Key design decisions:
//! - Members map is NOT maintained incrementally during union operations
//! - Instead, it's built once in `finalize()` by bucketing all elements by root
//! - This avoids complexity with path compression invalidating member lists

use fluree_db_core::Sid;
use hashbrown::HashMap;
use std::sync::Arc;

/// Equivalence class tracker for owl:sameAs (used during materialization)
///
/// Uses union-find with path compression and union-by-rank for efficient
/// equivalence class operations.
#[derive(Debug, Clone)]
pub struct SameAsTracker {
    /// Union-find structure: maps Sid -> parent (self if root)
    parent: HashMap<Sid, Sid>,
    /// Rank for union-by-rank optimization
    rank: HashMap<Sid, u8>,
}

impl SameAsTracker {
    /// Create a new empty tracker
    pub fn new() -> Self {
        Self {
            parent: HashMap::new(),
            rank: HashMap::new(),
        }
    }

    /// Ensure a Sid is tracked (creates singleton if not present)
    fn ensure(&mut self, x: &Sid) {
        if !self.parent.contains_key(x) {
            self.parent.insert(x.clone(), x.clone());
            self.rank.insert(x.clone(), 0);
        }
    }

    /// Find the canonical representative with path compression
    ///
    /// Uses iterative path compression: first finds root, then updates
    /// all nodes along the path to point directly to root.
    pub fn find(&mut self, x: &Sid) -> Sid {
        self.ensure(x);

        // First pass: find root
        let mut current = x.clone();
        while self.parent[&current] != current {
            current = self.parent[&current].clone();
        }
        let root = current;

        // Second pass: path compression
        let mut current = x.clone();
        while self.parent[&current] != root {
            let next = self.parent[&current].clone();
            self.parent.insert(current, root.clone());
            current = next;
        }

        root
    }

    /// Union two equivalence classes using union-by-rank
    ///
    /// Returns `true` if the two elements were in different classes
    /// (i.e., a merge actually occurred).
    pub fn union(&mut self, x: &Sid, y: &Sid) -> bool {
        let root_x = self.find(x);
        let root_y = self.find(y);

        if root_x == root_y {
            return false; // Already in same class
        }

        // Union by rank: attach smaller tree under larger
        let rank_x = self.rank[&root_x];
        let rank_y = self.rank[&root_y];

        match rank_x.cmp(&rank_y) {
            std::cmp::Ordering::Less => {
                self.parent.insert(root_x, root_y);
            }
            std::cmp::Ordering::Greater => {
                self.parent.insert(root_y, root_x);
            }
            std::cmp::Ordering::Equal => {
                self.parent.insert(root_y, root_x.clone());
                self.rank.insert(root_x, rank_x + 1);
            }
        }

        true
    }

    /// Check if x and y are in the same equivalence class
    pub fn same(&mut self, x: &Sid, y: &Sid) -> bool {
        self.find(x) == self.find(y)
    }

    /// Get the canonical representative without modifying (for read-only access)
    ///
    /// This does NOT do path compression, so it's O(log n) instead of O(α(n)).
    /// Use `find` if you need repeated lookups.
    pub fn canonical(&self, x: &Sid) -> Sid {
        if !self.parent.contains_key(x) {
            return x.clone();
        }

        let mut current = x.clone();
        while self.parent[&current] != current {
            current = self.parent[&current].clone();
        }
        current
    }

    /// Returns true if this tracker has any equivalence relations
    pub fn is_empty(&self) -> bool {
        // Empty if no entries, or all entries are singletons (parent == self)
        self.parent.iter().all(|(k, v)| k == v)
    }

    /// Get number of tracked Sids
    pub fn len(&self) -> usize {
        self.parent.len()
    }

    /// Finalize: compress all paths and build FrozenSameAs
    ///
    /// This is the only place where the members map is built, avoiding
    /// the complexity of maintaining it during union operations.
    pub fn finalize(mut self) -> FrozenSameAs {
        // 1. Compress all paths and collect (element, root) pairs
        let elements: Vec<Sid> = self.parent.keys().cloned().collect();
        let mut element_to_root: HashMap<Sid, Sid> = HashMap::with_capacity(elements.len());

        for elem in &elements {
            let root = self.find(elem);
            element_to_root.insert(elem.clone(), root);
        }

        // 2. Bucket elements by root
        let mut root_to_members: HashMap<Sid, Vec<Sid>> = HashMap::new();
        for (elem, root) in &element_to_root {
            root_to_members
                .entry(root.clone())
                .or_default()
                .push(elem.clone());
        }

        // 3. Convert to Arc<[Sid]> for cheap sharing
        let members: HashMap<Sid, Arc<[Sid]>> = root_to_members
            .into_iter()
            .map(|(root, mut members)| {
                members.sort_unstable(); // Deterministic order
                (root, members.into())
            })
            .collect();

        FrozenSameAs {
            canonical: element_to_root,
            members,
        }
    }
}

impl Default for SameAsTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Frozen equivalence state for query-time use (immutable after reasoning)
///
/// This is the read-only version of SameAsTracker, optimized for lookups:
/// - O(1) canonical representative lookup
/// - O(1) equivalence class enumeration
#[derive(Debug, Clone)]
pub struct FrozenSameAs {
    /// Sid -> canonical representative
    canonical: HashMap<Sid, Sid>,
    /// Root -> all members (built once in finalize, O(1) lookup)
    members: HashMap<Sid, Arc<[Sid]>>,
}

/// Empty slice for Sids not in any equivalence class
static EMPTY_SIDS: &[Sid] = &[];

impl FrozenSameAs {
    /// Create an empty FrozenSameAs (no equivalences)
    pub fn empty() -> Self {
        Self {
            canonical: HashMap::new(),
            members: HashMap::new(),
        }
    }

    /// Get the canonical representative for a Sid
    ///
    /// Returns the Sid itself if not part of any equivalence class.
    pub fn canonical(&self, x: Sid) -> Sid {
        self.canonical.get(&x).cloned().unwrap_or(x)
    }

    /// Expand a Sid to all equivalents (including itself)
    ///
    /// Returns a slice of all Sids in the same equivalence class.
    /// If the Sid is not part of any class, returns an empty slice
    /// (the caller should treat the Sid as its own singleton class).
    pub fn expand(&self, x: Sid) -> &[Sid] {
        // First get canonical, then look up members
        if let Some(root) = self.canonical.get(&x) {
            self.members
                .get(root)
                .map(std::convert::AsRef::as_ref)
                .unwrap_or(EMPTY_SIDS)
        } else {
            EMPTY_SIDS
        }
    }

    /// Check if a Sid is tracked in this equivalence structure
    pub fn contains(&self, x: &Sid) -> bool {
        self.canonical.contains_key(x)
    }

    /// Get the number of equivalence classes
    pub fn num_classes(&self) -> usize {
        self.members.len()
    }

    /// Get total number of tracked Sids
    pub fn num_elements(&self) -> usize {
        self.canonical.len()
    }

    /// Check if empty (no equivalences)
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Iterate over all equivalence classes
    ///
    /// Returns an iterator over (root, members) pairs.
    pub fn members_iter(&self) -> impl Iterator<Item = (&Sid, &[Sid])> {
        self.members
            .iter()
            .map(|(root, members)| (root, members.as_ref()))
    }
}

impl Default for FrozenSameAs {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sid(n: u16) -> Sid {
        Sid::new(n, format!("test:{n}"))
    }

    #[test]
    fn test_singleton_find() {
        let mut tracker = SameAsTracker::new();
        let a = sid(1);

        // Finding a singleton should return itself
        assert_eq!(tracker.find(&a), a);
    }

    #[test]
    fn test_basic_union() {
        let mut tracker = SameAsTracker::new();
        let a = sid(1);
        let b = sid(2);

        // Initially different
        assert!(!tracker.same(&a, &b));

        // Union should succeed
        assert!(tracker.union(&a, &b));

        // Now same
        assert!(tracker.same(&a, &b));

        // Second union should return false (already same)
        assert!(!tracker.union(&a, &b));
    }

    #[test]
    fn test_transitivity() {
        let mut tracker = SameAsTracker::new();
        let a = sid(1);
        let b = sid(2);
        let c = sid(3);

        tracker.union(&a, &b);
        tracker.union(&b, &c);

        // Transitivity: a same as c through b
        assert!(tracker.same(&a, &c));
    }

    #[test]
    fn test_finalize_and_expand() {
        let mut tracker = SameAsTracker::new();
        let a = sid(1);
        let b = sid(2);
        let c = sid(3);
        let d = sid(4);

        // Create two equivalence classes: {a, b, c} and {d}
        tracker.union(&a, &b);
        tracker.union(&b, &c);
        // d is standalone

        let frozen = tracker.finalize();

        // All of a, b, c should have the same canonical
        let canonical_a = frozen.canonical(a.clone());
        assert_eq!(frozen.canonical(b.clone()), canonical_a);
        assert_eq!(frozen.canonical(c.clone()), canonical_a);

        // d should be its own canonical (or not tracked)
        // Since d was never involved in a union, it won't be in the tracker
        assert_eq!(frozen.canonical(d.clone()), d); // Returns d itself

        // Expand should return all three members for any of a, b, c
        let expanded = frozen.expand(a.clone());
        assert_eq!(expanded.len(), 3);
        assert!(expanded.contains(&a));
        assert!(expanded.contains(&b));
        assert!(expanded.contains(&c));

        // Same expansion for b and c
        assert_eq!(frozen.expand(b).len(), 3);
        assert_eq!(frozen.expand(c).len(), 3);

        // d was never unioned, so expand returns empty
        assert!(frozen.expand(d).is_empty());
    }

    #[test]
    fn test_path_compression() {
        let mut tracker = SameAsTracker::new();

        // Create a chain: 1 -> 2 -> 3 -> 4 -> 5
        for i in 1..5 {
            tracker.union(&sid(i), &sid(i + 1));
        }

        // Finding any element should compress the path
        let root = tracker.find(&sid(1));

        // After compression, all should point directly to root
        for i in 1..=5 {
            assert_eq!(tracker.find(&sid(i)), root);
        }
    }
}
