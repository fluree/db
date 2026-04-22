//! Predicate-indexed delta set for efficient rule joins.
//!
//! During semi-naive evaluation, we need to quickly find:
//! - All new facts with a specific predicate
//! - All new facts with a specific (predicate, subject)
//! - All new facts with a specific (predicate, object)

use fluree_db_core::flake::Flake;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::Sid;
use hashbrown::HashMap;

use super::util::canonicalize_flake;
use crate::same_as::SameAsTracker;

/// Predicate-indexed delta set for efficient rule joins
///
/// During semi-naive evaluation, we need to quickly find:
/// - All new facts with a specific predicate
/// - All new facts with a specific (predicate, subject)
/// - All new facts with a specific (predicate, object)
#[derive(Debug, Default)]
pub struct DeltaSet {
    /// All flakes in this delta, for iteration
    flakes: Vec<Flake>,
    /// Index by predicate -> list of flake indices
    by_p: HashMap<Sid, Vec<usize>>,
    /// Index by (predicate, subject) -> list of flake indices
    by_ps: HashMap<(Sid, Sid), Vec<usize>>,
    /// Index by (predicate, object_sid) -> list of flake indices
    /// Only includes flakes where object is a Ref
    by_po: HashMap<(Sid, Sid), Vec<usize>>,
}

impl DeltaSet {
    /// Create a new empty delta set
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a delta set with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            flakes: Vec::with_capacity(capacity),
            by_p: HashMap::new(),
            by_ps: HashMap::new(),
            by_po: HashMap::new(),
        }
    }

    /// Add a flake to the delta set
    pub fn push(&mut self, flake: Flake) {
        let idx = self.flakes.len();

        // Index by predicate
        self.by_p.entry(flake.p.clone()).or_default().push(idx);

        // Index by (predicate, subject)
        self.by_ps
            .entry((flake.p.clone(), flake.s.clone()))
            .or_default()
            .push(idx);

        // Index by (predicate, object) if object is a Ref
        if let FlakeValue::Ref(o) = &flake.o {
            self.by_po
                .entry((flake.p.clone(), o.clone()))
                .or_default()
                .push(idx);
        }

        self.flakes.push(flake);
    }

    /// Get all flakes with a specific predicate
    pub fn get_by_p(&self, p: &Sid) -> impl Iterator<Item = &Flake> {
        self.by_p
            .get(p)
            .into_iter()
            .flat_map(|indices| indices.iter().map(|&i| &self.flakes[i]))
    }

    /// Get all flakes with a specific (predicate, subject)
    pub fn get_by_ps(&self, p: &Sid, s: &Sid) -> impl Iterator<Item = &Flake> {
        self.by_ps
            .get(&(p.clone(), s.clone()))
            .into_iter()
            .flat_map(|indices| indices.iter().map(|&i| &self.flakes[i]))
    }

    /// Get all flakes with a specific (predicate, object)
    /// Only works for flakes where object is a Ref
    pub fn get_by_po(&self, p: &Sid, o: &Sid) -> impl Iterator<Item = &Flake> {
        self.by_po
            .get(&(p.clone(), o.clone()))
            .into_iter()
            .flat_map(|indices| indices.iter().map(|&i| &self.flakes[i]))
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.flakes.is_empty()
    }

    /// Get number of flakes
    pub fn len(&self) -> usize {
        self.flakes.len()
    }

    /// Iterate over all flakes
    pub fn iter(&self) -> impl Iterator<Item = &Flake> {
        self.flakes.iter()
    }

    /// Clear the delta set
    pub fn clear(&mut self) {
        self.flakes.clear();
        self.by_p.clear();
        self.by_ps.clear();
        self.by_po.clear();
    }

    /// Recanonicalize all flakes using sameAs equivalence (eq-rep-s/o)
    ///
    /// This rebuilds the delta set with all subjects and objects replaced
    /// by their canonical representatives. Used when sameAs changes during
    /// fixpoint iteration to ensure derived facts use canonical forms.
    pub fn recanonicalize(self, same_as: &SameAsTracker) -> Self {
        let mut result = DeltaSet::with_capacity(self.flakes.len());
        for flake in self.flakes {
            let canonical = canonicalize_flake(&flake, same_as);
            result.push(canonical);
        }
        result
    }
}
