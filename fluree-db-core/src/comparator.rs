//! Index comparators for Flakes
//!
//! Fluree uses 4 different index orderings to optimize different query patterns:
//!
//! | Index | Order | Use Case |
//! |-------|-------|----------|
//! | SPOT | s, p, o, t | Subject lookups |
//! | PSOT | p, s, o, t | Predicate-subject lookups |
//! | POST | p, o, s, t | Property value lookups |
//! | OPST | o, p, s, t | Reference lookups (object is SID) |
//!
//! ## Strict Total Ordering
//!
//! All comparators use strict total ordering - no nil-as-wildcard.
//! Use explicit min/max bounds for wildcard queries.

use crate::flake::Flake;
use std::cmp::Ordering;
use std::fmt;

/// Index type enumeration
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IndexType {
    /// Subject-Predicate-Object-Transaction
    Spot,
    /// Predicate-Subject-Object-Transaction
    Psot,
    /// Predicate-Object-Subject-Transaction
    Post,
    /// Object-Predicate-Subject-Transaction (for refs only)
    Opst,
}

impl IndexType {
    /// Get all index types
    pub fn all() -> &'static [IndexType] {
        &[
            IndexType::Spot,
            IndexType::Psot,
            IndexType::Post,
            IndexType::Opst,
        ]
    }

    /// Get the comparator function for this index type
    pub fn comparator(&self) -> fn(&Flake, &Flake) -> Ordering {
        match self {
            IndexType::Spot => cmp_spot,
            IndexType::Psot => cmp_psot,
            IndexType::Post => cmp_post,
            IndexType::Opst => cmp_opst,
        }
    }

    /// Compare two flakes using this index's ordering
    pub fn compare(&self, a: &Flake, b: &Flake) -> Ordering {
        self.comparator()(a, b)
    }

    /// Select the best index for a query based on known components
    ///
    /// Arguments indicate which query components are bound (not wildcards).
    ///
    /// Index selection priority:
    /// - SPOT: Subject bound (most selective)
    /// - PSOT: Predicate bound, object unbound (property-join pattern)
    /// - POST: Predicate and object bound (value lookup)
    /// - OPST: Object bound and is reference (reverse traversal)
    /// - SPOT: Default fallback
    pub fn for_query(s_bound: bool, p_bound: bool, o_bound: bool, o_is_ref: bool) -> IndexType {
        if s_bound {
            IndexType::Spot
        } else if p_bound && !o_bound {
            // Property-join: get all subjects with predicate P
            IndexType::Psot
        } else if p_bound && o_bound {
            // Value lookup: find subjects where P = V
            IndexType::Post
        } else if o_bound && o_is_ref {
            IndexType::Opst
        } else {
            IndexType::Spot // default
        }
    }

    /// Get the short name of this index
    pub fn name(&self) -> &'static str {
        match self {
            IndexType::Spot => "spot",
            IndexType::Psot => "psot",
            IndexType::Post => "post",
            IndexType::Opst => "opst",
        }
    }
}

impl fmt::Display for IndexType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl std::str::FromStr for IndexType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "spot" => Ok(IndexType::Spot),
            "psot" => Ok(IndexType::Psot),
            "post" => Ok(IndexType::Post),
            "opst" => Ok(IndexType::Opst),
            _ => Err(format!("Unknown index type: {s}")),
        }
    }
}

// === Helper functions for comparing flake components ===

/// Compare objects, taking datatype into account
///
/// Same datatype: compare values directly
/// Different datatypes: compare by type discriminant, then datatype
fn cmp_object(f1: &Flake, f2: &Flake) -> Ordering {
    // First compare values
    f1.o.cmp(&f2.o)
        // Then compare datatypes if values are equal
        .then_with(|| f1.dt.cmp(&f2.dt))
}

/// Compare metadata
fn cmp_meta(f1: &Flake, f2: &Flake) -> Ordering {
    match (&f1.m, &f2.m) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(m1), Some(m2)) => m1.cmp(m2),
    }
}

// === Index-specific comparators ===

/// SPOT comparator: Subject, Predicate, Object, Transaction
///
/// Used for subject-centric queries like "all facts about subject X".
pub fn cmp_spot(f1: &Flake, f2: &Flake) -> Ordering {
    f1.s.cmp(&f2.s)
        .then_with(|| f1.p.cmp(&f2.p))
        .then_with(|| cmp_object(f1, f2))
        .then_with(|| f1.t.cmp(&f2.t))
        .then_with(|| f1.op.cmp(&f2.op))
        .then_with(|| cmp_meta(f1, f2))
}

/// PSOT comparator: Predicate, Subject, Object, Transaction
///
/// Used for predicate-subject queries like "all subjects with predicate P".
pub fn cmp_psot(f1: &Flake, f2: &Flake) -> Ordering {
    f1.p.cmp(&f2.p)
        .then_with(|| f1.s.cmp(&f2.s))
        .then_with(|| cmp_object(f1, f2))
        .then_with(|| f1.t.cmp(&f2.t))
        .then_with(|| f1.op.cmp(&f2.op))
        .then_with(|| cmp_meta(f1, f2))
}

/// POST comparator: Predicate, Object, Subject, Transaction
///
/// Used for value lookups like "all subjects where P = V".
pub fn cmp_post(f1: &Flake, f2: &Flake) -> Ordering {
    f1.p.cmp(&f2.p)
        .then_with(|| cmp_object(f1, f2))
        .then_with(|| f1.s.cmp(&f2.s))
        .then_with(|| f1.t.cmp(&f2.t))
        .then_with(|| f1.op.cmp(&f2.op))
        .then_with(|| cmp_meta(f1, f2))
}

/// OPST comparator: Object, Predicate, Subject, Transaction
///
/// Used for reference traversal like "all subjects pointing to object O".
/// The object is compared as a SID (for references).
pub fn cmp_opst(f1: &Flake, f2: &Flake) -> Ordering {
    // For OPST, object is compared first (as a reference/SID)
    cmp_object(f1, f2)
        .then_with(|| f1.p.cmp(&f2.p))
        .then_with(|| f1.s.cmp(&f2.s))
        .then_with(|| f1.t.cmp(&f2.t))
        .then_with(|| f1.op.cmp(&f2.op))
        .then_with(|| cmp_meta(f1, f2))
}

/// Wrapper that provides Ord for a specific index type
///
/// This allows using standard library sorting with index-specific ordering.
pub struct FlakeOrd<'a> {
    pub flake: &'a Flake,
    pub index_type: IndexType,
}

impl<'a> FlakeOrd<'a> {
    pub fn new(flake: &'a Flake, index_type: IndexType) -> Self {
        Self { flake, index_type }
    }
}

impl PartialEq for FlakeOrd<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.index_type.compare(self.flake, other.flake) == Ordering::Equal
    }
}

impl Eq for FlakeOrd<'_> {}

impl PartialOrd for FlakeOrd<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FlakeOrd<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.index_type.compare(self.flake, other.flake)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sid::Sid;
    use crate::value::FlakeValue;

    fn make_flake(s: u16, p: u16, o: i64, t: i64) -> Flake {
        Flake::new(
            Sid::new(s, format!("s{s}")),
            Sid::new(p, format!("p{p}")),
            FlakeValue::Long(o),
            Sid::new(2, "long"),
            t,
            true,
            None,
        )
    }

    #[test]
    fn test_spot_ordering() {
        let f1 = make_flake(1, 1, 1, 1);
        let f2 = make_flake(1, 1, 1, 2); // same s,p,o, different t
        let f3 = make_flake(1, 1, 2, 1); // same s,p, different o
        let f4 = make_flake(1, 2, 1, 1); // same s, different p
        let f5 = make_flake(2, 1, 1, 1); // different s

        // SPOT: s first
        assert_eq!(cmp_spot(&f1, &f2), Ordering::Less); // t differs
        assert_eq!(cmp_spot(&f1, &f3), Ordering::Less); // o differs
        assert_eq!(cmp_spot(&f1, &f4), Ordering::Less); // p differs
        assert_eq!(cmp_spot(&f1, &f5), Ordering::Less); // s differs
    }

    #[test]
    fn test_psot_ordering() {
        let f1 = make_flake(1, 1, 1, 1);
        let f2 = make_flake(2, 1, 1, 1); // different s, same p
        let f3 = make_flake(1, 2, 1, 1); // different p

        // PSOT: p first, then s
        assert_eq!(cmp_psot(&f1, &f2), Ordering::Less); // s differs, p same
        assert_eq!(cmp_psot(&f1, &f3), Ordering::Less); // p differs
        assert_eq!(cmp_psot(&f2, &f3), Ordering::Less); // p differs (p1 < p2)
    }

    #[test]
    fn test_post_ordering() {
        let f1 = make_flake(1, 1, 1, 1);
        let f2 = make_flake(2, 1, 1, 1); // different s, same p,o
        let f3 = make_flake(1, 1, 2, 1); // different o

        // POST: p first, then o, then s
        assert_eq!(cmp_post(&f1, &f2), Ordering::Less); // s differs
        assert_eq!(cmp_post(&f1, &f3), Ordering::Less); // o differs
    }

    #[test]
    fn test_index_type_for_query() {
        // Subject bound -> SPOT
        assert_eq!(
            IndexType::for_query(true, false, false, false),
            IndexType::Spot
        );
        assert_eq!(
            IndexType::for_query(true, true, false, false),
            IndexType::Spot
        );
        assert_eq!(
            IndexType::for_query(true, true, true, false),
            IndexType::Spot
        );

        // Predicate bound, object unbound -> PSOT (property-join)
        assert_eq!(
            IndexType::for_query(false, true, false, false),
            IndexType::Psot
        );

        // Predicate and object bound -> POST (value lookup)
        assert_eq!(
            IndexType::for_query(false, true, true, false),
            IndexType::Post
        );
        assert_eq!(
            IndexType::for_query(false, true, true, true),
            IndexType::Post
        );

        // Object bound (ref) -> OPST
        assert_eq!(
            IndexType::for_query(false, false, true, true),
            IndexType::Opst
        );

        // Default -> SPOT
        assert_eq!(
            IndexType::for_query(false, false, false, false),
            IndexType::Spot
        );
        assert_eq!(
            IndexType::for_query(false, false, true, false),
            IndexType::Spot
        );
    }

    #[test]
    fn test_index_type_from_str() {
        assert_eq!("spot".parse::<IndexType>().unwrap(), IndexType::Spot);
        assert_eq!("PSOT".parse::<IndexType>().unwrap(), IndexType::Psot);
        assert!("invalid".parse::<IndexType>().is_err());
    }

    #[test]
    fn test_flake_ord_wrapper() {
        let f1 = make_flake(1, 2, 3, 4);
        let f2 = make_flake(2, 1, 3, 4);

        // SPOT ordering: f1 < f2 (s1 < s2)
        let ord1 = FlakeOrd::new(&f1, IndexType::Spot);
        let ord2 = FlakeOrd::new(&f2, IndexType::Spot);
        assert!(ord1 < ord2);

        // PSOT ordering: f2 < f1 (p1 < p2)
        let ord1 = FlakeOrd::new(&f1, IndexType::Psot);
        let ord2 = FlakeOrd::new(&f2, IndexType::Psot);
        assert!(ord2 < ord1);
    }
}
