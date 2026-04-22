//! Tests for DeltaSet and DerivedSet data structures.

use super::*;
use crate::same_as::SameAsTracker;

#[test]
fn test_delta_set_basic() {
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(1, 10, 2, 1));
    delta.push(make_ref_flake(2, 10, 3, 1));
    delta.push(make_ref_flake(1, 20, 4, 1));

    assert_eq!(delta.len(), 3);

    // By predicate
    let by_p10: Vec<_> = delta.get_by_p(&sid(10)).collect();
    assert_eq!(by_p10.len(), 2);

    // By (predicate, subject)
    let by_ps: Vec<_> = delta.get_by_ps(&sid(10), &sid(1)).collect();
    assert_eq!(by_ps.len(), 1);

    // By (predicate, object)
    let by_po: Vec<_> = delta.get_by_po(&sid(10), &sid(2)).collect();
    assert_eq!(by_po.len(), 1);
}

#[test]
fn test_derived_set_dedup() {
    let mut derived = DerivedSet::new();

    // Add first flake
    assert!(derived.try_add(make_ref_flake(1, 10, 2, 1)));
    assert_eq!(derived.len(), 1);

    // Try to add duplicate - should be rejected
    assert!(!derived.try_add(make_ref_flake(1, 10, 2, 2))); // different t, same SPO
    assert_eq!(derived.len(), 1);

    // Add different flake
    assert!(derived.try_add(make_ref_flake(1, 10, 3, 1)));
    assert_eq!(derived.len(), 2);
}

#[test]
fn test_recanonicalize_subjects_and_objects() {
    // Create sameAs equivalences: 1 ≡ 2, 3 ≡ 4
    let mut tracker = SameAsTracker::new();
    tracker.union(&sid(1), &sid(2)); // canonical will be one of 1 or 2
    tracker.union(&sid(3), &sid(4)); // canonical will be one of 3 or 4

    // Create delta with non-canonical subjects/objects
    let mut delta = DeltaSet::new();
    // P(2, 4) - both subject and object are non-canonical (assuming 1 and 3 are canonical)
    delta.push(make_ref_flake(2, 10, 4, 1));
    // P(1, 3) - already canonical
    delta.push(make_ref_flake(1, 10, 3, 1));

    // Recanonicalize
    let canonical_delta = delta.recanonicalize(&tracker);

    assert_eq!(canonical_delta.len(), 2);

    // Verify both flakes now use canonical representatives
    let canonical_1 = tracker.canonical(&sid(1));
    let canonical_3 = tracker.canonical(&sid(3));

    for flake in canonical_delta.iter() {
        // Subject should be canonical representative of {1, 2}
        assert_eq!(flake.s, canonical_1);
        // Object should be canonical representative of {3, 4}
        if let FlakeValue::Ref(o) = &flake.o {
            assert_eq!(*o, canonical_3);
        } else {
            panic!("Expected Ref object");
        }
    }
}

#[test]
fn test_recanonicalize_preserves_non_ref_objects() {
    // Create sameAs equivalence: 1 ≡ 2
    let mut tracker = SameAsTracker::new();
    tracker.union(&sid(1), &sid(2));

    // Create delta with non-Ref object
    let mut delta = DeltaSet::new();
    let flake_with_string = Flake::new(
        sid(2), // non-canonical subject
        sid(10),
        FlakeValue::String("hello".into()),
        sid(0),
        1,
        true,
        None,
    );
    delta.push(flake_with_string);

    // Recanonicalize
    let canonical_delta = delta.recanonicalize(&tracker);

    assert_eq!(canonical_delta.len(), 1);
    let flake = canonical_delta.iter().next().unwrap();

    // Subject should be canonicalized
    let canonical_1 = tracker.canonical(&sid(1));
    assert_eq!(flake.s, canonical_1);

    // Object should be preserved as String
    if let FlakeValue::String(s) = &flake.o {
        assert_eq!(s.as_str(), "hello");
    } else {
        panic!("Expected String object");
    }
}

#[test]
fn test_recanonicalize_no_change_when_already_canonical() {
    // Create sameAs equivalence: 1 ≡ 2 (assume 1 is canonical)
    let mut tracker = SameAsTracker::new();
    tracker.union(&sid(1), &sid(2));
    let canonical = tracker.canonical(&sid(1));

    // Create delta with already-canonical subject
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(
        canonical.namespace_code,
        10,
        5, // object not in any equivalence class
        1,
    ));

    // Recanonicalize
    let canonical_delta = delta.recanonicalize(&tracker);

    assert_eq!(canonical_delta.len(), 1);
    let flake = canonical_delta.iter().next().unwrap();

    // Subject should remain the same (already canonical)
    assert_eq!(flake.s, canonical);

    // Object not in equivalence class should be unchanged
    if let FlakeValue::Ref(o) = &flake.o {
        assert_eq!(*o, sid(5));
    }
}
