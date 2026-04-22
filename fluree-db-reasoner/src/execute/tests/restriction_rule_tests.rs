//! Tests for OWL restriction rules (cls-*).
//!
//! This module tests:
//! - HasValue rules (cls-hv1, cls-hv2)
//! - SomeValuesFrom rule (cls-svf1)
//! - AllValuesFrom rule (cls-avf)
//! - MaxCardinality rules (cls-maxc2, cls-maxqc)
//! - IntersectionOf rules (cls-int1, cls-int2)
//! - UnionOf rule (cls-uni)
//! - OneOf rule (cls-oo)

use super::*;
use crate::restrictions::{
    ClassRef, ParsedRestriction, RestrictionIndex, RestrictionType, RestrictionValue,
};
use crate::same_as::SameAsTracker;
use crate::types::{ChainElement, PropertyExpression};
use fluree_vocab::namespaces::{OWL, RDF};
use fluree_vocab::predicates::RDF_TYPE;

// ============================================================================
// HasValue Rule Tests
// ============================================================================

#[test]
fn test_has_value_backward_rule() {
    // Create a hasValue restriction: C (sid 100) is a restriction with hasValue(P10, v50)
    let mut index = RestrictionIndex::new();
    index.add_restriction_for_test(ParsedRestriction {
        restriction_id: sid(100),
        restriction_type: RestrictionType::HasValue {
            property: PropertyExpression::Named(sid(10)),
            value: RestrictionValue::Ref(sid(50)),
        },
    });

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    // Delta has type(x, C) where x=1 and C=100 (the restriction class)
    let mut delta = DeltaSet::new();
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(100)),
        sid(0),
        1,
        true,
        None,
    ));

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let mut diagnostics = ReasoningDiagnostics::default();

    let mut ctx = RuleContext {
        delta: &delta,
        derived: &derived,
        new_delta: &mut new_delta,
        same_as: &same_as,
        rdf_type_sid: &rdf_type_sid,
        t: 1,
        diagnostics: &mut diagnostics,
    };

    apply_has_value_backward_rule(&index, &mut ctx);

    // Should derive P(1, v) = P10(1, 50)
    assert_eq!(new_delta.len(), 1);
    let derived_flake = new_delta.iter().next().unwrap();
    assert_eq!(derived_flake.s, sid(1));
    assert_eq!(derived_flake.p, sid(10));
    if let FlakeValue::Ref(o) = &derived_flake.o {
        assert_eq!(*o, sid(50));
    } else {
        panic!("Expected Ref object");
    }

    assert_eq!(diagnostics.rules_fired.get("cls-hv1"), Some(&1));
}

#[test]
fn test_has_value_forward_rule() {
    // Create a hasValue restriction: C (sid 100) is a restriction with hasValue(P10, v50)
    let mut index = RestrictionIndex::new();
    index.add_restriction_for_test(ParsedRestriction {
        restriction_id: sid(100),
        restriction_type: RestrictionType::HasValue {
            property: PropertyExpression::Named(sid(10)),
            value: RestrictionValue::Ref(sid(50)),
        },
    });

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    // Delta has P10(1, 50) - x has the required property value
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(1, 10, 50, 1));

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let mut diagnostics = ReasoningDiagnostics::default();

    let mut ctx = RuleContext {
        delta: &delta,
        derived: &derived,
        new_delta: &mut new_delta,
        same_as: &same_as,
        rdf_type_sid: &rdf_type_sid,
        t: 1,
        diagnostics: &mut diagnostics,
    };

    apply_has_value_forward_rule(&index, &mut ctx);

    // Should derive type(1, C) = type(1, 100)
    assert_eq!(new_delta.len(), 1);
    let derived_flake = new_delta.iter().next().unwrap();
    assert_eq!(derived_flake.s, sid(1));
    assert_eq!(derived_flake.p, rdf_type_sid);
    if let FlakeValue::Ref(c) = &derived_flake.o {
        assert_eq!(*c, sid(100));
    } else {
        panic!("Expected Ref object");
    }

    assert_eq!(diagnostics.rules_fired.get("cls-hv2"), Some(&1));
}

#[test]
fn test_has_value_forward_rule_no_match() {
    // Create a hasValue restriction: C requires P10 to have value 50
    let mut index = RestrictionIndex::new();
    index.add_restriction_for_test(ParsedRestriction {
        restriction_id: sid(100),
        restriction_type: RestrictionType::HasValue {
            property: PropertyExpression::Named(sid(10)),
            value: RestrictionValue::Ref(sid(50)),
        },
    });

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    // Delta has P10(1, 60) - WRONG value (not 50)
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(1, 10, 60, 1));

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let mut diagnostics = ReasoningDiagnostics::default();

    let mut ctx = RuleContext {
        delta: &delta,
        derived: &derived,
        new_delta: &mut new_delta,
        same_as: &same_as,
        rdf_type_sid: &rdf_type_sid,
        t: 1,
        diagnostics: &mut diagnostics,
    };

    apply_has_value_forward_rule(&index, &mut ctx);

    // Should derive nothing - value doesn't match
    assert_eq!(new_delta.len(), 0);
}

// ============================================================================
// SomeValuesFrom Rule Tests
// ============================================================================

#[test]
fn test_some_values_from_rule() {
    // Create a someValuesFrom restriction
    let mut index = RestrictionIndex::new();
    index.add_restriction_for_test(ParsedRestriction {
        restriction_id: sid(100),
        restriction_type: RestrictionType::SomeValuesFrom {
            property: PropertyExpression::Named(sid(10)),
            target_class: ClassRef::Named(sid(200)),
        },
    });

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    // Delta has P10(1, 2) and type(2, D)
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(1, 10, 2, 1));
    delta.push(Flake::new(
        sid(2),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(200)),
        sid(0),
        1,
        true,
        None,
    ));

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let mut diagnostics = ReasoningDiagnostics::default();

    let mut ctx = RuleContext {
        delta: &delta,
        derived: &derived,
        new_delta: &mut new_delta,
        same_as: &same_as,
        rdf_type_sid: &rdf_type_sid,
        t: 1,
        diagnostics: &mut diagnostics,
    };

    apply_some_values_from_rule(&index, &mut ctx);

    // Should derive type(1, C)
    assert_eq!(new_delta.len(), 1);
    let derived_flake = new_delta.iter().next().unwrap();
    assert_eq!(derived_flake.s, sid(1));
    if let FlakeValue::Ref(c) = &derived_flake.o {
        assert_eq!(*c, sid(100));
    } else {
        panic!("Expected Ref object");
    }

    assert_eq!(diagnostics.rules_fired.get("cls-svf1"), Some(&1));
}

// ============================================================================
// AllValuesFrom Rule Tests
// ============================================================================

#[test]
fn test_all_values_from_rule() {
    // Create an allValuesFrom restriction
    let mut index = RestrictionIndex::new();
    index.add_restriction_for_test(ParsedRestriction {
        restriction_id: sid(100),
        restriction_type: RestrictionType::AllValuesFrom {
            property: PropertyExpression::Named(sid(10)),
            target_class: ClassRef::Named(sid(200)),
        },
    });

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    // Delta has type(1, C) and P10(1, 2)
    let mut delta = DeltaSet::new();
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(100)),
        sid(0),
        1,
        true,
        None,
    ));
    delta.push(make_ref_flake(1, 10, 2, 1));

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let mut diagnostics = ReasoningDiagnostics::default();

    let mut ctx = RuleContext {
        delta: &delta,
        derived: &derived,
        new_delta: &mut new_delta,
        same_as: &same_as,
        rdf_type_sid: &rdf_type_sid,
        t: 1,
        diagnostics: &mut diagnostics,
    };

    apply_all_values_from_rule(&index, &mut ctx);

    // Should derive type(2, D)
    assert_eq!(new_delta.len(), 1);
    let derived_flake = new_delta.iter().next().unwrap();
    assert_eq!(derived_flake.s, sid(2));
    if let FlakeValue::Ref(c) = &derived_flake.o {
        assert_eq!(*c, sid(200));
    } else {
        panic!("Expected Ref object");
    }

    assert_eq!(diagnostics.rules_fired.get("cls-avf"), Some(&1));
}

#[test]
fn test_all_values_from_rule_with_chain_property() {
    // Create an allValuesFrom restriction over a property chain: P10 o P20
    let mut index = RestrictionIndex::new();
    index.add_restriction_for_test(ParsedRestriction {
        restriction_id: sid(100),
        restriction_type: RestrictionType::AllValuesFrom {
            property: PropertyExpression::Chain(vec![
                ChainElement::direct(sid(10)),
                ChainElement::direct(sid(20)),
            ]),
            target_class: ClassRef::Named(sid(200)),
        },
    });

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    // Delta has type(1, C), P10(1, 2), P20(2, 3)
    let mut delta = DeltaSet::new();
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(100)),
        sid(0),
        1,
        true,
        None,
    ));
    delta.push(make_ref_flake(1, 10, 2, 1));
    delta.push(make_ref_flake(2, 20, 3, 1));

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let mut diagnostics = ReasoningDiagnostics::default();

    let mut ctx = RuleContext {
        delta: &delta,
        derived: &derived,
        new_delta: &mut new_delta,
        same_as: &same_as,
        rdf_type_sid: &rdf_type_sid,
        t: 1,
        diagnostics: &mut diagnostics,
    };

    apply_all_values_from_rule(&index, &mut ctx);

    // Should derive type(3, D) by following the chain from 1 -> 2 -> 3
    assert_eq!(new_delta.len(), 1);
    let derived_flake = new_delta.iter().next().unwrap();
    assert_eq!(derived_flake.s, sid(3));
    if let FlakeValue::Ref(c) = &derived_flake.o {
        assert_eq!(*c, sid(200));
    } else {
        panic!("Expected Ref object");
    }

    assert_eq!(diagnostics.rules_fired.get("cls-avf"), Some(&1));
}

#[test]
fn test_all_values_from_rule_with_chain_across_delta_and_derived() {
    // Create an allValuesFrom restriction over a property chain: P10 o P20
    let mut index = RestrictionIndex::new();
    index.add_restriction_for_test(ParsedRestriction {
        restriction_id: sid(100),
        restriction_type: RestrictionType::AllValuesFrom {
            property: PropertyExpression::Chain(vec![
                ChainElement::direct(sid(10)),
                ChainElement::direct(sid(20)),
            ]),
            target_class: ClassRef::Named(sid(200)),
        },
    });

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    // The new type and first hop are in delta, while the second hop already exists in derived.
    // Chain-aware restriction evaluation must traverse the union of both sets.
    let mut delta = DeltaSet::new();
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(100)),
        sid(0),
        1,
        true,
        None,
    ));
    delta.push(make_ref_flake(1, 10, 2, 1));

    let mut derived = DerivedSet::new();
    derived.try_add(make_ref_flake(2, 20, 3, 0));

    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let mut diagnostics = ReasoningDiagnostics::default();

    let mut ctx = RuleContext {
        delta: &delta,
        derived: &derived,
        new_delta: &mut new_delta,
        same_as: &same_as,
        rdf_type_sid: &rdf_type_sid,
        t: 1,
        diagnostics: &mut diagnostics,
    };

    apply_all_values_from_rule(&index, &mut ctx);

    assert_eq!(new_delta.len(), 1);
    let derived_flake = new_delta.iter().next().unwrap();
    assert_eq!(derived_flake.s, sid(3));
    if let FlakeValue::Ref(c) = &derived_flake.o {
        assert_eq!(*c, sid(200));
    } else {
        panic!("Expected Ref object");
    }
}

// ============================================================================
// MaxCardinality Rule Tests
// ============================================================================

#[test]
fn test_max_cardinality_rule() {
    // Create a maxCardinality=1 restriction
    let mut index = RestrictionIndex::new();
    index.add_restriction_for_test(ParsedRestriction {
        restriction_id: sid(100),
        restriction_type: RestrictionType::MaxCardinality1 {
            property: PropertyExpression::Named(sid(10)),
        },
    });

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
    let owl_same_as_sid = owl::same_as_sid();

    // Delta has type(1, C) and P10(1, 2), P10(1, 3) - two values
    let mut delta = DeltaSet::new();
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(100)),
        sid(0),
        1,
        true,
        None,
    ));
    delta.push(make_ref_flake(1, 10, 2, 1));
    delta.push(make_ref_flake(1, 10, 3, 1));

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let mut diagnostics = ReasoningDiagnostics::default();

    let mut ctx = IdentityRuleContext {
        delta: &delta,
        derived: &derived,
        new_delta: &mut new_delta,
        same_as: &same_as,
        owl_same_as_sid: &owl_same_as_sid,
        rdf_type_sid: &rdf_type_sid,
        t: 1,
        same_as_changed: false,
        diagnostics: &mut diagnostics,
    };

    apply_max_cardinality_rule(&index, &mut ctx);

    // Should derive sameAs(2, 3)
    assert_eq!(new_delta.len(), 1);
    let same_as_flake = new_delta.iter().next().unwrap();
    assert_eq!(same_as_flake.p.namespace_code, OWL);
    assert_eq!(same_as_flake.p.name.as_ref(), "sameAs");

    assert_eq!(diagnostics.rules_fired.get("cls-maxc2"), Some(&1));
}

// ============================================================================
// IntersectionOf Rule Tests
// ============================================================================

#[test]
fn test_intersection_backward_rule() {
    // Create an intersection restriction: I is intersectionOf [C1, C2]
    let mut index = RestrictionIndex::new();
    index.add_restriction_for_test(ParsedRestriction {
        restriction_id: sid(100),
        restriction_type: RestrictionType::IntersectionOf {
            members: vec![ClassRef::Named(sid(200)), ClassRef::Named(sid(300))],
        },
    });

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    // Delta has type(1, I)
    let mut delta = DeltaSet::new();
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(100)),
        sid(0),
        1,
        true,
        None,
    ));

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let mut diagnostics = ReasoningDiagnostics::default();

    let mut ctx = RuleContext {
        delta: &delta,
        derived: &derived,
        new_delta: &mut new_delta,
        same_as: &same_as,
        rdf_type_sid: &rdf_type_sid,
        t: 1,
        diagnostics: &mut diagnostics,
    };

    apply_intersection_backward_rule(&index, &mut ctx);

    // Should derive type(1, C1) and type(1, C2)
    assert_eq!(new_delta.len(), 2);

    let types: HashSet<Sid> = new_delta
        .iter()
        .filter_map(|f| {
            if let FlakeValue::Ref(cls) = &f.o {
                Some(cls.clone())
            } else {
                None
            }
        })
        .collect();

    assert!(types.contains(&sid(200)));
    assert!(types.contains(&sid(300)));
    assert_eq!(diagnostics.rules_fired.get("cls-int2"), Some(&2));
}

#[test]
fn test_intersection_forward_rule() {
    // Create an intersection restriction: I is intersectionOf [C1, C2]
    let mut index = RestrictionIndex::new();
    index.add_restriction_for_test(ParsedRestriction {
        restriction_id: sid(100),
        restriction_type: RestrictionType::IntersectionOf {
            members: vec![ClassRef::Named(sid(200)), ClassRef::Named(sid(300))],
        },
    });

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    // Delta has type(1, C1) and type(1, C2)
    let mut delta = DeltaSet::new();
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(200)),
        sid(0),
        1,
        true,
        None,
    ));
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(300)),
        sid(0),
        1,
        true,
        None,
    ));

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let mut diagnostics = ReasoningDiagnostics::default();

    let mut ctx = RuleContext {
        delta: &delta,
        derived: &derived,
        new_delta: &mut new_delta,
        same_as: &same_as,
        rdf_type_sid: &rdf_type_sid,
        t: 1,
        diagnostics: &mut diagnostics,
    };

    apply_intersection_forward_rule(&index, &mut ctx);

    // Should derive type(1, I)
    assert_eq!(new_delta.len(), 1);
    let derived_flake = new_delta.iter().next().unwrap();
    assert_eq!(derived_flake.s, sid(1));
    if let FlakeValue::Ref(cls) = &derived_flake.o {
        assert_eq!(*cls, sid(100));
    } else {
        panic!("Expected Ref object");
    }

    assert_eq!(diagnostics.rules_fired.get("cls-int1"), Some(&1));
}

// ============================================================================
// UnionOf Rule Tests
// ============================================================================

#[test]
fn test_union_rule() {
    // Create a union restriction: U is unionOf [C1, C2]
    let mut index = RestrictionIndex::new();
    index.add_restriction_for_test(ParsedRestriction {
        restriction_id: sid(100),
        restriction_type: RestrictionType::UnionOf {
            members: vec![ClassRef::Named(sid(200)), ClassRef::Named(sid(300))],
        },
    });

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    // Delta has type(1, C1) - only one member type
    let mut delta = DeltaSet::new();
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(200)),
        sid(0),
        1,
        true,
        None,
    ));

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let mut diagnostics = ReasoningDiagnostics::default();

    let mut ctx = RuleContext {
        delta: &delta,
        derived: &derived,
        new_delta: &mut new_delta,
        same_as: &same_as,
        rdf_type_sid: &rdf_type_sid,
        t: 1,
        diagnostics: &mut diagnostics,
    };

    apply_union_rule(&index, &mut ctx);

    // Should derive type(1, U) because x has type C1 which is a member
    assert_eq!(new_delta.len(), 1);
    let derived_flake = new_delta.iter().next().unwrap();
    assert_eq!(derived_flake.s, sid(1));
    if let FlakeValue::Ref(cls) = &derived_flake.o {
        assert_eq!(*cls, sid(100));
    } else {
        panic!("Expected Ref object");
    }

    assert_eq!(diagnostics.rules_fired.get("cls-uni"), Some(&1));
}

// ============================================================================
// OneOf Rule Tests
// ============================================================================

#[test]
fn test_one_of_rule() {
    // Create a oneOf restriction: C is oneOf [i1, i2, i3]
    let mut index = RestrictionIndex::new();
    index.add_restriction_for_test(ParsedRestriction {
        restriction_id: sid(100),
        restriction_type: RestrictionType::OneOf {
            individuals: vec![sid(1), sid(2), sid(3)],
        },
    });

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    // Empty delta - oneOf rule fires for all listed individuals
    let delta = DeltaSet::new();
    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let mut diagnostics = ReasoningDiagnostics::default();

    let mut ctx = RuleContext {
        delta: &delta,
        derived: &derived,
        new_delta: &mut new_delta,
        same_as: &same_as,
        rdf_type_sid: &rdf_type_sid,
        t: 1,
        diagnostics: &mut diagnostics,
    };

    apply_one_of_rule(&index, &mut ctx);

    // Should derive type(i, C) for each individual
    assert_eq!(new_delta.len(), 3);

    let subjects: HashSet<Sid> = new_delta.iter().map(|f| f.s.clone()).collect();
    assert!(subjects.contains(&sid(1)));
    assert!(subjects.contains(&sid(2)));
    assert!(subjects.contains(&sid(3)));

    assert_eq!(diagnostics.rules_fired.get("cls-oo"), Some(&3));
}
