//! Tests for property rules (prp-*).
//!
//! This module tests:
//! - Symmetric property rule (prp-symp)
//! - Transitive property rule (prp-trp)
//! - Inverse property rule (prp-inv)
//! - Domain rule (prp-dom)
//! - Range rule (prp-rng)
//! - SubPropertyOf rule (prp-spo1)
//! - Property chain rule (prp-spo2)
//! - Functional property rule (prp-fp)
//! - Inverse functional property rule (prp-ifp)
//! - HasKey rule (prp-key)

use super::*;
use crate::same_as::SameAsTracker;
use crate::types::{ChainElement, PropertyChain};
use fluree_vocab::namespaces::{OWL, RDF};
use fluree_vocab::predicates::RDF_TYPE;

#[test]
fn test_symmetric_rule() {
    let mut symmetric = HashSet::new();
    symmetric.insert(sid(10)); // predicate 10 is symmetric

    let ontology = OntologyRL::new(symmetric, HashSet::new(), HashMap::new(), 1);

    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(1, 10, 2, 1)); // P(1, 2)

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
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
    apply_symmetric_rule(&ontology, &mut ctx);

    // Should derive P(2, 1)
    assert_eq!(new_delta.len(), 1);
    let derived_flake = new_delta.iter().next().unwrap();
    assert_eq!(derived_flake.s, sid(2));
    assert_eq!(derived_flake.p, sid(10));
    if let FlakeValue::Ref(o) = &derived_flake.o {
        assert_eq!(*o, sid(1));
    } else {
        panic!("Expected Ref object");
    }
}

#[test]
fn test_transitive_rule() {
    let mut transitive = HashSet::new();
    transitive.insert(sid(10)); // predicate 10 is transitive

    let ontology = OntologyRL::new(HashSet::new(), transitive, HashMap::new(), 1);

    // Setup: derived has P(2, 3)
    let mut derived = DerivedSet::new();
    derived.try_add(make_ref_flake(2, 10, 3, 1));

    // Delta has P(1, 2)
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(1, 10, 2, 1));

    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
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
    apply_transitive_rule(&ontology, &mut ctx);

    // Should derive P(1, 3) from P(1,2) ⋈ P(2,3)
    assert_eq!(new_delta.len(), 1);
    let derived_flake = new_delta.iter().next().unwrap();
    assert_eq!(derived_flake.s, sid(1));
    assert_eq!(derived_flake.p, sid(10));
    if let FlakeValue::Ref(o) = &derived_flake.o {
        assert_eq!(*o, sid(3));
    } else {
        panic!("Expected Ref object");
    }
}

#[test]
fn test_domain_rule() {
    // Create ontology with domain declaration: property 10 has domain class 100
    let mut domain: HashMap<Sid, Vec<Sid>> = HashMap::new();
    domain.insert(sid(10), vec![sid(100)]);

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        domain,
        HashMap::new(),
        HashMap::new(), // super_properties
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        HashSet::new(), // inverse_functional_properties
        HashMap::new(), // has_keys
        1,
    );

    // Delta has P(1, 2) where P=10
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(1, 10, 2, 1));

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
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
    apply_domain_rule(&ontology, &mut ctx);

    // Should derive rdf:type(1, 100)
    assert_eq!(new_delta.len(), 1);
    let type_flake = new_delta.iter().next().unwrap();
    assert_eq!(type_flake.s, sid(1)); // subject of original fact
    assert_eq!(type_flake.p.namespace_code, RDF);
    assert_eq!(type_flake.p.name.as_ref(), RDF_TYPE);
    assert_eq!(type_flake.dt, Sid::new(1, "id")); // rdf:type object is a Ref → dt must be $id
    if let FlakeValue::Ref(class) = &type_flake.o {
        assert_eq!(*class, sid(100)); // domain class
    } else {
        panic!("Expected Ref object");
    }
}

#[test]
fn test_domain_rule_sets_dt_id_even_for_literal_objects() {
    // Create ontology with domain declaration: property 10 has domain class 100
    let mut domain: HashMap<Sid, Vec<Sid>> = HashMap::new();
    domain.insert(sid(10), vec![sid(100)]);

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        domain,
        HashMap::new(),
        HashMap::new(), // super_properties
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        HashSet::new(), // inverse_functional_properties
        HashMap::new(), // has_keys
        1,
    );

    // Delta has P(1, "hello") - domain inference should still type the subject
    let mut delta = DeltaSet::new();
    delta.push(Flake::new(
        sid(1),
        sid(10),
        FlakeValue::String("hello".into()),
        sid(0), // dt for the *source* flake is irrelevant to derived rdf:type dt
        1,
        true,
        None,
    ));

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
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
    apply_domain_rule(&ontology, &mut ctx);

    assert_eq!(new_delta.len(), 1);
    let type_flake = new_delta.iter().next().unwrap();
    assert_eq!(type_flake.p.namespace_code, RDF);
    assert_eq!(type_flake.p.name.as_ref(), RDF_TYPE);
    assert_eq!(type_flake.dt, Sid::new(1, "id")); // must be $id since object is Ref
    assert!(matches!(type_flake.o, FlakeValue::Ref(_)));
}

#[test]
fn test_range_rule() {
    // Create ontology with range declaration: property 10 has range class 200
    let mut range: HashMap<Sid, Vec<Sid>> = HashMap::new();
    range.insert(sid(10), vec![sid(200)]);

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        range,
        HashMap::new(), // super_properties
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        HashSet::new(), // inverse_functional_properties
        HashMap::new(), // has_keys
        1,
    );

    // Delta has P(1, 2) where P=10
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(1, 10, 2, 1));

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
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
    apply_range_rule(&ontology, &mut ctx);

    // Should derive rdf:type(2, 200) - object gets typed
    assert_eq!(new_delta.len(), 1);
    let type_flake = new_delta.iter().next().unwrap();
    assert_eq!(type_flake.s, sid(2)); // object of original fact
    assert_eq!(type_flake.p.namespace_code, RDF);
    assert_eq!(type_flake.p.name.as_ref(), RDF_TYPE);
    if let FlakeValue::Ref(class) = &type_flake.o {
        assert_eq!(*class, sid(200)); // range class
    } else {
        panic!("Expected Ref object");
    }
}

#[test]
fn test_range_rule_ignores_literals() {
    // Create ontology with range declaration: property 10 has range class 200
    let mut range: HashMap<Sid, Vec<Sid>> = HashMap::new();
    range.insert(sid(10), vec![sid(200)]);

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        range,
        HashMap::new(), // super_properties
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        HashSet::new(), // inverse_functional_properties
        HashMap::new(), // has_keys
        1,
    );

    // Delta has P(1, "hello") - literal object
    let mut delta = DeltaSet::new();
    delta.push(Flake::new(
        sid(1),
        sid(10),
        FlakeValue::String("hello".into()),
        sid(0),
        1,
        true,
        None,
    ));

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
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
    apply_range_rule(&ontology, &mut ctx);

    // Should NOT derive anything - literals don't get rdf:type
    assert_eq!(new_delta.len(), 0);
}

#[test]
fn test_sub_property_rule() {
    // Create ontology with sub-property relationship:
    // property 10 is a subPropertyOf property 20
    let mut super_properties: HashMap<Sid, Vec<Sid>> = HashMap::new();
    super_properties.insert(sid(10), vec![sid(20)]);

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        super_properties,
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        HashSet::new(), // inverse_functional_properties
        HashMap::new(), // has_keys
        1,
    );

    // Delta has P1(1, 2) where P1=10
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(1, 10, 2, 1));

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
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
    apply_sub_property_rule(&ontology, &mut ctx);

    // Should derive P2(1, 2) where P2=20
    assert_eq!(new_delta.len(), 1);
    let derived_flake = new_delta.iter().next().unwrap();
    assert_eq!(derived_flake.s, sid(1)); // same subject
    assert_eq!(derived_flake.p, sid(20)); // super-property
    if let FlakeValue::Ref(obj) = &derived_flake.o {
        assert_eq!(*obj, sid(2)); // same object
    } else {
        panic!("Expected Ref object");
    }
}

#[test]
fn test_sub_property_rule_transitive_chain() {
    // Create ontology with transitive sub-property chain:
    // property 10 subPropertyOf 20 subPropertyOf 30
    // So 10 has super-properties [20, 30]
    let mut super_properties: HashMap<Sid, Vec<Sid>> = HashMap::new();
    super_properties.insert(sid(10), vec![sid(20), sid(30)]);

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        super_properties,
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        HashSet::new(), // inverse_functional_properties
        HashMap::new(), // has_keys
        1,
    );

    // Delta has P1(1, 2) where P1=10
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(1, 10, 2, 1));

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
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
    apply_sub_property_rule(&ontology, &mut ctx);

    // Should derive both P2(1, 2) and P3(1, 2)
    assert_eq!(new_delta.len(), 2);

    let predicates: Vec<Sid> = new_delta.iter().map(|f| f.p.clone()).collect();
    assert!(predicates.contains(&sid(20)));
    assert!(predicates.contains(&sid(30)));
}

#[test]
fn test_property_chain_rule() {
    // Create ontology with property chain:
    // Property 30 (derived) = chain of P1=10, P2=20
    // P1(u0, u1), P2(u1, u2) → P30(u0, u2)
    let property_chains = vec![PropertyChain::new(
        sid(30),
        vec![ChainElement::direct(sid(10)), ChainElement::direct(sid(20))],
    )];

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_properties
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        property_chains,
        HashSet::new(), // functional_properties
        HashSet::new(), // inverse_functional_properties
        HashMap::new(), // has_keys
        1,
    );

    // Delta has P1(1, 2) and P2(2, 3)
    // So chain should derive P30(1, 3)
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(1, 10, 2, 1)); // P1(1, 2)
    delta.push(make_ref_flake(2, 20, 3, 1)); // P2(2, 3)

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
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
    apply_property_chain_rule(&ontology, &mut ctx);

    // Should derive P30(1, 3)
    assert_eq!(new_delta.len(), 1);
    let chain_flake = new_delta.iter().next().unwrap();
    assert_eq!(chain_flake.s, sid(1)); // u0 from P1
    assert_eq!(chain_flake.p, sid(30)); // derived property
    if let FlakeValue::Ref(obj) = &chain_flake.o {
        assert_eq!(*obj, sid(3)); // u2 from P2
    } else {
        panic!("Expected Ref object");
    }
}

#[test]
fn test_property_chain_rule_with_derived() {
    // Test that chains work when one part is in derived (already computed)
    let property_chains = vec![PropertyChain::new(
        sid(30),
        vec![ChainElement::direct(sid(10)), ChainElement::direct(sid(20))],
    )];

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_properties
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        property_chains,
        HashSet::new(), // functional_properties
        HashSet::new(), // inverse_functional_properties
        HashMap::new(), // has_keys
        1,
    );

    // P1(1, 2) is in derived (already computed)
    let mut derived = DerivedSet::new();
    derived.try_add(make_ref_flake(1, 10, 2, 1)); // P1(1, 2)

    // P2(2, 3) is new in delta
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(2, 20, 3, 1)); // P2(2, 3)

    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
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
    apply_property_chain_rule(&ontology, &mut ctx);

    // Should derive P30(1, 3)
    assert_eq!(new_delta.len(), 1);
    let chain_flake = new_delta.iter().next().unwrap();
    assert_eq!(chain_flake.s, sid(1));
    assert_eq!(chain_flake.p, sid(30));
    if let FlakeValue::Ref(obj) = &chain_flake.o {
        assert_eq!(*obj, sid(3));
    } else {
        panic!("Expected Ref object");
    }
}

#[test]
fn test_property_chain_rule_with_inverse() {
    // Create ontology with property chain containing inverse:
    // hasSibling = hasParent o hasParent^-1 (inverse)
    // This means: hasParent(X, Y), hasParent(Z, Y) → hasSibling(X, Z)
    // (people with the same parent are siblings)
    //
    // Property IDs: 10=hasParent, 30=hasSibling
    let property_chains = vec![PropertyChain::new(
        sid(30), // hasSibling
        vec![
            ChainElement::direct(sid(10)),  // hasParent
            ChainElement::inverse(sid(10)), // hasParent^-1
        ],
    )];

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        property_chains,
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        1,
    );

    // Alice(1) hasParent Parent(2)
    // Bob(3) hasParent Parent(2)
    // Should derive: Alice hasSibling Bob and Bob hasSibling Alice
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(1, 10, 2, 1)); // hasParent(Alice, Parent)
    delta.push(make_ref_flake(3, 10, 2, 1)); // hasParent(Bob, Parent)

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
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
    apply_property_chain_rule(&ontology, &mut ctx);

    // Should derive hasSibling(1, 3) and hasSibling(3, 1)
    // Plus self-sibling (Alice sibling Alice, Bob sibling Bob) - 4 total
    // But we might get duplicates filtered by derived check...
    // The important thing is we get the cross-sibling relationships
    let sibling_facts: Vec<_> = new_delta.iter().filter(|f| f.p == sid(30)).collect();

    // Check we have the cross-sibling relationship (1 -> 3 or 3 -> 1)
    let has_cross_sibling = sibling_facts.iter().any(|f| {
        (f.s == sid(1) && matches!(&f.o, FlakeValue::Ref(o) if *o == sid(3)))
            || (f.s == sid(3) && matches!(&f.o, FlakeValue::Ref(o) if *o == sid(1)))
    });
    assert!(
        has_cross_sibling,
        "Expected hasSibling relationship between 1 and 3"
    );
}

#[test]
fn test_property_chain_rule_inverse_last_requires_forward_inverse_lookup() {
    // Chain with an inverse element at the LAST position:
    // P30 = P10 o P20^-1
    //
    // Semantics:
    //   P10(u0, u1) and P20(u2, u1)  =>  P30(u0, u2)
    //
    // This test forces the inverse-last lookup to happen via *forward extension* by:
    // - Putting P10 in delta
    // - Putting P20 in derived (so seeding from the inverse element is NOT possible)
    let property_chains = vec![PropertyChain::new(
        sid(30),
        vec![
            ChainElement::direct(sid(10)),
            ChainElement::inverse(sid(20)),
        ],
    )];

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        property_chains,
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        1,
    );

    // Delta: P10(1, 2)
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(1, 10, 2, 1));

    // Derived: P20(3, 2)  (note: subject=3, object=2)
    let mut derived = DerivedSet::new();
    derived.try_add(make_ref_flake(3, 20, 2, 1));

    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
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
    apply_property_chain_rule(&ontology, &mut ctx);

    // Expect: P30(1, 3)
    let got = new_delta.iter().any(|f| {
        f.p == sid(30) && f.s == sid(1) && matches!(&f.o, FlakeValue::Ref(o) if *o == sid(3))
    });
    assert!(got, "Expected derived chain fact P30(1,3)");
}

#[test]
fn test_property_chain_rule_inverse_first_requires_backward_inverse_lookup() {
    // Chain with an inverse element at the FIRST position:
    // P30 = P10^-1 o P20
    //
    // Semantics:
    //   P10(u1, u0) and P20(u1, u2)  =>  P30(u0, u2)
    //
    // This test forces the inverse-first lookup to happen via *backward extension* by:
    // - Putting P20 in delta
    // - Putting P10 in derived (so the inverse-first step must consult derived)
    let property_chains = vec![PropertyChain::new(
        sid(30),
        vec![
            ChainElement::inverse(sid(10)),
            ChainElement::direct(sid(20)),
        ],
    )];

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        property_chains,
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        1,
    );

    // Delta: P20(2, 3)
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(2, 20, 3, 1));

    // Derived: P10(2, 1)
    let mut derived = DerivedSet::new();
    derived.try_add(make_ref_flake(2, 10, 1, 1));

    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
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
    apply_property_chain_rule(&ontology, &mut ctx);

    // Expect: P30(1, 3)
    let got = new_delta.iter().any(|f| {
        f.p == sid(30) && f.s == sid(1) && matches!(&f.o, FlakeValue::Ref(o) if *o == sid(3))
    });
    assert!(got, "Expected derived chain fact P30(1,3)");
}

#[test]
fn test_property_chain_rule_length_3() {
    // Create ontology with 3-element chain:
    // hasGreatGrandparent = hasParent o hasParent o hasParent
    // Property IDs: 10=hasParent, 30=hasGreatGrandparent
    let property_chains = vec![PropertyChain::new(
        sid(30), // hasGreatGrandparent
        vec![
            ChainElement::direct(sid(10)),
            ChainElement::direct(sid(10)),
            ChainElement::direct(sid(10)),
        ],
    )];

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        property_chains,
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        1,
    );

    // Person(1) -> Parent(2) -> Grandparent(3) -> GreatGrandparent(4)
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(1, 10, 2, 1)); // hasParent(1, 2)
    delta.push(make_ref_flake(2, 10, 3, 1)); // hasParent(2, 3)
    delta.push(make_ref_flake(3, 10, 4, 1)); // hasParent(3, 4)

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
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
    apply_property_chain_rule(&ontology, &mut ctx);

    // Should derive hasGreatGrandparent(1, 4)
    let chain_facts: Vec<_> = new_delta.iter().filter(|f| f.p == sid(30)).collect();

    assert_eq!(chain_facts.len(), 1);
    let fact = chain_facts[0];
    assert_eq!(fact.s, sid(1));
    if let FlakeValue::Ref(obj) = &fact.o {
        assert_eq!(*obj, sid(4));
    } else {
        panic!("Expected Ref object");
    }
}

#[test]
fn test_functional_property_rule() {
    // Create ontology with functional property: property 10 is functional
    let mut functional_properties: HashSet<Sid> = HashSet::new();
    functional_properties.insert(sid(10));

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_properties
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        functional_properties,
        HashSet::new(), // inverse_functional_properties
        HashMap::new(), // has_keys
        1,
    );

    // Delta has P(1, 2) and P(1, 3) - same subject, different objects
    // Since P is functional, objects 2 and 3 must be sameAs
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(1, 10, 2, 1)); // P(1, 2)
    delta.push(make_ref_flake(1, 10, 3, 1)); // P(1, 3)

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let owl_same_as_sid = owl::same_as_sid();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
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
    apply_functional_property_rule(&ontology, &mut ctx);

    // Should derive owl:sameAs(2, 3) or owl:sameAs(3, 2)
    assert_eq!(new_delta.len(), 1);
    let same_as_flake = new_delta.iter().next().unwrap();
    assert_eq!(same_as_flake.p.namespace_code, OWL);
    assert_eq!(same_as_flake.p.name.as_ref(), "sameAs");

    // The subjects should be 2 or 3 (one of the objects from original facts)
    let s_is_2_or_3 = same_as_flake.s == sid(2) || same_as_flake.s == sid(3);
    assert!(s_is_2_or_3);

    if let FlakeValue::Ref(o) = &same_as_flake.o {
        let o_is_2_or_3 = *o == sid(2) || *o == sid(3);
        assert!(o_is_2_or_3);
        // Subject and object should be different
        assert_ne!(same_as_flake.s, *o);
    } else {
        panic!("Expected Ref object");
    }
}

#[test]
fn test_functional_property_rule_with_derived() {
    // Test that functional property rule finds conflicts between delta and derived
    let mut functional_properties: HashSet<Sid> = HashSet::new();
    functional_properties.insert(sid(10));

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        functional_properties,
        HashSet::new(),
        HashMap::new(), // has_keys
        1,
    );

    // P(1, 2) is in derived (already computed)
    let mut derived = DerivedSet::new();
    derived.try_add(make_ref_flake(1, 10, 2, 1)); // P(1, 2)

    // P(1, 3) is new in delta
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(1, 10, 3, 1)); // P(1, 3)

    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let owl_same_as_sid = owl::same_as_sid();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
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
    apply_functional_property_rule(&ontology, &mut ctx);

    // Should derive owl:sameAs between 2 and 3
    assert_eq!(new_delta.len(), 1);
    let same_as_flake = new_delta.iter().next().unwrap();
    assert_eq!(same_as_flake.p.namespace_code, OWL);
    assert_eq!(same_as_flake.p.name.as_ref(), "sameAs");
}

#[test]
fn test_functional_property_rule_no_conflict() {
    // Test that no sameAs is derived when there's no conflict
    let mut functional_properties: HashSet<Sid> = HashSet::new();
    functional_properties.insert(sid(10));

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        functional_properties,
        HashSet::new(),
        HashMap::new(), // has_keys
        1,
    );

    // Delta has P(1, 2) and P(2, 3) - different subjects, no conflict
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(1, 10, 2, 1)); // P(1, 2)
    delta.push(make_ref_flake(2, 10, 3, 1)); // P(2, 3)

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let owl_same_as_sid = owl::same_as_sid();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
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
    apply_functional_property_rule(&ontology, &mut ctx);

    // No sameAs should be derived
    assert_eq!(new_delta.len(), 0);
}

#[test]
fn test_inverse_functional_property_rule() {
    // Create ontology with inverse-functional property: property 10 is inverse-functional
    let mut inverse_functional_properties: HashSet<Sid> = HashSet::new();
    inverse_functional_properties.insert(sid(10));

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        inverse_functional_properties,
        HashMap::new(), // has_keys
        1,
    );

    // Delta has P(1, 3) and P(2, 3) - same object, different subjects
    // Since P is inverse-functional, subjects 1 and 2 must be sameAs
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(1, 10, 3, 1)); // P(1, 3)
    delta.push(make_ref_flake(2, 10, 3, 1)); // P(2, 3)

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let owl_same_as_sid = owl::same_as_sid();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
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
    apply_inverse_functional_property_rule(&ontology, &mut ctx);

    // Should derive owl:sameAs(1, 2) or owl:sameAs(2, 1)
    assert_eq!(new_delta.len(), 1);
    let same_as_flake = new_delta.iter().next().unwrap();
    assert_eq!(same_as_flake.p.namespace_code, OWL);
    assert_eq!(same_as_flake.p.name.as_ref(), "sameAs");

    // The subjects should be 1 or 2 (one of the subjects from original facts)
    let s_is_1_or_2 = same_as_flake.s == sid(1) || same_as_flake.s == sid(2);
    assert!(s_is_1_or_2);

    if let FlakeValue::Ref(o) = &same_as_flake.o {
        let o_is_1_or_2 = *o == sid(1) || *o == sid(2);
        assert!(o_is_1_or_2);
        // Subject and object should be different
        assert_ne!(same_as_flake.s, *o);
    } else {
        panic!("Expected Ref object");
    }
}

#[test]
fn test_inverse_functional_property_rule_with_derived() {
    // Test that inverse-functional property rule finds conflicts between delta and derived
    let mut inverse_functional_properties: HashSet<Sid> = HashSet::new();
    inverse_functional_properties.insert(sid(10));

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        inverse_functional_properties,
        HashMap::new(), // has_keys
        1,
    );

    // P(1, 3) is in derived (already computed)
    let mut derived = DerivedSet::new();
    derived.try_add(make_ref_flake(1, 10, 3, 1)); // P(1, 3)

    // P(2, 3) is new in delta
    let mut delta = DeltaSet::new();
    delta.push(make_ref_flake(2, 10, 3, 1)); // P(2, 3)

    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let owl_same_as_sid = owl::same_as_sid();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
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
    apply_inverse_functional_property_rule(&ontology, &mut ctx);

    // Should derive owl:sameAs between 1 and 2
    assert_eq!(new_delta.len(), 1);
    let same_as_flake = new_delta.iter().next().unwrap();
    assert_eq!(same_as_flake.p.namespace_code, OWL);
    assert_eq!(same_as_flake.p.name.as_ref(), "sameAs");
}

#[test]
fn test_inverse_functional_property_triggered_by_sameas() {
    // Test the critical scenario: sameAs merges objects, creating an ifp conflict
    // even though no new P facts arrived
    //
    // Setup: P is inverse-functional
    // Derived has: P(x1, a) and P(x2, b)
    // New sameAs(a, b) arrives - after merge, both facts have canonical object = canonical(a) = canonical(b)
    // This should trigger ifp: sameAs(x1, x2)

    let mut inverse_functional_properties: HashSet<Sid> = HashSet::new();
    inverse_functional_properties.insert(sid(10)); // P is inverse-functional

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        inverse_functional_properties,
        HashMap::new(), // has_keys
        1,
    );

    // P(x1, a) = P(1, 3) and P(x2, b) = P(2, 4) already in derived
    let mut derived = DerivedSet::new();
    derived.try_add(make_ref_flake(1, 10, 3, 1)); // P(x1=1, a=3)
    derived.try_add(make_ref_flake(2, 10, 4, 1)); // P(x2=2, b=4)

    // Delta is empty for P facts (no new P arrived)
    let delta = DeltaSet::new();

    // But sameAs changed: a ≡ b (3 ≡ 4)
    let mut same_as = SameAsTracker::new();
    same_as.union(&sid(3), &sid(4)); // Merge objects a=3 and b=4

    let mut new_delta = DeltaSet::new();
    let owl_same_as_sid = owl::same_as_sid();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
    let mut diagnostics = ReasoningDiagnostics::default();

    // Call with same_as_changed = true
    let mut ctx = IdentityRuleContext {
        delta: &delta,
        derived: &derived,
        new_delta: &mut new_delta,
        same_as: &same_as,
        owl_same_as_sid: &owl_same_as_sid,
        rdf_type_sid: &rdf_type_sid,
        t: 1,
        same_as_changed: true,
        diagnostics: &mut diagnostics,
    };
    apply_inverse_functional_property_rule(&ontology, &mut ctx);

    // Should derive sameAs(x1, x2) = sameAs(1, 2) because after canonicalization,
    // both P(1, 3) and P(2, 4) have the same object (canonical of 3 == canonical of 4)
    assert_eq!(
        new_delta.len(),
        1,
        "Should derive sameAs(x1, x2) when sameAs merges objects"
    );
    let same_as_flake = new_delta.iter().next().unwrap();
    assert_eq!(same_as_flake.p.namespace_code, OWL);
    assert_eq!(same_as_flake.p.name.as_ref(), "sameAs");

    // Verify it's sameAs between subjects 1 and 2
    let involves_1_and_2 = (same_as_flake.s == sid(1) || same_as_flake.s == sid(2))
        && matches!(&same_as_flake.o, FlakeValue::Ref(o) if *o == sid(1) || *o == sid(2));
    assert!(
        involves_1_and_2,
        "sameAs should be between subjects 1 and 2"
    );
}

#[test]
fn test_functional_property_triggered_by_sameas() {
    // Test the critical scenario: sameAs merges subjects, creating an fp conflict
    // even though no new P facts arrived
    //
    // Setup: P is functional
    // Derived has: P(a, y1) and P(b, y2)
    // New sameAs(a, b) arrives - after merge, both facts have canonical subject = canonical(a) = canonical(b)
    // This should trigger fp: sameAs(y1, y2)

    let mut functional_properties: HashSet<Sid> = HashSet::new();
    functional_properties.insert(sid(10)); // P is functional

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        functional_properties,
        HashSet::new(),
        HashMap::new(), // has_keys
        1,
    );

    // P(a, y1) = P(1, 3) and P(b, y2) = P(2, 4) already in derived
    let mut derived = DerivedSet::new();
    derived.try_add(make_ref_flake(1, 10, 3, 1)); // P(a=1, y1=3)
    derived.try_add(make_ref_flake(2, 10, 4, 1)); // P(b=2, y2=4)

    // Delta is empty for P facts (no new P arrived)
    let delta = DeltaSet::new();

    // But sameAs changed: a ≡ b (1 ≡ 2)
    let mut same_as = SameAsTracker::new();
    same_as.union(&sid(1), &sid(2)); // Merge subjects a=1 and b=2

    let mut new_delta = DeltaSet::new();
    let owl_same_as_sid = owl::same_as_sid();
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);
    let mut diagnostics = ReasoningDiagnostics::default();

    // Call with same_as_changed = true
    let mut ctx = IdentityRuleContext {
        delta: &delta,
        derived: &derived,
        new_delta: &mut new_delta,
        same_as: &same_as,
        owl_same_as_sid: &owl_same_as_sid,
        rdf_type_sid: &rdf_type_sid,
        t: 1,
        same_as_changed: true,
        diagnostics: &mut diagnostics,
    };
    apply_functional_property_rule(&ontology, &mut ctx);

    // Should derive sameAs(y1, y2) = sameAs(3, 4) because after canonicalization,
    // both P(1, 3) and P(2, 4) have the same subject (canonical of 1 == canonical of 2)
    assert_eq!(
        new_delta.len(),
        1,
        "Should derive sameAs(y1, y2) when sameAs merges subjects"
    );
    let same_as_flake = new_delta.iter().next().unwrap();
    assert_eq!(same_as_flake.p.namespace_code, OWL);
    assert_eq!(same_as_flake.p.name.as_ref(), "sameAs");

    // Verify it's sameAs between objects 3 and 4
    let involves_3_and_4 = (same_as_flake.s == sid(3) || same_as_flake.s == sid(4))
        && matches!(&same_as_flake.o, FlakeValue::Ref(o) if *o == sid(3) || *o == sid(4));
    assert!(involves_3_and_4, "sameAs should be between objects 3 and 4");
}

#[test]
fn test_has_key_rule() {
    // hasKey(Class=100, [KeyProp=20])
    // Two instances (1 and 2) of class 100 with same key value -> sameAs(1, 2)
    //
    // Setup:
    // - Class 100 has hasKey [property 20]
    // - Instance 1: type(1, 100), prop(1, 20, keyValue=50)
    // - Instance 2: type(2, 100), prop(2, 20, keyValue=50)
    // Expected: sameAs(1, 2)

    // Create ontology with hasKey declaration
    let mut has_keys: HashMap<Sid, Vec<Vec<Sid>>> = HashMap::new();
    has_keys.insert(sid(100), vec![vec![sid(20)]]); // Class 100 has key [property 20]

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_properties
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        HashSet::new(), // inverse_functional_properties
        has_keys,
        1,
    );

    // Create rdf:type SID
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    // Delta has:
    // - type(1, 100), type(2, 100)
    // - prop(1, 20, 50), prop(2, 20, 50)  -- same key value 50
    let mut delta = DeltaSet::new();

    // Type assertions
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(100)),
        sid(0),
        1,
        true,
        None,
    ));
    delta.push(Flake::new(
        sid(2),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(100)),
        sid(0),
        1,
        true,
        None,
    ));

    // Key property assertions (both have same value 50)
    delta.push(make_ref_flake(1, 20, 50, 1)); // prop(1, 20, 50)
    delta.push(make_ref_flake(2, 20, 50, 1)); // prop(2, 20, 50)

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let owl_same_as_sid = owl::same_as_sid();
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
    apply_has_key_rule(&ontology, &mut ctx);

    // Should derive sameAs(1, 2) because both instances have same key value
    assert_eq!(new_delta.len(), 1, "Should derive one sameAs fact");

    let same_as_flake = new_delta.iter().next().unwrap();
    assert_eq!(same_as_flake.p.namespace_code, OWL);
    assert_eq!(same_as_flake.p.name.as_ref(), "sameAs");

    // Verify it's between instances 1 and 2
    let involves_1_and_2 = (same_as_flake.s == sid(1) || same_as_flake.s == sid(2))
        && matches!(&same_as_flake.o, FlakeValue::Ref(o) if *o == sid(1) || *o == sid(2));
    assert!(
        involves_1_and_2,
        "sameAs should be between instances 1 and 2"
    );

    assert!(diagnostics.rules_fired.get("prp-key").is_some());
}

#[test]
fn test_has_key_rule_no_match() {
    // hasKey(Class=100, [KeyProp=20])
    // Two instances with DIFFERENT key values -> no sameAs

    let mut has_keys: HashMap<Sid, Vec<Vec<Sid>>> = HashMap::new();
    has_keys.insert(sid(100), vec![vec![sid(20)]]);

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_properties
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        HashSet::new(), // inverse_functional_properties
        has_keys,
        1,
    );

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    let mut delta = DeltaSet::new();

    // Type assertions
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(100)),
        sid(0),
        1,
        true,
        None,
    ));
    delta.push(Flake::new(
        sid(2),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(100)),
        sid(0),
        1,
        true,
        None,
    ));

    // Key property assertions (DIFFERENT values: 50 vs 60)
    delta.push(make_ref_flake(1, 20, 50, 1)); // prop(1, 20, 50)
    delta.push(make_ref_flake(2, 20, 60, 1)); // prop(2, 20, 60) - DIFFERENT!

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let owl_same_as_sid = owl::same_as_sid();
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
    apply_has_key_rule(&ontology, &mut ctx);

    // Should derive nothing because key values differ
    assert_eq!(
        new_delta.len(),
        0,
        "Should not derive sameAs when key values differ"
    );
}

#[test]
fn test_has_key_rule_missing_key_property() {
    // hasKey(Class=100, [KeyProp=20])
    // One instance missing the key property -> no sameAs

    let mut has_keys: HashMap<Sid, Vec<Vec<Sid>>> = HashMap::new();
    has_keys.insert(sid(100), vec![vec![sid(20)]]);

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_properties
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        HashSet::new(), // inverse_functional_properties
        has_keys,
        1,
    );

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    let mut delta = DeltaSet::new();

    // Type assertions
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(100)),
        sid(0),
        1,
        true,
        None,
    ));
    delta.push(Flake::new(
        sid(2),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(100)),
        sid(0),
        1,
        true,
        None,
    ));

    // Only instance 1 has the key property
    delta.push(make_ref_flake(1, 20, 50, 1)); // prop(1, 20, 50)
                                              // Instance 2 is MISSING the key property

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let owl_same_as_sid = owl::same_as_sid();
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
    apply_has_key_rule(&ontology, &mut ctx);

    // Should derive nothing because instance 2 is missing the required key property
    assert_eq!(
        new_delta.len(),
        0,
        "Should not derive sameAs when key property is missing"
    );
}

#[test]
fn test_has_key_rule_multi_key() {
    // hasKey(Class=100, [KeyProp1=20, KeyProp2=21])
    // Two instances with same values for BOTH key properties -> sameAs

    let mut has_keys: HashMap<Sid, Vec<Vec<Sid>>> = HashMap::new();
    has_keys.insert(sid(100), vec![vec![sid(20), sid(21)]]); // Composite key

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_properties
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        HashSet::new(), // inverse_functional_properties
        has_keys,
        1,
    );

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    let mut delta = DeltaSet::new();

    // Type assertions
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(100)),
        sid(0),
        1,
        true,
        None,
    ));
    delta.push(Flake::new(
        sid(2),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(100)),
        sid(0),
        1,
        true,
        None,
    ));

    // Both instances have same values for both key properties
    delta.push(make_ref_flake(1, 20, 50, 1)); // prop(1, 20, 50)
    delta.push(make_ref_flake(1, 21, 60, 1)); // prop(1, 21, 60)
    delta.push(make_ref_flake(2, 20, 50, 1)); // prop(2, 20, 50)
    delta.push(make_ref_flake(2, 21, 60, 1)); // prop(2, 21, 60)

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let owl_same_as_sid = owl::same_as_sid();
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
    apply_has_key_rule(&ontology, &mut ctx);

    // Should derive sameAs(1, 2) because both composite key values match
    assert_eq!(
        new_delta.len(),
        1,
        "Should derive one sameAs fact for composite key match"
    );

    let same_as_flake = new_delta.iter().next().unwrap();
    let involves_1_and_2 = (same_as_flake.s == sid(1) || same_as_flake.s == sid(2))
        && matches!(&same_as_flake.o, FlakeValue::Ref(o) if *o == sid(1) || *o == sid(2));
    assert!(
        involves_1_and_2,
        "sameAs should be between instances 1 and 2"
    );
}

#[test]
fn test_has_key_rule_multi_valued_key_skipped() {
    // hasKey(Class=100, [KeyProp=20])
    // Instance 1 has MULTIPLE values for the key property -> should be skipped

    let mut has_keys: HashMap<Sid, Vec<Vec<Sid>>> = HashMap::new();
    has_keys.insert(sid(100), vec![vec![sid(20)]]);

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_properties
        HashMap::new(), // super_classes
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        HashSet::new(), // inverse_functional_properties
        has_keys,
        1,
    );

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    let mut delta = DeltaSet::new();

    // Type assertions
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(100)),
        sid(0),
        1,
        true,
        None,
    ));
    delta.push(Flake::new(
        sid(2),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(100)),
        sid(0),
        1,
        true,
        None,
    ));

    // Instance 1 has TWO values for key property (ambiguous!)
    delta.push(make_ref_flake(1, 20, 50, 1)); // prop(1, 20, 50)
    delta.push(make_ref_flake(1, 20, 51, 1)); // prop(1, 20, 51) - SECOND VALUE!

    // Instance 2 has single value
    delta.push(make_ref_flake(2, 20, 50, 1)); // prop(2, 20, 50)

    let derived = DerivedSet::new();
    let mut new_delta = DeltaSet::new();
    let same_as = SameAsTracker::new();
    let owl_same_as_sid = owl::same_as_sid();
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
    apply_has_key_rule(&ontology, &mut ctx);

    // Should derive nothing because instance 1 has multiple key values (ambiguous)
    // and instance 2 is the only valid instance (can't match with itself)
    assert_eq!(
        new_delta.len(),
        0,
        "Should not derive sameAs when instance has multi-valued key"
    );
}
