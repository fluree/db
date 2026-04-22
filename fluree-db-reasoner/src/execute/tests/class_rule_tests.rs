//! Tests for class hierarchy rules (cax-*).
//!
//! This module tests:
//! - SubClassOf rule (cax-sco)
//! - EquivalentClass rule (cax-eqc)

use super::*;
use crate::same_as::SameAsTracker;
use fluree_vocab::namespaces::RDF;
use fluree_vocab::predicates::RDF_TYPE;

#[test]
fn test_subclass_rule() {
    // Test cax-sco: type(x, C1), subClassOf(C1, C2) → type(x, C2)
    // Class 10 (Dog) is a subclass of class 20 (Animal)

    // Create ontology with subClassOf relationship
    let mut super_classes: HashMap<Sid, Vec<Sid>> = HashMap::new();
    super_classes.insert(sid(10), vec![sid(20)]); // Dog subClassOf Animal

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_properties
        super_classes,
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        HashSet::new(), // inverse_functional_properties
        HashMap::new(), // has_keys
        1,
    );

    // Create rdf:type SID
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    // Delta has type(1, Dog) - instance 1 is a Dog
    let mut delta = DeltaSet::new();
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(10)), // Dog
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

    apply_subclass_rule(&ontology, &mut ctx);

    // Should derive type(1, Animal)
    assert_eq!(new_delta.len(), 1);
    let derived_flake = new_delta.iter().next().unwrap();
    assert_eq!(derived_flake.s, sid(1));
    assert_eq!(derived_flake.p, rdf_type_sid);
    if let FlakeValue::Ref(c) = &derived_flake.o {
        assert_eq!(c, &sid(20)); // Animal
    } else {
        panic!("Expected Ref object");
    }

    assert_eq!(diagnostics.rules_fired.get("cax-sco"), Some(&1));
}

#[test]
fn test_subclass_rule_transitive() {
    // Test cax-sco with transitive hierarchy:
    // Poodle (10) → Dog (20) → Animal (30)
    // type(x, Poodle) should derive type(x, Dog) AND type(x, Animal)

    let mut super_classes: HashMap<Sid, Vec<Sid>> = HashMap::new();
    // Poodle has Dog and Animal as superclasses (transitive closure)
    super_classes.insert(sid(10), vec![sid(20), sid(30)]); // Poodle → Dog, Animal

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_properties
        super_classes,
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        HashSet::new(), // inverse_functional_properties
        HashMap::new(), // has_keys
        1,
    );

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    // Delta has type(1, Poodle)
    let mut delta = DeltaSet::new();
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(10)), // Poodle
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

    apply_subclass_rule(&ontology, &mut ctx);

    // Should derive type(1, Dog) AND type(1, Animal)
    assert_eq!(new_delta.len(), 2);

    // Collect derived types
    let derived_types: HashSet<Sid> = new_delta
        .iter()
        .filter_map(|f| {
            if let FlakeValue::Ref(c) = &f.o {
                Some(c.clone())
            } else {
                None
            }
        })
        .collect();

    assert!(derived_types.contains(&sid(20))); // Dog
    assert!(derived_types.contains(&sid(30))); // Animal

    assert_eq!(diagnostics.rules_fired.get("cax-sco"), Some(&2));
}

#[test]
fn test_subclass_rule_no_superclass() {
    // Test that no derivation happens for classes without superclasses

    // Empty super_classes - no hierarchy
    let super_classes: HashMap<Sid, Vec<Sid>> = HashMap::new();

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_properties
        super_classes,
        HashMap::new(), // equivalent_classes
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        HashSet::new(), // inverse_functional_properties
        HashMap::new(), // has_keys
        1,
    );

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    // Delta has type(1, SomeClass) where SomeClass has no superclasses
    let mut delta = DeltaSet::new();
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(10)),
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

    apply_subclass_rule(&ontology, &mut ctx);

    // Should derive nothing
    assert_eq!(new_delta.len(), 0);
    assert!(diagnostics.rules_fired.get("cax-sco").is_none());
}

#[test]
fn test_equivalent_class_rule() {
    // Test cax-eqc: type(x, C1), equivalentClass(C1, C2) → type(x, C2)
    // Class 10 (Male) is equivalent to class 20 (Man)

    // Create ontology with equivalentClass relationship (bidirectional)
    let mut equivalent_classes: HashMap<Sid, Vec<Sid>> = HashMap::new();
    equivalent_classes.insert(sid(10), vec![sid(20)]); // Male → Man
    equivalent_classes.insert(sid(20), vec![sid(10)]); // Man → Male

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_properties
        HashMap::new(), // super_classes
        equivalent_classes,
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        HashSet::new(), // inverse_functional_properties
        HashMap::new(), // has_keys
        1,
    );

    // Create rdf:type SID
    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    // Delta has type(1, Male) - instance 1 is Male
    let mut delta = DeltaSet::new();
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(10)), // Male
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

    apply_equivalent_class_rule(&ontology, &mut ctx);

    // Should derive type(1, Man)
    assert_eq!(new_delta.len(), 1);
    let derived_flake = new_delta.iter().next().unwrap();
    assert_eq!(derived_flake.s, sid(1));
    assert_eq!(derived_flake.p, rdf_type_sid);
    if let FlakeValue::Ref(c) = &derived_flake.o {
        assert_eq!(c, &sid(20)); // Man
    } else {
        panic!("Expected Ref object");
    }

    assert_eq!(diagnostics.rules_fired.get("cax-eqc"), Some(&1));
}

#[test]
fn test_equivalent_class_rule_bidirectional() {
    // Test that equivalentClass works in both directions:
    // type(x, Man) should also derive type(x, Male)

    let mut equivalent_classes: HashMap<Sid, Vec<Sid>> = HashMap::new();
    equivalent_classes.insert(sid(10), vec![sid(20)]); // Male → Man
    equivalent_classes.insert(sid(20), vec![sid(10)]); // Man → Male

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_properties
        HashMap::new(), // super_classes
        equivalent_classes,
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        HashSet::new(), // inverse_functional_properties
        HashMap::new(), // has_keys
        1,
    );

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    // Delta has type(1, Man) - testing the reverse direction
    let mut delta = DeltaSet::new();
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(20)), // Man
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

    apply_equivalent_class_rule(&ontology, &mut ctx);

    // Should derive type(1, Male)
    assert_eq!(new_delta.len(), 1);
    let derived_flake = new_delta.iter().next().unwrap();
    assert_eq!(derived_flake.s, sid(1));
    if let FlakeValue::Ref(c) = &derived_flake.o {
        assert_eq!(c, &sid(10)); // Male
    } else {
        panic!("Expected Ref object");
    }

    assert_eq!(diagnostics.rules_fired.get("cax-eqc"), Some(&1));
}

#[test]
fn test_equivalent_class_rule_no_equivalent() {
    // Test that no derivation happens for classes without equivalent classes

    // Empty equivalent_classes - no equivalence
    let equivalent_classes: HashMap<Sid, Vec<Sid>> = HashMap::new();

    let ontology = OntologyRL::new_full(
        HashSet::new(),
        HashSet::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(), // super_properties
        HashMap::new(), // super_classes
        equivalent_classes,
        Vec::new(),     // property_chains
        HashSet::new(), // functional_properties
        HashSet::new(), // inverse_functional_properties
        HashMap::new(), // has_keys
        1,
    );

    let rdf_type_sid = Sid::new(RDF, RDF_TYPE);

    // Delta has type(1, SomeClass) where SomeClass has no equivalents
    let mut delta = DeltaSet::new();
    delta.push(Flake::new(
        sid(1),
        rdf_type_sid.clone(),
        FlakeValue::Ref(sid(10)),
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

    apply_equivalent_class_rule(&ontology, &mut ctx);

    // Should derive nothing
    assert_eq!(new_delta.len(), 0);
    assert!(diagnostics.rules_fired.get("cax-eqc").is_none());
}
