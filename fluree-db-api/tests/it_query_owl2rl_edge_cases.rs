//! OWL2-RL edge case integration tests
//!
//! These tests validate complex OWL2-RL patterns that go beyond basic rule coverage:
//! - Complex combinations (union+intersection, chain+allValuesFrom)
//! - Edge cases (3+ branches, nested unions, double inverse)
//! - Negative tests (partial conditions shouldn't trigger inference)
//!

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, normalize_rows};

// =============================================================================
// Restriction Edge Cases
// =============================================================================

#[tokio::test]
async fn owl2rl_allvaluesfrom_with_inverse_property() {
    // Test: allValuesFrom restriction on an inverse property
    // Formulation ≡ Specification ∩ ∀(isMemberOf)⁻.Ingredient
    // If x is Formulation and y isMemberOf x, then y must be Ingredient
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/edge/avf-inverse");

    // Build schema with allValuesFrom on inverse property
    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "@graph": [
            // Define classes
            {"@id": "ex:Specification", "@type": "owl:Class"},
            {"@id": "ex:Ingredient", "@type": "owl:Class"},
            {"@id": "ex:isMemberOf", "@type": "owl:ObjectProperty"},

            // Build the inverse property reference
            {"@id": "ex:_invProp", "@type": "owl:ObjectProperty", "owl:inverseOf": {"@id": "ex:isMemberOf"}},

            // Build the restriction: ∀(isMemberOf)⁻.Ingredient
            {"@id": "ex:_restr1", "@type": "owl:Restriction",
             "owl:onProperty": {"@id": "ex:_invProp"},
             "owl:allValuesFrom": {"@id": "ex:Ingredient"}},

            // Build intersection list
            {"@id": "ex:_list1", "rdf:first": {"@id": "ex:Specification"}, "rdf:rest": {"@id": "ex:_list2"}},
            {"@id": "ex:_list2", "rdf:first": {"@id": "ex:_restr1"}, "rdf:rest": {"@id": "rdf:nil"}},

            // Define equivalentClass
            {"@id": "ex:_intClass", "@type": "owl:Class", "owl:intersectionOf": {"@id": "ex:_list1"}},
            {"@id": "ex:Formulation", "@type": "owl:Class", "owl:equivalentClass": {"@id": "ex:_intClass"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    // Insert data: f is a Formulation, y and z are members of f
    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:f", "@type": ["ex:Formulation", "ex:Specification"]},
            {"@id": "ex:y", "ex:isMemberOf": {"@id": "ex:f"}},
            {"@id": "ex:z", "ex:isMemberOf": {"@id": "ex:f"}}
        ]
    });
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    // Query: what types does y have?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?type"],
        "where": {"@id": "ex:y", "@type": "?type"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:Ingredient")),
        "y should be inferred as Ingredient via allValuesFrom on inverse property, got {results:?}"
    );

    // Also check z
    let q2 = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?type"],
        "where": {"@id": "ex:z", "@type": "?type"},
        "reasoning": "owl2rl"
    });

    let rows2 = support::query_jsonld(&fluree, &ledger, &q2)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results2 = normalize_rows(&rows2);

    assert!(
        results2.contains(&json!("ex:Ingredient")),
        "z should also be inferred as Ingredient, got {results2:?}"
    );
}

#[tokio::test]
async fn owl2rl_multi_same_property_restrictions() {
    // Test: Multiple someValuesFrom restrictions on the same property in one intersection
    // DrugProduct ≡ ManufacturedItem ∩ ∃isCategorizedBy.DosageForm ∩ ∃isCategorizedBy.RouteOfAdmin
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/edge/multi-same-prop");

    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "@graph": [
            // Define classes and properties
            {"@id": "ex:ManufacturedItem", "@type": "owl:Class"},
            {"@id": "ex:DosageForm", "@type": "owl:Class"},
            {"@id": "ex:RouteOfAdmin", "@type": "owl:Class"},
            {"@id": "ex:isCategorizedBy", "@type": "owl:ObjectProperty"},

            // Build restrictions
            {"@id": "ex:_restr1", "@type": "owl:Restriction",
             "owl:onProperty": {"@id": "ex:isCategorizedBy"},
             "owl:someValuesFrom": {"@id": "ex:DosageForm"}},
            {"@id": "ex:_restr2", "@type": "owl:Restriction",
             "owl:onProperty": {"@id": "ex:isCategorizedBy"},
             "owl:someValuesFrom": {"@id": "ex:RouteOfAdmin"}},

            // Build intersection list with 3 items
            {"@id": "ex:_list1", "rdf:first": {"@id": "ex:ManufacturedItem"}, "rdf:rest": {"@id": "ex:_list2"}},
            {"@id": "ex:_list2", "rdf:first": {"@id": "ex:_restr1"}, "rdf:rest": {"@id": "ex:_list3"}},
            {"@id": "ex:_list3", "rdf:first": {"@id": "ex:_restr2"}, "rdf:rest": {"@id": "rdf:nil"}},

            // Define equivalentClass
            {"@id": "ex:_intClass", "@type": "owl:Class", "owl:intersectionOf": {"@id": "ex:_list1"}},
            {"@id": "ex:DrugProduct", "@type": "owl:Class", "owl:equivalentClass": {"@id": "ex:_intClass"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            // product1 meets ALL criteria
            {"@id": "ex:dosage1", "@type": "ex:DosageForm"},
            {"@id": "ex:route1", "@type": "ex:RouteOfAdmin"},
            {"@id": "ex:product1", "@type": "ex:ManufacturedItem",
             "ex:isCategorizedBy": [{"@id": "ex:dosage1"}, {"@id": "ex:route1"}]},

            // product2 is missing RouteOfAdmin categorization
            {"@id": "ex:dosage2", "@type": "ex:DosageForm"},
            {"@id": "ex:product2", "@type": "ex:ManufacturedItem",
             "ex:isCategorizedBy": {"@id": "ex:dosage2"}}
        ]
    });
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    // Query: who is DrugProduct?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?s"],
        "where": {"@id": "?s", "@type": "ex:DrugProduct"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:product1")),
        "product1 should be DrugProduct (has both categorizations), got {results:?}"
    );
    assert!(
        !results.contains(&json!("ex:product2")),
        "product2 should NOT be DrugProduct (missing RouteOfAdmin), got {results:?}"
    );
}

// =============================================================================
// Union Edge Cases
// =============================================================================

#[tokio::test]
async fn owl2rl_union_3_plus_branches() {
    // Test: unionOf with 3+ branches (not just 2)
    // MultiTarget ≡ Protein ∪ Receptor ∪ Enzyme ∪ Antibody
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/edge/union-3plus");

    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "@graph": [
            // Define classes
            {"@id": "ex:Protein", "@type": "owl:Class"},
            {"@id": "ex:Receptor", "@type": "owl:Class"},
            {"@id": "ex:Enzyme", "@type": "owl:Class"},
            {"@id": "ex:Antibody", "@type": "owl:Class"},

            // Build 4-element union list
            {"@id": "ex:_list1", "rdf:first": {"@id": "ex:Protein"}, "rdf:rest": {"@id": "ex:_list2"}},
            {"@id": "ex:_list2", "rdf:first": {"@id": "ex:Receptor"}, "rdf:rest": {"@id": "ex:_list3"}},
            {"@id": "ex:_list3", "rdf:first": {"@id": "ex:Enzyme"}, "rdf:rest": {"@id": "ex:_list4"}},
            {"@id": "ex:_list4", "rdf:first": {"@id": "ex:Antibody"}, "rdf:rest": {"@id": "rdf:nil"}},

            // Define equivalentClass
            {"@id": "ex:_uniClass", "@type": "owl:Class", "owl:unionOf": {"@id": "ex:_list1"}},
            {"@id": "ex:MultiTarget", "@type": "owl:Class", "owl:equivalentClass": {"@id": "ex:_uniClass"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:prot1", "@type": "ex:Protein"},
            {"@id": "ex:rec1", "@type": "ex:Receptor"},
            {"@id": "ex:enz1", "@type": "ex:Enzyme"},
            {"@id": "ex:ab1", "@type": "ex:Antibody"},
            {"@id": "ex:other1", "@type": "ex:Unrelated"}
        ]
    });
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    // Query: who is MultiTarget?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?s"],
        "where": {"@id": "?s", "@type": "ex:MultiTarget"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    // All 4 union members should be MultiTarget
    assert!(
        results.contains(&json!("ex:prot1")),
        "Protein should be MultiTarget"
    );
    assert!(
        results.contains(&json!("ex:rec1")),
        "Receptor should be MultiTarget"
    );
    assert!(
        results.contains(&json!("ex:enz1")),
        "Enzyme should be MultiTarget"
    );
    assert!(
        results.contains(&json!("ex:ab1")),
        "Antibody should be MultiTarget"
    );
    assert!(
        !results.contains(&json!("ex:other1")),
        "Unrelated should NOT be MultiTarget"
    );
}

#[tokio::test]
async fn owl2rl_nested_unions() {
    // Test: Nested union (union containing another union)
    // NestedTarget ≡ SimpleTarget ∪ (ComplexA ∪ ComplexB)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/edge/nested-union");

    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "@graph": [
            // Define classes
            {"@id": "ex:SimpleTarget", "@type": "owl:Class"},
            {"@id": "ex:ComplexA", "@type": "owl:Class"},
            {"@id": "ex:ComplexB", "@type": "owl:Class"},

            // Build inner union (ComplexA ∪ ComplexB)
            {"@id": "ex:_innerList1", "rdf:first": {"@id": "ex:ComplexA"}, "rdf:rest": {"@id": "ex:_innerList2"}},
            {"@id": "ex:_innerList2", "rdf:first": {"@id": "ex:ComplexB"}, "rdf:rest": {"@id": "rdf:nil"}},
            {"@id": "ex:_innerUnion", "@type": "owl:Class", "owl:unionOf": {"@id": "ex:_innerList1"}},

            // Build outer union (SimpleTarget ∪ _innerUnion)
            {"@id": "ex:_outerList1", "rdf:first": {"@id": "ex:SimpleTarget"}, "rdf:rest": {"@id": "ex:_outerList2"}},
            {"@id": "ex:_outerList2", "rdf:first": {"@id": "ex:_innerUnion"}, "rdf:rest": {"@id": "rdf:nil"}},
            {"@id": "ex:_outerUnion", "@type": "owl:Class", "owl:unionOf": {"@id": "ex:_outerList1"}},

            // Define equivalentClass
            {"@id": "ex:NestedTarget", "@type": "owl:Class", "owl:equivalentClass": {"@id": "ex:_outerUnion"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:simple1", "@type": "ex:SimpleTarget"},
            {"@id": "ex:complexA1", "@type": "ex:ComplexA"},
            {"@id": "ex:complexB1", "@type": "ex:ComplexB"}
        ]
    });
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    // Query: who is NestedTarget?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?s"],
        "where": {"@id": "?s", "@type": "ex:NestedTarget"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:simple1")),
        "SimpleTarget should be NestedTarget"
    );
    assert!(
        results.contains(&json!("ex:complexA1")),
        "ComplexA should be NestedTarget (via inner union)"
    );
    assert!(
        results.contains(&json!("ex:complexB1")),
        "ComplexB should be NestedTarget (via inner union)"
    );
}

#[tokio::test]
async fn owl2rl_union_with_intersection() {
    // Test: Union combined with intersection
    // UnionIntersection ≡ (DrugTarget ∪ Biomarker) ∩ ∃hasFunction.TherapeuticFunction
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/edge/union-intersection");

    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "@graph": [
            // Define classes and property
            {"@id": "ex:DrugTarget", "@type": "owl:Class"},
            {"@id": "ex:Biomarker", "@type": "owl:Class"},
            {"@id": "ex:TherapeuticFunction", "@type": "owl:Class"},
            {"@id": "ex:hasFunction", "@type": "owl:ObjectProperty"},

            // Build inner union (DrugTarget ∪ Biomarker)
            {"@id": "ex:_unionList1", "rdf:first": {"@id": "ex:DrugTarget"}, "rdf:rest": {"@id": "ex:_unionList2"}},
            {"@id": "ex:_unionList2", "rdf:first": {"@id": "ex:Biomarker"}, "rdf:rest": {"@id": "rdf:nil"}},
            {"@id": "ex:_unionClass", "@type": "owl:Class", "owl:unionOf": {"@id": "ex:_unionList1"}},

            // Build restriction ∃hasFunction.TherapeuticFunction
            {"@id": "ex:_restr1", "@type": "owl:Restriction",
             "owl:onProperty": {"@id": "ex:hasFunction"},
             "owl:someValuesFrom": {"@id": "ex:TherapeuticFunction"}},

            // Build intersection: union ∩ restriction
            {"@id": "ex:_intList1", "rdf:first": {"@id": "ex:_unionClass"}, "rdf:rest": {"@id": "ex:_intList2"}},
            {"@id": "ex:_intList2", "rdf:first": {"@id": "ex:_restr1"}, "rdf:rest": {"@id": "rdf:nil"}},
            {"@id": "ex:_intClass", "@type": "owl:Class", "owl:intersectionOf": {"@id": "ex:_intList1"}},

            // Define equivalentClass
            {"@id": "ex:UnionIntersection", "@type": "owl:Class", "owl:equivalentClass": {"@id": "ex:_intClass"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:func1", "@type": "ex:TherapeuticFunction"},
            // drug1 is DrugTarget with function
            {"@id": "ex:drug1", "@type": "ex:DrugTarget", "ex:hasFunction": {"@id": "ex:func1"}},
            // bio1 is Biomarker with function
            {"@id": "ex:bio1", "@type": "ex:Biomarker", "ex:hasFunction": {"@id": "ex:func1"}},
            // drug2 is DrugTarget WITHOUT function
            {"@id": "ex:drug2", "@type": "ex:DrugTarget"}
        ]
    });
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    // Query: who is UnionIntersection?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?s"],
        "where": {"@id": "?s", "@type": "ex:UnionIntersection"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:drug1")),
        "drug1 should be UnionIntersection (DrugTarget + function)"
    );
    assert!(
        results.contains(&json!("ex:bio1")),
        "bio1 should be UnionIntersection (Biomarker + function)"
    );
    assert!(
        !results.contains(&json!("ex:drug2")),
        "drug2 should NOT be UnionIntersection (missing function)"
    );
}

// =============================================================================
// Inverse Property Edge Cases
// =============================================================================

#[tokio::test]
async fn owl2rl_inverse_in_deeper_chain() {
    // Test: Inverse property in a property chain of length >= 3
    // Define: hasGrandparent = hasParent ∘ hasChild⁻ ∘ hasParent
    // (This is a contrived chain, but tests inverse handling in chains)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/edge/inverse-chain");

    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "@graph": [
            // Define properties
            {"@id": "ex:hasParent", "@type": "owl:ObjectProperty"},
            {"@id": "ex:hasChild", "@type": "owl:ObjectProperty", "owl:inverseOf": {"@id": "ex:hasParent"}},

            // Build property chain: hasRelative = hasParent ∘ hasParent (simpler chain for testing)
            {"@id": "ex:_chainList1", "rdf:first": {"@id": "ex:hasParent"}, "rdf:rest": {"@id": "ex:_chainList2"}},
            {"@id": "ex:_chainList2", "rdf:first": {"@id": "ex:hasParent"}, "rdf:rest": {"@id": "rdf:nil"}},
            {"@id": "ex:hasGrandparent", "@type": "owl:ObjectProperty", "owl:propertyChainAxiom": {"@id": "ex:_chainList1"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice", "ex:hasParent": {"@id": "ex:bob"}},
            {"@id": "ex:bob", "ex:hasParent": {"@id": "ex:charlie"}}
        ]
    });
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    // Query for alice's grandparent via chain
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?gp"],
        "where": {"@id": "ex:alice", "ex:hasGrandparent": "?gp"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:charlie")),
        "alice should have grandparent charlie via chain, got {results:?}"
    );

    // Also test that inverse property works
    let q2 = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?child"],
        "where": {"@id": "ex:bob", "ex:hasChild": "?child"},
        "reasoning": "owl2rl"
    });

    let rows2 = support::query_jsonld(&fluree, &ledger, &q2)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results2 = normalize_rows(&rows2);

    assert!(
        results2.contains(&json!("ex:alice")),
        "bob should have child alice via inverse, got {results2:?}"
    );
}

#[tokio::test]
async fn owl2rl_double_inverse_normalization() {
    // Test: Double inverse should normalize to original property
    // If R⁻⁻ should equal R
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/edge/double-inverse");

    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "@graph": [
            // Define base property
            {"@id": "ex:originalProp", "@type": "owl:ObjectProperty"},
            // Define inverse of original
            {"@id": "ex:inverseProp", "@type": "owl:ObjectProperty", "owl:inverseOf": {"@id": "ex:originalProp"}},
            // Define inverse of inverse (should be same as original)
            {"@id": "ex:doubleInverseProp", "@type": "owl:ObjectProperty", "owl:inverseOf": {"@id": "ex:inverseProp"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:source", "ex:originalProp": {"@id": "ex:target"}}
        ]
    });
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    // Query via double inverse (should be same as original)
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?t"],
        "where": {"@id": "ex:source", "ex:doubleInverseProp": "?t"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:target")),
        "double inverse should normalize: source->doubleInverseProp->target, got {results:?}"
    );

    // Also verify single inverse works in opposite direction
    let q2 = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?s"],
        "where": {"@id": "ex:target", "ex:inverseProp": "?s"},
        "reasoning": "owl2rl"
    });

    let rows2 = support::query_jsonld(&fluree, &ledger, &q2)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results2 = normalize_rows(&rows2);

    assert!(
        results2.contains(&json!("ex:source")),
        "single inverse should work: target->inverseProp->source, got {results2:?}"
    );
}

// =============================================================================
// Negative Tests (Pathology Checks)
// =============================================================================

#[tokio::test]
async fn owl2rl_partial_conditions_no_inference() {
    // Test: When only partial conditions of an intersection are met, no inference should happen
    // ConjunctiveClass ≡ ∃hasA.ClassA ∩ ∃hasB.ClassB ∩ ∃hasC.ClassC
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/edge/negative-partial");

    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "@graph": [
            // Define classes and properties
            {"@id": "ex:ClassA", "@type": "owl:Class"},
            {"@id": "ex:ClassB", "@type": "owl:Class"},
            {"@id": "ex:ClassC", "@type": "owl:Class"},
            {"@id": "ex:hasA", "@type": "owl:ObjectProperty"},
            {"@id": "ex:hasB", "@type": "owl:ObjectProperty"},
            {"@id": "ex:hasC", "@type": "owl:ObjectProperty"},

            // Build restrictions
            {"@id": "ex:_restr1", "@type": "owl:Restriction", "owl:onProperty": {"@id": "ex:hasA"}, "owl:someValuesFrom": {"@id": "ex:ClassA"}},
            {"@id": "ex:_restr2", "@type": "owl:Restriction", "owl:onProperty": {"@id": "ex:hasB"}, "owl:someValuesFrom": {"@id": "ex:ClassB"}},
            {"@id": "ex:_restr3", "@type": "owl:Restriction", "owl:onProperty": {"@id": "ex:hasC"}, "owl:someValuesFrom": {"@id": "ex:ClassC"}},

            // Build intersection list
            {"@id": "ex:_list1", "rdf:first": {"@id": "ex:_restr1"}, "rdf:rest": {"@id": "ex:_list2"}},
            {"@id": "ex:_list2", "rdf:first": {"@id": "ex:_restr2"}, "rdf:rest": {"@id": "ex:_list3"}},
            {"@id": "ex:_list3", "rdf:first": {"@id": "ex:_restr3"}, "rdf:rest": {"@id": "rdf:nil"}},

            // Define equivalentClass
            {"@id": "ex:_intClass", "@type": "owl:Class", "owl:intersectionOf": {"@id": "ex:_list1"}},
            {"@id": "ex:ConjunctiveClass", "@type": "owl:Class", "owl:equivalentClass": {"@id": "ex:_intClass"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:a1", "@type": "ex:ClassA"},
            {"@id": "ex:b1", "@type": "ex:ClassB"},
            {"@id": "ex:c1", "@type": "ex:ClassC"},

            // partial1: only has A and B (missing C)
            {"@id": "ex:partial1", "ex:hasA": {"@id": "ex:a1"}, "ex:hasB": {"@id": "ex:b1"}},

            // complete1: has all three
            {"@id": "ex:complete1", "ex:hasA": {"@id": "ex:a1"}, "ex:hasB": {"@id": "ex:b1"}, "ex:hasC": {"@id": "ex:c1"}}
        ]
    });
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    // Query: who is ConjunctiveClass?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?s"],
        "where": {"@id": "?s", "@type": "ex:ConjunctiveClass"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        !results.contains(&json!("ex:partial1")),
        "partial1 should NOT be ConjunctiveClass (missing hasC), got {results:?}"
    );
    assert!(
        results.contains(&json!("ex:complete1")),
        "complete1 should be ConjunctiveClass (has all conditions), got {results:?}"
    );
}

#[tokio::test]
async fn owl2rl_hasvalue_class_to_property_entailment() {
    // Test: Class membership should entail hasValue property
    // KilogramMagnitude ≡ Magnitude ∩ ∃hasUnit.{kg}
    // If x is KilogramMagnitude, then x hasUnit kg
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/edge/hasvalue-entailment");

    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "@graph": [
            // Define classes and property
            {"@id": "ex:Magnitude", "@type": "owl:Class"},
            {"@id": "ex:Unit", "@type": "owl:Class"},
            {"@id": "ex:kg", "@type": "ex:Unit"},
            {"@id": "ex:hasUnit", "@type": "owl:ObjectProperty"},

            // Build hasValue restriction
            {"@id": "ex:_restr1", "@type": "owl:Restriction",
             "owl:onProperty": {"@id": "ex:hasUnit"},
             "owl:hasValue": {"@id": "ex:kg"}},

            // Build intersection: Magnitude ∩ hasValue restriction
            {"@id": "ex:_list1", "rdf:first": {"@id": "ex:Magnitude"}, "rdf:rest": {"@id": "ex:_list2"}},
            {"@id": "ex:_list2", "rdf:first": {"@id": "ex:_restr1"}, "rdf:rest": {"@id": "rdf:nil"}},

            // Define equivalentClass
            {"@id": "ex:_intClass", "@type": "owl:Class", "owl:intersectionOf": {"@id": "ex:_list1"}},
            {"@id": "ex:KilogramMagnitude", "@type": "owl:Class", "owl:equivalentClass": {"@id": "ex:_intClass"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    // Insert instance that is explicitly KilogramMagnitude (without specifying hasUnit)
    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:mass1", "@type": ["ex:KilogramMagnitude", "ex:Magnitude"]}
        ]
    });
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    // Query: what unit does mass1 have? (should be inferred)
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?unit"],
        "where": {"@id": "ex:mass1", "ex:hasUnit": "?unit"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:kg")),
        "mass1 should have inferred hasUnit kg via hasValue backward entailment, got {results:?}"
    );
}
