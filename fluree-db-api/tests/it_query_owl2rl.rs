//! OWL2-RL integration tests
//!
//! These tests validate OWL2-RL forward-chaining materialization rules.
//!
//! Test coverage:
//! - eq-sym, eq-trans: owl:sameAs symmetry and transitivity
//! - prp-dom, prp-rng: rdfs:domain, rdfs:range
//! - prp-symp: owl:SymmetricProperty
//! - prp-trp: owl:TransitiveProperty
//! - prp-inv: owl:inverseOf
//! - prp-fp: owl:FunctionalProperty
//! - prp-ifp: owl:InverseFunctionalProperty
//! - prp-spo1: rdfs:subPropertyOf
//! - prp-spo2: owl:propertyChainAxiom
//! - prp-key: owl:hasKey
//! - cax-sco: rdfs:subClassOf
//! - cax-eqc: owl:equivalentClass
//! - cls-hv: owl:hasValue restrictions
//! - cls-svf: owl:someValuesFrom restrictions
//! - cls-avf: owl:allValuesFrom restrictions
//! - cls-maxc: owl:maxCardinality restrictions
//! - cls-int: owl:intersectionOf
//! - cls-uni: owl:unionOf
//! - cls-oo: owl:oneOf

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, normalize_rows};

// =============================================================================
// Equality Tests (eq-sym, eq-trans)
// =============================================================================

#[tokio::test]
async fn owl2rl_same_as_symmetry() {
    // Test: owl:sameAs(a, b) => owl:sameAs(b, a)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/same-as-sym");

    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {"@id": "ex:carol", "owl:sameAs": {"@id": "ex:carol-lynn"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;

    // Query for sameAs of carol-lynn (should include carol via symmetry)
    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "select": "?same",
        "where": {"@id": "ex:carol-lynn", "owl:sameAs": "?same"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    // Should include both carol and carol-lynn (sameAs is reflexive and symmetric)
    assert!(
        results.contains(&json!("ex:carol")),
        "carol-lynn should be sameAs carol, got {results:?}"
    );
}

#[tokio::test]
async fn owl2rl_same_as_transitivity() {
    // Test: sameAs(a,b) ∧ sameAs(b,c) => sameAs(a,c)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/same-as-trans");

    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {"@id": "ex:carol1", "owl:sameAs": {"@id": "ex:carol2"}},
            {"@id": "ex:carol2", "owl:sameAs": {"@id": "ex:carol3"}},
            {"@id": "ex:carol3", "owl:sameAs": {"@id": "ex:carol4"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;

    // Query for all things sameAs carol1 (should include all 4 via transitivity)
    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "select": "?same",
        "where": {"@id": "ex:carol1", "owl:sameAs": "?same"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    // Should include all 4 carols
    assert!(
        results.contains(&json!("ex:carol2")),
        "carol1 sameAs carol2"
    );
    assert!(
        results.contains(&json!("ex:carol3")),
        "carol1 sameAs carol3"
    );
    assert!(
        results.contains(&json!("ex:carol4")),
        "carol1 sameAs carol4"
    );
}

// =============================================================================
// Property Rules (prp-*)
// =============================================================================

#[tokio::test]
async fn owl2rl_symmetric_property() {
    // Test: SymmetricProperty(P) ∧ P(x,y) => P(y,x)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/prp-symp");

    // Define livesWith as symmetric and assert person-a livesWith person-b
    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "@graph": [
            {"@id": "ex:livesWith", "@type": ["owl:ObjectProperty", "owl:SymmetricProperty"]},
            {"@id": "ex:person-a", "ex:livesWith": {"@id": "ex:person-b"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;

    // Query: who does person-b live with?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?x",
        "where": {"@id": "ex:person-b", "ex:livesWith": "?x"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        rows,
        json!(["ex:person-a"]),
        "person-b should live with person-a via symmetry"
    );
}

#[tokio::test]
async fn owl2rl_transitive_property() {
    // Test: TransitiveProperty(P) ∧ P(x,y) ∧ P(y,z) => P(x,z)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/prp-trp");

    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {"@id": "ex:livesWith", "@type": ["owl:ObjectProperty", "owl:TransitiveProperty"]},
            {"@id": "ex:person-a", "ex:livesWith": {"@id": "ex:person-b"}},
            {"@id": "ex:person-b", "ex:livesWith": {"@id": "ex:person-c"}},
            {"@id": "ex:person-c", "ex:livesWith": {"@id": "ex:person-d"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;

    // Query: who does person-a live with?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?people",
        "where": {"@id": "ex:person-a", "ex:livesWith": "?people"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    // Should include b, c, d via transitivity
    assert!(results.contains(&json!("ex:person-b")));
    assert!(results.contains(&json!("ex:person-c")));
    assert!(results.contains(&json!("ex:person-d")));
}

#[tokio::test]
async fn owl2rl_inverse_of() {
    // Test: inverseOf(P1, P2) ∧ P1(x,y) => P2(y,x)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/prp-inv");

    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {"@id": "ex:child", "@type": "owl:ObjectProperty", "owl:inverseOf": {"@id": "ex:parents"}},
            {"@id": "ex:son", "ex:parents": [{"@id": "ex:mom"}, {"@id": "ex:dad"}]},
            {"@id": "ex:alice", "ex:child": {"@id": "ex:bob"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;

    // Query: who is mom's child?
    let q1 = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?x",
        "where": {"@id": "ex:mom", "ex:child": "?x"},
        "reasoning": "owl2rl"
    });
    let rows1 = support::query_jsonld(&fluree, &ledger, &q1)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(rows1, json!(["ex:son"]), "mom's child should be son");

    // Query: who is bob's parent?
    let q2 = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?x",
        "where": {"@id": "ex:bob", "ex:parents": "?x"},
        "reasoning": "owl2rl"
    });
    let rows2 = support::query_jsonld(&fluree, &ledger, &q2)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(rows2, json!(["ex:alice"]), "bob's parent should be alice");
}

#[tokio::test]
async fn owl2rl_domain_rule() {
    // Test: domain(P, C) ∧ P(x, y) => type(x, C)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/prp-dom");

    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        },
        "@graph": [
            {"@id": "ex:parents", "@type": "owl:ObjectProperty",
             "rdfs:domain": [{"@id": "ex:Person"}, {"@id": "ex:Child"}]},
            {"@id": "ex:brian", "ex:parents": {"@id": "ex:carol"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;

    // Query: what type is brian?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?t",
        "where": {"@id": "ex:brian", "@type": "?t"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:Person")),
        "brian should be type Person"
    );
    assert!(
        results.contains(&json!("ex:Child")),
        "brian should be type Child"
    );
}

#[tokio::test]
async fn owl2rl_range_rule() {
    // Test: range(P, C) ∧ P(x, y) => type(y, C) [when y is Ref]
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/prp-rng");

    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        },
        "@graph": [
            {"@id": "ex:parents", "@type": "owl:ObjectProperty",
             "rdfs:range": [{"@id": "ex:Person"}, {"@id": "ex:Parent"}]},
            {"@id": "ex:brian", "ex:parents": {"@id": "ex:carol"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;

    // Query: what type is carol?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?t",
        "where": {"@id": "ex:carol", "@type": "?t"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:Person")),
        "carol should be type Person"
    );
    assert!(
        results.contains(&json!("ex:Parent")),
        "carol should be type Parent"
    );
}

#[tokio::test]
async fn owl2rl_functional_property() {
    // Test: FunctionalProperty(P) ∧ P(x, y1) ∧ P(x, y2) => sameAs(y1, y2)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/prp-fp");

    // First, declare the property as functional
    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "@graph": [
            {"@id": "ex:mother", "@type": ["rdf:Property", "owl:FunctionalProperty"]}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    // Then, insert data that triggers the rule
    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:brian",
        "ex:mother": [{"@id": "ex:carol"}, {"@id": "ex:carol2"}]
    });
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    // Query: who is sameAs carol?
    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "select": "?same",
        "where": {"@id": "ex:carol", "owl:sameAs": "?same"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    // carol and carol2 should be sameAs each other
    assert!(
        results.contains(&json!("ex:carol2")),
        "carol should be sameAs carol2, got {results:?}"
    );
}

#[tokio::test]
async fn owl2rl_inverse_functional_property() {
    // Test: InverseFunctionalProperty(P) ∧ P(x1, y) ∧ P(x2, y) => sameAs(x1, x2)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/prp-ifp");

    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {"@id": "ex:ssn", "@type": ["owl:ObjectProperty", "owl:InverseFunctionalProperty"]},
            {"@id": "ex:brian1", "ex:ssn": {"@id": "ex:ssn-123"}},
            {"@id": "ex:brian2", "ex:ssn": {"@id": "ex:ssn-123"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;

    // Query: who is sameAs brian1?
    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "select": "?same",
        "where": {"@id": "ex:brian1", "owl:sameAs": "?same"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:brian2")),
        "brian1 should be sameAs brian2"
    );
}

#[tokio::test]
async fn owl2rl_sub_property_of() {
    // Test: subPropertyOf(P1, P2) ∧ P1(x, y) => P2(x, y)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/prp-spo1");

    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        },
        "@graph": [
            {"@id": "ex:mother", "@type": "owl:ObjectProperty", "rdfs:subPropertyOf": {"@id": "ex:parents"}},
            {"@id": "ex:father", "@type": "owl:ObjectProperty", "rdfs:subPropertyOf": {"@id": "ex:parents"}},
            {"@id": "ex:bob", "ex:mother": {"@id": "ex:alice-mom"}, "ex:father": {"@id": "ex:greg-dad"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;

    // Query: who are bob's parents?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?p",
        "where": {"@id": "ex:bob", "ex:parents": "?p"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(results.contains(&json!("ex:alice-mom")));
    assert!(results.contains(&json!("ex:greg-dad")));
}

#[tokio::test]
async fn owl2rl_property_chain_axiom() {
    // Test: propertyChainAxiom(P, [P1, P2]) ∧ P1(x, y) ∧ P2(y, z) => P(x, z)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/prp-spo2");

    // Use explicit RDF list encoding since @list isn't properly supported
    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "@graph": [
            // Build RDF list: (_list1 first parents; rest (_list2 first parents; rest nil))
            {"@id": "ex:_list1", "rdf:first": {"@id": "ex:parents"}, "rdf:rest": {"@id": "ex:_list2"}},
            {"@id": "ex:_list2", "rdf:first": {"@id": "ex:parents"}, "rdf:rest": {"@id": "rdf:nil"}},
            {"@id": "ex:grandparent", "@type": "owl:ObjectProperty",
             "owl:propertyChainAxiom": {"@id": "ex:_list1"}},
            {"@id": "ex:person-a", "ex:parents": [{"@id": "ex:mom"}, {"@id": "ex:dad"}]},
            {"@id": "ex:mom", "ex:parents": [{"@id": "ex:mom-mom"}, {"@id": "ex:mom-dad"}]},
            {"@id": "ex:dad", "ex:parents": [{"@id": "ex:dad-mom"}, {"@id": "ex:dad-dad"}]}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;

    // Query: who are person-a's grandparents?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?gp",
        "where": {"@id": "ex:person-a", "ex:grandparent": "?gp"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(results.contains(&json!("ex:mom-mom")));
    assert!(results.contains(&json!("ex:mom-dad")));
    assert!(results.contains(&json!("ex:dad-mom")));
    assert!(results.contains(&json!("ex:dad-dad")));
}

#[tokio::test]
async fn owl2rl_has_key() {
    // Test: hasKey(C, [P]) ∧ type(x, C) ∧ type(y, C) ∧ P(x, v) ∧ P(y, v) => sameAs(x, y)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/prp-key");

    // Use explicit RDF list encoding since @list isn't properly supported
    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "@graph": [
            // Build RDF list for hasKey: (_list1 first hasWaitingListN; rest nil)
            {"@id": "ex:_list1", "rdf:first": {"@id": "ex:hasWaitingListN"}, "rdf:rest": {"@id": "rdf:nil"}},
            {"@id": "ex:RegisteredPatient", "@type": "owl:Class",
             "owl:hasKey": {"@id": "ex:_list1"}},
            {"@id": "ex:brian", "@type": "ex:RegisteredPatient", "ex:hasWaitingListN": {"@id": "ex:ssn123"}},
            {"@id": "ex:brian2", "@type": "ex:RegisteredPatient", "ex:hasWaitingListN": {"@id": "ex:ssn123"}},
            {"@id": "ex:bob", "@type": "ex:RegisteredPatient", "ex:hasWaitingListN": {"@id": "ex:ssn456"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;

    // Query: who is sameAs brian?
    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "select": "?same",
        "where": {"@id": "ex:brian", "owl:sameAs": "?same"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:brian2")),
        "brian and brian2 share same key value"
    );
    assert!(
        !results.contains(&json!("ex:bob")),
        "bob has different key value"
    );
}

// =============================================================================
// Class Rules (cax-*)
// =============================================================================

#[tokio::test]
async fn owl2rl_subclass_of() {
    // Test: type(x, C1) ∧ subClassOf(C1, C2) => type(x, C2)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/cax-sco");

    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        },
        "@graph": [
            {"@id": "ex:Employee", "@type": "owl:Class", "rdfs:subClassOf": {"@id": "ex:Person"}},
            {"@id": "ex:Person", "@type": "owl:Class", "rdfs:subClassOf": {"@id": "ex:Human"}},
            {"@id": "ex:brian", "@type": "ex:Person"},
            {"@id": "ex:laura", "@type": "ex:Employee"},
            {"@id": "ex:alice", "@type": "ex:Human"}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;

    // Query: who is type Human?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?s",
        "where": {"@id": "?s", "@type": "ex:Human"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    // All three should be Human (alice directly, brian and laura via subclass chain)
    assert!(results.contains(&json!("ex:alice")));
    assert!(results.contains(&json!("ex:brian")));
    assert!(results.contains(&json!("ex:laura")));
}

#[tokio::test]
async fn owl2rl_equivalent_class() {
    // Test: type(x, C1) ∧ equivalentClass(C1, C2) => type(x, C2)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/cax-eqc");

    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {"@id": "ex:Human", "@type": "owl:Class", "owl:equivalentClass": {"@id": "ex:Person"}},
            {"@id": "ex:brian", "@type": "ex:Person"},
            {"@id": "ex:laura", "@type": "ex:Human"}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;

    // Query: who is type Person?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?s",
        "where": {"@id": "?s", "@type": "ex:Person"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    // Both should be Person (brian directly, laura via equivalentClass)
    assert!(results.contains(&json!("ex:brian")));
    assert!(results.contains(&json!("ex:laura")));
}

// =============================================================================
// Restriction Rules (cls-*)
// =============================================================================

#[tokio::test]
async fn owl2rl_has_value_forward() {
    // Test: type(x, C) where C equivalentClass restriction with hasValue => P(x, v)
    // Note: This tests the "backward" entailment: class membership => property value
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/cls-hv-forward");

    // Insert restriction definition with explicit @id for the blank node
    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {"@id": "ex:_restr1", "@type": "owl:Restriction",
             "owl:onProperty": {"@id": "ex:hasUnit"},
             "owl:hasValue": {"@id": "ex:kg"}},
            {"@id": "ex:KilogramMagnitude", "@type": "owl:Class",
             "owl:equivalentClass": {"@id": "ex:_restr1"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:mass1",
        "@type": "ex:KilogramMagnitude"
    });
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    // Query: what is mass1's unit?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?unit",
        "where": {"@id": "ex:mass1", "ex:hasUnit": "?unit"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        rows,
        json!(["ex:kg"]),
        "mass1 should have inferred hasUnit ex:kg"
    );
}

#[tokio::test]
async fn owl2rl_has_value_backward() {
    // Test: P(x, v) where restriction hasValue(P, v) => type(x, C)
    // This tests the "forward" entailment: property value => class membership
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/cls-hv-backward");

    // Insert schema with explicit @id for restriction
    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {"@id": "ex:_restr1", "@type": "owl:Restriction",
             "owl:onProperty": {"@id": "ex:hasUnit"},
             "owl:hasValue": {"@id": "ex:kg"}},
            {"@id": "ex:KilogramMagnitude", "@type": "owl:Class",
             "owl:equivalentClass": {"@id": "ex:_restr1"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:mass1", "ex:hasUnit": {"@id": "ex:kg"}},
            {"@id": "ex:mass2", "ex:hasUnit": {"@id": "ex:lb"}}
        ]
    });
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    // Query: who is type KilogramMagnitude?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?s",
        "where": {"@id": "?s", "@type": "ex:KilogramMagnitude"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(results.contains(&json!("ex:mass1")), "mass1 has kg unit");
    assert!(
        !results.contains(&json!("ex:mass2")),
        "mass2 has different unit"
    );
}

#[tokio::test]
async fn owl2rl_some_values_from() {
    // Test: someValuesFrom restriction classification
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/cls-svf");

    // Insert schema with explicit @id for restriction
    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {"@id": "ex:Winery", "@type": "owl:Class"},
            {"@id": "ex:_restr1", "@type": "owl:Restriction",
             "owl:onProperty": {"@id": "ex:hasMaker"},
             "owl:someValuesFrom": {"@id": "ex:Winery"}},
            {"@id": "ex:Wine", "@type": "owl:Class",
             "owl:equivalentClass": {"@id": "ex:_restr1"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:winery1", "@type": "ex:Winery"},
            {"@id": "ex:textile-co", "@type": "ex:TextileFactory"},
            {"@id": "ex:wine1", "ex:hasMaker": {"@id": "ex:winery1"}},
            {"@id": "ex:shirt1", "ex:hasMaker": {"@id": "ex:textile-co"}}
        ]
    });
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    // Query: who is type Wine?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?s",
        "where": {"@id": "?s", "@type": "ex:Wine"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:wine1")),
        "wine1 has a Winery maker"
    );
    assert!(
        !results.contains(&json!("ex:shirt1")),
        "shirt1 maker is not a Winery"
    );
}

#[tokio::test]
async fn owl2rl_all_values_from() {
    // Test: allValuesFrom restriction inference
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/cls-avf");

    // Insert schema with explicit @id for restriction
    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {"@id": "ex:_restr1", "@type": "owl:Restriction",
             "owl:onProperty": {"@id": "ex:contains"},
             "owl:allValuesFrom": {"@id": "ex:Item"}},
            {"@id": "ex:Container", "@type": "owl:Class",
             "owl:equivalentClass": {"@id": "ex:_restr1"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:box1",
        "@type": "ex:Container",
        "ex:contains": {"@id": "ex:thing1"}
    });
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    // Query: what type is thing1?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?t",
        "where": {"@id": "ex:thing1", "@type": "?t"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:Item")),
        "thing1 should be inferred as Item"
    );
}

#[tokio::test]
async fn owl2rl_max_cardinality() {
    // Test: maxCardinality=1 restriction produces sameAs
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/cls-maxc");

    // Insert schema with explicit @id for restriction
    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {"@id": "ex:_restr1", "@type": "owl:Restriction",
             "owl:onProperty": {"@id": "ex:mother"},
             "owl:maxCardinality": 1},
            {"@id": "ex:Person", "@type": "owl:Class",
             "owl:equivalentClass": {"@id": "ex:_restr1"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:brian",
        "@type": "ex:Person",
        "ex:mother": [{"@id": "ex:carol"}, {"@id": "ex:carol2"}, {"@id": "ex:carol3"}]
    });
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    // Query: who is sameAs carol?
    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "select": "?same",
        "where": {"@id": "ex:carol", "owl:sameAs": "?same"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    // All three carols should be sameAs due to maxCardinality=1
    assert!(results.contains(&json!("ex:carol2")));
    assert!(results.contains(&json!("ex:carol3")));
}

#[tokio::test]
async fn owl2rl_intersection_of() {
    // Test: intersectionOf rule (cls-int1, cls-int2)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/cls-int");

    // Insert schema with explicit @id for class definition
    // Use rdf:first/rdf:rest for RDF list encoding
    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "@graph": [
            // Build RDF list manually: (_list1 first Woman; rest (_list2 first Parent; rest nil))
            {"@id": "ex:_list1", "rdf:first": {"@id": "ex:Woman"}, "rdf:rest": {"@id": "ex:_list2"}},
            {"@id": "ex:_list2", "rdf:first": {"@id": "ex:Parent"}, "rdf:rest": {"@id": "rdf:nil"}},
            {"@id": "ex:_cls1", "@type": "owl:Class", "owl:intersectionOf": {"@id": "ex:_list1"}},
            {"@id": "ex:Mother", "@type": "owl:Class", "owl:equivalentClass": {"@id": "ex:_cls1"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:carol", "@type": ["ex:Woman", "ex:Parent"]},
            {"@id": "ex:alice", "@type": "ex:Woman"},
            {"@id": "ex:jen", "@type": "ex:Mother"}
        ]
    });
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    // Query: who is type Mother?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?s",
        "where": {"@id": "?s", "@type": "ex:Mother"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:carol")),
        "carol has both Woman and Parent types"
    );
    assert!(
        results.contains(&json!("ex:jen")),
        "jen is explicitly Mother"
    );
    assert!(
        !results.contains(&json!("ex:alice")),
        "alice is only Woman, not Parent"
    );
}

#[tokio::test]
async fn owl2rl_union_of() {
    // Test: unionOf rule (cls-uni)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/cls-uni");

    // Insert schema with explicit @id for class definition using RDF list
    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "@graph": [
            {"@id": "ex:_list1", "rdf:first": {"@id": "ex:Mother"}, "rdf:rest": {"@id": "ex:_list2"}},
            {"@id": "ex:_list2", "rdf:first": {"@id": "ex:Father"}, "rdf:rest": {"@id": "rdf:nil"}},
            {"@id": "ex:_cls1", "@type": "owl:Class", "owl:unionOf": {"@id": "ex:_list1"}},
            {"@id": "ex:Parent", "@type": "owl:Class", "owl:equivalentClass": {"@id": "ex:_cls1"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:carol", "@type": "ex:Mother"},
            {"@id": "ex:bob", "@type": "ex:Father"},
            {"@id": "ex:alice", "@type": "ex:Woman"}
        ]
    });
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    // Query: who is type Parent?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?s",
        "where": {"@id": "?s", "@type": "ex:Parent"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:carol")),
        "carol is Mother (union member)"
    );
    assert!(
        results.contains(&json!("ex:bob")),
        "bob is Father (union member)"
    );
    assert!(
        !results.contains(&json!("ex:alice")),
        "alice is Woman (not in union)"
    );
}

#[tokio::test]
async fn owl2rl_one_of() {
    // Test: oneOf rule (cls-oo)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/cls-oo");

    // Insert schema with explicit @id for class definition using RDF list
    let schema = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "@graph": [
            {"@id": "ex:_list1", "rdf:first": {"@id": "ex:Red"}, "rdf:rest": {"@id": "ex:_list2"}},
            {"@id": "ex:_list2", "rdf:first": {"@id": "ex:Green"}, "rdf:rest": {"@id": "rdf:nil"}},
            {"@id": "ex:_cls1", "@type": "owl:Class", "owl:oneOf": {"@id": "ex:_list1"}},
            {"@id": "ex:RedOrGreen", "@type": "owl:Class", "owl:equivalentClass": {"@id": "ex:_cls1"}}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    // Note: Red, Green, Blue are already referenced in the list, so they exist
    // The oneOf rule should assign types to the enumerated individuals

    // Query: who is type RedOrGreen?
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?s",
        "where": {"@id": "?s", "@type": "ex:RedOrGreen"},
        "reasoning": "owl2rl"
    });

    let rows = support::query_jsonld(&fluree, &ledger1, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger1.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(results.contains(&json!("ex:Red")));
    assert!(results.contains(&json!("ex:Green")));
    assert!(!results.contains(&json!("ex:Blue")));
}

// =============================================================================
// Reasoning disabled test
// =============================================================================

#[tokio::test]
async fn owl2rl_disabled_shows_no_derived_facts() {
    // Test that without reasoning enabled, derived facts are not visible
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "owl2rl/disabled");

    let data = json!({
        "@context": {
            "ex": "http://example.org/",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {"@id": "ex:livesWith", "@type": ["owl:ObjectProperty", "owl:SymmetricProperty"]},
            {"@id": "ex:person-a", "ex:livesWith": {"@id": "ex:person-b"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &data).await.unwrap().ledger;

    // Query WITHOUT reasoning
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?x"],
        "where": {"@id": "ex:person-b", "ex:livesWith": "?x"},
        "reasoning": "none"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();

    // Without reasoning, person-b does NOT live with person-a (symmetric inference not applied)
    assert_eq!(rows, json!([]), "no results without reasoning");
}
