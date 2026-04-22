//! Property / predicate integration tests
//!
//! We keep `@context` explicit and compare results order-insensitively when ordering is not defined.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};

async fn seed_subject_as_predicate(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let ctx = json!({
        "id": "@id",
        "schema": "http://schema.org/",
        "ex": "http://example.com/",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    });

    // Insert predicate IRIs as subjects first (matches intent: "iri-cache lookups")
    let db1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx,
                "@graph": [
                    {"@id":"ex:unlabeled-pred","ex:description":"created as a subject first"},
                    {"@id":"ex:labeled-pred","@type":"rdf:Property","ex:description":"created as a subject first, labelled as Property"}
                ]
            }),
        )
        .await
        .unwrap()
        .ledger;

    // Insert a node that uses those subjects as predicates (and nested object)
    fluree
        .insert(
            db1,
            &json!({
                "@context": ctx,
                "@graph": [
                    {
                        "@id": "ex:subject-as-predicate",
                        "ex:labeled-pred": "labeled",
                        "ex:unlabeled-pred": "unlabeled",
                        "ex:new-pred": {"@id":"ex:nested","ex:unlabeled-pred":"unlabeled-nested"}
                    }
                ]
            }),
        )
        .await
        .unwrap()
        .ledger
}

#[tokio::test]
async fn subjects_as_predicates_variable_predicate_scan() {
    // Scenario: subjects-as-predicates / "via variable selector"
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_subject_as_predicate(&fluree, "property:subject-as-predicate").await;

    let ctx = json!({"id":"@id","ex":"http://example.com/"});
    let q = json!({
        "@context": ctx,
        "select": ["?p"],
        "where": {"@id":"ex:subject-as-predicate","?p":"?o"}
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([
            "ex:labeled-pred",
            "ex:new-pred",
            "ex:unlabeled-pred"
        ]))
    );
}

#[tokio::test]
async fn subjects_as_predicates_reverse_crawl_without_star() {
    // Scenario: subjects-as-predicates / "via reverse no subgraph"
    //
    // NOTE: We intentionally avoid `["*"]` (graph crawl) here because select [*] graph crawl
    // parity is still in progress; this test validates reverse traversal + predicate-as-subject.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_subject_as_predicate(&fluree, "property:reverse-crawl").await;

    let base_ctx = json!({"id":"@id","ex":"http://example.com/"});
    let ctx = json!([base_ctx, {"ex:reversed-pred": {"@reverse": "ex:new-pred"}}]);

    let q = json!({
        "@context": ctx,
        // Include "*" so graph crawl formatter includes @id and we can assert the reverse edge
        // without relying on explicit "id" selection behavior.
        "select": {"ex:nested": ["*","ex:reversed-pred"]},
        // Our parser requires a WHERE clause; this is equivalent to selecting by subject id.
        "where": {"@id":"ex:nested"}
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();
    let arr = rows.as_array().expect("rows array");
    assert_eq!(arr.len(), 1);
    let obj = arr[0].as_object().expect("row object");
    let reversed = obj.get("ex:reversed-pred").expect("reverse key present");
    assert_eq!(reversed, &json!({"@id":"ex:subject-as-predicate"}));
}

#[tokio::test]
async fn equivalent_properties_equivalent_symmetric_transitive_and_graph_crawl() {
    // Scenario: equivalent-properties-test
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "query/equivalent-properties");

    // Seed equivalentProperty chain across 3 vocabularies.
    let schema = json!({
        "@context": {
            "vocab1": "http://vocab1.example.org/",
            "vocab2": "http://vocab2.example.org/",
            "vocab3": "http://vocab3.example.fr/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {"@id":"vocab1:givenName","@type":"rdf:Property"},
            {"@id":"vocab2:firstName","@type":"rdf:Property","owl:equivalentProperty":{"@id":"vocab1:givenName"}},
            {"@id":"vocab3:prenom","@type":"rdf:Property","owl:equivalentProperty":{"@id":"vocab2:firstName"}}
        ]
    });

    let ledger1 = fluree.insert(ledger0, &schema).await.unwrap().ledger;

    // Seed data using each equivalent property.
    let data = json!({
        "@context": {
            "vocab1": "http://vocab1.example.org/",
            "vocab2": "http://vocab2.example.org/",
            "vocab3": "http://vocab3.example.fr/",
            "ex": "http://example.org/ns/"
        },
        "@graph": [
            {"@id":"ex:brian","ex:age":50,"vocab1:givenName":"Brian"},
            {"@id":"ex:ben","vocab2:firstName":"Ben"},
            {"@id":"ex:francois","vocab3:prenom":"Francois"}
        ]
    });
    let ledger = fluree.insert(ledger1, &data).await.unwrap().ledger;

    // Querying for the property defined to be equivalent returns all values.
    let q1 = json!({
        "@context": {"vocab2":"http://vocab2.example.org/"},
        "select": ["?name"],
        "where": {"vocab2:firstName":"?name"},
        "reasoning": "owl2ql"
    });
    let rows1 = support::query_jsonld(&fluree, &ledger, &q1)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows1),
        normalize_rows(&json!(["Ben", "Brian", "Francois"]))
    );

    // Querying for the symmetric property.
    let q2 = json!({
        "@context": {"vocab1":"http://vocab1.example.org/"},
        "select": ["?name"],
        "where": {"vocab1:givenName":"?name"},
        "reasoning": "owl2ql"
    });
    let rows2 = support::query_jsonld(&fluree, &ledger, &q2)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows2),
        normalize_rows(&json!(["Ben", "Brian", "Francois"]))
    );

    // Querying for the transitive properties.
    let q3 = json!({
        "@context": {"vocab3":"http://vocab3.example.fr/"},
        "select": ["?name"],
        "where": {"vocab3:prenom":"?name"},
        "reasoning": "owl2ql"
    });
    let rows3 = support::query_jsonld(&fluree, &ledger, &q3)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows3),
        normalize_rows(&json!(["Ben", "Brian", "Francois"]))
    );

    // Querying with graph crawl.
    let q4 = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "vocab1": "http://vocab1.example.org/",
            "vocab2": "http://vocab2.example.org/",
            "vocab3": "http://vocab3.example.fr/"
        },
        "select": {"?s":["*"]},
        "where": {"@id":"?s","vocab2:firstName":"?name"},
        "reasoning": "owl2ql"
    });
    let rows4 = support::query_jsonld(&fluree, &ledger, &q4)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(
        normalize_rows(&rows4),
        normalize_rows(&json!([
            {"@id":"ex:ben","vocab2:firstName":"Ben"},
            {"@id":"ex:brian","vocab1:givenName":"Brian","ex:age":50},
            {"@id":"ex:francois","vocab3:prenom":"Francois"}
        ]))
    );
}

#[tokio::test]
async fn rdfs_subpropertyof_expansion() {
    // Scenario: rdfs-subpropertyof-test
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "query/rdfs-subpropertyof");

    // Seed property hierarchy.
    let insert1 = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        },
        "@graph": [
            {"@id":"ex:biologicalMother","@type":"rdf:Property","rdfs:subPropertyOf":[{"@id":"ex:mother"},{"@id":"ex:biologicalParent"}]},
            {"@id":"ex:biologicalFather","@type":"rdf:Property","rdfs:subPropertyOf":[{"@id":"ex:father"},{"@id":"ex:biologicalParent"}]}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &insert1).await.unwrap().ledger;

    let insert2 = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        },
        "@graph": [
            {"@id":"ex:biologicalParent","@type":"rdf:Property","rdfs:subPropertyOf":{"@id":"ex:parent"}},
            {"@id":"ex:stepParent","@type":"rdf:Property","rdfs:subPropertyOf":{"@id":"ex:parent"}},
            {"@id":"ex:father","@type":"rdf:Property","rdfs:subPropertyOf":{"@id":"ex:parent"}},
            {"@id":"ex:stepFather","@type":"rdf:Property","rdfs:subPropertyOf":{"@id":"ex:stepParent"}},
            {"@id":"ex:stepMother","@type":"rdf:Property","rdfs:subPropertyOf":{"@id":"ex:stepParent"}}
        ]
    });
    let ledger2 = fluree.insert(ledger1, &insert2).await.unwrap().ledger;

    // Seed equivalent property for stepDad -> stepFather (OWL).
    let insert3 = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "owl": "http://www.w3.org/2002/07/owl#"
        },
        "@graph": [
            {"@id":"ex:stepDad","@type":"rdf:Property","owl:equivalentProperty":{"@id":"ex:stepFather"}}
        ]
    });
    let ledger3 = fluree.insert(ledger2, &insert3).await.unwrap().ledger;

    // Seed data.
    let insert4 = json!({
        "@context": {"ex":"http://example.org/ns/"},
        "@id":"ex:bob",
        "ex:biologicalMother":{"@id":"ex:alice"},
        "ex:biologicalFather":{"@id":"ex:george"},
        "ex:stepFather":{"@id":"ex:john"},
        "ex:stepDad":{"@id":"ex:jerry"},
        "ex:stepMother":{"@id":"ex:mary"}
    });
    let ledger = fluree.insert(ledger3, &insert4).await.unwrap().ledger;

    // Querying one-level up in subproperty hierarchy.
    let q1 = json!({
        "@context": {"ex":"http://example.org/ns/"},
        "select": ["?parent"],
        "where": {"@id":"ex:bob","ex:biologicalParent":"?parent"}
        // relies on default auto-RDFS (hierarchy exists)
    });
    let rows1 = support::query_jsonld(&fluree, &ledger, &q1)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows1),
        normalize_rows(&json!(["ex:alice", "ex:george"]))
    );

    // Querying the top-level property which includes equivalent property stepDad.
    // Use owl2ql to ensure equivalentProperty contributes to expansion.
    let q2 = json!({
        "@context": {"ex":"http://example.org/ns/"},
        "select": ["?parent"],
        "where": {"@id":"ex:bob","ex:parent":"?parent"},
        "reasoning": "owl2ql"
    });
    let rows2 = support::query_jsonld(&fluree, &ledger, &q2)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows2),
        normalize_rows(&json!([
            "ex:alice",
            "ex:george",
            "ex:jerry",
            "ex:john",
            "ex:mary"
        ]))
    );

    // Sanity: explicit "none" disables auto-RDFS (so parent expansion disappears).
    let q3 = json!({
        "@context": {"ex":"http://example.org/ns/"},
        "select": ["?parent"],
        "where": {"@id":"ex:bob","ex:parent":"?parent"},
        "reasoning": "none"
    });
    let rows3 = support::query_jsonld(&fluree, &ledger, &q3)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(rows3, json!([]));
}

// owl:equivalentProperty behavior is covered in owl reasoning tests, not here.
