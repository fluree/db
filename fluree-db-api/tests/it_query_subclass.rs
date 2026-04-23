//! Subclass query integration tests
//!
//! These tests depend on subclass reasoning (`rdfs:subClassOf` hierarchy) being applied
//! during `@type` matching.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};
use tempfile::TempDir;

async fn seed_schema_creative_work(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    // Seed a Movie + Book instance, plus schema class hierarchy:
    // Book subClassOf CreativeWork
    // Movie subClassOf CreativeWork
    // CreativeWork subClassOf Thing
    // Use fully-expanded IRIs for transact (avoids JSON-LD prefix expansion differences).
    let insert1 = json!({
        "@graph": [
            {
                "@id": "https://www.wikidata.org/wiki/Q42",
                "@type":"https://schema.org/Person",
                "https://schema.org/name":"Douglas Adams"
            },
            {
                "@id": "https://www.wikidata.org/wiki/Q3107329",
                "@type": ["https://schema.org/Book"],
                "https://schema.org/name": "The Hitchhiker's Guide to the Galaxy",
                "https://schema.org/isbn": "0-330-25864-8",
                "https://schema.org/author": {"@id":"https://www.wikidata.org/wiki/Q42"}
            },
            {
                "@id": "https://www.wikidata.org/wiki/Q836821",
                "@type": ["https://schema.org/Movie"],
                "https://schema.org/name": "The Hitchhiker's Guide to the Galaxy",
                "https://schema.org/disambiguatingDescription": "2005 British-American comic science fiction film directed by Garth Jennings",
                "https://schema.org/titleEIDR": "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                "https://schema.org/isBasedOn": {"@id":"https://www.wikidata.org/wiki/Q3107329"}
            }
        ]
    });
    let db1 = fluree.insert(ledger0, &insert1).await.unwrap().ledger;

    let insert2 = json!({
        "@id": "https://schema.org/CreativeWork",
        "@type": "http://www.w3.org/2000/01/rdf-schema#Class",
        "http://www.w3.org/2000/01/rdf-schema#comment": "The most generic kind of creative work, including books, movies, photographs, software programs, etc.",
        "http://www.w3.org/2000/01/rdf-schema#label": "CreativeWork",
        "http://www.w3.org/2000/01/rdf-schema#subClassOf": {"@id":"https://schema.org/Thing"}
    });
    let db2 = fluree
        .update(
            db1,
            &json!({
                "insert": insert2
            }),
        )
        .await
        .unwrap()
        .ledger;

    let insert3 = json!({
        "@graph": [
            {"@id":"https://schema.org/Book","http://www.w3.org/2000/01/rdf-schema#subClassOf":{"@id":"https://schema.org/CreativeWork"}},
            {"@id":"https://schema.org/Movie","http://www.w3.org/2000/01/rdf-schema#subClassOf":{"@id":"https://schema.org/CreativeWork"}}
        ]
    });
    fluree
        .update(
            db2,
            &json!({
                "insert": insert3
            }),
        )
        .await
        .unwrap()
        .ledger
}

#[tokio::test]
async fn subclass_creative_work_returns_book_and_movie_instances() {
    // Scenario: subclass-test
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_schema_creative_work(&fluree, "query/subclass:main").await;

    // Sanity: we can find the movie directly by its concrete @type.
    let q_any = json!({
        "select": ["?p","?o"],
        "where": {"@id":"https://www.wikidata.org/wiki/Q836821","?p":"?o"}
    });
    let any = support::query_jsonld(&fluree, &ledger, &q_any)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert!(
        !any.as_array().unwrap().is_empty(),
        "expected some triples for wiki/Q836821, got none"
    );

    let q_types = json!({
        "select": "?t",
        "where": [{"@id":"https://www.wikidata.org/wiki/Q836821","@type":"?t"}]
    });
    let types = support::query_jsonld(&fluree, &ledger, &q_types)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert!(
        !types.as_array().unwrap().is_empty(),
        "expected at least one rdf:type for wiki/Q836821"
    );

    let q_movie = json!({
        "select": "?s",
        "where": {"@id":"?s","@type":"https://schema.org/Movie"}
    });
    let movie_rows = support::query_jsonld(&fluree, &ledger, &q_movie)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(movie_rows, json!(["https://www.wikidata.org/wiki/Q836821"]));

    let q = json!({
        "select": {"?s": ["*"]},
        "where": {"@id":"?s","@type":"https://schema.org/CreativeWork"}
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([
            {
                "@id":"https://www.wikidata.org/wiki/Q3107329",
                "@type":"https://schema.org/Book",
                "https://schema.org/name":"The Hitchhiker's Guide to the Galaxy",
                "https://schema.org/isbn":"0-330-25864-8",
                "https://schema.org/author":{"@id":"https://www.wikidata.org/wiki/Q42"}
            },
            {
                "@id":"https://www.wikidata.org/wiki/Q836821",
                "@type":"https://schema.org/Movie",
                "https://schema.org/name":"The Hitchhiker's Guide to the Galaxy",
                "https://schema.org/disambiguatingDescription":"2005 British-American comic science fiction film directed by Garth Jennings",
                "https://schema.org/titleEIDR":"10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
                "https://schema.org/isBasedOn":{"@id":"https://www.wikidata.org/wiki/Q3107329"}
            }
        ]))
    );
}

async fn seed_humanoid(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let ctx = json!({
        "id":"@id",
        "type":"@type",
        "ex":"http://example.org/ns/",
        "schema":"http://schema.org/",
        "rdfs":"http://www.w3.org/2000/01/rdf-schema#"
    });

    let insert_people = json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:freddy","@type":"ex:Yeti","schema:name":"Freddy"},
            {"@id":"ex:letty","@type":"ex:Yeti","schema:name":"Leticia"},
            {"@id":"ex:betty","@type":"ex:Yeti","schema:name":"Betty"},
            {"@id":"ex:andrew","@type":"schema:Person","schema:name":"Andrew Johnson"}
        ]
    });
    let db1 = fluree.insert(ledger0, &insert_people).await.unwrap().ledger;

    let insert_schema = json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:Humanoid","@type":"rdfs:Class"},
            {"@id":"ex:Yeti","rdfs:subClassOf":{"@id":"ex:Humanoid"}},
            {"@id":"schema:Person","rdfs:subClassOf":{"@id":"ex:Humanoid"}}
        ]
    });
    fluree
        .update(db1, &json!({"insert": insert_schema}))
        .await
        .unwrap()
        .ledger
}

#[tokio::test]
async fn subclass_inferencing_issue_core_48() {
    // Scenario: subclass-inferencing-test
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_humanoid(&fluree, "query/subclass:infer").await;

    let q = json!({
        "@context": {
            "id":"@id",
            "type":"@type",
            "ex":"http://example.org/ns/",
            "schema":"http://schema.org/"
        },
        "where": {"@id":"?s","@type":"ex:Humanoid"},
        "select": {"?s":["*"]}
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([
            {"@id":"ex:andrew","@type":"schema:Person","schema:name":"Andrew Johnson"},
            {"@id":"ex:betty","@type":"ex:Yeti","schema:name":"Betty"},
            {"@id":"ex:freddy","@type":"ex:Yeti","schema:name":"Freddy"},
            {"@id":"ex:letty","@type":"ex:Yeti","schema:name":"Leticia"}
        ]))
    );
}

#[tokio::test]
async fn subclass_inferencing_after_load_issue_core_48() {
    // Scenario: subclass-inferencing-after-load-test
    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap();

    let fluree = FlureeBuilder::file(storage_path)
        .build()
        .expect("build file fluree");
    let ledger_id = "subclass-inferencing-test:main";

    let ctx = json!({
        "id":"@id",
        "type":"@type",
        "ex":"http://example.org/ns/",
        "schema":"http://schema.org/",
        "rdfs":"http://www.w3.org/2000/01/rdf-schema#"
    });

    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();
    let insert_people = json!({
        "@context": ctx,
        "insert": [
            {"@id":"ex:freddy","@type":"ex:Yeti","schema:name":"Freddy"},
            {"@id":"ex:letty","@type":"ex:Yeti","schema:name":"Leticia"},
            {"@id":"ex:betty","@type":"ex:Yeti","schema:name":"Betty"},
            {"@id":"ex:andrew","@type":"schema:Person","schema:name":"Andrew Johnson"}
        ]
    });
    let ledger1 = fluree.update(ledger0, &insert_people).await.unwrap().ledger;

    let insert_schema = json!({
        "@context": ctx,
        "insert": [
            {"@id":"ex:Humanoid","@type":"rdfs:Class"},
            {"@id":"ex:Yeti","rdfs:subClassOf":{"@id":"ex:Humanoid"}},
            {"@id":"schema:Person","rdfs:subClassOf":{"@id":"ex:Humanoid"}}
        ]
    });
    let _ledger2 = fluree.update(ledger1, &insert_schema).await.unwrap().ledger;

    let fluree2 = FlureeBuilder::file(storage_path)
        .build()
        .expect("build file fluree2");
    let loaded = fluree2.ledger(ledger_id).await.unwrap();

    let q = json!({
        "@context": ctx,
        "where": {"@id":"?s","@type":"ex:Humanoid"},
        "select": {"?s":["*"]}
    });
    let rows = support::query_jsonld(&fluree2, &loaded, &q)
        .await
        .unwrap()
        .to_jsonld_async(loaded.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([
            {"@id":"ex:andrew","@type":"schema:Person","schema:name":"Andrew Johnson"},
            {"@id":"ex:betty","@type":"ex:Yeti","schema:name":"Betty"},
            {"@id":"ex:freddy","@type":"ex:Yeti","schema:name":"Freddy"},
            {"@id":"ex:letty","@type":"ex:Yeti","schema:name":"Leticia"}
        ]))
    );
}

#[tokio::test]
async fn subclass_nested_stages() {
    // Scenario: subclass-nested-stages
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "query/subclass:nested-stages");

    let db1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex":"http://example.org/"},
                "@graph": [
                    {"@id":"ex:brian","@type":"ex:Person","ex:name":"Brian"},
                    {"@id":"ex:laura","@type":"ex:Employee","ex:name":"Laura"},
                    {"@id":"ex:alice","@type":"ex:Human","ex:name":"Alice"}
                ]
            }),
        )
        .await
        .unwrap()
        .ledger;

    let db2 = fluree
        .update(
            db1,
            &json!({
                "@context": {"ex":"http://example.org/","rdfs":"http://www.w3.org/2000/01/rdf-schema#"},
                "insert": [{"@id":"ex:Person","rdfs:subClassOf":{"@id":"ex:Human"}}]
            }),
        )
        .await
        .unwrap()
        .ledger;

    let db3 = fluree
        .update(
            db2,
            &json!({
                "@context": {"ex":"http://example.org/","rdfs":"http://www.w3.org/2000/01/rdf-schema#"},
                "insert": [{"@id":"ex:Employee","rdfs:subClassOf":{"@id":"ex:Person"}}]
            }),
        )
        .await
        .unwrap()
        .ledger;

    let q = json!({
        "@context": {"ex":"http://example.org/"},
        "select": "?s",
        "where": {"@id":"?s","@type":"ex:Human"}
    });
    let rows = support::query_jsonld(&fluree, &db3, &q)
        .await
        .unwrap()
        .to_jsonld(&db3.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!(["ex:alice", "ex:brian", "ex:laura"]))
    );
}
