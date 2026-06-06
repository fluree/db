//! Inline ontology axioms via `opts.ontology` (top-level
//! `ontology` field on a JSON-LD query).
//!
//! The query supplies RDFS/OWL axioms inline; the reasoner parses
//! them into a `SchemaBundleFlakes` overlay and layers it on top
//! of any `f:schemaSource` configured on the ledger. The axioms
//! themselves never persist.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, normalize_rows};

#[tokio::test]
async fn inline_ontology_subclass_drives_rdfs_reasoning() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "test/inline-ontology/subclass:main");

    // Seed an instance whose type is a subclass of the target the
    // query asks for. With no schema axioms, the query won't see it.
    let seed = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id":   "ex:alice",
        "@type": "ex:Employee",
        "ex:name": "Alice"
    });
    let r = fluree.insert(ledger, &seed).await.expect("seed alice");
    let ledger = r.ledger;

    // Without inline ontology + reasoning: the query for ex:Person
    // misses alice (no subclass entailment is in the ledger).
    let baseline_q = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select":   "?name",
        "where":    {"@id": "?p", "@type": "ex:Person", "ex:name": "?name"}
    });
    let view = fluree
        .db("test/inline-ontology/subclass:main")
        .await
        .expect("load db");
    let rows = fluree
        .query(&view, &baseline_q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert!(
        normalize_rows(&rows).is_empty(),
        "without inline ontology + rdfs reasoning the query must not see alice"
    );

    // With inline ontology declaring ex:Employee rdfs:subClassOf
    // ex:Person, RDFS reasoning entails alice's type as ex:Person.
    let q = json!({
        "@context":  {"ex": "http://example.org/ns/"},
        "select":    "?name",
        "where":     {"@id": "?p", "@type": "ex:Person", "ex:name": "?name"},
        "reasoning": "rdfs",
        "ontology": {
            "@context": {
                "ex":   "http://example.org/ns/",
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
            },
            "@id":            "ex:Employee",
            "rdfs:subClassOf": {"@id": "ex:Person"}
        }
    });
    let rows = fluree
        .query(&view, &q)
        .await
        .expect("inline ontology query must succeed")
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);
    assert!(
        results.contains(&json!("Alice")),
        "inline rdfs:subClassOf must entail ex:Person; got: {results:?}"
    );
}

#[tokio::test]
async fn inline_ontology_does_not_persist_after_query() {
    // Run a query with inline ontology that would entail more
    // results; then run the same query without `ontology`. The
    // second query must NOT see the inline axioms — they were
    // transient.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "test/inline-ontology/transient:main");

    let seed = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id":   "ex:bob",
        "@type": "ex:Contractor",
        "ex:name": "Bob"
    });
    fluree.insert(ledger, &seed).await.expect("seed bob");

    let view = fluree
        .db("test/inline-ontology/transient:main")
        .await
        .expect("load db");
    let ledger = fluree
        .ledger("test/inline-ontology/transient:main")
        .await
        .expect("reload ledger");

    // Query 1: with inline ontology → bob counts as Person.
    let with_ontology = json!({
        "@context":  {"ex": "http://example.org/ns/"},
        "select":    "?name",
        "where":     {"@id": "?p", "@type": "ex:Person", "ex:name": "?name"},
        "reasoning": "rdfs",
        "ontology": {
            "@context": {
                "ex":   "http://example.org/ns/",
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
            },
            "@id":             "ex:Contractor",
            "rdfs:subClassOf": {"@id": "ex:Person"}
        }
    });
    let rows = fluree
        .query(&view, &with_ontology)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert!(
        normalize_rows(&rows).contains(&json!("Bob")),
        "first query with ontology should see bob"
    );

    // Query 2: no ontology → entailment is gone.
    let no_ontology = json!({
        "@context":  {"ex": "http://example.org/ns/"},
        "select":    "?name",
        "where":     {"@id": "?p", "@type": "ex:Person", "ex:name": "?name"},
        "reasoning": "rdfs"
    });
    let rows = fluree
        .query(&view, &no_ontology)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert!(
        normalize_rows(&rows).is_empty(),
        "without ontology on the second query the entailment must be gone"
    );
}
