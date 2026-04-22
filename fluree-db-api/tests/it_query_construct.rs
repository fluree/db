//! CONSTRUCT integration tests
//!

mod support;

use fluree_db_api::{FlureeBuilder, LedgerState, Novelty};
use fluree_db_core::LedgerSnapshot;
use serde_json::{json, Map, Value as JsonValue};

fn context_people() -> JsonValue {
    json!({
        // Allow un-prefixed terms like "label"/"name"/"config"/"date"
        "@vocab": "http://example.org/",
        "person": "http://example.org/Person#",
        "ex": "http://example.org/",
        "foaf": "http://xmlns.com/foaf/0.1/",
        "schema": "http://schema.org/"
    })
}

fn people_data() -> JsonValue {
    json!([
        {"@id":"ex:jdoe","@type":"ex:Person","person:handle":"jdoe","person:fullName":"Jane Doe","person:favNums":[3,7,42,99]},
        {"@id":"ex:bbob","@type":"ex:Person","person:handle":"bbob","person:fullName":"Billy Bob","person:friend":{"@id":"ex:jbob"},"person:favNums":[23]},
        {"@id":"ex:jbob","@type":"ex:Person","person:handle":"jbob","person:friend":{"@id":"ex:fbueller"},"person:fullName":"Jenny Bob","person:favNums":[8,6,7,5,3,0,9]},
        {"@id":"ex:fbueller","@type":"ex:Person","person:handle":"dankeshön","person:fullName":"Ferris Bueller"},
        {"@id":"ex:alice","foaf:givenname":"Alice","foaf:family_name":"Hacker"},
        {"@id":"ex:bob","foaf:firstname":"Bob","foaf:surname":"Hacker"},
        {"@id":"ex:fran",
         "name":{"@value":"Francois","@language":"fr"},
         // Rust transact currently supports @json values only when @value is a string.
         // Expected CONSTRUCT output also stringifies the JSON.
         "config":{"@type":"@json","@value":"{\"paths\":[\"dev\",\"src\"]}"},
         "date":{"@value":"2020-10-20","@type":"http://www.w3.org/2001/XMLSchema#date"}}
    ])
}

async fn seed_people() -> (fluree_db_api::Fluree, LedgerState) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/construct:people";

    let db0 = LedgerSnapshot::genesis(ledger_id);
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let tx = json!({
        "@context": context_people(),
        "@graph": people_data()
    });

    let committed = fluree.insert(ledger0, &tx).await.expect("insert people");
    (fluree, committed.ledger)
}

fn normalize_construct(mut v: JsonValue) -> JsonValue {
    // Sort @graph entries by @id and sort any array values for stable comparison.
    let obj = v
        .as_object_mut()
        .expect("construct result must be an object");
    if let Some(JsonValue::Array(graph)) = obj.get_mut("@graph") {
        for node in graph.iter_mut() {
            if let JsonValue::Object(m) = node {
                for (_k, vv) in m.iter_mut() {
                    if let JsonValue::Array(arr) = vv {
                        arr.sort_by_key(std::string::ToString::to_string);
                    }
                }
            }
        }
        graph.sort_by(|a, b| {
            let aid = a.get("@id").and_then(|x| x.as_str()).unwrap_or("");
            let bid = b.get("@id").and_then(|x| x.as_str()).unwrap_or("");
            aid.cmp(bid)
        });
    }
    v
}

#[tokio::test]
async fn construct_basic() {
    let (fluree, ledger) = seed_people().await;
    let ctx = context_people();

    let query = json!({
        "@context": ctx,
        "where": [{"@id":"?s","person:fullName":"?fullName"}],
        "construct": [{"@id":"?s","label":"?fullName"}]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let actual = normalize_construct(result.to_construct(&ledger.snapshot).expect("to_construct"));

    let expected = normalize_construct(json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:bbob","label":["Billy Bob"]},
            {"@id":"ex:fbueller","label":["Ferris Bueller"]},
            {"@id":"ex:jbob","label":["Jenny Bob"]},
            {"@id":"ex:jdoe","label":["Jane Doe"]}
        ]
    }));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn construct_no_prefix_context_full_iris() {
    // We prefer explicit contexts.
    // This variant uses an empty @context and full IRIs in WHERE/CONSTRUCT.
    let (fluree, ledger) = seed_people().await;

    let query = json!({
        "@context": {},
        "where": [{"@id":"?s","http://example.org/Person#fullName":"?fullName"}],
        "construct": [{"@id":"?s","http://example.org/label":"?fullName"}]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let actual = normalize_construct(result.to_construct(&ledger.snapshot).expect("to_construct"));

    let expected = normalize_construct(json!({
        "@context": {},
        "@graph": [
            {"@id":"http://example.org/bbob","http://example.org/label":["Billy Bob"]},
            {"@id":"http://example.org/fbueller","http://example.org/label":["Ferris Bueller"]},
            {"@id":"http://example.org/jbob","http://example.org/label":["Jenny Bob"]},
            {"@id":"http://example.org/jdoe","http://example.org/label":["Jane Doe"]}
        ]
    }));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn construct_multiple_clauses() {
    let (fluree, ledger) = seed_people().await;

    // Include "id" aliasing for @id
    let mut ctx_map: Map<String, JsonValue> = context_people()
        .as_object()
        .expect("context object")
        .clone();
    ctx_map.insert("id".to_string(), JsonValue::String("@id".to_string()));
    let ctx = JsonValue::Object(ctx_map);

    let query = json!({
        "@context": ctx,
        "where": [
            {"@id":"?s","person:fullName":"?fullName"},
            {"@id":"?s","person:favNums":"?num"}
        ],
        "construct": [
            {"@id":"?s","name":"?fullName"},
            {"@id":"?s","num":"?num"}
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let actual = normalize_construct(result.to_construct(&ledger.snapshot).expect("to_construct"));

    let expected = normalize_construct(json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:bbob","name":["Billy Bob"],"num":[23]},
            {"@id":"ex:jbob","name":["Jenny Bob"],"num":[0,3,5,6,7,8,9]},
            {"@id":"ex:jdoe","name":["Jane Doe"],"num":[3,7,42,99]}
        ]
    }));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn construct_multiple_clauses_different_subjects() {
    let (fluree, ledger) = seed_people().await;
    let ctx = context_people();

    let query = json!({
        "@context": ctx,
        "where": [
            {"@id":"?s","person:fullName":"?fullName"},
            {"@id":"?s","person:friend":"?friend"},
            {"@id":"?friend","person:fullName":"?friendName"},
            {"@id":"?friend","person:favNums":"?friendNum"}
        ],
        "construct": [
            {"@id":"?s","myname":"?fullName"},
            {"@id":"?s","friendname":"?friendName"},
            {"@id":"?friend","name":"?friendName"},
            {"@id":"?friend","num":"?friendNum"}
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let actual = normalize_construct(result.to_construct(&ledger.snapshot).expect("to_construct"));

    let expected = normalize_construct(json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:bbob","myname":["Billy Bob"],"friendname":["Jenny Bob"]},
            {"@id":"ex:jbob","name":["Jenny Bob"],"num":[0,3,5,6,7,8,9]}
        ]
    }));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn construct_at_type_values_are_unwrapped() {
    let (fluree, ledger) = seed_people().await;
    let ctx = context_people();

    let query = json!({
        "@context": ctx,
        "where": [{"@id":"?s","@type":"?o"}],
        "construct": [{"@id":"?s","@type":"?o"}]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let actual = normalize_construct(result.to_construct(&ledger.snapshot).expect("to_construct"));

    // Only the 4 ex:Person nodes have @type
    let expected = normalize_construct(json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:bbob","@type":"Person"},
            {"@id":"ex:fbueller","@type":"Person"},
            {"@id":"ex:jbob","@type":"Person"},
            {"@id":"ex:jdoe","@type":"Person"}
        ]
    }));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn construct_class_patterns_in_template() {
    let (fluree, ledger) = seed_people().await;
    let ctx = context_people();

    let query = json!({
        "@context": ctx,
        "where": [{"@id":"?s","@type":"ex:Person"}],
        "construct": [{"@id":"?s","@type":"ex:Human"}]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let actual = normalize_construct(result.to_construct(&ledger.snapshot).expect("to_construct"));

    let expected = normalize_construct(json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:bbob","@type":"Human"},
            {"@id":"ex:fbueller","@type":"Human"},
            {"@id":"ex:jbob","@type":"Human"},
            {"@id":"ex:jdoe","@type":"Human"}
        ]
    }));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn construct_id_only_patterns_produce_no_triples() {
    let (fluree, ledger) = seed_people().await;
    let ctx = context_people();

    let query = json!({
        "@context": ctx,
        "where": [{"@id":"?s","@type":"ex:Person"}],
        "construct": [{"@id":"?s"}]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let actual = normalize_construct(result.to_construct(&ledger.snapshot).expect("to_construct"));

    let expected = normalize_construct(json!({
        "@context": ctx,
        "@graph": []
    }));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn construct_unbound_vars_are_not_included() {
    let (fluree, ledger) = seed_people().await;
    let ctx = context_people();

    let query = json!({
        "@context": ctx,
        "where": [
            {"@id":"?s","?p":"?o"},
            ["optional", {"@id":"?s","@type":"?type"}],
            ["optional", {"@id":"?s","foaf:givenname":"?name"}]
        ],
        "construct": [{"@id":"?s","name":"?name","@type":"?type"}]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let actual = normalize_construct(result.to_construct(&ledger.snapshot).expect("to_construct"));

    let expected = normalize_construct(json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:alice","name":["Alice"]},
            {"@id":"ex:bbob","@type":"Person"},
            {"@id":"ex:fbueller","@type":"Person"},
            {"@id":"ex:jbob","@type":"Person"},
            {"@id":"ex:jdoe","@type":"Person"}
        ]
    }));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn construct_value_metadata_displays() {
    // Scenario: "value metadata displays" (language tags + xsd:date + @json)
    let (fluree, ledger) = seed_people().await;
    let ctx = context_people();

    let query = json!({
        "@context": ctx,
        "where": [
            {"@id":"?s","config":"?config"},
            {"@id":"?s","name":"?name"},
            {"@id":"?s","date":"?date"}
        ],
        "construct": [
            {"@id":"?s","json":"?config"},
            {"@id":"?s","name":"?name"},
            {"@id":"?s","date":"?date"}
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let actual = normalize_construct(result.to_construct(&ledger.snapshot).expect("to_construct"));

    // Note: Rust formats dates as typed strings.
    let expected = normalize_construct(json!({
        "@context": ctx,
        "@graph": [{
            "@id": "ex:fran",
            // Note: Rust emits RDF 1.1 JSON datatype IRI (rdf:JSON) in output formatting.
            "json": [{"@value":"{\"paths\":[\"dev\",\"src\"]}","@type":"http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON"}],
            "name": [{"@value":"Francois","@language":"fr"}],
            "date": [{"@value":"2020-10-20","@type":"http://www.w3.org/2001/XMLSchema#date"}]
        }]
    }));

    assert_eq!(actual, expected);
}
