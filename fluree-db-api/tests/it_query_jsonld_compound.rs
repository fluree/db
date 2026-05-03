//! JSON-LD compound query integration tests
//!

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{normalize_rows, seed_people_compound_dataset};

#[tokio::test]
async fn compound_two_tuple_select_with_crawl_and_values() {
    // Scenario: two-tuple-select-with-crawl + values variant
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people_compound_dataset(&fluree, "query/compound:main").await;

    // NOTE: Some clients return tuple rows mixing scalars + a crawled object.
    // Rust currently formats expansion selections as **objects only** (expansion output),
    // so we assert the crawled friends and keep a separate ignored parity test below.
    let q = json!({
        "@context": {"schema":"http://schema.org/","ex":"http://example.org/ns/"},
        "select": {"?f": ["*"]},
        "where": {
            "schema:name": "Cam",
            "ex:friend": {"@id":"?f", "schema:age":"?age"}
        }
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
            {"@id":"ex:alice","@type":"ex:User","schema:age":50,"schema:email":"alice@example.org","schema:name":"Alice","ex:favNums":[9,42,76]},
            {"@id":"ex:brian","@type":"ex:User","schema:age":50,"schema:email":"brian@example.org","schema:name":"Brian","ex:favNums":7}
        ]))
    );

    let q2 = json!({
        "@context": {"schema":"http://schema.org/","ex":"http://example.org/ns/"},
        "values": [["?name"], [["Cam"]]],
        "select": {"?f": ["*"]},
        "where": {
            "schema:name": "?name",
            "ex:friend": {"@id":"?f", "schema:age":"?age"}
        }
    });
    let rows2 = support::query_jsonld(&fluree, &ledger, &q2)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(normalize_rows(&rows2), normalize_rows(&rows));
}

#[tokio::test]
async fn compound_two_tuple_select_with_crawl_scalar_plus_object() {
    // Scenario: two-tuple-select-with-crawl + values variant
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people_compound_dataset(&fluree, "query/compound:tuple-crawl").await;

    let ctx = json!({"schema":"http://schema.org/","ex":"http://example.org/ns/"});
    let q = json!({
        "@context": ctx,
        "select": ["?age", {"?f": ["*"]}],
        "where": {
            "schema:name": "Cam",
            "ex:friend": {"@id":"?f", "schema:age":"?age"}
        }
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    let expected = json!([
        [50, {"@id":"ex:alice","@type":"ex:User","schema:age":50,"schema:email":"alice@example.org","schema:name":"Alice","ex:favNums":[9,42,76]}],
        [50, {"@id":"ex:brian","@type":"ex:User","schema:age":50,"schema:email":"brian@example.org","schema:name":"Brian","ex:favNums":7}]
    ]);
    assert_eq!(normalize_rows(&rows), normalize_rows(&expected));

    let q2 = json!({
        "@context": ctx,
        "values": [["?name"], [["Cam"]]],
        "select": ["?age", {"?f": ["*"]}],
        "where": {
            "schema:name": "?name",
            "ex:friend": {"@id":"?f", "schema:age":"?age"}
        }
    });
    let rows2 = support::query_jsonld(&fluree, &ledger, &q2)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(normalize_rows(&rows2), normalize_rows(&expected));
}

#[tokio::test]
async fn compound_passthrough_variables_and_select_one() {
    // Scenario: pass-through vars + selectOne
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people_compound_dataset(&fluree, "query/compound:passthrough").await;

    let q = json!({
        "@context": {"schema":"http://schema.org/","ex":"http://example.org/ns/"},
        "select": ["?name","?age","?email"],
        "where": {
            "schema:name": "Cam",
            "ex:friend": {"schema:name":"?name","schema:age":"?age","schema:email":"?email"}
        }
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([
            ["Alice", 50, "alice@example.org"],
            ["Brian", 50, "brian@example.org"]
        ]))
    );

    let q_one = json!({
        "@context": {"schema":"http://schema.org/","ex":"http://example.org/ns/"},
        "selectOne": ["?name","?age","?email"],
        "where": {
            "schema:name": "Cam",
            "ex:friend": {"schema:name":"?name","schema:age":"?age","schema:email":"?email"}
        }
    });
    let one = support::query_jsonld(&fluree, &ledger, &q_one)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    // SelectOne returns the first row (order not defined); assert it is one of the expected rows.
    assert!(
        one == json!(["Alice", 50, "alice@example.org"])
            || one == json!(["Brian", 50, "brian@example.org"])
    );
}

#[tokio::test]
async fn compound_multicard_duplicates_and_ordering() {
    // Scenario: multi-card results duplicate single-card values; ordering tests
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people_compound_dataset(&fluree, "query/compound:multicard").await;

    let q = json!({
        "@context": {"schema":"http://schema.org/","ex":"http://example.org/ns/"},
        "select": ["?name","?favNums"],
        "where": {"schema:name":"?name","ex:favNums":"?favNums"}
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([
            ["Alice", 9],
            ["Alice", 42],
            ["Alice", 76],
            ["Brian", 7],
            ["Cam", 5],
            ["Cam", 10]
        ]))
    );

    let q_asc = json!({
        "@context": {"schema":"http://schema.org/","ex":"http://example.org/ns/"},
        "select": ["?name","?favNums"],
        "where": {"schema:name":"?name","ex:favNums":"?favNums"},
        "orderBy": "?favNums"
    });
    let asc = support::query_jsonld(&fluree, &ledger, &q_asc)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        asc,
        json!([
            ["Cam", 5],
            ["Brian", 7],
            ["Alice", 9],
            ["Cam", 10],
            ["Alice", 42],
            ["Alice", 76]
        ])
    );

    let q_desc = json!({
        "@context": {"schema":"http://schema.org/","ex":"http://example.org/ns/"},
        "select": ["?name","?favNums"],
        "where": {"schema:name":"?name","ex:favNums":"?favNums"},
        "orderBy": "(desc ?favNums)"
    });
    let desc = support::query_jsonld(&fluree, &ledger, &q_desc)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        desc,
        json!([
            ["Alice", 76],
            ["Alice", 42],
            ["Cam", 10],
            ["Alice", 9],
            ["Brian", 7],
            ["Cam", 5]
        ])
    );

    let q_multi = json!({
        "@context": {"schema":"http://schema.org/","ex":"http://example.org/ns/"},
        "select": ["?name","?favNums"],
        "where": {"schema:name":"?name","ex:favNums":"?favNums"},
        "orderBy": ["?name", "(desc ?favNums)"]
    });
    let multi = support::query_jsonld(&fluree, &ledger, &q_multi)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        multi,
        json!([
            ["Alice", 76],
            ["Alice", 42],
            ["Alice", 9],
            ["Brian", 7],
            ["Cam", 10],
            ["Cam", 5]
        ])
    );
}

#[tokio::test]
async fn compound_group_by_multicard_without_aggregate() {
    // Scenario: group-by with a multicardinality value (no aggregate)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people_compound_dataset(&fluree, "query/compound:groupby").await;

    let q = json!({
        "@context": {"schema":"http://schema.org/","ex":"http://example.org/ns/"},
        "select": ["?name","?favNums"],
        "where": {"schema:name":"?name","ex:favNums":"?favNums"},
        "groupBy": ["?name"],
        "orderBy": "?name"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        rows,
        json!([["Alice", [9, 42, 76]], ["Brian", [7]], ["Cam", [5, 10]]])
    );
}

#[tokio::test]
async fn compound_s_p_o_and_object_subject_joins_with_expansion() {
    // Scenario: s/p/o check + object-subject joins
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people_compound_dataset(&fluree, "query/compound:spo").await;

    let q_spo = json!({
        "@context": {"schema":"http://schema.org/","ex":"http://example.org/ns/"},
        "select": ["?s","?p","?o"],
        "where": {"@id":"?s","schema:age":34,"?p":"?o"}
    });
    let spo = support::query_jsonld(&fluree, &ledger, &q_spo)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&spo),
        normalize_rows(&json!([
            // When predicate is a variable, rdf:type compacts to its full IRI (not "@type").
            [
                "ex:cam",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "ex:User"
            ],
            ["ex:cam", "schema:age", 34],
            ["ex:cam", "schema:email", "cam@example.org"],
            ["ex:cam", "schema:name", "Cam"],
            ["ex:cam", "ex:favNums", 5],
            ["ex:cam", "ex:favNums", 10],
            ["ex:cam", "ex:friend", "ex:alice"],
            ["ex:cam", "ex:friend", "ex:brian"]
        ]))
    );

    let q_join = json!({
        "@context": {"schema":"http://schema.org/","ex":"http://example.org/ns/"},
        "select": {"?s": ["*", {"ex:friend": ["*"]}]},
        "where": {"@id":"?s","ex:friend":{"schema:name":"Alice"}}
    });
    let joined = support::query_jsonld(&fluree, &ledger, &q_join)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    // Result should include Cam + expanded friends (order-insensitive).
    assert_eq!(
        normalize_rows(&joined),
        normalize_rows(&json!([{
            "@id":"ex:cam",
            "@type":"ex:User",
            "schema:name":"Cam",
            "schema:email":"cam@example.org",
            "schema:age":34,
            "ex:favNums":[5,10],
            "ex:friend":[
                {"@id":"ex:alice","@type":"ex:User","schema:name":"Alice","schema:email":"alice@example.org","schema:age":50,"ex:favNums":[9,42,76]},
                {"@id":"ex:brian","@type":"ex:User","schema:name":"Brian","schema:email":"brian@example.org","schema:age":50,"ex:favNums":7}
            ]
        }]))
    );
}
