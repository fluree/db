//! Reverse predicate integration tests
//!
//! We focus first on reverse predicates **in WHERE** (query semantics).
//! Graph crawl output using reverse selections and policy wrapping are included but ignored for now.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};

async fn seed_reverse_friends(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let ctx = json!({
        "id": "@id",
        "type": "@type",
        "schema": "http://schema.org/",
        "ex": "http://example.org/ns/"
    });

    fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx,
                "@graph": [
                    {"@id":"ex:brian","@type":"ex:User","schema:name":"Brian","ex:friend":[{"@id":"ex:alice"}]},
                    {"@id":"ex:alice","@type":"ex:User","schema:name":"Alice"},
                    {"@id":"ex:cam","@type":"ex:User","schema:name":"Cam","ex:friend":[{"@id":"ex:brian"},{"@id":"ex:alice"}]}
                ]
            }),
        )
        .await
        .unwrap()
        .ledger
}

async fn seed_reverse_family(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let ctx = json!({
        "id": "@id",
        "type": "@type",
        "ex": "http://example.org/ns/"
    });

    fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx,
                "@graph": [
                    {"@id":"ex:dad","@type":"ex:Person","ex:name":"Dad","ex:child":{"@id":"ex:kid"}},
                    {"@id":"ex:mom","@type":"ex:Person","ex:name":"Mom","ex:child":{"@id":"ex:kid"}},
                    {"@id":"ex:kid","@type":"ex:Person","ex:name":"Kiddo"},
                    {"@id":"ex:school","@type":"ex:Organization","ex:student":{"@id":"ex:kid"}}
                ]
            }),
        )
        .await
        .unwrap()
        .ledger
}

#[tokio::test]
async fn reverse_predicate_in_where_selects_inverse_edges() {
    // Scenario: context-reverse-test (adapted: WHERE-based assertion, avoids reverse graph crawl formatting)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_reverse_friends(&fluree, "reverse:friends").await;

    let ctx = json!({
        "schema":"http://schema.org/",
        "ex":"http://example.org/ns/",
        "friended": {"@reverse": "ex:friend"}
    });

    // Who has friended Brian? (i.e. ?who --ex:friend--> ex:brian)
    let q = json!({
        "@context": ctx,
        "select": ["?name","?who"],
        "distinct": true,
        "where": [
            {"@id":"ex:brian","schema:name":"?name"},
            {"@id":"ex:brian","friended":"?who"}
        ]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([["Brian", "ex:cam"]]))
    );
}

#[tokio::test]
async fn reverse_predicate_in_where_finds_kid() {
    // Scenario: reverse-preds-in-where-and-select / "where clause" (adapted: avoid graph crawl selector)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_reverse_family(&fluree, "reverse:family").await;

    let q = json!({
        "@context": {
            "ex":"http://example.org/ns/",
            "parent":{"@reverse":"ex:child"}
        },
        "where": {"@id":"?s","parent":"?x"},
        "select": "?s",
        "distinct": true
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(normalize_rows(&rows), normalize_rows(&json!(["ex:kid"])));
}

#[tokio::test]
async fn reverse_at_type_in_where_finds_classes() {
    // Scenario: reverse-preds-in-where-and-select / "@type reverse"
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_reverse_family(&fluree, "reverse:type").await;

    let q = json!({
        "@context": {
            "ex":"http://example.org/ns/",
            "isTypeObject":{"@reverse":"@type"}
        },
        "where": {"@id":"?class","isTypeObject":"?x"},
        "select": "?class",
        "distinct": true
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!(["ex:Person", "ex:Organization"]))
    );
}

#[tokio::test]
async fn forward_at_type_in_where_finds_classes() {
    // Scenario: reverse-preds-in-where-and-select / "@type forward"
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_reverse_family(&fluree, "reverse:type-forward").await;

    let q = json!({
        "@context": {"ex":"http://example.org/ns/"},
        "where": {"@id":"?x","@type":"?class"},
        "select": "?class",
        "distinct": true
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!(["ex:Person", "ex:Organization"]))
    );
}

#[tokio::test]
async fn context_reverse_select_one_graph_crawl() {
    // Scenario: context-reverse-test
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_reverse_friends(&fluree, "reverse-friends-graph-crawl:main").await;

    // 1) single reverse edge, no container
    let q1 = json!({
        "@context": [
            {"schema":"http://schema.org/","ex":"http://example.org/ns/"},
            {"friended": {"@reverse": "ex:friend"}}
        ],
        "select": {"ex:brian": ["schema:name","friended"]}
    });

    let r1 = support::query_jsonld(&fluree, &ledger, &q1)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    assert_eq!(
        r1,
        json!([{
            "schema:name":"Brian",
            "friended": {"@id":"ex:cam"}
        }])
    );

    // 2) force set container for reverse field
    let q2 = json!({
        "@context": [
            {"schema":"http://schema.org/","ex":"http://example.org/ns/"},
            {"friended": {"@reverse": "ex:friend", "@container":"@set"}}
        ],
        "select": {"ex:brian": ["schema:name","friended"]}
    });

    let r2 = support::query_jsonld(&fluree, &ledger, &q2)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    assert_eq!(
        r2,
        json!([{
            "schema:name":"Brian",
            "friended": [{"@id":"ex:cam"}]
        }])
    );

    // 3) multiple reverse edges yields array
    let q3 = json!({
        "@context": [
            {"schema":"http://schema.org/","ex":"http://example.org/ns/"},
            {"friended": {"@reverse": "ex:friend"}}
        ],
        "select": {"ex:alice": ["schema:name","friended"]}
    });

    let r3 = support::query_jsonld(&fluree, &ledger, &q3)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    assert_eq!(
        r3,
        json!([{
            "schema:name":"Alice",
            "friended": [{"@id":"ex:brian"},{"@id":"ex:cam"}]
        }])
    );
}

#[tokio::test]
async fn reverse_predicate_in_where_selects_parents() {
    // Same dataset as reverse-preds-in-where-and-select, but assert the inverse edges directly.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_reverse_family(&fluree, "reverse:family2").await;

    let ctx = json!({
        "ex":"http://example.org/ns/",
        "parent": {"@reverse":"ex:child"}
    });

    let q = json!({
        "@context": ctx,
        "select": "?parent",
        "distinct": true,
        "where": {"@id":"ex:kid","parent":"?parent"}
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!(["ex:mom", "ex:dad"]))
    );
}

#[tokio::test]
async fn type_reverse_and_forward_agree_on_classes() {
    // Scenario: reverse-preds-in-where-and-select / "@type reverse" + "@type forward"
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_reverse_family(&fluree, "reverse:types").await;

    let q_reverse = json!({
        "@context": {"ex":"http://example.org/ns/","isTypeObject":{"@reverse":"@type"}},
        "select": "?class",
        "distinct": true,
        "where": {"@id":"?class","isTypeObject":"?x"}
    });
    let q_forward = json!({
        "@context": {"ex":"http://example.org/ns/"},
        "select": "?class",
        "distinct": true,
        "where": {"@id":"?x","@type":"?class"}
    });

    let rows_reverse = support::query_jsonld(&fluree, &ledger, &q_reverse)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let rows_forward = support::query_jsonld(&fluree, &ledger, &q_forward)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();

    assert_eq!(
        normalize_rows(&rows_reverse),
        normalize_rows(&json!(["ex:Person", "ex:Organization"]))
    );
    assert_eq!(
        normalize_rows(&rows_forward),
        normalize_rows(&json!(["ex:Person", "ex:Organization"]))
    );
}

#[tokio::test]
async fn inline_reverse_key_in_graph_crawl_top_level() {
    // The AST documents {"@reverse:friended": ["*"]} as inline reverse-in-select
    // syntax. Verify it works without needing a context alias.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_reverse_friends(&fluree, "reverse-friends-inline:top").await;

    let q = json!({
        "@context": {"schema":"http://schema.org/","ex":"http://example.org/ns/"},
        "select": {"ex:brian": ["schema:name", {"@reverse:ex:friend": ["@id"]}]}
    });

    let r = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    assert_eq!(
        r,
        json!([{
            "schema:name":"Brian",
            "ex:friend": {"@id":"ex:cam"}
        }])
    );
}

#[tokio::test]
async fn inline_reverse_key_nested_inside_forward_property() {
    // Nested inline @reverse under a forward property: expand cam's friends,
    // then from each friend follow the inline reverse edge back to its friender.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_reverse_friends(&fluree, "reverse-friends-inline:nested").await;

    let q = json!({
        "@context": {"schema":"http://schema.org/","ex":"http://example.org/ns/"},
        "select": {"ex:cam": [
            "schema:name",
            {"ex:friend": ["schema:name", {"@reverse:ex:friend": ["@id"]}]}
        ]}
    });

    let r = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    // Brian has one friender (Cam); Alice has two frienders (Brian, Cam).
    // Nested crawl objects carry @id automatically.
    assert_eq!(
        r,
        json!([{
            "schema:name":"Cam",
            "ex:friend": [
                {"@id":"ex:alice","schema:name":"Alice","ex:friend": [{"@id":"ex:brian"},{"@id":"ex:cam"}]},
                {"@id":"ex:brian","schema:name":"Brian","ex:friend": {"@id":"ex:cam"}}
            ]
        }])
    );
}

#[tokio::test]
async fn reverse_alias_does_not_rewrite_forward_predicate() {
    // Bug: when the context defines `friendOf: {"@reverse": "ex:friend"}` and a node
    // has a FORWARD ex:friend edge, the forward predicate must stay as its own
    // compact form (ex:friend) — not be rewritten to the reverse alias.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_reverse_friends(&fluree, "reverse-friends-noleak:main").await;

    // Brian has a forward ex:friend -> alice and is friended by cam.
    let q = json!({
        "@context": [
            {"schema":"http://schema.org/","ex":"http://example.org/ns/"},
            {"friendOf": {"@reverse": "ex:friend"}}
        ],
        "select": {"ex:brian": ["schema:name", "ex:friend", "friendOf"]}
    });

    let r = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    let obj = r.as_array().and_then(|a| a.first()).unwrap();
    let map = obj.as_object().unwrap();
    assert!(
        map.contains_key("ex:friend"),
        "forward ex:friend predicate must keep its own name, got: {obj}"
    );
    assert_eq!(map.get("ex:friend"), Some(&json!({"@id":"ex:alice"})));
    assert_eq!(map.get("friendOf"), Some(&json!({"@id":"ex:cam"})));
}
