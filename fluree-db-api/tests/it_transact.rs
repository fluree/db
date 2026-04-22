//! Transact integration tests
//!
//! Tests core transaction functionality including validation, data types, and API behavior.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::normalize_rows;

// Helper function to create a standard context
fn default_context() -> serde_json::Value {
    support::default_context()
}

#[tokio::test]
async fn staging_data_invalid_transactions() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Test 1: invalid transaction - insert with only @id (no properties)
    let ledger1 = fluree.create_ledger("tx/staging1:main").await.unwrap();
    let invalid_txn = json!({
        "@context": [default_context(), {"ex": "http://example.org/ns/"}],
        "insert": {"@id": "ex:alice"}
    });

    let result = fluree.update(ledger1, &invalid_txn).await;
    assert!(result.is_err(), "Should reject transaction with only @id");

    // Test 2: invalid transaction - empty insert
    let ledger2 = fluree.create_ledger("tx/staging2:main").await.unwrap();
    let empty_txn = json!({
        "@context": [default_context(), {"ex": "http://example.org/ns/"}],
        "insert": {}
    });

    let result = fluree.update(ledger2, &empty_txn).await;
    assert!(result.is_err(), "Should reject empty insert");

    // Test 3: empty node in insert array
    let ledger3 = fluree.create_ledger("tx/staging3:main").await.unwrap();
    let empty_node_txn = json!({
        "@context": [default_context(), {"ex": "http://example.org/ns/"}],
        "insert": [
            {"@id": "ex:alice", "schema:name": "Alice"},
            {}
        ]
    });

    // This should succeed but with warnings about empty nodes
    let result = fluree.update(ledger3, &empty_node_txn).await;
    assert!(
        result.is_ok(),
        "Should allow empty nodes with other valid data"
    );
}

#[tokio::test]
async fn staging_data_allow_false_values() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("tx/bools:main").await.unwrap();

    // Test allowing `false` values in transactions
    let txn = json!({
        "@context": [default_context(), {"ex": "http://example.org/ns/"}],
        "insert": {"@id": "ex:alice", "ex:isCool": false}
    });

    let ledger1 = fluree.update(ledger0, &txn).await.unwrap().ledger;

    // Query to verify false value was stored
    let query = json!({
        "@context": [default_context(), {"ex": "http://example.org/ns/"}],
        "select": ["?s", "?p", "?o"],
        "where": {"@id": "?s", "?p": "?o"}
    });

    let result = support::query_jsonld(&fluree, &ledger1, &query)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger1.snapshot).unwrap();

    assert_eq!(jsonld, json!([["ex:alice", "ex:isCool", false]]));
}

#[tokio::test]
async fn staging_data_mixed_data_types() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("tx/mixed-dts:main").await.unwrap();

    // Test mixed data types: ref & string. IRI references must be explicit
    // via `{"@id": ...}` — bare strings are always literals.
    let txn = json!({
        "@context": [default_context(), {"ex": "http://example.org/ns/"}],
        "insert": {
            "@id": "ex:brian",
            "ex:favCoffeeShop": [{"@id": "wiki:Q37158"}, "Clemmons Coffee"]
        }
    });

    let ledger1 = fluree.update(ledger0, &txn).await.unwrap().ledger;

    // Query directly using the updated ledger (transactions auto-commit)
    let ledger2 = ledger1;

    let query = json!({
        "@context": [default_context(), {"ex": "http://example.org/ns/"}],
        "select": {"ex:brian": ["*"]}
    });

    let result = support::query_jsonld(&fluree, &ledger2, &query)
        .await
        .unwrap();
    let jsonld = result
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();

    // Should return mixed array with ref and string
    let brian = &jsonld.as_array().unwrap()[0];
    let coffee_shops = brian["ex:favCoffeeShop"].as_array().unwrap();
    assert!(coffee_shops.contains(&json!({"@id": "wiki:Q37158"})));
    assert!(coffee_shops.contains(&json!("Clemmons Coffee")));
}

#[tokio::test]
async fn staging_data_mixed_data_types_numeric() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("tx/mixed-dts-num:main").await.unwrap();

    // Test mixed data types: num & string in @list
    let txn = json!({
        "@context": [default_context(), {"ex": "http://example.org/ns/"}],
        "insert": {
            "@id": "ex:wes",
            "ex:aFewOfMyFavoriteThings": {"@list": [2011, "jabalí"]}
        }
    });

    let ledger1 = fluree.update(ledger0, &txn).await.unwrap().ledger;

    // Query directly using the updated ledger (transactions auto-commit)
    let ledger2 = ledger1;

    let query = json!({
        "@context": [default_context(), {"ex": "http://example.org/ns/"}],
        "select": {"ex:wes": ["*"]}
    });

    let result = support::query_jsonld(&fluree, &ledger2, &query)
        .await
        .unwrap();
    let jsonld = result
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();

    // Should preserve ordered list with mixed types
    let wes = &jsonld.as_array().unwrap()[0];
    assert_eq!(wes["ex:aFewOfMyFavoriteThings"], json!([2011, "jabalí"]));
}

#[tokio::test]
async fn iri_value_maps() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("any-iri:main").await.unwrap();

    // Test IRI value maps - inserting data with IRI values that get properly expanded
    let insert_txn = json!({
        "@context": {"ex": "http://example.com/"},
        "insert": [{
            "@id": "ex:foo",
            "ex:bar": {"@type": "@id", "@value": "ex:baz"}
        }]
    });

    let ledger1 = fluree.update(ledger0, &insert_txn).await.unwrap().ledger;

    // Query to verify IRI expansion and proper handling
    let query = json!({
        "@context": null,  // Use null context to see expanded IRIs
        "select": {"http://example.com/foo": ["*"]}
    });

    let result = support::query_jsonld(&fluree, &ledger1, &query)
        .await
        .unwrap();
    let jsonld = result
        .to_jsonld_async(ledger1.as_graph_db_ref(0))
        .await
        .unwrap();

    let expected = json!([{
        "@id": "http://example.com/foo",
        "http://example.com/bar": {"@id": "http://example.com/baz"}
    }]);

    assert_eq!(jsonld, expected);
}

#[tokio::test]
async fn object_var_test() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("var-in-obj:main").await.unwrap();

    // Test variables in object positions
    let txn1 = json!({
        "@context": {"ex": "http://example.org/"},
        "insert": {
            "@id": "ex:jane",
            "ex:friend": {
                "@id": "ex:alice",
                "ex:bestFriend": {"@id": "ex:bob"}
            }
        }
    });

    let ledger1 = fluree.update(ledger0, &txn1).await.unwrap().ledger;

    let txn2 = json!({
        "@context": {"ex": "http://example.org/"},
        "where": {"@id": "?s", "ex:friend": {"ex:bestFriend": "?bestFriend"}},
        "insert": {"@id": "?s", "ex:friendBFF": {"@id": "?bestFriend"}}
    });

    let ledger2 = fluree.update(ledger1, &txn2).await.unwrap().ledger;

    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": {"ex:jane": ["*"]},
        "depth": 3
    });

    let result = support::query_jsonld(&fluree, &ledger2, &query)
        .await
        .unwrap();
    let jsonld = result
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();

    let jane = &jsonld.as_array().unwrap()[0];
    assert_eq!(jane["@id"], "ex:jane");

    let friend = jane["ex:friend"].as_object().unwrap();
    assert_eq!(friend["@id"], "ex:alice");

    let best_friend = friend["ex:bestFriend"].as_object().unwrap();
    assert_eq!(best_friend["@id"], "ex:bob");

    let friend_bff = jane["ex:friendBFF"].as_object().unwrap();
    assert_eq!(friend_bff["@id"], "ex:bob");
}

#[tokio::test]
async fn transact_api_test() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_name = "example-ledger:main";
    let ledger0 = fluree.create_ledger(ledger_name).await.unwrap();

    let mut context = default_context();
    context.as_object_mut().unwrap().remove("f");

    let seed_txn = json!({
        "@context": [context.clone(), {"ex": "http://example.org/ns/"}],
        "insert": {"@id": "ex:firstTransaction", "@type": "ex:Nothing"}
    });
    let _ledger1 = fluree.update(ledger0, &seed_txn).await.unwrap().ledger;

    // Top-level context used for transaction nodes
    let txn = json!({
        "@context": [
            context.clone(),
            {"ex": "http://example.org/ns/"},
            {"f": "https://ns.flur.ee/db#", "foo": "http://foo.com/", "id": "@id"}
        ],
        "ledger": ledger_name,
        "insert": [
            {"id": "ex:alice", "@type": "ex:User", "foo:bar": "foo", "schema:name": "Alice"},
            {"id": "ex:bob", "@type": "ex:User", "foo:baz": "baz", "schema:name": "Bob"}
        ]
    });
    let ledger2 = fluree.update_with_ledger(&txn).await.unwrap().ledger;
    let rows = support::query_jsonld(
        &fluree,
        &ledger2,
        &json!({
            "@context": [context.clone(), {"ex": "http://example.org/ns/"}, {"foo": "http://foo.com/"}],
            "select": {"?s": ["*"]},
            "where": {"@id": "?s", "@type": "ex:User"}
        }),
    )
    .await
    .unwrap()
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([
            {"@id":"ex:bob","@type":"ex:User","schema:name":"Bob","foo:baz":"baz"},
            {"@id":"ex:alice","@type":"ex:User","schema:name":"Alice","foo:bar":"foo"}
        ]))
    );

    // Aliased @id is correctly identified
    let txn2 = json!({
        "@context": [context.clone(), {"ex": "http://example.org/ns/"}, {"id-alias": "@id"}],
        "ledger": ledger_name,
        "insert": {"id-alias": "ex:alice", "schema:givenName": "Alicia"}
    });
    let ledger3 = fluree.update_with_ledger(&txn2).await.unwrap().ledger;
    let rows2 = support::query_jsonld(
        &fluree,
        &ledger3,
        &json!({
            "@context": [
                context.clone(),
                {"ex": "http://example.org/ns/"},
                {"foo": "http://foo.com/", "bar": "http://bar.com/"}
            ],
            "select": {"?s": ["*"]},
            "where": {"@id": "?s", "@type": "ex:User"}
        }),
    )
    .await
    .unwrap()
    .to_jsonld_async(ledger3.as_graph_db_ref(0))
    .await
    .unwrap();
    assert_eq!(
        normalize_rows(&rows2),
        normalize_rows(&json!([
            {"@id":"ex:bob","@type":"ex:User","schema:name":"Bob","foo:baz":"baz"},
            {"@id":"ex:alice","@type":"ex:User","schema:name":"Alice","foo:bar":"foo","schema:givenName":"Alicia"}
        ]))
    );

    // @context inside node is correctly handled
    let txn3 = json!({
        "@context": {"f": "https://ns.flur.ee/db#"},
        "ledger": ledger_name,
        "insert": [{
            "@context": [context.clone(), {"ex": "http://example.org/ns/"}, {"quux": "http://quux.com/"}],
            "@id": "ex:alice",
            "quux:corge": "grault"
        }]
    });
    let ledger4 = fluree.update_with_ledger(&txn3).await.unwrap().ledger;
    let rows3 = support::query_jsonld(
        &fluree,
        &ledger4,
        &json!({
            "@context": [
                context.clone(),
                {"ex": "http://example.org/ns/"},
                {"foo": "http://foo.com/", "bar": "http://bar.com/", "quux": "http://quux.com/"}
            ],
            "select": {"?s": ["*"]},
            "where": {"@id": "?s", "@type": "ex:User"}
        }),
    )
    .await
    .unwrap()
    .to_jsonld_async(ledger4.as_graph_db_ref(0))
    .await
    .unwrap();
    assert_eq!(
        normalize_rows(&rows3),
        normalize_rows(&json!([
            {"@id":"ex:bob","@type":"ex:User","schema:name":"Bob","foo:baz":"baz"},
            {"@id":"ex:alice","@type":"ex:User","schema:name":"Alice","foo:bar":"foo","schema:givenName":"Alicia","quux:corge":"grault"}
        ]))
    );

    // Fuel tracking works on transactions
    let txn4 = json!({
        "@context": {"f": "https://ns.flur.ee/db#"},
        "ledger": ledger_name,
        "insert": [{
            "@context": [context.clone(), {"ex": "http://example.org/ns/"}, {"quux": "http://quux.com/"}],
            "@id": "ex:alice",
            "quux:corge": "grault"
        }],
        "opts": { "meta": true }
    });
    let (_result, tally) = fluree.update_with_ledger_tracked(&txn4).await.unwrap();
    assert!(
        tally.is_some(),
        "tracking tally should be returned when opts.meta is true"
    );

    // Throws on invalid txn (missing ledger)
    let bad_txn = json!({
        "@context": ["", {"quux": "http://quux.com/"}],
        "insert": {"@id": "ex:cam", "quux:corge": "grault"}
    });
    let err = fluree
        .update_with_ledger(&bad_txn)
        .await
        .expect_err("expected missing-ledger error");
    assert_eq!(
        err.to_string(),
        "Invalid configuration: Invalid transaction, missing required key: ledger."
    );
}

#[tokio::test]
async fn base_and_vocab_test() {
    // Scenario: transact-test/base-and-vocab-test
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("cookbook/base:main").await.unwrap();

    let ctx = json!({
        "@base": "http://example.org/",
        "@vocab": "http://example.org/terms/",
        "ex": "http://example.org/terms/",
        "f": "https://ns.flur.ee/db#"
    });
    let insert_graph = json!({
        "@context": ctx,
        "@graph": [{
            "@id": "nessie",
            "@type": "http://example.org/terms/SeaMonster",
            "http://example.org/terms/isScary": false
        }]
    });
    let ledger1 = fluree.insert(ledger0, &insert_graph).await.unwrap().ledger;

    let q_full = json!({
        "select": {"?m": ["*"]},
        "where": {
            "@id": "?m",
            "@type": "http://example.org/terms/SeaMonster"
        }
    });
    let r_full = support::query_jsonld(&fluree, &ledger1, &q_full)
        .await
        .unwrap()
        .to_jsonld_async(ledger1.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(
        r_full,
        json!([{
            "@id": "http://example.org/nessie",
            "@type": "http://example.org/terms/SeaMonster",
            "http://example.org/terms/isScary": false
        }])
    );

    let q_vocab = json!({
        "@context": {
            "@vocab": "http://example.org/terms/"
        },
        "select": {"?m": ["*"]},
        "where": {"@id": "?m", "@type": "SeaMonster"}
    });
    let r_vocab = support::query_jsonld(&fluree, &ledger1, &q_vocab)
        .await
        .unwrap()
        .to_jsonld_async(ledger1.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(
        r_vocab,
        json!([{
            "@id": "http://example.org/nessie",
            "@type": "SeaMonster",
            "isScary": false
        }])
    );

    let ledger2 = fluree.create_ledger("cookbook/base2:main").await.unwrap();
    let insert_object = json!({
        "@context": ctx,
        "insert": {
            "@id": "nessie",
            "@type": "http://example.org/terms/SeaMonster",
            "http://example.org/terms/isScary": false
        }
    });
    let ledger3 = fluree.update(ledger2, &insert_object).await.unwrap().ledger;
    let r2 = support::query_jsonld(&fluree, &ledger3, &q_vocab)
        .await
        .unwrap()
        .to_jsonld_async(ledger3.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(
        r2,
        json!([{
            "@id": "http://example.org/nessie",
            "@type": "SeaMonster",
            "isScary": false
        }])
    );
}

#[tokio::test]
async fn json_objects() {
    // Scenario: transact-test/json-objects
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("jsonpls:main").await.unwrap();

    let txn = json!({
        "@context": [default_context(), {"ex": "http://example.org/ns/"}],
        "insert": [
            {
                "@id": "ex:alice",
                "@type": "ex:Person",
                "ex:json": {"@type": "@json", "@value": {"json": "data", "is": ["cool", "right?", 1, false, 1.0]}}
            },
            {
                "@id": "ex:bob",
                "@type": "ex:Person",
                "ex:json": {"@type": "@json", "@value": {":edn": "data", ":is": ["cool", "right?", 1, false, 1.0]}}
            }
        ]
    });
    let ledger1 = fluree.update(ledger0, &txn).await.unwrap().ledger;

    let q_graph = json!({
        "@context": [default_context(), {"ex": "http://example.org/ns/"}],
        "where": {"@id": "?s", "@type": "ex:Person"},
        "select": {"?s": ["*"]}
    });
    let r_graph = support::query_jsonld(&fluree, &ledger1, &q_graph)
        .await
        .unwrap()
        .to_jsonld_async(ledger1.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(
        normalize_rows(&r_graph),
        normalize_rows(&json!([
            {"@id": "ex:alice", "@type": "ex:Person", "ex:json": {"json": "data", "is": ["cool", "right?", 1, false, 1.0]}},
            {"@id": "ex:bob", "@type": "ex:Person", "ex:json": {":edn": "data", ":is": ["cool", "right?", 1, false, 1.0]}}
        ]))
    );

    let q_select = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": "?json",
        "where": {"@id": "?s", "ex:json": "?json"}
    });
    let r_select = support::query_jsonld(&fluree, &ledger1, &q_select)
        .await
        .unwrap()
        .to_jsonld(&ledger1.snapshot)
        .unwrap();
    assert_eq!(
        normalize_rows(&r_select),
        normalize_rows(&json!([
            {":edn": "data", ":is": ["cool", "right?", 1, false, 1.0]},
            {"json": "data", "is": ["cool", "right?", 1, false, 1.0]}
        ]))
    );
}

#[tokio::test]
async fn no_where_solutions() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("insert-delete:main").await.unwrap();

    // Insert initial data
    let insert_txn = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "insert": {"@id": "ex:andrew", "schema:name": "Andrew"}
    });

    let ledger1 = fluree.update(ledger0, &insert_txn).await.unwrap().ledger;

    // Update with WHERE that matches nothing (no existing description)
    // Should still insert the new description
    let update_txn = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "where": {"@id": "ex:andrew", "schema:description": "?o"},
        "delete": {"@id": "ex:andrew", "schema:description": "?o"},
        "insert": {"@id": "ex:andrew", "schema:description": "He's great!"}
    });

    let ledger2 = fluree.update(ledger1, &update_txn).await.unwrap().ledger;

    let query = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "selectOne": {"ex:andrew": ["*"]}
    });

    let result = support::query_jsonld(&fluree, &ledger2, &query)
        .await
        .unwrap();
    let jsonld = result
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();

    let andrew = jsonld.as_object().unwrap();
    assert_eq!(andrew["@id"], "ex:andrew");
    assert_eq!(andrew["schema:name"], "Andrew");
    assert_eq!(andrew["schema:description"], "He's great!");
}

#[tokio::test]
async fn transaction_iri_special_char() {
    // Scenario: transact-test/transaction-iri-special-char
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("transaction-iri-special-char:main")
        .await
        .unwrap();

    let tx1 = json!({
        "@context": {"ex": "http://example.org/"},
        "insert": [{
            "@id": "ex:a\u{0b83}",
            "@type": "ex:Foo",
            "ex:desc": "try special \u{0b83} as second iri char"
        }]
    });
    let ledger1 = fluree.update(ledger0, &tx1).await.unwrap().ledger;

    let tx2 = json!({
        "@context": {"ex": "http://example.org/"},
        "insert": [{
            "@id": "ex:\u{0b83}b",
            "@type": "ex:Foo",
            "ex:desc": "try special \u{0b83} as first iri char"
        }]
    });
    let ledger2 = fluree.update(ledger1, &tx2).await.unwrap().ledger;

    let q1 = json!({
        "@context": {"ex": "http://example.org/"},
        "select": {"ex:a\u{0b83}": ["*"]}
    });
    let r1 = support::query_jsonld(&fluree, &ledger2, &q1)
        .await
        .unwrap()
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(
        r1,
        json!([{
            "@id": "ex:a\u{0b83}",
            "@type": "ex:Foo",
            "ex:desc": "try special \u{0b83} as second iri char"
        }])
    );

    let q2 = json!({
        "@context": {"ex": "http://example.org/"},
        "select": {"ex:\u{0b83}b": ["*"]}
    });
    let r2 = support::query_jsonld(&fluree, &ledger2, &q2)
        .await
        .unwrap()
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(
        r2,
        json!([{
            "@id": "ex:\u{0b83}b",
            "@type": "ex:Foo",
            "ex:desc": "try special \u{0b83} as first iri char"
        }])
    );
}

#[tokio::test]
async fn transact_with_explicit_commit() {
    use fluree_db_transact::TxnType;

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/explicit-commit:main")
        .await
        .unwrap();

    let txn = json!({
        "@context": [default_context(), {"ex": "http://example.org/ns/"}],
        "@graph": [{"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice"}]
    });

    let stage = fluree
        .stage_transaction(
            ledger0,
            TxnType::Insert,
            &txn,
            fluree_db_transact::TxnOpts::default(),
            None,
        )
        .await
        .unwrap();
    let (receipt, ledger1) = fluree
        .commit_staged(
            stage.view,
            stage.ns_registry,
            &fluree_db_ledger::IndexConfig::default(),
            fluree_db_transact::CommitOpts::default(),
        )
        .await
        .unwrap();

    assert_eq!(receipt.t, 1);

    let query = json!({
        "@context": [default_context(), {"ex": "http://example.org/ns/"}],
        "select": {"ex:alice": ["*"]}
    });
    let rows = support::query_jsonld(&fluree, &ledger1, &query)
        .await
        .unwrap()
        .to_jsonld_async(ledger1.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(
        rows,
        json!([{"@id":"ex:alice","@type":"ex:User","schema:name":"Alice"}])
    );
}

// =============================================================================
// Insert tests (from it_transact_insert.rs)
// =============================================================================

fn ctx_ex_schema() -> serde_json::Value {
    json!({
        "ex": "http://example.org/ns/",
        "schema": "http://schema.org/"
    })
}

#[tokio::test]
async fn insert_data_then_query_names() {
    use fluree_db_api::{LedgerState, Novelty};
    use fluree_db_core::LedgerSnapshot;

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/transact-insert:basic";

    let db0 = LedgerSnapshot::genesis(ledger_id);
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let inserted = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx_ex_schema(),
                "@graph": [
                    {"@id":"ex:alice","@type":"ex:User","schema:name":"Alice","schema:age":42},
                    {"@id":"ex:bob","@type":"ex:User","schema:name":"Bob","schema:age":22}
                ]
            }),
        )
        .await
        .expect("insert");

    let query = json!({
        "@context": ctx_ex_schema(),
        "select": ["?name"],
        "where": {"schema:name": "?name"}
    });

    let result = support::query_jsonld(&fluree, &inserted.ledger, &query)
        .await
        .expect("query");
    let mut rows = result
        .to_jsonld(&inserted.ledger.snapshot)
        .expect("to_jsonld");
    let arr = rows.as_array_mut().expect("rows array");
    arr.sort_by_key(std::string::ToString::to_string);

    assert_eq!(rows, json!(["Alice", "Bob"]));
}

#[tokio::test]
async fn insert_invalid_type_literal_errors() {
    use fluree_db_api::{ApiError, LedgerState, Novelty};
    use fluree_db_core::LedgerSnapshot;

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/transact-insert:invalid-type";

    let db0 = LedgerSnapshot::genesis(ledger_id);
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let txn = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "insert": [{
            "@id": "ex:bad",
            "@type": [{
                "@value": "not-a-iri",
                "@type": "xsd:string"
            }]
        }]
    });

    let err = match fluree.update(ledger0, &txn).await {
        Ok(_) => panic!("expected invalid @type literal to error"),
        Err(e) => e,
    };

    match err {
        ApiError::Transact(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("@type") || msg.contains("type"),
                "expected error mentioning @type, got: {msg}"
            );
        }
        other => panic!("expected ApiError::Transact, got: {other}"),
    }
}

// =============================================================================
// Retraction tests (from it_transact_retraction.rs)
// =============================================================================

#[tokio::test]
async fn retract_property_removes_only_that_property() {
    use fluree_db_api::{LedgerState, Novelty};
    use fluree_db_core::LedgerSnapshot;

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/transact-retraction:prop";

    let db0 = LedgerSnapshot::genesis(ledger_id);
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx_ex_schema(),
                "@graph": [
                    {"@id":"ex:alice","@type":"ex:User","schema:name":"Alice","schema:age":42},
                    {"@id":"ex:bob","@type":"ex:User","schema:name":"Bob","schema:age":22},
                    {"@id":"ex:jane","@type":"ex:User","schema:name":"Jane","schema:age":30}
                ]
            }),
        )
        .await
        .expect("insert");

    let updated = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx_ex_schema(),
                "delete": { "@id": "ex:alice", "schema:age": 42 }
            }),
        )
        .await
        .expect("update delete");

    let q = json!({
        "@context": ctx_ex_schema(),
        "select": { "ex:alice": ["*"] }
    });
    let result = support::query_jsonld(&fluree, &updated.ledger, &q)
        .await
        .expect("query");
    let jsonld = result
        .to_jsonld_async(updated.ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    let rows = jsonld.as_array().expect("rows array");
    assert_eq!(rows.len(), 1);
    let alice = rows[0].as_object().expect("row object");

    assert_eq!(alice.get("@id").and_then(|v| v.as_str()), Some("ex:alice"));
    assert_eq!(
        alice.get("schema:name").and_then(|v| v.as_str()),
        Some("Alice")
    );
    assert!(
        !alice.contains_key("schema:age"),
        "Alice should no longer have schema:age after delete"
    );
}

#[tokio::test]
async fn retracting_ordered_lists_removes_list_values() {
    use fluree_db_api::{LedgerState, Novelty};
    use fluree_db_core::LedgerSnapshot;

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/transact-retraction:list";

    let db0 = LedgerSnapshot::genesis(ledger_id);
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let ctx = json!({
        "ex": "http://example.org/ns/",
        "id": "@id",
        "ex:items2": { "@container": "@list" }
    });

    let seeded = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx,
                "@graph": [{
                    "id": "ex:list-test",
                    "ex:items1": { "@list": ["zero","one","two","three"] },
                    "ex:items2": ["four","five","six","seven"]
                }]
            }),
        )
        .await
        .expect("insert list");

    let q = json!({"@context": ctx, "select": {"ex:list-test": ["*"]}});

    let before = support::query_jsonld(&fluree, &seeded.ledger, &q)
        .await
        .expect("query before");
    let before_json = before
        .to_jsonld_async(seeded.ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async before");

    assert_eq!(
        before_json,
        json!([{
            "@id": "ex:list-test",
            "ex:items1": ["zero","one","two","three"],
            "ex:items2": ["four","five","six","seven"]
        }])
    );

    let updated = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx,
                "delete": { "id": "ex:list-test", "ex:items1": "?items1", "ex:items2": "?items2" },
                "where":  { "id": "ex:list-test", "ex:items1": "?items1", "ex:items2": "?items2" }
            }),
        )
        .await
        .expect("update delete lists");

    let after = support::query_jsonld(&fluree, &updated.ledger, &q)
        .await
        .expect("query after");
    let after_json = after
        .to_jsonld_async(updated.ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async after");

    assert_eq!(after_json, json!([{"@id": "ex:list-test"}]));
}

// =============================================================================
// Turtle/RDF transaction tests (from it_transact_turtle.rs)
// =============================================================================

const TURTLE_SAMPLE: &str = r#"@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
# --- Named Node ---
ex:foo ex:name "Foo's Name" ;
       ex:age  "42"^^xsd:integer .
# --- Blank Node related to other blank node ---
_:b1 a ex:Person ;
     ex:name "Blank Node" ;
     ex:age  "41"^^xsd:integer ;
     ex:friend _:b1 .
# --- Numeric datatype without ---
_:b2 rdf:type ex:Person ;
     ex:name "Blank 2" ;
     ex:age 33 .
"#;

#[tokio::test]
async fn turtle_insert() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("tx/turtle-insert:main").await.unwrap();

    let ledger1 = fluree
        .insert_turtle(ledger0, TURTLE_SAMPLE)
        .await
        .unwrap()
        .ledger;

    let ctx = json!({
        "ex": "http://example.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let query = json!({
        "@context": ctx,
        "select": {"?s": ["*"]},
        "where": {
            "@id": "?s",
            "ex:age": {"@value": 42, "@type": "xsd:integer"}
        }
    });
    let rows = support::query_jsonld(&fluree, &ledger1, &query)
        .await
        .unwrap()
        .to_jsonld_async(ledger1.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(
        rows,
        json!([{"@id":"ex:foo","ex:name":"Foo's Name","ex:age":42}])
    );

    let query2 = json!({
        "@context": ctx,
        "select": {"?s": ["*"]},
        "where": {
            "@id": "?s",
            "ex:age": {"@value": 41, "@type": "xsd:integer"}
        }
    });
    let rows2 = support::query_jsonld(&fluree, &ledger1, &query2)
        .await
        .unwrap()
        .to_jsonld_async(ledger1.as_graph_db_ref(0))
        .await
        .unwrap();
    let row2 = rows2.as_array().unwrap()[0].as_object().unwrap();
    let bnode_id = row2.get("@id").and_then(|v| v.as_str()).unwrap();
    assert!(bnode_id.starts_with("_:"));
    assert_eq!(row2.get("@type").unwrap(), "ex:Person");
    assert_eq!(row2.get("ex:name").unwrap(), "Blank Node");
    assert_eq!(row2.get("ex:age").unwrap(), 41);
    let friend = row2.get("ex:friend").unwrap().as_object().unwrap();
    assert_eq!(friend.get("@id").unwrap(), bnode_id);

    let query3 = json!({
        "@context": ctx,
        "select": {"?s": ["*"]},
        "where": {"@id":"?s","ex:age":33}
    });
    let rows3 = support::query_jsonld(&fluree, &ledger1, &query3)
        .await
        .unwrap()
        .to_jsonld_async(ledger1.as_graph_db_ref(0))
        .await
        .unwrap();
    let row3 = rows3.as_array().unwrap()[0].as_object().unwrap();
    let bnode_id2 = row3.get("@id").and_then(|v| v.as_str()).unwrap();
    assert!(bnode_id2.starts_with("_:"));
    assert_eq!(row3.get("@type").unwrap(), "ex:Person");
    assert_eq!(row3.get("ex:name").unwrap(), "Blank 2");
    assert_eq!(row3.get("ex:age").unwrap(), 33);
}

#[tokio::test]
async fn turtle_insert_and_commit() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/turtle-insert-commit:main")
        .await
        .unwrap();

    let ledger1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [{"@id":"ex:pre","ex:bar":3}]
            }),
        )
        .await
        .unwrap()
        .ledger;

    let ledger2 = fluree
        .insert_turtle(ledger1, TURTLE_SAMPLE)
        .await
        .unwrap()
        .ledger;

    let query = json!({
        "@context": {"ex":"http://example.org/","xsd":"http://www.w3.org/2001/XMLSchema#"},
        "select": {"?s":["*"]},
        "where": {"@id":"?s","ex:age":{"@value":42,"@type":"xsd:integer"}}
    });
    let rows = support::query_jsonld(&fluree, &ledger2, &query)
        .await
        .unwrap()
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(
        rows,
        json!([{"@id":"ex:foo","ex:name":"Foo's Name","ex:age":42}])
    );
    assert_eq!(ledger2.t(), 2);
}

#[tokio::test]
async fn turtle_upsert() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("tx/turtle-upsert:main").await.unwrap();
    let ledger1 = fluree
        .insert_turtle(ledger0, TURTLE_SAMPLE)
        .await
        .unwrap()
        .ledger;

    let turtle_update = r#"@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:foo ex:name "UPDATED Name" ;
       ex:age  "33"^^xsd:integer ."#;

    let ledger2 = fluree
        .upsert_turtle(ledger1, turtle_update)
        .await
        .unwrap()
        .ledger;

    let rows = support::query_jsonld(
        &fluree,
        &ledger2,
        &json!({
            "@context": {"ex":"http://example.org/"},
            "select": {"ex:foo":["*"]}
        }),
    )
    .await
    .unwrap()
    .to_jsonld_async(ledger2.as_graph_db_ref(0))
    .await
    .unwrap();
    assert_eq!(
        rows,
        json!([{"@id":"ex:foo","ex:name":"UPDATED Name","ex:age":33}])
    );
}

#[tokio::test]
async fn turtle_upsert_and_commit() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/turtle-upsert-commit:main")
        .await
        .unwrap();

    let ledger1 = fluree
        .insert_turtle(ledger0, TURTLE_SAMPLE)
        .await
        .unwrap()
        .ledger;

    let turtle_update = r#"@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:foo ex:name "UPDATED Name" ;
       ex:age  "33"^^xsd:integer ."#;

    let ledger2 = fluree
        .upsert_turtle(ledger1, turtle_update)
        .await
        .unwrap()
        .ledger;

    let rows = support::query_jsonld(
        &fluree,
        &ledger2,
        &json!({
            "@context": {"ex":"http://example.org/"},
            "select": {"ex:foo":["*"]}
        }),
    )
    .await
    .unwrap()
    .to_jsonld_async(ledger2.as_graph_db_ref(0))
    .await
    .unwrap();
    assert_eq!(
        rows,
        json!([{"@id":"ex:foo","ex:name":"UPDATED Name","ex:age":33}])
    );
    assert_eq!(ledger2.t(), 2);
}

/// Regression test: when the same entity appears multiple times in an @graph
/// array (e.g., a member object duplicated once per channel), the transaction
/// should deduplicate and store only one copy of each unique fact.
#[tokio::test]
async fn duplicate_entities_in_graph_are_deduped() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("tx/dedup-graph:main").await.unwrap();

    // Insert 3 channels, each referencing the same 2 members.
    // The members appear as separate top-level @graph objects each time
    // (mimicking how a webhook might serialize denormalized data).
    let txn = json!({
        "@context": [default_context(), {"ex": "http://example.org/ns/"}],
        "insert": {
            "@graph": [
                // Member Alice — appears 3 times (once per channel)
                {"@id": "ex:alice", "@type": "ex:Member", "schema:name": "Alice", "schema:email": "alice@example.org"},
                {"@id": "ex:alice", "@type": "ex:Member", "schema:name": "Alice", "schema:email": "alice@example.org"},
                {"@id": "ex:alice", "@type": "ex:Member", "schema:name": "Alice", "schema:email": "alice@example.org"},
                // Member Bob — appears 3 times
                {"@id": "ex:bob", "@type": "ex:Member", "schema:name": "Bob", "schema:email": "bob@example.org"},
                {"@id": "ex:bob", "@type": "ex:Member", "schema:name": "Bob", "schema:email": "bob@example.org"},
                {"@id": "ex:bob", "@type": "ex:Member", "schema:name": "Bob", "schema:email": "bob@example.org"},
                // 3 channels referencing the members
                {"@id": "ex:ch1", "@type": "ex:Channel", "schema:name": "general", "ex:member": [{"@id": "ex:alice"}, {"@id": "ex:bob"}]},
                {"@id": "ex:ch2", "@type": "ex:Channel", "schema:name": "random",  "ex:member": [{"@id": "ex:alice"}, {"@id": "ex:bob"}]},
                {"@id": "ex:ch3", "@type": "ex:Channel", "schema:name": "dev",     "ex:member": [{"@id": "ex:alice"}, {"@id": "ex:bob"}]}
            ]
        }
    });

    let result = fluree.update(ledger0, &txn).await.unwrap();
    let ledger = result.ledger;

    // Verify dedup at the flake level: the commit receipt should reflect
    // deduplicated counts, not the inflated raw count.
    let flake_count = result.receipt.flake_count;
    // Expected unique data flakes:
    //   Alice: type + name + email = 3
    //   Bob:   type + name + email = 3
    //   ch1:   type + name + 2 member refs = 4
    //   ch2:   type + name + 2 member refs = 4
    //   ch3:   type + name + 2 member refs = 4
    //   Total data: 18
    // Without dedup this would be 30 (members tripled).
    // Commit metadata adds a few more, but should stay well under 50.
    assert!(
        flake_count < 50,
        "expected < 50 total flakes (deduped), got {flake_count}"
    );

    // Query Alice's properties — should see exactly 1 of each, not 3
    let query = json!({
        "@context": [default_context(), {"ex": "http://example.org/ns/"}],
        "select": {"ex:alice": ["*"]}
    });
    let jsonld = support::query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .unwrap();

    // Verify Alice has exactly 1 name and 1 email (not 3 of each)
    let alice = &jsonld[0];
    assert_eq!(alice["schema:name"], "Alice");
    assert_eq!(alice["schema:email"], "alice@example.org");
}
