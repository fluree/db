//! Transact upsert integration tests
//!
//! Tests upsert functionality where existing data gets replaced rather than merged.

mod support;

use fluree_db_api::FlureeBuilder;
use fluree_db_core::comparator::IndexType;
use fluree_db_core::{load_commit_by_id, FlakeValue};
use serde_json::json;
use support::normalize_rows;

// Helper function to create a standard context
fn ctx() -> serde_json::Value {
    json!({
        "ex": "http://example.org/ns/",
        "schema": "http://schema.org/"
    })
}

/// Test that OPTIONAL patterns work in transaction WHERE clauses.
///
/// This test verifies that the query parser's full pattern support (including OPTIONAL)
/// is now available in transactions after the parser unification refactor.
#[tokio::test]
async fn upsert_parsing() {
    // Transactions with OPTIONAL patterns in WHERE clause.
    // The key behavior is that OPTIONAL allows "delete if exists" semantics
    // without failing when the data doesn't exist.

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/upsert-parsing:main")
        .await
        .unwrap();

    // Insert initial data - only alice has both name and age
    let initial_txn = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [
            {"@id": "ex:alice", "schema:name": "Alice", "ex:age": 30}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &initial_txn).await.unwrap().ledger;

    // Update with OPTIONAL pattern - should work even for fields that don't exist
    // Upsert pattern: use OPTIONAL so missing fields don't fail
    let update_txn = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "where": [
            ["optional", {"@id": "ex:alice", "schema:name": "?name"}],
            ["optional", {"@id": "ex:alice", "ex:nickname": "?nick"}]
        ],
        "delete": [
            {"@id": "ex:alice", "schema:name": "?name"},
            {"@id": "ex:alice", "ex:nickname": "?nick"}
        ],
        "insert": [
            {"@id": "ex:alice", "schema:name": "Alice Updated", "ex:nickname": "Ali"}
        ]
    });

    // This should succeed - OPTIONAL allows missing patterns
    let result = fluree.update(ledger1, &update_txn).await;
    assert!(
        result.is_ok(),
        "Update with OPTIONAL should succeed: {:?}",
        result.err()
    );

    let ledger2 = result.unwrap().ledger;

    // Query to verify the update worked
    let query = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "where": {"@id": "ex:alice"},
        "select": {"ex:alice": ["*"]}
    });

    let result = support::query_jsonld(&fluree, &ledger2, &query)
        .await
        .unwrap();
    let jsonld = result
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();

    let alice = &jsonld[0];
    // Name should be updated
    assert_eq!(alice["schema:name"], json!("Alice Updated"));
    // Nickname should be added
    assert_eq!(alice["ex:nickname"], json!("Ali"));
    // Age should still be there (wasn't touched by the update)
    assert_eq!(alice["ex:age"], json!(30));
}

#[tokio::test]
async fn upsert_data() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger
    let ledger0 = fluree.create_ledger("tx/upsert-test:main").await.unwrap();

    // First insert some initial data
    let initial_txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "ex:nums": [1, 2, 3], "schema:age": 42},
            {"@id": "ex:bob", "@type": "ex:User", "schema:name": "Bob", "ex:nums": [1, 2, 3], "schema:age": 22}
        ]
    });

    let ledger1 = fluree.insert(ledger0, &initial_txn).await.unwrap().ledger;

    // Now upsert - this should replace existing data
    let upsert_txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "ex:nums": [4, 5, 6], "schema:name": "Alice2"},
            {"@id": "ex:bob", "ex:nums": [4, 5, 6], "schema:name": "Bob2"},
            {"@id": "ex:jane", "ex:nums": [4, 5, 6], "schema:name": "Jane2"}
        ]
    });

    let ledger2 = fluree.upsert(ledger1, &upsert_txn).await.unwrap().ledger;

    // Query to verify upsert behavior
    let query = json!({
        "@context": ctx(),
        "select": {"?id": ["*"]},
        "where": {"@id": "?id", "schema:name": "?name"}
    });

    let result = support::query_jsonld(&fluree, &ledger2, &query)
        .await
        .unwrap();
    let jsonld = result
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();

    // Should have 3 users with updated data
    assert_eq!(jsonld.as_array().unwrap().len(), 3);

    let mut users: Vec<_> = jsonld
        .as_array()
        .unwrap()
        .iter()
        .map(|user| {
            let obj = user.as_object().unwrap();
            let id = obj["@id"].as_str().unwrap();
            let name = obj["schema:name"].as_str().unwrap();
            let nums = obj["ex:nums"].clone();
            (id.to_string(), name.to_string(), nums)
        })
        .collect();

    users.sort_by(|a, b| a.0.cmp(&b.0));

    // Alice should have updated name and nums, but keep original age and type
    assert_eq!(users[0].0, "ex:alice");
    assert_eq!(users[0].1, "Alice2");
    assert_eq!(users[0].2, json!([4, 5, 6]));

    // Bob should have updated name and nums, but keep original age and type
    assert_eq!(users[1].0, "ex:bob");
    assert_eq!(users[1].1, "Bob2");
    assert_eq!(users[1].2, json!([4, 5, 6]));

    // Jane should be new with just name and nums
    assert_eq!(users[2].0, "ex:jane");
    assert_eq!(users[2].1, "Jane2");
    assert_eq!(users[2].2, json!([4, 5, 6]));
}

#[tokio::test]
async fn upsert_no_changes() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger0 = fluree.create_ledger("tx/upsert2:main").await.unwrap();
    let sample_insert_txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "ex:nums": [1, 2, 3], "schema:age": 42},
            {"@id": "ex:bob", "@type": "ex:User", "schema:name": "Bob", "ex:nums": [1, 2, 3], "schema:age": 22}
        ]
    });
    let ledger1 = fluree
        .insert(ledger0, &sample_insert_txn)
        .await
        .unwrap()
        .ledger;

    let query = json!({
        "@context": ctx(),
        "select": {"?id": ["*"]},
        "where": {"@id": "?id", "schema:name": "?name"}
    });
    let result1 = support::query_jsonld(&fluree, &ledger1, &query)
        .await
        .unwrap();
    let jsonld1 = result1
        .to_jsonld_async(ledger1.as_graph_db_ref(0))
        .await
        .unwrap();

    let ledger2 = fluree
        .upsert(ledger1, &sample_insert_txn)
        .await
        .unwrap()
        .ledger;
    let result2 = support::query_jsonld(&fluree, &ledger2, &query)
        .await
        .unwrap();
    let jsonld2 = result2
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();

    let ledger3 = fluree
        .upsert(ledger2, &sample_insert_txn)
        .await
        .unwrap()
        .ledger;
    let result3 = support::query_jsonld(&fluree, &ledger3, &query)
        .await
        .unwrap();
    let jsonld3 = result3
        .to_jsonld_async(ledger3.as_graph_db_ref(0))
        .await
        .unwrap();

    assert_eq!(normalize_rows(&jsonld1), normalize_rows(&jsonld2));
    assert_eq!(normalize_rows(&jsonld1), normalize_rows(&jsonld3));
}

#[tokio::test]
async fn upsert_typed_string_retract_and_assert_use_same_datatype_sid() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tx/upsert-typed-string-datatype-sid:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let initial_txn = json!({
        "@context": {"xsd": "http://www.w3.org/2001/XMLSchema#"},
        "@graph": [
            {
                "@id": "http://example.org/s",
                "http://example.org/p": {
                    "@value": "before",
                    "@type": "xsd:string"
                }
            }
        ]
    });
    let ledger1 = fluree.insert(ledger0, &initial_txn).await.unwrap().ledger;

    let upsert_txn = json!({
        "@context": {
            "ex": "http://example.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {
                "@id": "ex:s",
                "ex:p": {
                    "@value": "after",
                    "@type": "xsd:string"
                }
            }
        ]
    });
    let result = fluree.upsert(ledger1, &upsert_txn).await.unwrap();

    let content_store = fluree.content_store(ledger_id);
    let commit = load_commit_by_id(&content_store, &result.receipt.commit_id)
        .await
        .expect("load upsert commit");

    let retract = commit
        .flakes
        .iter()
        .find(|f| {
            !f.op
                && matches!(&f.o, FlakeValue::String(s) if s == "before")
                && f.p.name.as_ref() == "p"
        })
        .expect("retract flake for previous typed string");
    let assert = commit
        .flakes
        .iter()
        .find(|f| {
            f.op && matches!(&f.o, FlakeValue::String(s) if s == "after")
                && f.p.name.as_ref() == "p"
        })
        .expect("assert flake for replacement typed string");

    assert_eq!(
        retract.dt, assert.dt,
        "upsert should reuse the same datatype SID on retract and assert for xsd:string literals"
    );
}

#[tokio::test]
async fn upsert_multicardinal_data() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger
    let ledger0 = fluree.create_ledger("tx/upsert3:main").await.unwrap();

    // Insert initial multicardinal data
    let initial_txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:User", "ex:letter": ["a", "b", "c", "d"], "ex:num": [2, 4, 6, 8]},
            {"@id": "ex:bob", "@type": "ex:User", "ex:letter": ["a", "b", "c", "d"], "ex:num": [2, 4, 6, 8]}
        ]
    });

    let ledger1 = fluree.insert(ledger0, &initial_txn).await.unwrap().ledger;

    // Upsert to replace multicardinal properties
    let upsert_txn = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [
            {"@id": "ex:alice", "ex:letter": ["e", "f", "g", "h"], "ex:num": [3, 5, 7, 9]},
            {"@id": "ex:bob", "ex:letter": ["e", "f", "g", "h"], "ex:num": [3, 5, 7, 9]}
        ]
    });

    let ledger2 = fluree.upsert(ledger1, &upsert_txn).await.unwrap().ledger;

    // Query to verify multicardinal upsert worked
    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where": {"@id": "?s", "@type": "ex:User"},
        "select": {"?s": ["*"]}
    });

    let result = support::query_jsonld(&fluree, &ledger2, &query)
        .await
        .unwrap();
    let jsonld = result
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();

    // Should have 2 users with updated multicardinal data
    assert_eq!(jsonld.as_array().unwrap().len(), 2);

    for user in jsonld.as_array().unwrap() {
        let obj = user.as_object().unwrap();
        assert_eq!(obj["ex:letter"], json!(["e", "f", "g", "h"]));
        assert_eq!(obj["ex:num"], json!([3, 5, 7, 9]));
        assert_eq!(obj["@type"], json!("ex:User"));
    }
}

#[tokio::test]
async fn upsert_cancels_identical_pairs_in_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/upsert-cancel-pairs:main")
        .await
        .unwrap();

    let ctx = json!({
        "ex": "http://example.org/ns/",
        "schema": "http://schema.org/"
    });
    let insert = json!({
        "@context": ctx,
        "@graph": [{
            "@id": "ex:alice",
            "@type": "ex:User",
            "schema:name": "Alice",
            "ex:nums": [1, 2]
        }]
    });
    let ledger1 = fluree.insert(ledger0, &insert).await.unwrap().ledger;

    let upsert = json!({
        "@context": ctx,
        "@graph": [{
            "@id": "ex:alice",
            "schema:name": "Alice2",
            "ex:nums": [1, 2, 3]
        }]
    });
    let ledger2 = fluree.upsert(ledger1, &upsert).await.unwrap().ledger;

    let s = ledger2
        .snapshot
        .encode_iri("http://example.org/ns/alice")
        .expect("subject sid");
    let p_name = ledger2
        .snapshot
        .encode_iri("http://schema.org/name")
        .expect("name sid");
    let p_nums = ledger2
        .snapshot
        .encode_iri("http://example.org/ns/nums")
        .expect("nums sid");

    let spot_ids: Vec<_> = ledger2.novelty.iter_index(IndexType::Spot).collect();
    let mut name_flakes = 0;
    let mut nums_flakes = 0;
    for id in spot_ids {
        let flake = ledger2.novelty.get_flake(id);
        if flake.s == s {
            if flake.p == p_name {
                name_flakes += 1;
            } else if flake.p == p_nums {
                nums_flakes += 1;
            }
        }
    }

    assert_eq!(
        name_flakes, 3,
        "schema:name asserts Alice, then retracts Alice and asserts Alice2"
    );
    assert_eq!(
        nums_flakes, 3,
        "ex:nums went from [1 2] to [1 2 3], total of 3 flakes"
    );
}

#[tokio::test]
async fn upsert_and_commit() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("tx/upsert:main").await.unwrap();

    let sample_insert_txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "ex:nums": [1, 2, 3], "schema:age": 42},
            {"@id": "ex:bob", "@type": "ex:User", "schema:name": "Bob", "ex:nums": [1, 2, 3], "schema:age": 22}
        ]
    });
    let ledger1 = fluree
        .insert(ledger0, &sample_insert_txn)
        .await
        .unwrap()
        .ledger;

    let sample_upsert_txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "ex:nums": [4, 5, 6], "schema:name": "Alice2"},
            {"@id": "ex:bob", "ex:nums": [4, 5, 6], "schema:name": "Bob2"},
            {"@id": "ex:jane", "ex:nums": [4, 5, 6], "schema:name": "Jane2"}
        ]
    });
    let ledger2 = fluree
        .upsert(ledger1, &sample_upsert_txn)
        .await
        .unwrap()
        .ledger;

    let query = json!({
        "@context": ctx(),
        "select": {"?id": ["*"]},
        "where": {"@id": "?id", "schema:name": "?name"}
    });
    let result = support::query_jsonld(&fluree, &ledger2, &query)
        .await
        .unwrap();
    let jsonld = result
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([
            {"@id":"ex:alice","@type":"ex:User","schema:age":42,"ex:nums":[4,5,6],"schema:name":"Alice2"},
            {"@id":"ex:bob","@type":"ex:User","schema:age":22,"ex:nums":[4,5,6],"schema:name":"Bob2"},
            {"@id":"ex:jane","ex:nums":[4,5,6],"schema:name":"Jane2"}
        ]))
    );
}

/// Upsert with @json typed values should replace (retract old + assert new),
/// not append. This is the same "replace mode" semantics as for scalar values.
///
/// Regression test: upsert was observed to append @json values instead of
/// replacing them, while the explicit where+delete+insert pattern worked.
#[tokio::test]
async fn upsert_json_type_replaces_not_appends() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("tx/upsert-json:main").await.unwrap();

    // Insert initial @json data
    let initial_txn = json!({
        "@context": ctx(),
        "@graph": [{
            "@id": "ex:config",
            "ex:data": {"@value": {"seed": true, "version": 1}, "@type": "@json"}
        }]
    });
    let ledger1 = fluree.insert(ledger0, &initial_txn).await.unwrap().ledger;

    // Upsert with a different @json value — should replace, not append
    let upsert_txn = json!({
        "@context": ctx(),
        "@graph": [{
            "@id": "ex:config",
            "ex:data": {"@value": {"seed": false, "version": 2}, "@type": "@json"}
        }]
    });
    let ledger2 = fluree.upsert(ledger1, &upsert_txn).await.unwrap().ledger;

    // Query the result
    let query = json!({
        "@context": ctx(),
        "select": {"ex:config": ["*"]}
    });
    let result = support::query_jsonld(&fluree, &ledger2, &query)
        .await
        .unwrap()
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();

    let config = &result[0];
    let data = &config["ex:data"];

    // After upsert, there should be exactly ONE @json value (the new one),
    // not an array of [old, new].
    assert!(
        !data.is_array(),
        "ex:data should be a single value after upsert, not an array. Got: {data}"
    );

    // Verify it's the NEW value
    assert_eq!(data["version"], 2, "should have new version after upsert");
    assert_eq!(
        data["seed"], false,
        "should have new seed value after upsert"
    );
}

/// Same scenario as above, but using explicit where+delete+insert (update).
/// This is the control case — the user reports this pattern works correctly.
#[tokio::test]
async fn update_json_type_replaces_via_where_delete_insert() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("tx/update-json:main").await.unwrap();

    // Insert initial @json data
    let initial_txn = json!({
        "@context": ctx(),
        "@graph": [{
            "@id": "ex:config",
            "ex:data": {"@value": {"seed": true, "version": 1}, "@type": "@json"}
        }]
    });
    let ledger1 = fluree.insert(ledger0, &initial_txn).await.unwrap().ledger;

    // Update with explicit where+delete+insert pattern
    let update_txn = json!({
        "@context": ctx(),
        "where": {"@id": "ex:config", "ex:data": "?old"},
        "delete": {"@id": "ex:config", "ex:data": "?old"},
        "insert": {
            "@id": "ex:config",
            "ex:data": {"@value": {"seed": false, "version": 2}, "@type": "@json"}
        }
    });
    let ledger2 = fluree.update(ledger1, &update_txn).await.unwrap().ledger;

    // Query the result
    let query = json!({
        "@context": ctx(),
        "select": {"ex:config": ["*"]}
    });
    let result = support::query_jsonld(&fluree, &ledger2, &query)
        .await
        .unwrap()
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();

    let config = &result[0];
    let data = &config["ex:data"];

    // After update, there should be exactly ONE @json value (the new one)
    assert!(
        !data.is_array(),
        "ex:data should be a single value after update, not an array. Got: {data}"
    );

    // Verify it's the NEW value
    assert_eq!(data["version"], 2, "should have new version after update");
    assert_eq!(
        data["seed"], false,
        "should have new seed value after update"
    );
}

/// Upsert idempotence for @json values: upserting the same @json value twice
/// should produce identical results (no duplicates).
#[tokio::test]
async fn upsert_json_type_idempotent() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/upsert-json-idem:main")
        .await
        .unwrap();

    let txn = json!({
        "@context": ctx(),
        "@graph": [{
            "@id": "ex:config",
            "ex:data": {"@value": {"key": "val"}, "@type": "@json"}
        }]
    });

    let ledger1 = fluree.insert(ledger0, &txn).await.unwrap().ledger;
    let ledger2 = fluree.upsert(ledger1.clone(), &txn).await.unwrap().ledger;

    let query = json!({
        "@context": ctx(),
        "select": {"ex:config": ["*"]}
    });

    let result1 = support::query_jsonld(&fluree, &ledger1, &query)
        .await
        .unwrap()
        .to_jsonld_async(ledger1.as_graph_db_ref(0))
        .await
        .unwrap();
    let result2 = support::query_jsonld(&fluree, &ledger2, &query)
        .await
        .unwrap()
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();

    assert_eq!(
        normalize_rows(&result1),
        normalize_rows(&result2),
        "upserting the same @json value should be idempotent"
    );
}

/// Novelty enforces RDF set semantics: inserting the same triple multiple
/// times across separate commits should not create duplicate assertions.
/// Only the first assertion is kept; subsequent duplicates are silently dropped.
#[tokio::test]
async fn novelty_dedup_prevents_duplicate_assertions() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree.create_ledger("tx/novelty-dedup:main").await.unwrap();

    let ctx = json!({
        "ex": "http://example.org/ns/"
    });

    // Insert "open" 4 times across separate commits
    let mut ledger = fluree
        .insert(
            ledger0,
            &json!({ "@context": ctx, "@graph": [{"@id": "ex:task1", "ex:status": "open"}] }),
        )
        .await
        .unwrap()
        .ledger;

    for _ in 0..3 {
        ledger = fluree
            .insert(
                ledger,
                &json!({ "@context": ctx, "@graph": [{"@id": "ex:task1", "ex:status": "open"}] }),
            )
            .await
            .unwrap()
            .ledger;
    }

    // Also add distinct values
    ledger = fluree
        .insert(
            ledger,
            &json!({ "@context": ctx, "@graph": [{"@id": "ex:task1", "ex:status": "in-progress"}] }),
        )
        .await
        .unwrap()
        .ledger;

    ledger = fluree
        .insert(
            ledger,
            &json!({ "@context": ctx, "@graph": [{"@id": "ex:task1", "ex:status": "blocked"}] }),
        )
        .await
        .unwrap()
        .ledger;

    // Novelty dedup: only 3 distinct values should exist, not 6
    let query = json!({
        "@context": ctx,
        "select": "?status",
        "where": { "@id": "ex:task1", "ex:status": "?status" }
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .unwrap()
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();
    let mut statuses: Vec<String> = result
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    statuses.sort();
    assert_eq!(
        statuses,
        vec!["blocked", "in-progress", "open"],
        "novelty should deduplicate: 4 inserts of 'open' should yield 1 copy"
    );

    // Upsert replaces all 3 distinct values with 1 new value
    let upsert_txn = json!({
        "@context": ctx,
        "@graph": [{"@id": "ex:task1", "ex:status": "complete"}]
    });
    let ledger_after = fluree.upsert(ledger, &upsert_txn).await.unwrap().ledger;

    let post = support::query_jsonld(&fluree, &ledger_after, &query)
        .await
        .unwrap()
        .to_jsonld_async(ledger_after.as_graph_db_ref(0))
        .await
        .unwrap();
    assert_eq!(
        post,
        json!(["complete"]),
        "upsert should replace all values with single 'complete'. Got: {post}"
    );
}

/// Upsert one new value when multiple existing values live in novelty only.
///
/// Regression test: when values are accumulated across multiple transactions
/// and all exist only in novelty (not persisted to the binary index), upsert
/// must retract ALL existing values — not just one.
#[tokio::test]
async fn upsert_retracts_all_novelty_only_multicardinal_values() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/upsert-novelty-multi:main")
        .await
        .unwrap();

    let ctx = json!({
        "ex": "http://example.org/ns/",
        "schema": "http://schema.org/"
    });

    // Transaction 1: insert entity with multiple values
    let txn1 = json!({
        "@context": ctx,
        "@graph": [{"@id": "ex:item", "ex:tag": ["alpha", "beta", "gamma"]}]
    });
    let ledger1 = fluree.insert(ledger0, &txn1).await.unwrap().ledger;

    // Transaction 2: add more values via a second insert (still novelty only)
    let txn2 = json!({
        "@context": ctx,
        "@graph": [{"@id": "ex:item", "ex:tag": ["delta", "epsilon"]}]
    });
    let ledger2 = fluree.insert(ledger1, &txn2).await.unwrap().ledger;

    // Verify we now have 5 values, all in novelty only
    let query = json!({
        "@context": ctx,
        "select": {"ex:item": ["*"]}
    });
    let pre = support::query_jsonld(&fluree, &ledger2, &query)
        .await
        .unwrap()
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();
    let pre_tags = &pre[0]["ex:tag"];
    assert_eq!(
        pre_tags.as_array().unwrap().len(),
        5,
        "should have 5 tags before upsert: {pre_tags}"
    );

    // Upsert with a SINGLE new value — should replace all 5
    let upsert_txn = json!({
        "@context": ctx,
        "@graph": [{"@id": "ex:item", "ex:tag": "zulu"}]
    });
    let ledger3 = fluree.upsert(ledger2, &upsert_txn).await.unwrap().ledger;

    // Query: should have exactly ONE tag value now
    let post = support::query_jsonld(&fluree, &ledger3, &query)
        .await
        .unwrap()
        .to_jsonld_async(ledger3.as_graph_db_ref(0))
        .await
        .unwrap();
    let post_tags = &post[0]["ex:tag"];
    assert_eq!(
        post_tags,
        &json!("zulu"),
        "upsert should replace all 5 novelty-only values with single new value. Got: {post_tags}"
    );
}

/// Same test but with values accumulated in a single transaction (array form).
#[tokio::test]
async fn upsert_single_value_replaces_array_in_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/upsert-array-to-single:main")
        .await
        .unwrap();

    let ctx = json!({
        "ex": "http://example.org/ns/"
    });

    // Insert 4 values at once
    let txn = json!({
        "@context": ctx,
        "@graph": [{"@id": "ex:item", "ex:color": ["red", "green", "blue", "yellow"]}]
    });
    let ledger1 = fluree.insert(ledger0, &txn).await.unwrap().ledger;

    // Upsert with a single value
    let upsert = json!({
        "@context": ctx,
        "@graph": [{"@id": "ex:item", "ex:color": "purple"}]
    });
    let ledger2 = fluree.upsert(ledger1, &upsert).await.unwrap().ledger;

    let query = json!({
        "@context": ctx,
        "select": {"ex:item": ["*"]}
    });
    let result = support::query_jsonld(&fluree, &ledger2, &query)
        .await
        .unwrap()
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();

    assert_eq!(
        result[0]["ex:color"],
        json!("purple"),
        "upsert should replace 4 values with 1. Got: {}",
        result[0]["ex:color"]
    );
}

/// Upsert with @json values after data has been indexed (binary store path).
///
/// When data lives in the binary index (not just novelty), the query engine may
/// return `EncodedLit` bindings for @json values. If `binding_to_flake_object`
/// doesn't handle these, the retraction is silently skipped → append instead of replace.
#[cfg(feature = "native")]
#[tokio::test]
async fn upsert_json_type_replaces_after_reindex() {
    use fluree_db_api::ReindexOptions;

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tx/upsert-json-indexed:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    // Insert initial @json data
    let initial_txn = json!({
        "@context": ctx(),
        "@graph": [{
            "@id": "ex:config",
            "ex:data": {"@value": {"seed": true, "version": 1}, "@type": "@json"}
        }]
    });
    let _ledger1 = fluree.insert(ledger0, &initial_txn).await.unwrap().ledger;

    // Build a binary index so data moves out of novelty-only
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");

    // Load a fresh LedgerState with binary range_provider
    let handle = fluree.ledger_cached(ledger_id).await.unwrap();
    let ledger_indexed = handle.snapshot().await.to_ledger_state();

    // Verify the loaded state has binary range provider
    assert!(
        ledger_indexed.snapshot.range_provider.is_some(),
        "loaded state should have binary range provider after reindex"
    );

    // Upsert with a different @json value — should replace, not append
    let upsert_txn = json!({
        "@context": ctx(),
        "@graph": [{
            "@id": "ex:config",
            "ex:data": {"@value": {"seed": false, "version": 2}, "@type": "@json"}
        }]
    });
    let ledger2 = fluree
        .upsert(ledger_indexed, &upsert_txn)
        .await
        .unwrap()
        .ledger;

    // Query the result
    let query = json!({
        "@context": ctx(),
        "select": {"ex:config": ["*"]}
    });
    let result = support::query_jsonld(&fluree, &ledger2, &query)
        .await
        .unwrap()
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();

    let config = &result[0];
    let data = &config["ex:data"];

    // After upsert, there should be exactly ONE @json value (the new one)
    assert!(
        !data.is_array(),
        "ex:data should be a single value after upsert (indexed path), not an array. Got: {data}"
    );

    // The @json value may come back as either:
    // - Unwrapped object: {"seed": false, "version": 2}  (novelty path)
    // - Wrapped: {"@value": "{...}", "@type": "rdf:JSON"} (indexed path)
    // Handle both by parsing the inner value if wrapped.
    let inner = if let Some(at_value) = data.get("@value") {
        if let Some(s) = at_value.as_str() {
            serde_json::from_str::<serde_json::Value>(s).expect("parse @json @value string")
        } else {
            at_value.clone()
        }
    } else {
        data.clone()
    };

    assert_eq!(
        inner["version"], 2,
        "should have new version after upsert (indexed path)"
    );
    assert_eq!(
        inner["seed"], false,
        "should have new seed value after upsert (indexed path)"
    );
}
