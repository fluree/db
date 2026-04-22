//! Policy + multi-ledger (dataset/from array) integration tests
//!
//! Ensures policy enforcement applies correctly when `query_connection` runs in
//! multi-ledger mode (`from: [..]`), which uses the dataset execution path.

use fluree_db_api::FlureeBuilder;
use serde_json::json;

mod support;
use support::assert_index_defaults;

#[tokio::test]
async fn policy_applies_in_multi_ledger_query_connection() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Seed two ledgers with distinct subjects, both having schema:ssn.
    let alias1 = "policy/federation-1:main";
    let alias2 = "policy/federation-2:main";

    let _ = support::seed_user_with_ssn(&fluree, alias1, "ex:alice1", "111-11-1111").await;
    let _ = support::seed_user_with_ssn(&fluree, alias2, "ex:alice2", "222-22-2222").await;

    // Baseline: without policy, multi-ledger query returns 2 rows.
    let q = json!({
        "@context": {"ex":"http://example.org/ns/","schema":"http://schema.org/"},
        "from": [alias1, alias2],
        "select": ["?s", "?ssn"],
        "where": {"@id":"?s", "schema:ssn":"?ssn"},
        "orderBy": "?s"
    });

    let out = fluree.query_connection(&q).await.expect("query baseline");
    // Use ledger1 Db for formatting; JSON-LD formatting uses canonical IRIs for dataset joins.
    let ledger1 = fluree.ledger(alias1).await.expect("ledger1");
    let jsonld = out.to_jsonld(&ledger1.snapshot).expect("to_jsonld");
    let rows = jsonld.as_array().expect("rows");
    assert_eq!(rows.len(), 2, "expected 2 rows without policy: {rows:?}");

    // With policy denying schema:ssn, query should return 0 rows.
    let deny_ssn = json!([{
        "@id": "ex:denySsnPolicy",
        "@type": "f:AccessPolicy",
        "f:action": "f:view",
        "f:onProperty": [{"@id": "http://schema.org/ssn"}],
        "f:allow": false
    }]);

    let q_policy = json!({
        "@context": {"ex":"http://example.org/ns/","schema":"http://schema.org/","f":"https://ns.flur.ee/db#"},
        "from": [alias1, alias2],
        "opts": {"policy": deny_ssn, "default-allow": true},
        "select": ["?s", "?ssn"],
        "where": {"@id":"?s", "schema:ssn":"?ssn"}
    });

    let out = fluree
        .query_connection(&q_policy)
        .await
        .expect("query policy");
    let jsonld = out.to_jsonld(&ledger1.snapshot).expect("to_jsonld policy");
    assert_eq!(jsonld, json!([]), "policy should filter ssn across dataset");
}
