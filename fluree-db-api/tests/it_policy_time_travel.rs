//! Policy + time travel integration tests
//!
//! Ensures view policy enforcement works when `from` includes a `t` time spec
//! (time-travel path in query-connection).

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{assert_index_defaults, genesis_ledger};

#[tokio::test]
async fn policy_applies_to_time_travel_queries() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/time-travel:main";

    // t=1
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let tx1 = json!({
        "@context": {"ex":"http://example.org/ns/","schema":"http://schema.org/"},
        "@graph": [{
            "@id": "ex:alice",
            "@type": "ex:User",
            "schema:name": "Alice",
            "schema:ssn": "111-11-1111"
        }]
    });
    let _ = fluree.insert(ledger0, &tx1).await.expect("tx1");

    // t=2 (any second commit so time-travel path is exercised)
    let ledger1 = fluree.ledger(ledger_id).await.expect("ledger at t=1");
    let tx2 = json!({
        "@context": {"ex":"http://example.org/ns/","schema":"http://schema.org/"},
        "@graph": [{
            "@id": "ex:bob",
            "@type": "ex:User",
            "schema:name": "Bob",
            "schema:ssn": "222-22-2222"
        }]
    });
    let _ = fluree.insert(ledger1, &tx2).await.expect("tx2");

    // Baseline (no policy): time-travel at t=1 should see Alice's SSN.
    let q_ssn_t1 = json!({
        "@context": {"ex":"http://example.org/ns/","schema":"http://schema.org/"},
        "from": {"@id": ledger_id, "t": 1},
        "select": ["?ssn"],
        "where": {"@id":"ex:alice", "schema:ssn":"?ssn"}
    });
    let out = fluree
        .query_connection(&q_ssn_t1)
        .await
        .expect("query ssn t=1 baseline");
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let jsonld = out.to_jsonld(&ledger.snapshot).expect("to_jsonld baseline");
    assert_eq!(jsonld, json!(["111-11-1111"]));

    // Policy: deny schema:ssn but allow everything else.
    let policy = json!([
        {
            "@id": "ex:denySsnPolicy",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:onProperty": [{"@id": "http://schema.org/ssn"}],
            "f:allow": false
        },
        {
            "@id": "ex:allowAllPolicy",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:allow": true
        }
    ]);

    // With policy: requiring ssn should yield 0 rows at t=1.
    let q_ssn_t1_policy = json!({
        "@context": {"ex":"http://example.org/ns/","schema":"http://schema.org/","f":"https://ns.flur.ee/db#"},
        "from": {"@id": ledger_id, "t": 1},
        "opts": {"policy": policy.clone(), "default-allow": true},
        "select": ["?ssn"],
        "where": {"@id":"ex:alice", "schema:ssn":"?ssn"}
    });
    let out = fluree
        .query_connection(&q_ssn_t1_policy)
        .await
        .expect("query ssn t=1 policy");
    let jsonld = out.to_jsonld(&ledger.snapshot).expect("to_jsonld policy");
    assert_eq!(jsonld, json!([]));

    // With policy: non-ssn fields still visible at t=1.
    let q_name_t1_policy = json!({
        "@context": {"ex":"http://example.org/ns/","schema":"http://schema.org/","f":"https://ns.flur.ee/db#"},
        "from": {"@id": ledger_id, "t": 1},
        "opts": {"policy": policy.clone(), "default-allow": true},
        "select": ["?name"],
        "where": {"@id":"ex:alice", "schema:name":"?name"}
    });
    let out = fluree
        .query_connection(&q_name_t1_policy)
        .await
        .expect("query name t=1 policy");
    let jsonld = out
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld name policy");
    assert_eq!(jsonld, json!(["Alice"]));
}
