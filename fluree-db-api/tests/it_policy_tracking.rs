//! Policy/fuel tracking integration tests
//!
//! These tests focus on the *tracking* surfaces (policy stats + fuel).

mod support;

use fluree_db_api::policy_builder;
use fluree_db_api::{
    CommitOpts, FlureeBuilder, IndexConfig, QueryConnectionOptions, TrackedTransactionInput,
    TxnOpts, TxnType,
};
use serde_json::json;
use std::collections::HashMap;
use support::{assert_index_defaults, genesis_ledger};

#[tokio::test]
async fn transact_policy_denied_includes_policy_and_fuel_tracking() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // Seed base ledger with identities.
    let ledger0 = genesis_ledger(&fluree, "policy/target:main");
    let seed = json!({
        "@context": { "a": "http://a.co/" },
        "@graph": [
            {"@id":"http://a.co/burt","a:name":"Burt","a:sameAs":{"@id":"http://a.co/burt"}},
            {"@id":"http://a.co/charles","a:name":"Chuck","a:sameAs":{"@id":"http://a.co/charles"}}
        ]
    });
    let ledger = fluree.insert(ledger0, &seed).await.expect("seed").ledger;

    // Inline policy (query-based): identity can only modify itself.
    // This mirrors the shape used by stored f:query policies (stored as JSON string).
    let policy = json!([{
        "@id": "http://a.co/wishlistCreatePolicy",
        "f:action": "f:modify",
        "f:required": true,
        "f:exMessage": "User can only create a wishlist linked to their own identity.",
        "f:onProperty": [{"@id": "http://a.co/wishlist"}],
        "f:query": serde_json::to_string(&json!({
            "@context": { "a": "http://a.co/" },
            "where": [
                {"@id":"?$this","a:sameAs":"?$identity"}
            ]
        }))
        .expect("policy query json string")
    }]);

    let qc_opts = QueryConnectionOptions {
        policy: Some(policy),
        policy_values: Some(HashMap::from([(
            "?$identity".to_string(),
            json!({"@id": "http://a.co/charles"}),
        )])),
        ..Default::default()
    };
    let policy_ctx = policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &qc_opts,
        &[0],
    )
    .await
    .expect("build policy context");

    // Attempt to create a wishlist on Burt as Charles: should be denied.
    let txn = json!({
        "@context": {
            "a": "http://a.co/",
            "f": "https://ns.flur.ee/db#"
        },
        "insert": [
            {
                "@id": "http://a.co/burt",
                "a:wishlist": { "@id": "http://a.co/burt-wish1" }
            },
            {
                "@id": "http://a.co/burt-wish1",
                "a:name": "Burt's Birthday",
                "a:summary": "My birthday wishlist"
            }
        ],
        "opts": { "meta": true }
    });

    let input =
        TrackedTransactionInput::new(TxnType::Update, &txn, TxnOpts::default(), &policy_ctx);
    let err = match fluree
        .transact_tracked_with_policy(
            ledger,
            input,
            CommitOpts::default(),
            &IndexConfig::default(),
        )
        .await
    {
        Ok((_ok, _tally)) => panic!("expected policy denial error"),
        Err(e) => e,
    };

    assert_eq!(
        err.error,
        "User can only create a wishlist linked to their own identity."
    );

    let policy_stats = err.policy.expect("policy stats should be present");
    assert_eq!(
        policy_stats
            .get("http://a.co/wishlistCreatePolicy")
            .unwrap()
            .executed,
        1
    );
    assert_eq!(
        policy_stats
            .get("http://a.co/wishlistCreatePolicy")
            .unwrap()
            .allowed,
        0
    );

    // Fuel should be tracked when opts.meta=true. Cost = 100 fuel transaction
    // baseline + 1 micro-fuel per non-schema flake (3 here) = 100.003 fuel.
    assert_eq!(err.fuel, Some(100.003));
}
