//! Policy/fuel tracking integration tests
//!
//! These tests focus on the *tracking* surfaces (policy stats + fuel).

use crate::support::{assert_index_defaults, genesis_ledger, seed_people_with_ssn};
use fluree_db_api::policy_builder;
use fluree_db_api::{
    CommitOpts, FlureeBuilder, GovernanceOptions, IndexConfig, PolicyEnforcement,
    TrackedTransactionInput, TxnOpts, TxnType,
};
use serde_json::json;
use std::collections::HashMap;

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

    let qc_opts = GovernanceOptions {
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
            &IndexConfig {
                reindex_min_bytes: 100_000,
                reindex_max_bytes: 1_000_000_000,
            },
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

    // Fuel should be tracked when opts.meta=true. Cost = 10 fuel transaction
    // baseline + 1 micro-fuel per non-schema flake (3 here) = 10.003 fuel.
    assert_eq!(err.fuel, Some(10.003));
}

/// Read-path policy tally: a query whose policies actually execute reports
/// per-policy `executed`/`allowed`, and reports enforcement as active with a
/// non-empty view-policy set.
///
/// Nothing pinned the read path before: the only policy-tally test in the
/// suite was the transaction case above.
#[tokio::test]
async fn read_tracked_under_policies_reports_executed_and_allowed() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let _ = seed_people_with_ssn(&fluree, "policy/track-read:main").await;

    // Two property policies so both halves of the tally are exercised: one
    // that denies every flake it sees, one that allows every flake it sees.
    let policy = json!([
        {
            "@id": "http://example.org/ns/ssnRestriction",
            "f:required": true,
            "f:onProperty": [{"@id": "http://schema.org/ssn"}],
            "f:action": "f:view",
            "f:allow": false
        },
        {
            "@id": "http://example.org/ns/nameGrant",
            "f:onProperty": [{"@id": "http://schema.org/name"}],
            "f:action": "f:view",
            "f:allow": true
        }
    ]);

    let query = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "from": "policy/track-read:main",
        "opts": {"policy": policy, "default-allow": true, "meta": {"policy": true}},
        "select": ["?s", "?name"],
        "where": {"@id": "?s", "@type": "ex:User", "schema:name": "?name"}
    });

    let response = fluree
        .query_connection_tracked(&query)
        .await
        .expect("tracked query");

    let stats = response.policy.expect("policy stats present");
    let grant = stats
        .get("http://example.org/ns/nameGrant")
        .expect("granting policy executed");
    assert!(grant.executed > 0, "granting policy should execute");
    assert_eq!(
        grant.allowed, grant.executed,
        "every flake the granting policy saw should be allowed"
    );

    assert_eq!(
        response.policy_enforcement,
        Some(PolicyEnforcement {
            enforced: true,
            denies_all_data: false,
        }),
        "policies are present, so the view set is not empty"
    );
}

/// The case the reporter hit: an identity with no applicable policies and
/// `default-allow: false`. `policy` is `{}` because nothing ever executes —
/// the enforcement record is what separates this from an anonymous request.
#[tokio::test]
async fn read_tracked_zero_policy_identity_reports_deny_all() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let _ = seed_people_with_ssn(&fluree, "policy/track-deny:main").await;

    let query = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "from": "policy/track-deny:main",
        "opts": {
            "identity": "http://example.org/ns/nobody",
            "default-allow": false,
            "meta": {"policy": true}
        },
        "select": ["?s", "?name"],
        "where": {"@id": "?s", "@type": "ex:User", "schema:name": "?name"}
    });

    let response = fluree
        .query_connection_tracked(&query)
        .await
        .expect("tracked query");

    assert_eq!(response.result, json!([]), "fail-closed: no data rows");
    assert_eq!(
        response.policy,
        Some(HashMap::new()),
        "no policy executes under a default-deny with an empty policy set"
    );
    assert_eq!(
        response.policy_enforcement,
        Some(PolicyEnforcement {
            enforced: true,
            denies_all_data: true,
        }),
    );
}

/// An anonymous request builds no policy context at all. Its output must be
/// exactly what it was before the enforcement record existed: the empty stats
/// map and no enforcement field.
#[tokio::test]
async fn read_tracked_anonymous_reports_no_enforcement() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let _ = seed_people_with_ssn(&fluree, "policy/track-anon:main").await;

    let query = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "from": "policy/track-anon:main",
        "opts": {"meta": {"policy": true}},
        "select": ["?s", "?name"],
        "where": {"@id": "?s", "@type": "ex:User", "schema:name": "?name"}
    });

    let response = fluree
        .query_connection_tracked(&query)
        .await
        .expect("tracked query");

    assert_eq!(
        response.result.as_array().map(Vec::len),
        Some(2),
        "unenforced request sees every row"
    );
    assert_eq!(response.policy, Some(HashMap::new()));
    assert_eq!(
        response.policy_enforcement, None,
        "no policy context was built, so no enforcement is claimed"
    );
}
