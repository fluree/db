//! Policy transaction (modify) enforcement tests.
//!
//! Scenario reference: `fluree.snapshot.policy.tx-test`
//!
//! Tests modify-policy enforcement including:
//! - f:onProperty modify policies with f:query
//! - View-only policies blocking all modifications
//! - Custom error messages via f:exMessage

mod support;

use fluree_db_api::policy_builder;
use fluree_db_api::{
    CommitOpts, FlureeBuilder, IndexConfig, QueryConnectionOptions, TrackedTransactionInput,
    TxnOpts, TxnType,
};
use serde_json::json;
use std::collections::HashMap;
use support::{assert_index_defaults, genesis_ledger};

/// Helper to seed test data with users.
async fn seed_users(fluree: &support::MemoryFluree, ledger_id: &str) -> fluree_db_api::LedgerState {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    let txn = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [
            {
                "@id": "http://example.org/ns/alice",
                "@type": "http://example.org/ns/User",
                "http://schema.org/name": "Alice",
                "http://schema.org/email": "alice@flur.ee",
                "http://schema.org/ssn": "111-11-1111"
            },
            {
                "@id": "http://example.org/ns/john",
                "@type": "http://example.org/ns/User",
                "http://schema.org/name": "John",
                "http://schema.org/email": "john@flur.ee",
                "http://schema.org/ssn": "888-88-8888"
            },
            // Identity subjects linking to users
            {
                "@id": "http://example.org/ns/aliceIdentity",
                "http://example.org/ns/user": {"@id": "http://example.org/ns/alice"}
            },
            {
                "@id": "http://example.org/ns/johnIdentity",
                "http://example.org/ns/user": {"@id": "http://example.org/ns/john"}
            }
        ]
    });

    fluree.insert(ledger0, &txn).await.unwrap().ledger
}

/// Test: User can modify their own email (property policy allows)
///
/// property-policy-tx-enforcement (john-allowed case)
#[tokio::test]
async fn modify_policy_allows_own_property() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_users(&fluree, "modify_policy_allows_own").await;

    // Policy: only identity's own user can modify their email
    let policy = json!([
        {
            "@id": "ex:emailRestriction",
            "f:required": true,
            "f:onProperty": [{"@id": "http://schema.org/email"}],
            "f:action": [{"@id": "f:modify"}],
            "f:exMessage": "Only users can update their own emails.",
            "f:query": serde_json::to_string(&json!({
                "where": {
                    "@id": "?$identity",
                    "http://example.org/ns/user": {"@id": "?$this"}
                }
            })).unwrap()
        },
        {
            "@id": "ex:defaultAllow",
            "f:action": [{"@id": "f:view"}, {"@id": "f:modify"}],
            "f:allow": true
        }
    ]);

    // Build policy context with John as identity
    let qc_opts = QueryConnectionOptions {
        policy: Some(policy),
        policy_values: Some(HashMap::from([(
            "?$identity".to_string(),
            json!({"@id": "http://example.org/ns/johnIdentity"}),
        )])),
        default_allow: false,
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

    // John updating his own email - should succeed
    let update_txn = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "where": {
            "@id": "ex:john",
            "schema:email": "?email"
        },
        "delete": {
            "@id": "ex:john",
            "schema:email": "?email"
        },
        "insert": {
            "@id": "ex:john",
            "schema:email": "updated@flur.ee"
        }
    });

    // This should succeed - John is updating his own email
    let input = TrackedTransactionInput::new(
        TxnType::Update,
        &update_txn,
        TxnOpts::default(),
        &policy_ctx,
    );
    let result = fluree
        .transact_tracked_with_policy(
            ledger,
            input,
            CommitOpts::default(),
            &IndexConfig {
                reindex_min_bytes: 100_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await;

    assert!(
        result.is_ok(),
        "John should be allowed to update his own email: {:?}",
        result.err()
    );

    // Verify the update happened
    let (tx_result, _tally) = result.unwrap();
    let query = json!({
        "select": "?email",
        "where": {
            "@id": "http://example.org/ns/john",
            "http://schema.org/email": "?email"
        }
    });

    let query_result = support::query_jsonld(&fluree, &tx_result.ledger, &query)
        .await
        .unwrap();
    let rows = query_result.to_jsonld(&tx_result.ledger.snapshot).unwrap();
    let arr = rows.as_array().unwrap();

    assert_eq!(arr.len(), 1, "Should have 1 row, got: {arr:?}");
    assert_eq!(
        arr[0], "updated@flur.ee",
        "Email should be updated, got: {arr:?}"
    );
}

/// Test: User cannot modify another user's email (property policy denies)
///
/// property-policy-tx-enforcement (alice-not-allowed case)
#[tokio::test]
async fn modify_policy_denies_other_property() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_users(&fluree, "modify_policy_denies_other").await;

    // Policy: only identity's own user can modify their email
    let policy = json!([
        {
            "@id": "ex:emailRestriction",
            "f:required": true,
            "f:onProperty": [{"@id": "http://schema.org/email"}],
            "f:action": [{"@id": "f:modify"}],
            "f:exMessage": "Only users can update their own emails.",
            "f:query": serde_json::to_string(&json!({
                "where": {
                    "@id": "?$identity",
                    "http://example.org/ns/user": {"@id": "?$this"}
                }
            })).unwrap()
        },
        {
            "@id": "ex:defaultAllow",
            "f:action": [{"@id": "f:view"}, {"@id": "f:modify"}],
            "f:allow": true
        }
    ]);

    // Build policy context with Alice as identity
    let qc_opts = QueryConnectionOptions {
        policy: Some(policy),
        policy_values: Some(HashMap::from([(
            "?$identity".to_string(),
            json!({"@id": "http://example.org/ns/aliceIdentity"}),
        )])),
        default_allow: false,
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

    // Alice trying to update John's email - should fail
    let update_txn = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "where": {
            "@id": "ex:john",
            "schema:email": "?email"
        },
        "delete": {
            "@id": "ex:john",
            "schema:email": "?email"
        },
        "insert": {
            "@id": "ex:john",
            "schema:email": "hacked@evil.com"
        }
    });

    // This should fail - Alice is not allowed to update John's email
    let input = TrackedTransactionInput::new(
        TxnType::Update,
        &update_txn,
        TxnOpts::default(),
        &policy_ctx,
    );
    let result = fluree
        .transact_tracked_with_policy(
            ledger,
            input,
            CommitOpts::default(),
            &IndexConfig {
                reindex_min_bytes: 100_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await;

    assert!(
        result.is_err(),
        "Alice should NOT be allowed to update John's email"
    );

    let err = result.unwrap_err();

    // Should contain the custom error message
    assert_eq!(
        err.error, "Only users can update their own emails.",
        "Error should be the custom f:exMessage"
    );
}

/// Test: View-only policy blocks all modifications
///
/// view-only-policy-restricts-tx (first test case)
#[tokio::test]
async fn view_only_policy_blocks_modify() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "view_only_blocks");

    // Insert some data
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "insert": {
            "@id": "ex:alice",
            "ex:name": "Alice"
        }
    });

    let ledger = fluree.update(ledger0, &txn).await.unwrap().ledger;

    // Policy: only allows view, no modify
    let policy = json!([{
        "@id": "ex:viewOnly",
        "f:action": [{"@id": "f:view"}],
        "f:allow": true
    }]);

    // Build policy context (no modify policies)
    let qc_opts = QueryConnectionOptions {
        policy: Some(policy),
        default_allow: false,
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

    // Try to insert new data - should fail
    let update_txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "insert": {
            "@id": "ex:john",
            "ex:name": "John"
        }
    });

    let input = TrackedTransactionInput::new(
        TxnType::Update,
        &update_txn,
        TxnOpts::default(),
        &policy_ctx,
    );
    let result = fluree
        .transact_tracked_with_policy(
            ledger,
            input,
            CommitOpts::default(),
            &IndexConfig {
                reindex_min_bytes: 100_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await;

    assert!(
        result.is_err(),
        "View-only policy should block modifications, but got: {result:?}"
    );

    let err = result.unwrap_err();
    // "Database policy denies all modifications."
    // Rust uses: "Policy enforcement prevents modification."
    assert!(
        err.error.contains("denied")
            || err.error.contains("Policy")
            || err.error.contains("modification"),
        "Error should indicate policy denial: {}",
        err.error
    );
}

/// Test: Always-false modify query denies with custom message
///
/// view-only-policy-restricts-tx (second test case)
#[tokio::test]
async fn modify_query_always_false_denies() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "modify_always_false");

    // Insert some data
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "insert": {
            "@id": "ex:alice",
            "ex:name": "Alice"
        }
    });

    let ledger = fluree.update(ledger0, &txn).await.unwrap().ledger;

    // Policy: modify query always returns false (impossible pattern)
    let policy = json!([
        {
            "@id": "ex:alwaysFalseModify",
            "f:required": true,
            "f:action": [{"@id": "f:modify"}],
            "f:exMessage": "Sample policy always returns false - denied!",
            "f:query": serde_json::to_string(&json!({
                "where": {"http://nonexistent.org/blah": "?$this"}
            })).unwrap()
        },
        {
            "@id": "ex:viewAll",
            "f:action": [{"@id": "f:view"}],
            "f:allow": true
        }
    ]);

    // Build policy context
    let qc_opts = QueryConnectionOptions {
        policy: Some(policy),
        default_allow: false,
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

    // Try to insert new data - should fail with custom message
    let update_txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "insert": {
            "@id": "ex:john",
            "ex:name": "John"
        }
    });

    let input = TrackedTransactionInput::new(
        TxnType::Update,
        &update_txn,
        TxnOpts::default(),
        &policy_ctx,
    );
    let result = fluree
        .transact_tracked_with_policy(
            ledger,
            input,
            CommitOpts::default(),
            &IndexConfig {
                reindex_min_bytes: 100_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await;

    assert!(result.is_err(), "Always-false modify query should deny");

    let err = result.unwrap_err();
    // The custom message should be in the error
    assert_eq!(
        err.error, "Sample policy always returns false - denied!",
        "Error should be the custom f:exMessage"
    );
}

/// V4 regression: a transaction WHERE clause is a READ and must be filtered by
/// the requesting identity's view policy. Otherwise a writer with modify access
/// to some property can launder hidden values by conditionally writing based on
/// a WHERE match against data they cannot view — e.g.
/// `INSERT { ?s ex:exposed true } WHERE { ?s ex:salary ?sal }` reveals which
/// subjects have a salary even though the identity can't view `ex:salary`.
///
/// The fix attaches the view enforcer to the WHERE dataset, so the match phase
/// sees only viewable flakes. This test isolates the read filter: there is NO
/// modify restriction, so the writes themselves are always permitted — only the
/// WHERE read is constrained.
#[tokio::test]
async fn where_read_respects_view_policy() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "where_view_policy");

    // Seed two users with a hidden salary and a visible name.
    let seed = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:User", "ex:name": "Alice", "ex:salary": 100},
            {"@id": "ex:bob",   "@type": "ex:User", "ex:name": "Bob",   "ex:salary": 500}
        ]
    });
    let ledger = fluree.insert(ledger0, &seed).await.unwrap().ledger;

    let index_cfg = IndexConfig {
        reindex_min_bytes: 100_000,
        reindex_max_bytes: 1_000_000_000,
    };

    // --- Baseline (no policy): the salary-probe WHERE matches both users and
    // writes the flag, confirming the attack query is valid and WOULD leak
    // without view enforcement. ---
    let baseline = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where":  {"@id": "?s", "ex:salary": "?sal"},
        "insert": {"@id": "?s", "ex:rootexposed": true}
    });
    let ledger = fluree.update(ledger, &baseline).await.unwrap().ledger;
    let rootexposed = support::query_jsonld(
        &fluree,
        &ledger,
        &json!({"select": "?s", "where": {"@id": "?s", "http://example.org/ns/rootexposed": "?e"}}),
    )
    .await
    .unwrap()
    .to_jsonld(&ledger.snapshot)
    .unwrap();
    assert_eq!(
        rootexposed.as_array().map(Vec::len).unwrap_or(0),
        2,
        "baseline: an unrestricted salary-probe WHERE must match both users: {rootexposed:#?}"
    );

    // Everything is viewable + modifiable by default EXCEPT ex:salary, which a
    // required view-deny hides. No modify policy → writes are unconstrained, so
    // any blocked write is purely the WHERE read being filtered.
    let policy = json!([{
        "@id": "ex:salaryHidden",
        "f:required": true,
        "f:onProperty": [{"@id": "http://example.org/ns/salary"}],
        "f:action": [{"@id": "f:view"}],
        "f:allow": false
    }]);
    let qc_opts = QueryConnectionOptions {
        policy: Some(policy),
        default_allow: true,
        ..Default::default()
    };

    // --- Attack under the restricted identity: salary is view-denied, so the
    // WHERE matches nothing → no flakes → an empty transaction. The flag is
    // never written. (Before the fix the WHERE matched both users and committed
    // ex:exposed.) The attack errors and does not advance the ledger, so the
    // control below runs on a clone of the pre-attack state. ---
    let policy_ctx = policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &qc_opts,
        &[0],
    )
    .await
    .expect("build policy context (attack)");

    let attack = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where":  {"@id": "?s", "ex:salary": "?sal"},
        "insert": {"@id": "?s", "ex:exposed": true}
    });
    let input = TrackedTransactionInput::new(TxnType::Update, &attack, TxnOpts::default(), &policy_ctx);
    let attack_result = fluree
        .transact_tracked_with_policy(ledger.clone(), input, CommitOpts::default(), &index_cfg)
        .await;
    let err = attack_result.expect_err("salary-probe WHERE must be fully view-filtered (empty tx)");
    assert!(
        err.error.contains("Empty transaction") || err.error.contains("no flakes"),
        "expected an empty transaction (WHERE matched nothing under view policy), got: {}",
        err.error
    );

    // --- Control: probe a VIEWABLE property; the WHERE must still match both
    // users (no over-filtering). ---
    let policy_ctx = policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &qc_opts,
        &[0],
    )
    .await
    .expect("build policy context (control)");

    let control = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where":  {"@id": "?s", "ex:name": "?n"},
        "insert": {"@id": "?s", "ex:greeted": true}
    });
    let input = TrackedTransactionInput::new(TxnType::Update, &control, TxnOpts::default(), &policy_ctx);
    let (control_result, _) = fluree
        .transact_tracked_with_policy(ledger, input, CommitOpts::default(), &index_cfg)
        .await
        .expect("control transaction should succeed (ex:name is viewable)");
    let ledger = control_result.ledger;

    let greeted = support::query_jsonld(
        &fluree,
        &ledger,
        &json!({"select": "?s", "where": {"@id": "?s", "http://example.org/ns/greeted": "?g"}}),
    )
    .await
    .unwrap()
    .to_jsonld(&ledger.snapshot)
    .unwrap();
    assert_eq!(
        greeted.as_array().map(Vec::len).unwrap_or(0),
        2,
        "WHERE over a viewable property must still match (no over-filtering): {greeted:#?}"
    );
}

/// W1/W2 regression: writing data as Turtle must enforce f:modify policy, just
/// like the JSON-LD/SPARQL paths. Before the fix the Turtle write path never
/// carried the PolicyContext into staging, so an authorized writer could bypass
/// fine-grained modify rules entirely by sending the same data as Turtle.
#[tokio::test]
async fn turtle_insert_enforces_modify_policy() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "policy/turtle-modify:main");

    // Seed ex:secret so the property exists in the namespace table; the policy
    // must encode the same SID the write produces.
    let seed = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:item0", "ex:secret": "existing"}]
    });
    let ledger = fluree.insert(ledger0, &seed).await.unwrap().ledger;

    // Deny modifying ex:secret; everything else writable (default-allow: true),
    // so the only thing blocking the write is the f:modify rule on ex:secret.
    let policy = json!([{
        "@id": "ex:noSecretWrite",
        "f:required": true,
        "f:onProperty": [{"@id": "http://example.org/ns/secret"}],
        "f:action": "f:modify",
        "f:allow": false
    }]);
    let qc_opts = QueryConnectionOptions {
        policy: Some(policy),
        default_allow: true,
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

    let turtle = "@prefix ex: <http://example.org/ns/> .\nex:item1 ex:secret \"classified\" .\n";
    let cfg = IndexConfig {
        reindex_min_bytes: 100_000,
        reindex_max_bytes: 1_000_000_000,
    };

    // With the modify policy: the Turtle write touches a denied property → rejected.
    let denied = fluree
        .insert_turtle_with_opts(
            ledger.clone(),
            turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &cfg,
            Some(&policy_ctx),
        )
        .await;
    assert!(
        denied.is_err(),
        "Turtle write of a modify-denied property must be rejected, got: {denied:?}"
    );

    // Without a policy (root): the identical Turtle write succeeds.
    let ok = fluree
        .insert_turtle_with_opts(
            ledger,
            turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &cfg,
            None,
        )
        .await;
    assert!(
        ok.is_ok(),
        "Turtle write must succeed without a modify policy, got: {:?}",
        ok.err()
    );
}
